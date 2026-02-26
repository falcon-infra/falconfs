/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "connection_pool/pg_connection.h"
#include <sstream>
#include "falcon_meta_param_generated.h"
#include "falcon_meta_response_generated.h"
#include "utils/error_log.h"

PGConnection::PGConnection(PGConnectionWorkFinishNotifyFunc func, const char *ip, const int port, const char *userName)
{
    m_workerFinishNotifyFunc = func;
    working = true;
    std::stringstream ss;
    ss << "hostaddr=" << ip << " port=" << port << " user=" << userName << " dbname=postgres";
    conn = PQconnectdb(ss.str().c_str());
    if (PQstatus(conn) != CONNECTION_OK) {
        throw std::runtime_error(std::string("pg connection error: ") + PQerrorMessage(conn));
    }
    PGresult *res = PQexec(conn, "SELECT falcon_prepare_commands();");
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        throw std::runtime_error(std::string("pg connection error: ") + PQresultErrorMessage(res));
    }

    SerializedDataInit(&replyBuilder, NULL, 0, 0, NULL);
    this->thread = std::thread(&PGConnection::BackgroundWorker, this);
}

void PGConnection::BackgroundWorker()
{
    while (working) {
        if (!working)
            break;
        std::shared_ptr<BaseWorkerTask> baseWorkerTaskPtr(nullptr);
        m_workerTaskQueue.pull(baseWorkerTaskPtr);
        baseWorkerTaskPtr->DoWork(conn, flatBufferBuilder, replyBuilder);
        // now no one handle the ptr, auto release WorkerTask
        baseWorkerTaskPtr = nullptr;

        // notify worker finish and ready for an new work.
        m_workerFinishNotifyFunc(this);
    }
}

void PGConnection::Exec(std::shared_ptr<BaseWorkerTask> workerTaskPtr)
{
    this->m_workerTaskQueue.push(workerTaskPtr);
}

void PGConnection::Stop() { working = false; }

PGConnection::~PGConnection()
{
    Stop();
    thread.join();
    if (conn) {
        PQfinish(conn);
        conn = nullptr;
    }
    SerializedDataDestroy(&replyBuilder);
}
