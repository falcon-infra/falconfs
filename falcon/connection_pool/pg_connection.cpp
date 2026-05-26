/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "connection_pool/pg_connection.h"
#include <iostream>
#include <sstream>
#include "falcon_meta_param_generated.h"
#include "falcon_meta_response_generated.h"

PGConnection::PGConnection(PGConnectionWorkFinishNotifyFunc func, const char *ip, const int port, const char *userName)
{
    m_workerFinishNotifyFunc = func;
    working.store(true);
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
    while (working.load()) {
        std::shared_ptr<BaseWorkerTask> baseWorkerTaskPtr(nullptr);
        boost::concurrent::queue_op_status status = m_workerTaskQueue.wait_pull(baseWorkerTaskPtr);
        if (status == boost::concurrent::queue_op_status::closed || !working.load()) {
            break;
        }
        if (baseWorkerTaskPtr == nullptr) {
            continue;
        }
        try {
            baseWorkerTaskPtr->DoWork(conn, flatBufferBuilder, replyBuilder);
        } catch (const std::exception &) {
            PGresult *res;
            while ((res = PQgetResult(conn)) != NULL)
                PQclear(res);
        }
        // now no one handle the ptr, auto release WorkerTask
        baseWorkerTaskPtr = nullptr;

        // notify worker finish and ready for an new work.
        m_workerFinishNotifyFunc(this);
    }
}

void PGConnection::Exec(std::shared_ptr<BaseWorkerTask> workerTaskPtr)
{
    if (!working.load()) {
        return;
    }
    this->m_workerTaskQueue.wait_push(workerTaskPtr);
}

void PGConnection::Stop()
{
    if (working.exchange(false)) {
        m_workerTaskQueue.close();
    }
}

PGConnection::~PGConnection()
{
    Stop();
    if (thread.joinable()) {
        thread.join();
    }
    if (conn) {
        PQfinish(conn);
        conn = nullptr;
    }
    SerializedDataDestroy(&replyBuilder);
}
