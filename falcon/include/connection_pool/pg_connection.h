/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef FALCON_POOLER_PG_CONNECTION_H
#define FALCON_POOLER_PG_CONNECTION_H

#include <flatbuffers/flatbuffers.h>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>
#include <memory>
#include "concurrentqueue/blockingconcurrentqueue.h"
#include "connection_pool/pg_connection_pool.h"
#include "libpq-fe.h"
#include "remote_connection_utils/serialized_data.h"

class PGConnectionPool;

class PGConnection {
  private:
    bool working;

    PGConnectionPool *parent;

    flatbuffers::FlatBufferBuilder flatBufferBuilder;
    SerializedData replyBuilder;

    std::shared_ptr<WorkerTask> taskToExec;
    moodycamel::BlockingConcurrentQueue<std::shared_ptr<WorkerTask>> tasksToExec;
    std::thread thread;

  public:
    PGconn *conn;

    PGConnection(PGConnectionPool *parent, const char *ip, const int port, const char *userName);
    ~PGConnection();

    void BackgroundWorker();

    void Exec(std::shared_ptr<WorkerTask> taskToExec);

    void Stop();
};

#endif
