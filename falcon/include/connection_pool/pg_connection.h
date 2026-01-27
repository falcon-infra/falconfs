/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef FALCON_POOLER_PG_CONNECTION_H
#define FALCON_POOLER_PG_CONNECTION_H

#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>
#include <boost/thread/concurrent_queues/sync_queue.hpp>
#include <flatbuffers/flatbuffers.h>
#include "falcon_worker_task.h"
#include "libpq-fe.h"
#include "remote_connection_utils/serialized_data.h"

class PGConnection {
  private:
    typedef std::function<void(PGConnection *conn)> PGConnectionWorkFinishNotifyFunc;
    bool working;
    PGConnectionWorkFinishNotifyFunc m_workerFinishNotifyFunc;
    flatbuffers::FlatBufferBuilder flatBufferBuilder;
    SerializedData replyBuilder;

    boost::concurrent::sync_queue<std::shared_ptr<BaseWorkerTask>> m_workerTaskQueue;
    std::thread thread;
    PGconn *conn;

  public:
    PGConnection(PGConnectionWorkFinishNotifyFunc func, const char *ip, const int port, const char *userName);
    ~PGConnection();

    void BackgroundWorker();

    void Exec(std::shared_ptr<BaseWorkerTask> taskToExec);

    void Stop();
};

#endif
