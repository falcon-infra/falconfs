/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "connection_pool/pg_connection_pool.h"

#include "connection_pool/pg_connection.h"
#include "connection_pool/connection_pool_config.h"

void PGConnectionPool::BackgroundPoolManager()
{
    std::vector<falcon::meta_proto::AsyncMetaServiceJob *> notSupportTasks;
    notSupportTasks.reserve(FalconConnectionPoolBatchSize);
    int waitTime = 100; // microseconds
    while (working) {
        if (!working) break;

        int maxCount = 0;
        bool withTasks = true;
        while (withTasks) {
            int emptyCount = 0;
            for (int i = 0; i <= TaskSupportBatchType::NOT_SUPPORT; ++i) {
                int queueSizeApprox = supportBatchTaskList[i].task->jobList.size_approx();
                if (queueSizeApprox == 0) {
                    emptyCount++;
                    continue;
                }
                maxCount = std::max(maxCount, queueSizeApprox);
                int toDequeue = std::min(queueSizeApprox, FalconConnectionPoolBatchSize);
                if (i < TaskSupportBatchType::NOT_SUPPORT) {
                    BatchDequeueExec(toDequeue, i);
                } else {
                    SingleDequeueExec(toDequeue, notSupportTasks);
                }
            }
            if (emptyCount == TaskSupportBatchType::NOT_SUPPORT + 1) {
                withTasks = false;
            }
        }
        // determine the amount to batch
        waitTime = AdjustWaitTime(waitTime, maxCount);
        std::this_thread::sleep_for(std::chrono::microseconds(waitTime));
    }
}

int PGConnectionPool::AdjustWaitTime(int prevTime, size_t reqInLoop)
{
    if (FalconConnectionPoolWaitAdjust == 0) {
        return prevTime;
    }
    if (reqInLoop <= size_t(FalconConnectionPoolBatchSize * 2)) {
        return std::min(prevTime * 2, FalconConnectionPoolWaitMax);
    } else {
        return std::max(prevTime / 2, FalconConnectionPoolWaitMin);
    }
}

int PGConnectionPool::BatchDequeueExec(int toDequeue, int queueIndex)
{
    auto taskVecPtr = std::make_shared<WorkerTask>();
    taskVecPtr->isBatch = true;
    taskVecPtr->jobList.reserve(toDequeue);
    int count = supportBatchTaskList[queueIndex].task->jobList.try_dequeue_bulk(
        std::back_inserter(taskVecPtr->jobList), 
        toDequeue
    );
    if (count == 0) {
        return 0;
    }
    PGConnection *conn = GetPGConnection(); // get idle connection, may block
    conn->Exec(taskVecPtr);
    return count;
}

int PGConnectionPool::SingleDequeueExec(int toDequeue, std::vector<falcon::meta_proto::AsyncMetaServiceJob *> &tasksContainer)
{
    tasksContainer.clear();
    size_t count = supportBatchTaskList[TaskSupportBatchType::NOT_SUPPORT].task->jobList.try_dequeue_bulk(
        std::back_inserter(tasksContainer), 
        toDequeue
    );
    if (count == 0) {
        return 0;
    }
    for (auto &e : tasksContainer) {
        PGConnection *conn = GetPGConnection();
        auto taskVecPtr = std::make_shared<WorkerTask>();
        taskVecPtr->jobList.emplace_back(e);
        conn->Exec(taskVecPtr);
    }
    return count;
}

PGConnectionPool::PGConnectionPool(const uint16_t port,
                                   const char *userName,
                                   const int connPoolSize,
                                   const uint16_t pendingTaskBufferMaxSize,
                                   const uint16_t batchTaskBufferMaxSize)
{
    for (int i = 0; i < connPoolSize; ++i) {
        PGConnection *conn = new PGConnection(this, "127.0.0.1", port, userName);
        currentManagedConn.insert(conn);
        connPool.push(conn);
    }
    this->pendingTaskBufferMaxSize = pendingTaskBufferMaxSize;
    this->batchTaskBufferMaxSize = batchTaskBufferMaxSize;

    for (int i = 0; i <= TaskSupportBatchType::NOT_SUPPORT; ++i) {
        supportBatchTaskList[i].task = new Task(batchTaskBufferMaxSize);
        supportBatchTaskList[i].task->isBatch = (i != TaskSupportBatchType::NOT_SUPPORT);
    }

    working = true;
    backgroundPoolManager = std::thread(&PGConnectionPool::BackgroundPoolManager, this);
}

void PGConnectionPool::ReaddWorkingPGConnection(PGConnection *conn)
{
    {
        std::unique_lock<std::mutex> lk(connPoolMutex);
        connPool.push(conn);
    }
    cvPoolNotEmpty.notify_one();
}

PGConnection *PGConnectionPool::GetPGConnection()
{
    PGConnection *result = NULL;
    {
        std::unique_lock<std::mutex> lk(connPoolMutex);
        cvPoolNotEmpty.wait(lk, [this]() -> bool { return !this->connPool.empty(); });

        result = connPool.front();
        connPool.pop();
    }
    return result;
}

// lifetime of job must be longer than this function. it will be freed later
void PGConnectionPool::DispatchAsyncMetaServiceJob(falcon::meta_proto::AsyncMetaServiceJob *job)
{
    // we only allow batch with others if:
    // 1. explicit allow batch with others
    // 2. all of the operations have a same type
    // 3. operation type support batch
    const falcon::meta_proto::MetaRequest *request = job->GetRequest();
    if (request->type_size() == 0)
        return;
    falcon::meta_proto::MetaServiceType type = request->type(0);
    TaskSupportBatchType taskSupportBatchType = ConvertMetaServiceTypeToTaskSupportBatchType(type);
    bool allowBatchWithOthers =
        request->allow_batch_with_others() && taskSupportBatchType != TaskSupportBatchType::NOT_SUPPORT;
    if (allowBatchWithOthers) {
        for (int i = 1; i < request->type_size(); ++i)
            if (request->type(i) != type) {
                allowBatchWithOthers = false;
                break;
            }
    }

    if (allowBatchWithOthers) {
        while (!supportBatchTaskList[taskSupportBatchType].task->jobList.enqueue(job)) {
            std::cout << "DispatchAsyncMetaServiceJob: enqueue failed, type = " << taskSupportBatchType << std::endl;
            std::this_thread::yield();
        }
    } else {
        while (!supportBatchTaskList[TaskSupportBatchType::NOT_SUPPORT].task->jobList.enqueue(job)) {
            std::cout << "DispatchAsyncMetaServiceJob: enqueue failed, type = " << taskSupportBatchType << std::endl;
            std::this_thread::yield();
        }
    }
}

void PGConnectionPool::Stop()
{
    working = false;
}

PGConnectionPool::~PGConnectionPool()
{
    Stop();
    for (auto it = currentManagedConn.begin(); it != currentManagedConn.end(); ++it) {
        (*it)->Stop();
    }
    for (auto it = currentManagedConn.begin(); it != currentManagedConn.end(); ++it) {
        delete (*it);
    }
    backgroundPoolManager.join();
    currentManagedConn.clear();
}
