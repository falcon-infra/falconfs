/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "connection_pool/pg_connection_pool.h"
#include <atomic>
#include <condition_variable>
#include <iostream>
#include <memory>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "base_comm_adapter/base_meta_service_job.h"
#include "connection_pool/connection_pool_config.h"
#include "connection_pool/falcon_batch_service_def.h"
#include "connection_pool/falcon_worker_task.h"
#include "connection_pool/pg_connection.h"
#include "connection_pool/falcon_concurrent_queue.h"
#include "perf_counter/perf_stat.h"

class PGConnectionPool {
  private:
    std::unordered_set<PGConnection *> currentManagedConn;

    bool working;

    std::queue<PGConnection *> connPool;
    std::mutex connPoolMutex;
    std::condition_variable cvPoolNotEmpty;

    std::mutex pendingTaskMutex;
    std::condition_variable cvPendingTaskNotEmpty;
    std::condition_variable cvPendingTaskNotFull;
    uint16_t pendingTaskBufferMaxSize;

    class TaskSupportBatch {
      public:
        pg_connection_pool::ConcurrentQueue<BaseMetaServiceJob *> jobList;
        std::mutex taskMutex;
        std::condition_variable cvBatchNotFull;
    };
    TaskSupportBatch supportBatchTaskList[int(FalconBatchServiceType::END)];
    uint16_t batchTaskBufferMaxSize;

    std::thread backgroundPoolManager;

    // define private construct function to avoid create single instance
    PGConnectionPool() = default;

    // get one idle connection to do work
    PGConnection *GetPGConnection();

    // background function for create work task and dispatch work task to idle connection.
    void BackgroundPoolManager();

    // create batch job work task and dispatch to connection
    int BatchDequeueExec(int toDequeue, int queueIndex);

    // create single job work task and dispatch to connection
    int SingleDequeueExec(int toDequeue);

    // adjust sleep interval while no jobs waiting to work
    int AdjustWaitTime(int prevTime, size_t reqInLoop);

  public:
    ~PGConnectionPool() = default;

    // single instance interface
    static PGConnectionPool &GetInstance()
    {
        static PGConnectionPool pgConnectionPool;
        return pgConnectionPool;
    }

    // interface for communication server to call, used to dispatch meta service job to connection pool
    void DispatchMetaServiceJob(BaseMetaServiceJob *job);

    bool Init(const uint16_t port,
              const char *userName,
              const int connPoolSize,
              const uint16_t pendingTaskBufferMaxSize,
              const uint16_t batchTaskBufferMaxSize);
    void Destroy();
};

void PGConnectionPool::BackgroundPoolManager()
{
    int waitTime = 100; // microseconds
    while (working) {
        if (!working)
            break;

        int maxCount = 0;
        bool withTasks = true;
        while (withTasks) {
            int emptyCount = 0;
            for (int i = 0; i <= (int)FalconBatchServiceType::NOT_SUPPORT; ++i) {
                int queueSizeApprox = supportBatchTaskList[i].jobList.size_approx();
                if (queueSizeApprox == 0) {
                    emptyCount++;
                    continue;
                }
                maxCount = std::max(maxCount, queueSizeApprox);
                int toDequeue = std::min(queueSizeApprox, FalconConnectionPoolBatchSize);
                if (i < (int)FalconBatchServiceType::NOT_SUPPORT) {
                    BatchDequeueExec(toDequeue, i);
                } else {
                    SingleDequeueExec(toDequeue);
                }
            }
            if (emptyCount == (int)FalconBatchServiceType::NOT_SUPPORT + 1) {
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
    std::vector<BaseMetaServiceJob *> jobList;
    jobList.reserve(toDequeue);
    std::function func = [&jobList](BaseMetaServiceJob *job) {
        jobList.emplace_back(job);
    };
    size_t count = supportBatchTaskList[queueIndex].jobList.dequeue_bulk(
        std::move(func),
        toDequeue
    );
    if (count == 0) {
        return 0;
    }

    /* Report inQueueLatency and restart timer for connWait measurement */
    for (auto &job : jobList) {
        job->stageTimer.EndAndRestart(GetInQueueLatencyData());
    }

    auto workerTaskPtr = std::make_shared<BatchWorkerTask>(GetFalconConnectionPoolShmemAllocator(), jobList);
    if (workerTaskPtr == nullptr) {
        throw std::runtime_error("BatchDequeueExec make_shared<BatchWorkerTask> failed, out of memory.");
    }

    PGConnection *conn = GetPGConnection(); // get idle connection, may block

    /* Report connWaitLatency and restart timer for workerWait measurement */
    for (auto &job : jobList) {
        job->stageTimer.EndAndRestart(GetConnWaitLatencyData());
    }

    conn->Exec(workerTaskPtr);
    return count;
}

int PGConnectionPool::SingleDequeueExec(int toDequeue)
{
    std::vector<BaseMetaServiceJob *> singleJobList;
    singleJobList.reserve(toDequeue);
    std::function func = [&singleJobList](BaseMetaServiceJob *job) {
        singleJobList.emplace_back(job);
    };
    size_t count = supportBatchTaskList[(int)FalconBatchServiceType::NOT_SUPPORT].jobList.dequeue_bulk(
        std::move(func),
        toDequeue
    );
    if (count == 0) {
        return 0;
    }

    /* Report inQueueLatency and restart timer for connWait measurement */
    for (auto &job : singleJobList) {
        job->stageTimer.EndAndRestart(GetInQueueLatencyData());
    }

    for (auto &job : singleJobList) {
        auto workerTaskPtr = std::make_shared<SingleWorkerTask>(GetFalconConnectionPoolShmemAllocator(), job);
        if (workerTaskPtr == nullptr) {
            throw std::runtime_error("BatchDequeueExec make_shared<BatchWorkerTask> failed, out of memory.");
        }
        PGConnection *conn = GetPGConnection(); // get idle connection, may block

        /* Report connWaitLatency and restart timer for workerWait measurement */
        job->stageTimer.EndAndRestart(GetConnWaitLatencyData());

        conn->Exec(workerTaskPtr);
    }
    return count;
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
void PGConnectionPool::DispatchMetaServiceJob(BaseMetaServiceJob *job)
{
    // we only allow batch with others if:
    // 1. explicit allow batch with others
    // 2. all of the operations have a same type
    // 3. operation type support batch
    if (job->IsEmptyRequest()) {
        return;
    }

    FalconMetaServiceType falconSupportType = job->GetFalconMetaServiceType(0);
    FalconBatchServiceType FalconBatchServiceType = job->IsAllowBatchProcess()
                                                        ? FalconMetaServiceTypeToBatchServiceType(falconSupportType)
                                                        : FalconBatchServiceType::NOT_SUPPORT;

    job->opcodeForE2E = falconSupportType;

    job->e2eTimer.Start();

    /* Start latency timer for inQueue measurement */
    job->stageTimer.Start();

    while (!supportBatchTaskList[(int)FalconBatchServiceType].jobList.enqueue(job)) {
        std::cout << "DispatchMetaServiceJob: enqueue failed, type = " << (int)FalconBatchServiceType << std::endl;
        std::this_thread::yield();
    }
}

bool PGConnectionPool::Init(const uint16_t port,
                            const char *userName,
                            const int connPoolSize,
                            const uint16_t pendingTaskBufferMaxSize,
                            const uint16_t batchTaskBufferMaxSize)
{
    auto workerFinishNotifyFunc = [this](PGConnection *conn) {
        {
            std::unique_lock<std::mutex> lk(connPoolMutex);
            connPool.push(conn);
        }
        cvPoolNotEmpty.notify_one();
    };

    for (int i = 0; i < connPoolSize; ++i) {
        PGConnection *conn = new PGConnection(workerFinishNotifyFunc, "127.0.0.1", port, userName);
        currentManagedConn.insert(conn);
        connPool.push(conn);
    }
    this->pendingTaskBufferMaxSize = pendingTaskBufferMaxSize;
    this->batchTaskBufferMaxSize = batchTaskBufferMaxSize;

    working = true;
    backgroundPoolManager = std::thread(&PGConnectionPool::BackgroundPoolManager, this);
    return true;
}

void PGConnectionPool::Destroy()
{
    // wait all jobs finished, max wait times is 10 second.
    int waitIntervalTime = 100;
    int waitMaxCnt = 100;
    for (int i = 0; i <= (int)FalconBatchServiceType::NOT_SUPPORT; ++i) {
        int curWaitCnt = 0;
        while (supportBatchTaskList[i].jobList.size_approx() > 0 && waitMaxCnt > curWaitCnt) {
            std::this_thread::sleep_for(std::chrono::microseconds(waitIntervalTime));
            curWaitCnt++;
        }
    }

    working = false;
    for (auto it = currentManagedConn.begin(); it != currentManagedConn.end(); ++it) {
        (*it)->Stop();
    }
    for (auto it = currentManagedConn.begin(); it != currentManagedConn.end(); ++it) {
        delete (*it);
    }
    backgroundPoolManager.join();
    currentManagedConn.clear();
}

bool StartPGConnectionPool()
{
    // postgres connection pool init for process jobs dispatched by communication Server
    char *userName = getenv("USER");
    return PGConnectionPool::GetInstance().Init(FalconPGPort, userName, FalconConnectionPoolSize, 20, 400);
}

void DestroyPGConnectionPool() { PGConnectionPool::GetInstance().Destroy(); }

// communication server callback function used to dispatch request to PGConnectionPool
void FalconDispatchMetaJob2PGConnectionPool(void *job)
{
    BaseMetaServiceJob *metaJob = static_cast<BaseMetaServiceJob *>(job);
    PGConnectionPool::GetInstance().DispatchMetaServiceJob(metaJob);
}