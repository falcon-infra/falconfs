/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef FALCON_WORKER_TASK_H
#define FALCON_WORKER_TASK_H

#include <flatbuffers/flatbuffers.h>
#include <vector>
#include "base_comm_adapter/base_kv_cache_service_job.h"
#include "base_comm_adapter/base_meta_service_job.h"
#include "connection_pool/falcon_concurrent_queue.h"
#include "libpq-fe.h"
#include "remote_connection_utils/serialized_data.h"
#include "utils/falcon_shmem_allocator.h"

// define base class for worker task
class BaseWorkerTask {
  protected:
    // the shmem Allocator, used for transfer data between processes on the same server.
    FalconShmemAllocator *m_allocator{nullptr};

  public:
    BaseWorkerTask(FalconShmemAllocator *allocator)
        : m_allocator(allocator)
    {
    }
    virtual ~BaseWorkerTask() {}
    // Derives need implement this function do there worker
    // Here reuse FlatBufferBuilder & SerializedData for high performance
    virtual void
    DoWork(PGconn *conn, flatbuffers::FlatBufferBuilder &flatBufferBuilder, SerializedData &replyBuilder) = 0;
};

class SingleWorkerTask : public BaseWorkerTask {
  private:
    BaseMetaServiceJob *m_job{nullptr};

  public:
    SingleWorkerTask(FalconShmemAllocator *allocator, BaseMetaServiceJob *job)
        : BaseWorkerTask(allocator),
          m_job(job)
    {
    }
    ~SingleWorkerTask() override
    {
        if (m_job != nullptr) {
            m_job->MarkFailed();
            m_job->Done();
            delete m_job;
        }
    }
    // implement logic of SingleWorker process
    void DoWork(PGconn *conn, flatbuffers::FlatBufferBuilder &flatBufferBuilder, SerializedData &replyBuilder) override;
};

class BatchWorkerTask : public BaseWorkerTask {
  private:
    std::vector<BaseMetaServiceJob *> m_jobList;

  public:
    BatchWorkerTask(FalconShmemAllocator *allocator, std::vector<BaseMetaServiceJob *> jobList)
        : BaseWorkerTask(allocator),
          m_jobList(std::move(jobList))
    {
    }
    ~BatchWorkerTask() override
    {
        for (auto *job : m_jobList) {
            if (job != nullptr) {
                job->MarkFailed();
                job->Done();
                delete job;
            }
        }
    }
    // implement logic of BatchWorker process
    void DoWork(PGconn *conn, flatbuffers::FlatBufferBuilder &flatBufferBuilder, SerializedData &replyBuilder) override;
};

/* v6.4 \u00a74.1.1: one KV cache job = one BRPC sub-batch. The worker hands the
 * job's serialized request to the plugin-registered process-job callback,
 * which invokes the engine's `Batch*SplitForPoolWorker` against the shared
 * DRAM cache and uses this worker's libpq connection for the catalog
 * round-trip. Each PGConnection worker therefore drives its own PG backend in
 * parallel \u2014 catalog access scales horizontally across the connection pool
 * instead of serializing through one connection. */
class KVCacheWorkerTask : public BaseWorkerTask {
  private:
    BaseKVCacheServiceJob *m_job{nullptr};

  public:
    KVCacheWorkerTask(FalconShmemAllocator *allocator, BaseKVCacheServiceJob *job)
        : BaseWorkerTask(allocator),
          m_job(job)
    {
    }
    ~KVCacheWorkerTask() override {}
    void DoWork(PGconn *conn, flatbuffers::FlatBufferBuilder &flatBufferBuilder, SerializedData &replyBuilder) override;
};

#endif // FALCON_WORKER_TASK_H
