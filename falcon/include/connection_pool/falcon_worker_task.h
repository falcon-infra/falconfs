/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef FALCON_WORKER_TASK_H
#define FALCON_WORKER_TASK_H

#include <flatbuffers/flatbuffers.h>
#include <vector>
#include "base_comm_adapter/base_meta_service_job.h"
#include "concurrentqueue/concurrentqueue.h"
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
    ~SingleWorkerTask() override {}
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
    ~BatchWorkerTask() override {}
    // implement logic of BatchWorker process
    void DoWork(PGconn *conn, flatbuffers::FlatBufferBuilder &flatBufferBuilder, SerializedData &replyBuilder) override;
};

#endif // FALCON_WORKER_TASK_H
