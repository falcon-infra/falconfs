/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef FALCON_POOLER_TASK_H
#define FALCON_POOLER_TASK_H

#include <brpc/server.h>
#include <vector>
#include <chrono>
#include "falcon_meta_rpc.pb.h"

namespace falcon::meta_proto
{

class AsyncMetaServiceJob {
  private:
    brpc::Controller *cntl;
    const MetaRequest *request;
    Empty *response;
    google::protobuf::Closure *done;

  public:
    // 性能统计时间点
    std::chrono::steady_clock::time_point create_time;      // 创建时间
    std::chrono::steady_clock::time_point dequeue_time;     // 出队时间（开始处理）
    std::chrono::steady_clock::time_point shmem_done_time;  // 共享内存复制完成
    std::chrono::steady_clock::time_point pg_send_time;     // 发送SQL时间
    std::chrono::steady_clock::time_point pg_result_time;   // 收到SQL结果时间
    std::chrono::steady_clock::time_point process_done_time; // 结果处理完成

    AsyncMetaServiceJob(brpc::Controller *cntl,
                        const MetaRequest *request,
                        Empty *response,
                        google::protobuf::Closure *done)
        : cntl(cntl),
          request(request),
          response(response),
          done(done),
          create_time(std::chrono::steady_clock::now())
    {
    }
    brpc::Controller *GetCntl() { return cntl; }
    const MetaRequest *GetRequest() { return request; }
    Empty *GetResponse() { return response; }
    void Done() { done->Run(); }
};

} // namespace falcon::meta_proto

class Task {
  public:
    bool isBatch;
    std::vector<falcon::meta_proto::AsyncMetaServiceJob *> jobList;
    Task(int n)
    {
        isBatch = false;
        jobList.reserve(n);
    }
    Task() { isBatch = false; }
};

#endif
