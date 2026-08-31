/* Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 *
 * Concrete BRPC subclass of BaseKVCacheServiceJob. Built by the BRPC plugin
 * handlers and dispatched via `FalconDispatchMetaJob2PGConnectionPool` onto
 * the connection-pool kvTaskList. The pool worker eventually calls Done(),
 * which parses the serialized response back into the BRPC response message
 * and invokes the BRPC closure.
 */
#ifndef BRPC_KV_CACHE_SERVICE_JOB_H
#define BRPC_KV_CACHE_SERVICE_JOB_H

#include <google/protobuf/message.h>
#include <google/protobuf/service.h>

#include <string>
#include <utility>

#include "base_comm_adapter/base_kv_cache_service_job.h"

class BrpcKVCacheServiceJob : public BaseKVCacheServiceJob {
public:
    BrpcKVCacheServiceJob(FalconKVServiceMethod method,
                          std::string serialized_request,
                          ::google::protobuf::Message *response_msg,
                          ::google::protobuf::Closure *done)
        : method_(method),
          request_(std::move(serialized_request)),
          response_msg_(response_msg),
          done_(done) {}

    ~BrpcKVCacheServiceJob() override = default;

    FalconKVServiceMethod GetMethod() const override { return method_; }
    const std::string &GetSerializedRequest() const override { return request_; }
    void SetSerializedResponse(std::string resp) override { response_ = std::move(resp); }

    /* Called from the pool worker after KVCacheWorkerTask::DoWork completes.
     * Parses the serialized protobuf response into the BRPC response message
     * and triggers the BRPC closure. */
    void Done() override
    {
        if (response_msg_ != nullptr && !response_.empty()) {
            (void) response_msg_->ParseFromString(response_);
        }
        if (done_ != nullptr) {
            done_->Run();
        }
    }

private:
    FalconKVServiceMethod method_;
    std::string request_;
    std::string response_;
    ::google::protobuf::Message *response_msg_;
    ::google::protobuf::Closure *done_;
};

#endif  // BRPC_KV_CACHE_SERVICE_JOB_H
