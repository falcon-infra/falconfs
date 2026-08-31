/* Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 *
 * BRPC service adapters for the v6.4 KV cache. Each handler builds a
 * `BrpcKVCacheServiceJob` and dispatches it to the FalconFS connection pool;
 * the engine work + catalog round-trip happen on the pool worker thread, not
 * the BRPC worker thread (v6.4 \u00a74.1.1).
 */
#ifndef BRPC_KV_SERVICE_IMP_H
#define BRPC_KV_SERVICE_IMP_H

#include <brpc/server.h>

#include "base_comm_adapter/comm_server_interface.h"
#include "kv_data_service.pb.h"
#include "kv_metadata_service.pb.h"
#include "vllm_kv_cache/src/store/kv_data_service_impl.h"

namespace falcon::kv_proto {

class BrpcKVMetadataServiceImpl : public ::falconfs::kv::KVMetadataService {
public:
    explicit BrpcKVMetadataServiceImpl(falcon_meta_job_dispatch_func dispatchFunc)
        : dispatchFunc_(dispatchFunc) {}
    ~BrpcKVMetadataServiceImpl() override = default;

    void BatchLookupWithLease(::google::protobuf::RpcController *controller,
                              const ::falconfs::kv::BatchLookupRequest *request,
                              ::falconfs::kv::BatchLookupResponse *response,
                              ::google::protobuf::Closure *done) override;
    void BatchAllocateWithLease(::google::protobuf::RpcController *controller,
                                const ::falconfs::kv::BatchAllocateRequest *request,
                                ::falconfs::kv::BatchAllocateResponse *response,
                                ::google::protobuf::Closure *done) override;
    void BatchRenewLease(::google::protobuf::RpcController *controller,
                         const ::falconfs::kv::BatchRenewLeaseRequest *request,
                         ::falconfs::kv::BatchRenewLeaseResponse *response,
                         ::google::protobuf::Closure *done) override;
    void BatchUpdateBlockStatus(::google::protobuf::RpcController *controller,
                                const ::falconfs::kv::BatchUpdateStatusRequest *request,
                                ::falconfs::kv::BatchUpdateStatusResponse *response,
                                ::google::protobuf::Closure *done) override;
    void BatchFreeAllocated(::google::protobuf::RpcController *controller,
                            const ::falconfs::kv::BatchFreeAllocatedRequest *request,
                            ::falconfs::kv::BatchFreeAllocatedResponse *response,
                            ::google::protobuf::Closure *done) override;

private:
    falcon_meta_job_dispatch_func dispatchFunc_;
};

/* KV data path stays in-process; it does not need a catalog round-trip, so it
 * runs directly on the BRPC worker thread. */
class BrpcKVDataServiceImpl : public ::falconfs::kv::KVDataService {
public:
    explicit BrpcKVDataServiceImpl(std::shared_ptr<::falconfs::kv::KVDataServiceImpl> impl)
        : impl_(std::move(impl)) {}
    ~BrpcKVDataServiceImpl() override = default;

    void WriteBlock(::google::protobuf::RpcController *controller,
                    const ::falconfs::kv::WriteBlockRequest *request,
                    ::falconfs::kv::WriteBlockResponse *response,
                    ::google::protobuf::Closure *done) override;
    void ReadBlock(::google::protobuf::RpcController *controller,
                   const ::falconfs::kv::ReadBlockRequest *request,
                   ::falconfs::kv::ReadBlockResponse *response,
                   ::google::protobuf::Closure *done) override;
    void ReadFromSSD(::google::protobuf::RpcController *controller,
                       const ::falconfs::kv::ReadFromSSDRequest *request,
                       ::falconfs::kv::ReadFromSSDResponse *response,
                       ::google::protobuf::Closure *done) override;

    void BatchWriteBlock(::google::protobuf::RpcController *controller,
                         const ::falconfs::kv::BatchWriteBlockRequest *request,
                         ::falconfs::kv::BatchWriteBlockResponse *response,
                         ::google::protobuf::Closure *done) override;
    void BatchReadBlock(::google::protobuf::RpcController *controller,
                        const ::falconfs::kv::BatchReadBlockRequest *request,
                        ::falconfs::kv::BatchReadBlockResponse *response,
                        ::google::protobuf::Closure *done) override;
    void BatchReadFromSSD(::google::protobuf::RpcController *controller,
                          const ::falconfs::kv::BatchReadFromSSDRequest *request,
                          ::falconfs::kv::BatchReadFromSSDResponse *response,
                          ::google::protobuf::Closure *done) override;

private:
    std::shared_ptr<::falconfs::kv::KVDataServiceImpl> impl_;
};

}  // namespace falcon::kv_proto

#endif  // BRPC_KV_SERVICE_IMP_H
