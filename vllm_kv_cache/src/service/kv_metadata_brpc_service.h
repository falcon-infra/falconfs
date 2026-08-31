// brpc-compatible adapter that bridges the protobuf-generated
// `KVMetadataService` abstract base class to `KVMetadataServiceImpl`.
//
// The adapter follows the same pattern as the existing FalconFS meta service
// (`falcon/brpc_comm_adapter/brpc_meta_service_imp.cpp`): the brpc server
// invokes the protobuf-generated method, the adapter forwards to the engine-
// backed `KVMetadataServiceImpl`, and finally calls `done->Run()`.
//
// The adapter does NOT depend on the brpc library; it only depends on the
// abstract types from `google::protobuf` so that it can be linked into both
// brpc-server processes and standalone gtest binaries. Any brpc-specific
// behavior (Controller cancellation, attachment iobufs) is owned by the
// process that registers the adapter with `brpc::Server`.
#pragma once

#include <memory>

#include "kv_metadata_service.pb.h"
#include "vllm_kv_cache/src/metadata/kv_metadata_service_impl.h"

namespace falconfs::kv {

class KVMetadataBrpcServiceAdapter : public KVMetadataService {
public:
    explicit KVMetadataBrpcServiceAdapter(std::shared_ptr<KVMetadataServiceImpl> impl);
    ~KVMetadataBrpcServiceAdapter() override = default;

    void BatchLookupWithLease(::google::protobuf::RpcController* controller,
                              const BatchLookupRequest* request,
                              BatchLookupResponse* response,
                              ::google::protobuf::Closure* done) override;

    void BatchAllocateWithLease(::google::protobuf::RpcController* controller,
                                const BatchAllocateRequest* request,
                                BatchAllocateResponse* response,
                                ::google::protobuf::Closure* done) override;

    void BatchRenewLease(::google::protobuf::RpcController* controller,
                         const BatchRenewLeaseRequest* request,
                         BatchRenewLeaseResponse* response,
                         ::google::protobuf::Closure* done) override;

    void BatchUpdateBlockStatus(::google::protobuf::RpcController* controller,
                                const BatchUpdateStatusRequest* request,
                                BatchUpdateStatusResponse* response,
                                ::google::protobuf::Closure* done) override;

    void BatchFreeAllocated(::google::protobuf::RpcController* controller,
                            const BatchFreeAllocatedRequest* request,
                            BatchFreeAllocatedResponse* response,
                            ::google::protobuf::Closure* done) override;

private:
    std::shared_ptr<KVMetadataServiceImpl> impl_;
};

}  // namespace falconfs::kv
