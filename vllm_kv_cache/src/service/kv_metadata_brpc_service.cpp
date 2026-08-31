#include "vllm_kv_cache/src/service/kv_metadata_brpc_service.h"

namespace falconfs::kv {

KVMetadataBrpcServiceAdapter::KVMetadataBrpcServiceAdapter(std::shared_ptr<KVMetadataServiceImpl> impl)
    : impl_(std::move(impl)) {}

namespace {

struct ClosureGuard {
    ::google::protobuf::Closure* done;
    explicit ClosureGuard(::google::protobuf::Closure* d) : done(d) {}
    ~ClosureGuard() {
        if (done != nullptr) done->Run();
    }
};

}  // namespace

void KVMetadataBrpcServiceAdapter::BatchLookupWithLease(::google::protobuf::RpcController* /*controller*/,
                                                        const BatchLookupRequest* request,
                                                        BatchLookupResponse* response,
                                                        ::google::protobuf::Closure* done) {
    ClosureGuard guard(done);
    impl_->BatchLookupWithLease(*request, response);
}

void KVMetadataBrpcServiceAdapter::BatchAllocateWithLease(::google::protobuf::RpcController* /*controller*/,
                                                          const BatchAllocateRequest* request,
                                                          BatchAllocateResponse* response,
                                                          ::google::protobuf::Closure* done) {
    ClosureGuard guard(done);
    impl_->BatchAllocateWithLease(*request, response);
}

void KVMetadataBrpcServiceAdapter::BatchRenewLease(::google::protobuf::RpcController* /*controller*/,
                                                   const BatchRenewLeaseRequest* request,
                                                   BatchRenewLeaseResponse* response,
                                                   ::google::protobuf::Closure* done) {
    ClosureGuard guard(done);
    impl_->BatchRenewLease(*request, response);
}

void KVMetadataBrpcServiceAdapter::BatchUpdateBlockStatus(::google::protobuf::RpcController* /*controller*/,
                                                          const BatchUpdateStatusRequest* request,
                                                          BatchUpdateStatusResponse* response,
                                                          ::google::protobuf::Closure* done) {
    ClosureGuard guard(done);
    impl_->BatchUpdateBlockStatus(*request, response);
}

void KVMetadataBrpcServiceAdapter::BatchFreeAllocated(::google::protobuf::RpcController* /*controller*/,
                                                      const BatchFreeAllocatedRequest* request,
                                                      BatchFreeAllocatedResponse* response,
                                                      ::google::protobuf::Closure* done) {
    ClosureGuard guard(done);
    impl_->BatchFreeAllocated(*request, response);
}

}  // namespace falconfs::kv
