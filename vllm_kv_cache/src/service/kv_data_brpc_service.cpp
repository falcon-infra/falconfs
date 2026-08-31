#include "vllm_kv_cache/src/service/kv_data_brpc_service.h"

#include <brpc/controller.h>
#include <butil/iobuf.h>

#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>

namespace falconfs::kv {

KVDataBrpcServiceAdapter::KVDataBrpcServiceAdapter(std::shared_ptr<KVDataServiceImpl> impl)
    : impl_(std::move(impl)) {}

namespace {

struct ClosureGuard {
    ::google::protobuf::Closure* done;
    explicit ClosureGuard(::google::protobuf::Closure* d) : done(d) {}
    ~ClosureGuard() {
        if (done != nullptr) done->Run();
    }
};

void AppendU64LEIOBuf(butil::IOBuf* out, uint64_t v)
{
    char buf[8];
    for (int i = 0; i < 8; ++i) buf[i] = static_cast<char>((v >> (8 * i)) & 0xff);
    out->append(buf, sizeof(buf));
}

bool ReadU64LE(const std::string& in, size_t* pos, uint64_t* v)
{
    if (*pos + 8 > in.size()) return false;
    uint64_t x = 0;
    for (int i = 0; i < 8; ++i) x |= (static_cast<uint64_t>(static_cast<unsigned char>(in[*pos + i])) << (8 * i));
    *pos += 8;
    *v = x;
    return true;
}

bool DecodePayloadFrames(const std::string& in, int n, std::vector<std::string>* payloads)
{
    if (payloads == nullptr) return false;
    payloads->clear();
    payloads->reserve(std::max(0, n));
    size_t pos = 0;
    for (int i = 0; i < n; ++i) {
        uint64_t len = 0;
        if (!ReadU64LE(in, &pos, &len)) return false;
        if (len > static_cast<uint64_t>(in.size() - pos)) return false;
        payloads->emplace_back(in.data() + pos, static_cast<size_t>(len));
        pos += static_cast<size_t>(len);
    }
    return pos == in.size();
}

}  // namespace

void KVDataBrpcServiceAdapter::WriteBlock(::google::protobuf::RpcController* controller,
                                          const WriteBlockRequest* request,
                                          WriteBlockResponse* response,
                                          ::google::protobuf::Closure* done) {
    ClosureGuard guard(done);
    auto* cntl = dynamic_cast<brpc::Controller*>(controller);
    if (cntl != nullptr && cntl->request_attachment().size() > 0 &&
        request->has_item() && request->item().payload().empty()) {
        std::string payload;
        cntl->request_attachment().copy_to(&payload);
        impl_->WriteBlockPayload(*request, payload, response);
        return;
    }
    impl_->WriteBlock(*request, response);
}

void KVDataBrpcServiceAdapter::ReadBlock(::google::protobuf::RpcController* controller,
                                         const ReadBlockRequest* request,
                                         ReadBlockResponse* response,
                                         ::google::protobuf::Closure* done) {
    ClosureGuard guard(done);
    impl_->ReadBlock(*request, response);
    auto* cntl = dynamic_cast<brpc::Controller*>(controller);
    if (cntl != nullptr && response->has_result() &&
        !response->result().payload().empty()) {
        cntl->response_attachment().append(response->result().payload());
        response->mutable_result()->clear_payload();
    }
}

void KVDataBrpcServiceAdapter::ReadFromSSD(::google::protobuf::RpcController* controller,
                                           const ReadFromSSDRequest* request,
                                           ReadFromSSDResponse* response,
                                           ::google::protobuf::Closure* done) {
    ClosureGuard guard(done);
    impl_->ReadFromSSD(*request, response);
    auto* cntl = dynamic_cast<brpc::Controller*>(controller);
    if (cntl != nullptr && response->has_result() &&
        !response->result().payload().empty()) {
        cntl->response_attachment().append(response->result().payload());
        response->mutable_result()->clear_payload();
    }
}

void KVDataBrpcServiceAdapter::BatchWriteBlock(::google::protobuf::RpcController* controller,
                                               const BatchWriteBlockRequest* request,
                                               BatchWriteBlockResponse* response,
                                               ::google::protobuf::Closure* done) {
    ClosureGuard guard(done);
    auto* cntl = dynamic_cast<brpc::Controller*>(controller);
    if (cntl != nullptr && cntl->request_attachment().size() > 0) {
        std::string framed;
        cntl->request_attachment().copy_to(&framed);
        std::vector<std::string> payloads;
        if (DecodePayloadFrames(framed, request->items_size(), &payloads)) {
            impl_->BatchWriteBlockPayloads(*request, payloads, response);
            return;
        }
    }
    impl_->BatchWriteBlock(*request, response);
}

void KVDataBrpcServiceAdapter::BatchReadBlock(::google::protobuf::RpcController* controller,
                                              const BatchReadBlockRequest* request,
                                              BatchReadBlockResponse* response,
                                              ::google::protobuf::Closure* done) {
    ClosureGuard guard(done);
    impl_->BatchReadBlock(*request, response);
    auto* cntl = dynamic_cast<brpc::Controller*>(controller);
    if (cntl != nullptr && response->results_size() > 0) {
        bool any_payload = false;
        for (int i = 0; i < response->results_size(); ++i) {
            any_payload = any_payload || !response->results(i).payload().empty();
        }
        if (any_payload) {
            auto* attachment = &cntl->response_attachment();
            for (int i = 0; i < response->results_size(); ++i) {
                const std::string& payload = response->results(i).payload();
                AppendU64LEIOBuf(attachment, static_cast<uint64_t>(payload.size()));
                if (!payload.empty()) attachment->append(payload);
            }
            for (int i = 0; i < response->results_size(); ++i) {
                response->mutable_results(i)->clear_payload();
            }
        }
    }
}

void KVDataBrpcServiceAdapter::BatchReadFromSSD(::google::protobuf::RpcController* /*controller*/,
                                                const BatchReadFromSSDRequest* request,
                                                BatchReadFromSSDResponse* response,
                                                ::google::protobuf::Closure* done) {
    ClosureGuard guard(done);
    impl_->BatchReadFromSSD(*request, response);
}

}  // namespace falconfs::kv
