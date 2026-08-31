// v6.5 P4: client-side KV store facade registry + local SHM / remote BRPC facades.
#include "vllm_kv_cache/src/store/kv_store_facade.h"

#include "kv_common.pb.h"
#include "kv_data_service.pb.h"

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <butil/iobuf.h>

#include <fcntl.h>
#include <libpq-fe.h>
#include <sys/mman.h>
#include <sys/select.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <cstring>
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace falconfs::kv {

namespace {

int32_t EnvInt32(const char* k, int32_t d)
{
    const char* v = std::getenv(k);
    if (v == nullptr || v[0] == '\0') return d;
    return static_cast<int32_t>(std::strtol(v, nullptr, 10));
}

int64_t SteadyMs()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

void FillErr(ItemResultMeta* m, const char* msg)
{
    m->set_success(false);
    m->set_error_code(::falconfs::kv::ErrorCode::INTERNAL_ERROR);
    m->set_retryable(false);
    m->set_error_message(msg);
}

std::string ShmOpenName(const std::string& shm_name)
{
    if (!shm_name.empty() && shm_name[0] == '/') return shm_name;
    return "/" + shm_name;
}

std::string MakeEndpoint(const char* host, int brpc_port)
{
    std::ostringstream o;
    o << host << ":" << brpc_port;
    return o.str();
}

void AppendU64LEIOBuf(butil::IOBuf* out, uint64_t v)
{
    char buf[8];
    for (int i = 0; i < 8; ++i) buf[i] = static_cast<char>((v >> (8 * i)) & 0xff);
    out->append(buf, sizeof(buf));
}

void AppendPayloadFramesToIOBuf(const std::vector<std::string>& payloads, butil::IOBuf* out)
{
    for (const auto& payload : payloads) {
        AppendU64LEIOBuf(out, static_cast<uint64_t>(payload.size()));
        if (!payload.empty()) out->append(payload);
    }
}

void AppendPayloadViewFramesToIOBuf(const std::vector<KVStoreWritePayloadView>& payloads, butil::IOBuf* out)
{
    for (const auto& payload : payloads) {
        AppendU64LEIOBuf(out, static_cast<uint64_t>(payload.size));
        if (payload.data != nullptr && payload.size > 0) out->append(payload.data, payload.size);
    }
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

void IKVStoreFacade::BatchWriteBlockPayloadViews(const BatchWriteBlockRequest& req,
                                                 const std::vector<KVStoreWritePayloadView>& views,
                                                 BatchWriteBlockResponse* resp)
{
    std::vector<std::string> payloads;
    payloads.reserve(views.size());
    for (const auto& view : views) {
        if (view.data != nullptr && view.size > 0) {
            payloads.emplace_back(view.data, view.size);
        } else {
            payloads.emplace_back();
        }
    }
    BatchWriteBlockPayloads(req, payloads, resp);
}

void IKVStoreFacade::BatchReadBlockPayloadViews(const BatchReadBlockRequest& req,
                                                BatchReadBlockResponse* resp,
                                                std::vector<KVStoreReadPayloadView>* views)
{
    std::vector<std::string> payloads;
    BatchReadBlockPayloads(req, resp, &payloads);
    if (views == nullptr) return;
    views->clear();
    views->resize(payloads.size());
    for (size_t i = 0; i < payloads.size(); ++i) {
        auto& view = (*views)[i];
        view.ok = i < static_cast<size_t>(resp->results_size()) &&
                  resp->results(static_cast<int>(i)).has_result() &&
                  resp->results(static_cast<int>(i)).result().success();
        view.owned_payload = std::move(payloads[i]);
        view.data = view.owned_payload.data();
        view.size = view.owned_payload.size();
        view.kind = IsLocal() ? "local_native_buffer" : "remote_attachment_buffer";
    }
}

class UnhealthyKVStoreFacade final : public IKVStoreFacade {
public:
    explicit UnhealthyKVStoreFacade(int32_t sid) : sid_(sid) {}

    int32_t StoreNodeId() const override { return sid_; }
    bool    IsLocal() const override { return false; }
    bool    IsHealthy() const override { return false; }

    void BatchWriteBlock(const BatchWriteBlockRequest& req,
                         BatchWriteBlockResponse* resp) override
    {
        for (int i = 0; i < req.items_size(); ++i) {
            auto* wr = resp->add_results();
            wr->set_block_hash(req.items(i).block_hash());
            FillErr(wr->mutable_result(), "store unhealthy");
        }
    }
    void BatchReadBlock(const BatchReadBlockRequest& req, BatchReadBlockResponse* resp) override
    {
        for (int i = 0; i < req.items_size(); ++i) {
            auto* rr = resp->add_results();
            rr->set_block_hash(req.items(i).block_hash());
            FillErr(rr->mutable_result(), "store unhealthy");
        }
    }
    void BatchReadBlockPayloads(const BatchReadBlockRequest& req, BatchReadBlockResponse* resp,
                                std::vector<std::string>* payloads) override
    {
        BatchReadBlock(req, resp);
        if (payloads != nullptr) payloads->assign(req.items_size(), std::string());
    }
    void BatchReadFromSSD(const BatchReadFromSSDRequest& req,
                          BatchReadFromSSDResponse* resp) override
    {
        for (int i = 0; i < req.items_size(); ++i) {
            auto* sr = resp->add_results();
            sr->set_block_hash(req.items(i).block_hash());
            FillErr(sr->mutable_result(), "store unhealthy");
        }
    }

    void WriteBlock(const WriteBlockRequest& req, WriteBlockResponse* resp) override
    {
        auto* wr = resp->mutable_result();
        wr->set_block_hash(req.item().block_hash());
        FillErr(wr->mutable_result(), "store unhealthy");
    }
    void WriteBlockPayload(const WriteBlockRequest& req,
                           const char* /*payload*/,
                           size_t /*payload_len*/,
                           WriteBlockResponse* resp) override
    {
        WriteBlock(req, resp);
    }
    void BatchWriteBlockPayloads(const BatchWriteBlockRequest& req,
                                 const std::vector<std::string>& /*payloads*/,
                                 BatchWriteBlockResponse* resp) override
    {
        BatchWriteBlock(req, resp);
    }
    void ReadBlock(const ReadBlockRequest& req, ReadBlockResponse* resp) override
    {
        auto* rr = resp->mutable_result();
        rr->set_block_hash(req.item().block_hash());
        FillErr(rr->mutable_result(), "store unhealthy");
    }
    void ReadBlockPayload(const ReadBlockRequest& req,
                          ReadBlockResponse* resp,
                          std::string* /*payload*/) override
    {
        ReadBlock(req, resp);
    }
    void ReadFromSSD(const ReadFromSSDRequest& req, ReadFromSSDResponse* resp) override
    {
        auto* sr = resp->mutable_result();
        sr->set_block_hash(req.item().block_hash());
        FillErr(sr->mutable_result(), "store unhealthy");
    }

private:
    int32_t sid_;
};

class RemoteKVStoreFacade final : public IKVStoreFacade {
public:
    RemoteKVStoreFacade(int32_t sid, std::unique_ptr<brpc::Channel> ch)
        : sid_(sid), ch_(std::move(ch))
    {
    }

    int32_t StoreNodeId() const override { return sid_; }
    bool    IsLocal() const override { return false; }
    bool    IsHealthy() const override { return ch_ != nullptr; }

    void BatchWriteBlock(const BatchWriteBlockRequest& req,
                         BatchWriteBlockResponse* resp) override
    {
        std::vector<std::string> payloads;
        payloads.reserve(req.items_size());
        BatchWriteBlockRequest wire(req);
        for (int i = 0; i < wire.items_size(); ++i) {
            payloads.push_back(wire.items(i).payload());
            wire.mutable_items(i)->clear_payload();
        }
        BatchWriteBlockPayloads(wire, payloads, resp);
    }
    void BatchWriteBlockPayloads(const BatchWriteBlockRequest& req,
                                 const std::vector<std::string>& payloads,
                                 BatchWriteBlockResponse* resp) override
    {
        if (ch_ == nullptr) return;
        KVDataService_Stub stub(ch_.get());
        brpc::Controller cntl;
        BatchWriteBlockRequest wire(req);
        for (int i = 0; i < wire.items_size(); ++i) wire.mutable_items(i)->clear_payload();
        const bool use_attachment = static_cast<int>(payloads.size()) == req.items_size();
        if (use_attachment) AppendPayloadFramesToIOBuf(payloads, &cntl.request_attachment());
        stub.BatchWriteBlock(&cntl, &wire, resp, nullptr);
    }
    void BatchWriteBlockPayloadViews(const BatchWriteBlockRequest& req,
                                     const std::vector<KVStoreWritePayloadView>& payloads,
                                     BatchWriteBlockResponse* resp) override
    {
        if (ch_ == nullptr) return;
        KVDataService_Stub stub(ch_.get());
        brpc::Controller cntl;
        BatchWriteBlockRequest wire(req);
        for (int i = 0; i < wire.items_size(); ++i) wire.mutable_items(i)->clear_payload();
        const bool use_attachment = static_cast<int>(payloads.size()) == req.items_size();
        if (use_attachment) AppendPayloadViewFramesToIOBuf(payloads, &cntl.request_attachment());
        stub.BatchWriteBlock(&cntl, &wire, resp, nullptr);
    }
    void BatchReadBlock(const BatchReadBlockRequest& req, BatchReadBlockResponse* resp) override
    {
        std::vector<std::string> payloads;
        BatchReadBlockPayloads(req, resp, &payloads);
        for (int i = 0; i < resp->results_size() && i < static_cast<int>(payloads.size()); ++i) {
            if (!payloads[i].empty() && resp->results(i).payload().empty()) {
                resp->mutable_results(i)->set_payload(std::move(payloads[i]));
            }
        }
    }
    void BatchReadBlockPayloads(const BatchReadBlockRequest& req, BatchReadBlockResponse* resp,
                                std::vector<std::string>* payloads) override
    {
        if (ch_ == nullptr) return;
        KVDataService_Stub stub(ch_.get());
        brpc::Controller cntl;
        stub.BatchReadBlock(&cntl, &req, resp, nullptr);
        if (payloads == nullptr) return;
        if (cntl.response_attachment().size() > 0) {
            std::string framed;
            cntl.response_attachment().copy_to(&framed);
            if (DecodePayloadFrames(framed, resp->results_size(), payloads)) {
                for (int i = 0; i < resp->results_size(); ++i) resp->mutable_results(i)->clear_payload();
                return;
            }
        }
        payloads->clear();
        payloads->reserve(resp->results_size());
        for (int i = 0; i < resp->results_size(); ++i) {
            payloads->push_back(resp->results(i).payload());
            resp->mutable_results(i)->clear_payload();
        }
    }
    void BatchReadFromSSD(const BatchReadFromSSDRequest& req,
                          BatchReadFromSSDResponse* resp) override
    {
        if (ch_ == nullptr) return;
        KVDataService_Stub stub(ch_.get());
        brpc::Controller cntl;
        stub.BatchReadFromSSD(&cntl, &req, resp, nullptr);
    }

    void WriteBlock(const WriteBlockRequest& req, WriteBlockResponse* resp) override
    {
        if (ch_ == nullptr) return;
        WriteBlockPayload(req, req.has_item() ? req.item().payload().data() : nullptr,
                          req.has_item() ? req.item().payload().size() : 0, resp);
    }
    void WriteBlockPayload(const WriteBlockRequest& req,
                           const char* payload,
                           size_t payload_len,
                           WriteBlockResponse* resp) override
    {
        if (ch_ == nullptr) return;
        KVDataService_Stub stub(ch_.get());
        brpc::Controller cntl;
        WriteBlockRequest wire(req);
        const bool use_attachment = (payload != nullptr && payload_len > 0);
        if (use_attachment) {
            wire.mutable_item()->clear_payload();
        }
        for (int attempt = 0; attempt < 12; ++attempt) {
            cntl.Reset();
            resp->Clear();
            if (use_attachment) {
                cntl.request_attachment().append(payload, payload_len);
            }
            stub.WriteBlock(&cntl, &wire, resp, nullptr);
            if (!cntl.Failed()) {
                return;
            }
            if (attempt < 11) {
                std::this_thread::sleep_for(std::chrono::milliseconds(50 * std::min(attempt + 1, 24)));
            }
        }
        auto* wr = resp->mutable_result();
        wr->set_block_hash(req.has_item() ? req.item().block_hash() : std::string());
        FillErr(wr->mutable_result(), cntl.ErrorText().c_str());
    }
    void ReadBlock(const ReadBlockRequest& req, ReadBlockResponse* resp) override
    {
        std::string payload;
        ReadBlockPayload(req, resp, &payload);
        if (!payload.empty() && resp->has_result() && resp->result().payload().empty()) {
            resp->mutable_result()->set_payload(std::move(payload));
        }
    }
    void ReadBlockPayload(const ReadBlockRequest& req,
                          ReadBlockResponse* resp,
                          std::string* payload) override
    {
        if (ch_ == nullptr) return;
        KVDataService_Stub stub(ch_.get());
        brpc::Controller cntl;
        bool ok = false;
        for (int attempt = 0; attempt < 12; ++attempt) {
            cntl.Reset();
            resp->Clear();
            stub.ReadBlock(&cntl, &req, resp, nullptr);
            if (!cntl.Failed()) {
                ok = true;
                break;
            }
            if (attempt < 11) {
                std::this_thread::sleep_for(std::chrono::milliseconds(50 * std::min(attempt + 1, 24)));
            }
        }
        if (!ok) {
            auto* rr = resp->mutable_result();
            rr->set_block_hash(req.has_item() ? req.item().block_hash() : std::string());
            FillErr(rr->mutable_result(), cntl.ErrorText().c_str());
            return;
        }
        if (payload == nullptr) return;
        if (cntl.response_attachment().size() > 0) {
            cntl.response_attachment().copy_to(payload);
            if (resp->has_result()) resp->mutable_result()->clear_payload();
        } else if (resp->has_result() && !resp->result().payload().empty()) {
            *payload = std::move(*resp->mutable_result()->mutable_payload());
            resp->mutable_result()->clear_payload();
        }
    }
    void ReadFromSSD(const ReadFromSSDRequest& req, ReadFromSSDResponse* resp) override
    {
        if (ch_ == nullptr) return;
        KVDataService_Stub stub(ch_.get());
        brpc::Controller cntl;
        stub.ReadFromSSD(&cntl, &req, resp, nullptr);
        if (cntl.response_attachment().size() > 0 && resp->has_result() &&
            resp->result().payload().empty()) {
            std::string payload;
            cntl.response_attachment().copy_to(&payload);
            resp->mutable_result()->set_payload(std::move(payload));
        }
    }

private:
    int32_t sid_{0};
    std::unique_ptr<brpc::Channel> ch_;
};

class LocalKVStoreShmFacade final : public IKVStoreFacade {
public:
    LocalKVStoreShmFacade(int32_t sid,
                          std::shared_ptr<LocalShmHandle> shm,
                          std::unique_ptr<brpc::Channel> ssd_ch)
        : sid_(sid), shm_(std::move(shm)), ssd_ch_(std::move(ssd_ch))
    {
    }

    int32_t StoreNodeId() const override { return sid_; }
    bool    IsLocal() const override { return true; }
    bool    IsHealthy() const override
    {
        return shm_ && shm_->base != nullptr && shm_->len > 0 && shm_->fd >= 0;
    }

    void BatchWriteBlock(const BatchWriteBlockRequest& req,
                         BatchWriteBlockResponse* resp) override
    {
        std::vector<std::string> payloads;
        payloads.reserve(req.items_size());
        for (const auto& it : req.items()) payloads.push_back(it.payload());
        BatchWriteBlockPayloads(req, payloads, resp);
    }

    void BatchWriteBlockPayloads(const BatchWriteBlockRequest& req,
                                 const std::vector<std::string>& payloads,
                                 BatchWriteBlockResponse* resp) override
    {
        std::vector<KVStoreWritePayloadView> views;
        views.reserve(payloads.size());
        for (const auto& payload : payloads) views.push_back({payload.data(), payload.size()});
        BatchWriteBlockPayloadViews(req, views, resp);
    }

    void BatchWriteBlockPayloadViews(const BatchWriteBlockRequest& req,
                                     const std::vector<KVStoreWritePayloadView>& payloads,
                                     BatchWriteBlockResponse* resp) override
    {
        if (!IsHealthy()) {
            for (int i = 0; i < req.items_size(); ++i) {
                auto* wr = resp->add_results();
                wr->set_block_hash(req.items(i).block_hash());
                FillErr(wr->mutable_result(), "local shm not mapped");
            }
            return;
        }
        for (int i = 0; i < req.items_size(); ++i) {
            const WriteItem& it = req.items(i);
            auto* wr          = resp->add_results();
            wr->set_block_hash(it.block_hash());
            const size_t off = static_cast<size_t>(it.pool_offset());
            const KVStoreWritePayloadView payload =
                (i < static_cast<int>(payloads.size())) ? payloads[static_cast<size_t>(i)]
                                                        : KVStoreWritePayloadView{it.payload().data(), it.payload().size()};
            const size_t psz = payload.size;
            if (it.pool_offset() < 0 || off + psz > shm_->len) {
                FillErr(wr->mutable_result(), "pool_offset/payload out of range");
                continue;
            }
            if (payload.data != nullptr && psz > 0) std::memcpy(static_cast<char*>(shm_->base) + off, payload.data, psz);
            wr->mutable_result()->set_success(true);
            wr->set_bytes_written(static_cast<int32_t>(psz));
        }
    }

    void BatchReadBlock(const BatchReadBlockRequest& req, BatchReadBlockResponse* resp) override
    {
        std::vector<std::string> payloads;
        BatchReadBlockPayloads(req, resp, &payloads);
        for (int i = 0; i < resp->results_size() && i < static_cast<int>(payloads.size()); ++i) {
            if (!payloads[i].empty()) resp->mutable_results(i)->set_payload(std::move(payloads[i]));
        }
    }

    void BatchReadBlockPayloads(const BatchReadBlockRequest& req, BatchReadBlockResponse* resp,
                                std::vector<std::string>* payloads) override
    {
        if (payloads != nullptr) payloads->clear();
        if (!IsHealthy()) {
            for (int i = 0; i < req.items_size(); ++i) {
                auto* rr = resp->add_results();
                rr->set_block_hash(req.items(i).block_hash());
                FillErr(rr->mutable_result(), "local shm not mapped");
                if (payloads != nullptr) payloads->push_back(std::string());
            }
            return;
        }
        if (payloads != nullptr) payloads->reserve(req.items_size());
        for (int i = 0; i < req.items_size(); ++i) {
            const ReadItem& it = req.items(i);
            auto* rr          = resp->add_results();
            rr->set_block_hash(it.block_hash());
            const size_t off = static_cast<size_t>(it.pool_offset());
            const int bs     = it.block_size() > 0 ? it.block_size() : shm_->block_size;
            if (it.pool_offset() < 0 || off + static_cast<size_t>(bs) > shm_->len) {
                FillErr(rr->mutable_result(), "pool_offset out of range");
                if (payloads != nullptr) payloads->push_back(std::string());
                continue;
            }
            const char* p = static_cast<const char*>(shm_->base) + off;
            if (payloads != nullptr) payloads->emplace_back(p, static_cast<size_t>(bs));
            rr->mutable_result()->set_success(true);
            rr->set_compression(CompressionType::COMPRESSION_NONE);
            rr->set_original_size(bs);
        }
    }

    void BatchReadBlockPayloadViews(const BatchReadBlockRequest& req,
                                    BatchReadBlockResponse* resp,
                                    std::vector<KVStoreReadPayloadView>* views) override
    {
        if (views != nullptr) views->clear();
        if (!IsHealthy()) {
            for (int i = 0; i < req.items_size(); ++i) {
                auto* rr = resp->add_results();
                rr->set_block_hash(req.items(i).block_hash());
                FillErr(rr->mutable_result(), "local shm not mapped");
                if (views != nullptr) views->push_back(KVStoreReadPayloadView{});
            }
            return;
        }
        if (views != nullptr) views->reserve(req.items_size());
        for (int i = 0; i < req.items_size(); ++i) {
            const ReadItem& it = req.items(i);
            auto* rr = resp->add_results();
            rr->set_block_hash(it.block_hash());
            const size_t off = static_cast<size_t>(it.pool_offset());
            const int bs = it.block_size() > 0 ? it.block_size() : shm_->block_size;
            if (it.pool_offset() < 0 || off + static_cast<size_t>(bs) > shm_->len) {
                FillErr(rr->mutable_result(), "pool_offset out of range");
                if (views != nullptr) views->push_back(KVStoreReadPayloadView{});
                continue;
            }
            const char* p = static_cast<const char*>(shm_->base) + off;
            rr->mutable_result()->set_success(true);
            rr->set_compression(CompressionType::COMPRESSION_NONE);
            rr->set_original_size(bs);
            if (views != nullptr) {
                KVStoreReadPayloadView view;
                view.ok = true;
                view.data = p;
                view.size = static_cast<size_t>(bs);
                view.owner = shm_;
                view.kind = "local_shm_view";
                views->push_back(std::move(view));
            }
        }
    }

    void BatchReadFromSSD(const BatchReadFromSSDRequest& req,
                          BatchReadFromSSDResponse* resp) override
    {
        if (ssd_ch_ == nullptr) {
            for (int i = 0; i < req.items_size(); ++i) {
                auto* sr = resp->add_results();
                sr->set_block_hash(req.items(i).block_hash());
                FillErr(sr->mutable_result(), "ssd path requires store BRPC");
            }
            return;
        }
        KVDataService_Stub stub(ssd_ch_.get());
        brpc::Controller cntl;
        stub.BatchReadFromSSD(&cntl, &req, resp, nullptr);
    }

    void WriteBlock(const WriteBlockRequest& req, WriteBlockResponse* resp) override
    {
        if (!IsHealthy()) {
            auto* wr = resp->mutable_result();
            wr->set_block_hash(req.item().block_hash());
            FillErr(wr->mutable_result(), "local shm not mapped");
            return;
        }
        const WriteItem& it = req.item();
        WriteBlockPayload(req, it.payload().data(), it.payload().size(), resp);
    }

    void WriteBlockPayload(const WriteBlockRequest& req,
                           const char* payload,
                           size_t payload_len,
                           WriteBlockResponse* resp) override
    {
        if (!IsHealthy()) {
            auto* wr = resp->mutable_result();
            wr->set_block_hash(req.item().block_hash());
            FillErr(wr->mutable_result(), "local shm not mapped");
            return;
        }
        const WriteItem& it = req.item();
        auto* wr          = resp->mutable_result();
        wr->set_block_hash(it.block_hash());
        const size_t off = static_cast<size_t>(it.pool_offset());
        const size_t psz = payload_len;
        if (it.pool_offset() < 0 || off + psz > shm_->len) {
            FillErr(wr->mutable_result(), "pool_offset/payload out of range");
            return;
        }
        if (payload != nullptr && psz > 0) {
            std::memcpy(static_cast<char*>(shm_->base) + off, payload, psz);
        }
        wr->mutable_result()->set_success(true);
        wr->set_bytes_written(static_cast<int32_t>(psz));
    }

    void ReadBlock(const ReadBlockRequest& req, ReadBlockResponse* resp) override
    {
        std::string payload;
        ReadBlockPayload(req, resp, &payload);
        if (!payload.empty() && resp->has_result()) {
            resp->mutable_result()->set_payload(std::move(payload));
        }
    }

    void ReadBlockPayload(const ReadBlockRequest& req,
                          ReadBlockResponse* resp,
                          std::string* payload) override
    {
        if (!IsHealthy()) {
            auto* rr = resp->mutable_result();
            rr->set_block_hash(req.item().block_hash());
            FillErr(rr->mutable_result(), "local shm not mapped");
            return;
        }
        const ReadItem& it = req.item();
        auto* rr          = resp->mutable_result();
        rr->set_block_hash(it.block_hash());
        const size_t off = static_cast<size_t>(it.pool_offset());
        const int bs     = it.block_size() > 0 ? it.block_size() : shm_->block_size;
        if (it.pool_offset() < 0 || off + static_cast<size_t>(bs) > shm_->len) {
            FillErr(rr->mutable_result(), "pool_offset out of range");
            return;
        }
        const char* src = static_cast<const char*>(shm_->base) + off;
        if (payload != nullptr) {
            payload->assign(src, static_cast<size_t>(bs));
        }
        rr->mutable_result()->set_success(true);
        rr->set_compression(CompressionType::COMPRESSION_NONE);
        rr->set_original_size(bs);
    }

    void ReadFromSSD(const ReadFromSSDRequest& req, ReadFromSSDResponse* resp) override
    {
        if (ssd_ch_ == nullptr) {
            auto* sr = resp->mutable_result();
            sr->set_block_hash(req.item().block_hash());
            FillErr(sr->mutable_result(), "ssd path requires store BRPC");
            return;
        }
        KVDataService_Stub stub(ssd_ch_.get());
        brpc::Controller cntl;
        stub.ReadFromSSD(&cntl, &req, resp, nullptr);
    }

private:
    int32_t sid_{0};
    std::shared_ptr<LocalShmHandle> shm_;
    std::unique_ptr<brpc::Channel> ssd_ch_;
};

struct KVStoreFacadeRegistry::State {
    std::mutex              mu;
    std::condition_variable cv;
    std::string             local_host;
    std::string             conninfo;
    std::atomic<bool>       stop{false};
    std::atomic<uint64_t>   gen{0};

    std::map<int32_t, std::shared_ptr<IKVStoreFacade>> manual;
    std::map<int32_t, std::shared_ptr<IKVStoreFacade>> resolved;
    std::map<int32_t, std::string>                    dn_endpoints;
    int32_t                                           num_dns = 0;
    int32_t                                           num_stores = 0;

    std::thread poll_thread;
    std::thread notify_thread;

    int32_t period_ms        = 5000;
    int32_t min_interval_ms  = 1000;
    int64_t last_refresh_ms  = 0;
    bool    notify_enabled   = false;

    static std::unique_ptr<brpc::Channel> MakeChannel(const std::string& ep, int timeout_ms)
    {
        auto ch = std::make_unique<brpc::Channel>();
        brpc::ChannelOptions opt;
        opt.protocol            = "baidu_std";
        opt.timeout_ms          = timeout_ms;
        opt.connect_timeout_ms  = 2000;
        opt.max_retry           = 0;
        opt.connection_type     = "pooled";
        if (ch->Init(ep.c_str(), &opt) != 0) return nullptr;
        return ch;
    }

    void RefreshFromCatalog()
    {
        std::map<int32_t, std::shared_ptr<IKVStoreFacade>> next;
        std::map<int32_t, std::shared_ptr<IKVStoreFacade>> man_copy;
        std::map<int32_t, std::string> next_dns;
        int32_t next_num_dns = 0;
        int32_t next_num_stores = 0;
        {
            std::lock_guard<std::mutex> lk(mu);
            man_copy = manual;
        }
        const int timeout_ms = EnvInt32("FALCON_KV_FACADE_BRPC_TIMEOUT_MS", 30000);

        if (!conninfo.empty()) {
            PGconn* c = PQconnectdb(conninfo.c_str());
            if (PQstatus(c) == CONNECTION_OK) {
                PGresult* dn = PQexec(
                    c,
                    "SELECT server_id, pg_host, kv_brpc_port, healthy "
                    "FROM pg_catalog.falcon_dn_node ORDER BY server_id;");
                if (PQresultStatus(dn) == PGRES_TUPLES_OK) {
                    const int rows = PQntuples(dn);
                    for (int i = 0; i < rows; ++i) {
                        const int32_t sid =
                            static_cast<int32_t>(std::strtol(PQgetvalue(dn, i, 0), nullptr, 10));
                        const char* host = PQgetvalue(dn, i, 1);
                        const int brpc_port =
                            static_cast<int>(std::strtol(PQgetvalue(dn, i, 2), nullptr, 10));
                        const char* healthy_v = PQgetvalue(dn, i, 3);
                        const bool healthy =
                            (PQgetisnull(dn, i, 3) == 0 && healthy_v[0] == 't');
                        if (!healthy) continue;
                        next_dns[sid] = MakeEndpoint(host, brpc_port);
                        ++next_num_dns;
                    }
                }
                PQclear(dn);

                PGresult* r = PQexec(c,
                                     "SELECT store_node_id, host_node_name, host, brpc_port, "
                                     "shm_name, dram_pool_bytes, block_size, healthy "
                                     "FROM pg_catalog.falcon_store_node ORDER BY store_node_id;");
                if (PQresultStatus(r) == PGRES_TUPLES_OK) {
                    const int rows = PQntuples(r);
                    for (int i = 0; i < rows; ++i) {
                        const int32_t sid =
                            static_cast<int32_t>(std::strtol(PQgetvalue(r, i, 0), nullptr, 10));
                        const char* host_node = PQgetvalue(r, i, 1);
                        const char* host      = PQgetvalue(r, i, 2);
                        const int brpc_port =
                            static_cast<int>(std::strtol(PQgetvalue(r, i, 3), nullptr, 10));
                        const char* shm_name = PQgetvalue(r, i, 4);
                        const int64_t dram =
                            std::strtoll(PQgetvalue(r, i, 5), nullptr, 10);
                        const int32_t bs =
                            static_cast<int32_t>(std::strtol(PQgetvalue(r, i, 6), nullptr, 10));
                        const char* healthy_v = PQgetvalue(r, i, 7);
                        const bool healthy =
                            (PQgetisnull(r, i, 7) == 0 && healthy_v[0] == 't');
                        ++next_num_stores;

                        if (!healthy) {
                            next[sid] = std::make_shared<UnhealthyKVStoreFacade>(sid);
                            continue;
                        }

                        const std::string ep = MakeEndpoint(host, brpc_port);

                        const bool colocated =
                            (local_host == std::string(host_node)) && shm_name[0] != '\0' && dram > 0;
                        if (colocated) {
                            auto ssd_ch = MakeChannel(ep, timeout_ms);
                            const std::string path = ShmOpenName(shm_name);
                            const int fd          = ::shm_open(path.c_str(), O_RDWR, 0666);
                            if (fd >= 0) {
                                void* base =
                                    ::mmap(nullptr, static_cast<size_t>(dram), PROT_READ | PROT_WRITE,
                                           MAP_SHARED, fd, 0);
                                if (base != MAP_FAILED) {
                                    auto h = std::shared_ptr<LocalShmHandle>(new LocalShmHandle(),
                                                                             [fd, dram](LocalShmHandle* p) {
                                                                                 if (p->base != nullptr &&
                                                                                     p->base != MAP_FAILED) {
                                                                                     ::munmap(p->base,
                                                                                              static_cast<
                                                                                                  size_t>(
                                                                                                  dram));
                                                                                 }
                                                                                 if (fd >= 0) ::close(fd);
                                                                                 delete p;
                                                                             });
                                    h->fd         = fd;
                                    h->base       = base;
                                    h->len        = static_cast<size_t>(dram);
                                    h->block_size = bs > 0 ? bs : 65536;
                                    next[sid] = std::make_shared<LocalKVStoreShmFacade>(
                                        sid, std::move(h), std::move(ssd_ch));
                                    continue;
                                }
                                ::close(fd);
                            }
                        }
                        auto kvch = MakeChannel(ep, timeout_ms);
                        if (kvch) {
                            next[sid] = std::make_shared<RemoteKVStoreFacade>(sid, std::move(kvch));
                        } else {
                            next[sid] = std::make_shared<UnhealthyKVStoreFacade>(sid);
                        }
                    }
                }
                PQclear(r);
            }
            PQfinish(c);
        }

        for (const auto& kv : man_copy) {
            next[kv.first] = kv.second;
        }

        {
            std::lock_guard<std::mutex> lk(mu);
            resolved = std::move(next);
            dn_endpoints = std::move(next_dns);
            num_dns = next_num_dns;
            num_stores = next_num_stores;
            gen.fetch_add(1, std::memory_order_release);
        }
        cv.notify_all();
    }

    void PollLoop()
    {
        while (!stop.load(std::memory_order_acquire)) {
            for (int t = 0; t < period_ms && !stop.load(std::memory_order_acquire); t += 100) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
            if (stop.load(std::memory_order_acquire)) break;
            const int64_t now = SteadyMs();
            if (now - last_refresh_ms < min_interval_ms) continue;
            last_refresh_ms = now;
            RefreshFromCatalog();
        }
    }

    void NotifyLoop()
    {
        PGconn* c = PQconnectdb(conninfo.c_str());
        if (PQstatus(c) != CONNECTION_OK) {
            PQfinish(c);
            return;
        }
        (void)PQexec(c, "LISTEN falcon_kv_store_membership");
        (void)PQexec(c, "LISTEN falcon_kv_dn_membership");
        while (!stop.load(std::memory_order_acquire)) {
            if (PQconsumeInput(c) != 1) {
                std::this_thread::sleep_for(std::chrono::milliseconds(200));
                continue;
            }
            PGnotify* n = nullptr;
            while ((n = PQnotifies(c)) != nullptr) {
                last_refresh_ms = 0;
                PQfreemem(n);
            }
            const int sock = PQsocket(c);
            if (sock < 0) break;
            fd_set rfds;
            FD_ZERO(&rfds);
            FD_SET(sock, &rfds);
            struct timeval tv;
            tv.tv_sec  = 0;
            tv.tv_usec = 200000;
            (void)select(sock + 1, &rfds, nullptr, nullptr, &tv);
        }
        PQfinish(c);
    }
};

KVStoreFacadeRegistry::KVStoreFacadeRegistry() : state_(new State) {}

KVStoreFacadeRegistry::~KVStoreFacadeRegistry() { Stop(); }

void KVStoreFacadeRegistry::Start(const std::string& local_host_node_name,
                                  const std::string& cn_libpq_conninfo)
{
    Stop();
    state_->stop.store(false, std::memory_order_release);
    state_->local_host      = local_host_node_name;
    state_->conninfo        = cn_libpq_conninfo;
    state_->period_ms       = EnvInt32("FALCON_KV_MEMBERSHIP_REFRESH_PERIOD_MS", 5000);
    state_->min_interval_ms = EnvInt32("FALCON_KV_MEMBERSHIP_REFRESH_MIN_INTERVAL_MS", 1000);
    state_->notify_enabled =
        EnvInt32("FALCON_KV_MEMBERSHIP_NOTIFY_ENABLED", 0) != 0;
    state_->last_refresh_ms = 0;
    state_->RefreshFromCatalog();

    state_->poll_thread = std::thread([this] { state_->PollLoop(); });
    if (state_->notify_enabled && !state_->conninfo.empty()) {
        state_->notify_thread = std::thread([this] { state_->NotifyLoop(); });
    }
}

void KVStoreFacadeRegistry::Stop()
{
    state_->stop.store(true, std::memory_order_release);
    state_->cv.notify_all();
    if (state_->poll_thread.joinable()) state_->poll_thread.join();
    if (state_->notify_thread.joinable()) state_->notify_thread.join();
}

RefreshResult KVStoreFacadeRegistry::RefreshNow(int32_t /*timeout_ms*/)
{
    const int64_t now = SteadyMs();
    {
        std::lock_guard<std::mutex> lk(state_->mu);
        if (now - state_->last_refresh_ms < state_->min_interval_ms) {
            RefreshResult out{};
            out.num_dns = state_->num_dns;
            out.num_stores = state_->num_stores;
            out.generation = state_->gen.load(std::memory_order_acquire);
            return out;
        }
        state_->last_refresh_ms = now;
    }
    state_->RefreshFromCatalog();
    RefreshResult out{};
    {
        std::lock_guard<std::mutex> lk(state_->mu);
        out.num_dns = state_->num_dns;
        out.num_stores = state_->num_stores;
        out.generation = state_->gen.load(std::memory_order_acquire);
    }
    return out;
}

void KVStoreFacadeRegistry::NotifyRpcFailure(int32_t /*store_or_dn_id*/, RpcFailureKind /*kind*/)
{
    state_->last_refresh_ms = 0;
}

uint64_t KVStoreFacadeRegistry::Generation() const noexcept
{
    return state_->gen.load(std::memory_order_acquire);
}

void KVStoreFacadeRegistry::RegisterLocalShm(int32_t store_id, LocalShmHandle* handle)
{
    if (handle == nullptr) return;
    std::shared_ptr<LocalShmHandle> shr(handle, [](LocalShmHandle*) {});
    const int timeout_ms = EnvInt32("FALCON_KV_FACADE_BRPC_TIMEOUT_MS", 30000);
    std::string ssd_ep;
    const char* env = std::getenv("FALCON_KV_STORE_BRPC_ENDPOINT");
    if (env != nullptr && env[0] != '\0') ssd_ep = env;
    auto ssd_ch = ssd_ep.empty() ? nullptr : State::MakeChannel(ssd_ep, timeout_ms);
    auto fac    = std::make_shared<LocalKVStoreShmFacade>(store_id, std::move(shr), std::move(ssd_ch));
    std::lock_guard<std::mutex> lk(state_->mu);
    state_->manual[store_id] = fac;
}

void KVStoreFacadeRegistry::RegisterRemote(int32_t store_id, const std::string& brpc_endpoint)
{
    const int timeout_ms = EnvInt32("FALCON_KV_FACADE_BRPC_TIMEOUT_MS", 30000);
    auto ch              = State::MakeChannel(brpc_endpoint, timeout_ms);
    std::shared_ptr<IKVStoreFacade> fac;
    if (ch) {
        fac = std::make_shared<RemoteKVStoreFacade>(store_id, std::move(ch));
    } else {
        fac = std::make_shared<UnhealthyKVStoreFacade>(store_id);
    }
    std::lock_guard<std::mutex> lk(state_->mu);
    state_->manual[store_id] = fac;
}

std::shared_ptr<IKVStoreFacade> KVStoreFacadeRegistry::Resolve(int32_t store_id)
{
    std::lock_guard<std::mutex> lk(state_->mu);
    auto it = state_->resolved.find(store_id);
    if (it == state_->resolved.end()) return nullptr;
    return it->second;
}

std::map<int32_t, std::string> KVStoreFacadeRegistry::SnapshotDnEndpoints()
{
    std::lock_guard<std::mutex> lk(state_->mu);
    return state_->dn_endpoints;
}

std::map<int32_t, bool> KVStoreFacadeRegistry::SnapshotStoreLocality()
{
    std::map<int32_t, bool> out;
    std::lock_guard<std::mutex> lk(state_->mu);
    for (const auto& kv : state_->resolved) {
        out[kv.first] = kv.second && kv.second->IsLocal();
    }
    return out;
}

}  // namespace falconfs::kv
