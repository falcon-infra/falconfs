#include "vllm_kv_cache/src/service/kv_store_admin_brpc_service.h"

#include <functional>
#include <string>

#include "kv_common.pb.h"
#include "kv_store_admin_service.pb.h"
#include "vllm_kv_cache/src/metadata/kv_metadata_engine.h"
#include "vllm_kv_cache/src/store/kv_store_engine.h"

namespace falconfs::kv {

namespace {

void FillItemOk(ItemResultMeta* m)
{
    m->set_success(true);
    m->set_error_code(ErrorCode::OK);
    m->set_retryable(false);
    m->clear_error_message();
}

void FillItemErr(ItemResultMeta* m, ErrorCode code, bool retryable, const char* msg)
{
    m->set_success(false);
    m->set_error_code(code);
    m->set_retryable(retryable);
    if (msg != nullptr) {
        m->set_error_message(msg);
    } else {
        m->clear_error_message();
    }
}

void FillFromEngineMeta(const EngineResultMeta& src, ItemResultMeta* dst)
{
    dst->set_success(src.success);
    dst->set_error_code(static_cast<ErrorCode>(src.error_code));
    dst->set_retryable(src.retryable);
    dst->set_error_message(src.error_message);
}

class KVStoreAdminBrpcServiceImplDN final : public KVStoreAdminBrpcServiceImplBase {
public:
    explicit KVStoreAdminBrpcServiceImplDN(std::shared_ptr<KVMetadataEngine> engine,
                                           std::function<void()> after_region_registered,
                                           std::function<void(int32_t, const std::string&)> after_store_epoch_bump)
        : engine_(std::move(engine)),
          after_region_registered_(std::move(after_region_registered)),
          after_store_epoch_bump_(std::move(after_store_epoch_bump)) {}

    void RegisterStoreRegion(const RegisterStoreRegionRequest& req,
                             RegisterStoreRegionResponse* resp) override {
        if (engine_ == nullptr) {
            FillItemErr(resp->mutable_result(), ErrorCode::INTERNAL_ERROR, false, "metadata engine null");
            return;
        }
        const StoreRegionInfo& pr = req.region();
        EngineStoreRegion r;
        r.store_node_id = pr.store_node_id();
        r.base_offset   = pr.base_offset();
        r.region_bytes  = pr.region_bytes();
        r.block_size    = pr.block_size();
        r.store_epoch   = pr.store_epoch();

        const bool had_region = engine_->HasRegion(r.store_node_id);
        const int64_t old_epoch = engine_->RegionStoreEpoch(r.store_node_id);
        EngineResultMeta meta = engine_->RegisterStoreRegion(r);
        FillFromEngineMeta(meta, resp->mutable_result());
        resp->set_dn_epoch(engine_->DnEpoch());
        if (meta.success && had_region && r.store_epoch > old_epoch && after_store_epoch_bump_) {
            after_store_epoch_bump_(r.store_node_id, req.store_brpc_endpoint());
        }
        if (meta.success && !had_region && after_region_registered_) {
            after_region_registered_();
        }
    }

    void Heartbeat(const HeartbeatRequest& req, HeartbeatResponse* resp) override {
        (void) req;
        if (engine_ == nullptr) {
            FillItemErr(resp->mutable_result(), ErrorCode::INTERNAL_ERROR, false, "metadata engine null");
            return;
        }
        FillItemOk(resp->mutable_result());
        resp->set_dn_epoch(engine_->DnEpoch());
        resp->set_marked_unhealthy(false);
    }

    void SpillBlockToSSD(const SpillBlockToSSDRequest& /*req*/,
                         SpillBlockToSSDResponse* resp) override {
        FillItemErr(resp->mutable_result(), ErrorCode::INTERNAL_ERROR, false,
                    "SpillBlockToSSD not hosted on DN (v6.5 P2 falcon_kv_store)");
    }

    void ValidateEvictedPaths(const ValidateEvictedPathsRequest& /*req*/,
                              ValidateEvictedPathsResponse* resp) override {
        FillItemErr(resp->mutable_result(), ErrorCode::INTERNAL_ERROR, false,
                    "ValidateEvictedPaths not hosted on DN (use Store KVStoreAdminService)");
    }

private:
    std::shared_ptr<KVMetadataEngine> engine_;
    std::function<void()> after_region_registered_;
    std::function<void(int32_t, const std::string&)> after_store_epoch_bump_;
};

class KVStoreAdminBrpcServiceImplStore final : public KVStoreAdminBrpcServiceImplBase {
public:
    explicit KVStoreAdminBrpcServiceImplStore(std::shared_ptr<KVStoreEngine> engine)
        : engine_(std::move(engine)) {}

    void RegisterStoreRegion(const RegisterStoreRegionRequest& /*req*/,
                             RegisterStoreRegionResponse* resp) override {
        FillItemErr(resp->mutable_result(), ErrorCode::INTERNAL_ERROR, false,
                    "RegisterStoreRegion not hosted on falcon_kv_store");
    }

    void Heartbeat(const HeartbeatRequest& /*req*/, HeartbeatResponse* resp) override {
        FillItemErr(resp->mutable_result(), ErrorCode::INTERNAL_ERROR, false,
                    "Heartbeat not hosted on falcon_kv_store (use DN KVStoreAdminService)");
    }

    void SpillBlockToSSD(const SpillBlockToSSDRequest& req, SpillBlockToSSDResponse* resp) override {
        if (engine_ == nullptr) {
            FillItemErr(resp->mutable_result(), ErrorCode::INTERNAL_ERROR, false, "store engine null");
            return;
        }
        const std::string hash(req.block_hash().data(), static_cast<std::size_t>(req.block_hash().size()));
        std::string evicted_path;
        StoreWriteResult wr =
            engine_->SpillBlockToSSD(hash,
                                     req.pool_offset(),
                                     req.expected_version(),
                                     req.expected_store_epoch(),
                                     req.dram_read_size(),
                                     &evicted_path);
        ItemResultMeta* m = resp->mutable_result();
        m->set_success(wr.result.success);
        m->set_error_code(static_cast<ErrorCode>(wr.result.error_code));
        m->set_retryable(wr.result.retryable);
        m->set_error_message(wr.result.error_message);
        if (wr.result.success) {
            resp->set_evicted_path(std::move(evicted_path));
            resp->set_crc32(wr.crc32);
        }
    }


    void ValidateEvictedPaths(const ValidateEvictedPathsRequest& req,
                              ValidateEvictedPathsResponse* resp) override {
        if (engine_ == nullptr) {
            FillItemErr(resp->mutable_result(), ErrorCode::INTERNAL_ERROR, false, "store engine null");
            return;
        }
        if (req.store_node_id() != engine_->StoreNodeId()) {
            FillItemErr(resp->mutable_result(), ErrorCode::INVALID_ARGUMENT, false, "store_node_id mismatch");
            return;
        }
        FillItemOk(resp->mutable_result());
        int64_t valid = 0;
        int64_t invalid = 0;
        for (const auto& item : req.items()) {
            auto* r = resp->add_results();
            r->set_block_hash(item.block_hash());
            const std::string hash(item.block_hash().data(), static_cast<std::size_t>(item.block_hash().size()));
            StoreResultMeta vr = engine_->ValidateEvictedPath(hash, item.evicted_path());
            auto* m = r->mutable_result();
            m->set_success(vr.success);
            m->set_error_code(static_cast<ErrorCode>(vr.error_code));
            m->set_retryable(vr.retryable);
            m->set_error_message(vr.error_message);
            r->set_valid(vr.success);
            if (vr.success) {
                ++valid;
            } else {
                ++invalid;
            }
        }
        resp->set_valid_count(valid);
        resp->set_invalid_count(invalid);
    }

private:
    std::shared_ptr<KVStoreEngine> engine_;
};

struct ClosureGuard {
    ::google::protobuf::Closure* done;
    explicit ClosureGuard(::google::protobuf::Closure* d) : done(d) {}
    ~ClosureGuard() {
        if (done != nullptr) {
            done->Run();
        }
    }
};

}  // namespace

KVStoreAdminBrpcServiceAdapter::KVStoreAdminBrpcServiceAdapter(
    std::shared_ptr<KVStoreAdminBrpcServiceImplBase> impl)
    : impl_(std::move(impl)) {}

void KVStoreAdminBrpcServiceAdapter::RegisterStoreRegion(::google::protobuf::RpcController* /*controller*/,
                                                         const RegisterStoreRegionRequest* request,
                                                         RegisterStoreRegionResponse* response,
                                                         ::google::protobuf::Closure* done) {
    ClosureGuard guard(done);
    impl_->RegisterStoreRegion(*request, response);
}

void KVStoreAdminBrpcServiceAdapter::Heartbeat(::google::protobuf::RpcController* /*controller*/,
                                               const HeartbeatRequest* request,
                                               HeartbeatResponse* response,
                                               ::google::protobuf::Closure* done) {
    ClosureGuard guard(done);
    impl_->Heartbeat(*request, response);
}

void KVStoreAdminBrpcServiceAdapter::SpillBlockToSSD(::google::protobuf::RpcController* /*controller*/,
                                                     const SpillBlockToSSDRequest* request,
                                                     SpillBlockToSSDResponse* response,
                                                     ::google::protobuf::Closure* done) {
    ClosureGuard guard(done);
    impl_->SpillBlockToSSD(*request, response);
}

void KVStoreAdminBrpcServiceAdapter::ValidateEvictedPaths(::google::protobuf::RpcController* /*controller*/,
                                                          const ValidateEvictedPathsRequest* request,
                                                          ValidateEvictedPathsResponse* response,
                                                          ::google::protobuf::Closure* done) {
    ClosureGuard guard(done);
    impl_->ValidateEvictedPaths(*request, response);
}

std::shared_ptr<KVStoreAdminBrpcServiceImplBase> CreateKVStoreAdminBrpcServiceImplForDn(
    std::shared_ptr<KVMetadataEngine> engine,
    std::function<void()> after_region_registered,
    std::function<void(int32_t, const std::string&)> after_store_epoch_bump) {
    return std::make_shared<KVStoreAdminBrpcServiceImplDN>(std::move(engine),
                                                           std::move(after_region_registered),
                                                           std::move(after_store_epoch_bump));
}

std::shared_ptr<KVStoreAdminBrpcServiceImplBase> CreateKVStoreAdminBrpcServiceImplForStore(
    std::shared_ptr<KVStoreEngine> engine) {
    return std::make_shared<KVStoreAdminBrpcServiceImplStore>(std::move(engine));
}

}  // namespace falconfs::kv
