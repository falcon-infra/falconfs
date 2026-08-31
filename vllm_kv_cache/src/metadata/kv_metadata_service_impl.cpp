#include "vllm_kv_cache/src/metadata/kv_metadata_service_impl.h"

#include <chrono>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace falconfs::kv {

namespace {

EngineResultMeta MakeRemoteTierRefusalMeta() {
    EngineResultMeta m{};
    m.success = false;
    m.error_code = static_cast<int32_t>(ErrorCode::INTERNAL_ERROR);
    m.retryable = false;
    m.error_message = "use *SplitForPoolWorker";
    return m;
}

EngineResultMeta CatalogStoredWithoutDramSlot() {
    EngineResultMeta err{};
    err.success = false;
    err.error_code = static_cast<int32_t>(ErrorCode::CAS_CONFLICT);
    err.retryable = true;
    err.error_message = "catalog STORED row has no DRAM slot";
    return err;
}
}  // namespace

KVMetadataServiceImpl::KVMetadataServiceImpl()
    : engine_(std::make_shared<KVMetadataEngine>()) {}

KVMetadataServiceImpl::KVMetadataServiceImpl(std::shared_ptr<KVMetadataEngine> engine)
    : engine_(std::move(engine)) {}

KVMetadataServiceImpl::KVMetadataServiceImpl(std::shared_ptr<KVMetadataEngine> engine, ClockFn clock)
    : engine_(std::move(engine)), clock_(std::move(clock)) {}

int64_t KVMetadataServiceImpl::Now() const {
    if (clock_) return clock_();
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

void KVMetadataServiceImpl::FillResultMeta(const EngineResultMeta& src, ItemResultMeta* dst) {
    dst->set_success(src.success);
    dst->set_error_code(static_cast<ErrorCode>(src.error_code));
    dst->set_retryable(src.retryable);
    dst->set_error_message(src.error_message);
}

void KVMetadataServiceImpl::FillLocation(const EngineBlockLocation& src, BlockLocation* dst) {
    dst->set_store_node_id(src.store_node_id);
    dst->set_pool_offset(src.pool_offset);
    dst->set_evicted_path(src.evicted_path);
    dst->set_store_epoch(src.store_epoch);
}

void KVMetadataServiceImpl::FillLease(const EngineLeaseInfo& src, LeaseInfo* dst) {
    dst->set_lease_token(src.lease_token);
    dst->set_lease_expire_ms(src.lease_expire_ms);
    dst->set_dn_epoch(src.dn_epoch);
    dst->set_store_epoch(src.store_epoch);
}

void KVMetadataServiceImpl::FillAllocateResultFromEngine(const EngineAllocateResult& src,
                                                           AllocateResult* result) {
    FillResultMeta(src.result, result->mutable_result());
    if (src.row.has_value()) {
        FillLocation(src.row->location, result->mutable_location());
        result->set_version(src.row->version);
    }
    if (src.lease.has_value()) {
        FillLease(*src.lease, result->mutable_lease());
    }
    result->set_reused_existing_allocation(src.reused_existing_allocation);
}

void KVMetadataServiceImpl::FillLookupResultFromEngine(const EngineLookupResult& out,
                                                       LookupResult* result) {
    FillResultMeta(out.result, result->mutable_result());
    if (out.row.has_value()) {
        result->set_status(static_cast<BlockStatus>(out.row->status));
        FillLocation(out.row->location, result->mutable_location());
        result->set_version(out.row->version);
    } else {
        result->set_status(BlockStatus::BLOCK_STATUS_UNSPECIFIED);
    }
    if (out.lease.has_value()) {
        FillLease(*out.lease, result->mutable_lease());
    }
    result->set_cacheable(out.cacheable);
    result->set_allocated_pending_store(out.allocated_pending_store);
    result->set_evicted_catalog_hit(out.evicted_catalog_hit);
}

void KVMetadataServiceImpl::BatchLookupWithLease(const BatchLookupRequest& request,
                                                 BatchLookupResponse* response) {
    response->clear_results();
    if (engine_->CatalogTier() == EngineCatalogTier::REMOTE_LIBPQ) {
        const int64_t now_ms = Now();
        const EngineResultMeta err = MakeRemoteTierRefusalMeta();
        for (const auto& item : request.items()) {
            auto* result = response->add_results();
            result->set_block_hash(item.block_hash());
            FillResultMeta(err, result->mutable_result());
        }
        response->set_server_time_ms(now_ms);
        return;
    }
    const int64_t now_ms = Now();
    for (const auto& item : request.items()) {
        auto* result = response->add_results();
        result->set_block_hash(item.block_hash());
        EngineLookupResult out = engine_->Lookup(item.block_hash(), item.renew_lease_on_hit(), now_ms);
        FillLookupResultFromEngine(out, result);
    }
    response->set_server_time_ms(now_ms);
}

void KVMetadataServiceImpl::BatchLookupWithLeaseSplitForPoolWorker(
    const BatchLookupRequest& request,
    BatchLookupResponse* response,
    const std::function<std::string(const std::string& sub_batch_payload)>& run_catalog_sub_batch) {
    response->clear_results();
    const int64_t now_ms = Now();
    std::vector<int> unresolved;
    BatchLookupRequest sub;
    sub.mutable_meta()->CopyFrom(request.meta());

    for (int i = 0; i < request.items_size(); ++i) {
        auto* slot = response->add_results();
        slot->set_block_hash(request.items(i).block_hash());
    }

    for (int i = 0; i < request.items_size(); ++i) {
        const auto& item = request.items(i);
        EngineLookupResult dram =
            engine_->LookupDramCacheOnly(item.block_hash(), item.renew_lease_on_hit(), now_ms);
        if (!dram.needs_catalog) {
            FillLookupResultFromEngine(dram, response->mutable_results(i));
            continue;
        }
        unresolved.push_back(i);
        sub.add_items()->CopyFrom(item);
    }

    if (sub.items_size() == 0) {
        response->set_server_time_ms(now_ms);
        return;
    }

    std::string cat_bytes;
    try {
        cat_bytes = run_catalog_sub_batch(sub.SerializeAsString());
    } catch (...) {
        cat_bytes.clear();
    }

    const int64_t after_ms = Now();
    BatchLookupResponse cat;
    if (cat_bytes.empty() || !cat.ParseFromString(cat_bytes) ||
        cat.results_size() != sub.items_size()) {
        EngineResultMeta err{};
        err.success = false;
        err.error_code = static_cast<int32_t>(ErrorCode::INTERNAL_ERROR);
        err.retryable = true;
        err.error_message = "catalog sub-batch failed";
        for (int idx : unresolved) {
            auto* r = response->mutable_results(idx);
            FillResultMeta(err, r->mutable_result());
            r->set_status(BlockStatus::BLOCK_STATUS_UNSPECIFIED);
        }
        response->set_server_time_ms(after_ms);
        return;
    }

    for (std::size_t j = 0; j < unresolved.size(); ++j) {
        const int i = unresolved[static_cast<std::size_t>(j)];
        const LookupResult& cr = cat.results(static_cast<int>(j));
        LookupResult* dest = response->mutable_results(i);
        dest->CopyFrom(cr);
        dest->set_block_hash(request.items(i).block_hash());
        if (request.items(i).renew_lease_on_hit() &&
            dest->result().success() &&
            dest->status() == BlockStatus::BLOCK_STATUS_STORED &&
            !dest->has_lease()) {
            FillResultMeta(CatalogStoredWithoutDramSlot(), dest->mutable_result());
            dest->set_status(BlockStatus::BLOCK_STATUS_UNSPECIFIED);
            dest->clear_location();
            dest->clear_version();
        }
    }
    response->set_server_time_ms(after_ms);
}

namespace {

struct AllocatePendingGroup {
    std::vector<int> result_indices;
    int32_t store_node_id = 0;
    int64_t slot_idx = 0;
    std::string block_hash;
};

}  // namespace

void KVMetadataServiceImpl::BatchAllocateWithLeaseSplitForPoolWorker(
    const BatchAllocateRequest& request,
    BatchAllocateResponse* response,
    const std::function<std::string(const std::string& sub_batch_payload)>& run_catalog_sub_batch) {
    response->clear_results();
    const int64_t now_ms = Now();
    for (int i = 0; i < request.items_size(); ++i) {
        response->add_results()->set_block_hash(request.items(i).block_hash());
    }

    BatchAllocateRequest sub;
    sub.mutable_meta()->CopyFrom(request.meta());
    sub.set_deduplicate_in_request(false);
    std::vector<AllocatePendingGroup> pending_groups;

    auto process_groups = [&](const std::vector<std::string>& hash_order,
                              const std::unordered_map<std::string, std::vector<int>>& groups) {
        for (const std::string& h : hash_order) {
            const std::vector<int>& indices = groups.at(h);
            const auto& item0 = request.items(indices[0]);
            EngineAllocatePass1Outcome p1 = engine_->AllocatePass1ReserveBitmap(
                item0.block_hash(),
                item0.block_size(),
                now_ms,
                item0.preferred_store_id(),
                item0.allow_fallback_store());
            if (p1.kind == EngineAllocatePass1Outcome::Kind::ReusedDram) {
                for (int idx : indices) {
                    FillAllocateResultFromEngine(p1.reused_dram, response->mutable_results(idx));
                    response->mutable_results(idx)->set_block_hash(request.items(idx).block_hash());
                }
                continue;
            }
            if (p1.kind == EngineAllocatePass1Outcome::Kind::Failed) {
                for (int idx : indices) {
                    FillResultMeta(p1.fail_meta, response->mutable_results(idx)->mutable_result());
                    response->mutable_results(idx)->set_reused_existing_allocation(false);
                }
                continue;
            }
            AllocateItem* si = sub.add_items();
            si->CopyFrom(item0);
            si->set_worker_reserved_store_node_id(p1.store_node_id);
            si->set_worker_reserved_pool_offset(p1.pool_offset);
            AllocatePendingGroup g;
            g.result_indices = indices;
            g.store_node_id = p1.store_node_id;
            g.slot_idx = p1.slot_idx;
            g.block_hash = item0.block_hash();
            pending_groups.push_back(std::move(g));
        }
    };

    if (!request.deduplicate_in_request()) {
        std::vector<std::string> hash_order;
        std::unordered_map<std::string, std::vector<int>> groups;
        for (int i = 0; i < request.items_size(); ++i) {
            const std::string key = request.items(i).block_hash() + '\0' + std::to_string(i);
            hash_order.push_back(key);
            groups[key] = {i};
        }
        process_groups(hash_order, groups);
    } else {
        std::vector<std::string> hash_order;
        std::unordered_map<std::string, std::vector<int>> groups;
        for (int i = 0; i < request.items_size(); ++i) {
            const std::string& h = request.items(i).block_hash();
            auto& v = groups[h];
            if (v.empty()) {
                hash_order.push_back(h);
            }
            v.push_back(i);
        }
        process_groups(hash_order, groups);
    }

    if (sub.items_size() == 0) {
        response->set_server_time_ms(now_ms);
        return;
    }

    std::string cat_bytes;
    try {
        cat_bytes = run_catalog_sub_batch(sub.SerializeAsString());
    } catch (...) {
        cat_bytes.clear();
    }
    const int64_t after_ms = Now();
    BatchAllocateResponse cat;
    if (cat_bytes.empty() || !cat.ParseFromString(cat_bytes) ||
        cat.results_size() != sub.items_size()) {
        EngineResultMeta err{};
        err.success = false;
        err.error_code = static_cast<int32_t>(ErrorCode::INTERNAL_ERROR);
        err.retryable = true;
        err.error_message = "catalog sub-batch failed";
        for (const AllocatePendingGroup& g : pending_groups) {
            engine_->RollbackAllocatePass1Reservation(g.store_node_id, g.slot_idx);
            for (int idx : g.result_indices) {
                FillResultMeta(err, response->mutable_results(idx)->mutable_result());
            }
        }
        response->set_server_time_ms(after_ms);
        return;
    }

    for (std::size_t j = 0; j < pending_groups.size(); ++j) {
        const AllocatePendingGroup& g = pending_groups[j];
        const AllocateResult& cr = cat.results(static_cast<int>(j));
        if (cr.result().success()) {
            EngineAllocateResult committed = engine_->CommitAllocatePass1AfterCatalogInsert(
                g.block_hash, g.store_node_id, g.slot_idx, after_ms);
            for (int idx : g.result_indices) {
                FillAllocateResultFromEngine(committed, response->mutable_results(idx));
                response->mutable_results(idx)->set_block_hash(request.items(idx).block_hash());
            }
        } else {
            engine_->RollbackAllocatePass1Reservation(g.store_node_id, g.slot_idx);
            for (int idx : g.result_indices) {
                response->mutable_results(idx)->CopyFrom(cr);
                response->mutable_results(idx)->set_block_hash(request.items(idx).block_hash());
            }
        }
    }
    response->set_server_time_ms(after_ms);
}

void KVMetadataServiceImpl::BatchRenewLeaseSplitForPoolWorker(
    const BatchRenewLeaseRequest& request,
    BatchRenewLeaseResponse* response,
    const std::function<std::string(const std::string& sub_batch_payload)>& /*run_catalog_sub_batch*/) {
    response->clear_results();
    const int64_t now_ms = Now();

    for (int i = 0; i < request.items_size(); ++i) {
        response->add_results()->set_block_hash(request.items(i).block_hash());
    }
    for (int i = 0; i < request.items_size(); ++i) {
        const auto& item = request.items(i);
        KVMetadataEngine::RenewLeasePass1Outcome p1 = engine_->RenewLeasePass1(
            item.block_hash(),
            item.lease_token(),
            item.expected_dn_epoch(),
            item.expected_store_epoch(),
            request.requested_ttl_ms(),
            now_ms);
        FillResultMeta(p1.dram.result, response->mutable_results(i)->mutable_result());
        if (p1.dram.result.success && p1.dram.lease.has_value()) {
            FillLease(*p1.dram.lease, response->mutable_results(i)->mutable_lease());
        }
    }

    response->set_server_time_ms(now_ms);
}

void KVMetadataServiceImpl::BatchUpdateBlockStatusSplitForPoolWorker(
    const BatchUpdateStatusRequest& request,
    BatchUpdateStatusResponse* response,
    const std::function<std::string(const std::string& sub_batch_payload)>& run_catalog_sub_batch) {
    response->clear_results();
    const int64_t now_ms = Now();
    BatchUpdateStatusRequest sub;
    sub.mutable_meta()->CopyFrom(request.meta());
    std::vector<int> unresolved;

    for (int i = 0; i < request.items_size(); ++i) {
        response->add_results()->set_block_hash(request.items(i).block_hash());
    }
    for (int i = 0; i < request.items_size(); ++i) {
        const auto& item = request.items(i);
        std::optional<EngineUpdateStatusResult> early = engine_->UpdateStatusPass1OrCatalog(
            item.block_hash(),
            static_cast<int32_t>(item.expected_from_status()),
            static_cast<int32_t>(item.to_status()),
            item.expected_version(),
            item.evicted_path(),
            item.allow_noop_if_already_target(),
            now_ms);
        if (early.has_value()) {
            FillResultMeta(early->result, response->mutable_results(i)->mutable_result());
            response->mutable_results(i)->set_new_version(early->new_version);
            response->mutable_results(i)->set_current_status(static_cast<BlockStatus>(early->current_status));
            continue;
        }
        unresolved.push_back(i);
        sub.add_items()->CopyFrom(item);
    }

    if (sub.items_size() == 0) {
        response->set_server_time_ms(now_ms);
        return;
    }

    std::string cat_bytes;
    try {
        cat_bytes = run_catalog_sub_batch(sub.SerializeAsString());
    } catch (...) {
        cat_bytes.clear();
    }
    const int64_t after_ms = Now();
    BatchUpdateStatusResponse cat;
    if (cat_bytes.empty() || !cat.ParseFromString(cat_bytes) ||
        cat.results_size() != sub.items_size()) {
        EngineResultMeta err{};
        err.success = false;
        err.error_code = static_cast<int32_t>(ErrorCode::INTERNAL_ERROR);
        err.retryable = true;
        err.error_message = "catalog sub-batch failed";
        for (int idx : unresolved) {
            FillResultMeta(err, response->mutable_results(idx)->mutable_result());
        }
        response->set_server_time_ms(after_ms);
        return;
    }

    for (std::size_t j = 0; j < unresolved.size(); ++j) {
        const int i = unresolved[j];
        const StatusUpdateResult& cr = cat.results(static_cast<int>(j));
        StatusUpdateResult* dest = response->mutable_results(i);
        dest->CopyFrom(cr);
        dest->set_block_hash(request.items(i).block_hash());
        if (cr.result().success()) {
            engine_->ApplyUpdateStatusAfterCatalogSuccess(
                request.items(i).block_hash(),
                static_cast<int32_t>(request.items(i).to_status()),
                cr.new_version(),
                static_cast<int32_t>(request.items(i).expected_from_status()));
        }
    }
    response->set_server_time_ms(after_ms);
}

void KVMetadataServiceImpl::BatchFreeAllocatedSplitForPoolWorker(
    const BatchFreeAllocatedRequest& request,
    BatchFreeAllocatedResponse* response,
    const std::function<std::string(const std::string& sub_batch_payload)>& run_catalog_sub_batch) {
    response->clear_results();
    const int64_t now_ms = Now();
    BatchFreeAllocatedRequest sub;
    sub.mutable_meta()->CopyFrom(request.meta());
    std::vector<int> unresolved;

    for (int i = 0; i < request.items_size(); ++i) {
        response->add_results()->set_block_hash(request.items(i).block_hash());
    }
    for (int i = 0; i < request.items_size(); ++i) {
        const auto& item = request.items(i);
        std::optional<EngineFreeAllocatedResult> early = engine_->FreeAllocatedPass1OrCatalog(
            item.block_hash(), item.expected_version(), item.force(), now_ms);
        if (early.has_value()) {
            FillResultMeta(early->result, response->mutable_results(i)->mutable_result());
            response->mutable_results(i)->set_new_version(early->new_version);
            continue;
        }
        unresolved.push_back(i);
        sub.add_items()->CopyFrom(item);
    }

    if (sub.items_size() == 0) {
        response->set_server_time_ms(now_ms);
        return;
    }

    std::string cat_bytes;
    try {
        cat_bytes = run_catalog_sub_batch(sub.SerializeAsString());
    } catch (...) {
        cat_bytes.clear();
    }
    const int64_t after_ms = Now();
    BatchFreeAllocatedResponse cat;
    if (cat_bytes.empty() || !cat.ParseFromString(cat_bytes) ||
        cat.results_size() != sub.items_size()) {
        EngineResultMeta err{};
        err.success = false;
        err.error_code = static_cast<int32_t>(ErrorCode::INTERNAL_ERROR);
        err.retryable = true;
        err.error_message = "catalog sub-batch failed";
        for (int idx : unresolved) {
            FillResultMeta(err, response->mutable_results(idx)->mutable_result());
        }
        response->set_server_time_ms(after_ms);
        return;
    }

    for (std::size_t j = 0; j < unresolved.size(); ++j) {
        const int i = unresolved[j];
        const FreeAllocatedResult& cr = cat.results(static_cast<int>(j));
        FreeAllocatedResult* dest = response->mutable_results(i);
        dest->CopyFrom(cr);
        dest->set_block_hash(request.items(i).block_hash());
        if (cr.result().success()) {
            engine_->ApplyFreeAfterCatalogDeleteSuccess(request.items(i).block_hash(),
                                                        request.items(i).expected_version());
        }
    }
    response->set_server_time_ms(after_ms);
}

void KVMetadataServiceImpl::BatchAllocateWithLease(const BatchAllocateRequest& request,
                                                   BatchAllocateResponse* response) {
    if (engine_->CatalogTier() == EngineCatalogTier::REMOTE_LIBPQ) {
        response->Clear();
        response->clear_results();
        const int64_t now_ms = Now();
        const EngineResultMeta err = MakeRemoteTierRefusalMeta();
        for (const auto& item : request.items()) {
            auto* result = response->add_results();
            result->set_block_hash(item.block_hash());
            FillResultMeta(err, result->mutable_result());
        }
        response->set_server_time_ms(now_ms);
        return;
    }
    response->clear_results();
    const int64_t now_ms = Now();
    std::unordered_map<std::string, EngineAllocateResult> dedup_cache;
    for (const auto& item : request.items()) {
        auto* result = response->add_results();
        result->set_block_hash(item.block_hash());

        EngineAllocateResult out;
        if (request.deduplicate_in_request()) {
            auto hit = dedup_cache.find(item.block_hash());
            if (hit != dedup_cache.end()) {
                out = hit->second;
            } else {
                out = engine_->Allocate(item.block_hash(),
                                        item.block_size(),
                                        now_ms,
                                        item.preferred_store_id(),
                                        item.allow_fallback_store());
                dedup_cache.emplace(item.block_hash(), out);
            }
        } else {
            out = engine_->Allocate(item.block_hash(),
                                    item.block_size(),
                                    now_ms,
                                    item.preferred_store_id(),
                                    item.allow_fallback_store());
        }

        FillResultMeta(out.result, result->mutable_result());
        if (out.row.has_value()) {
            FillLocation(out.row->location, result->mutable_location());
            result->set_version(out.row->version);
        }
        if (out.lease.has_value()) {
            FillLease(*out.lease, result->mutable_lease());
        }
        result->set_reused_existing_allocation(out.reused_existing_allocation);
    }
    response->set_server_time_ms(now_ms);
}

void KVMetadataServiceImpl::BatchRenewLease(const BatchRenewLeaseRequest& request,
                                            BatchRenewLeaseResponse* response) {
    if (engine_->CatalogTier() == EngineCatalogTier::REMOTE_LIBPQ) {
        response->Clear();
        response->clear_results();
        const int64_t now_ms = Now();
        const EngineResultMeta err = MakeRemoteTierRefusalMeta();
        for (const auto& item : request.items()) {
            auto* result = response->add_results();
            result->set_block_hash(item.block_hash());
            FillResultMeta(err, result->mutable_result());
        }
        response->set_server_time_ms(now_ms);
        return;
    }
    response->clear_results();
    const int64_t now_ms = Now();
    for (const auto& item : request.items()) {
        auto* result = response->add_results();
        result->set_block_hash(item.block_hash());
        EngineRenewLeaseResult out = engine_->RenewLease(item.block_hash(),
                                                         item.lease_token(),
                                                         item.expected_dn_epoch(),
                                                         item.expected_store_epoch(),
                                                         request.requested_ttl_ms(),
                                                         now_ms);
        FillResultMeta(out.result, result->mutable_result());
        if (out.lease.has_value()) {
            FillLease(*out.lease, result->mutable_lease());
        }
    }
    response->set_server_time_ms(now_ms);
}

void KVMetadataServiceImpl::BatchUpdateBlockStatus(const BatchUpdateStatusRequest& request,
                                                   BatchUpdateStatusResponse* response) {
    if (engine_->CatalogTier() == EngineCatalogTier::REMOTE_LIBPQ) {
        response->Clear();
        response->clear_results();
        const int64_t now_ms = Now();
        const EngineResultMeta err = MakeRemoteTierRefusalMeta();
        for (const auto& item : request.items()) {
            auto* result = response->add_results();
            result->set_block_hash(item.block_hash());
            FillResultMeta(err, result->mutable_result());
        }
        response->set_server_time_ms(now_ms);
        return;
    }
    response->clear_results();
    const int64_t now_ms = Now();
    for (const auto& item : request.items()) {
        auto* result = response->add_results();
        result->set_block_hash(item.block_hash());
        EngineUpdateStatusResult out = engine_->UpdateStatus(item.block_hash(),
                                                             static_cast<int32_t>(item.expected_from_status()),
                                                             static_cast<int32_t>(item.to_status()),
                                                             item.expected_version(),
                                                             item.evicted_path(),
                                                             item.allow_noop_if_already_target(),
                                                             now_ms);
        FillResultMeta(out.result, result->mutable_result());
        result->set_new_version(out.new_version);
        result->set_current_status(static_cast<BlockStatus>(out.current_status));
    }
    response->set_server_time_ms(now_ms);
}

void KVMetadataServiceImpl::BatchFreeAllocated(const BatchFreeAllocatedRequest& request,
                                               BatchFreeAllocatedResponse* response) {
    if (engine_->CatalogTier() == EngineCatalogTier::REMOTE_LIBPQ) {
        response->Clear();
        response->clear_results();
        const int64_t now_ms = Now();
        const EngineResultMeta err = MakeRemoteTierRefusalMeta();
        for (const auto& item : request.items()) {
            auto* result = response->add_results();
            result->set_block_hash(item.block_hash());
            FillResultMeta(err, result->mutable_result());
        }
        response->set_server_time_ms(now_ms);
        return;
    }
    response->clear_results();
    const int64_t now_ms = Now();
    for (const auto& item : request.items()) {
        auto* result = response->add_results();
        result->set_block_hash(item.block_hash());
        EngineFreeAllocatedResult out = engine_->FreeAllocated(item.block_hash(),
                                                               item.expected_version(),
                                                               item.force(),
                                                               now_ms);
        FillResultMeta(out.result, result->mutable_result());
        result->set_new_version(out.new_version);
    }
    response->set_server_time_ms(now_ms);
}

}  // namespace falconfs::kv
