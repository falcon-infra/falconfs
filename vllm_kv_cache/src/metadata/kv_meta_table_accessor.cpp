#include "vllm_kv_cache/src/metadata/kv_meta_table_accessor.h"

#include <algorithm>

namespace falconfs::kv {

bool InMemoryKVMetaTableAccessor::Lookup(int32_t shard_id,
                                         const std::string& block_hash,
                                         AccessorRow* out) {
    std::lock_guard<std::mutex> lock(mu_);
    auto sit = shards_.find(shard_id);
    if (sit == shards_.end()) return false;
    auto rit = sit->second.find(block_hash);
    if (rit == sit->second.end()) return false;
    if (out != nullptr) *out = rit->second;
    return true;
}

bool InMemoryKVMetaTableAccessor::InsertAllocated(int32_t shard_id,
                                                  const std::string& block_hash,
                                                  const AccessorInsertSpec& spec,
                                                  int64_t now_ms) {
    std::lock_guard<std::mutex> lock(mu_);
    auto& shard = shards_[shard_id];
    if (shard.find(block_hash) != shard.end()) {
        return false;
    }
    AccessorRow row;
    row.kv_group_idx = spec.kv_group_idx;
    row.layer_mask = spec.layer_mask;
    row.status = 1;  // ALLOCATED
    row.store_node_id = spec.store_node_id;
    row.pool_offset = spec.pool_offset;
    row.version = 1;  // v6.4 §3.1 / §7.4 step 5: new-allocate row.version = 1
    row.updated_at_ms = now_ms;
    shard.emplace(block_hash, std::move(row));
    return true;
}

AccessorCASResult InMemoryKVMetaTableAccessor::CASStatusUpdate(int32_t shard_id,
                                                                const std::string& block_hash,
                                                                int32_t expected_from_status,
                                                                int32_t to_status,
                                                                int64_t expected_version,
                                                                const std::string& evicted_path,
                                                                int64_t now_ms) {
    AccessorCASResult out;
    std::lock_guard<std::mutex> lock(mu_);
    auto sit = shards_.find(shard_id);
    if (sit == shards_.end()) {
        out.not_found = true;
        return out;
    }
    auto rit = sit->second.find(block_hash);
    if (rit == sit->second.end()) {
        out.not_found = true;
        return out;
    }
    AccessorRow& row = rit->second;
    out.current_status = row.status;
    out.current_version = row.version;
    if (row.status != expected_from_status || row.version != expected_version) {
        out.conflict = true;
        out.retryable = true;
        return out;
    }
    row.status = to_status;
    row.version += 1;
    row.updated_at_ms = now_ms;
    if (!evicted_path.empty()) {
        row.evicted_path = evicted_path;
    }
    out.current_status = to_status;
    out.current_version = row.version;
    out.success = true;
    return out;
}

bool InMemoryKVMetaTableAccessor::Delete(int32_t shard_id,
                                         const std::string& block_hash,
                                         int64_t expected_version,
                                         bool* out_conflict) {
    if (out_conflict != nullptr) *out_conflict = false;
    std::lock_guard<std::mutex> lock(mu_);
    auto sit = shards_.find(shard_id);
    if (sit == shards_.end()) return false;
    auto rit = sit->second.find(block_hash);
    if (rit == sit->second.end()) return false;
    if (expected_version >= 0 && rit->second.version != expected_version) {
        if (out_conflict != nullptr) *out_conflict = true;
        return false;
    }
    sit->second.erase(rit);
    return true;
}

void InMemoryKVMetaTableAccessor::ScanForRecovery(int32_t shard_id,
                                                   const std::vector<int32_t>& wanted_status,
                                                   const RowCallback& cb) {
    std::lock_guard<std::mutex> lock(mu_);
    auto sit = shards_.find(shard_id);
    if (sit == shards_.end()) return;
    for (const auto& kv : sit->second) {
        if (!wanted_status.empty() &&
            std::find(wanted_status.begin(), wanted_status.end(), kv.second.status) == wanted_status.end()) {
            continue;
        }
        cb(kv.first, kv.second);
    }
}

int64_t InMemoryKVMetaTableAccessor::LoadDnEpoch(int32_t shard_id) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = dn_epochs_.find(shard_id);
    if (it == dn_epochs_.end()) {
        return 1;
    }
    return it->second;
}

int64_t InMemoryKVMetaTableAccessor::BumpDnEpoch(int32_t shard_id) {
    std::lock_guard<std::mutex> lock(mu_);
    int64_t& epoch = dn_epochs_[shard_id];
    if (epoch <= 0) {
        epoch = 1;
    }
    ++epoch;
    return epoch;
}

bool InMemoryKVMetaTableAccessor::UpsertForRecovery(int32_t shard_id,
                                                    const std::string& block_hash,
                                                    const AccessorRow& row) {
    std::lock_guard<std::mutex> lock(mu_);
    shards_[shard_id][block_hash] = row;
    return true;
}

}  // namespace falconfs::kv
