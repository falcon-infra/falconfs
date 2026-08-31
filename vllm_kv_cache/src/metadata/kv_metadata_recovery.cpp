#include "vllm_kv_cache/src/metadata/kv_metadata_recovery.h"

#include <vector>

#include "vllm_kv_cache/src/common/kv_types.h"

namespace falconfs::kv {

namespace {

void ScanAndApplyRecoveryRows(IKVMetaTableAccessor* accessor,
                              KVMetadataEngine* engine,
                              int32_t shard_id,
                              int64_t now_ms,
                              MetadataRecoveryStats* stats) {
    const std::vector<int32_t> wanted = {
        static_cast<int32_t>(BlockStatus::ALLOCATED),
        static_cast<int32_t>(BlockStatus::STORED),
        static_cast<int32_t>(BlockStatus::EVICTING),
        static_cast<int32_t>(BlockStatus::EVICTED),
    };

    accessor->ScanForRecovery(
        shard_id,
        wanted,
        [&](const std::string& block_hash, const AccessorRow& row) {
            int32_t status = row.status;
            if (status == static_cast<int32_t>(BlockStatus::EVICTING)) {
                // Reconcile interrupted eviction to a safe STORED state.
                status = static_cast<int32_t>(BlockStatus::STORED);
                ++stats->reconciled_evicting_rows;
            }

            EngineBlockLocation location;
            location.store_node_id = row.store_node_id;
            location.pool_offset   = row.pool_offset;
            location.evicted_path  = row.evicted_path;
            location.store_epoch   = 0;

            if (status == static_cast<int32_t>(BlockStatus::ALLOCATED) ||
                status == static_cast<int32_t>(BlockStatus::STORED)) {
                EngineResultMeta mark = engine->MarkBitmapOccupied(row.store_node_id, row.pool_offset);
                if (mark.success) {
                    ++stats->bitmap_marked;
                }
            }

            EngineResultMeta restored = engine->RestoreRow(block_hash, status, location, row.version, now_ms);
            if (!restored.success) {
                return;
            }
            ++stats->recovered_rows;
            if (status == static_cast<int32_t>(BlockStatus::ALLOCATED)) {
                ++stats->restored_allocated_rows;
            } else if (status == static_cast<int32_t>(BlockStatus::STORED)) {
                ++stats->restored_stored_rows;
            } else if (status == static_cast<int32_t>(BlockStatus::EVICTED)) {
                ++stats->restored_evicted_rows;
            }
        });
}

}  // namespace

MetadataRecoveryStats ReplayRecoverMetadataFromAccessor(IKVMetaTableAccessor* accessor,
                                                        KVMetadataEngine* engine,
                                                        int32_t shard_id,
                                                        int64_t now_ms) {
    MetadataRecoveryStats stats;
    if (accessor == nullptr || engine == nullptr) {
        return stats;
    }
    ScanAndApplyRecoveryRows(accessor, engine, shard_id, now_ms, &stats);
    return stats;
}

MetadataRecoveryStats RecoverMetadataFromAccessor(IKVMetaTableAccessor* accessor,
                                                  KVMetadataEngine* engine,
                                                  int32_t shard_id,
                                                  int64_t now_ms) {
    MetadataRecoveryStats stats;
    if (accessor == nullptr || engine == nullptr) {
        return stats;
    }

    // Persisted fencing epoch bumps first so any stale lease renew is rejected.
    // v6 §15.1 step 3: the catalog row is the source of truth — bump it
    // atomically and mirror the new value into the engine so any in-flight
    // lease token (carrying the pre-restart dn_epoch) is rejected immediately.
    const int64_t new_epoch = accessor->BumpDnEpoch(shard_id);
    engine->SetDnEpochFromCatalog(new_epoch);
    stats.bumped_dn_epoch = new_epoch;

    ScanAndApplyRecoveryRows(accessor, engine, shard_id, now_ms, &stats);
    return stats;
}

}  // namespace falconfs::kv
