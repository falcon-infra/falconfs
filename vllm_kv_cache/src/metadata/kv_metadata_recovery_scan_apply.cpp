// Applies `ScanShardForRecoveryResponse` from protobuf. Kept separate from
// `kv_metadata_recovery.cpp` so this TU never mixes `kv_metadata_service.pb.h`
// with `kv_types.h` (duplicate type names in namespace falconfs::kv).

#include "vllm_kv_cache/src/metadata/kv_metadata_recovery.h"

#include "kv_metadata_service.pb.h"
#include "vllm_kv_cache/src/metadata/kv_metadata_engine.h"

namespace falconfs::kv {

ScanShardRecoveryApplyStats ApplyScanShardForRecoveryResponse(KVMetadataEngine* engine,
                                                              const std::string& serialized_response,
                                                              int64_t now_ms) {
    ScanShardRecoveryApplyStats stats;
    ScanShardForRecoveryResponse resp;
    if (engine == nullptr || serialized_response.empty() || !resp.ParseFromString(serialized_response) ||
        !resp.result().success()) {
        return stats;
    }

    constexpr int32_t kPbAllocated = 1;
    constexpr int32_t kPbStored = 2;
    constexpr int32_t kPbEvicting = 3;

    engine->SetDnEpochFromCatalog(resp.dn_epoch_after_bump());
    ++stats.shards_scanned;

    for (int i = 0; i < resp.rows_size(); ++i) {
        const RecoveryRow& row = resp.rows(i);
        const std::string hash(row.block_hash().begin(), row.block_hash().end());

        int32_t status = static_cast<int32_t>(row.status());
        if (status == kPbEvicting) {
            status = kPbStored;
            ++stats.reconciled_evicting;
        }

        EngineBlockLocation loc;
        if (row.has_location()) {
            loc.store_node_id = row.location().store_node_id();
            loc.pool_offset = row.location().pool_offset();
            loc.evicted_path = row.location().evicted_path();
            loc.store_epoch = row.location().store_epoch();
        }

        if (status == kPbAllocated || status == kPbStored) {
            (void)engine->MarkBitmapOccupied(loc.store_node_id, loc.pool_offset);
        }

        (void)engine->RestoreRow(hash, status, loc, row.version(), now_ms);
        ++stats.rows_applied;
    }

    return stats;
}

}  // namespace falconfs::kv
