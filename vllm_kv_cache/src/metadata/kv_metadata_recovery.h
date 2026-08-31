#pragma once

#include <cstdint>
#include <string>

#include "vllm_kv_cache/src/metadata/kv_meta_table_accessor.h"
#include "vllm_kv_cache/src/metadata/kv_metadata_engine.h"

namespace falconfs::kv {

struct MetadataRecoveryStats {
    int64_t recovered_rows = 0;
    int64_t restored_stored_rows = 0;
    int64_t restored_allocated_rows = 0;
    int64_t restored_evicted_rows = 0;
    int64_t reconciled_evicting_rows = 0;
    int64_t bitmap_marked = 0;
    int64_t bumped_dn_epoch = 0;
};

// Rebuild in-memory metadata state from persisted table rows.
// This is a startup helper and mirrors v6 recovery phases:
// 1) bump dn_epoch fencing,
// 2) scan persisted rows by status,
// 3) repopulate bitmap occupancy and row/LRU state.
MetadataRecoveryStats RecoverMetadataFromAccessor(IKVMetaTableAccessor* accessor,
                                                  KVMetadataEngine* engine,
                                                  int32_t shard_id,
                                                  int64_t now_ms);

// Second-pass recovery after a Store region is registered on the DN: replays
// the persisted scan without bumping dn_epoch again (v6.5 P1).
MetadataRecoveryStats ReplayRecoverMetadataFromAccessor(IKVMetaTableAccessor* accessor,
                                                       KVMetadataEngine* engine,
                                                       int32_t shard_id,
                                                       int64_t now_ms);

// v6.4 libpq recovery: apply one `ScanShardForRecoveryResponse` protobuf blob to the engine.
struct ScanShardRecoveryApplyStats {
    int64_t shards_scanned = 0;
    int64_t rows_applied = 0;
    int64_t reconciled_evicting = 0;
};

ScanShardRecoveryApplyStats ApplyScanShardForRecoveryResponse(KVMetadataEngine* engine,
                                                              const std::string& serialized_response,
                                                              int64_t now_ms);

}  // namespace falconfs::kv
