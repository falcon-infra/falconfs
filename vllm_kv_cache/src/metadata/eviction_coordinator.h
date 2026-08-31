// Eviction coordinator (v6 §14).
//
// Picks cold candidates from `KVMetadataEngine`'s LRU, drives them through
// the v6 two-phase eviction state machine (`STORED -> EVICTING -> EVICTED`),
// and rolls back to `STORED` if the spill callback fails. The spill callback
// is decoupled from the coordinator so unit tests can drive it without any
// real DRAM/SSD plumbing, while production code wires it to
// `KVStoreEngine::SpillBlockToSSD`.
#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>

#include "vllm_kv_cache/src/metadata/kv_metadata_engine.h"

namespace falconfs::kv {

struct EvictionConfig {
    int max_per_cycle = 64;
    int64_t now_ms = 0;
};

struct EvictionCycleResult {
    int candidates_considered = 0;
    int evicted = 0;
    int rolled_back = 0;
    int skipped_active_lease = 0;
    int skipped_other = 0;
};

class EvictionCoordinator {
public:
    // Spill callback: after STORED→EVICTING CAS, spill bytes from
    // `(pool_offset, store_epoch)` at `version`, return `{ok, evicted_path}`.
    // On failure, the coordinator CAS-rolls the row back to STORED.
    struct SpillOutcome {
        bool ok = false;
        std::string evicted_path;
    };
    using SpillFn = std::function<SpillOutcome(const std::string& block_hash,
                                               int64_t version,
                                               int64_t pool_offset,
                                               int64_t store_epoch)>;

    // Status-update callback contract (v6 §14.3): performs a CAS state
    // transition on the row identified by `block_hash` (e.g. STORED ->
    // EVICTING -> EVICTED -> STORED-rollback). The callback owns the
    // catalog round-trip semantics; tests pass `engine_->UpdateStatus`
    // directly (LOCAL_FALLBACK), production wires libpq through the
    // BRPC service `Batch*SplitForPoolWorker` form (REMOTE_LIBPQ).
    // Default-constructed coordinators fall back to `engine_->UpdateStatus`
    // so the existing unit-test surface keeps working.
    using StatusUpdateFn = std::function<EngineUpdateStatusResult(
        const std::string& block_hash,
        int32_t expected_from_status,
        int32_t to_status,
        int64_t expected_version,
        const std::string& evicted_path,
        bool allow_noop_if_already_target,
        int64_t now_ms)>;

    EvictionCoordinator(std::shared_ptr<KVMetadataEngine> engine, SpillFn spill_fn);
    EvictionCoordinator(std::shared_ptr<KVMetadataEngine> engine,
                        SpillFn spill_fn,
                        StatusUpdateFn status_update_fn);

    EvictionCycleResult RunOneCycle(const EvictionConfig& cfg);

private:
    EngineUpdateStatusResult DoUpdateStatus(const std::string& block_hash,
                                            int32_t expected_from_status,
                                            int32_t to_status,
                                            int64_t expected_version,
                                            const std::string& evicted_path,
                                            bool allow_noop_if_already_target,
                                            int64_t now_ms);

    std::shared_ptr<KVMetadataEngine> engine_;
    SpillFn spill_fn_;
    StatusUpdateFn status_update_fn_;
};

}  // namespace falconfs::kv
