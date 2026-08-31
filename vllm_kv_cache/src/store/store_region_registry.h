// Store region registry + state machine (v6 §5.1, §5.2.1).
//
// Each Store partitions its DRAM into per-DN continuous regions and registers
// one region with each owner DN. The registry holds those mappings, tracks the
// last heartbeat per region, and exposes a small state machine that gates
// allocations and reads.
#pragma once

#include <cstdint>
#include <mutex>
#include <unordered_map>
#include <utility>
#include <vector>

namespace falconfs::kv {

enum class StoreRegionState : int32_t {
    HEALTHY = 0,
    DRAINING = 1,
    SUSPECT = 2,
    OFFLINE = 3,
    QUARANTINED = 4,
};

struct StoreRegion {
    int32_t store_node_id = 0;
    int32_t owner_dn_id = 0;
    int64_t base_offset = 0;
    int64_t region_bytes = 0;
    int64_t block_size = 0;
    int64_t store_epoch = 0;
    StoreRegionState state = StoreRegionState::HEALTHY;
    int64_t last_heartbeat_ms = 0;
};

class StoreRegionRegistry {
public:
    StoreRegionRegistry(int64_t suspect_after_ms = 3000, int64_t offline_after_ms = 10000);

    int64_t SuspectAfterMs() const { return suspect_after_ms_; }
    int64_t OfflineAfterMs() const { return offline_after_ms_; }

    // Register or replace a region. Sets state to HEALTHY by default.
    void RegisterRegion(const StoreRegion& region, int64_t now_ms);

    // Update the last_heartbeat_ms for a region. If the region is currently in
    // SUSPECT and the new store_epoch matches, transitions back to HEALTHY.
    bool Heartbeat(int32_t store_node_id, int32_t owner_dn_id, int64_t store_epoch, int64_t now_ms);

    // Apply heartbeat-timeout transitions. Returns true if any region changed state.
    bool TickHeartbeats(int64_t now_ms);

    // Operator transitions.
    bool StartDrain(int32_t store_node_id, int32_t owner_dn_id);
    bool Quarantine(int32_t store_node_id, int32_t owner_dn_id);
    bool MarkHealthy(int32_t store_node_id, int32_t owner_dn_id);

    bool CanAllocate(int32_t store_node_id, int32_t owner_dn_id) const;
    bool CanRead(int32_t store_node_id, int32_t owner_dn_id) const;

    StoreRegion Get(int32_t store_node_id, int32_t owner_dn_id) const;
    bool Has(int32_t store_node_id, int32_t owner_dn_id) const;

    std::vector<StoreRegion> List() const;

private:
    using Key = std::pair<int32_t, int32_t>;
    struct KeyHash {
        std::size_t operator()(const Key& k) const noexcept {
            return (static_cast<std::size_t>(k.first) << 32) ^ static_cast<std::size_t>(k.second);
        }
    };

    int64_t suspect_after_ms_;
    int64_t offline_after_ms_;
    mutable std::mutex mu_;
    std::unordered_map<Key, StoreRegion, KeyHash> regions_;
};

}  // namespace falconfs::kv
