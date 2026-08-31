#include "vllm_kv_cache/src/store/store_region_registry.h"

namespace falconfs::kv {

StoreRegionRegistry::StoreRegionRegistry(int64_t suspect_after_ms, int64_t offline_after_ms)
    : suspect_after_ms_(suspect_after_ms), offline_after_ms_(offline_after_ms) {}

void StoreRegionRegistry::RegisterRegion(const StoreRegion& region, int64_t now_ms) {
    std::lock_guard<std::mutex> lock(mu_);
    StoreRegion stored = region;
    stored.last_heartbeat_ms = now_ms;
    if (stored.state == StoreRegionState::QUARANTINED) {
        // Preserve QUARANTINED if explicitly registered as such.
    } else {
        stored.state = StoreRegionState::HEALTHY;
    }
    regions_[{region.store_node_id, region.owner_dn_id}] = stored;
}

bool StoreRegionRegistry::Heartbeat(int32_t store_node_id,
                                    int32_t owner_dn_id,
                                    int64_t store_epoch,
                                    int64_t now_ms) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = regions_.find({store_node_id, owner_dn_id});
    if (it == regions_.end()) {
        return false;
    }
    StoreRegion& r = it->second;
    if (store_epoch != r.store_epoch) {
        // New epoch indicates Store restart; force reconciliation by quarantining.
        r.state = StoreRegionState::QUARANTINED;
        r.store_epoch = store_epoch;
        r.last_heartbeat_ms = now_ms;
        return true;
    }
    r.last_heartbeat_ms = now_ms;
    bool changed = false;
    if (r.state == StoreRegionState::SUSPECT || r.state == StoreRegionState::OFFLINE) {
        r.state = StoreRegionState::HEALTHY;
        changed = true;
    }
    return changed;
}

bool StoreRegionRegistry::TickHeartbeats(int64_t now_ms) {
    std::lock_guard<std::mutex> lock(mu_);
    bool changed = false;
    for (auto& kv : regions_) {
        StoreRegion& r = kv.second;
        if (r.state == StoreRegionState::DRAINING || r.state == StoreRegionState::QUARANTINED) {
            continue;
        }
        const int64_t gap = now_ms - r.last_heartbeat_ms;
        if (gap >= offline_after_ms_) {
            if (r.state != StoreRegionState::OFFLINE) {
                r.state = StoreRegionState::OFFLINE;
                changed = true;
            }
        } else if (gap >= suspect_after_ms_) {
            if (r.state != StoreRegionState::SUSPECT && r.state != StoreRegionState::OFFLINE) {
                r.state = StoreRegionState::SUSPECT;
                changed = true;
            }
        }
    }
    return changed;
}

bool StoreRegionRegistry::StartDrain(int32_t store_node_id, int32_t owner_dn_id) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = regions_.find({store_node_id, owner_dn_id});
    if (it == regions_.end()) return false;
    it->second.state = StoreRegionState::DRAINING;
    return true;
}

bool StoreRegionRegistry::Quarantine(int32_t store_node_id, int32_t owner_dn_id) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = regions_.find({store_node_id, owner_dn_id});
    if (it == regions_.end()) return false;
    it->second.state = StoreRegionState::QUARANTINED;
    return true;
}

bool StoreRegionRegistry::MarkHealthy(int32_t store_node_id, int32_t owner_dn_id) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = regions_.find({store_node_id, owner_dn_id});
    if (it == regions_.end()) return false;
    it->second.state = StoreRegionState::HEALTHY;
    return true;
}

bool StoreRegionRegistry::CanAllocate(int32_t store_node_id, int32_t owner_dn_id) const {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = regions_.find({store_node_id, owner_dn_id});
    if (it == regions_.end()) return false;
    return it->second.state == StoreRegionState::HEALTHY;
}

bool StoreRegionRegistry::CanRead(int32_t store_node_id, int32_t owner_dn_id) const {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = regions_.find({store_node_id, owner_dn_id});
    if (it == regions_.end()) return false;
    const StoreRegionState s = it->second.state;
    // Reads are allowed everywhere except OFFLINE and QUARANTINED.
    return s == StoreRegionState::HEALTHY || s == StoreRegionState::DRAINING || s == StoreRegionState::SUSPECT;
}

StoreRegion StoreRegionRegistry::Get(int32_t store_node_id, int32_t owner_dn_id) const {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = regions_.find({store_node_id, owner_dn_id});
    if (it == regions_.end()) return StoreRegion{};
    return it->second;
}

bool StoreRegionRegistry::Has(int32_t store_node_id, int32_t owner_dn_id) const {
    std::lock_guard<std::mutex> lock(mu_);
    return regions_.find({store_node_id, owner_dn_id}) != regions_.end();
}

std::vector<StoreRegion> StoreRegionRegistry::List() const {
    std::lock_guard<std::mutex> lock(mu_);
    std::vector<StoreRegion> out;
    out.reserve(regions_.size());
    for (const auto& kv : regions_) out.push_back(kv.second);
    return out;
}

}  // namespace falconfs::kv
