#include "vllm_kv_cache/src/metadata/lease_manager.h"

namespace falconfs::kv {

LeaseManager::LeaseManager(int64_t dn_epoch, int64_t default_ttl_ms)
    : dn_epoch_(dn_epoch), default_ttl_ms_(default_ttl_ms), next_token_(1) {}

void LeaseManager::SetDnEpoch(int64_t dn_epoch) {
    std::lock_guard<std::mutex> lock(mu_);
    dn_epoch_ = dn_epoch;
}

int64_t LeaseManager::NewToken() {
    return next_token_++;
}

LeaseInfo LeaseManager::Grant(const std::string& block_hash,
                              int64_t store_epoch,
                              int64_t now_ms) {
    std::lock_guard<std::mutex> lock(mu_);
    LeaseInfo& lease = leases_[block_hash];
    int64_t expire_ms = now_ms + default_ttl_ms_;
    if (lease.lease_token != 0 && lease.lease_expire_ms > now_ms) {
        // Active lease: extend expiry, refresh epochs (anti-eviction).
        lease.lease_expire_ms = expire_ms;
        lease.dn_epoch = dn_epoch_;
        lease.store_epoch = store_epoch;
        return lease;
    }
    lease.lease_token = NewToken();
    lease.lease_expire_ms = expire_ms;
    lease.dn_epoch = dn_epoch_;
    lease.store_epoch = store_epoch;
    return lease;
}

LeaseRenewResult LeaseManager::Renew(const std::string& block_hash,
                                     int64_t lease_token,
                                     int64_t expected_dn_epoch,
                                     int64_t expected_store_epoch,
                                     int64_t now_ms,
                                     std::optional<int64_t> ttl_ms_override) {
    LeaseRenewResult out;
    std::lock_guard<std::mutex> lock(mu_);
    // Reject stale-epoch requests up front, even if the in-memory lease entry
    // has already been wiped by recovery (v6 §15.4).
    if (expected_dn_epoch != dn_epoch_) {
        out.result = ItemResult::Err(ErrorCode::STALE_EPOCH, true, "stale dn epoch");
        return out;
    }
    auto it = leases_.find(block_hash);
    if (it == leases_.end()) {
        out.result = ItemResult::Err(ErrorCode::LEASE_EXPIRED, true, "lease missing");
        return out;
    }
    LeaseInfo& lease = it->second;
    if (expected_dn_epoch != lease.dn_epoch) {
        out.result = ItemResult::Err(ErrorCode::STALE_EPOCH, true, "stale dn epoch");
        return out;
    }
    if (expected_store_epoch != lease.store_epoch) {
        out.result = ItemResult::Err(ErrorCode::STALE_EPOCH, true, "stale store epoch");
        return out;
    }
    if (lease.lease_token != lease_token) {
        out.result = ItemResult::Err(ErrorCode::LEASE_TOKEN_MISMATCH, true,
                                     "lease token mismatch");
        return out;
    }
    if (lease.lease_expire_ms <= now_ms) {
        out.result = ItemResult::Err(ErrorCode::LEASE_EXPIRED, true, "lease expired");
        return out;
    }
    int64_t ttl = ttl_ms_override.value_or(default_ttl_ms_);
    lease.lease_expire_ms = now_ms + ttl;
    out.result = ItemResult::Ok();
    out.lease = lease;
    return out;
}

bool LeaseManager::CanEvict(const std::string& block_hash, int64_t now_ms) const {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = leases_.find(block_hash);
    if (it == leases_.end()) return true;
    return it->second.lease_expire_ms <= now_ms;
}

void LeaseManager::Drop(const std::string& block_hash) {
    std::lock_guard<std::mutex> lock(mu_);
    leases_.erase(block_hash);
}

void LeaseManager::Clear() {
    std::lock_guard<std::mutex> lock(mu_);
    leases_.clear();
}

std::optional<LeaseInfo> LeaseManager::Peek(const std::string& block_hash) const {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = leases_.find(block_hash);
    if (it == leases_.end()) return std::nullopt;
    return it->second;
}

}  // namespace falconfs::kv
