// Ownerless anti-eviction lease manager.
//
// A lease guards a DRAM block from being evicted while a client is reading or
// about to read it. Leases do NOT confer ownership; multiple clients may grant
// or renew protection for the same block. Stale `dn_epoch` or `store_epoch`
// requests are rejected with STALE_EPOCH; mismatched lease tokens are rejected
// with LEASE_TOKEN_MISMATCH; expired leases are rejected with LEASE_EXPIRED.
#pragma once

#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

#include "vllm_kv_cache/src/common/kv_types.h"

namespace falconfs::kv {

struct LeaseRenewResult {
    ItemResult result;
    LeaseInfo lease;
};

class LeaseManager {
public:
    explicit LeaseManager(int64_t dn_epoch = 1, int64_t default_ttl_ms = 5000);

    int64_t DnEpoch() const { return dn_epoch_; }
    int64_t DefaultTtlMs() const { return default_ttl_ms_; }
    void SetDnEpoch(int64_t dn_epoch);

    // Grant or extend the anti-eviction lease for `block_hash`. If a valid
    // lease exists, the existing token is preserved and the expiry is extended.
    LeaseInfo Grant(const std::string& block_hash, int64_t store_epoch, int64_t now_ms);

    // Renew an existing lease using the caller's lease_token and the
    // corresponding dn/store epochs. Returns a per-item error mirroring the
    // Python reference's `LeaseManager.renew`.
    LeaseRenewResult Renew(const std::string& block_hash,
                           int64_t lease_token,
                           int64_t expected_dn_epoch,
                           int64_t expected_store_epoch,
                           int64_t now_ms,
                           std::optional<int64_t> ttl_ms_override = std::nullopt);

    // Returns true when no active lease guards the block.
    bool CanEvict(const std::string& block_hash, int64_t now_ms) const;

    // Drop the lease entry. Used when a row is freed or transitions away from
    // STORED.
    void Drop(const std::string& block_hash);

    // Drops all leases. Used during DN-restart recovery (v6 §15.1) before the
    // recovery scan re-grants short leases for `STORED` rows under the new
    // `dn_epoch`.
    void Clear();

    // Test/recovery helper.
    std::optional<LeaseInfo> Peek(const std::string& block_hash) const;

private:
    int64_t NewToken();

    mutable std::mutex mu_;
    int64_t dn_epoch_;
    int64_t default_ttl_ms_;
    int64_t next_token_;
    std::unordered_map<std::string, LeaseInfo> leases_;
};

}  // namespace falconfs::kv
