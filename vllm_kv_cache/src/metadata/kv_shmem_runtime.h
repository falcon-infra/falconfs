#pragma once

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace falconfs::kv {

struct EngineStoreRegion;

struct KVRuntimeBitmapAllocateResult {
    bool ok = false;
    int64_t pool_offset = 0;
};

struct KVRuntimeLeaseGrantResult {
    bool ok = false;
    int64_t lease_token = 0;
    int64_t lease_expire_ms = 0;
};

struct KVRuntimeLeaseRenewResult {
    bool ok = false;
    int64_t lease_expire_ms = 0;
};

struct KVRuntimeMetaCASResult {
    bool success = false;
    bool not_found = false;
    bool conflict = false;
    bool retryable = false;
    int32_t current_status = 0;
    int64_t current_version = 0;
};

// Runtime hook set used only when KV metadata runs inside the PG extension
// process and wants to use LWLock-protected shmem primitives instead of
// in-process maps.
struct KVShmemRuntimeOps {
    // ── Bitmap allocator ──────────────────────────────────────────────────
    std::function<bool(const EngineStoreRegion&)> register_region;
    std::function<KVRuntimeBitmapAllocateResult(int32_t store_node_id)> bitmap_allocate;
    std::function<bool(int32_t store_node_id, int64_t pool_offset)> bitmap_free;
    std::function<bool(int32_t store_node_id, int64_t pool_offset)> bitmap_mark_occupied;
    std::function<std::optional<std::pair<int64_t, int64_t>>(int32_t store_node_id)> bitmap_stats;

    // ── DRAM block meta shmem (lease + CLOCK LRU) ─────────────────────────
    // Insert a new entry when a DRAM block is allocated.
    std::function<bool(const std::string& block_hash,
                       int32_t store_node_id,
                       int64_t pool_offset,
                       int64_t store_epoch,
                       int64_t dn_epoch,
                       int32_t status)> dram_meta_insert;
    // Update in-shmem status (ALLOCATED→STORED, STORED→EVICTING, …).
    std::function<bool(const std::string& block_hash,
                       int32_t new_status)> dram_meta_update_status;
    // Grant / extend anti-eviction lease; sets CLOCK ref_bit.
    std::function<KVRuntimeLeaseGrantResult(const std::string& block_hash,
                                            int64_t store_epoch,
                                            int64_t now_ms,
                                            int64_t ttl_ms)> dram_meta_grant_lease;
    // Renew existing lease (validates token + epoch); sets CLOCK ref_bit.
    std::function<KVRuntimeLeaseRenewResult(const std::string& block_hash,
                                            int64_t lease_token,
                                            int64_t expected_dn_epoch,
                                            int64_t current_dn_epoch,
                                            int64_t expected_store_epoch,
                                            int64_t now_ms,
                                            int64_t ttl_ms)> dram_meta_renew_lease;
    // Returns true when the block has no active lease (safe to evict).
    std::function<bool(const std::string& block_hash, int64_t now_ms)> dram_meta_can_evict;
    // Zero the lease token without removing the entry.
    std::function<void(const std::string& block_hash)> dram_meta_drop;
    // Remove the entry entirely (called when the block is evicted to SSD).
    std::function<bool(const std::string& block_hash)> dram_meta_remove;
    // Clear all lease tokens at DN restart.
    std::function<void()> dram_meta_clear;
    // CLOCK LRU scan: deliver up to `limit` cold STORED candidates via cb.
    std::function<void(int64_t now_ms,
                       int32_t limit,
                       const std::function<void(const std::string& block_hash)>& cb)>
        dram_meta_cold_candidates;

    // ── Lease shmem (legacy fallback when dram_meta_* not available) ──────
    std::function<KVRuntimeLeaseGrantResult(const std::string& block_hash,
                                            int64_t store_epoch,
                                            int64_t now_ms,
                                            int64_t ttl_ms)> lease_grant;
    std::function<KVRuntimeLeaseRenewResult(const std::string& block_hash,
                                            int64_t lease_token,
                                            int64_t expected_dn_epoch,
                                            int64_t current_dn_epoch,
                                            int64_t expected_store_epoch,
                                            int64_t now_ms,
                                            int64_t ttl_ms)> lease_renew;
    std::function<bool(const std::string& block_hash, int64_t now_ms)> lease_can_evict;
    std::function<void(const std::string& block_hash)> lease_drop;
    std::function<void()> lease_clear;
};

void InstallKVShmemRuntimeOps(KVShmemRuntimeOps ops);
void ClearKVShmemRuntimeOps();
std::optional<KVShmemRuntimeOps> GetKVShmemRuntimeOps();

// Concrete PG-extension binding: wires KVShmemRuntimeOps to the LWLock-
// guarded shmem primitives in `falcon/metadb/kv_shmem.c`.
void InstallKVShmemRuntimeOpsFromPg();

}  // namespace falconfs::kv
