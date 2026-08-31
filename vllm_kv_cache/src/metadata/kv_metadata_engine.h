#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace falconfs::kv {

struct EngineResultMeta {
    bool success = false;
    int32_t error_code = 0;
    bool retryable = false;
    std::string error_message;
};

struct EngineLeaseInfo {
    int64_t lease_token = 0;
    int64_t lease_expire_ms = 0;
    int64_t dn_epoch = 0;
    int64_t store_epoch = 0;
};

struct EngineBlockLocation {
    int32_t store_node_id = 0;
    int64_t pool_offset = 0;
    std::string evicted_path;
    int64_t store_epoch = 0;
};

struct EngineBlockMeta {
    int32_t status = 0;
    EngineBlockLocation location;
    int64_t version = 0;
};

struct EngineLookupResult {
    EngineResultMeta result;
    std::optional<EngineBlockMeta> row;
    std::optional<EngineLeaseInfo> lease;
    bool cacheable = false;
    // v6.4 §8 / §13: DRAM or catalog ALLOCATED without loadable pool_offset / lease.
    bool allocated_pending_store = false;
    // v6.4 §13: authoritative EVICTED row read from catalog (not NOT_FOUND).
    bool evicted_catalog_hit = false;
    // True when the block is not in the DRAM shard index; pool workers must
    // consult PostgreSQL catalog (FalconKvblock*) via libpq, not locally.
    bool needs_catalog = false;
};

struct EngineAllocateResult {
    EngineResultMeta result;
    std::optional<EngineBlockMeta> row;
    std::optional<EngineLeaseInfo> lease;
    bool reused_existing_allocation = false;
};

struct EngineRenewLeaseResult {
    EngineResultMeta result;
    std::optional<EngineLeaseInfo> lease;
};

struct EngineUpdateStatusResult {
    EngineResultMeta result;
    int64_t new_version = 0;
    int32_t current_status = 0;
};

struct EngineFreeAllocatedResult {
    EngineResultMeta result;
    int64_t new_version = 0;
};

// v6.4 pool-worker Pass-1 for allocate: DRAM reuse, or bitmap reservation before catalog INSERT.
struct EngineAllocatePass1Outcome {
    enum class Kind { ReusedDram, ReservedBitmap, Failed };
    Kind kind = Kind::Failed;
    EngineAllocateResult reused_dram{};
    EngineResultMeta fail_meta{};
    int32_t store_node_id = 0;
    int64_t pool_offset = 0;
    int64_t slot_idx = 0;
};

// Mirrors the per-DN store-region info in v6 §5.1. The engine owns one
// `BitmapAllocator` per registered region and routes allocations using
// affinity rules from §5.3.
struct EngineStoreRegion {
    int32_t store_node_id = 0;
    int64_t base_offset = 0;
    int64_t region_bytes = 0;
    int32_t block_size = 0;
    int64_t store_epoch = 0;
};

// Region states mirroring StoreRegionRegistry. Allocation only considers
// HEALTHY regions; reads continue from HEALTHY/DRAINING/SUSPECT.
enum class EngineRegionState : int32_t {
    HEALTHY = 0,
    DRAINING = 1,
    SUSPECT = 2,
    OFFLINE = 3,
    QUARANTINED = 4,
};

// v6.4 §3: LOCAL_FALLBACK uses in-process IKVMetaTableAccessor for unit tests;
// REMOTE_LIBPQ forbids any in-process catalog read/write — only libpq sub-batches.
enum class EngineCatalogTier : int32_t {
    LOCAL_FALLBACK = 0,
    REMOTE_LIBPQ = 1,
};

class KVMetadataEngine {
public:
    KVMetadataEngine(int32_t store_node_id = 1,
                     int32_t dn_id = 1,
                     int64_t region_bytes = 64LL * 65536LL,
                     int32_t block_size = 65536,
                     int64_t dn_epoch = 1,
                     int64_t store_epoch = 1,
                     int32_t kvblock_shard_id = 1);
    ~KVMetadataEngine();

    KVMetadataEngine(const KVMetadataEngine&) = delete;
    KVMetadataEngine& operator=(const KVMetadataEngine&) = delete;
    KVMetadataEngine(KVMetadataEngine&&) noexcept;
    KVMetadataEngine& operator=(KVMetadataEngine&&) noexcept;

    int32_t DnId() const;
    int64_t DnEpoch() const;
    int32_t BlockSize() const;

    // Multi-region registration (v6 §5.2). Region with the same store_node_id
    // is replaced. Initial state is HEALTHY.
    EngineResultMeta RegisterStoreRegion(const EngineStoreRegion& region);
    EngineResultMeta SetRegionState(int32_t store_node_id, EngineRegionState state);
    bool HasRegion(int32_t store_node_id) const;
    int64_t RegionStoreEpoch(int32_t store_node_id) const;

    // Recovery API (v6 §6.4, §15.1):
    // 1. After RegisterStoreRegion(), call MarkBitmapOccupied for every row
    //    referencing this region in ALLOCATED/STORED/EVICTING.
    // 2. Call RestoreRow to repopulate metadata rows from the persisted
    //    catalog.
    // 3. BumpDnEpoch increments the dn_epoch and clears the lease map; clients
    //    holding old lease tokens get STALE_EPOCH on renew.
    EngineResultMeta MarkBitmapOccupied(int32_t store_node_id, int64_t pool_offset);
    EngineResultMeta RestoreRow(const std::string& block_hash,
                                int32_t status,
                                const EngineBlockLocation& location,
                                int64_t version,
                                int64_t now_ms);
    int64_t BumpDnEpoch();

    // After libpq recovery / catalog bump, sync in-process fencing without re-bumping PG.
    void SetDnEpochFromCatalog(int64_t epoch);

    EngineLookupResult Lookup(const std::string& block_hash, bool renew_lease_on_hit, int64_t now_ms);
    // v6.4 Pass-1: DRAM-resident metadata only. When `needs_catalog` is set, the
    // caller must run the catalog sub-batch in a PostgreSQL backend process.
    EngineLookupResult LookupDramCacheOnly(const std::string& block_hash,
                                           bool renew_lease_on_hit,
                                           int64_t now_ms);

    // When false (DN bgworker shared engine), Lookup() does not read fallback/PG hooks after a
    // DRAM miss — only the pool-worker catalog sub-batch may observe catalog (v6.4 §4.1.1).
    void SetAllowInlineMetaTableLookup(bool allow);
    bool AllowInlineMetaTableLookup() const;

    void SetCatalogTier(EngineCatalogTier tier);
    EngineCatalogTier CatalogTier() const;

    EngineAllocatePass1Outcome AllocatePass1ReserveBitmap(const std::string& block_hash,
                                                          int32_t block_size,
                                                          int64_t now_ms,
                                                          int32_t preferred_store_id,
                                                          bool allow_fallback_store);
    void RollbackAllocatePass1Reservation(int32_t store_node_id, int64_t slot_idx);
    EngineAllocateResult CommitAllocatePass1AfterCatalogInsert(const std::string& block_hash,
                                                               int32_t store_node_id,
                                                               int64_t slot_idx,
                                                               int64_t now_ms);

    // v6.4 §11.3: RenewLease is DRAM-only; no catalog sub-batch (miss => LEASE_EXPIRED).
    struct RenewLeasePass1Outcome {
        EngineRenewLeaseResult dram{};
    };
    RenewLeasePass1Outcome RenewLeasePass1(const std::string& block_hash,
                                           int64_t lease_token,
                                           int64_t expected_dn_epoch,
                                           int64_t expected_store_epoch,
                                           int32_t requested_ttl_ms,
                                           int64_t now_ms);

    // Update/Free: Pass-1 DRAM validation; nullopt => include item in catalog sub-batch.
    std::optional<EngineUpdateStatusResult> UpdateStatusPass1OrCatalog(
        const std::string& block_hash,
        int32_t expected_from_status,
        int32_t to_status,
        int64_t expected_version,
        const std::string& evicted_path,
        bool allow_noop_if_already_target,
        int64_t now_ms);
    void ApplyUpdateStatusAfterCatalogSuccess(const std::string& block_hash,
                                              int32_t to_status,
                                              int64_t new_version,
                                              int32_t previous_status_in_dram);

    std::optional<EngineFreeAllocatedResult> FreeAllocatedPass1OrCatalog(const std::string& block_hash,
                                                                           int64_t expected_version,
                                                                           bool force,
                                                                           int64_t now_ms);
    void ApplyFreeAfterCatalogDeleteSuccess(const std::string& block_hash, int64_t deleted_version);

    // Allocate using affinity policy (v6 §5.3):
    // 1. preferred_store_id if HEALTHY and has space,
    // 2. otherwise least-used HEALTHY region (most free blocks),
    // 3. fallback only allowed when allow_fallback_store=true.
    EngineAllocateResult Allocate(const std::string& block_hash,
                                  int32_t block_size,
                                  int64_t now_ms,
                                  int32_t preferred_store_id = 0,
                                  bool allow_fallback_store = true);

    EngineRenewLeaseResult RenewLease(const std::string& block_hash,
                                      int64_t lease_token,
                                      int64_t expected_dn_epoch,
                                      int64_t expected_store_epoch,
                                      int32_t requested_ttl_ms,
                                      int64_t now_ms);
    EngineUpdateStatusResult UpdateStatus(const std::string& block_hash,
                                          int32_t expected_from_status,
                                          int32_t to_status,
                                          int64_t expected_version,
                                          const std::string& evicted_path,
                                          bool allow_noop_if_already_target,
                                          int64_t now_ms);
    EngineFreeAllocatedResult FreeAllocated(const std::string& block_hash,
                                            int64_t expected_version,
                                            bool force,
                                            int64_t now_ms);

    // Eviction support (v6 §14). `ColdCandidates` scans the shmem DRAM meta
    // HTAB using a CLOCK algorithm (ref_bit cleared on first sight, collected
    // on second). `now_ms` is used to determine whether active leases exclude
    // candidates. `CanEvict` checks whether the block's lease is absent or
    // expired.
    std::vector<std::string> ColdCandidates(std::size_t limit, int64_t now_ms = 0) const;
    bool CanEvict(const std::string& block_hash, int64_t now_ms) const;

    // v6 §14.1: minimum free-blocks ratio across all registered regions, in
    // [0.0, 1.0]. The eviction worker uses this to decide whether the cluster
    // is below `falcon_kv.eviction_low_watermark` and an aggressive cycle is
    // warranted. Returns 1.0 when no regions are registered (no pressure).
    double MinRegionFreeRatio() const;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace falconfs::kv
