#include <gtest/gtest.h>

#include <optional>
#include <string>

#include "kv_common.pb.h"
#include "vllm_kv_cache/src/metadata/kv_metadata_engine.h"
#include "vllm_kv_cache/src/metadata/kv_shmem_runtime.h"

namespace falconfs::kv {

TEST(KVMetadataEngine, AllocateLookupAndRenewLeaseFlow) {
    KVMetadataEngine engine;

    EngineAllocateResult alloc = engine.Allocate("b1", 65536, 1000);
    ASSERT_TRUE(alloc.result.success);
    ASSERT_TRUE(alloc.row.has_value());
    ASSERT_TRUE(alloc.lease.has_value());
    EXPECT_EQ(alloc.row->status, static_cast<int32_t>(BlockStatus::BLOCK_STATUS_ALLOCATED));
    EXPECT_FALSE(alloc.reused_existing_allocation);

    EngineUpdateStatusResult to_stored = engine.UpdateStatus(
        "b1",
        static_cast<int32_t>(BlockStatus::BLOCK_STATUS_ALLOCATED),
        static_cast<int32_t>(BlockStatus::BLOCK_STATUS_STORED),
        /*expected_version=*/1,
        "",
        false,
        1010);
    ASSERT_TRUE(to_stored.result.success);
    EXPECT_EQ(to_stored.new_version, 2);

    EngineLookupResult lookup = engine.Lookup("b1", true, 1020);
    ASSERT_TRUE(lookup.result.success);
    ASSERT_TRUE(lookup.row.has_value());
    ASSERT_TRUE(lookup.lease.has_value());
    EXPECT_EQ(lookup.row->status, static_cast<int32_t>(BlockStatus::BLOCK_STATUS_STORED));
    EXPECT_TRUE(lookup.cacheable);

    EngineRenewLeaseResult renew = engine.RenewLease("b1",
                                                     lookup.lease->lease_token,
                                                     lookup.lease->dn_epoch,
                                                     lookup.lease->store_epoch,
                                                     3000,
                                                     1030);
    EXPECT_TRUE(renew.result.success);
    ASSERT_TRUE(renew.lease.has_value());
    EXPECT_EQ(renew.lease->lease_token, lookup.lease->lease_token);
    EXPECT_EQ(renew.lease->lease_expire_ms, 4030);
}

TEST(KVMetadataEngine, CasConflictAndEvictedRequiresPath) {
    KVMetadataEngine engine;
    ASSERT_TRUE(engine.Allocate("b2", 65536, 1000).result.success);

    EngineUpdateStatusResult wrong_from = engine.UpdateStatus(
        "b2",
        static_cast<int32_t>(BlockStatus::BLOCK_STATUS_STORED),
        static_cast<int32_t>(BlockStatus::BLOCK_STATUS_EVICTING),
        /*expected_version=*/1,
        "",
        false,
        1010);
    EXPECT_FALSE(wrong_from.result.success);
    EXPECT_EQ(wrong_from.result.error_code, static_cast<int32_t>(ErrorCode::CAS_CONFLICT));

    ASSERT_TRUE(engine.UpdateStatus("b2",
                                    static_cast<int32_t>(BlockStatus::BLOCK_STATUS_ALLOCATED),
                                    static_cast<int32_t>(BlockStatus::BLOCK_STATUS_STORED),
                                    /*expected_version=*/1,
                                    "",
                                    false,
                                    1020)
                    .result.success);

    ASSERT_TRUE(engine.UpdateStatus("b2",
                                    static_cast<int32_t>(BlockStatus::BLOCK_STATUS_STORED),
                                    static_cast<int32_t>(BlockStatus::BLOCK_STATUS_EVICTING),
                                    /*expected_version=*/2,
                                    "",
                                    false,
                                    1030)
                    .result.success);

    EngineUpdateStatusResult missing_path = engine.UpdateStatus(
        "b2",
        static_cast<int32_t>(BlockStatus::BLOCK_STATUS_EVICTING),
        static_cast<int32_t>(BlockStatus::BLOCK_STATUS_EVICTED),
        /*expected_version=*/3,
        "",
        false,
        1040);
    EXPECT_FALSE(missing_path.result.success);
    EXPECT_EQ(missing_path.result.error_code, static_cast<int32_t>(ErrorCode::INVALID_ARGUMENT));
}

TEST(KVMetadataEngine, MultiRegionAllocationPrefersStoreThenLeastUsed) {
    KVMetadataEngine engine(/*store_node_id=*/1, /*dn_id=*/1,
                            /*region_bytes=*/4 * 65536LL, /*block_size=*/65536);
    EngineStoreRegion r2;
    r2.store_node_id = 2;
    r2.base_offset = 0;  // independent address space per region
    r2.region_bytes = 8 * 65536LL;
    r2.block_size = 65536;
    r2.store_epoch = 1;
    ASSERT_TRUE(engine.RegisterStoreRegion(r2).success);

    EngineAllocateResult preferred = engine.Allocate(
        "k-pref", /*block_size=*/65536, /*now_ms=*/1000,
        /*preferred_store_id=*/1, /*allow_fallback_store=*/true);
    ASSERT_TRUE(preferred.result.success);
    EXPECT_EQ(preferred.row->location.store_node_id, 1);

    // Without preference, region 2 has more free blocks (8 vs 3 left), so it
    // wins the least-used tiebreaker.
    EngineAllocateResult fallback_pick = engine.Allocate(
        "k-no-pref", /*block_size=*/65536, /*now_ms=*/1010,
        /*preferred_store_id=*/0, /*allow_fallback_store=*/true);
    ASSERT_TRUE(fallback_pick.result.success);
    EXPECT_EQ(fallback_pick.row->location.store_node_id, 2);
}

TEST(KVMetadataEngine, DrainingRegionSkippedAndFallbackHonored) {
    KVMetadataEngine engine(/*store_node_id=*/1, /*dn_id=*/1,
                            /*region_bytes=*/65536LL, /*block_size=*/65536);
    EngineStoreRegion r2;
    r2.store_node_id = 2;
    r2.base_offset = 0;
    r2.region_bytes = 65536;
    r2.block_size = 65536;
    r2.store_epoch = 1;
    ASSERT_TRUE(engine.RegisterStoreRegion(r2).success);
    ASSERT_TRUE(engine.SetRegionState(/*store_node_id=*/1, EngineRegionState::DRAINING).success);

    EngineAllocateResult prefer_drain_no_fallback = engine.Allocate(
        "no-fb", 65536, 1000, /*preferred=*/1, /*allow_fallback_store=*/false);
    EXPECT_FALSE(prefer_drain_no_fallback.result.success);
    EXPECT_EQ(prefer_drain_no_fallback.result.error_code,
              static_cast<int32_t>(ErrorCode::THROTTLED));

    EngineAllocateResult prefer_drain_with_fallback = engine.Allocate(
        "with-fb", 65536, 1010, /*preferred=*/1, /*allow_fallback_store=*/true);
    ASSERT_TRUE(prefer_drain_with_fallback.result.success);
    EXPECT_EQ(prefer_drain_with_fallback.row->location.store_node_id, 2);
}

TEST(KVMetadataEngine, RecoveryRebuildsBitmapAndRows) {
    KVMetadataEngine engine(/*store_node_id=*/1, /*dn_id=*/1,
                            /*region_bytes=*/4 * 65536LL, /*block_size=*/65536);
    EngineBlockLocation loc;
    loc.store_node_id = 1;
    loc.pool_offset = 65536;  // 2nd block
    loc.store_epoch = 1;
    ASSERT_TRUE(engine.MarkBitmapOccupied(1, loc.pool_offset).success);
    ASSERT_TRUE(engine.RestoreRow("recovered",
                                  static_cast<int32_t>(BlockStatus::BLOCK_STATUS_STORED),
                                  loc, /*version=*/3, /*now_ms=*/1000)
                    .success);

    EngineLookupResult lookup = engine.Lookup("recovered", /*renew=*/true, /*now_ms=*/1010);
    ASSERT_TRUE(lookup.result.success);
    EXPECT_EQ(lookup.row->location.pool_offset, 65536);
    EXPECT_EQ(lookup.row->status, static_cast<int32_t>(BlockStatus::BLOCK_STATUS_STORED));
    ASSERT_TRUE(lookup.lease.has_value());
}

TEST(KVMetadataEngine, RenewLeaseMissingDramSlotReturnsLeaseExpired) {
    KVMetadataEngine engine;
    EngineRenewLeaseResult r = engine.RenewLease(
        "no-such-block",
        /*lease_token=*/1,
        /*expected_dn_epoch=*/1,
        /*expected_store_epoch=*/1,
        /*requested_ttl_ms=*/1000,
        /*now_ms=*/1000);
    EXPECT_FALSE(r.result.success);
    EXPECT_EQ(r.result.error_code, static_cast<int32_t>(ErrorCode::LEASE_EXPIRED));
    EXPECT_TRUE(r.result.retryable);
}

TEST(KVMetadataEngine, BumpDnEpochInvalidatesOldLeaseRenew) {
    KVMetadataEngine engine;
    EngineAllocateResult alloc = engine.Allocate("e1", 65536, 1000);
    ASSERT_TRUE(alloc.result.success);
    int64_t old_epoch = alloc.lease->dn_epoch;
    int64_t old_token = alloc.lease->lease_token;

    int64_t new_epoch = engine.BumpDnEpoch();
    EXPECT_GT(new_epoch, old_epoch);

    EngineRenewLeaseResult stale = engine.RenewLease(
        "e1", old_token, /*expected_dn_epoch=*/old_epoch, /*expected_store_epoch=*/1,
        /*requested_ttl_ms=*/0, /*now_ms=*/1010);
    EXPECT_FALSE(stale.result.success);
    EXPECT_EQ(stale.result.error_code, static_cast<int32_t>(ErrorCode::STALE_EPOCH));
}

TEST(KVMetadataEngine, FreeAllocatedEnforcesStatusUnlessForced) {
    KVMetadataEngine engine;
    ASSERT_TRUE(engine.Allocate("b3", 65536, 1000).result.success);
    ASSERT_TRUE(engine.UpdateStatus("b3",
                                    static_cast<int32_t>(BlockStatus::BLOCK_STATUS_ALLOCATED),
                                    static_cast<int32_t>(BlockStatus::BLOCK_STATUS_STORED),
                                    /*expected_version=*/1,
                                    "",
                                    false,
                                    1010)
                    .result.success);

    EngineFreeAllocatedResult no_force = engine.FreeAllocated("b3", 2, false, 1020);
    EXPECT_FALSE(no_force.result.success);
    EXPECT_EQ(no_force.result.error_code, static_cast<int32_t>(ErrorCode::INVALID_ARGUMENT));

    EngineFreeAllocatedResult forced = engine.FreeAllocated("b3", 2, true, 1030);
    EXPECT_TRUE(forced.result.success);
    EXPECT_EQ(forced.new_version, 3);

    EngineLookupResult after_free = engine.Lookup("b3", false, 1040);
    EXPECT_FALSE(after_free.result.success);
    EXPECT_EQ(after_free.result.error_code, static_cast<int32_t>(ErrorCode::NOT_FOUND));
}

TEST(KVMetadataEngine, SharedCatalogWorksWithoutDramMetaShmemOps) {
    // v6.4: optional KVShmemRuntimeOps installs must not require removed PG catalog
    // callbacks (`meta_*`); LOCAL_FALLBACK metadata uses `fallback_meta_` + DRAM slots only.
    struct ScopedShmemOps {
        explicit ScopedShmemOps(KVShmemRuntimeOps ops) { InstallKVShmemRuntimeOps(std::move(ops)); }
        ~ScopedShmemOps() { ClearKVShmemRuntimeOps(); }
    };

    ScopedShmemOps guard(KVShmemRuntimeOps{});
    KVMetadataEngine engine;

    // Allocate must succeed: bitmap + in-process cache are self-contained.
    EngineAllocateResult alloc = engine.Allocate("v64-block", 65536, 1000);
    EXPECT_TRUE(alloc.result.success);
    EXPECT_TRUE(alloc.row.has_value());

    // Lookup hits the in-process hash index immediately (no catalog round-trip).
    EngineLookupResult lookup_alloc = engine.Lookup("v64-block", false, 1005);
    // v6.4 §8: ALLOCATED in DRAM returns AllocatedPendingStore (success, no lease).
    ASSERT_TRUE(lookup_alloc.result.success);
    ASSERT_TRUE(lookup_alloc.row.has_value());
    EXPECT_TRUE(lookup_alloc.allocated_pending_store);
    EXPECT_EQ(lookup_alloc.row->location.pool_offset, -1);
    EXPECT_FALSE(lookup_alloc.lease.has_value());

    // UpdateStatus ALLOCATED -> STORED works through in-process meta slot.
    EngineUpdateStatusResult upd = engine.UpdateStatus(
        "v64-block",
        static_cast<int32_t>(BlockStatus::BLOCK_STATUS_ALLOCATED),
        static_cast<int32_t>(BlockStatus::BLOCK_STATUS_STORED),
        /*expected_version=*/1, "", false, 1010);
    EXPECT_TRUE(upd.result.success);
    EXPECT_EQ(upd.new_version, 2);

    // Lookup now succeeds (STORED in cache, no catalog needed).
    EngineLookupResult lookup_stored = engine.Lookup("v64-block", true, 1020);
    EXPECT_TRUE(lookup_stored.result.success);
    ASSERT_TRUE(lookup_stored.row.has_value());
    EXPECT_EQ(lookup_stored.row->status,
              static_cast<int32_t>(BlockStatus::BLOCK_STATUS_STORED));
    EXPECT_TRUE(lookup_stored.cacheable);

    // FreeAllocated (force) works through in-process slot + catalog delete stub.
    EngineFreeAllocatedResult freed = engine.FreeAllocated("v64-block", 2, true, 1030);
    EXPECT_TRUE(freed.result.success);
    EXPECT_EQ(freed.new_version, 3);

    // After free: block is gone from hash index; catalog stub returns nullopt.
    EngineLookupResult after_free = engine.Lookup("v64-block", false, 1040);
    EXPECT_FALSE(after_free.result.success);
    EXPECT_EQ(after_free.result.error_code,
              static_cast<int32_t>(ErrorCode::NOT_FOUND));
}

TEST(KVMetadataEngine, RemoteLibpqTierForbidsInlineCatalog) {
    KVMetadataEngine engine;
    engine.SetCatalogTier(EngineCatalogTier::REMOTE_LIBPQ);

    EngineAllocateResult alloc = engine.Allocate("remote-b1", 65536, 1000);
    EXPECT_FALSE(alloc.result.success);
    EXPECT_EQ(alloc.result.error_code, static_cast<int32_t>(ErrorCode::INTERNAL_ERROR));
    EXPECT_NE(alloc.result.error_message.find("catalog path required"), std::string::npos);

    EngineLookupResult miss = engine.Lookup("missing-remote", false, 1000);
    EXPECT_FALSE(miss.result.success);
    EXPECT_EQ(miss.result.error_code, static_cast<int32_t>(ErrorCode::NOT_FOUND));
    EXPECT_NE(miss.result.error_message.find("catalog path required"), std::string::npos);

    EngineBlockLocation loc;
    loc.store_node_id = 1;
    loc.pool_offset = 0;
    loc.store_epoch = 1;
    ASSERT_TRUE(engine.RestoreRow("remote-b2",
                                  /*STORED*/ 2,
                                  loc,
                                  /*version=*/1,
                                  /*now_ms=*/1000)
                    .success);

    EngineUpdateStatusResult upd = engine.UpdateStatus(
        "remote-b2",
        /*STORED*/ 2,
        /*EVICTING*/ 3,
        /*expected_version=*/1,
        "",
        false,
        1010);
    EXPECT_FALSE(upd.result.success);
    EXPECT_EQ(upd.result.error_code, static_cast<int32_t>(ErrorCode::INTERNAL_ERROR));

    EngineFreeAllocatedResult freed = engine.FreeAllocated("remote-b2", 1, true, 1020);
    EXPECT_FALSE(freed.result.success);
    EXPECT_EQ(freed.result.error_code, static_cast<int32_t>(ErrorCode::INTERNAL_ERROR));
}

TEST(KVMetadataEngine, LookupEvictedCatalogHitAfterDropSlot) {
    KVMetadataEngine engine;
    ASSERT_TRUE(engine.Allocate("ev", 65536, 1000).result.success);
    ASSERT_TRUE(engine.UpdateStatus("ev",
                                    static_cast<int32_t>(BlockStatus::BLOCK_STATUS_ALLOCATED),
                                    static_cast<int32_t>(BlockStatus::BLOCK_STATUS_STORED),
                                    /*expected_version=*/1,
                                    "",
                                    false,
                                    1010)
                    .result.success);
    ASSERT_TRUE(engine.UpdateStatus("ev",
                                    static_cast<int32_t>(BlockStatus::BLOCK_STATUS_STORED),
                                    static_cast<int32_t>(BlockStatus::BLOCK_STATUS_EVICTING),
                                    /*expected_version=*/2,
                                    "",
                                    false,
                                    1020)
                    .result.success);
    ASSERT_TRUE(engine.UpdateStatus("ev",
                                    static_cast<int32_t>(BlockStatus::BLOCK_STATUS_EVICTING),
                                    static_cast<int32_t>(BlockStatus::BLOCK_STATUS_EVICTED),
                                    /*expected_version=*/3,
                                    "/ssd/evicted/ev",
                                    false,
                                    1030)
                    .result.success);

    EngineLookupResult lk = engine.Lookup("ev", false, 2000);
    ASSERT_TRUE(lk.result.success);
    ASSERT_TRUE(lk.row.has_value());
    EXPECT_TRUE(lk.evicted_catalog_hit);
    EXPECT_EQ(lk.row->status, static_cast<int32_t>(BlockStatus::BLOCK_STATUS_EVICTED));
    EXPECT_EQ(lk.row->location.evicted_path, "/ssd/evicted/ev");
    EXPECT_TRUE(lk.cacheable);
}


TEST(KVMetadataEngine, StoreEpochRestartClearsStaleDramRuntime) {
    KVMetadataEngine engine(/*store_node_id=*/1, /*dn_id=*/1,
                            /*region_bytes=*/4 * 65536LL, /*block_size=*/65536,
                            /*dn_epoch=*/1, /*store_epoch=*/1);

    EngineAllocateResult alloc = engine.Allocate("restart-block", 65536, 1000);
    ASSERT_TRUE(alloc.result.success);
    ASSERT_TRUE(alloc.lease.has_value());
    ASSERT_TRUE(engine.UpdateStatus("restart-block",
                                    static_cast<int32_t>(BlockStatus::BLOCK_STATUS_ALLOCATED),
                                    static_cast<int32_t>(BlockStatus::BLOCK_STATUS_STORED),
                                    /*expected_version=*/1,
                                    "",
                                    false,
                                    1010)
                    .result.success);

    EngineLookupResult before = engine.LookupDramCacheOnly("restart-block", true, 1020);
    ASSERT_TRUE(before.result.success);
    ASSERT_TRUE(before.row.has_value());
    ASSERT_TRUE(before.lease.has_value());
    EXPECT_EQ(before.row->location.store_epoch, 1);

    EngineStoreRegion restarted;
    restarted.store_node_id = 1;
    restarted.base_offset = 0;
    restarted.region_bytes = 4 * 65536LL;
    restarted.block_size = 65536;
    restarted.store_epoch = 2;
    ASSERT_TRUE(engine.RegisterStoreRegion(restarted).success);

    EngineStoreRegion stale_region = restarted;
    stale_region.store_epoch = 1;
    EngineResultMeta stale_register = engine.RegisterStoreRegion(stale_region);
    EXPECT_FALSE(stale_register.success);
    EXPECT_EQ(stale_register.error_code, static_cast<int32_t>(ErrorCode::STALE_EPOCH));

    EngineLookupResult after = engine.LookupDramCacheOnly("restart-block", true, 1030);
    EXPECT_TRUE(after.result.success);
    EXPECT_FALSE(after.row.has_value());
    EXPECT_TRUE(after.needs_catalog);

    EngineRenewLeaseResult stale = engine.RenewLease(
        "restart-block",
        before.lease->lease_token,
        before.lease->dn_epoch,
        before.lease->store_epoch,
        /*requested_ttl_ms=*/1000,
        /*now_ms=*/1040);
    EXPECT_FALSE(stale.result.success);
    EXPECT_EQ(stale.result.error_code, static_cast<int32_t>(ErrorCode::LEASE_EXPIRED));

    EngineAllocateResult fresh = engine.Allocate("fresh-after-restart", 65536, 1050);
    ASSERT_TRUE(fresh.result.success);
    ASSERT_TRUE(fresh.row.has_value());
    EXPECT_EQ(fresh.row->location.store_epoch, 2);
    EXPECT_EQ(fresh.row->location.pool_offset, 0);
}

}  // namespace falconfs::kv
