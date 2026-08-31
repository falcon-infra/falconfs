#include <gtest/gtest.h>

#include "kv_metadata_service.pb.h"
#include "vllm_kv_cache/src/metadata/kv_meta_table_accessor.h"
#include "vllm_kv_cache/src/metadata/kv_metadata_recovery.h"

namespace falconfs::kv {

namespace {
constexpr int32_t kAllocated = 1;
constexpr int32_t kStored = 2;
constexpr int32_t kEvicting = 3;
constexpr int32_t kEvicted = 4;
}

TEST(KVMetadataRecovery, RestoresRowsAndReconcilesEvicting) {
    InMemoryKVMetaTableAccessor acc;
    AccessorInsertSpec spec;
    spec.store_node_id = 1;

    spec.pool_offset = 0;
    ASSERT_TRUE(acc.InsertAllocated(/*shard=*/1, "a", spec, /*now_ms=*/10));
    spec.pool_offset = 65536;
    ASSERT_TRUE(acc.InsertAllocated(/*shard=*/1, "b", spec, /*now_ms=*/10));
    spec.pool_offset = 131072;
    ASSERT_TRUE(acc.InsertAllocated(/*shard=*/1, "c", spec, /*now_ms=*/10));

    ASSERT_TRUE(acc.CASStatusUpdate(1, "b", kAllocated, kStored, 1, "", 20).success);
    ASSERT_TRUE(acc.CASStatusUpdate(1, "c", kAllocated, kEvicting, 1, "", 20).success);

    // Seed an evicted row too.
    spec.pool_offset = 196608;
    ASSERT_TRUE(acc.InsertAllocated(/*shard=*/1, "d", spec, /*now_ms=*/10));
    ASSERT_TRUE(acc.CASStatusUpdate(1, "d", kAllocated, kEvicted, 1, "/tmp/evicted/d", 20).success);

    KVMetadataEngine engine(/*store_node_id=*/1,
                            /*dn_id=*/1,
                            /*region_bytes=*/64LL * 65536LL,
                            /*block_size=*/65536,
                            /*dn_epoch=*/1,
                            /*store_epoch=*/1);

    MetadataRecoveryStats stats = RecoverMetadataFromAccessor(&acc, &engine, /*shard_id=*/1, /*now_ms=*/1000);
    EXPECT_EQ(stats.bumped_dn_epoch, 2);
    EXPECT_EQ(stats.recovered_rows, 4);
    EXPECT_EQ(stats.restored_allocated_rows, 1);
    EXPECT_EQ(stats.restored_stored_rows, 2);      // b + reconciled c
    EXPECT_EQ(stats.restored_evicted_rows, 1);
    EXPECT_EQ(stats.reconciled_evicting_rows, 1);
    EXPECT_EQ(stats.bitmap_marked, 3);             // allocated/stored/reconciled-stored

    EngineLookupResult lk_b = engine.Lookup("b", /*renew=*/false, /*now_ms=*/2000);
    ASSERT_TRUE(lk_b.result.success);
    EXPECT_EQ(lk_b.row->status, kStored);

    EngineLookupResult lk_c = engine.Lookup("c", /*renew=*/false, /*now_ms=*/2000);
    ASSERT_TRUE(lk_c.result.success);
    EXPECT_EQ(lk_c.row->status, kStored);
}

TEST(KVMetadataRecovery, ApplyScanShardForRecoveryResponseSetsEpochAndRows) {
    KVMetadataEngine engine(/*store_node_id=*/1,
                            /*dn_id=*/1,
                            /*region_bytes=*/64LL * 65536LL,
                            /*block_size=*/65536,
                            /*dn_epoch=*/1,
                            /*store_epoch=*/1,
                            /*kvblock_shard_id=*/1);

    ScanShardForRecoveryResponse resp;
    resp.mutable_result()->set_success(true);
    resp.mutable_result()->set_error_code(ErrorCode::OK);
    resp.set_dn_epoch_after_bump(42);

    auto add_row = [&](const std::string& hash, BlockStatus st, int64_t off, int64_t ver) {
        auto* row = resp.add_rows();
        row->set_block_hash(hash);
        row->set_status(st);
        row->mutable_location()->set_store_node_id(1);
        row->mutable_location()->set_pool_offset(off);
        row->mutable_location()->set_store_epoch(1);
        row->set_version(ver);
        row->set_updated_at_ms(100);
    };

    add_row("ra", BlockStatus::BLOCK_STATUS_ALLOCATED, 0, 0);
    add_row("rs", BlockStatus::BLOCK_STATUS_STORED, 65536, 1);
    add_row("re", BlockStatus::BLOCK_STATUS_EVICTING, 131072, 1);

    const ScanShardRecoveryApplyStats st =
        ApplyScanShardForRecoveryResponse(&engine, resp.SerializeAsString(), /*now_ms=*/5000);
    EXPECT_EQ(st.shards_scanned, 1);
    EXPECT_EQ(st.rows_applied, 3);
    EXPECT_EQ(st.reconciled_evicting, 1);
    EXPECT_EQ(engine.DnEpoch(), 42);

    EngineLookupResult lka = engine.Lookup("ra", false, 6000);
    ASSERT_TRUE(lka.result.success);
    EXPECT_EQ(lka.row->status, kAllocated);

    EngineLookupResult lks = engine.Lookup("rs", false, 6000);
    ASSERT_TRUE(lks.result.success);
    EXPECT_EQ(lks.row->status, kStored);

    EngineLookupResult lke = engine.Lookup("re", false, 6000);
    ASSERT_TRUE(lke.result.success);
    EXPECT_EQ(lke.row->status, kStored);
}


TEST(KVMetadataRecovery, LargeDnRecoveryStressRestoresRowsAndBitmap) {
    InMemoryKVMetaTableAccessor acc;
    constexpr int kRows = 384;
    constexpr int kBlockSize = 65536;
    AccessorInsertSpec spec;
    spec.store_node_id = 1;

    int expected_allocated = 0;
    int expected_stored = 0;
    int expected_evicted = 0;
    int expected_evicting = 0;
    for (int i = 0; i < kRows; ++i) {
        const std::string hash = "stress-" + std::to_string(i);
        spec.pool_offset = static_cast<int64_t>(i) * kBlockSize;
        ASSERT_TRUE(acc.InsertAllocated(/*shard=*/1, hash, spec, /*now_ms=*/10 + i));
        switch (i % 4) {
        case 0:
            ++expected_allocated;
            break;
        case 1:
            ASSERT_TRUE(acc.CASStatusUpdate(1, hash, kAllocated, kStored, 1, "", 20 + i).success);
            ++expected_stored;
            break;
        case 2:
            ASSERT_TRUE(acc.CASStatusUpdate(1, hash, kAllocated, kEvicting, 1, "", 20 + i).success);
            ++expected_evicting;
            break;
        default:
            ASSERT_TRUE(acc.CASStatusUpdate(1, hash, kAllocated, kEvicted, 1,
                                            "/tmp/evicted/" + hash, 20 + i).success);
            ++expected_evicted;
            break;
        }
    }

    KVMetadataEngine engine(/*store_node_id=*/1,
                            /*dn_id=*/1,
                            /*region_bytes=*/static_cast<int64_t>(kRows + 8) * kBlockSize,
                            /*block_size=*/kBlockSize,
                            /*dn_epoch=*/7,
                            /*store_epoch=*/1);

    MetadataRecoveryStats stats = RecoverMetadataFromAccessor(&acc, &engine, /*shard_id=*/1,
                                                              /*now_ms=*/100000);
    EXPECT_EQ(stats.bumped_dn_epoch, 2);
    EXPECT_EQ(stats.recovered_rows, kRows);
    EXPECT_EQ(stats.restored_allocated_rows, expected_allocated);
    EXPECT_EQ(stats.restored_stored_rows, expected_stored + expected_evicting);
    EXPECT_EQ(stats.reconciled_evicting_rows, expected_evicting);
    EXPECT_EQ(stats.restored_evicted_rows, expected_evicted);
    EXPECT_EQ(stats.bitmap_marked, expected_allocated + expected_stored + expected_evicting);

    EngineLookupResult allocated = engine.Lookup("stress-0", /*renew=*/false, /*now_ms=*/101000);
    ASSERT_TRUE(allocated.result.success);
    EXPECT_EQ(allocated.row->status, kAllocated);

    EngineLookupResult stored = engine.Lookup("stress-1", /*renew=*/false, /*now_ms=*/101000);
    ASSERT_TRUE(stored.result.success);
    EXPECT_EQ(stored.row->status, kStored);

    EngineLookupResult reconciled = engine.Lookup("stress-2", /*renew=*/false, /*now_ms=*/101000);
    ASSERT_TRUE(reconciled.result.success);
    EXPECT_EQ(reconciled.row->status, kStored);
}

TEST(KVMetadataRecovery, LateStoreRegistrationParksAndReplaysRows) {
    InMemoryKVMetaTableAccessor acc;
    constexpr int kBlockSize = 65536;
    AccessorInsertSpec spec;
    spec.store_node_id = 2;
    spec.pool_offset = 0;
    ASSERT_TRUE(acc.InsertAllocated(/*shard=*/1, "late-store-row", spec, /*now_ms=*/10));
    ASSERT_TRUE(acc.CASStatusUpdate(1, "late-store-row", kAllocated, kStored, 1, "", 20).success);

    KVMetadataEngine engine(/*store_node_id=*/1,
                            /*dn_id=*/1,
                            /*region_bytes=*/8LL * kBlockSize,
                            /*block_size=*/kBlockSize,
                            /*dn_epoch=*/3,
                            /*store_epoch=*/1);

    MetadataRecoveryStats first = RecoverMetadataFromAccessor(&acc, &engine, /*shard_id=*/1,
                                                              /*now_ms=*/1000);
    EXPECT_EQ(first.recovered_rows, 1);
    EXPECT_EQ(first.restored_stored_rows, 1);
    EXPECT_FALSE(engine.HasRegion(2));

    EngineStoreRegion region;
    region.store_node_id = 2;
    region.base_offset = 0;
    region.region_bytes = 8LL * kBlockSize;
    region.block_size = kBlockSize;
    region.store_epoch = 1;
    ASSERT_TRUE(engine.RegisterStoreRegion(region).success);

    EngineLookupResult replayed = engine.Lookup("late-store-row", /*renew=*/false,
                                                /*now_ms=*/2000);
    ASSERT_TRUE(replayed.result.success);
    ASSERT_TRUE(replayed.row.has_value());
    EXPECT_EQ(replayed.row->status, kStored);
    EXPECT_EQ(replayed.row->location.store_node_id, 2);
}

}  // namespace falconfs::kv
