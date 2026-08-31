#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "kv_common.pb.h"
#include "vllm_kv_cache/src/store/kv_store_engine.h"
#include "vllm_kv_cache/src/store/ssd_spill_manager.h"
#include "vllm_kv_cache/src/store/store_region_registry.h"

namespace falconfs::kv {

TEST(KVStoreEngine, WriteReadSuccessWithEpochAndVersion) {
    KVStoreEngine engine;
    StoreWriteResult wr = engine.Write("h1",
                                       /*pool_offset=*/0,
                                       "payload",
                                       static_cast<int32_t>(CompressionType::COMPRESSION_NONE),
                                       /*original_size=*/7,
                                       /*block_size=*/65536,
                                       /*expected_version=*/0,
                                       /*expected_store_epoch=*/1,
                                       /*verify_checksum=*/false,
                                       /*checksum_hint=*/0);
    ASSERT_TRUE(wr.result.success);
    EXPECT_GT(wr.bytes_written, 0);

    StoreReadResult rr = engine.Read("h1",
                                     /*pool_offset=*/0,
                                     /*read_length=*/7,
                                     /*expected_version=*/0,
                                     /*expected_store_epoch=*/1);
    ASSERT_TRUE(rr.result.success);
    EXPECT_EQ(rr.payload, "payload");
    EXPECT_EQ(rr.original_size, 7);
}

TEST(KVStoreEngine, RejectsStaleEpochAndOversizedPayload) {
    KVStoreEngine engine;
    StoreWriteResult stale = engine.Write("h2", 0, "x", 0, 1, 65536, 0, 99, false, 0);
    EXPECT_FALSE(stale.result.success);
    EXPECT_EQ(stale.result.error_code, static_cast<int32_t>(ErrorCode::STALE_EPOCH));

    std::string oversized(70000, 'a');
    StoreWriteResult big = engine.Write("h2", 0, oversized, 0, static_cast<int32_t>(oversized.size()), 65536, 0, 1, false, 0);
    EXPECT_FALSE(big.result.success);
    EXPECT_EQ(big.result.error_code, static_cast<int32_t>(ErrorCode::INVALID_ARGUMENT));
}

TEST(KVStoreEngine, RegionRegistryGatesAllocationAndReads) {
    auto registry = std::make_shared<StoreRegionRegistry>(3000, 10000);
    StoreRegion r;
    r.store_node_id = 1;
    r.owner_dn_id = 7;
    r.store_epoch = 1;
    r.region_bytes = 64 * 65536;
    r.block_size = 65536;
    registry->RegisterRegion(r, /*now_ms=*/0);

    KVStoreEngine engine(/*store_node_id=*/1);
    engine.SetRegionRegistry(registry, /*owner_dn_id=*/7);

    StoreWriteResult ok_write = engine.Write("rk", 0, "hello-region", 0, 12, 65536, 0, 1, false, 0);
    ASSERT_TRUE(ok_write.result.success);

    registry->StartDrain(1, 7);
    StoreWriteResult drain_write = engine.Write("rk2", 65536, "x", 0, 1, 65536, 0, 1, false, 0);
    EXPECT_FALSE(drain_write.result.success);
    EXPECT_EQ(drain_write.result.error_code, static_cast<int32_t>(ErrorCode::STORE_WRITE_FAILED));

    // Reads still allowed during DRAINING for the previously written block.
    StoreReadResult drain_read = engine.Read("rk", 0, 12, 0, 1);
    EXPECT_TRUE(drain_read.result.success);
}

TEST(KVStoreEngine, SsdSpillRoundtripWithRealSpillManager) {
    namespace fs = std::filesystem;
    const auto root = (fs::temp_directory_path() / ("falconfs_kv_engine_ssd_" + std::to_string(::getpid()))).string();
    fs::create_directories(root);
    auto spill = std::make_shared<SSDSpillManager>(root);

    KVStoreEngine engine(/*store_node_id=*/4);
    engine.SetSSDSpillManager(spill);

    ASSERT_TRUE(engine
                    .Write("spill-key",
                           /*pool_offset=*/0,
                           /*payload=*/"persisted-bytes",
                           /*compression=*/static_cast<int32_t>(CompressionType::COMPRESSION_NONE),
                           /*original_size=*/15,
                           /*block_size=*/65536,
                           /*expected_version=*/0,
                           /*expected_store_epoch=*/1,
                           /*verify_checksum=*/false,
                           /*checksum_hint=*/0)
                    .result.success);

    std::string evicted_path;
    StoreWriteResult sp = engine.SpillBlockToSSD("spill-key",
                                                 /*pool_offset=*/0,
                                                 /*expected_version=*/1,
                                                 /*expected_store_epoch=*/1,
                                                 /*dram_read_size=*/15,
                                                 &evicted_path);
    ASSERT_TRUE(sp.result.success);
    EXPECT_EQ(sp.bytes_written, 15);
    EXPECT_TRUE(spill->ValidatePath(evicted_path));

    StoreReadResult rr = engine.ReadFromSSD("spill-key", evicted_path, /*expected_version=*/1);
    ASSERT_TRUE(rr.result.success);
    EXPECT_EQ(rr.payload, "persisted-bytes");
    EXPECT_EQ(rr.original_size, 15);
}

TEST(KVStoreEngine, SsdReadPathValidationAndSuccess) {
    KVStoreEngine engine;
    engine.PutSSDForTest("h3",
                         "/tmp/falconfs/evicted/h3.bin",
                         "ssd-data",
                         static_cast<int32_t>(CompressionType::COMPRESSION_NONE),
                         8,
                         /*version=*/3);

    StoreReadResult bad_path = engine.ReadFromSSD("h3", "../escape", 3);
    EXPECT_FALSE(bad_path.result.success);
    EXPECT_EQ(bad_path.result.error_code, static_cast<int32_t>(ErrorCode::INVALID_ARGUMENT));

    StoreReadResult ok = engine.ReadFromSSD("h3", "/tmp/falconfs/evicted/h3.bin", 3);
    ASSERT_TRUE(ok.result.success);
    EXPECT_EQ(ok.payload, "ssd-data");
    EXPECT_EQ(ok.original_size, 8);
}

TEST(KVStoreEngine, HeartbeatSenderRefreshesRegionLiveness) {
    auto registry = std::make_shared<StoreRegionRegistry>(/*suspect_after_ms=*/20,
                                                          /*offline_after_ms=*/40);
    StoreRegion r;
    r.store_node_id = 1;
    r.owner_dn_id = 8;
    r.store_epoch = 1;
    r.region_bytes = 64 * 65536;
    r.block_size = 65536;
    registry->RegisterRegion(r, /*now_ms=*/0);

    KVStoreEngine engine(/*store_node_id=*/1);
    engine.SetRegionRegistry(registry, /*owner_dn_id=*/8);

    int sends = 0;
    std::vector<int64_t> observed_heartbeat_ms;
    engine.SetHeartbeatSender([&](int32_t owner_dn_id, int32_t store_node_id, int64_t store_epoch, int64_t now_ms) {
        EXPECT_EQ(owner_dn_id, 8);
        EXPECT_EQ(store_node_id, 1);
        EXPECT_EQ(store_epoch, 1);
        ++sends;
        observed_heartbeat_ms.push_back(now_ms);
        return true;
    });

    const auto targets = engine.HeartbeatTargets();
    ASSERT_EQ(targets.size(), 1U);
    EXPECT_EQ(targets[0].owner_dn_id, 8);
    EXPECT_EQ(targets[0].store_epoch, 1);

    EXPECT_EQ(engine.SendHeartbeats(/*now_ms=*/10), 1);
    EXPECT_EQ(sends, 1);

    // Without another heartbeat, the region should time out.
    EXPECT_TRUE(registry->TickHeartbeats(/*now_ms=*/31));
    EXPECT_FALSE(registry->CanAllocate(1, 8));

    // Sending heartbeats again should recover the region to HEALTHY.
    EXPECT_EQ(engine.SendHeartbeats(/*now_ms=*/35), 1);
    ASSERT_EQ(observed_heartbeat_ms.size(), 2U);
    EXPECT_EQ(observed_heartbeat_ms[0], 10);
    EXPECT_EQ(observed_heartbeat_ms[1], 35);
    EXPECT_TRUE(registry->CanAllocate(1, 8));
}

}  // namespace falconfs::kv
