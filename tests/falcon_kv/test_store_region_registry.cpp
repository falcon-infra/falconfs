#include <gtest/gtest.h>

#include "vllm_kv_cache/src/store/store_region_registry.h"

namespace falconfs::kv {

TEST(StoreRegionRegistry, RegisterAndAllocateOnHealthyOnly) {
    StoreRegionRegistry registry(/*suspect=*/3000, /*offline=*/10000);
    StoreRegion r;
    r.store_node_id = 1;
    r.owner_dn_id = 2;
    r.base_offset = 0;
    r.region_bytes = 1024;
    r.block_size = 64;
    r.store_epoch = 5;
    registry.RegisterRegion(r, /*now_ms=*/100);

    EXPECT_TRUE(registry.Has(1, 2));
    EXPECT_TRUE(registry.CanAllocate(1, 2));
    EXPECT_TRUE(registry.CanRead(1, 2));

    EXPECT_TRUE(registry.StartDrain(1, 2));
    EXPECT_FALSE(registry.CanAllocate(1, 2));
    EXPECT_TRUE(registry.CanRead(1, 2));  // reads allowed during DRAINING
}

TEST(StoreRegionRegistry, HeartbeatTimeoutTransitions) {
    StoreRegionRegistry registry(3000, 10000);
    StoreRegion r;
    r.store_node_id = 7;
    r.owner_dn_id = 1;
    r.store_epoch = 2;
    registry.RegisterRegion(r, /*now_ms=*/0);
    EXPECT_EQ(registry.Get(7, 1).state, StoreRegionState::HEALTHY);

    // Within suspect window: still healthy.
    EXPECT_FALSE(registry.TickHeartbeats(/*now_ms=*/2999));
    EXPECT_EQ(registry.Get(7, 1).state, StoreRegionState::HEALTHY);

    // After suspect threshold: SUSPECT.
    EXPECT_TRUE(registry.TickHeartbeats(/*now_ms=*/3000));
    EXPECT_EQ(registry.Get(7, 1).state, StoreRegionState::SUSPECT);
    EXPECT_FALSE(registry.CanAllocate(7, 1));
    EXPECT_TRUE(registry.CanRead(7, 1));

    // After offline threshold: OFFLINE.
    EXPECT_TRUE(registry.TickHeartbeats(/*now_ms=*/10000));
    EXPECT_EQ(registry.Get(7, 1).state, StoreRegionState::OFFLINE);
    EXPECT_FALSE(registry.CanAllocate(7, 1));
    EXPECT_FALSE(registry.CanRead(7, 1));

    // Heartbeat with same epoch returns to HEALTHY.
    EXPECT_TRUE(registry.Heartbeat(7, 1, /*store_epoch=*/2, /*now_ms=*/10500));
    EXPECT_EQ(registry.Get(7, 1).state, StoreRegionState::HEALTHY);
}

TEST(StoreRegionRegistry, NewEpochTriggersQuarantine) {
    StoreRegionRegistry registry(3000, 10000);
    StoreRegion r;
    r.store_node_id = 9;
    r.owner_dn_id = 3;
    r.store_epoch = 1;
    registry.RegisterRegion(r, /*now_ms=*/0);

    EXPECT_TRUE(registry.Heartbeat(9, 3, /*store_epoch=*/2, /*now_ms=*/100));
    EXPECT_EQ(registry.Get(9, 3).state, StoreRegionState::QUARANTINED);
    EXPECT_FALSE(registry.CanAllocate(9, 3));
    EXPECT_FALSE(registry.CanRead(9, 3));

    EXPECT_TRUE(registry.MarkHealthy(9, 3));
    EXPECT_TRUE(registry.CanAllocate(9, 3));
    EXPECT_TRUE(registry.CanRead(9, 3));
}

TEST(StoreRegionRegistry, ListReturnsAllRegisteredRegions) {
    StoreRegionRegistry registry(3000, 10000);
    StoreRegion a;
    a.store_node_id = 1;
    a.owner_dn_id = 1;
    StoreRegion b;
    b.store_node_id = 1;
    b.owner_dn_id = 2;
    registry.RegisterRegion(a, 0);
    registry.RegisterRegion(b, 0);
    auto list = registry.List();
    EXPECT_EQ(list.size(), 2u);
}

}  // namespace falconfs::kv
