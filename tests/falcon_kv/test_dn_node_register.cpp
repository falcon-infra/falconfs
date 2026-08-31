#include <gtest/gtest.h>

#include "vllm_kv_cache/src/metadata/kv_metadata_engine.h"

namespace falconfs::kv {

// v6.5 P1: DN metadata engine boots without an implicit Store region; the
// first explicit RegisterStoreRegion attaches DRAM backing (catalog replay is
// layered separately in the bgworker).
TEST(DnNodeRegisterReadiness, EngineStartsWithoutRegionUntilRegister) {
    KVMetadataEngine engine(/*store_node_id=*/1,
                            /*dn_id=*/1,
                            /*region_bytes=*/0,
                            /*block_size=*/65536,
                            /*dn_epoch=*/1,
                            /*store_epoch=*/1,
                            /*kvblock_shard_id=*/1);
    EXPECT_FALSE(engine.HasRegion(1));

    EngineStoreRegion r{};
    r.store_node_id = 1;
    r.base_offset   = 0;
    r.region_bytes  = 64LL * 65536LL;
    r.block_size    = 65536;
    r.store_epoch   = 1;
    ASSERT_TRUE(engine.RegisterStoreRegion(r).success);
    EXPECT_TRUE(engine.HasRegion(1));
}

}  // namespace falconfs::kv
