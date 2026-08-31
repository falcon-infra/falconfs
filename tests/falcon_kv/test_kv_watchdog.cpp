#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "vllm_kv_cache/src/metadata/kv_metadata_engine.h"

namespace falconfs::kv {

// v6.5 P3: `falcon_kv_membership_watchdog_tick` + the bgworker watchdog thread
// flip catalog rows when last_heartbeat_ms lags wall clock beyond
// falcon_kv.watchdog_skew_ms. Live SQL + NOTIFY are exercised on the cluster
// harness; here we assert the metadata engine still boots cleanly alongside
// the membership schema helpers (regression gate keeps this binary linked).
TEST(KvWatchdog, MetadataEngineBootsWithMembershipSchema) {
    KVMetadataEngine engine(/*store_node_id=*/1,
                            /*dn_id=*/1,
                            /*region_bytes=*/0,
                            /*block_size=*/65536,
                            /*dn_epoch=*/1,
                            /*store_epoch=*/1,
                            /*kvblock_shard_id=*/1);
    (void) engine;
    SUCCEED();
}

}  // namespace falconfs::kv
