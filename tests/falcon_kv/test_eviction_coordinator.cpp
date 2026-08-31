// EvictionCoordinator unit tests (v6 §14).
#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "kv_common.pb.h"
#include "vllm_kv_cache/src/metadata/eviction_coordinator.h"
#include "vllm_kv_cache/src/metadata/kv_metadata_engine.h"

namespace falconfs::kv {

namespace {

constexpr int32_t kAllocated = static_cast<int32_t>(BlockStatus::BLOCK_STATUS_ALLOCATED);
constexpr int32_t kStored = static_cast<int32_t>(BlockStatus::BLOCK_STATUS_STORED);
constexpr int32_t kEvicted = static_cast<int32_t>(BlockStatus::BLOCK_STATUS_EVICTED);

// Allocates a block, transitions it to STORED, and returns its current version.
void PreloadStored(KVMetadataEngine& engine, const std::string& block_hash, int64_t now_ms) {
    EngineAllocateResult alloc = engine.Allocate(block_hash, /*block_size=*/65536, now_ms);
    ASSERT_TRUE(alloc.result.success);
    EngineUpdateStatusResult to_stored = engine.UpdateStatus(
        block_hash, kAllocated, kStored, /*expected_version=*/1,
        /*evicted_path=*/"", /*allow_noop_if_already_target=*/false, now_ms);
    ASSERT_TRUE(to_stored.result.success);
}

}  // namespace

TEST(EvictionCoordinator, EvictsColdStoredCandidatesEndToEnd) {
    auto engine = std::make_shared<KVMetadataEngine>();
    PreloadStored(*engine, "cold-1", /*now_ms=*/100);
    PreloadStored(*engine, "cold-2", /*now_ms=*/110);

    int spill_calls = 0;
    EvictionCoordinator::SpillFn spill = [&](const std::string& block_hash, int64_t /*version*/,
                                             int64_t /*pool_offset*/, int64_t /*store_epoch*/) {
        ++spill_calls;
        EvictionCoordinator::SpillOutcome out;
        out.ok = true;
        out.evicted_path = "/tmp/falcon_kv/spill/" + block_hash + ".kv";
        return out;
    };
    EvictionCoordinator coord(engine, spill);

    // Use a now_ms past the default lease TTL (5s) so leases granted on
    // allocate are naturally expired and CanEvict returns true for both rows.
    EvictionCycleResult res = coord.RunOneCycle({/*max_per_cycle=*/16, /*now_ms=*/200000});
    EXPECT_EQ(res.candidates_considered, 2);
    EXPECT_EQ(res.evicted, 2);
    EXPECT_EQ(res.rolled_back, 0);
    EXPECT_EQ(spill_calls, 2);

    // Both rows should now be EVICTED with evicted_path populated.
    EngineLookupResult lk1 = engine->Lookup("cold-1", /*renew=*/false, /*now_ms=*/200010);
    ASSERT_TRUE(lk1.row.has_value());
    EXPECT_EQ(lk1.row->status, kEvicted);
    EXPECT_EQ(lk1.row->location.evicted_path, "/tmp/falcon_kv/spill/cold-1.kv");
}

TEST(EvictionCoordinator, ActiveLeaseSkipsCandidate) {
    auto engine = std::make_shared<KVMetadataEngine>();
    PreloadStored(*engine, "warm", /*now_ms=*/100);

    // Renew lease (Lookup with renew=true grants a fresh lease that is not
    // yet expired at now_ms=120).
    EngineLookupResult lookup = engine->Lookup("warm", /*renew=*/true, /*now_ms=*/110);
    ASSERT_TRUE(lookup.lease.has_value());
    ASSERT_GT(lookup.lease->lease_expire_ms, 120);

    int spill_calls = 0;
    EvictionCoordinator::SpillFn spill = [&](const std::string&, int64_t, int64_t, int64_t) {
        ++spill_calls;
        return EvictionCoordinator::SpillOutcome{true, "/never"};
    };
    EvictionCoordinator coord(engine, spill);

    EvictionCycleResult res = coord.RunOneCycle({/*max_per_cycle=*/16, /*now_ms=*/120});
    EXPECT_EQ(res.candidates_considered, 1);
    EXPECT_EQ(res.skipped_active_lease, 1);
    EXPECT_EQ(res.evicted, 0);
    EXPECT_EQ(spill_calls, 0);

    EngineLookupResult after = engine->Lookup("warm", /*renew=*/false, /*now_ms=*/130);
    ASSERT_TRUE(after.row.has_value());
    EXPECT_EQ(after.row->status, kStored);
}

TEST(EvictionCoordinator, SpillFailureRollsBackToStored) {
    auto engine = std::make_shared<KVMetadataEngine>();
    PreloadStored(*engine, "fail-1", /*now_ms=*/100);

    EvictionCoordinator::SpillFn fail_spill = [&](const std::string&, int64_t, int64_t, int64_t) {
        return EvictionCoordinator::SpillOutcome{false, ""};
    };
    EvictionCoordinator coord(engine, fail_spill);

    EvictionCycleResult res = coord.RunOneCycle({/*max_per_cycle=*/16, /*now_ms=*/200000});
    EXPECT_EQ(res.candidates_considered, 1);
    EXPECT_EQ(res.evicted, 0);
    EXPECT_EQ(res.rolled_back, 1);

    EngineLookupResult after = engine->Lookup("fail-1", /*renew=*/false, /*now_ms=*/200010);
    ASSERT_TRUE(after.row.has_value());
    EXPECT_EQ(after.row->status, kStored);
}

}  // namespace falconfs::kv
