#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "vllm_kv_cache/src/metadata/kv_meta_table_accessor.h"

namespace falconfs::kv {

namespace {
constexpr int32_t kAllocated = 1;
constexpr int32_t kStored = 2;
}  // namespace

TEST(InMemoryKVMetaTableAccessor, InsertLookupCASUpdateAndDelete) {
    InMemoryKVMetaTableAccessor acc;
    AccessorInsertSpec spec;
    spec.kv_group_idx = 1;
    spec.layer_mask = 7;
    spec.store_node_id = 4;
    spec.pool_offset = 65536;
    ASSERT_TRUE(acc.InsertAllocated(/*shard=*/1, "k1", spec, /*now_ms=*/100));
    EXPECT_FALSE(acc.InsertAllocated(1, "k1", spec, 110));  // duplicate

    AccessorRow row;
    ASSERT_TRUE(acc.Lookup(1, "k1", &row));
    EXPECT_EQ(row.status, kAllocated);
    EXPECT_EQ(row.version, 1);
    EXPECT_EQ(row.store_node_id, 4);

    AccessorCASResult cas =
        acc.CASStatusUpdate(1, "k1", kAllocated, kStored, /*expected_version=*/1,
                            /*evicted_path=*/"", /*now_ms=*/120);
    EXPECT_TRUE(cas.success);
    EXPECT_EQ(cas.current_version, 2);
    EXPECT_EQ(cas.current_status, kStored);

    AccessorCASResult conflict =
        acc.CASStatusUpdate(1, "k1", kAllocated, kStored, /*expected_version=*/1,
                            /*evicted_path=*/"", /*now_ms=*/130);
    EXPECT_FALSE(conflict.success);
    EXPECT_TRUE(conflict.conflict);
    EXPECT_TRUE(conflict.retryable);

    bool del_conflict = false;
    EXPECT_FALSE(acc.Delete(1, "k1", /*expected_version=*/1, &del_conflict));
    EXPECT_TRUE(del_conflict);
    EXPECT_TRUE(acc.Delete(1, "k1", /*expected_version=*/2, &del_conflict));
    EXPECT_FALSE(acc.Lookup(1, "k1", &row));
}

TEST(InMemoryKVMetaTableAccessor, ScanForRecoveryFiltersByStatus) {
    InMemoryKVMetaTableAccessor acc;
    AccessorInsertSpec spec;
    ASSERT_TRUE(acc.InsertAllocated(2, "a", spec, 100));
    ASSERT_TRUE(acc.InsertAllocated(2, "b", spec, 100));
    ASSERT_TRUE(acc.CASStatusUpdate(2, "a", kAllocated, kStored, 1, "", 110).success);

    std::vector<std::string> stored_hashes;
    acc.ScanForRecovery(2, /*wanted=*/{kStored},
                        [&](const std::string& bh, const AccessorRow& /*r*/) {
                            stored_hashes.push_back(bh);
                        });
    EXPECT_EQ(stored_hashes.size(), 1u);
    EXPECT_EQ(stored_hashes[0], "a");
}

TEST(InMemoryKVMetaTableAccessor, PersistedDnEpochLoadAndBump) {
    InMemoryKVMetaTableAccessor acc;
    EXPECT_EQ(acc.LoadDnEpoch(/*shard=*/9), 1);
    EXPECT_EQ(acc.BumpDnEpoch(/*shard=*/9), 2);
    EXPECT_EQ(acc.LoadDnEpoch(/*shard=*/9), 2);
    EXPECT_EQ(acc.BumpDnEpoch(/*shard=*/9), 3);
}

}  // namespace falconfs::kv
