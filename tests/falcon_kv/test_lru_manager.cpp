#include <gtest/gtest.h>

#include "vllm_kv_cache/src/metadata/lru_manager.h"

namespace falconfs::kv {

TEST(LRUManager, ColdEndIsLeastRecentlyUsed) {
    LRUManager lru;
    lru.AddStored("a");
    lru.AddStored("b");
    lru.Touch("a");
    auto cold = lru.ColdCandidates(2);
    ASSERT_EQ(cold.size(), 2u);
    EXPECT_EQ(cold[0], "b");
    EXPECT_EQ(cold[1], "a");
}

TEST(LRUManager, RemoveDropsBlock) {
    LRUManager lru;
    lru.AddStored("a");
    lru.AddStored("b");
    lru.Remove("b");
    auto cold = lru.ColdCandidates(2);
    ASSERT_EQ(cold.size(), 1u);
    EXPECT_EQ(cold[0], "a");
    EXPECT_EQ(lru.Size(), 1u);
}

TEST(LRUManager, TouchReinsertsExisting) {
    LRUManager lru;
    lru.AddStored("a");
    lru.AddStored("b");
    lru.AddStored("c");
    lru.Touch("a");
    auto cold = lru.ColdCandidates(3);
    ASSERT_EQ(cold.size(), 3u);
    EXPECT_EQ(cold.front(), "b");
    EXPECT_EQ(cold.back(), "a");
}

TEST(LRUManager, RemoveAbsentBlockIsNoop) {
    LRUManager lru;
    lru.Remove("missing");
    EXPECT_EQ(lru.Size(), 0u);
}

}  // namespace falconfs::kv
