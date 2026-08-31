#include <gtest/gtest.h>

#include <stdexcept>
#include <vector>

#include "vllm_kv_cache/src/metadata/kv_subtx.h"

namespace falconfs::kv {

TEST(KVSubTransaction, ProcessInSubBatchesCommitsEachChunk) {
    NoopKVSubTransaction tx;
    std::vector<int> items(20);
    for (int i = 0; i < 20; ++i) items[i] = i;

    int processed = 0;
    std::size_t chunks =
        ProcessInSubBatches(items, kBatchOperationGroupSize, &tx,
                            [&](int /*x*/) { ++processed; });

    EXPECT_EQ(processed, 20);
    EXPECT_EQ(chunks, 3u);  // 8 + 8 + 4
    EXPECT_EQ(tx.BeginCount(), 3);
    EXPECT_EQ(tx.CommitCount(), 3);
    EXPECT_EQ(tx.RollbackCount(), 0);
}

TEST(KVSubTransaction, ProcessInSubBatchesRollsBackOnException) {
    NoopKVSubTransaction tx;
    std::vector<int> items{1, 2, 3, 4, 5, 6, 7, 8, 9};
    EXPECT_THROW(
        ProcessInSubBatches(items, /*group_size=*/4, &tx,
                            [](int x) {
                                if (x == 6) throw std::runtime_error("boom");
                            }),
        std::runtime_error);
    EXPECT_EQ(tx.BeginCount(), 2);
    EXPECT_EQ(tx.CommitCount(), 1);
    EXPECT_EQ(tx.RollbackCount(), 1);
}

TEST(KVSubTransaction, BatchOperationGroupSizeMatchesV6) {
    EXPECT_EQ(kBatchOperationGroupSize, 8);
}

}  // namespace falconfs::kv
