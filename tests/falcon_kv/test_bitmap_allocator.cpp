#include <gtest/gtest.h>

#include "vllm_kv_cache/src/metadata/bitmap_allocator.h"

namespace falconfs::kv {

TEST(BitmapAllocator, AllocateAndFreeWithinContinuousRegion) {
    BitmapAllocator alloc(/*store_node_id=*/1, /*owner_dn_id=*/2,
                          /*base_offset=*/1024, /*region_bytes=*/4 * 64,
                          /*block_size=*/64, /*store_epoch=*/1);
    EXPECT_EQ(alloc.TotalBlocks(), 4);
    EXPECT_EQ(alloc.FreeBlocks(), 4);

    auto a = alloc.Allocate();
    ASSERT_TRUE(a.result.success);
    EXPECT_EQ(a.pool_offset, 1024);
    EXPECT_EQ(alloc.FreeBlocks(), 3);

    auto free_res = alloc.Free(a.pool_offset);
    EXPECT_TRUE(free_res.success);
    EXPECT_EQ(alloc.FreeBlocks(), 4);
}

TEST(BitmapAllocator, RejectDoubleFreeAndOutOfRange) {
    BitmapAllocator alloc(1, 2, 1024, 2 * 64, 64, 1);
    auto a = alloc.Allocate();
    ASSERT_TRUE(a.result.success);
    EXPECT_TRUE(alloc.Free(a.pool_offset).success);

    auto bad_double_free = alloc.Free(a.pool_offset);
    EXPECT_FALSE(bad_double_free.success);
    EXPECT_EQ(bad_double_free.error_code, ErrorCode::INTERNAL_ERROR);

    auto out_of_range = alloc.Free(0);
    EXPECT_FALSE(out_of_range.success);
    EXPECT_EQ(out_of_range.error_code, ErrorCode::INVALID_ARGUMENT);
}

TEST(BitmapAllocator, ExhaustionReturnsRetryableThrottled) {
    BitmapAllocator alloc(1, 2, 0, 64, 64, 1);
    auto first = alloc.Allocate();
    EXPECT_TRUE(first.result.success);
    auto second = alloc.Allocate();
    EXPECT_FALSE(second.result.success);
    EXPECT_EQ(second.result.error_code, ErrorCode::THROTTLED);
    EXPECT_TRUE(second.result.retryable);
}

TEST(BitmapAllocator, MarkOccupiedSupportsRecoveryScan) {
    BitmapAllocator alloc(1, 2, 0, 4 * 64, 64, 1);
    EXPECT_TRUE(alloc.MarkOccupied(64).success);
    EXPECT_TRUE(alloc.MarkOccupied(192).success);
    EXPECT_EQ(alloc.FreeBlocks(), 2);
    auto duplicate = alloc.MarkOccupied(64);
    EXPECT_FALSE(duplicate.success);
    EXPECT_EQ(duplicate.error_code, ErrorCode::INTERNAL_ERROR);
}

TEST(BitmapAllocator, RejectUnalignedFree) {
    BitmapAllocator alloc(1, 2, 1024, 4 * 64, 64, 1);
    auto bad = alloc.Free(1024 + 5);
    EXPECT_FALSE(bad.success);
    EXPECT_EQ(bad.error_code, ErrorCode::INVALID_ARGUMENT);
}

}  // namespace falconfs::kv
