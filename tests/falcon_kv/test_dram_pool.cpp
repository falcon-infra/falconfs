#include <gtest/gtest.h>

#include <cstdlib>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "vllm_kv_cache/src/store/dram_pool.h"

namespace falconfs::kv {

TEST(DramPool, AlignedWriteAndReadRoundtrip) {
    DramPool pool(/*region_bytes=*/4 * 4096, /*block_size=*/4096);
    EXPECT_EQ(pool.RegionBytes(), 4 * 4096u);
    EXPECT_EQ(pool.BlockSize(), 4096u);

    DramPoolWriteResult w = pool.Write(/*pool_offset=*/0, "hello");
    EXPECT_TRUE(w.ok);
    EXPECT_EQ(w.bytes_written, 5);

    std::string out;
    EXPECT_TRUE(pool.Read(/*pool_offset=*/0, /*size=*/5, &out));
    EXPECT_EQ(out, "hello");
}

TEST(DramPool, RejectsUnalignedAndOutOfRangeOffsets) {
    DramPool pool(8192, 4096);
    EXPECT_FALSE(pool.ValidOffset(-1));
    EXPECT_FALSE(pool.ValidOffset(1));        // unaligned
    EXPECT_FALSE(pool.ValidOffset(8192));     // out of range
    EXPECT_TRUE(pool.ValidOffset(0));
    EXPECT_TRUE(pool.ValidOffset(4096));

    DramPoolWriteResult w = pool.Write(1, "x");
    EXPECT_FALSE(w.ok);

    std::string out;
    EXPECT_FALSE(pool.Read(8192, 1, &out));
}

TEST(DramPool, OversizedPayloadRejected) {
    DramPool pool(4096, 1024);
    std::string big(2048, 'a');
    DramPoolWriteResult w = pool.Write(0, big);
    EXPECT_FALSE(w.ok);
}

TEST(DramPool, RejectsNonMultipleSize) {
    EXPECT_THROW(DramPool(5000, 4096), std::invalid_argument);
}

TEST(DramPool, ConcurrentWritesDisjointStripes) {
    DramPool pool(8192, 4096);
    ASSERT_GE(pool.NumStripes(), 2u);
    std::vector<std::thread> threads;
    threads.emplace_back([&] {
        DramPoolWriteResult w = pool.Write(0, "aaaa");
        ASSERT_TRUE(w.ok);
    });
    threads.emplace_back([&] {
        DramPoolWriteResult w = pool.Write(4096, "bbbb");
        ASSERT_TRUE(w.ok);
    });
    for (auto& t : threads) {
        t.join();
    }
    std::string a;
    std::string b;
    ASSERT_TRUE(pool.Read(0, 4, &a));
    ASSERT_TRUE(pool.Read(4096, 4, &b));
    EXPECT_EQ(a, "aaaa");
    EXPECT_EQ(b, "bbbb");
}

TEST(DramPool, ConcurrentReadsSameSlot) {
    DramPool pool(4096, 4096);
    ASSERT_TRUE(pool.Write(0, "concurrent-read").ok);
    constexpr int kReaders = 16;
    std::vector<std::thread> threads;
    threads.reserve(kReaders);
    for (int i = 0; i < kReaders; ++i) {
        threads.emplace_back([&pool] {
            std::string out;
            ASSERT_TRUE(pool.Read(0, 15, &out));
            EXPECT_EQ(out, "concurrent-read");
        });
    }
    for (auto& t : threads) {
        t.join();
    }
}

TEST(DramPool, StripeCountFromEnv) {
    ASSERT_EQ(::setenv("FALCON_KV_STORE_DRAM_STRIPES", "3", 1), 0);
    DramPool pool(4096, 4096);
    EXPECT_EQ(pool.NumStripes(), 3u);
    ::unsetenv("FALCON_KV_STORE_DRAM_STRIPES");
}

}  // namespace falconfs::kv
