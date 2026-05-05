#include "test_disk_cache.h"
#include "disk_cache/disk_cache.h"
#include "util/utils.h"

std::string DiskCacheUT::rootPath = "/tmp/testdir/";

TEST_F(DiskCacheUT, Start)
{
    DiskCache diskCache;
    int ret = diskCache.Start(rootPath, 100, 0.2, 0.2);
    EXPECT_EQ(ret, 0);
}

TEST_F(DiskCacheUT, EvictUsingLRU)
{
    DiskCache diskCache;
    int ret = diskCache.Start(rootPath, 100, 0.2, 0.2);
    EXPECT_EQ(ret, 0);

    // Ensure the GetFilePath method generates the correct path
    SetRootPath(rootPath);
    SetTotalDirectory(100);

    uint64_t itemSize = 1024; // 1KB
    for (uint64_t i = 1; i <= 3; ++i) {
        std::string fileName = GetFilePath(i);
        FILE* file = fopen(fileName.c_str(), "w");
        EXPECT_NE(file, nullptr);

        char buffer[itemSize];
        memset(buffer, 'a' + i, itemSize);
        size_t written = fwrite(buffer, 1, itemSize, file);
        EXPECT_EQ(written, itemSize);
        fclose(file);

        diskCache.InsertAndUpdate(i, itemSize, false);
        EXPECT_TRUE(diskCache.Find(i, false));
    }

    // Access the second item and move it to the end of the LRU list
    diskCache.Find(2, true);
    // Decrements the reference count, ensuring that the item can be evicted
    diskCache.Unpin(2);

    // Simulate a situation where disk space is insufficient
    uint64_t mockTotalCap = 5 * 1024;  // 5KB
    uint64_t mockFreeCap = 1 * 1024;   // 1KB
    uint64_t mockUsedCap = 3 * 1024;  // 3KB
    DiskCacheTestHelper::MockLowDiskSpace(diskCache, mockTotalCap, mockFreeCap, mockUsedCap);
    uint64_t evictSize = 2 * 1024; // 2KB
    // Bypassing the GetCurFreeRatio call
    DiskCacheTestHelper::CleanupForEvict(diskCache, evictSize);

    EXPECT_TRUE(diskCache.Find(2, false)) << "The most recently used item 2 should still exist";
    EXPECT_FALSE(diskCache.Find(1, false)) << "Item 1 should be evicted";
    EXPECT_FALSE(diskCache.Find(3, false)) << "Item 3 should be evicted";

    // Clean up test data
    diskCache.Delete(2);
}


int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
