#include "test_disk_cache.h"
#define private public
#include "disk_cache/disk_cache.h"
#undef private
#include "util/utils.h"

#include <fstream>

std::string DiskCacheUT::rootPath = "/tmp/testdir/";

TEST_F(DiskCacheUT, Start)
{
    int ret = DiskCache::GetInstance().Start(rootPath, 100, 0.2, 0.2);
    EXPECT_EQ(ret, 0);
}

TEST_F(DiskCacheUT, StartWithZeroRatioUsesDirectFileChecks)
{
    std::string directRoot = "/tmp/testdir_zero_ratio";
    std::filesystem::remove_all(directRoot);
    std::filesystem::create_directories(directRoot + "/0");
    SetRootPath(directRoot);
    SetTotalDirectory(1);

    DiskCache cache;
    EXPECT_EQ(cache.Start(directRoot, 1, 0.0, 0.0), 0);

    uint64_t key = 100;
    std::string file = GetFilePath(key);
    EXPECT_FALSE(cache.Find(key, false));
    {
        std::ofstream out(file);
        out << "cached";
    }
    EXPECT_TRUE(cache.Find(key, false));
    EXPECT_EQ(cache.Delete(key), 0);
    EXPECT_FALSE(std::filesystem::exists(file));

    std::filesystem::remove_all(directRoot);
}

TEST_F(DiskCacheUT, InsertUpdatePinAndDeleteLifecycle)
{
    std::string cacheRoot = "/tmp/testdir_lifecycle";
    std::filesystem::remove_all(cacheRoot);
    for (int i = 0; i < 3; ++i) {
        std::filesystem::create_directories(cacheRoot + "/" + std::to_string(i));
    }
    SetRootPath(cacheRoot);
    SetTotalDirectory(3);

    DiskCache cache;

    uint64_t key = 301;
    std::string file = GetFilePath(key);
    {
        std::ofstream out(file);
        out << "cache-data";
    }

    EXPECT_FALSE(cache.Find(key, false));
    cache.InsertAndUpdate(key, 10, false);
    EXPECT_TRUE(cache.Find(key, false));
    EXPECT_TRUE(cache.Find(key, true));
    cache.Unpin(key);

    EXPECT_TRUE(cache.Update(key, 20));
    EXPECT_TRUE(cache.Add(key, 5));
    EXPECT_FALSE(cache.Update(999999, 1));
    EXPECT_FALSE(cache.Add(999999, 1));

    EXPECT_EQ(cache.Delete(key), 0);
    EXPECT_FALSE(cache.Find(key, false));
    EXPECT_FALSE(std::filesystem::exists(file));

    std::filesystem::remove_all(cacheRoot);
}

TEST_F(DiskCacheUT, DeleteOldCacheSkipsPinnedEntry)
{
    std::string cacheRoot = "/tmp/testdir_delete_old";
    std::filesystem::remove_all(cacheRoot);
    for (int i = 0; i < 2; ++i) {
        std::filesystem::create_directories(cacheRoot + "/" + std::to_string(i));
    }
    SetRootPath(cacheRoot);
    SetTotalDirectory(2);

    DiskCache cache;

    uint64_t pinnedKey = 42;
    uint64_t unpinnedKey = 43;
    std::string pinnedFile = GetFilePath(pinnedKey);
    std::string unpinnedFile = GetFilePath(unpinnedKey);
    {
        std::ofstream out(pinnedFile);
        out << "pinned";
    }
    {
        std::ofstream out(unpinnedFile);
        out << "unpinned";
    }

    cache.InsertAndUpdate(pinnedKey, 6, true);
    cache.InsertAndUpdate(unpinnedKey, 8, false);
    cache.DeleteOldCacheWithNoPin(pinnedKey);
    EXPECT_TRUE(cache.Find(pinnedKey, false));
    EXPECT_TRUE(std::filesystem::exists(pinnedFile));

    cache.DeleteOldCacheWithNoPin(unpinnedKey);
    EXPECT_FALSE(cache.Find(unpinnedKey, false));
    EXPECT_FALSE(std::filesystem::exists(unpinnedFile));

    cache.Unpin(pinnedKey);
    EXPECT_EQ(cache.Delete(pinnedKey), 0);
    std::filesystem::remove_all(cacheRoot);
}

TEST_F(DiskCacheUT, PreAllocSpaceReservesAndReleasesCapacity)
{
    DiskCache cache;
    cache.freeCap.store(1024);
    cache.reservedCap.store(0);

    EXPECT_TRUE(cache.PreAllocSpace(256));
    EXPECT_EQ(cache.reservedCap.load(), 256U);
    EXPECT_TRUE(cache.HasFreeSpace());

    cache.FreePreAllocSpace(128);
    EXPECT_EQ(cache.reservedCap.load(), 128U);
}

TEST_F(DiskCacheUT, CleanupEvictsUnpinnedFilesAndKeepsPinnedFiles)
{
    std::string cacheRoot = "/tmp/testdir_cleanup";
    std::filesystem::remove_all(cacheRoot);
    for (int i = 0; i < 2; ++i) {
        std::filesystem::create_directories(cacheRoot + "/" + std::to_string(i));
    }
    SetRootPath(cacheRoot);
    SetTotalDirectory(2);

    DiskCache cache;
    cache.rootDir = cacheRoot;
    cache.totalCap = 100;
    cache.totalInodes = 100;
    cache.freeCap.store(10);
    cache.freeInodes = 10;
    cache.blockRatio = 0.1;
    cache.inodeRatio = 0.1;
    cache.bgFreeRatio = 0.5;

    uint64_t pinnedKey = 200;
    uint64_t evictKey = 201;
    std::string pinnedFile = GetFilePath(pinnedKey);
    std::string evictFile = GetFilePath(evictKey);
    {
        std::ofstream out(pinnedFile);
        out << "pinned";
    }
    {
        std::ofstream out(evictFile);
        out << "evict";
    }

    cache.InsertAndUpdate(pinnedKey, 20, true);
    cache.InsertAndUpdate(evictKey, 20, false);
    cache.Cleanup();

    EXPECT_TRUE(cache.Find(pinnedKey, false));
    EXPECT_TRUE(std::filesystem::exists(pinnedFile));
    EXPECT_FALSE(cache.Find(evictKey, false));
    EXPECT_FALSE(std::filesystem::exists(evictFile));

    cache.Unpin(pinnedKey);
    EXPECT_EQ(cache.Delete(pinnedKey), 0);
    std::filesystem::remove_all(cacheRoot);
}

TEST_F(DiskCacheUT, StartScansExistingCacheFiles)
{
    std::string cacheRoot = "/tmp/testdir_scan";
    std::filesystem::remove_all(cacheRoot);
    for (int i = 0; i < 2; ++i) {
        std::filesystem::create_directories(cacheRoot + "/" + std::to_string(i));
    }
    SetRootPath(cacheRoot);
    SetTotalDirectory(2);

    uint64_t key = 302;
    std::string file = GetFilePath(key);
    {
        std::ofstream out(file);
        out << "existing-cache";
    }

    DiskCache cache;
    EXPECT_EQ(cache.Start(cacheRoot, 2, 0.000001, 0.000001), 0);
    EXPECT_TRUE(cache.Find(key, false));
    EXPECT_EQ(cache.Delete(key), 0);
    std::filesystem::remove_all(cacheRoot);
}

TEST_F(DiskCacheUT, ConstructorWalkAndSpaceFailureBranches)
{
    DiskCache ratioCache(0.7);
    EXPECT_FLOAT_EQ(ratioCache.freeRatio, 0.7F);

    EXPECT_EQ(DiskCache::Walk("/tmp/falconfs_missing_disk_cache_dir"), RETURN_ERROR);

    DiskCache cache;
    cache.totalCap = 100;
    cache.freeCap.store(10);
    cache.usedCap = 0;
    cache.totalInodes = 100;
    cache.freeInodes = 10;
    cache.bgFreeRatio = 0.2;
    cache.freeRatio = 0.2;
    EXPECT_EQ(cache.CheckSpaceEnough(), RETURN_ERROR);
}

TEST_F(DiskCacheUT, DeleteFailureAndStopModeBranches)
{
    std::string cacheRoot = "/tmp/testdir_delete_failure";
    std::filesystem::remove_all(cacheRoot);
    std::filesystem::create_directories(cacheRoot + "/0");
    SetRootPath(cacheRoot);
    SetTotalDirectory(1);

    DiskCache cache;
    cache.rootDir = cacheRoot;
    cache.totalDirNum = 1;
    cache.stop = false;
    cache.freeCap.store(1000);
    cache.InsertAndUpdate(500, 10, false);
    EXPECT_LT(cache.Delete(500), 0);

    cache.InsertAndUpdate(501, 10, false);
    cache.DeleteOldCacheWithNoPin(501);
    EXPECT_TRUE(cache.inodeToCacheIter.find(501) != cache.inodeToCacheIter.end());

    std::string directFile = GetFilePath(502);
    {
        std::ofstream out(directFile);
        out << "direct";
    }
    cache.stop = true;
    EXPECT_TRUE(cache.Find(502, false));
    EXPECT_EQ(cache.Delete(502), 0);
    EXPECT_FALSE(cache.Find(502, false));

    std::filesystem::remove_all(cacheRoot);
}

TEST_F(DiskCacheUT, CleanupForEvictAndPreAllocFailureBranches)
{
    std::string cacheRoot = "/tmp/testdir_cleanup_for_evict";
    std::filesystem::remove_all(cacheRoot);
    std::filesystem::create_directories(cacheRoot + "/0");
    SetRootPath(cacheRoot);
    SetTotalDirectory(1);

    DiskCache cache;
    cache.rootDir = cacheRoot;
    cache.totalDirNum = 1;
    cache.stop = false;
    cache.totalCap = 100;
    cache.totalInodes = 100;
    cache.freeCap.store(1);
    cache.freeInodes = 1;
    cache.blockRatio = 0.01;
    cache.inodeRatio = 0.01;
    cache.freeRatio = 0.5;
    cache.bgFreeRatio = 0.5;
    cache.hasFreeSpace.store(true);

    uint64_t pinnedKey = 601;
    uint64_t missingKey = 602;
    std::string pinnedFile = GetFilePath(pinnedKey);
    {
        std::ofstream out(pinnedFile);
        out << "pinned";
    }
    cache.InsertAndUpdate(pinnedKey, 20, true);
    cache.InsertAndUpdate(missingKey, 20, false);

    cache.CleanupForEvict(10);
    EXPECT_TRUE(cache.Find(pinnedKey, false));
    EXPECT_TRUE(cache.inodeToCacheIter.find(missingKey) != cache.inodeToCacheIter.end());

    cache.rootDir = "/tmp/testdir_cleanup_for_evict_missing_root";
    cache.freeCap.store(1);
    cache.reservedCap.store(0);
    EXPECT_FALSE(cache.PreAllocSpace(1000));
    EXPECT_FALSE(cache.HasFreeSpace());

    cache.Unpin(pinnedKey);
    cache.Delete(pinnedKey);
    std::filesystem::remove_all(cacheRoot);
}

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
