#include <fstream>

#include "test_disk_cache.h"
#include "disk_cache/disk_cache.h"
#include "util/utils.h"

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

TEST_F(DiskCacheUT, ZeroRatioStopModeCoversNoopBranches)
{
    std::string cacheRoot = "/tmp/testdir_zero_stop";
    std::filesystem::remove_all(cacheRoot);
    std::filesystem::create_directories(cacheRoot + "/0");
    SetRootPath(cacheRoot);
    SetTotalDirectory(1);

    DiskCache cache;
    ASSERT_EQ(cache.Start(cacheRoot, 1, 0.0, 0.0), 0);

    uint64_t key = 404;
    std::string file = GetFilePath(key);
    {
        std::ofstream out(file);
        out << "stop-mode";
    }

    EXPECT_TRUE(cache.Find(key, true));
    cache.InsertAndUpdate(key, 9, true);
    EXPECT_TRUE(cache.Update(key, 10));
    EXPECT_TRUE(cache.Add(key, 1));
    cache.Pin(key);
    cache.Unpin(key);
    EXPECT_TRUE(cache.PreAllocSpace(1024));
    cache.FreePreAllocSpace(1024);
    EXPECT_TRUE(cache.HasFreeSpace());
    EXPECT_EQ(cache.Delete(key), 0);
    EXPECT_EQ(cache.Delete(key), -1);

    std::filesystem::remove_all(cacheRoot);
}

TEST_F(DiskCacheUT, UtilityEnvironmentBranches)
{
    SetRootPath("/tmp/util_root");
    SetTotalDirectory(8);
    EXPECT_EQ(GetFilePath(17), "/tmp/util_root/1/17-large");

    int randomValue = GenerateRandom(1, 3);
    EXPECT_GE(randomValue, 1);
    EXPECT_LE(randomValue, 3);

    setenv("USER", "falcon_user", 1);
    ASSERT_TRUE(GetUserName().has_value());
    EXPECT_EQ(GetUserName().value(), "falcon_user");
    unsetenv("USER");
    EXPECT_FALSE(GetUserName().has_value());

    EXPECT_EQ(SplitIp("10.0.0.1:56039").value(), "10.0.0.1");
    EXPECT_EQ(SplitIp("10.0.0.1").value(), "10.0.0.1");

    unsetenv("POD_IP");
    unsetenv("BRPC_PORT");
    EXPECT_EQ(GetPodIPPort(), "127.0.0.1:56039");
    setenv("POD_IP", "10.1.1.1", 1);
    EXPECT_EQ(GetPodIPPort(), "10.1.1.1:56039");
    setenv("BRPC_PORT", "56100", 1);
    EXPECT_EQ(GetPodIPPort(), "10.1.1.1:56100");
    unsetenv("POD_IP");
    unsetenv("BRPC_PORT");

    unsetenv("STORAGE_THRESHOLD");
    EXPECT_FLOAT_EQ(GetStorageThreshold(true), 0.8F);
    EXPECT_FLOAT_EQ(GetStorageThreshold(false), 1.0F);
    setenv("STORAGE_THRESHOLD", "0.42", 1);
    EXPECT_FLOAT_EQ(GetStorageThreshold(true), 0.42F);
    unsetenv("STORAGE_THRESHOLD");

    unsetenv("PARENT_PATH_LEVEL");
    EXPECT_EQ(GetParentPathLevel(), -1);
    setenv("PARENT_PATH_LEVEL", "3", 1);
    EXPECT_EQ(GetParentPathLevel(), 3);
    unsetenv("PARENT_PATH_LEVEL");
}

TEST_F(DiskCacheUT, PublicEvictAndFailureBranches)
{
    std::string cacheRoot = "/tmp/testdir_public_evict";
    std::filesystem::remove_all(cacheRoot);
    std::filesystem::create_directories(cacheRoot + "/0");
    SetRootPath(cacheRoot);
    SetTotalDirectory(1);

    DiskCache cache(0.4);
    EXPECT_EQ(cache.Start(cacheRoot, 1, 2.0, 2.0), RETURN_ERROR);

    uint64_t pinnedKey = 501;
    uint64_t removableKey = 502;
    uint64_t missingFileKey = 503;
    {
        std::ofstream out(GetFilePath(pinnedKey));
        out << "pinned";
    }
    {
        std::ofstream out(GetFilePath(removableKey));
        out << "removable";
    }
    cache.InsertAndUpdate(pinnedKey, 10, true);
    cache.InsertAndUpdate(removableKey, 10, false);
    cache.InsertAndUpdate(missingFileKey, 10, false);
    cache.InsertAndUpdate(missingFileKey, 20, false);

    cache.Evict(UINT64_MAX / 4);
    EXPECT_TRUE(cache.Find(pinnedKey, false));
    EXPECT_FALSE(cache.Find(removableKey, false));
    EXPECT_FALSE(std::filesystem::exists(GetFilePath(removableKey)));

    std::string missingRoot = "/tmp/testdir_public_evict_start_missing";
    std::filesystem::remove_all(missingRoot);
    DiskCache startFailureCache;
    EXPECT_EQ(startFailureCache.Start(missingRoot, 1, 0.1, 0.1), RETURN_ERROR);

    uint64_t deleteMissingKey = 504;
    cache.InsertAndUpdate(deleteMissingKey, 1, false);
    EXPECT_LT(cache.Delete(deleteMissingKey), 0);

    uint64_t oldMissingKey = 505;
    cache.InsertAndUpdate(oldMissingKey, 1, false);
    cache.DeleteOldCacheWithNoPin(oldMissingKey);
    EXPECT_TRUE(cache.Find(oldMissingKey, false));

    EXPECT_FALSE(cache.PreAllocSpace(UINT64_MAX / 4));
    EXPECT_FALSE(cache.HasFreeSpace());

    cache.Unpin(pinnedKey);
    EXPECT_EQ(cache.Delete(pinnedKey), 0);

    std::filesystem::remove_all(cacheRoot);
}

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
