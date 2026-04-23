#include "metadata_ut_client.h"

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <gtest/gtest.h>
#include <unistd.h>

namespace {

class FalconMetadataUT : public testing::Test {
  protected:
    struct SliceIndex {
        std::string filename;
        uint64_t inodeId;
        uint32_t chunkId;
    };

    static void SetUpTestSuite()
    {
        const char *serverIp = std::getenv("SERVER_IP");
        const char *serverPort = std::getenv("SERVER_PORT");
        if (serverIp == nullptr || serverPort == nullptr) {
            initError_ = "SERVER_IP or SERVER_PORT is not set";
            return;
        }

        int clientNumber = 4;
        if (const char *clientNumberEnv = std::getenv("FALCON_METADATA_UT_CLIENT_NUM")) {
            clientNumber = std::max(1, std::atoi(clientNumberEnv));
        }

        client_ = std::make_unique<MetadataUtClient>();
        if (!client_->Init(serverIp, std::atoi(serverPort), clientNumber, &initError_)) {
            client_.reset();
        }
    }

    static void TearDownTestSuite()
    {
        if (client_) {
            client_->Shutdown();
            client_.reset();
        }
    }

    void SetUp() override
    {
        if (!client_) {
            GTEST_SKIP() << "metadata UT client is not initialized: " << initError_;
        }

        root_ = BuildUniqueRoot();
        ASSERT_EQ(SUCCESS, client_->Mkdir(root_)) << "root=" << root_;
    }

    void TearDown() override
    {
        if (!client_) {
            return;
        }

        for (auto it = sliceIndexes_.rbegin(); it != sliceIndexes_.rend(); ++it) {
            client_->SliceDel(it->filename, it->inodeId, it->chunkId);
        }
        for (auto it = kvKeys_.rbegin(); it != kvKeys_.rend(); ++it) {
            client_->KvDel(*it);
        }
        for (auto it = files_.rbegin(); it != files_.rend(); ++it) {
            client_->Unlink(*it);
        }
        for (auto it = dirs_.rbegin(); it != dirs_.rend(); ++it) {
            client_->Rmdir(*it);
        }
        if (!root_.empty()) {
            client_->Rmdir(root_);
        }
    }

    static std::string Sanitize(const std::string &name)
    {
        std::string result = name;
        std::replace_if(result.begin(), result.end(), [](char ch) {
            unsigned char uch = static_cast<unsigned char>(ch);
            return !(std::isalnum(uch) || ch == '_');
        }, '_');
        return result;
    }

    std::string BuildUniqueRoot()
    {
        const auto *testInfo = testing::UnitTest::GetInstance()->current_test_info();
        uint64_t seq = rootSeq_.fetch_add(1, std::memory_order_relaxed);
        auto now = std::chrono::steady_clock::now().time_since_epoch().count();
        return "/metadata_ut_" + std::to_string(getpid()) + "_" + std::to_string(now) + "_" +
               std::to_string(seq) + "_" + Sanitize(testInfo->name());
    }

    void TrackFile(const std::string &path)
    {
        files_.push_back(path);
    }

    void TrackDir(const std::string &path)
    {
        dirs_.push_back(path);
    }

    void TrackKvKey(const std::string &key)
    {
        kvKeys_.push_back(key);
    }

    void TrackSlice(const std::string &filename, uint64_t inodeId, uint32_t chunkId)
    {
        sliceIndexes_.push_back({filename, inodeId, chunkId});
    }

    static void WaitForStart(const std::atomic<bool> &start)
    {
        while (!start.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
    }

    template <typename Fn>
    static std::pair<int, int> RunConcurrent(int threadNum, Fn fn)
    {
        std::atomic<bool> start = false;
        std::atomic<int> success = 0;
        std::atomic<int> failure = 0;
        std::vector<std::thread> threads;
        threads.reserve(threadNum);
        for (int i = 0; i < threadNum; ++i) {
            threads.emplace_back([&, i]() {
                WaitForStart(start);
                FalconErrorCode ret = fn(i);
                if (ret == SUCCESS) {
                    success.fetch_add(1, std::memory_order_relaxed);
                } else {
                    failure.fetch_add(1, std::memory_order_relaxed);
                }
            });
        }
        start.store(true, std::memory_order_release);
        for (auto &thread : threads) {
            thread.join();
        }
        return {success.load(), failure.load()};
    }

    static std::vector<uint64_t> KvValueKeys(uint64_t base)
    {
        return {base, base + 1};
    }

    static std::vector<uint64_t> KvLocations()
    {
        return {0, 2048};
    }

    static std::vector<uint32_t> KvSizes()
    {
        return {2048, 2048};
    }

    static std::set<std::string> ToSet(const std::vector<std::string> &entries)
    {
        return std::set<std::string>(entries.begin(), entries.end());
    }

    static void ExpectMissingOrEmptySlice(FalconErrorCode ret, const MetadataUtClient::SliceValue &value)
    {
        if (ret == SUCCESS) {
            EXPECT_EQ(0U, value.sliceNum);
        }
    }

    static std::unique_ptr<MetadataUtClient> client_;
    static std::string initError_;
    static std::atomic<uint64_t> rootSeq_;

    std::string root_;
    std::vector<std::string> files_;
    std::vector<std::string> dirs_;
    std::vector<std::string> kvKeys_;
    std::vector<SliceIndex> sliceIndexes_;
};

std::unique_ptr<MetadataUtClient> FalconMetadataUT::client_;
std::string FalconMetadataUT::initError_;
std::atomic<uint64_t> FalconMetadataUT::rootSeq_{0};

TEST_F(FalconMetadataUT, FileCreateStatOpenCloseUnlink)
{
    const std::string file = root_ + "/file_0";
    TrackFile(file);

    uint64_t createInodeId = 0;
    int32_t createNodeId = 0;
    ASSERT_EQ(SUCCESS, client_->Create(file, &createInodeId, &createNodeId));
    EXPECT_NE(0U, createInodeId);

    struct stat stbuf = {};
    ASSERT_EQ(SUCCESS, client_->Stat(file, &stbuf));

    uint64_t openInodeId = 0;
    int64_t size = 0;
    int32_t openNodeId = 0;
    struct stat openStat = {};
    ASSERT_EQ(SUCCESS, client_->Open(file, &openInodeId, &size, &openNodeId, &openStat));
    EXPECT_EQ(createInodeId, openInodeId);
    EXPECT_EQ(SUCCESS, client_->Close(file, size, 0, openNodeId));

    ASSERT_EQ(SUCCESS, client_->Unlink(file));
    EXPECT_NE(SUCCESS, client_->Stat(file, &stbuf));
}

TEST_F(FalconMetadataUT, DirectoryP0LifecycleAndErrors)
{
    uint64_t rootInode = 0;
    ASSERT_EQ(SUCCESS, client_->OpenDir(root_, &rootInode));
    EXPECT_NE(0U, rootInode);

    const std::string dupDir = root_ + "/dup_dir";
    TrackDir(dupDir);
    ASSERT_EQ(SUCCESS, client_->Mkdir(dupDir));
    EXPECT_NE(SUCCESS, client_->Mkdir(dupDir));

    const std::string missingParentDir = root_ + "/missing/subdir";
    EXPECT_NE(SUCCESS, client_->Mkdir(missingParentDir));
    struct stat stbuf = {};
    EXPECT_NE(SUCCESS, client_->Stat(missingParentDir, &stbuf));

    const std::string file = root_ + "/file_for_opendir";
    TrackFile(file);
    ASSERT_EQ(SUCCESS, client_->Create(file));
    uint64_t ignoredInode = 0;
    EXPECT_NE(SUCCESS, client_->OpenDir(file, &ignoredInode));

    const std::string readDirRoot = root_ + "/readdir_root";
    const std::string dirA = readDirRoot + "/dir_a";
    const std::string dirB = readDirRoot + "/dir_b";
    const std::string fileA = readDirRoot + "/file_a";
    const std::string fileB = readDirRoot + "/file_b";
    TrackDir(readDirRoot);
    TrackDir(dirA);
    TrackDir(dirB);
    TrackFile(fileA);
    TrackFile(fileB);
    ASSERT_EQ(SUCCESS, client_->Mkdir(readDirRoot));
    ASSERT_EQ(SUCCESS, client_->Mkdir(dirA));
    ASSERT_EQ(SUCCESS, client_->Mkdir(dirB));
    ASSERT_EQ(SUCCESS, client_->Create(fileA));
    ASSERT_EQ(SUCCESS, client_->Create(fileB));

    std::vector<std::string> entries;
    ASSERT_EQ(SUCCESS, client_->ReadDir(readDirRoot, &entries));
    std::set<std::string> entrySet(entries.begin(), entries.end());
    EXPECT_TRUE(entrySet.contains("dir_a"));
    EXPECT_TRUE(entrySet.contains("dir_b"));
    EXPECT_TRUE(entrySet.contains("file_a"));
    EXPECT_TRUE(entrySet.contains("file_b"));

    EXPECT_NE(SUCCESS, client_->Rmdir(readDirRoot));
    entries.clear();
    ASSERT_EQ(SUCCESS, client_->ReadDir(readDirRoot, &entries));
    entrySet = std::set<std::string>(entries.begin(), entries.end());
    EXPECT_TRUE(entrySet.contains("dir_a"));
    EXPECT_TRUE(entrySet.contains("file_a"));

    ASSERT_EQ(SUCCESS, client_->Rmdir(dirA));
    ASSERT_EQ(SUCCESS, client_->Rmdir(dirB));
    ASSERT_EQ(SUCCESS, client_->Unlink(fileA));
    ASSERT_EQ(SUCCESS, client_->Unlink(fileB));
    ASSERT_EQ(SUCCESS, client_->Rmdir(readDirRoot));
}

TEST_F(FalconMetadataUT, DirectoryRejectsNonEmptyRmdir)
{
    const std::string childDir = root_ + "/child_dir";
    TrackDir(childDir);

    ASSERT_EQ(SUCCESS, client_->Mkdir(childDir));
    EXPECT_NE(SUCCESS, client_->Rmdir(root_));
    ASSERT_EQ(SUCCESS, client_->Rmdir(childDir));
    dirs_.clear();
}

TEST_F(FalconMetadataUT, ReadDirEmptyMissingPaginationAndDeleteVisibility)
{
    const std::string emptyDir = root_ + "/empty_readdir";
    TrackDir(emptyDir);
    ASSERT_EQ(SUCCESS, client_->Mkdir(emptyDir));

    std::vector<std::string> entries;
    ASSERT_EQ(SUCCESS, client_->ReadDir(emptyDir, &entries));
    EXPECT_TRUE(entries.empty());
    EXPECT_NE(SUCCESS, client_->ReadDir(root_ + "/missing_readdir", &entries));

    const std::string pageDir = root_ + "/paged_readdir";
    TrackDir(pageDir);
    ASSERT_EQ(SUCCESS, client_->Mkdir(pageDir));
    constexpr int kEntryCount = 1030;
    for (int i = 0; i < kEntryCount; ++i) {
        const std::string file = pageDir + "/file_" + std::to_string(i);
        TrackFile(file);
        ASSERT_EQ(SUCCESS, client_->Create(file));
    }

    entries.clear();
    ASSERT_EQ(SUCCESS, client_->ReadDir(pageDir, &entries));
    std::set<std::string> entrySet = ToSet(entries);
    EXPECT_EQ(static_cast<size_t>(kEntryCount), entrySet.size());
    for (int i = 0; i < kEntryCount; ++i) {
        EXPECT_TRUE(entrySet.contains("file_" + std::to_string(i)));
    }

    ASSERT_EQ(SUCCESS, client_->Unlink(pageDir + "/file_17"));
    entries.clear();
    ASSERT_EQ(SUCCESS, client_->ReadDir(pageDir, &entries));
    entrySet = ToSet(entries);
    EXPECT_FALSE(entrySet.contains("file_17"));
}

TEST_F(FalconMetadataUT, ConcurrentReadDirWhileCreating)
{
    constexpr int kFileCount = 48;
    constexpr int kReaderCount = 4;
    const std::string dir = root_ + "/concurrent_readdir";
    TrackDir(dir);
    ASSERT_EQ(SUCCESS, client_->Mkdir(dir));
    for (int i = 0; i < kFileCount; ++i) {
        TrackFile(dir + "/file_" + std::to_string(i));
    }

    std::atomic<bool> start = false;
    std::atomic<bool> done = false;
    std::atomic<int> readSuccess = 0;
    std::atomic<int> readFailure = 0;

    std::thread creator([&]() {
        WaitForStart(start);
        for (int i = 0; i < kFileCount; ++i) {
            ASSERT_EQ(SUCCESS, client_->Create(dir + "/file_" + std::to_string(i)));
        }
        done.store(true, std::memory_order_release);
    });

    std::vector<std::thread> readers;
    readers.reserve(kReaderCount);
    for (int i = 0; i < kReaderCount; ++i) {
        readers.emplace_back([&]() {
            WaitForStart(start);
            while (!done.load(std::memory_order_acquire)) {
                std::vector<std::string> entries;
                FalconErrorCode ret = client_->ReadDir(dir, &entries);
                if (ret == SUCCESS) {
                    readSuccess.fetch_add(1, std::memory_order_relaxed);
                } else {
                    readFailure.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    start.store(true, std::memory_order_release);
    creator.join();
    for (auto &reader : readers) {
        reader.join();
    }

    std::vector<std::string> entries;
    ASSERT_EQ(SUCCESS, client_->ReadDir(dir, &entries));
    EXPECT_EQ(static_cast<size_t>(kFileCount), ToSet(entries).size());
    EXPECT_GT(readSuccess.load(), 0);
    EXPECT_EQ(0, readFailure.load());
}

TEST_F(FalconMetadataUT, ConcurrentCreateSameDirectoryAndFile)
{
    constexpr int kThreadNum = 16;

    const std::string dir = root_ + "/same_dir";
    TrackDir(dir);
    auto [dirSuccess, dirFailure] = RunConcurrent(kThreadNum, [&](int) { return client_->Mkdir(dir); });
    EXPECT_EQ(1, dirSuccess);
    EXPECT_EQ(kThreadNum - 1, dirFailure);
    uint64_t dirInode = 0;
    EXPECT_EQ(SUCCESS, client_->OpenDir(dir, &dirInode));

    const std::string file = root_ + "/same_file";
    TrackFile(file);
    auto [fileSuccess, fileFailure] = RunConcurrent(kThreadNum, [&](int) { return client_->Create(file); });
    EXPECT_EQ(1, fileSuccess);
    EXPECT_EQ(kThreadNum - 1, fileFailure);
    struct stat stbuf = {};
    EXPECT_EQ(SUCCESS, client_->Stat(file, &stbuf));
}

TEST_F(FalconMetadataUT, FileP0ErrorsAndDeepPathLifecycle)
{
    const std::string dupFile = root_ + "/dup_file";
    TrackFile(dupFile);
    ASSERT_EQ(SUCCESS, client_->Create(dupFile));
    EXPECT_NE(SUCCESS, client_->Create(dupFile));
    struct stat stbuf = {};
    EXPECT_EQ(SUCCESS, client_->Stat(dupFile, &stbuf));

    const std::string missingParentFile = root_ + "/missing_parent/file";
    EXPECT_NE(SUCCESS, client_->Create(missingParentFile));
    EXPECT_NE(SUCCESS, client_->Stat(missingParentFile, &stbuf));

    const std::string dir = root_ + "/open_dir";
    TrackDir(dir);
    ASSERT_EQ(SUCCESS, client_->Mkdir(dir));
    uint64_t inodeId = 0;
    int64_t size = 0;
    int32_t nodeId = 0;
    /*
     * Current metadata Open accepts a directory path and returns its metadata.
     * Keep this as a current-behavior assertion; if directory-open protection is
     * added later, this should move to an explicit negative test.
     */
    EXPECT_EQ(SUCCESS, client_->Open(dir, &inodeId, &size, &nodeId, &stbuf));

    const std::string deletedFile = root_ + "/delete_target";
    TrackFile(deletedFile);
    ASSERT_EQ(SUCCESS, client_->Create(deletedFile));
    ASSERT_EQ(SUCCESS, client_->Unlink(deletedFile));
    EXPECT_NE(SUCCESS, client_->Stat(deletedFile, &stbuf));
    EXPECT_NE(SUCCESS, client_->Open(deletedFile, &inodeId, &size, &nodeId, &stbuf));

    const std::vector<std::string> deepDirs = {
        root_ + "/a",
        root_ + "/a/b",
        root_ + "/a/b/c",
        root_ + "/a/b/c/d",
        root_ + "/a/b/c/d/e",
    };
    for (const auto &deepDir : deepDirs) {
        TrackDir(deepDir);
        ASSERT_EQ(SUCCESS, client_->Mkdir(deepDir));
    }
    const std::string deepFile = root_ + "/a/b/c/d/e/file_deep";
    TrackFile(deepFile);
    ASSERT_EQ(SUCCESS, client_->Create(deepFile));
    ASSERT_EQ(SUCCESS, client_->Stat(deepFile, &stbuf));
    ASSERT_EQ(SUCCESS, client_->Open(deepFile, &inodeId, &size, &nodeId, &stbuf));
    ASSERT_EQ(SUCCESS, client_->Close(deepFile, size, 0, nodeId));
    ASSERT_EQ(SUCCESS, client_->Unlink(deepFile));
}

TEST_F(FalconMetadataUT, CloseUpdatesSizeAndUnlinkRecreateLifecycle)
{
    struct stat stbuf = {};
    EXPECT_NE(SUCCESS, client_->Unlink(root_ + "/missing_unlink"));

    const std::string file = root_ + "/close_size";
    TrackFile(file);
    ASSERT_EQ(SUCCESS, client_->Create(file));

    uint64_t inodeId = 0;
    int64_t size = 0;
    int32_t nodeId = 0;
    ASSERT_EQ(SUCCESS, client_->Open(file, &inodeId, &size, &nodeId, &stbuf));
    ASSERT_EQ(SUCCESS, client_->Close(file, 12345, 0, nodeId));
    ASSERT_EQ(SUCCESS, client_->Stat(file, &stbuf));
    EXPECT_EQ(12345, stbuf.st_size);

    ASSERT_EQ(SUCCESS, client_->Unlink(file));
    EXPECT_NE(SUCCESS, client_->Stat(file, &stbuf));
    ASSERT_EQ(SUCCESS, client_->Create(file, &inodeId, &nodeId));
    ASSERT_EQ(SUCCESS, client_->Stat(file, &stbuf));
    EXPECT_EQ(0, stbuf.st_size);
}

TEST_F(FalconMetadataUT, CloseUsesCurrentPathAfterRename)
{
    const std::string src = root_ + "/close_rename_src";
    const std::string dst = root_ + "/close_rename_dst";
    TrackFile(src);
    TrackFile(dst);
    ASSERT_EQ(SUCCESS, client_->Create(src));

    uint64_t inodeId = 0;
    int64_t size = 0;
    int32_t nodeId = 0;
    struct stat stbuf = {};
    ASSERT_EQ(SUCCESS, client_->Open(src, &inodeId, &size, &nodeId, &stbuf));
    ASSERT_EQ(SUCCESS, client_->Rename(src, dst));

    EXPECT_NE(SUCCESS, client_->Close(src, 777, 0, nodeId));
    ASSERT_EQ(SUCCESS, client_->Close(dst, 888, 0, nodeId));
    ASSERT_EQ(SUCCESS, client_->Stat(dst, &stbuf));
    EXPECT_EQ(888, stbuf.st_size);
    EXPECT_NE(SUCCESS, client_->Stat(src, &stbuf));
}

TEST_F(FalconMetadataUT, PathValidationRejectsMalformedInputs)
{
    struct stat stbuf = {};
    EXPECT_NE(SUCCESS, client_->Create(""));
    EXPECT_NE(SUCCESS, client_->Mkdir(""));
    EXPECT_NE(SUCCESS, client_->Stat("", &stbuf));
    EXPECT_NE(SUCCESS, client_->Create("relative_file"));
    EXPECT_NE(SUCCESS, client_->Mkdir("relative_dir"));
    EXPECT_NE(SUCCESS, client_->Create(root_ + "/trailing_slash/"));
}

TEST_F(FalconMetadataUT, ConcurrentOpenAndUnlinkKeepsConsistentFinalState)
{
    constexpr int kOpenThreads = 8;
    constexpr int kOpenIterations = 20;

    const std::string file = root_ + "/open_unlink_target";
    TrackFile(file);
    ASSERT_EQ(SUCCESS, client_->Create(file));

    std::atomic<bool> start = false;
    std::atomic<bool> done = false;
    std::atomic<int> openSuccess = 0;
    std::atomic<int> openFailure = 0;
    std::atomic<int> closeAttempt = 0;

    std::vector<std::thread> openers;
    openers.reserve(kOpenThreads);
    for (int i = 0; i < kOpenThreads; ++i) {
        openers.emplace_back([&]() {
            WaitForStart(start);
            for (int iter = 0; iter < kOpenIterations && !done.load(std::memory_order_acquire); ++iter) {
                uint64_t inodeId = 0;
                int64_t size = 0;
                int32_t nodeId = 0;
                struct stat stbuf = {};
                FalconErrorCode ret = client_->Open(file, &inodeId, &size, &nodeId, &stbuf);
                if (ret == SUCCESS) {
                    openSuccess.fetch_add(1, std::memory_order_relaxed);
                    client_->Close(file, size, 0, nodeId);
                    closeAttempt.fetch_add(1, std::memory_order_relaxed);
                } else {
                    openFailure.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    std::thread unlinker([&]() {
        WaitForStart(start);
        EXPECT_EQ(SUCCESS, client_->Unlink(file));
        done.store(true, std::memory_order_release);
    });

    start.store(true, std::memory_order_release);
    unlinker.join();
    for (auto &thread : openers) {
        thread.join();
    }

    struct stat stbuf = {};
    EXPECT_NE(SUCCESS, client_->Stat(file, &stbuf));
    EXPECT_GT(openSuccess.load() + openFailure.load(), 0);
    EXPECT_EQ(openSuccess.load(), closeAttempt.load());
}

TEST_F(FalconMetadataUT, ConcurrentUnlinkAndStatKeepsSingleFinalState)
{
    constexpr int kStatThreads = 8;
    constexpr int kIterations = 50;
    const std::string file = root_ + "/unlink_stat_target";
    TrackFile(file);
    ASSERT_EQ(SUCCESS, client_->Create(file));

    std::atomic<bool> start = false;
    std::atomic<bool> done = false;
    std::atomic<int> statSuccess = 0;
    std::atomic<int> statFailure = 0;

    std::vector<std::thread> staters;
    staters.reserve(kStatThreads);
    for (int i = 0; i < kStatThreads; ++i) {
        staters.emplace_back([&]() {
            WaitForStart(start);
            for (int iter = 0; iter < kIterations && !done.load(std::memory_order_acquire); ++iter) {
                struct stat stbuf = {};
                FalconErrorCode ret = client_->Stat(file, &stbuf);
                if (ret == SUCCESS) {
                    statSuccess.fetch_add(1, std::memory_order_relaxed);
                } else {
                    statFailure.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    std::thread unlinker([&]() {
        WaitForStart(start);
        EXPECT_EQ(SUCCESS, client_->Unlink(file));
        done.store(true, std::memory_order_release);
    });

    start.store(true, std::memory_order_release);
    unlinker.join();
    for (auto &thread : staters) {
        thread.join();
    }

    struct stat stbuf = {};
    EXPECT_NE(SUCCESS, client_->Stat(file, &stbuf));
    EXPECT_GT(statSuccess.load() + statFailure.load(), 0);
}

TEST_F(FalconMetadataUT, NonEmptyDirectoryRenamePreservesChildren)
{
    const std::string srcDir = root_ + "/nonempty_src";
    const std::string dstDir = root_ + "/nonempty_dst";
    const std::string srcFile = srcDir + "/child";
    const std::string dstFile = dstDir + "/child";
    TrackDir(srcDir);
    TrackDir(dstDir);
    TrackFile(srcFile);
    TrackFile(dstFile);
    ASSERT_EQ(SUCCESS, client_->Mkdir(srcDir));
    ASSERT_EQ(SUCCESS, client_->Create(srcFile));

    ASSERT_EQ(SUCCESS, client_->Rename(srcDir, dstDir));
    uint64_t dirInode = 0;
    EXPECT_NE(SUCCESS, client_->OpenDir(srcDir, &dirInode));
    EXPECT_EQ(SUCCESS, client_->OpenDir(dstDir, &dirInode));
    struct stat stbuf = {};
    EXPECT_NE(SUCCESS, client_->Stat(srcFile, &stbuf));
    EXPECT_EQ(SUCCESS, client_->Stat(dstFile, &stbuf));
}

TEST_F(FalconMetadataUT, RenameSelfAndInvalidTargetKeepsState)
{
    const std::string file = root_ + "/rename_self";
    TrackFile(file);
    ASSERT_EQ(SUCCESS, client_->Create(file));
    struct stat stbuf = {};
    client_->Rename(file, file);
    EXPECT_EQ(SUCCESS, client_->Stat(file, &stbuf));

    const std::string dir = root_ + "/rename_parent";
    const std::string child = dir + "/child";
    TrackDir(dir);
    TrackDir(child);
    ASSERT_EQ(SUCCESS, client_->Mkdir(dir));
    ASSERT_EQ(SUCCESS, client_->Mkdir(child));
    EXPECT_NE(SUCCESS, client_->Rename(dir, child + "/moved_parent"));
    uint64_t inode = 0;
    EXPECT_EQ(SUCCESS, client_->OpenDir(dir, &inode));
    EXPECT_EQ(SUCCESS, client_->OpenDir(child, &inode));
}

TEST_F(FalconMetadataUT, RenameFailuresKeepSourceAndDestinationState)
{
    const std::string src = root_ + "/rename_fail_src";
    const std::string dstMissingParent = root_ + "/missing_parent/rename_fail_dst";
    TrackFile(src);
    ASSERT_EQ(SUCCESS, client_->Create(src));

    struct stat srcStatBefore = {};
    ASSERT_EQ(SUCCESS, client_->Stat(src, &srcStatBefore));
    EXPECT_NE(SUCCESS, client_->Rename(src, dstMissingParent));

    struct stat srcStatAfter = {};
    EXPECT_EQ(SUCCESS, client_->Stat(src, &srcStatAfter));
    EXPECT_EQ(srcStatBefore.st_ino, srcStatAfter.st_ino);
    EXPECT_NE(SUCCESS, client_->Stat(dstMissingParent, &srcStatAfter));

    const std::string dirSrc = root_ + "/rename_fail_dir_src";
    const std::string fileDst = root_ + "/rename_fail_file_dst";
    TrackDir(dirSrc);
    TrackFile(fileDst);
    ASSERT_EQ(SUCCESS, client_->Mkdir(dirSrc));
    ASSERT_EQ(SUCCESS, client_->Create(fileDst));
    client_->Rename(dirSrc, fileDst);

    uint64_t dirInode = 0;
    EXPECT_EQ(SUCCESS, client_->OpenDir(dirSrc, &dirInode));
    EXPECT_EQ(SUCCESS, client_->Stat(fileDst, &srcStatAfter));
}

TEST_F(FalconMetadataUT, RenameAndAttributeP0)
{
    const std::string src = root_ + "/src_file";
    const std::string dst = root_ + "/dst_file";
    TrackFile(src);
    TrackFile(dst);
    ASSERT_EQ(SUCCESS, client_->Create(src));
    ASSERT_EQ(SUCCESS, client_->Rename(src, dst));
    struct stat stbuf = {};
    EXPECT_NE(SUCCESS, client_->Stat(src, &stbuf));
    EXPECT_EQ(SUCCESS, client_->Stat(dst, &stbuf));

    const std::string dirA = root_ + "/ren_a";
    const std::string dirB = root_ + "/ren_b";
    const std::string crossSrc = dirA + "/src";
    const std::string crossDst = dirB + "/dst";
    TrackDir(dirA);
    TrackDir(dirB);
    TrackFile(crossSrc);
    TrackFile(crossDst);
    ASSERT_EQ(SUCCESS, client_->Mkdir(dirA));
    ASSERT_EQ(SUCCESS, client_->Mkdir(dirB));
    ASSERT_EQ(SUCCESS, client_->Create(crossSrc));
    ASSERT_EQ(SUCCESS, client_->Rename(crossSrc, crossDst));
    EXPECT_NE(SUCCESS, client_->Stat(crossSrc, &stbuf));
    EXPECT_EQ(SUCCESS, client_->Stat(crossDst, &stbuf));

    EXPECT_NE(SUCCESS, client_->Rename(root_ + "/missing_src", root_ + "/new_dst"));
    EXPECT_NE(SUCCESS, client_->Stat(root_ + "/new_dst", &stbuf));

    ASSERT_EQ(SUCCESS, client_->Chmod(dst, 0755));
    ASSERT_EQ(SUCCESS, client_->Stat(dst, &stbuf));
    EXPECT_EQ(0755U, static_cast<unsigned>(stbuf.st_mode & 0777));

    ASSERT_EQ(SUCCESS, client_->Chown(dst, 1234, 5678));
    ASSERT_EQ(SUCCESS, client_->Stat(dst, &stbuf));
    EXPECT_EQ(1234U, stbuf.st_uid);
    EXPECT_EQ(5678U, stbuf.st_gid);

    const long oldMtimeNsec = stbuf.st_mtim.tv_nsec;
    ASSERT_EQ(SUCCESS, client_->UtimeNs(dst, 111111111, 222222222));
    ASSERT_EQ(SUCCESS, client_->Stat(dst, &stbuf));
    EXPECT_NE(oldMtimeNsec, stbuf.st_mtim.tv_nsec);

    const std::string missing = root_ + "/missing_attr";
    EXPECT_NE(SUCCESS, client_->UtimeNs(missing, 1, 2));
    EXPECT_NE(SUCCESS, client_->Chmod(missing, 0644));
    EXPECT_NE(SUCCESS, client_->Chown(missing, 1, 2));
}

TEST_F(FalconMetadataUT, DirectoryAttributesAndRenamePreserveMetadata)
{
    const std::string dir = root_ + "/attr_dir";
    const std::string renamed = root_ + "/attr_dir_renamed";
    TrackDir(dir);
    TrackDir(renamed);
    ASSERT_EQ(SUCCESS, client_->Mkdir(dir));

    struct stat stbuf = {};
    ASSERT_EQ(SUCCESS, client_->Chmod(dir, 0701));
    ASSERT_EQ(SUCCESS, client_->Chown(dir, 2345, 6789));
    ASSERT_EQ(SUCCESS, client_->UtimeNs(dir, 333333333, 444444444));
    ASSERT_EQ(SUCCESS, client_->Stat(dir, &stbuf));
    EXPECT_EQ(0701U, static_cast<unsigned>(stbuf.st_mode & 0777));
    EXPECT_EQ(2345U, stbuf.st_uid);
    EXPECT_EQ(6789U, stbuf.st_gid);

    ASSERT_EQ(SUCCESS, client_->Rename(dir, renamed));
    ASSERT_EQ(SUCCESS, client_->Stat(renamed, &stbuf));
    EXPECT_EQ(0701U, static_cast<unsigned>(stbuf.st_mode & 0777));
    EXPECT_EQ(2345U, stbuf.st_uid);
    EXPECT_EQ(6789U, stbuf.st_gid);
    EXPECT_NE(SUCCESS, client_->Stat(dir, &stbuf));
}

TEST_F(FalconMetadataUT, AttributeBoundaryValues)
{
    const std::string file = root_ + "/attr_boundary_file";
    TrackFile(file);
    ASSERT_EQ(SUCCESS, client_->Create(file));

    struct stat stbuf = {};
    ASSERT_EQ(SUCCESS, client_->Chmod(file, 0000));
    ASSERT_EQ(SUCCESS, client_->Stat(file, &stbuf));
    EXPECT_EQ(0000U, static_cast<unsigned>(stbuf.st_mode & 0777));

    ASSERT_EQ(SUCCESS, client_->Chmod(file, 0777));
    ASSERT_EQ(SUCCESS, client_->Stat(file, &stbuf));
    EXPECT_EQ(0777U, static_cast<unsigned>(stbuf.st_mode & 0777));

    ASSERT_EQ(SUCCESS, client_->Chmod(file, 01777));
    ASSERT_EQ(SUCCESS, client_->Stat(file, &stbuf));
    EXPECT_EQ(0777U, static_cast<unsigned>(stbuf.st_mode & 0777));

    ASSERT_EQ(SUCCESS, client_->Chown(file, 0, std::numeric_limits<uint32_t>::max()));
    ASSERT_EQ(SUCCESS, client_->Stat(file, &stbuf));
    EXPECT_EQ(0U, stbuf.st_uid);
    EXPECT_EQ(std::numeric_limits<uint32_t>::max(), static_cast<uint32_t>(stbuf.st_gid));

    const long oldMtimeNsec = stbuf.st_mtim.tv_nsec;
    ASSERT_EQ(SUCCESS, client_->UtimeNs(file, -1, -1));
    ASSERT_EQ(SUCCESS, client_->Stat(file, &stbuf));
    EXPECT_TRUE(stbuf.st_mtim.tv_nsec != oldMtimeNsec || stbuf.st_mtim.tv_sec >= 0);
}

TEST_F(FalconMetadataUT, RenameDirectoryAndExistingTargetSemantics)
{
    struct stat stbuf = {};

    const std::string dirSrc = root_ + "/dir_src";
    const std::string dirDst = root_ + "/dir_dst";
    TrackDir(dirSrc);
    TrackDir(dirDst);
    ASSERT_EQ(SUCCESS, client_->Mkdir(dirSrc));
    ASSERT_EQ(SUCCESS, client_->Rename(dirSrc, dirDst));
    EXPECT_NE(SUCCESS, client_->OpenDir(dirSrc));
    uint64_t dirInode = 0;
    EXPECT_EQ(SUCCESS, client_->OpenDir(dirDst, &dirInode));
    EXPECT_NE(0U, dirInode);

    const std::string src = root_ + "/rename_src_existing";
    const std::string dst = root_ + "/rename_dst_existing";
    TrackFile(src);
    TrackFile(dst);
    ASSERT_EQ(SUCCESS, client_->Create(src));
    ASSERT_EQ(SUCCESS, client_->Create(dst));

    FalconErrorCode renameRet = client_->Rename(src, dst);
    FalconErrorCode srcStat = client_->Stat(src, &stbuf);
    FalconErrorCode dstStat = client_->Stat(dst, &stbuf);
    EXPECT_EQ(SUCCESS, dstStat);
    if (renameRet == SUCCESS) {
        EXPECT_NE(SUCCESS, srcStat);
    } else {
        EXPECT_EQ(SUCCESS, srcStat);
    }
}

TEST_F(FalconMetadataUT, RenameFileDirectoryConflictSemantics)
{
    const std::string file = root_ + "/rename_conflict_file";
    const std::string dir = root_ + "/rename_conflict_dir";
    TrackFile(file);
    TrackDir(dir);
    ASSERT_EQ(SUCCESS, client_->Create(file));
    ASSERT_EQ(SUCCESS, client_->Mkdir(dir));

    FalconErrorCode fileToDir = client_->Rename(file, dir);
    struct stat stbuf = {};
    uint64_t inode = 0;
    if (fileToDir == SUCCESS) {
        EXPECT_NE(SUCCESS, client_->Stat(file, &stbuf));
        EXPECT_NE(SUCCESS, client_->OpenDir(dir, &inode));
        EXPECT_EQ(SUCCESS, client_->Stat(dir, &stbuf));
    } else {
        EXPECT_EQ(SUCCESS, client_->Stat(file, &stbuf));
        EXPECT_EQ(SUCCESS, client_->OpenDir(dir, &inode));
    }
}

TEST_F(FalconMetadataUT, ManyFileRenameReadDirConsistency)
{
    constexpr int kFileCount = 64;
    const std::string dirA = root_ + "/many_rename_a";
    const std::string dirB = root_ + "/many_rename_b";
    TrackDir(dirA);
    TrackDir(dirB);
    ASSERT_EQ(SUCCESS, client_->Mkdir(dirA));
    ASSERT_EQ(SUCCESS, client_->Mkdir(dirB));

    for (int i = 0; i < kFileCount; ++i) {
        TrackFile(dirA + "/file_" + std::to_string(i));
        TrackFile(dirB + "/renamed_" + std::to_string(i));
        ASSERT_EQ(SUCCESS, client_->Create(dirA + "/file_" + std::to_string(i)));
    }

    for (int i = 0; i < kFileCount; i += 2) {
        ASSERT_EQ(SUCCESS, client_->Rename(dirA + "/file_" + std::to_string(i), dirB + "/renamed_" + std::to_string(i)));
    }

    std::vector<std::string> entriesA;
    std::vector<std::string> entriesB;
    ASSERT_EQ(SUCCESS, client_->ReadDir(dirA, &entriesA));
    ASSERT_EQ(SUCCESS, client_->ReadDir(dirB, &entriesB));
    std::set<std::string> setA = ToSet(entriesA);
    std::set<std::string> setB = ToSet(entriesB);
    for (int i = 0; i < kFileCount; ++i) {
        if (i % 2 == 0) {
            EXPECT_FALSE(setA.contains("file_" + std::to_string(i)));
            EXPECT_TRUE(setB.contains("renamed_" + std::to_string(i)));
        } else {
            EXPECT_TRUE(setA.contains("file_" + std::to_string(i)));
            EXPECT_FALSE(setB.contains("renamed_" + std::to_string(i)));
        }
    }
}

TEST_F(FalconMetadataUT, ReadDirReflectsRenameAndDeleteAcrossTypes)
{
    const std::string dir = root_ + "/readdir_mutation";
    const std::string childDir = dir + "/child_dir";
    const std::string childFile = dir + "/child_file";
    const std::string renamedDir = dir + "/renamed_dir";
    const std::string renamedFile = dir + "/renamed_file";
    TrackDir(dir);
    TrackDir(childDir);
    TrackDir(renamedDir);
    TrackFile(childFile);
    TrackFile(renamedFile);
    ASSERT_EQ(SUCCESS, client_->Mkdir(dir));
    ASSERT_EQ(SUCCESS, client_->Mkdir(childDir));
    ASSERT_EQ(SUCCESS, client_->Create(childFile));
    ASSERT_EQ(SUCCESS, client_->Rename(childDir, renamedDir));
    ASSERT_EQ(SUCCESS, client_->Rename(childFile, renamedFile));

    std::vector<std::string> entries;
    ASSERT_EQ(SUCCESS, client_->ReadDir(dir, &entries));
    std::set<std::string> entrySet = ToSet(entries);
    EXPECT_FALSE(entrySet.contains("child_dir"));
    EXPECT_FALSE(entrySet.contains("child_file"));
    EXPECT_TRUE(entrySet.contains("renamed_dir"));
    EXPECT_TRUE(entrySet.contains("renamed_file"));

    ASSERT_EQ(SUCCESS, client_->Unlink(renamedFile));
    ASSERT_EQ(SUCCESS, client_->Rmdir(renamedDir));
    entries.clear();
    ASSERT_EQ(SUCCESS, client_->ReadDir(dir, &entries));
    entrySet = ToSet(entries);
    EXPECT_FALSE(entrySet.contains("renamed_dir"));
    EXPECT_FALSE(entrySet.contains("renamed_file"));
}

TEST_F(FalconMetadataUT, HotRenameWithConcurrentReadDir)
{
    constexpr int kIterations = 20;
    constexpr int kReaderThreads = 4;
    const std::string src = root_ + "/readdir_hot_src";
    const std::string dst = root_ + "/readdir_hot_dst";
    TrackFile(src);
    TrackFile(dst);
    ASSERT_EQ(SUCCESS, client_->Create(src));

    std::atomic<bool> start = false;
    std::atomic<bool> done = false;
    std::atomic<int> readSuccess = 0;
    std::atomic<int> readRpcFailure = 0;
    std::atomic<int> readMissingBoth = 0;

    std::thread renamer([&]() {
        WaitForStart(start);
        for (int i = 0; i < kIterations; ++i) {
            ASSERT_EQ(SUCCESS, client_->Rename(src, dst));
            ASSERT_EQ(SUCCESS, client_->Rename(dst, src));
        }
        done.store(true, std::memory_order_release);
    });

    std::vector<std::thread> readers;
    readers.reserve(kReaderThreads);
    for (int i = 0; i < kReaderThreads; ++i) {
        readers.emplace_back([&]() {
            WaitForStart(start);
            while (!done.load(std::memory_order_acquire)) {
                std::vector<std::string> entries;
                FalconErrorCode ret = client_->ReadDir(root_, &entries);
                if (ret == SUCCESS) {
                    std::set<std::string> entrySet = ToSet(entries);
                    if (entrySet.contains("readdir_hot_src") || entrySet.contains("readdir_hot_dst")) {
                        readSuccess.fetch_add(1, std::memory_order_relaxed);
                    } else {
                        readMissingBoth.fetch_add(1, std::memory_order_relaxed);
                    }
                } else {
                    readRpcFailure.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    start.store(true, std::memory_order_release);
    renamer.join();
    for (auto &reader : readers) {
        reader.join();
    }

    std::vector<std::string> entries;
    ASSERT_EQ(SUCCESS, client_->ReadDir(root_, &entries));
    std::set<std::string> entrySet = ToSet(entries);
    EXPECT_TRUE(entrySet.contains("readdir_hot_src"));
    EXPECT_FALSE(entrySet.contains("readdir_hot_dst"));
    EXPECT_GT(readSuccess.load(), 0);
    EXPECT_EQ(0, readRpcFailure.load());
}

TEST_F(FalconMetadataUT, HotRenameWithConcurrentStatOpen)
{
    constexpr int kIterations = 20;
    constexpr int kReaderThreads = 4;
    const std::string src = root_ + "/hot_src";
    const std::string dst = root_ + "/hot_dst";
    TrackFile(src);
    TrackFile(dst);
    ASSERT_EQ(SUCCESS, client_->Create(src));

    std::atomic<bool> start = false;
    std::atomic<bool> done = false;
    std::atomic<int> readerSuccess = 0;
    std::atomic<int> readerFailure = 0;

    std::thread renamer([&]() {
        WaitForStart(start);
        for (int i = 0; i < kIterations; ++i) {
            ASSERT_EQ(SUCCESS, client_->Rename(src, dst));
            ASSERT_EQ(SUCCESS, client_->Rename(dst, src));
        }
        done.store(true, std::memory_order_release);
    });

    std::vector<std::thread> readers;
    for (int i = 0; i < kReaderThreads; ++i) {
        readers.emplace_back([&]() {
            WaitForStart(start);
            while (!done.load(std::memory_order_acquire)) {
                struct stat stbuf = {};
                FalconErrorCode statSrc = client_->Stat(src, &stbuf);
                FalconErrorCode statDst = client_->Stat(dst, &stbuf);
                if (statSrc == SUCCESS || statDst == SUCCESS) {
                    readerSuccess.fetch_add(1, std::memory_order_relaxed);
                } else {
                    readerFailure.fetch_add(1, std::memory_order_relaxed);
                }
                uint64_t inodeId = 0;
                int64_t size = 0;
                int32_t nodeId = 0;
                client_->Open(src, &inodeId, &size, &nodeId, &stbuf);
                client_->Open(dst, &inodeId, &size, &nodeId, &stbuf);
            }
        });
    }

    start.store(true, std::memory_order_release);
    renamer.join();
    for (auto &reader : readers) {
        reader.join();
    }

    struct stat stbuf = {};
    EXPECT_EQ(SUCCESS, client_->Stat(src, &stbuf));
    EXPECT_NE(SUCCESS, client_->Stat(dst, &stbuf));
    EXPECT_GT(readerSuccess.load(), 0);
}

TEST_F(FalconMetadataUT, FileNameValidationBoundary)
{
    const std::string validSpecialName = root_ + "/name.with-dash_and_123";
    TrackFile(validSpecialName);
    ASSERT_EQ(SUCCESS, client_->Create(validSpecialName));
    struct stat stbuf = {};
    EXPECT_EQ(SUCCESS, client_->Stat(validSpecialName, &stbuf));

    const std::string overlongName = root_ + "/" + std::string(512, 'x');
    FalconErrorCode createRet = client_->Create(overlongName);
    if (createRet == SUCCESS) {
        TrackFile(overlongName);
        EXPECT_EQ(SUCCESS, client_->Stat(overlongName, &stbuf));
    } else {
        EXPECT_NE(SUCCESS, client_->Stat(overlongName, &stbuf));
    }
}

TEST_F(FalconMetadataUT, KvPutGetDelRoundTrip)
{
    const std::string key = root_ + "/kv_key_0";
    TrackKvKey(key);

    const std::vector<uint64_t> valueKey = {101, 102};
    const std::vector<uint64_t> location = {0, 2048};
    const std::vector<uint32_t> size = {2048, 2048};

    ASSERT_EQ(SUCCESS, client_->KvPut(key, 4096, valueKey, location, size));

    MetadataUtClient::KvValue value;
    ASSERT_EQ(SUCCESS, client_->KvGet(key, &value));
    EXPECT_EQ(4096U, value.valueLen);
    EXPECT_EQ(2U, value.sliceNum);
    ASSERT_EQ(2U, value.slices.size());
    EXPECT_EQ(101U, value.slices[0].valueKey);
    EXPECT_EQ(0U, value.slices[0].location);
    EXPECT_EQ(2048U, value.slices[0].size);
    EXPECT_EQ(102U, value.slices[1].valueKey);
    EXPECT_EQ(2048U, value.slices[1].location);
    EXPECT_EQ(2048U, value.slices[1].size);

    ASSERT_EQ(SUCCESS, client_->KvDel(key));
    MetadataUtClient::KvValue deletedValue;
    EXPECT_NE(SUCCESS, client_->KvGet(key, &deletedValue));
}

TEST_F(FalconMetadataUT, KvP0DuplicateAndHotConcurrency)
{
    const std::string duplicateKey = root_ + "/kv_duplicate";
    TrackKvKey(duplicateKey);
    ASSERT_EQ(SUCCESS, client_->KvPut(duplicateKey, 4096, KvValueKeys(1000), KvLocations(), KvSizes()));
    FalconErrorCode secondPut = client_->KvPut(duplicateKey, 8192, KvValueKeys(2000), KvLocations(), KvSizes());
    MetadataUtClient::KvValue value;
    ASSERT_EQ(SUCCESS, client_->KvGet(duplicateKey, &value));
    if (secondPut == SUCCESS) {
        const bool keptOldValue = value.valueLen == 4096U && value.slices[0].valueKey == 1000U;
        const bool overwroteWithNewValue = value.valueLen == 8192U && value.slices[0].valueKey == 2000U;
        EXPECT_TRUE(keptOldValue || overwroteWithNewValue);
    } else {
        EXPECT_EQ(4096U, value.valueLen);
        EXPECT_EQ(1000U, value.slices[0].valueKey);
    }

    const std::string hotKey = root_ + "/kv_hot";
    TrackKvKey(hotKey);
    constexpr int kThreadNum = 12;
    auto [success, failure] = RunConcurrent(kThreadNum, [&](int threadId) {
        if (threadId % 3 == 0) {
            return client_->KvPut(hotKey, 4096 + threadId, KvValueKeys(3000 + threadId), KvLocations(), KvSizes());
        }
        if (threadId % 3 == 1) {
            MetadataUtClient::KvValue ignored;
            return client_->KvGet(hotKey, &ignored);
        }
        return client_->KvDel(hotKey);
    });
    EXPECT_EQ(kThreadNum, success + failure);

    MetadataUtClient::KvValue finalValue;
    FalconErrorCode finalRet = client_->KvGet(hotKey, &finalValue);
    if (finalRet == SUCCESS) {
        EXPECT_EQ(2U, finalValue.sliceNum);
        EXPECT_EQ(2U, finalValue.slices.size());
    }
}

TEST_F(FalconMetadataUT, KvMissingDeleteReputAndVariableSliceCounts)
{
    const std::string missingKey = root_ + "/kv_missing";
    MetadataUtClient::KvValue value;
    EXPECT_NE(SUCCESS, client_->KvGet(missingKey, &value));
    EXPECT_NE(SUCCESS, client_->KvDel(missingKey));

    const std::string key = root_ + "/kv_reput";
    TrackKvKey(key);
    ASSERT_EQ(SUCCESS, client_->KvPut(key, 1024, {1}, {0}, {1024}));
    ASSERT_EQ(SUCCESS, client_->KvGet(key, &value));
    EXPECT_EQ(1024U, value.valueLen);
    EXPECT_EQ(1U, value.sliceNum);
    ASSERT_EQ(SUCCESS, client_->KvDel(key));
    EXPECT_NE(SUCCESS, client_->KvGet(key, &value));

    ASSERT_EQ(SUCCESS, client_->KvPut(key, 8192, {2, 3, 4, 5}, {0, 2048, 4096, 6144}, {2048, 2048, 2048, 2048}));
    ASSERT_EQ(SUCCESS, client_->KvGet(key, &value));
    EXPECT_EQ(8192U, value.valueLen);
    EXPECT_EQ(4U, value.sliceNum);
    ASSERT_EQ(4U, value.slices.size());
    EXPECT_EQ(5U, value.slices[3].valueKey);
}

TEST_F(FalconMetadataUT, KvInputValidationAndLongKeys)
{
    const std::string key = root_ + "/kv_invalid";
    EXPECT_EQ(PROGRAM_ERROR, client_->KvPut(key, 4096, {1, 2}, {0}, {4096}));
    EXPECT_EQ(PROGRAM_ERROR, client_->KvPut(key, 4096, {1}, {0, 2048}, {4096}));

    const std::string longKey = root_ + "/kv_" + std::string(240, 'k');
    FalconErrorCode putRet = client_->KvPut(longKey, 4096, KvValueKeys(12000), KvLocations(), KvSizes());
    if (putRet == SUCCESS) {
        TrackKvKey(longKey);
        MetadataUtClient::KvValue value;
        EXPECT_EQ(SUCCESS, client_->KvGet(longKey, &value));
        EXPECT_EQ(2U, value.sliceNum);
    } else {
        MetadataUtClient::KvValue value;
        EXPECT_NE(SUCCESS, client_->KvGet(longKey, &value));
    }
}

TEST_F(FalconMetadataUT, KvLargeSliceArrayRoundTrip)
{
    constexpr int kSliceNum = 128;
    const std::string key = root_ + "/kv_large_array";
    TrackKvKey(key);

    std::vector<uint64_t> valueKeys;
    std::vector<uint64_t> locations;
    std::vector<uint32_t> sizes;
    valueKeys.reserve(kSliceNum);
    locations.reserve(kSliceNum);
    sizes.reserve(kSliceNum);
    for (int i = 0; i < kSliceNum; ++i) {
        valueKeys.push_back(60000 + i);
        locations.push_back(static_cast<uint64_t>(i) * 1024);
        sizes.push_back(1024);
    }

    ASSERT_EQ(SUCCESS, client_->KvPut(key, kSliceNum * 1024, valueKeys, locations, sizes));
    MetadataUtClient::KvValue value;
    ASSERT_EQ(SUCCESS, client_->KvGet(key, &value));
    EXPECT_EQ(static_cast<uint32_t>(kSliceNum * 1024), value.valueLen);
    EXPECT_EQ(kSliceNum, value.sliceNum);
    ASSERT_EQ(static_cast<size_t>(kSliceNum), value.slices.size());
    EXPECT_EQ(valueKeys.front(), value.slices.front().valueKey);
    EXPECT_EQ(valueKeys.back(), value.slices.back().valueKey);
    EXPECT_EQ(locations.back(), value.slices.back().location);
}

TEST_F(FalconMetadataUT, KvDeleteIsolationAcrossKeys)
{
    const std::string keyA = root_ + "/kv_delete_isolation_a";
    const std::string keyB = root_ + "/kv_delete_isolation_b";
    TrackKvKey(keyA);
    TrackKvKey(keyB);

    ASSERT_EQ(SUCCESS, client_->KvPut(keyA, 4096, KvValueKeys(71000), KvLocations(), KvSizes()));
    ASSERT_EQ(SUCCESS, client_->KvPut(keyB, 8192, {72000, 72001, 72002}, {0, 2048, 4096}, {2048, 2048, 4096}));
    ASSERT_EQ(SUCCESS, client_->KvDel(keyA));

    MetadataUtClient::KvValue valueA;
    EXPECT_NE(SUCCESS, client_->KvGet(keyA, &valueA));

    MetadataUtClient::KvValue valueB;
    ASSERT_EQ(SUCCESS, client_->KvGet(keyB, &valueB));
    EXPECT_EQ(8192U, valueB.valueLen);
    ASSERT_EQ(3U, valueB.slices.size());
    EXPECT_EQ(72002U, valueB.slices[2].valueKey);
}

TEST_F(FalconMetadataUT, KvMultiKeyShardConcurrency)
{
    constexpr int kKeyNum = 48;

    for (int i = 0; i < kKeyNum; ++i) {
        TrackKvKey(root_ + "/kv_multi_" + std::to_string(i));
    }

    auto [putSuccess, putFailure] = RunConcurrent(kKeyNum, [&](int i) {
        const std::string key = root_ + "/kv_multi_" + std::to_string(i);
        return client_->KvPut(key, 4096 + i, KvValueKeys(10000 + i), KvLocations(), KvSizes());
    });
    EXPECT_EQ(kKeyNum, putSuccess);
    EXPECT_EQ(0, putFailure);

    auto [getSuccess, getFailure] = RunConcurrent(kKeyNum, [&](int i) {
        const std::string key = root_ + "/kv_multi_" + std::to_string(i);
        MetadataUtClient::KvValue value;
        FalconErrorCode ret = client_->KvGet(key, &value);
        if (ret != SUCCESS || value.sliceNum != 2 || value.valueLen != static_cast<uint32_t>(4096 + i)) {
            return PROGRAM_ERROR;
        }
        return SUCCESS;
    });
    EXPECT_EQ(kKeyNum, getSuccess);
    EXPECT_EQ(0, getFailure);

    auto [delSuccess, delFailure] = RunConcurrent(kKeyNum, [&](int i) {
        const std::string key = root_ + "/kv_multi_" + std::to_string(i);
        return client_->KvDel(key);
    });
    EXPECT_EQ(kKeyNum, delSuccess);
    EXPECT_EQ(0, delFailure);
}

TEST_F(FalconMetadataUT, SliceFetchPutGetDelRoundTrip)
{
    uint64_t startId = 0;
    uint64_t endId = 0;
    ASSERT_EQ(SUCCESS, client_->FetchSliceId(3, &startId, &endId));
    ASSERT_EQ(startId + 3, endId);

    const std::string filename = root_ + "/slice_file_0";
    const uint64_t inodeId = 900001;
    const uint32_t chunkId = 0;
    TrackSlice(filename, inodeId, chunkId);

    const std::vector<uint64_t> inodeIds = {inodeId, inodeId, inodeId};
    const std::vector<uint32_t> chunkIds = {chunkId, chunkId, chunkId};
    const std::vector<uint64_t> sliceIds = {startId, startId + 1, startId + 2};
    const std::vector<uint32_t> sliceSizes = {4096, 4096, 4096};
    const std::vector<uint32_t> sliceOffsets = {0, 4096, 8192};
    const std::vector<uint32_t> sliceLens = {4096, 4096, 4096};
    const std::vector<uint32_t> sliceLoc1 = {1, 1, 1};
    const std::vector<uint32_t> sliceLoc2 = {10, 11, 12};

    ASSERT_EQ(SUCCESS, client_->SlicePut(filename,
                                         inodeIds,
                                         chunkIds,
                                         sliceIds,
                                         sliceSizes,
                                         sliceOffsets,
                                         sliceLens,
                                         sliceLoc1,
                                         sliceLoc2));

    MetadataUtClient::SliceValue value;
    ASSERT_EQ(SUCCESS, client_->SliceGet(filename, inodeId, chunkId, &value));
    EXPECT_EQ(3U, value.sliceNum);
    ASSERT_EQ(3U, value.sliceId.size());
    EXPECT_EQ(sliceIds, value.sliceId);
    EXPECT_EQ(sliceOffsets, value.sliceOffset);
    EXPECT_EQ(sliceLens, value.sliceLen);
    EXPECT_EQ(sliceLoc2, value.sliceLoc2);

    ASSERT_EQ(SUCCESS, client_->SliceDel(filename, inodeId, chunkId));
    MetadataUtClient::SliceValue deletedValue;
    ExpectMissingOrEmptySlice(client_->SliceGet(filename, inodeId, chunkId, &deletedValue), deletedValue);
}

TEST_F(FalconMetadataUT, SliceDeleteMissingReputAndOffsetOrdering)
{
    const std::string filename = root_ + "/slice_reput";
    const uint64_t inodeId = 990501;
    const uint32_t chunkId = 3;
    client_->SliceDel(filename, inodeId, chunkId);

    uint64_t startId = 0;
    uint64_t endId = 0;
    ASSERT_EQ(SUCCESS, client_->FetchSliceId(4, &startId, &endId));
    TrackSlice(filename, inodeId, chunkId);
    ASSERT_EQ(SUCCESS, client_->SlicePut(filename,
                                         {inodeId, inodeId},
                                         {chunkId, chunkId},
                                         {startId, startId + 1},
                                         {4096, 4096},
                                         {4096, 0},
                                         {4096, 4096},
                                         {1, 1},
                                         {41, 42}));
    MetadataUtClient::SliceValue value;
    ASSERT_EQ(SUCCESS, client_->SliceGet(filename, inodeId, chunkId, &value));
    ASSERT_EQ(2U, value.sliceNum);
    EXPECT_EQ(std::vector<uint32_t>({4096, 0}), value.sliceOffset);

    ASSERT_EQ(SUCCESS, client_->SliceDel(filename, inodeId, chunkId));
    ExpectMissingOrEmptySlice(client_->SliceGet(filename, inodeId, chunkId, &value), value);

    ASSERT_EQ(SUCCESS, client_->SlicePut(filename,
                                         {inodeId},
                                         {chunkId},
                                         {startId + 2},
                                         {8192},
                                         {0},
                                         {8192},
                                         {2},
                                         {43}));
    ASSERT_EQ(SUCCESS, client_->SliceGet(filename, inodeId, chunkId, &value));
    EXPECT_EQ(1U, value.sliceNum);
    EXPECT_EQ(8192U, value.sliceSize[0]);
}

TEST_F(FalconMetadataUT, SliceInputValidationAndMissingDelete)
{
    const std::string filename = root_ + "/slice_invalid";
    const uint64_t inodeId = 990601;
    EXPECT_EQ(PROGRAM_ERROR, client_->SlicePut(filename, {}, {}, {}, {}, {}, {}, {}, {}));
    EXPECT_EQ(PROGRAM_ERROR, client_->SlicePut(filename, {inodeId}, {0, 1}, {1}, {4096}, {0}, {4096}, {1}, {1}));

    MetadataUtClient::SliceValue value;
    FalconErrorCode missingRet = client_->SliceGet(filename, inodeId, 0, &value);
    if (missingRet == SUCCESS) {
        EXPECT_EQ(0U, value.sliceNum);
    }
    client_->SliceDel(filename, inodeId, 0);
    missingRet = client_->SliceGet(filename, inodeId, 0, &value);
    if (missingRet == SUCCESS) {
        EXPECT_EQ(0U, value.sliceNum);
    }
}

TEST_F(FalconMetadataUT, SliceLargeArrayRoundTrip)
{
    constexpr int kSliceNum = 128;
    const std::string filename = root_ + "/slice_large_array";
    const uint64_t inodeId = 990701;
    const uint32_t chunkId = 7;
    TrackSlice(filename, inodeId, chunkId);

    uint64_t startId = 0;
    uint64_t endId = 0;
    ASSERT_EQ(SUCCESS, client_->FetchSliceId(kSliceNum, &startId, &endId));
    ASSERT_EQ(startId + kSliceNum, endId);

    std::vector<uint64_t> inodeIds;
    std::vector<uint32_t> chunkIds;
    std::vector<uint64_t> sliceIds;
    std::vector<uint32_t> sliceSizes;
    std::vector<uint32_t> sliceOffsets;
    std::vector<uint32_t> sliceLens;
    std::vector<uint32_t> sliceLoc1;
    std::vector<uint32_t> sliceLoc2;
    inodeIds.reserve(kSliceNum);
    chunkIds.reserve(kSliceNum);
    sliceIds.reserve(kSliceNum);
    sliceSizes.reserve(kSliceNum);
    sliceOffsets.reserve(kSliceNum);
    sliceLens.reserve(kSliceNum);
    sliceLoc1.reserve(kSliceNum);
    sliceLoc2.reserve(kSliceNum);
    for (int i = 0; i < kSliceNum; ++i) {
        inodeIds.push_back(inodeId);
        chunkIds.push_back(chunkId);
        sliceIds.push_back(startId + i);
        sliceSizes.push_back(1024);
        sliceOffsets.push_back(i * 1024);
        sliceLens.push_back(1024);
        sliceLoc1.push_back(3);
        sliceLoc2.push_back(700 + i);
    }

    ASSERT_EQ(SUCCESS,
              client_->SlicePut(filename, inodeIds, chunkIds, sliceIds, sliceSizes, sliceOffsets, sliceLens, sliceLoc1, sliceLoc2));
    MetadataUtClient::SliceValue value;
    ASSERT_EQ(SUCCESS, client_->SliceGet(filename, inodeId, chunkId, &value));
    EXPECT_EQ(kSliceNum, value.sliceNum);
    ASSERT_EQ(static_cast<size_t>(kSliceNum), value.sliceId.size());
    EXPECT_EQ(sliceIds.front(), value.sliceId.front());
    EXPECT_EQ(sliceIds.back(), value.sliceId.back());
    EXPECT_EQ(sliceOffsets.back(), value.sliceOffset.back());
    EXPECT_EQ(sliceLoc2.back(), value.sliceLoc2.back());
}

TEST_F(FalconMetadataUT, SliceMissingAndMultiChunkSemantics)
{
    const std::string missingFile = root_ + "/missing_slice_file";
    MetadataUtClient::SliceValue missingValue;
    ExpectMissingOrEmptySlice(client_->SliceGet(missingFile, 990001, 0, &missingValue), missingValue);

    const std::string filename = root_ + "/slice_multi_chunk";
    uint64_t startId = 0;
    uint64_t endId = 0;
    ASSERT_EQ(SUCCESS, client_->FetchSliceId(6, &startId, &endId));
    ASSERT_EQ(startId + 6, endId);

    const uint64_t inodeId = 990002;
    TrackSlice(filename, inodeId, 0);
    TrackSlice(filename, inodeId, 1);
    ASSERT_EQ(SUCCESS, client_->SlicePut(filename,
                                         {inodeId, inodeId},
                                         {0, 1},
                                         {startId, startId + 1},
                                         {4096, 4096},
                                         {0, 0},
                                         {4096, 4096},
                                         {1, 1},
                                         {31, 32}));

    MetadataUtClient::SliceValue chunk0;
    MetadataUtClient::SliceValue chunk1;
    ASSERT_EQ(SUCCESS, client_->SliceGet(filename, inodeId, 0, &chunk0));
    ASSERT_EQ(SUCCESS, client_->SliceGet(filename, inodeId, 1, &chunk1));
    EXPECT_EQ(1U, chunk0.sliceNum);
    EXPECT_EQ(1U, chunk1.sliceNum);
    EXPECT_EQ(startId, chunk0.sliceId[0]);
    EXPECT_EQ(startId + 1, chunk1.sliceId[0]);
}

TEST_F(FalconMetadataUT, SliceDeleteOneChunkKeepsOtherChunks)
{
    const std::string filename = root_ + "/slice_delete_one_chunk";
    const uint64_t inodeId = 993001;
    TrackSlice(filename, inodeId, 0);
    TrackSlice(filename, inodeId, 1);

    ASSERT_EQ(SUCCESS, client_->SlicePut(filename,
                                         {inodeId, inodeId},
                                         {0, 1},
                                         {81000, 81001},
                                         {4096, 4096},
                                         {0, 0},
                                         {4096, 4096},
                                         {1, 1},
                                         {11, 12}));
    ASSERT_EQ(SUCCESS, client_->SliceDel(filename, inodeId, 0));

    MetadataUtClient::SliceValue deletedChunk;
    ExpectMissingOrEmptySlice(client_->SliceGet(filename, inodeId, 0, &deletedChunk), deletedChunk);

    MetadataUtClient::SliceValue keptChunk;
    ASSERT_EQ(SUCCESS, client_->SliceGet(filename, inodeId, 1, &keptChunk));
    ASSERT_EQ(1U, keptChunk.sliceNum);
    EXPECT_EQ(81001U, keptChunk.sliceId[0]);
    EXPECT_EQ(12U, keptChunk.sliceLoc2[0]);
}

TEST_F(FalconMetadataUT, SliceFilenameAndInodeIsolation)
{
    const std::string fileA = root_ + "/slice_isolation_a";
    const std::string fileB = root_ + "/slice_isolation_b";
    constexpr uint64_t kSharedInode = 993101;
    constexpr uint64_t kOtherInode = 993102;
    TrackSlice(fileA, kSharedInode, 0);
    TrackSlice(fileA, kOtherInode, 0);
    TrackSlice(fileB, kSharedInode, 0);

    ASSERT_EQ(SUCCESS, client_->SlicePut(fileA,
                                         {kSharedInode, kOtherInode},
                                         {0, 0},
                                         {82000, 82001},
                                         {4096, 4096},
                                         {0, 0},
                                         {4096, 4096},
                                         {1, 1},
                                         {21, 22}));
    ASSERT_EQ(SUCCESS, client_->SlicePut(fileB, {kSharedInode}, {0}, {82002}, {4096}, {0}, {4096}, {1}, {23}));

    MetadataUtClient::SliceValue valueA;
    ASSERT_EQ(SUCCESS, client_->SliceGet(fileA, kSharedInode, 0, &valueA));
    ASSERT_EQ(1U, valueA.sliceNum);
    EXPECT_EQ(82000U, valueA.sliceId[0]);

    MetadataUtClient::SliceValue valueOtherInode;
    ASSERT_EQ(SUCCESS, client_->SliceGet(fileA, kOtherInode, 0, &valueOtherInode));
    ASSERT_EQ(1U, valueOtherInode.sliceNum);
    EXPECT_EQ(82001U, valueOtherInode.sliceId[0]);

    MetadataUtClient::SliceValue valueB;
    ASSERT_EQ(SUCCESS, client_->SliceGet(fileB, kSharedInode, 0, &valueB));
    ASSERT_EQ(1U, valueB.sliceNum);
    EXPECT_EQ(82002U, valueB.sliceId[0]);
}

TEST_F(FalconMetadataUT, FetchSliceIdSingleAndTypeIsolation)
{
    uint64_t fileStart1 = 0;
    uint64_t fileEnd1 = 0;
    uint64_t fileStart2 = 0;
    uint64_t fileEnd2 = 0;
    uint64_t kvStart1 = 0;
    uint64_t kvEnd1 = 0;
    uint64_t kvStart2 = 0;
    uint64_t kvEnd2 = 0;

    ASSERT_EQ(SUCCESS, client_->FetchSliceId(1000, &fileStart1, &fileEnd1, 1));
    EXPECT_EQ(fileStart1 + 1000, fileEnd1);
    ASSERT_EQ(SUCCESS, client_->FetchSliceId(7, &fileStart2, &fileEnd2, 1));
    EXPECT_EQ(fileEnd1, fileStart2);
    EXPECT_EQ(fileStart2 + 7, fileEnd2);

    ASSERT_EQ(SUCCESS, client_->FetchSliceId(5, &kvStart1, &kvEnd1, 0));
    EXPECT_EQ(kvStart1 + 5, kvEnd1);
    ASSERT_EQ(SUCCESS, client_->FetchSliceId(6, &kvStart2, &kvEnd2, 0));
    EXPECT_EQ(kvEnd1, kvStart2);
    EXPECT_EQ(kvStart2 + 6, kvEnd2);
}

TEST_F(FalconMetadataUT, FetchSliceIdBoundaryCountsAndUnknownType)
{
    uint64_t startId = 0;
    uint64_t endId = 0;
    FalconErrorCode zeroRet = client_->FetchSliceId(0, &startId, &endId);
    if (zeroRet == SUCCESS) {
        EXPECT_EQ(startId, endId);
    }

    ASSERT_EQ(SUCCESS, client_->FetchSliceId(65536, &startId, &endId, 1));
    EXPECT_EQ(startId + 65536, endId);

    uint64_t unknownStart = 0;
    uint64_t unknownEnd = 0;
    FalconErrorCode unknownRet = client_->FetchSliceId(9, &unknownStart, &unknownEnd, 99);
    if (unknownRet == SUCCESS) {
        EXPECT_EQ(unknownStart + 9, unknownEnd);
    }
}

TEST_F(FalconMetadataUT, SliceP0DuplicateAndHotConcurrency)
{
    const std::string filename = root_ + "/slice_duplicate";
    const uint64_t inodeId = 910001;
    const uint32_t chunkId = 0;
    TrackSlice(filename, inodeId, chunkId);

    uint64_t startId = 0;
    uint64_t endId = 0;
    ASSERT_EQ(SUCCESS, client_->FetchSliceId(4, &startId, &endId));
    ASSERT_EQ(SUCCESS, client_->SlicePut(filename,
                                         {inodeId, inodeId},
                                         {chunkId, chunkId},
                                         {startId, startId + 1},
                                         {4096, 4096},
                                         {0, 4096},
                                         {4096, 4096},
                                         {1, 1},
                                         {11, 12}));
    FalconErrorCode secondPut = client_->SlicePut(filename,
                                                  {inodeId},
                                                  {chunkId},
                                                  {startId + 2},
                                                  {8192},
                                                  {0},
                                                  {8192},
                                                  {2},
                                                  {22});
    MetadataUtClient::SliceValue value;
    FalconErrorCode getRet = client_->SliceGet(filename, inodeId, chunkId, &value);
    ASSERT_EQ(SUCCESS, getRet);
    if (secondPut == SUCCESS) {
        EXPECT_GE(value.sliceNum, 1U);
    } else {
        EXPECT_EQ(2U, value.sliceNum);
    }

    const std::string hotFile = root_ + "/slice_hot";
    const uint64_t hotInodeId = 920001;
    const uint32_t hotChunkId = 0;
    TrackSlice(hotFile, hotInodeId, hotChunkId);
    auto [success, failure] = RunConcurrent(12, [&](int threadId) {
        if (threadId % 3 == 0) {
            return client_->SlicePut(hotFile,
                                     {hotInodeId},
                                     {hotChunkId},
                                     {startId + 100 + static_cast<uint64_t>(threadId)},
                                     {4096},
                                     {0},
                                     {4096},
                                     {1},
                                     {static_cast<uint32_t>(threadId)});
        }
        if (threadId % 3 == 1) {
            MetadataUtClient::SliceValue ignored;
            return client_->SliceGet(hotFile, hotInodeId, hotChunkId, &ignored);
        }
        return client_->SliceDel(hotFile, hotInodeId, hotChunkId);
    });
    EXPECT_EQ(12, success + failure);
}

TEST_F(FalconMetadataUT, BatchSingleSemanticCompare)
{
    constexpr int kCount = 16;

    for (int i = 0; i < kCount; ++i) {
        const std::string file = root_ + "/single_file_" + std::to_string(i);
        ASSERT_EQ(SUCCESS, client_->Create(file));
        struct stat stbuf = {};
        ASSERT_EQ(SUCCESS, client_->Stat(file, &stbuf));
        ASSERT_EQ(SUCCESS, client_->Unlink(file));
    }
    auto [fileBatchSuccess, fileBatchFailure] = RunConcurrent(kCount, [&](int i) {
        const std::string file = root_ + "/batch_file_" + std::to_string(i);
        FalconErrorCode ret = client_->Create(file);
        if (ret != SUCCESS) {
            return ret;
        }
        struct stat stbuf = {};
        ret = client_->Stat(file, &stbuf);
        if (ret != SUCCESS) {
            return ret;
        }
        return client_->Unlink(file);
    });
    EXPECT_EQ(kCount, fileBatchSuccess);
    EXPECT_EQ(0, fileBatchFailure);

    for (int i = 0; i < kCount; ++i) {
        const std::string key = root_ + "/single_kv_" + std::to_string(i);
        ASSERT_EQ(SUCCESS, client_->KvPut(key, 4096, KvValueKeys(4000 + i), KvLocations(), KvSizes()));
        MetadataUtClient::KvValue value;
        ASSERT_EQ(SUCCESS, client_->KvGet(key, &value));
        ASSERT_EQ(SUCCESS, client_->KvDel(key));
    }
    auto [kvBatchSuccess, kvBatchFailure] = RunConcurrent(kCount, [&](int i) {
        const std::string key = root_ + "/batch_kv_" + std::to_string(i);
        FalconErrorCode ret = client_->KvPut(key, 4096, KvValueKeys(5000 + i), KvLocations(), KvSizes());
        if (ret != SUCCESS) {
            return ret;
        }
        MetadataUtClient::KvValue value;
        ret = client_->KvGet(key, &value);
        if (ret != SUCCESS || value.sliceNum != 2) {
            return PROGRAM_ERROR;
        }
        return client_->KvDel(key);
    });
    EXPECT_EQ(kCount, kvBatchSuccess);
    EXPECT_EQ(0, kvBatchFailure);

    for (int i = 0; i < kCount; ++i) {
        const std::string filename = root_ + "/single_slice_" + std::to_string(i);
        const uint64_t inodeId = 930000 + i;
        ASSERT_EQ(SUCCESS, client_->SlicePut(filename, {inodeId}, {0}, {6000U + static_cast<uint64_t>(i)},
                                             {4096}, {0}, {4096}, {1}, {1}));
        MetadataUtClient::SliceValue value;
        ASSERT_EQ(SUCCESS, client_->SliceGet(filename, inodeId, 0, &value));
        ASSERT_EQ(SUCCESS, client_->SliceDel(filename, inodeId, 0));
    }
    auto [sliceBatchSuccess, sliceBatchFailure] = RunConcurrent(kCount, [&](int i) {
        const std::string filename = root_ + "/batch_slice_" + std::to_string(i);
        const uint64_t inodeId = 940000 + i;
        FalconErrorCode ret = client_->SlicePut(filename, {inodeId}, {0}, {7000U + static_cast<uint64_t>(i)},
                                                {4096}, {0}, {4096}, {1}, {1});
        if (ret != SUCCESS) {
            return ret;
        }
        MetadataUtClient::SliceValue value;
        ret = client_->SliceGet(filename, inodeId, 0, &value);
        if (ret != SUCCESS || value.sliceNum != 1) {
            return PROGRAM_ERROR;
        }
        return client_->SliceDel(filename, inodeId, 0);
    });
    EXPECT_EQ(kCount, sliceBatchSuccess);
    EXPECT_EQ(0, sliceBatchFailure);
}

TEST_F(FalconMetadataUT, BatchFilePartialFailureDetailedState)
{
    constexpr int kThreadNum = 20;
    const std::string existing = root_ + "/batch_file_existing";
    TrackFile(existing);
    ASSERT_EQ(SUCCESS, client_->Create(existing));
    for (int i = 1; i < kThreadNum; i += 2) {
        TrackFile(root_ + "/batch_file_new_" + std::to_string(i));
    }

    auto [success, failure] = RunConcurrent(kThreadNum, [&](int i) {
        if (i % 2 == 0) {
            return client_->Create(existing);
        }
        return client_->Create(root_ + "/batch_file_new_" + std::to_string(i));
    });
    EXPECT_EQ(kThreadNum / 2, success);
    EXPECT_EQ(kThreadNum / 2, failure);

    struct stat stbuf = {};
    EXPECT_EQ(SUCCESS, client_->Stat(existing, &stbuf));
    for (int i = 1; i < kThreadNum; i += 2) {
        EXPECT_EQ(SUCCESS, client_->Stat(root_ + "/batch_file_new_" + std::to_string(i), &stbuf));
    }
}

TEST_F(FalconMetadataUT, BatchKvPartialFailureDetailedState)
{
    constexpr int kThreadNum = 20;
    const std::string existing = root_ + "/batch_kv_existing";
    TrackKvKey(existing);
    ASSERT_EQ(SUCCESS, client_->KvPut(existing, 4096, KvValueKeys(41000), KvLocations(), KvSizes()));
    for (int i = 1; i < kThreadNum; i += 2) {
        TrackKvKey(root_ + "/batch_kv_new_" + std::to_string(i));
    }

    auto [success, failure] = RunConcurrent(kThreadNum, [&](int i) {
        if (i % 2 == 0) {
            return client_->KvPut(existing, 8192, KvValueKeys(42000 + i), KvLocations(), KvSizes());
        }
        return client_->KvPut(root_ + "/batch_kv_new_" + std::to_string(i),
                              4096,
                              KvValueKeys(43000 + i),
                              KvLocations(),
                              KvSizes());
    });
    EXPECT_EQ(kThreadNum, success + failure);

    MetadataUtClient::KvValue value;
    EXPECT_EQ(SUCCESS, client_->KvGet(existing, &value));
    for (int i = 1; i < kThreadNum; i += 2) {
        ASSERT_EQ(SUCCESS, client_->KvGet(root_ + "/batch_kv_new_" + std::to_string(i), &value));
        EXPECT_EQ(4096U, value.valueLen);
    }
}

TEST_F(FalconMetadataUT, BatchSlicePartialFailureDetailedState)
{
    constexpr int kThreadNum = 20;
    const std::string existing = root_ + "/batch_slice_existing";
    const uint64_t existingInode = 992000;
    TrackSlice(existing, existingInode, 0);
    ASSERT_EQ(SUCCESS, client_->SlicePut(existing, {existingInode}, {0}, {50000}, {4096}, {0}, {4096}, {1}, {1}));
    for (int i = 1; i < kThreadNum; i += 2) {
        TrackSlice(root_ + "/batch_slice_new_" + std::to_string(i), 992100 + i, 0);
    }

    auto [success, failure] = RunConcurrent(kThreadNum, [&](int i) {
        if (i % 2 == 0) {
            return client_->SlicePut(existing,
                                    {existingInode},
                                    {0},
                                    {static_cast<uint64_t>(51000 + i)},
                                    {4096},
                                    {0},
                                    {4096},
                                    {1},
                                    {static_cast<uint32_t>(i)});
        }
        const std::string filename = root_ + "/batch_slice_new_" + std::to_string(i);
        const uint64_t inodeId = 992100 + i;
        return client_->SlicePut(filename,
                                {inodeId},
                                {0},
                                {static_cast<uint64_t>(52000 + i)},
                                {4096},
                                {0},
                                {4096},
                                {1},
                                {static_cast<uint32_t>(i)});
    });
    EXPECT_EQ(kThreadNum, success + failure);

    MetadataUtClient::SliceValue value;
    EXPECT_EQ(SUCCESS, client_->SliceGet(existing, existingInode, 0, &value));
    for (int i = 1; i < kThreadNum; i += 2) {
        const std::string filename = root_ + "/batch_slice_new_" + std::to_string(i);
        const uint64_t inodeId = 992100 + i;
        ASSERT_EQ(SUCCESS, client_->SliceGet(filename, inodeId, 0, &value));
        EXPECT_EQ(1U, value.sliceNum);
    }
}

TEST_F(FalconMetadataUT, BatchPartialSuccessAndFailureIsolation)
{
    constexpr int kThreadNum = 16;

    const std::string existingFile = root_ + "/partial_existing_file";
    TrackFile(existingFile);
    for (int i = 1; i < kThreadNum; i += 2) {
        TrackFile(root_ + "/partial_new_file_" + std::to_string(i));
    }
    ASSERT_EQ(SUCCESS, client_->Create(existingFile));

    auto [fileSuccess, fileFailure] = RunConcurrent(kThreadNum, [&](int i) {
        if (i % 2 == 0) {
            return client_->Create(existingFile);
        }
        const std::string file = root_ + "/partial_new_file_" + std::to_string(i);
        return client_->Create(file);
    });
    EXPECT_GT(fileSuccess, 0);
    EXPECT_GT(fileFailure, 0);

    const std::string existingKey = root_ + "/partial_existing_kv";
    TrackKvKey(existingKey);
    for (int i = 1; i < kThreadNum; i += 2) {
        TrackKvKey(root_ + "/partial_new_kv_" + std::to_string(i));
    }
    ASSERT_EQ(SUCCESS, client_->KvPut(existingKey, 4096, KvValueKeys(20000), KvLocations(), KvSizes()));

    auto [kvSuccess, kvFailure] = RunConcurrent(kThreadNum, [&](int i) {
        if (i % 2 == 0) {
            return client_->KvPut(existingKey, 8192, KvValueKeys(21000 + i), KvLocations(), KvSizes());
        }
        const std::string key = root_ + "/partial_new_kv_" + std::to_string(i);
        return client_->KvPut(key, 4096, KvValueKeys(22000 + i), KvLocations(), KvSizes());
    });
    EXPECT_EQ(kThreadNum, kvSuccess + kvFailure);
    MetadataUtClient::KvValue existingValue;
    EXPECT_EQ(SUCCESS, client_->KvGet(existingKey, &existingValue));

    const std::string existingSliceFile = root_ + "/partial_existing_slice";
    const uint64_t inodeId = 990100;
    TrackSlice(existingSliceFile, inodeId, 0);
    for (int i = 1; i < kThreadNum; i += 2) {
        TrackSlice(root_ + "/partial_new_slice_" + std::to_string(i), 990200 + i, 0);
    }
    ASSERT_EQ(SUCCESS, client_->SlicePut(existingSliceFile, {inodeId}, {0}, {30000}, {4096}, {0}, {4096}, {1}, {1}));

    auto [sliceSuccess, sliceFailure] = RunConcurrent(kThreadNum, [&](int i) {
        if (i % 2 == 0) {
            return client_->SlicePut(existingSliceFile,
                                    {inodeId},
                                    {0},
                                    {static_cast<uint64_t>(31000 + i)},
                                    {4096},
                                    {0},
                                    {4096},
                                    {1},
                                    {static_cast<uint32_t>(i)});
        }
        const std::string file = root_ + "/partial_new_slice_" + std::to_string(i);
        const uint64_t newInodeId = 990200 + i;
        return client_->SlicePut(file,
                                {newInodeId},
                                {0},
                                {static_cast<uint64_t>(32000 + i)},
                                {4096},
                                {0},
                                {4096},
                                {1},
                                {static_cast<uint32_t>(i)});
    });
    EXPECT_EQ(kThreadNum, sliceSuccess + sliceFailure);
    MetadataUtClient::SliceValue sliceValue;
    EXPECT_EQ(SUCCESS, client_->SliceGet(existingSliceFile, inodeId, 0, &sliceValue));
}

TEST_F(FalconMetadataUT, BatchMissingParentDoesNotBlockValidFileCreates)
{
    constexpr int kThreadNum = 24;
    for (int i = 0; i < kThreadNum; i += 2) {
        TrackFile(root_ + "/batch_valid_file_" + std::to_string(i));
    }

    auto [success, failure] = RunConcurrent(kThreadNum, [&](int i) {
        if (i % 2 == 0) {
            return client_->Create(root_ + "/batch_valid_file_" + std::to_string(i));
        }
        return client_->Create(root_ + "/missing_parent_for_batch/file_" + std::to_string(i));
    });
    EXPECT_EQ(kThreadNum / 2, success);
    EXPECT_EQ(kThreadNum / 2, failure);

    struct stat stbuf = {};
    for (int i = 0; i < kThreadNum; i += 2) {
        EXPECT_EQ(SUCCESS, client_->Stat(root_ + "/batch_valid_file_" + std::to_string(i), &stbuf));
    }
    for (int i = 1; i < kThreadNum; i += 2) {
        EXPECT_NE(SUCCESS, client_->Stat(root_ + "/missing_parent_for_batch/file_" + std::to_string(i), &stbuf));
    }
}

TEST_F(FalconMetadataUT, BatchHighConcurrencyStability)
{
    constexpr int kThreadNum = 96;

    for (int i = 0; i < kThreadNum; ++i) {
        if (i % 3 == 0) {
            TrackFile(root_ + "/batch_stable_file_" + std::to_string(i));
        } else if (i % 3 == 1) {
            TrackKvKey(root_ + "/batch_stable_kv_" + std::to_string(i));
        } else {
            TrackSlice(root_ + "/batch_stable_slice_" + std::to_string(i), 991000 + i, 0);
        }
    }

    auto [success, failure] = RunConcurrent(kThreadNum, [&](int i) {
        if (i % 3 == 0) {
            const std::string file = root_ + "/batch_stable_file_" + std::to_string(i);
            FalconErrorCode ret = client_->Create(file);
            if (ret != SUCCESS) {
                return ret;
            }
            struct stat stbuf = {};
            ret = client_->Stat(file, &stbuf);
            if (ret != SUCCESS) {
                return ret;
            }
            return client_->Unlink(file);
        }
        if (i % 3 == 1) {
            const std::string key = root_ + "/batch_stable_kv_" + std::to_string(i);
            FalconErrorCode ret = client_->KvPut(key, 4096, KvValueKeys(33000 + i), KvLocations(), KvSizes());
            if (ret != SUCCESS) {
                return ret;
            }
            MetadataUtClient::KvValue value;
            ret = client_->KvGet(key, &value);
            if (ret != SUCCESS || value.sliceNum != 2) {
                return PROGRAM_ERROR;
            }
            return client_->KvDel(key);
        }

        const std::string filename = root_ + "/batch_stable_slice_" + std::to_string(i);
        const uint64_t inodeId = 991000 + i;
        FalconErrorCode ret = client_->SlicePut(filename,
                                                {inodeId},
                                                {0},
                                                {static_cast<uint64_t>(34000 + i)},
                                                {4096},
                                                {0},
                                                {4096},
                                                {1},
                                                {static_cast<uint32_t>(i)});
        if (ret != SUCCESS) {
            return ret;
        }
        MetadataUtClient::SliceValue value;
        ret = client_->SliceGet(filename, inodeId, 0, &value);
        if (ret != SUCCESS || value.sliceNum != 1) {
            return PROGRAM_ERROR;
        }
        return client_->SliceDel(filename, inodeId, 0);
    });

    EXPECT_EQ(kThreadNum, success);
    EXPECT_EQ(0, failure);
}

TEST_F(FalconMetadataUT, ConcurrentFetchSliceIdRangesDoNotOverlap)
{
    constexpr int kThreadNum = 8;
    constexpr uint32_t kCountPerThread = 8;

    std::vector<std::pair<uint64_t, uint64_t>> ranges;
    ranges.reserve(kThreadNum);
    std::mutex rangesMutex;
    std::atomic<int> failures = 0;

    std::vector<std::thread> threads;
    threads.reserve(kThreadNum);
    for (int i = 0; i < kThreadNum; ++i) {
        threads.emplace_back([&]() {
            uint64_t startId = 0;
            uint64_t endId = 0;
            FalconErrorCode ret = client_->FetchSliceId(kCountPerThread, &startId, &endId);
            if (ret != SUCCESS || endId != startId + kCountPerThread) {
                failures.fetch_add(1, std::memory_order_relaxed);
                return;
            }
            std::lock_guard<std::mutex> lock(rangesMutex);
            ranges.emplace_back(startId, endId);
        });
    }

    for (auto &thread : threads) {
        thread.join();
    }

    ASSERT_EQ(0, failures.load());
    ASSERT_EQ(static_cast<size_t>(kThreadNum), ranges.size());
    std::sort(ranges.begin(), ranges.end());
    for (size_t i = 1; i < ranges.size(); ++i) {
        EXPECT_LE(ranges[i - 1].second, ranges[i].first);
    }
}

TEST_F(FalconMetadataUT, ConcurrentFetchSliceIdTypeIsolation)
{
    constexpr int kThreadNum = 12;
    constexpr uint32_t kCountPerThread = 32;
    std::vector<std::pair<uint64_t, uint64_t>> kvRanges;
    std::vector<std::pair<uint64_t, uint64_t>> fileRanges;
    std::mutex mutex;
    std::atomic<int> failures = 0;

    std::vector<std::thread> threads;
    threads.reserve(kThreadNum * 2);
    for (int type = 0; type <= 1; ++type) {
        for (int i = 0; i < kThreadNum; ++i) {
            threads.emplace_back([&, type]() {
                uint64_t startId = 0;
                uint64_t endId = 0;
                FalconErrorCode ret = client_->FetchSliceId(kCountPerThread, &startId, &endId, static_cast<uint8_t>(type));
                if (ret != SUCCESS || endId != startId + kCountPerThread) {
                    failures.fetch_add(1, std::memory_order_relaxed);
                    return;
                }
                std::lock_guard<std::mutex> lock(mutex);
                if (type == 0) {
                    kvRanges.emplace_back(startId, endId);
                } else {
                    fileRanges.emplace_back(startId, endId);
                }
            });
        }
    }
    for (auto &thread : threads) {
        thread.join();
    }

    ASSERT_EQ(0, failures.load());
    ASSERT_EQ(static_cast<size_t>(kThreadNum), kvRanges.size());
    ASSERT_EQ(static_cast<size_t>(kThreadNum), fileRanges.size());
    std::sort(kvRanges.begin(), kvRanges.end());
    std::sort(fileRanges.begin(), fileRanges.end());
    for (size_t i = 1; i < kvRanges.size(); ++i) {
        EXPECT_LE(kvRanges[i - 1].second, kvRanges[i].first);
        EXPECT_LE(fileRanges[i - 1].second, fileRanges[i].first);
    }
}

} // namespace

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
