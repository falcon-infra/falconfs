#include <fcntl.h>
#include <unistd.h>

#include <filesystem>
#include <string>

#include <gtest/gtest.h>

#define private public
#include "disk_cache/disk_cache.h"
#include "write_stream/stream_assembler.h"
#undef private

namespace {

void ResetDiskCacheForWriteStream(uint64_t freeCap = 1024 * 1024)
{
    auto &cache = DiskCache::GetInstance();
    cache.stop = false;
    cache.rootDir = "/tmp/write_stream_coverage";
    cache.totalDirNum = 1;
    cache.freeRatio = 0.1;
    cache.bgFreeRatio = 0.1;
    cache.totalCap = freeCap * 2;
    cache.totalInodes = 1024;
    cache.freeInodes = 1024;
    cache.blockRatio = 0.9;
    cache.inodeRatio = 0.9;
    cache.freeCap.store(freeCap);
    cache.usedCap = 0;
    cache.reservedCap.store(0);
    cache.hasFreeSpace.store(true);
    cache.inodeToCacheIter.clear();
    cache.cacheItems.clear();
    std::filesystem::create_directories(cache.rootDir + "/0");
}

int OpenTmpFile(const std::string &name)
{
    std::filesystem::create_directories("/tmp/write_stream_coverage");
    std::string path = "/tmp/write_stream_coverage/" + name;
    int fd = open(path.c_str(), O_CREAT | O_TRUNC | O_RDWR, 0600);
    EXPECT_GE(fd, 0);
    return fd;
}

WriteStream::MergedSlice MakeMergedSlice(const std::string &payload, off_t offset)
{
    char *buf = static_cast<char *>(malloc(payload.size()));
    EXPECT_NE(buf, nullptr);
    memcpy(buf, payload.data(), payload.size());
    ExpandableMemory memory(buf, payload.size());
    WriteStream::Slice slice(memory, payload.size(), offset);
    return WriteStream::MergedSlice(std::move(slice));
}

} // namespace

TEST(WriteStreamCoverageUT, LocalDirectAndPersistErrorBranches)
{
    ResetDiskCacheForWriteStream();
    auto &cache = DiskCache::GetInstance();
    uint64_t inode = 7001;
    cache.InsertAndUpdate(inode, 0, false);

    int fd = OpenTmpFile("direct_write");
    ASSERT_GE(fd, 0);

    WriteStream stream;
    stream.SetFd(fd);
    stream.SetInodeId(inode);
    stream.SetDirect(true);

    std::string payload = "direct payload";
    FalconWriteBuffer buffer{payload.data(), payload.size()};
    EXPECT_EQ(stream.Push(buffer, 0, 0), 0);
    EXPECT_EQ(stream.GetSize(), 0U);

    EXPECT_EQ(stream.PersistToFile(nullptr, payload.size(), 0, payload.size()), -EINVAL);

    WriteStream missingFd;
    EXPECT_EQ(missingFd.Persist(0), -EBADF);

    close(fd);
}

TEST(WriteStreamCoverageUT, LocalPersistCoversPwriteAndDiskCacheFailures)
{
    ResetDiskCacheForWriteStream();
    std::string payload = "persist";

    WriteStream badFd;
    badFd.SetInodeId(8001);
    EXPECT_EQ(badFd.PersistToFile(payload.data(), payload.size(), 0, 0), -EBADF);

    int fd = OpenTmpFile("disk_cache_add_failure");
    ASSERT_GE(fd, 0);
    WriteStream noCacheItem;
    noCacheItem.SetFd(fd);
    noCacheItem.SetInodeId(8002);
    EXPECT_EQ(noCacheItem.PersistToFile(payload.data(), payload.size(), 0, 0), -ENOENT);
    close(fd);

    ResetDiskCacheForWriteStream(1);
    auto &cache = DiskCache::GetInstance();
    cache.rootDir = "/tmp/write_stream_coverage_missing_root";
    int fullFd = OpenTmpFile("prealloc_failure");
    ASSERT_GE(fullFd, 0);
    WriteStream fullCache;
    fullCache.SetFd(fullFd);
    fullCache.SetInodeId(8003);
    EXPECT_EQ(fullCache.PersistToFile(payload.data(), payload.size(), 0, 0), -ENOSPC);
    close(fullFd);
}

TEST(WriteStreamCoverageUT, CompletePersistsBufferedLocalData)
{
    ResetDiskCacheForWriteStream();
    auto &cache = DiskCache::GetInstance();
    uint64_t inode = 9001;
    cache.InsertAndUpdate(inode, 0, false);

    int fd = OpenTmpFile("complete_buffered");
    ASSERT_GE(fd, 0);

    WriteStream stream;
    stream.SetFd(fd);
    stream.SetInodeId(inode);
    std::string payload = "buffered-data";
    ASSERT_TRUE(stream.data.Append(payload.data(), payload.size(), 0));
    EXPECT_EQ(stream.Complete(0, true, true), 0);
    EXPECT_EQ(stream.GetSize(), 0U);
    close(fd);
}

TEST(WriteStreamCoverageUT, MergeCoversDisjointPreviousAndFollowingOverlaps)
{
    WriteStream stream;

    EXPECT_EQ(stream.Merge(MakeMergedSlice("aaaa", 0)), 4);
    EXPECT_EQ(stream.stream.size(), 1U);

    EXPECT_EQ(stream.Merge(MakeMergedSlice("bbbb", 20)), 4);
    EXPECT_EQ(stream.stream.size(), 2U);

    EXPECT_GT(stream.Merge(MakeMergedSlice("prev-overlap", 2)), 0);
    EXPECT_EQ(stream.stream.size(), 2U);

    EXPECT_EQ(stream.Merge(MakeMergedSlice("cccccccccc", 40)), 10);
    EXPECT_GT(stream.Merge(MakeMergedSlice("next", 38)), 0);
    EXPECT_EQ(stream.stream.size(), 3U);
}

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
