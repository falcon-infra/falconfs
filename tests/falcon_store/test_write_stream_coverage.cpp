#include <fcntl.h>
#include <unistd.h>

#include <filesystem>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "disk_cache/disk_cache.h"
#include "write_stream/stream_assembler.h"

namespace {

void PrepareDiskCacheForWriteStream()
{
    static bool started = false;
    if (started) {
        return;
    }
    std::string root = "/tmp/write_stream_coverage";
    std::filesystem::remove_all(root);
    std::filesystem::create_directories(root + "/0");
    (void)DiskCache::GetInstance().Start(root, 1, 0.1, 0.1);
    started = true;
}

int OpenTmpFile(const std::string &name)
{
    std::filesystem::create_directories("/tmp/write_stream_coverage");
    std::string path = "/tmp/write_stream_coverage/" + name;
    int fd = open(path.c_str(), O_CREAT | O_TRUNC | O_RDWR, 0600);
    EXPECT_GE(fd, 0);
    return fd;
}

} // namespace

TEST(WriteStreamCoverageUT, LocalDirectAndPersistErrorBranches)
{
    PrepareDiskCacheForWriteStream();
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
    PrepareDiskCacheForWriteStream();
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

}

TEST(WriteStreamCoverageUT, CompletePersistsBufferedLocalData)
{
    PrepareDiskCacheForWriteStream();
    auto &cache = DiskCache::GetInstance();
    uint64_t inode = 9001;
    cache.InsertAndUpdate(inode, 0, false);

    int fd = OpenTmpFile("complete_buffered");
    ASSERT_GE(fd, 0);

    WriteStream stream;
    stream.SetFd(fd);
    stream.SetInodeId(inode);
    std::string payload = "buffered-data";
    FalconWriteBuffer buffer{payload.data(), payload.size()};
    EXPECT_EQ(stream.Push(buffer, 0, 0), 0);
    EXPECT_EQ(stream.Complete(0, true, true), 0);
    EXPECT_EQ(stream.GetSize(), 0U);
    close(fd);
}

TEST(WriteStreamCoverageUT, RemoteBufferedPushAndSetFdBranches)
{
    WriteStream stream;
    auto fakeClient = std::shared_ptr<FalconIOClient>(reinterpret_cast<FalconIOClient *>(0x1), [](FalconIOClient *) {});
    stream.SetClient(nullptr);
    stream.SetClient(fakeClient);

    std::string first = "abc";
    std::string second = "def";
    FalconWriteBuffer empty{first.data(), 0};
    FalconWriteBuffer firstBuffer{first.data(), first.size()};
    FalconWriteBuffer secondBuffer{second.data(), second.size()};

    EXPECT_EQ(stream.Push(empty, 0, 0), 0);
    EXPECT_EQ(stream.Push(firstBuffer, 0, 0), 0);
    EXPECT_EQ(stream.GetSize(), first.size());
    EXPECT_EQ(stream.Push(secondBuffer, first.size(), first.size()), 0);
    EXPECT_EQ(stream.GetSize(), first.size() + second.size());

    WriteStream fdStream;
    EXPECT_EQ(fdStream.SetFd(123), 0);
    EXPECT_EQ(fdStream.SetFd(123), 0);
    EXPECT_EQ(fdStream.SetFd(124), -EBADF);
}

TEST(WriteStreamCoverageUT, MemoryAndSliceHelpersCoverInlineBranches)
{
    ExpandableMemory memory;
    EXPECT_TRUE(memory.Empty());
    EXPECT_TRUE(memory.Append("abc", 3));
    EXPECT_FALSE(memory.Empty());
    EXPECT_EQ(memory.Size(), 3U);
    EXPECT_EQ(std::string(memory.Get().get(), memory.Get().get() + 3), "abc");

    EXPECT_TRUE(memory.Reserve(16));
    ExpandableMemory replacement;
    EXPECT_TRUE(replacement.Append("XY", 2));
    EXPECT_TRUE(memory.Replace(1, 2, replacement));
    EXPECT_EQ(std::string(memory.Get().get(), memory.Get().get() + 3), "aXY");
    memory.Clear();
    EXPECT_TRUE(memory.Empty());
    memory.Clean();
    EXPECT_EQ(memory.Get(), nullptr);
    EXPECT_EQ(memory.Size(), 0U);

    ExpandableMemory backing;
    ASSERT_TRUE(backing.Append("hello", 5));
    WriteStream::MergedSlice singleMerged(WriteStream::Slice(backing, 5, 10));
    EXPECT_EQ(singleMerged.Get().get(), backing.Get().get());

    ExpandableMemory left;
    ExpandableMemory right;
    ASSERT_TRUE(left.Append("ab", 2));
    ASSERT_TRUE(right.Append("CD", 2));
    WriteStream::MergedSlice leftMerged(WriteStream::Slice(left, 2, 4));
    WriteStream::MergedSlice rightMerged(WriteStream::Slice(right, 2, 6));
    std::vector<WriteStream::MergedSlice> toMerge;
    toMerge.push_back(std::move(rightMerged));
    toMerge.push_back(std::move(leftMerged));
    WriteStream::MergedSlice merged(std::move(toMerge));
    EXPECT_EQ(merged.offset, 4);
    EXPECT_EQ(merged.size, 4U);
    auto mergedData = merged.Get();
    ASSERT_NE(mergedData, nullptr);
    EXPECT_EQ(std::string(mergedData.get(), mergedData.get() + 4), "abCD");

    ExpandableMemory copyOut;
    merged.Get(copyOut);
    ASSERT_NE(copyOut.Get(), nullptr);
    EXPECT_EQ(std::string(copyOut.Get().get(), copyOut.Get().get() + 4), "abCD");

    WriteStream::SerialData serial;
    EXPECT_TRUE(serial.Empty());
    EXPECT_EQ(serial.End(), 0U);
    EXPECT_TRUE(serial.Append("zz", 2, 7));
    EXPECT_FALSE(serial.Empty());
    EXPECT_EQ(serial.End(), 9U);
    serial.Clear();
    EXPECT_TRUE(serial.Empty());
    EXPECT_EQ(serial.End(), 0U);
}

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
