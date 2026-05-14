#include <fcntl.h>
#include <unistd.h>

#include <arpa/inet.h>
#include <brpc/server.h>
#include <filesystem>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <vector>

#include <gtest/gtest.h>

#include "disk_cache/disk_cache.h"
#include "write_stream/stream_assembler.h"

namespace {

class WriteStreamRemoteIOService : public falcon::brpc_io::RemoteIOService {
  public:
    explicit WriteStreamRemoteIOService(int errorCode)
        : errorCode_(errorCode)
    {
    }

    void WriteFile(google::protobuf::RpcController *cntlBase,
                   const falcon::brpc_io::WriteRequest *,
                   falcon::brpc_io::WriteReply *response,
                   google::protobuf::Closure *done) override
    {
        brpc::ClosureGuard doneGuard(done);
        auto *cntl = static_cast<brpc::Controller *>(cntlBase);
        response->set_error_code(errorCode_);
        response->set_write_size(cntl->request_attachment().size());
    }

    void CloseFile(google::protobuf::RpcController *,
                   const falcon::brpc_io::CloseRequest *,
                   falcon::brpc_io::ErrorCodeOnlyReply *response,
                   google::protobuf::Closure *done) override
    {
        brpc::ClosureGuard doneGuard(done);
        response->set_error_code(errorCode_);
    }

  private:
    int errorCode_;
};

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

int GetUnusedLoopbackPortForWriteStream()
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }
    sockaddr_in addr {};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    if (bind(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) != 0) {
        close(fd);
        return -1;
    }
    socklen_t len = sizeof(addr);
    if (getsockname(fd, reinterpret_cast<sockaddr *>(&addr), &len) != 0) {
        close(fd);
        return -1;
    }
    int port = ntohs(addr.sin_port);
    close(fd);
    return port;
}

} // namespace

TEST(WriteStreamCoverageUT, LocalDirectAndPersistErrorBranches)
{
    /* Exercise local Direct And Persist Error branches and assert the relevant success or failure branch. */
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

TEST(WriteStreamCoverageUT, LocalNonDirectPushAndEmptyPersistBranches)
{
    /* Exercise local Non Direct Push And Empty Persist branches and assert the relevant success or failure branch. */
    PrepareDiskCacheForWriteStream();
    auto &cache = DiskCache::GetInstance();
    uint64_t inode = 7101;
    cache.InsertAndUpdate(inode, 0, false);

    int fd = OpenTmpFile("non_direct_push");
    ASSERT_GE(fd, 0);

    WriteStream stream;
    stream.SetFd(fd);
    stream.SetInodeId(inode);

    std::string payload = "non-direct payload";
    FalconWriteBuffer empty{payload.data(), 0};
    FalconWriteBuffer buffer{payload.data(), payload.size()};
    EXPECT_EQ(stream.Push(empty, 0, 0), 0);
    EXPECT_EQ(stream.Push(buffer, 0, 0), 0);
    EXPECT_EQ(stream.PersistToFile(payload.data(), 0, 0, payload.size()), 0);

    close(fd);
}

TEST(WriteStreamCoverageUT, LocalPersistCoversPwriteAndDiskCacheFailures)
{
    /* Exercise local Persist covers Pwrite And Disk Cache Failures and assert the relevant success or failure branch. */
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
    /* Exercise Complete Persists Buffered local Data and assert the relevant success or failure branch. */
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
    /* Exercise remote Buffered Push And Set Fd branches and assert the relevant success or failure branch. */
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

TEST(WriteStreamCoverageUT, RemoteClientSuccessAndErrorBranches)
{
    /* Exercise remote Client Success And Error branches and assert the relevant success or failure branch. */
    for (int errorCode : {0, -EIO}) {
        int port = GetUnusedLoopbackPortForWriteStream();
        ASSERT_GT(port, 0);

        WriteStreamRemoteIOService service(errorCode);
        brpc::Server server;
        ASSERT_EQ(server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE), 0);
        ASSERT_EQ(server.Start(port, nullptr), 0);

        auto channel = std::make_shared<brpc::Channel>();
        brpc::ChannelOptions options;
        ASSERT_EQ(channel->Init(("127.0.0.1:" + std::to_string(port)).c_str(), &options), 0);
        auto client = std::make_shared<FalconIOClient>(channel);

        WriteStream stream;
        stream.SetClient(client);
        ASSERT_EQ(stream.SetFd(4242), 0);

        std::string payload = "remote-write-stream";
        FalconWriteBuffer first{payload.data(), 6};
        FalconWriteBuffer second{payload.data() + 6, payload.size() - 6};
        EXPECT_EQ(stream.Push(first, 0, 0), 0);
        EXPECT_EQ(stream.Push(second, 6, 6), 0);
        EXPECT_EQ(stream.GetSize(), payload.size());
        EXPECT_EQ(stream.Complete(payload.size(), true, false), errorCode);
        EXPECT_EQ(stream.GetSize(), 0U);
        EXPECT_EQ(stream.Complete(payload.size(), false, false), errorCode);
        EXPECT_EQ(stream.PersistToFile(payload.data(), payload.size(), 0, 0), errorCode);

        server.Stop(0);
        server.Join();
    }
}

TEST(WriteStreamCoverageUT, MemoryAndSliceHelpersCoverInlineBranches)
{
    /* Exercise Memory And Slice Helpers Cover Inline branches and assert the relevant success or failure branch. */
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
