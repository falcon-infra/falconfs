#include "test_falcon_store.h"

#include "buffer/dir_open_instance.h"
#include "connection/node.h"
#include "disk_cache/disk_cache.h"

#include <arpa/inet.h>
#include <brpc/server.h>
#include <netinet/in.h>
#include <sys/socket.h>

unsigned long myHash(std::string &str);

namespace {

void ResetFalconStatsForCoverage()
{
    for (int i = 0; i < STATS_END; ++i) {
        FalconStats::GetInstance().stats[i] = 0;
        FalconStats::GetInstance().storedStats[i] = 0;
    }
}

int GetUnusedLoopbackPortForStoreCoverage()
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

class MockRemoteIOService : public falcon::brpc_io::RemoteIOService {
  public:
    enum class Mode { Success, Error, BadSize };

    explicit MockRemoteIOService(Mode mode)
        : mode_(mode)
    {
    }

    void OpenFile(google::protobuf::RpcController *,
                  const falcon::brpc_io::OpenRequest *,
                  falcon::brpc_io::OpenReply *response,
                  google::protobuf::Closure *done) override
    {
        brpc::ClosureGuard doneGuard(done);
        response->set_error_code(mode_ == Mode::Error ? -EIO : 0);
        response->set_physical_fd(4242);
    }

    void CloseFile(google::protobuf::RpcController *,
                   const falcon::brpc_io::CloseRequest *,
                   falcon::brpc_io::ErrorCodeOnlyReply *response,
                   google::protobuf::Closure *done) override
    {
        brpc::ClosureGuard doneGuard(done);
        response->set_error_code(mode_ == Mode::Error ? -EIO : 0);
    }

    void ReadFile(google::protobuf::RpcController *cntlBase,
                  const falcon::brpc_io::ReadRequest *request,
                  falcon::brpc_io::ErrorCodeOnlyReply *response,
                  google::protobuf::Closure *done) override
    {
        brpc::ClosureGuard doneGuard(done);
        response->set_error_code(mode_ == Mode::Error ? -EIO : 0);
        if (mode_ != Mode::Error) {
            auto *cntl = static_cast<brpc::Controller *>(cntlBase);
            std::string payload(mode_ == Mode::BadSize ? request->read_size() + 1 : request->read_size(), 'r');
            cntl->response_attachment().append(payload);
        }
    }

    void ReadSmallFile(google::protobuf::RpcController *cntlBase,
                       const falcon::brpc_io::ReadSmallFileRequest *request,
                       falcon::brpc_io::ErrorCodeOnlyReply *response,
                       google::protobuf::Closure *done) override
    {
        brpc::ClosureGuard doneGuard(done);
        response->set_error_code(mode_ == Mode::Error ? -EIO : 0);
        if (mode_ != Mode::Error) {
            auto *cntl = static_cast<brpc::Controller *>(cntlBase);
            size_t payloadSize = mode_ == Mode::BadSize ? request->read_size() + 1 : request->read_size();
            cntl->response_attachment().append(std::string(payloadSize, 's'));
        }
    }

    void WriteFile(google::protobuf::RpcController *cntlBase,
                   const falcon::brpc_io::WriteRequest *,
                   falcon::brpc_io::WriteReply *response,
                   google::protobuf::Closure *done) override
    {
        brpc::ClosureGuard doneGuard(done);
        auto *cntl = static_cast<brpc::Controller *>(cntlBase);
        response->set_error_code(mode_ == Mode::Error ? -EIO : 0);
        response->set_write_size(mode_ == Mode::BadSize ? cntl->request_attachment().size() + 1
                                                        : cntl->request_attachment().size());
    }

    void DeleteFile(google::protobuf::RpcController *,
                    const falcon::brpc_io::DeleteRequest *,
                    falcon::brpc_io::ErrorCodeOnlyReply *response,
                    google::protobuf::Closure *done) override
    {
        brpc::ClosureGuard doneGuard(done);
        response->set_error_code(mode_ == Mode::Error ? -EIO : 0);
    }

    void StatFS(google::protobuf::RpcController *,
                const falcon::brpc_io::StatFSRequest *,
                falcon::brpc_io::StatFSReply *response,
                google::protobuf::Closure *done) override
    {
        brpc::ClosureGuard doneGuard(done);
        response->set_error_code(mode_ == Mode::Error ? -EIO : 0);
        response->set_fblocks(11);
        response->set_fbfree(12);
        response->set_fbavail(13);
        response->set_ffiles(14);
        response->set_fffree(15);
    }

    void TruncateOpenInstance(google::protobuf::RpcController *,
                              const falcon::brpc_io::TruncateOpenInstanceRequest *,
                              falcon::brpc_io::ErrorCodeOnlyReply *response,
                              google::protobuf::Closure *done) override
    {
        brpc::ClosureGuard doneGuard(done);
        response->set_error_code(mode_ == Mode::Error ? -EIO : 0);
    }

    void TruncateFile(google::protobuf::RpcController *,
                      const falcon::brpc_io::TruncateFileRequest *,
                      falcon::brpc_io::ErrorCodeOnlyReply *response,
                      google::protobuf::Closure *done) override
    {
        brpc::ClosureGuard doneGuard(done);
        response->set_error_code(mode_ == Mode::Error ? -EIO : 0);
    }

    void CheckConnection(google::protobuf::RpcController *,
                         const falcon::brpc_io::CheckConnectionRequest *,
                         falcon::brpc_io::ErrorCodeOnlyReply *response,
                         google::protobuf::Closure *done) override
    {
        brpc::ClosureGuard doneGuard(done);
        response->set_error_code(mode_ == Mode::Error ? -EIO : 0);
    }

    void StatCluster(google::protobuf::RpcController *,
                     const falcon::brpc_io::StatClusterRequest *,
                     falcon::brpc_io::StatClusterReply *response,
                     google::protobuf::Closure *done) override
    {
        brpc::ClosureGuard doneGuard(done);
        response->set_error_code(mode_ == Mode::Error ? -EIO : 0);
        for (int i = 0; i < STATS_END; ++i) {
            response->add_stats(i + 1);
        }
    }

  private:
    Mode mode_;
};

} // namespace

std::shared_ptr<FalconConfig> FalconStoreUT::config = GetInit().GetFalconConfig();
std::shared_ptr<OpenInstance> FalconStoreUT::openInstance = nullptr;
char *FalconStoreUT::writeBuf = nullptr;
size_t FalconStoreUT::size = 0;
char *FalconStoreUT::readBuf = nullptr;
size_t FalconStoreUT::readSize = 0;
char *FalconStoreUT::readBuf2 = nullptr;

/* ------------------------------------------- open local -------------------------------------------*/

TEST_F(FalconStoreUT, CreateLocalWRonly)
{
    /* Exercise Create local Write Only and assert the relevant success or failure branch. */
    NewOpenInstance(100, StoreNode::GetInstance()->GetNodeId(), "/OpenLocal", O_WRONLY | O_CREAT);

    int ret = FalconStore::GetInstance()->OpenFile(openInstance.get());
    EXPECT_EQ(ret, 0);
}

TEST_F(FalconStoreUT, OpenLocalWRonlyExist)
{
    /* Exercise Open local Write Only Exist and assert the relevant success or failure branch. */
    NewOpenInstance(100, StoreNode::GetInstance()->GetNodeId(), "/OpenLocal", O_WRONLY);

    int ret = FalconStore::GetInstance()->OpenFile(openInstance.get());
    EXPECT_EQ(ret, 0);
}

TEST_F(FalconStoreUT, OpenLocalWRonlyNoneExist)
{
    /* Exercise Open local Write Only missing and assert the relevant success or failure branch. */
    NewOpenInstance(101, StoreNode::GetInstance()->GetNodeId(), "/OpenLocalNoneExist", O_WRONLY);
    openInstance->originalSize = 1;

    int ret = FalconStore::GetInstance()->OpenFile(openInstance.get());
    if (config->GetBool(FalconPropertyKey::FALCON_PERSIST)) {
        EXPECT_EQ(ret, -EIO);
    } else {
        EXPECT_EQ(ret, -ENOENT);
    }
}

TEST_F(FalconStoreUT, OpenLocalRDonlyExist)
{
    /* Exercise Open local Read Only Exist and assert the relevant success or failure branch. */
    NewOpenInstance(100, StoreNode::GetInstance()->GetNodeId(), "/OpenLocal", O_RDONLY);

    int ret = FalconStore::GetInstance()->OpenFile(openInstance.get());
    EXPECT_EQ(ret, 0);
}

TEST_F(FalconStoreUT, OpenLocalRDonlyNoneExist)
{
    /* Exercise Open local Read Only missing and assert the relevant success or failure branch. */
    NewOpenInstance(101, StoreNode::GetInstance()->GetNodeId(), "/OpenLocalNoneExist", O_RDONLY);
    openInstance->originalSize = 1;

    int ret = FalconStore::GetInstance()->OpenFile(openInstance.get());
    if (config->GetBool(FalconPropertyKey::FALCON_PERSIST)) {
        EXPECT_EQ(ret, 0);
    } else {
        EXPECT_EQ(ret, -ENOENT);
    }
}

TEST_F(FalconStoreUT, OpenLocalRDWRExist)
{
    /* Exercise Open local Read Write Exist and assert the relevant success or failure branch. */
    NewOpenInstance(100, StoreNode::GetInstance()->GetNodeId(), "/OpenLocal", O_RDWR);

    int ret = FalconStore::GetInstance()->OpenFile(openInstance.get());
    EXPECT_EQ(ret, 0);
}

TEST_F(FalconStoreUT, OpenLocalRDWRNoneExist)
{
    /* Exercise Open local Read Write missing and assert the relevant success or failure branch. */
    NewOpenInstance(101, StoreNode::GetInstance()->GetNodeId(), "/OpenLocalNoneExist", O_RDWR);

    openInstance->originalSize = 1;

    int ret = FalconStore::GetInstance()->OpenFile(openInstance.get());
    if (config->GetBool(FalconPropertyKey::FALCON_PERSIST)) {
        EXPECT_EQ(ret, 0);
    } else {
        EXPECT_EQ(ret, -ENOENT);
    }
}

/* ------------------------------------------- open remote -------------------------------------------*/

TEST_F(FalconStoreUT, CreateRemoteWRonly)
{
    /* Exercise Create remote Write Only and assert the relevant success or failure branch. */
    NewOpenInstance(200, StoreNode::GetInstance()->GetNodeId() + 1, "/OpenRemote", O_WRONLY | O_CREAT);

    int ret = FalconStore::GetInstance()->OpenFile(openInstance.get());
    EXPECT_EQ(ret, 0);
}

TEST_F(FalconStoreUT, OpenRemoteWRonlyExist)
{
    /* Exercise Open remote Write Only Exist and assert the relevant success or failure branch. */
    NewOpenInstance(200, StoreNode::GetInstance()->GetNodeId() + 1, "/OpenRemote", O_WRONLY);

    int ret = FalconStore::GetInstance()->OpenFile(openInstance.get());
    EXPECT_EQ(ret, 0);
}

TEST_F(FalconStoreUT, OpenRemoteWRonlyNoneExist)
{
    /* Exercise Open remote Write Only missing and assert the relevant success or failure branch. */
    NewOpenInstance(201, StoreNode::GetInstance()->GetNodeId() + 1, "/OpenRemoteNoneExist", O_WRONLY);
    openInstance->originalSize = 1;

    int ret = FalconStore::GetInstance()->OpenFile(openInstance.get());
    if (config->GetBool(FalconPropertyKey::FALCON_PERSIST)) {
        EXPECT_EQ(ret, -EIO);
    } else {
        EXPECT_EQ(ret, -ENOENT);
    }
}

TEST_F(FalconStoreUT, OpenRemoteRDonlyExist)
{
    /* Exercise Open remote Read Only Exist and assert the relevant success or failure branch. */
    NewOpenInstance(200, StoreNode::GetInstance()->GetNodeId() + 1, "/OpenRemote", O_RDONLY);

    int ret = FalconStore::GetInstance()->OpenFile(openInstance.get());
    EXPECT_EQ(ret, 0);
}

TEST_F(FalconStoreUT, OpenRemoteRDonlyNoneExist)
{
    /* Exercise Open remote Read Only missing and assert the relevant success or failure branch. */
    NewOpenInstance(201, StoreNode::GetInstance()->GetNodeId() + 1, "/OpenRemoteNoneExist", O_RDONLY);
    openInstance->originalSize = 1;

    int ret = FalconStore::GetInstance()->OpenFile(openInstance.get());
    if (config->GetBool(FalconPropertyKey::FALCON_PERSIST)) {
        EXPECT_EQ(ret, 0);
    } else {
        EXPECT_EQ(ret, -ENOENT);
    }
}

TEST_F(FalconStoreUT, OpenRemoteRDWRExist)
{
    /* Exercise Open remote Read Write Exist and assert the relevant success or failure branch. */
    NewOpenInstance(200, StoreNode::GetInstance()->GetNodeId() + 1, "/OpenRemote", O_RDWR);

    int ret = FalconStore::GetInstance()->OpenFile(openInstance.get());
    EXPECT_EQ(ret, 0);
}

TEST_F(FalconStoreUT, OpenRemoteRDWRNoneExist)
{
    /* Exercise Open remote Read Write missing and assert the relevant success or failure branch. */
    NewOpenInstance(201, StoreNode::GetInstance()->GetNodeId() + 1, "/OpenRemoteNoneExist", O_RDWR);
    openInstance->originalSize = 1;

    int ret = FalconStore::GetInstance()->OpenFile(openInstance.get());
    if (config->GetBool(FalconPropertyKey::FALCON_PERSIST)) {
        EXPECT_EQ(ret, -EIO);
    } else {
        EXPECT_EQ(ret, -ENOENT);
    }
}

TEST_F(FalconStoreUT, OpenStats)
{
    /* Exercise Open Stats and assert the relevant success or failure branch. */
    std::vector<size_t> stats(STATS_END);
    int ret = client->StatCluster(-1, stats, true);
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(stats[FUSE_READ_OPS], 0);
    EXPECT_EQ(stats[FUSE_WRITE_OPS], 0);
    EXPECT_EQ(stats[FUSE_READ], 0);
    EXPECT_EQ(stats[FUSE_WRITE], 0);
    EXPECT_EQ(stats[BLOCKCACHE_READ], 0);
    EXPECT_EQ(stats[BLOCKCACHE_WRITE], 0);
    EXPECT_EQ(stats[OBJ_GET], 0);
    EXPECT_EQ(stats[OBJ_PUT], 0);
}

TEST_F(FalconStoreUT, ParentPathHelper)
{
    /* Exercise Parent Path Helper and assert the relevant success or failure branch. */
    EXPECT_EQ(GetParentPath("/a/b/c", -1), "/a/b");
    EXPECT_EQ(GetParentPath("/a/b/c", 1), "/");
    EXPECT_EQ(GetParentPath("/a/b/c", 2), "/a/");
    EXPECT_EQ(GetParentPath("/a/b/c", 5), "/a/b/");
}

TEST_F(FalconStoreUT, PublicHelpersDoNotRequirePrivateAccess)
{
    /* Exercise public Helpers Do Not Require Private Access and assert the relevant success or failure branch. */
    auto *store = FalconStore::GetInstance();
    std::string nodeConfig = "coverage-node-config";
    store->SetFalconStoreParam(nodeConfig);
    std::string path = "/private/helper/file";
    EXPECT_NE(myHash(path), 0UL);
}

TEST_F(FalconStoreUT, BrpcWritePublicBranches)
{
    /* Exercise BRPC Write public branches and assert the relevant success or failure branch. */
    auto *store = FalconStore::GetInstance();
    const std::string payload = "brpc-nondirect-payload";

    NewOpenInstance(912799, StoreNode::GetInstance()->GetNodeId(), "/brpc/write-prealloc-fail", O_WRONLY | O_CREAT);
    openInstance->physicalFd = static_cast<uint64_t>(-1);
    butil::IOBuf preallocFailBuf;
    preallocFailBuf.append(payload);
    EXPECT_LT(store->WriteLocalFileForBrpc(openInstance.get(), preallocFailBuf, static_cast<off_t>(1ULL << 50)), 0);

    NewOpenInstance(912800, StoreNode::GetInstance()->GetNodeId(), "/brpc/write-ok", O_WRONLY | O_CREAT);
    std::string writeFile = GetFilePath(openInstance->inodeId);
    int fd = open(writeFile.c_str(), O_CREAT | O_RDWR, 0755);
    ASSERT_GE(fd, 0);
    openInstance->physicalFd = fd;
    DiskCache::GetInstance().InsertAndUpdate(openInstance->inodeId, 0, true);

    butil::IOBuf ioBuf;
    ioBuf.append(payload);
    EXPECT_EQ(store->WriteLocalFileForBrpc(openInstance.get(), ioBuf, 0), 0);
    EXPECT_EQ(openInstance->currentSize.load(), payload.size());
    close(fd);
    DiskCache::GetInstance().Unpin(openInstance->inodeId);

    NewOpenInstance(912801, StoreNode::GetInstance()->GetNodeId(), "/brpc/write-update-fail", O_WRONLY | O_CREAT);
    writeFile = GetFilePath(openInstance->inodeId);
    fd = open(writeFile.c_str(), O_CREAT | O_RDWR, 0755);
    ASSERT_GE(fd, 0);
    openInstance->physicalFd = fd;
    butil::IOBuf failingIoBuf;
    failingIoBuf.append(payload);
    EXPECT_EQ(store->WriteLocalFileForBrpc(openInstance.get(), failingIoBuf, 0), 0);
    close(fd);

    FalconStats::GetInstance().stats[BLOCKCACHE_WRITE] = 0;

    NewOpenInstance(912802, StoreNode::GetInstance()->GetNodeId(), "/brpc/direct-write-ok", O_WRONLY | O_CREAT | __O_DIRECT);
    writeFile = GetFilePath(openInstance->inodeId);
    fd = open(writeFile.c_str(), O_CREAT | O_RDWR, 0755);
    ASSERT_GE(fd, 0);
    openInstance->physicalFd = fd;
    DiskCache::GetInstance().InsertAndUpdate(openInstance->inodeId, 0, true);
    butil::IOBuf directIoBuf;
    directIoBuf.append(payload);
    EXPECT_EQ(store->WriteLocalFileForBrpc(openInstance.get(), directIoBuf, 0), 0);
    EXPECT_EQ(openInstance->currentSize.load(), payload.size());
    close(fd);
    DiskCache::GetInstance().Unpin(openInstance->inodeId);
    FalconStats::GetInstance().stats[BLOCKCACHE_WRITE] = 0;

    NewOpenInstance(912803, StoreNode::GetInstance()->GetNodeId(), "/brpc/direct-write-bad-fd", O_WRONLY | O_CREAT | __O_DIRECT);
    openInstance->physicalFd = static_cast<uint64_t>(-1);
    DiskCache::GetInstance().InsertAndUpdate(openInstance->inodeId, 0, true);
    butil::IOBuf directBadFdBuf;
    directBadFdBuf.append(payload);
    EXPECT_EQ(store->WriteLocalFileForBrpc(openInstance.get(), directBadFdBuf, 0), -EBADF);
    DiskCache::GetInstance().Unpin(openInstance->inodeId);

    NewOpenInstance(912804, StoreNode::GetInstance()->GetNodeId(), "/brpc/non-direct-write-bad-fd", O_WRONLY | O_CREAT);
    openInstance->physicalFd = static_cast<uint64_t>(-2);
    DiskCache::GetInstance().InsertAndUpdate(openInstance->inodeId, 0, true);
    butil::IOBuf nondirectBadFdBuf;
    nondirectBadFdBuf.append(payload);
    EXPECT_EQ(store->WriteLocalFileForBrpc(openInstance.get(), nondirectBadFdBuf, 0), -EINVAL);
    DiskCache::GetInstance().Unpin(openInstance->inodeId);
}

TEST_F(FalconStoreUT, CloseTmpFilesPublicBranches)
{
    /* Exercise Close Tmp Files public branches and assert the relevant success or failure branch. */
    auto *store = FalconStore::GetInstance();

    NewOpenInstance(912900, StoreNode::GetInstance()->GetNodeId(), "/close/no-fd", O_WRONLY | O_CREAT);
    openInstance->physicalFd = UINT64_MAX;
    EXPECT_EQ(store->CloseTmpFiles(openInstance.get(), true, false), 0);

    NewOpenInstance(912901, -1, "/close/no-node", O_WRONLY | O_CREAT);
    std::string closeFile = GetFilePath(openInstance->inodeId);
    int fd = open(closeFile.c_str(), O_CREAT | O_RDWR, 0755);
    ASSERT_GE(fd, 0);
    openInstance->physicalFd = fd;
    EXPECT_EQ(store->CloseTmpFiles(openInstance.get(), true, false), 0);
    close(fd);

    NewOpenInstance(912902, StoreNode::GetInstance()->GetNodeId(), "/close/local", O_WRONLY | O_CREAT);
    closeFile = GetFilePath(openInstance->inodeId);
    fd = open(closeFile.c_str(), O_CREAT | O_RDWR, 0755);
    ASSERT_GE(fd, 0);
    openInstance->physicalFd = fd;
    openInstance->currentSize = 4;
    DiskCache::GetInstance().InsertAndUpdate(openInstance->inodeId, 0, true);
    EXPECT_EQ(store->CloseTmpFiles(openInstance.get(), true, false), -EBADF);
    EXPECT_TRUE(openInstance->isFlushed);
    EXPECT_EQ(store->CloseTmpFiles(openInstance.get(), false, false), -EBADF);
    close(fd);
    DiskCache::GetInstance().Unpin(openInstance->inodeId);
}

TEST_F(FalconStoreUT, SmallFilePublicMissBranches)
{
    /* Exercise Small File public Miss branches and assert the relevant success or failure branch. */
    auto *store = FalconStore::GetInstance();
    const std::string payload = "small-miss-payload";

    NewOpenInstance(913000, StoreNode::GetInstance()->GetNodeId(), "/small/missing-cache", O_RDONLY);
    openInstance->readBufferSize = payload.size();
    openInstance->readBuffer.reset(new char[openInstance->readBufferSize], std::default_delete<char[]>());
    EXPECT_EQ(store->ReadSmallFiles(openInstance.get()), -ENOENT);

    NewOpenInstance(913001, StoreNode::GetInstance()->GetNodeId(), "/small/stale-cache", O_RDONLY);
    openInstance->readBufferSize = payload.size();
    openInstance->readBuffer.reset(new char[openInstance->readBufferSize], std::default_delete<char[]>());
    DiskCache::GetInstance().InsertAndUpdate(openInstance->inodeId, payload.size(), false);
    EXPECT_EQ(store->ReadSmallFiles(openInstance.get()), -ENOENT);

    NewOpenInstance(913005, StoreNode::GetInstance()->GetNodeId(), "/small/node-fail-stale-cache", O_RDONLY);
    openInstance->nodeFail = true;
    openInstance->readBufferSize = payload.size();
    openInstance->readBuffer.reset(new char[openInstance->readBufferSize], std::default_delete<char[]>());
    DiskCache::GetInstance().InsertAndUpdate(openInstance->inodeId, payload.size(), false);
    EXPECT_EQ(store->ReadSmallFiles(openInstance.get()), -ENOENT);

    char buffer[64] = {};
    DiskCache::GetInstance().InsertAndUpdate(913100, payload.size(), false);
    EXPECT_EQ(store->ReadSmallFilesForBrpc(913100, "/brpc/stale-cache", buffer, payload.size(), O_RDONLY, false),
              -ENOENT);

    DiskCache::GetInstance().InsertAndUpdate(913104, payload.size(), false);
    EXPECT_EQ(store->ReadSmallFilesForBrpc(913104, "/brpc/node-fail-stale-cache", buffer, payload.size(), O_RDONLY,
                                           true),
              -ENOENT);

    memset(buffer, 0, sizeof(buffer));
    EXPECT_EQ(store->ReadSmallFilesForBrpc(913101, "/brpc/no-storage", buffer, payload.size(), O_RDONLY, false),
              -ENOENT);

    NewOpenInstance(913002, StoreNode::GetInstance()->GetNodeId(), "/small/short-cache", O_RDONLY);
    openInstance->readBufferSize = payload.size();
    openInstance->readBuffer.reset(new char[openInstance->readBufferSize], std::default_delete<char[]>());
    std::string shortFile = GetFilePath(openInstance->inodeId);
    int fd = open(shortFile.c_str(), O_CREAT | O_RDWR, 0755);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(pwrite(fd, payload.data(), payload.size() - 1, 0), static_cast<ssize_t>(payload.size() - 1));
    close(fd);
    DiskCache::GetInstance().InsertAndUpdate(openInstance->inodeId, payload.size(), false);
    EXPECT_EQ(store->ReadSmallFiles(openInstance.get()), -ENOENT);

    uint64_t brpcShortInode = 913102;
    std::string brpcShortFile = GetFilePath(brpcShortInode);
    fd = open(brpcShortFile.c_str(), O_CREAT | O_RDWR, 0755);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(pwrite(fd, payload.data(), payload.size() - 1, 0), static_cast<ssize_t>(payload.size() - 1));
    close(fd);
    DiskCache::GetInstance().InsertAndUpdate(brpcShortInode, payload.size(), false);
    memset(buffer, 0, sizeof(buffer));
    EXPECT_EQ(store->ReadSmallFilesForBrpc(brpcShortInode, "/brpc/short-cache", buffer, payload.size(), O_RDONLY, false),
              -ENOENT);

    uint64_t brpcOkInode = 913103;
    std::string brpcOkFile = GetFilePath(brpcOkInode);
    fd = open(brpcOkFile.c_str(), O_CREAT | O_RDWR, 0755);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(pwrite(fd, payload.data(), payload.size(), 0), static_cast<ssize_t>(payload.size()));
    close(fd);
    DiskCache::GetInstance().InsertAndUpdate(brpcOkInode, payload.size(), false);
    memset(buffer, 0, sizeof(buffer));
    EXPECT_EQ(store->ReadSmallFilesForBrpc(brpcOkInode, "/brpc/cache-hit", buffer, payload.size(), O_RDONLY, false), 0);
    EXPECT_EQ(std::string(buffer, payload.size()), payload);

    NewOpenInstance(913004, StoreNode::GetInstance()->GetNodeId(), "/small/cache-hit", O_RDONLY);
    openInstance->readBufferSize = payload.size();
    openInstance->readBuffer.reset(new char[openInstance->readBufferSize], std::default_delete<char[]>());
    std::string cacheHitFile = GetFilePath(openInstance->inodeId);
    fd = open(cacheHitFile.c_str(), O_CREAT | O_RDWR, 0755);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(pwrite(fd, payload.data(), payload.size(), 0), static_cast<ssize_t>(payload.size()));
    close(fd);
    DiskCache::GetInstance().InsertAndUpdate(openInstance->inodeId, payload.size(), false);
    EXPECT_EQ(store->ReadSmallFiles(openInstance.get()), 0);
    EXPECT_EQ(std::string(openInstance->readBuffer.get(), payload.size()), payload);
    DiskCache::GetInstance().Unpin(openInstance->inodeId);

    FalconStats::GetInstance().stats[BLOCKCACHE_READ] = 0;
    FalconStats::GetInstance().stats[BLOCKCACHE_WRITE] = 0;
}

TEST_F(FalconStoreUT, OpenStatAndTruncatePublicBranches)
{
    /* Exercise Open Stat And Truncate public branches and assert the relevant success or failure branch. */
    auto *store = FalconStore::GetInstance();

    NewOpenInstance(913149, StoreNode::GetInstance()->GetNodeId(), "/open/stale-cache", O_RDONLY);
    DiskCache::GetInstance().InsertAndUpdate(openInstance->inodeId, 1, false);
    EXPECT_EQ(store->OpenFile(openInstance.get()), -ENOENT);

    NewOpenInstance(913150, StoreNode::GetInstance()->GetNodeId(), "/open/missing-cache-write-exists", O_WRONLY);
    openInstance->originalSize = 1;
    std::string path = GetFilePath(openInstance->inodeId);
    int fd = open(path.c_str(), O_CREAT | O_RDWR, 0755);
    ASSERT_GE(fd, 0);
    close(fd);
    EXPECT_EQ(store->OpenFile(openInstance.get()), 0);
    close(openInstance->physicalFd);
    unlink(path.c_str());

    NewOpenInstance(913151, StoreNode::GetInstance()->GetNodeId(), "/open/missing-cache-read-exists", O_RDONLY);
    openInstance->originalSize = 1;
    path = GetFilePath(openInstance->inodeId);
    fd = open(path.c_str(), O_CREAT | O_RDWR, 0755);
    ASSERT_GE(fd, 0);
    close(fd);
    EXPECT_EQ(store->OpenFile(openInstance.get()), 0);
    close(openInstance->physicalFd);
    unlink(path.c_str());

    NewOpenInstance(913152, StoreNode::GetInstance()->GetNodeId(), "/open/node-fail-cache", O_RDONLY);
    openInstance->nodeFail = true;
    openInstance->originalSize = 1;
    DiskCache::GetInstance().InsertAndUpdate(openInstance->inodeId, 1, false);
    EXPECT_EQ(store->OpenFile(openInstance.get()), -ENOENT);

    uint64_t fblocks = 0;
    uint64_t fbfree = 0;
    uint64_t fbavail = 0;
    uint64_t ffiles = 0;
    uint64_t fffree = 0;
    EXPECT_EQ(store->StatFSForBrpc("not-local-endpoint", fblocks, fbfree, fbavail, ffiles, fffree), 0);
    std::string localEndpoint = StoreNode::GetInstance()->GetRpcEndPoint(StoreNode::GetInstance()->GetNodeId());
    EXPECT_EQ(store->StatFSForBrpc(localEndpoint, fblocks, fbfree, fbavail, ffiles, fffree), 0);
    EXPECT_GT(fblocks, 0U);

    NewOpenInstance(913200, StoreNode::GetInstance()->GetNodeId(), "/truncate/local", O_RDWR | O_CREAT);
    std::string fileName = GetFilePath(openInstance->inodeId);
    fd = open(fileName.c_str(), O_CREAT | O_RDWR, 0755);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(pwrite(fd, "truncate-data", 13, 0), 13);
    openInstance->physicalFd = fd;
    openInstance->isOpened = true;
    EXPECT_EQ(store->TruncateFile(openInstance.get(), 4), 0);
    close(fd);

    NewOpenInstance(913202, StoreNode::GetInstance()->GetNodeId(), "/truncate/bad-fd", O_RDWR | O_CREAT);
    openInstance->physicalFd = static_cast<uint64_t>(-1);
    openInstance->isOpened = true;
    EXPECT_EQ(store->TruncateFile(openInstance.get(), 4), -EBADF);

    NewOpenInstance(913201, StoreNode::GetInstance()->GetNodeId(), "/truncate/open-instance", O_RDWR | O_CREAT);
    EXPECT_EQ(store->TruncateOpenInstance(openInstance.get(), 2), 0);
    EXPECT_EQ(openInstance->currentSize.load(), 2U);
    EXPECT_EQ(openInstance->originalSize, 2U);

    NewOpenInstance(913203, StoreNode::GetInstance()->GetNodeId() + 20, "/truncate/remote-open-instance-no-client",
                    O_RDWR);
    openInstance->isOpened = true;
    EXPECT_EQ(store->TruncateOpenInstance(openInstance.get(), 3), 0);
    EXPECT_EQ(openInstance->currentSize.load(), 3U);
    EXPECT_EQ(openInstance->originalSize, 3U);
}

TEST_F(FalconStoreUT, ReadPublicBranches)
{
    /* Exercise Read public branches and assert the relevant success or failure branch. */
    auto *store = FalconStore::GetInstance();

    NewOpenInstance(913300, StoreNode::GetInstance()->GetNodeId(), "/read/small-buffer", O_RDONLY);
    const std::string payload = "abcdef";
    openInstance->originalSize = payload.size();
    openInstance->currentSize = payload.size();
    openInstance->readBufferSize = payload.size();
    openInstance->readBuffer.reset(new char[payload.size()], std::default_delete<char[]>());
    memcpy(openInstance->readBuffer.get(), payload.data(), payload.size());

    char smallBuf[16] = {};
    EXPECT_EQ(store->ReadFile(openInstance.get(), smallBuf, 3, 1), 3);
    EXPECT_EQ(std::string(smallBuf, 3), "bcd");
    memset(smallBuf, 0, sizeof(smallBuf));
    EXPECT_EQ(store->ReadFile(openInstance.get(), smallBuf, 4, 4), 2);
    EXPECT_EQ(std::string(smallBuf, 2), "ef");
    EXPECT_EQ(store->ReadFile(openInstance.get(), smallBuf, 4, payload.size()), 0);

    NewOpenInstance(913301, StoreNode::GetInstance()->GetNodeId(), "/read/local-file", O_RDONLY);
    std::string fileName = GetFilePath(openInstance->inodeId);
    int fd = open(fileName.c_str(), O_CREAT | O_RDWR, 0755);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(pwrite(fd, payload.data(), payload.size(), 0), static_cast<ssize_t>(payload.size()));
    openInstance->physicalFd = fd;
    openInstance->currentSize = payload.size();
    char readFileBuf[16] = {};
    EXPECT_EQ(store->ReadFileLR(readFileBuf, 1, openInstance.get(), 3), 3);
    EXPECT_EQ(std::string(readFileBuf, 3), "bcd");
    EXPECT_EQ(store->ReadFileLR(readFileBuf, payload.size(), openInstance.get(), 3), 0);

    openInstance->originalSize = config->GetUint32(FalconPropertyKey::FALCON_BIG_FILE_READ_SIZE);
    openInstance->isOpened = true;
    openInstance->preReadStarted = true;
    openInstance->directReadFile = true;
    memset(readFileBuf, 0, sizeof(readFileBuf));
    EXPECT_EQ(store->ReadFile(openInstance.get(), readFileBuf, 2, 2), 2);
    EXPECT_EQ(std::string(readFileBuf, 2), "cd");
    close(fd);

    NewOpenInstance(913302, StoreNode::GetInstance()->GetNodeId(), "/read/direct-local-file", O_RDONLY | __O_DIRECT);
    fileName = GetFilePath(openInstance->inodeId);
    fd = open(fileName.c_str(), O_CREAT | O_RDWR, 0755);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(pwrite(fd, payload.data(), payload.size(), 0), static_cast<ssize_t>(payload.size()));
    openInstance->physicalFd = fd;
    openInstance->currentSize = payload.size();
    openInstance->originalSize = config->GetUint32(FalconPropertyKey::FALCON_BIG_FILE_READ_SIZE);
    openInstance->isOpened = true;
    openInstance->preReadStarted = true;
    openInstance->directReadFile = true;
    memset(readFileBuf, 0, sizeof(readFileBuf));
    EXPECT_EQ(store->ReadFile(openInstance.get(), readFileBuf, 3, 1), 3);
    EXPECT_EQ(std::string(readFileBuf, 3), "bcd");
    close(fd);

    NewOpenInstance(913303, StoreNode::GetInstance()->GetNodeId(), "/read/local-bad-fd", O_RDONLY);
    openInstance->physicalFd = static_cast<uint64_t>(-2);
    openInstance->currentSize = payload.size();
    EXPECT_EQ(store->ReadFileLR(readFileBuf, 0, openInstance.get(), 3), -EBADF);

    NewOpenInstance(913304, StoreNode::GetInstance()->GetNodeId(), "/read/direct-bad-fd", O_RDONLY | __O_DIRECT);
    openInstance->physicalFd = static_cast<uint64_t>(-2);
    openInstance->currentSize = payload.size();
    openInstance->originalSize = config->GetUint32(FalconPropertyKey::FALCON_BIG_FILE_READ_SIZE);
    openInstance->isOpened = true;
    openInstance->preReadStarted = true;
    openInstance->directReadFile = true;
    memset(readFileBuf, 0, sizeof(readFileBuf));
    EXPECT_EQ(store->ReadFile(openInstance.get(), readFileBuf, 3, 0), -EBADF);

    ResetFalconStatsForCoverage();
}

TEST_F(FalconStoreUT, PublicBoundaryBranches)
{
    /* Exercise public Boundary branches and assert the relevant success or failure branch. */
    auto *store = FalconStore::GetInstance();
    const std::string payload = "boundary-payload";

    NewOpenInstance(913350, StoreNode::GetInstance()->GetNodeId(), "/open/already-has-fd", O_RDWR | O_CREAT);
    std::string fileName = GetFilePath(openInstance->inodeId);
    int fd = open(fileName.c_str(), O_CREAT | O_RDWR, 0755);
    ASSERT_GE(fd, 0);
    openInstance->physicalFd = fd;
    EXPECT_EQ(store->OpenFile(openInstance.get()), 0);
    close(fd);

    NewOpenInstance(913351, StoreNode::GetInstance()->GetNodeId() + 20, "/read/remote-unreachable", O_RDONLY);
    openInstance->currentSize = payload.size();
    char readFileBuf[32] = {};
    EXPECT_EQ(store->ReadFileLR(readFileBuf, 0, openInstance.get(), payload.size()), -EHOSTUNREACH);
    EXPECT_TRUE(openInstance->remoteFailed.load());
    EXPECT_EQ(store->ReadFileLR(readFileBuf, 0, openInstance.get(), payload.size()), -1);

    NewOpenInstance(913354, -1, "/open/auto-alloc-node/path", O_WRONLY | O_CREAT);
    EXPECT_EQ(store->OpenFile(openInstance.get()), 0);
    if (StoreNode::GetInstance()->IsLocal(openInstance->nodeId) && openInstance->physicalFd != UINT64_MAX) {
        close(openInstance->physicalFd);
        DiskCache::GetInstance().Unpin(openInstance->inodeId);
    }

    NewOpenInstance(913352, StoreNode::GetInstance()->GetNodeId(), "/close/auto-flush", O_WRONLY | O_CREAT);
    ASSERT_EQ(store->OpenFile(openInstance.get()), 0);
    openInstance->isOpened = true;
    openInstance->currentSize = payload.size();
    EXPECT_EQ(store->CloseTmpFiles(openInstance.get(), false, false), 0);
    EXPECT_TRUE(openInstance->isFlushed);

    ResetFalconStatsForCoverage();
}

TEST_F(FalconStoreUT, DeleteAndStatPublicBranches)
{
    /* Exercise Delete And Stat public branches and assert the relevant success or failure branch. */
    auto *store = FalconStore::GetInstance();

    EXPECT_EQ(store->DeleteFiles(913400, StoreNode::GetInstance()->GetNodeId(), "/delete/missing"), -ENOENT);

    DiskCache::GetInstance().InsertAndUpdate(913399, 4, false);
    EXPECT_EQ(store->DeleteFiles(913399, StoreNode::GetInstance()->GetNodeId(), "/delete/stale-cache"), -ENOENT);

    uint64_t inodeId = 913401;
    std::string fileName = GetFilePath(inodeId);
    int fd = open(fileName.c_str(), O_CREAT | O_RDWR, 0755);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(pwrite(fd, "delete", 6, 0), 6);
    close(fd);
    DiskCache::GetInstance().InsertAndUpdate(inodeId, 6, false);
    EXPECT_EQ(store->DeleteFiles(inodeId, StoreNode::GetInstance()->GetNodeId(), "/delete/local"), 0);

    EXPECT_EQ(store->DeleteFiles(913402, StoreNode::GetInstance()->GetNodeId() + 20, "/delete/remote-missing"),
              -EHOSTUNREACH);

    struct statvfs vfsBuf;
    memset(&vfsBuf, 0, sizeof(vfsBuf));
    EXPECT_EQ(store->StatFS(&vfsBuf), 0);
    EXPECT_GT(vfsBuf.f_blocks, 0U);

    std::vector<size_t> stats;
    EXPECT_EQ(store->StatCluster(StoreNode::GetInstance()->GetNodeId() + 20, stats, true), 0);
    EXPECT_EQ(store->StatCluster(StoreNode::GetInstance()->GetNodeId(), stats, false), 0);
    EXPECT_EQ(stats.size(), static_cast<size_t>(STATS_END));
    EXPECT_EQ(store->StatCluster(-1, stats, true), 0);
    EXPECT_EQ(stats.size(), static_cast<size_t>(STATS_END));

    ResetFalconStatsForCoverage();
}

TEST_F(FalconStoreUT, AsyncWriteRemoteSmallAndSyncFlushPublicBranches)
{
    /* Exercise Async Write remote Small And Sync Flush public branches and assert the relevant success or failure branch. */
    auto *store = FalconStore::GetInstance();
    const std::string payload = "async-cache-payload";

    NewOpenInstance(913500, StoreNode::GetInstance()->GetNodeId() + 20, "/small/remote-unreachable", O_RDONLY);
    openInstance->originalSize = payload.size();
    openInstance->readBufferSize = payload.size();
    openInstance->readBuffer.reset(new char[payload.size()], std::default_delete<char[]>());
    EXPECT_EQ(store->ReadSmallFiles(openInstance.get()), -EHOSTUNREACH);

    NewOpenInstance(913503, StoreNode::GetInstance()->GetNodeId(), "/close/sync-flush", O_WRONLY | O_CREAT);
    ASSERT_EQ(store->OpenFile(openInstance.get()), 0);
    openInstance->isOpened = true;
    openInstance->writeCnt = 1;
    EXPECT_EQ(store->CloseTmpFiles(openInstance.get(), true, true), 0);
    EXPECT_TRUE(openInstance->isFlushed);
    EXPECT_EQ(store->CloseTmpFiles(openInstance.get(), false, false), 0);

    ResetFalconStatsForCoverage();
}

TEST_F(FalconStoreUT, FalconIOClientNetworkFailureBranches)
{
    /* Exercise Falcon IO Client Network Failure branches and assert the relevant success or failure branch. */
    auto badChannel = std::make_shared<brpc::Channel>();
    brpc::ChannelOptions options;
    ASSERT_EQ(badChannel->Init("127.0.0.1:1", &options), 0);
    FalconIOClient badClient(badChannel);

    uint64_t physicalFd = 0;
    std::string path = "/io-client/network-failure";
    char buffer[16] = {};
    std::vector<size_t> stats;
    struct StatFSBuf statFsBuf {};

    EXPECT_GT(badClient.OpenFile(914000, O_RDONLY, physicalFd, 0, path, false), 0);
    EXPECT_LT(badClient.CloseFile(physicalFd, true, false, buffer, sizeof(buffer), 0), 0);
    EXPECT_LT(badClient.ReadFile(914000, O_RDONLY, buffer, physicalFd, sizeof(buffer), 0, path), 0);
    EXPECT_GT(badClient.ReadSmallFile(914000, sizeof(buffer), path, buffer, O_RDONLY, false), 0);
    EXPECT_LT(badClient.WriteFile(physicalFd, buffer, sizeof(buffer), 0), 0);
    EXPECT_LT(badClient.DeleteFile(914000, StoreNode::GetInstance()->GetNodeId() + 99, path), 0);
    EXPECT_LT(badClient.StatFS(path, &statFsBuf), 0);
    EXPECT_LT(badClient.TruncateOpenInstance(physicalFd, 4), 0);
    EXPECT_LT(badClient.TruncateFile(physicalFd, 4), 0);
    EXPECT_LT(badClient.CheckConnection(), 0);
    EXPECT_LT(badClient.StatCluster(StoreNode::GetInstance()->GetNodeId(), stats, true), 0);
}

TEST_F(FalconStoreUT, RemoteIOServiceImplPublicErrorBranches)
{
    /* Exercise remote IO Service Impl public Error branches and assert the relevant success or failure branch. */
    falcon::brpc_io::RemoteIOServiceImpl service;
    brpc::Controller cntl;

    falcon::brpc_io::ReadRequest readRequest;
    falcon::brpc_io::ErrorCodeOnlyReply errorReply;
    readRequest.set_physical_fd(UINT64_MAX - 100);
    readRequest.set_read_size(-1);
    service.ReadFile(&cntl, &readRequest, &errorReply, nullptr);
    EXPECT_EQ(errorReply.error_code(), -EAGAIN);

    errorReply.Clear();
    readRequest.set_read_size(4);
    service.ReadFile(&cntl, &readRequest, &errorReply, nullptr);
    EXPECT_EQ(errorReply.error_code(), -EBADF);

    falcon::brpc_io::WriteRequest writeRequest;
    falcon::brpc_io::WriteReply writeReply;
    writeRequest.set_physical_fd(UINT64_MAX - 101);
    service.WriteFile(&cntl, &writeRequest, &writeReply, nullptr);
    EXPECT_EQ(writeReply.error_code(), -EBADF);

    falcon::brpc_io::CloseRequest closeRequest;
    closeRequest.set_physical_fd(UINT64_MAX - 102);
    closeRequest.set_flush(false);
    closeRequest.set_sync(false);
    closeRequest.set_offset(0);
    errorReply.Clear();
    service.CloseFile(&cntl, &closeRequest, &errorReply, nullptr);
    EXPECT_EQ(errorReply.error_code(), -EBADF);

    falcon::brpc_io::TruncateOpenInstanceRequest truncateOpenRequest;
    truncateOpenRequest.set_physical_fd(UINT64_MAX - 103);
    truncateOpenRequest.set_size(1);
    errorReply.Clear();
    service.TruncateOpenInstance(&cntl, &truncateOpenRequest, &errorReply, nullptr);
    EXPECT_EQ(errorReply.error_code(), -EBADF);

    falcon::brpc_io::TruncateFileRequest truncateFileRequest;
    truncateFileRequest.set_physical_fd(UINT64_MAX - 104);
    truncateFileRequest.set_size(1);
    errorReply.Clear();
    service.TruncateFile(&cntl, &truncateFileRequest, &errorReply, nullptr);
    EXPECT_EQ(errorReply.error_code(), -EBADF);

    falcon::brpc_io::ReadSmallFileRequest smallReadRequest;
    smallReadRequest.set_inode_id(913600);
    smallReadRequest.set_read_size(-1);
    smallReadRequest.set_path("/remote-io/small-negative");
    smallReadRequest.set_oflags(O_RDONLY);
    smallReadRequest.set_node_fail(false);
    errorReply.Clear();
    service.ReadSmallFile(&cntl, &smallReadRequest, &errorReply, nullptr);
    EXPECT_EQ(errorReply.error_code(), -EAGAIN);

    smallReadRequest.set_read_size(9 * 1024);
    smallReadRequest.set_path("/remote-io/small-missing-large-buffer");
    smallReadRequest.set_oflags(O_RDONLY);
    errorReply.Clear();
    service.ReadSmallFile(&cntl, &smallReadRequest, &errorReply, nullptr);
    EXPECT_LT(errorReply.error_code(), 0);

    smallReadRequest.set_read_size(1);
    smallReadRequest.set_path("/remote-io/small-missing-direct");
    smallReadRequest.set_oflags(O_RDONLY | __O_DIRECT);
    errorReply.Clear();
    service.ReadSmallFile(&cntl, &smallReadRequest, &errorReply, nullptr);
    EXPECT_LT(errorReply.error_code(), 0);

    falcon::brpc_io::DeleteRequest deleteRequest;
    deleteRequest.set_inode_id(913601);
    deleteRequest.set_node_id(StoreNode::GetInstance()->GetNodeId());
    deleteRequest.set_path("/remote-io/delete-missing");
    errorReply.Clear();
    service.DeleteFile(&cntl, &deleteRequest, &errorReply, nullptr);
    EXPECT_EQ(errorReply.error_code(), -ENOENT);

    falcon::brpc_io::StatFSRequest statFsRequest;
    falcon::brpc_io::StatFSReply statFsReply;
    statFsRequest.set_path(StoreNode::GetInstance()->GetRpcEndPoint(StoreNode::GetInstance()->GetNodeId()));
    service.StatFS(&cntl, &statFsRequest, &statFsReply, nullptr);
    EXPECT_EQ(statFsReply.error_code(), 0);
    EXPECT_GT(statFsReply.fblocks(), 0U);

    falcon::brpc_io::CheckConnectionRequest checkRequest;
    errorReply.Clear();
    service.CheckConnection(&cntl, &checkRequest, &errorReply, nullptr);
    EXPECT_EQ(errorReply.error_code(), 0);

    falcon::brpc_io::StatClusterRequest statClusterRequest;
    falcon::brpc_io::StatClusterReply statClusterReply;
    statClusterRequest.set_node_id(StoreNode::GetInstance()->GetNodeId());
    statClusterRequest.set_scatter(false);
    service.StatCluster(&cntl, &statClusterRequest, &statClusterReply, nullptr);
    EXPECT_EQ(statClusterReply.error_code(), 0);
    EXPECT_EQ(statClusterReply.stats_size(), STATS_END);

    auto instance = std::make_shared<OpenInstance>();
    instance->inodeId = 913602;
    instance->nodeId = StoreNode::GetInstance()->GetNodeId();
    instance->path = "/remote-io/closed";
    instance->oflags = O_RDWR | O_CREAT;
    instance->isClosed = true;
    uint64_t fd = FalconFd::GetInstance()->AttachFd(instance->path, instance);
    ASSERT_NE(fd, UINT64_MAX);

    readRequest.set_physical_fd(fd);
    readRequest.set_read_size(4);
    errorReply.Clear();
    service.ReadFile(&cntl, &readRequest, &errorReply, nullptr);
    EXPECT_EQ(errorReply.error_code(), -ETIMEDOUT);

    writeRequest.set_physical_fd(fd);
    writeReply.Clear();
    service.WriteFile(&cntl, &writeRequest, &writeReply, nullptr);
    EXPECT_EQ(writeReply.error_code(), -ETIMEDOUT);

    closeRequest.set_physical_fd(fd);
    errorReply.Clear();
    service.CloseFile(&cntl, &closeRequest, &errorReply, nullptr);
    EXPECT_EQ(errorReply.error_code(), -ETIMEDOUT);

    truncateOpenRequest.set_physical_fd(fd);
    errorReply.Clear();
    service.TruncateOpenInstance(&cntl, &truncateOpenRequest, &errorReply, nullptr);
    EXPECT_EQ(errorReply.error_code(), -ETIMEDOUT);

    truncateFileRequest.set_physical_fd(fd);
    errorReply.Clear();
    service.TruncateFile(&cntl, &truncateFileRequest, &errorReply, nullptr);
    EXPECT_EQ(errorReply.error_code(), -ETIMEDOUT);

    FalconFd::GetInstance()->DeleteOpenInstance(fd, false);
}

TEST_F(FalconStoreUT, FalconIOClientResponseBranches)
{
    /* Exercise Falcon IO Client Response branches and assert the relevant success or failure branch. */
    for (auto mode : {MockRemoteIOService::Mode::Success, MockRemoteIOService::Mode::Error,
                      MockRemoteIOService::Mode::BadSize}) {
        int port = GetUnusedLoopbackPortForStoreCoverage();
        ASSERT_GT(port, 0);

        MockRemoteIOService service(mode);
        brpc::Server server;
        ASSERT_EQ(server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE), 0);
        ASSERT_EQ(server.Start(port, nullptr), 0);

        auto channel = std::make_shared<brpc::Channel>();
        brpc::ChannelOptions options;
        ASSERT_EQ(channel->Init(("127.0.0.1:" + std::to_string(port)).c_str(), &options), 0);
        FalconIOClient ioClient(channel);

        uint64_t physicalFd = 0;
        std::string path = "/io-client/response";
        char buffer[16] = {};
        std::vector<size_t> stats;
        struct StatFSBuf statFsBuf {};
        const bool success = mode == MockRemoteIOService::Mode::Success;
        const bool badSize = mode == MockRemoteIOService::Mode::BadSize;

        int openRet = ioClient.OpenFile(914100, O_RDONLY, physicalFd, 0, path, false);
        EXPECT_EQ(openRet, success || badSize ? 0 : -EIO);
        if (success || badSize) {
            EXPECT_EQ(physicalFd, 4242U);
        }
        EXPECT_EQ(ioClient.CloseFile(physicalFd, true, false, buffer, sizeof(buffer), 0), success || badSize ? 0 : -EIO);

        int readRet = ioClient.ReadFile(914100, O_RDONLY, buffer, physicalFd, 4, 0, path);
        EXPECT_EQ(readRet, success ? 4 : -EIO);
        int smallReadRet = ioClient.ReadSmallFile(914100, 4, path, buffer, O_RDONLY, false);
        EXPECT_EQ(smallReadRet, success ? 0 : -EIO);
        EXPECT_EQ(ioClient.WriteFile(physicalFd, buffer, 4, 0), success ? 0 : -EIO);

        EXPECT_EQ(ioClient.DeleteFile(914100, StoreNode::GetInstance()->GetNodeId(), path), success || badSize ? 0 : -EIO);
        EXPECT_EQ(ioClient.StatFS(path, &statFsBuf), success || badSize ? 0 : -EIO);
        if (success || badSize) {
            EXPECT_EQ(statFsBuf.f_blocks, 11U);
        }
        EXPECT_EQ(ioClient.TruncateOpenInstance(physicalFd, 4), success || badSize ? 0 : -EIO);
        EXPECT_EQ(ioClient.TruncateFile(physicalFd, 4), success || badSize ? 0 : -EIO);
        EXPECT_EQ(ioClient.CheckConnection(), success || badSize ? 0 : -EIO);
        EXPECT_EQ(ioClient.StatCluster(StoreNode::GetInstance()->GetNodeId(), stats, true), success || badSize ? 0 : -EIO);
        if (success || badSize) {
            EXPECT_EQ(stats.size(), static_cast<size_t>(STATS_END));
        }

        server.Stop(0);
        server.Join();
    }
}

TEST_F(FalconStoreUT, RemoteStorePublicOpenAndSmallReadBranches)
{
    /* Exercise remote Store public Open And Small Read branches and assert the relevant success or failure branch. */
    auto *store = FalconStore::GetInstance();
    auto *storeNode = StoreNode::GetInstance();

    std::unordered_map<int, std::string> originalNodes;
    for (int id : storeNode->GetAllNodeId()) {
        originalNodes.emplace(id, storeNode->GetRpcEndPoint(id));
    }

    for (auto mode : {MockRemoteIOService::Mode::Success, MockRemoteIOService::Mode::Error}) {
        int port = GetUnusedLoopbackPortForStoreCoverage();
        ASSERT_GT(port, 0);

        MockRemoteIOService service(mode);
        brpc::Server server;
        ASSERT_EQ(server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE), 0);
        ASSERT_EQ(server.Start(port, nullptr), 0);

        std::unordered_map<int, std::string> nodes = originalNodes;
        int remoteNodeId = storeNode->GetNodeId() + 77 + static_cast<int>(mode);
        nodes[remoteNodeId] = "127.0.0.1:" + std::to_string(port);
        storeNode->UpdateNodeConfigByValue(nodes);

        NewOpenInstance(914200 + static_cast<int>(mode), remoteNodeId, "/remote-store/open", O_RDWR);
        int openRet = store->OpenFile(openInstance.get());
        EXPECT_EQ(openRet, mode == MockRemoteIOService::Mode::Success ? 0 : -EIO);
        if (mode == MockRemoteIOService::Mode::Success) {
            EXPECT_EQ(openInstance->physicalFd, 4242U);
        }

        NewOpenInstance(914210 + static_cast<int>(mode), remoteNodeId, "/remote-store/small", O_RDONLY);
        openInstance->originalSize = 4;
        openInstance->readBufferSize = 4;
        openInstance->readBuffer.reset(new char[4], std::default_delete<char[]>());
        int smallRet = store->ReadSmallFiles(openInstance.get());
        EXPECT_EQ(smallRet, mode == MockRemoteIOService::Mode::Success ? 0 : -EIO);
        if (mode == MockRemoteIOService::Mode::Success) {
            EXPECT_EQ(std::string(openInstance->readBuffer.get(), 4), "ssss");
        }

        server.Stop(0);
        server.Join();
    }

    storeNode->UpdateNodeConfigByValue(originalNodes);
}

/* ------------------------------------------- write local -------------------------------------------*/

size_t writeLocalSize = 0;

TEST_F(FalconStoreUT, WriteLocalLarge)
{
    /* Exercise Write local Large and assert the relevant success or failure branch. */
    NewOpenInstance(1000, StoreNode::GetInstance()->GetNodeId(), "/WriteLocal", O_WRONLY | O_CREAT);

    size_t size = FALCON_STORE_STREAM_MAX_SIZE + 1;
    char *buf = (char *)malloc(size);
    strcpy(buf, "abc");

    int ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), buf, size, 0);
    EXPECT_EQ(ret, 0);
    writeLocalSize += size;
    auto bufferedSize = openInstance->writeStream.GetSize();
    EXPECT_EQ(bufferedSize, 0);
    EXPECT_EQ(openInstance->currentSize.load(), size);
    free(buf);
}

TEST_F(FalconStoreUT, WriteLocalZero)
{
    /* Exercise Write local Zero and assert the relevant success or failure branch. */
    NewOpenInstance(1000, StoreNode::GetInstance()->GetNodeId(), "/WriteLocal", O_WRONLY);

    size_t size = FALCON_STORE_STREAM_MAX_SIZE + 1;
    char *buf = (char *)malloc(size);
    strcpy(buf, "abc");

    int ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), buf, 0, size);
    EXPECT_EQ(ret, 0);
    auto bufferedSize = openInstance->writeStream.GetSize();
    EXPECT_EQ(bufferedSize, 0);
    EXPECT_EQ(openInstance->currentSize.load(), 0);
    free(buf);
}

TEST_F(FalconStoreUT, WriteLocalSeq)
{
    /* Exercise Write local Seq and assert the relevant success or failure branch. */
    NewOpenInstance(1000, StoreNode::GetInstance()->GetNodeId(), "/WriteLocal", O_WRONLY);

    size_t size = FALCON_STORE_STREAM_MAX_SIZE / 2;
    char *buf = (char *)malloc(size);
    strcpy(buf, "abc");
    // local no buffer cache
    int ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), buf, size, 0);
    EXPECT_EQ(ret, 0);
    writeLocalSize += size;
    ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), buf, size, size);
    EXPECT_EQ(ret, 0);
    writeLocalSize += size;
    auto bufferedSize = openInstance->writeStream.GetSize();
    EXPECT_EQ(bufferedSize, 0);
    // perisist previous to cache file, new to buffer
    ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), buf, size, size * 2);
    EXPECT_EQ(ret, 0);
    writeLocalSize += size;
    bufferedSize = openInstance->writeStream.GetSize();
    EXPECT_EQ(bufferedSize, 0);
    EXPECT_EQ(openInstance->currentSize.load(), size * 3);

    free(buf);
}

TEST_F(FalconStoreUT, WriteLocalRandom)
{
    /* Exercise Write local Random and assert the relevant success or failure branch. */
    NewOpenInstance(1000, StoreNode::GetInstance()->GetNodeId(), "/WriteLocal", O_WRONLY);

    size_t size = FALCON_STORE_STREAM_MAX_SIZE / 2;
    char *buf = (char *)malloc(size);
    strcpy(buf, "abc");

    int ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), buf, size, size);
    EXPECT_EQ(ret, 0);
    writeLocalSize += size;
    auto bufferedSize = openInstance->writeStream.GetSize();
    EXPECT_EQ(bufferedSize, 0);
    ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), buf, size, 0);
    EXPECT_EQ(ret, 0);
    writeLocalSize += size;
    bufferedSize = openInstance->writeStream.GetSize();
    EXPECT_EQ(bufferedSize, 0);
    EXPECT_EQ(openInstance->currentSize.load(), size * 2);
    free(buf);
}

TEST_F(FalconStoreUT, WriteLocalSeqToRandom)
{
    /* Exercise Write local Sequential To Random and assert the relevant success or failure branch. */
    NewOpenInstance(1000, StoreNode::GetInstance()->GetNodeId(), "/WriteLocal", O_WRONLY);

    size_t size = FALCON_STORE_STREAM_MAX_SIZE / 4;
    char *buf = (char *)malloc(size);
    strcpy(buf, "abc");

    int ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), buf, size, 0);
    EXPECT_EQ(ret, 0);
    writeLocalSize += size;
    ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), buf, size, size);
    EXPECT_EQ(ret, 0);
    writeLocalSize += size;
    auto bufferedSize = openInstance->writeStream.GetSize();
    EXPECT_EQ(bufferedSize, 0);
    ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), buf, size, 0);
    EXPECT_EQ(ret, 0);
    writeLocalSize += size;
    bufferedSize = openInstance->writeStream.GetSize();
    EXPECT_EQ(bufferedSize, 0);
    EXPECT_EQ(openInstance->currentSize.load(), size * 2);
    free(buf);
}

TEST_F(FalconStoreUT, WriteLocalStats)
{
    /* Exercise Write local Stats and assert the relevant success or failure branch. */
    // wait until stats are updated
    sleep(1);
    std::vector<size_t> stats(STATS_END);
    int ret = client->StatCluster(-1, stats, true);
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(stats[FUSE_READ_OPS], 0);
    EXPECT_EQ(stats[FUSE_WRITE_OPS], 0);
    EXPECT_EQ(stats[FUSE_READ], 0);
    EXPECT_EQ(stats[FUSE_WRITE], 0);
    EXPECT_EQ(stats[BLOCKCACHE_READ], 0);
    EXPECT_EQ(stats[BLOCKCACHE_WRITE], writeLocalSize);
    EXPECT_EQ(stats[OBJ_GET], 0);
    EXPECT_EQ(stats[OBJ_PUT], 0);
}

/* ------------------------------------------- write remote -------------------------------------------*/

size_t writeRemoteSize = 0;

TEST_F(FalconStoreUT, WriteRemoteLarge)
{
    /* Exercise Write remote Large and assert the relevant success or failure branch. */
    NewOpenInstance(2000, StoreNode::GetInstance()->GetNodeId() + 1, "/WriteRemote", O_WRONLY | O_CREAT);

    size_t size = FALCON_STORE_STREAM_MAX_SIZE + 1;
    char *buf = (char *)malloc(size);
    strcpy(buf, "abc");

    int ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), buf, size, 0);
    writeRemoteSize += size;
    EXPECT_EQ(ret, 0);
    auto bufferedSize = openInstance->writeStream.GetSize();
    EXPECT_EQ(bufferedSize, 0);
    EXPECT_EQ(openInstance->currentSize.load(), size);
    free(buf);
}

TEST_F(FalconStoreUT, WriteRemoteZero)
{
    /* Exercise Write remote Zero and assert the relevant success or failure branch. */
    NewOpenInstance(2000, StoreNode::GetInstance()->GetNodeId() + 1, "/WriteRemote", O_WRONLY);

    size_t size = FALCON_STORE_STREAM_MAX_SIZE + 1;
    char *buf = (char *)malloc(size);
    strcpy(buf, "abc");

    int ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), buf, 0, size);
    EXPECT_EQ(ret, 0);
    auto bufferedSize = openInstance->writeStream.GetSize();
    EXPECT_EQ(bufferedSize, 0);
    EXPECT_EQ(openInstance->currentSize.load(), 0);
    free(buf);
}

TEST_F(FalconStoreUT, WriteRemoteSeq)
{
    /* Exercise Write remote Seq and assert the relevant success or failure branch. */
    NewOpenInstance(2000, StoreNode::GetInstance()->GetNodeId() + 1, "/WriteRemote", O_WRONLY);

    size_t size = FALCON_STORE_STREAM_MAX_SIZE / 2;
    char *buf = (char *)malloc(size);
    strcpy(buf, "abc");
    // write to buffer
    int ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), buf, size, 0);
    EXPECT_EQ(ret, 0);
    ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), buf, size, size);
    EXPECT_EQ(ret, 0);
    auto bufferedSize = openInstance->writeStream.GetSize();
    EXPECT_EQ(bufferedSize, size * 2);
    // perisist previous to cache file, new to buffer
    writeRemoteSize += bufferedSize;
    ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), buf, size, size * 2);
    EXPECT_EQ(ret, 0);
    writeRemoteSize += 0;
    bufferedSize = openInstance->writeStream.GetSize();
    EXPECT_EQ(bufferedSize, size);
    EXPECT_EQ(openInstance->currentSize.load(), size * 3);

    free(buf);
}

TEST_F(FalconStoreUT, WriteRemoteRandom)
{
    /* Exercise Write remote Random and assert the relevant success or failure branch. */
    NewOpenInstance(2000, StoreNode::GetInstance()->GetNodeId() + 1, "/WriteRemote", O_WRONLY);

    size_t size = FALCON_STORE_STREAM_MAX_SIZE / 2;
    char *buf = (char *)malloc(size);
    strcpy(buf, "abc");

    int ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), buf, size, size);
    EXPECT_EQ(ret, 0);
    auto bufferedSize = openInstance->writeStream.GetSize();
    EXPECT_EQ(bufferedSize, size);
    writeRemoteSize += size;
    ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), buf, size, 0);
    EXPECT_EQ(ret, 0);
    writeRemoteSize += 0;
    bufferedSize = openInstance->writeStream.GetSize();
    EXPECT_EQ(bufferedSize, size);
    EXPECT_EQ(openInstance->currentSize.load(), size * 2);
    free(buf);
}

TEST_F(FalconStoreUT, WriteRemoteSeqToRandom)
{
    /* Exercise Write remote Sequential To Random and assert the relevant success or failure branch. */
    NewOpenInstance(2000, StoreNode::GetInstance()->GetNodeId() + 1, "/WriteRemote", O_WRONLY);

    size_t size = FALCON_STORE_STREAM_MAX_SIZE / 4;
    char *buf = (char *)malloc(size);
    strcpy(buf, "abc");

    int ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), buf, size, 0);
    EXPECT_EQ(ret, 0);
    ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), buf, size, size);
    EXPECT_EQ(ret, 0);
    auto bufferedSize = openInstance->writeStream.GetSize();
    EXPECT_EQ(bufferedSize, size * 2);
    writeRemoteSize += size;
    writeRemoteSize += size;
    ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), buf, size, 0);
    EXPECT_EQ(ret, 0);
    writeRemoteSize += 0;
    bufferedSize = openInstance->writeStream.GetSize();
    EXPECT_EQ(bufferedSize, size);
    EXPECT_EQ(openInstance->currentSize.load(), size * 2);
    free(buf);
}

TEST_F(FalconStoreUT, WriteRemoteStats)
{
    /* Exercise Write remote Stats and assert the relevant success or failure branch. */
    // wait until stats are updated
    sleep(1);
    std::vector<size_t> stats(STATS_END);
    int ret = client->StatCluster(-1, stats, true);
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(stats[FUSE_READ_OPS], 0);
    EXPECT_EQ(stats[FUSE_WRITE_OPS], 0);
    EXPECT_EQ(stats[FUSE_READ], 0);
    EXPECT_EQ(stats[FUSE_WRITE], 0);
    EXPECT_EQ(stats[BLOCKCACHE_READ], 0);
    EXPECT_EQ(stats[BLOCKCACHE_WRITE], writeRemoteSize);
    EXPECT_EQ(stats[OBJ_GET], 0);
    EXPECT_EQ(stats[OBJ_PUT], 0);
}

/* ------------------------------------------- read local -------------------------------------------*/

size_t readLocalSize = 0;

TEST_F(FalconStoreUT, ReadLocalSeqSmall)
{
    /* Exercise Read local Seq Small and assert the relevant success or failure branch. */
    writeLocalSize = 0;
    NewOpenInstance(10000, StoreNode::GetInstance()->GetNodeId(), "/ReadLocalSmall", O_WRONLY | O_CREAT);
    ResetBuf(false);
    int ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), writeBuf, size, 0);
    EXPECT_EQ(ret, 0);
    writeLocalSize += size;

    NewOpenInstance(10000, StoreNode::GetInstance()->GetNodeId(), "/ReadLocalSmall", O_RDONLY);
    openInstance->originalSize = size;
    openInstance->currentSize = size;
    auto buffer = std::shared_ptr<char>((char *)malloc(size), free);
    openInstance->readBuffer = buffer;
    openInstance->readBufferSize = size;
    ret = FalconStore::GetInstance()->ReadSmallFiles(openInstance.get());
    EXPECT_EQ(ret, 0);
    readLocalSize += size;

    ret = FalconStore::GetInstance()->ReadFile(openInstance.get(), readBuf, readSize, 0);
    EXPECT_EQ(ret, readSize);
    EXPECT_EQ(0, memcmp(writeBuf, readBuf, readSize));
    ret = FalconStore::GetInstance()->ReadFile(openInstance.get(), readBuf, readSize, readSize);
    EXPECT_EQ(ret, readSize);
    EXPECT_EQ(0, memcmp(writeBuf + readSize, readBuf, readSize));
}

TEST_F(FalconStoreUT, ReadLocalRandomSmall)
{
    /* Exercise Read local Random Small and assert the relevant success or failure branch. */
    NewOpenInstance(10000, StoreNode::GetInstance()->GetNodeId(), "/ReadLocalSmall", O_RDONLY);
    openInstance->originalSize = size;
    openInstance->currentSize = size;
    auto buffer = std::shared_ptr<char>((char *)malloc(size), free);
    openInstance->readBuffer = buffer;
    openInstance->readBufferSize = size;
    int ret = FalconStore::GetInstance()->ReadSmallFiles(openInstance.get());
    EXPECT_EQ(ret, 0);
    readLocalSize += size;

    memset(readBuf, 0, readSize);

    ret = FalconStore::GetInstance()->ReadFile(openInstance.get(), readBuf, readSize, readSize);
    EXPECT_EQ(ret, readSize);
    EXPECT_EQ(0, memcmp(writeBuf + readSize, readBuf, readSize));
    ret = FalconStore::GetInstance()->ReadFile(openInstance.get(), readBuf, readSize, 0);
    EXPECT_EQ(ret, readSize);
    EXPECT_EQ(0, memcmp(writeBuf, readBuf, readSize));
}

TEST_F(FalconStoreUT, ReadLocalSeqLarge)
{
    /* Exercise Read local Seq Large and assert the relevant success or failure branch. */
    NewOpenInstance(10001, StoreNode::GetInstance()->GetNodeId(), "/ReadLocalLarge", O_WRONLY | O_CREAT);
    ResetBuf(true);

    int ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), writeBuf, size, 0);
    EXPECT_EQ(ret, 0);
    writeLocalSize += size;

    NewOpenInstance(10001, StoreNode::GetInstance()->GetNodeId(), "/ReadLocalLarge", O_RDONLY);
    openInstance->originalSize = size;
    openInstance->currentSize = size;

    ret = FalconStore::GetInstance()->ReadFile(openInstance.get(), readBuf, readSize, 0);
    EXPECT_EQ(ret, readSize);
    EXPECT_EQ(0, memcmp(writeBuf, readBuf, readSize));
    readLocalSize += readSize;
    ret = FalconStore::GetInstance()->ReadFile(openInstance.get(), readBuf, readSize, readSize);
    EXPECT_EQ(ret, readSize);
    EXPECT_EQ(0, memcmp(writeBuf + readSize, readBuf, readSize));
    readLocalSize += readSize;
}

TEST_F(FalconStoreUT, ReadLocalRandomLarge)
{
    /* Exercise Read local Random Large and assert the relevant success or failure branch. */
    NewOpenInstance(10001, StoreNode::GetInstance()->GetNodeId(), "/ReadLocalLarge", O_RDONLY);
    openInstance->originalSize = size;
    openInstance->currentSize = size;

    memset(readBuf, 0, readSize);

    int ret = FalconStore::GetInstance()->ReadFile(openInstance.get(), readBuf, readSize, readSize);
    EXPECT_EQ(ret, readSize);
    EXPECT_EQ(0, memcmp(writeBuf + readSize, readBuf, readSize));
    readLocalSize += readSize;
    ret = FalconStore::GetInstance()->ReadFile(openInstance.get(), readBuf, readSize, 0);
    EXPECT_EQ(ret, readSize);
    EXPECT_EQ(0, memcmp(writeBuf, readBuf, readSize));
    readLocalSize += readSize;
}

TEST_F(FalconStoreUT, ReadLocalSeqToRandomLarge)
{
    /* Exercise Read local Sequential To Random Large and assert the relevant success or failure branch. */
    NewOpenInstance(10001, StoreNode::GetInstance()->GetNodeId(), "/ReadLocalLarge", O_RDONLY);
    openInstance->originalSize = size;
    openInstance->currentSize = size;

    memset(readBuf, 0, readSize);

    int ret = FalconStore::GetInstance()->ReadFile(openInstance.get(), readBuf, readSize, 0);
    EXPECT_EQ(ret, readSize);
    EXPECT_EQ(0, memcmp(writeBuf, readBuf, readSize));
    readLocalSize += readSize;
    ret = FalconStore::GetInstance()->ReadFile(openInstance.get(), readBuf, readSize, readSize);
    EXPECT_EQ(ret, readSize);
    EXPECT_EQ(0, memcmp(writeBuf + readSize, readBuf, readSize));
    readLocalSize += readSize;
    // not serial
    ret = FalconStore::GetInstance()->ReadFile(openInstance.get(), readBuf, readSize, 0);
    EXPECT_EQ(ret, readSize);
    EXPECT_EQ(0, memcmp(writeBuf, readBuf, readSize));
    readLocalSize += readSize;
}

TEST_F(FalconStoreUT, ReadLocalExceed)
{
    /* Exercise Read local Exceed and assert the relevant success or failure branch. */
    NewOpenInstance(10001, StoreNode::GetInstance()->GetNodeId(), "/ReadLocalLarge", O_RDONLY);
    openInstance->originalSize = size;
    openInstance->currentSize = size;

    memset(readBuf, 0, readSize);
    int ret = FalconStore::GetInstance()->ReadFile(openInstance.get(), readBuf, readSize, size - 1);
    EXPECT_EQ(ret, 1);
    readLocalSize += 1;
    ret = FalconStore::GetInstance()->ReadFile(openInstance.get(), readBuf, readSize, size);
    EXPECT_EQ(ret, 0);
    readLocalSize += 0;
}

TEST_F(FalconStoreUT, ReadLocalHole)
{
    /* Exercise Read local Hole and assert the relevant success or failure branch. */
    NewOpenInstance(10001, StoreNode::GetInstance()->GetNodeId(), "/ReadLocalLarge", O_WRONLY);
    int ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), writeBuf, size, size * 2);
    EXPECT_EQ(ret, 0);
    writeLocalSize += size;

    NewOpenInstance(10001, StoreNode::GetInstance()->GetNodeId(), "/ReadLocalLarge", O_RDONLY);
    openInstance->originalSize = size * 3;
    openInstance->currentSize = size * 3;

    memset(readBuf, 0, readSize);
    ret = FalconStore::GetInstance()->ReadFile(openInstance.get(), readBuf, readSize, size);
    EXPECT_EQ(ret, readSize);
    readLocalSize += readSize;
    void *zeroBlock = std::memset(new char[readSize], 0, readSize);
    bool result = std::memcmp(readBuf, zeroBlock, readSize) == 0;
    EXPECT_TRUE(result);
    delete[] static_cast<char *>(zeroBlock);
}

TEST_F(FalconStoreUT, ReadLocalStats)
{
    /* Exercise Read local Stats and assert the relevant success or failure branch. */
    // wait until stats are updated
    sleep(1);
    std::vector<size_t> stats(STATS_END);
    int ret = client->StatCluster(-1, stats, true);
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(stats[FUSE_READ_OPS], 0);
    EXPECT_EQ(stats[FUSE_WRITE_OPS], 0);
    EXPECT_EQ(stats[FUSE_READ], 0);
    EXPECT_EQ(stats[FUSE_WRITE], 0);
    EXPECT_EQ(stats[BLOCKCACHE_READ], readLocalSize);
    EXPECT_EQ(stats[BLOCKCACHE_WRITE], writeLocalSize);
    EXPECT_EQ(stats[OBJ_GET], 0);
    EXPECT_EQ(stats[OBJ_PUT], 0);
}

/* ------------------------------------------- read remote -------------------------------------------*/

size_t readRemoteSize = 0;

TEST_F(FalconStoreUT, ReadRemoteSeqSmall)
{
    /* Exercise Read remote Seq Small and assert the relevant success or failure branch. */
    writeRemoteSize = 0;
    NewOpenInstance(20000, StoreNode::GetInstance()->GetNodeId() + 1, "/ReadRemoteSmall", O_WRONLY | O_CREAT);
    ResetBuf(false);

    int ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), writeBuf, size, 0);
    EXPECT_EQ(ret, 0);
    writeRemoteSize += size;

    NewOpenInstance(20000, StoreNode::GetInstance()->GetNodeId() + 1, "/ReadRemoteSmall", O_RDONLY);
    openInstance->originalSize = size;
    openInstance->currentSize = size;
    auto buffer = std::shared_ptr<char>((char *)malloc(size), free);
    openInstance->readBuffer = buffer;
    openInstance->readBufferSize = size;
    ret = FalconStore::GetInstance()->ReadSmallFiles(openInstance.get());
    EXPECT_EQ(ret, 0);
    readRemoteSize += size;
    EXPECT_EQ(FalconStats::GetInstance().stats[BLOCKCACHE_READ], readRemoteSize);

    ret = FalconStore::GetInstance()->ReadFile(openInstance.get(), readBuf, readSize, 0);
    EXPECT_EQ(ret, readSize);
    EXPECT_EQ(0, memcmp(writeBuf, readBuf, readSize));
    ret = FalconStore::GetInstance()->ReadFile(openInstance.get(), readBuf, readSize, readSize);
    EXPECT_EQ(ret, readSize);
    EXPECT_EQ(0, memcmp(writeBuf + readSize, readBuf, readSize));
}

TEST_F(FalconStoreUT, ReadRemoteRandomSmall)
{
    /* Exercise Read remote Random Small and assert the relevant success or failure branch. */
    NewOpenInstance(20000, StoreNode::GetInstance()->GetNodeId() + 1, "/ReadRemoteSmall", O_RDONLY);
    openInstance->originalSize = size;
    openInstance->currentSize = size;
    auto buffer = std::shared_ptr<char>((char *)malloc(size), free);
    openInstance->readBuffer = buffer;
    openInstance->readBufferSize = size;
    int ret = FalconStore::GetInstance()->ReadSmallFiles(openInstance.get());
    EXPECT_EQ(ret, 0);
    readRemoteSize += size;
    EXPECT_EQ(FalconStats::GetInstance().stats[BLOCKCACHE_READ], readRemoteSize);

    memset(readBuf, 0, readSize);

    ret = FalconStore::GetInstance()->ReadFile(openInstance.get(), readBuf, readSize, readSize);
    EXPECT_EQ(ret, readSize);
    EXPECT_EQ(0, memcmp(writeBuf + readSize, readBuf, readSize));
    ret = FalconStore::GetInstance()->ReadFile(openInstance.get(), readBuf, readSize, 0);
    EXPECT_EQ(ret, readSize);
    EXPECT_EQ(0, memcmp(writeBuf, readBuf, readSize));
}

TEST_F(FalconStoreUT, ReadRemoteStats)
{
    /* Exercise Read remote Stats and assert the relevant success or failure branch. */
    // large remote file has preread, unable to determine actual read value
    // wait until stats are updated
    sleep(1);
    std::vector<size_t> stats(STATS_END);
    int ret = client->StatCluster(-1, stats, true);
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(stats[FUSE_READ_OPS], 0);
    EXPECT_EQ(stats[FUSE_WRITE_OPS], 0);
    EXPECT_EQ(stats[FUSE_READ], 0);
    EXPECT_EQ(stats[FUSE_WRITE], 0);
    EXPECT_EQ(stats[BLOCKCACHE_READ], readRemoteSize);
    EXPECT_EQ(stats[BLOCKCACHE_WRITE], writeRemoteSize);
    EXPECT_EQ(stats[OBJ_GET], 0);
    EXPECT_EQ(stats[OBJ_PUT], 0);
}

TEST_F(FalconStoreUT, ReadRemoteSeqLarge)
{
    /* Exercise Read remote Seq Large and assert the relevant success or failure branch. */
    NewOpenInstance(20001, StoreNode::GetInstance()->GetNodeId() + 1, "/ReadRemoteLarge", O_WRONLY | O_CREAT);
    ResetBuf(true);

    int ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), writeBuf, size, 0);
    EXPECT_EQ(ret, 0);

    NewOpenInstance(20001, StoreNode::GetInstance()->GetNodeId() + 1, "/ReadRemoteLarge", O_RDONLY);
    openInstance->originalSize = size;
    openInstance->currentSize = size;

    ret = FalconStore::GetInstance()->ReadFile(openInstance.get(), readBuf, readSize, 0);
    EXPECT_EQ(ret, readSize);
    EXPECT_EQ(0, memcmp(writeBuf, readBuf, readSize));
    ret = FalconStore::GetInstance()->ReadFile(openInstance.get(), readBuf, readSize, readSize);
    EXPECT_EQ(ret, readSize);
    EXPECT_EQ(0, memcmp(writeBuf + readSize, readBuf, readSize));
}

TEST_F(FalconStoreUT, ReadRemoteRandomLarge)
{
    /* Exercise Read remote Random Large and assert the relevant success or failure branch. */
    NewOpenInstance(20001, StoreNode::GetInstance()->GetNodeId() + 1, "/ReadRemoteLarge", O_RDONLY);
    openInstance->originalSize = size;
    openInstance->currentSize = size;

    memset(readBuf, 0, readSize);

    int ret = FalconStore::GetInstance()->ReadFile(openInstance.get(), readBuf, readSize, readSize);
    EXPECT_EQ(ret, readSize);
    EXPECT_EQ(0, memcmp(writeBuf + readSize, readBuf, readSize));
    ret = FalconStore::GetInstance()->ReadFile(openInstance.get(), readBuf, readSize, 0);
    EXPECT_EQ(ret, readSize);
    EXPECT_EQ(0, memcmp(writeBuf, readBuf, readSize));
}

TEST_F(FalconStoreUT, ReadRemoteSeqToRandomLarge)
{
    /* Exercise Read remote Sequential To Random Large and assert the relevant success or failure branch. */
    NewOpenInstance(20001, StoreNode::GetInstance()->GetNodeId() + 1, "/ReadRemoteLarge", O_RDONLY);
    openInstance->originalSize = size;
    openInstance->currentSize = size;

    memset(readBuf, 0, readSize);

    int ret = FalconStore::GetInstance()->ReadFile(openInstance.get(), readBuf, readSize, 0);
    EXPECT_EQ(ret, readSize);
    EXPECT_EQ(0, memcmp(writeBuf, readBuf, readSize));
    ret = FalconStore::GetInstance()->ReadFile(openInstance.get(), readBuf, readSize, readSize);
    EXPECT_EQ(ret, readSize);
    EXPECT_EQ(0, memcmp(writeBuf + readSize, readBuf, readSize));
    // not serial
    ret = FalconStore::GetInstance()->ReadFile(openInstance.get(), readBuf, readSize, 0);
    EXPECT_EQ(ret, readSize);
    EXPECT_EQ(0, memcmp(writeBuf, readBuf, readSize));
}

TEST_F(FalconStoreUT, ReadRemoteExceed)
{
    /* Exercise Read remote Exceed and assert the relevant success or failure branch. */
    NewOpenInstance(20001, StoreNode::GetInstance()->GetNodeId() + 1, "/ReadRemoteLarge", O_RDONLY);
    openInstance->originalSize = size;
    openInstance->currentSize = size;

    memset(readBuf, 0, readSize);
    int ret = FalconStore::GetInstance()->ReadFile(openInstance.get(), readBuf, readSize, size - 1);
    EXPECT_EQ(ret, 1);
    ret = FalconStore::GetInstance()->ReadFile(openInstance.get(), readBuf, readSize, size);
    EXPECT_EQ(ret, 0);
}

TEST_F(FalconStoreUT, ReadRemoteHole)
{
    /* Exercise Read remote Hole and assert the relevant success or failure branch. */
    NewOpenInstance(20001, StoreNode::GetInstance()->GetNodeId() + 1, "/ReadRemoteLarge", O_WRONLY);
    int ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), writeBuf, size, size * 2);
    EXPECT_EQ(ret, 0);

    NewOpenInstance(20001, StoreNode::GetInstance()->GetNodeId() + 1, "/ReadRemoteLarge", O_RDONLY);
    openInstance->originalSize = size * 3;
    openInstance->currentSize = size * 3;

    memset(readBuf, 0, readSize);
    ret = FalconStore::GetInstance()->ReadFile(openInstance.get(), readBuf, readSize, size);
    EXPECT_EQ(ret, readSize);
    void *zeroBlock = std::memset(new char[readSize], 0, readSize);
    bool result = std::memcmp(readBuf, zeroBlock, readSize) == 0;
    EXPECT_TRUE(result);
    delete[] static_cast<char *>(zeroBlock);
}

/* ------------------------------------------- RDWR local -------------------------------------------*/
// all large file
TEST_F(FalconStoreUT, PrereadWriteLocal)
{
    /* Exercise Preread Write local and assert the relevant success or failure branch. */
    NewOpenInstance(10001, StoreNode::GetInstance()->GetNodeId(), "/ReadLocalLarge", O_RDWR);
    openInstance->originalSize = size;
    openInstance->currentSize = size;
    ResetBuf(true);

    int ret = FalconStore::GetInstance()->ReadFile(openInstance.get(), readBuf, readSize, 0);
    EXPECT_EQ(ret, readSize);
    EXPECT_EQ(0, memcmp(writeBuf, readBuf, readSize));

    ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), writeBuf, size, 0);
    EXPECT_EQ(ret, 0);
}

TEST_F(FalconStoreUT, ReadWriteLocal)
{
    /* Exercise Read Write local and assert the relevant success or failure branch. */
    NewOpenInstance(10001, StoreNode::GetInstance()->GetNodeId(), "/ReadLocalLarge", O_RDWR);
    openInstance->originalSize = size;
    openInstance->currentSize = size;

    memset(readBuf, 0, readSize);

    int ret = FalconStore::GetInstance()->ReadFile(openInstance.get(), readBuf, readSize, 0);
    EXPECT_EQ(ret, readSize);
    EXPECT_EQ(0, memcmp(writeBuf, readBuf, readSize));
    ret = FalconStore::GetInstance()->ReadFile(openInstance.get(), readBuf, readSize, 0);
    EXPECT_EQ(ret, readSize);
    EXPECT_EQ(0, memcmp(writeBuf, readBuf, readSize));

    ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), writeBuf, size, 0);
    EXPECT_EQ(ret, 0);
}

TEST_F(FalconStoreUT, PrereadWriteReadLocal)
{
    /* Exercise Preread Write Read local and assert the relevant success or failure branch. */
    NewOpenInstance(10001, StoreNode::GetInstance()->GetNodeId(), "/ReadLocalLarge", O_RDWR);
    openInstance->originalSize = size;
    openInstance->currentSize = size;

    memset(readBuf, 0, readSize);

    int ret = FalconStore::GetInstance()->ReadFile(openInstance.get(), readBuf, readSize, 0);
    EXPECT_EQ(ret, readSize);
    EXPECT_EQ(0, memcmp(writeBuf, readBuf, readSize));

    ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), writeBuf, size, 0);
    EXPECT_EQ(ret, 0);

    ret = FalconStore::GetInstance()->ReadFile(openInstance.get(), readBuf, readSize, readSize);
    EXPECT_EQ(ret, readSize);
    EXPECT_EQ(0, memcmp(writeBuf + readSize, readBuf, readSize));
}

TEST_F(FalconStoreUT, ReadWriteReadLocal)
{
    /* Exercise Read Write Read local and assert the relevant success or failure branch. */
    NewOpenInstance(10001, StoreNode::GetInstance()->GetNodeId(), "/ReadLocalLarge", O_RDWR);
    openInstance->originalSize = size;
    openInstance->currentSize = size;

    memset(readBuf, 0, readSize);

    int ret = FalconStore::GetInstance()->ReadFile(openInstance.get(), readBuf, readSize, 0);
    EXPECT_EQ(ret, readSize);
    EXPECT_EQ(0, memcmp(writeBuf, readBuf, readSize));
    ret = FalconStore::GetInstance()->ReadFile(openInstance.get(), readBuf, readSize, 0);
    EXPECT_EQ(ret, readSize);
    EXPECT_EQ(0, memcmp(writeBuf, readBuf, readSize));

    ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), writeBuf, size, 0);
    EXPECT_EQ(ret, 0);

    ret = FalconStore::GetInstance()->ReadFile(openInstance.get(), readBuf, readSize, readSize);
    EXPECT_EQ(ret, readSize);
    EXPECT_EQ(0, memcmp(writeBuf + readSize, readBuf, readSize));
}

TEST_F(FalconStoreUT, WritePreReadWriteLocal)
{
    /* Exercise Write Pre Read Write local and assert the relevant success or failure branch. */
    NewOpenInstance(10001, StoreNode::GetInstance()->GetNodeId(), "/ReadLocalLarge", O_RDWR);
    openInstance->originalSize = size;
    openInstance->currentSize = size;

    memset(readBuf, 0, readSize);

    int ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), writeBuf, size, 0);
    EXPECT_EQ(ret, 0);
    // pre read should be stopped
    ret = FalconStore::GetInstance()->ReadFile(openInstance.get(), readBuf, readSize, 0);
    EXPECT_EQ(ret, readSize);
    EXPECT_EQ(0, memcmp(writeBuf, readBuf, readSize));
    ret = FalconStore::GetInstance()->ReadFile(openInstance.get(), readBuf, readSize, readSize);
    EXPECT_EQ(ret, readSize);
    EXPECT_EQ(0, memcmp(writeBuf + readSize, readBuf, readSize));

    ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), writeBuf, size, 0);
    EXPECT_EQ(ret, 0);
}

TEST_F(FalconStoreUT, WriteReadWriteLocal)
{
    /* Exercise Write Read Write local and assert the relevant success or failure branch. */
    NewOpenInstance(10001, StoreNode::GetInstance()->GetNodeId(), "/ReadLocalLarge", O_RDWR);
    openInstance->originalSize = size;
    openInstance->currentSize = size;

    memset(readBuf, 0, readSize);

    int ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), writeBuf, size, 0);
    EXPECT_EQ(ret, 0);
    // pre read should be stopped
    ret = FalconStore::GetInstance()->ReadFile(openInstance.get(), readBuf, readSize, 0);
    EXPECT_EQ(ret, readSize);
    EXPECT_EQ(0, memcmp(writeBuf, readBuf, readSize));
    ret = FalconStore::GetInstance()->ReadFile(openInstance.get(), readBuf, readSize, 0);
    EXPECT_EQ(ret, readSize);
    EXPECT_EQ(0, memcmp(writeBuf, readBuf, readSize));

    ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), writeBuf, size, 0);
    EXPECT_EQ(ret, 0);
}

/* ------------------------------------------- RDWR remote -------------------------------------------*/
// all large file
TEST_F(FalconStoreUT, PrereadWriteRemote)
{
    /* Exercise Preread Write remote and assert the relevant success or failure branch. */
    NewOpenInstance(20001, StoreNode::GetInstance()->GetNodeId() + 1, "/ReadRemoteLarge", O_RDWR);
    openInstance->originalSize = size;
    openInstance->currentSize = size;

    ResetBuf(true);

    int ret = FalconStore::GetInstance()->ReadFile(openInstance.get(), readBuf, readSize, 0);
    EXPECT_EQ(ret, readSize);
    EXPECT_EQ(0, memcmp(writeBuf, readBuf, readSize));

    ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), writeBuf, size, 0);
    EXPECT_EQ(ret, 0);
}

TEST_F(FalconStoreUT, ReadWriteRemote)
{
    /* Exercise Read Write remote and assert the relevant success or failure branch. */
    NewOpenInstance(20001, StoreNode::GetInstance()->GetNodeId() + 1, "/ReadRemoteLarge", O_RDWR);
    openInstance->originalSize = size;
    openInstance->currentSize = size;

    memset(readBuf, 0, readSize);

    int ret = FalconStore::GetInstance()->ReadFile(openInstance.get(), readBuf, readSize, 0);
    EXPECT_EQ(ret, readSize);
    EXPECT_EQ(0, memcmp(writeBuf, readBuf, readSize));
    ret = FalconStore::GetInstance()->ReadFile(openInstance.get(), readBuf, readSize, 0);
    EXPECT_EQ(ret, readSize);
    EXPECT_EQ(0, memcmp(writeBuf, readBuf, readSize));

    ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), writeBuf, size, 0);
    EXPECT_EQ(ret, 0);
}

TEST_F(FalconStoreUT, PrereadWriteReadRemote)
{
    /* Exercise Preread Write Read remote and assert the relevant success or failure branch. */
    NewOpenInstance(20001, StoreNode::GetInstance()->GetNodeId() + 1, "/ReadRemoteLarge", O_RDWR);
    openInstance->originalSize = size;
    openInstance->currentSize = size;

    memset(readBuf, 0, readSize);

    int ret = FalconStore::GetInstance()->ReadFile(openInstance.get(), readBuf, readSize, 0);
    EXPECT_EQ(ret, readSize);
    EXPECT_EQ(0, memcmp(writeBuf, readBuf, readSize));

    ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), writeBuf, size, 0);
    EXPECT_EQ(ret, 0);

    ret = FalconStore::GetInstance()->ReadFile(openInstance.get(), readBuf, readSize, readSize);
    EXPECT_EQ(ret, readSize);
    EXPECT_EQ(0, memcmp(writeBuf + readSize, readBuf, readSize));
}

TEST_F(FalconStoreUT, ReadWriteReadRemote)
{
    /* Exercise Read Write Read remote and assert the relevant success or failure branch. */
    NewOpenInstance(20001, StoreNode::GetInstance()->GetNodeId() + 1, "/ReadRemoteLarge", O_RDWR);
    openInstance->originalSize = size;
    openInstance->currentSize = size;

    memset(readBuf, 0, readSize);

    int ret = FalconStore::GetInstance()->ReadFile(openInstance.get(), readBuf, readSize, 0);
    EXPECT_EQ(ret, readSize);
    EXPECT_EQ(0, memcmp(writeBuf, readBuf, readSize));
    ret = FalconStore::GetInstance()->ReadFile(openInstance.get(), readBuf, readSize, 0);
    EXPECT_EQ(ret, readSize);
    EXPECT_EQ(0, memcmp(writeBuf, readBuf, readSize));

    ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), writeBuf, size, 0);
    EXPECT_EQ(ret, 0);

    ret = FalconStore::GetInstance()->ReadFile(openInstance.get(), readBuf, readSize, readSize);
    EXPECT_EQ(ret, readSize);
    EXPECT_EQ(0, memcmp(writeBuf + readSize, readBuf, readSize));
}

TEST_F(FalconStoreUT, WritePreReadWriteRemote)
{
    /* Exercise Write Pre Read Write remote and assert the relevant success or failure branch. */
    NewOpenInstance(20001, StoreNode::GetInstance()->GetNodeId() + 1, "/ReadRemoteLarge", O_RDWR);
    openInstance->originalSize = size;
    openInstance->currentSize = size;

    memset(readBuf, 0, readSize);

    int ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), writeBuf, size, 0);
    EXPECT_EQ(ret, 0);
    // pre read should be stopped
    ret = FalconStore::GetInstance()->ReadFile(openInstance.get(), readBuf, readSize, 0);
    EXPECT_EQ(ret, readSize);
    EXPECT_EQ(0, memcmp(writeBuf, readBuf, readSize));
    ret = FalconStore::GetInstance()->ReadFile(openInstance.get(), readBuf, readSize, readSize);
    EXPECT_EQ(ret, readSize);
    EXPECT_EQ(0, memcmp(writeBuf + readSize, readBuf, readSize));

    ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), writeBuf, size, 0);
    EXPECT_EQ(ret, 0);
}

TEST_F(FalconStoreUT, WriteReadWriteRemote)
{
    /* Exercise Write Read Write remote and assert the relevant success or failure branch. */
    NewOpenInstance(20001, StoreNode::GetInstance()->GetNodeId() + 1, "/ReadRemoteLarge", O_RDWR);
    openInstance->originalSize = size;
    openInstance->currentSize = size;

    memset(readBuf, 0, readSize);

    int ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), writeBuf, size, 0);
    EXPECT_EQ(ret, 0);
    // pre read should be stopped
    ret = FalconStore::GetInstance()->ReadFile(openInstance.get(), readBuf, readSize, 0);
    EXPECT_EQ(ret, readSize);
    EXPECT_EQ(0, memcmp(writeBuf, readBuf, readSize));
    ret = FalconStore::GetInstance()->ReadFile(openInstance.get(), readBuf, readSize, 0);
    EXPECT_EQ(ret, readSize);
    EXPECT_EQ(0, memcmp(writeBuf, readBuf, readSize));

    ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), writeBuf, size, 0);
    EXPECT_EQ(ret, 0);
}

/* ------------------------------------------- close local -------------------------------------------*/

TEST_F(FalconStoreUT, FlushLocal)
{
    /* Exercise Flush local and assert the relevant success or failure branch. */
    NewOpenInstance(100000, StoreNode::GetInstance()->GetNodeId(), "/CloseLocal", O_RDWR | O_CREAT);

    ResetBuf(true);

    int ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), writeBuf, FALCON_STORE_STREAM_MAX_SIZE, 0);
    EXPECT_EQ(ret, 0);

    ret = FalconStore::GetInstance()->CloseTmpFiles(openInstance.get(), true, true);
    EXPECT_EQ(ret, 0);
}

TEST_F(FalconStoreUT, ReleaseLocal)
{
    /* Exercise Release local and assert the relevant success or failure branch. */
    NewOpenInstance(100000, StoreNode::GetInstance()->GetNodeId(), "/CloseLocal", O_RDWR);

    memset(readBuf, 0, readSize);

    int ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), writeBuf, FALCON_STORE_STREAM_MAX_SIZE, 0);
    EXPECT_EQ(ret, 0);

    ret = FalconStore::GetInstance()->CloseTmpFiles(openInstance.get(), true, true);
    EXPECT_EQ(ret, 0);
    ret = FalconStore::GetInstance()->CloseTmpFiles(openInstance.get(), false, true);
    EXPECT_EQ(ret, 0);
}

TEST_F(FalconStoreUT, ReleaseWithoutFlushLocal)
{
    /* Exercise Release Without Flush local and assert the relevant success or failure branch. */
    NewOpenInstance(100000, StoreNode::GetInstance()->GetNodeId(), "/CloseLocal", O_RDWR);

    memset(readBuf, 0, readSize);

    int ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), writeBuf, FALCON_STORE_STREAM_MAX_SIZE, 0);
    EXPECT_EQ(ret, 0);

    ret = FalconStore::GetInstance()->CloseTmpFiles(openInstance.get(), false, true);
    EXPECT_EQ(ret, 0);
}

TEST_F(FalconStoreUT, FlushTwiceLocal)
{
    /* Exercise Flush Twice local and assert the relevant success or failure branch. */
    NewOpenInstance(100000, StoreNode::GetInstance()->GetNodeId(), "/CloseLocal", O_RDWR);

    memset(readBuf, 0, readSize);

    int ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), writeBuf, FALCON_STORE_STREAM_MAX_SIZE, 0);
    EXPECT_EQ(ret, 0);

    ret = FalconStore::GetInstance()->CloseTmpFiles(openInstance.get(), true, true);
    EXPECT_EQ(ret, 0);
    ret = FalconStore::GetInstance()->CloseTmpFiles(openInstance.get(), true, true);
    EXPECT_EQ(ret, 0);
    ret = FalconStore::GetInstance()->CloseTmpFiles(openInstance.get(), false, true);
    EXPECT_EQ(ret, 0);
}

/* ------------------------------------------- close remote -------------------------------------------*/

TEST_F(FalconStoreUT, FlushRemote)
{
    /* Exercise Flush remote and assert the relevant success or failure branch. */
    NewOpenInstance(200000, StoreNode::GetInstance()->GetNodeId() + 1, "/CloseRemote", O_RDWR | O_CREAT);

    memset(readBuf, 0, readSize);

    int ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), writeBuf, FALCON_STORE_STREAM_MAX_SIZE, 0);
    EXPECT_EQ(ret, 0);

    ret = FalconStore::GetInstance()->CloseTmpFiles(openInstance.get(), true, true);
    EXPECT_EQ(ret, 0);
}

TEST_F(FalconStoreUT, ReleaseRemote)
{
    /* Exercise Release remote and assert the relevant success or failure branch. */
    NewOpenInstance(200000, StoreNode::GetInstance()->GetNodeId() + 1, "/CloseRemote", O_RDWR);

    memset(readBuf, 0, readSize);

    int ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), writeBuf, FALCON_STORE_STREAM_MAX_SIZE, 0);
    EXPECT_EQ(ret, 0);

    ret = FalconStore::GetInstance()->CloseTmpFiles(openInstance.get(), true, true);
    EXPECT_EQ(ret, 0);
    ret = FalconStore::GetInstance()->CloseTmpFiles(openInstance.get(), false, true);
    EXPECT_EQ(ret, 0);
}

TEST_F(FalconStoreUT, ReleaseWithoutFlushRemote)
{
    /* Exercise Release Without Flush remote and assert the relevant success or failure branch. */
    NewOpenInstance(200000, StoreNode::GetInstance()->GetNodeId() + 1, "/CloseRemote", O_RDWR);

    memset(readBuf, 0, readSize);

    int ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), writeBuf, FALCON_STORE_STREAM_MAX_SIZE, 0);
    EXPECT_EQ(ret, 0);

    ret = FalconStore::GetInstance()->CloseTmpFiles(openInstance.get(), false, true);
    EXPECT_EQ(ret, 0);
}

TEST_F(FalconStoreUT, FlushTwiceRemote)
{
    /* Exercise Flush Twice remote and assert the relevant success or failure branch. */
    NewOpenInstance(200000, StoreNode::GetInstance()->GetNodeId() + 1, "/CloseRemote", O_RDWR);

    memset(readBuf, 0, readSize);

    int ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), writeBuf, FALCON_STORE_STREAM_MAX_SIZE, 0);
    EXPECT_EQ(ret, 0);

    ret = FalconStore::GetInstance()->CloseTmpFiles(openInstance.get(), true, true);
    EXPECT_EQ(ret, 0);
    ret = FalconStore::GetInstance()->CloseTmpFiles(openInstance.get(), true, true);
    EXPECT_EQ(ret, 0);
    ret = FalconStore::GetInstance()->CloseTmpFiles(openInstance.get(), false, true);
    EXPECT_EQ(ret, 0);
}

/* ------------------------------------------- delete local -------------------------------------------*/

TEST_F(FalconStoreUT, DeleteLocal)
{
    /* Exercise Delete local and assert the relevant success or failure branch. */
    uint64_t inodeId = 100;
    int nodeId = StoreNode::GetInstance()->GetNodeId();
    std::string path = "/OpenLocal";
    int ret = FalconStore::GetInstance()->DeleteFiles(inodeId, nodeId, path);
    EXPECT_EQ(ret, 0);

    inodeId = 1000;
    path = "/WriteLocal";
    ret = FalconStore::GetInstance()->DeleteFiles(inodeId, nodeId, path);
    EXPECT_EQ(ret, 0);

    inodeId = 10000;
    path = "/ReadLocalSmall";
    ret = FalconStore::GetInstance()->DeleteFiles(inodeId, nodeId, path);
    EXPECT_EQ(ret, 0);

    inodeId = 10001;
    path = "/ReadLocalLarge";
    ret = FalconStore::GetInstance()->DeleteFiles(inodeId, nodeId, path);
    EXPECT_EQ(ret, 0);

    inodeId = 100000;
    path = "/CloseLocal";
    ret = FalconStore::GetInstance()->DeleteFiles(inodeId, nodeId, path);
    EXPECT_EQ(ret, 0);
}

TEST_F(FalconStoreUT, DeleteLocalNoneExist)
{
    /* Exercise Delete local missing and assert the relevant success or failure branch. */
    uint64_t inodeId = 1000000;
    int nodeId = StoreNode::GetInstance()->GetNodeId();
    std::string path = "/DeleteLocalNoneExist";
    int ret = FalconStore::GetInstance()->DeleteFiles(inodeId, nodeId, path);
    if (config->GetBool(FalconPropertyKey::FALCON_PERSIST)) {
        EXPECT_EQ(ret, 0);
    } else {
        EXPECT_EQ(ret, -ENOENT);
    }
}

/* ------------------------------------------- delete remote -------------------------------------------*/

TEST_F(FalconStoreUT, DeleteRemote)
{
    /* Exercise Delete remote and assert the relevant success or failure branch. */
    uint64_t inodeId = 200;
    int nodeId = StoreNode::GetInstance()->GetNodeId();
    std::string path = "/OpenRemote";
    int ret = FalconStore::GetInstance()->DeleteFiles(inodeId, nodeId, path);
    EXPECT_EQ(ret, 0);

    inodeId = 2000;
    path = "/WriteRemote";
    ret = FalconStore::GetInstance()->DeleteFiles(inodeId, nodeId, path);
    EXPECT_EQ(ret, 0);

    inodeId = 20000;
    path = "/ReadRemoteSmall";
    ret = FalconStore::GetInstance()->DeleteFiles(inodeId, nodeId, path);
    EXPECT_EQ(ret, 0);

    inodeId = 20001;
    path = "/ReadRemoteLarge";
    ret = FalconStore::GetInstance()->DeleteFiles(inodeId, nodeId, path);
    EXPECT_EQ(ret, 0);

    inodeId = 200000;
    path = "/CloseRemote";
    ret = FalconStore::GetInstance()->DeleteFiles(inodeId, nodeId, path);
    EXPECT_EQ(ret, 0);
}

TEST_F(FalconStoreUT, DeleteRemoteNoneExist)
{
    /* Exercise Delete remote missing and assert the relevant success or failure branch. */
    uint64_t inodeId = 2000000;
    int nodeId = StoreNode::GetInstance()->GetNodeId() + 1;
    std::string path = "/DeleteRemoteNoneExist";
    int ret = FalconStore::GetInstance()->DeleteFiles(inodeId, nodeId, path);
    if (config->GetBool(FalconPropertyKey::FALCON_PERSIST)) {
        EXPECT_EQ(ret, 0);
    } else {
        EXPECT_EQ(ret, -ENOENT);
    }
}

/* ------------------------------------------- statFs -------------------------------------------*/

TEST_F(FalconStoreUT, StatFs)
{
    /* Exercise Stat FS and assert the relevant success or failure branch. */
    struct statvfs vfsbuf;
    int ret = FalconStore::GetInstance()->StatFS(&vfsbuf);
    EXPECT_EQ(ret, 0);
}

/* ------------------------------------------- truncate file local -------------------------------------------*/

TEST_F(FalconStoreUT, TruncateFileLocal)
{
    /* Exercise Truncate File local and assert the relevant success or failure branch. */
    NewOpenInstance(10000000, StoreNode::GetInstance()->GetNodeId(), "/TruncateFileLocal", O_WRONLY | O_CREAT);

    int ret = FalconStore::GetInstance()->TruncateFile(openInstance.get(), 1000);
    EXPECT_EQ(ret, 0);
    ret = FalconStore::GetInstance()->DeleteFiles(openInstance->inodeId, openInstance->nodeId, openInstance->path);
    EXPECT_EQ(ret, 0);
}

TEST_F(FalconStoreUT, TruncateFileLocalNoneExist)
{
    /* Exercise Truncate File local missing and assert the relevant success or failure branch. */
    NewOpenInstance(10000001, StoreNode::GetInstance()->GetNodeId(), "/TruncateFileLocalNoneExist", O_WRONLY);
    openInstance->originalSize = 1;

    int ret = FalconStore::GetInstance()->TruncateFile(openInstance.get(), 1000);
    if (config->GetBool(FalconPropertyKey::FALCON_PERSIST)) {
        EXPECT_EQ(ret, -EIO);
    } else {
        EXPECT_EQ(ret, -ENOENT);
    }
}

/* ------------------------------------------- truncate file remote -------------------------------------------*/

TEST_F(FalconStoreUT, TruncateFileRemote)
{
    /* Exercise Truncate File remote and assert the relevant success or failure branch. */
    NewOpenInstance(20000000, StoreNode::GetInstance()->GetNodeId() + 1, "/TruncateFileRemote", O_WRONLY | O_CREAT);

    int ret = FalconStore::GetInstance()->TruncateFile(openInstance.get(), 1000);
    EXPECT_EQ(ret, 0);
    ret = FalconStore::GetInstance()->DeleteFiles(openInstance->inodeId, openInstance->nodeId, openInstance->path);
    EXPECT_EQ(ret, 0);
}

TEST_F(FalconStoreUT, TruncateFileRemoteNoneExist)
{
    /* Exercise Truncate File remote missing and assert the relevant success or failure branch. */
    NewOpenInstance(20000001, StoreNode::GetInstance()->GetNodeId() + 1, "/TruncateFileRemoteNoneExist", O_WRONLY);
    openInstance->originalSize = 1;

    int ret = FalconStore::GetInstance()->TruncateFile(openInstance.get(), 1000);
    if (config->GetBool(FalconPropertyKey::FALCON_PERSIST)) {
        EXPECT_EQ(ret, -EIO);
    } else {
        EXPECT_EQ(ret, -ENOENT);
    }
}

/* ------------------------------------------- truncate openInstance local -------------------------------------------*/

TEST_F(FalconStoreUT, TruncateOpenInstanceLocal)
{
    /* Exercise Truncate Open Instance local and assert the relevant success or failure branch. */
    NewOpenInstance(10000010, StoreNode::GetInstance()->GetNodeId(), "/TruncateOpenInstanceLocal", O_WRONLY);

    int ret = FalconStore::GetInstance()->TruncateOpenInstance(openInstance.get(), 1000);
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(openInstance->originalSize, 1000);
    EXPECT_EQ(openInstance->currentSize, 1000);
}

TEST_F(FalconStoreUT, WriteTruncateOpenInstanceLocal)
{
    /* Exercise Write Truncate Open Instance local and assert the relevant success or failure branch. */
    NewOpenInstance(10000010, StoreNode::GetInstance()->GetNodeId(), "/TruncateOpenInstanceLocal", O_WRONLY | O_CREAT);

    ResetBuf(true);

    int ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), writeBuf, size, 0);
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(openInstance->originalSize, 0);
    EXPECT_EQ(openInstance->currentSize, size);

    ret = FalconStore::GetInstance()->TruncateOpenInstance(openInstance.get(), 1000);
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(openInstance->originalSize, 1000);
    EXPECT_EQ(openInstance->currentSize, 1000);
}

/* ------------------------------------------- truncate openInstance remote
 * -------------------------------------------*/

TEST_F(FalconStoreUT, TruncateOpenInstanceRemote)
{
    /* Exercise Truncate Open Instance remote and assert the relevant success or failure branch. */
    NewOpenInstance(20000010, StoreNode::GetInstance()->GetNodeId() + 1, "/TruncateOpenInstanceRemote", O_WRONLY);

    int ret = FalconStore::GetInstance()->TruncateOpenInstance(openInstance.get(), 1000);
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(openInstance->originalSize, 1000);
    EXPECT_EQ(openInstance->currentSize, 1000);
}

TEST_F(FalconStoreUT, WriteTruncateOpenInstanceRemote)
{
    /* Exercise Write Truncate Open Instance remote and assert the relevant success or failure branch. */
    NewOpenInstance(20000010,
                    StoreNode::GetInstance()->GetNodeId() + 1,
                    "/TruncateOpenInstanceRemote",
                    O_WRONLY | O_CREAT);

    ResetBuf(true);
    int ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), writeBuf, size, 0);
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(openInstance->originalSize, 0);
    EXPECT_EQ(openInstance->currentSize, size);

    ret = FalconStore::GetInstance()->TruncateOpenInstance(openInstance.get(), 1000);
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(openInstance->originalSize, 1000);
    EXPECT_EQ(openInstance->currentSize, 1000);
    sleep(2);
}

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
