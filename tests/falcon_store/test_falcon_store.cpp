#include "test_falcon_store.h"

#include "connection/node.h"
#include "disk_cache/disk_cache.h"

unsigned long myHash(std::string &str);

namespace {

class FakeStorage : public Storage {
  public:
    ssize_t readResult = 0;
    int putFileResult = 0;
    int deleteResult = 0;
    int copyResult = 0;
    std::string payload = "storage-payload";
    int readCalls = 0;
    int putFileCalls = 0;
    int deleteCalls = 0;
    int copyCalls = 0;
    int statCalls = 0;

    void DeleteInstance() override {}
    int Init() override { return 0; }
    ssize_t ReadObject(const std::string &, uint64_t, uint64_t size, int fd, char *destBuffer) override
    {
        readCalls++;
        if (readResult < 0) {
            return readResult;
        }
        size_t bytes = size == 0 ? payload.size() : std::min<size_t>(size, payload.size());
        if (destBuffer != nullptr) {
            memcpy(destBuffer, payload.data(), bytes);
        }
        if (fd >= 0 && bytes > 0) {
            (void)pwrite(fd, payload.data(), bytes, 0);
        }
        return static_cast<ssize_t>(bytes);
    }
    int PutFile(const std::string &, const std::string &) override
    {
        putFileCalls++;
        return putFileResult;
    }
    ssize_t PutBuffer(const std::string &, const char *, const uint64_t size, const uint64_t) override
    {
        return static_cast<ssize_t>(size);
    }
    int DeleteObject(const std::string &) override
    {
        deleteCalls++;
        return deleteResult;
    }
    int CopyObject(const std::string &, const std::string &) override
    {
        copyCalls++;
        return copyResult;
    }
    int StatFs(struct statvfs *vfsbuf) override
    {
        statCalls++;
        memset(vfsbuf, 0, sizeof(*vfsbuf));
        vfsbuf->f_blocks = 10;
        vfsbuf->f_bfree = 8;
        return 0;
    }
};

void ResetFalconStatsForCoverage()
{
    for (int i = 0; i < STATS_END; ++i) {
        FalconStats::GetInstance().stats[i] = 0;
        FalconStats::GetInstance().storedStats[i] = 0;
    }
}

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
    NewOpenInstance(100, StoreNode::GetInstance()->GetNodeId(), "/OpenLocal", O_WRONLY | O_CREAT);

    int ret = FalconStore::GetInstance()->OpenFile(openInstance.get());
    EXPECT_EQ(ret, 0);
}

TEST_F(FalconStoreUT, OpenLocalWRonlyExist)
{
    NewOpenInstance(100, StoreNode::GetInstance()->GetNodeId(), "/OpenLocal", O_WRONLY);

    int ret = FalconStore::GetInstance()->OpenFile(openInstance.get());
    EXPECT_EQ(ret, 0);
}

TEST_F(FalconStoreUT, OpenLocalWRonlyNoneExist)
{
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
    NewOpenInstance(100, StoreNode::GetInstance()->GetNodeId(), "/OpenLocal", O_RDONLY);

    int ret = FalconStore::GetInstance()->OpenFile(openInstance.get());
    EXPECT_EQ(ret, 0);
}

TEST_F(FalconStoreUT, OpenLocalRDonlyNoneExist)
{
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
    NewOpenInstance(100, StoreNode::GetInstance()->GetNodeId(), "/OpenLocal", O_RDWR);

    int ret = FalconStore::GetInstance()->OpenFile(openInstance.get());
    EXPECT_EQ(ret, 0);
}

TEST_F(FalconStoreUT, OpenLocalRDWRNoneExist)
{
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
    NewOpenInstance(200, StoreNode::GetInstance()->GetNodeId() + 1, "/OpenRemote", O_WRONLY | O_CREAT);

    int ret = FalconStore::GetInstance()->OpenFile(openInstance.get());
    EXPECT_EQ(ret, 0);
}

TEST_F(FalconStoreUT, OpenRemoteWRonlyExist)
{
    NewOpenInstance(200, StoreNode::GetInstance()->GetNodeId() + 1, "/OpenRemote", O_WRONLY);

    int ret = FalconStore::GetInstance()->OpenFile(openInstance.get());
    EXPECT_EQ(ret, 0);
}

TEST_F(FalconStoreUT, OpenRemoteWRonlyNoneExist)
{
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
    NewOpenInstance(200, StoreNode::GetInstance()->GetNodeId() + 1, "/OpenRemote", O_RDONLY);

    int ret = FalconStore::GetInstance()->OpenFile(openInstance.get());
    EXPECT_EQ(ret, 0);
}

TEST_F(FalconStoreUT, OpenRemoteRDonlyNoneExist)
{
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
    NewOpenInstance(200, StoreNode::GetInstance()->GetNodeId() + 1, "/OpenRemote", O_RDWR);

    int ret = FalconStore::GetInstance()->OpenFile(openInstance.get());
    EXPECT_EQ(ret, 0);
}

TEST_F(FalconStoreUT, OpenRemoteRDWRNoneExist)
{
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
    EXPECT_EQ(GetParentPath("/a/b/c", -1), "/a/b");
    EXPECT_EQ(GetParentPath("/a/b/c", 1), "/");
    EXPECT_EQ(GetParentPath("/a/b/c", 2), "/a/");
    EXPECT_EQ(GetParentPath("/a/b/c", 5), "/a/b/");
}

TEST_F(FalconStoreUT, PrivateHelpersAndStorageDownloadBranches)
{
    auto *store = FalconStore::GetInstance();
    std::string nodeConfig = "coverage-node-config";
    store->SetFalconStoreParam(nodeConfig);
    EXPECT_EQ(store->nodeConfig, nodeConfig);
    EXPECT_TRUE(store->ConnectionError(ETIMEDOUT));
    EXPECT_FALSE(store->ConnectionError(0));
    EXPECT_TRUE(store->IoError(-EIO));
    EXPECT_FALSE(store->IoError(0));

    store->parentPathLevel = 2;
    std::string path = "/private/helper/file";
    EXPECT_NE(myHash(path), 0UL);
    int pathNode = store->PathToNodeId(path);
    EXPECT_GE(pathNode, 0);
    EXPECT_TRUE(store->nodeHash.count("/private/") > 0);

    NewOpenInstance(912700, -1, "/alloc/local", O_RDWR | O_CREAT);
    store->toLocal = true;
    store->isInference = false;
    store->AllocNodeId(openInstance.get());
    EXPECT_EQ(openInstance->nodeId, StoreNode::GetInstance()->GetNodeId());

    NewOpenInstance(912701, -1, "/alloc/inference/file", O_RDWR | O_CREAT);
    store->toLocal = false;
    store->isInference = true;
    store->AllocNodeId(openInstance.get());
    EXPECT_GE(openInstance->nodeId, 0);

    NewOpenInstance(912702, StoreNode::GetInstance()->GetNodeId(), "/random/direct", O_RDONLY | __O_DIRECT);
    openInstance->currentSize = 512;
    std::string directFile = GetFilePath(openInstance->inodeId);
    int fd = open(directFile.c_str(), O_CREAT | O_RDWR, 0755);
    ASSERT_GE(fd, 0);
    std::string payload(512, 'd');
    ASSERT_EQ(pwrite(fd, payload.data(), payload.size(), 0), static_cast<ssize_t>(payload.size()));
    openInstance->physicalFd = fd;
    char readBuffer[512] = {};
    FalconReadBuffer falconReadBuffer{readBuffer, sizeof(readBuffer)};
    EXPECT_EQ(store->RandomRead(falconReadBuffer, openInstance.get(), 0), static_cast<int>(sizeof(readBuffer)));
    close(fd);
    unlink(directFile.c_str());

    FakeStorage fakeStorage;
    Storage *oldStorage = store->storage;
    bool oldPersist = store->persistToStorage;
    store->storage = &fakeStorage;
    store->persistToStorage = true;
    NewOpenInstance(912703, StoreNode::GetInstance()->GetNodeId(), "/download/sync", O_RDONLY);
    openInstance->originalSize = fakeStorage.payload.size();
    openInstance->readBufferSize = fakeStorage.payload.size();
    openInstance->readBuffer.reset(new char[openInstance->readBufferSize], std::default_delete<char[]>());
    EXPECT_EQ(store->DownLoadFromStorage(openInstance.get(), true, true), 0);
    EXPECT_EQ(fakeStorage.readCalls, 1);
    EXPECT_TRUE(DiskCache::GetInstance().Find(openInstance->inodeId, false));
    DiskCache::GetInstance().Delete(openInstance->inodeId);

    fakeStorage.readResult = -EIO;
    NewOpenInstance(912704, StoreNode::GetInstance()->GetNodeId(), "/download/fail", O_RDONLY);
    openInstance->originalSize = 4;
    openInstance->readBufferSize = 4;
    openInstance->readBuffer.reset(new char[openInstance->readBufferSize], std::default_delete<char[]>());
    EXPECT_EQ(store->DownLoadFromStorage(openInstance.get(), true, true), -EIO);

    char brpcBuffer[32] = {};
    fakeStorage.readResult = 0;
    EXPECT_EQ(store->DownLoadFromStorageForBrpc(912705, "/download/brpc", brpcBuffer, fakeStorage.payload.size(),
                                                true, true),
              0);
    EXPECT_EQ(std::string(brpcBuffer, brpcBuffer + fakeStorage.payload.size()), fakeStorage.payload);
    EXPECT_TRUE(DiskCache::GetInstance().Find(912705, false));
    DiskCache::GetInstance().Delete(912705);

    fakeStorage.readResult = -EIO;
    EXPECT_EQ(store->DownLoadFromStorageForBrpc(912706, "/download/brpc_fail", brpcBuffer, 4, true, true), -EIO);

    fakeStorage.readResult = 0;
    fakeStorage.putFileResult = 0;
    EXPECT_EQ(store->FlushToStorage("/flush/object", 912705), 0);
    fakeStorage.putFileResult = -1;
    EXPECT_EQ(store->FlushToStorage("/flush/object", 912705), -EIO);
    EXPECT_EQ(store->CopyData("/src/object", "/dst/object"), 0);
    EXPECT_EQ(fakeStorage.copyCalls, 1);
    EXPECT_EQ(store->DeleteDataAfterRename("/dst/object"), 0);
    EXPECT_EQ(fakeStorage.deleteCalls, 1);
    struct statvfs vfsbuf {};
    EXPECT_EQ(store->StatFsStorage(&vfsbuf), 0);
    EXPECT_EQ(vfsbuf.f_blocks, 10U);

    store->storage = oldStorage;
    store->persistToStorage = oldPersist;
}

TEST_F(FalconStoreUT, BrpcWritePublicBranches)
{
    auto *store = FalconStore::GetInstance();
    const std::string payload = "brpc-nondirect-payload";

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
}

TEST_F(FalconStoreUT, CloseTmpFilesPublicBranches)
{
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

    char buffer[64] = {};
    DiskCache::GetInstance().InsertAndUpdate(913100, payload.size(), false);
    EXPECT_EQ(store->ReadSmallFilesForBrpc(913100, "/brpc/stale-cache", buffer, payload.size(), O_RDONLY, false),
              -ENOENT);

    memset(buffer, 0, sizeof(buffer));
    EXPECT_EQ(store->ReadSmallFilesForBrpc(913101, "/brpc/no-storage", buffer, payload.size(), O_RDONLY, false),
              -ENOENT);

    FalconStats::GetInstance().stats[BLOCKCACHE_READ] = 0;
    FalconStats::GetInstance().stats[BLOCKCACHE_WRITE] = 0;
}

TEST_F(FalconStoreUT, OpenStatAndTruncatePublicBranches)
{
    auto *store = FalconStore::GetInstance();

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
}

TEST_F(FalconStoreUT, ReadPublicBranches)
{
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

    ResetFalconStatsForCoverage();
}

TEST_F(FalconStoreUT, DeleteAndStatPublicBranches)
{
    auto *store = FalconStore::GetInstance();

    EXPECT_EQ(store->DeleteFiles(913400, StoreNode::GetInstance()->GetNodeId(), "/delete/missing"), -ENOENT);

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

    ResetFalconStatsForCoverage();
}

/* ------------------------------------------- write local -------------------------------------------*/

size_t writeLocalSize = 0;

TEST_F(FalconStoreUT, WriteLocalLarge)
{
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
    NewOpenInstance(100000, StoreNode::GetInstance()->GetNodeId(), "/CloseLocal", O_RDWR | O_CREAT);

    ResetBuf(true);

    int ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), writeBuf, FALCON_STORE_STREAM_MAX_SIZE, 0);
    EXPECT_EQ(ret, 0);

    ret = FalconStore::GetInstance()->CloseTmpFiles(openInstance.get(), true, true);
    EXPECT_EQ(ret, 0);
}

TEST_F(FalconStoreUT, ReleaseLocal)
{
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
    NewOpenInstance(100000, StoreNode::GetInstance()->GetNodeId(), "/CloseLocal", O_RDWR);

    memset(readBuf, 0, readSize);

    int ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), writeBuf, FALCON_STORE_STREAM_MAX_SIZE, 0);
    EXPECT_EQ(ret, 0);

    ret = FalconStore::GetInstance()->CloseTmpFiles(openInstance.get(), false, true);
    EXPECT_EQ(ret, 0);
}

TEST_F(FalconStoreUT, FlushTwiceLocal)
{
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
    NewOpenInstance(200000, StoreNode::GetInstance()->GetNodeId() + 1, "/CloseRemote", O_RDWR | O_CREAT);

    memset(readBuf, 0, readSize);

    int ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), writeBuf, FALCON_STORE_STREAM_MAX_SIZE, 0);
    EXPECT_EQ(ret, 0);

    ret = FalconStore::GetInstance()->CloseTmpFiles(openInstance.get(), true, true);
    EXPECT_EQ(ret, 0);
}

TEST_F(FalconStoreUT, ReleaseRemote)
{
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
    NewOpenInstance(200000, StoreNode::GetInstance()->GetNodeId() + 1, "/CloseRemote", O_RDWR);

    memset(readBuf, 0, readSize);

    int ret = FalconStore::GetInstance()->WriteFile(openInstance.get(), writeBuf, FALCON_STORE_STREAM_MAX_SIZE, 0);
    EXPECT_EQ(ret, 0);

    ret = FalconStore::GetInstance()->CloseTmpFiles(openInstance.get(), false, true);
    EXPECT_EQ(ret, 0);
}

TEST_F(FalconStoreUT, FlushTwiceRemote)
{
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
    struct statvfs vfsbuf;
    int ret = FalconStore::GetInstance()->StatFS(&vfsbuf);
    EXPECT_EQ(ret, 0);
}

/* ------------------------------------------- truncate file local -------------------------------------------*/

TEST_F(FalconStoreUT, TruncateFileLocal)
{
    NewOpenInstance(10000000, StoreNode::GetInstance()->GetNodeId(), "/TruncateFileLocal", O_WRONLY | O_CREAT);

    int ret = FalconStore::GetInstance()->TruncateFile(openInstance.get(), 1000);
    EXPECT_EQ(ret, 0);
    ret = FalconStore::GetInstance()->DeleteFiles(openInstance->inodeId, openInstance->nodeId, openInstance->path);
    EXPECT_EQ(ret, 0);
}

TEST_F(FalconStoreUT, TruncateFileLocalNoneExist)
{
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
    NewOpenInstance(20000000, StoreNode::GetInstance()->GetNodeId() + 1, "/TruncateFileRemote", O_WRONLY | O_CREAT);

    int ret = FalconStore::GetInstance()->TruncateFile(openInstance.get(), 1000);
    EXPECT_EQ(ret, 0);
    ret = FalconStore::GetInstance()->DeleteFiles(openInstance->inodeId, openInstance->nodeId, openInstance->path);
    EXPECT_EQ(ret, 0);
}

TEST_F(FalconStoreUT, TruncateFileRemoteNoneExist)
{
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
    NewOpenInstance(10000010, StoreNode::GetInstance()->GetNodeId(), "/TruncateOpenInstanceLocal", O_WRONLY);

    int ret = FalconStore::GetInstance()->TruncateOpenInstance(openInstance.get(), 1000);
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(openInstance->originalSize, 1000);
    EXPECT_EQ(openInstance->currentSize, 1000);
}

TEST_F(FalconStoreUT, WriteTruncateOpenInstanceLocal)
{
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
    NewOpenInstance(20000010, StoreNode::GetInstance()->GetNodeId() + 1, "/TruncateOpenInstanceRemote", O_WRONLY);

    int ret = FalconStore::GetInstance()->TruncateOpenInstance(openInstance.get(), 1000);
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(openInstance->originalSize, 1000);
    EXPECT_EQ(openInstance->currentSize, 1000);
}

TEST_F(FalconStoreUT, WriteTruncateOpenInstanceRemote)
{
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
