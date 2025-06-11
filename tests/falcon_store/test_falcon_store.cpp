#include "test_falcon_store.h"

#include "connection/node.h"

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
