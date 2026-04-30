#include <cerrno>
#include <cstring>
#include <string>
#include <sys/stat.h>
#include <sys/statvfs.h>

#include <gtest/gtest.h>

#include "falcon_code.h"
#include "remote_connection_utils/error_code_def.h"
#include "stats/falcon_stats.h"

namespace {

struct MockFalconApiState {
    int mkdirRet = SUCCESS;
    int createRet = SUCCESS;
    int openRet = SUCCESS;
    int unlinkRet = SUCCESS;
    int openDirRet = SUCCESS;
    int readDirRet = SUCCESS;
    int closeRet = SUCCESS;
    int getStatRet = SUCCESS;
    int closeDirRet = SUCCESS;
    int destroyRet = SUCCESS;
    int rmDirRet = SUCCESS;
    int writeRet = SUCCESS;
    int readRet = 0;
    int renameRet = SUCCESS;
    int renamePersistRet = SUCCESS;
    int fsyncRet = SUCCESS;
    int statFsRet = SUCCESS;
    int utimensRet = SUCCESS;
    int chownRet = SUCCESS;
    int chmodRet = SUCCESS;
    int truncateRet = SUCCESS;

    uint64_t nextFd = 100;
    std::string lastPath;
    std::string lastDstPath;
    bool lastCloseFlush = false;
    int lastCloseDatasync = -1;
    int64_t lastAccessTime = -2;
    int64_t lastModifyTime = -2;
};

MockFalconApiState g_mockState;

void ResetMockState()
{
    g_mockState = MockFalconApiState{};
    for (auto &stat : FalconStats::GetInstance().stats) {
        stat.store(0);
    }
}

} // namespace

#define FALCONFS_FUSE_MAIN_DISABLE_MAIN
#include "falcon_client/fuse_main.cpp"

int ErrorCodeToErrno(int errorCode)
{
    return errorCode == FILE_EXISTS ? EEXIST : EIO;
}

int FalconMkdir(const std::string &path)
{
    g_mockState.lastPath = path;
    return g_mockState.mkdirRet;
}

int FalconCreate(const std::string &path, uint64_t &fd, int, struct stat *stbuf)
{
    g_mockState.lastPath = path;
    fd = g_mockState.nextFd;
    if (stbuf) {
        stbuf->st_mode = S_IFREG | 0644;
    }
    return g_mockState.createRet;
}

int FalconOpen(const std::string &path, int, uint64_t &fd, struct stat *stbuf)
{
    g_mockState.lastPath = path;
    fd = g_mockState.nextFd;
    if (stbuf) {
        stbuf->st_mode = S_IFREG | 0644;
    }
    return g_mockState.openRet;
}

int FalconUnlink(const std::string &path)
{
    g_mockState.lastPath = path;
    return g_mockState.unlinkRet;
}

int FalconOpenDir(const std::string &path, struct FalconFuseInfo *fi)
{
    g_mockState.lastPath = path;
    fi->fh = g_mockState.nextFd;
    return g_mockState.openDirRet;
}

int FalconReadDir(const std::string &path, void *buf, FalconFuseFiller filler, off_t, struct FalconFuseInfo *)
{
    g_mockState.lastPath = path;
    if (filler != nullptr) {
        filler(buf, "entry", nullptr, 0);
    }
    return g_mockState.readDirRet;
}

int FalconClose(const std::string &path, uint64_t, bool isFlush, int datasync)
{
    g_mockState.lastPath = path;
    g_mockState.lastCloseFlush = isFlush;
    g_mockState.lastCloseDatasync = datasync;
    return g_mockState.closeRet;
}

int FalconGetStat(const std::string &path, struct stat *stbuf)
{
    g_mockState.lastPath = path;
    if (stbuf) {
        stbuf->st_mode = S_IFREG | 0644;
        stbuf->st_size = 4096;
    }
    return g_mockState.getStatRet;
}

int FalconCloseDir(uint64_t)
{
    return g_mockState.closeDirRet;
}

int FalconDestroy()
{
    return g_mockState.destroyRet;
}

int FalconRmDir(const std::string &path)
{
    g_mockState.lastPath = path;
    return g_mockState.rmDirRet;
}

int FalconWrite(uint64_t, const std::string &path, const char *, size_t, off_t)
{
    g_mockState.lastPath = path;
    return g_mockState.writeRet;
}

int FalconRead(const std::string &path, uint64_t, char *buffer, size_t size, off_t)
{
    g_mockState.lastPath = path;
    if (buffer && size > 0) {
        buffer[0] = 'x';
    }
    return g_mockState.readRet;
}

int FalconRename(const std::string &srcName, const std::string &dstName)
{
    g_mockState.lastPath = srcName;
    g_mockState.lastDstPath = dstName;
    return g_mockState.renameRet;
}

int FalconRenamePersist(const std::string &srcName, const std::string &dstName)
{
    g_mockState.lastPath = srcName;
    g_mockState.lastDstPath = dstName;
    return g_mockState.renamePersistRet;
}

int FalconFsync(const std::string &path, uint64_t, int datasync)
{
    g_mockState.lastPath = path;
    g_mockState.lastCloseDatasync = datasync;
    return g_mockState.fsyncRet;
}

int FalconStatFS(struct statvfs *vfsbuf)
{
    if (vfsbuf) {
        vfsbuf->f_bsize = 4096;
    }
    return g_mockState.statFsRet;
}

int FalconUtimens(const std::string &path, int64_t accessTime, int64_t modifyTime)
{
    g_mockState.lastPath = path;
    g_mockState.lastAccessTime = accessTime;
    g_mockState.lastModifyTime = modifyTime;
    return g_mockState.utimensRet;
}

int FalconChown(const std::string &path, uid_t, gid_t)
{
    g_mockState.lastPath = path;
    return g_mockState.chownRet;
}

int FalconChmod(const std::string &path, mode_t)
{
    g_mockState.lastPath = path;
    return g_mockState.chmodRet;
}

int FalconTruncate(const std::string &path, off_t)
{
    g_mockState.lastPath = path;
    return g_mockState.truncateRet;
}

namespace {

int CountFiller(void *buf, const char *, const struct stat *, off_t)
{
    auto *count = static_cast<int *>(buf);
    ++(*count);
    return 0;
}

class FuseMainMockUT : public testing::Test {
protected:
    void SetUp() override
    {
        ResetMockState();
        g_persist = false;
    }
};

TEST_F(FuseMainMockUT, RejectsInvalidArgumentsBeforeCallingFalconApi)
{
    struct fuse_file_info fi {};
    char buffer[8] {};
    struct stat st {};
    struct statvfs vfs {};

    EXPECT_EQ(-EINVAL, DoGetAttr(nullptr, &st));
    EXPECT_EQ(-EINVAL, DoMkDir("", 0755));
    EXPECT_EQ(-EINVAL, DoOpen(nullptr, &fi));
    EXPECT_EQ(-EINVAL, DoOpenAtomic("", &st, 0644, &fi));
    EXPECT_EQ(-EINVAL, DoOpenDir(nullptr, &fi));
    EXPECT_EQ(-EINVAL, DoReadDir("", buffer, nullptr, 0, &fi));
    EXPECT_EQ(-EINVAL, DoCreate(nullptr, 0644, &fi));
    EXPECT_EQ(-EINVAL, DoAccess("", 0));
    EXPECT_EQ(-EINVAL, DoRelease(nullptr, &fi));
    EXPECT_EQ(-EINVAL, DoReleaseDir("", &fi));
    EXPECT_EQ(-EINVAL, DoUnlink(nullptr));
    EXPECT_EQ(-EINVAL, DoRmDir(""));
    EXPECT_EQ(-EINVAL, DoWrite(nullptr, buffer, sizeof(buffer), 0, &fi));
    EXPECT_EQ(-EINVAL, DoRead("/file", nullptr, sizeof(buffer), 0, &fi));
    EXPECT_EQ(-EINVAL, DoSetXAttr("/file", "k", nullptr, 0, 0));
    EXPECT_EQ(-EINVAL, DoTruncate(nullptr, 0));
    EXPECT_EQ(-EINVAL, DoFtruncate("", 0, &fi));
    EXPECT_EQ(-EINVAL, DoFlush(nullptr, &fi));
    EXPECT_EQ(-EINVAL, DoRename("/src", ""));
    EXPECT_EQ(-EINVAL, DoFsync(nullptr, 0, &fi));
    EXPECT_EQ(-EINVAL, DoStatfs("", &vfs));
    EXPECT_EQ(-EINVAL, DoUtimens(nullptr, nullptr));
    EXPECT_EQ(-EINVAL, DoChmod("", 0644));
    EXPECT_EQ(-EINVAL, DoChown(nullptr, 1, 1));
}

TEST_F(FuseMainMockUT, DispatchesSuccessfulMetadataCallbacks)
{
    struct fuse_file_info fi {};
    fi.flags = O_CREAT;
    fi.fh = 77;
    struct stat st {};
    struct statvfs vfs {};
    char readBuffer[8] {};
    int fillerCount = 0;
    g_mockState.readRet = 3;

    EXPECT_EQ(0, DoGetAttr("/file", &st));
    EXPECT_EQ(S_IFREG | 0644, st.st_mode);
    EXPECT_EQ(0, DoMkDir("/dir", 0755));
    EXPECT_EQ(0, DoOpen("/file", &fi));
    EXPECT_EQ(g_mockState.nextFd, fi.fh);
    EXPECT_EQ(0, DoOpenAtomic("/created", &st, 0644, &fi));
    EXPECT_EQ(0, DoCreate("/new", 0644, &fi));
    EXPECT_EQ(0, DoAccess("/file", R_OK));
    EXPECT_EQ(0, DoOpenDir("/dir", &fi));
    EXPECT_EQ(0, DoReadDir("/dir", &fillerCount, CountFiller, 0, &fi));
    EXPECT_EQ(1, fillerCount);
    EXPECT_EQ(0, DoRelease("/file", &fi));
    EXPECT_FALSE(g_mockState.lastCloseFlush);
    EXPECT_EQ(0, DoReleaseDir("/dir", &fi));
    EXPECT_EQ(0, DoUnlink("/file"));
    EXPECT_EQ(0, DoRmDir("/dir"));
    EXPECT_EQ(4, DoWrite("/file", "data", 4, 0, &fi));
    EXPECT_EQ(3, DoRead("/file", readBuffer, sizeof(readBuffer), 0, &fi));
    EXPECT_EQ('x', readBuffer[0]);
    EXPECT_EQ(0, DoSetXAttr("/file", "k", "v", 1, 0));
    EXPECT_EQ(0, DoTruncate("/file", 1));
    EXPECT_EQ(0, DoFtruncate("/file", 2, &fi));
    EXPECT_EQ(0, DoFlush("/file", &fi));
    EXPECT_TRUE(g_mockState.lastCloseFlush);
    EXPECT_EQ(0, DoRename("/src", "/dst"));
    EXPECT_EQ("/src", g_mockState.lastPath);
    EXPECT_EQ("/dst", g_mockState.lastDstPath);
    EXPECT_EQ(0, DoFsync("/file", 1, &fi));
    EXPECT_EQ(1, g_mockState.lastCloseDatasync);
    EXPECT_EQ(0, DoStatfs("/file", &vfs));
    EXPECT_EQ(4096U, vfs.f_bsize);
    EXPECT_EQ(0, DoUtimens("/file", nullptr));
    EXPECT_EQ(-1, g_mockState.lastAccessTime);
    EXPECT_EQ(0, DoChmod("/file", 0600));
    EXPECT_EQ(0, DoChown("/file", 1, 1));

    DoDestroy(nullptr);
}

TEST_F(FuseMainMockUT, ConvertsPositiveFalconErrorsToNegativeErrno)
{
    struct fuse_file_info fi {};
    struct stat st {};
    struct statvfs vfs {};
    g_mockState.mkdirRet = FILE_EXISTS;
    g_mockState.getStatRet = 999;
    g_mockState.createRet = 999;
    g_mockState.closeRet = 999;
    g_mockState.statFsRet = 999;

    EXPECT_EQ(-EEXIST, DoMkDir("/dir", 0755));
    EXPECT_EQ(-EIO, DoGetAttr("/file", &st));
    EXPECT_EQ(-EIO, DoCreate("/file", 0644, &fi));
    EXPECT_EQ(-EIO, DoRelease("/file", &fi));
    EXPECT_EQ(-EIO, DoStatfs("/file", &vfs));
}

TEST_F(FuseMainMockUT, HandlesLookupFastPathAndPersistRename)
{
    struct stat st {};
    char lookupPath[] = {'/', '\1', '1', 'd', 'i', 'r', '\0'};
    EXPECT_EQ(0, DoGetAttr(lookupPath, &st));
    EXPECT_EQ(040777, st.st_mode);
    EXPECT_STREQ("/dir", lookupPath);

    g_persist = true;
    EXPECT_EQ(0, DoRename("/old", "/new"));
    EXPECT_EQ("/old", g_mockState.lastPath);
    EXPECT_EQ("/new", g_mockState.lastDstPath);
}

TEST_F(FuseMainMockUT, DispatchesExplicitUtimensValues)
{
    struct timespec tv[2] {};
    tv[0].tv_sec = 11;
    tv[1].tv_sec = 22;

    EXPECT_EQ(0, DoUtimens("/file", tv));
    EXPECT_EQ(11, g_mockState.lastAccessTime);
    EXPECT_EQ(22, g_mockState.lastModifyTime);
}

} // namespace
