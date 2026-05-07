#include "error_code.h"
#include "router.h"
#include "falcon_meta.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cerrno>
#include <ctime>
#include <cstdlib>
#include <fcntl.h>
#include <cstring>
#include <memory>
#include <string>
#include <sys/statvfs.h>
#include <thread>
#include <unordered_map>
#include <unistd.h>
#include <vector>

extern "C" {
#include "remote_connection_utils/error_code_def.h"
}

#define FUSE_USE_VERSION 26
#include <fuse/fuse.h>

extern "C" int FalconFuseMainNoop(int, char **, const struct fuse_operations *, void *);
extern "C" void FalconFuseOptFreeArgsNoop(struct fuse_args *);

#define main FalconFuseMainCoverageEntry
#undef fuse_main
#define fuse_main FalconFuseMainNoop
#define fuse_opt_free_args FalconFuseOptFreeArgsNoop
#include "../../falcon_client/fuse_main.cpp"
#undef fuse_opt_free_args
#undef fuse_main
#undef main

extern "C" int FalconFuseMainNoop(int, char **, const struct fuse_operations *, void *)
{
    return 0;
}

extern "C" void FalconFuseOptFreeArgsNoop(struct fuse_args *) {}

TEST(FalconClientErrorCodeUT, MapsKnownFalconErrorsToErrno)
{
    EXPECT_EQ(ErrorCodeToErrno(SUCCESS), 0);
    EXPECT_EQ(ErrorCodeToErrno(PATH_IS_INVALID), ENOENT);
    EXPECT_EQ(ErrorCodeToErrno(WRONG_WORKER), ESRCH);
    EXPECT_EQ(ErrorCodeToErrno(OBSOLETE_SHARD), ENXIO);
    EXPECT_EQ(ErrorCodeToErrno(REMOTE_QUERY_FAILED), EREMOTEIO);
    EXPECT_EQ(ErrorCodeToErrno(ARGUMENT_ERROR), EINVAL);
    EXPECT_EQ(ErrorCodeToErrno(INODE_ROW_TYPE_ERROR), EILSEQ);
    EXPECT_EQ(ErrorCodeToErrno(FILE_EXISTS), EEXIST);
    EXPECT_EQ(ErrorCodeToErrno(FILE_NOT_EXISTS), ENOENT);
    EXPECT_EQ(ErrorCodeToErrno(OUT_OF_MEMORY), ENOMEM);
    EXPECT_EQ(ErrorCodeToErrno(STREAM_ERROR), EIO);
    EXPECT_EQ(ErrorCodeToErrno(PROGRAM_ERROR), EFAULT);
    EXPECT_EQ(ErrorCodeToErrno(SMART_ROUTE_ERROR), EHOSTUNREACH);
    EXPECT_EQ(ErrorCodeToErrno(SERVER_FAULT), ECONNRESET);
    EXPECT_EQ(ErrorCodeToErrno(UNDEFINED), EAGAIN);
    EXPECT_EQ(ErrorCodeToErrno(MKDIR_DUPLICATE), MKDIR_DUPLICATE);
    EXPECT_EQ(ErrorCodeToErrno(XKEY_EXISTS), EEXIST);
    EXPECT_EQ(ErrorCodeToErrno(XKEY_NOT_EXISTS), ENOENT);
    EXPECT_EQ(ErrorCodeToErrno(ZK_NOT_CONNECT), ENOTCONN);
    EXPECT_EQ(ErrorCodeToErrno(ZK_FETCH_RESULT_FAILED), EIO);
    EXPECT_EQ(ErrorCodeToErrno(TRANSACTION_FAULT), EIO);
    EXPECT_EQ(ErrorCodeToErrno(POOLED_FAULT), EAGAIN);
    EXPECT_EQ(ErrorCodeToErrno(NOT_FOUND_FD), EBADF);
    EXPECT_EQ(ErrorCodeToErrno(GET_ALL_WORKER_CONN_FAILED), EHOSTUNREACH);
    EXPECT_EQ(ErrorCodeToErrno(IO_ERROR), EIO);
    EXPECT_EQ(ErrorCodeToErrno(PATH_NOT_EXISTS), ENOENT);
    EXPECT_EQ(ErrorCodeToErrno(PATH_VERIFY_FAILED), EINVAL);
    EXPECT_EQ(ErrorCodeToErrno(999999), EIO);
}

TEST(FalconClientMetaUT, InvalidFdOperationsReturnErrorsWithoutService)
{
    const uint64_t missing_fd = UINT64_MAX - 7;
    char read_buffer[8] = {};
    const char write_buffer[] = "falcon";

    EXPECT_EQ(FalconWrite(missing_fd, "/missing", write_buffer, sizeof(write_buffer), 0), -EBADF);
    EXPECT_EQ(FalconRead("/missing", missing_fd, read_buffer, sizeof(read_buffer), 0), -EBADF);
    EXPECT_EQ(FalconClose("/missing", missing_fd, false, -1), NOT_FOUND_FD);
    EXPECT_EQ(FalconClose("/missing", missing_fd, true, -1), NOT_FOUND_FD);
    EXPECT_EQ(FalconFsync("/missing", missing_fd, 0), NOT_FOUND_FD);
    EXPECT_EQ(FalconFsync("/missing", missing_fd, 1), NOT_FOUND_FD);
    EXPECT_EQ(FalconCloseDir(missing_fd), NOT_FOUND_FD);
}

TEST(FalconClientFuseWrapperUT, InvalidArgumentsReturnEinval)
{
    struct fuse_file_info fi {};
    struct stat stbuf {};
    struct statvfs vfsbuf {};
    char buffer[8] = {};

    EXPECT_EQ(DoGetAttr(nullptr, &stbuf), -EINVAL);
    EXPECT_EQ(DoGetAttr("", &stbuf), -EINVAL);
    EXPECT_EQ(DoMkDir(nullptr, 0755), -EINVAL);
    EXPECT_EQ(DoOpen(nullptr, &fi), -EINVAL);
    EXPECT_EQ(DoOpenAtomic(nullptr, &stbuf, 0644, &fi), -EINVAL);
    EXPECT_EQ(DoOpenDir(nullptr, &fi), -EINVAL);
    EXPECT_EQ(DoReadDir(nullptr, buffer, nullptr, 0, &fi), -EINVAL);
    EXPECT_EQ(DoCreate(nullptr, 0644, &fi), -EINVAL);
    EXPECT_EQ(DoRelease(nullptr, &fi), -EINVAL);
    EXPECT_EQ(DoReleaseDir(nullptr, &fi), -EINVAL);
    EXPECT_EQ(DoUnlink(nullptr), -EINVAL);
    EXPECT_EQ(DoRmDir(nullptr), -EINVAL);
    EXPECT_EQ(DoWrite(nullptr, buffer, sizeof(buffer), 0, &fi), -EINVAL);
    EXPECT_EQ(DoWrite("/path", nullptr, sizeof(buffer), 0, &fi), -EINVAL);
    EXPECT_EQ(DoRead(nullptr, buffer, sizeof(buffer), 0, &fi), -EINVAL);
    EXPECT_EQ(DoSetXAttr(nullptr, "key", "value", 5, 0), -EINVAL);
    EXPECT_EQ(DoSetXAttr("/path", "key", nullptr, 5, 0), -EINVAL);
    EXPECT_EQ(DoTruncate(nullptr, 0), -EINVAL);
    EXPECT_EQ(DoFtruncate(nullptr, 0, &fi), -EINVAL);
    EXPECT_EQ(DoFlush(nullptr, &fi), -EINVAL);
    EXPECT_EQ(DoRename(nullptr, "/dst"), -EINVAL);
    EXPECT_EQ(DoRename("/src", nullptr), -EINVAL);
    EXPECT_EQ(DoFsync(nullptr, 0, &fi), -EINVAL);
    EXPECT_EQ(DoStatfs(nullptr, &vfsbuf), -EINVAL);
    EXPECT_EQ(DoStatfs("/path", nullptr), -EINVAL);
    EXPECT_EQ(DoUtimens(nullptr, nullptr), -EINVAL);
    EXPECT_EQ(DoChmod(nullptr, 0644), -EINVAL);
    EXPECT_EQ(DoChown(nullptr, getuid(), getgid()), -EINVAL);
}

TEST(FalconClientFuseWrapperUT, LocalBranchesWithoutService)
{
    struct stat stbuf {};
    char syntheticPath[] = "/\0011middle";

    EXPECT_EQ(DoAccess("/any", 0), 0);
    EXPECT_EQ(DoSetXAttr("/any", "key", "value", 5, 0), 0);
    EXPECT_EQ(DoGetAttr(syntheticPath, &stbuf), 0);
    EXPECT_TRUE(S_ISDIR(stbuf.st_mode));
}

TEST(FalconClientFuseWrapperUT, MainStatsModeRejectsUnknownCommand)
{
    char arg0[] = "falcon_client";
    char arg1[] = "stats-invalid";
    char *argv[] = {arg0, arg1};
    EXPECT_EQ(FalconFuseMainCoverageEntry(2, argv), 1);
}

namespace {

bool ServiceCoverageEnabled()
{
    const char *enabled = std::getenv("FALCON_CLIENT_SERVICE_COVERAGE");
    return enabled != nullptr && std::string(enabled) == "1";
}

std::string GetEnvOrDefault(const char *key, const char *fallback)
{
    const char *value = std::getenv(key);
    return value != nullptr && *value != '\0' ? std::string(value) : std::string(fallback);
}

int GetIntEnvOrDefault(const char *key, int fallback)
{
    const char *value = std::getenv(key);
    if (value == nullptr || *value == '\0') {
        return fallback;
    }
    return std::atoi(value);
}

bool InitFalconClientOrSkip()
{
    if (!ServiceCoverageEnabled()) {
        return false;
    }

    std::string serverIp = GetEnvOrDefault("SERVER_IP", "127.0.0.1");
    int serverPort = GetIntEnvOrDefault("SERVER_PORT", 55510);

    constexpr int kMaxRetry = 6;
    for (int i = 0; i < kMaxRetry; ++i) {
        int ret = FalconInit(serverIp, serverPort);
        if (ret == SUCCESS) {
            return true;
        }
        FalconDestroy();
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    return false;
}

std::string BuildUniquePath(const char *suffix)
{
    return "/falcon_client_coverage_" + std::to_string(getpid()) + "_" +
           std::to_string(time(nullptr)) + "_" + suffix;
}

int ReaddirFiller(void *buf, const char *name, const struct stat *st, off_t off)
{
    auto *entries = static_cast<std::vector<std::string> *>(buf);
    entries->push_back(name);
    EXPECT_GE(off, 0);
    if (st != nullptr) {
        EXPECT_NE(st->st_mode, 0U);
    }
    return 0;
}

int StopAfterFirstReaddirFiller(void *buf, const char *name, const struct stat *, off_t)
{
    auto *entries = static_cast<std::vector<std::string> *>(buf);
    entries->push_back(name);
    return entries->size() >= 1 ? 1 : 0;
}

}  // namespace

TEST(FalconClientFuseWrapperServiceUT, MetadataAndIoWrappers)
{
    if (!InitFalconClientOrSkip()) {
        GTEST_SKIP() << "Falcon client service coverage is disabled or service is unavailable";
    }

    std::string dir = BuildUniquePath("fuse_dir");
    std::string file = dir + "/file";
    std::string renamed = dir + "/renamed";

    ASSERT_EQ(DoMkDir(dir.c_str(), 0755), 0);

    struct fuse_file_info fi {};
    fi.flags = O_RDWR | O_CREAT;
    ASSERT_EQ(DoCreate(file.c_str(), 0644, &fi), 0);
    ASSERT_NE(fi.fh, 0U);

    const char payload[] = "fuse-wrapper-coverage";
    EXPECT_EQ(DoWrite(file.c_str(), payload, sizeof(payload), 0, &fi), static_cast<int>(sizeof(payload)));
    EXPECT_EQ(DoFsync(file.c_str(), 0, &fi), 0);
    EXPECT_EQ(DoFlush(file.c_str(), &fi), 0);
    EXPECT_EQ(DoRelease(file.c_str(), &fi), 0);

    struct stat stbuf {};
    EXPECT_EQ(DoGetAttr(file.c_str(), &stbuf), 0);
    EXPECT_TRUE(S_ISREG(stbuf.st_mode));
    std::string syntheticLookup = dir + "/\0010file";
    std::vector<char> syntheticPath(syntheticLookup.begin(), syntheticLookup.end());
    syntheticPath.push_back('\0');
    memset(&stbuf, 0, sizeof(stbuf));
    EXPECT_EQ(DoGetAttr(syntheticPath.data(), &stbuf), 0);
    EXPECT_TRUE(S_ISREG(stbuf.st_mode));

    struct fuse_file_info atomicFi {};
    atomicFi.flags = O_CREAT;
    EXPECT_EQ(DoOpenAtomic(file.c_str(), &stbuf, 0644, &atomicFi), 0);
    if (atomicFi.fh != 0 && atomicFi.fh != UINT64_MAX) {
        EXPECT_EQ(DoRelease(file.c_str(), &atomicFi), 0);
    }
    struct fuse_file_info exclusiveFi {};
    exclusiveFi.flags = O_CREAT | O_EXCL;
    EXPECT_EQ(DoOpenAtomic(file.c_str(), &stbuf, 0644, &exclusiveFi), -EEXIST);

    struct fuse_file_info readFi {};
    readFi.flags = O_RDONLY;
    EXPECT_EQ(DoOpen(file.c_str(), &readFi), 0);
    char readBuffer[sizeof(payload)] = {};
    EXPECT_EQ(DoRead(file.c_str(), readBuffer, sizeof(readBuffer), 0, &readFi), static_cast<int>(sizeof(readBuffer)));
    EXPECT_EQ(std::memcmp(readBuffer, payload, sizeof(payload)), 0);
    EXPECT_EQ(DoRelease(file.c_str(), &readFi), 0);

    EXPECT_EQ(DoTruncate(file.c_str(), 4), 0);
    struct fuse_file_info truncFi {};
    truncFi.flags = O_RDWR;
    EXPECT_EQ(DoOpen(file.c_str(), &truncFi), 0);
    EXPECT_EQ(DoFtruncate(file.c_str(), 2, &truncFi), 0);
    EXPECT_EQ(DoRelease(file.c_str(), &truncFi), 0);

    EXPECT_EQ(DoUtimens(file.c_str(), nullptr), 0);
    const struct timespec explicitTimes[2] = {{.tv_sec = 1, .tv_nsec = 0}, {.tv_sec = 2, .tv_nsec = 0}};
    EXPECT_EQ(DoUtimens(file.c_str(), explicitTimes), 0);
    EXPECT_EQ(DoChmod(file.c_str(), 0644), 0);
    EXPECT_EQ(DoChown(file.c_str(), getuid(), getgid()), 0);
    EXPECT_EQ(DoRename(file.c_str(), renamed.c_str()), 0);

    FalconFuseInfo dirFi {};
    EXPECT_EQ(DoOpenDir(dir.c_str(), reinterpret_cast<struct fuse_file_info *>(&dirFi)), 0);
    std::vector<std::string> entries;
    EXPECT_EQ(DoReadDir(dir.c_str(), &entries, ReaddirFiller, 0, reinterpret_cast<struct fuse_file_info *>(&dirFi)), 0);
    std::vector<std::string> stoppedEntries;
    EXPECT_EQ(DoReadDir(dir.c_str(),
                        &stoppedEntries,
                        StopAfterFirstReaddirFiller,
                        1,
                        reinterpret_cast<struct fuse_file_info *>(&dirFi)),
              0);
    EXPECT_EQ(DoReleaseDir(dir.c_str(), reinterpret_cast<struct fuse_file_info *>(&dirFi)), 0);

    struct statvfs vfsbuf {};
    EXPECT_EQ(DoStatfs("/", &vfsbuf), 0);

    EXPECT_EQ(DoUnlink(renamed.c_str()), 0);
    EXPECT_EQ(DoRmDir(dir.c_str()), 0);
    DoDestroy(nullptr);
}

TEST(FalconClientMetaServiceUT, CreateReadWriteRenameAndCleanup)
{
    if (!InitFalconClientOrSkip()) {
        GTEST_SKIP() << "Falcon client service coverage is disabled or service is unavailable";
    }

    std::string dir = BuildUniquePath("dir");
    std::string file = dir + "/file";
    std::string renamed = dir + "/renamed";

    EXPECT_EQ(FalconMkdir(dir), SUCCESS);
    EXPECT_NE(FalconMkdir(dir), SUCCESS);

    struct stat stbuf;
    std::memset(&stbuf, 0, sizeof(stbuf));
    uint64_t fd = 0;
    EXPECT_EQ(FalconCreate(file, fd, O_RDWR | O_CREAT, &stbuf), SUCCESS);
    ASSERT_NE(fd, 0U);

    const char payload[] = "falcon-client-service-coverage";
    EXPECT_EQ(FalconWrite(fd, file, payload, sizeof(payload), 0), 0);
    EXPECT_EQ(FalconFsync(file, fd, 0), SUCCESS);
    EXPECT_EQ(FalconClose(file, fd), SUCCESS);

    std::memset(&stbuf, 0, sizeof(stbuf));
    EXPECT_EQ(FalconGetStat(file, &stbuf), SUCCESS);
    EXPECT_TRUE(S_ISREG(stbuf.st_mode));
    EXPECT_GE(stbuf.st_size, static_cast<off_t>(sizeof(payload)));

    fd = 0;
    std::memset(&stbuf, 0, sizeof(stbuf));
    EXPECT_EQ(FalconOpen(file, O_RDONLY, fd, &stbuf), SUCCESS);
    ASSERT_NE(fd, 0U);

    char readBuffer[sizeof(payload)] = {};
    EXPECT_EQ(FalconRead(file, fd, readBuffer, sizeof(readBuffer), 0), static_cast<int>(sizeof(readBuffer)));
    EXPECT_EQ(std::memcmp(readBuffer, payload, sizeof(payload)), 0);
    EXPECT_EQ(FalconClose(file, fd), SUCCESS);

    EXPECT_EQ(FalconRename(file, renamed), SUCCESS);
    EXPECT_EQ(FalconGetStat(file, &stbuf), FILE_NOT_EXISTS);
    EXPECT_EQ(FalconGetStat(renamed, &stbuf), SUCCESS);

    EXPECT_EQ(FalconTruncate(renamed, 4), SUCCESS);
    EXPECT_EQ(FalconGetStat(renamed, &stbuf), SUCCESS);
    EXPECT_GE(stbuf.st_size, 4);

    struct statvfs vfsbuf;
    std::memset(&vfsbuf, 0, sizeof(vfsbuf));
    EXPECT_EQ(FalconStatFS(&vfsbuf), SUCCESS);

    EXPECT_EQ(FalconUnlink(renamed), SUCCESS);
    EXPECT_EQ(FalconRmDir(dir), SUCCESS);
    FalconDestroy();
}

TEST(FalconClientMetaServiceUT, DirectoryAndAttributeOperations)
{
    if (!InitFalconClientOrSkip()) {
        GTEST_SKIP() << "Falcon client service coverage is disabled or service is unavailable";
    }

    std::string dir = BuildUniquePath("listdir");
    std::string file = dir + "/entry";

    EXPECT_EQ(FalconMkdir(dir), SUCCESS);

    struct stat stbuf;
    std::memset(&stbuf, 0, sizeof(stbuf));
    uint64_t fd = 0;
    EXPECT_EQ(FalconCreate(file, fd, O_RDWR | O_CREAT, &stbuf), SUCCESS);
    EXPECT_EQ(FalconClose(file, fd), SUCCESS);

    FalconFuseInfo fi {};
    EXPECT_EQ(FalconOpenDir(dir, &fi), SUCCESS);
    std::vector<std::string> entries;
    EXPECT_EQ(FalconReadDir(dir, &entries, ReaddirFiller, 0, &fi), SUCCESS);
    EXPECT_NE(std::find(entries.begin(), entries.end(), "."), entries.end());
    EXPECT_NE(std::find(entries.begin(), entries.end(), ".."), entries.end());
    EXPECT_EQ(FalconCloseDir(fi.fh), SUCCESS);

    EXPECT_EQ(FalconUtimens(file, 1, 2), SUCCESS);
    EXPECT_EQ(FalconChmod(file, 0644), SUCCESS);
    EXPECT_EQ(FalconChown(file, getuid(), getgid()), SUCCESS);

    EXPECT_EQ(FalconUnlink(file), SUCCESS);
    EXPECT_EQ(FalconRmDir(dir), SUCCESS);
    FalconDestroy();
}

TEST(FalconClientMetaServiceUT, RouterAndMissingPathBranches)
{
    if (!InitFalconClientOrSkip()) {
        GTEST_SKIP() << "Falcon client service coverage is disabled or service is unavailable";
    }

    ASSERT_NE(router, nullptr);
    EXPECT_NE(router->GetCoordinatorConn(), nullptr);
    EXPECT_EQ(router->GetWorkerConnByPath("relative/path"), nullptr);
    EXPECT_EQ(router->GetWorkerConnByPath(""), nullptr);
    EXPECT_NE(router->GetWorkerConnByPath("/"), nullptr);
    EXPECT_NE(router->GetWorkerConnByPath("/trailing/"), nullptr);

    std::unordered_map<std::string, std::shared_ptr<Connection>> workers;
    EXPECT_EQ(router->GetAllWorkerConnection(workers), SUCCESS);
    ASSERT_FALSE(workers.empty());
    int existingWorkerId = workers.begin()->second->server.id;
    EXPECT_NE(router->GetWorkerConnBySvrId(existingWorkerId), nullptr);
    EXPECT_EQ(router->GetWorkerConnBySvrId(999999), nullptr);

    std::string missing = BuildUniquePath("missing");
    struct stat stbuf;
    std::memset(&stbuf, 0, sizeof(stbuf));
    uint64_t fd = 0;
    FalconFuseInfo fi {};

    EXPECT_NE(FalconOpen(missing, O_RDONLY, fd, &stbuf), SUCCESS);
    EXPECT_NE(FalconGetStat(missing, &stbuf), SUCCESS);
    EXPECT_NE(FalconUnlink(missing), SUCCESS);
    EXPECT_NE(FalconRmDir(missing), SUCCESS);
    EXPECT_NE(FalconRename(missing, missing + "_dst"), SUCCESS);
    EXPECT_NE(FalconOpenDir(missing, &fi), SUCCESS);
    EXPECT_NE(FalconUtimens(missing, 1, 2), SUCCESS);
    EXPECT_NE(FalconChmod(missing, 0644), SUCCESS);
    EXPECT_NE(FalconChown(missing, getuid(), getgid()), SUCCESS);
    EXPECT_NE(FalconTruncate(missing, 16), SUCCESS);

    FalconDestroy();
}

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
