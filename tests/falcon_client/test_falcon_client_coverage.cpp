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

}  // namespace

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

    auto savedShardTable = router->TestShardTable();
    auto savedRouteMap = router->TestRouteMap();
    auto savedCoordinator = router->TestCoordinatorConn();

    router->TestShardTable().clear();
    EXPECT_THROW(router->GetWorkerConnByPath("/corrupt"), std::runtime_error);

    router->TestShardTable().clear();
    router->TestRouteMap().clear();
    ServerIdentifier missingServer("127.0.0.1", 56049, 9999);
    router->TestShardTable().emplace(INT32_MAX, missingServer);
    EXPECT_THROW(router->GetWorkerConnByPath("/missing-server"), std::runtime_error);

    router->TestShardTable() = savedShardTable;
    router->TestRouteMap() = savedRouteMap;
    ASSERT_FALSE(router->TestRouteMap().empty());
    auto existingConn = router->TestRouteMap().begin()->second;
    auto staleWorkerConn =
        std::make_shared<Connection>(ServerIdentifier(existingConn->server.ip, existingConn->server.port + 1,
                                                      existingConn->server.id));
    EXPECT_EQ(router->TryToUpdateWorkerConn(staleWorkerConn), existingConn);
    auto unknownWorkerConn = std::make_shared<Connection>(ServerIdentifier("127.0.0.1", 56049, 99999));
    EXPECT_THROW(router->TryToUpdateWorkerConn(unknownWorkerConn), std::runtime_error);

    auto staleCoordinator =
        std::make_shared<Connection>(ServerIdentifier(savedCoordinator->server.ip, savedCoordinator->server.port + 1,
                                                      savedCoordinator->server.id));
    EXPECT_EQ(router->TryToUpdateCNConn(staleCoordinator), savedCoordinator);

    router->TestShardTable() = savedShardTable;
    router->TestRouteMap() = savedRouteMap;
    router->TestCoordinatorConn() = savedCoordinator;

    FalconDestroy();
}

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
