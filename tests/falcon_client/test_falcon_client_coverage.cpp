#include "error_code.h"
#include "falcon_meta.h"

#include <gtest/gtest.h>

#include <cerrno>
#include <fcntl.h>
#include <string>
#include <sys/statvfs.h>

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

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
