/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "connection/falcon_io_client.h"

#include "log/logging.h"

static int BrpcErrorCodeToFuseErrno(int brpcErrorCode)
{
    // append more case here if you encountered EFAULT
    switch (brpcErrorCode) {
    case 0:
        return 0;
    case brpc::ENOSERVICE:
    case brpc::ENOMETHOD:
        return EOPNOTSUPP;
    case brpc::EREQUEST:
        return EINVAL;
    case brpc::ERPCAUTH:
        return EPERM;
    case brpc::ERPCTIMEDOUT:
        return ETIMEDOUT;
    case brpc::EFAILEDSOCKET:
        return EIO;
    default:
        return EFAULT;
    }
}

/* return 0: OK; return negative: remote IO error, return positive: network error */
int FalconIOClient::OpenFile(uint64_t inodeId,
                             int oflags,
                             uint64_t &physicalFd,
                             uint64_t originalSize,
                             const std::string &path,
                             bool nodeFail)
{
    falcon::brpc_io::OpenRequest request;
    request.set_inode_id(inodeId);
    request.set_oflags(oflags);
    request.set_path(path);
    request.set_size(originalSize);
    request.set_node_fail(nodeFail);
    falcon::brpc_io::OpenReply response;
    brpc::Controller cntl;
    cntl.set_timeout_ms(10000);

    stub->OpenFile(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        FALCON_LOG(LOG_ERROR) << "Open file by brpc failed " << cntl.ErrorText() << "error code: " << cntl.ErrorCode();
        return BrpcErrorCodeToFuseErrno(cntl.ErrorCode()); // positive reply
    }

    if (response.error_code() != 0) {
        FALCON_LOG(LOG_ERROR) << "FalconIOClient::OpenFile failed: " << strerror(-response.error_code());
        return response.error_code();
    }

    physicalFd = response.physical_fd();
    FALCON_LOG(LOG_INFO) << "Open file successfully! you have opened falconFd: " << physicalFd;
    return 0;
}

// return 0: OK, return negative: error of both network and IO
int FalconIOClient::CloseFile(uint64_t physicalFd,
                              bool isFlush,
                              bool isSync,
                              const char *buf,
                              size_t size,
                              off_t offset)
{
    falcon::brpc_io::CloseRequest request;
    request.set_physical_fd(physicalFd);
    request.set_flush(isFlush);
    request.set_sync(isSync);
    request.set_offset(offset);

    falcon::brpc_io::ErrorCodeOnlyReply response;
    brpc::Controller cntl;
    cntl.set_timeout_ms(10000);
#ifdef USE_RDMA
    cntl.request_attachment().append((void *)buf, size);
#else
    auto dummyDeleter = [](void *) -> void {};
    cntl.request_attachment().append_user_data((void *)buf, size, dummyDeleter);
#endif

    stub->CloseFile(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        FALCON_LOG(LOG_ERROR) << "Close file by brpc failed " << cntl.ErrorText() << "error code: " << cntl.ErrorCode();
        return -BrpcErrorCodeToFuseErrno(cntl.ErrorCode());
    }

    if (response.error_code() != 0) {
        FALCON_LOG(LOG_ERROR) << "FalconIOClient::CloseFile failed: " << strerror(-response.error_code());
        return response.error_code();
    }

    FALCON_LOG(LOG_INFO) << "Close file successfully!";
    return 0;
}

// return positive: read length, return negative error of both network and IO
int FalconIOClient::ReadFile(uint64_t /*inodeId*/,
                             int /*oflags*/,
                             char *readBuffer,
                             uint64_t &physicalFd,
                             int bufferSize,
                             off_t offset,
                             const std::string &path)
{
    // have to open file before call this method
    falcon::brpc_io::ReadRequest request;
    request.set_physical_fd(physicalFd);
    request.set_offset(offset);
    request.set_read_size(bufferSize);
    request.set_path(path);
    falcon::brpc_io::ErrorCodeOnlyReply response;
    brpc::Controller cntl;
    cntl.set_timeout_ms(10000);

    stub->ReadFile(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        FALCON_LOG(LOG_ERROR) << "Read file by brpc failed " << cntl.ErrorText() << "error code: " << cntl.ErrorCode();
        return -BrpcErrorCodeToFuseErrno(cntl.ErrorCode()); // -errno
    }

    if (response.error_code() != 0) {
        FALCON_LOG(LOG_ERROR) << "FalconIOClient::ReadFile failed: " << strerror(-response.error_code());
        return response.error_code(); // negative reply
    }

    int retLen = cntl.response_attachment().size();
    if (retLen > bufferSize) {
        FALCON_LOG(LOG_ERROR) << "Return more bytes than requested.";
        return -EIO;
    }

    cntl.response_attachment().cutn(readBuffer, retLen);
    FALCON_LOG(LOG_INFO) << "In FalconIOClient::ReadFile(): read file successfully! you have read: " << retLen
                         << " bytes";
    return retLen;
}

// return 0: OK; return negative: remote IO error, return positive: network error
ssize_t FalconIOClient::ReadSmallFile(uint64_t inodeId,
                                      ssize_t size,
                                      std::string &path,
                                      char *readBuffer,
                                      int oflags,
                                      bool nodeFail)
{
    falcon::brpc_io::ReadSmallFileRequest request;
    request.set_inode_id(inodeId);
    request.set_read_size(size);
    request.set_path(path);
    request.set_oflags(oflags);
    request.set_node_fail(nodeFail);
    falcon::brpc_io::ErrorCodeOnlyReply response;
    brpc::Controller cntl;
    cntl.set_timeout_ms(10000);

    stub->ReadSmallFile(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        FALCON_LOG(LOG_ERROR) << "Read small file by brpc failed " << cntl.ErrorText()
                              << "error code: " << cntl.ErrorCode();
        return BrpcErrorCodeToFuseErrno(cntl.ErrorCode()); // positive reply
    }

    if (response.error_code() != 0) {
        FALCON_LOG(LOG_ERROR) << "FalconIOClient::ReadSmallFile failed: " << strerror(-response.error_code());
        return response.error_code();
    }

    int retLen = cntl.response_attachment().size();
    if (retLen != size) {
        FALCON_LOG(LOG_ERROR) << "Return bytes doesn't equal to requested.";
        return -EIO;
    }

    cntl.response_attachment().cutn(readBuffer, retLen);
    FALCON_LOG(LOG_INFO) << "In FalconIOClient::ReadSmallFile(): read file successfully! you have read: " << size
                         << " bytes";
    return 0;
}

// return 0: OK, return negative: error of both network and IO
int FalconIOClient::WriteFile(uint64_t physicalFd, const char *writeBuffer, uint64_t size, off_t offset)
{
    falcon::brpc_io::WriteRequest request;
    request.set_physical_fd(physicalFd);
    request.set_offset(offset);
    falcon::brpc_io::WriteReply response;
    brpc::Controller cntl;
    cntl.set_timeout_ms(10000);
#ifdef USE_RDMA
    cntl.request_attachment().append((void *)writeBuffer, size);
#else
    auto dummyDeleter = [](void *) -> void {};
    cntl.request_attachment().append_user_data((void *)writeBuffer, size, dummyDeleter);
#endif

    stub->WriteFile(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        FALCON_LOG(LOG_ERROR) << "WriteFile by brpc failed " << cntl.ErrorText() << "error code: " << cntl.ErrorCode();
        return -BrpcErrorCodeToFuseErrno(cntl.ErrorCode());
    }

    if (response.error_code() != 0) {
        FALCON_LOG(LOG_ERROR) << "FalconIOClient::WriteFile failed: " << strerror(-response.error_code());
        return response.error_code();
    }

    int writedSize = response.write_size();
    if ((uint64_t)writedSize != size) {
        FALCON_LOG(LOG_ERROR) << "Write size doesn't equal to requested.";
        return -EIO;
    }

    FALCON_LOG(LOG_INFO) << "In FalconIOClient::WriteFile(): write file successfully! you have write: " << writedSize
                         << " bytes";
    return 0;
}

// return 0: OK, return negative: error of both network and IO
int FalconIOClient::DeleteFile(uint64_t inodeId, int nodeId, std::string &path)
{
    falcon::brpc_io::DeleteRequest request;
    request.set_inode_id(inodeId);
    request.set_node_id(nodeId);
    request.set_path(path);
    falcon::brpc_io::ErrorCodeOnlyReply response;
    brpc::Controller cntl;
    cntl.set_timeout_ms(10000);

    stub->DeleteFile(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        FALCON_LOG(LOG_ERROR) << "Delete file by brpc failed " << cntl.ErrorText()
                              << "error code: " << cntl.ErrorCode();
        return -BrpcErrorCodeToFuseErrno(cntl.ErrorCode());
    }

    if (response.error_code() != 0) {
        FALCON_LOG(LOG_ERROR) << "FalconIOClient::Delete failed: " << strerror(-response.error_code());
        return response.error_code();
    }

    FALCON_LOG(LOG_INFO) << "Delete file successfully!";
    return 0;
}

// return 0: OK, return negative: error of both network and IO
int FalconIOClient::StatFS(std::string &path, struct StatFSBuf *fsBuf)
{
    falcon::brpc_io::StatFSRequest request;
    request.set_path(path);
    falcon::brpc_io::StatFSReply response;
    brpc::Controller cntl;
    cntl.set_timeout_ms(10000);

    stub->StatFS(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        FALCON_LOG(LOG_ERROR) << "StatFS by brpc failed " << cntl.ErrorText() << "error code: " << cntl.ErrorCode();
        return -BrpcErrorCodeToFuseErrno(cntl.ErrorCode());
    }

    if (response.error_code() != 0) {
        FALCON_LOG(LOG_ERROR) << "FalconIOClient::StatFS failed: " << strerror(-response.error_code());
        return response.error_code();
    }

    fsBuf->f_blocks = response.fblocks();
    fsBuf->f_bfree = response.fbfree();
    fsBuf->f_bavail = response.fbavail();
    fsBuf->f_files = response.ffiles();
    fsBuf->f_ffree = response.fffree();
    FALCON_LOG(LOG_INFO) << "StatFS successfully!";
    return 0;
}

// return 0: OK, return negative: error of both network and IO
int FalconIOClient::TruncateOpenInstance(uint64_t physicalFd, off_t size)
{
    falcon::brpc_io::TruncateOpenInstanceRequest request;
    request.set_physical_fd(physicalFd);
    request.set_size(size);
    falcon::brpc_io::ErrorCodeOnlyReply response;
    brpc::Controller cntl;
    cntl.set_timeout_ms(10000);

    stub->TruncateOpenInstance(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        FALCON_LOG(LOG_ERROR) << "TruncateOpenInstance by brpc failed " << cntl.ErrorText()
                              << "error code: " << cntl.ErrorCode();
        return -BrpcErrorCodeToFuseErrno(cntl.ErrorCode());
    }

    if (response.error_code() != 0) {
        FALCON_LOG(LOG_ERROR) << "FalconIOClient::TruncateOpenInstance failed: " << strerror(-response.error_code());
        return response.error_code();
    }

    FALCON_LOG(LOG_INFO) << "TruncateOpenInstance successfully!";
    return 0;
}

// return 0: OK, return negative: error of both network and IO
int FalconIOClient::TruncateFile(uint64_t physicalFd, off_t size)
{
    falcon::brpc_io::TruncateFileRequest request;
    request.set_physical_fd(physicalFd);
    request.set_size(size);
    falcon::brpc_io::ErrorCodeOnlyReply response;
    brpc::Controller cntl;
    cntl.set_timeout_ms(10000);

    stub->TruncateFile(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        FALCON_LOG(LOG_ERROR) << "TruncateFile by brpc failed " << cntl.ErrorText()
                              << "error code: " << cntl.ErrorCode();
        return -BrpcErrorCodeToFuseErrno(cntl.ErrorCode());
    }

    if (response.error_code() != 0) {
        FALCON_LOG(LOG_ERROR) << "FalconIOClient::TruncateFile failed: " << strerror(-response.error_code());
        return response.error_code();
    }

    FALCON_LOG(LOG_INFO) << "TruncateFile successfully!";
    return 0;
}

int FalconIOClient::CheckConnection()
{
    falcon::brpc_io::CheckConnectionRequest request;
    falcon::brpc_io::ErrorCodeOnlyReply response;
    brpc::Controller cntl;
    cntl.set_timeout_ms(10000);

    stub->CheckConnection(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        FALCON_LOG(LOG_ERROR) << "CheckConnection by brpc failed " << cntl.ErrorText()
                              << "error code: " << cntl.ErrorCode();
        return -BrpcErrorCodeToFuseErrno(cntl.ErrorCode());
    }

    if (response.error_code() != 0) {
        FALCON_LOG(LOG_ERROR) << "FalconIOClient::CheckConnection failed: " << strerror(-response.error_code());
        return response.error_code();
    }
    return 0;
}

int FalconIOClient::StatCluster(int nodeId, std::vector<size_t> &stats, bool scatter)
{
    falcon::brpc_io::StatClusterRequest request;
    falcon::brpc_io::StatClusterReply response;
    brpc::Controller cntl;
    cntl.set_timeout_ms(10000);

    request.set_scatter(scatter);
    request.set_node_id(nodeId);

    stub->StatCluster(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        FALCON_LOG(LOG_ERROR) << "StatCluster by brpc failed " << cntl.ErrorText()
                              << "error code: " << cntl.ErrorCode();
        return -BrpcErrorCodeToFuseErrno(cntl.ErrorCode());
    }

    if (response.error_code() != 0) {
        FALCON_LOG(LOG_ERROR) << "FalconIOClient::StatCluster failed: " << strerror(-response.error_code());
        return response.error_code();
    }

    stats.resize(STATS_END);
    stats.assign(response.stats().begin(), response.stats().end());

    return 0;
}