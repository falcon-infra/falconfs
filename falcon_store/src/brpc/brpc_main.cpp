/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "brpc/brpc_server.h"

#include <fcntl.h>
#include <unistd.h>

#include <brpc/server.h>
#include <bthread/unstable.h>
#include <butil/iobuf.h>
#include <memory_resource>
#include <print>

#include "buffer/dir_open_instance.h"
#include "buffer/open_instance.h"
#include "connection/node.h"
#include "falcon_store/falcon_store.h"
#include "log/logging.h"
#include "util/utils.h"

namespace falcon::brpc_io
{
constexpr size_t ALIGNMENT = 512;

void RemoteIOServiceImpl::OpenFile(google::protobuf::RpcController * /*cntl_base*/,
                                   const OpenRequest *request,
                                   OpenReply *response,
                                   google::protobuf::Closure *done)
{
    brpc::ClosureGuard doneGuard(done);

    uint64_t inodeId = request->inode_id();
    uint64_t size = request->size();
    int oflags = request->oflags();
    const std::string &path = request->path();
    bool nodeFail = request->node_fail();
    FALCON_LOG(LOG_INFO) << "Receive OpenFile rpc request, path = " << path << ", file size = " << size;

    std::shared_ptr<OpenInstance> openInstance = FalconFd::GetInstance()->WaitGetNewOpenInstance(false);
    if (openInstance == nullptr) {
        response->set_error_code(-ENOMEM);
        response->set_physical_fd(0);
        FALCON_LOG(LOG_ERROR) << "OpenFile rpc request return with failure";
        return;
    }

    openInstance->inodeId = inodeId;
    openInstance->path = path;
    openInstance->oflags = oflags;
    openInstance->physicalFd = UINT64_MAX;
    openInstance->originalSize = size;
    openInstance->currentSize = size;
    openInstance->directReadFile.store(true);
    openInstance->nodeId = StoreNode::GetInstance()->GetNodeId();
    openInstance->isRemoteCall = true;
    openInstance->nodeFail = nodeFail;

    int ret = FalconStore::GetInstance()->OpenFile(openInstance.get());
    if (ret != 0) {
        response->set_error_code(ret);
        response->set_physical_fd(0);
        FALCON_LOG(LOG_ERROR) << "OpenFile rpc request return with failure";
    } else {
        uint64_t fd = FalconFd::GetInstance()->AttachFd(path, openInstance);
        response->set_error_code(0);
        response->set_physical_fd(fd);
        openInstance->isOpened = true;
        FALCON_LOG(LOG_INFO) << "OpenFile rpc request return with falconFd = " << fd;
    }
}

void RemoteIOServiceImpl::CloseFile(google::protobuf::RpcController *cntl_base,
                                    const CloseRequest *request,
                                    ErrorCodeOnlyReply *response,
                                    google::protobuf::Closure *done)
{
    brpc::ClosureGuard doneGuard(done);
    auto *cntl = static_cast<brpc::Controller *>(cntl_base);

    uint64_t fd = request->physical_fd();
    bool flush = request->flush();
    bool sync = request->sync();
    off_t offset = request->offset();
    butil::IOBuf &buffer = cntl->request_attachment();

    FALCON_LOG(LOG_INFO) << "Receive CloseFile rpc request, fd = " << fd;

    std::shared_ptr<OpenInstance> openInstance = FalconFd::GetInstance()->GetOpenInstanceByFd(fd);
    int ret = 0;
    if (!openInstance) {
        FALCON_LOG(LOG_ERROR) << "CloseFile(): fd " << fd << " not found";
        response->set_error_code(-EBADF);
        return;
    }

    std::unique_lock<std::shared_mutex> closeLock(openInstance->closeMutex);
    if (openInstance->isClosed) {
        response->set_error_code(-ETIMEDOUT);
        return;
    }

    if (!buffer.empty()) {
        openInstance->writeCnt++;
        ret = FalconStore::GetInstance()->WriteLocalFileForBrpc(openInstance.get(), buffer, offset);
        if (ret != 0) {
            FALCON_LOG(LOG_ERROR) << "WriteLocalFileForBrpc failed, ret = " << ret;
        }
    }

    int closeRet = FalconStore::GetInstance()->CloseTmpFiles(openInstance.get(), flush, sync);
    ret = (ret == 0) ? closeRet : ret;

    if (ret != 0) {
        FALCON_LOG(LOG_ERROR) << "CloseFile rpc request failed, ret = " << ret;
    }
    if (!flush) {
        openInstance->isClosed = true;
        FalconFd::GetInstance()->DeleteOpenInstance(fd, false);
    }

    response->set_error_code(ret);
}

void RemoteIOServiceImpl::ReadFile(google::protobuf::RpcController *cntl_base,
                                   const ReadRequest *request,
                                   ErrorCodeOnlyReply *response,
                                   google::protobuf::Closure *done)
{
    brpc::ClosureGuard doneGuard(done);
    auto *cntl = static_cast<brpc::Controller *>(cntl_base);

    uint64_t fd = request->physical_fd();
    int readSize = request->read_size();
    uint64_t offset = request->offset();
    FALCON_LOG(LOG_INFO) << "Receive ReadFile rpc request, fd = " << fd << ", offset = " << offset
                         << ", size = " << readSize;

    if (readSize < 0) {
        response->set_error_code(-EAGAIN);
        return;
    }

    std::shared_ptr<OpenInstance> openInstance = FalconFd::GetInstance()->GetOpenInstanceByFd(fd);
    if (openInstance == nullptr) {
        FALCON_LOG(LOG_ERROR) << "ReadFile(): impossibly, fd " << fd << " not found";
        response->set_error_code(-EBADF);
        return;
    }

    std::shared_lock<std::shared_mutex> closeLock(openInstance->closeMutex);
    if (openInstance->isClosed) {
        response->set_error_code(-ETIMEDOUT);
        return;
    }

    char *buffer;
    size_t allocSize = readSize;
    if (openInstance->oflags & __O_DIRECT) {
        allocSize = (readSize / ALIGNMENT + (readSize % ALIGNMENT != 0)) * ALIGNMENT;
        buffer = static_cast<char *>(aligned_alloc(ALIGNMENT, allocSize));
    } else {
        buffer = static_cast<char *>(malloc(allocSize));
    }
    if (buffer == nullptr) {
        FALCON_LOG(LOG_ERROR) << "Allocation failed for size " << allocSize;
        response->set_error_code(-ENOMEM);
        return;
    }

    int retSize = FalconStore::GetInstance()->ReadFileLR(buffer, offset, openInstance.get(), allocSize);
    if (retSize < 0) {
        free(buffer);
        FALCON_LOG(LOG_ERROR) << "ReadFile rpc failed, fd = " << fd << ", error = " << retSize;
        response->set_error_code(retSize);
        return;
    }

    response->set_error_code(0);
#ifdef USE_RDMA
    cntl->response_attachment().append(buffer, retSize);
    free(buffer);
#else
    cntl->response_attachment().append_user_data(buffer, retSize, [](void *buf) { free(buf); });
#endif
}

void RemoteIOServiceImpl::ReadSmallFile(google::protobuf::RpcController *cntl_base,
                                        const ReadSmallFileRequest *request,
                                        ErrorCodeOnlyReply *response,
                                        google::protobuf::Closure *done)
{
    brpc::ClosureGuard doneGuard(done);
    auto *cntl = static_cast<brpc::Controller *>(cntl_base);

    uint64_t inodeId = request->inode_id();
    int readSize = request->read_size();
    const std::string &path = request->path();
    int32_t oflags = request->oflags();
    bool nodeFail = request->node_fail();
    FALCON_LOG(LOG_INFO) << "Receive ReadSmallFile rpc request, inode = " << inodeId << ", size = " << readSize;

    if (readSize < 0 || readSize > (int)READ_BIGFILE_SIZE) {
        response->set_error_code(-EAGAIN);
        return;
    }

    auto allocBuffer = [](size_t size, bool needAlign, auto &fallbackAlloc) -> std::pair<char *, size_t> {
        thread_local char smallBuffer[8 * 1024]; // 8KB线程本地缓存
        thread_local std::pmr::monotonic_buffer_resource localResource(smallBuffer, sizeof(smallBuffer));

        size_t actualSize = needAlign ? ((size + ALIGNMENT - 1) / ALIGNMENT) * ALIGNMENT : size;
        if (needAlign) {
            return {static_cast<char *>(aligned_alloc(ALIGNMENT, actualSize)), actualSize};
        }

        if (actualSize <= sizeof(smallBuffer)) {
            return {static_cast<char *>(localResource.allocate(actualSize)), actualSize};
        }
        return {static_cast<char *>(fallbackAlloc.allocate(actualSize)), actualSize};
    };
    auto &pool = RemoteIOServer::GetMemoryPool();
    bool needAlign = (oflags & __O_DIRECT);
    auto [buffer, allocatedSize] = allocBuffer(readSize, needAlign, pool);

    if (buffer == nullptr) {
        FALCON_LOG(LOG_ERROR) << "Allocation failed for size " << readSize;
        response->set_error_code(-ENOMEM);
        return;
    }

    int ret = FalconStore::GetInstance()->ReadSmallFilesForBrpc(inodeId, path, buffer, readSize, oflags, nodeFail);
    if (ret < 0) {
        FALCON_LOG(LOG_ERROR) << "ReadSmallFile rpc failed, inodeId = " << inodeId << ", error = " << ret;
        response->set_error_code(ret);
        return;
    }

    response->set_error_code(0);
    std::function<void(void *)> deleter =
        needAlign ? static_cast<std::function<void(void *)>>([](void *buf) { free(buf); })
                  : static_cast<std::function<void(void *)>>(
                        [allocatedSize, &pool, buffer](void *) { pool.deallocate(buffer, allocatedSize); });
#ifdef USE_RDMA
    cntl->response_attachment().append(buffer, readSize);
    deleter(buffer);
#else
    cntl->response_attachment().append_user_data(buffer, readSize, deleter);
#endif
}

void RemoteIOServiceImpl::WriteFile(google::protobuf::RpcController *cntl_base,
                                    const WriteRequest *request,
                                    WriteReply *response,
                                    google::protobuf::Closure *done)
{
    brpc::ClosureGuard doneGuard(done);
    auto *cntl = static_cast<brpc::Controller *>(cntl_base);

    uint64_t fd = request->physical_fd();
    uint64_t offset = request->offset();
    butil::IOBuf &buffer = cntl->request_attachment();
    int writeSize = buffer.size();
    FALCON_LOG(LOG_INFO) << "Receive WriteFile rpc request, fd = " << fd;

    std::shared_ptr<OpenInstance> openInstance = FalconFd::GetInstance()->GetOpenInstanceByFd(fd);
    if (openInstance == nullptr) {
        FALCON_LOG(LOG_ERROR) << "WriteFile(): impossibly, fd " << fd << " not found";
        response->set_error_code(-EBADF);
        return;
    }

    std::shared_lock<std::shared_mutex> closeLock(openInstance->closeMutex);
    if (openInstance->isClosed) {
        response->set_error_code(-ETIMEDOUT);
        return;
    }

    openInstance->writeCnt++;
    int ret = FalconStore::GetInstance()->WriteLocalFileForBrpc(openInstance.get(), buffer, offset);
    if (ret < 0) {
        response->set_error_code(ret);
        return;
    }

    response->set_error_code(0);
    response->set_write_size(writeSize);
}

void RemoteIOServiceImpl::DeleteFile(google::protobuf::RpcController * /*cntl_base*/,
                                     const DeleteRequest *request,
                                     ErrorCodeOnlyReply *response,
                                     google::protobuf::Closure *done)
{
    brpc::ClosureGuard doneGuard(done);

    uint64_t inodeId = request->inode_id();
    int nodeId = request->node_id();
    const std::string &path = request->path();
    FALCON_LOG(LOG_INFO) << "Receive DeleteFile rpc request, inode = " << inodeId << " nodeid = " << nodeId;

    int ret = FalconStore::GetInstance()->DeleteFiles(inodeId, -1, path);
    response->set_error_code(ret);
}

void RemoteIOServiceImpl::StatFS(google::protobuf::RpcController * /*cntl_base*/,
                                 const StatFSRequest *request,
                                 StatFSReply *response,
                                 google::protobuf::Closure *done)
{
    brpc::ClosureGuard doneGuard(done);

    const std::string &path = request->path();

    uint64_t fblocks = 0;
    uint64_t fbfree = 0;
    uint64_t fbavail = 0;
    uint64_t ffiles = 0;
    uint64_t fffree = 0;
    int ret = FalconStore::GetInstance()->StatFSForBrpc(path, fblocks, fbfree, fbavail, ffiles, fffree);
    if (ret < 0) {
        response->set_error_code(ret);
        return;
    }

    response->set_error_code(0);
    response->set_fblocks(fblocks);
    response->set_fbfree(fbfree);
    response->set_fbavail(fbavail);
    response->set_ffiles(ffiles);
    response->set_fffree(fffree);
}

void RemoteIOServiceImpl::TruncateOpenInstance(google::protobuf::RpcController * /*cntl_base*/,
                                               const TruncateOpenInstanceRequest *request,
                                               ErrorCodeOnlyReply *response,
                                               google::protobuf::Closure *done)
{
    brpc::ClosureGuard doneGuard(done);

    uint64_t fd = request->physical_fd();
    off_t size = request->size();
    FALCON_LOG(LOG_INFO) << "Receive TruncateOpenInstance rpc request, fd = " << fd << " size = " << size;

    std::shared_ptr<OpenInstance> openInstance = FalconFd::GetInstance()->GetOpenInstanceByFd(fd);
    if (openInstance == nullptr) {
        FALCON_LOG(LOG_ERROR) << "TruncateOpenInstance(): impossibly, fd " << fd << " not found";
        response->set_error_code(-EBADF);
        return;
    }

    std::shared_lock<std::shared_mutex> closeLock(openInstance->closeMutex);
    if (openInstance->isClosed) {
        response->set_error_code(-ETIMEDOUT);
        return;
    }

    int ret = FalconStore::GetInstance()->TruncateOpenInstance(openInstance.get(), size);
    response->set_error_code(ret);
}

void RemoteIOServiceImpl::TruncateFile(google::protobuf::RpcController * /*cntl_base*/,
                                       const TruncateFileRequest *request,
                                       ErrorCodeOnlyReply *response,
                                       google::protobuf::Closure *done)
{
    brpc::ClosureGuard doneGuard(done);

    uint64_t fd = request->physical_fd();
    off_t size = request->size();
    FALCON_LOG(LOG_INFO) << "Receive TruncateFile rpc request, fd = " << fd << " size = " << size;

    std::shared_ptr<OpenInstance> openInstance = FalconFd::GetInstance()->GetOpenInstanceByFd(fd);
    if (openInstance == nullptr) {
        FALCON_LOG(LOG_ERROR) << "TruncateFile(): impossibly, fd " << fd << " not found";
        response->set_error_code(-EBADF);
        return;
    }

    std::shared_lock<std::shared_mutex> closeLock(openInstance->closeMutex);
    if (openInstance->isClosed) {
        response->set_error_code(-ETIMEDOUT);
        return;
    }

    openInstance->writeCnt++;
    int ret = FalconStore::GetInstance()->TruncateFile(openInstance.get(), size);
    response->set_error_code(ret);
}

void RemoteIOServiceImpl::CheckConnection(google::protobuf::RpcController * /*cntl_base*/,
                                          const CheckConnectionRequest * /*request*/,
                                          ErrorCodeOnlyReply *response,
                                          google::protobuf::Closure *done)
{
    brpc::ClosureGuard doneGuard(done);
    response->set_error_code(0);
}

void RemoteIOServiceImpl::StatCluster(google::protobuf::RpcController *cntl_base,
                                      const StatClusterRequest *request,
                                      StatClusterReply *response,
                                      google::protobuf::Closure *done)
{
    brpc::ClosureGuard doneGuard(done);
    
    bool scatter = request->scatter();
    int nodeId = request->node_id();

    std::vector<size_t> currentStats;
    int ret = FalconStore::GetInstance()->StatCluster(nodeId, currentStats, scatter);
    for (auto &e : currentStats) {
        response->add_stats(e);
    }
    response->set_error_code(ret);
}

int RemoteIOServer::Run()
{
    falcon::brpc_io::RemoteIOServiceImpl remoteIOServiceImpl;
    if (server.AddService(&remoteIOServiceImpl, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        FALCON_LOG(LOG_ERROR) << "Fail to add service";
        return -1;
    }

    butil::EndPoint point;
    butil::str2endpoint(endPoint.c_str(), &point);
    brpc::ServerOptions options;
#ifdef USE_RDMA
    std::println("BRPC is configured to use RDMA");
    options.use_rdma = true;
#endif
    if (server.Start(point, &options) != 0) {
        FALCON_LOG(LOG_ERROR) << "Fail to start EchoServer";
        return -1;
    }

    {
        std::lock_guard<std::mutex> lk(mutexStart);
        isStarted = true;
    }
    cvStart.notify_all();

    {
        std::unique_lock<std::mutex> lk(mutexStart);
        cvReady.wait(lk, [&]() { return (isStarted && isReady) || (!isStarted && !isReady); });
        if (!isReady) {
            FALCON_LOG(LOG_INFO) << "init failed";
            return 0;
        }
    }
    // warm up
    GetMemoryPool();

    FALCON_LOG(LOG_INFO) << "running successfully";
    server.RunUntilAskedToQuit();
    return 0;
}

void RemoteIOServer::Stop()
{
    std::lock_guard<std::mutex> lk(mutexStart);
    server.Stop(0);
    server.Join();
    isStarted = false;
    isReady = false;
    cvReady.notify_all();
}

void RemoteIOServer::SetReadyFlag()
{
    std::lock_guard<std::mutex> lk(mutexStart);
    isReady = true;
    cvReady.notify_all();
}

} // namespace falcon::brpc_io
