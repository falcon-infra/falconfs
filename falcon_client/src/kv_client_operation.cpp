#include "kv_client_operation.h"
#include <fcntl.h>
#include <securec.h>
#include <sys/mman.h>
#include <cstring>
#include "kv_ipc_client.h"
#include "kv_ipc_message.h"
#include "kv_utils.h"
#include "log/logging.h"

KvClientOperation *KvClientOperation::gInstance = nullptr;
std::mutex KvClientOperation::gLock;
KvIpcClientPtr KvClientOperation::mIpcClient = nullptr;

int32_t KvClientOperation::Initialize(std::string &path)
{
    std::lock_guard<std::mutex> guard(mMutex);
    if (mInited) {
        FALCON_LOG(LOG_INFO) << "KvClientOperation has been initialized";
        return 0;
    }
    mIpcClient = std::make_shared<KvIpcClient>(path);
    if (mIpcClient == nullptr) {
        FALCON_LOG(LOG_ERROR) << "KvIpcClient new failed";
        return -1;
    }

    mIpcClient->RegisterConnectCallBack(
        [this]() {
            FALCON_LOG(LOG_INFO) << "connect to service restore.";
            std::unique_lock<std::mutex> lockGuard(mMutex);
            return ConnectMmapProcess();
        },
        [this]() {
            FALCON_LOG(LOG_INFO) << "connect to service shutdown.";
            std::unique_lock<std::mutex> lockGuard(mMutex);
            DisConnectUnmapProcess();
        });

    auto ret = mIpcClient->Start();
    if (ret != 0) {
        FALCON_LOG(LOG_ERROR) << "KvIpcClient start failed";
        return -1;
    }

    ret = mIpcClient->Connect();
    if (ret != 0) {
        mIpcClient->Stop();
        mIpcClient = nullptr;
        FALCON_LOG(LOG_ERROR) << "KvIpcClient connect failed";
        return -1;
    }

    // 获取server mmap信息
    ret = ConnectMmapProcess();
    if (ret != 0) {
        mIpcClient->Stop();
        mIpcClient = nullptr;
        FALCON_LOG(LOG_ERROR) << "KvIpcClient connect mmap failed";
        return -1;
    }
    mInited = true;
    return 0;
}

int32_t KvClientOperation::KvDeleteKey(const std::string &key)
{
    if (mIpcClient == nullptr) {
        FALCON_LOG(LOG_ERROR) << "KvIpcClient is nullptr";
        return -1;
    }

    KvOperationReq req;
    req.flags = 0;
    req.Key(key);
    KvOperationResp resp;
    auto ret = mIpcClient->SyncCall<KvOperationReq, KvOperationResp>(IPC_OP_KV_DELETE, req, resp);
    if (ret != 0) {
        FALCON_LOG(LOG_ERROR) << "KvIpcClient sync call get shm data info failed";
        return -1;
    }
    if (resp.result != 0) {
        FALCON_LOG(LOG_ERROR) << "KvIpcClient sync call delete key failed, result " << resp.result;
        return resp.result;
    }

    return 0;
}

int32_t KvClientOperation::KvGetShmData(const std::string &key, void *vaule)
{
    if (mIpcClient == nullptr) {
        FALCON_LOG(LOG_ERROR) << "KvIpcClient is nullptr";
        return -1;
    }
    std::vector<uintptr_t> blkMappedAddress;
    KvAllocateMoreBlockReq req;
    req.flags = 0;
    req.Key(key);
    KvAllocateMoreBlockResp *resp = nullptr;
    uint32_t respLen;
    auto ret = mIpcClient->SyncCall<KvAllocateMoreBlockReq, KvAllocateMoreBlockResp>(IPC_OP_KV_GET_FROM_SHM,
                                                                                     req,
                                                                                     &resp,
                                                                                     respLen);
    if (ret != 0) {
        FALCON_LOG(LOG_ERROR) << "KvIpcClient sync call get shm data info failed";
        return -1;
    }
    if (resp == nullptr) {
        FALCON_LOG(LOG_ERROR) << "KvIpcClient sync call server response nullptr";
        return -1;
    }

    ret = KvCheckAllocateBlockResp(resp, respLen);
    if (ret != 0) {
        FALCON_LOG(LOG_ERROR) << "KvIpcClient sync call check resp info failed";
        free(resp);
        return -1;
    }

    FALCON_LOG(LOG_INFO) << " Kv get resp info: " << resp->ToString();

    blkMappedAddress.reserve(resp->blockCount);
    uint32_t blockSize = resp->blockSize;
    uint32_t blockCount = resp->blockCount;
    uint32_t vauleLen = resp->valueLen;
    for (auto i = 0UL; i < blockCount; i++) {
        blkMappedAddress.push_back(reinterpret_cast<uintptr_t>(mSharedFileAddress + resp->dataBlock[i]));
    }
    free(resp);

    KvReadDataFromShm(blockSize, blockCount, vauleLen, vaule, blkMappedAddress);

    // 读取block完成
    KvGetDataFinishedReq request;
    request.flags = 0;
    request.Key(key);
    KvGetDataFinishedResp response;
    ret =
        mIpcClient->SyncCall<KvGetDataFinishedReq, KvGetDataFinishedResp>(IPC_OP_KV_GET_SHM_FINISH, request, response);
    if (ret != 0) {
        FALCON_LOG(LOG_ERROR) << "KvIpcClient sync call get data finish failed";
        return -1;
    }
    if (response.result != 0) {
        FALCON_LOG(LOG_ERROR) << "KvIpcClient sync call get data failed, result " << resp->result;
        return -1;
    }
    return 0;
}

int32_t KvClientOperation::KvCheckAllocateBlockResp(KvAllocateMoreBlockResp *resp, uint32_t len)
{
    if (resp->result != 0) {
        FALCON_LOG(LOG_ERROR) << "KvIpcClient response failed, result " << resp->result;
        return -1;
    }

    if (resp->blockSize == 0UL || resp->blockCount == 0UL) {
        FALCON_LOG(LOG_ERROR) << "KvIpcClient check response param failed, blockSize " << resp->blockSize
                              << " blockCount" << resp->blockCount;
        return -1;
    }

    auto dataShouldSize = sizeof(KvAllocateMoreBlockResp) + resp->blockCount * sizeof(uint64_t);
    if (dataShouldSize > len) {
        FALCON_LOG(LOG_ERROR) << "KvIpcClient check response param failed, dataShouldSize " << dataShouldSize << " len"
                              << len;
        return -1;
    }

    auto sharedFileSize = mSharedFileEndAddress - mSharedFileAddress;

    for (auto i = 0U; i < resp->blockCount; i++) {
        if (resp->dataBlock[i] >= sharedFileSize || resp->dataBlock[i] + resp->blockSize > sharedFileSize) {
            FALCON_LOG(LOG_ERROR) << "KvIpcClient check response param failed";
            return -1;
        }
    }
    return 0;
}

int32_t KvClientOperation::KvAllocateMoreBlock4Put(const std::string &key,
                                                   const size_t len,
                                                   uint32_t &blkSize,
                                                   uint32_t &blkCount,
                                                   std::vector<uintptr_t> &blkMappedAddress)
{
    if (mIpcClient == nullptr) {
        FALCON_LOG(LOG_ERROR) << "KvIpcClient is nullptr";
        return -1;
    }

    KvAllocateMoreBlockReq req;
    req.flags = 0;
    req.Key(key);
    req.valueLen = static_cast<uint32_t>(len);
    KvAllocateMoreBlockResp *resp = nullptr;
    uint32_t respLen;
    auto ret = mIpcClient->SyncCall<KvAllocateMoreBlockReq, KvAllocateMoreBlockResp>(IPC_OP_KV_ALLOCATE_MORE_BLOCK,
                                                                                     req,
                                                                                     &resp,
                                                                                     respLen);
    if (ret != 0) {
        FALCON_LOG(LOG_ERROR) << "KvIpcClient sync call send put data finish info failed";
        return -1;
    }

    if (resp == nullptr) {
        FALCON_LOG(LOG_ERROR) << "KvIpcClient sync call server response nullptr";
        return -1;
    }

    ret = KvCheckAllocateBlockResp(resp, respLen);
    if (ret != 0) {
        FALCON_LOG(LOG_ERROR) << "KvIpcClient sync call check resp info failed";
        free(resp);
        return -1;
    }

    FALCON_LOG(LOG_INFO) << "put allocate blocks info: " << resp->ToString();

    blkSize = resp->blockSize;
    blkCount = resp->blockCount;

    blkMappedAddress.reserve(blkCount);
    for (auto i = 0UL; i < resp->blockCount; i++) {
        blkMappedAddress.push_back(reinterpret_cast<uintptr_t>(mSharedFileAddress + resp->dataBlock[i]));
    }
    free(resp);
    return 0;
}

void KvClientOperation::KvWriteData2Shm(const uint32_t blkSize,
                                        const uint32_t blkCount,
                                        const size_t len,
                                        void *vaule,
                                        std::vector<uintptr_t> &blkMappedAddress)
{
    auto copySize = 0;
    auto leftBytes = 0;
    for (auto i = 0UL; i < blkCount; i++) {
        if (i == blkCount - 1) {
            leftBytes = len - copySize;
        } else {
            leftBytes = blkSize;
        }
        std::memcpy(reinterpret_cast<void *>(blkMappedAddress[i]), static_cast<uint8_t *>(vaule), leftBytes);
        vaule = static_cast<uint8_t *>(vaule) + leftBytes;
        copySize += leftBytes;
    }
}

void KvClientOperation::KvReadDataFromShm(const uint32_t blkSize,
                                          const uint32_t blkCount,
                                          const size_t len,
                                          void *vaule,
                                          std::vector<uintptr_t> &blkMappedAddress)
{
    auto copySize = 0;
    auto leftBytes = 0;
    for (auto i = 0UL; i < blkCount; i++) {
        if (i == blkCount - 1) {
            leftBytes = len - copySize;
        } else {
            leftBytes = blkSize;
        }
        std::memcpy(vaule, reinterpret_cast<uint8_t *>(blkMappedAddress[i]), leftBytes);
        vaule = static_cast<uint8_t *>(vaule) + leftBytes;
        copySize += leftBytes;
    }
}

int32_t KvClientOperation::KvPutShmData(const std::string &key, void *vaule, const size_t len)
{
    if (mIpcClient == nullptr) {
        FALCON_LOG(LOG_ERROR) << "KvIpcClient is nullptr";
        return -1;
    }

    std::vector<uintptr_t> blkMappedAddress;
    uint32_t blockSize;
    uint32_t blockCount;
    auto ret = KvAllocateMoreBlock4Put(key, len, blockSize, blockCount, blkMappedAddress);
    if (ret != 0) {
        FALCON_LOG(LOG_ERROR) << "AllocateMoreBlock4Put failed ret" << ret;
        return -1;
    }

    KvWriteData2Shm(blockSize, blockCount, len, vaule, blkMappedAddress);

    KvOperationReq req;
    req.flags = 0;
    req.Key(key);
    req.valueLen = static_cast<uint32_t>(len);
    KvOperationResp resp;
    ret = mIpcClient->SyncCall<KvOperationReq, KvOperationResp>(IPC_OP_KV_PUT_SHM_FINISH, req, resp);
    if (ret != 0) {
        FALCON_LOG(LOG_ERROR) << "KvIpcClient sync call send put data finish info failed";
        return -1;
    }
    if (resp.result != 0) {
        FALCON_LOG(LOG_ERROR) << "KvIpcClient sync call send put data finish info failed, result " << resp.result;
        return -1;
    }
    return 0;
}

int32_t KvClientOperation::ConnectMmapProcess(void)
{
    if (mIpcClient == nullptr) {
        FALCON_LOG(LOG_ERROR) << "KvIpcClient is nullptr";
        return -1;
    }
    KvSharedFileInfoReq req;
    req.flags = 0;
    KvSharedFileInfoResp resp;
    auto ret =
        mIpcClient->SyncCall<KvSharedFileInfoReq, KvSharedFileInfoResp>(IPC_OP_KV_GET_SHARED_FILE_INFO, req, resp);
    if (ret != 0) {
        FALCON_LOG(LOG_ERROR) << "KvIpcClient sync call get shared file info failed";
        return -1;
    }

    if (resp.result != 0) {
        FALCON_LOG(LOG_ERROR) << "KvIpcClient sync call get shared file info failed, result " << resp.result;
        return -1;
    }

    if (resp.shardFileSize > MAX_SHARED_FILE_SIZE) {
        FALCON_LOG(LOG_ERROR) << "KvIpcClient sync call shared file size " << resp.shardFileSize
                              << " exceeded max size " << MAX_SHARED_FILE_SIZE;
        return -1;
    }

    mShardFileSize = resp.shardFileSize;
    FALCON_LOG(LOG_INFO) << "shared file info " << resp.ToString();

    mIpcClient->ReceiveFD(mSharedFd);

    /* mmap */
    auto mappedAddress = mmap(nullptr, mShardFileSize, PROT_READ | PROT_WRITE, MAP_SHARED, mSharedFd, 0);
    if (mappedAddress == MAP_FAILED) {
        close(mSharedFd);
        mSharedFd = -1;
        FALCON_LOG(LOG_ERROR) << "KvIpcClient mmap failed";
        return -1;
    }

    mSharedFileAddress = reinterpret_cast<uintptr_t>(mappedAddress);
    mSharedFileEndAddress = mSharedFileAddress + mShardFileSize;

    return 0;
}

void KvClientOperation::DisConnectUnmapProcess(void)
{
    if (mSharedFd >= 0) {
        munmap((void *)mSharedFileAddress, mSharedFileEndAddress - mSharedFileAddress);
        close(mSharedFd);
        mSharedFd = -1;
    }
}

void KvClientOperation::UnInitialize(void)
{
    std::lock_guard<std::mutex> guard(mMutex);
    if (!mInited) {
        FALCON_LOG(LOG_INFO) << "KvClientOperation has not been initialized";
        return;
    }
    mIpcClient->Stop();
    mIpcClient = nullptr;
    mInited = false;
    DisConnectUnmapProcess();
}