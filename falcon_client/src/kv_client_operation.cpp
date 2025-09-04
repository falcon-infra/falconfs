#include <sys/mman.h>
#include <securec.h>
#include <fcntl.h>
#include <cstring>
#include "kv_ipc_client.h"
#include "kv_ipc_message.h"
#include "kv_utils.h"
#include "log/logging.h"
#include "kv_client_operation.h"

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

    // 获取server测 mmap信息
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

int32_t KvClientOperation::KvGetShmData(const std::string& key, void* vaule)
{
    if (mIpcClient == nullptr) {
        FALCON_LOG(LOG_ERROR) << "KvIpcClient is nullptr";
        return -1;
    }
    KvOperationReq req;
    req.flags = 0;
    req.Key(key);
    KvOperationResp resp;
    auto ret = mIpcClient->SyncCall<KvOperationReq, KvOperationResp>(IPC_OP_KV_GET_FROM_SHM, req, resp);
    if (ret != 0) {
        FALCON_LOG(LOG_ERROR) << "KvIpcClient sync call get shm data info failed";
        return -1;
    }
    if (resp.result != 0) {
        FALCON_LOG(LOG_ERROR) << "KvIpcClient sync call get shm data info failed, result " << resp.result;
        return -1;
    }

    FALCON_LOG(LOG_INFO) << " get resp info: " << resp.ToString();

    auto len = resp.valueLen;
    
    // 从SHM copy到value
    auto err = memcpy_s(vaule, len, reinterpret_cast<void *>(mSharedFileAddress), len);
    if (err != 0) {
        FALCON_LOG(LOG_ERROR) << "memcpy_s failed, err " << err;
        return -1;
    }

    return 0;
}

int32_t KvClientOperation::KvPutShmData(const std::string &key, const void* vaule, const size_t len)
{
    if (mIpcClient == nullptr) {
        FALCON_LOG(LOG_ERROR) << "KvIpcClient is nullptr";
        return -1;
    }

    FALCON_LOG(LOG_INFO) << "client mSharedFileAddress: " << mSharedFileAddress;
    // copy数据到SHM
    auto err = memcpy_s(reinterpret_cast<void *>(mSharedFileAddress), len, vaule, len);
    if (err != 0) {
        FALCON_LOG(LOG_ERROR) << "memcpy_s failed, err " << err;
        return -1;
    }

    KvOperationReq req;
    req.flags = 0;
    req.Key(key);
    req.valueLen = static_cast<uint32_t>(len);
    KvOperationResp resp;
    auto ret = mIpcClient->SyncCall<KvOperationReq, KvOperationResp>(IPC_OP_KV_PUT_SHM_FINISH, req, resp);
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
    auto ret = mIpcClient->SyncCall<KvSharedFileInfoReq, KvSharedFileInfoResp>(IPC_OP_KV_GET_SHARED_FILE_INFO, req, resp);
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