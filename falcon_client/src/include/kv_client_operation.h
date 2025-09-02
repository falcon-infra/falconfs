#ifndef FALCONFS_KV_CLIENT_OPERATION_H
#define FALCONFS_KV_CLIENT_OPERATION_H

#include "kv_ipc_client.h"
#include "kv_ipc_message.h"
#include "hcom/kv_dlopen_init.h"

class KvClientOperation {
public:
    static inline KvClientOperation *Instance()
    {
        if (gInstance == nullptr) {
            std::lock_guard<std::mutex> guard(gLock);
            if (gInstance == nullptr) {
                gInstance = new (std::nothrow) KvClientOperation();
                if (gInstance == nullptr) {
                    FALCON_LOG(LOG_ERROR) << "Failed to new KvClientOperation object, probably out of memory";
                    return nullptr;
                }
            }
        }
        return gInstance;
    }

public:
    ~KvClientOperation() = default;
    int32_t Initialize(std::string &path);
    void UnInitialize();
    // kv操作集
    int32_t ConnectMmapProcess();
    void DisConnectUnmapProcess();
    int32_t KvPutShmData(const std::string &key, const void *vaule, const size_t len);
    int32_t KvGetShmData(const std::string &key, void *vaule);
    int32_t KvDeleteKey(const std::string &key);

private:
    KvClientOperation() = default;
    static KvIpcClientPtr mIpcClient;
    int mSharedFd = -1;
    uintptr_t mSharedFileAddress = 0;
    uintptr_t mSharedFileEndAddress = 0;
    std::uint64_t mShardFileSize = 0;
    std::mutex mMutex;
    bool mInited = false;

private:
    static std::mutex gLock;
    static KvClientOperation *gInstance;
};

#endif // FALCONFS_KV_CLIENT_OPERATION_H
