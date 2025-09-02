#ifndef FALCONFS_KV_SERVICE_OPERATION_H
#define FALCONFS_KV_SERVICE_OPERATION_H

#include "hcom/kv_dlopen_init.h"
#include "hcom/kv_hcom_service.h"
#include "kv_ipc_message.h"
#include "kv_ipc_server.h"
#include "kv_utils.h"

class KvServiceOperation {
public:
    int32_t Initialize();
    void UnInitialize();
    static std::shared_ptr<KvServiceOperation> Instance()
    {
        static auto instance = std::make_shared<KvServiceOperation>();
        return instance;
    }

public:
    static void CommonCb(void *arg, Service_Context context)
    {
        return;
    }

    static int GetShareMemoryFd(void)
    {
        return mSharedFd;
    }

    int32_t KvShmAndIpcServiceInit();
    int32_t ServiceShmInit();
    static int32_t RegisterHandlers();
    // TODO service新链接 注册消息
    static int32_t HandleNewConnection(Hcom_Channel channel, uint64_t usrCtx, const char *payLoad);
    // TODO server断链了 如何处理kv信息
    static void HandleConnectionBroken(Hcom_Channel channel, uint64_t usrCtx, const char *payLoad);
    static int32_t HandleGetSharedFileInfo(Service_Context ctx, uint64_t usrCtx);
    static int32_t HandleKvPutData2Shm(Service_Context ctx, uint64_t usrCtx);
    static int32_t HandleKvGetDataFromShm(Service_Context ctx, uint64_t usrCtx);
    static int32_t HandleKvDeleteKey(Service_Context ctx, uint64_t usrCtx);

private:
    bool mShmInit = false;
    static int mSharedFd;
    static uint64_t mSharedFileSize;
    static uintptr_t mSharedFileAddress;
    static KvIpcServerPtr mIpcServer;
    static std::mutex mMutex;
    static bool mInited;
};
#endif // FALCONFS_KV_SERVICE_OPERATION_H
