#include "kv_ipc_server.h"
#include <iostream>
#include <string.h>
#include "hcom/kv_hcom_service.h"
#include "kv_utils.h"
#include "log/logging.h"
#include "kv_service_operation.h"

NewRequestHandler KvIpcServer::mHandlers[MAX_NEW_REQ_HANDLER] = {};
NewChannelHandler KvIpcServer::mHandleNewChannel = nullptr;
ChannelBrokenHandler KvIpcServer::mHandleBrokenChannel = nullptr;
std::mutex KvIpcServer::mMutex;

int32_t KvIpcServer::CreateService()
{
    FALCON_LOG(LOG_INFO) << "Start to create ipc service";
    if (mService != 0) {
        FALCON_LOG(LOG_ERROR) << "Service has been already created";
        return -1;
    }

    Service_Options options;
    options.maxSendRecvDataSize = MAX_MESSAGE_SIZE + RECEIVE_SEG_SIZE;
    options.workerGroupId = 0;
    options.workerGroupThreadCount = 1;
    options.workerGroupMode = C_SERVICE_EVENT_POLLING;
    options.workerThreadPriority = 0;
    //    strcpy_s(options.workerGroupCpuRange, sizeof(options.workerGroupCpuRange), "1-1");
    strcpy(options.workerGroupCpuRange, "1-1");
    auto ret = Service_Create(mServiceType, "ipc_service", options, &mService);
    if (ret != 0) {
        FALCON_LOG(LOG_ERROR) << "Create service failed.";
        return -1;
    }

    // 注册hcom 日志打印
    Service_SetExternalLogger(&Print);
    // Service_SetDeviceIpMask(service, ipSeg);
    Service_RegisterHandler(mService, C_SERVICE_REQUEST_RECEIVED, &KvIpcServer::RequestReceived, 1);
    Service_RegisterHandler(mService, C_SERVICE_REQUEST_POSTED, KvIpcServer::RequestPosted, 1);
    Service_RegisterHandler(mService, C_SERVICE_READWRITE_DONE, KvIpcServer::OneSideDone, 1);
    Service_RegisterChannelBrokerHandler(mService, KvIpcServer::ChannelBroken, C_CHANNEL_BROKEN_ALL, 0);
    std::string urlProto = "uds://" + socketPath + ":0600";
    const char *url = urlProto.c_str();
    Service_Bind(mService, url, KvIpcServer::NewChannel);
    auto result = Service_Start(mService);
    if (result != 0) {
        FALCON_LOG(LOG_ERROR) << "failed to start service " << result;
        return -1;
    }

    FALCON_LOG(LOG_INFO) << "Start to create ipc service, success.";
    return 0;
}

void KvIpcServer::Stop()
{
    std::unique_lock<std::mutex> guard(mMutex);
    if (!mStarted || mService == 0) {
        FALCON_LOG(LOG_INFO) << "IpcClient was not started";
        return;
    }
    Service_Destroy(mService, "ipc_service");
    mService = 0;
    mStarted = false;
    FALCON_LOG(LOG_INFO) << "Ipc service stopped.";
}

int KvIpcServer::RequestPosted(Service_Context ctx, uint64_t usrCtx)
{
    return 0;
}

int KvIpcServer::OneSideDone(Service_Context ctx, uint64_t usrCtx)
{
    return 0;
}

int KvIpcServer::ChannelBroken(Hcom_Channel channel, uint64_t usrCtx, const char *payLoad)
{
    FALCON_LOG(LOG_INFO) << "channel " << Channel_GetId(channel) << " broken "
                         << " payload " << std::string(payLoad);

    if (mHandleBrokenChannel != nullptr) {
        mHandleBrokenChannel(channel, usrCtx, payLoad);
    }
    return 0;
}

int KvIpcServer::NewChannel(Hcom_Channel channel, uint64_t usrCtx, const char *payLoad)
{
    FALCON_LOG(LOG_INFO) << "a new channel id: " << Channel_GetId(channel) << " payload " << std::string(payLoad);

    int fds[1]{};
    fds[0] = KvServiceOperation::GetShareMemoryFd();
    int ret = SendFds(channel, fds, 1);
    if (UNLIKELY(ret != 0)) {
        FALCON_LOG(LOG_ERROR) << "Failed to send fd to client";
        return ret;
    }
    if (mHandleNewChannel != nullptr) {
        mHandleNewChannel(channel, usrCtx, payLoad);
    }
    return 0;
}

int KvIpcServer::RequestReceived(Service_Context ctx, uint64_t usrCtx)
{
    Hcom_Channel tmpChannel;
    if (Service_GetChannel(ctx, &tmpChannel) != 0) {
        FALCON_LOG(LOG_ERROR) << "Get channel failed";
        return -1;
    }
    uint16_t opCode = Service_GetOpCode(ctx);
    FALCON_LOG(LOG_INFO) << "opcode: " << opCode;

    if (LIKELY(opCode >= MAX_NEW_REQ_HANDLER)) {
        FALCON_LOG(LOG_ERROR) << "Invalid opcode " << opCode;
        return -1;
    }

    auto &handler = mHandlers[opCode];
    if (UNLIKELY(handler == nullptr)) {
        FALCON_LOG(LOG_ERROR) << "Invalid opcode " << opCode << ", no handle registered";
        return -1;
    }

    // TODO 根据opcode判断 IPC_OP_KV_PUT_SHM_FINISH IPC_OP_KV_GET_FROM_SHM IPC_OP_KV_DELETE 触发bio协程处理
    // mRequestExecutor->AddTask(handler, ctx, usrCtx);
    return handler(ctx, usrCtx);
}

int32_t KvIpcServer::Start()
{
    std::unique_lock<std::mutex> guard(mMutex);
    if (mStarted) {
        FALCON_LOG(LOG_INFO) << "IpcServer has been already started";
        return 0;
    }
    auto ret = CreateService();
    if (ret != 0) {
        FALCON_LOG(LOG_ERROR) << "Create service failed";
        return -1;
    }
    mStarted = true;
    return 0;
}
