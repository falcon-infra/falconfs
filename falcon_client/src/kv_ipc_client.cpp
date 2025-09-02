#include <string.h>
#include <atomic>
#include <mutex>
#include "kv_ipc_client.h"
#include "hcom/kv_hcom_err.h"
#include "hcom/kv_hcom_service.h"
#include "kv_utils.h"
#include "log/logging.h"

Hcom_Channel KvIpcClient::mChannel = 0;
Hcom_Service KvIpcClient::mService = 0;
std::mutex KvIpcClient::mMutex;
std::mutex KvIpcClient::mConnectionMutex;
std::atomic<bool> KvIpcClient::mChannelConnected = false;
std::function<int32_t()> KvIpcClient::mConnectCallback;
std::function<void()> KvIpcClient::mDisconnectCallback;

int KvIpcClient::RequestReceived(Service_Context ctx, uint64_t usrCtx)
{
    return 0;
}

int KvIpcClient::RequestPosted(Service_Context ctx, uint64_t usrCtx)
{
    return 0;
}

int KvIpcClient::OneSideDone(Service_Context ctx, uint64_t usrCtx)
{
    return 0;
}

void KvIpcClient::ShutDownConnection(void)
{
    std::unique_lock<std::mutex> lockGuard{ mConnectionMutex };
    if (mChannel == 0) {
        return;
    }

    //断链回调
    mDisconnectCallback();
    Service_DisConnect(mService, mChannel);
    mChannel = 0;
    FALCON_LOG(LOG_INFO) << "Shutting down connection to " << Channel_GetId(mChannel);
}

int32_t KvIpcClient::Connect()
{
    int32_t ret = 0;
    if (UNLIKELY(mService == 0)) {
        FALCON_LOG(LOG_ERROR) << "Ipc client not started.";
        return -1;
    }

    Service_ConnectOptions options;
    options.clientGroupId = 0;
    options.serverGroupId = 0;
    options.linkCount = EP_SIZE;
    options.mode = C_CLIENT_WORKER_POLL;
    options.cbType = C_CHANNEL_FUNC_CB;

    std::string urlProto = "uds://" + mSocketFullPath;
    const char *url = urlProto.c_str();
    uint8_t attempt = 3;
    for (uint32_t i = 0; i < attempt; ++i) {
        if ((ret = Service_Connect(mService, url, &mChannel, options)) == 0) {
            mChannelConnected.store(true);
            FALCON_LOG(LOG_INFO) << "Connect to server success.";
            break;
        }
    }
    if (ret != 0) {
        FALCON_LOG(LOG_ERROR) << "Connect to server failed.";
        return -1;
    }

    mTimeout = CHANNEL_DEFAULT_TIMEOUT;
    Channel_SetChannelTimeOut(mChannel, mTimeout, mTimeout);
    FALCON_LOG(LOG_INFO) << "connect to server success, channelId " << Channel_GetId(mChannel) << ", set timeout(s)"
                         << mTimeout;
    return 0;
}

void KvIpcClient::Stop(void)
{
    std::lock_guard<std::mutex> guard(mMutex);
    if (UNLIKELY(mService == 0 || !mStarted)) {
        FALCON_LOG(LOG_INFO) << "Ipc client not started.";
        return;
    }
    Service_Destroy(mService, "ipc_client");
    mService = 0;
    mStarted = false;
    FALCON_LOG(LOG_INFO) << "Ipc client stopped.";
}

void KvIpcClient::RestoreConnection(void)
{
    auto ret = 0;
    Service_ConnectOptions options;
    options.clientGroupId = 0;
    options.serverGroupId = 0;
    options.linkCount = 4;
    options.mode = C_CLIENT_WORKER_POLL;
    options.cbType = C_CHANNEL_FUNC_CB;

    uint32_t attempt = 3;
    std::string urlProto = "uds://" + mSocketFullPath;
    const char *url = urlProto.c_str();
    std::unique_lock<std::mutex> lockGuard{ mConnectionMutex };

    for (uint32_t i = 0; i < attempt; ++i) {
        if ((ret = Service_Connect(mService, url, &mChannel, options)) == 0) {
            mChannelConnected.store(true);
            FALCON_LOG(LOG_INFO) << "Connect to server success.";
            break;
        }
    }
    if (ret != 0) {
        FALCON_LOG(LOG_ERROR) << "Connect to server failed.";
        return;
    }

    // 重连回调
    auto result = mConnectCallback();
    if (result != 0) {
        FALCON_LOG(LOG_ERROR) << "Connect callback failed.";
        if (mChannel != 0) {
            Service_DisConnect(mService, mChannel);
            mChannel = 0;
        }
    }

    return;
}

int KvIpcClient::ChannelBroken(Hcom_Channel channel, uint64_t usrCtx, const char *payLoad)
{
    mChannelConnected.store(false);
    FALCON_LOG(LOG_INFO) << "Channel broken.";
    ShutDownConnection();
    return 0;
}

int32_t KvIpcClient::Start(void)
{
    std::lock_guard<std::mutex> guard(mMutex);
    if (mStarted) {
        FALCON_LOG(LOG_ERROR) << "Ipc client has already started.";
        return -1;
    }
    auto ret = CreateService();
    if (ret != 0) {
        FALCON_LOG(LOG_ERROR) << "Create service failed.";
        return -1;
    }
    mStarted = true;
    return 0;
}

int32_t KvIpcClient::CreateService(void)
{
    FALCON_LOG(LOG_INFO) << "Start to create ipc client";
    if (mService != 0) {
        FALCON_LOG(LOG_ERROR) << "Service already created.";
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

    Service_SetExternalLogger(&Print);
    
    auto ret = Service_Create(mServiceType, "ipc_client", options, &mService);
    if (ret != 0) {
        FALCON_LOG(LOG_ERROR) << "Create service failed.";
        return -1;
    }

    Service_RegisterHandler(mService, C_SERVICE_REQUEST_RECEIVED, &KvIpcClient::RequestReceived, 1);
    Service_RegisterHandler(mService, C_SERVICE_REQUEST_POSTED, &KvIpcClient::RequestPosted, 1);
    Service_RegisterHandler(mService, C_SERVICE_READWRITE_DONE, &KvIpcClient::OneSideDone, 1);
    Service_RegisterChannelBrokerHandler(mService, &KvIpcClient::ChannelBroken, C_CHANNEL_BROKEN_ALL, 0);

    ret = Service_Start(mService);
    if (ret != 0) {
        FALCON_LOG(LOG_ERROR) << "Service start failed";
        return -1;
    }

    FALCON_LOG(LOG_INFO) << "Start to create ipc client, success";
    mStarted = true;
    return 0;
}

int KvIpcClient::ReceiveFD(int32_t &fd)
{
    if (mChannel == 0) {
        FALCON_LOG(LOG_ERROR) << "Channel is not connected.";
        return -1;
    }
    int fds[1]{};
    auto ret = ReceiveFds(mChannel, fds, 1, 10000);
    if (ret != 0) {
        FALCON_LOG(LOG_ERROR) << "Failed to receive shared fd from server, ret " << ret;
        return -1;
    }
    fd = fds[0];
    return 0;
}