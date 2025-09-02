#ifndef FALCONFS_KV_IPC_CLIENT_H
#define FALCONFS_KV_IPC_CLIENT_H

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <mutex>
#include <string>
#include "hcom/kv_dlopen_init.h"
#include "hcom/kv_hcom_err.h"
#include "hcom/kv_hcom_service.h"
#include "kv_ipc_message.h"
#include "kv_utils.h"
#include "log/logging.h"

using namespace ock::hcom;
class KvIpcClient {
public:
    KvIpcClient(std::string &path) : mSocketFullPath (std::move(path)) {}
    ~KvIpcClient() = default;

    int32_t Start();
    void Stop();

    int32_t Connect();
    static void ShutDownConnection();
    void RestoreConnection();

    static void RegisterConnectCallBack(const std::function<int32_t()> &connectCb, const std::function<void()> &disconnectCb)
    {
        mConnectCallback = connectCb;
        mDisconnectCallback = disconnectCb;
    }
    template <typename TReq, typename TResp> int32_t SyncCall(KvOpCode opCode, TReq &req, TResp &resp)
    {
        if (mChannel == 0) {
            FALCON_LOG(LOG_ERROR) << "Channel is not initialized, try to restore connection";
            RestoreConnection();
        }
        Channel_Request request = {.address = static_cast<void *>(&req), .size = sizeof(req), .opcode = opCode};
        Channel_Response response =  {.address = static_cast<void *>(&resp), .size = sizeof(resp), .errorCode = 0};
        static constexpr int32_t maxRetryTimes = 3;
        for (auto i = 0; i < maxRetryTimes; i++) {
            if (mChannel == 0) {
                RestoreConnection();
                continue;
            }
            auto result = Channel_Call(mChannel, request, &response, nullptr);
            if (UNLIKELY(result == SerCode::SER_NOT_ESTABLISHED)) {
                RestoreConnection();
                continue;
            }
            if (UNLIKELY(result != 0 || response.errorCode != 0)) {
                FALCON_LOG(LOG_ERROR) << "Failed to call server with op " << opCode << ", result " <<
                    std::to_string(result) << ", error code " << response.errorCode;
                return result;
            }
            break;
        }
        return 0;
    }
    int ReceiveFD(int32_t &fd);
public:
    int32_t CreateService();
    static int ChannelBroken(Hcom_Channel channel, uint64_t usrCtx, const char *payLoad);
    static int RequestReceived(Service_Context ctx, uint64_t usrCtx);
    static int RequestPosted(Service_Context ctx, uint64_t usrCtx);
    static int OneSideDone(Service_Context ctx, uint64_t usrCtx);


private:
    Service_Type mServiceType = C_SERVICE_SHM;
    static Hcom_Service mService;
    static Hcom_Channel mChannel;
    int16_t mTimeout = -1;

    static std::mutex mMutex;
    bool mStarted = false;
    std::string mSocketFullPath;
    static std::mutex mConnectionMutex;
    static std::atomic<bool> mChannelConnected;
    static std::function<int32_t()> mConnectCallback;
    static std::function<void()> mDisconnectCallback;
};
using KvIpcClientPtr = std::shared_ptr<KvIpcClient>;

#endif // FALCONFS_KV_IPC_CLIENT_H
