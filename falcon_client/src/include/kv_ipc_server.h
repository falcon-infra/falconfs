#ifndef FALCONFS_KV_IPC_SERVER_H
#define FALCONFS_KV_IPC_SERVER_H

#include <cstdio>
#include <cstdlib>
#include <mutex>
#include <string>
#include "hcom/kv_dlopen_init.h"
#include "hcom/kv_hcom_err.h"
#include "hcom/kv_hcom_service.h"
#include "kv_ipc_message.h"
#include "kv_utils.h"
#include "log/logging.h"

class KvIpcServer {
public:
    KvIpcServer(std::string &path) : socketPath(std::move(path)) {}
    int32_t Start();
    void Stop();
    ~KvIpcServer() = default;

public:
    int32_t CreateService();
    static int NewChannel(Hcom_Channel channel, uint64_t usrCtx, const char *payLoad);
    static int ChannelBroken(Hcom_Channel channel, uint64_t usrCtx, const char *payLoad);
    static int RequestReceived(Service_Context ctx, uint64_t usrCtx);
    static int RequestPosted(Service_Context ctx, uint64_t usrCtx);
    static int OneSideDone(Service_Context ctx, uint64_t usrCtx);

    static int32_t RegisterNewRequestHandler(uint32_t opCode, const NewRequestHandler &h)
    {
        std::lock_guard<std::mutex> guard(mMutex);
        if (opCode >= MAX_NEW_REQ_HANDLER) {
            FALCON_LOG(LOG_ERROR) << "Invalid opCode " << opCode << " which should be less than " <<
                MAX_NEW_REQ_HANDLER;
            return -1;
        }
        if (mHandlers[opCode] != nullptr) {
            FALCON_LOG(LOG_ERROR) << "Handler for opCode " << opCode << " already registered";
            return -1;
        }
        mHandlers[opCode] = h;
        return 0;
    }

    static int32_t RegisterNewChannelHandler(const NewChannelHandler &h)
    {
        std::lock_guard<std::mutex> guard(mMutex);
        if (mHandleNewChannel != nullptr) {
            FALCON_LOG(LOG_ERROR) << "Handler for new channel already registered";
            return -1;
        }
        mHandleNewChannel = h;
        return 0;
    }

    static int32_t RegisterChannelBrokenHandler(const ChannelBrokenHandler &h)
    {
        std::lock_guard<std::mutex> guard(mMutex);
        if (mHandleBrokenChannel != nullptr) {
            FALCON_LOG(LOG_ERROR) << "Handler for broken channel already registered";
            return -1;
        }

        mHandleBrokenChannel = h;
        return 0;
    }

    inline bool IsStarted() const
    {
        return mStarted;
    }

private:
    static constexpr uint32_t MAX_NEW_REQ_HANDLER = 5;
    Service_Type mServiceType = C_SERVICE_SHM;
    Hcom_Service mService = 0;
    Hcom_Channel mChannel = 0;
    static NewRequestHandler mHandlers[MAX_NEW_REQ_HANDLER];
    static NewChannelHandler mHandleNewChannel;
    static ChannelBrokenHandler mHandleBrokenChannel;

    std::string socketPath;
    static std::mutex mMutex;
    bool mStarted = false;
};

using KvIpcServerPtr = std::shared_ptr<KvIpcServer>;

#endif // FALCONFS_KV_IPC_SERVER_H
