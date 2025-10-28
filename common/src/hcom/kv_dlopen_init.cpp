#include <dlfcn.h>
#include <cstdio>
#include <iostream>
#include "hcom/kv_hcom_service.h"
#include "log/logging.h"
#include "hcom/kv_dlopen_init.h"
#include "securec.h"

using Call = int (*)(Hcom_Channel channel, Channel_Request req, Channel_Response *rsp, Channel_Callback *cb);
using GetId = uint64_t (*)(Hcom_Channel channel);
using Bind = int (*)(Hcom_Service service, const char *listenerUrl, Service_ChannelHandler h);
using GetChannel = int (*)(Service_Context context, Hcom_Channel *channel);
using GetResult = int (*)(Service_Context context, int *result);
using GetRspCtx = int (*)(Service_Context context, Channel_ReplyContext *rspCtx);
using Create = int (*)(Service_Type t, const char *name, Service_Options options, Hcom_Service *service);
using Start = int (*)(Hcom_Service service);
using Destroy = int (*)(Hcom_Service service, const char *name);
using Connect = int (*)(Hcom_Service service, const char *serverUrl, Hcom_Channel *channel,
                        Service_ConnectOptions options);
using DisConnect = int (*)(Hcom_Service service, Hcom_Channel channel);
using RegisterHandler = void (*)(Hcom_Service service, Service_HandlerType t, Service_RequestHandler h,
                                 uint64_t usrCtx);
using SetChannelTimeout = void (*)(Hcom_Channel channel, int16_t oneSideTimeout, int16_t twoSideTimeout);
using RegisterChannelBrokerHandler = void (*)(Hcom_Service service, Service_ChannelHandler h,
                                              Service_ChannelPolicy policy, uint64_t usrCtx);
using SetExternalLogger = void (*)(Service_LogHandler h);
using GetOpCode = uint16_t (*)(Service_Context context);
using GetMessageData = void *(*)(Service_Context context);
using GetMessageDataLen = uint32_t (*)(Service_Context context);
using GetContextTpye = int (*)(Service_Context context, Service_ContextType *type);
using Reply = int (*)(Hcom_Channel channel, Channel_Request req, Channel_ReplyContext ctx, Channel_Callback *cb);

using SendFds = int (*)(Hcom_Channel channel, int fds[], uint32_t len);
using ReceiveFds = int (*)(Hcom_Channel channel, int fds[], uint32_t len, int32_t timeoutSec);
using SetTlsOptions = void (*)(Hcom_Service service,
                           bool enableTls,
                           Service_TlsVersion version,
                           Service_CipherSuite cipherSuite,
                           Hcom_TlsGetCertCb certCb,
                           Hcom_TlsGetPrivateKeyCb priKeyCb,
                           Hcom_TlsGetCACb caCb);
static constexpr uint16_t DL_SYMBOL_FUNC_ARR_LEN = 4;

using HcomFunc = struct {
    Call call;
    Reply reply;
    GetId getId;
    Bind bind;
    Create create;
    Start start;
    Destroy destroy;
    Connect connect;
    SendFds sendFds;
    ReceiveFds receiveFds;
    DisConnect disConnect;
    GetChannel getChannel;
    GetResult getResult;
    GetRspCtx getRspCtx;
    SetExternalLogger setExternalLogger;
    SetChannelTimeout setChannelTimeout;
    RegisterHandler registerHandler;
    RegisterChannelBrokerHandler registerChannelBrokerHandler;
    GetOpCode getOpCode;
    GetMessageData getMessageData;
    GetMessageDataLen getMessageDataLen;
    GetContextTpye getContextTpye;
    SetTlsOptions setTlsOptions;
};

void *g_hcomDl = nullptr;
static HcomFunc g_hcomFunc;

int32_t KvLoadSymbol(void *libHandle, std::string symbol, void **symLibHandle)
{
    const char *dlsymErr = nullptr;

    *symLibHandle = dlsym(libHandle, const_cast<char *>(symbol.c_str()));
    dlsymErr = dlerror();
    if (dlsymErr != nullptr) {
        FALCON_LOG(LOG_ERROR) << "Can not find symbol " << symbol << " in .so, Dlerror: " << dlsymErr;
        return -1;
    }

    return 0;
}

static int KvDlsymServiceLayerServiceInitFunc(void)
{
    int ret =
        KvLoadSymbol(g_hcomDl, "Service_SetExternalLogger", reinterpret_cast<void **>(&g_hcomFunc.setExternalLogger));
    if (ret != 0) {
        return -1;
    }

    ret = KvLoadSymbol(g_hcomDl, "Service_Create", reinterpret_cast<void **>(&g_hcomFunc.create));
    if (ret != 0) {
        return -1;
    }

    ret = KvLoadSymbol(g_hcomDl, "Service_Start", reinterpret_cast<void **>(&g_hcomFunc.start));
    if (ret != 0) {
        return -1;
    }

    ret = KvLoadSymbol(g_hcomDl, "Service_Destroy", reinterpret_cast<void **>(&g_hcomFunc.destroy));
    if (ret != 0) {
        return -1;
    }

    ret = KvLoadSymbol(g_hcomDl, "Service_DisConnect", reinterpret_cast<void **>(&g_hcomFunc.disConnect));
    if (ret != 0) {
        return -1;
    }

    ret = KvLoadSymbol(g_hcomDl, "Service_Connect", reinterpret_cast<void **>(&g_hcomFunc.connect));
    if (ret != 0) {
        return -1;
    }

    ret = KvLoadSymbol(g_hcomDl, "Service_Bind", reinterpret_cast<void **>(&g_hcomFunc.bind));
    if (ret != 0) {
        return -1;
    }
    return 0;
}

static int KvDlsymServiceLayerServiceCreateRegisterFunc(void)
{
    auto ret =
        KvLoadSymbol(g_hcomDl, "Service_RegisterHandler", reinterpret_cast<void **>(&g_hcomFunc.registerHandler));
    if (ret != 0) {
        return -1;
    }

    ret = KvLoadSymbol(g_hcomDl, "Service_RegisterChannelBrokerHandler",
                       reinterpret_cast<void **>(&g_hcomFunc.registerChannelBrokerHandler));
    if (ret != 0) {
        return -1;
    }
    ret = KvLoadSymbol(g_hcomDl,
                       "Service_SetTlsOptions",
                       reinterpret_cast<void **>(&g_hcomFunc.setTlsOptions));
    if (ret != 0) {
        return -1;
    }
    return 0;
}

static int KvDlsymServiceLayerServiceHandlerFunc(void)
{
    int ret = KvLoadSymbol(g_hcomDl, "Service_GetChannel", reinterpret_cast<void **>(&g_hcomFunc.getChannel));
    if (ret != 0) {
        return -1;
    }

    ret = KvLoadSymbol(g_hcomDl, "Service_GetResult", reinterpret_cast<void **>(&g_hcomFunc.getResult));
    if (ret != 0) {
        return -1;
    }

    ret = KvLoadSymbol(g_hcomDl, "Service_GetRspCtx", reinterpret_cast<void **>(&g_hcomFunc.getRspCtx));
    if (ret != 0) {
        return -1;
    }

    ret = KvLoadSymbol(g_hcomDl, "Service_GetOpCode", reinterpret_cast<void **>(&g_hcomFunc.getOpCode));
    if (ret != 0) {
        return -1;
    }

    ret = KvLoadSymbol(g_hcomDl, "Service_GetMessageData", reinterpret_cast<void **>(&g_hcomFunc.getMessageData));
    if (ret != 0) {
        return -1;
    }

    ret = KvLoadSymbol(g_hcomDl, "Service_GetMessageDataLen", reinterpret_cast<void **>(&g_hcomFunc.getMessageDataLen));
    if (ret != 0) {
        return -1;
    }

    ret = KvLoadSymbol(g_hcomDl, "Service_GetContextType", reinterpret_cast<void **>(&g_hcomFunc.getContextTpye));
    if (ret != 0) {
        return -1;
    }
    return 0;
}

static int KvDlsymServiceLayerChannelHandleFunc(void)
{
    int ret = KvLoadSymbol(g_hcomDl, "Channel_Call", reinterpret_cast<void **>(&g_hcomFunc.call));
    if (ret != 0) {
        return -1;
    }

    ret = KvLoadSymbol(g_hcomDl, "Channel_Reply", reinterpret_cast<void **>(&g_hcomFunc.reply));
    if (ret != 0) {
        return -1;
    }

    ret = KvLoadSymbol(g_hcomDl, "Channel_GetId", reinterpret_cast<void **>(&g_hcomFunc.getId));
    if (ret != 0) {
        return -1;
    }

    ret = KvLoadSymbol(g_hcomDl, "Channel_SetChannelTimeOut", reinterpret_cast<void **>(&g_hcomFunc.setChannelTimeout));
    if (ret != 0) {
        return -1;
    }

    ret = KvLoadSymbol(g_hcomDl, "Channel_SendFds", reinterpret_cast<void **>(&g_hcomFunc.sendFds));
    if (ret != 0) {
        return -1;
    }

    ret = KvLoadSymbol(g_hcomDl, "Channel_ReceiveFds", reinterpret_cast<void **>(&g_hcomFunc.receiveFds));
    if (ret != 0) {
        return -1;
    }

    return 0;
}

int32_t KvOpenHcomDl(void **libHandle, std::string &path)
{
    *libHandle = dlopen(const_cast<char *>(path.c_str()), RTLD_LAZY);
    if (*libHandle == nullptr) {
        FALCON_LOG(LOG_ERROR) << "Can not open library, path: " << path << " Dlerror: " << dlerror();
        return -1;
    }
    return 0;
}

void KvCloseDl(void *libHandle)
{
    (void)dlclose(libHandle);
}

using DlSymbolFunction = int (*)(void);
using DlSymFuncs = struct {
    std::string_view name;
    DlSymbolFunction func;
};

static DlSymFuncs DlSymFuncsArr[DL_SYMBOL_FUNC_ARR_LEN] = {
    { "KvDlsymServiceLayerChannelHandleFunc", KvDlsymServiceLayerChannelHandleFunc },
    { "KvDlsymServiceLayerServiceCreateRegisterFunc", KvDlsymServiceLayerServiceCreateRegisterFunc },
    { "KvDlsymServiceLayerServiceHandlerFunc", KvDlsymServiceLayerServiceHandlerFunc },
    { "KvDlsymServiceLayerServiceInitFunc", KvDlsymServiceLayerServiceInitFunc },
};

int KvDlsym(void)
{
    for (uint16_t i = 0; i < DL_SYMBOL_FUNC_ARR_LEN; i++) {
        int ret = DlSymFuncsArr[i].func();
        if (ret != 0) {
            FALCON_LOG(LOG_ERROR) << "HCOM dlsymbol function:" << DlSymFuncsArr[i].name << " failed.";
            return -1;
        }
    }
    return 0;
}

int InitKvHcomIpcDl(std::string &dlPath, uint32_t pathLen)
{
    if (dlPath.empty() || pathLen == 0) {
        FALCON_LOG(LOG_ERROR) << "Dlopen hcom path is nullptr";
        return -1;
    }

    if (g_hcomDl != nullptr) {
        FALCON_LOG(LOG_INFO) << "Dlopen hcom already load";
        return 0;
    }

    auto ret = KvOpenHcomDl(&g_hcomDl, dlPath);
    if (ret != 0) {
        FALCON_LOG(LOG_ERROR) << "Dlopen hcom so failed";
        return -1;
    }

    ret = KvDlsym();
    if (ret != 0) {
        FinishKvHcomIpcDl();
        FALCON_LOG(LOG_ERROR) << "Dlsym hcom so failed";
        return -1;
    }
    return 0;
}

void FinishKvHcomIpcDl(void)
{
    if (g_hcomDl != nullptr) {
        KvCloseDl(g_hcomDl);
        g_hcomDl = nullptr;
    }

    if (memset_sp(&g_hcomFunc, sizeof(g_hcomFunc), 0, sizeof(g_hcomFunc)) != EOK) {
        FALCON_LOG(LOG_ERROR) << "Failed to memset g_hcomFunc";
    }
}

// 接口打桩

int Channel_Call(Hcom_Channel channel, Channel_Request req, Channel_Response *rsp, Channel_Callback *cb)
{
    int ret = 0;
    if (g_hcomFunc.call != nullptr) {
        ret = g_hcomFunc.call(channel, req, rsp, cb);
    } else {
        ret = -1;
    }
    return ret;
}

int Service_Create(Service_Type t, const char *name, Service_Options options, Hcom_Service *service)
{
    int ret = 0;
    if (g_hcomFunc.create != nullptr) {
        ;
        ret = g_hcomFunc.create(t, name, options, service);
    } else {
        ret = -1;
    }
    return ret;
}

void Service_RegisterHandler(Hcom_Service service, Service_HandlerType t, Service_RequestHandler h, uint64_t usrCtx)
{
    if (g_hcomFunc.registerHandler != nullptr) {
        g_hcomFunc.registerHandler(service, t, h, usrCtx);
    }
}

void Service_SetExternalLogger(Service_LogHandler h)
{
    if (g_hcomFunc.setExternalLogger != nullptr) {
        g_hcomFunc.setExternalLogger(h);
    }
}

void Service_RegisterChannelBrokerHandler(Hcom_Service service, Service_ChannelHandler h, Service_ChannelPolicy policy,
                                          uint64_t usrCtx)
{
    if (g_hcomFunc.registerChannelBrokerHandler != nullptr) {
        g_hcomFunc.registerChannelBrokerHandler(service, h, policy, usrCtx);
    }
}

uint64_t Channel_GetId(Hcom_Channel channel)
{
    int ret = 0;
    if (g_hcomFunc.getId != nullptr) {
        ret = g_hcomFunc.getId(channel);
    } else {
        ret = -1;
    }
    return ret;
}

int Service_Start(Hcom_Service service)
{
    int ret = 0;
    if (g_hcomFunc.start != nullptr) {
        ret = g_hcomFunc.start(service);
    } else {
        ret = -1;
    }
    return ret;
}

int Service_Connect(Hcom_Service service, const char *serverUrl, Hcom_Channel *channel, Service_ConnectOptions options)
{
    int ret = 0;
    if (g_hcomFunc.connect != nullptr) {
        ret = g_hcomFunc.connect(service, serverUrl, channel, options);
    } else {
        ret = -1;
    }
    return ret;
}

void Channel_SetChannelTimeOut(Hcom_Channel channel, int16_t oneSideTimeout, int16_t twoSideTimeout)
{
    if (g_hcomFunc.setChannelTimeout != nullptr) {
        g_hcomFunc.setChannelTimeout(channel, oneSideTimeout, twoSideTimeout);
    }
}

int Service_Destroy(Hcom_Service service, const char *name)
{
    int ret = 0;
    if (g_hcomFunc.destroy != nullptr) {
        ret = g_hcomFunc.destroy(service, name);
    } else {
        ret = -1;
    }
    return ret;
}

int Service_GetContextType(Service_Context context, Service_ContextType *type)
{
    int ret = 0;
    if (g_hcomFunc.getContextTpye != nullptr) {
        ret = g_hcomFunc.getContextTpye(context, type);
    } else {
        ret = -1;
    }
    return ret;
}

uint16_t Service_GetOpCode(Service_Context context)
{
    int ret = 0;
    if (g_hcomFunc.getOpCode != nullptr) {
        ret = g_hcomFunc.getOpCode(context);
    } else {
        ret = -1;
    }
    return ret;
}

void *Service_GetMessageData(Service_Context context)
{
    void *ret = nullptr;
    if (g_hcomFunc.getMessageData != nullptr) {
        ret = g_hcomFunc.getMessageData(context);
    }
    return ret;
}

uint32_t Service_GetMessageDataLen(Service_Context context)
{
    int ret = 0;
    if (g_hcomFunc.getMessageDataLen != nullptr) {
        ret = g_hcomFunc.getMessageDataLen(context);
    } else {
        ret = -1;
    }
    return ret;
}

int Service_GetChannel(Service_Context context, Hcom_Channel *channel)
{
    int ret = 0;
    if (g_hcomFunc.getChannel != nullptr) {
        ret = g_hcomFunc.getChannel(context, channel);
    } else {
        ret = -1;
    }
    return ret;
}

int Service_Bind(Hcom_Service service, const char *listenerUrl, Service_ChannelHandler h)
{
    int ret = 0;
    if (g_hcomFunc.bind != nullptr) {
        ret = g_hcomFunc.bind(service, listenerUrl, h);
    } else {
        ret = -1;
    }
    return ret;
}

int Service_DisConnect(Hcom_Service service, Hcom_Channel channel)
{
    int ret = 0;
    if (g_hcomFunc.disConnect != nullptr) {
        ret = g_hcomFunc.disConnect(service, channel);
    } else {
        ret = -1;
    }
    return ret;
}

int Service_GetRspCtx(Service_Context context, Channel_ReplyContext *rspCtx)
{
    int ret = 0;
    if (g_hcomFunc.getRspCtx != nullptr) {
        ret = g_hcomFunc.getRspCtx(context, rspCtx);
    } else {
        ret = -1;
    }
    return ret;
}

int Channel_Reply(Hcom_Channel channel, Channel_Request req, Channel_ReplyContext ctx, Channel_Callback *cb)
{
    int ret = 0;
    if (g_hcomFunc.reply != nullptr) {
        ret = g_hcomFunc.reply(channel, req, ctx, cb);
    } else {
        ret = -1;
    }
    return ret;
}

int Channel_SendFds(Hcom_Channel channel, int fds[], uint32_t len)
{
    int ret = 0;
    if (g_hcomFunc.sendFds != nullptr) {
        ret = g_hcomFunc.sendFds(channel, fds, len);
    } else {
        ret = -1;
    }
    return ret;
}

int Channel_ReceiveFds(Hcom_Channel channel, int fds[], uint32_t len, int32_t timeoutSec)
{
    int ret = 0;
    if (g_hcomFunc.receiveFds != nullptr) {
        ret = g_hcomFunc.receiveFds(channel, fds, len, timeoutSec);
    } else {
        ret = -1;
    }
    return ret;
}

void Service_SetTlsOptions(Hcom_Service service,
                           bool enableTls,
                           Service_TlsVersion version,
                           Service_CipherSuite cipherSuite,
                           Hcom_TlsGetCertCb certCb,
                           Hcom_TlsGetPrivateKeyCb priKeyCb,
                           Hcom_TlsGetCACb caCb)
{
    if (g_hcomFunc.setTlsOptions != nullptr) {
        g_hcomFunc.setTlsOptions(service, enableTls, version, cipherSuite, certCb, priKeyCb, caCb);
    } else {
        return;
    }
}