#ifndef HCOM_CAPI_V2_HCOM_SERVICE_C_V2_H_
#define HCOM_CAPI_V2_HCOM_SERVICE_C_V2_H_

#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef uintptr_t Hcom_Channel;
/*
 * @brief Service context, which used for callback as param
 */
typedef uintptr_t Service_Context;

/*
 * @brief service, which include oob & multi protocols(TCP/RDMA/SHM) workers & callback etc
 */
typedef uintptr_t Hcom_Service;

/*
 * @brief Channel, represent multi connections(EPs) of one protocol
 *
 * two side operation, Hcom_ChannelSend
 * read operation from remote, Hcom_ChannelRead
 * write operation from remote, Hcom_ChannelWrite
 */
typedef uintptr_t Hcom_Channel;

typedef uintptr_t Service_MemoryRegion;

/*
 * @brief Service context, which used for callback as param
 */
typedef uintptr_t Service_Context;

/*
 * Callback function which will be invoked by async use mode
 */
typedef void (*Channel_CallbackFunc)(void *arg, Service_Context context);
/*
 * @brief Callback function definition
 * 1) new endpoint connected from client, only need to register this at sever side
 * 2) endpoint is broken, called when RDMA qp detection error or broken
 */
typedef int (*Service_ChannelHandler)(Hcom_Channel channel, uint64_t usrCtx, const char *payLoad);
typedef void (*Service_IdleHandler)(uint8_t wkrGrpIdx, uint16_t idxInGrp, uint64_t usrCtx);
typedef int (*Service_RequestHandler)(Service_Context ctx, uint64_t usrCtx);

/*
 * @brief ValidateSecInfo callback function, when oob connect build, this function will be called to validate auth info
 * if this function not set oob will not validate secure info
 *
 * @param flag             [in] flag received in auth process
 * @param ctx              [in] ctx received in auth process
 * @param input            [in] secure info received
 * @param inputLen         [in] secure info length
 */
typedef int (*Hcom_SecInfoValidator)(uint64_t ctx, int64_t flag, const char *input, uint32_t inputLen);

/*
 * @brief External log callback function
 *
 * @param level            [in] level, 0/1/2/3 represent debug/info/warn/error
 * @param msg              [in] message, log message with name:code-line-number
 */
typedef void (*Service_LogHandler)(int level, const char *msg);

/*
 * @brief Worker polling type
 * 1 For RDMA:
 * C_BUSY_POLLING, means cpu 100% polling no matter there is request cb, better performance but cost dedicated CPU
 * C_EVENT_POLLING, waiting on OS kernel for request cb
 * 2 For TCP/UDS
 * only event pooling is supported
 */
typedef enum {
    C_SERVICE_BUSY_POLLING = 0,
    C_SERVICE_EVENT_POLLING = 1,
} Service_WorkerMode;

typedef enum {
    C_CLIENT_WORKER_POLL = 0,
    C_CLIENT_SELF_POLL = 1,
} Service_ClientPollingMode;

typedef enum {
    C_CHANNEL_FUNC_CB = 0,   // use channel function param (const NetCallback *cb)
    C_CHANNEL_GLOBAL_CB = 1, // use service RegisterOpHandler
} Channel_CBType;

typedef enum {
    HIGH_LEVEL_BLOCK,   /* spin-wait by busy loop */
    LOW_LEVEL_BLOCK,    /* full sleep */
} Channel_FlowCtrlLevel;

typedef enum {
    C_SERVICE_RDMA = 0,
    C_SERVICE_TCP = 1,
    C_SERVICE_UDS = 2,
    C_SERVICE_SHM = 3,
    C_SERVICE_UB = 4,
    C_SERVICE_UBOE = 5,
    C_SERVICE_UBC = 6,
    C_SERVICE_HSHMEM = 7,
} Service_Type;

typedef enum {
    C_CHANNEL_BROKEN_ALL = 0, /* when one ep broken, all eps broken */
    C_CHANNEL_RECONNECT = 1,  /* when one ep broken, try re-connect first. If re-connect fail, broken all eps */
    C_CHANNEL_KEEP_ALIVE = 2, /* when one ep broken, keep left eps alive until all eps broken */
} Service_ChannelPolicy;

/*
 * @brief Enum for callback register [new endpoint connected or endpoint broken]
 */
typedef enum {
    C_CHANNEL_NEW = 0,
    C_CHANNEL_BROKEN = 1,
} Service_ChannelHandlerType;

typedef enum {
    C_SERVICE_REQUEST_RECEIVED = 0,
    C_SERVICE_REQUEST_POSTED = 1,
    C_SERVICE_READWRITE_DONE = 2,
} Service_HandlerType;

typedef enum {
    SERVICE_ROUND_ROBIN = 0,
    SERVICE_HASH_IP_PORT = 1,
} Service_LBPolicy;

typedef enum {
    C_SERVICE_TLS_1_2 = 771,
    C_SERVICE_TLS_1_3 = 772,
} Service_TlsVersion;

typedef enum {
    C_SERVICE_AES_GCM_128 = 0,
    C_SERVICE_AES_GCM_256 = 1,
    C_SERVICE_AES_CCM_128 = 2,
    C_SERVICE_CHACHA20_POLY1305 = 3,
} Service_CipherSuite;

typedef enum {
    C_SERVICE_NET_SEC_DISABLED = 0,
    C_SERVICE_NET_SEC_ONE_WAY = 1,
    C_SERVICE_NET_SEC_TWO_WAY = 2,
} Service_SecType;

/*
 * @brief Context type, part of Service_Context, sync mode is not aware most of them
 */
typedef enum {
    SERVICE_RECEIVED = 0,     /* support invoke all functions */
    SERVICE_RECEIVED_RAW = 1, /* support invoke most functions except Service_GetOpInfo() */
    SERVICE_SENT = 2,         /* support invoke basic functions except
                                 Service_GetMessage() * 3、Service_GetRspCtx()、 */
    SERVICE_SENT_RAW = 3,     /* support invoke basic functions except
                                 Service_GetMessage() * 3、、Service_GetRspCtx()、Service_GetOpInfo() */
    SERVICE_ONE_SIDE = 4,     /* support invoke basic functions except
                                 Service_GetMessage() * 3、、Service_GetRspCtx()、Service_GetOpInfo() */

    SERVICE_INVALID_OP_TYPE = 255,
} Service_ContextType;

typedef struct {
    uint32_t maxSendRecvDataSize;
    uint16_t workerGroupId;
    uint16_t workerGroupThreadCount;
    Service_WorkerMode workerGroupMode;
    int8_t workerThreadPriority;
    char workerGroupCpuRange[64];   // worker group cpu range, for example 6-10
} Service_Options;

typedef struct {
    Channel_CallbackFunc cb; // User callback function
    void *arg;               // Argument of callback
} Channel_Callback;


typedef struct {
    uint16_t clientGroupId;     // worker group id of client
    uint16_t serverGroupId;     // worker group id of server
    uint8_t linkCount;     // actual link count of the channel
    Service_ClientPollingMode mode;
    Channel_CBType cbType;
    char payLoad[512];
} Service_ConnectOptions;

typedef struct {
    void *address;    /* pointer of data */
    uint32_t size;              /* size of data */
    uint16_t opcode;
} Channel_Request;

typedef struct {
    void *address;              /* pointer of data */
    uint32_t size;              /* size of data */
    int16_t errorCode;          /* error code of response */
} Channel_Response;

typedef struct {
    void *rspCtx;
    int16_t errorCode;
} Channel_ReplyContext;

typedef struct {
    uint32_t keys[4];
} OneSideKey;

/*
 * @brief Read/write mr info for one side rdma operation
 */
typedef struct {
    uintptr_t lAddress; // local memory region address
    OneSideKey lKey;      // local memory region key
    uint64_t size;      // data size
} Service_MemoryRegionInfo;

typedef struct {
    void *lAddress;
    void *rAddress;
    OneSideKey lKey;
    OneSideKey rKey;
    uint32_t size;
} Channel_OneSideRequest;

typedef struct {
    uint64_t intervalTimeMs;
    uint64_t thresholdByte;
    Channel_FlowCtrlLevel flowCtrlLevel;
} Channel_FlowCtrlOptions;

int Service_Create(Service_Type t, const char *name, Service_Options options, Hcom_Service *service);

int Service_Bind(Hcom_Service service, const char *listenerUrl, Service_ChannelHandler h);

int Service_Start(Hcom_Service service);

int Service_Destroy(Hcom_Service service, const char *name);

int Service_Connect(Hcom_Service service, const char *serverUrl, Hcom_Channel *channel, Service_ConnectOptions options);

int Service_DisConnect(Hcom_Service service, Hcom_Channel channel);

int Service_RegisterMemoryRegion(Hcom_Service service, uint64_t size, Service_MemoryRegion *mr);

int Service_GetMemoryRegionInfo(Service_MemoryRegion mr, Service_MemoryRegionInfo *info);

int Service_RegisterAssignMemoryRegion(
    Hcom_Service service, uintptr_t address, uint64_t size, Service_MemoryRegion *mr);

int Service_DestroyMemoryRegion(Hcom_Service service, Service_MemoryRegion mr);

void Service_RegisterChannelBrokerHandler(Hcom_Service service, Service_ChannelHandler h,
    Service_ChannelPolicy policy, uint64_t usrCtx);

void Service_RegisterIdleHandler(Hcom_Service service, Service_IdleHandler h, uint64_t usrCtx);

void Service_RegisterHandler(Hcom_Service service, Service_HandlerType t, Service_RequestHandler h,
    uint64_t usrCtx);

void Service_AddWorkerGroup(Hcom_Service service, int8_t priority, uint16_t workerGroupId, uint32_t threadCount,
    const char *cpuIdsRange);

void Service_AddListener(Hcom_Service service, const char *url, uint16_t workerCount);

void Service_SetConnectLBPolicy(Hcom_Service service, Service_LBPolicy lbPolicy);

void Service_SetTcpUserTimeOutSec(Hcom_Service service, uint16_t timeOutSec);

void Service_SetTcpSendZCopy(Hcom_Service service, bool tcpSendZCopy);

void Service_SetDeviceIpMask(Hcom_Service service, const char *ipMask);

void Service_SetDeviceIpGroup(Hcom_Service service, const char *ipGroup);

void Service_SetCompletionQueueDepth(Hcom_Service service, uint16_t depth);

void Service_SetSendQueueSize(Hcom_Service service, uint32_t sqSize);

void Service_SetRecvQueueSize(Hcom_Service service, uint32_t rqSize);

void Service_SetQueuePrePostSize(Hcom_Service service, uint32_t prePostSize);

void Service_SetPollingBatchSize(Hcom_Service service, uint16_t pollSize);

void Service_SetEventPollingTimeOutUs(Hcom_Service service, uint16_t pollTimeout);

void Service_SetTimeOutDetectionThreadNum(Hcom_Service service, uint32_t threadNum);

void Service_SetMaxConnectionCount(Hcom_Service service, uint32_t maxConnCount);

void Service_SetHeartBeatOptions(Hcom_Service service, uint16_t idleSec, uint16_t probeTimes, uint16_t intervalSec);

void Service_SetMultiRailOptions(Hcom_Service service, bool enable, uint32_t threshold);

void Channel_Refer(Hcom_Channel channel);
void Channel_DeRefer(Hcom_Channel channel);
int Channel_Send(Hcom_Channel channel, Channel_Request req, Channel_Callback *cb);
int Channel_Call(Hcom_Channel channel, Channel_Request req, Channel_Response *rsp, Channel_Callback *cb);
int Channel_Reply(Hcom_Channel channel, Channel_Request req, Channel_ReplyContext ctx, Channel_Callback *cb);
int Channel_Put(Hcom_Channel channel, Channel_OneSideRequest req, Channel_Callback *cb);
int Channel_Get(Hcom_Channel channel, Channel_OneSideRequest req, Channel_Callback *cb);
int Channel_SetFlowControlConfig(Hcom_Channel channel, Channel_FlowCtrlOptions opt);
void Channel_SetChannelTimeOut(Hcom_Channel channel, int16_t oneSideTimeout, int16_t twoSideTimeout);
void Channel_Close(Hcom_Channel channel);
uint64_t Channel_GetId(Hcom_Channel channel);

int Service_GetRspCtx(Service_Context context, Channel_ReplyContext *rspCtx);
int Service_GetChannel(Service_Context context, Hcom_Channel *channel);
int Service_GetContextType(Service_Context context, Service_ContextType *type);
int Service_GetResult(Service_Context context, int *result);
uint16_t Service_GetOpCode(Service_Context context);
void *Service_GetMessageData(Service_Context context);
uint32_t Service_GetMessageDataLen(Service_Context context);

// V2 SHM临时新增  for shared file
int Channel_SendFds(Hcom_Channel channel, int fds[], uint32_t len);
int Channel_ReceiveFds(Hcom_Channel channel, int fds[], uint32_t len, int32_t timeoutSec);

/*
 * @brief Set external logger function
 *
 * @param h                [in] the log function ptr
 */
void Service_SetExternalLogger(Service_LogHandler h);

#ifdef __cplusplus
}
#endif

#endif // HCOM_HCOM_SERVICE_C_V2_H
