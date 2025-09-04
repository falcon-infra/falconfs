#include <fcntl.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <cstdlib>
#include <dlfcn.h>
#include <unistd.h>
#include <mutex>
#include <cerrno>
#include <vector>
#include <linux/version.h>
#include "kv_service_operation.h"
#include "hcom/kv_hcom_service.h"
#include "kv_ipc_message.h"
#include "kv_ipc_server.h"
#include "log/logging.h"
#include "kv_utils.h"
#include "falcon_kv_server.h"

uint64_t KvServiceOperation::mSharedFileSize = 0;
uintptr_t KvServiceOperation::mSharedFileAddress = 0;
std::mutex KvServiceOperation::mMutex = {};
bool KvServiceOperation::mInited = false;
KvIpcServerPtr KvServiceOperation::mIpcServer = nullptr;
int KvServiceOperation::mSharedFd = -1;

int32_t KvServiceOperation::HandleNewConnection(Hcom_Channel channel, uint64_t usrCtx, const char *payLoad)
{
    // TODO 记录kv channel信息
    FALCON_LOG(LOG_INFO) << "KvServiceOperation HandleNewConnection";
    return 0;
}

void KvServiceOperation::HandleConnectionBroken(Hcom_Channel channel, uint64_t usrCtx, const char *payLoad)
{
    // TODO 断链后，处理内存kv数据信息，释放blocks
    FALCON_LOG(LOG_INFO) << "KvServiceOperation HandleGetSharedFileInfo";
    return;
}

int32_t KvServiceOperation::KvShmAndIpcServiceInit(void)
{
    auto ret = ServiceShmInit();
    if (ret != 0) {
        FALCON_LOG(LOG_ERROR) << "service init shared memory failed";
        return -1;
    }
    ret = Initialize();
    if (ret != 0) {
        FALCON_LOG(LOG_ERROR) << "init ipc service failed, ret:" << ret;
        return -1;
    }

    return 0;
}

int32_t KvServiceOperation::ServiceShmInit()
{
    if (mShmInit) {
        return 0;
    }
    FALCON_LOG(LOG_INFO) << "Start to init shared memory";
    int fd = -1;
    std::string name = "falconfsShm";
#if LINUX_VERSION_CODE < KERNEL_VERSION(3, 17, 0)
    fd = shm_open(name.c_str(), O_CREAT | O_RDWR | O_EXCL | O_CLOEXEC, 600UL);
#else
    fd = syscall(SYS_memfd_create, name.c_str(), 0);
#endif

    if (fd < 0) {
        FALCON_LOG(LOG_ERROR) << "create shared memory failed, errno:" << errno;
        return -1;
    }

    if (ftruncate(fd, DEFAULT_SHARED_FILE_SIZE) != 0) {
        SafeCloseFd(fd);
        FALCON_LOG(LOG_ERROR) << "ftruncate failed, errno:" << errno;
        return -1;
    }

    /* mmap */
    auto mappedAddress = mmap(nullptr, DEFAULT_SHARED_FILE_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (mappedAddress == MAP_FAILED) {
        SafeCloseFd(fd);
        FALCON_LOG(LOG_ERROR) << "mmap failed, errno:" << errno;
        return -1;
    }

    /* owner set 1B per 4K, make sure physical page allocated */
    FALCON_LOG(LOG_INFO) << "Start to reserve physical memory from OS";
    {
        auto startAddr = static_cast<uint8_t *>(mappedAddress);
        MultiReservePhysicalPage(startAddr, DEFAULT_SHARED_FILE_SIZE);
        auto pos = startAddr + (DEFAULT_SHARED_FILE_SIZE - 1L);
        *pos = 0;
    }
    FALCON_LOG(LOG_INFO) << "Finished to reserve physical memory from OS";

    mSharedFd = fd;
    mSharedFileAddress = (reinterpret_cast<uintptr_t>(mappedAddress));
    mSharedFileSize = DEFAULT_SHARED_FILE_SIZE;

    mShmInit = true;
    return 0;
}

int32_t KvServiceOperation::HandleKvDeleteKey(Service_Context ctx, uint64_t usrCtx)
{
    if (!mInited) {
        FALCON_LOG(LOG_INFO) << "KvServiceOperation has not initialize";
        return -1;
    }
    void *msgData = Service_GetMessageData(ctx);
    if (msgData == NULL) {
        FALCON_LOG(LOG_ERROR) << "msgData is null";
        return -1;
    }
    uint32_t msgLength = Service_GetMessageDataLen(ctx);
    auto req = static_cast<KvOperationReq *>(msgData);
    if (msgLength != sizeof(KvOperationReq)) {
        FALCON_LOG(LOG_ERROR) << "msgLength is invalid";
        return -1;
    }

    FALCON_LOG(LOG_INFO) << " Kv delete req " << req->ToString();

    auto key = req->Key();
    // 调用kv server处理接口，删除元数据key
    KVServer &kv = KVServer::getInstance();
    auto ret = kv.Delete(key);
    if (ret != 0) {
        FALCON_LOG(LOG_ERROR) << "Kv delete failed, ret " << ret;
    }

    KvOperationResp resp;
    resp.result = ret;

    Hcom_Channel channel;
    if (Service_GetChannel(ctx, &channel) != 0) {
        FALCON_LOG(LOG_ERROR) << "Get channel failed";
        return -1;
    }

    Channel_ReplyContext replyCtx;
    if (Service_GetRspCtx(ctx, &replyCtx) != 0) {
        FALCON_LOG(LOG_ERROR) << "Get response ctx failed";
        return -1;
    }
    replyCtx.errorCode = ret;

    Channel_Request response = { static_cast<void *>(&resp), sizeof(KvOperationResp), 0 };

    Channel_Callback cb;
    cb.cb = KvServiceOperation::CommonCb;
    cb.arg = NULL;

    if (Channel_Reply(channel, response, replyCtx, &cb) != 0) {
        FALCON_LOG(LOG_ERROR) << "failed to post message to data to server";
        return -1;
    }
    return 0;
}

int32_t KvServiceOperation::HandleKvGetDataFromShm(Service_Context ctx, uint64_t usrCtx)
{
    if (!mInited) {
        FALCON_LOG(LOG_INFO) << "KvServiceOperation has not initialize";
        return -1;
    }
    void *msgData = Service_GetMessageData(ctx);
    if (msgData == NULL) {
        FALCON_LOG(LOG_ERROR) << "msgData is null";
        return -1;
    }
    uint32_t msgLength = Service_GetMessageDataLen(ctx);
    auto req = static_cast<KvOperationReq *>(msgData);
    if (msgLength != sizeof(KvOperationReq)) {
        FALCON_LOG(LOG_ERROR) << "msgLength is invalid";
        return -1;
    }

    FALCON_LOG(LOG_INFO) << " Kv get req " << req->ToString();

    auto key = req->Key();
    uint32_t valueLen = 0;
    KVServer &kv = KVServer::getInstance();
    // 调用kv server处理接口，读取bio数据到SHM
    auto ret = kv.Get(key, reinterpret_cast<void *>(mSharedFileAddress), valueLen);
    if (ret != 0) {
        FALCON_LOG(LOG_ERROR) << "get data from bio failed, ret " << ret;
    }

    KvOperationResp resp;
    resp.result = ret;
    resp.valueLen = valueLen;

    Hcom_Channel channel;
    if (Service_GetChannel(ctx, &channel) != 0) {
        FALCON_LOG(LOG_ERROR) << "Get channel failed";
        return -1;
    }

    Channel_ReplyContext replyCtx;
    if (Service_GetRspCtx(ctx, &replyCtx) != 0) {
        FALCON_LOG(LOG_ERROR) << "Get response ctx failed";
        return -1;
    }
    replyCtx.errorCode = ret;

    Channel_Request response = { static_cast<void *>(&resp), sizeof(KvOperationResp), 0 };

    Channel_Callback cb;
    cb.cb = KvServiceOperation::CommonCb;
    cb.arg = NULL;

    if (Channel_Reply(channel, response, replyCtx, &cb) != 0) {
        FALCON_LOG(LOG_ERROR) << "failed to post message to data to server";
        return -1;
    }

    return 0;
}

int32_t KvServiceOperation::HandleKvPutData2Shm(Service_Context ctx, uint64_t usrCtx)
{
    if (!mInited) {
        FALCON_LOG(LOG_INFO) << "KvServiceOperation has not initialize";
        return -1;
    }
    void *msgData = Service_GetMessageData(ctx);
    if (msgData == NULL) {
        FALCON_LOG(LOG_ERROR) << "msgData is null";
        return -1;
    }
    uint32_t msgLength = Service_GetMessageDataLen(ctx);
    auto req = static_cast<KvOperationReq *>(msgData);
    if (msgLength != sizeof(KvOperationReq)) {
        FALCON_LOG(LOG_ERROR) << "msgLength is invalid";
        return -1;
    }

    FALCON_LOG(LOG_INFO) << " Kv put req: " << req->ToString();

    uint32_t valueLen = req->valueLen;
    std::string key = req->Key();

    // 调用kv server处理接口，读取内存共享数据，写入bio
    KVServer &kv = KVServer::getInstance();
    auto ret = kv.Put(key, reinterpret_cast<void *>(mSharedFileAddress), valueLen);
    if (ret != 0) {
        FALCON_LOG(LOG_ERROR) << "put data to bio failed, ret: " << ret;
    }

    KvOperationResp resp;
    resp.result = ret;

    Hcom_Channel channel;
    if (Service_GetChannel(ctx, &channel) != 0) {
        FALCON_LOG(LOG_ERROR) << "Get channel failed";
        return -1;
    }

    Channel_ReplyContext replyCtx;
    if (Service_GetRspCtx(ctx, &replyCtx) != 0) {
        FALCON_LOG(LOG_ERROR) << "Get response ctx failed";
        return -1;
    }
    replyCtx.errorCode = ret;

    Channel_Request response = { static_cast<void *>(&resp), sizeof(KvOperationResp), 0 };

    Channel_Callback cb;
    cb.cb = KvServiceOperation::CommonCb;
    cb.arg = NULL;

    FALCON_LOG(LOG_INFO) << "replyCtx.errorCode: " << replyCtx.errorCode;

    if (Channel_Reply(channel, response, replyCtx, &cb) != 0) {
        FALCON_LOG(LOG_ERROR) << "failed to post message to data to server";
        return -1;
    }

    return 0;
}

int32_t KvServiceOperation::HandleGetSharedFileInfo(Service_Context ctx, uint64_t usrCtx)
{
    if (!mInited) {
        FALCON_LOG(LOG_INFO) << "KvServiceOperation has not initialize";
        return -1;
    }

    void *msgData = Service_GetMessageData(ctx);
    uint32_t msgLength = Service_GetMessageDataLen(ctx);
    if (msgData == NULL) {
        FALCON_LOG(LOG_ERROR) << "msgData is null";
        return -1;
    }
    if (msgLength != sizeof(KvSharedFileInfoReq)) {
        FALCON_LOG(LOG_ERROR) << "msgLength is invalid";
        return -1;
    }

    // uint16_t opCode = Service_GetOpCode(ctx);
    KvSharedFileInfoResp resp;
    resp.result = 0;
    resp.shardFileSize = mSharedFileSize;

    Hcom_Channel channel;
    if (Service_GetChannel(ctx, &channel) != 0) {
        FALCON_LOG(LOG_ERROR) << "Get channel failed";
        return -1;
    }

    Channel_ReplyContext replyCtx;
    if (Service_GetRspCtx(ctx, &replyCtx) != 0) {
        FALCON_LOG(LOG_ERROR) << "Get response ctx failed";
        return -1;
    }
    replyCtx.errorCode = 0;

    Channel_Request req = { static_cast<void *>(&resp), sizeof(KvSharedFileInfoResp), 0 };

    Channel_Callback cb;
    cb.cb = KvServiceOperation::CommonCb;
    cb.arg = NULL;

    if (Channel_Reply(channel, req, replyCtx, &cb) != 0) {
        FALCON_LOG(LOG_ERROR) << "failed to post message to data to server";
        return -1;
    }

    return 0;
}

int32_t KvServiceOperation::RegisterHandlers(void)
{
    mIpcServer->RegisterNewChannelHandler(&KvServiceOperation::HandleNewConnection);
    mIpcServer->RegisterChannelBrokenHandler(&KvServiceOperation::HandleConnectionBroken);
    mIpcServer->RegisterNewRequestHandler(IPC_OP_KV_GET_SHARED_FILE_INFO, &KvServiceOperation::HandleGetSharedFileInfo);
    mIpcServer->RegisterNewRequestHandler(IPC_OP_KV_PUT_SHM_FINISH, &KvServiceOperation::HandleKvPutData2Shm);
    mIpcServer->RegisterNewRequestHandler(IPC_OP_KV_GET_FROM_SHM, &KvServiceOperation::HandleKvGetDataFromShm);
    mIpcServer->RegisterNewRequestHandler(IPC_OP_KV_DELETE, &KvServiceOperation::HandleKvDeleteKey);

    return 0;
}

int32_t KvServiceOperation::Initialize(void)
{
    char *WORKSPACE_PATH = std::getenv("WORKSPACE_PATH");
    if (!WORKSPACE_PATH) {
        FALCON_LOG(LOG_ERROR) << "WORKSPACE_PATH not set";
        return -1;
    }
    std::string workerPath = WORKSPACE_PATH ? WORKSPACE_PATH : "";
    std::string socketPath = workerPath + "/falconfs_kv_socket.s";

    std::lock_guard<std::mutex> guard(mMutex);
    if (mInited) {
        FALCON_LOG(LOG_INFO) << "KvServiceOperation has been initialized";
        return 0;
    }

    mIpcServer = std::make_shared<KvIpcServer>(socketPath);
    if (UNLIKELY(mIpcServer == nullptr)) {
        FALCON_LOG(LOG_ERROR) << "Failed to new IpcServer, probably out memory";
        return -1;
    }

    auto ret = RegisterHandlers();
    if (UNLIKELY(ret != 0)) {
        FALCON_LOG(LOG_ERROR) << "Failed to register handlers, probably out memory";
        return -1;
    }

    ret = mIpcServer->Start();
    if (UNLIKELY(ret != 0)) {
        mIpcServer->Stop();
        mIpcServer = nullptr;
        FALCON_LOG(LOG_ERROR) << "Failed to start IpcServer";
        return -1;
    }
    FALCON_LOG(LOG_INFO) << "IpcServer started";
    mInited = true;
    return 0;
}

void KvServiceOperation::UnInitialize(void)
{
    if (!mInited) {
        FALCON_LOG(LOG_INFO) << "KvServiceOperation has not been initialized";
        return;
    }
    mIpcServer->Stop();
    mIpcServer = nullptr;
    mInited = false;
}