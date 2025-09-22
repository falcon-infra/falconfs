#include "kv_service_operation.h"
#include <unistd.h>
#include "falcon_kv_server.h"
#include "hcom/kv_hcom_service.h"
#include "kv_ipc_message.h"
#include "kv_ipc_server.h"
#include "kv_utils.h"
#include "log/logging.h"
#include "memory_pool.h"
#include "resource_pool.h"

uint64_t KvServiceOperation::mSharedFileSize = 0;
uintptr_t KvServiceOperation::mSharedFileAddress = 0;
std::mutex KvServiceOperation::mMutex = {};
bool KvServiceOperation::mInited = false;
KvIpcServerPtr KvServiceOperation::mIpcServer = nullptr;
int KvServiceOperation::mSharedFd = -1;

// TODO 考虑到C++注册函数到C接口，ABI兼容性问题，类成员变量和成员函数暂时设置成static
auto blkSize = DEFAULT_BLOCK_SIZE;
auto blkNum = DEFAULT_SHARED_FILE_SIZE / DEFAULT_BLOCK_SIZE;
ResourcePool::Configure KvServiceOperation::poolConfig = {"falcon_kv", blkSize *blkNum, blkSize};
MemoryPool KvServiceOperation::pool(poolConfig);

std::mutex KvServiceOperation::kvInfoMappingLock;
std::unordered_map<Hcom_Channel, std::vector<keyInfoMap>> KvServiceOperation::kvInfoMapping;

// KvServiceOperation::KvServiceOperation(uint64_t blkSize, uint64_t blkCnt, std::string name)
//     : blockSize{blkSize},
//       blockNum{blkCnt},
//       poolName{name},
//       poolConfig{"falcon_kv", blkSize * blkCnt, blkSize}
//     //   pool{poolConfig}
// {
// }

int32_t KvServiceOperation::HandleNewConnection(Hcom_Channel channel, uint64_t usrCtx, const char *payLoad)
{
    // TODO 记录kv channel信息
    FALCON_LOG(LOG_INFO) << "new channel " << channel;
    return 0;
}

void KvServiceOperation::HandleConnectionBroken(Hcom_Channel channel, uint64_t usrCtx, const char *payLoad)
{
    // release channel key blocks
    FALCON_LOG(LOG_INFO) << "channel broken " << channel;
    std::unique_lock<std::mutex> lock(kvInfoMappingLock);
    kvInfoMapping.erase(channel);
    lock.unlock();
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

    auto ret = pool.Initialize();
    if (ret != 0) {
        FALCON_LOG(LOG_ERROR) << "pool initialize failed";
        return -1;
    }

    auto attribute = pool.GetAttribute();
    mSharedFd = attribute.fd;
    mSharedFileAddress = (reinterpret_cast<uintptr_t>(attribute.address));
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

    ret = Reply(ctx, resp.result, static_cast<void *>(&resp), sizeof(KvOperationResp));
    if (ret != 0) {
        FALCON_LOG(LOG_ERROR) << "Reply failed ret " << ret;
        return -1;
    }

    return 0;
}

int32_t KvServiceOperation::HandleKvGetDataFromShmImpl(uint32_t &valueLen,
                                                       std::string &key,
                                                       Service_Context &ctx,
                                                       uint64_t &blockSize,
                                                       std::vector<uint64_t> &blockIds)
{
    std::vector<void *> blockAddrs;
    blockAddrs.reserve(blockIds.size());
    for (auto i = 0UL; i < blockIds.size(); i++) {
        uint64_t blkOffset;
        if (!pool.BlockOffset(blockIds[i], blkOffset)) {
            blkOffset = 0;
        }
        blockAddrs.push_back(reinterpret_cast<void *>(mSharedFileAddress + blkOffset));
    }

    // 调用kv server处理接口，读取内存共享数据，写入bio
    KVServer &kv = KVServer::getInstance();
    auto ret = kv.Get(key, blockAddrs, blockSize);
    if (ret != 0) {
        FALCON_LOG(LOG_ERROR) << "get data from bio failed, ret " << ret;
        ReleaseMultiBlocks(blockIds);
        blockIds.clear();
    }
    if (ret == 0) {
        // 记录key到 get map中
        RecordPutKeyMapInfo(ctx, key, blockSize, blockIds);
    }

    uint32_t respLen = sizeof(KvAllocateMoreBlockResp) + sizeof(uint64_t) * (blockIds.size());
    auto buffer = new (std::nothrow) uint8_t[respLen];
    if (buffer == nullptr) {
        ReleaseMultiBlocks(blockIds);
        FALCON_LOG(LOG_ERROR) << "Failed to allocate buffer respLen" << respLen;
        return -1;
    }
    auto *resp = reinterpret_cast<KvAllocateMoreBlockResp *>(buffer);
    resp->result = ret;
    resp->valueLen = valueLen;
    resp->blockSize = blockSize;
    resp->blockCount = static_cast<uint32_t>(blockIds.size());
    for (auto i = 0UL; i < resp->blockCount; i++) {
        uint64_t blkOffset;
        if (!pool.BlockOffset(blockIds[i], blkOffset)) {
            blkOffset = 0;
        }
        resp->dataBlock[i] = blkOffset;
    }

    ret = Reply(ctx, resp->result, static_cast<void *>(resp), respLen);
    if (ret != 0) {
        ReleaseMultiBlocks(blockIds);
        FALCON_LOG(LOG_ERROR) << "Reply failed ret " << ret;
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
    auto req = static_cast<KvAllocateMoreBlockReq *>(msgData);
    if (msgLength != sizeof(KvAllocateMoreBlockReq)) {
        FALCON_LOG(LOG_ERROR) << "msgLength is invalid";
        return -1;
    }

    FALCON_LOG(LOG_INFO) << "Kv get req info: " << req->ToString();

    auto key = req->Key();
    uint32_t valueLen = 0;

    KVServer &kv = KVServer::getInstance();
    auto ret = kv.GetValueLen(key, valueLen);
    if (ret != 0) {
        FALCON_LOG(LOG_ERROR) << "get value len failed, ret " << ret;
    }

    uint64_t blockSize = 0;
    std::vector<uint64_t> blockIds;
    ret = AllocateDataBlocks(valueLen, blockIds, blockSize);
    if (ret != 0) {
        FALCON_LOG(LOG_ERROR) << "Allocate data more blocks failed";
        return -1;
    }
    // TODO 打印blocks
    FALCON_LOG(LOG_INFO) << "wxt get blockSize " << blockSize << " blockCount " << blockIds.size() << " key " << key;
    for (auto i = 0UL; i < blockIds.size(); i++) {
        FALCON_LOG(LOG_INFO) << "wxt get block idx: " << blockIds[i];
    }

    return HandleKvGetDataFromShmImpl(valueLen, key, ctx, blockSize, blockIds);
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

    std::shared_ptr<KvInfo> keyInfo = nullptr;
    GetKvInfoFromMapInfo(ctx, key, keyInfo);
    if (!keyInfo->Valid() || keyInfo == nullptr) {
        FALCON_LOG(LOG_ERROR) << "KeyInfo blocks is nullprt";
        return -1;
    }

    KVServer &kv = KVServer::getInstance();
    uint32_t blockCount = static_cast<uint32_t>(keyInfo->blocks->size());
    std::vector<void *> blockAddrs;
    blockAddrs.reserve(blockCount);
    for (auto i = 0UL; i < blockCount; i++) {
        uint64_t blkOffset;
        if (!pool.BlockOffset(keyInfo->blocks->at(i), blkOffset)) {
            blkOffset = 0;
        }
        blockAddrs.push_back(reinterpret_cast<void *>(mSharedFileAddress + blkOffset));
    }
    // 调用kv server处理接口，读取内存共享数据，写入bio
    auto ret = kv.Put(key, valueLen, blockAddrs, keyInfo->blockSize);
    if (ret != 0) {
        FALCON_LOG(LOG_ERROR) << "put data to bio failed, ret: " << ret;
    }

    // 写bio完成 释放blocks
    ReleasePutKeyMapInfo(ctx, key);

    KvOperationResp resp;
    resp.result = ret;

    ret = Reply(ctx, resp.result, static_cast<void *>(&resp), sizeof(KvOperationResp));
    if (ret != 0) {
        FALCON_LOG(LOG_ERROR) << "Reply failed ret " << ret;
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

    KvSharedFileInfoResp resp;
    resp.result = 0;
    resp.shardFileSize = mSharedFileSize;
    auto attribute = pool.GetAttribute();
    resp.blockSize = attribute.blockSize;

    auto ret = Reply(ctx, resp.result, static_cast<void *>(&resp), sizeof(KvSharedFileInfoResp));
    if (ret != 0) {
        FALCON_LOG(LOG_ERROR) << "Reply failed ret " << ret;
        return -1;
    }

    return 0;
}

bool KvServiceOperation::AllocateMultiBlocks(uint64_t count, std::vector<uint64_t> &blocks)
{
    uint64_t blockId;
    blocks.clear();
    blocks.reserve(count);
    auto retryCount = 0;
    for (auto i = 0UL; i < count; i++) {
        auto success = pool.Allocate(blockId);
        while (!success) {
            success = pool.Allocate(blockId);
            if (success) {
                retryCount = 0;
                break;
            }
            if (++retryCount > ALLOCATE_BLOCKS_RETRY_MAX) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
        if (!success) {
            FALCON_LOG(LOG_ERROR) << "Failed to allocate multi-block block";
            ReleaseMultiBlocks(blocks);
            blocks.clear();
            return false;
        }
        blocks.emplace_back(blockId);
    }
    return true;
}

int32_t KvServiceOperation::AllocateDataBlocks(uint32_t bytes, std::vector<uint64_t> &blocks, uint64_t &blockSize)
{
    auto attribute = pool.GetAttribute();
    blockSize = attribute.blockSize;
    auto needBlockCount = (bytes + blockSize - 1UL) / blockSize;
    if (!AllocateMultiBlocks(needBlockCount, blocks)) {
        FALCON_LOG(LOG_ERROR) << "Allocate data more blocks falied";
        return -1;
    }

    return 0;
}

void KvServiceOperation::ReleaseMultiBlocks(const std::vector<uint64_t> &blocks)
{
    for (auto &blockId : blocks) {
        pool.Release(blockId);
    }
    return;
}

void KvServiceOperation::RecordPutKeyMapInfo(Service_Context ctx,
                                             std::string &key,
                                             uint64_t blockSize,
                                             const std::vector<uint64_t> &blocks)
{
    Hcom_Channel channel;
    if (Service_GetChannel(ctx, &channel) != 0) {
        FALCON_LOG(LOG_ERROR) << "Get channel failed";
        return;
    }

    std::unique_lock<std::mutex> lock(kvInfoMappingLock);
    auto KeyInfo = std::make_shared<KvInfo>(key, blockSize, pool);
    if (!KeyInfo->Valid()) {
        FALCON_LOG(LOG_ERROR) << "Put KeyInfo intialize failed";
        return;
    }
    for (auto &block : blocks) {
        KeyInfo->blocks->push_back(block);
    }

    std::unordered_map<std::string, std::shared_ptr<KvInfo>> KeyMap;
    KeyMap.insert({key, KeyInfo});
    kvInfoMapping[channel].push_back(KeyMap);
    return;
}

void KvServiceOperation::ReleasePutKeyMapInfo(Service_Context ctx, std::string &key)
{
    Hcom_Channel channel;
    if (Service_GetChannel(ctx, &channel) != 0) {
        FALCON_LOG(LOG_ERROR) << "Get channel failed";
        return;
    }
    std::unique_lock<std::mutex> lock(kvInfoMappingLock);
    auto it = kvInfoMapping.find(channel);
    if (it != kvInfoMapping.end()) {
        auto &KvInfoVec = it->second;
        auto pred = [&](const auto &infoMap) { return infoMap.find(key) != infoMap.end(); };
        auto mapIt = std::find_if(KvInfoVec.begin(), KvInfoVec.end(), pred);
        if (mapIt != KvInfoVec.end()) {
            KvInfoVec.erase(mapIt);
        }
    }
    lock.unlock();
}

void KvServiceOperation::GetKvInfoFromMapInfo(Service_Context ctx, std::string &key, std::shared_ptr<KvInfo> &info)
{
    Hcom_Channel channel;
    if (Service_GetChannel(ctx, &channel) != 0) {
        FALCON_LOG(LOG_ERROR) << "Get channel failed";
        return;
    }
    std::unique_lock<std::mutex> lock(kvInfoMappingLock);
    auto it = kvInfoMapping.find(channel);
    if (it != kvInfoMapping.end()) {
        auto &KvInfoVec = it->second;
        for (auto &infoMap : KvInfoVec) {
            if (infoMap.find(key) != infoMap.end()) {
                info = infoMap.at(key);
                break;
            }
        }
    }
    lock.unlock();
}

int32_t KvServiceOperation::HandleAllocateMoreBlocks(Service_Context ctx, uint64_t usrCtx)
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
    if (msgLength != sizeof(KvAllocateMoreBlockReq)) {
        FALCON_LOG(LOG_ERROR) << "msgLength is invalid";
        return -1;
    }
    auto req = static_cast<KvAllocateMoreBlockReq *>(msgData);

    uint32_t valueLen = req->valueLen;
    std::string key = req->Key();
    uint64_t blockSize = 0;
    std::vector<uint64_t> blockIds;

    auto ret = AllocateDataBlocks(valueLen, blockIds, blockSize);
    if (ret != 0) {
        FALCON_LOG(LOG_ERROR) << "Allocate data more blocks failed";
        return -1;
    }
    RecordPutKeyMapInfo(ctx, key, blockSize, blockIds);

    uint32_t respLen = sizeof(KvAllocateMoreBlockResp) + sizeof(uint64_t) * (blockIds.size());
    auto buffer = new (std::nothrow) uint8_t[respLen];
    if (buffer == nullptr) {
        ReleaseMultiBlocks(blockIds);
        FALCON_LOG(LOG_ERROR) << "Failed to allocate buffer respLen" << respLen;
        return -1;
    }
    auto *resp = reinterpret_cast<KvAllocateMoreBlockResp *>(buffer);
    resp->result = ret;
    resp->blockSize = blockSize;
    resp->blockCount = static_cast<uint32_t>(blockIds.size());
    for (auto i = 0UL; i < resp->blockCount; i++) {
        uint64_t blkOffset;
        if (!pool.BlockOffset(blockIds[i], blkOffset)) {
            blkOffset = 0;
        }
        resp->dataBlock[i] = blkOffset;
    }

    // TODO 打印blocks
    FALCON_LOG(LOG_INFO) << "wxt put blockSize " << resp->blockSize << " blockCount " << resp->blockCount << " key "
                         << key;
    for (auto i = 0UL; i < resp->blockCount; i++) {
        FALCON_LOG(LOG_INFO) << "wxt put block idx: " << blockIds[i];
    }

    ret = Reply(ctx, resp->result, static_cast<void *>(resp), respLen);
    if (ret != 0) {
        ReleaseMultiBlocks(blockIds);
        FALCON_LOG(LOG_ERROR) << "Reply failed ret " << ret;
        return -1;
    }

    return 0;
}

int32_t KvServiceOperation::Reply(Service_Context &ctx, int32_t retCode, void *resp, uint32_t respSize)
{
    Hcom_Channel channel;
    if (Service_GetChannel(ctx, &channel) != 0) {
        FALCON_LOG(LOG_ERROR) << "Get channel failed";
        return -1;
    }
    FALCON_LOG(LOG_INFO) << " wxt channel: " << channel;

    Channel_ReplyContext replyCtx;
    if (Service_GetRspCtx(ctx, &replyCtx) != 0) {
        FALCON_LOG(LOG_ERROR) << "Get response ctx failed";
        return -1;
    }
    replyCtx.errorCode = retCode;

    Channel_Request response = {resp, respSize, 0};

    Channel_Callback cb;
    cb.cb = KvServiceOperation::CommonCb;
    cb.arg = NULL;

    if (Channel_Reply(channel, response, replyCtx, &cb) != 0) {
        FALCON_LOG(LOG_ERROR) << "failed to post message to data to server";
        return -1;
    }
    return 0;
}

int32_t KvServiceOperation::HandleGetFinishFreeBlocks(Service_Context ctx, uint64_t usrCtx)
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
    auto req = static_cast<KvGetDataFinishedReq *>(msgData);
    if (msgLength != sizeof(KvGetDataFinishedReq)) {
        FALCON_LOG(LOG_ERROR) << "msgLength is invalid";
        return -1;
    }

    auto key = req->Key();
    // 读bio完成 释放blocks
    ReleasePutKeyMapInfo(ctx, key);

    KvGetDataFinishedResp resp;
    resp.result = 0;

    auto ret = Reply(ctx, resp.result, static_cast<void *>(&resp), sizeof(KvGetDataFinishedResp));
    if (ret != 0) {
        FALCON_LOG(LOG_ERROR) << "Reply failed ret " << ret;
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
    mIpcServer->RegisterNewRequestHandler(IPC_OP_KV_ALLOCATE_MORE_BLOCK, &KvServiceOperation::HandleAllocateMoreBlocks);
    mIpcServer->RegisterNewRequestHandler(IPC_OP_KV_GET_SHM_FINISH, &KvServiceOperation::HandleGetFinishFreeBlocks);

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