#ifndef FALCONFS_KV_SERVICE_OPERATION_H
#define FALCONFS_KV_SERVICE_OPERATION_H

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>
#include "hcom/kv_dlopen_init.h"
#include "hcom/kv_hcom_service.h"
#include "kv_ipc_message.h"
#include "kv_ipc_server.h"
#include "kv_utils.h"
#include "memory_pool.h"
#include "resource_pool.h"

struct KvInfo {

    KvInfo(std::string key, const uint64_t blockSize, MemoryPool &pool)
        : key{std::move(key)},
          blockSize{blockSize},
          memPool{&pool}
    {
        blocks = new (std::nothrow) std::vector<uint64_t>;
    }

    inline bool Valid() const noexcept { return blocks != nullptr; }

    ~KvInfo() noexcept
    {
        if (blocks != nullptr) {
            for (auto &block : *blocks) {
                memPool->Release(block);
            }
        }
        delete blocks;
        blocks = nullptr;
        memPool = nullptr;
    }

    std::string key;
    uint64_t blockSize{0UL};
    std::vector<uint64_t> *blocks{nullptr};
    MemoryPool *memPool{nullptr};
};

using keyInfoMap = std::unordered_map<std::string, std::shared_ptr<KvInfo>>;

class KvServiceOperation {
  public:
    // KvServiceOperation(uint64_t blkSize, uint64_t blkCnt, std::string name);
    int32_t Initialize();
    void UnInitialize();
    static std::shared_ptr<KvServiceOperation> Instance()
    {
        // 后续配置文件读取，暂时默认值
        // auto blkSize = DEFAULT_BLOCK_SIZE;
        // auto blkNum = DEFAULT_SHARED_FILE_SIZE / DEFAULT_BLOCK_SIZE;
        // static auto instance = std::make_shared<KvServiceOperation>(blkSize, blkNum, "falcon_kv");
        static auto instance = std::make_shared<KvServiceOperation>();
        return instance;
    }

  public:
    static void CommonCb(void *arg, Service_Context context) { return; }
    static int GetShareMemoryFd(void) { return mSharedFd; }
    int32_t KvShmAndIpcServiceInit();
    int32_t ServiceShmInit();
    static int32_t RegisterHandlers();
    static int32_t HandleNewConnection(Hcom_Channel channel, uint64_t usrCtx, const char *payLoad);
    static void HandleConnectionBroken(Hcom_Channel channel, uint64_t usrCtx, const char *payLoad);
    static int32_t HandleGetSharedFileInfo(Service_Context ctx, uint64_t usrCtx);
    static int32_t HandleKvPutData2Shm(Service_Context ctx, uint64_t usrCtx);
    static int32_t HandleKvGetDataFromShm(Service_Context ctx, uint64_t usrCtx);
    static int32_t HandleKvDeleteKey(Service_Context ctx, uint64_t usrCtx);
    static int32_t HandleAllocateMoreBlocks(Service_Context ctx, uint64_t usrCtx);
    static int32_t HandleGetFinishFreeBlocks(Service_Context ctx, uint64_t usrCtx);

  private:
    static int32_t Reply(Service_Context &ctx, int32_t retCode, void *resp, uint32_t respSize);
    static int32_t AllocateDataBlocks(uint32_t bytes, std::vector<uint64_t> &blocks, uint64_t &blockSize);
    static bool AllocateMultiBlocks(uint64_t count, std::vector<uint64_t> &blocks);
    static void ReleaseMultiBlocks(const std::vector<uint64_t> &blocks);
    static void RecordPutKeyMapInfo(Service_Context ctx, std::string &key, uint64_t blockSize, const std::vector<uint64_t> &blocks);
    static void ReleasePutKeyMapInfo(Service_Context ctx, std::string &key);
    static void GetKvInfoFromMapInfo(Service_Context ctx, std::string &key, std::shared_ptr<KvInfo> &info);
    static int32_t HandleKvGetDataFromShmImpl(uint32_t &valueLen, std::string &key, Service_Context &ctx, uint64_t &blockSize, std::vector<uint64_t> &blockIds);
    

  private:
    static ResourcePool::Configure poolConfig;
    uint64_t blockSize { 0 };
    uint64_t blockNum { 0 };
    std::string poolName {"falcon_kv"};
    bool mShmInit = false;
    static int mSharedFd;
    static uint64_t mSharedFileSize;
    static uintptr_t mSharedFileAddress;
    static KvIpcServerPtr mIpcServer;
    static std::mutex mMutex;
    static bool mInited;

    static MemoryPool pool;
    static std::mutex kvInfoMappingLock;
    static std::unordered_map<uintptr_t, std::vector<keyInfoMap>> kvInfoMapping;
};
#endif // FALCONFS_KV_SERVICE_OPERATION_H
