
#pragma once

#include <semaphore.h>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <stdexcept>
#include <string>
#include <vector>


#include "bio_c.h"
#include "slice_key_gen.h"
#include "falcon_kv_meta.h"

using MutexType = std::mutex;
using UniqueLock = std::unique_lock<MutexType>;

using SemType = sem_t;
inline int semaphore_init(SemType *sem_, unsigned int limit) { return sem_init(sem_, 0, limit); }
inline int semaphore_wait(SemType *sem_) { return sem_wait(sem_); }
inline int semaphore_post(SemType *sem_) { return sem_post(sem_); }
inline int semaphore_destroy(SemType *sem_) { return sem_destroy(sem_); }

// context for async put/get/remove
struct SyncContext
{
    std::atomic<uint16_t> pending_count;
    MutexType wait_mutex;
    std::condition_variable wait_cv;
};

class Semaphore;

struct AsyncPutContext
{
    std::shared_ptr<Semaphore> concurrency_sem; // 并发信号量
    CResult result;                             // 写入结果状态
    std::shared_ptr<SyncContext> sync_ctx;      // 回调中使用外部定义的 wait_cv
    uint64_t slice_key;                         // 日志打印
};

struct AsyncGetContext
{
    std::shared_ptr<Semaphore> concurrency_sem; // 并发信号量
    CResult result;                             // 读取结果状态
    uint32_t sliceSize;                         // 读取总数据大小
    std::shared_ptr<SyncContext> sync_ctx;      // 回调中使用外部定义的 wait_cv
    uint64_t slice_key;                         // 日志打印
};

// RAII 包装信号量，防止中途return后，sem_destroy没有执行
class Semaphore {
  public:
    explicit Semaphore(uint16_t limit)
    {
        if (sem_init(&sem_, 0, limit) != 0) {
            throw std::runtime_error("Failed to init semaphore");
        }
    }
    ~Semaphore() { sem_destroy(&sem_); }

    void wait() { sem_wait(&sem_); }
    void post() { sem_post(&sem_); }

    // 禁止拷贝
    Semaphore(const Semaphore &) = delete;
    Semaphore &operator=(const Semaphore &) = delete;

  private:
    SemType sem_;
};

class KVServer {
private:
    uint32_t ioConcurrency_;
    uint64_t tenantId;

    SliceKeyGenerator &key_gen;

    static constexpr uint32_t SLICE_SIZE = 2 * 1024 * 1024; // slice 大小：2MB

    KVServer();
    ~KVServer() = default;
    KVServer(const KVServer &) = delete;
    KVServer &operator=(const KVServer &) = delete;

    bool putSlices(FormData_kv_index &kvMetaInfo, const std::vector<void*>& blockAddrs,
                         uint32_t blockSize, uint16_t sem_limit, BioLoadCallback putcallback);
    bool putMeta(FormData_kv_index &kvMetaInfo);

    bool getSlices(FormData_kv_index &kvMetaInfo,
                   std::vector<void*>& blockAddrs,
                   uint32_t blockSize,
                   uint32_t &valTotalSize,
                   uint16_t sem_limit,
                   BioGetCallbackFunc getcallback);
    bool getMeta(FormData_kv_index &kvMetaInfo);

    bool deleteSlices(FormData_kv_index &kvMetaInfo);
    bool deleteMeta(std::string &key);

    static void putCallback(void *context, int32_t result);
    static void getCallback(void *context, int32_t result, uint32_t sliceValSize);

    // get blockPtr by offset when using putSlices/getSlices
    static int ctz32(std::uint32_t x) {   // 只在这个类里用
        if (x == 0) return 32;
        #ifdef _MSC_VER
                unsigned long i;
                _BitScanForward(&i, x);
                return static_cast<int>(i);
        #else
                return __builtin_ctz(x);
        #endif
            }
    static inline char* getPtrAtOffset(
        uint64_t offset,
        const std::vector<void*>& blockAddrs,
        uint32_t blockSize)
    {
        // if blockSize is power of 2
        if ((blockSize & (blockSize - 1)) == 0) {
            uint32_t block_idx = offset >> ctz32(blockSize);
            uint32_t in_block_offset = offset & (blockSize - 1);
            return static_cast<char*>(blockAddrs[block_idx]) + in_block_offset;
        } else {
            uint32_t block_idx = offset / blockSize;
            uint32_t in_block_offset = offset % blockSize;
            return static_cast<char*>(blockAddrs[block_idx]) + in_block_offset;
        }
    }

public:
    static KVServer &getInstance();

    int32_t Put(const std::string &key, uint32_t valSize, const std::vector<void*> blockAddrs, uint32_t blockSize);
    int32_t Get(const std::string &key, std::vector<void*> blockAddrs, uint32_t blockSize);
    int32_t Delete(std::string &key);
    int32_t GetValueLen(const std::string &key, uint32_t &valTotalSize);
};
