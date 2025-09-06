
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

// context for async put/get/remove
struct SyncContext
{
    std::atomic<uint16_t> pending_count;
    std::mutex wait_mutex; // TODO: 更换 mutex
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
    sem_t sem_; // TODO: 更换 mutex
};

class KVServer {
  private:
    uint32_t ioConcurrency_;
    uint64_t tenantId;

    SliceKeyGenerator& key_gen;

    static constexpr uint32_t SLICE_SIZE = 4 * 1024 * 1024; // slice 大小：4MB

    KVServer();
    ~KVServer() = default;
    KVServer(const KVServer &) = delete;
    KVServer &operator=(const KVServer &) = delete;

    bool putSlices(FormData_kv_index &kvMetaInfo, const char *valPtr, uint16_t sem_limit, BioLoadCallback putcallback);
    bool putMeta(FormData_kv_index &kvMetaInfo);

    bool getSlices(FormData_kv_index &kvMetaInfo,
                   char *valPtr,
                   uint32_t &valTotalSize,
                   uint16_t sem_limit,
                   BioGetCallbackFunc getcallback);
    bool getMeta(FormData_kv_index &kvMetaInfo);

    bool deleteSlices(FormData_kv_index &kvMetaInfo);
    bool deleteMeta(std::string &key);

    static void putCallback(void *context, int32_t result);
    static void getCallback(void *context, int32_t result, uint32_t sliceValSize);

  public:
    static KVServer &getInstance();

    int32_t Put(const std::string &key, const void *valPtr, uint32_t valSize);
    int32_t Get(const std::string &key, void *valPtr, uint32_t &valTotalSize);
    int32_t Delete(std::string &key);
};
