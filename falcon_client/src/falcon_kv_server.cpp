#include "log/logging.h"
#include "slice_key_gen.h"
#include "falcon_kv_meta.h"
#include "falcon_kv_server.h"

KVServer &KVServer::getInstance()
{
    static KVServer instance;
    return instance;
}

KVServer::KVServer()
    : key_gen(SliceKeyGenerator::getInstance())
{
    // init ioConcurrency
    ioConcurrency_ = 10; // TODO: change ioConcurrency_
    // init tenantId TODO: change tenantId
    tenantId = static_cast<uint64_t>(0);

    FALCON_LOG(LOG_INFO) << "KVServer initialized with ioConcurrency=" << ioConcurrency_;
}

void KVServer::putCallback(void *context, int32_t result)
{
    AsyncPutContext *ctx = static_cast<AsyncPutContext *>(context);
    ctx->result = static_cast<CResult>(result);

    ctx->concurrency_sem->post();

    uint16_t left = ctx->sync_ctx->pending_count.fetch_sub(1, std::memory_order_acq_rel);
    if (left == 1) { // all async bio get finished
        std::unique_lock<std::mutex> lock(ctx->sync_ctx->wait_mutex);

        FALCON_LOG(LOG_INFO) << "All async bio put completed.";

        ctx->sync_ctx->wait_cv.notify_one();
    }

    FALCON_LOG(LOG_INFO) << "Async put for slice key=" << ctx->slice_key << " completed with result=" << result;
};

void KVServer::getCallback(void *context, int32_t result, uint32_t sliceValSize)
{
    AsyncGetContext *ctx = static_cast<AsyncGetContext *>(context);
    ctx->result = static_cast<CResult>(result);
    ctx->sliceSize = sliceValSize; // accumulate value size of each slice

    ctx->concurrency_sem->post();

    uint16_t left = ctx->sync_ctx->pending_count.fetch_sub(1, std::memory_order_acq_rel);
    if (left == 1) { // all async bio get finished
        std::unique_lock<std::mutex> lock(ctx->sync_ctx->wait_mutex);

        FALCON_LOG(LOG_INFO) << "All async bio get completed.";

        ctx->sync_ctx->wait_cv.notify_one();
    }

    FALCON_LOG(LOG_INFO) << "Async get for slice key=" << ctx->slice_key << " completed with result=" << result
                         << " and slice size=" << sliceValSize;
};

int32_t KVServer::Put(const std::string &key, uint32_t valSize, const std::vector<void*> blockAddrs, uint32_t blockSize)
{
    // 0 validate arguments
    if (key.empty() || valSize <= 0 || blockAddrs.empty() || blockSize <= 0) {
        FALCON_LOG(LOG_ERROR) << "[PUT] Arguments invalid with key=" << key
                              << ", valSize=" << valSize
                              << ", blockSize=" << blockSize
                              << ", len of blockAddrs=" << blockAddrs.size();
        return -1;
    }

    // 1 construct metadata information about kvcache and each slice
    // 1.1 calculate slice_num
    uint32_t tmp_slice_num = (valSize + SLICE_SIZE - 1) / SLICE_SIZE;
    if (tmp_slice_num > UINT16_MAX) {
        FALCON_LOG(LOG_ERROR) << "[PUT] Total slice number is greater than UINT16_MAX";
        return -1;
    }
    uint16_t slice_num = static_cast<uint16_t>(tmp_slice_num);

    FormData_kv_index kvmetainfo;
    kvmetainfo.key = key;
    kvmetainfo.valueLen = valSize;
    kvmetainfo.sliceNum = slice_num;
    kvmetainfo.slicesMeta.clear();

    std::vector<uint64_t> slice_keys = key_gen.getKeys(slice_num);
    if (slice_keys.empty()) {
        FALCON_LOG(LOG_ERROR) << "[PUT] None of slice keys are generated";
        return -1;
    }

    uint64_t cur_slice_key = -1;
    uint64_t offset = 0;
    uint16_t cur_slice_id = 0;
    while (offset < valSize) {
        // get key for current slice
        cur_slice_key = slice_keys[cur_slice_id];

        uint64_t cur_slice_size = (offset + SLICE_SIZE < valSize) ? SLICE_SIZE : (valSize - offset);

        // get location for current slice
        std::unique_ptr<ObjLocation> location = std::make_unique<ObjLocation>();
        CResult status = BioCalcLocation(tenantId, cur_slice_key, location.get()); // 同步
        if (status != CResult::RET_CACHE_OK) {
            FALCON_LOG(LOG_ERROR) << "[PUT] BioCalcLocation failed with returned status " << status;
            return -1;
        }

        // construct current slice_meta and put into kvmetainfo
        kvmetainfo.slicesMeta.emplace_back(cur_slice_key, cur_slice_size, location->location[0]);

        offset += cur_slice_size;
        ++cur_slice_id;
    }

    // 2 gather all block data
    auto allData = std::make_shared<std::vector<uint8_t>>(valSize);
    uint32_t copied = 0;
    uint8_t* dst = allData->data();

    for (const void* blkAddr : blockAddrs) {
        if (copied >= valSize) break;

        const uint8_t* src = static_cast<const uint8_t*>(blkAddr);
        size_t copySize = std::min(blockSize, valSize - copied);

        std::memcpy(dst + copied, src, copySize);
        copied += copySize;
    }

    if (copied != valSize) {
        FALCON_LOG(LOG_ERROR) << "Data copy incomplete. Copied: " << copied << ", Expected: " << valSize;
        return -1;
    }

    // 2 start putting to bio
    const char* value_ptr = reinterpret_cast<const char*>(allData->data());
    // call PutSlices, wait until all put finished
    bool ret = putSlices(kvmetainfo, value_ptr, ioConcurrency_, KVServer::putCallback);
    if (!ret) {
        FALCON_LOG(LOG_ERROR) << "[PUT] putSlices failed";
        return -1;
    }

    // 3 persist kvmetainfo into pg through metaclient
    // retry several times if failed
    ret = putMeta(kvmetainfo);
    if (!ret) {
        // delete already put data
        ret = deleteSlices(kvmetainfo);
        if (!ret) {
            FALCON_LOG(LOG_ERROR) << "[PUT] deleteSlices failed";
            return -1;
        }

        FALCON_LOG(LOG_ERROR) << "[PUT] putMeta failed";
        return -1;
    }

    return 0;


}

bool KVServer::putSlices(FormData_kv_index &kvMetaInfo,
                         const char *valPtr,
                         uint16_t sem_limit,
                         BioLoadCallback putcallback)
{
    uint16_t num_slices = kvMetaInfo.sliceNum;

    FALCON_LOG(LOG_INFO) << "Starting async put for " << num_slices << " slices.";

    // 1. create semaphore and initialized as sem_limit
    auto concurrency_sem = std::make_shared<Semaphore>(sem_limit);
    // define pending_count, cv, mutex for put
    auto sync_ctx = std::make_shared<SyncContext>();

    // 2. prepare for context and callback for bioAsyncPut
    auto ioctx_pool = std::make_shared<std::vector<AsyncPutContext>>(num_slices);

    // 3. parallel async bioput
    for (uint16_t i = 0; i < num_slices; ++i) {
        // 3.1 get context
        AsyncPutContext *ctx = &(*ioctx_pool)[i];
        ctx->concurrency_sem = concurrency_sem;
        ctx->sync_ctx = sync_ctx;

        // 3.2 wait for available concurrency
        concurrency_sem->wait();

        // 3.3 pending_count_put ++
        sync_ctx->pending_count.fetch_add(1, std::memory_order_acq_rel);

        // 3.4 async put
        uint64_t slice_key = kvMetaInfo.slicesMeta[i].sliceKey;
        ctx->slice_key = slice_key;
        std::string slice_key_str = std::to_string(slice_key);
        const char *slice_value_ptr = valPtr + (i * SLICE_SIZE);
        uint32_t length = kvMetaInfo.slicesMeta[i].size;
        ObjLocation location = {{kvMetaInfo.slicesMeta[i].location}};

        FALCON_LOG(LOG_INFO) << "Async put for slice key=" << slice_key_str << " started.";

        CResult ret =
            BioAsyncPut(tenantId, slice_key_str.c_str(), slice_value_ptr, (uint64_t)length, location, putcallback, ctx);
        if (ret != CResult::RET_CACHE_OK) {
            sync_ctx->pending_count.fetch_sub(1, std::memory_order_acq_rel); // task submit failed
            concurrency_sem->post();
            return false;
        } // else start asyncput for the current slice
    }

    // 4. wait until all async put finished
    {
        std::unique_lock<std::mutex> lock(sync_ctx->wait_mutex);
        sync_ctx->wait_cv.wait(lock, [&sync_ctx]() { return (sync_ctx->pending_count.load() == 0); });
    }

    // 5. check the async callback func result
    for (uint16_t i = 0; i < ioctx_pool->size(); ++i){
        if ((*ioctx_pool)[i].result != CResult::RET_CACHE_OK){
            FALCON_LOG(LOG_ERROR) << "Async put for slice " << i << " is failed.";
            return false;
        }
    }

    FALCON_LOG(LOG_INFO) << "Async put completed for all slices.";

    return true;
}

bool KVServer::putMeta(FormData_kv_index &kvmetainfo)
{
    int base_delay_ms = 100;
    int max_retries = 3;
    for (int attemp = 0; attemp < max_retries; ++attemp) {
        try {
            int ret = FalconPut(kvmetainfo);
            if (ret == 0) {
                FALCON_LOG(LOG_INFO) << "Metadata successfully persisted.";
                return true;
            }
        } catch (...) {
            // calculate delay retry time interval
            int delay_ms = std::min(1000, base_delay_ms * (1 << attemp));
            std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));

            FALCON_LOG(LOG_WARNING) << "Retrying metadata put, attempt=" << attemp + 1;
        }
    }
    FALCON_LOG(LOG_ERROR) << "Failed to persist metadata after " << max_retries << " times retries.";
    return false;
}

int32_t KVServer::Get(const std::string &key, std::vector<void*> blockAddrs, uint32_t blockSize)
{
    // 0. validate argument
    uint32_t numBlocks = blockAddrs.size();

    if (key.empty() || blockAddrs.empty() || blockSize <= 0) {
        FALCON_LOG(LOG_ERROR) << "[GET] Arguments invalid with key=" << key
                        << ", blockSize=" << blockSize
                        << ", len of blockAddrs=" << numBlocks;
        return -1;
    }
    
    for (uint32_t i = 0; i < numBlocks; ++i) {
        if (blockAddrs[i] == nullptr) {
            FALCON_LOG(LOG_ERROR) << "[GET] blockAddrs[" << i << "] is nullptr";
            return -1;
        }
        // FALCON_LOG(LOG_INFO) << "blockAddrs[" << i << "] is " << blockAddrs[i];
    }

    // 1. get metakvinfo
    FormData_kv_index kvmetainfo;
    kvmetainfo.key = key;
    bool ret = getMeta(kvmetainfo);
    if (!ret) {
        FALCON_LOG(LOG_ERROR) << "[GET] getMeta failed";
        return -1;
    }

    // 2. async get
    // 2.1 allocate temp buffer used by getSlices
    uint32_t valTotalSize = 0;
    std::vector<char> tempBuffer(kvmetainfo.valueLen);
    ret = getSlices(kvmetainfo, tempBuffer.data(), valTotalSize, ioConcurrency_, KVServer::getCallback);
    if (!ret) {
        FALCON_LOG(LOG_ERROR) << "[GET] getSlices failed";
        return -1;
    }
    // 2.2 copy value from temp buffer to blockAddrs
    char* srcPtr = tempBuffer.data();
    uint32_t copied = 0;
    uint32_t bytesToCopy = 0;

    for (uint32_t i = 0; i < numBlocks; ++i) {
        if (i == (numBlocks-1)) {
            bytesToCopy = kvmetainfo.valueLen - copied;
        }else {
            bytesToCopy = blockSize;
        }
        std::memcpy(blockAddrs[i], srcPtr, bytesToCopy);
	// FALCON_LOG(LOG_INFO) << "copy from srcPtr=" << static_cast<void*>(srcPtr) << " to blockAddrs[" << i << "]=" << blockAddrs[i] <<", bytes to copy=" << bytesToCopy;
        srcPtr += bytesToCopy;
        copied += bytesToCopy;
    }

    return 0;
}

bool KVServer::getSlices(FormData_kv_index &kvMetaInfo,
                         char *valPtr,
                         uint32_t &valTotalSize,
                         uint16_t sem_limit,
                         BioGetCallbackFunc getcallback)
{
    uint16_t num_slices = kvMetaInfo.sliceNum;

    FALCON_LOG(LOG_INFO) << "Starting async get for " << num_slices << " slices.";

    // 1. create semaphore and initialized as sem_limit
    auto concurrency_sem = std::make_shared<Semaphore>(sem_limit);
    // define pending_count, cv, mutex for get
    auto sync_ctx = std::make_shared<SyncContext>();

    // set iocontext pool
    auto ioctx_pool = std::make_shared<std::vector<AsyncGetContext>>(num_slices);

    for (uint16_t i = 0; i < num_slices; ++i) {
        AsyncGetContext *ctx = &(*ioctx_pool)[i];
        ctx->concurrency_sem = concurrency_sem;
        ctx->sync_ctx = sync_ctx;

        concurrency_sem->wait();

        sync_ctx->pending_count.fetch_add(1, std::memory_order_acq_rel);

        uint64_t slice_key = kvMetaInfo.slicesMeta[i].sliceKey;
        ctx->slice_key = slice_key;
        std::string slice_key_str = std::to_string(slice_key);
        uint32_t length = kvMetaInfo.slicesMeta[i].size;
        ObjLocation location = {{kvMetaInfo.slicesMeta[i].location}};
        char *slice_value_ptr = valPtr + (i * SLICE_SIZE);

        FALCON_LOG(LOG_INFO) << "Async get for slice key=" << slice_key_str << " started.";

        CResult ret =
            BioAsyncGet(tenantId, slice_key_str.c_str(), 0, length, location, slice_value_ptr, getcallback, ctx);
        if (ret != CResult::RET_CACHE_OK) {
            sync_ctx->pending_count.fetch_sub(1, std::memory_order_acq_rel); // task submit failed
            concurrency_sem->post();
            return false;
        }
    }

    {
        std::unique_lock<std::mutex> lock(sync_ctx->wait_mutex);
        sync_ctx->wait_cv.wait(lock, [&sync_ctx]() { return (sync_ctx->pending_count.load() == 0); });
    }

    for (auto &ctx : *ioctx_pool) {
	if (ctx.result != CResult::RET_CACHE_OK){
            FALCON_LOG(LOG_ERROR) << "Async get for at least one slice failed.";
            return false;
        }
        valTotalSize += ctx.sliceSize;
    }
    if (valTotalSize != kvMetaInfo.valueLen) {
        FALCON_LOG(LOG_ERROR) << "Total got slices size is not equal to expected. Expected=" << kvMetaInfo.valueLen
                              << ", got=" << valTotalSize;
        return false; // total got slices size is not equal to expected
    }

    FALCON_LOG(LOG_INFO) << "Async get completed for all slices. Total size=" << valTotalSize;

    return true;
}

bool KVServer::getMeta(FormData_kv_index &kvMetaInfo)
{
    int base_delay_ms = 100;
    int max_retries = 3;
    for (int attemp = 0; attemp < max_retries; ++attemp) {
        try {
            int ret = FalconGet(kvMetaInfo);
            if (ret == 0) {
                FALCON_LOG(LOG_INFO) << "Metadata successfully retrieved.";
                return true;
            }
        } catch (...) {
            // calculate delay retry time interval
            int delay_ms = std::min(1000, base_delay_ms * (1 << attemp));
            std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));

            FALCON_LOG(LOG_WARNING) << "Retrying metadata get, attempt=" << attemp + 1;
        }
    }
    FALCON_LOG(LOG_ERROR) << "Failed to retrieve metadata after " << max_retries << " times retries.";
    return false;
}

int32_t KVServer::GetValueLen(const std::string &key, uint32_t &valTotalSize)
{
    // validate argument
    if (key.empty()){
        FALCON_LOG(LOG_ERROR) << "[GetValueLen] Arguments invalid with key=" << key;
        return -1;
    }

    FALCON_LOG(LOG_INFO) << "Starting get value length for key=" << key;

    // get meta info
    FormData_kv_index kvmetainfo;
    kvmetainfo.key = key;
    bool ret = getMeta(kvmetainfo);
    if (!ret) {
        FALCON_LOG(LOG_ERROR) << "[GetValueLen] getMeta failed";
        return -1;
    }

    valTotalSize = kvmetainfo.valueLen;

    return 0;
}

int32_t KVServer::Delete(std::string &key)
{
    // validate argument
    if (key.empty()) {
        FALCON_LOG(LOG_ERROR) << "[Delete] Arguments invalid with key=" << key;
        return -1;
    }

    FALCON_LOG(LOG_INFO) << "Starting delete operation for key=" << key;

    // get meta info
    FormData_kv_index kvmetainfo;
    kvmetainfo.key = key;
    bool ret = getMeta(kvmetainfo);
    if (!ret) {
        FALCON_LOG(LOG_ERROR) << "[Delete] getMeta failed";
        return -1;
    }

    // delete data -- sync
    ret = deleteSlices(kvmetainfo);
    if (!ret) {
        FALCON_LOG(LOG_ERROR) << "[Delete] deleteSlices failed";
        return -1;
    }

    // delete meta data
    ret = deleteMeta(key);
    if (!ret) {
        FALCON_LOG(LOG_ERROR) << "[Delete] deleteMeta failed";
        return -1;
    }

    FALCON_LOG(LOG_INFO) << "Delete operation for key=" << key << " completed successfully.";

    return 0;
}

bool KVServer::deleteSlices(FormData_kv_index &kvMetaInfo)
{
    uint16_t sliceNum = kvMetaInfo.sliceNum;
    FALCON_LOG(LOG_INFO) << "Starting delete operation for " << sliceNum << " slices.";

    for (uint16_t i = 0; i < sliceNum; ++i) {
        uint64_t slice_key = kvMetaInfo.slicesMeta[i].sliceKey;
        std::string slice_key_str = std::to_string(slice_key);
        ObjLocation location = {{kvMetaInfo.slicesMeta[i].location}};

        FALCON_LOG(LOG_INFO) << "Attempting to delete slice with key=" << slice_key_str;

        int base_delay_ms = 100;
        int max_tries = 3;
        for (int attemp = 0; attemp < max_tries; ++attemp) {
            try {
                CResult ret = BioDelete(tenantId, slice_key_str.c_str(), location);
                if (ret == CResult::RET_CACHE_OK) {
                    FALCON_LOG(LOG_INFO) << "Slice with key=" << slice_key_str << " deleted successfully.";
                    break;
                } else {
                    if (attemp == max_tries - 1) {
                        FALCON_LOG(LOG_ERROR) << "Failed to delete slice with key=" << slice_key_str << " after "
                                              << max_tries << " attempts.";
                        return false;
                    }
                }
            } catch (...) {
                // calculate delay retry time interval
                int delay_ms = std::min(1000, base_delay_ms * (1 << attemp));
                std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));

                FALCON_LOG(LOG_WARNING) << "Retrying slice data delete for key=" << slice_key_str
                                        << ", attempt=" << attemp + 1;
            }
        }
    }
    return true;
}

bool KVServer::deleteMeta(std::string &key)
{
    int base_delay_ms = 100;
    int max_retries = 3;
    for (int attemp = 0; attemp < max_retries; ++attemp) {
        try {
            int ret = FalconDelete(key);
            if (ret == 0) {
                return true;
            }
        } catch (...) {
            // calculate delay retry time interval
            int delay_ms = std::min(1000, base_delay_ms * (1 << attemp));
            std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));

            FALCON_LOG(LOG_WARNING) << "Retrying metadata delete, attempt=" << attemp + 1;
        }
    }
    FALCON_LOG(LOG_ERROR) << "Failed to delete metadata after " << max_retries << " times retries.";
    return false;
}
