#include "slice_key_gen.h"

#include "falcon_kv_meta.h"

#include "log/logging.h"

SliceKeyGenerator &SliceKeyGenerator::getInstance()
{
    static SliceKeyGenerator instance;
    return instance;
}

SliceKeyGenerator::SliceKeyGenerator()
{
    batch_size = 4096; // TODO: get from config

    // 双 buffer 填充并初始化
    buffers[0] = fetchNow(batch_size);
    buffers[1] = fetchNow(batch_size);

    if (!buffers[0].valid || !buffers[1].valid) {
        FALCON_LOG(LOG_ERROR) << "Failed to initialize the double buffers of slice keys";
        throw std::runtime_error("Failed to initialize the double buffers of slice keys");
    }

    cur_buf_idx = 0; // 先从第 0 个 buffer 开始使用

    // start prefetch thread
    prefetchThread_ = std::thread(&SliceKeyGenerator::backgroundPrefetch, this, batch_size);
    FALCON_LOG(LOG_INFO) << "SliceKeyGenerator initialized with batch size=" << batch_size;
}

KeyRange SliceKeyGenerator::fetchNow(uint32_t bs)
{
    if (bs <= 0)
        return KeyRange();

    const int max_retries = 3;
    const int base_delayms = 10;

    KeyRange result;

    for (int attemp = 0; attemp < max_retries; ++attemp) {
        try {
            std::pair<uint64_t, uint64_t> ret = sliceKeyRangeFetch(bs);

            result.start = ret.first;
            result.end = ret.second;
            result.valid = (ret.first <= ret.second);

            if (result.valid) {
                FALCON_LOG(LOG_INFO) << "Successfully fetched key range: start=" << result.start
                                     << ", end=" << result.end << ", attempt=" << attemp + 1;
                return result;
            }
        } catch (...) {
            // calculate delay retry time interval
            int delay_ms = std::min(1000, base_delayms * (1 << attemp));
            std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));

            FALCON_LOG(LOG_WARNING) << "Retrying remote key range fetch, attempt=" << attemp + 1;
        }
    }
    FALCON_LOG(LOG_ERROR) << "Failed to fetch key range after " << max_retries << " attempts";
    return result; // valid = false
}

void SliceKeyGenerator::backgroundPrefetch(uint32_t bs)
{
    FALCON_LOG(LOG_INFO) << "Background slice key prefetch thread started with batch size=" << bs;

    while (true) {
        // 获取 mtx 锁
        UniqueLock lock(mtx);

        // 等待唤醒或者退出
        cv.wait(lock, [this]() { return pending_fetch.load() || should_stop.load(); });

        if (should_stop)
            FALCON_LOG(LOG_INFO) << "Background prefetch thread exiting.";
        break;

        int next_buf_idx = 1 - cur_buf_idx;

        buffers[next_buf_idx] = fetchNow(bs);
        if (!buffers[next_buf_idx].valid) {
            FALCON_LOG(LOG_ERROR) << "Failed to fetch batch of slice keys in backgroundPrefetch";
            throw std::runtime_error("Failed to fetch batch of slice keys in backgroundPrefetch");
        }

        pending_fetch = false;
        FALCON_LOG(LOG_INFO) << "Background prefetch completed for buffer index=" << next_buf_idx;
    }
}

void SliceKeyGenerator::triggerPrefetch(uint32_t bs)
{

    if (pending_fetch.load()) {
        FALCON_LOG(LOG_INFO) << "Slice key prefetching is running.";
        return; // 已经在预取
    }

    int next_buf_idx = 1 - cur_buf_idx;
    if (buffers[next_buf_idx].empty()) {
        pending_fetch = true;
        cv.notify_one(); // 唤醒后台线程
        FALCON_LOG(LOG_INFO) << "Background prefetch thread is triggered for buffer index=" << next_buf_idx;
    }
}

std::vector<uint64_t> SliceKeyGenerator::getKeys(uint16_t count)
{
    if (count <= 0)
        return std::vector<uint64_t>{};

    // 加锁避免多个线程同时调用 getKeys 函数
    LockGuard lock(mtx);

    std::vector<uint64_t> result;
    result.reserve(count);
    uint16_t remaining = count;

    FALCON_LOG(LOG_INFO) << "Starting to get " << remaining << " slice keys.";

    while (remaining > 0) {
        KeyRange &curbuf = buffers[cur_buf_idx];

        // 假如当前的 buffer 已经空
        if (curbuf.empty()) {
            FALCON_LOG(LOG_INFO) << "Current buffer is empty for buffers-" << cur_buf_idx;
            // 如果另一个 buffer 可以 take 一些 slice keys
            int next_buf_idx = 1 - cur_buf_idx;
            if (buffers[next_buf_idx].valid && (!buffers[next_buf_idx].empty())) {
                FALCON_LOG(LOG_INFO) << "Next buffer has slice keys for buffers-" << next_buf_idx;
                // 把 cur_buf_idx 变成 next_buf_idx
                cur_buf_idx = next_buf_idx;
                // 唤醒后台线程开始填充已经空的 buffer
                triggerPrefetch(batch_size);
            } else {
                FALCON_LOG(LOG_WARNING) << "Both buffers are empty.";
                // 否则（说明两个buffer都没有数据了）
                // 直接现场从远程把缺少的都一次性取过来
                KeyRange remain_range = fetchNow(remaining);
                if (!remain_range.valid) {
                    FALCON_LOG(LOG_ERROR) << "Failed to fetch remaining keys";
                    throw std::runtime_error("Failed to fetch remaining keys");
                }
                for (uint16_t i = 0; i < remaining; ++i) {
                    result.push_back(remain_range.start + i);
                }
                FALCON_LOG(LOG_INFO) << "Successfully fetched remaining " << remaining << " slice keys.";
                return result;
            }
        } else {
            // 计算可以 take away 的 slice keys 数量
            uint16_t take_num = std::min(static_cast<uint64_t>(remaining), curbuf.end - curbuf.start + 1);
            for (uint16_t i = 0; i < take_num; ++i) {
                result.push_back(curbuf.start + i);
            }
            curbuf.start += take_num;
            remaining -= take_num;
            FALCON_LOG(LOG_INFO) << "Fetched " << take_num << " slice keys, remaining=" << remaining;
        }
    }
    FALCON_LOG(LOG_INFO) << "Successfully fetched all " << count << " slice keys.";
    return result;
}

SliceKeyGenerator::~SliceKeyGenerator()
{
    should_stop = true;
    cv.notify_all();
    if (prefetchThread_.joinable()) {
        prefetchThread_.join();
    }
    FALCON_LOG(LOG_INFO) << "SliceKeyGenerator destroyed.";
}