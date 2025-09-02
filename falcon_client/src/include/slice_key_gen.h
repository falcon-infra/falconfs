#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <vector>

struct KeyRange
{
    uint64_t start;
    uint64_t end;
    bool valid; // 从远程获取数据是否正确

    KeyRange()
        : start(0),
          end(-1),
          valid(false)
    {
    }

    // 是否有可以被 take 的 slickeys
    bool empty()
    {
        if (start > end || !valid) {
            return true;
        }
        return false;
    }
};

class SliceKeyGenerator {
  private:
    uint32_t batch_size; // 单次从 pg 预取的总 slicekeys 数量
    KeyRange buffers[2]; // 双缓冲，用于异步加载 slicekeys
    int cur_buf_idx;     // 当前在使用的 buffer id, be 0 or 1

    SliceKeyGenerator();
    ~SliceKeyGenerator();

    KeyRange fetchNow(uint32_t bs); // 同步获取 bs 个 slicekeys

    void backgroundPrefetch(uint32_t bs); // 异步预取填充到另一个 buffer
    void triggerPrefetch(uint32_t bs);    // 唤醒预取线程

    std::thread prefetchThread_;
    std::mutex mtx; // 避免 getKeys 被同时调用 TODO: 更换 mutex
    std::condition_variable cv;  // 阻塞和唤醒预取线程中使用
    std::atomic<bool> should_stop;   // 任务结束时也没必要继续阻塞
    std::atomic<bool> pending_fetch; // 正在等待预取结束标志，避免重复预取

  public:
    static SliceKeyGenerator &getInstance(); // 单例对象获取
    SliceKeyGenerator(const SliceKeyGenerator &) = delete;
    SliceKeyGenerator &operator=(const SliceKeyGenerator &) = delete;

    std::vector<uint64_t> getKeys(uint16_t count); // 获取唯一的 count 个 slice keys，线程安全
};