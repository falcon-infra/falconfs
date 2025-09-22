#ifndef FALCONFS_KV_UTILS_H
#define FALCONFS_KV_UTILS_H

#include <pthread.h>
#include <stdio.h>
#include <syscall.h>
#include <unistd.h>
#include <atomic>
#include <mutex>
#include <vector>


#ifndef ROUND_UP
#define ROUND_UP(x, align) (((x) + (align)-1) & ~((align)-1))
#endif

#ifndef ROUND_DOWN
#define ROUND_DOWN(x, align) ((x) & ~((align)-1))
#endif

#ifndef LIKELY
#define LIKELY(x) (__builtin_expect(!!(x), 1) != 0)
#endif

#ifndef UNLIKELY
#define UNLIKELY(x) (__builtin_expect(!!(x), 0) != 0)
#endif

void Print(int level, const char *msg);

inline void SafeCloseFd(int32_t &fd)
{
    auto tmpFd = fd;
    if (UNLIKELY(tmpFd < 0)) {
        return;
    }
    if (__sync_bool_compare_and_swap(&fd, tmpFd, -1)) {
        close(tmpFd);
    }
}

class SpinLock {
  public:
    SpinLock() = default;
    ~SpinLock() = default;
    SpinLock(const SpinLock &) = delete;
    SpinLock(SpinLock &&) = delete;
    SpinLock &operator=(const SpinLock &) = delete;
    SpinLock &operator=(SpinLock &&) = delete;

    inline void TryLock() { mFlag.test_and_set(std::memory_order_acquire); }
    inline void Lock()
    {
        while (mFlag.test_and_set(std::memory_order_acquire)) {
        }
    }

    inline void Unlock() { mFlag.clear(std::memory_order_release); }

  private:
    std::atomic_flag mFlag = ATOMIC_FLAG_INIT;
};


#endif // FALCONFS_KV_UTILS_H
