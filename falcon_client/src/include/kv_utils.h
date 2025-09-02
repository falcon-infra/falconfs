#ifndef FALCONFS_KV_UTILS_H
#define FALCONFS_KV_UTILS_H

#include <syscall.h>
#include <unistd.h>
#include <stdio.h>

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


// TODO 后续适配Falcon Log
void Print(int level, const char *msg);
void ClientPrint(int level, const char *msg);

void MultiReservePhysicalPage(uint8_t *mappedAddress, uint64_t size);

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
#endif  // FALCONFS_KV_UTILS_H
