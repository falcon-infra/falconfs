//
// Created by w00573979 on 2025/9/11.
//
#include <fcntl.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <syscall.h>
#include <unistd.h>
#include <linux/version.h>
#include <cerrno>
#include <cstring>
#include <thread>
#include <vector>
#include "utils.h"
#include "kv_ipc_message.h"
#include "log/logging.h"
#include "memory_pool.h"

static constexpr uint64_t DEFAULT_PAGE_SIZE = 4096UL;

void MultiReservePhysicalPage(uint8_t *mappedAddress, uint64_t size)
{
    // auto cpuCores = std::thread::hardware_concurrency();
    // auto taskThreads = cpuCores / SPLIT_OF_CPU_CORES_FLAG;
    // TODO 暂时设置16线程 踩物理页
    uint8_t taskThreads = 16;
    /* reserver physical page task */
    auto reserveTask = [](uint64_t reserveSize, uint8_t *startPos) {
        uint64_t setLength = 0UL;
        while (setLength < reserveSize) {
            *startPos = 0;
            setLength += DEFAULT_PAGE_SIZE;
            startPos += DEFAULT_PAGE_SIZE;
        }
    };

    if (size < DEFAULT_PAGE_SIZE * taskThreads || taskThreads == 0) {
        reserveTask(size, mappedAddress);
        return;
    }

    auto completePageCount = size / DEFAULT_PAGE_SIZE;
    auto everyReservePages = completePageCount / taskThreads;
    auto everyReserveSize = everyReservePages * DEFAULT_PAGE_SIZE;

    std::vector<std::thread> threadPool;
    for (uint8_t i = 0; i < taskThreads; ++i) {
        auto startPos = mappedAddress + i * everyReserveSize;
        if (i == taskThreads - 1) {
            everyReserveSize = size - i * everyReserveSize;
        }
        threadPool.emplace_back(reserveTask, everyReserveSize, startPos);
    }

    /* wait all reserve */
    for (auto &item : threadPool) {
        item.join();
    }
}

MemoryPool::MemoryPool() noexcept
    : ResourcePool{},
      blockHead{nullptr}
{
}

MemoryPool::MemoryPool(const Configure &config) noexcept
    : ResourcePool{config},
      blockHead{nullptr}
{
}

MemoryPool::~MemoryPool() noexcept = default;

int MemoryPool::Initialize() noexcept
{
    FALCON_LOG(LOG_INFO) << "Start to init shared memory";

    if (attribute.totalSize == 0UL || attribute.blockSize == 0UL || (attribute.totalSize % attribute.blockSize) != 0) {
        FALCON_LOG(LOG_ERROR) << "input attribute invalid";
        return -1;
    }
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

    attribute.address = static_cast<uint8_t *>(mappedAddress);
    attribute.fd = fd;
    MakeBlocksInPool();

    FALCON_LOG(LOG_INFO) << "Finished to reserve physical memory from OS";

    return 0;
}

void MemoryPool::Destroy() noexcept
{
    if (attribute.address == nullptr) {
        return;
    }
    munmap(attribute.address, attribute.totalSize);
    close(attribute.fd);
    attribute.address = nullptr;
    attribute.fd = -1;
    blockHead = nullptr;
}

void MemoryPool::MakeBlocksInPool() noexcept
{
    auto blockCount = attribute.totalSize / attribute.blockSize;
    blockHead = static_cast<LinkedBlock *>(static_cast<void *>(attribute.address));
    auto last = blockHead;
    for (auto i = 1UL; i < blockCount; i++) {
        auto pos = attribute.address + i * attribute.blockSize;
        auto block = static_cast<LinkedBlock *>(static_cast<void *>(pos));
        last->next = block;
        last = block;
    }
    last->next = nullptr;
}

bool MemoryPool::AllocateImpl(uint64_t &blockId) noexcept
{
    if (attribute.address == nullptr) {
        return false;
    }
    lock.Lock();
    if (blockHead == nullptr) {
        lock.Unlock();
        return false;
    }

    auto tmpBlock = blockHead;
    blockHead = tmpBlock->next;
    lock.Unlock();

    blockId = (static_cast<uint8_t *>(static_cast<void *>(tmpBlock)) - attribute.address) / attribute.blockSize;

    return true;
}

bool MemoryPool::ReleaseImpl(uint64_t blockId) noexcept
{
    if (attribute.address == nullptr) {
        return false;
    }
    auto blockOffset = blockId * attribute.blockSize;
    if (blockOffset >= attribute.totalSize) {
        return false;
    }

    auto blockNode = static_cast<LinkedBlock *>(static_cast<void *>(attribute.address + blockOffset));
    lock.Lock();
    blockNode->next = blockHead;
    blockHead = blockNode;
    lock.Unlock();
    return true;
}