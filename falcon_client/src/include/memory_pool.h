//
// Created by w00573979 on 2025/9/11.
//

#ifndef FALCONFS_MEMORY_POOL_H
#define FALCONFS_MEMORY_POOL_H

#include "resource_pool.h"
#include "kv_utils.h"

class MemoryPool : public ResourcePool {
public:
    explicit MemoryPool() noexcept;
    explicit MemoryPool(const Configure &config) noexcept;
    ~MemoryPool() noexcept override;

    int Initialize() noexcept override;
    void Destroy() noexcept override;

protected:
    bool AllocateImpl(uint64_t &blockId) noexcept override;
    bool ReleaseImpl(uint64_t blockId) noexcept override;

private:
    void MakeBlocksInPool() noexcept;
    // int CreateSharedMemory(const std::string &path) noexcept;

private:
    struct LinkedBlock{
        LinkedBlock *next{ nullptr };
    };
    SpinLock lock;
    LinkedBlock *blockHead{ nullptr };
};


#endif // FALCONFS_MEMORY_POOL_H
