//
// Created by w00573979 on 2025/9/11.
//
#include "resource_pool.h"

ResourcePool::ResourcePool(const Configure &config) noexcept
    : attribute{config},
      blockCount{attribute.totalSize / attribute.blockSize},
      allocateBlockCount {0UL}
{
}

ResourcePool::~ResourcePool() noexcept = default;

bool ResourcePool::Allocate(uint64_t &blockId) noexcept
{
    auto ret = AllocateImpl(blockId);
    if (ret) {
        allocateBlockCount.fetch_add(1UL);
    }
    return ret;
}

void ResourcePool::Release(uint64_t blockId) noexcept
{
    auto ret = ReleaseImpl(blockId);
    if (ret) {
        allocateBlockCount.fetch_sub(1UL);
    }
    return;
}

bool ResourcePool::BlockOffset(uint64_t blockId, uint64_t &offset) const noexcept
{
    if (blockId >= blockCount) {
        return false;
    }
    offset = blockId * attribute.blockSize;
    return true;
}