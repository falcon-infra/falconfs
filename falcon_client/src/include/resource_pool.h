#ifndef FALCONFS_RESOURCE_POOL_H
#define FALCONFS_RESOURCE_POOL_H

#include <atomic>
#include <cstdint>
#include <string>

class ResourcePool {
  public:
    struct Configure
    {
        std::string name;
        uint64_t totalSize;
        uint64_t blockSize;
        Configure() noexcept
            : name{},
              totalSize{0UL},
              blockSize{0UL}
        {
        }
        Configure(std::string nm, uint64_t ts, uint64_t bs) noexcept
            : name{std::move(nm)},
              totalSize{ts},
              blockSize{bs}
        {
        }
    };

    struct Attribute : public Configure
    {
        int fd;
        uint8_t *address;

        Attribute() : Configure{}, fd{-1}, address{nullptr}
        {

        }
        Attribute(const Configure &config)
            : Configure{config},
              fd{-1},
              address{nullptr}
        {
        }
    };

  public:
    ResourcePool() = default;
    ResourcePool(const Configure &config) noexcept;
    virtual ~ResourcePool() noexcept;

    virtual int Initialize() noexcept = 0;
    virtual void Destroy() noexcept = 0;
    bool Allocate(uint64_t &blockId) noexcept;
    void Release(uint64_t blockId) noexcept;

    bool BlockOffset(uint64_t blockId, uint64_t &offset) const noexcept;

    const Attribute &GetAttribute() const noexcept { return attribute; }

    uint64_t GetAllocateBlockCount() const noexcept { return allocateBlockCount.load(); }

    uint64_t GetRemainBlockCount() const noexcept { return blockCount - allocateBlockCount.load(); }

  protected:
    virtual bool AllocateImpl(uint64_t &blockId) noexcept = 0;
    virtual bool ReleaseImpl(uint64_t blockId) noexcept = 0;

  protected:
    Attribute attribute;
    uint64_t blockCount { 0UL};
    std::atomic<uint64_t> allocateBlockCount {0UL};
};

#endif // FALCONFS_RESOURCE_POOL_H