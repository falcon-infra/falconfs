#include "vllm_kv_cache/src/store/dram_pool.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <sstream>
#include <stdexcept>

namespace falconfs::kv {

namespace {

std::string ShmOpenPath(const std::string& name) {
    if (!name.empty() && name[0] == '/') {
        return name;
    }
    return "/" + name;
}

void* MapPosixShm(const std::string& segment_name, std::size_t bytes, int* out_fd) {
    const std::string path = ShmOpenPath(segment_name);
    int fd = ::shm_open(path.c_str(), O_CREAT | O_RDWR, 0666);
    if (fd < 0) {
        std::ostringstream o;
        o << "DramPool: shm_open failed for " << path << " errno=" << errno;
        throw std::runtime_error(o.str());
    }
    if (::ftruncate(fd, static_cast<off_t>(bytes)) != 0) {
        int e = errno;
        ::close(fd);
        std::ostringstream o;
        o << "DramPool: ftruncate failed errno=" << e;
        throw std::runtime_error(o.str());
    }
    void* addr =
        ::mmap(nullptr, bytes, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (addr == MAP_FAILED) {
        int e = errno;
        ::close(fd);
        std::ostringstream o;
        o << "DramPool: mmap MAP_SHARED failed errno=" << e;
        throw std::runtime_error(o.str());
    }
    *out_fd = fd;
    return addr;
}

void* MapAnonymous(std::size_t bytes, bool try_huge_pages, bool* used_huge_pages) {
    int flags = MAP_PRIVATE | MAP_ANONYMOUS;
    *used_huge_pages = false;

#ifdef MAP_HUGETLB
    if (try_huge_pages) {
        void* addr = ::mmap(nullptr, bytes, PROT_READ | PROT_WRITE, flags | MAP_HUGETLB, -1, 0);
        if (addr != MAP_FAILED) {
            *used_huge_pages = true;
            return addr;
        }
    }
#endif

    void* addr = ::mmap(nullptr, bytes, PROT_READ | PROT_WRITE, flags, -1, 0);
    return addr;
}

std::size_t ResolveStripeCount() {
    const char* es = std::getenv("FALCON_KV_STORE_DRAM_STRIPES");
    if (es == nullptr || es[0] == '\0') {
        return 64;
    }
    char* end = nullptr;
    long v = std::strtol(es, &end, 10);
    if (end == es || v < 1) {
        return 64;
    }
    if (v > 4096) {
        return 4096;
    }
    return static_cast<std::size_t>(v);
}

}  // namespace

DramPool::DramPool(std::size_t region_bytes,
                   std::size_t block_size,
                   bool try_huge_pages,
                   const std::string& posix_shm_segment_name)
    : region_bytes_(region_bytes),
      block_size_(block_size),
      num_stripes_(ResolveStripeCount()) {
    if (region_bytes == 0 || block_size == 0 || (region_bytes % block_size) != 0) {
        throw std::invalid_argument("DramPool: region_bytes must be a positive multiple of block_size");
    }
    if (!posix_shm_segment_name.empty()) {
        int fd = -1;
        base_ = MapPosixShm(posix_shm_segment_name, region_bytes_, &fd);
        shm_fd_ = fd;
        used_huge_pages_ = false;
    } else {
        base_ = MapAnonymous(region_bytes_, try_huge_pages, &used_huge_pages_);
        if (base_ == MAP_FAILED) {
            base_ = nullptr;
            throw std::runtime_error("DramPool: mmap failed");
        }
        shm_fd_ = -1;
    }
    stripe_locks_ = std::make_unique<std::shared_mutex[]>(num_stripes_);
}

DramPool::~DramPool() { Unmap(); }

void DramPool::Unmap() {
    stripe_locks_.reset();
    if (base_ != nullptr && base_ != MAP_FAILED) {
        ::munmap(base_, region_bytes_);
        base_ = nullptr;
    }
    if (shm_fd_ >= 0) {
        ::close(shm_fd_);
        shm_fd_ = -1;
    }
}

bool DramPool::ValidOffset(int64_t pool_offset) const {
    if (pool_offset < 0) return false;
    auto offset = static_cast<std::size_t>(pool_offset);
    if (offset >= region_bytes_) return false;
    return (offset % block_size_) == 0;
}

std::size_t DramPool::StripeIndex(int64_t pool_offset) const {
    const std::size_t slot = static_cast<std::size_t>(pool_offset) / block_size_;
    return slot % num_stripes_;
}

DramPoolWriteResult DramPool::Write(int64_t pool_offset, const std::string& payload) {
    DramPoolWriteResult result;
    if (!ValidOffset(pool_offset)) {
        return result;
    }
    if (payload.size() > block_size_) {
        return result;
    }
    auto offset = static_cast<std::size_t>(pool_offset);
    if (offset + block_size_ > region_bytes_) {
        return result;
    }
    const std::size_t si = StripeIndex(pool_offset);
    std::unique_lock<std::shared_mutex> g(stripe_locks_[si]);
    char* dst = static_cast<char*>(base_) + offset;
    if (!payload.empty()) {
        std::memcpy(dst, payload.data(), payload.size());
    }
    if (payload.size() < block_size_) {
        std::memset(dst + payload.size(), 0, block_size_ - payload.size());
    }
    result.ok = true;
    result.bytes_written = static_cast<int32_t>(payload.size());
    return result;
}

bool DramPool::Read(int64_t pool_offset, int32_t size, std::string* out) const {
    if (out == nullptr) return false;
    if (!ValidOffset(pool_offset)) return false;
    if (size < 0 || static_cast<std::size_t>(size) > block_size_) return false;
    auto offset = static_cast<std::size_t>(pool_offset);
    if (offset + static_cast<std::size_t>(size) > region_bytes_) return false;
    const std::size_t si = StripeIndex(pool_offset);
    std::shared_lock<std::shared_mutex> g(stripe_locks_[si]);
    const char* src = static_cast<const char*>(base_) + offset;
    out->assign(src, static_cast<std::size_t>(size));
    return true;
}

}  // namespace falconfs::kv
