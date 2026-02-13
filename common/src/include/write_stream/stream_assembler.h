/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#pragma once


#include <unistd.h>
#include <algorithm>
#include <memory>
#include <set>
#include <shared_mutex>
#include <vector>

#include "buffer/falcon_buffer.h"
#include "buffer/mem_pool.h"
#include "connection/falcon_io_client.h"
#include "log/logging.h"

#define FALCON_STORE_STREAM_MAX_SIZE (256 * 1024)

class ExpandableMemory {
  public:
    ExpandableMemory() = default;
    ExpandableMemory(char *buf, size_t bufSize)
    {
        ptr = std::shared_ptr<char>(buf, free);
        size = bufSize;
        capacity = bufSize;
    }
    ExpandableMemory(const ExpandableMemory &) = default;
    ExpandableMemory &operator=(const ExpandableMemory &rhs) = default;
    bool Append(const char *buf, size_t appendSize)
    {
        if (!Expand(appendSize)) {
            return false;
        }
        (void)memcpy(ptr.get() + size, buf, appendSize);
        size += appendSize;
        return true;
    }
    bool Replace(size_t offset, size_t size, const ExpandableMemory &fromReplace)
    {
        if (!Reserve(size + offset)) {
            return false;
        }

        (void)memcpy(ptr.get() + offset, fromReplace.Get().get(), size);
        size = std::max(size, offset + size);
        return true;
    }
    bool Reserve(size_t reservedSize)
    {
        if (reservedSize > capacity) {
            capacity = reservedSize;
            char *tmpBuf = (char *)malloc(capacity);
            if (tmpBuf == nullptr) {
                FALCON_LOG(LOG_ERROR) << "ExpandableMemory:: append() malloc failed";
                return false;
            }

            if (ptr.get()) {
                (void)memcpy(tmpBuf, ptr.get(), size);
            }

            ptr = std::shared_ptr<char>(tmpBuf, free);
        }
        return true;
    }
    bool Expand(size_t appendSize)
    {
        if (appendSize + size > capacity) {
            capacity = appendSize < size ? capacity * 2 : (appendSize + size) * 2;
            char *tmpBuf = (char *)malloc(capacity);
            if (tmpBuf == nullptr) {
                FALCON_LOG(LOG_ERROR) << "ExpandableMemory::append() malloc failed";
                return false;
            }

            if (ptr.get()) {
                (void)memcpy(tmpBuf, ptr.get(), size);
            }
            ptr = std::shared_ptr<char>(tmpBuf, free);
        }
        return true;
    }
    bool Empty() { return size == 0; }
    void Clear() { size = 0; }
    void Clean()
    {
        ptr = nullptr;
        size = 0;
        capacity = 0;
    }
    std::shared_ptr<char> Get() const { return ptr; }
    size_t Size() { return size; }
    std::shared_ptr<char> ptr = nullptr;
    size_t size = 0;
    size_t capacity = 0;
};

class FixMemory {
  public:
    FixMemory() = default;
    ~FixMemory()
    {
        if (mem) {
            writeMemPool.free(mem);
        }
    }
    bool Append(const char *buf, size_t appendSize)
    {
        if (mem == nullptr) {
            mem = (char *)writeMemPool.alloc();
            if (mem == nullptr) {
                FALCON_LOG(LOG_ERROR) << "FixMemory get allocated nullptr";
                return false;
            }
        }
        if (size + appendSize > capacity) {
            FALCON_LOG(LOG_ERROR) << "FixMemory append data too large";
            return false;
        }
        (void)memcpy(mem + size, buf, appendSize);
        size += appendSize;
        return true;
    }
    char *c_str() { return mem; }
    void Clear() { size = 0; }
    char *mem = nullptr;
    size_t size = 0;
    size_t capacity = FALCON_STORE_STREAM_MAX_SIZE;

    static MemPool writeMemPool;
};

class WriteStream {
  public:
    class Slice {
      public:
        Slice(const ExpandableMemory &buf, const size_t size, const off_t offset)
            : buf(buf),
              size(size),
              offset(offset)
        {
        }
        Slice(const Slice &) = default;
        Slice &operator=(const Slice &) = default;
        ExpandableMemory buf;
        mutable size_t size;
        off_t offset;
        bool operator<(const Slice &rhs) const { return offset < rhs.offset; }
    };

    class MergedSlice {
      public:
        void Get(ExpandableMemory &mem) const
        {
            for (auto &slice : slices) {
                mem.Replace(slice.offset - offset, slice.size, slice.buf);
            }
        }
        std::shared_ptr<char> Get() const
        {
            if (slices.size() == 1) {
                return slices.front().buf.Get();
            }

            ExpandableMemory tmpBuf;
            tmpBuf.Reserve(size);

            for (auto &slice : slices) {
                tmpBuf.Replace(slice.offset - offset, slice.size, slice.buf);
            }

            slices = {Slice{tmpBuf, size, offset}};
            return slices.front().buf.Get();
        }
        MergedSlice(Slice &&slice)
            : size(slice.size),
              offset(slice.offset),
              memoryOccupancy(slice.size)
        {
            slices.push_back(std::move(slice));
        }
        MergedSlice(std::vector<MergedSlice> &&toMerge)
            : size(0),
              offset(LONG_MAX),
              memoryOccupancy(0)
        {
            // first two dont overlap, 3rd does
            size_t maxEnd = 0;
            for (auto &ms : toMerge) {
                for (auto &slice : ms.slices) {
                    slices.emplace_back(std::move(slice));
                }
                offset = offset < ms.offset ? offset : ms.offset;
                maxEnd = std::max(maxEnd, ms.offset + ms.size);
                memoryOccupancy += ms.memoryOccupancy;
            }
            size = maxEnd - offset;
        }

        mutable std::vector<Slice> slices;
        mutable size_t size;
        off_t offset;
        size_t memoryOccupancy;
        bool operator<(const MergedSlice &rhs) const { return offset < rhs.offset; }
    };

    class SerialData {
      public:
        SerialData() = default;
        bool Append(const char *appendBuf, size_t appendSize, off_t appendOffset)
        {
            if (!buf.Append(appendBuf, appendSize)) {
                return false;
            }
            if (offset == -1) {
                offset = appendOffset;
            }
            size += appendSize;
            return true;
        }
        void Clear()
        {
            buf.Clear();
            offset = -1;
            size = 0;
        }
        bool Empty() { return size == 0; }
        size_t End() { return size + (offset == -1 ? 0 : offset); }
        FixMemory buf;
        size_t size = 0;
        off_t offset = -1;
    };

    WriteStream() = default;
    ~WriteStream() = default;

    int Push(FalconWriteBuffer buf, off_t offset, uint64_t currentSize);
    int PersistToFile(const char *buf, size_t size, off_t offset, uint64_t currentSize);
    int Persist(uint64_t currentSize);

    int Complete(uint64_t currentSize, bool isFlush, bool isSync = false);

    int SetFd(uint64_t newPhysicalFd);
    void SetInodeId(uint64_t newInodeId) { inodeId = newInodeId; }
    void SetDirect(bool isDirect) { direct = isDirect; }
    void SetClient(std::shared_ptr<FalconIOClient> falconIOClient);
    uint64_t GetSize();

  private:
    int64_t Merge(MergedSlice &&slice); // can return negative

    std::set<MergedSlice> stream; // (offset, size, content)
    uint64_t physicalFd = UINT64_MAX;
    std::shared_ptr<FalconIOClient> client = nullptr;
    std::shared_mutex mutex;
    SerialData data;
    uint64_t inodeId = 0;
    bool direct = false;
};
