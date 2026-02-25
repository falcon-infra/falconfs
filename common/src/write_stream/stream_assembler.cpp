/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "write_stream/stream_assembler.h"

#include "disk_cache/disk_cache.h"
#include "stats/falcon_stats.h"

MemPool FixMemory::writeMemPool(FALCON_STORE_STREAM_MAX_SIZE, 500);

int WriteStream::Push(FalconWriteBuffer buf, off_t offset, uint64_t currentSize)
{
    if (buf.size <= 0) {
        return 0;
    }

    /* align the buffer if write to loacl */
    if (client == nullptr) {
        if (!direct) {
            return PersistToFile(buf.ptr, buf.size, offset, currentSize);
        } else {
            int alignedNum = buf.size / 512 + static_cast<int>(buf.size % 512 != 0);
            char *alignedBuf = (char *)aligned_alloc(512, 512 * alignedNum);
            if (alignedBuf == nullptr) {
                FALCON_LOG(LOG_ERROR) << "aligned_alloc failed: " << strerror(errno);
                return -ENOMEM;
            }
            (void)memcpy(alignedBuf, buf.ptr, buf.size);
            int ret = PersistToFile(alignedBuf, buf.size, offset, currentSize);
            free(alignedBuf);
            return ret;
        }
    }

    std::unique_lock<std::shared_mutex> xlock(mutex);
    int ret = 0;
    /* Data not in order or large data, persist the current m_data buffer */
    if (data.End() != (size_t)offset || buf.size >= FALCON_STORE_STREAM_MAX_SIZE) {
        ret = Persist(currentSize);
        if (ret != 0) {
            return ret;
        }
    }

    /* Large data, persist incoming data */
    if (buf.size >= FALCON_STORE_STREAM_MAX_SIZE) {
        xlock.unlock();
        return PersistToFile(buf.ptr, buf.size, offset, currentSize);
    }

    /* Too much data in m_data, persist */
    if (data.size + buf.size > FALCON_STORE_STREAM_MAX_SIZE) {
        ret = Persist(currentSize);
        if (ret != 0) {
            return ret;
        }
    }
    /* concatanate new data to m_data, copy */
    data.Append(buf.ptr, buf.size, offset);

    return 0;
}

/*
 * Write data directly to file.
 */
int WriteStream::PersistToFile(const char *buf, size_t size, off_t offset, uint64_t currentSize)
{
    if (size <= 0) {
        return 0;
    }

    if (buf == nullptr) {
        FALCON_LOG(LOG_ERROR) << "In WriteStream::persistToFile(): empty buf";
        return -EINVAL;
    }

    ssize_t retSize = 0;
    if (client != nullptr) {
        retSize = -ETIMEDOUT;
        for (int i = 0; i < BRPC_RETRY_NUM && retSize == -ETIMEDOUT; ++i) {
            retSize = client->WriteFile(physicalFd, buf, size, offset);
            if (retSize == -ETIMEDOUT) {
                sleep(BRPC_RETRY_DELEY);
                FALCON_LOG(LOG_ERROR) << "Reach timeout, retry num is " << i;
            }
        }
        if (retSize != 0) {
            FALCON_LOG(LOG_ERROR) << "In WriteStream::persistToFile(): remote persist failed";
            return retSize;
        }
    } else {
        uint64_t newSize = std::max(offset + size, currentSize);
        uint64_t sizeToAdd = newSize - currentSize;
        if (!DiskCache::GetInstance().PreAllocSpace(sizeToAdd)) {
            FALCON_LOG(LOG_ERROR) << "In WriteStream::persistToFile(): Can not pre-allocate enough space!";
            return -ENOSPC;
        }
        FalconStats::GetInstance().stats[BLOCKCACHE_WRITE] += size;
        retSize = pwrite(physicalFd, buf, size, offset);
        if (retSize < 0) {
            int err = errno;
            FALCON_LOG(LOG_ERROR) << "In WriteStream::persistToFile(): pwrite failed" << strerror(err);
            DiskCache::GetInstance().FreePreAllocSpace(sizeToAdd);
            return -err;
        }
        if (!DiskCache::GetInstance().Add(inodeId, sizeToAdd)) {
            DiskCache::GetInstance().FreePreAllocSpace(sizeToAdd);
            FALCON_LOG(LOG_ERROR) << "WriteStream::persistToFile(): DiskCache Add failed!";
            return -ENOENT;
        }
        DiskCache::GetInstance().FreePreAllocSpace(sizeToAdd);
        return 0;
    }
    return 0;
}

/*
 * Called by CloseTmpFile. Write data in m_data buffer to file. Call close file rpc.
 */
int WriteStream::Complete(uint64_t currentSize, bool isFlush, bool isSync)
{
    std::unique_lock<std::shared_mutex> xlock(mutex);
    if (client != nullptr) {
        if (!data.Empty()) {
            int ret = -ETIMEDOUT;
            for (int i = 0; i < BRPC_RETRY_NUM && ret == -ETIMEDOUT; ++i) {
                ret = client->CloseFile(physicalFd, isFlush, isSync, data.buf.c_str(), data.size, data.offset);
                if (ret == -ETIMEDOUT) {
                    sleep(BRPC_RETRY_DELEY);
                    FALCON_LOG(LOG_ERROR) << "Reach timeout, retry num is " << i;
                }
            }
            data.Clear();
            return ret;
        }
        return client->CloseFile(physicalFd, isFlush, isSync, nullptr, 0, 0);
    }

    return Persist(currentSize);
}

int WriteStream::SetFd(uint64_t newPhysicalFd)
{
    std::unique_lock<std::shared_mutex> lock(mutex);
    if (physicalFd != UINT64_MAX && physicalFd != newPhysicalFd) {
        FALCON_LOG(LOG_ERROR) << "In WriteStream::setFd(): fd already set " << newPhysicalFd;
        return -EBADF;
    }
    physicalFd = newPhysicalFd;
    return 0;
}

void WriteStream::SetClient(std::shared_ptr<FalconIOClient> falconIOClient)
{
    std::unique_lock<std::shared_mutex> lock(mutex);
    if (falconIOClient == nullptr) {
        return;
    }
    client = falconIOClient;
}

/*
 * Merge the slice of data. Unused.
 */
int64_t WriteStream::Merge(MergedSlice &&slice)
{
    int64_t ret = 0;
    uint64_t end = slice.offset + slice.size;
    std::vector<MergedSlice> mergeQ;
    auto it = stream.upper_bound(slice);
    if (it != stream.begin() && (size_t)std::prev(it)->offset + std::prev(it)->size >= (size_t)slice.offset) {
        // prev one is the first less than or equal to, and also overlaps
        end = std::max(end, std::prev(it)->offset + std::prev(it)->size);
        ret -= std::prev(it)->memoryOccupancy;
        auto internal_node = stream.extract(std::prev(it));
        mergeQ.emplace_back(std::move(internal_node.value()));
    }

    // merge the end of overlap
    while (it != stream.end() && end >= (size_t)it->offset) {
        ret -= it->memoryOccupancy;
        auto prevIt = it;
        it = std::next(it);
        if (end < prevIt->offset + prevIt->size) {
            end = prevIt->offset + prevIt->size;
            auto internal_node = stream.extract(prevIt);
            mergeQ.emplace_back(std::move(internal_node.value()));
        }
    }

    if (mergeQ.empty()) {
        // no overlap
        ret += slice.memoryOccupancy;
        stream.insert(std::forward<MergedSlice>(slice));
    } else {
        mergeQ.emplace_back(std::forward<MergedSlice>(slice)); // new slice is the last to update
        MergedSlice &&ms = MergedSlice(std::move(mergeQ));
        ret += ms.memoryOccupancy;
        stream.insert(std::forward<MergedSlice>(ms));
    }
    return ret;
}

/*
 * Called by complete and push, persist data in m data buffer to file
 */
int WriteStream::Persist(uint64_t currentSize)
{
    if (client == nullptr && physicalFd == UINT64_MAX) {
        FALCON_LOG(LOG_ERROR) << "In WriteStream::persist(): fd not set";
        return -EBADF;
    }

    int ret = 0;
    if (!data.Empty()) {
        ret = PersistToFile(data.buf.c_str(), data.size, data.offset, currentSize);
    }
    data.Clear();

    return ret;
}

/*
 * Get current data size in m_data buffer
 */
uint64_t WriteStream::GetSize()
{
    std::shared_lock<std::shared_mutex> lock(mutex);
    return data.size;
}
