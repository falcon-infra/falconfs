/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "disk_cache/disk_cache.h"

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <algorithm>
#include <format>

#include <sys/statfs.h>
#include <sys/time.h>

#include "log/logging.h"
#include "util/utils.h"

std::vector<CacheItem> DiskCache::initCacheVector;
std::mutex DiskCache::initCacheMutex;

DiskCache::DiskCache(float ratio) { freeRatio = ratio; }

DiskCache::~DiskCache()
{
    stop = true;
    if (cleanupThread.joinable()) {
        cleanupThread.join();
    }
    inodeToCacheIter.clear();
    cacheItems.clear();
}

int DiskCache::Start(std::string &path, int dirNum, float ratio, float bgEvitRatio)
{
    rootDir = path;
    totalDirNum = dirNum;
    freeRatio = ratio;
    int ret = RETURN_OK;
    if (ratio == 0) {
        stop = true;
        return ret;
    }
    bgFreeRatio = bgEvitRatio;
    ret = ScanCache();
    if (ret != RETURN_OK) {
        return ret;
    }

    ret = GetCurFreeRatio();
    if (ret != RETURN_OK) {
        FALCON_LOG(LOG_ERROR) << "Get Current Free Ratio failed";
        return ret;
    }
    ret = CheckSpaceEnough();
    if (ret != RETURN_OK) {
        return ret;
    }

    cleanupThread = std::thread(&DiskCache::CheckFreeSpace, this);
    return RETURN_OK;
}

int DiskCache::ScanCache()
{
    std::vector<std::thread> initCacheThreads;

    for (int i = 0; i < totalDirNum; ++i) {
        std::string dirPath = std::format("{}/{}", rootDir, i);

        initCacheThreads.emplace_back(Walk, dirPath);
    }
    for (auto &thread : initCacheThreads) {
        thread.join();
    }
    if (initCacheVector.empty()) {
        return RETURN_OK;
    }

    std::sort(initCacheVector.begin(), initCacheVector.end(), [](const CacheItem &first, const CacheItem &second) {
        return first.atime < second.atime;
    });
    for (CacheItem cache : initCacheVector) {
        InsertAndUpdate(cache.inode, cache.size, false);
    }
    initCacheVector.clear();
    return RETURN_OK;
}

int DiskCache::Walk(std::string dirPath)
{
    DIR *const dir = opendir(dirPath.c_str());
    if (!dir) {
        return RETURN_ERROR;
    }
    std::vector<CacheItem> cacheVector;
    for (const struct dirent *f = readdir(dir); f; f = readdir(dir)) {
        if (strcmp(f->d_name, ".") == 0 || strcmp(f->d_name, "..") == 0) {
            continue;
        }
        struct stat st;
        errno_t err = memset_s(&st, sizeof(st), 0, sizeof(st));
        if (err != 0) {
            FALCON_LOG(LOG_ERROR) << "Secure func failed: " << err;
            return RETURN_ERROR;
        }
        std::string filePath = dirPath + "/" + f->d_name;
        stat(filePath.c_str(), &st);
        CacheItem cache;
        cache.inode = atoll(f->d_name);
        cache.atime = static_cast<uint64_t>(st.st_atime);
        cache.size = st.st_size;
        cache.refs = 0;
        cacheVector.emplace_back(cache);
    }
    if (closedir(dir)) {
        return RETURN_ERROR;
    }
    std::lock_guard<std::mutex> lk(initCacheMutex);
    initCacheVector.insert(initCacheVector.end(), cacheVector.begin(), cacheVector.end());
    return RETURN_OK;
}

int DiskCache::GetCurFreeRatio()
{
    struct statfs diskInfo;
    errno_t err = memset_s(&diskInfo, sizeof(diskInfo), 0, sizeof(diskInfo));
    if (err != 0) {
        FALCON_LOG(LOG_ERROR) << "Secure func failed: " << err;
        return RETURN_ERROR;
    }
    int32_t ret = statfs(rootDir.c_str(), &diskInfo);
    if (ret != 0) {
        FALCON_LOG(LOG_ERROR) << "Get disk(" << rootDir << ") stat ret " << ret << ": " << strerror(errno);
        return RETURN_ERROR;
    }

    totalCap = static_cast<uint64_t>(diskInfo.f_bsize) * static_cast<uint64_t>(diskInfo.f_blocks);
    uint64_t capacity = static_cast<uint64_t>(diskInfo.f_bsize) * static_cast<uint64_t>(diskInfo.f_bavail);
    freeCap.store(capacity);
    blockRatio = capacity * 1.0 / totalCap;
    totalInodes = static_cast<uint64_t>(diskInfo.f_files);
    freeInodes = static_cast<uint64_t>(diskInfo.f_ffree);
    inodeRatio = freeInodes * 1.0 / totalInodes;

    return RETURN_OK;
}

void DiskCache::CheckFreeSpace()
{
    while (!stop) {
        {
            std::lock_guard<std::mutex> lock(mutex);
            int ret = GetCurFreeRatio();
            if (ret != RETURN_OK) {
                break;
            }
            if (blockRatio < bgFreeRatio || inodeRatio < bgFreeRatio) {
                hasFreeSpace = false;
                Cleanup();
            }
            hasFreeSpace = blockRatio >= bgFreeRatio && inodeRatio >= bgFreeRatio;
        }
        sleep(10);
    }
}

void DiskCache::CleanupForEvict(uint64_t preAllocSize)
{
    // lock
    uint64_t toFreeCap = 0;
    uint64_t toFreeInode = 0;
    float freeBlockRatio = blockRatio - (preAllocSize + reservedCap) * 1.0 / totalCap;
    if (freeBlockRatio < freeRatio) {
        toFreeCap = (uint64_t)(totalCap * (freeRatio - freeBlockRatio));
        FALCON_LOG(LOG_WARNING) << "DiskCache::CleanupForEvict(): Evict file due to block limit, data toFreeCap = "
                                << toFreeCap;
        if (toFreeCap > usedCap) {
            toFreeCap = usedCap;
        }
    }

    if (inodeRatio < freeRatio) {
        toFreeInode = (uint64_t)(totalInodes * (freeRatio - inodeRatio));
        FALCON_LOG(LOG_WARNING) << "DiskCache::CleanupForEvict(): Evict file due to inode limit, inodes toFreeInode = "
                                << toFreeInode;
        if (toFreeInode > inodeToCacheIter.size()) {
            toFreeInode = inodeToCacheIter.size();
        }
    }

    uint64_t freedCap = 0;
    uint64_t freedInode = 0;

    for (auto it = cacheItems.begin(); it != cacheItems.end();) {
        if (it->refs > 0) {
            ++it;
            continue;
        }
        uint64_t key = it->inode;
        uint64_t size = it->size;
        std::string fileName = GetFilePath(key);
        int ret = remove(fileName.c_str());
        if (ret == 0) {
            freedCap += size;
            freedInode++;
            it = cacheItems.erase(it);
            inodeToCacheIter.erase(key);
            usedCap -= size;
            freeCap += size;
            FALCON_LOG(LOG_WARNING) << "Evict file: " << fileName;
        } else {
            ++it;
            FALCON_LOG(LOG_WARNING) << "Evict file: " << fileName << " failed: " << strerror(errno);
        }
        if (freedCap >= toFreeCap && freedInode >= toFreeInode) {
            FALCON_LOG(LOG_WARNING) << "DiskCache::CleanupForEvict(): Evicted " << freedInode << " files, all size is "
                                    << freedCap;
            break;
        }
    }
}

void DiskCache::Cleanup()
{
    // lock
    uint64_t toFreeCap = 0;
    uint64_t toFreeInode = 0;
    float freeRatio = bgFreeRatio;
    if (blockRatio < freeRatio) {
        toFreeCap = (uint64_t)(totalCap * (freeRatio - blockRatio));
        FALCON_LOG(LOG_WARNING) << "DiskCache::Cleanup(): Evict file due to block limit, data toFreeCap = "
                                << toFreeCap;
        if (toFreeCap > usedCap) {
            toFreeCap = usedCap;
        }
    }

    if (inodeRatio < freeRatio) {
        toFreeInode = (uint64_t)(totalInodes * (freeRatio - inodeRatio));
        FALCON_LOG(LOG_WARNING) << "DiskCache::Cleanup(): Evict file due to inode limit, inodes toFreeInode = "
                                << toFreeInode;
        if (toFreeInode > inodeToCacheIter.size()) {
            toFreeInode = inodeToCacheIter.size();
        }
    }

    uint64_t freedCap = 0;
    uint64_t freedInode = 0;

    for (auto it = cacheItems.begin(); it != cacheItems.end();) {
        if (it->refs > 0) {
            ++it;
            continue;
        }
        uint64_t key = it->inode;
        uint64_t size = it->size;
        std::string fileName = GetFilePath(key);
        int ret = remove(fileName.c_str());
        if (ret == 0) {
            freedCap += size;
            freedInode++;
            it = cacheItems.erase(it);
            inodeToCacheIter.erase(key);
            usedCap -= size;
            freeCap += size;
            FALCON_LOG(LOG_WARNING) << "Evict file: " << fileName;
        } else {
            ++it;
            FALCON_LOG(LOG_WARNING) << "Evict file: " << fileName << " failed: " << strerror(errno);
        }
        if (freedCap >= toFreeCap && freedInode >= toFreeInode) {
            FALCON_LOG(LOG_WARNING) << "DiskCache::CleanupForEvict(): Evicted " << freedInode << " files, all size is "
                                    << freedCap;
            break;
        }
    }
}

int DiskCache::Delete(uint64_t key)
{
    if (stop) {
        std::string fileName = GetFilePath(key);
        int ret = remove(fileName.c_str());
        return ret;
    }
    std::lock_guard<std::mutex> lock(mutex);
    if (inodeToCacheIter.find(key) != inodeToCacheIter.end()) {
        int ret = 0;
        auto elem = inodeToCacheIter[key];
        uint64_t size = elem->size;
        std::string fileName = GetFilePath(key);
        ret = remove(fileName.c_str());
        if (ret != 0) {
            int err = errno;
            FALCON_LOG(LOG_ERROR) << "Delete file: " << fileName << " failed: " << strerror(err);
            return -err;
        }
        cacheItems.erase(elem);
        inodeToCacheIter.erase(key);
        usedCap -= size;
        freeCap += size;
        FALCON_LOG(LOG_INFO) << "Delete file: " << fileName;
    }
    return 0;
}

void DiskCache::Pin(uint64_t key)
{
    if (stop) {
        return;
    }
    inodeToCacheIter[key]->refs += 1;
    inodeToCacheIter[key]->atime = static_cast<uint64_t>(time(nullptr));
}

void DiskCache::Unpin(uint64_t key)
{
    if (stop) {
        return;
    }
    std::lock_guard<std::mutex> lock(mutex);
    if (inodeToCacheIter.find(key) != inodeToCacheIter.end() && inodeToCacheIter[key]->refs > 0) {
        inodeToCacheIter[key]->refs -= 1;
    }
}

bool DiskCache::Find(uint64_t key, bool needPin)
{
    if (stop) {
        std::string fileName = GetFilePath(key);
        return access(fileName.c_str(), F_OK) == 0;
    }
    std::lock_guard<std::mutex> lock(mutex);
    if (inodeToCacheIter.find(key) != inodeToCacheIter.end()) {
        if (needPin) {
            Pin(key);
        }
        return true;
    }
    return false;
}

void DiskCache::DeleteOldCacheWithNoPin(uint64_t key)
{
    std::lock_guard<std::mutex> lock(mutex);
    if (inodeToCacheIter.find(key) != inodeToCacheIter.end()) {
        if (inodeToCacheIter[key]->refs <= 0) {
            int ret = 0;
            auto elem = inodeToCacheIter[key];
            uint64_t size = elem->size;
            std::string fileName = GetFilePath(key);
            ret = remove(fileName.c_str());
            if (ret != 0) {
                int err = errno;
                FALCON_LOG(LOG_ERROR) << "DeleteOldCacheWithNoPin file: " << fileName << " failed: " << strerror(err);
                return;
            }
            cacheItems.erase(elem);
            inodeToCacheIter.erase(key);
            usedCap -= size;
            freeCap += size;
        }
    }
}

void DiskCache::InsertAndUpdate(uint64_t key, uint64_t size, bool needPin)
{
    if (stop) {
        return;
    }
    std::lock_guard<std::mutex> lock(mutex);
    if (inodeToCacheIter.find(key) != inodeToCacheIter.end()) {
        // update
        usedCap += static_cast<int64_t>(size - inodeToCacheIter[key]->size);
        freeCap -= static_cast<int64_t>(size - inodeToCacheIter[key]->size);
        inodeToCacheIter[key]->atime = static_cast<uint64_t>(time(nullptr));
        inodeToCacheIter[key]->size = size;
        //
    } else {
        // insert
        CacheItem elem;
        elem.atime = static_cast<uint64_t>(time(nullptr));
        elem.size = size;
        elem.inode = key;
        cacheItems.emplace_back(elem);
        inodeToCacheIter[key] = prev(cacheItems.end());
        usedCap += size;
        freeCap -= size;
        if (needPin) {
            Pin(key);
        }
        //
    }
}

bool DiskCache::Update(uint64_t key, uint64_t size)
{
    if (stop) {
        return true;
    }
    std::lock_guard<std::mutex> lock(mutex);
    if (inodeToCacheIter.find(key) != inodeToCacheIter.end()) {
        // update
        if (size <= inodeToCacheIter[key]->size) {
            return true;
        }
        usedCap += static_cast<int64_t>(size - inodeToCacheIter[key]->size);
        freeCap -= static_cast<int64_t>(size - inodeToCacheIter[key]->size);
        inodeToCacheIter[key]->atime = static_cast<uint64_t>(time(nullptr));
        inodeToCacheIter[key]->size = size;
        // FALCON_LOG(LOG_INFO) << "Add Cache, inode =  " << key << ", size = " << size << ", usedCap = " << usedCap;
    } else {
        FALCON_LOG(LOG_ERROR) << "In DiskCache::Add(), inode " << key << " not found";
        return false;
    }
    return true;
}

bool DiskCache::Add(uint64_t key, uint64_t size)
{
    if (stop) {
        return true;
    }
    std::lock_guard<std::mutex> lock(mutex);
    if (inodeToCacheIter.find(key) != inodeToCacheIter.end()) {
        // update
        usedCap += static_cast<int64_t>(size);
        freeCap -= static_cast<int64_t>(size);
        inodeToCacheIter[key]->atime = static_cast<uint64_t>(time(nullptr));
        inodeToCacheIter[key]->size += size;
        // FALCON_LOG(LOG_INFO) << "Add Cache, inode =  " << key << ", size = " << size << ", usedCap = " << usedCap;
    } else {
        FALCON_LOG(LOG_ERROR) << "In DiskCache::Add(), inode " << key << " not found";
        return false;
    }
    return true;
}

void DiskCache::Evict(uint64_t size)
{
    std::lock_guard<std::mutex> lock(mutex);
    GetCurFreeRatio();
    CleanupForEvict(size);
}

bool DiskCache::PreAllocSpace(uint64_t size)
{
    if (stop) {
        return true;
    }
    std::lock_guard<std::mutex> lock(allocMutex);
    //
    if (reservedCap + size < freeCap.load()) {
        reservedCap += size;
        return true;
    } else {
        hasFreeSpace = false;
        int retryCnt = 3;
        do {
            if (retryCnt == 0) {
                FALCON_LOG(LOG_WARNING) << "PreAllocSpace failed, size = " << size << " ,reservedCap = " << reservedCap
                                        << " ,freeCap = " << freeCap.load();
                return false;
            }
            Evict(size);
            --retryCnt;
            sleep(1);
        } while (reservedCap + size >= freeCap.load() && retryCnt >= 0);
        hasFreeSpace = true;
        reservedCap += size;
        return true;
    }
}

void DiskCache::FreePreAllocSpace(uint64_t size)
{
    if (stop) {
        return;
    }
    std::lock_guard<std::mutex> lock(allocMutex);
    reservedCap -= size;
}

bool DiskCache::HasFreeSpace() { return hasFreeSpace.load(); }

int DiskCache::CheckSpaceEnough()
{
    float blockRatio = (freeCap + usedCap) * 1.0 / totalCap;
    float inodeRatio = (freeInodes + inodeToCacheIter.size()) * 1.0 / totalInodes;
    if (blockRatio <= bgFreeRatio || inodeRatio <= bgFreeRatio || blockRatio <= freeRatio || inodeRatio < freeRatio) {
        FALCON_LOG(LOG_ERROR) << "The free space can not support FalconFS running";
        FALCON_LOG(LOG_ERROR) << "Free space is not enough";
        return RETURN_ERROR;
    }
    return RETURN_OK;
}
