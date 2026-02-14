/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#pragma once

#include <dirent.h>

#include <atomic>
#include <list>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#ifndef RETURN_OK
#define RETURN_OK 0
#endif

#ifndef RETURN_ERROR
#define RETURN_ERROR (-1)
#endif

struct CacheItem
{
    uint64_t inode{0};
    uint64_t size{0};
    uint64_t atime{0};
    uint32_t refs{0};
};

class DiskCache {
  public:
    static DiskCache &GetInstance()
    {
        static DiskCache instance;
        return instance;
    }
    DiskCache() = default;
    DiskCache(float ratio);
    ~DiskCache();
    int Start(std::string &path, int dirNum, float ratio, float bgEvitRatio);
    bool Find(uint64_t key, bool needPin);
    void DeleteOldCacheWithNoPin(uint64_t key);
    void InsertAndUpdate(uint64_t key, uint64_t size, bool needPin);
    bool Add(uint64_t key, uint64_t size);
    bool Update(uint64_t key, uint64_t size);
    int Delete(uint64_t key);
    void Evict(uint64_t size);
    void Unpin(uint64_t key);
    void Pin(uint64_t key);
    bool PreAllocSpace(uint64_t size);
    void FreePreAllocSpace(uint64_t size);
    bool HasFreeSpace();

  private:
    uint64_t totalCap{0};
    std::atomic<uint64_t> freeCap{0};
    float blockRatio{0.0};
    uint64_t totalInodes{0};
    uint64_t freeInodes{0};
    float inodeRatio{0.0};
    float freeRatio{0.1};
    float bgFreeRatio{0.2};

    bool testOBS = false;

    uint64_t usedCap{0};

    std::string rootDir;
    std::list<CacheItem> cacheItems;
    using cacheIterator = std::list<CacheItem>::iterator;
    std::unordered_map<uint64_t, cacheIterator> inodeToCacheIter;
    std::mutex mutex;

    std::thread cleanupThread;
    std::atomic<bool> stop{false};
    std::atomic<bool> hasFreeSpace{true};

    int totalDirNum{101};

    std::atomic<uint64_t> reservedCap{0};
    std::mutex allocMutex;

    static std::mutex initCacheMutex;

    static std::vector<CacheItem> initCacheVector;
    int GetCurFreeRatio();
    void CheckFreeSpace();
    void Cleanup();
    void CleanupForEvict(uint64_t size);
    int ScanCache();
    static int Walk(std::string dirPath);
    int CheckSpaceEnough();
};
