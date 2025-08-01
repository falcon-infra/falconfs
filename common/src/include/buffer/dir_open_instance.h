/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#pragma once

#include <fcntl.h>
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <atomic>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include <sys/stat.h>
#include <sys/types.h>

#include "buffer/open_instance.h"
#include "connection.h"

constexpr int START_FD = 3;
constexpr int MAX_OPENINSTANCE_NUM = 40000;

struct DirOpenInstance
{
    uint64_t fd;
    std::unordered_map<std::string, std::shared_ptr<Connection>> workers;
    std::vector<std::string> partialEntryVec;
    std::vector<unsigned int> fileModes;
    std::unordered_map<std::string, int> readFileCount;
    std::unordered_map<std::string, int> readFileCountIndex;
    std::unordered_map<std::string, std::string> lastFileNames;
    std::unordered_map<std::string, int> lastShardIndexes;
    std::set<std::string> allWorkerIPAndPorts;
    uint32_t offset;
    std::unordered_map<std::string, std::shared_ptr<Connection>> workingWorkers;

    DirOpenInstance(uint64_t obtainedFd)
    {
        fd = obtainedFd;
        offset = 0;
    }

    void SetAllWorkerInfo(std::unordered_map<std::string, std::shared_ptr<Connection>> tmpWorkers)
    {
        workers = tmpWorkers;
        workingWorkers = tmpWorkers;
        for (auto &tmpWorker : tmpWorkers) {
            std::string ipPort = tmpWorker.first;
            readFileCount.emplace(ipPort, 0);
            readFileCountIndex.emplace(ipPort, 0);
            lastFileNames.emplace(ipPort, "");
            allWorkerIPAndPorts.emplace(ipPort);
            lastShardIndexes.emplace(ipPort, -1);
        }
    }
    void ResetDirOpenInstance()
    {
        workers.clear();
        partialEntryVec.clear();
        fileModes.clear();
        readFileCount.clear();
        readFileCountIndex.clear();
        lastFileNames.clear();
        workingWorkers.clear();
        allWorkerIPAndPorts.clear();
        offset = 0;
    }
};

class FalconFd {
  public:
    static FalconFd *GetInstance();
    uint64_t AttachFd(uint64_t inodeId,
                      int oflags,
                      std::shared_ptr<char> readBuffer,
                      uint64_t size,
                      std::string path,
                      int nodeId = -1,
                      int backupNodeId = -1,
                      bool isRead = true);
    uint64_t AttachFd(const std::string &path, std::shared_ptr<OpenInstance> openInstance);
    std::shared_ptr<OpenInstance> GetOpenInstanceByFd(uint64_t fd);
    int DeleteOpenInstance(uint64_t fd, bool subCnt = true);
    uint64_t AttachDirFd(uint64_t inodeId);
    DirOpenInstance *GetDirOpenInstanceByFd(uint64_t fd);
    int DeleteDirOpenInstance(uint64_t fd);
    uint64_t ObtainFd();
    void AddOpenInstance(uint64_t fd, std::shared_ptr<OpenInstance> openInstance);
    int AddDirOpenInstance(uint64_t fd, DirOpenInstance *dirOpenInstance);
    std::shared_ptr<OpenInstance> WaitGetNewOpenInstance(bool addCnt = true);
    void ReleaseOpenInstance();
    std::unordered_set<std::shared_ptr<OpenInstance>> GetInodetoOpenInstanceSet(uint64_t inodeId);

  private:
    std::unordered_map<uint64_t, std::shared_ptr<OpenInstance>> openInstanceMap;
    std::unordered_map<uint64_t, DirOpenInstance *> dirOpenInstanceMap;
    std::unordered_map<uint64_t, std::unordered_set<std::shared_ptr<OpenInstance>>> inodeToOpenInstanceMap;
    std::atomic<uint64_t> nextFD{START_FD};
    std::shared_mutex inodeToOpenInstanceMutex;
    std::shared_mutex openInstanceMapMutex;
    std::shared_mutex dirOpenInstanceMutex;
    std::atomic<uint32_t> currOpenInstance = 0;
    std::condition_variable newOpenInstanceCV;
};
