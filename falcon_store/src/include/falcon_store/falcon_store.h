/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#pragma once

#include <fcntl.h>
#include <securec.h>
#include <atomic>
#include <cstring>
#include <memory>
#include <mutex>
#include <unordered_map>

#include <sys/statvfs.h>

#include <butil/iobuf.h>

#include "buffer/falcon_buffer.h"
#include "buffer/open_instance.h"
#include "storage/storage.h"
#include "thread_pool/thread_pool.h"
#include "util/file_lock.h"

class FalconStore {
  public:
    void SetFalconStoreParam(std::string &newNodeConfig);
    static FalconStore *GetInstance();
    void DeleteInstance();

    /*-----------------read-----------------*/
    int ReadFile(OpenInstance *openInstance, char *buffer, size_t size, off_t offset);
    ssize_t ReadFileLR(char *readBuffer, off_t offset, OpenInstance *openInstance, size_t readBufferSize);
    int ReadSmallFiles(OpenInstance *openInstance);
    int
    ReadSmallFilesForBrpc(uint64_t inodeId, const std::string &path, char *buf, size_t size, int oflags, bool nodeFail);

    /*-----------------func-----------------*/
    int OpenFile(OpenInstance *openInstance);
    int WriteFile(OpenInstance *openInstance, const char *buf, size_t size, off_t offset);
    int WriteLocalFileForBrpc(OpenInstance *openInstance, butil::IOBuf &buf, off_t offset);
    int CloseTmpFiles(OpenInstance *openInstance, bool isFlush, bool isSync);
    int DeleteFiles(uint64_t inodeId, int nodeId, std::string path);
    int StatFS(struct statvfs *vfsbuf);
    int StatFSForBrpc(const std::string &path,
                      uint64_t &fblocks,
                      uint64_t &fbfree,
                      uint64_t &fbavail,
                      uint64_t &ffiles,
                      uint64_t &fffree);
    int CopyData(const std::string &srcName, const std::string &dstName);
    int DeleteDataAfterRename(const std::string &objectName);
    int TruncateFile(OpenInstance *openInstance, off_t size);
    int TruncateOpenInstance(OpenInstance *openInstance, off_t size);
    int TruncateFileForBrpc(uint64_t inodeId, off_t size);
    int StatCluster(int nodeId, std::vector<size_t> &currentStats, bool scatter);

    /*-----------------util-----------------*/
    int GetInitStatus();
    int InitStore();

  private:
    /*-----------------read-----------------*/
    bool StartPreReadThreaded(OpenInstance *openInstance);
    void StopPreReadThreaded(OpenInstance *openInstance);
    int ReadToBuffer(FalconReadBuffer buf, OpenInstance *openInstance, off_t offset);
    int RandomRead(FalconReadBuffer buf, OpenInstance *openInstance, off_t offset);
    int SequenceRead(FalconReadBuffer buf, OpenInstance *openInstance, off_t offset);
    int WriteToFileAsync(uint64_t inodeId, std::string &fileName, std::shared_ptr<char> buf, size_t bufSize);

    /*-----------------func-----------------*/
    int OpenFileFromRemote(OpenInstance *openInstance, bool largeFile);

    /*-----------------util-----------------*/
    int PathToNodeId(std::string &path);
    void AllocNodeId(OpenInstance *openInstance);
    bool ConnectionError(int err);
    bool IoError(int err);

    /*-----------------storage-----------------*/
    int DownLoadFromStorage(OpenInstance *openInstance, bool isSync, bool toBuffer = false);
    /* buf is oraganized by other, and must exist in process */
    int DownLoadFromStorageForBrpc(uint64_t inodeId,
                                   const std::string &path,
                                   char *buf,
                                   size_t bufSize,
                                   bool isSync,
                                   bool toBuffer);
    int FlushToStorage(std::string path, uint64_t inodeId);
    int StatFsStorage(struct statvfs *vfsbuf);

  private:
    FalconStore() { initStatus = InitStore(); }
    std::string nodeConfig{};
    int initStatus = 0;
    bool asyncToObs{false};
    bool persistToStorage{true};
    int parentPathLevel{-1};
    bool isInference = true;
    bool toLocal = false;
    FileLock fileLock;
    std::unordered_map<std::string, std::atomic<uint64_t>> nodeHash;
    std::mutex mutex;
    std::string dataPath;
    std::unique_ptr<ThreadPool> storeThreadPool;
    Storage *storage;
    std::jthread statsThread;
};

std::string GetParentPath(const std::string &path, int level = -1);
