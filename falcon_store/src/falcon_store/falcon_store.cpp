/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "falcon_store/falcon_store.h"

#include "conf/falcon_property_key.h"
#include "connection/node.h"
#include "disk_cache/disk_cache.h"
#include "falcon_code.h"
#include "init/falcon_init.h"
#include "stats/falcon_stats.h"
#include "storage/obs_storage.h"
#include "util/utils.h"

void FalconStore::SetFalconStoreParam(std::string &newNodeConfig) { nodeConfig = newNodeConfig; }

FalconStore *FalconStore::GetInstance()
{
    static FalconStore instance;
    return &instance;
}

void FalconStore::DeleteInstance()
{
    StoreNode::DeleteInstance();
    if (storage) {
        storage->DeleteInstance();
    }
}

int FalconStore::GetInitStatus() { return initStatus; }

int FalconStore::InitStore()
{
    int ret = GetInit().Init();
    if (ret != FALCON_SUCCESS) {
        FALCON_LOG(LOG_ERROR) << "Falcon init failed";
        return ret;
    }
    auto &config = GetInit().GetFalconConfig();
    std::string rootPath = config->GetString(FalconPropertyKey::FALCON_CACHE_ROOT);
    uint32_t totalDirectory = config->GetUint32(FalconPropertyKey::FALCON_DIR_NUM);
    uint32_t blockSize = config->GetUint32(FalconPropertyKey::FALCON_BLOCK_SIZE);
    FALCON_BLOCK_SIZE = blockSize;
    uint32_t bigFileReadSize = config->GetUint32(FalconPropertyKey::FALCON_BIG_FILE_READ_SIZE);
    std::string clusterView = config->GetArray(FalconPropertyKey::FALCON_CLUSTER_VIEW);
    asyncToObs = config->GetBool(FalconPropertyKey::FALCON_ASYNC);
    persistToStorage = config->GetBool(FalconPropertyKey::FALCON_PERSIST);
    uint32_t preBlockNum = config->GetUint32(FalconPropertyKey::FALCON_PRE_BLOCKNUM);
    uint32_t threadNum = config->GetUint32(FalconPropertyKey::FALCON_THREAD_NUM);
    float storageThreshold = GetStorageThreshold(persistToStorage);
    parentPathLevel = GetParentPathLevel();
    isInference = config->GetBool(FalconPropertyKey::FALCON_IS_INFERENCE);
    toLocal = config->GetBool(FalconPropertyKey::FALCON_TO_LOCAL);
    std::string mountPath = config->GetString(FalconPropertyKey::FALCON_MOUNT_PATH);

    FALCON_LOG(LOG_INFO) << "falcon_cache rootPath: " << rootPath;

    dataPath = rootPath;
    if (persistToStorage) {
        storage = OBSStorage::GetInstance();

        ret = storage->Init();
        if (ret != FALCON_SUCCESS) {
            FALCON_LOG(LOG_ERROR) << "storage init fail " << ret;
            return ret;
        }
    }

    READ_BIGFILE_SIZE = bigFileReadSize;
    SetRootPath(rootPath);
    SetTotalDirectory(totalDirectory);
    float diskFreeRatio = 0;
    float bgDiskFreeRatio = 0;
    if (storageThreshold < 1) {
        diskFreeRatio = 1.0 - storageThreshold;
        bgDiskFreeRatio = 1.1 - storageThreshold;
    }
    ret = DiskCache::GetInstance().Start(rootPath, totalDirectory, diskFreeRatio, bgDiskFreeRatio);
    if (ret != 0) {
        FALCON_LOG(LOG_ERROR) << "DiskCache start failed";
        return 1;
    }
    MemPool().GetInstance().init(FALCON_BLOCK_SIZE, preBlockNum);
    storeThreadPool = ThreadPool::CreateThreadPool(threadNum, 100000, "store thread pool");
    if (storeThreadPool == nullptr || storeThreadPool->Start() != 0) {
        FALCON_LOG(LOG_ERROR) << "Falcon threadpool init failed";
        return 1;
    }
#ifdef ZK_INIT
    ret = StoreNode::GetInstance()->SetNodeConfig(rootPath);
    if (ret != 0) {
        FALCON_LOG(LOG_ERROR) << "Falcon StoreNode init failed";
        return ret;
    }
#else
    int nodeId = config->GetUint32(FalconPropertyKey::FALCON_NODE_ID);
    ret = StoreNode::GetInstance()->SetNodeConfig(nodeId, clusterView);
    if (ret != 0) {
        FALCON_LOG(LOG_ERROR) << "Falcon StoreNode init failed";
        return ret;
    }
#endif

    return 0;
}

std::string GetParentPath(const std::string &path, int level)
{
    // path start with '/'
    std::string parentPath = "/";
    if (level == -1) {
        size_t endPos = path.size() - 1;
        for (; endPos > 0; endPos--) {
            if (path[endPos] == '/') {
                break;
            }
        }
        parentPath = path.substr(0, endPos);
    } else {
        char separator = '/';
        std::stringstream ss(path);
        std::string str;
        std::vector<std::string> parentPathVec;
        int tmpLevel = 1;
        while (getline(ss, str, separator)) {
            if (str.empty()) {
                continue;
            }
            parentPathVec.push_back(str);
            tmpLevel += 1;
            if (tmpLevel >= level + 1) {
                break;
            }
        };
        if (tmpLevel == level + 1) {
            for (int i = 0; i < level - 1; ++i) {
                parentPath = parentPath + parentPathVec[i] + "/";
            }
        } else {
            for (size_t i = 0; i < parentPathVec.size() - 1; ++i) {
                parentPath = parentPath + parentPathVec[i] + "/";
            }
        }
    }
    return parentPath;
}

unsigned long myHash(std::string &str)
{
    unsigned long hash = 5381;
    for (auto c : str) {
        hash = ((hash << 5) + hash) + c;
    }

    return hash;
}

int FalconStore::PathToNodeId(std::string &path)
{
    std::string parentPath = GetParentPath(path, parentPathLevel);
    if (!parentPath.empty()) {
        std::unique_lock<std::mutex> lock(mutex);
        if (nodeHash.count(parentPath) == 0) {
            nodeHash[parentPath] = StoreNode::GetInstance()->AllocNode(myHash(parentPath));
        }
        return nodeHash[parentPath];
    }
    return StoreNode::GetInstance()->AllocNode(myHash(parentPath));
}

void FalconStore::AllocNodeId(OpenInstance *openInstance)
{
    if (openInstance->nodeId == -1) {
        if (toLocal && DiskCache::GetInstance().HasFreeSpace()) {
            openInstance->nodeId = StoreNode::GetInstance()->GetNodeId();
            return;
        }
        if (isInference) {
            openInstance->nodeId = PathToNodeId(openInstance->path);
        } else {
            openInstance->nodeId = StoreNode::GetInstance()->AllocNode(openInstance->inodeId);
        }
    }
}
bool FalconStore::ConnectionError(int err) { return err > 0; }

bool FalconStore::IoError(int err) { return err < 0; }

/*---------------------- write ----------------------*/

// WriteLocalFileForBrpc can only be called from brpc server
int FalconStore::WriteLocalFileForBrpc(OpenInstance *openInstance, butil::IOBuf &buf, off_t offset)
{
    size_t writeSize = buf.size();
    uint64_t currentSize = openInstance->currentSize.load();
    uint64_t newSize = std::max(openInstance->currentSize.load(), offset + writeSize);
    uint64_t sizeToAdd = newSize - currentSize;
    bool isDirect = openInstance->oflags & __O_DIRECT;

    if (!DiskCache::GetInstance().PreAllocSpace(sizeToAdd)) {
        FALCON_LOG(LOG_ERROR) << "WriteLocalFileForBrpc(): Can not pre-allocate enough space!";
        return -ENOSPC;
    }
    if (!isDirect) {
        while (writeSize > 0) {
            ssize_t nwrite = buf.pcut_into_file_descriptor(openInstance->physicalFd, offset, writeSize);
            if (nwrite < 0 || nwrite > (ssize_t)writeSize) {
                offset += nwrite > 0 ? nwrite : 0;
                if ((uint64_t)offset > currentSize) {
                    openInstance->currentSize = offset;
                    if (!DiskCache::GetInstance().Update(openInstance->inodeId, offset)) {
                        FALCON_LOG(LOG_ERROR) << "WriteLocalFileForBrpc(): DiskCache Update failed!";
                        DiskCache::GetInstance().FreePreAllocSpace(sizeToAdd);
                        return -ENOENT;
                    }
                }
                DiskCache::GetInstance().FreePreAllocSpace(sizeToAdd);
                return -EINVAL;
            }
            FalconStats::GetInstance().stats[BLOCKCACHE_WRITE] += nwrite;
            writeSize -= nwrite;
            offset += nwrite;
        }
    } else {
        int alignedNum = writeSize / 512 + static_cast<int>(writeSize % 512 != 0);
        char *alignedBuf = (char *)aligned_alloc(512, 512 * alignedNum);
        if (alignedBuf == nullptr) {
            FALCON_LOG(LOG_ERROR) << "aligned_alloc failed: " << strerror(errno);
            DiskCache::GetInstance().FreePreAllocSpace(sizeToAdd);
            return -ENOMEM;
        }
        size_t bytes_cut = buf.cutn(alignedBuf, writeSize);
        if (bytes_cut < writeSize) {
            FALCON_LOG(LOG_ERROR) << "WriteLocalFileForBrpc(): cntn not enough data in IOBuf";
            DiskCache::GetInstance().FreePreAllocSpace(sizeToAdd);
            free(alignedBuf);
            return -EIO;
        }
        ssize_t retSize = pwrite(openInstance->physicalFd, alignedBuf, writeSize, offset);
        free(alignedBuf);
        if (retSize < 0) {
            int err = errno;
            FALCON_LOG(LOG_ERROR) << "WriteLocalFileForBrpc(): pwrite failed" << strerror(err);
            DiskCache::GetInstance().FreePreAllocSpace(sizeToAdd);
            return -err;
        }
        FalconStats::GetInstance().stats[BLOCKCACHE_WRITE] += retSize;
    }

    openInstance->currentSize = newSize;
    if (!DiskCache::GetInstance().Update(openInstance->inodeId, newSize)) {
        FALCON_LOG(LOG_ERROR) << "WriteLocalFileForBrpc(): DiskCache Update failed!";
        DiskCache::GetInstance().FreePreAllocSpace(sizeToAdd);
        return -ENOENT;
    }
    DiskCache::GetInstance().FreePreAllocSpace(sizeToAdd);
    return 0;
}

int FalconStore::WriteFile(OpenInstance *openInstance, const char *buf, size_t size, off_t offset)
{
    FALCON_LOG(LOG_INFO) << "WriteFile(): called";
    int ret = 0;
    FalconWriteBuffer falconBuf{buf, size};

    // if read -> write, read stream outdated, discard
    // no guarantee on concurrent read and write from fuse
    if (openInstance->preReadStarted.load() && !openInstance->preReadStopped.exchange(true)) {
        FALCON_LOG(LOG_INFO) << "WriteFile(): StopPreReadThreaded";
        StopPreReadThreaded(openInstance);
    }

    // open file, init physical fd
    if (!openInstance->isOpened.load()) {
        std::unique_lock<std::shared_mutex> openLock(openInstance->fileMutex);
        ret = OpenFile(openInstance);
        if (ret != 0) {
            openInstance->writeFail = true;
            return ret;
        }
        openInstance->isOpened = true;
    }

    ret = openInstance->writeStream.Push(falconBuf, offset, openInstance->currentSize.load());
    if (ret != 0) {
        FALCON_LOG(LOG_ERROR) << "WriteFile(): openInstance->stream.push() failed";
        openInstance->writeFail = true;
        return ret;
    }

    if (size != 0) {
        std::unique_lock<std::shared_mutex> sizeLock(openInstance->fileMutex);
        openInstance->currentSize = std::max(openInstance->currentSize.load(), size + offset);
    }

    return 0;
}

/*---------------------- read ----------------------*/

/*
 * Called by fuse
 */
int FalconStore::ReadFile(OpenInstance *openInstance, char *buf, size_t size, off_t offset)
{
    int ret = 0;
    int err = 0;
    FalconReadBuffer falconBuf{buf, size};

    /* first persist the current write stream to let data to be read */
    if (openInstance->writeStream.GetSize() > 0) {
        /* write will wait for local cache to be loaded from obs, so safe to call persist */
        FALCON_LOG(LOG_INFO) << "In ReadFile(): Persisting the written";
        ret = openInstance->writeStream.Complete(openInstance->currentSize.load(), true, false);
        if (ret != 0) {
            FALCON_LOG(LOG_ERROR) << "In ReadFile(): persist written before read failed";
            return ret;
        }
    }

    /* original small file is in read buffer after read */
    /* for read large files or read after write, sync on file content */
    if (openInstance->originalSize >= READ_BIGFILE_SIZE || (openInstance->oflags & O_ACCMODE) != O_RDONLY) {
        /* open file, init physical fd */
        if (!openInstance->isOpened.load()) {
            std::unique_lock<std::shared_mutex> openLock(openInstance->fileMutex);
            ret = OpenFile(openInstance);
            if (ret != 0) {
                FALCON_LOG(LOG_ERROR) << "In ReadFile(): big file OpenFile() failed";
                return ret;
            }
            openInstance->isOpened = true;
        }

        /* init and start the read stream */
        if (!openInstance->preReadStarted.exchange(true)) {
            if (StoreNode::GetInstance()->IsLocal(openInstance->nodeId)) {
                StopPreReadThreaded(openInstance);
            } else if (!StartPreReadThreaded(openInstance)) {
                StartPreReadThreaded(openInstance);
            }
        }

        int readSize = ReadToBuffer(falconBuf, openInstance, offset);
        if (readSize < 0) {
            FALCON_LOG(LOG_ERROR) << "In ReadFile(): ReadToBuffer() failed";
        }
        return readSize;
    } else {
        /* read small files */
        std::shared_lock<std::shared_mutex> lock(openInstance->fileMutex);
        if (offset + size <= (size_t)openInstance->readBufferSize) {
            err = memcpy_s(falconBuf.ptr, falconBuf.size, openInstance->readBuffer.get() + offset, size);
            if (err != 0) {
                FALCON_LOG(LOG_ERROR) << "Secure func failed: " << err;
                return -EIO;
            }
            return size;
        } else if (offset < (ssize_t)openInstance->readBufferSize) {
            err = memcpy_s(falconBuf.ptr,
                           falconBuf.size,
                           openInstance->readBuffer.get() + offset,
                           openInstance->readBufferSize - offset);
            if (err != 0) {
                FALCON_LOG(LOG_ERROR) << "Secure func failed: " << err;
                return -EIO;
            }
            return openInstance->readBufferSize - offset;
        }
    }
    return 0;
}

/*
 * Called by ReadFile to start fill readStream
 */
bool FalconStore::StartPreReadThreaded(OpenInstance *openInstance)
{
    int fileBlocks = (openInstance->currentSize + FALCON_BLOCK_SIZE - 1) / FALCON_BLOCK_SIZE;

    if (!openInstance->readStream.Init(openInstance, fileBlocks, FALCON_BLOCK_SIZE)) {
        return false;
    }
    openInstance->readStream.StartPushThreaded();
    return true;
}

/*
 * Called by WriteFile to stop readStream in case of write
 */
void FalconStore::StopPreReadThreaded(OpenInstance *openInstance)
{
    /* stop only once */
    openInstance->preReadStopped.store(true);
    openInstance->directReadFile.store(true);
    openInstance->readStream.StopPushThreaded();
}

/*
 * Called by ReadFile to read large or written file from readStream or file itself
 */
int FalconStore::ReadToBuffer(FalconReadBuffer buf, OpenInstance *openInstance, off_t offset)
{
    FALCON_LOG(LOG_INFO) << "FalconStore::ReadToBuffer(): called";

    /* if random read is marked, no lock is needed */
    if (openInstance->directReadFile.load()) {
        return RandomRead(buf, openInstance, offset);
    }
    /* for sequence read, make sure to read in order without concurrency */
    /* peek and store atomically */
    std::unique_lock<std::shared_mutex> offsetAndBuffLock(openInstance->fileMutex);
    if (openInstance->directReadFile.load() || openInstance->serialReadEnd != offset) {
        StopPreReadThreaded(openInstance);
        offsetAndBuffLock.unlock();
        return RandomRead(buf, openInstance, offset);
    }
    return SequenceRead(buf, openInstance, offset);
}

/*
 * Called to read file directly
 */
int FalconStore::RandomRead(FalconReadBuffer buf, OpenInstance *openInstance, off_t offset)
{
    // read file directly, rather than from read stream
    if ((openInstance->oflags & __O_DIRECT) == 0) {
        return ReadFileLR(buf.ptr, offset, openInstance, buf.size);
    } else {
        int alignedNum = buf.size / 512 + static_cast<int>(buf.size % 512 != 0);
        char *alignedBuf = (char *)aligned_alloc(512, 512 * alignedNum);
        if (alignedBuf == nullptr) {
            FALCON_LOG(LOG_ERROR) << "aligned_alloc failed: " << strerror(errno);
            return -ENOMEM;
        }
        int ret = ReadFileLR(alignedBuf, offset, openInstance, buf.size);
        if (ret < 0) {
            free(alignedBuf);
            return ret;
        }
        int err = memcpy_s(buf.ptr, buf.size, alignedBuf, buf.size);
        if (err != 0) {
            FALCON_LOG(LOG_ERROR) << "Secure func failed: " << err;
            ret = -EIO;
        }
        free(alignedBuf);
        return ret;
    }
}

/*
 * Called to read readStream
 */
int FalconStore::SequenceRead(FalconReadBuffer buf, OpenInstance *openInstance, off_t /*offset*/)
{
    // read file from read stream
    int retSize = openInstance->readStream.WaitPop(buf.ptr, buf.size);
    if (retSize > 0) {
        openInstance->serialReadEnd += retSize;
    }
    return retSize;
}

/*
 * Read from local cache file, remote cache file, or obs
 * local file read, if failed read obs
 * remote file read, if failed read obs
 * local file read called by rpc, if failed return failure
 */
ssize_t FalconStore::ReadFileLR(char *readBuffer, off_t offset, OpenInstance *openInstance, size_t readBufferSize)
{
    if (offset >= (ssize_t)openInstance->currentSize) {
        return 0;
    }
    int retSize = -1;
    ssize_t checkReadLength = std::min(readBufferSize, openInstance->currentSize - offset);

    if (StoreNode::GetInstance()->IsLocal(openInstance->nodeId)) {
        if (openInstance->physicalFd != UINT64_MAX && !fileLock.TestLocked(openInstance->inodeId, LockMode::X)) {
            /* not locked, read cache file */
            FalconStats::GetInstance().stats[BLOCKCACHE_READ] += checkReadLength;
            retSize = pread(openInstance->physicalFd, readBuffer, readBufferSize, offset);
            if (retSize != checkReadLength) {
                int err = errno;
                if (err == EAGAIN) {
                    retSize = pread(openInstance->physicalFd, readBuffer, checkReadLength, offset);
                    if (retSize != checkReadLength) {
                        err = errno;
                        FALCON_LOG(LOG_ERROR) << "In ReadFileLR(): pread fd = " << openInstance->physicalFd
                                              << " failed : " << strerror(err);
                        retSize = -err;
                    }
                } else {
                    FALCON_LOG(LOG_ERROR)
                        << "In ReadFileLR(): pread fd = " << openInstance->physicalFd << " failed : " << strerror(err);
                    retSize = -err;
                }
            }
        }
    } else {
        /* if read file rpc failed, no need to call rpc again */
        if (!openInstance->remoteFailed) {
            std::shared_ptr<FalconIOClient> falconIOClient =
                StoreNode::GetInstance()->GetRpcConnection(openInstance->nodeId);
            retSize = -EHOSTUNREACH;
            if (falconIOClient != nullptr) {
                for (int i = 0; i < BRPC_RETRY_NUM; ++i) {
                    retSize = falconIOClient->ReadFile(openInstance->inodeId,
                                                       openInstance->oflags,
                                                       readBuffer,
                                                       openInstance->physicalFd,
                                                       readBufferSize,
                                                       offset,
                                                       openInstance->path);
                    if (retSize == -ETIMEDOUT) {
                        sleep(BRPC_RETRY_DELEY);
                        FALCON_LOG(LOG_ERROR) << "Reach timeout, retry num is " << i;
                    } else {
                        break;
                    }
                }
            }
            if (retSize != checkReadLength) {
                FALCON_LOG(LOG_ERROR) << "In ReadFileLR(): read remote failed: " << strerror(-retSize) << ", for node "
                                      << openInstance->nodeId;
                openInstance->remoteFailed = true;
            }
        }
    }
    /* Read cache file failed and called by fuse not rpc -> read obs */
    if (retSize < 0 && !openInstance->isRemoteCall && persistToStorage) {
        FALCON_LOG(LOG_DEBUG) << "ReadFile from obs : " << openInstance->path;
        retSize = storage->ReadObject(openInstance->path.substr(1), offset, readBufferSize, -1, readBuffer);
        if (retSize < 0) {
            FALCON_LOG(LOG_ERROR) << "In ReadFileLR(): obs ReadObject() failed";
            retSize = -EIO;
        }
    }
    return retSize;
}

/*---------------------- open ----------------------*/

/*
 * Called by fuse
 */
int FalconStore::OpenFile(OpenInstance *openInstance)
{
    FALCON_LOG(LOG_INFO) << "OpenFile(): called by " << (openInstance->isRemoteCall ? "remote" : "fuse");
    int ret = 0;
    int err = 0;
    if (openInstance->physicalFd == UINT64_MAX) {
        /* nodeId of a new file is allocated */
        AllocNodeId(openInstance);

        if (!StoreNode::GetInstance()->IsLocal(openInstance->nodeId)) {
            /* file resides on remote node */
            bool largeFile = true;
            ret = OpenFileFromRemote(openInstance, largeFile);
            if (ret != 0) {
                FALCON_LOG(LOG_ERROR) << "OpenFile(): open remote file failed";
                return ret;
            }
        } else {
            /* file resides on local node */
            std::string fileName = GetFilePath(openInstance->inodeId);
            if (openInstance->nodeFail) {
                DiskCache::GetInstance().DeleteOldCacheWithNoPin(openInstance->inodeId);
            }
            if (DiskCache::GetInstance().Find(openInstance->inodeId, true)) {
                /* Cache Hits: read file from cache */
                int localFd = open(fileName.c_str(), openInstance->oflags, 0755);
                if (localFd < 0) {
                    err = errno;
                    DiskCache::GetInstance().Unpin(openInstance->inodeId);
                    FALCON_LOG(LOG_ERROR) << "OpenFile(): open local file " << fileName << " failed: " << strerror(err);
                    return -err;
                }
                openInstance->physicalFd = static_cast<uint64_t>(localFd);
                FALCON_LOG(LOG_INFO) << "OpenFile(): Opened existed local file " << fileName
                                     << " , fd = " << openInstance->physicalFd;
            } else {
                /* Cache Miss: either newly created file or cache file evicted */
                if ((openInstance->oflags & O_ACCMODE) != O_RDONLY) {
                    /* Cache Miss: WR/RDWR case, sync load file from obs */
                    if ((openInstance->oflags & O_CREAT) == 0 && openInstance->originalSize > 0) {
                        if (!persistToStorage) {
                            if (access(fileName.c_str(), F_OK) == 0) {
                                FALCON_LOG(LOG_ERROR) << "OpenFile(): cache file " << fileName
                                                      << " missing in diskCache but exists in ext for write";
                            } else {
                                FALCON_LOG(LOG_ERROR) << "OpenFile(): cache file " << fileName << " missing in write";
                            }
                            return -ENOENT;
                        }
                        FALCON_LOG(LOG_INFO) << "OpenFile(): Loading evicted write only cache file";
                        ret = DownLoadFromStorage(openInstance, true);
                        if (ret != 0) {
                            return ret;
                        }
                        /* file is pinned in disk cache now */
                    }
                    int localFd = open(fileName.c_str(), openInstance->oflags | O_CREAT, 0755);
                    if (localFd < 0) {
                        err = errno;
                        FALCON_LOG(LOG_ERROR)
                            << "OpenFile(): create local cache file " << fileName << " failed: " << strerror(err);
                        return -err;
                    }
                    openInstance->physicalFd = static_cast<uint64_t>(localFd);
                    /* here insert the new file to disk cache and pin, visible to other user */
                    if (openInstance->originalSize == 0 || (openInstance->oflags & O_CREAT) != 0) {
                        DiskCache::GetInstance().InsertAndUpdate(openInstance->inodeId, 0, true);
                    }
                    FALCON_LOG(LOG_INFO) << "OpenFile(): create local cache file " << fileName
                                         << " , fd = " << openInstance->physicalFd;
                } else {
                    /* Cache Miss: RD case, background load file from obs */
                    if (!persistToStorage) {
                        if (access(fileName.c_str(), F_OK) == 0) {
                            FALCON_LOG(LOG_ERROR) << "OpenFile(): cache file " << fileName
                                                  << " missing in diskCache but exists in ext for read";
                        } else {
                            FALCON_LOG(LOG_ERROR) << "OpenFile(): cache file " << fileName << " missing in read";
                        }
                        return -ENOENT;
                    }
                    /* only trigger the download, do not wait for it */
                    ret = DownLoadFromStorage(openInstance, false);
                    if (ret != 0) {
                        return ret;
                    }
                }
            }
        }
        openInstance->writeStream.SetInodeId(openInstance->inodeId);
        openInstance->writeStream.SetDirect(openInstance->oflags & __O_DIRECT);
        ret =
            openInstance->writeStream.SetFd(openInstance->physicalFd); // set physicalFd in local, or falconFd in remote
    }
    return ret;
}

/*
 * Called by OpenFile and ReadSmallFile, sync or async load obs.
 * if toBuffer == true, isSync should be true, or buffer useless
 */
int FalconStore::DownLoadFromStorage(OpenInstance *openInstance, bool isSync, bool toBuffer)
{
    std::string path = openInstance->path;
    uint64_t inodeId = openInstance->inodeId;
    uint64_t fileSize = openInstance->originalSize;
    std::string fileName = GetFilePath(openInstance->inodeId);
    std::shared_ptr<char> readBuffer = openInstance->readBuffer;
    size_t bufSize = openInstance->readBufferSize;

    /* isSync == true will wait to get file lock. or it will try to get lock */
    auto lockerPtr = std::make_shared<FileLocker>(&fileLock, inodeId, LockMode::X, isSync);
    if (lockerPtr == nullptr) {
        return -ENOMEM;
    }
    if (!lockerPtr->isLocked()) {
        FALCON_LOG(LOG_INFO) << "DownLoadFromStorage(): No need to load obs, other acquired the lock, abort";
        return 0;
    }

    /* if file exists in disk cache, pin directly if it is sync */
    if (DiskCache::GetInstance().Find(openInstance->inodeId, isSync)) {
        FALCON_LOG(LOG_INFO) << "DownLoadFromStorage(): No need to load obs, other created local file, abort";
        return 0;
    }

    if (!DiskCache::GetInstance().PreAllocSpace(fileSize)) {
        FALCON_LOG(LOG_ERROR) << "DownLoadFromStorage(): Can not pre-allocate enough space!";
        return -ENOSPC;
    }

    /* here cache file must not exist */
    auto fd = open(fileName.c_str(), O_WRONLY | O_CREAT, 0755);
    if (fd < 0) {
        int err = errno;
        FALCON_LOG(LOG_ERROR) << "DownLoadFromStorage(): Create local file for loading failed: " << strerror(err);
        DiskCache::GetInstance().FreePreAllocSpace(fileSize);
        return -err;
    }

    /* pass a copy of shared_ptr to make sure destructed */
    auto loadObs = [=, this]() {
        int size = 0;
        if (toBuffer) {
            size = storage->ReadObject(path.substr(1), 0, bufSize, fd, readBuffer.get());
        } else {
            size = storage->ReadObject(path.substr(1), 0, 0, fd, nullptr);
        }

        close(fd);
        if (size < 0) {
            FALCON_LOG(LOG_ERROR) << "DownLoadFromStorage(): Loading file from obs failed";
            if (std::remove(fileName.c_str()) != 0) {
                FALCON_LOG(LOG_ERROR) << "DownLoadFromStorage(): Delete obs tmp file failed" << strerror(errno);
            }
            size = -EIO;
        } else {
            DiskCache::GetInstance().InsertAndUpdate(inodeId, fileSize, isSync);
        }
        DiskCache::GetInstance().FreePreAllocSpace(fileSize);
        return size < 0 ? size : 0;
    };

    if (isSync) {
        return loadObs();
    } else {
        storeThreadPool->Submit({.taskName = "", .task = loadObs});
    }

    return 0;
}

/*
 * Called by OpenFile and ReadSmallFile. Large file try open and return, small file read obs if failed
 */
int FalconStore::OpenFileFromRemote(OpenInstance *openInstance, bool largeFile)
{
    ssize_t ret = EHOSTUNREACH;
    int nodeCnt = StoreNode::GetInstance()->GetNumberofAllNodes();
    std::shared_ptr<FalconIOClient> falconIOClient = nullptr;

    /* Loop on connection error, switch node after that */
    int retryNum = BRPC_RETRY_NUM;
    for (int i = 0; i < nodeCnt && ConnectionError(ret) && retryNum > 0; i++) {
        falconIOClient = StoreNode::GetInstance()->GetRpcConnection(openInstance->nodeId);
        if (falconIOClient) {
            if (largeFile) {
                ret = falconIOClient->OpenFile(openInstance->inodeId,
                                               openInstance->oflags,
                                               openInstance->physicalFd,
                                               openInstance->originalSize,
                                               openInstance->path,
                                               openInstance->nodeFail);
            } else {
                ret = falconIOClient->ReadSmallFile(openInstance->inodeId,
                                                    openInstance->originalSize,
                                                    openInstance->path,
                                                    openInstance->readBuffer.get(),
                                                    openInstance->oflags,
                                                    openInstance->nodeFail);
            }
        }
        if (ConnectionError(ret)) {
            if (ret != ETIMEDOUT) {
                StoreNode::GetInstance()->DeleteNode(openInstance->nodeId);
            }
            if (retryNum > 0 && ret == ETIMEDOUT) {
                sleep(BRPC_RETRY_DELEY);
                FALCON_LOG(LOG_ERROR) << "Reach timeout, retry num is " << i;
                retryNum--;
                continue;
            }
            /* in inference scenario, do not switch node in non-create case */
            if (!persistToStorage && (openInstance->oflags & O_CREAT) == 0) {
                break;
            }
            auto backupNode = openInstance->nodeId;
            openInstance->nodeId = StoreNode::GetInstance()->AllocNode(openInstance->inodeId);
            FALCON_LOG(LOG_WARNING) << "OpenFileFromRemote(): failed at node  " << backupNode << " , switch "
                                    << openInstance->nodeId;
            openInstance->nodeFail = true;
            if (isInference) {
                nodeHash[GetParentPath(openInstance->path, parentPathLevel)] = openInstance->nodeId;
            }
            if (StoreNode::GetInstance()->IsLocal(openInstance->nodeId)) {
                return OpenFile(openInstance);
            }
        }
    }

    /* Success for all file, Or failed for large file */
    if (ret == 0 || largeFile) {
        if (ConnectionError(ret)) {
            FALCON_LOG(LOG_ERROR) << "OpenFileFromRemote(): connection failed for all nodes";
        } else if (IoError(ret)) {
            FALCON_LOG(LOG_ERROR) << "OpenFileFromRemote(): open remote file " << openInstance->path
                                  << " failed: " << strerror(-ret) << ", for node " << openInstance->nodeId;
        } else {
            openInstance->writeStream.SetClient(falconIOClient);
        }
        return ret > 0 ? -ret : ret;
    }

    /* Any error for small file, read obs itself */
    if (persistToStorage) {
        FALCON_LOG(LOG_WARNING) << "OpenFileFromRemote(): small read remote failed, read obs instead";
        ret = storage->ReadObject(openInstance->path.substr(1),
                                  0,
                                  openInstance->readBufferSize,
                                  -1,
                                  openInstance->readBuffer.get());
        if (ret < 0) {
            FALCON_LOG(LOG_ERROR) << "OpenFileFromRemote(): obs ReadObject() " << openInstance->path << " failed";
            return -EIO;
        }
        ret = 0;
    } else {
        FALCON_LOG(LOG_ERROR) << "OpenFileFromRemote(): small read file remote failed";
    }

    return ret > 0 ? -ret : ret;
}

/*---------------------- close ----------------------*/

/*
 * Called by flush/close on large file or any file after write
 */
int FalconStore::CloseTmpFiles(OpenInstance *openInstance, bool isFlush, bool isSync)
{
    FALCON_LOG(LOG_INFO) << "FalconStore::CloseTmpFiles() called to " << (isFlush ? "flush" : "close") << " file "
                         << openInstance->path;

    /* physical file should be opened after RW */
    if (openInstance->physicalFd == UINT64_MAX) {
        FALCON_LOG(LOG_WARNING) << "FalconStore::CloseTmpFiles() fd not set";
        return 0;
    }
    /* nodeId should be allocated after RW */
    if (openInstance->nodeId == -1) {
        FALCON_LOG(LOG_WARNING) << "FalconStore::CloseTmpFiles() nodeId not set";
        return 0;
    }
    /* Above only happens if open failed */

    /* close may be called without flush */
    int ret = 0;
    if (isFlush) {
        openInstance->isFlushed = true;
    } else if (!openInstance->isFlushed) {
        FALCON_LOG(LOG_WARNING) << "CloseTmpFiles(): close called without flush";
        ret = CloseTmpFiles(openInstance, true, isSync);
        if (ret != 0) {
            FALCON_LOG(LOG_ERROR) << "CloseTmpFiles(): call flush in close failed";
            openInstance->writeFail = true;
        }
    }

    /* stop the possible preRead thread */
    if (!isFlush && !openInstance->isRemoteCall) {
        StopPreReadThreaded(openInstance);
        openInstance->readStream.WaitPushEnded();
    }

    /* first persist the writeStream, then rpc call remote to flush or close */
    if (!openInstance->isRemoteCall) {
        int completeRet = openInstance->writeStream.Complete(openInstance->currentSize.load(), isFlush, isSync);
        if (completeRet != 0) {
            FALCON_LOG(LOG_ERROR) << "In FalconStore::CloseTmpFiles() call complete() failed for node "
                                  << openInstance->nodeId;
            openInstance->writeFail = true;
            ret = completeRet;
        }
    }

    /* local cache file on this node */
    if (StoreNode::GetInstance()->IsLocal(openInstance->nodeId)) {
        /* close file */
        if (!isFlush) {
            close(openInstance->physicalFd);
            DiskCache::GetInstance().Unpin(openInstance->inodeId);
            return ret;
        }
        /* flush file */
        /* update diskcache file size, do not pin */
        DiskCache::GetInstance().InsertAndUpdate(openInstance->inodeId, openInstance->currentSize, false);
        if (openInstance->writeCnt > 0 && !openInstance->writeFail) {
            if (isSync) {
                fsync(openInstance->physicalFd);
                FALCON_LOG(LOG_INFO) << "CloseTmpFiles(): file " << openInstance->path << " fsync-ed";
            }
            /* flush file to storage, e.g. obs */
            if (persistToStorage) {
                ret = FlushToStorage(openInstance->path, openInstance->inodeId);
                openInstance->writeFail = (ret != 0);
            }
        }
    }
    return ret;
}

int FalconStore::FlushToStorage(std::string path, uint64_t inodeId)
{
    int ret = 0;
    std::string object = path.substr(1);
    std::string localFile = GetFilePath(inodeId);

    ret = storage->PutFile(object, localFile);
    if (ret == 0) {
        FALCON_LOG(LOG_INFO) << "Flush file " << object << " to obs succeeded!";
    } else {
        FALCON_LOG(LOG_ERROR) << "Flush file " << object << " to obs failed!";
    }
    return ret == 0 ? ret : -EIO;
}

/*---------------------- small file open ----------------------*/

/*
 *This function is called on open, and will be called only once in each IO procedure.
 * File on local node, if failed read obs and bg load obs
 * File on remote node, if failed read obs
 * File on local node but called by rpc server, if failed bg load obs and return failure
 */
int FalconStore::ReadSmallFiles(OpenInstance *openInstance)
{
    uint64_t inodeId = openInstance->inodeId;
    size_t bufSize = openInstance->readBufferSize;
    char *readBuffer = openInstance->readBuffer.get();
    std::string path = openInstance->path;
    int ret = 0;

    /* nodeId of a new file is allocated */
    AllocNodeId(openInstance);

    /* File resides on remote node */
    if (!StoreNode::GetInstance()->IsLocal(openInstance->nodeId)) {
        bool largeFile = false;
        return OpenFileFromRemote(openInstance, largeFile);
    }

    /* File resides on local node */
    std::string fileName = GetFilePath(inodeId);

    if (openInstance->nodeFail) {
        DiskCache::GetInstance().DeleteOldCacheWithNoPin(inodeId);
    }
    /* Check if in disk cache. True then pin the file */
    if (DiskCache::GetInstance().Find(inodeId, true)) {
        /* Cache Hit: read whole file to read buffer */
        int localFd = open(fileName.c_str(), O_RDONLY);
        if (localFd < 0) {
            int err = errno;
            FALCON_LOG(LOG_ERROR) << "ReadSmallFiles(): open file failed : " << fileName << " << " << strerror(errno);
            DiskCache::GetInstance().Unpin(inodeId);
            return -err;
        }
        FalconStats::GetInstance().stats[BLOCKCACHE_READ] += bufSize;
        ssize_t retSize = pread(localFd, readBuffer, bufSize, 0);
        if (retSize != (ssize_t)bufSize) {
            int err = errno;
            if (err == EAGAIN) {
                retSize = pread(localFd, readBuffer, bufSize, 0);
                if (retSize == (ssize_t)bufSize) {
                    close(localFd);
                    DiskCache::GetInstance().Unpin(inodeId);
                    return 0;
                }
                err = errno;
            }
            FALCON_LOG(LOG_ERROR) << "ReadSmallFiles(): Pread size is not equal to size: " << strerror(err);
            close(localFd);
            DiskCache::GetInstance().Unpin(inodeId);
            return -err;
        }
        close(localFd);
        /* unpin the file after close */
        DiskCache::GetInstance().Unpin(inodeId);
    } else {
        /* Cache Miss: load file from obs */
        if (!persistToStorage) {
            FALCON_LOG(LOG_ERROR) << "ReadSmallFiles(): no local cache exists";
            return -ENOENT;
        }

        /* may write, sync download file from obs to file and buffer */
        if ((openInstance->oflags & O_ACCMODE) != O_RDONLY) {
            FALCON_LOG(LOG_INFO) << "ReadSmallFiles(): may write, sync download file from obs to file and buffer";
            bool isSync = true;
            bool toBuffer = true;
            ret = DownLoadFromStorage(openInstance, isSync, toBuffer);
            return ret;
        }

        /* O_RDONLY, no need to wait for cache ready */
        /* Call is from rpc server. Async load obs and Return err to let caller read obs to buffer itself */
        if (openInstance->isRemoteCall) {
            FALCON_LOG(LOG_INFO) << "ReadSmallFiles(): remote call, bg load obs and return failure";
            // bg load obs and return failure
            bool isSync = false;
            ret = DownLoadFromStorage(openInstance, isSync);
            return ret == 0 ? -ENOENT : ret;
        }

        /* Call is from fuse user. Sync read obs to buffer and Async write to local file */
        /* Sync read obs to read buffer */
        ret = storage->ReadObject(path.substr(1), 0, bufSize, -1, readBuffer);
        if (ret < 0) {
            FALCON_LOG(LOG_ERROR) << "Obs read failed";
            return -EIO;
        }
        /* Async write to local cache file */
        /* Read buffer is read only after initialization above */
        return WriteToFileAsync(inodeId, fileName, openInstance->readBuffer, bufSize);
    }
    return 0;
}

/*
 * Called by OpenFile and ReadSmallFile. Large file try open and return, small file read obs if failed
 * Use a shared_ptr from read buffer to store the file content
 */
int FalconStore::WriteToFileAsync(uint64_t inodeId, std::string &fileName, std::shared_ptr<char> buf, size_t bufSize)
{
    auto lockerPtr = std::make_shared<FileLocker>(&fileLock, inodeId, LockMode::X, false);
    if (lockerPtr == nullptr) {
        return -ENOMEM;
    }
    if (!lockerPtr->isLocked()) {
        FALCON_LOG(LOG_INFO) << "WriteToFileAsync(): No need to write local file, other acquired the lock, abort";
        return 0;
    }

    if (DiskCache::GetInstance().Find(inodeId, false)) {
        FALCON_LOG(LOG_INFO) << "WriteToFileAsync(): No need to write local file, other created local file, abort";
        return 0;
    }
    if (!DiskCache::GetInstance().PreAllocSpace(bufSize)) {
        FALCON_LOG(LOG_ERROR) << "WriteToFileAsync(): Can not pre-allocate enough space!";
        return -ENOSPC;
    }

    /* Cache file must not exist. Create it */
    auto fd = open(fileName.c_str(), O_WRONLY | O_CREAT, 0755);
    if (fd < 0) {
        int err = errno;
        FALCON_LOG(LOG_ERROR) << "WriteToFileAsync(): open file failed : " << fileName << ", " << strerror(errno);
        DiskCache::GetInstance().FreePreAllocSpace(bufSize);
        return -err;
    }

    /* Async write the file to local file */
    ThreadTask task;
    task.task = [fd, buf, bufSize, inodeId, lockerPtr]() {
        FalconStats::GetInstance().stats[BLOCKCACHE_WRITE] += bufSize;
        int retSize = pwrite(fd, buf.get(), bufSize, 0);
        int err = errno;
        close(fd);
        if (retSize < 0) {
            FALCON_LOG(LOG_ERROR) << "WriteToFileAsync(): pwrite failed : " << strerror(err);
        } else {
            DiskCache::GetInstance().InsertAndUpdate(inodeId, bufSize, false);
        }
        DiskCache::GetInstance().FreePreAllocSpace(bufSize);
    };

    storeThreadPool->Submit(task);

    return 0;
}

/*
 * Called by brpc server only
 */
int FalconStore::ReadSmallFilesForBrpc(uint64_t inodeId,
                                       const std::string &path,
                                       char *buf,
                                       size_t size,
                                       int oflags,
                                       bool nodeFail)
{
    int ret = 0;

    /* File resides on local node */
    std::string fileName = GetFilePath(inodeId);
    /* Check if in disk cache. True then pin the file */
    if (nodeFail) {
        DiskCache::GetInstance().DeleteOldCacheWithNoPin(inodeId);
    }

    if (DiskCache::GetInstance().Find(inodeId, true)) {
        /* Cache Hit: read whole file to read buffer */
        int localFd = open(fileName.c_str(), O_RDONLY);
        if (localFd < 0) {
            int err = errno;
            FALCON_LOG(LOG_ERROR) << "ReadSmallFilesForBrpc(): open file " << fileName
                                  << " failed : " << strerror(errno);
            DiskCache::GetInstance().Unpin(inodeId);
            return -err;
        }
        FalconStats::GetInstance().stats[BLOCKCACHE_READ] += size;
        ssize_t retSize = pread(localFd, buf, size, 0);
        if (retSize != (ssize_t)size) {
            int err = errno;
            if (err == EAGAIN) {
                retSize = pread(localFd, buf, size, 0);
                if (retSize == (ssize_t)size) {
                    close(localFd);
                    DiskCache::GetInstance().Unpin(inodeId);
                    return 0;
                }
                err = errno;
            }
            FALCON_LOG(LOG_ERROR) << "ReadSmallFilesForBrpc(): Pread size not equal: " << strerror(err);
            close(localFd);
            DiskCache::GetInstance().Unpin(inodeId);
            return -err;
        }
        close(localFd);
        /* unpin the file after close */
        DiskCache::GetInstance().Unpin(inodeId);
    } else {
        /* Cache Miss: load file from obs */
        if (!persistToStorage) {
            FALCON_LOG(LOG_ERROR) << "ReadSmallFilesForBrpc(): no local cache exists";
            return -ENOENT;
        }

        /* may write, sync download file from obs to file and buffer */
        if ((oflags & O_ACCMODE) != O_RDONLY) {
            FALCON_LOG(LOG_INFO)
                << "ReadSmallFilesForBrpc(): may write, sync download file from obs to file and buffer";
            bool isSync = true;
            bool toBuffer = true;
            ret = DownLoadFromStorageForBrpc(inodeId, path, buf, size, isSync, toBuffer);
            return ret;
        }

        /* O_RDONLY, no need to wait for cache ready */
        /* Async load obs and Return err to let caller read obs to buffer itself */
        FALCON_LOG(LOG_INFO) << "ReadSmallFilesForBrpc(): remote call, bg load obs and return failure";
        // bg load obs and return failure
        bool isSync = false;
        ret = DownLoadFromStorageForBrpc(inodeId, path, buf, size, isSync, false);
        return ret == 0 ? -ENOENT : ret;
    }
    return ret;
}

/*
 * Used by brpc server only, called by ReadSmallFilesForBrpc
 */
int FalconStore::DownLoadFromStorageForBrpc(uint64_t inodeId,
                                            const std::string &path,
                                            char *buf,
                                            size_t bufSize,
                                            bool isSync,
                                            bool toBuffer)
{
    std::string fileName = GetFilePath(inodeId);

    /* isSync == true will wait to get file lock. or it will try to get lock */
    auto lockerPtr = std::make_shared<FileLocker>(&fileLock, inodeId, LockMode::X, isSync);
    if (lockerPtr == nullptr) {
        return -ENOMEM;
    }
    if (!lockerPtr->isLocked()) {
        FALCON_LOG(LOG_INFO) << "DownLoadFromStorage(): No need to load obs, other acquired the lock, abort";
        return 0;
    }

    /* if file exists in disk cache, pin directly if it is sync */
    if (DiskCache::GetInstance().Find(inodeId, isSync)) {
        FALCON_LOG(LOG_INFO) << "DownLoadFromStorage(): No need to load obs, other created local file, abort";
        return 0;
    }

    if (!DiskCache::GetInstance().PreAllocSpace(bufSize)) {
        FALCON_LOG(LOG_ERROR) << "DownLoadFromStorage(): Can not pre-allocate enough space!";
        return -ENOSPC;
    }

    /* here cache file must not exist */
    auto fd = open(fileName.c_str(), O_WRONLY | O_CREAT, 0755);
    if (fd < 0) {
        int err = errno;
        FALCON_LOG(LOG_ERROR) << "DownLoadFromStorage(): Create local file for loading failed: " << strerror(err);
        DiskCache::GetInstance().FreePreAllocSpace(bufSize);
        return -err;
    }

    /* pass a copy of shared_ptr to make sure destructed */
    auto loadObs = [=, this]() {
        int size = 0;
        if (toBuffer) {
            size = storage->ReadObject(path.substr(1), 0, bufSize, fd, buf);
        } else {
            size = storage->ReadObject(path.substr(1), 0, 0, fd, nullptr);
        }

        close(fd);
        if (size < 0) {
            FALCON_LOG(LOG_ERROR) << "DownLoadFromStorage(): Loading file from obs failed";
            if (std::remove(fileName.c_str()) != 0) {
                FALCON_LOG(LOG_ERROR) << "DownLoadFromStorage(): Delete obs tmp file failed" << strerror(errno);
            }
            size = -EIO;
        } else {
            DiskCache::GetInstance().InsertAndUpdate(inodeId, bufSize, isSync);
        }
        DiskCache::GetInstance().FreePreAllocSpace(bufSize);
        return size < 0 ? size : 0;
    };

    if (isSync) {
        return loadObs();
    } else {
        storeThreadPool->Submit({.taskName = "", .task = loadObs});
    }

    return 0;
}

/*---------------------- other func ----------------------*/

int FalconStore::DeleteFiles(uint64_t inodeId, int nodeId, std::string path)
{
    int ret = 0;
    if (nodeId == -1 || StoreNode::GetInstance()->IsLocal(nodeId)) {
        if (DiskCache::GetInstance().Find(inodeId, false)) {
            ret = DiskCache::GetInstance().Delete(inodeId);
            if (ret != 0) {
                return ret;
            }
        } else if (!persistToStorage) {
            FALCON_LOG(LOG_ERROR) << "Delete file " << GetFilePath(inodeId) << " failed : " << strerror(ENOENT);
            return -ENOENT;
        }
    } else {
        /* remote file to delete */
        std::shared_ptr<FalconIOClient> falconIOClient = StoreNode::GetInstance()->GetRpcConnection(nodeId);
        if (falconIOClient != nullptr) {
            ret = falconIOClient->DeleteFile(inodeId, nodeId, path);
            if (ret != 0) {
                FALCON_LOG(LOG_ERROR) << "Delete remote file failed : " << strerror(-ret) << ", for node " << nodeId;
            } else {
                return ret;
            }
        } else {
            return -EHOSTUNREACH;
        }
    }

    if (persistToStorage) {
        ret = storage->DeleteObject(path.substr(1));
        if (ret != 0) {
            FALCON_LOG(LOG_ERROR) << "delete file from obs failed! ";
            return -EIO;
        }
    }
    return ret;
}

int FalconStore::StatFS(struct statvfs *vfsbuf)
{
    if (persistToStorage) {
        return StatFsStorage(vfsbuf);
    }

    // statfs locally
    int ret = statvfs(dataPath.c_str(), vfsbuf);
    if (ret != 0) {
        int err = errno;
        FALCON_LOG(LOG_ERROR) << "StatFS failed: " << strerror(errno);
        return -err;
    }
    // statfs remotelly by grpc
    std::vector<int> nodeVector = StoreNode::GetInstance()->GetAllNodeId();
    for (int nodeId : nodeVector) {
        if (StoreNode::GetInstance()->IsLocal(nodeId)) {
            continue;
        }
        std::shared_ptr<FalconIOClient> falconIOClient = StoreNode::GetInstance()->GetRpcConnection(nodeId);
        // falconIOClient shouldn't be null
        if (falconIOClient != nullptr) {
            struct StatFSBuf remoteVfsBuf;
            errno_t err = memset_s(&remoteVfsBuf, sizeof(remoteVfsBuf), 0, sizeof(remoteVfsBuf));
            if (err != 0) {
                FALCON_LOG(LOG_ERROR) << "Secure func failed: " << err;
                return -EIO;
            }
            std::string rpcEndpoint = StoreNode::GetInstance()->GetRpcEndPoint(nodeId);
            ret = falconIOClient->StatFS(rpcEndpoint, &remoteVfsBuf);
            if (ret != 0) {
                FALCON_LOG(LOG_ERROR) << "statFS remote failed: " << strerror(-ret) << ", for node " << nodeId;
                continue;
            }
            vfsbuf->f_blocks += remoteVfsBuf.f_blocks;
            vfsbuf->f_bfree += remoteVfsBuf.f_bfree;
            vfsbuf->f_bavail += remoteVfsBuf.f_bavail;
            vfsbuf->f_files += remoteVfsBuf.f_files;
            vfsbuf->f_ffree += remoteVfsBuf.f_ffree;
        } else {
            return -EHOSTUNREACH;
        }
    }
    return 0;
}

int FalconStore::StatFsStorage(struct statvfs *vfsbuf) { return storage->StatFs(vfsbuf); }

int FalconStore::StatFSForBrpc(const std::string &path,
                               uint64_t &fblocks,
                               uint64_t &fbfree,
                               uint64_t &fbavail,
                               uint64_t &ffiles,
                               uint64_t &fffree)
{
    if (!StoreNode::GetInstance()->IsLocal(path)) {
        return 0;
    }
    struct statvfs vfsBuf;
    errno_t err = memset_s(&vfsBuf, sizeof(vfsBuf), 0, sizeof(vfsBuf));
    if (err != 0) {
        FALCON_LOG(LOG_ERROR) << "Secure func failed: " << err;
        return -EIO;
    }
    int ret = statvfs(dataPath.c_str(), &vfsBuf);
    if (ret == 0) {
        fblocks = vfsBuf.f_blocks;
        fbfree = vfsBuf.f_bfree;
        fbavail = vfsBuf.f_bavail;
        ffiles = vfsBuf.f_files;
        fffree = vfsBuf.f_ffree;
    } else {
        ret = -errno;
        FALCON_LOG(LOG_ERROR) << "statfs failed : " << strerror(errno);
    }
    return ret;
}

int FalconStore::CopyData(const std::string &srcName, const std::string &dstName)
{
    std::string srcObject = srcName.substr(1);
    std::string dstObject = dstName.substr(1);
    return storage->CopyObject(srcObject, dstObject);
}

int FalconStore::DeleteDataAfterRename(const std::string &objectName)
{
    return storage->DeleteObject(objectName.substr(1));
}

/* O_TRUNC must be called with O_WR */
int FalconStore::TruncateFile(OpenInstance *openInstance, off_t size)
{
    FALCON_LOG(LOG_INFO) << "FalconStore::TruncateFile() called by " << (openInstance->isRemoteCall ? "rpc" : "fuse")
                         << " on " << openInstance->path << " to size = " << size;
    int ret = 0;

    /* open file, init physical fd */
    if (!openInstance->isOpened.load()) {
        std::unique_lock<std::shared_mutex> openLock(openInstance->fileMutex);
        ret = OpenFile(openInstance);
        if (ret != 0) {
            FALCON_LOG(LOG_ERROR) << "In TruncateFile(): OpenFile() failed";
            return ret;
        }
        openInstance->isOpened = true;
    }

    if (StoreNode::GetInstance()->IsLocal(openInstance->nodeId)) {
        ret = ftruncate(openInstance->physicalFd, size);
        if (ret != 0) {
            int err = errno;
            std::string fileName = GetFilePath(openInstance->inodeId);
            FALCON_LOG(LOG_ERROR) << "Truncate file " << fileName << " failed : " << strerror(err);
            return -err;
        }
    } else {
        /* remote file to truncate */
        std::shared_ptr<FalconIOClient> falconIOClient =
            StoreNode::GetInstance()->GetRpcConnection(openInstance->nodeId);
        if (falconIOClient != nullptr) {
            ret = falconIOClient->TruncateFile(openInstance->physicalFd, size);
            if (ret != 0) {
                FALCON_LOG(LOG_ERROR) << "Truncate remote file failed : " << strerror(-ret) << ", for node "
                                      << openInstance->nodeId;
                return ret;
            }
        } else {
            return -EHOSTUNREACH;
        }
    }
    return ret;
}

/* truncate openInstance only, which means change size in memory not cache file */
int FalconStore::TruncateOpenInstance(OpenInstance *openInstance, off_t size)
{
    int ret = 0;

    // persist the current write stream to let currentSize updated
    if (openInstance->writeStream.GetSize() > 0) {
        // write will wait for local cache to be loaded from obs, so safe to call complete
        FALCON_LOG(LOG_INFO) << "In TruncateOpenInstance(): Persisting the written";
        ret = openInstance->writeStream.Complete(openInstance->currentSize.load(), true, false);
        if (ret != 0) {
            FALCON_LOG(LOG_ERROR) << "In TruncateOpenInstance(): persist written before truncate failed";
            return ret;
        }
    }

    if (!StoreNode::GetInstance()->IsLocal(openInstance->nodeId) && openInstance->isOpened.load()) {
        // remote openInstance to truncate
        std::shared_ptr<FalconIOClient> falconIOClient =
            StoreNode::GetInstance()->GetRpcConnection(openInstance->nodeId);
        if (falconIOClient != nullptr) {
            int ret = falconIOClient->TruncateOpenInstance(openInstance->physicalFd, size);
            if (ret != 0) {
                FALCON_LOG(LOG_ERROR) << "Truncate remote openInstance failed : " << strerror(-ret) << ", for node "
                                      << openInstance->nodeId;
                return ret;
            }
        }
    }

    // truncate openInstance, update both sizes to let future write reflect meta size
    std::unique_lock<std::shared_mutex> sizeLock(openInstance->fileMutex);
    openInstance->currentSize = size;
    openInstance->originalSize = size;
    return 0;
}

int FalconStore::StatCluster(int nodeId, std::vector<size_t> &currentStats, bool scatter)
{
    if (nodeId != -1 && !StoreNode::GetInstance()->IsLocal(nodeId)) {
        return 0;
    }

    /* Get local stats */
    currentStats.clear();
    currentStats.resize(STATS_END);
    currentStats.assign(FalconStats::GetInstance().storedStats, FalconStats::GetInstance().storedStats + STATS_END);
    if (!scatter) {
        return 0;
    }

    /* Get remote stats */
    auto nodeVector = StoreNode::GetInstance()->GetAllNodeId();
    for (auto nodeId : nodeVector) {
        if (StoreNode::GetInstance()->IsLocal(nodeId)) {
            continue;
        }
        auto ioClient = StoreNode::GetInstance()->GetRpcConnection(nodeId);
        int ret = -EHOSTUNREACH;
        std::vector<size_t> remoteStats(STATS_END);
        if (ioClient != nullptr) {
            ret = ioClient->StatCluster(nodeId, remoteStats, false);
        }
        if (ret != 0) {
            FALCON_LOG(LOG_WARNING) << "StatCluster rpc failed: " << strerror(-ret) << " for node " << nodeId;
            continue;
        }
        for (int i = FUSE_OPS; i < OPS_END; i++) {
            currentStats[i] += remoteStats[i];
        }
        for (int i = FUSE_LAT; i < LAT_END; i += 2) {
            currentStats[i] += remoteStats[i];
        }
        for (int i = FUSE_LAT_MAX; i < LAT_END; i += 2) {
            currentStats[i] = std::max(currentStats[i], remoteStats[i]);
        }
        for (int i = FUSE_READ; i < STATS_END; i++) {
            currentStats[i] += remoteStats[i];
        }
    }
    return 0;
}