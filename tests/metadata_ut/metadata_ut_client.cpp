#include "metadata_ut_client.h"

#include <algorithm>
#include <exception>
#include <limits>
#include <unordered_map>

bool MetadataUtClient::Init(const std::string &serverIp, int serverPort, int clientNumber, std::string *errorMessage)
{
    if (clientNumber <= 0) {
        if (errorMessage) {
            *errorMessage = "clientNumber must be positive";
        }
        return false;
    }

    try {
        routers_.clear();
        routers_.reserve(clientNumber);
        ServerIdentifier coordinator(serverIp, serverPort);
        for (int i = 0; i < clientNumber; ++i) {
            routers_.emplace_back(std::make_shared<Router>(coordinator));
        }
        routerIndex_.store(0, std::memory_order_relaxed);
        return true;
    } catch (const std::exception &ex) {
        if (errorMessage) {
            *errorMessage = ex.what();
        }
        routers_.clear();
        return false;
    }
}

void MetadataUtClient::Shutdown()
{
    routers_.clear();
}

std::shared_ptr<Router> MetadataUtClient::NextRouter()
{
    if (routers_.empty()) {
        return nullptr;
    }
    const uint64_t index = routerIndex_.fetch_add(1, std::memory_order_relaxed) % routers_.size();
    return routers_[index];
}

std::shared_ptr<Connection> MetadataUtClient::CoordinatorConn()
{
    auto router = NextRouter();
    return router ? router->GetCoordinatorConn() : nullptr;
}

std::shared_ptr<Connection> MetadataUtClient::WorkerConnByPath(const std::string &path)
{
    auto router = NextRouter();
    return router ? router->GetWorkerConnByPath(path) : nullptr;
}

FalconErrorCode MetadataUtClient::Mkdir(const std::string &path)
{
    auto conn = CoordinatorConn();
    return conn ? conn->Mkdir(path.c_str()) : PROGRAM_ERROR;
}

FalconErrorCode MetadataUtClient::OpenDir(const std::string &path, uint64_t *inodeId)
{
    auto conn = CoordinatorConn();
    if (!conn) {
        return PROGRAM_ERROR;
    }

    uint64_t localInodeId = 0;
    FalconErrorCode ret = conn->OpenDir(path.c_str(), localInodeId);
    if (ret == SUCCESS && inodeId) {
        *inodeId = localInodeId;
    }
    return ret;
}

FalconErrorCode MetadataUtClient::ReadDir(const std::string &path, std::vector<std::string> *entries)
{
    auto router = NextRouter();
    if (!router) {
        return PROGRAM_ERROR;
    }

    std::unordered_map<std::string, std::shared_ptr<Connection>> workerInfo;
    int ret = router->GetAllWorkerConnection(workerInfo);
    if (ret != SUCCESS) {
        return static_cast<FalconErrorCode>(ret);
    }

    if (entries) {
        entries->clear();
    }

    for (auto &[server, conn] : workerInfo) {
        int32_t lastShardIndex = -1;
        std::string lastFileName;
        while (true) {
            Connection::ReadDirResponse response;
            const char *lastFileNamePtr = lastFileName.empty() ? nullptr : lastFileName.c_str();
            ret = conn->ReadDir(path.c_str(), response, 1024, lastShardIndex, lastFileNamePtr);
            if (ret != SUCCESS) {
                return static_cast<FalconErrorCode>(ret);
            }

            auto resultList = response.response->result_list();
            if (entries && resultList) {
                entries->reserve(entries->size() + resultList->size());
                for (flatbuffers::uoffset_t i = 0; i < resultList->size(); ++i) {
                    entries->push_back(resultList->Get(i)->file_name()->str());
                }
            }

            const bool workerDone = resultList == nullptr || resultList->size() < 1024;
            if (workerDone) {
                break;
            }
            lastShardIndex = response.response->last_shard_index();
            lastFileName = response.response->last_file_name() ? response.response->last_file_name()->str() : "";
        }
    }

    return SUCCESS;
}

FalconErrorCode MetadataUtClient::Rmdir(const std::string &path)
{
    auto conn = CoordinatorConn();
    return conn ? conn->Rmdir(path.c_str()) : PROGRAM_ERROR;
}

FalconErrorCode MetadataUtClient::Create(const std::string &path, uint64_t *inodeId, int32_t *nodeId)
{
    auto conn = WorkerConnByPath(path);
    if (!conn) {
        return PROGRAM_ERROR;
    }

    uint64_t localInodeId = 0;
    int32_t localNodeId = 0;
    struct stat stbuf = {};
    FalconErrorCode ret = conn->Create(path.c_str(), localInodeId, localNodeId, &stbuf);
    if (ret == SUCCESS) {
        if (inodeId) {
            *inodeId = localInodeId;
        }
        if (nodeId) {
            *nodeId = localNodeId;
        }
    }
    return ret;
}

FalconErrorCode MetadataUtClient::Stat(const std::string &path, struct stat *stbuf)
{
    auto conn = WorkerConnByPath(path);
    return conn ? conn->Stat(path.c_str(), stbuf) : PROGRAM_ERROR;
}

FalconErrorCode MetadataUtClient::Open(const std::string &path,
                                       uint64_t *inodeId,
                                       int64_t *size,
                                       int32_t *nodeId,
                                       struct stat *stbuf)
{
    auto conn = WorkerConnByPath(path);
    if (!conn) {
        return PROGRAM_ERROR;
    }

    uint64_t localInodeId = 0;
    int64_t localSize = 0;
    int32_t localNodeId = 0;
    FalconErrorCode ret = conn->Open(path.c_str(), localInodeId, localSize, localNodeId, stbuf);
    if (ret == SUCCESS) {
        if (inodeId) {
            *inodeId = localInodeId;
        }
        if (size) {
            *size = localSize;
        }
        if (nodeId) {
            *nodeId = localNodeId;
        }
    }
    return ret;
}

FalconErrorCode MetadataUtClient::Close(const std::string &path, int64_t size, uint64_t mtime, int32_t nodeId)
{
    auto conn = WorkerConnByPath(path);
    return conn ? conn->Close(path.c_str(), size, mtime, nodeId) : PROGRAM_ERROR;
}

FalconErrorCode MetadataUtClient::Unlink(const std::string &path)
{
    auto conn = WorkerConnByPath(path);
    if (!conn) {
        return PROGRAM_ERROR;
    }

    uint64_t inodeId = 0;
    int64_t size = 0;
    int32_t nodeId = 0;
    return conn->Unlink(path.c_str(), inodeId, size, nodeId);
}

FalconErrorCode MetadataUtClient::Rename(const std::string &src, const std::string &dst)
{
    auto conn = CoordinatorConn();
    return conn ? conn->Rename(src.c_str(), dst.c_str()) : PROGRAM_ERROR;
}

FalconErrorCode MetadataUtClient::UtimeNs(const std::string &path, int64_t atime, int64_t mtime)
{
    auto conn = WorkerConnByPath(path);
    return conn ? conn->UtimeNs(path.c_str(), atime, mtime) : PROGRAM_ERROR;
}

FalconErrorCode MetadataUtClient::Chown(const std::string &path, uid_t uid, gid_t gid)
{
    auto conn = WorkerConnByPath(path);
    return conn ? conn->Chown(path.c_str(), uid, gid) : PROGRAM_ERROR;
}

FalconErrorCode MetadataUtClient::Chmod(const std::string &path, mode_t mode)
{
    auto conn = WorkerConnByPath(path);
    return conn ? conn->Chmod(path.c_str(), mode) : PROGRAM_ERROR;
}

FalconErrorCode MetadataUtClient::KvPut(const std::string &key,
                                        uint32_t valueLen,
                                        const std::vector<uint64_t> &valueKey,
                                        const std::vector<uint64_t> &location,
                                        const std::vector<uint32_t> &size)
{
    if (valueKey.size() != location.size() || valueKey.size() != size.size() ||
        valueKey.size() > std::numeric_limits<uint16_t>::max()) {
        return PROGRAM_ERROR;
    }

    auto conn = WorkerConnByPath(key);
    return conn ? conn->KvPut(key.c_str(), valueLen, static_cast<uint16_t>(valueKey.size()), valueKey, location, size)
                : PROGRAM_ERROR;
}

FalconErrorCode MetadataUtClient::KvGet(const std::string &key, KvValue *value)
{
    auto conn = WorkerConnByPath(key);
    if (!conn) {
        return PROGRAM_ERROR;
    }

    Connection::KvGetResult result;
    FalconErrorCode ret = conn->KvGet(key.c_str(), result);
    if (ret == SUCCESS && value) {
        value->valueLen = result.valueLen;
        value->sliceNum = result.sliceNum;
        value->slices.clear();
        value->slices.reserve(result.slices.size());
        for (const auto &slice : result.slices) {
            value->slices.push_back({slice.valueKey, slice.location, slice.size});
        }
    }
    return ret;
}

FalconErrorCode MetadataUtClient::KvDel(const std::string &key)
{
    auto conn = WorkerConnByPath(key);
    return conn ? conn->KvDel(key.c_str()) : PROGRAM_ERROR;
}

FalconErrorCode MetadataUtClient::FetchSliceId(uint32_t count, uint64_t *startId, uint64_t *endId, uint8_t type)
{
    auto conn = CoordinatorConn();
    if (!conn) {
        return PROGRAM_ERROR;
    }

    uint64_t localStartId = 0;
    uint64_t localEndId = 0;
    FalconErrorCode ret = conn->FetchSliceId(count, type, localStartId, localEndId);
    if (ret == SUCCESS) {
        if (startId) {
            *startId = localStartId;
        }
        if (endId) {
            *endId = localEndId;
        }
    }
    return ret;
}

FalconErrorCode MetadataUtClient::SlicePut(const std::string &filename,
                                           const std::vector<uint64_t> &inodeId,
                                           const std::vector<uint32_t> &chunkId,
                                           const std::vector<uint64_t> &sliceId,
                                           const std::vector<uint32_t> &sliceSize,
                                           const std::vector<uint32_t> &sliceOffset,
                                           const std::vector<uint32_t> &sliceLen,
                                           const std::vector<uint32_t> &sliceLoc1,
                                           const std::vector<uint32_t> &sliceLoc2)
{
    const size_t sliceNum = inodeId.size();
    if (sliceNum == 0 || sliceNum > std::numeric_limits<uint32_t>::max() ||
        chunkId.size() != sliceNum || sliceId.size() != sliceNum || sliceSize.size() != sliceNum ||
        sliceOffset.size() != sliceNum || sliceLen.size() != sliceNum || sliceLoc1.size() != sliceNum ||
        sliceLoc2.size() != sliceNum) {
        return PROGRAM_ERROR;
    }

    auto conn = WorkerConnByPath(filename);
    return conn ? conn->SlicePut(filename.c_str(),
                                 static_cast<uint32_t>(sliceNum),
                                 inodeId,
                                 chunkId,
                                 sliceId,
                                 sliceSize,
                                 sliceOffset,
                                 sliceLen,
                                 sliceLoc1,
                                 sliceLoc2)
                : PROGRAM_ERROR;
}

FalconErrorCode MetadataUtClient::SliceGet(const std::string &filename,
                                           uint64_t inodeId,
                                           uint32_t chunkId,
                                           SliceValue *value)
{
    auto conn = WorkerConnByPath(filename);
    if (!conn) {
        return PROGRAM_ERROR;
    }

    Connection::SliceGetResult result;
    FalconErrorCode ret = conn->SliceGet(filename.c_str(), inodeId, chunkId, result);
    if (ret == SUCCESS && value) {
        value->sliceNum = result.sliceNum;
        value->inodeId = result.inodeId;
        value->chunkId = result.chunkId;
        value->sliceId = result.sliceId;
        value->sliceSize = result.sliceSize;
        value->sliceOffset = result.sliceOffset;
        value->sliceLen = result.sliceLen;
        value->sliceLoc1 = result.sliceLoc1;
        value->sliceLoc2 = result.sliceLoc2;
    }
    return ret;
}

FalconErrorCode MetadataUtClient::SliceDel(const std::string &filename, uint64_t inodeId, uint32_t chunkId)
{
    auto conn = WorkerConnByPath(filename);
    return conn ? conn->SliceDel(filename.c_str(), inodeId, chunkId) : PROGRAM_ERROR;
}
