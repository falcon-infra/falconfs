#ifndef TESTS_METADATA_UT_METADATA_UT_CLIENT_H
#define TESTS_METADATA_UT_METADATA_UT_CLIENT_H

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <sys/types.h>
#include <vector>

#include <sys/stat.h>

#include "connection.h"
#include "remote_connection_utils/error_code_def.h"
#include "router.h"

class MetadataUtClient {
  public:
    struct KvSlice {
        uint64_t valueKey;
        uint64_t location;
        uint32_t size;
    };

    struct KvValue {
        uint32_t valueLen = 0;
        uint16_t sliceNum = 0;
        std::vector<KvSlice> slices;
    };

    struct SliceValue {
        uint32_t sliceNum = 0;
        std::vector<uint64_t> inodeId;
        std::vector<uint32_t> chunkId;
        std::vector<uint64_t> sliceId;
        std::vector<uint32_t> sliceSize;
        std::vector<uint32_t> sliceOffset;
        std::vector<uint32_t> sliceLen;
        std::vector<uint32_t> sliceLoc1;
        std::vector<uint32_t> sliceLoc2;
    };

    bool Init(const std::string &serverIp, int serverPort, int clientNumber, std::string *errorMessage);
    void Shutdown();

    FalconErrorCode Mkdir(const std::string &path);
    FalconErrorCode OpenDir(const std::string &path, uint64_t *inodeId = nullptr);
    FalconErrorCode ReadDir(const std::string &path, std::vector<std::string> *entries);
    FalconErrorCode Rmdir(const std::string &path);
    FalconErrorCode Create(const std::string &path, uint64_t *inodeId = nullptr, int32_t *nodeId = nullptr);
    FalconErrorCode Stat(const std::string &path, struct stat *stbuf);
    FalconErrorCode Open(const std::string &path,
                         uint64_t *inodeId,
                         int64_t *size,
                         int32_t *nodeId,
                         struct stat *stbuf);
    FalconErrorCode Close(const std::string &path, int64_t size, uint64_t mtime, int32_t nodeId);
    FalconErrorCode Unlink(const std::string &path);
    FalconErrorCode Rename(const std::string &src, const std::string &dst);
    FalconErrorCode UtimeNs(const std::string &path, int64_t atime, int64_t mtime);
    FalconErrorCode Chown(const std::string &path, uid_t uid, gid_t gid);
    FalconErrorCode Chmod(const std::string &path, mode_t mode);

    FalconErrorCode KvPut(const std::string &key,
                          uint32_t valueLen,
                          const std::vector<uint64_t> &valueKey,
                          const std::vector<uint64_t> &location,
                          const std::vector<uint32_t> &size);
    FalconErrorCode KvGet(const std::string &key, KvValue *value);
    FalconErrorCode KvDel(const std::string &key);

    FalconErrorCode FetchSliceId(uint32_t count, uint64_t *startId, uint64_t *endId, uint8_t type = 0);
    FalconErrorCode SlicePut(const std::string &filename,
                             const std::vector<uint64_t> &inodeId,
                             const std::vector<uint32_t> &chunkId,
                             const std::vector<uint64_t> &sliceId,
                             const std::vector<uint32_t> &sliceSize,
                             const std::vector<uint32_t> &sliceOffset,
                             const std::vector<uint32_t> &sliceLen,
                             const std::vector<uint32_t> &sliceLoc1,
                             const std::vector<uint32_t> &sliceLoc2);
    FalconErrorCode SliceGet(const std::string &filename, uint64_t inodeId, uint32_t chunkId, SliceValue *value);
    FalconErrorCode SliceDel(const std::string &filename, uint64_t inodeId, uint32_t chunkId);

  private:
    std::shared_ptr<Router> NextRouter();
    std::shared_ptr<Connection> CoordinatorConn();
    std::shared_ptr<Connection> WorkerConnByPath(const std::string &path);

    std::vector<std::shared_ptr<Router>> routers_;
    std::atomic<uint64_t> routerIndex_ = 0;
};

#endif
