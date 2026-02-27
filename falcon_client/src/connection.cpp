/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "connection.h"

#include <memory>
#include <string>

#include <brpc/server.h>

#include "falcon_meta_param_generated.h"
#include "log/logging.h"

#ifdef S_BLKSIZE
#define ST_NBLOCKSIZE S_BLKSIZE
#else
#define ST_NBLOCKSIZE 512
#endif

#define ST_BLKSIZE 4096

#define ALLOW_BATCH_WITH_OTHERS true

static void BrpcDummyDeleter(void *) {}

inline falcon::meta_fbs::AnyMetaParam ToFlatBuffersType(falcon::meta_proto::MetaServiceType type)
{
    switch (type) {
    case falcon::meta_proto::PLAIN_COMMAND:
        return falcon::meta_fbs::AnyMetaParam_PlainCommandParam;
    case falcon::meta_proto::MKDIR:
    case falcon::meta_proto::CREATE:
    case falcon::meta_proto::STAT:
    case falcon::meta_proto::OPEN:
    case falcon::meta_proto::UNLINK:
    case falcon::meta_proto::OPENDIR:
    case falcon::meta_proto::RMDIR:
        return falcon::meta_fbs::AnyMetaParam_PathOnlyParam;
    case falcon::meta_proto::CLOSE:
        return falcon::meta_fbs::AnyMetaParam_CloseParam;
    case falcon::meta_proto::READDIR:
        return falcon::meta_fbs::AnyMetaParam_ReadDirParam;
    case falcon::meta_proto::RENAME:
        return falcon::meta_fbs::AnyMetaParam_RenameParam;
    case falcon::meta_proto::UTIMENS:
        return falcon::meta_fbs::AnyMetaParam_UtimeNsParam;
    case falcon::meta_proto::CHOWN:
        return falcon::meta_fbs::AnyMetaParam_ChownParam;
    case falcon::meta_proto::CHMOD:
        return falcon::meta_fbs::AnyMetaParam_ChmodParam;
    case falcon::meta_proto::KV_PUT:
        return falcon::meta_fbs::AnyMetaParam_KVParam;
    case falcon::meta_proto::KV_GET:
    case falcon::meta_proto::KV_DEL:
        return falcon::meta_fbs::AnyMetaParam_KeyOnlyParam;
    case falcon::meta_proto::SLICE_PUT:
        return falcon::meta_fbs::AnyMetaParam_SliceInfoParam;
    case falcon::meta_proto::SLICE_GET:
    case falcon::meta_proto::SLICE_DEL:
        return falcon::meta_fbs::AnyMetaParam_SliceIndexParam;
    case falcon::meta_proto::FETCH_SLICE_ID:
        return falcon::meta_fbs::AnyMetaParam_SliceIdParam;
    default:
        throw std::runtime_error("Unknown service type");
    }
}

template <typename ParamBuilder, typename ResponseHandler, typename ResultType>
FalconErrorCode Connection::ProcessRequest(falcon::meta_proto::MetaServiceType proto_type,
                                           const ParamBuilder &paramBuilder,
                                           ResponseHandler responseHandler,
                                           ConnectionCache *cache,
                                           ResultType *result)
{
    if (!cache)
        cache = &ThreadLocalConnectionCache;

    // 1. Prepare param
    SerializedDataClear(&cache->serializedDataBuffer);
    cache->flatBufferBuilder.Clear();
    auto type = ToFlatBuffersType(proto_type);

    auto param = paramBuilder(cache->flatBufferBuilder);
    auto metaParam = falcon::meta_fbs::CreateMetaParam(cache->flatBufferBuilder, type, param.Union());
    cache->flatBufferBuilder.Finish(metaParam);

    char *p = SerializedDataApplyForSegment(&cache->serializedDataBuffer, cache->flatBufferBuilder.GetSize());
    memcpy(p, cache->flatBufferBuilder.GetBufferPointer(), cache->flatBufferBuilder.GetSize());

    // 2. Construct request
    falcon::meta_proto::MetaRequest request;
    request.add_type(proto_type);
    if (proto_type == falcon::meta_proto::MKDIR || proto_type == falcon::meta_proto::CREATE ||
        proto_type == falcon::meta_proto::STAT || proto_type == falcon::meta_proto::OPEN ||
        proto_type == falcon::meta_proto::CLOSE || proto_type == falcon::meta_proto::UNLINK) {
        request.set_allow_batch_with_others(ALLOW_BATCH_WITH_OTHERS);
    }
    brpc::Controller cntl;
    cntl.set_timeout_ms(10000);
    cntl.request_attachment().append_user_data(cache->serializedDataBuffer.buffer,
                                               cache->serializedDataBuffer.size,
                                               BrpcDummyDeleter);

    // 3. Send request
    falcon::meta_proto::Empty dummyResponse;
    stub.MetaCall(&cntl, &request, &dummyResponse, nullptr);
    if (cntl.Failed()) {
        FALCON_LOG(LOG_ERROR) << __func__ << ": Send request failed, error code = "
                              << cntl.ErrorCode() << ", error text = " 
                              << cntl.ErrorText();

        if (cntl.ErrorCode() == brpc::ELOGOFF || cntl.ErrorCode() == EHOSTDOWN) {
            return SERVER_FAULT;
        } else {
            return REMOTE_QUERY_FAILED;
        }
    }

    // 4. Parse response
    size_t responseBufferSize = cntl.response_attachment().size();
    std::unique_ptr<char[]> tempBuffer = std::make_unique<char[]>(responseBufferSize);
    cntl.response_attachment().cutn(tempBuffer.get(), responseBufferSize);

    // Store buffer in result if provided
    SerializedData response;
    if constexpr (std::is_same_v<ResultType, ReadDirResponse>) {
        result->buffer = std::move(tempBuffer);
        SerializedDataInit(&response, result->buffer.get(), responseBufferSize, responseBufferSize, nullptr);
    } else if constexpr (!std::is_same_v<ResultType, void>) {
        result->responseBuffer = std::move(tempBuffer);
        SerializedDataInit(&response, result->responseBuffer.get(), responseBufferSize, responseBufferSize, nullptr);
    } else {
        SerializedDataInit(&response, tempBuffer.get(), responseBufferSize, responseBufferSize, nullptr);
    }

    sd_size_t responseSize = SerializedDataNextSeveralItemSize(&response, 0, 1);
    if (responseSize == (sd_size_t)-1) {
        FALCON_LOG(LOG_ERROR) << "returned data is corrupt.";
        return REMOTE_QUERY_FAILED;
    }

    flatbuffers::Verifier verifier((uint8_t *)response.buffer + SERIALIZED_DATA_ALIGNMENT,
                                   responseSize - SERIALIZED_DATA_ALIGNMENT);
    if (!verifier.VerifyBuffer<falcon::meta_fbs::MetaResponse>()) {
        FALCON_LOG(LOG_ERROR) << "Meta response is corrupt.";
        return REMOTE_QUERY_FAILED;
    }

    auto metaResponse = falcon::meta_fbs::GetMetaResponse((uint8_t *)response.buffer + SERIALIZED_DATA_ALIGNMENT);
    // If Create returns FILE_EXISTS, we should call responseHandler too.
    if (metaResponse->error_code() != SUCCESS &&
        !(metaResponse->response_type() == falcon::meta_fbs::AnyMetaResponse::AnyMetaResponse_CreateResponse
          && metaResponse->error_code() == FILE_EXISTS)) {
        if (metaResponse->error_code() < LAST_FALCON_ERROR_CODE)
            return (FalconErrorCode)metaResponse->error_code();
        return PROGRAM_ERROR;
    }

    return responseHandler(metaResponse, result);
}

static timespec ConvertTimestampFromPGToUnix(uint64_t t)
{
    // seconds from 1970-01-01 to 2000-01-01
    const static uint64_t SECONDS_DIFF_BETWEEN_PG_AND_UNIX = 946684800;
    timespec res;
    res.tv_sec = t / 1000000 + SECONDS_DIFF_BETWEEN_PG_AND_UNIX;
    res.tv_nsec = t % 1000000;
    return res;
}

FalconErrorCode Connection::PlainCommand(const char *command, PlainCommandResult &result, ConnectionCache *cache)
{
    auto paramBuilder = [command](flatbuffers::FlatBufferBuilder &builder) {
        return falcon::meta_fbs::CreatePlainCommandParamDirect(builder, command);
    };

    auto responseHandler = [](const falcon::meta_fbs::MetaResponse *metaResponse, PlainCommandResult *result) {
        if (metaResponse->response_type() != falcon::meta_fbs::AnyMetaResponse::AnyMetaResponse_PlainCommandResponse)
            return PROGRAM_ERROR;
        result->response = metaResponse->response_as_PlainCommandResponse();
        return SUCCESS;
    };

    return ProcessRequest(falcon::meta_proto::PLAIN_COMMAND, paramBuilder, responseHandler, cache, &result);
}

FalconErrorCode Connection::Mkdir(const char *path, ConnectionCache *cache)
{
    auto paramBuilder = [path](flatbuffers::FlatBufferBuilder &builder) {
        return falcon::meta_fbs::CreatePathOnlyParamDirect(builder, path);
    };

    auto responseHandler = [](const falcon::meta_fbs::MetaResponse *metaResponse, void *) {
        return metaResponse->error_code() < LAST_FALCON_ERROR_CODE ? (FalconErrorCode)metaResponse->error_code()
                                                                   : PROGRAM_ERROR;
    };

    return ProcessRequest(falcon::meta_proto::MKDIR, paramBuilder, responseHandler, cache);
}

FalconErrorCode
Connection::Create(const char *path, uint64_t &inodeId, int32_t &nodeId, struct stat *stbuf, ConnectionCache *cache)
{
    auto paramBuilder = [path](flatbuffers::FlatBufferBuilder &builder) {
        return falcon::meta_fbs::CreatePathOnlyParamDirect(builder, path);
    };

    auto responseHandler = [&inodeId, &nodeId, stbuf](const falcon::meta_fbs::MetaResponse *metaResponse, void *) {
        if (metaResponse->response_type() != falcon::meta_fbs::AnyMetaResponse::AnyMetaResponse_CreateResponse) {
            return PROGRAM_ERROR;
        }

        auto createResponse = metaResponse->response_as_CreateResponse();
        inodeId = createResponse->st_ino();
        nodeId = createResponse->node_id();

        if (stbuf) {
            stbuf->st_ino = createResponse->st_ino();
            stbuf->st_dev = createResponse->st_dev();
            stbuf->st_mode = createResponse->st_mode();
            stbuf->st_nlink = createResponse->st_nlink();
            stbuf->st_uid = createResponse->st_uid();
            stbuf->st_gid = createResponse->st_gid();
            stbuf->st_rdev = createResponse->st_rdev();
            stbuf->st_size = createResponse->st_size();
            stbuf->st_blksize = ST_BLKSIZE;
            stbuf->st_blocks = (stbuf->st_size + ST_BLKSIZE - 1) / ST_BLKSIZE * (ST_BLKSIZE / ST_NBLOCKSIZE);
            stbuf->st_atim = ConvertTimestampFromPGToUnix(createResponse->st_atim());
            stbuf->st_mtim = ConvertTimestampFromPGToUnix(createResponse->st_mtim());
            stbuf->st_ctim = ConvertTimestampFromPGToUnix(createResponse->st_ctim());
        }

        return (FalconErrorCode)metaResponse->error_code();
    };

    return ProcessRequest(falcon::meta_proto::CREATE, paramBuilder, responseHandler, cache);
}

FalconErrorCode Connection::Stat(const char *path, struct stat *stbuf, ConnectionCache *cache)
{
    auto paramBuilder = [path](flatbuffers::FlatBufferBuilder &builder) {
        return falcon::meta_fbs::CreatePathOnlyParamDirect(builder, path);
    };

    auto responseHandler = [stbuf](const falcon::meta_fbs::MetaResponse *metaResponse, void *) {
        if (metaResponse->response_type() != falcon::meta_fbs::AnyMetaResponse_StatResponse) {
            return PROGRAM_ERROR;
        }

        auto statResponse = metaResponse->response_as_StatResponse();
        if (stbuf) {
            stbuf->st_ino = statResponse->st_ino();
            stbuf->st_dev = statResponse->st_dev();
            stbuf->st_mode = statResponse->st_mode();
            stbuf->st_nlink = statResponse->st_nlink();
            stbuf->st_uid = statResponse->st_uid();
            stbuf->st_gid = statResponse->st_gid();
            stbuf->st_rdev = statResponse->st_rdev();
            stbuf->st_size = statResponse->st_size();
            stbuf->st_blksize = ST_BLKSIZE;
            stbuf->st_blocks = (stbuf->st_size + ST_BLKSIZE - 1) / ST_BLKSIZE * (ST_BLKSIZE / ST_NBLOCKSIZE);
            stbuf->st_atim = ConvertTimestampFromPGToUnix(statResponse->st_atim());
            stbuf->st_mtim = ConvertTimestampFromPGToUnix(statResponse->st_mtim());
            stbuf->st_ctim = ConvertTimestampFromPGToUnix(statResponse->st_ctim());
        }
        return (FalconErrorCode)metaResponse->error_code();
    };

    return ProcessRequest(falcon::meta_proto::STAT, paramBuilder, responseHandler, cache);
}

FalconErrorCode Connection::Open(const char *path,
                                 uint64_t &inodeId,
                                 int64_t &size,
                                 int32_t &nodeId,
                                 struct stat *stbuf,
                                 ConnectionCache *cache)
{
    auto paramBuilder = [path](flatbuffers::FlatBufferBuilder &builder) {
        return falcon::meta_fbs::CreatePathOnlyParamDirect(builder, path);
    };

    auto responseHandler = [&inodeId, &size, &nodeId, stbuf](const falcon::meta_fbs::MetaResponse *metaResponse,
                                                             void *) {
        if (metaResponse->response_type() != falcon::meta_fbs::AnyMetaResponse_OpenResponse) {
            return PROGRAM_ERROR;
        }

        auto openResponse = metaResponse->response_as_OpenResponse();
        inodeId = openResponse->st_ino();
        size = openResponse->st_size();
        nodeId = openResponse->node_id();

        if (stbuf) {
            stbuf->st_ino = openResponse->st_ino();
            stbuf->st_dev = openResponse->st_dev();
            stbuf->st_mode = openResponse->st_mode();
            stbuf->st_nlink = openResponse->st_nlink();
            stbuf->st_uid = openResponse->st_uid();
            stbuf->st_gid = openResponse->st_gid();
            stbuf->st_rdev = openResponse->st_rdev();
            stbuf->st_size = openResponse->st_size();
            stbuf->st_blksize = ST_BLKSIZE;
            stbuf->st_blocks = (stbuf->st_size + ST_BLKSIZE - 1) / ST_BLKSIZE * (ST_BLKSIZE / ST_NBLOCKSIZE);
            stbuf->st_atim = ConvertTimestampFromPGToUnix(openResponse->st_atim());
            stbuf->st_mtim = ConvertTimestampFromPGToUnix(openResponse->st_mtim());
            stbuf->st_ctim = ConvertTimestampFromPGToUnix(openResponse->st_ctim());
        }

        return (FalconErrorCode)metaResponse->error_code();
    };

    return ProcessRequest(falcon::meta_proto::OPEN, paramBuilder, responseHandler, cache);
}

FalconErrorCode
Connection::Close(const char *path, int64_t size, uint64_t mtime, int32_t nodeId, ConnectionCache *cache)
{
    auto paramBuilder = [path, size, mtime, nodeId](flatbuffers::FlatBufferBuilder &builder) {
        return falcon::meta_fbs::CreateCloseParamDirect(builder, path, size, mtime, nodeId);
    };

    auto responseHandler = [](const falcon::meta_fbs::MetaResponse *metaResponse, void *) {
        return metaResponse->error_code() < LAST_FALCON_ERROR_CODE
                   ? static_cast<FalconErrorCode>(metaResponse->error_code())
                   : PROGRAM_ERROR;
    };

    return ProcessRequest(falcon::meta_proto::CLOSE, paramBuilder, responseHandler, cache);
}

FalconErrorCode
Connection::Unlink(const char *path, uint64_t &inodeId, int64_t &size, int32_t &nodeId, ConnectionCache *cache)
{
    auto paramBuilder = [path](flatbuffers::FlatBufferBuilder &builder) {
        return falcon::meta_fbs::CreatePathOnlyParamDirect(builder, path);
    };

    auto responseHandler = [&inodeId, &size, &nodeId](const falcon::meta_fbs::MetaResponse *metaResponse, void *) {
        if (metaResponse->response_type() != falcon::meta_fbs::AnyMetaResponse_UnlinkResponse) {
            return PROGRAM_ERROR;
        }

        auto unlinkResponse = metaResponse->response_as_UnlinkResponse();
        inodeId = unlinkResponse->st_ino();
        size = unlinkResponse->st_size();
        nodeId = unlinkResponse->node_id();

        return static_cast<FalconErrorCode>(metaResponse->error_code());
    };

    return ProcessRequest(falcon::meta_proto::UNLINK, paramBuilder, responseHandler, cache);
}

FalconErrorCode Connection::ReadDir(const char *path,
                                    ReadDirResponse &readDirResponse,
                                    int32_t maxReadCount,
                                    int32_t lastShardIndex,
                                    const char *lastFileName,
                                    ConnectionCache *cache)
{
    auto paramBuilder = [=](flatbuffers::FlatBufferBuilder &builder) {
        return falcon::meta_fbs::CreateReadDirParamDirect(builder, path, maxReadCount, lastShardIndex, lastFileName);
    };

    auto responseHandler = [](const falcon::meta_fbs::MetaResponse *metaResponse, ReadDirResponse *result) {
        if (metaResponse->response_type() != falcon::meta_fbs::AnyMetaResponse_ReadDirResponse) {
            return PROGRAM_ERROR;
        }
        result->response = metaResponse->response_as_ReadDirResponse();
        return SUCCESS;
    };

    return ProcessRequest(falcon::meta_proto::READDIR, paramBuilder, responseHandler, cache, &readDirResponse);
}

FalconErrorCode Connection::OpenDir(const char *path, uint64_t &inodeId, ConnectionCache *cache)
{
    auto paramBuilder = [path](flatbuffers::FlatBufferBuilder &builder) {
        return falcon::meta_fbs::CreatePathOnlyParamDirect(builder, path);
    };

    auto responseHandler = [&inodeId](const falcon::meta_fbs::MetaResponse *metaResponse, void *) {
        if (metaResponse->response_type() != falcon::meta_fbs::AnyMetaResponse_OpenDirResponse) {
            return PROGRAM_ERROR;
        }
        auto openDirResponse = metaResponse->response_as_OpenDirResponse();
        inodeId = openDirResponse->st_ino();
        return SUCCESS;
    };

    return ProcessRequest(falcon::meta_proto::OPENDIR, paramBuilder, responseHandler, cache);
}

FalconErrorCode Connection::Rmdir(const char *path, ConnectionCache *cache)
{
    auto paramBuilder = [path](flatbuffers::FlatBufferBuilder &builder) {
        return falcon::meta_fbs::CreatePathOnlyParamDirect(builder, path);
    };

    auto responseHandler = [](const falcon::meta_fbs::MetaResponse *metaResponse, void *) {
        return metaResponse->error_code() < LAST_FALCON_ERROR_CODE
                   ? static_cast<FalconErrorCode>(metaResponse->error_code())
                   : PROGRAM_ERROR;
    };

    return ProcessRequest(falcon::meta_proto::RMDIR, paramBuilder, responseHandler, cache);
}

FalconErrorCode Connection::Rename(const char *src, const char *dst, ConnectionCache *cache)
{
    auto paramBuilder = [src, dst](flatbuffers::FlatBufferBuilder &builder) {
        return falcon::meta_fbs::CreateRenameParamDirect(builder, src, dst);
    };

    auto responseHandler = [](const falcon::meta_fbs::MetaResponse *metaResponse, void *) {
        return metaResponse->error_code() < LAST_FALCON_ERROR_CODE
                   ? static_cast<FalconErrorCode>(metaResponse->error_code())
                   : PROGRAM_ERROR;
    };

    return ProcessRequest(falcon::meta_proto::RENAME, paramBuilder, responseHandler, cache);
}

FalconErrorCode Connection::UtimeNs(const char *path, int64_t atime, int64_t mtime, ConnectionCache *cache)
{
    auto paramBuilder = [path, atime, mtime](flatbuffers::FlatBufferBuilder &builder) {
        return falcon::meta_fbs::CreateUtimeNsParamDirect(builder, path, atime, mtime);
    };

    auto responseHandler = [](const falcon::meta_fbs::MetaResponse *metaResponse, void *) {
        return metaResponse->error_code() < LAST_FALCON_ERROR_CODE
                   ? static_cast<FalconErrorCode>(metaResponse->error_code())
                   : PROGRAM_ERROR;
    };

    return ProcessRequest(falcon::meta_proto::UTIMENS, paramBuilder, responseHandler, cache);
}

FalconErrorCode Connection::Chown(const char *path, uint32_t uid, uint32_t gid, ConnectionCache *cache)
{
    auto paramBuilder = [path, uid, gid](flatbuffers::FlatBufferBuilder &builder) {
        return falcon::meta_fbs::CreateChownParamDirect(builder, path, uid, gid);
    };

    auto responseHandler = [](const falcon::meta_fbs::MetaResponse *metaResponse, void *) {
        if (metaResponse->error_code() < LAST_FALCON_ERROR_CODE) {
            return static_cast<FalconErrorCode>(metaResponse->error_code());
        }
        return PROGRAM_ERROR;
    };

    return ProcessRequest(falcon::meta_proto::CHOWN, paramBuilder, responseHandler, cache);
}

FalconErrorCode Connection::Chmod(const char *path, uint32_t mode, ConnectionCache *cache)
{
    auto paramBuilder = [path, mode](flatbuffers::FlatBufferBuilder &builder) {
        return falcon::meta_fbs::CreateChmodParamDirect(builder, path, mode);
    };

    auto responseHandler = [](const falcon::meta_fbs::MetaResponse *metaResponse, void *) {
        if (metaResponse->error_code() < LAST_FALCON_ERROR_CODE) {
            return static_cast<FalconErrorCode>(metaResponse->error_code());
        }
        return PROGRAM_ERROR;
    };

    return ProcessRequest(falcon::meta_proto::CHMOD, paramBuilder, responseHandler, cache);
}

// KV Operations
FalconErrorCode Connection::KvPut(const char *key,
                                  uint32_t valueLen,
                                  uint16_t sliceNum,
                                  const std::vector<uint64_t> &valueKey,
                                  const std::vector<uint64_t> &location,
                                  const std::vector<uint32_t> &size,
                                  ConnectionCache *cache)
{
    auto paramBuilder = [key, valueLen, sliceNum, &valueKey, &location, &size](flatbuffers::FlatBufferBuilder &builder) {
        return falcon::meta_fbs::CreateKVParamDirect(builder, key, valueLen, sliceNum, &valueKey, &location, &size);
    };

    auto responseHandler = [](const falcon::meta_fbs::MetaResponse *metaResponse, void *) {
        return metaResponse->error_code() < LAST_FALCON_ERROR_CODE
                   ? static_cast<FalconErrorCode>(metaResponse->error_code())
                   : PROGRAM_ERROR;
    };

    return ProcessRequest(falcon::meta_proto::KV_PUT, paramBuilder, responseHandler, cache);
}

FalconErrorCode Connection::KvGet(const char *key, KvGetResult &result, ConnectionCache *cache)
{
    auto paramBuilder = [key](flatbuffers::FlatBufferBuilder &builder) {
        return falcon::meta_fbs::CreateKeyOnlyParamDirect(builder, key);
    };

    auto responseHandler = [](const falcon::meta_fbs::MetaResponse *metaResponse, KvGetResult *result) {
        if (metaResponse->response_type() != falcon::meta_fbs::AnyMetaResponse_GetKVMetaResponse) {
            return PROGRAM_ERROR;
        }

        auto kvResponse = metaResponse->response_as_GetKVMetaResponse();
        result->valueLen = kvResponse->value_len();
        result->sliceNum = kvResponse->slice_num();

        if (kvResponse->value_key() && kvResponse->location() && kvResponse->size()) {
            size_t count = std::min({kvResponse->value_key()->size(),
                                     kvResponse->location()->size(),
                                     kvResponse->size()->size()});
            result->slices.resize(count);
            for (size_t i = 0; i < count; ++i) {
                result->slices[i].valueKey = kvResponse->value_key()->Get(i);
                result->slices[i].location = kvResponse->location()->Get(i);
                result->slices[i].size = kvResponse->size()->Get(i);
            }
        }

        return static_cast<FalconErrorCode>(metaResponse->error_code());
    };

    return ProcessRequest(falcon::meta_proto::KV_GET, paramBuilder, responseHandler, cache, &result);
}

FalconErrorCode Connection::KvDel(const char *key, ConnectionCache *cache)
{
    auto paramBuilder = [key](flatbuffers::FlatBufferBuilder &builder) {
        return falcon::meta_fbs::CreateKeyOnlyParamDirect(builder, key);
    };

    auto responseHandler = [](const falcon::meta_fbs::MetaResponse *metaResponse, void *) {
        return metaResponse->error_code() < LAST_FALCON_ERROR_CODE
                   ? static_cast<FalconErrorCode>(metaResponse->error_code())
                   : PROGRAM_ERROR;
    };

    return ProcessRequest(falcon::meta_proto::KV_DEL, paramBuilder, responseHandler, cache);
}

// Slice Operations
FalconErrorCode Connection::SlicePut(const char *filename,
                                     uint32_t sliceNum,
                                     const std::vector<uint64_t> &inodeId,
                                     const std::vector<uint32_t> &chunkId,
                                     const std::vector<uint64_t> &sliceId,
                                     const std::vector<uint32_t> &sliceSize,
                                     const std::vector<uint32_t> &sliceOffset,
                                     const std::vector<uint32_t> &sliceLen,
                                     const std::vector<uint32_t> &sliceLoc1,
                                     const std::vector<uint32_t> &sliceLoc2,
                                     ConnectionCache *cache)
{
    auto paramBuilder = [filename, sliceNum, &inodeId, &chunkId, &sliceId, &sliceSize, &sliceOffset, &sliceLen,
                         &sliceLoc1, &sliceLoc2](flatbuffers::FlatBufferBuilder &builder) {
        return falcon::meta_fbs::CreateSliceInfoParamDirect(builder,
                                                            filename,
                                                            sliceNum,
                                                            &inodeId,
                                                            &chunkId,
                                                            &sliceId,
                                                            &sliceSize,
                                                            &sliceOffset,
                                                            &sliceLen,
                                                            &sliceLoc1,
                                                            &sliceLoc2);
    };

    auto responseHandler = [](const falcon::meta_fbs::MetaResponse *metaResponse, void *) {
        return metaResponse->error_code() < LAST_FALCON_ERROR_CODE
                   ? static_cast<FalconErrorCode>(metaResponse->error_code())
                   : PROGRAM_ERROR;
    };

    return ProcessRequest(falcon::meta_proto::SLICE_PUT, paramBuilder, responseHandler, cache);
}

FalconErrorCode Connection::SliceGet(const char *filename, uint64_t inodeId, uint32_t chunkId,
                                     SliceGetResult &result, ConnectionCache *cache)
{
    auto paramBuilder = [filename, inodeId, chunkId](flatbuffers::FlatBufferBuilder &builder) {
        return falcon::meta_fbs::CreateSliceIndexParamDirect(builder, filename, inodeId, chunkId);
    };

    auto responseHandler = [](const falcon::meta_fbs::MetaResponse *metaResponse, SliceGetResult *result) {
        if (metaResponse->response_type() != falcon::meta_fbs::AnyMetaResponse_SliceInfoResponse) {
            return PROGRAM_ERROR;
        }

        auto sliceResponse = metaResponse->response_as_SliceInfoResponse();
        result->sliceNum = sliceResponse->slicenum();

        auto copyVector = [](auto *fbsVec, auto &stdVec) {
            if (fbsVec) {
                stdVec.assign(fbsVec->begin(), fbsVec->end());
            }
        };

        copyVector(sliceResponse->inodeid(), result->inodeId);
        copyVector(sliceResponse->chunkid(), result->chunkId);
        copyVector(sliceResponse->sliceid(), result->sliceId);
        copyVector(sliceResponse->slicesize(), result->sliceSize);
        copyVector(sliceResponse->sliceoffset(), result->sliceOffset);
        copyVector(sliceResponse->slicelen(), result->sliceLen);
        copyVector(sliceResponse->sliceloc1(), result->sliceLoc1);
        copyVector(sliceResponse->sliceloc2(), result->sliceLoc2);

        return static_cast<FalconErrorCode>(metaResponse->error_code());
    };

    return ProcessRequest(falcon::meta_proto::SLICE_GET, paramBuilder, responseHandler, cache, &result);
}

FalconErrorCode Connection::SliceDel(const char *filename, uint64_t inodeId, uint32_t chunkId, ConnectionCache *cache)
{
    auto paramBuilder = [filename, inodeId, chunkId](flatbuffers::FlatBufferBuilder &builder) {
        return falcon::meta_fbs::CreateSliceIndexParamDirect(builder, filename, inodeId, chunkId);
    };

    auto responseHandler = [](const falcon::meta_fbs::MetaResponse *metaResponse, void *) {
        return metaResponse->error_code() < LAST_FALCON_ERROR_CODE
                   ? static_cast<FalconErrorCode>(metaResponse->error_code())
                   : PROGRAM_ERROR;
    };

    return ProcessRequest(falcon::meta_proto::SLICE_DEL, paramBuilder, responseHandler, cache);
}

FalconErrorCode Connection::FetchSliceId(uint32_t count, uint8_t type, uint64_t &startId,
                                         uint64_t &endId, ConnectionCache *cache)
{
    auto paramBuilder = [count, type](flatbuffers::FlatBufferBuilder &builder) {
        return falcon::meta_fbs::CreateSliceIdParam(builder, count, type);
    };

    auto responseHandler = [&startId, &endId](const falcon::meta_fbs::MetaResponse *metaResponse, void *) {
        if (metaResponse->response_type() != falcon::meta_fbs::AnyMetaResponse_SliceIdResponse) {
            return PROGRAM_ERROR;
        }

        auto sliceIdResponse = metaResponse->response_as_SliceIdResponse();
        startId = sliceIdResponse->startid();
        endId = sliceIdResponse->endid();

        return static_cast<FalconErrorCode>(metaResponse->error_code());
    };

    return ProcessRequest(falcon::meta_proto::FETCH_SLICE_ID, paramBuilder, responseHandler, cache);
}
