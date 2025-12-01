/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "connection_pool/falcon_meta_service_internal.h"
#include "connection_pool/pg_connection.h"
#include "connection_pool/connection_pool_config.h"
#include "connection_pool/pg_connection_pool.h"
#include "falcon_meta_rpc.pb.h"
#include "falcon_meta_param_generated.h"
#include "falcon_meta_response_generated.h"
#include "connection_pool/task.h"
#include <brpc/controller.h>
#include <google/protobuf/stubs/callback.h>
#include <cstring>
#include <cstdlib>
#include <vector>
#include <mutex>

extern "C" {
#define FALCON_REMOTE_CONNECTION_DEF_SERIALIZED_DATA_IMPLEMENT
#include "remote_connection_utils/serialized_data.h"
}

namespace falcon {
namespace meta_service {

static void HandleFalconMetaResponse(brpc::Controller* cntl,
                                     falcon::meta_proto::Empty* proto_response,
                                     AsyncFalconMetaServiceJob* original_job)
{
    FalconMetaServiceResponse& response = original_job->GetResponse();
    response.opcode = original_job->GetRequest().operation;

    if (cntl->Failed()) {
        fprintf(stderr, "[WARNING] [FalconMetaService] RPC failed: %s\n", cntl->ErrorText().c_str());
        response.status = -1;
        response.data = nullptr;
    } else {
        if (!FalconMetaServiceSerializer::DeserializeResponseFromFlatBuffers(
                cntl->response_attachment(),
                &response,
                original_job->GetRequest().operation)) {
            fprintf(stderr, "[WARNING] [FalconMetaService] Failed to deserialize response for opcode=%d\n",
                 static_cast<int>(original_job->GetRequest().operation));
            response.status = -1;
        }
    }

    original_job->Done();

    delete cntl;
    delete proto_response;
    delete original_job;
}

}  // namespace meta_service
}  // namespace falcon

namespace falcon {
namespace meta_service {

FalconMetaService* FalconMetaService::instance = nullptr;
std::mutex FalconMetaService::instanceMutex;

FalconMetaService::FalconMetaService() : initialized(false)
{
}

FalconMetaService* FalconMetaService::Instance()
{
    std::lock_guard<std::mutex> lock(instanceMutex);
    if (instance == nullptr) {
        instance = new FalconMetaService();
    }
    return instance;
}

bool FalconMetaService::Init(int port, int pool_size)
{
    std::lock_guard<std::mutex> lock(instanceMutex);

    if (initialized) {
        return true;
    }

    if (port <= 0 || port > 65535) {
        fprintf(stderr, "[WARNING] [FalconMetaService] Invalid port number: %d\n", port);
        return false;
    }

    if (pool_size <= 0) {
        fprintf(stderr, "[WARNING] [FalconMetaService] Invalid pool size: %d\n", pool_size);
        return false;
    }

    char* user_name = getenv("USER");
    if (!user_name) {
        fprintf(stderr, "[WARNING] [FalconMetaService] Cannot get USER from environment\n");
        return false;
    }

    try {
        pgConnectionPool = std::make_shared<PGConnectionPool>(
            port, user_name, pool_size, 20, 400);

        fprintf(stderr, "[LOG] [FalconMetaService] Initialized with: port=%d, user=%s, poolSize=%d\n",
             port, user_name, pool_size);

        initialized = true;
        return true;
    } catch (const std::exception& e) {
        fprintf(stderr, "[WARNING] [FalconMetaService] Failed to initialize connection pool: %s\n", e.what());
        return false;
    }
}

FalconMetaService::~FalconMetaService()
{
    if (pgConnectionPool) {
        pgConnectionPool->Stop();
        pgConnectionPool.reset();
    }
}

int FalconMetaService::DispatchFalconMetaServiceJob(AsyncFalconMetaServiceJob* job)
{
    if (pgConnectionPool == nullptr) {
        fprintf(stderr, "[WARNING] [FalconMetaService] DispatchJob failed: connection pool is null\n");
        if (job != nullptr) {
            job->GetResponse().status = -1;
            job->Done();
            delete job;
        }
        return -1;
    }

    FalconMetaServiceRequest& request = job->GetRequest();

    fprintf(stderr, "[LOG] [FalconMetaService] DispatchFalconMetaServiceJob: opcode=%d(%s)\n",
         static_cast<int>(request.operation),
         FalconMetaOperationTypeName(request.operation));

    brpc::Controller* cntl = new brpc::Controller();
    falcon::meta_proto::MetaRequest* proto_request = new falcon::meta_proto::MetaRequest();
    falcon::meta_proto::Empty* proto_response = new falcon::meta_proto::Empty();

    FalconErrorCode serializeError = FalconMetaServiceSerializer::SerializeRequestToFlatBuffers(
            request, proto_request, &cntl->request_attachment());
    if (serializeError != SUCCESS) {
        fprintf(stderr, "[WARNING] [FalconMetaService] Failed to serialize request for opcode=%d, error=%d\n",
             static_cast<int>(request.operation), static_cast<int>(serializeError));
        job->GetResponse().status = serializeError;
        job->Done();
        delete job;
        delete cntl;
        delete proto_request;
        delete proto_response;
        return -1;
    }

    google::protobuf::Closure* done_callback = brpc::NewCallback(
        &HandleFalconMetaResponse, cntl, proto_response, job);

    falcon::meta_proto::AsyncMetaServiceJob* brpc_job =
        new falcon::meta_proto::AsyncMetaServiceJob(cntl, proto_request, proto_response, done_callback);

    pgConnectionPool->DispatchAsyncMetaServiceJob(brpc_job);

    return 0;
}

int FalconMetaService::SubmitFalconMetaRequest(const FalconMetaServiceRequest& request,
                                               FalconMetaServiceCallback callback,
                                               void* user_context)
{
    if (!initialized) {
        fprintf(stderr, "[WARNING] [FalconMetaService] Service not initialized. Call Init() first.\n");
        return -1;
    }

    fprintf(stderr, "[LOG] [FalconMetaService] SubmitFalconMetaRequest: opcode=%d(%s)\n",
         static_cast<int>(request.operation),
         FalconMetaOperationTypeName(request.operation));

    AsyncFalconMetaServiceJob* job = new AsyncFalconMetaServiceJob(request, callback, user_context);

    return DispatchFalconMetaServiceJob(job);
}

static falcon::meta_proto::MetaServiceType ConvertToProtoType(FalconMetaOperationType op)
{
    switch (op) {
        case DFC_PUT_KEY_META: return falcon::meta_proto::MetaServiceType::KV_PUT;
        case DFC_GET_KV_META: return falcon::meta_proto::MetaServiceType::KV_GET;
        case DFC_DELETE_KV_META: return falcon::meta_proto::MetaServiceType::KV_DEL;
        case DFC_MKDIR: return falcon::meta_proto::MetaServiceType::MKDIR;
        case DFC_MKDIR_SUB_MKDIR: return falcon::meta_proto::MetaServiceType::MKDIR_SUB_MKDIR;
        case DFC_MKDIR_SUB_CREATE: return falcon::meta_proto::MetaServiceType::MKDIR_SUB_CREATE;
        case DFC_CREATE: return falcon::meta_proto::MetaServiceType::CREATE;
        case DFC_STAT: return falcon::meta_proto::MetaServiceType::STAT;
        case DFC_OPEN: return falcon::meta_proto::MetaServiceType::OPEN;
        case DFC_CLOSE: return falcon::meta_proto::MetaServiceType::CLOSE;
        case DFC_UNLINK: return falcon::meta_proto::MetaServiceType::UNLINK;
        case DFC_READDIR: return falcon::meta_proto::MetaServiceType::READDIR;
        case DFC_OPENDIR: return falcon::meta_proto::MetaServiceType::OPENDIR;
        case DFC_RMDIR: return falcon::meta_proto::MetaServiceType::RMDIR;
        case DFC_RMDIR_SUB_RMDIR: return falcon::meta_proto::MetaServiceType::RMDIR_SUB_RMDIR;
        case DFC_RMDIR_SUB_UNLINK: return falcon::meta_proto::MetaServiceType::RMDIR_SUB_UNLINK;
        case DFC_RENAME: return falcon::meta_proto::MetaServiceType::RENAME;
        case DFC_RENAME_SUB_RENAME_LOCALLY: return falcon::meta_proto::MetaServiceType::RENAME_SUB_RENAME_LOCALLY;
        case DFC_RENAME_SUB_CREATE: return falcon::meta_proto::MetaServiceType::RENAME_SUB_CREATE;
        case DFC_UTIMENS: return falcon::meta_proto::MetaServiceType::UTIMENS;
        case DFC_CHOWN: return falcon::meta_proto::MetaServiceType::CHOWN;
        case DFC_CHMOD: return falcon::meta_proto::MetaServiceType::CHMOD;
        case DFC_SLICE_PUT: return falcon::meta_proto::MetaServiceType::SLICE_PUT;
        case DFC_SLICE_GET: return falcon::meta_proto::MetaServiceType::SLICE_GET;
        case DFC_SLICE_DEL: return falcon::meta_proto::MetaServiceType::SLICE_DEL;
        case DFC_FETCH_SLICE_ID: return falcon::meta_proto::MetaServiceType::FETCH_SLICE_ID;
        default: return falcon::meta_proto::MetaServiceType::PLAIN_COMMAND;
    }
}

static bool ValidateNameLength(const std::string& name) {
    return name.length() <= FALCON_MAX_NAME_LENGTH;
}

static bool ValidatePathComponentLengths(const std::string& path) {
    if (path.empty()) return true;

    size_t start = 0;
    if (path[0] == '/') start = 1;

    size_t pos;
    while ((pos = path.find('/', start)) != std::string::npos) {
        if (pos > start && (pos - start) > FALCON_MAX_NAME_LENGTH) {
            return false;
        }
        start = pos + 1;
    }
    if (path.length() > start && (path.length() - start) > FALCON_MAX_NAME_LENGTH) {
        return false;
    }
    return true;
}

FalconErrorCode FalconMetaServiceSerializer::SerializeRequestToFlatBuffers(
    const FalconMetaServiceRequest& request,
    falcon::meta_proto::MetaRequest* proto_request,
    butil::IOBuf* attachment)
{
    falcon::meta_proto::MetaServiceType proto_type = ConvertToProtoType(request.operation);
    proto_request->add_type(proto_type);

    if (proto_type == falcon::meta_proto::MKDIR ||
        proto_type == falcon::meta_proto::CREATE ||
        proto_type == falcon::meta_proto::STAT ||
        proto_type == falcon::meta_proto::OPEN ||
        proto_type == falcon::meta_proto::CLOSE ||
        proto_type == falcon::meta_proto::UNLINK) {
        proto_request->set_allow_batch_with_others(true);
    }

    flatbuffers::FlatBufferBuilder builder(1024);
    flatbuffers::Offset<falcon::meta_fbs::MetaParam> meta_param;

    switch (request.operation) {
        case DFC_MKDIR:
        case DFC_CREATE:
        case DFC_STAT:
        case DFC_OPEN:
        case DFC_UNLINK:
        case DFC_OPENDIR:
        case DFC_RMDIR: {
            const PathOnlyParam* param = meta_param_helper::Get<PathOnlyParam>(request.file_params);
            if (!param) return ARGUMENT_ERROR;
            if (!ValidatePathComponentLengths(param->path)) {
                fprintf(stderr, "[WARNING] [FalconMetaService] Path component exceeds %zu bytes: %s\n",
                     FALCON_MAX_NAME_LENGTH, param->path.c_str());
                return INVALID_PARAMETER;
            }
            auto path = builder.CreateString(param->path);
            auto fbs_param = falcon::meta_fbs::CreatePathOnlyParam(builder, path);
            meta_param = falcon::meta_fbs::CreateMetaParam(builder,
                falcon::meta_fbs::AnyMetaParam_PathOnlyParam, fbs_param.Union());
            break;
        }

        case DFC_CLOSE: {
            const CloseParam* param = meta_param_helper::Get<CloseParam>(request.file_params);
            if (!param) return ARGUMENT_ERROR;
            if (!ValidatePathComponentLengths(param->path)) {
                fprintf(stderr, "[WARNING] [FalconMetaService] Path component exceeds %zu bytes: %s\n",
                     FALCON_MAX_NAME_LENGTH, param->path.c_str());
                return INVALID_PARAMETER;
            }
            auto path = builder.CreateString(param->path);
            auto fbs_param = falcon::meta_fbs::CreateCloseParam(builder, path,
                param->st_size, param->st_mtim, param->node_id);
            meta_param = falcon::meta_fbs::CreateMetaParam(builder,
                falcon::meta_fbs::AnyMetaParam_CloseParam, fbs_param.Union());
            break;
        }

        case DFC_READDIR: {
            const ReadDirParam* param = meta_param_helper::Get<ReadDirParam>(request.file_params);
            if (!param) return ARGUMENT_ERROR;
            if (!ValidatePathComponentLengths(param->path)) {
                fprintf(stderr, "[WARNING] [FalconMetaService] Path component exceeds %zu bytes: %s\n",
                     FALCON_MAX_NAME_LENGTH, param->path.c_str());
                return INVALID_PARAMETER;
            }
            auto path = builder.CreateString(param->path);
            auto last_file_name = builder.CreateString(param->last_file_name);
            auto fbs_param = falcon::meta_fbs::CreateReadDirParam(builder, path,
                param->max_read_count, param->last_shard_index, last_file_name);
            meta_param = falcon::meta_fbs::CreateMetaParam(builder,
                falcon::meta_fbs::AnyMetaParam_ReadDirParam, fbs_param.Union());
            break;
        }

        case DFC_MKDIR_SUB_MKDIR: {
            const MkdirSubMkdirParam* param = meta_param_helper::Get<MkdirSubMkdirParam>(request.file_params);
            if (!param) return ARGUMENT_ERROR;
            if (!ValidateNameLength(param->name)) {
                fprintf(stderr, "[WARNING] [FalconMetaService] Name exceeds %zu bytes: %s\n",
                     FALCON_MAX_NAME_LENGTH, param->name.c_str());
                return INVALID_PARAMETER;
            }
            auto name = builder.CreateString(param->name);
            auto fbs_param = falcon::meta_fbs::CreateMkdirSubMkdirParam(builder,
                param->parent_id, name, param->inode_id);
            meta_param = falcon::meta_fbs::CreateMetaParam(builder,
                falcon::meta_fbs::AnyMetaParam_MkdirSubMkdirParam, fbs_param.Union());
            break;
        }

        case DFC_MKDIR_SUB_CREATE: {
            const MkdirSubCreateParam* param = meta_param_helper::Get<MkdirSubCreateParam>(request.file_params);
            if (!param) return ARGUMENT_ERROR;
            if (!ValidateNameLength(param->name)) {
                fprintf(stderr, "[WARNING] [FalconMetaService] Name exceeds %zu bytes: %s\n",
                     FALCON_MAX_NAME_LENGTH, param->name.c_str());
                return INVALID_PARAMETER;
            }
            auto name = builder.CreateString(param->name);
            auto fbs_param = falcon::meta_fbs::CreateMkdirSubCreateParam(builder,
                param->parent_id_part_id, name, param->inode_id, param->st_mode,
                param->st_mtim, param->st_size);
            meta_param = falcon::meta_fbs::CreateMetaParam(builder,
                falcon::meta_fbs::AnyMetaParam_MkdirSubCreateParam, fbs_param.Union());
            break;
        }

        case DFC_RMDIR_SUB_RMDIR: {
            const RmdirSubRmdirParam* param = meta_param_helper::Get<RmdirSubRmdirParam>(request.file_params);
            if (!param) return ARGUMENT_ERROR;
            if (!ValidateNameLength(param->name)) {
                fprintf(stderr, "[WARNING] [FalconMetaService] Name exceeds %zu bytes: %s\n",
                     FALCON_MAX_NAME_LENGTH, param->name.c_str());
                return INVALID_PARAMETER;
            }
            auto name = builder.CreateString(param->name);
            auto fbs_param = falcon::meta_fbs::CreateRmdirSubRmdirParam(builder, param->parent_id, name);
            meta_param = falcon::meta_fbs::CreateMetaParam(builder,
                falcon::meta_fbs::AnyMetaParam_RmdirSubRmdirParam, fbs_param.Union());
            break;
        }

        case DFC_RMDIR_SUB_UNLINK: {
            const RmdirSubUnlinkParam* param = meta_param_helper::Get<RmdirSubUnlinkParam>(request.file_params);
            if (!param) return ARGUMENT_ERROR;
            if (!ValidateNameLength(param->name)) {
                fprintf(stderr, "[WARNING] [FalconMetaService] Name exceeds %zu bytes: %s\n",
                     FALCON_MAX_NAME_LENGTH, param->name.c_str());
                return INVALID_PARAMETER;
            }
            auto name = builder.CreateString(param->name);
            auto fbs_param = falcon::meta_fbs::CreateRmdirSubUnlinkParam(builder,
                param->parent_id_part_id, name);
            meta_param = falcon::meta_fbs::CreateMetaParam(builder,
                falcon::meta_fbs::AnyMetaParam_RmdirSubUnlinkParam, fbs_param.Union());
            break;
        }

        case DFC_RENAME: {
            const RenameParam* param = meta_param_helper::Get<RenameParam>(request.file_params);
            if (!param) return ARGUMENT_ERROR;
            if (!ValidatePathComponentLengths(param->src)) {
                fprintf(stderr, "[WARNING] [FalconMetaService] Source path component exceeds %zu bytes: %s\n",
                     FALCON_MAX_NAME_LENGTH, param->src.c_str());
                return INVALID_PARAMETER;
            }
            if (!ValidatePathComponentLengths(param->dst)) {
                fprintf(stderr, "[WARNING] [FalconMetaService] Destination path component exceeds %zu bytes: %s\n",
                     FALCON_MAX_NAME_LENGTH, param->dst.c_str());
                return INVALID_PARAMETER;
            }
            auto src = builder.CreateString(param->src);
            auto dst = builder.CreateString(param->dst);
            auto fbs_param = falcon::meta_fbs::CreateRenameParam(builder, src, dst);
            meta_param = falcon::meta_fbs::CreateMetaParam(builder,
                falcon::meta_fbs::AnyMetaParam_RenameParam, fbs_param.Union());
            break;
        }

        case DFC_RENAME_SUB_RENAME_LOCALLY: {
            const RenameSubRenameLocallyParam* param = meta_param_helper::Get<RenameSubRenameLocallyParam>(request.file_params);
            if (!param) return ARGUMENT_ERROR;
            if (!ValidateNameLength(param->src_name)) {
                fprintf(stderr, "[WARNING] [FalconMetaService] Source name exceeds %zu bytes: %s\n",
                     FALCON_MAX_NAME_LENGTH, param->src_name.c_str());
                return INVALID_PARAMETER;
            }
            if (!ValidateNameLength(param->dst_name)) {
                fprintf(stderr, "[WARNING] [FalconMetaService] Destination name exceeds %zu bytes: %s\n",
                     FALCON_MAX_NAME_LENGTH, param->dst_name.c_str());
                return INVALID_PARAMETER;
            }
            auto src_name = builder.CreateString(param->src_name);
            auto dst_name = builder.CreateString(param->dst_name);
            auto fbs_param = falcon::meta_fbs::CreateRenameSubRenameLocallyParam(builder,
                param->src_parent_id, param->src_parent_id_part_id, src_name,
                param->dst_parent_id, param->dst_parent_id_part_id, dst_name,
                param->target_is_directory, param->directory_inode_id, param->src_lock_order);
            meta_param = falcon::meta_fbs::CreateMetaParam(builder,
                falcon::meta_fbs::AnyMetaParam_RenameSubRenameLocallyParam, fbs_param.Union());
            break;
        }

        case DFC_RENAME_SUB_CREATE: {
            const RenameSubCreateParam* param = meta_param_helper::Get<RenameSubCreateParam>(request.file_params);
            if (!param) return ARGUMENT_ERROR;
            if (!ValidateNameLength(param->name)) {
                fprintf(stderr, "[WARNING] [FalconMetaService] Name exceeds %zu bytes: %s\n",
                     FALCON_MAX_NAME_LENGTH, param->name.c_str());
                return INVALID_PARAMETER;
            }
            auto name = builder.CreateString(param->name);
            auto fbs_param = falcon::meta_fbs::CreateRenameSubCreateParam(builder,
                param->parentid_partid, name, param->st_ino, param->st_dev, param->st_mode,
                param->st_nlink, param->st_uid, param->st_gid, param->st_rdev, param->st_size,
                param->st_blksize, param->st_blocks, param->st_atim, param->st_mtim,
                param->st_ctim, param->node_id);
            meta_param = falcon::meta_fbs::CreateMetaParam(builder,
                falcon::meta_fbs::AnyMetaParam_RenameSubCreateParam, fbs_param.Union());
            break;
        }

        case DFC_UTIMENS: {
            const UtimeNsParam* param = meta_param_helper::Get<UtimeNsParam>(request.file_params);
            if (!param) return ARGUMENT_ERROR;
            if (!ValidatePathComponentLengths(param->path)) {
                fprintf(stderr, "[WARNING] [FalconMetaService] Path component exceeds %zu bytes: %s\n",
                     FALCON_MAX_NAME_LENGTH, param->path.c_str());
                return INVALID_PARAMETER;
            }
            auto path = builder.CreateString(param->path);
            auto fbs_param = falcon::meta_fbs::CreateUtimeNsParam(builder, path,
                param->st_atim, param->st_mtim);
            meta_param = falcon::meta_fbs::CreateMetaParam(builder,
                falcon::meta_fbs::AnyMetaParam_UtimeNsParam, fbs_param.Union());
            break;
        }

        case DFC_CHOWN: {
            const ChownParam* param = meta_param_helper::Get<ChownParam>(request.file_params);
            if (!param) return ARGUMENT_ERROR;
            if (!ValidatePathComponentLengths(param->path)) {
                fprintf(stderr, "[WARNING] [FalconMetaService] Path component exceeds %zu bytes: %s\n",
                     FALCON_MAX_NAME_LENGTH, param->path.c_str());
                return INVALID_PARAMETER;
            }
            auto path = builder.CreateString(param->path);
            auto fbs_param = falcon::meta_fbs::CreateChownParam(builder, path,
                param->st_uid, param->st_gid);
            meta_param = falcon::meta_fbs::CreateMetaParam(builder,
                falcon::meta_fbs::AnyMetaParam_ChownParam, fbs_param.Union());
            break;
        }

        case DFC_CHMOD: {
            const ChmodParam* param = meta_param_helper::Get<ChmodParam>(request.file_params);
            if (!param) return ARGUMENT_ERROR;
            if (!ValidatePathComponentLengths(param->path)) {
                fprintf(stderr, "[WARNING] [FalconMetaService] Path component exceeds %zu bytes: %s\n",
                     FALCON_MAX_NAME_LENGTH, param->path.c_str());
                return INVALID_PARAMETER;
            }
            auto path = builder.CreateString(param->path);
            auto fbs_param = falcon::meta_fbs::CreateChmodParam(builder, path, param->st_mode);
            meta_param = falcon::meta_fbs::CreateMetaParam(builder,
                falcon::meta_fbs::AnyMetaParam_ChmodParam, fbs_param.Union());
            break;
        }

        case DFC_PUT_KEY_META: {
            std::vector<uint64_t> value_keys, locations;
            std::vector<uint32_t> sizes;
            for (const auto& slice : request.kv_data.dataSlices) {
                value_keys.push_back(slice.value_key);
                locations.push_back(slice.location);
                sizes.push_back(slice.size);
            }
            auto key = builder.CreateString(request.kv_data.key);
            auto vk_vec = builder.CreateVector(value_keys);
            auto loc_vec = builder.CreateVector(locations);
            auto sz_vec = builder.CreateVector(sizes);
            auto fbs_param = falcon::meta_fbs::CreateKVParam(builder, key,
                request.kv_data.valueLen, request.kv_data.sliceNum, vk_vec, loc_vec, sz_vec);
            meta_param = falcon::meta_fbs::CreateMetaParam(builder,
                falcon::meta_fbs::AnyMetaParam_KVParam, fbs_param.Union());
            break;
        }

        case DFC_GET_KV_META:
        case DFC_DELETE_KV_META: {
            auto key = builder.CreateString(request.kv_data.key);
            auto fbs_param = falcon::meta_fbs::CreateKeyOnlyParam(builder, key);
            meta_param = falcon::meta_fbs::CreateMetaParam(builder,
                falcon::meta_fbs::AnyMetaParam_KeyOnlyParam, fbs_param.Union());
            break;
        }

        case DFC_PLAIN_COMMAND: {
            const PlainCommandParam* param = meta_param_helper::Get<PlainCommandParam>(request.file_params);
            if (!param) return ARGUMENT_ERROR;
            auto command = builder.CreateString(param->command);
            auto fbs_param = falcon::meta_fbs::CreatePlainCommandParam(builder, command);
            meta_param = falcon::meta_fbs::CreateMetaParam(builder,
                falcon::meta_fbs::AnyMetaParam_PlainCommandParam, fbs_param.Union());
            break;
        }

        case DFC_SLICE_GET:
        case DFC_SLICE_DEL: {
            const SliceIndexParam* param = meta_param_helper::Get<SliceIndexParam>(request.file_params);
            if (!param) return ARGUMENT_ERROR;
            auto filename = builder.CreateString(param->filename);
            auto fbs_param = falcon::meta_fbs::CreateSliceIndexParam(builder, filename,
                param->inodeid, param->chunkid);
            meta_param = falcon::meta_fbs::CreateMetaParam(builder,
                falcon::meta_fbs::AnyMetaParam_SliceIndexParam, fbs_param.Union());
            break;
        }

        case DFC_SLICE_PUT: {
            const SliceInfoParam* param = meta_param_helper::Get<SliceInfoParam>(request.file_params);
            if (!param) return ARGUMENT_ERROR;
            auto filename = builder.CreateString(param->filename);
            auto inodeid_vec = builder.CreateVector(param->inodeid);
            auto chunkid_vec = builder.CreateVector(param->chunkid);
            auto sliceid_vec = builder.CreateVector(param->sliceid);
            auto slicesize_vec = builder.CreateVector(param->slicesize);
            auto sliceoffset_vec = builder.CreateVector(param->sliceoffset);
            auto slicelen_vec = builder.CreateVector(param->slicelen);
            auto sliceloc1_vec = builder.CreateVector(param->sliceloc1);
            auto sliceloc2_vec = builder.CreateVector(param->sliceloc2);
            auto fbs_param = falcon::meta_fbs::CreateSliceInfoParam(builder, filename,
                param->slicenum, inodeid_vec, chunkid_vec, sliceid_vec, slicesize_vec,
                sliceoffset_vec, slicelen_vec, sliceloc1_vec, sliceloc2_vec);
            meta_param = falcon::meta_fbs::CreateMetaParam(builder,
                falcon::meta_fbs::AnyMetaParam_SliceInfoParam, fbs_param.Union());
            break;
        }

        case DFC_FETCH_SLICE_ID: {
            auto fbs_param = falcon::meta_fbs::CreateSliceIdParam(builder,
                request.sliceid_param.count, request.sliceid_param.type);
            meta_param = falcon::meta_fbs::CreateMetaParam(builder,
                falcon::meta_fbs::AnyMetaParam_SliceIdParam, fbs_param.Union());
            break;
        }

        default:
            return ARGUMENT_ERROR;
    }

    builder.Finish(meta_param);

    SerializedData sd;
    SerializedDataInit(&sd, NULL, 0, 0, NULL);
    char* buf = SerializedDataApplyForSegment(&sd, builder.GetSize());
    if (!buf) {
        fprintf(stderr, "[WARNING] [FalconMetaService] SerializeRequest: failed to allocate buffer, size=%u\n",
             builder.GetSize());
        return OUT_OF_MEMORY;
    }
    memcpy(buf, builder.GetBufferPointer(), builder.GetSize());

    attachment->append(sd.buffer, sd.size);
    SerializedDataDestroy(&sd);

    return SUCCESS;
}

bool FalconMetaServiceSerializer::DeserializeResponseFromFlatBuffers(
    const butil::IOBuf& attachment,
    FalconMetaServiceResponse* response,
    FalconMetaOperationType operation)
{
    if (attachment.size() < sizeof(sd_size_t)) {
        fprintf(stderr, "[WARNING] [FalconMetaService] DeserializeResponse: attachment too small, size=%zu\n",
             attachment.size());
        return false;
    }

    std::vector<char> buffer(attachment.size());
    attachment.copy_to(&buffer[0], attachment.size());

    SerializedData sd;
    if (!SerializedDataInit(&sd, &buffer[0], buffer.size(), buffer.size(), NULL)) {
        fprintf(stderr, "[WARNING] [FalconMetaService] DeserializeResponse: SerializedDataInit failed\n");
        return false;
    }

    sd_size_t item_size = SerializedDataNextSeveralItemSize(&sd, 0, 1);
    if (item_size == (sd_size_t)-1) {
        fprintf(stderr, "[WARNING] [FalconMetaService] DeserializeResponse: invalid item size\n");
        return false;
    }

    char* fbs_data = &buffer[0] + SERIALIZED_DATA_ALIGNMENT;
    sd_size_t fbs_size = *(sd_size_t*)&buffer[0];
    if (!SystemIsLittleEndian()) {
        fbs_size = ConvertBetweenBigAndLittleEndian(fbs_size);
    }

    flatbuffers::Verifier verifier((uint8_t*)fbs_data, fbs_size);
    if (!verifier.VerifyBuffer<falcon::meta_fbs::MetaResponse>()) {
        fprintf(stderr, "[WARNING] [FalconMetaService] DeserializeResponse: FlatBuffers verification failed\n");
        return false;
    }

    const falcon::meta_fbs::MetaResponse* meta_response = falcon::meta_fbs::GetMetaResponse(fbs_data);
    response->opcode = operation;
    response->status = meta_response->error_code();

    if (response->status != SUCCESS) {
        fprintf(stderr, "[LOG] [FalconMetaService] DeserializeResponse: opcode=%d, error_code=%d, creating empty response\n",
             static_cast<int>(operation), response->status);

        switch (operation) {
            case DFC_CREATE: {
                response->data = new CreateResponse();
                memset(response->data, 0, sizeof(CreateResponse));
                return true;
            }
            case DFC_STAT: {
                response->data = new StatResponse();
                memset(response->data, 0, sizeof(StatResponse));
                return true;
            }
            case DFC_OPEN: {
                response->data = new OpenResponse();
                memset(response->data, 0, sizeof(OpenResponse));
                return true;
            }
            case DFC_UNLINK: {
                response->data = new UnlinkResponse();
                memset(response->data, 0, sizeof(UnlinkResponse));
                return true;
            }
            case DFC_READDIR: {
                response->data = new ReadDirResponse();
                memset(response->data, 0, sizeof(ReadDirResponse));
                return true;
            }
            case DFC_OPENDIR: {
                response->data = new OpenDirResponse();
                memset(response->data, 0, sizeof(OpenDirResponse));
                return true;
            }
            case DFC_GET_KV_META: {
                response->data = new KvDataResponse();
                memset(response->data, 0, sizeof(KvDataResponse));
                return true;
            }
            case DFC_SLICE_GET: {
                response->data = new SliceInfoResponse();
                memset(response->data, 0, sizeof(SliceInfoResponse));
                return true;
            }
            case DFC_PLAIN_COMMAND: {
                response->data = new PlainCommandResponse();
                memset(response->data, 0, sizeof(PlainCommandResponse));
                return true;
            }
            default:
                response->data = nullptr;
                return true;
        }
    }

    switch (operation) {
        case DFC_MKDIR:
        case DFC_RMDIR:
        case DFC_CLOSE:
        case DFC_RENAME:
        case DFC_UTIMENS:
        case DFC_CHOWN:
        case DFC_CHMOD:
        case DFC_PUT_KEY_META:
        case DFC_DELETE_KV_META:
        case DFC_SLICE_PUT:
        case DFC_SLICE_DEL:
            response->data = nullptr;
            return true;

        case DFC_CREATE: {
            if (meta_response->response_type() != falcon::meta_fbs::AnyMetaResponse_CreateResponse) {
                return false;
            }
            const auto* fbs_resp = meta_response->response_as_CreateResponse();
            CreateResponse* create_resp = new CreateResponse();
            create_resp->st_ino = fbs_resp->st_ino();
            create_resp->node_id = fbs_resp->node_id();
            create_resp->st_dev = fbs_resp->st_dev();
            create_resp->st_mode = fbs_resp->st_mode();
            create_resp->st_nlink = fbs_resp->st_nlink();
            create_resp->st_uid = fbs_resp->st_uid();
            create_resp->st_gid = fbs_resp->st_gid();
            create_resp->st_rdev = fbs_resp->st_rdev();
            create_resp->st_size = fbs_resp->st_size();
            create_resp->st_blksize = fbs_resp->st_blksize();
            create_resp->st_blocks = fbs_resp->st_blocks();
            create_resp->st_atim = fbs_resp->st_atim();
            create_resp->st_mtim = fbs_resp->st_mtim();
            create_resp->st_ctim = fbs_resp->st_ctim();
            response->data = create_resp;
            return true;
        }

        case DFC_STAT: {
            if (meta_response->response_type() != falcon::meta_fbs::AnyMetaResponse_StatResponse) {
                return false;
            }
            const auto* fbs_resp = meta_response->response_as_StatResponse();
            StatResponse* stat_resp = new StatResponse();
            stat_resp->st_ino = fbs_resp->st_ino();
            stat_resp->st_dev = fbs_resp->st_dev();
            stat_resp->st_mode = fbs_resp->st_mode();
            stat_resp->st_nlink = fbs_resp->st_nlink();
            stat_resp->st_uid = fbs_resp->st_uid();
            stat_resp->st_gid = fbs_resp->st_gid();
            stat_resp->st_rdev = fbs_resp->st_rdev();
            stat_resp->st_size = fbs_resp->st_size();
            stat_resp->st_blksize = fbs_resp->st_blksize();
            stat_resp->st_blocks = fbs_resp->st_blocks();
            stat_resp->st_atim = fbs_resp->st_atim();
            stat_resp->st_mtim = fbs_resp->st_mtim();
            stat_resp->st_ctim = fbs_resp->st_ctim();
            response->data = stat_resp;
            return true;
        }

        case DFC_OPEN: {
            if (meta_response->response_type() != falcon::meta_fbs::AnyMetaResponse_OpenResponse) {
                return false;
            }
            const auto* fbs_resp = meta_response->response_as_OpenResponse();
            OpenResponse* open_resp = new OpenResponse();
            open_resp->st_ino = fbs_resp->st_ino();
            open_resp->node_id = fbs_resp->node_id();
            open_resp->st_dev = fbs_resp->st_dev();
            open_resp->st_mode = fbs_resp->st_mode();
            open_resp->st_nlink = fbs_resp->st_nlink();
            open_resp->st_uid = fbs_resp->st_uid();
            open_resp->st_gid = fbs_resp->st_gid();
            open_resp->st_rdev = fbs_resp->st_rdev();
            open_resp->st_size = fbs_resp->st_size();
            open_resp->st_blksize = fbs_resp->st_blksize();
            open_resp->st_blocks = fbs_resp->st_blocks();
            open_resp->st_atim = fbs_resp->st_atim();
            open_resp->st_mtim = fbs_resp->st_mtim();
            open_resp->st_ctim = fbs_resp->st_ctim();
            response->data = open_resp;
            return true;
        }

        case DFC_UNLINK: {
            if (meta_response->response_type() != falcon::meta_fbs::AnyMetaResponse_UnlinkResponse) {
                return false;
            }
            const auto* fbs_resp = meta_response->response_as_UnlinkResponse();
            UnlinkResponse* unlink_resp = new UnlinkResponse();
            unlink_resp->st_ino = fbs_resp->st_ino();
            unlink_resp->st_size = fbs_resp->st_size();
            unlink_resp->node_id = fbs_resp->node_id();
            response->data = unlink_resp;
            return true;
        }

        case DFC_OPENDIR: {
            if (meta_response->response_type() != falcon::meta_fbs::AnyMetaResponse_OpenDirResponse) {
                return false;
            }
            const auto* fbs_resp = meta_response->response_as_OpenDirResponse();
            OpenDirResponse* opendir_resp = new OpenDirResponse();
            opendir_resp->st_ino = fbs_resp->st_ino();
            response->data = opendir_resp;
            return true;
        }

        case DFC_READDIR: {
            if (meta_response->response_type() != falcon::meta_fbs::AnyMetaResponse_ReadDirResponse) {
                return false;
            }
            const auto* fbs_resp = meta_response->response_as_ReadDirResponse();
            ReadDirResponse* readdir_resp = new ReadDirResponse();
            readdir_resp->last_shard_index = fbs_resp->last_shard_index();
            if (fbs_resp->last_file_name()) {
                readdir_resp->last_file_name = fbs_resp->last_file_name()->str();
            }
            if (fbs_resp->result_list()) {
                for (const auto* entry : *fbs_resp->result_list()) {
                    OneReadDirResponse one_entry;
                    if (entry->file_name()) {
                        one_entry.file_name = entry->file_name()->str();
                    }
                    one_entry.st_mode = entry->st_mode();
                    readdir_resp->result_list.push_back(one_entry);
                }
            }
            response->data = readdir_resp;
            return true;
        }

        case DFC_GET_KV_META: {
            if (meta_response->response_type() != falcon::meta_fbs::AnyMetaResponse_GetKVMetaResponse) {
                return false;
            }
            const auto* fbs_resp = meta_response->response_as_GetKVMetaResponse();
            KvDataResponse* kv_resp = new KvDataResponse();
            kv_resp->kv_data.valueLen = fbs_resp->value_len();
            kv_resp->kv_data.sliceNum = fbs_resp->slice_num();
            if (fbs_resp->value_key() && fbs_resp->location() && fbs_resp->size()) {
                for (size_t i = 0; i < fbs_resp->value_key()->size(); ++i) {
                    FormDataSlice slice;
                    slice.value_key = fbs_resp->value_key()->Get(i);
                    slice.location = fbs_resp->location()->Get(i);
                    slice.size = fbs_resp->size()->Get(i);
                    kv_resp->kv_data.dataSlices.push_back(slice);
                }
            }
            response->data = kv_resp;
            return true;
        }

        case DFC_SLICE_GET: {
            if (meta_response->response_type() != falcon::meta_fbs::AnyMetaResponse_SliceInfoResponse) {
                return false;
            }
            const auto* fbs_resp = meta_response->response_as_SliceInfoResponse();
            SliceInfoResponse* slice_resp = new SliceInfoResponse();
            slice_resp->slicenum = fbs_resp->slicenum();
            if (fbs_resp->inodeid()) {
                for (size_t i = 0; i < fbs_resp->inodeid()->size(); ++i) {
                    slice_resp->inodeid.push_back(fbs_resp->inodeid()->Get(i));
                    slice_resp->chunkid.push_back(fbs_resp->chunkid()->Get(i));
                    slice_resp->sliceid.push_back(fbs_resp->sliceid()->Get(i));
                    slice_resp->slicesize.push_back(fbs_resp->slicesize()->Get(i));
                    slice_resp->sliceoffset.push_back(fbs_resp->sliceoffset()->Get(i));
                    slice_resp->slicelen.push_back(fbs_resp->slicelen()->Get(i));
                    slice_resp->sliceloc1.push_back(fbs_resp->sliceloc1()->Get(i));
                    slice_resp->sliceloc2.push_back(fbs_resp->sliceloc2()->Get(i));
                }
            }
            response->data = slice_resp;
            return true;
        }

        case DFC_FETCH_SLICE_ID: {
            if (meta_response->response_type() != falcon::meta_fbs::AnyMetaResponse_SliceIdResponse) {
                return false;
            }
            const auto* fbs_resp = meta_response->response_as_SliceIdResponse();
            SliceIdResponse* sliceid_resp = new SliceIdResponse();
            sliceid_resp->start = fbs_resp->startid();
            sliceid_resp->end = fbs_resp->endid();
            response->data = sliceid_resp;
            return true;
        }

        case DFC_PLAIN_COMMAND: {
            if (meta_response->response_type() != falcon::meta_fbs::AnyMetaResponse_PlainCommandResponse) {
                return false;
            }
            const auto* fbs_resp = meta_response->response_as_PlainCommandResponse();
            PlainCommandResponse* plain_resp = new PlainCommandResponse();
            plain_resp->row = fbs_resp->row();
            plain_resp->col = fbs_resp->col();
            if (fbs_resp->data()) {
                for (const auto* item : *fbs_resp->data()) {
                    plain_resp->data.push_back(item->str());
                }
            }
            response->data = plain_resp;
            return true;
        }

        default:
            return false;
    }
}

} // namespace meta_service
} // namespace falcon
