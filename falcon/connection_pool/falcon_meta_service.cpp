/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "connection_pool/falcon_meta_service.h"
#include "connection_pool/pg_connection.h"
#include "connection_pool/connection_pool_config.h"
#include "connection_pool/pg_connection_pool.h"
#include "falcon_meta_rpc.pb.h"
#include "connection_pool/task.h"
#include <brpc/controller.h>
#include <google/protobuf/stubs/callback.h>
#include <libpq-fe.h>
#include <sstream>
#include <cstring>
#include <thread>
#include <cstdlib>
#include <vector>

namespace falcon {
namespace meta_service {

extern FalconMetaService* g_falcon_meta_service;

static void HandleFalconMetaResponse(brpc::Controller* cntl,
                                     falcon::meta_proto::Empty* proto_response,
                                     AsyncFalconMetaServiceJob* original_job)
{
    FalconMetaServiceResponse& response = original_job->GetResponse();
    if (!FalconMetaService::DeserializeResponseFromBinary(
            cntl->response_attachment(),
            &response,
            original_job->GetRequest().operation)) {
        response.status = -1;
    }

    original_job->Done();

    delete cntl;
    delete proto_response;
    delete original_job;
}

int SubmitFalconMetaRequest(const FalconMetaServiceRequest& request,
                            FalconMetaServiceCallback callback,
                            void* user_context)
{
    if (g_falcon_meta_service == nullptr) {
        return -1;
    }

    AsyncFalconMetaServiceJob* job = new AsyncFalconMetaServiceJob(request, callback, user_context);

    return g_falcon_meta_service->DispatchFalconMetaServiceJob(job);
}

int FalconMetaService::DispatchFalconMetaServiceJob(AsyncFalconMetaServiceJob* job)
{
    if (pgConnectionPool == nullptr) {
        if (job != nullptr) {
            job->GetResponse().status = -1;
            job->Done();
            delete job;
        }
        return -1;
    }

    FalconMetaServiceRequest& request = job->GetRequest();

    // 1. 创建 BRPC Controller 和 Protobuf 请求
    brpc::Controller* cntl = new brpc::Controller();
    falcon::meta_proto::MetaRequest* proto_request = new falcon::meta_proto::MetaRequest();
    falcon::meta_proto::Empty* proto_response = new falcon::meta_proto::Empty();

    // 2. 序列化请求参数为二进制格式
    if (!SerializeRequestToBinary(request, proto_request, &cntl->request_attachment())) {
        job->GetResponse().status = -1;
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

// 将 FalconMetaOperationType 转换为 falcon::meta_proto::MetaServiceType
static falcon::meta_proto::MetaServiceType ConvertToProtoType(FalconMetaOperationType op)
{
    switch (op) {
        case DFC_PUT_KEY_META: return falcon::meta_proto::MetaServiceType::PUT_KEY_META;
        case DFC_GET_KV_META: return falcon::meta_proto::MetaServiceType::GET_KV_META;
        case DFC_DELETE_KV_META: return falcon::meta_proto::MetaServiceType::DELETE_KEY_META;
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
        default: return falcon::meta_proto::MetaServiceType::PLAIN_COMMAND;
    }
}

bool FalconMetaService::SerializeRequestToBinary(
    const FalconMetaServiceRequest& request,
    falcon::meta_proto::MetaRequest* proto_request,
    butil::IOBuf* attachment)
{
    falcon::meta_proto::MetaServiceType proto_type = ConvertToProtoType(request.operation);
    proto_request->add_type(proto_type);
    proto_request->set_allow_batch_with_others(request.allow_batch_with_others());
    proto_request->set_format(falcon::meta_proto::SerializationFormat::BINARY);

    size_t total_size = sizeof(BinaryHeader);

    switch (request.operation) {
        case DFC_MKDIR:
        case DFC_CREATE:
        case DFC_STAT:
        case DFC_OPEN:
        case DFC_UNLINK:
        case DFC_OPENDIR:
        case DFC_RMDIR:
            // header + path_len(2) + path
            total_size += sizeof(uint16_t) + request.path.length();
            break;

        case DFC_READDIR: {
            const ReadDirParam* param = meta_param_helper::Get<ReadDirParam>(request.file_params);
            if (!param) return false;
            // header + path_len(2) + path + max_read_count(4) + last_shard_index(4) + last_file_name_len(2) + last_file_name
            total_size += sizeof(uint16_t) + param->path.length() + 4 + 4 + sizeof(uint16_t) + param->last_file_name.length();
            break;
        }

        case DFC_MKDIR_SUB_MKDIR: {
            const MkdirSubMkdirParam* param = meta_param_helper::Get<MkdirSubMkdirParam>(request.file_params);
            if (!param) return false;
            // header + parent_id(8) + name_len(2) + name + inode_id(8)
            total_size += 8 + sizeof(uint16_t) + param->name.length() + 8;
            break;
        }

        case DFC_MKDIR_SUB_CREATE: {
            const MkdirSubCreateParam* param = meta_param_helper::Get<MkdirSubCreateParam>(request.file_params);
            if (!param) return false;
            // header + parent_id_part_id(8) + name_len(2) + name + inode_id(8) + st_mode(4) + st_mtim(8) + st_size(8)
            total_size += 8 + sizeof(uint16_t) + param->name.length() + 8 + 4 + 8 + 8;
            break;
        }

        case DFC_RMDIR_SUB_RMDIR: {
            const RmdirSubRmdirParam* param = meta_param_helper::Get<RmdirSubRmdirParam>(request.file_params);
            if (!param) return false;
            // header + parent_id(8) + name_len(2) + name
            total_size += 8 + sizeof(uint16_t) + param->name.length();
            break;
        }

        case DFC_RMDIR_SUB_UNLINK: {
            const RmdirSubUnlinkParam* param = meta_param_helper::Get<RmdirSubUnlinkParam>(request.file_params);
            if (!param) return false;
            // header + parent_id_part_id(8) + name_len(2) + name
            total_size += 8 + sizeof(uint16_t) + param->name.length();
            break;
        }

        case DFC_RENAME_SUB_RENAME_LOCALLY: {
            const RenameSubRenameLocallyParam* param = meta_param_helper::Get<RenameSubRenameLocallyParam>(request.file_params);
            if (!param) return false;
            // header + src_parent_id(8) + src_parent_id_part_id(8) + src_name_len(2) + src_name
            //        + dst_parent_id(8) + dst_parent_id_part_id(8) + dst_name_len(2) + dst_name
            //        + target_is_directory(1) + directory_inode_id(8) + src_lock_order(4)
            total_size += 8 + 8 + sizeof(uint16_t) + param->src_name.length() +
                          8 + 8 + sizeof(uint16_t) + param->dst_name.length() +
                          1 + 8 + 4;
            break;
        }

        case DFC_RENAME_SUB_CREATE: {
            const RenameSubCreateParam* param = meta_param_helper::Get<RenameSubCreateParam>(request.file_params);
            if (!param) return false;
            // header + parentid_partid(8) + name_len(2) + name + 13个stat字段(8*10+4*3=92) + node_id(4)
            total_size += 8 + sizeof(uint16_t) + param->name.length() + 92 + 4;
            break;
        }

        case DFC_CLOSE: {
            const CloseParam* param = meta_param_helper::Get<CloseParam>(request.file_params);
            if (!param) return false;
            // header + path_len(2) + path + st_size(8) + st_mtim(8) + node_id(4)
            total_size += sizeof(uint16_t) + param->path.length() + 8 + 8 + 4;
            break;
        }

        case DFC_RENAME: {
            const RenameParam* param = meta_param_helper::Get<RenameParam>(request.file_params);
            if (!param) return false;
            // header + src_len(2) + src + dst_len(2) + dst
            total_size += sizeof(uint16_t) + param->src.length() +
                          sizeof(uint16_t) + param->dst.length();
            break;
        }

        case DFC_CHOWN: {
            const ChownParam* param = meta_param_helper::Get<ChownParam>(request.file_params);
            if (!param) return false;
            // header + path_len(2) + path + uid(4) + gid(4)
            total_size += sizeof(uint16_t) + param->path.length() + 4 + 4;
            break;
        }

        case DFC_CHMOD: {
            const ChmodParam* param = meta_param_helper::Get<ChmodParam>(request.file_params);
            if (!param) return false;
            // header + path_len(2) + path + mode(8)
            total_size += sizeof(uint16_t) + param->path.length() + 8;
            break;
        }

        case DFC_UTIMENS: {
            const UtimeNsParam* param = meta_param_helper::Get<UtimeNsParam>(request.file_params);
            if (!param) return false;
            // header + path_len(2) + path + atim(8) + mtim(8)
            total_size += sizeof(uint16_t) + param->path.length() + 8 + 8;
            break;
        }

        case DFC_PUT_KEY_META:
            // header + key_len(2) + key + value_len(4) + slice_num(2) + slices
            total_size += sizeof(uint16_t) + request.kv_data.key.length();
            total_size += sizeof(uint32_t);  // value_len
            total_size += sizeof(uint16_t);  // slice_num
            // 每个 slice: value_key(8) + location(8) + size(4)
            total_size += request.kv_data.dataSlices.size() * (8 + 8 + 4);
            break;

        case DFC_GET_KV_META:
        case DFC_DELETE_KV_META:
            // header + key_len(2) + key
            total_size += sizeof(uint16_t) + request.kv_data.key.length();
            break;

        case DFC_PLAIN_COMMAND: {
            const PlainCommandParam* param = meta_param_helper::Get<PlainCommandParam>(request.file_params);
            if (!param) return false;
            // header + command_len(2) + command
            total_size += sizeof(uint16_t) + param->command.length();
            break;
        }

        default:
            return false;
    }

    char* buffer = new char[total_size];
    memset(buffer, 0, total_size);
    char* p = buffer;

    BinaryHeader* header = (BinaryHeader*)p;
    header->signature = FALCON_BINARY_SIGNATURE;
    header->count = 1;
    header->operation_type = static_cast<uint32_t>(request.operation);
    header->reserved = 0;
    p += sizeof(BinaryHeader);

    auto write_string = [&p](const std::string& str) {
        uint16_t len = str.length();
        *(uint16_t*)p = len;
        p += sizeof(uint16_t);
        memcpy(p, str.c_str(), len);
        p += len;
    };

    switch (request.operation) {
        case DFC_MKDIR:
        case DFC_CREATE:
        case DFC_STAT:
        case DFC_OPEN:
        case DFC_UNLINK:
        case DFC_OPENDIR:
        case DFC_RMDIR:
            write_string(request.path);
            break;

        case DFC_READDIR: {
            const ReadDirParam* param = meta_param_helper::Get<ReadDirParam>(request.file_params);
            write_string(param->path);
            *(int32_t*)p = param->max_read_count;
            p += 4;
            *(int32_t*)p = param->last_shard_index;
            p += 4;
            write_string(param->last_file_name);
            break;
        }

        case DFC_MKDIR_SUB_MKDIR: {
            const MkdirSubMkdirParam* param = meta_param_helper::Get<MkdirSubMkdirParam>(request.file_params);
            *(uint64_t*)p = param->parent_id;
            p += 8;
            write_string(param->name);
            *(uint64_t*)p = param->inode_id;
            p += 8;
            break;
        }

        case DFC_MKDIR_SUB_CREATE: {
            const MkdirSubCreateParam* param = meta_param_helper::Get<MkdirSubCreateParam>(request.file_params);
            *(uint64_t*)p = param->parent_id_part_id;
            p += 8;
            write_string(param->name);
            *(uint64_t*)p = param->inode_id;
            p += 8;
            *(uint32_t*)p = param->st_mode;
            p += 4;
            *(uint64_t*)p = param->st_mtim;
            p += 8;
            *(int64_t*)p = param->st_size;
            p += 8;
            break;
        }

        case DFC_RMDIR_SUB_RMDIR: {
            const RmdirSubRmdirParam* param = meta_param_helper::Get<RmdirSubRmdirParam>(request.file_params);
            *(uint64_t*)p = param->parent_id;
            p += 8;
            write_string(param->name);
            break;
        }

        case DFC_RMDIR_SUB_UNLINK: {
            const RmdirSubUnlinkParam* param = meta_param_helper::Get<RmdirSubUnlinkParam>(request.file_params);
            *(uint64_t*)p = param->parent_id_part_id;
            p += 8;
            write_string(param->name);
            break;
        }

        case DFC_RENAME_SUB_RENAME_LOCALLY: {
            const RenameSubRenameLocallyParam* param = meta_param_helper::Get<RenameSubRenameLocallyParam>(request.file_params);
            *(uint64_t*)p = param->src_parent_id;
            p += 8;
            *(uint64_t*)p = param->src_parent_id_part_id;
            p += 8;
            write_string(param->src_name);
            *(uint64_t*)p = param->dst_parent_id;
            p += 8;
            *(uint64_t*)p = param->dst_parent_id_part_id;
            p += 8;
            write_string(param->dst_name);
            *(uint8_t*)p = param->target_is_directory ? 1 : 0;
            p += 1;
            *(uint64_t*)p = param->directory_inode_id;
            p += 8;
            *(int32_t*)p = param->src_lock_order;
            p += 4;
            break;
        }

        case DFC_RENAME_SUB_CREATE: {
            const RenameSubCreateParam* param = meta_param_helper::Get<RenameSubCreateParam>(request.file_params);
            *(uint64_t*)p = param->parentid_partid;
            p += 8;
            write_string(param->name);
            *(uint64_t*)p = param->st_ino;
            p += 8;
            *(uint64_t*)p = param->st_dev;
            p += 8;
            *(uint32_t*)p = param->st_mode;
            p += 4;
            *(uint64_t*)p = param->st_nlink;
            p += 8;
            *(uint32_t*)p = param->st_uid;
            p += 4;
            *(uint32_t*)p = param->st_gid;
            p += 4;
            *(uint64_t*)p = param->st_rdev;
            p += 8;
            *(int64_t*)p = param->st_size;
            p += 8;
            *(int64_t*)p = param->st_blksize;
            p += 8;
            *(int64_t*)p = param->st_blocks;
            p += 8;
            *(uint64_t*)p = param->st_atim;
            p += 8;
            *(uint64_t*)p = param->st_mtim;
            p += 8;
            *(uint64_t*)p = param->st_ctim;
            p += 8;
            *(int32_t*)p = param->node_id;
            p += 4;
            break;
        }

        case DFC_CLOSE: {
            const CloseParam* param = meta_param_helper::Get<CloseParam>(request.file_params);
            write_string(param->path);
            *(int64_t*)p = param->st_size;
            p += 8;
            *(uint64_t*)p = param->st_mtim;
            p += 8;
            *(int32_t*)p = param->node_id;
            p += 4;
            break;
        }

        case DFC_RENAME: {
            const RenameParam* param = meta_param_helper::Get<RenameParam>(request.file_params);
            write_string(param->src);
            write_string(param->dst);
            break;
        }

        case DFC_CHOWN: {
            const ChownParam* param = meta_param_helper::Get<ChownParam>(request.file_params);
            write_string(param->path);
            *(uint32_t*)p = param->st_uid;
            p += 4;
            *(uint32_t*)p = param->st_gid;
            p += 4;
            break;
        }

        case DFC_CHMOD: {
            const ChmodParam* param = meta_param_helper::Get<ChmodParam>(request.file_params);
            write_string(param->path);
            *(uint64_t*)p = param->st_mode;
            p += 8;
            break;
        }

        case DFC_UTIMENS: {
            const UtimeNsParam* param = meta_param_helper::Get<UtimeNsParam>(request.file_params);
            write_string(param->path);
            *(uint64_t*)p = param->st_atim;
            p += 8;
            *(uint64_t*)p = param->st_mtim;
            p += 8;
            break;
        }

        case DFC_PUT_KEY_META:
            write_string(request.kv_data.key);
            *(uint32_t*)p = request.kv_data.valueLen;
            p += 4;
            *(uint16_t*)p = request.kv_data.sliceNum;
            p += 2;
            for (const auto& slice : request.kv_data.dataSlices) {
                *(uint64_t*)p = slice.value_key;
                p += 8;
                *(uint64_t*)p = slice.location;
                p += 8;
                *(uint32_t*)p = slice.size;
                p += 4;
            }
            break;

        case DFC_GET_KV_META:
        case DFC_DELETE_KV_META:
            write_string(request.kv_data.key);
            break;

        case DFC_PLAIN_COMMAND: {
            const PlainCommandParam* param = meta_param_helper::Get<PlainCommandParam>(request.file_params);
            write_string(param->command);
            break;
        }

        default:
            delete[] buffer;
            return false;
    }

    attachment->append_user_data(buffer, total_size,
                                 [](void* buf) { delete[] (char*)buf; });

    return true;
}

bool FalconMetaService::DeserializeResponseFromBinary(
    const butil::IOBuf& attachment,
    FalconMetaServiceResponse* response,
    FalconMetaOperationType operation)
{
    if (attachment.size() < sizeof(int32_t)) {
        return false;
    }

    std::vector<char> buffer(attachment.size());
    attachment.copy_to(&buffer[0], attachment.size());

    char* p = &buffer[0];

    if (operation == DFC_MKDIR || operation == DFC_CREATE ||
        operation == DFC_RMDIR || operation == DFC_UNLINK ||
        operation == DFC_CLOSE || operation == DFC_RENAME ||
        operation == DFC_UTIMENS || operation == DFC_CHOWN ||
        operation == DFC_CHMOD || operation == DFC_OPENDIR ||
        operation == DFC_PUT_KEY_META || operation == DFC_DELETE_KV_META) {

        response->status = *(int32_t*)p;
        response->data = nullptr;
        return true;
    }

    if (operation == DFC_STAT) {
        int32_t status = *(int32_t*)p;
        p += sizeof(int32_t);

        response->status = status;
        if (status != 0) {
            response->data = nullptr;
            return true;
        }

        StatResponse* stat_resp = new StatResponse();
        stat_resp->st_ino = *(uint64_t*)p; p += 8;
        stat_resp->st_dev = *(uint64_t*)p; p += 8;
        stat_resp->st_mode = *(uint32_t*)p; p += 4;
        stat_resp->st_nlink = *(uint64_t*)p; p += 8;
        stat_resp->st_uid = *(uint32_t*)p; p += 4;
        stat_resp->st_gid = *(uint32_t*)p; p += 4;
        stat_resp->st_rdev = *(uint64_t*)p; p += 8;
        stat_resp->st_size = *(int64_t*)p; p += 8;
        stat_resp->st_blksize = *(int64_t*)p; p += 8;
        stat_resp->st_blocks = *(int64_t*)p; p += 8;
        stat_resp->st_atim = *(uint64_t*)p; p += 8;
        stat_resp->st_mtim = *(uint64_t*)p; p += 8;
        stat_resp->st_ctim = *(uint64_t*)p; p += 8;

        response->data = stat_resp;
        return true;
    }

    if (operation == DFC_OPEN) {
        int32_t status = *(int32_t*)p;
        p += sizeof(int32_t);

        response->status = status;
        if (status != 0) {
            response->data = nullptr;
            return true;
        }

        OpenResponse* open_resp = new OpenResponse();
        open_resp->st_ino = *(uint64_t*)p; p += 8;
        open_resp->node_id = *(int64_t*)p; p += 8;
        open_resp->st_dev = *(uint64_t*)p; p += 8;
        open_resp->st_mode = *(uint32_t*)p; p += 4;
        open_resp->st_nlink = *(uint64_t*)p; p += 8;
        open_resp->st_uid = *(uint32_t*)p; p += 4;
        open_resp->st_gid = *(uint32_t*)p; p += 4;
        open_resp->st_rdev = *(uint64_t*)p; p += 8;
        open_resp->st_size = *(int64_t*)p; p += 8;
        open_resp->st_blksize = *(int64_t*)p; p += 8;
        open_resp->st_blocks = *(int64_t*)p; p += 8;
        open_resp->st_atim = *(uint64_t*)p; p += 8;
        open_resp->st_mtim = *(uint64_t*)p; p += 8;
        open_resp->st_ctim = *(uint64_t*)p; p += 8;

        response->data = open_resp;
        return true;
    }

    if (operation == DFC_READDIR) {
        int32_t status = *(int32_t*)p;
        p += sizeof(int32_t);

        response->status = status;
        if (status != 0) {
            response->data = nullptr;
            return true;
        }

        ReadDirResponse* readdir_resp = new ReadDirResponse();
        readdir_resp->last_shard_index = *(int32_t*)p; p += 4;

        uint16_t name_len = *(uint16_t*)p; p += 2;
        readdir_resp->last_file_name = std::string(p, name_len);
        p += name_len;

        uint32_t entry_count = *(uint32_t*)p; p += 4;
        for (uint32_t i = 0; i < entry_count; i++) {
            OneReadDirResponse entry;

            uint16_t file_name_len = *(uint16_t*)p; p += 2;
            entry.file_name = std::string(p, file_name_len);
            p += file_name_len;

            entry.st_mode = *(uint32_t*)p; p += 4;

            readdir_resp->result_list.push_back(entry);
        }

        response->data = readdir_resp;
        return true;
    }

    // GET_KV_META 操作
    if (operation == DFC_GET_KV_META) {
        int32_t status = *(int32_t*)p;
        p += sizeof(int32_t);

        response->status = status;
        if (status != 0) {
            response->data = nullptr;
            return true;
        }

        KvDataResponse* kv_resp = new KvDataResponse();

        uint16_t key_len = *(uint16_t*)p; p += 2;
        kv_resp->kv_data.key = std::string(p, key_len);
        p += key_len;

        kv_resp->kv_data.valueLen = *(uint32_t*)p; p += 4;
        kv_resp->kv_data.sliceNum = *(uint16_t*)p; p += 2;

        for (uint16_t i = 0; i < kv_resp->kv_data.sliceNum; i++) {
            FormDataSlice slice;
            slice.value_key = *(uint64_t*)p; p += 8;
            slice.location = *(uint64_t*)p; p += 8;
            slice.size = *(uint32_t*)p; p += 4;
            kv_resp->kv_data.dataSlices.push_back(slice);
        }

        response->data = kv_resp;
        return true;
    }

    // PLAIN_COMMAND 操作
    if (operation == DFC_PLAIN_COMMAND) {
        int32_t status = *(int32_t*)p;
        p += sizeof(int32_t);

        response->status = status;
        if (status != 0) {
            response->data = nullptr;
            return true;
        }

        PlainCommandResponse* plain_resp = new PlainCommandResponse();
        plain_resp->row = *(uint32_t*)p; p += 4;
        plain_resp->col = *(uint32_t*)p; p += 4;

        for (uint32_t i = 0; i < plain_resp->row * plain_resp->col; i++) {
            uint16_t str_len = *(uint16_t*)p; p += 2;
            plain_resp->data.push_back(std::string(p, str_len));
            p += str_len;
        }

        response->data = plain_resp;
        return true;
    }

    return false;
}

} // namespace meta_service
} // namespace falcon
