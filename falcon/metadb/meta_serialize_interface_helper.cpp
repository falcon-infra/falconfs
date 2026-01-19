/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

extern "C" {
#include "metadb/meta_serialize_interface_helper.h"
}

#include "falcon_meta_param_generated.h"
#include "falcon_meta_response_generated.h"
#include "utils/error_log.h"

static flatbuffers::FlatBufferBuilder FlatBufferBuilderPerProcess;

FalconSupportMetaService MetaServiceTypeDecode(int32_t type){
    return static_cast<FalconSupportMetaService>(type);
}

int32_t MetaServiceTypeEncode(FalconSupportMetaService metaService){
    return static_cast<int32_t>(metaService);
}

bool SerializedDataMetaParamDecode(FalconSupportMetaService metaService,
                                   int count,
                                   SerializedData *param,
                                   MetaProcessInfoData *infoArray)
{
    sd_size_t p = 0;
    for (int i = 0; i < count; ++i) {
        uint8_t *buffer = (uint8_t *)param->buffer + p;
        sd_size_t size = SerializedDataNextSeveralItemSize(param, p, 1);
        if (size == (sd_size_t)-1) {
            FALCON_ELOG_LOG_EXTENDED("[debug] serialized param is corrupt.");
            return false;
        }

        uint8_t *itemBuffer = (uint8_t *)buffer + SERIALIZED_DATA_ALIGNMENT;
        size_t itemSize = size - SERIALIZED_DATA_ALIGNMENT;
        flatbuffers::Verifier verifier(itemBuffer, itemSize);
        if (!verifier.VerifyBuffer<falcon::meta_fbs::MetaParam>(NULL)) {
            FALCON_ELOG_LOG_EXTENDED("[debug] itemSize = %lu, serialized param is corrupt.", itemSize);
            return false;
        }
        auto metaParam = falcon::meta_fbs::GetMetaParam(itemBuffer);

        MetaProcessInfo info = infoArray + i;
        switch (metaService) {
        case FalconMetaServiceType::MKDIR:
        case FalconMetaServiceType::CREATE:
        case FalconMetaServiceType::STAT:
        case FalconMetaServiceType::OPEN:
        case FalconMetaServiceType::UNLINK:
        case FalconMetaServiceType::OPENDIR:
        case FalconMetaServiceType::RMDIR: {
            // path only param
            if (metaParam->param_type() != falcon::meta_fbs::AnyMetaParam::AnyMetaParam_PathOnlyParam) {
                FALCON_ELOG_LOG_EXTENDED("[debug] serialized param is corrupt.");
                return false;
            }
            info->path = metaParam->param_as_PathOnlyParam()->path()->c_str();
            break;
        }
        case FalconMetaServiceType::MKDIR_SUB_MKDIR: {
            if (metaParam->param_type() != falcon::meta_fbs::AnyMetaParam::AnyMetaParam_MkdirSubMkdirParam) {
                FALCON_ELOG_LOG_EXTENDED("[debug] serialized param is corrupt.");
                return false;
            }
            auto mkdirSubMkdirParam = metaParam->param_as_MkdirSubMkdirParam();
            info->parentId = mkdirSubMkdirParam->parent_id();
            info->name = const_cast<char *>(mkdirSubMkdirParam->name()->c_str());
            info->inodeId = mkdirSubMkdirParam->inode_id();
            break;
        }
        case FalconMetaServiceType::MKDIR_SUB_CREATE: {
            if (metaParam->param_type() != falcon::meta_fbs::AnyMetaParam::AnyMetaParam_MkdirSubCreateParam) {
                FALCON_ELOG_LOG_EXTENDED("[debug] serialized param is corrupt.");
                return false;
            }
            auto mkdirSubCreateParam = metaParam->param_as_MkdirSubCreateParam();
            info->parentId_partId = mkdirSubCreateParam->parent_id_part_id();
            info->name = const_cast<char *>(mkdirSubCreateParam->name()->c_str());
            info->inodeId = mkdirSubCreateParam->inode_id();
            info->st_mode = mkdirSubCreateParam->st_mode();
            info->st_size = mkdirSubCreateParam->st_size();
            info->st_mtim = mkdirSubCreateParam->st_mtim();
            break;
        }
        case FalconMetaServiceType::CLOSE: {
            if (metaParam->param_type() != falcon::meta_fbs::AnyMetaParam::AnyMetaParam_CloseParam) {
                FALCON_ELOG_LOG_EXTENDED("[debug] serialized param is corrupt.");
                return false;
            }
            auto closeParam = metaParam->param_as_CloseParam();
            info->path = closeParam->path()->c_str();
            info->st_size = closeParam->st_size();
            info->st_mtim = closeParam->st_mtim();
            info->node_id = closeParam->node_id();
            break;
        }
        case FalconMetaServiceType::READDIR: {
            if (metaParam->param_type() != falcon::meta_fbs::AnyMetaParam::AnyMetaParam_ReadDirParam) {
                FALCON_ELOG_LOG_EXTENDED("[debug] serialized param is corrupt.");
                return false;
            }
            auto readDirParam = metaParam->param_as_ReadDirParam();
            info->path = readDirParam->path()->c_str();
            info->readDirMaxReadCount = readDirParam->max_read_count();
            info->readDirLastShardIndex = readDirParam->last_shard_index();
            info->readDirLastFileName = readDirParam->last_file_name()->c_str();
            break;
        }
        case FalconMetaServiceType::RMDIR_SUB_RMDIR: {
            if (metaParam->param_type() != falcon::meta_fbs::AnyMetaParam::AnyMetaParam_RmdirSubRmdirParam) {
                FALCON_ELOG_LOG_EXTENDED("[debug] serialized param is corrupt.");
                return false;
            }
            auto rmdirSubRmdirParam = metaParam->param_as_RmdirSubRmdirParam();
            info->parentId = rmdirSubRmdirParam->parent_id();
            info->name = const_cast<char *>(rmdirSubRmdirParam->name()->c_str());
            break;
        }
        case FalconMetaServiceType::RMDIR_SUB_UNLINK: {
            if (metaParam->param_type() != falcon::meta_fbs::AnyMetaParam::AnyMetaParam_RmdirSubUnlinkParam) {
                FALCON_ELOG_LOG_EXTENDED("[debug] serialized param is corrupt.");
                return false;
            }
            auto rmdirSubUnlinkParam = metaParam->param_as_RmdirSubUnlinkParam();
            info->parentId_partId = rmdirSubUnlinkParam->parent_id_part_id();
            info->name = const_cast<char *>(rmdirSubUnlinkParam->name()->c_str());
            break;
        }
        case FalconMetaServiceType::RENAME: {
            if (metaParam->param_type() != falcon::meta_fbs::AnyMetaParam::AnyMetaParam_RenameParam) {
                FALCON_ELOG_LOG_EXTENDED("[debug] serialized param is corrupt.");
                return false;
            }
            auto renameParam = metaParam->param_as_RenameParam();
            info->path = renameParam->src()->c_str();
            info->dstPath = renameParam->dst()->c_str();
            break;
        }
        case FalconMetaServiceType::RENAME_SUB_RENAME_LOCALLY: {
            if (metaParam->param_type() != falcon::meta_fbs::AnyMetaParam::AnyMetaParam_RenameSubRenameLocallyParam) {
                FALCON_ELOG_LOG_EXTENDED("[debug] serialized param is corrupt.");
                return false;
            }
            auto renameSubRenameLocallyParam = metaParam->param_as_RenameSubRenameLocallyParam();
            info->parentId = renameSubRenameLocallyParam->src_parent_id();
            info->parentId_partId = renameSubRenameLocallyParam->src_parent_id_part_id();
            info->name = const_cast<char *>(renameSubRenameLocallyParam->src_name()->c_str());
            info->dstParentId =
                renameSubRenameLocallyParam->dst_parent_id(); // TOFO: ymz: modify 'dstParentIdPartId' to 'dstParentId'
            info->dstParentIdPartId = renameSubRenameLocallyParam->dst_parent_id_part_id();
            info->dstName = const_cast<char *>(renameSubRenameLocallyParam->dst_name()->c_str());
            info->targetIsDirectory = renameSubRenameLocallyParam->target_is_directory();
            info->srcLockOrder = renameSubRenameLocallyParam->src_lock_order();
            info->inodeId = renameSubRenameLocallyParam->directory_inode_id();
            break;
        }
        case FalconMetaServiceType::RENAME_SUB_CREATE: {
            if (metaParam->param_type() != falcon::meta_fbs::AnyMetaParam::AnyMetaParam_RenameSubCreateParam) {
                FALCON_ELOG_LOG_EXTENDED("[debug] serialized param is corrupt.");
                return false;
            }
            auto renameSubCreateParam = metaParam->param_as_RenameSubCreateParam();
            info->parentId_partId = renameSubCreateParam->parentid_partid();
            info->name = const_cast<char *>(renameSubCreateParam->name()->c_str());
            info->inodeId = renameSubCreateParam->st_ino();
            info->st_dev = renameSubCreateParam->st_dev();
            info->st_mode = renameSubCreateParam->st_mode();
            info->st_nlink = renameSubCreateParam->st_nlink();
            info->st_uid = renameSubCreateParam->st_uid();
            info->st_gid = renameSubCreateParam->st_gid();
            info->st_rdev = renameSubCreateParam->st_rdev();
            info->st_size = renameSubCreateParam->st_size();
            info->st_blksize = renameSubCreateParam->st_blksize();
            info->st_blocks = renameSubCreateParam->st_blocks();
            info->st_atim = renameSubCreateParam->st_atim();
            info->st_mtim = renameSubCreateParam->st_mtim();
            info->st_ctim = renameSubCreateParam->st_ctim();
            info->node_id = renameSubCreateParam->node_id();
            break;
        }
        case FalconMetaServiceType::UTIMENS: {
            if (metaParam->param_type() != falcon::meta_fbs::AnyMetaParam::AnyMetaParam_UtimeNsParam) {
                FALCON_ELOG_LOG_EXTENDED("[debug] serialized param is corrupt.");
                return false;
            }
            auto utimeNsParam = metaParam->param_as_UtimeNsParam();
            info->path = utimeNsParam->path()->c_str();
            info->st_atim = utimeNsParam->st_atim();
            info->st_mtim = utimeNsParam->st_mtim();
            break;
        }
        case FalconMetaServiceType::CHOWN: {
            if (metaParam->param_type() != falcon::meta_fbs::AnyMetaParam::AnyMetaParam_ChownParam) {
                FALCON_ELOG_LOG_EXTENDED("[debug] serialized param is corrupt.");
                return false;
            }
            auto chownParam = metaParam->param_as_ChownParam();
            info->path = chownParam->path()->c_str();
            info->st_uid = chownParam->st_uid();
            info->st_gid = chownParam->st_gid();
            break;
        }
        case FalconMetaServiceType::CHMOD: {
            if (metaParam->param_type() != falcon::meta_fbs::AnyMetaParam::AnyMetaParam_ChmodParam) {
                FALCON_ELOG_LOG_EXTENDED("[debug] serialized param is corrupt.");
                return false;
            }
            auto chmodParam = metaParam->param_as_ChmodParam();
            info->path = chmodParam->path()->c_str();
            info->st_mode = chmodParam->st_mode();
            break;
        }
        default:
            FALCON_ELOG_LOG_EXTENDED("[debug] serialized param is corrupt.");
            return false;
        }

        p += size;
    }
    return true;
}

bool SerializedDataMetaParamEncode(FalconMetaServiceType metaService,
                                   MetaProcessInfo *infoArray,
                                   int32_t *index,
                                   int count,
                                   flatbuffers::FlatBufferBuilder &builder,
                                   SerializedData *param)
{
    for (int i = 0; i < count; ++i) {
        MetaProcessInfo info = index == NULL ? (infoArray[i]) : (infoArray[index[i]]);

        builder.Clear();
        flatbuffers::Offset<falcon::meta_fbs::MetaParam> metaParam;
        switch (metaService) {
        case FalconMetaServiceType::MKDIR_SUB_MKDIR: {
            auto mkdirSubMkdirParam =
                falcon::meta_fbs::CreateMkdirSubMkdirParamDirect(builder, info->parentId, info->name, info->inodeId);
            metaParam = falcon::meta_fbs::CreateMetaParam(builder,
                                                          falcon::meta_fbs::AnyMetaParam_MkdirSubMkdirParam,
                                                          mkdirSubMkdirParam.Union());
            break;
        }
        case FalconMetaServiceType::MKDIR_SUB_CREATE: {
            auto mkdirSubCreateParam = falcon::meta_fbs::CreateMkdirSubCreateParamDirect(builder,
                                                                                         info->parentId_partId,
                                                                                         info->name,
                                                                                         info->inodeId,
                                                                                         info->st_mode,
                                                                                         info->st_mtim,
                                                                                         info->st_size);
            metaParam = falcon::meta_fbs::CreateMetaParam(builder,
                                                          falcon::meta_fbs::AnyMetaParam_MkdirSubCreateParam,
                                                          mkdirSubCreateParam.Union());
            break;
        }
        case FalconMetaServiceType::RMDIR_SUB_RMDIR: {
            auto rmdirSubRmdirParam =
                falcon::meta_fbs::CreateRmdirSubRmdirParamDirect(builder, info->parentId, info->name);
            metaParam = falcon::meta_fbs::CreateMetaParam(builder,
                                                          falcon::meta_fbs::AnyMetaParam_RmdirSubRmdirParam,
                                                          rmdirSubRmdirParam.Union());
            break;
        }
        case FalconMetaServiceType::RMDIR_SUB_UNLINK: {
            auto rmdirSubUnlinkParam =
                falcon::meta_fbs::CreateRmdirSubUnlinkParamDirect(builder, info->parentId_partId, info->name);
            metaParam = falcon::meta_fbs::CreateMetaParam(builder,
                                                          falcon::meta_fbs::AnyMetaParam_RmdirSubUnlinkParam,
                                                          rmdirSubUnlinkParam.Union());
            break;
        }
        case FalconMetaServiceType::RENAME_SUB_RENAME_LOCALLY: {
            auto renameSubRenameLocallyParam =
                falcon::meta_fbs::CreateRenameSubRenameLocallyParamDirect(builder,
                                                                          info->parentId,
                                                                          info->parentId_partId,
                                                                          info->name,
                                                                          info->dstParentId,
                                                                          info->dstParentIdPartId,
                                                                          info->dstName,
                                                                          info->targetIsDirectory,
                                                                          info->inodeId,
                                                                          info->srcLockOrder);
            metaParam = falcon::meta_fbs::CreateMetaParam(builder,
                                                          falcon::meta_fbs::AnyMetaParam_RenameSubRenameLocallyParam,
                                                          renameSubRenameLocallyParam.Union());
            break;
        }
        case FalconMetaServiceType::RENAME_SUB_CREATE: {
            auto renameSubCreateParam = falcon::meta_fbs::CreateRenameSubCreateParamDirect(builder,
                                                                                           info->parentId_partId,
                                                                                           info->name,
                                                                                           info->inodeId,
                                                                                           info->st_dev,
                                                                                           info->st_mode,
                                                                                           info->st_nlink,
                                                                                           info->st_uid,
                                                                                           info->st_gid,
                                                                                           info->st_rdev,
                                                                                           info->st_size,
                                                                                           info->st_blksize,
                                                                                           info->st_blocks,
                                                                                           info->st_atim,
                                                                                           info->st_mtim,
                                                                                           info->st_ctim,
                                                                                           info->node_id);
            metaParam = falcon::meta_fbs::CreateMetaParam(builder,
                                                          falcon::meta_fbs::AnyMetaParam_RenameSubCreateParam,
                                                          renameSubCreateParam.Union());
            break;
        }
        default:
            return false;
        }
        builder.Finish(metaParam);

        char *buffer = SerializedDataApplyForSegment(param, builder.GetSize());
        memcpy(buffer, builder.GetBufferPointer(), builder.GetSize());
    }
    return true;
}

bool SerializedDataMetaParamEncodeWithPerProcessFlatBufferBuilder(FalconMetaServiceType metaService,
                                                                  MetaProcessInfo *infoArray,
                                                                  int32_t *index,
                                                                  int count,
                                                                  SerializedData *param)
{
    return SerializedDataMetaParamEncode(metaService, infoArray, index, count, FlatBufferBuilderPerProcess, param);
}

bool SerializedDataMetaResponseDecode(FalconMetaServiceType metaService,
                                      int count,
                                      SerializedData *response,
                                      MetaProcessInfoData *infoArray)
{
    sd_size_t p = 0;
    for (int i = 0; i < count; i++) {
        uint8_t *buffer = (uint8_t *)response->buffer + p;
        sd_size_t size = SerializedDataNextSeveralItemSize(response, p, 1);
        if (size == (sd_size_t)-1)
            return false;

        uint8_t *itemBuffer = (uint8_t *)buffer + SERIALIZED_DATA_ALIGNMENT;
        size_t itemSize = size - SERIALIZED_DATA_ALIGNMENT;
        flatbuffers::Verifier verifier(itemBuffer, itemSize);
        if (!verifier.VerifyBuffer<falcon::meta_fbs::MetaResponse>(NULL))
            return false;
        auto metaResponse = falcon::meta_fbs::GetMetaResponse(itemBuffer);

        MetaProcessInfo info = infoArray + i;
        info->errorCode = (FalconErrorCode)metaResponse->error_code();
        switch (metaService) {
        case FalconMetaServiceType::MKDIR_SUB_MKDIR:
        case FalconMetaServiceType::MKDIR_SUB_CREATE:
        case FalconMetaServiceType::RMDIR_SUB_RMDIR:
        case FalconMetaServiceType::RMDIR_SUB_UNLINK:
        case FalconMetaServiceType::RENAME_SUB_CREATE: {
            // Error code only. Do nothing.
            break;
        }
        case FalconMetaServiceType::RENAME_SUB_RENAME_LOCALLY: {
            if (metaResponse->response_type() == falcon::meta_fbs::AnyMetaResponse::AnyMetaResponse_NONE) {
                // No extra data returned. Do nothing.
            } else if (metaResponse->response_type() ==
                       falcon::meta_fbs::AnyMetaResponse::AnyMetaResponse_RenameSubRenameLocallyResponse) {
                //
                auto renameSubRenameLocallyResponse = metaResponse->response_as_RenameSubRenameLocallyResponse();
                info->inodeId = renameSubRenameLocallyResponse->st_ino();
                info->st_dev = renameSubRenameLocallyResponse->st_dev();
                info->st_mode = renameSubRenameLocallyResponse->st_mode();
                info->st_nlink = renameSubRenameLocallyResponse->st_nlink();
                info->st_uid = renameSubRenameLocallyResponse->st_uid();
                info->st_gid = renameSubRenameLocallyResponse->st_gid();
                info->st_rdev = renameSubRenameLocallyResponse->st_rdev();
                info->st_size = renameSubRenameLocallyResponse->st_size();
                info->st_blksize = renameSubRenameLocallyResponse->st_blksize();
                info->st_blocks = renameSubRenameLocallyResponse->st_blocks();
                info->st_atim = renameSubRenameLocallyResponse->st_atim();
                info->st_mtim = renameSubRenameLocallyResponse->st_mtim();
                info->st_ctim = renameSubRenameLocallyResponse->st_ctim();
                info->node_id = renameSubRenameLocallyResponse->node_id();
            } else
                return false;
            break;
        }
        default:
            return false;
        }

        p += size;
    }
    return true;
}

static bool SerializedDataMetaResponseEncode(FalconMetaServiceType metaService,
                                             int count,
                                             MetaProcessInfoData *infoArray,
                                             flatbuffers::FlatBufferBuilder &builder,
                                             SerializedData *response)
{
    for (int i = 0; i < count; ++i) {
        builder.Clear();
        MetaProcessInfo info = infoArray + i;
        flatbuffers::Offset<falcon::meta_fbs::MetaResponse> metaResponse;
        if (info->errorCode != SUCCESS && info->errorCode != FILE_EXISTS) {
            //
            metaResponse = falcon::meta_fbs::CreateMetaResponse(builder, info->errorCode);
        } else {
            switch (metaService) {
            case FalconMetaServiceType::MKDIR:
            case FalconMetaServiceType::MKDIR_SUB_MKDIR:
            case FalconMetaServiceType::MKDIR_SUB_CREATE:
            case FalconMetaServiceType::CLOSE:
            case FalconMetaServiceType::RMDIR:
            case FalconMetaServiceType::RMDIR_SUB_RMDIR:
            case FalconMetaServiceType::RMDIR_SUB_UNLINK:
            case FalconMetaServiceType::RENAME:
            case FalconMetaServiceType::RENAME_SUB_CREATE:
            case FalconMetaServiceType::UTIMENS:
            case FalconMetaServiceType::CHOWN:
            case FalconMetaServiceType::CHMOD: {
                // error code only response
                metaResponse = falcon::meta_fbs::CreateMetaResponse(builder, info->errorCode);
                break;
            }
            case FalconMetaServiceType::CREATE: {
                // auto createResponse = falcon::meta_fbs::CreateCreateResponse(builder, info->inodeId);
                auto createResponse = falcon::meta_fbs::CreateCreateResponse(builder,
                                                                             info->inodeId,
                                                                             info->node_id,
                                                                             info->st_dev,
                                                                             info->st_mode,
                                                                             info->st_nlink,
                                                                             info->st_uid,
                                                                             info->st_gid,
                                                                             info->st_rdev,
                                                                             info->st_size,
                                                                             info->st_blksize,
                                                                             info->st_blocks,
                                                                             info->st_atim,
                                                                             info->st_mtim,
                                                                             info->st_ctim);
                metaResponse = falcon::meta_fbs::CreateMetaResponse(builder,
                                                                    info->errorCode,
                                                                    falcon::meta_fbs::AnyMetaResponse_CreateResponse,
                                                                    createResponse.Union());
                break;
            }
            case FalconMetaServiceType::STAT: {
                auto statResponse = falcon::meta_fbs::CreateStatResponse(builder,
                                                                         info->inodeId,
                                                                         info->st_dev,
                                                                         info->st_mode,
                                                                         info->st_nlink,
                                                                         info->st_uid,
                                                                         info->st_gid,
                                                                         info->st_rdev,
                                                                         info->st_size,
                                                                         info->st_blksize,
                                                                         info->st_blocks,
                                                                         info->st_atim,
                                                                         info->st_mtim,
                                                                         info->st_ctim);
                metaResponse = falcon::meta_fbs::CreateMetaResponse(builder,
                                                                    info->errorCode,
                                                                    falcon::meta_fbs::AnyMetaResponse_StatResponse,
                                                                    statResponse.Union());
                break;
            }
            case FalconMetaServiceType::OPEN: {
                auto openResponse = falcon::meta_fbs::CreateOpenResponse(builder,
                                                                         info->inodeId,
                                                                         info->node_id,
                                                                         info->st_dev,
                                                                         info->st_mode,
                                                                         info->st_nlink,
                                                                         info->st_uid,
                                                                         info->st_gid,
                                                                         info->st_rdev,
                                                                         info->st_size,
                                                                         info->st_blksize,
                                                                         info->st_blocks,
                                                                         info->st_atim,
                                                                         info->st_mtim,
                                                                         info->st_ctim);
                metaResponse = falcon::meta_fbs::CreateMetaResponse(builder,
                                                                    info->errorCode,
                                                                    falcon::meta_fbs::AnyMetaResponse_OpenResponse,
                                                                    openResponse.Union());
                break;
            }
            case FalconMetaServiceType::UNLINK: {
                auto unlinkResponse =
                    falcon::meta_fbs::CreateUnlinkResponse(builder, info->inodeId, info->st_size, info->node_id);
                metaResponse = falcon::meta_fbs::CreateMetaResponse(builder,
                                                                    info->errorCode,
                                                                    falcon::meta_fbs::AnyMetaResponse_UnlinkResponse,
                                                                    unlinkResponse.Union());
                break;
            }
            case FalconMetaServiceType::READDIR: {
                std::vector<flatbuffers::Offset<falcon::meta_fbs::OneReadDirResponse>> readDirResultList;
                for (int j = 0; j < info->readDirResultCount; ++j)
                    readDirResultList.push_back(
                        falcon::meta_fbs::CreateOneReadDirResponseDirect(builder,
                                                                         info->readDirResultList[j]->fileName,
                                                                         info->readDirResultList[j]->mode));
                auto readDirResponse = falcon::meta_fbs::CreateReadDirResponseDirect(builder,
                                                                                     info->readDirLastShardIndex,
                                                                                     info->readDirLastFileName,
                                                                                     &readDirResultList);
                metaResponse = falcon::meta_fbs::CreateMetaResponse(builder,
                                                                    info->errorCode,
                                                                    falcon::meta_fbs::AnyMetaResponse_ReadDirResponse,
                                                                    readDirResponse.Union());
                break;
            }
            case FalconMetaServiceType::OPENDIR: {
                auto openDirResponse = falcon::meta_fbs::CreateOpenDirResponse(builder, info->inodeId);
                metaResponse = falcon::meta_fbs::CreateMetaResponse(builder,
                                                                    info->errorCode,
                                                                    falcon::meta_fbs::AnyMetaResponse_OpenDirResponse,
                                                                    openDirResponse.Union());
                break;
            }
            case FalconMetaServiceType::RENAME_SUB_RENAME_LOCALLY: {
                if (info->parentId_partId != 0 && info->dstParentIdPartId == 0) {
                    auto renameSubRenameLocallyResponse =
                        falcon::meta_fbs::CreateRenameSubRenameLocallyResponse(builder,
                                                                               info->inodeId,
                                                                               info->st_dev,
                                                                               info->st_mode,
                                                                               info->st_nlink,
                                                                               info->st_uid,
                                                                               info->st_gid,
                                                                               info->st_rdev,
                                                                               info->st_size,
                                                                               info->st_blksize,
                                                                               info->st_blocks,
                                                                               info->st_atim,
                                                                               info->st_mtim,
                                                                               info->st_ctim,
                                                                               info->node_id);
                    metaResponse = falcon::meta_fbs::CreateMetaResponse(
                        builder,
                        info->errorCode,
                        falcon::meta_fbs::AnyMetaResponse_RenameSubRenameLocallyResponse,
                        renameSubRenameLocallyResponse.Union());
                } else {
                    metaResponse = falcon::meta_fbs::CreateMetaResponse(builder, info->errorCode);
                }
                break;
            }
            default:
                return false;
            }
        }
        builder.Finish(metaResponse);

        char *buffer = SerializedDataApplyForSegment(response, builder.GetSize());
        memcpy(buffer, builder.GetBufferPointer(), builder.GetSize());
    }
    return true;
}

bool SerializedDataMetaResponseEncodeWithPerProcessFlatBufferBuilder(FalconMetaServiceType metaService,
                                                                     int count,
                                                                     MetaProcessInfoData *infoArray,
                                                                     SerializedData *response)
{
    return SerializedDataMetaResponseEncode(metaService, count, infoArray, FlatBufferBuilderPerProcess, response);
}


bool SerializedKvMetaParamDecode(FalconSupportMetaService metaService, SerializedData *param,
    KvMetaProcessInfo infoData)
{
    uint8_t *buffer = (uint8_t *)param->buffer;
    sd_size_t size = SerializedDataNextSeveralItemSize(param, 0, 1);
    if (size == (sd_size_t) - 1) {
        return false;
    }
    uint8_t *itemBuffer = (uint8_t *)buffer + SERIALIZED_DATA_ALIGNMENT;
    size_t itemSize = size - SERIALIZED_DATA_ALIGNMENT;
    flatbuffers::Verifier verifier(itemBuffer, itemSize);
    if (!verifier.VerifyBuffer<falcon::meta_fbs::MetaParam>(NULL)) {
        return false;
    }
    auto metaParam = falcon::meta_fbs::GetMetaParam(itemBuffer);
    switch (metaService) {
        case FalconSupportMetaService::KV_PUT: {
            if (metaParam->param_type() != falcon::meta_fbs::AnyMetaParam::AnyMetaParam_KVParam) {
                return false;
            }
            auto kvParam = metaParam->param_as_KVParam();
            infoData->userkey = kvParam->key()->c_str();
            infoData->valuelen = kvParam->value_len();
            infoData->slicenum = kvParam->slice_num();
            // vector
            infoData->valuekey = const_cast<uint64_t*>(kvParam->value_key()->data());
            infoData->location = const_cast<uint64_t*>(kvParam->location()->data());
            infoData->slicelen = const_cast<uint32_t*>(kvParam->size()->data());
            break;
        }
        case FalconSupportMetaService::KV_GET:
        case FalconSupportMetaService::KV_DEL: {
            // key only param
            if (metaParam->param_type() != falcon::meta_fbs::AnyMetaParam::AnyMetaParam_KeyOnlyParam) {
                return false;
            }
            infoData->userkey = metaParam->param_as_KeyOnlyParam()->key()->c_str();
            break;
        }
        default:
            return false;
    }
    return true;
}

static bool SerializedkVMetaResponseEncode(FalconSupportMetaService metaService,
                                           KvMetaProcessInfo infoData,
                                           flatbuffers::FlatBufferBuilder &builder,
                                           SerializedData *response)
{
    builder.Clear();
    flatbuffers::Offset<falcon::meta_fbs::MetaResponse> metaResponse;
    if (infoData->errorCode != SUCCESS && infoData->errorCode != FILE_EXISTS) {
        metaResponse = falcon::meta_fbs::CreateMetaResponse(builder, infoData->errorCode);
    } else {
        switch (metaService) {
        case FalconSupportMetaService::KV_PUT:
        case FalconSupportMetaService::KV_DEL: {
            // error code only response
            metaResponse = falcon::meta_fbs::CreateMetaResponse(builder, infoData->errorCode);
            break;
        }
        case FalconSupportMetaService::KV_GET: {
            auto valueKeyFB = builder.CreateVector(infoData->valuekey, infoData->slicenum);
            auto locationFB = builder.CreateVector(infoData->location, infoData->slicenum);
            auto slicelenFB = builder.CreateVector(infoData->slicelen, infoData->slicenum);
            auto getkvmetaReponse = falcon::meta_fbs::CreateGetKVMetaResponse(builder,
                                                                              infoData->valuelen,
                                                                              infoData->slicenum,
                                                                              valueKeyFB,
                                                                              locationFB,
                                                                              slicelenFB);
           metaResponse = falcon::meta_fbs::CreateMetaResponse(builder,
                                                               infoData->errorCode,
                                                               falcon::meta_fbs::AnyMetaResponse_GetKVMetaResponse,
                                                               getkvmetaReponse.Union());
            break;
        }
        default:
            return false;
        }
    }
    builder.Finish(metaResponse);

    char *buffer = SerializedDataApplyForSegment(response, builder.GetSize());
    memcpy(buffer, builder.GetBufferPointer(), builder.GetSize());
    return true;
}

bool SerializedKvMetaResponseEncodeWithPerProcessFlatBufferBuilder(FalconSupportMetaService metaService,
                                                                   KvMetaProcessInfo infoData,
                                                                   SerializedData *response)


{
    return SerializedkVMetaResponseEncode(metaService, infoData, FlatBufferBuilderPerProcess, response);
}

bool SerializedSliceParamDecode(FalconSupportMetaService metaService,
                                int count,
                                SerializedData *param,
                                SliceProcessInfoData *infoArray)
{
    sd_size_t p = 0;
    for (int i = 0; i < count; ++i) {
        uint8_t *buffer = (uint8_t *)param->buffer + p;
        sd_size_t size = SerializedDataNextSeveralItemSize(param, p, 1);
        if (size == (sd_size_t) - 1) {
            return false;
        }

        uint8_t *itemBuffer = (uint8_t *)buffer + SERIALIZED_DATA_ALIGNMENT;
        size_t itemSize = size - SERIALIZED_DATA_ALIGNMENT;
        flatbuffers::Verifier verifier(itemBuffer, itemSize);
        if (!verifier.VerifyBuffer<falcon::meta_fbs::MetaParam>(NULL)) {
            return false;
        }
        auto metaParam = falcon::meta_fbs::GetMetaParam(itemBuffer);

        SliceProcessInfo info = infoArray + i;
        switch (metaService) {
            case FalconSupportMetaService::SLICE_PUT: {
                if (metaParam->param_type() != falcon::meta_fbs::AnyMetaParam::AnyMetaParam_SliceInfoParam) {
                    return false;
                }
                auto sliceParam = metaParam->param_as_SliceInfoParam();
                info->name = sliceParam->filename()->c_str();
                info->count = sliceParam->slicenum();
                info->inodeIds = const_cast<uint64_t *>(sliceParam->inodeid()->data());
                info->chunkIds = const_cast<uint32_t *>(sliceParam->chunkid()->data());
                info->sliceIds = const_cast<uint64_t *>(sliceParam->sliceid()->data());
                info->sliceSizes = const_cast<uint32_t *>(sliceParam->slicesize()->data());
                info->sliceOffsets = const_cast<uint32_t *>(sliceParam->sliceoffset()->data());
                info->sliceLens = const_cast<uint32_t *>(sliceParam->slicelen()->data());
                info->sliceLoc1s = const_cast<uint32_t *>(sliceParam->sliceloc1()->data());
                info->sliceloc2s = const_cast<uint32_t *>(sliceParam->sliceloc2()->data());
                break;
            }
            case FalconSupportMetaService::SLICE_GET:
            case FalconSupportMetaService::SLICE_DEL: {
                if (metaParam->param_type() != falcon::meta_fbs::AnyMetaParam::AnyMetaParam_SliceIndexParam) {
                    return false;
                }
                auto sliceParam = metaParam->param_as_SliceIndexParam();
                info->name = sliceParam->filename()->c_str();
                info->inputInodeid = sliceParam->inodeid();
                info->inputChunkid = sliceParam->chunkid();
                break;
            }
            default:
                return false;
        }

        p += size;
    }

    return true;
}

static bool SerializedSliceResponseEncode(FalconSupportMetaService metaService,
                                          int count,
                                          SliceProcessInfoData *infoArray,
                                          flatbuffers::FlatBufferBuilder &builder,
                                          SerializedData *response)
{
    for (int i = 0; i < count; ++i) {
        builder.Clear();
        SliceProcessInfo info = infoArray + i;
        flatbuffers::Offset<falcon::meta_fbs::MetaResponse> metaResponse;

        switch (metaService) {
            case FalconSupportMetaService::SLICE_PUT:
            case FalconSupportMetaService::SLICE_DEL: {
                metaResponse = falcon::meta_fbs::CreateMetaResponse(builder, info->errorCode);
                break;
            }
            case FalconSupportMetaService::SLICE_GET: {
                auto inodeIds = builder.CreateVector(info->inodeIds, info->count);
                auto chunkIds = builder.CreateVector(info->chunkIds, info->count);
                auto sliceIds = builder.CreateVector(info->sliceIds, info->count);
                auto sliceSizes = builder.CreateVector(info->sliceSizes, info->count);
                auto sliceOffsets = builder.CreateVector(info->sliceOffsets, info->count);
                auto sliceLens = builder.CreateVector(info->sliceLens, info->count);
                auto sliceLoc1s = builder.CreateVector(info->sliceLoc1s, info->count);
                auto sliceloc2s = builder.CreateVector(info->sliceloc2s, info->count);

                auto sliceResponse = falcon::meta_fbs::CreateSliceInfoResponse(
                    builder, info->count, inodeIds, chunkIds, sliceIds, sliceSizes,
                    sliceOffsets, sliceLens, sliceLoc1s, sliceloc2s
                );

                metaResponse = falcon::meta_fbs::CreateMetaResponse(builder,
                                                                    info->errorCode,
                                                                    falcon::meta_fbs::AnyMetaResponse_SliceInfoResponse,
                                                                    sliceResponse.Union());
                break;
            }
            default:
                return false;
        }

        builder.Finish(metaResponse);

        char *buffer = SerializedDataApplyForSegment(response, builder.GetSize());
        memcpy(buffer, builder.GetBufferPointer(), builder.GetSize());
    }
    return true;
}

bool SerializedSliceResponseEncodeWithPerProcessFlatBufferBuilder(FalconSupportMetaService metaService,
                                                                  int count,
                                                                  SliceProcessInfoData *infoArray,
                                                                  SerializedData *response)
{
    return SerializedSliceResponseEncode(metaService, count, infoArray, FlatBufferBuilderPerProcess, response);
}

bool SerializedSliceIdParamDecode(SerializedData *param, SliceIdProcessInfo infoData)
{
    uint8_t *buffer = (uint8_t *)param->buffer;
    sd_size_t size = SerializedDataNextSeveralItemSize(param, 0, 1);
    if (size == (sd_size_t) - 1) {
        return false;
    }

    uint8_t *itemBuffer = (uint8_t *)buffer + SERIALIZED_DATA_ALIGNMENT;
    size_t itemSize = size - SERIALIZED_DATA_ALIGNMENT;
    flatbuffers::Verifier verifier(itemBuffer, itemSize);
    if (!verifier.VerifyBuffer<falcon::meta_fbs::MetaParam>(NULL)) {
        return false;
    }

    auto metaParam = falcon::meta_fbs::GetMetaParam(itemBuffer);
    if (metaParam->param_type() != falcon::meta_fbs::AnyMetaParam::AnyMetaParam_SliceIdParam) {
        return false;
    }

    auto sliceIdParam = metaParam->param_as_SliceIdParam();
    infoData->count = sliceIdParam->count();
    infoData->type = sliceIdParam->type();

    return true;
}

bool SerializedSliceIdResponseEncodeWithPerProcessFlatBufferBuilder(SliceIdProcessInfo infoData, SerializedData *response)
{
    auto &builder = FlatBufferBuilderPerProcess;
    builder.Clear();

    auto sliceIdRes = falcon::meta_fbs::CreateSliceIdResponse(builder, infoData->start, infoData->end);
    auto metaResponse = falcon::meta_fbs::CreateMetaResponse(builder,
                                                             infoData->errorCode,
                                                             falcon::meta_fbs::AnyMetaResponse_SliceIdResponse,
                                                             sliceIdRes.Union());

    builder.Finish(metaResponse);
    char *buffer = SerializedDataApplyForSegment(response, builder.GetSize());
    memcpy(buffer, builder.GetBufferPointer(), builder.GetSize());

    return true;
}