/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */
#include "brpc_comm_adapter/brpc_meta_service_job.h"
#include "log/logging.h"

FalconMetaServiceType BrpcMetaServiceJob::MetaServiceTypeDecode(falcon::meta_proto::MetaServiceType type)
{
    switch (type) {
    case falcon::meta_proto::MetaServiceType::PLAIN_COMMAND:
        return FalconMetaServiceType::PLAIN_COMMAND;
    case falcon::meta_proto::MetaServiceType::MKDIR:
        return FalconMetaServiceType::MKDIR;
    case falcon::meta_proto::MetaServiceType::MKDIR_SUB_MKDIR:
        return FalconMetaServiceType::MKDIR_SUB_MKDIR;
    case falcon::meta_proto::MetaServiceType::MKDIR_SUB_CREATE:
        return FalconMetaServiceType::MKDIR_SUB_CREATE;
    case falcon::meta_proto::MetaServiceType::CREATE:
        return FalconMetaServiceType::CREATE;
    case falcon::meta_proto::MetaServiceType::STAT:
        return FalconMetaServiceType::STAT;
    case falcon::meta_proto::MetaServiceType::OPEN:
        return FalconMetaServiceType::OPEN;
    case falcon::meta_proto::MetaServiceType::CLOSE:
        return FalconMetaServiceType::CLOSE;
    case falcon::meta_proto::MetaServiceType::UNLINK:
        return FalconMetaServiceType::UNLINK;
    case falcon::meta_proto::MetaServiceType::READDIR:
        return FalconMetaServiceType::READDIR;
    case falcon::meta_proto::MetaServiceType::OPENDIR:
        return FalconMetaServiceType::OPENDIR;
    case falcon::meta_proto::MetaServiceType::RMDIR:
        return FalconMetaServiceType::RMDIR;
    case falcon::meta_proto::MetaServiceType::RMDIR_SUB_RMDIR:
        return FalconMetaServiceType::RMDIR_SUB_RMDIR;
    case falcon::meta_proto::MetaServiceType::RMDIR_SUB_UNLINK:
        return FalconMetaServiceType::RMDIR_SUB_UNLINK;
    case falcon::meta_proto::MetaServiceType::RENAME:
        return FalconMetaServiceType::RENAME;
    case falcon::meta_proto::MetaServiceType::RENAME_SUB_RENAME_LOCALLY:
        return FalconMetaServiceType::RENAME_SUB_RENAME_LOCALLY;
    case falcon::meta_proto::MetaServiceType::RENAME_SUB_CREATE:
        return FalconMetaServiceType::RENAME_SUB_CREATE;
    case falcon::meta_proto::MetaServiceType::UTIMENS:
        return FalconMetaServiceType::UTIMENS;
    case falcon::meta_proto::MetaServiceType::CHOWN:
        return FalconMetaServiceType::CHOWN;
    case falcon::meta_proto::MetaServiceType::CHMOD:
        return FalconMetaServiceType::CHMOD;
    default:
        return FalconMetaServiceType::NOT_SUPPORTED;
    }
}

FalconMetaServiceType BrpcMetaServiceJob::GetFalconMetaServiceType(int index)
{
    if (m_request->type_size() <= index) {
        FALCON_LOG_PRINTF(LOG_WARNING,
                          "try to get value from idx:%d which is out of type_size:%d",
                          index,
                          m_request->type_size());
        throw std::runtime_error("input index out of range.");
    }

    falcon::meta_proto::MetaServiceType type = m_request->type(index);

    FalconMetaServiceType supportType = MetaServiceTypeDecode(type);
    if (supportType == FalconMetaServiceType::NOT_SUPPORTED) {
        FALCON_LOG_PRINTF(LOG_WARNING, "Got unsupport serviceType:%d", type);
        throw std::runtime_error("got unsupport serviceType.");
    }

    return supportType;
}
