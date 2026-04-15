/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef FALCON_BATCH_SERVER_DEF_H
#define FALCON_BATCH_SERVER_DEF_H
#include "utils/falcon_meta_service_def.h"

// define service type that support process by batch
enum class FalconBatchServiceType {
    MKDIR = 0,
    CREATE,
    STAT,
    UNLINK,
    OPEN,
    CLOSE,
    KV_PUT,
    KV_GET,
    KV_DEL,
    SLICE_PUT,
    SLICE_GET,
    SLICE_DEL,
    NOT_SUPPORT,
    END
};

inline FalconBatchServiceType FalconMetaServiceTypeToBatchServiceType(const FalconMetaServiceType type)
{
    switch (type) {
    case FalconMetaServiceType::MKDIR:
        return FalconBatchServiceType::MKDIR;
    case FalconMetaServiceType::CREATE:
        return FalconBatchServiceType::CREATE;
    case FalconMetaServiceType::STAT:
        return FalconBatchServiceType::STAT;
    case FalconMetaServiceType::UNLINK:
        return FalconBatchServiceType::UNLINK;
    case FalconMetaServiceType::OPEN:
        return FalconBatchServiceType::OPEN;
    case FalconMetaServiceType::CLOSE:
        return FalconBatchServiceType::CLOSE;
    case FalconMetaServiceType::KV_PUT:
        return FalconBatchServiceType::KV_PUT;
    case FalconMetaServiceType::KV_GET:
        return FalconBatchServiceType::KV_GET;
    case FalconMetaServiceType::KV_DEL:
        return FalconBatchServiceType::KV_DEL;
    case FalconMetaServiceType::SLICE_PUT:
        return FalconBatchServiceType::SLICE_PUT;
    case FalconMetaServiceType::SLICE_GET:
        return FalconBatchServiceType::SLICE_GET;
    case FalconMetaServiceType::SLICE_DEL:
        return FalconBatchServiceType::SLICE_DEL;
    default:
        return FalconBatchServiceType::NOT_SUPPORT;
    }
}

#endif // FALCON_BATCH_SERVER_DEF_H
