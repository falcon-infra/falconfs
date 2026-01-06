/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef FALCON_BATCH_SERVER_DEF_H
#define FALCON_BATCH_SERVER_DEF_H
#include "utils/falcon_meta_service_def.h"

// define service type that support process by batch
enum class FalconBatchServiceType { MKDIR = 0, CREATE, STAT, UNLINK, OPEN, CLOSE, NOT_SUPPORT, END };

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
    default:
        return FalconBatchServiceType::NOT_SUPPORT;
    }
}

#endif // FALCON_BATCH_SERVER_DEF_H
