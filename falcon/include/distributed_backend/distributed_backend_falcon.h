/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef FALCON_DISTRIBUTED_BACKEND_FALCON_H
#define FALCON_DISTRIBUTED_BACKEND_FALCON_H

#include "postgres.h"

#include "metadb/metadata.h"

void FalconCreateDistributedDataTable(void);
void FalconCreateSliceTable(void);
void FalconCreateKvmetaTable(void);
void FalconPrepareCommands(void);

#endif
