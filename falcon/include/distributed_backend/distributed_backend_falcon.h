/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef FALCON_DISTRIBUTED_BACKEND_FALCON_H
#define FALCON_DISTRIBUTED_BACKEND_FALCON_H

#include "postgres.h"

#include "metadb/metadata.h"

void FalconCreateDistributedDataTable(void);
void FalconCreateDistributedDataTableByRangePoint(int);
void FalconDropDistributedDataTableByRangePoint(int);
void FalconCreateSliceTable(void);
void FalconCreateKvmetaTable(void);
/* v6.4 \u00a73.1: one falcon_kvblock_table per DN (not sharded by range_point). */
void FalconCreateKvblockTable(void);
void FalconPrepareCommands(void);

#endif
