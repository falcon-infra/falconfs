/* Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 *
 * Bridge implementation. Single shared function pointer with relaxed access:
 * registration happens once at plugin startup before any worker thread reads
 * it, and replacement is not supported.
 */

#include "connection_pool/falcon_kv_runtime_bridge.h"

#include <stddef.h>

static FalconKVProcessJobFn g_kv_process_job = NULL;

void FalconKVSetProcessJob(FalconKVProcessJobFn fn)
{
    g_kv_process_job = fn;
}

FalconKVProcessJobFn FalconKVGetProcessJob(void)
{
    return g_kv_process_job;
}
