/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef FALCON_CONNECTION_POOL_CONNECTION_POOL_H
#define FALCON_CONNECTION_POOL_CONNECTION_POOL_H

#include "connection_pool/connection_pool_config.h"
#include "utils/falcon_shmem_allocator.h"

__attribute__((visibility("default")))
void FalconDaemonConnectionPoolProcessMain(unsigned long int main_arg);
size_t FalconConnectionPoolShmemsize(void);
void FalconConnectionPoolShmemInit(void);
void RunConnectionPoolServer(void);

#endif
