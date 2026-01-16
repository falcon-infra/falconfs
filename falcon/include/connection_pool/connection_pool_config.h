/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef FALCON_CONNECTION_POOL_CONNECTION_POOL_CONFIG_H
#define FALCON_CONNECTION_POOL_CONNECTION_POOL_CONFIG_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>

extern int FalconPGPort;

#define FALCON_CONNECTION_POOL_PORT_DEFAULT 56999
extern int FalconConnectionPoolPort;

#define FALCON_NODE_LOCAL_IP_DEFAULT "127.0.0.1"
extern char *FalconNodeLocalIp;

#define FALCON_CONNECTION_POOL_SIZE_DEFAULT 32
extern int FalconConnectionPoolSize;

#define FALCON_CONNECTION_POOL_BATCH_SIZE_DEFAULT 512
extern int FalconConnectionPoolBatchSize;

#define FALCON_CONNECTION_POOL_WAIT_ADJUST_DEFAULT 1
extern int FalconConnectionPoolWaitAdjust;

#define FALCON_CONNECTION_POOL_WAIT_MIN_DEFAULT 1
extern int FalconConnectionPoolWaitMin;

#define FALCON_CONNECTION_POOL_WAIT_MAX_DEFAULT 512
extern int FalconConnectionPoolWaitMax;

#define FALCON_CONNECTION_POOL_SHMEM_SIZE_DEFAULT (256 * 1024 * 1024)
extern uint64_t FalconConnectionPoolShmemSize;

#define FALCON_CONNECTION_POOL_MAX_CONCURRENT_SOCKET 4096

#define FALCON_NODE_LOCAL_IP_DEFAULT "127.0.0.1"
extern char* FalconCommunicationServerIp;

extern char* FalconCommunicationPluginPath;

#ifdef __cplusplus
}
#endif

#endif
