/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef FALCON_CONNECTION_POOL_PG_CONNECTION_POOL_H
#define FALCON_CONNECTION_POOL_PG_CONNECTION_POOL_H

#ifdef __cplusplus
extern "C" {
#endif

bool StartPGConnectionPool();
void DestroyPGConnectionPool();
// communication server callback function used to dispatch request to PGConnectionPool
void FalconDispatchMetaJob2PGConnectionPool(void *job);
#ifdef __cplusplus
}
#endif

#endif
