/* Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 *
 * Function-pointer bridge that lets the connection-pool worker thread (in
 * falcon.so) invoke the KV metadata engine that lives inside libbrpcplugin.so.
 *
 * libbrpcplugin.so is dlopen'd into the bgworker process AFTER falcon.so is
 * loaded (the bgworker calls `dlopen` from `falcon_connection_pool.c`). At
 * plugin startup the plugin registers a single C entry point that processes a
 * KV cache job and uses the worker-supplied PGconn for the catalog round-trip.
 *
 * v6.4 \u00a74.1.1 mapping:
 *
 *   BRPC worker thread (libbrpcplugin.so) --
 *     builds BrpcKVCacheServiceJob -->
 *   FalconDispatchMetaJob2PGConnectionPool (falcon.so) -->
 *   PGConnectionPool::EnqueueKVCacheJob -> kvTaskList (falcon.so) -->
 *   PGConnection worker thread (one of N, each with its own libpq conn) -->
 *   KVCacheWorkerTask::DoWork (falcon.so) calls FalconKVProcessJob -->
 *   plugin-side impl runs engine->Batch*SplitForPoolWorker(req, resp,
 *     run_catalog_sub_batch=PQexecParams against the worker's PGconn).
 *
 * Multiple PGConnection workers run concurrently, each issuing its own libpq
 * round-trip to its own PG backend, so catalog access fans out to many
 * backends instead of serializing through one connection.
 */
#ifndef FALCON_KV_RUNTIME_BRIDGE_H
#define FALCON_KV_RUNTIME_BRIDGE_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* The callback signature the plugin registers. Returns 0 on success. The
 * implementation must allocate `*out_resp` with `malloc`; the caller frees it.
 *
 * `pg_conn_opaque` is a `PGconn *` (the worker's libpq connection). The plugin
 * uses it for one PQexecParams round-trip per sub-batch. The pointer is valid
 * only for the duration of the call.
 *
 * `method` is a `FalconKVServiceMethod` value. */
typedef int (*FalconKVProcessJobFn)(int method,
                                    const char *req_buf,
                                    int req_size,
                                    char **out_resp,
                                    int *out_resp_size,
                                    void *pg_conn_opaque);

/* Set / get the active process-job callback. Both functions are exported with
 * default visibility from falcon.so so libbrpcplugin.so can resolve them at
 * runtime via the dynamic linker. */
__attribute__((visibility("default")))
void FalconKVSetProcessJob(FalconKVProcessJobFn fn);

__attribute__((visibility("default")))
FalconKVProcessJobFn FalconKVGetProcessJob(void);

#ifdef __cplusplus
}
#endif

#endif  /* FALCON_KV_RUNTIME_BRIDGE_H */
