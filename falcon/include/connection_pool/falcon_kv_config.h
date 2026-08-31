/* Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 *
 * KV cache GUCs (v6 §14, §27). Owned by `falcon.so`, registered through
 * `DefineCustomIntVariable` in `falcon_init.c`, and read by the in-plugin
 * eviction worker in `libbrpcplugin.so` after `dlopen`. The globals are
 * exported with default visibility so the plugin can resolve them.
 */
#ifndef FALCON_CONNECTION_POOL_FALCON_KV_CONFIG_H
#define FALCON_CONNECTION_POOL_FALCON_KV_CONFIG_H

#ifdef __cplusplus
extern "C" {
#endif

/* Eviction thread cadence in milliseconds (clamped 50..60000 in plugin). */
#define FALCON_KV_EVICTION_PERIOD_MS_DEFAULT 200
extern int FalconKvEvictionPeriodMs;

/* Per-region free-blocks ratio (in percent, 0..100) below which the eviction
 * worker switches to aggressive mode (skip the period wait and immediately
 * run another cycle). */
#define FALCON_KV_EVICTION_LOW_WATERMARK_PCT_DEFAULT 10
extern int FalconKvEvictionLowWatermarkPct;

/* Maximum cold candidates handled per eviction cycle (clamped 1..1024). */
#define FALCON_KV_EVICTION_CHUNK_DEFAULT 64
extern int FalconKvEvictionChunk;

/* v6.5 P3: `host:port` of falcon_kv_store for DN eviction spill over BRPC.
 * Empty disables remote spill (default). */
extern char* FalconKvStoreSpillEndpoint;

/* v6.5 P3: membership watchdog cadence (ms) and heartbeat skew threshold (ms)
 * for flipping falcon_dn_node / falcon_store_node healthy=false. */
#define FALCON_KV_WATCHDOG_PERIOD_MS_DEFAULT 1000
#define FALCON_KV_WATCHDOG_SKEW_MS_DEFAULT 30000
extern int FalconKvWatchdogPeriodMs;
extern int FalconKvWatchdogSkewMs;

/* v6.5 P7: promote-on-read worker tuning knobs. */
#define FALCON_KV_PROMOTE_ENABLED_DEFAULT 1
#define FALCON_KV_PROMOTE_QUEUE_CAPACITY_DEFAULT 1024
#define FALCON_KV_PROMOTE_MAX_INFLIGHT_DEFAULT 64
extern int FalconKvPromoteEnabled;
extern int FalconKvPromoteQueueCapacity;
extern int FalconKvPromoteMaxInflight;

#ifdef __cplusplus
}
#endif

#endif  // FALCON_CONNECTION_POOL_FALCON_KV_CONFIG_H
