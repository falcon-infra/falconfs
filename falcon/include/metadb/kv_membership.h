/* Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 *
 * v6.5 §3.4 KV cache membership catalog accessor.
 *
 * Two CN-resident tables drive client-side discovery of the KV cache topology:
 *
 *   pg_catalog.falcon_dn_node     - one row per DN replication group; the
 *                                   denormalized counterpart of
 *                                   pg_catalog.falcon_foreign_server (server_id
 *                                   FK) carrying KV-only fields the file-meta
 *                                   path does not have (kv_brpc_port,
 *                                   host_node_name, dn_epoch).
 *
 *   pg_catalog.falcon_store_node  - one row per running falcon_kv_store daemon.
 *                                   Stores have no FalconFS analogue, so this
 *                                   table is independent of falcon_foreign_server.
 *
 * The C-language SQL functions declared here (one per RPC verb) are called by:
 *   - DN bgworker on startup / heartbeat / shutdown,
 *   - falcon_kv_store daemon on startup / heartbeat / shutdown,
 *   - cluster manager on DN primary failover.
 *
 * Each mutation function emits a `pg_notify('falcon_kv_dn_membership',...)` or
 * `pg_notify('falcon_kv_store_membership',...)` so Client-side membership
 * refresh loops (design §3.4.7.2) see the change without polling lag.
 */
#ifndef FALCON_KV_MEMBERSHIP_H
#define FALCON_KV_MEMBERSHIP_H

#include "postgres.h"
#include "fmgr.h"
#include "lib/stringinfo.h"

extern const char *FalconDnNodeTableName;
extern const char *FalconStoreNodeTableName;

/* DDL builder. Idempotent: callers should check `CheckIfRelationExists` first
 * before invoking this (mirrors FalconCreateKvblockTable). */
void ConstructCreateKvMembershipTablesCommand(StringInfo command);

/* Single SPI-level entry point that creates both tables (+grants +index) if
 * they do not already exist. Idempotent. */
void FalconCreateKvMembershipTables(void);

/* SQL function declarations. Bodies live in falcon/metadb/kv_membership.c.
 * Each registers via PG_FUNCTION_INFO_V1 so the SQL declaration in
 * falcon--1.0.sql can call them as `LANGUAGE C STRICT`. */
extern Datum falcon_create_kv_membership_tables(PG_FUNCTION_ARGS);

extern Datum falcon_dn_node_register(PG_FUNCTION_ARGS);
extern Datum falcon_dn_node_heartbeat(PG_FUNCTION_ARGS);
extern Datum falcon_dn_node_unregister(PG_FUNCTION_ARGS);
extern Datum falcon_dn_node_update_endpoint(PG_FUNCTION_ARGS);

extern Datum falcon_store_node_register(PG_FUNCTION_ARGS);
extern Datum falcon_store_node_heartbeat(PG_FUNCTION_ARGS);
extern Datum falcon_store_node_unregister(PG_FUNCTION_ARGS);

#endif /* FALCON_KV_MEMBERSHIP_H */
