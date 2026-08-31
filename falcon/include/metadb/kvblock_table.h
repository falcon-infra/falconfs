/* Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 *
 * v6.4 \u00a73.1 / \u00a74.2 catalog accessor for `falcon_kvblock_table`. This module
 * uses PostgreSQL internal APIs (table_open, systable_beginscan, heap_modify_tuple,
 * CatalogTupleUpdateWithInfo, simple_heap_delete) and never raw SQL on the hot
 * path. Each batch entry point processes the entire sub-batch inside one
 * BeginInternalSubTransaction so the libpq round-trip count from the plugin is
 * exactly one per sub-batch (v6.4 \u00a74.1.1, mirroring `BatchWorkerTask::DoWork`).
 */
#ifndef FALCON_KVBLOCK_TABLE_H
#define FALCON_KVBLOCK_TABLE_H

#include "postgres.h"
#include "lib/stringinfo.h"

#include "connection_pool/kv_catalog_wire.h"

/* Column ordering (1-based). Must match the CREATE TABLE statement built by
 * `ConstructCreateKvblockTableCommand`. */
#define Natts_falcon_kvblock_table              9
#define Anum_falcon_kvblock_table_block_hash    1
#define Anum_falcon_kvblock_table_kv_group_idx  2
#define Anum_falcon_kvblock_table_layer_mask    3
#define Anum_falcon_kvblock_table_status        4
#define Anum_falcon_kvblock_table_store_node_id 5
#define Anum_falcon_kvblock_table_pool_offset   6
#define Anum_falcon_kvblock_table_evicted_path  7
#define Anum_falcon_kvblock_table_version       8
#define Anum_falcon_kvblock_table_updated_at_ms 9

extern const char *KvblockTableName;
extern const char *KvblockTableIndexName;

/* DDL helper used by `falcon_create_kvblock_table()`. The table is created in
 * the falcon namespace and then moved to pg_catalog so it is reachable as an
 * unqualified relation name from SPI / libpq, matching how kvmeta_table is
 * handled in this codebase. */
void ConstructCreateKvblockTableCommand(StringInfo command, const char *name);

/* Resolves OIDs for the table and its primary-key index. Both are looked up
 * lazily and cached for the lifetime of the backend. */
Oid KvblockRelationId(void);
Oid KvblockRelationIndexId(void);

/* Batch entry points used by `falcon_kv_metadata_catalog_call`. Each takes a
 * read-only request buffer in the v6.4 wire format defined by kv_catalog_wire.h
 * and a pre-allocated response buffer sized to `KVCatalogResponseSize(method, count)`.
 *
 * They never throw out of the handler: per v6.4 \u00a74.1.2, per-item errors must be
 * surfaced as per-item result fields so the rest of the chunk continues. The
 * outer SQL function wraps the call in BeginInternalSubTransaction /
 * ReleaseCurrentSubTransaction. */
void FalconKVBlockBatchLookup(const char *req_buf, uint64_t req_size,
                              char *resp_buf, uint64_t resp_size);
void FalconKVBlockBatchInsertAllocated(const char *req_buf, uint64_t req_size,
                                       char *resp_buf, uint64_t resp_size);
void FalconKVBlockBatchCASStatusUpdate(const char *req_buf, uint64_t req_size,
                                       char *resp_buf, uint64_t resp_size);
void FalconKVBlockBatchDelete(const char *req_buf, uint64_t req_size,
                              char *resp_buf, uint64_t resp_size);
void FalconKVBlockBatchPromoteFromEvicted(const char *req_buf, uint64_t req_size,
                                          char *resp_buf, uint64_t resp_size);

/* v6 \u00a715.1 recovery scan: streams every row in `falcon_kvblock_table` into
 * `out_rows` (caller-allocated, capacity `out_rows_capacity`). Returns the
 * number of rows actually written. The returned set is not paged: callers
 * should size capacity to a worst-case based on engine block count.
 * `KVCatalogRecoveryRow` is defined in connection_pool/kv_catalog_wire.h. */
uint32_t FalconKVBlockScanForRecovery(KVCatalogRecoveryRow *out_rows,
                                      uint32_t out_rows_capacity);

/* v6 §15.2 Store restart reconciliation: Store DRAM is volatile, so rows that
 * point only to the restarted Store's DRAM are deleted. Existing EVICTED rows
 * with an SSD path are preserved because they can still answer SSD reads. */
void FalconKVBlockReconcileStoreRestart(int32_t store_node_id,
                                        int64_t now_ms,
                                        KVCatalogStoreRestartResponse *out);
uint32_t FalconKVBlockScanStoreEvicted(int32_t store_node_id,
                                       KVCatalogStoreEvictedRow *out_rows,
                                       uint32_t max_rows);
void FalconKVBlockDeleteInvalidEvicted(const char *req_buf, uint64_t req_size,
                                       char *resp_buf, uint64_t resp_size);

/* v6 \u00a715.4 dn_epoch fencing. The `falcon_kvblock_dn_epoch` table holds one
 * (shard_id, dn_epoch) row; absent rows are treated as `dn_epoch = 1` and
 * inserted lazily on first bump. */
int64_t FalconKVBlockLoadDnEpoch(int32_t shard_id);
int64_t FalconKVBlockBumpDnEpoch(int32_t shard_id);

#endif  /* FALCON_KVBLOCK_TABLE_H */
