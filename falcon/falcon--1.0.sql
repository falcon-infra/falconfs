/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

/* contrib/falcon/falcon--1.0.sql */

-- complain if script is soured in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION falcon" to load this file. \quit

CREATE SCHEMA falcon;

----------------------------------------------------------------
-- falcon_control
----------------------------------------------------------------
CREATE FUNCTION pg_catalog.falcon_start_background_service()
    RETURNS INTEGER
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$falcon_start_background_service$$;
COMMENT ON FUNCTION pg_catalog.falcon_start_background_service()
    IS 'falcon start background service';

----------------------------------------------------------------
-- falcon_distributed_transaction
----------------------------------------------------------------
CREATE TABLE falcon.falcon_distributed_transaction(
    nodeid  int NOT NULL,
    gid     text NOT NULL
);
CREATE INDEX falcon_distributed_transaction_index
    ON falcon.falcon_distributed_transaction USING btree(nodeid);
ALTER TABLE falcon.falcon_distributed_transaction SET SCHEMA pg_catalog;
ALTER TABLE pg_catalog.falcon_distributed_transaction
    ADD CONSTRAINT falcon_distributed_transaction_unique_constraint UNIQUE (nodeid, gid);
GRANT SELECT ON pg_catalog.falcon_distributed_transaction TO public;

----------------------------------------------------------------
-- falcon_foreign_server
----------------------------------------------------------------
CREATE TABLE falcon.falcon_foreign_server(
    server_id   int NOT NULL,
    server_name text NOT NULL,
    host        text NOT NULL,
    port        int NOT NULL,
    is_local    bool NOT NULL,
    user_name   text NOT NULL
);
CREATE UNIQUE INDEX falcon_foreign_server_index
    ON falcon.falcon_foreign_server using btree(server_id);
ALTER TABLE falcon.falcon_foreign_server SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.falcon_foreign_server TO public;

CREATE FUNCTION pg_catalog.falcon_insert_foreign_server(server_id int, server_name cstring, host cstring,
                                                 port int, is_local bool, user_name cstring)
    RETURNS INTEGER
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$falcon_insert_foreign_server$$;
COMMENT ON FUNCTION pg_catalog.falcon_insert_foreign_server(server_id int, server_name cstring, host cstring,
                                                     port int, is_local bool, user_name cstring)
    IS 'falcon insert foreign server';

CREATE FUNCTION pg_catalog.falcon_delete_foreign_server(server_id int)
    RETURNS INTEGER
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$falcon_delete_foreign_server$$;
COMMENT ON FUNCTION pg_catalog.falcon_delete_foreign_server(server_id int)
    IS 'falcon delete foreign server';

CREATE FUNCTION pg_catalog.falcon_update_foreign_server(server_id int, host cstring, port int)
    RETURNS INTEGER
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$falcon_update_foreign_server$$;
COMMENT ON FUNCTION pg_catalog.falcon_update_foreign_server(server_id int, host cstring, port int)
    IS 'falcon update foreign server';

CREATE FUNCTION pg_catalog.falcon_reload_foreign_server_cache()
    RETURNS INTEGER
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$falcon_reload_foreign_server_cache$$;
COMMENT ON FUNCTION pg_catalog.falcon_reload_foreign_server_cache()
    IS 'falcon reload foreign server cache';

CREATE FUNCTION pg_catalog.falcon_foreign_server_test(mode cstring)
    RETURNS INTEGER
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$falcon_foreign_server_test$$;
COMMENT ON FUNCTION pg_catalog.falcon_foreign_server_test(mode cstring)
    IS 'falcon foreign server test';

----------------------------------------------------------------
-- falcon_shard_table
----------------------------------------------------------------
CREATE TABLE falcon.falcon_shard_table(
    range_point int NOT NULL,
    server_id   int NOT NULL
);
CREATE UNIQUE INDEX falcon_shard_table_index ON falcon.falcon_shard_table using btree(range_point);
ALTER TABLE falcon.falcon_shard_table SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.falcon_shard_table TO public;

CREATE FUNCTION pg_catalog.falcon_build_shard_table(shard_count int)
    RETURNS INTEGER
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$falcon_build_shard_table$$;
COMMENT ON FUNCTION pg_catalog.falcon_build_shard_table(shard_count int)
    IS 'falcon build shard table';

CREATE FUNCTION pg_catalog.falcon_update_shard_table(range_point bigint[], server_id int[], lockInternal bool default true)
    RETURNS INTEGER
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$falcon_update_shard_table$$;
COMMENT ON FUNCTION pg_catalog.falcon_update_shard_table(range_point bigint[], server_id int[], lockInternal bool)
    IS 'falcon update shard table';

CREATE FUNCTION pg_catalog.falcon_renew_shard_table()
    RETURNS TABLE(range_min int, range_max int, host text, port int, server_id int)
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$falcon_renew_shard_table$$;
COMMENT ON FUNCTION pg_catalog.falcon_renew_shard_table()
    IS 'falcon renew shard table';

CREATE FUNCTION pg_catalog.falcon_reload_shard_table_cache()
    RETURNS INTEGER
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$falcon_reload_shard_table_cache$$;
COMMENT ON FUNCTION pg_catalog.falcon_reload_shard_table_cache()
    IS 'falcon reload shard table cache';

----------------------------------------------------------------
-- falcon_distributed_backend
----------------------------------------------------------------
CREATE FUNCTION pg_catalog.falcon_create_distributed_data_table()
    RETURNS INTEGER
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$falcon_create_distributed_data_table$$;
COMMENT ON FUNCTION pg_catalog.falcon_create_distributed_data_table()
    IS 'falcon create distributed data table';

CREATE FUNCTION pg_catalog.falcon_create_distributed_data_table_by_range_point(range_point int)
    RETURNS INTEGER
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$falcon_create_distributed_data_table_by_range_point$$;
COMMENT ON FUNCTION pg_catalog.falcon_create_distributed_data_table_by_range_point(int)
    IS 'falcon create distributed data table by range point';

CREATE FUNCTION pg_catalog.falcon_drop_distributed_data_table_by_range_point(range_point int)
    RETURNS INTEGER
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$falcon_drop_distributed_data_table_by_range_point$$;
COMMENT ON FUNCTION pg_catalog.falcon_drop_distributed_data_table_by_range_point(int)
    IS 'falcon drop distributed data table by range point';

CREATE FUNCTION pg_catalog.falcon_prepare_commands()
    RETURNS INTEGER
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$falcon_prepare_commands$$;
COMMENT ON FUNCTION pg_catalog.falcon_prepare_commands()
    IS 'falcon prepare commands';

----------------------------------------------------------------
-- falcon_dir_path_hash
----------------------------------------------------------------
CREATE FUNCTION pg_catalog.falcon_print_dir_path_hash_elem()
    RETURNS TABLE(fileName text, parentId bigint, inodeId bigint, isAcquired text)
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$falcon_print_dir_path_hash_elem$$;
COMMENT ON FUNCTION pg_catalog.falcon_print_dir_path_hash_elem()
    IS 'falcon print dir path hash elem';

CREATE FUNCTION pg_catalog.falcon_acquire_hash_lock(IN path cstring, IN parentId bigint, IN lockmode bigint)
    RETURNS INTEGER
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$falcon_acquire_hash_lock$$;
COMMENT ON FUNCTION pg_catalog.falcon_acquire_hash_lock(IN path cstring, IN parentId bigint, IN lockmode bigint)
    IS 'falcon acquire hash lock';

CREATE FUNCTION pg_catalog.falcon_release_hash_lock(IN path cstring, IN parentId bigint)
    RETURNS INTEGER
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$falcon_release_hash_lock$$;
COMMENT ON FUNCTION pg_catalog.falcon_release_hash_lock(IN path cstring, IN parentId bigint)
    IS 'falcon release hash lock';

----------------------------------------------------------------
-- falcon_transaction_cleanup
----------------------------------------------------------------
CREATE FUNCTION pg_catalog.falcon_transaction_cleanup_trigger()
    RETURNS INTEGER
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$falcon_transaction_cleanup_trigger$$;
COMMENT ON FUNCTION pg_catalog.falcon_transaction_cleanup_trigger()
    IS 'falcon transaction cleanup trigger';

CREATE FUNCTION pg_catalog.falcon_transaction_cleanup_test()
    RETURNS INTEGER
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$falcon_transaction_cleanup_test$$;
COMMENT ON FUNCTION pg_catalog.falcon_transaction_cleanup_test()
    IS 'falcon transaction cleanup test';

----------------------------------------------------------------
-- falcon_directory_table
----------------------------------------------------------------]
CREATE TABLE falcon.falcon_directory_table(
    parent_id bigint,
    name text,
    inodeid bigint
);
CREATE UNIQUE INDEX falcon_directory_table_index ON falcon.falcon_directory_table using btree(parent_id, name);
ALTER TABLE falcon.falcon_directory_table SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.falcon_directory_table TO public;

----------------------------------------------------------------
-- falcon_control
----------------------------------------------------------------
CREATE FUNCTION pg_catalog.falcon_clear_cached_relation_oid_func()
    RETURNS INTEGER
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$falcon_clear_cached_relation_oid_func$$;
COMMENT ON FUNCTION pg_catalog.falcon_clear_cached_relation_oid_func()
    IS 'falcon clear cached relation oid func';

CREATE FUNCTION pg_catalog.falcon_clear_user_data_func()
    RETURNS INTEGER
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$falcon_clear_user_data_func$$;
COMMENT ON FUNCTION pg_catalog.falcon_clear_user_data_func()
    IS 'falcon clear user data';

CREATE FUNCTION pg_catalog.falcon_clear_all_data_func()
    RETURNS INTEGER
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$falcon_clear_all_data_func$$;
COMMENT ON FUNCTION pg_catalog.falcon_clear_all_data_func()
    IS 'falcon clear all data';

CREATE FUNCTION pg_catalog.falcon_run_pooler_server_func()
    RETURNS INTEGER
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$falcon_run_pooler_server_func$$;
COMMENT ON FUNCTION pg_catalog.falcon_run_pooler_server_func()
    IS 'falcon run pooler server';

CREATE SEQUENCE falcon.pg_dfs_inodeid_seq
    MINVALUE 1
    INCREMENT BY 32
    MAXVALUE 9223372036854775807;
ALTER SEQUENCE falcon.pg_dfs_inodeid_seq SET SCHEMA pg_catalog;
CREATE TABLE falcon.dfs_directory_path(
    name text,
    inodeid bigint NOT NULL DEFAULT nextval('pg_dfs_inodeid_seq'),
    parentid bigint,
    subpartnum int
);
CREATE UNIQUE INDEX dfs_directory_path_index 
ON falcon.dfs_directory_path using btree(parentid, name);
ALTER TABLE falcon.dfs_directory_path SET SCHEMA pg_catalog;
-- end add--

----------------------------------------------------------------
-- falcon_plain_interface
----------------------------------------------------------------
CREATE FUNCTION pg_catalog.falcon_plain_mkdir(path cstring)
    RETURNS int
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$falcon_plain_mkdir$$;
COMMENT ON FUNCTION pg_catalog.falcon_plain_mkdir(path cstring) IS 'falcon plain mkdir';

CREATE FUNCTION pg_catalog.falcon_plain_create(path cstring)
    RETURNS int
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$falcon_plain_create$$;
COMMENT ON FUNCTION pg_catalog.falcon_plain_create(path cstring) IS 'falcon plain create';

CREATE FUNCTION pg_catalog.falcon_plain_stat(path cstring)
    RETURNS int
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$falcon_plain_stat$$;
COMMENT ON FUNCTION pg_catalog.falcon_plain_stat(path cstring) IS 'falcon plain stat';

CREATE FUNCTION pg_catalog.falcon_plain_rmdir(path cstring)
    RETURNS int
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$falcon_plain_rmdir$$;
COMMENT ON FUNCTION pg_catalog.falcon_plain_rmdir(path cstring) IS 'falcon plain rmdir';

CREATE FUNCTION pg_catalog.falcon_plain_readdir(path cstring)
    RETURNS text
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$falcon_plain_readdir$$;
COMMENT ON FUNCTION pg_catalog.falcon_plain_readdir(path cstring) IS 'falcon plain readdir';


----------------------------------------------------------------
-- falcon_serialize_interface
----------------------------------------------------------------
CREATE FUNCTION pg_catalog.falcon_meta_call_by_serialized_shmem_internal(type int, count int, shmem_shift bigint, signature bigint, stat_indices_shift bigint)
    RETURNS bigint
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$falcon_meta_call_by_serialized_shmem_internal$$;
COMMENT ON FUNCTION pg_catalog.falcon_meta_call_by_serialized_shmem_internal(type int, count int, shmem_shift bigint, signature bigint, stat_indices_shift bigint) IS 'falcon meta func by serialized shmem internal';

CREATE FUNCTION pg_catalog.falcon_meta_call_by_serialized_data(type int, count int, param bytea)
    RETURNS bytea
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$falcon_meta_call_by_serialized_data$$;
COMMENT ON FUNCTION pg_catalog.falcon_meta_call_by_serialized_data(type int, count int, param bytea) IS 'falcon meta call by serialized data';


----------------------------------------------------------------
-- v6.4 KV cache catalog: one libpq round-trip per sub-batch (\u00a74.1.1).
-- Method enum + POD-array payload defined in connection_pool/kv_catalog_wire.h.
----------------------------------------------------------------
CREATE FUNCTION pg_catalog.falcon_kv_metadata_catalog_call(method int, payload bytea)
    RETURNS bytea
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$falcon_kv_metadata_catalog_call$$;
COMMENT ON FUNCTION pg_catalog.falcon_kv_metadata_catalog_call(method int, payload bytea)
    IS 'v6.4 KV cache catalog batch op: one round-trip per sub-batch';

-----------------------------------------------------------------
-- falcon_kv_metadata_recovery_call (v6 §15.1 / §15.4):
--   - SCAN_FOR_RECOVERY scans falcon_kvblock_table and returns all rows.
--   - LOAD_DN_EPOCH / BUMP_DN_EPOCH manage falcon_kvblock_dn_epoch.
-- Method enum + payload format defined in connection_pool/kv_catalog_wire.h.
-----------------------------------------------------------------
CREATE FUNCTION pg_catalog.falcon_kv_metadata_recovery_call(method int, payload bytea)
    RETURNS bytea
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$falcon_kv_metadata_recovery_call$$;
COMMENT ON FUNCTION pg_catalog.falcon_kv_metadata_recovery_call(method int, payload bytea)
    IS 'v6 §15.1 KV cache DN-restart recovery: scan rows + dn_epoch fencing';


----------------------------------------------------------------
-- falcon_create_kvblock_table (v6.4 \u00a73.1: one row table per DN).
----------------------------------------------------------------
CREATE FUNCTION pg_catalog.falcon_create_kvblock_table()
    RETURNS INTEGER
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$falcon_create_kvblock_table$$;
COMMENT ON FUNCTION pg_catalog.falcon_create_kvblock_table()
    IS 'v6.4 KV cache catalog: create per-DN falcon_kvblock_table';


----------------------------------------------------------------
-- v6.5 KV cache membership (CN): falcon_dn_node + falcon_store_node + pg_notify
----------------------------------------------------------------
CREATE FUNCTION pg_catalog.falcon_create_kv_membership_tables()
    RETURNS INTEGER
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$falcon_create_kv_membership_tables$$;
COMMENT ON FUNCTION pg_catalog.falcon_create_kv_membership_tables()
    IS 'v6.5: create pg_catalog.falcon_dn_node and falcon_store_node if missing';

CREATE FUNCTION pg_catalog.falcon_dn_node_register(server_id int, host_node_name cstring, pg_host cstring,
    pg_port int, kv_brpc_port int, dn_epoch bigint)
    RETURNS INTEGER
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$falcon_dn_node_register$$;

CREATE FUNCTION pg_catalog.falcon_dn_node_heartbeat(server_id int, now_ms bigint)
    RETURNS INTEGER
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$falcon_dn_node_heartbeat$$;

CREATE FUNCTION pg_catalog.falcon_dn_node_unregister(server_id int)
    RETURNS INTEGER
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$falcon_dn_node_unregister$$;

CREATE FUNCTION pg_catalog.falcon_dn_node_update_endpoint(server_id int, host_node_name cstring, pg_host cstring,
    pg_port int, kv_brpc_port int)
    RETURNS INTEGER
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$falcon_dn_node_update_endpoint$$;

CREATE FUNCTION pg_catalog.falcon_store_node_register(store_node_id int, host_node_name cstring, host cstring,
    brpc_port int, runtime_dir cstring, shm_name cstring, dram_pool_bytes bigint, block_size int, store_epoch bigint)
    RETURNS INTEGER
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$falcon_store_node_register$$;

CREATE FUNCTION pg_catalog.falcon_store_node_heartbeat(store_node_id int, store_epoch bigint, now_ms bigint)
    RETURNS INTEGER
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$falcon_store_node_heartbeat$$;

CREATE FUNCTION pg_catalog.falcon_store_node_unregister(store_node_id int)
    RETURNS INTEGER
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$falcon_store_node_unregister$$;

CREATE FUNCTION pg_catalog.falcon_kv_membership_watchdog_tick(now_ms bigint, skew_ms bigint)
    RETURNS BIGINT
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$falcon_kv_membership_watchdog_tick$$;
COMMENT ON FUNCTION pg_catalog.falcon_kv_membership_watchdog_tick(bigint, bigint)
    IS 'v6.5 P3: mark falcon_store_node / falcon_dn_node unhealthy when heartbeat skew exceeds threshold';


----------------------------------------------------------------
-- falcon_create_slice_table
----------------------------------------------------------------
CREATE FUNCTION pg_catalog.falcon_create_slice_table()
    RETURNS INTEGER
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$falcon_create_slice_table$$;
COMMENT ON FUNCTION pg_catalog.falcon_create_slice_table()
    IS 'falcon build slice table';


----------------------------------------------------------------
-- falcon_create_kvmeta_table
----------------------------------------------------------------
CREATE FUNCTION pg_catalog.falcon_create_kvmeta_table()
    RETURNS INTEGER
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$falcon_create_kvmeta_table$$;
COMMENT ON FUNCTION pg_catalog.falcon_create_kvmeta_table()
    IS 'falcon build kvmeta shard table';


----------------------------------------------------------------
-- falcon_kvsliceid_table
----------------------------------------------------------------]
CREATE TABLE falcon.falcon_kvsliceid_table(
    keystr text,
    slice_id bigint
);
CREATE UNIQUE INDEX falcon_kvsliceid_table_index ON falcon.falcon_kvsliceid_table using btree(keystr);
ALTER TABLE falcon.falcon_kvsliceid_table SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.falcon_kvsliceid_table TO public;


----------------------------------------------------------------
-- falcon_filesliceid_table
----------------------------------------------------------------]
CREATE TABLE falcon.falcon_filesliceid_table(
    keystr text,
    slice_id bigint
);
CREATE UNIQUE INDEX falcon_filesliceid_table_index ON falcon.falcon_filesliceid_table using btree(keystr);
ALTER TABLE falcon.falcon_filesliceid_table SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.falcon_filesliceid_table TO public;

