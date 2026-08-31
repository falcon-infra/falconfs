/* Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 *
 * v6.5 §3.4.7 KV cache membership catalog (CN-resident).
 */

#include "metadb/kv_membership.h"

#include "executor/spi.h"
#include "utils/builtins.h"

#include "utils/error_log.h"
#include "utils/utils.h"

#include <string.h>
#include <sys/time.h>
#include <time.h>

const char *FalconDnNodeTableName   = "falcon_dn_node";
const char *FalconStoreNodeTableName = "falcon_store_node";

static bool SpiOkUpsert(int rc)
{
    return rc == SPI_OK_INSERT || rc == SPI_OK_UPDATE;
}

static int64 CurrentEpochMillis(void)
{
    struct timeval tv;

    if (gettimeofday(&tv, NULL) != 0)
        return (int64) (time(NULL) * 1000LL);
    return (int64) tv.tv_sec * 1000LL + (int64) (tv.tv_usec / 1000);
}

static void AppendCreateDnNodeTableDdl(StringInfo cmd)
{
    appendStringInfo(cmd,
                     "CREATE TABLE falcon.%s("
                     "server_id         INT     NOT NULL PRIMARY KEY "
                     "REFERENCES pg_catalog.falcon_foreign_server(server_id),"
                     "host_node_name    TEXT    NOT NULL,"
                     "pg_host           TEXT    NOT NULL,"
                     "pg_port           INT     NOT NULL,"
                     "kv_brpc_port      INT     NOT NULL,"
                     "dn_epoch          BIGINT  NOT NULL DEFAULT 0,"
                     "healthy           BOOLEAN NOT NULL DEFAULT true,"
                     "last_heartbeat_ms BIGINT  NOT NULL DEFAULT 0,"
                     "registered_at_ms  BIGINT  NOT NULL DEFAULT 0);"
                     "ALTER TABLE falcon.%s SET SCHEMA pg_catalog;"
                     "GRANT SELECT ON pg_catalog.%s TO public;"
                     "ALTER EXTENSION falcon ADD TABLE %s;",
                     FalconDnNodeTableName,
                     FalconDnNodeTableName,
                     FalconDnNodeTableName,
                     FalconDnNodeTableName);
}

static void AppendCreateStoreNodeTableDdl(StringInfo cmd)
{
    appendStringInfo(cmd,
                     "CREATE TABLE falcon.%s("
                     "store_node_id       INT     NOT NULL PRIMARY KEY,"
                     "host_node_name      TEXT    NOT NULL,"
                     "host                TEXT    NOT NULL,"
                     "brpc_port           INT     NOT NULL,"
                     "runtime_dir         TEXT    NOT NULL,"
                     "shm_name            TEXT    NOT NULL,"
                     "dram_pool_bytes     BIGINT  NOT NULL,"
                     "block_size          INT     NOT NULL,"
                     "store_epoch         BIGINT  NOT NULL DEFAULT 0,"
                     "healthy             BOOLEAN NOT NULL DEFAULT true,"
                     "last_heartbeat_ms   BIGINT  NOT NULL DEFAULT 0,"
                     "registered_at_ms    BIGINT  NOT NULL DEFAULT 0);"
                     "ALTER TABLE falcon.%s SET SCHEMA pg_catalog;"
                     "GRANT SELECT ON pg_catalog.%s TO public;"
                     "ALTER EXTENSION falcon ADD TABLE %s;",
                     FalconStoreNodeTableName,
                     FalconStoreNodeTableName,
                     FalconStoreNodeTableName,
                     FalconStoreNodeTableName);
}

void ConstructCreateKvMembershipTablesCommand(StringInfo command)
{
    AppendCreateDnNodeTableDdl(command);
    AppendCreateStoreNodeTableDdl(command);
}

void FalconCreateKvMembershipTables(void)
{
    const bool need_dn    = !CheckIfRelationExists(FalconDnNodeTableName, PG_CATALOG_NAMESPACE);
    const bool need_store = !CheckIfRelationExists(FalconStoreNodeTableName, PG_CATALOG_NAMESPACE);

    if (!need_dn && !need_store)
        return;

    StringInfo toExec = makeStringInfo();
    if (need_dn)
        AppendCreateDnNodeTableDdl(toExec);
    if (need_store)
        AppendCreateStoreNodeTableDdl(toExec);

    if (SPI_connect() != SPI_OK_CONNECT) {
        SPI_finish();
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "could not connect to SPI manager.");
    }

    if (SPI_execute(toExec->data, false, 0) != SPI_OK_UTILITY) {
        SPI_finish();
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "spi exec failed for KV membership tables.");
    }
    SPI_finish();
}

/* -------------------------------------------------------------------------- */
/* SQL-callable entry points                                                  */
/* -------------------------------------------------------------------------- */

PG_FUNCTION_INFO_V1(falcon_create_kv_membership_tables);
PG_FUNCTION_INFO_V1(falcon_dn_node_register);
PG_FUNCTION_INFO_V1(falcon_dn_node_heartbeat);
PG_FUNCTION_INFO_V1(falcon_dn_node_unregister);
PG_FUNCTION_INFO_V1(falcon_dn_node_update_endpoint);
PG_FUNCTION_INFO_V1(falcon_store_node_register);
PG_FUNCTION_INFO_V1(falcon_store_node_heartbeat);
PG_FUNCTION_INFO_V1(falcon_store_node_unregister);
PG_FUNCTION_INFO_V1(falcon_kv_membership_watchdog_tick);

Datum falcon_create_kv_membership_tables(PG_FUNCTION_ARGS)
{
    FalconCreateKvMembershipTables();
    PG_RETURN_INT32(0);
}

Datum falcon_dn_node_register(PG_FUNCTION_ARGS)
{
    const int32 server_id     = PG_GETARG_INT32(0);
    char       *host_node     = PG_GETARG_CSTRING(1);
    char       *pg_host       = PG_GETARG_CSTRING(2);
    const int32 pg_port       = PG_GETARG_INT32(3);
    const int32 kv_brpc_port  = PG_GETARG_INT32(4);
    const int64 dn_epoch      = PG_GETARG_INT64(5);
    const int64 now_ms        = CurrentEpochMillis();

    if (SPI_connect() != SPI_OK_CONNECT) {
        SPI_finish();
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "could not connect to SPI manager.");
    }

    const char *ins =
        "INSERT INTO pg_catalog.falcon_dn_node AS t ("
        "server_id, host_node_name, pg_host, pg_port, kv_brpc_port, dn_epoch, healthy,"
        "last_heartbeat_ms, registered_at_ms) "
        "VALUES ($1, $2::text, $3::text, $4, $5, $6, true, $7::bigint, $7::bigint) "
        "ON CONFLICT (server_id) DO UPDATE SET "
        "host_node_name    = EXCLUDED.host_node_name,"
        "pg_host           = EXCLUDED.pg_host,"
        "pg_port           = EXCLUDED.pg_port,"
        "kv_brpc_port      = EXCLUDED.kv_brpc_port,"
        "dn_epoch          = EXCLUDED.dn_epoch,"
        "healthy           = true,"
        "last_heartbeat_ms = EXCLUDED.last_heartbeat_ms;";

    Oid   argtypes[7] = {INT4OID, TEXTOID, TEXTOID, INT4OID, INT4OID, INT8OID, INT8OID};
    Datum values[7];
    char  nulls[8]    = {' ', ' ', ' ', ' ', ' ', ' ', ' ', '\0'};

    values[0] = Int32GetDatum(server_id);
    values[1] = CStringGetTextDatum(host_node);
    values[2] = CStringGetTextDatum(pg_host);
    values[3] = Int32GetDatum(pg_port);
    values[4] = Int32GetDatum(kv_brpc_port);
    values[5] = Int64GetDatum(dn_epoch);
    values[6] = Int64GetDatum(now_ms);

    if (!SpiOkUpsert(SPI_execute_with_args(ins, 7, argtypes, values, nulls, false, 0))) {
        SPI_finish();
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "falcon_dn_node_register: SPI insert failed.");
    }

    const char *notify_sql =
        "SELECT pg_notify('falcon_kv_dn_membership', json_build_object("
        " 'op', 'register',"
        " 'server_id', $1::int,"
        " 'host_node_name', $2::text,"
        " 'pg_host', $3::text,"
        " 'pg_port', $4::int,"
        " 'kv_brpc_port', $5::int,"
        " 'dn_epoch', $6::bigint)::text);";

    if (SPI_execute_with_args(notify_sql, 6, argtypes, values, nulls, false, 0) != SPI_OK_SELECT) {
        SPI_finish();
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "falcon_dn_node_register: pg_notify failed.");
    }

    SPI_finish();
    PG_RETURN_INT32(0);
}

Datum falcon_dn_node_heartbeat(PG_FUNCTION_ARGS)
{
    const int32 server_id = PG_GETARG_INT32(0);
    const int64 now_ms  = PG_GETARG_INT64(1);

    if (SPI_connect() != SPI_OK_CONNECT) {
        SPI_finish();
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "could not connect to SPI manager.");
    }

    const char *upd = "UPDATE pg_catalog.falcon_dn_node SET last_heartbeat_ms = $2::bigint, healthy = true "
                      "WHERE server_id = $1::int;";

    Oid   argtypes[2] = {INT4OID, INT8OID};
    Datum values[2];
    char  nulls[3]    = {' ', ' ', '\0'};

    values[0] = Int32GetDatum(server_id);
    values[1] = Int64GetDatum(now_ms);

    if (SPI_execute_with_args(upd, 2, argtypes, values, nulls, false, 0) != SPI_OK_UPDATE) {
        SPI_finish();
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "falcon_dn_node_heartbeat: SPI update failed.");
    }

    const char *notify_sql =
        "SELECT pg_notify('falcon_kv_dn_membership', json_build_object("
        " 'op', 'heartbeat', 'server_id', $1::int, 'last_heartbeat_ms', $2::bigint)::text);";

    if (SPI_execute_with_args(notify_sql, 2, argtypes, values, nulls, false, 0) != SPI_OK_SELECT) {
        SPI_finish();
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "falcon_dn_node_heartbeat: pg_notify failed.");
    }

    SPI_finish();
    PG_RETURN_INT32(0);
}

Datum falcon_dn_node_unregister(PG_FUNCTION_ARGS)
{
    const int32 server_id = PG_GETARG_INT32(0);

    if (SPI_connect() != SPI_OK_CONNECT) {
        SPI_finish();
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "could not connect to SPI manager.");
    }

    const char *del = "DELETE FROM pg_catalog.falcon_dn_node WHERE server_id = $1::int;";

    Oid   argtypes[1] = {INT4OID};
    Datum values[1];
    char  nulls[2]    = {' ', '\0'};

    values[0] = Int32GetDatum(server_id);

    if (SPI_execute_with_args(del, 1, argtypes, values, nulls, false, 0) != SPI_OK_DELETE) {
        SPI_finish();
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "falcon_dn_node_unregister: SPI delete failed.");
    }

    const char *notify_sql =
        "SELECT pg_notify('falcon_kv_dn_membership', json_build_object("
        " 'op', 'unregister', 'server_id', $1::int)::text);";

    if (SPI_execute_with_args(notify_sql, 1, argtypes, values, nulls, false, 0) != SPI_OK_SELECT) {
        SPI_finish();
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "falcon_dn_node_unregister: pg_notify failed.");
    }

    SPI_finish();
    PG_RETURN_INT32(0);
}

Datum falcon_dn_node_update_endpoint(PG_FUNCTION_ARGS)
{
    const int32 server_id    = PG_GETARG_INT32(0);
    char       *host_node    = PG_GETARG_CSTRING(1);
    char       *pg_host      = PG_GETARG_CSTRING(2);
    const int32 pg_port      = PG_GETARG_INT32(3);
    const int32 kv_brpc_port = PG_GETARG_INT32(4);

    if (SPI_connect() != SPI_OK_CONNECT) {
        SPI_finish();
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "could not connect to SPI manager.");
    }

    const char *upd =
        "UPDATE pg_catalog.falcon_dn_node SET "
        "host_node_name = $2::text, pg_host = $3::text, pg_port = $4::int, kv_brpc_port = $5::int, "
        "healthy = true "
        "WHERE server_id = $1::int;";

    Oid   argtypes[5] = {INT4OID, TEXTOID, TEXTOID, INT4OID, INT4OID};
    Datum values[5];
    char  nulls[6]    = {' ', ' ', ' ', ' ', ' ', '\0'};

    values[0] = Int32GetDatum(server_id);
    values[1] = CStringGetTextDatum(host_node);
    values[2] = CStringGetTextDatum(pg_host);
    values[3] = Int32GetDatum(pg_port);
    values[4] = Int32GetDatum(kv_brpc_port);

    if (SPI_execute_with_args(upd, 5, argtypes, values, nulls, false, 0) != SPI_OK_UPDATE) {
        SPI_finish();
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "falcon_dn_node_update_endpoint: SPI update failed.");
    }

    const char *notify_sql =
        "SELECT pg_notify('falcon_kv_dn_membership', json_build_object("
        " 'op', 'update_endpoint',"
        " 'server_id', $1::int,"
        " 'host_node_name', $2::text,"
        " 'pg_host', $3::text,"
        " 'pg_port', $4::int,"
        " 'kv_brpc_port', $5::int)::text);";

    if (SPI_execute_with_args(notify_sql, 5, argtypes, values, nulls, false, 0) != SPI_OK_SELECT) {
        SPI_finish();
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "falcon_dn_node_update_endpoint: pg_notify failed.");
    }

    SPI_finish();
    PG_RETURN_INT32(0);
}

Datum falcon_store_node_register(PG_FUNCTION_ARGS)
{
    const int32 store_node_id   = PG_GETARG_INT32(0);
    char       *host_node       = PG_GETARG_CSTRING(1);
    char       *host            = PG_GETARG_CSTRING(2);
    const int32 brpc_port       = PG_GETARG_INT32(3);
    char       *runtime_dir     = PG_GETARG_CSTRING(4);
    char       *shm_name        = PG_GETARG_CSTRING(5);
    const int64 dram_pool_bytes = PG_GETARG_INT64(6);
    const int32 block_size      = PG_GETARG_INT32(7);
    const int64 store_epoch     = PG_GETARG_INT64(8);
    const int64 now_ms          = CurrentEpochMillis();

    if (SPI_connect() != SPI_OK_CONNECT) {
        SPI_finish();
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "could not connect to SPI manager.");
    }

    const char *ins =
        "INSERT INTO pg_catalog.falcon_store_node AS t ("
        "store_node_id, host_node_name, host, brpc_port, runtime_dir, shm_name, dram_pool_bytes, block_size,"
        "store_epoch, healthy, last_heartbeat_ms, registered_at_ms) "
        "VALUES ($1, $2::text, $3::text, $4, $5::text, $6::text, $7::bigint, $8::int, $9::bigint, true, "
        "$10::bigint, $10::bigint) "
        "ON CONFLICT (store_node_id) DO UPDATE SET "
        "host_node_name      = EXCLUDED.host_node_name,"
        "host                = EXCLUDED.host,"
        "brpc_port           = EXCLUDED.brpc_port,"
        "runtime_dir         = EXCLUDED.runtime_dir,"
        "shm_name            = EXCLUDED.shm_name,"
        "dram_pool_bytes     = EXCLUDED.dram_pool_bytes,"
        "block_size          = EXCLUDED.block_size,"
        "store_epoch         = EXCLUDED.store_epoch,"
        "healthy             = true,"
        "last_heartbeat_ms = EXCLUDED.last_heartbeat_ms;";

    Oid   argtypes[10] = {INT4OID, TEXTOID, TEXTOID, INT4OID, TEXTOID, TEXTOID, INT8OID, INT4OID, INT8OID, INT8OID};
    Datum values[10];
    char  nulls[11];

    memset(nulls, ' ', sizeof(nulls));
    nulls[10] = '\0';

    values[0]  = Int32GetDatum(store_node_id);
    values[1]  = CStringGetTextDatum(host_node);
    values[2]  = CStringGetTextDatum(host);
    values[3]  = Int32GetDatum(brpc_port);
    values[4]  = CStringGetTextDatum(runtime_dir);
    values[5]  = CStringGetTextDatum(shm_name);
    values[6]  = Int64GetDatum(dram_pool_bytes);
    values[7]  = Int32GetDatum(block_size);
    values[8]  = Int64GetDatum(store_epoch);
    values[9]  = Int64GetDatum(now_ms);

    if (!SpiOkUpsert(SPI_execute_with_args(ins, 10, argtypes, values, nulls, false, 0))) {
        SPI_finish();
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "falcon_store_node_register: SPI insert failed.");
    }

    const char *notify_sql =
        "SELECT pg_notify('falcon_kv_store_membership', json_build_object("
        " 'op', 'register',"
        " 'store_node_id', $1::int,"
        " 'host_node_name', $2::text,"
        " 'host', $3::text,"
        " 'brpc_port', $4::int,"
        " 'runtime_dir', $5::text,"
        " 'shm_name', $6::text,"
        " 'dram_pool_bytes', $7::bigint,"
        " 'block_size', $8::int,"
        " 'store_epoch', $9::bigint)::text);";

    if (SPI_execute_with_args(notify_sql, 9, argtypes, values, nulls, false, 0) != SPI_OK_SELECT) {
        SPI_finish();
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "falcon_store_node_register: pg_notify failed.");
    }

    SPI_finish();
    PG_RETURN_INT32(0);
}

Datum falcon_store_node_heartbeat(PG_FUNCTION_ARGS)
{
    const int32 store_node_id = PG_GETARG_INT32(0);
    const int64 store_epoch   = PG_GETARG_INT64(1);
    const int64 now_ms        = PG_GETARG_INT64(2);

    if (SPI_connect() != SPI_OK_CONNECT) {
        SPI_finish();
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "could not connect to SPI manager.");
    }

    const char *upd =
        "UPDATE pg_catalog.falcon_store_node SET last_heartbeat_ms = $3::bigint, store_epoch = $2::bigint, "
        "healthy = true WHERE store_node_id = $1::int;";

    Oid   argtypes[3] = {INT4OID, INT8OID, INT8OID};
    Datum values[3];
    char  nulls[4]    = {' ', ' ', ' ', '\0'};

    values[0] = Int32GetDatum(store_node_id);
    values[1] = Int64GetDatum(store_epoch);
    values[2] = Int64GetDatum(now_ms);

    if (SPI_execute_with_args(upd, 3, argtypes, values, nulls, false, 0) != SPI_OK_UPDATE) {
        SPI_finish();
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "falcon_store_node_heartbeat: SPI update failed.");
    }

    const char *notify_sql =
        "SELECT pg_notify('falcon_kv_store_membership', json_build_object("
        " 'op', 'heartbeat', 'store_node_id', $1::int, 'store_epoch', $2::bigint, "
        " 'last_heartbeat_ms', $3::bigint)::text);";

    if (SPI_execute_with_args(notify_sql, 3, argtypes, values, nulls, false, 0) != SPI_OK_SELECT) {
        SPI_finish();
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "falcon_store_node_heartbeat: pg_notify failed.");
    }

    SPI_finish();
    PG_RETURN_INT32(0);
}

Datum falcon_store_node_unregister(PG_FUNCTION_ARGS)
{
    const int32 store_node_id = PG_GETARG_INT32(0);

    if (SPI_connect() != SPI_OK_CONNECT) {
        SPI_finish();
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "could not connect to SPI manager.");
    }

    const char *del = "DELETE FROM pg_catalog.falcon_store_node WHERE store_node_id = $1::int;";

    Oid   argtypes[1] = {INT4OID};
    Datum values[1];
    char  nulls[2]    = {' ', '\0'};

    values[0] = Int32GetDatum(store_node_id);

    if (SPI_execute_with_args(del, 1, argtypes, values, nulls, false, 0) != SPI_OK_DELETE) {
        SPI_finish();
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "falcon_store_node_unregister: SPI delete failed.");
    }

    const char *notify_sql =
        "SELECT pg_notify('falcon_kv_store_membership', json_build_object("
        " 'op', 'unregister', 'store_node_id', $1::int)::text);";

    if (SPI_execute_with_args(notify_sql, 1, argtypes, values, nulls, false, 0) != SPI_OK_SELECT) {
        SPI_finish();
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "falcon_store_node_unregister: pg_notify failed.");
    }

    SPI_finish();
    PG_RETURN_INT32(0);
}

Datum falcon_kv_membership_watchdog_tick(PG_FUNCTION_ARGS)
{
    const int64 now_ms  = PG_GETARG_INT64(0);
    const int64 skew_ms = PG_GETARG_INT64(1);
    uint64      n_flips = 0;

    if (SPI_connect() != SPI_OK_CONNECT) {
        SPI_finish();
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "could not connect to SPI manager.");
    }

    Oid   argtypes[2] = {INT8OID, INT8OID};
    Datum values[2];
    char  nulls[3] = {' ', ' ', '\0'};

    values[0] = Int64GetDatum(now_ms);
    values[1] = Int64GetDatum(skew_ms);

    const char *upd_store =
        "UPDATE pg_catalog.falcon_store_node SET healthy = false "
        "WHERE healthy IS TRUE AND registered_at_ms > 0 "
        "  AND ($1::bigint - last_heartbeat_ms) > $2::bigint;";

    if (SPI_execute_with_args(upd_store, 2, argtypes, values, nulls, false, 0) != SPI_OK_UPDATE) {
        SPI_finish();
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "falcon_kv_membership_watchdog_tick: store update failed.");
    }
    const uint64 n_store = SPI_processed;
    n_flips += n_store;

    if (n_store > 0) {
        const char *notify_sql =
            "SELECT pg_notify('falcon_kv_store_membership', json_build_object("
            " 'op', 'watchdog_unhealthy', 'table', 'falcon_store_node',"
            " 'rows', $3::bigint, 'now_ms', $1::bigint, 'skew_ms', $2::bigint)::text);";
        Oid   argtypes3[3] = {INT8OID, INT8OID, INT8OID};
        Datum values3[3];
        char  nulls3[4] = {' ', ' ', ' ', '\0'};

        values3[0] = Int64GetDatum(now_ms);
        values3[1] = Int64GetDatum(skew_ms);
        values3[2] = Int64GetDatum((int64) n_store);

        if (SPI_execute_with_args(notify_sql, 3, argtypes3, values3, nulls3, false, 0) != SPI_OK_SELECT) {
            SPI_finish();
            FALCON_ELOG_ERROR(PROGRAM_ERROR,
                              "falcon_kv_membership_watchdog_tick: store pg_notify failed.");
        }
    }

    const char *upd_dn =
        "UPDATE pg_catalog.falcon_dn_node SET healthy = false "
        "WHERE healthy IS TRUE AND registered_at_ms > 0 "
        "  AND ($1::bigint - last_heartbeat_ms) > $2::bigint;";

    if (SPI_execute_with_args(upd_dn, 2, argtypes, values, nulls, false, 0) != SPI_OK_UPDATE) {
        SPI_finish();
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "falcon_kv_membership_watchdog_tick: dn update failed.");
    }
    const uint64 n_dn = SPI_processed;
    n_flips += n_dn;

    if (n_dn > 0) {
        const char *notify_sql =
            "SELECT pg_notify('falcon_kv_dn_membership', json_build_object("
            " 'op', 'watchdog_unhealthy', 'table', 'falcon_dn_node',"
            " 'rows', $3::bigint, 'now_ms', $1::bigint, 'skew_ms', $2::bigint)::text);";
        Oid   argtypes3[3] = {INT8OID, INT8OID, INT8OID};
        Datum values3[3];
        char  nulls3[4] = {' ', ' ', ' ', '\0'};

        values3[0] = Int64GetDatum(now_ms);
        values3[1] = Int64GetDatum(skew_ms);
        values3[2] = Int64GetDatum((int64) n_dn);

        if (SPI_execute_with_args(notify_sql, 3, argtypes3, values3, nulls3, false, 0) != SPI_OK_SELECT) {
            SPI_finish();
            FALCON_ELOG_ERROR(PROGRAM_ERROR,
                              "falcon_kv_membership_watchdog_tick: dn pg_notify failed.");
        }
    }

    SPI_finish();
    PG_RETURN_INT64((int64) n_flips);
}
