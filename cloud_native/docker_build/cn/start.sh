#!/bin/bash
# 使用源码编译安装的 PostgreSQL 17
# 设置默认的安装目录
FALCONFS_INSTALL_DIR=${FALCONFS_INSTALL_DIR:-/usr/local/falconfs}
COMM_PLUGIN=${FALCON_COMM_PLUGIN:-brpc}

if [[ "${COMM_PLUGIN}" != "brpc" && "${COMM_PLUGIN}" != "hcom" ]]; then
    echo "ERROR: Unsupported FALCON_COMM_PLUGIN='${COMM_PLUGIN}' (use brpc or hcom)" >&2
    exit 1
fi

PG_BIN_DIR=${PG_BINDIR:-}
PG_LIB_DIR=${PG_LIBDIR:-}
if [ -z "${PG_BIN_DIR}" ] || [ -z "${PG_LIB_DIR}" ]; then
    if command -v pg_config >/dev/null 2>&1; then
        [ -z "${PG_BIN_DIR}" ] && PG_BIN_DIR="$(pg_config --bindir)"
        [ -z "${PG_LIB_DIR}" ] && PG_LIB_DIR="$(pg_config --libdir)"
    fi
fi

missing_pg_vars=()
[ -z "${PG_BIN_DIR}" ] && missing_pg_vars+=("PG_BINDIR")
[ -z "${PG_LIB_DIR}" ] && missing_pg_vars+=("PG_LIBDIR")
if [ "${#missing_pg_vars[@]}" -gt 0 ]; then
    echo "ERROR: PostgreSQL runtime paths are incomplete and pg_config is unavailable." >&2
    echo "Missing: ${missing_pg_vars[*]}" >&2
    echo "Install the PostgreSQL devel package that provides pg_config, or set:" >&2
    echo "  export PG_BINDIR=/path/to/postgresql/bin" >&2
    echo "  export PG_LIBDIR=/path/to/postgresql/lib" >&2
    exit 1
fi

export PATH=${PG_BIN_DIR}:$PATH
export LD_LIBRARY_PATH=${PG_LIB_DIR}:${FALCONFS_INSTALL_DIR}/falcon_meta/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}
if [ -d /usr/local/obs/lib ]; then
    export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/obs/lib
fi
DATA_DIR=${FALCONFS_INSTALL_DIR}/data
METADATA_DIR=${DATA_DIR}/metadata
COMM_PLUGIN_PATH="${FALCONFS_INSTALL_DIR}/falcon_meta/lib/postgresql/lib${COMM_PLUGIN}plugin.so"

if [ ! -f "${COMM_PLUGIN_PATH}" ]; then
    echo "ERROR: communication plugin not found: ${COMM_PLUGIN_PATH}" >&2
    exit 1
fi

if [ -d "${METADATA_DIR}/pg_wal" ]; then
    pg_ctl restart -D ${METADATA_DIR}
else
    initdb -D ${METADATA_DIR}
    cp ${FALCONFS_INSTALL_DIR}/falcon_cn/postgresql_falcon.conf ${METADATA_DIR}/postgresql.conf
    echo "host all all 0.0.0.0/0 trust" >>${METADATA_DIR}/pg_hba.conf
    echo "host replication all 0.0.0.0/0 trust" >>${METADATA_DIR}/pg_hba.conf
    # Ensure PostgreSQL can locate falcon.so from FalconFS private lib path.
    echo "dynamic_library_path = '\$libdir:${FALCONFS_INSTALL_DIR}/falcon_meta/lib/postgresql'" >>${METADATA_DIR}/postgresql.conf
    echo "shared_preload_libraries='falcon'" >>${METADATA_DIR}/postgresql.conf
    echo "listen_addresses='*'" >>${METADATA_DIR}/postgresql.conf
    echo "wal_level=logical" >>${METADATA_DIR}/postgresql.conf
    echo "max_wal_senders=10" >>${METADATA_DIR}/postgresql.conf
    echo "hot_standby=on" >>${METADATA_DIR}/postgresql.conf
    echo "synchronous_commit=on" >>${METADATA_DIR}/postgresql.conf
    echo "falcon_connection_pool.port = ${NODE_PORT:-5442}" >>${METADATA_DIR}/postgresql.conf
    echo "falcon_plugin.directory = '/FalconFS/plugins'" >>${METADATA_DIR}/postgresql.conf
    echo "falcon.local_ip = '${NODE_IP:-127.0.0.1}'" >>${METADATA_DIR}/postgresql.conf

    # default replica_server_num set to 2, compatible to ADS.
    replica_server_num=${replica_server_num:-2}
    sync_replica_num=$(((replica_server_num + 1) / 2))
    if [ "${replica_server_num}" == "0" ]; then
        echo "synchronous_standby_names=''" >>${METADATA_DIR}/postgresql.conf
    else
        echo "synchronous_standby_names='${sync_replica_num}(*)'" >>${METADATA_DIR}/postgresql.conf
    fi
    echo "full_page_writes=on" >>${METADATA_DIR}/postgresql.conf
    echo "wal_log_hints=on" >>${METADATA_DIR}/postgresql.conf
    echo "logging_collector=on" >>${METADATA_DIR}/postgresql.conf
    echo "log_filename='postgresql-.%a.log'" >>${METADATA_DIR}/postgresql.conf
    echo "log_truncate_on_rotation=on" >>${METADATA_DIR}/postgresql.conf
    echo "log_rotation_age=1440" >>${METADATA_DIR}/postgresql.conf
    echo "log_rotation_size=1000000" >>${METADATA_DIR}/postgresql.conf
    echo "falcon_connection_pool.port = 5442" >>${METADATA_DIR}/postgresql.conf
    echo "falcon_connection_pool.pool_size = 64" >>${METADATA_DIR}/postgresql.conf
    echo "falcon_connection_pool.shmem_size = 256" >>${METADATA_DIR}/postgresql.conf
    echo "falcon_connection_pool.batch_size = 1024" >>${METADATA_DIR}/postgresql.conf
    echo "falcon_connection_pool.wait_adjust = 1" >>${METADATA_DIR}/postgresql.conf
    echo "falcon_connection_pool.wait_min = 1" >>${METADATA_DIR}/postgresql.conf
    echo "falcon_connection_pool.wait_max = 500" >>${METADATA_DIR}/postgresql.conf
    echo "falcon_communication.plugin_path = '${COMM_PLUGIN_PATH}'" >>${METADATA_DIR}/postgresql.conf
    echo "falcon_communication.server_ip = '${POD_IP}'" >>${METADATA_DIR}/postgresql.conf
    pg_ctl start -D ${METADATA_DIR}
fi

bash ${FALCONFS_INSTALL_DIR}/falcon_cn/rm_logs.sh &
python3 ${FALCONFS_INSTALL_DIR}/falcon_cm/falcon_cm_cn.py
