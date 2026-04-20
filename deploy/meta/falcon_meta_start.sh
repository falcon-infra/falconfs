#!/bin/bash
DIR=$(dirname $(readlink -f "${BASH_SOURCE[0]}"))
source $DIR/falcon_meta_config.sh

# 安装/卸载 PostgreSQL falcon 扩展文件到系统目录
# PostgreSQL 在编译时确定扩展文件 (.control, .sql) 的查找位置，无法通过配置修改
install_falcon_extension() {
    local pg_ext_dir="$(pg_config --sharedir)/extension"
    local pg_lib_dir="$(pg_config --pkglibdir)"

    echo "Installing Falcon extension files to PostgreSQL system directories..."
    echo "  Extension files: $pg_ext_dir"
    echo "  Library files: $pg_lib_dir"
    sudo mkdir -p "$pg_ext_dir"
    sudo cp -f "$FALCONFS_INSTALL_DIR/falcon_meta/share/extension"/falcon* "$pg_ext_dir/" 2>/dev/null || true
    sudo cp -f "$FALCONFS_INSTALL_DIR/falcon_meta/lib/postgresql"/falcon*.so "$pg_lib_dir/" 2>/dev/null || true

    echo "Falcon extension files installed."
}

# Parse command line arguments
COMM_PLUGIN="brpc"
while [[ $# -gt 0 ]]; do
    case "$1" in
        --comm-plugin=*)
            COMM_PLUGIN="${1#*=}"
            shift
            ;;
        --comm-plugin)
            COMM_PLUGIN="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 [options]"
            echo "Options:"
            echo "  --comm-plugin=PLUGIN  Communication plugin: brpc (default) or hcom"
            echo "  -h, --help            Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

if [[ "$COMM_PLUGIN" != "brpc" && "$COMM_PLUGIN" != "hcom" ]]; then
    echo "Unsupported COMM_PLUGIN '$COMM_PLUGIN' (use brpc or hcom)"
    exit 1
fi

check_lock_conflict_for_port() {
    local port="$1"
    # NOTE: lock file path follows PostgreSQL unix_socket_directories (default /tmp).
    # If unix_socket_directories is changed in postgresql.conf, update this path accordingly.
    local lock_file="/tmp/.s.PGSQL.${port}.lock"

    if [ ! -e "$lock_file" ]; then
        return 0
    fi

    local owner
    owner=$(stat -c '%U' "$lock_file" 2>/dev/null || true)
    if [ -n "$owner" ] && [ "$owner" != "$USER" ]; then
        echo "Error: detected PostgreSQL lock conflict on $lock_file (owner: $owner, current user: $USER)." >&2
        exit 1
    fi
}

check_local_socket_lock_conflict() {
    local port

    if [[ "$cnIp" == "$localIp" ]]; then
        port="${cnPortPrefix}0"
        check_lock_conflict_for_port "$port"
    fi

    for ((n = 0; n < ${#workerIpList[@]}; n++)); do
        if [[ "${workerIpList[$n]}" == "$localIp" ]]; then
            for ((i = 0; i < ${workerNumList[$n]}; i++)); do
                port="${workerPortPrefix}${i}"
                check_lock_conflict_for_port "$port"
            done
        fi
    done
}

check_local_socket_lock_conflict
  
is_local_meta_running() {
    local path

    if [[ "$cnIp" == "$localIp" ]]; then
        path="${cnPathPrefix}0"
        if [ ! -d "$path" ] || ! pg_ctl status -D "$path" >/dev/null 2>&1; then
            return 1
        fi
    fi

    for ((n = 0; n < ${#workerIpList[@]}; n++)); do
        if [[ "${workerIpList[$n]}" == "$localIp" ]]; then
            for ((i = 0; i < ${workerNumList[$n]}; i++)); do
                path="${workerPathPrefix}${i}"
                if [ ! -d "$path" ] || ! pg_ctl status -D "$path" >/dev/null 2>&1; then
                    return 1
                fi
            done
        fi
    done

    return 0
}

if is_local_meta_running; then
    echo "Falcon metadata services are already running on local node, skip re-initialization"
    exit 0
fi

CPU_HALF=$(( $(nproc) / 2 ))
[ $CPU_HALF -eq 0 ] && CPU_HALF=32
FalconConnectionPoolSize=$CPU_HALF
FalconConnectionPoolBatchSize=1024
FalconConnectionPoolWaitAdjust=1
FalconConnectionPoolWaitMin=1
FalconConnectionPoolWaitMax=500
FalconConnectionPoolShmemSize=$((256)) #unit: MB
username=$USER

server_name_list=()
server_ip_list=()
server_port_list=()

shardcount=50

comm_plugin_path="$FALCONFS_INSTALL_DIR/falcon_meta/lib/postgresql/lib${COMM_PLUGIN}plugin.so"

# 安装 falcon 扩展到 PostgreSQL 系统目录
install_falcon_extension

if [[ "$cnIp" == "$localIp" ]]; then
    cnPath="${cnPathPrefix}0"
    cnPort="${cnPortPrefix}0"
    cnMonitorPort="${cnMonitorPortPrefix}0"
    cnPoolerPort="${cnPoolerPortPrefix}0"

    if [ ! -d "$cnPath" ]; then
        mkdir -p "$cnPath"
        initdb -D "$cnPath"

        # Configure PostgreSQL
        cp "$DIR/postgresql.conf.template" "$cnPath/postgresql.conf"
        cat >>"$cnPath/postgresql.conf" <<EOF
shared_preload_libraries = 'falcon'
port=$cnPort
listen_addresses = '*'
wal_level = logical
max_replication_slots = 8
max_wal_senders = 8
falcon_connection_pool.port = $cnPoolerPort
falcon_connection_pool.pool_size = $FalconConnectionPoolSize
falcon_connection_pool.shmem_size = $FalconConnectionPoolShmemSize
falcon_connection_pool.batch_size = $FalconConnectionPoolBatchSize
falcon_connection_pool.wait_adjust = $FalconConnectionPoolWaitAdjust
falcon_connection_pool.wait_min = $FalconConnectionPoolWaitMin
falcon_connection_pool.wait_max = $FalconConnectionPoolWaitMax
falcon_communication.plugin_path = '$comm_plugin_path'
falcon_communication.server_ip = '$cnIp'
falcon_plugin.directory = '$(cd $DIR/../.. && pwd)/plugins'
falcon.local_ip = '$localIp'
falcon.perf_enabled = on
EOF
        echo "host all all 0.0.0.0/0 trust" >>"$cnPath/pg_hba.conf"
    fi

    if ! pg_ctl status -D "$cnPath" &>/dev/null; then
        if ! pg_ctl start -l "$DIR/cnlogfile0.log" -D "$cnPath" -c; then
            echo "Error: failed to start coordinator PostgreSQL at $cnPath" >&2
            echo "Hint: check $DIR/cnlogfile0.log" >&2
            exit 1
        fi
    fi

    if ! psql -d postgres -h "$cnIp" -p "$cnPort" -tAc "SELECT 1 FROM pg_extension WHERE extname='falcon';" | grep -q 1; then
        psql -d postgres -h "$cnIp" -p "$cnPort" -c "CREATE EXTENSION falcon;"
    fi

    server_name_list+=("cn0")
    server_ip_list+=("$cnIp")
    server_port_list+=("$cnPort")
fi

for ((n = 0; n < ${#workerIpList[@]}; n++)); do
    workerIp="${workerIpList[$n]}"
    for ((i = 0; i < ${workerNumList[n]}; i++)); do
        workerPort="${workerPortPrefix}${i}"
        workerMonitorPort="${workerMonitorPortPrefix}${i}"

        if [[ "$workerIp" == "$localIp" ]]; then
            workerPath="${workerPathPrefix}${i}"
            workerPoolerPort="${workerPollerPortPrefix}${i}"

            if [ ! -d "$workerPath" ]; then
                mkdir -p "$workerPath"
                initdb -D "${workerPath}"

                cp "${DIR}/postgresql.conf.template" "${workerPath}/postgresql.conf"
                cat >>"${workerPath}/postgresql.conf" <<EOF
shared_preload_libraries = 'falcon'
port=${workerPort}
listen_addresses = '*'
wal_level = logical
max_replication_slots = 8
max_wal_senders = 8
falcon_connection_pool.port = ${workerPoolerPort}
falcon_connection_pool.pool_size = ${FalconConnectionPoolSize}
falcon_connection_pool.shmem_size = ${FalconConnectionPoolShmemSize}
falcon_connection_pool.batch_size = $FalconConnectionPoolBatchSize
falcon_connection_pool.wait_adjust = $FalconConnectionPoolWaitAdjust
falcon_connection_pool.wait_min = $FalconConnectionPoolWaitMin
falcon_connection_pool.wait_max = $FalconConnectionPoolWaitMax
falcon_communication.plugin_path = '$comm_plugin_path'
falcon_communication.server_ip = '${workerIp}'
falcon_plugin.directory = '$(cd $DIR/../.. && pwd)/plugins'
falcon.local_ip = '$localIp'
falcon.perf_enabled = on
EOF
                echo "host all all 0.0.0.0/0 trust" >>"${workerPath}/pg_hba.conf"
            fi

            if ! pg_ctl status -D "$workerPath" &>/dev/null; then
                if ! pg_ctl start -l "${DIR}/workerlogfile${i}.log" -D "${workerPath}" -c; then
                    echo "Error: failed to start worker PostgreSQL at $workerPath" >&2
                    echo "Hint: check ${DIR}/workerlogfile${i}.log" >&2
                    exit 1
                fi
            fi

            if ! psql -d postgres -h "${workerIp}" -p "${workerPort}" -tAc "SELECT 1 FROM pg_extension WHERE extname='falcon';" | grep -q 1; then
                psql -d postgres -h "${workerIp}" -p "${workerPort}" -c "CREATE EXTENSION falcon;"
            fi
        fi

        server_name_list+=("worker_${n}_${i}")
        server_ip_list+=("${workerIp}")
        server_port_list+=("${workerPort}")
    done
done

# Load Falcon
if [[ "$cnIp" == "$localIp" ]]; then
    server_num=${#server_port_list[@]}

    # Register all servers with each other
    for ((i = 0; i < server_num; i++)); do
        name=${server_name_list[i]}
        ip=${server_ip_list[i]}
        port=${server_port_list[i]}
        is_local=false

        for ((j = 0; j < server_num; j++)); do
            [[ $i -eq $j ]] && is_local=true || is_local=false

            # Check if server already exists before inserting
            psql_cmd="SELECT NOT EXISTS(SELECT 1 FROM falcon_foreign_server WHERE server_id = $i);"
            exists=$(psql -d postgres -h "${server_ip_list[j]}" -p "${server_port_list[j]}" -tAc "$psql_cmd")

            if [[ "$exists" == "t" ]]; then
                psql_cmd="select falcon_insert_foreign_server($i, '$name', '$ip', $port, $is_local, '$username');"
                echo "$psql_cmd"
                psql -d postgres -h "${server_ip_list[j]}" -p "${server_port_list[j]}" -c "$psql_cmd"
            else
                echo "Server $i already registered on ${server_name_list[j]}"
            fi
        done
    done

    # Initialize sharding and services on all servers
    for ((i = 0; i < server_num; i++)); do
        # Check if shard table is empty before building
        count=$(psql -d postgres -h "${server_ip_list[i]}" -p "${server_port_list[i]}" -tAc "SELECT COUNT(*) FROM falcon_shard_table;")
        if [[ "$count" == "0" ]]; then
            psql -d postgres -h "${server_ip_list[i]}" -p "${server_port_list[i]}" -c "select falcon_build_shard_table($shardcount);"
        else
            echo "Shard table already initialized on ${server_name_list[i]}"
        fi

        psql -d postgres -h "${server_ip_list[i]}" -p "${server_port_list[i]}" <<EOF
select falcon_create_distributed_data_table();
select falcon_create_slice_table();
select falcon_create_kvmeta_table();
select falcon_start_background_service();
EOF
    done

    # # Create root directory on coordinator
    psql -d postgres -h "$cnIp" -p "${cnPortPrefix}0" -c "SELECT falcon_plain_mkdir('/');"

fi
