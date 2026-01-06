#!/bin/bash
DIR=$(dirname $(readlink -f "${BASH_SOURCE[0]}"))
source $DIR/falcon_meta_config.sh

CPU_HALF=$(( $(nproc) / 2 ))
[ $CPU_HALF -eq 0 ] && CPU_HALF=32
FalconConnectionPoolSize=$CPU_HALF
FalconConnectionPoolBatchSize=1024
FalconConnectionPoolWaitAdjust=1
FalconConnectionPoolWaitMin=1
FalconConnectionPoolWaitMax=500
FalconConnectionPoolShmemSize=$((256)) #unit: MB
username=$USER
export PG_INSTALL_DIR="${PG_INSTALL_DIR:-$HOME/metadb}"

server_name_list=()
server_ip_list=()
server_port_list=()

shardcount=50

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
falcon_communication.plugin_path = '$PG_INSTALL_DIR/lib/postgresql/libbrpcplugin.so'
falcon_communication.server_ip = '$cnIp'
EOF
        echo "host all all 0.0.0.0/0 trust" >>"$cnPath/pg_hba.conf"
    fi

    if ! pg_ctl status -D "$cnPath" &>/dev/null; then
        pg_ctl start -l "$DIR/cnlogfile0.log" -D "$cnPath" -c
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
falcon_communication.plugin_path = '$PG_INSTALL_DIR/lib/postgresql/libbrpcplugin.so'
falcon_communication.server_ip = '${workerIp}'
EOF
                echo "host all all 0.0.0.0/0 trust" >>"${workerPath}/pg_hba.conf"
            fi

            if ! pg_ctl status -D "$workerPath" &>/dev/null; then
                pg_ctl start -l "${DIR}/workerlogfile${i}.log" -D "${workerPath}" -c
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
select falcon_start_background_service();
EOF
    done

    # # Create root directory on coordinator
    psql -d postgres -h "$cnIp" -p "${cnPortPrefix}0" -c "SELECT falcon_plain_mkdir('/');"

fi
