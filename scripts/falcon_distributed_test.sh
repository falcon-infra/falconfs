#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# FalconFS Single-Node Distributed Test Deployment Script
# Architecture: 1 CN (Coordinator) + 2 DNs (Workers) + 2 Clients (FUSE)
# Purpose: Local development and testing for distributed scenarios
# ============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
INSTALL_DIR="${FALCONFS_INSTALL_DIR:-/usr/local/falconfs}"
DEPLOY_DIR="$INSTALL_DIR/deploy"

# Regression runs invoke this script from non-login shells, where WSL may place
# the distro's postgresql-common wrappers ahead of the locally built PostgreSQL.
# Prefer the workspace install when present so pg_config/psql/postgres agree.
if [ -x /usr/local/pgsql/bin/pg_config ]; then
    export PATH="/usr/local/pgsql/bin:${PATH:-}"
fi

# Color definitions
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_step() { echo -e "${BLUE}[STEP]${NC} $1"; }

# ============================================================================
# Configuration
# ============================================================================

# Meta nodes (PostgreSQL)
CN_PORT=55500
CN_POOLER_PORT=55510
DN1_PORT=55520
DN1_POOLER_PORT=55530
DN2_PORT=55540
DN2_POOLER_PORT=55550
# Optional third DN (v6.5 mixed-colocation / 3-shard routing). Enabled when KV_THREE_DNS=1.
DN3_PORT=55560
DN3_POOLER_PORT=55570
META_DB_DIR="$HOME/falcon_metadata"

kv_three_dns_enabled() { [ "${KV_THREE_DNS:-0}" = "1" ]; }

# CSV of DN pooler TCP ports passed to falcon_kv_store for multi-region DRAM.
kv_store_poolers_csv() {
    if kv_three_dns_enabled; then
        echo "${DN1_POOLER_PORT},${DN2_POOLER_PORT},${DN3_POOLER_PORT}"
    else
        echo "${DN1_POOLER_PORT},${DN2_POOLER_PORT}"
    fi
}

# v6.5: default KV store BRPC port (DN falcon_kv.store_spill_endpoint must match first store).
KV_STORE_BRPC_PORT="${KV_STORE_BRPC_PORT:-18765}"
export KV_STORE_BRPC_PORT

# Falcon BRPC connection pool shared memory (MB) for CN/DN ``postgresql.conf``.
# Larger values reduce pressure when many parallel KV metadata RPCs hit the pool.
: "${FALCON_POOL_SHMEM_MB:=256}"
if [ "${KV_THREE_DNS:-0}" = "1" ] && [ "${FALCON_POOL_SHMEM_MB}" = "256" ]; then
    FALCON_POOL_SHMEM_MB=512
fi
export FALCON_POOL_SHMEM_MB

# Client nodes (FUSE mounts)
CLIENT1_RPC_PORT=56039
CLIENT1_MNT="/tmp/falcon_mnt1"
CLIENT1_CACHE="/tmp/falcon_client1_cache"
CLIENT1_LOG="/tmp/falcon_client1.log"

CLIENT2_RPC_PORT=56042
CLIENT2_MNT="/tmp/falcon_mnt2"
CLIENT2_CACHE="/tmp/falcon_client2_cache"
CLIENT2_LOG="/tmp/falcon_client2.log"

# ============================================================================
# Helper Functions
# ============================================================================

cleanup() {
    log_step "Cleaning up old environment..."
    
    # Stop all falcon_client processes
    pkill -9 falcon_client 2>/dev/null || true
    
    # Unmount all FUSE mounts
    for mnt in "$CLIENT1_MNT" "$CLIENT2_MNT"; do
        if mount | grep -q "$mnt"; then
            sudo umount -l "$mnt" 2>/dev/null || true
        fi
    done
    
    # Stop Meta (PostgreSQL)
    stop_pg_node "CN" "$META_DB_DIR/coordinator0" || true
    stop_pg_node "DN1" "$META_DB_DIR/worker0" || true
    stop_pg_node "DN2" "$META_DB_DIR/worker1" || true
    stop_pg_node "DN3" "$META_DB_DIR/worker2" || true
    kill_listener_on_port "$CN_PORT" "CN SQL" || true
    kill_listener_on_port "$CN_POOLER_PORT" "CN pooler" || true
    kill_listener_on_port "$DN1_PORT" "DN1 SQL" || true
    kill_listener_on_port "$DN1_POOLER_PORT" "DN1 pooler" || true
    kill_listener_on_port "$DN2_PORT" "DN2 SQL" || true
    kill_listener_on_port "$DN2_POOLER_PORT" "DN2 pooler" || true
    kill_listener_on_port "$DN3_PORT" "DN3 SQL" || true
    kill_listener_on_port "$DN3_POOLER_PORT" "DN3 pooler" || true
    kill_listener_on_port "${KV_STORE_BRPC_PORT:-18765}" "KV store BRPC" || true
    
    # Clean temporary files
    rm -rf "$META_DB_DIR" 2>/dev/null || true
    rm -rf "$CLIENT1_CACHE" "$CLIENT2_CACHE" 2>/dev/null || true
    rm -f "$CLIENT1_LOG" "$CLIENT2_LOG" 2>/dev/null || true
    
    sleep 2
    log_info "Cleanup completed"
}

check_port() {
    local port=$1
    if ss -tuln 2>/dev/null | grep -q ":${port} " || netstat -tuln 2>/dev/null | grep -q ":${port} "; then
        log_error "Port $port is already in use"
        return 1
    fi
    return 0
}

# Wait until something is listening on TCP `port` (BRPC / arbitrary), not Postgres.
wait_for_listen_tcp() {
    local port=$1
    local timeout_s=${2:-30}
    local i=0
    while [ "$i" -lt "$timeout_s" ]; do
        if ss -tln 2>/dev/null | grep -q ":${port} " || netstat -tln 2>/dev/null | grep -q ":${port} "; then
            return 0
        fi
        sleep 1
        i=$((i + 1))
    done
    return 1
}

# Wait until nothing is listening on TCP `port` (used after SIGTERM so the next
# bind does not fail with EADDRINUSE while the old brpc server drains).
wait_for_tcp_port_free() {
    local port=$1
    local timeout_s=${2:-45}
    local i=0
    while [ "$i" -lt "$timeout_s" ]; do
        if ! ss -tln 2>/dev/null | grep -q ":${port} " &&
            ! netstat -tln 2>/dev/null | grep -q ":${port} "; then
            return 0
        fi
        sleep 1
        i=$((i + 1))
    done
    return 1
}

# Wait until a FUSE client has mounted `mnt` (used after falcon_client starts).
wait_for_fuse_mount() {
    local mnt=$1
    local timeout_s=${2:-90}
    local i=0
    while [ "$i" -lt "$timeout_s" ]; do
        if command -v findmnt >/dev/null 2>&1; then
            if findmnt "$mnt" >/dev/null 2>&1; then
                return 0
            fi
        else
            if mount 2>/dev/null | grep -q " on ${mnt} "; then
                return 0
            fi
        fi
        sleep 1
        i=$((i + 1))
    done
    return 1
}

# Copy each DN's `falcon_dn_node` row onto the CN catalog. DN BRPC startup
# registers membership only in that DN's local database; Python
# `membership_refresh` / `KVStoreFacadeRegistry` read membership over libpq from
# the CN by default (`FALCON_KV_CN_CONNINFO` → port $CN_PORT), so the harness
# mirrors rows here for discovery to see both DNs.
mirror_kv_dn_membership_rows_to_cn() {
    log_step "Seeding CN pg_catalog.falcon_dn_node (harness topology for libpq discovery)..."
    # DN self-registration runs in the pooler BRPC worker; rows may not be visible
    # from every psql target. Always upsert onto CN using the same ports the pooler
    # uses (PostPortNumber == falcon_connection_pool.port), and align dn_epoch when
    # the local DN catalogs already have a row.
    local e1=1
    local e2=1
    local e3=1
    local te1 te2 te3
    local w
    for w in $(seq 1 30); do
        te1=$(psql -d postgres -h 127.0.0.1 -p "$DN1_PORT" -tAXc \
            "SELECT dn_epoch::text FROM pg_catalog.falcon_dn_node WHERE server_id=1 LIMIT 1;" 2>/dev/null | tr -d '[:space:]')
        te2=$(psql -d postgres -h 127.0.0.1 -p "$DN2_PORT" -tAXc \
            "SELECT dn_epoch::text FROM pg_catalog.falcon_dn_node WHERE server_id=2 LIMIT 1;" 2>/dev/null | tr -d '[:space:]')
        if kv_three_dns_enabled; then
            te3=$(psql -d postgres -h 127.0.0.1 -p "$DN3_PORT" -tAXc \
                "SELECT dn_epoch::text FROM pg_catalog.falcon_dn_node WHERE server_id=3 LIMIT 1;" 2>/dev/null | tr -d '[:space:]')
        else
            te3="ok"
        fi
        if [ -n "$te1" ] && [ -n "$te2" ] && [ -n "$te3" ]; then
            e1=$te1
            e2=$te2
            if kv_three_dns_enabled; then
                e3=$te3
            fi
            break
        fi
        sleep 2
    done

    if kv_three_dns_enabled; then
        if ! psql -v ON_ERROR_STOP=1 -d postgres -h 127.0.0.1 -p "$CN_PORT" -c "\
SELECT pg_catalog.falcon_dn_node_register(1, 'worker0'::cstring, '127.0.0.1'::cstring, ${DN1_POOLER_PORT}::int, ${DN1_POOLER_PORT}::int, ${e1}::bigint);\
SELECT pg_catalog.falcon_dn_node_register(2, 'worker1'::cstring, '127.0.0.1'::cstring, ${DN2_POOLER_PORT}::int, ${DN2_POOLER_PORT}::int, ${e2}::bigint);\
SELECT pg_catalog.falcon_dn_node_register(3, 'worker2'::cstring, '127.0.0.1'::cstring, ${DN3_POOLER_PORT}::int, ${DN3_POOLER_PORT}::int, ${e3}::bigint);\
" >/dev/null; then
            log_error "falcon_dn_node_register seed onto CN failed"
            return 1
        fi
        log_info "CN falcon_dn_node seed OK (server_id 1..3 dn_epochs ${e1},${e2},${e3})"
    else
        if ! psql -v ON_ERROR_STOP=1 -d postgres -h 127.0.0.1 -p "$CN_PORT" -c "\
SELECT pg_catalog.falcon_dn_node_register(1, 'worker0'::cstring, '127.0.0.1'::cstring, ${DN1_POOLER_PORT}::int, ${DN1_POOLER_PORT}::int, ${e1}::bigint);\
SELECT pg_catalog.falcon_dn_node_register(2, 'worker1'::cstring, '127.0.0.1'::cstring, ${DN2_POOLER_PORT}::int, ${DN2_POOLER_PORT}::int, ${e2}::bigint);\
" >/dev/null; then
            log_error "falcon_dn_node_register seed onto CN failed"
            return 1
        fi
        log_info "CN falcon_dn_node seed OK (server_id 1 dn_epoch=${e1}, server_id 2 dn_epoch=${e2})"
    fi
}

stop_pg_node() {
    local node_name=$1
    local data_dir=$2

    if [ ! -d "$data_dir" ]; then
        return 0
    fi

    if ! pg_ctl status -D "$data_dir" >/dev/null 2>&1; then
        log_info "$node_name already stopped"
        return 0
    fi

    if pg_ctl stop -D "$data_dir" -m fast -w -t 30 >/dev/null 2>&1; then
        log_info "Stopped $node_name"
        return 0
    fi

    log_warn "$node_name did not stop cleanly; forcing shutdown"

    local pid_file="$data_dir/postmaster.pid"
    local pid=""
    if [ -f "$pid_file" ]; then
        pid="$(sed -n '1p' "$pid_file" 2>/dev/null || true)"
    fi

    if [ -n "$pid" ]; then
        kill -TERM "$pid" 2>/dev/null || true
        sleep 2
        if kill -0 "$pid" 2>/dev/null; then
            kill -KILL "$pid" 2>/dev/null || true
        fi
    fi

    for _ in {1..10}; do
        if ! pg_ctl status -D "$data_dir" >/dev/null 2>&1; then
            log_info "Stopped $node_name (forced)"
            return 0
        fi
        sleep 1
    done

    log_error "Failed to stop $node_name"
    return 1
}

kill_listener_on_port() {
    local port=$1
    local label=$2
    local pids=""

    pids="$(ss -lptn "( sport = :${port} )" 2>/dev/null | awk -F'pid=' 'NF>1{split($2,a,","); print a[1]}' | sort -u)"
    if [ -z "$pids" ]; then
        return 0
    fi

    log_warn "Port $port still occupied ($label); terminating listener(s)"
    for pid in $pids; do
        kill -TERM "$pid" 2>/dev/null || true
    done
    sleep 1

    pids="$(ss -lptn "( sport = :${port} )" 2>/dev/null | awk -F'pid=' 'NF>1{split($2,a,","); print a[1]}' | sort -u)"
    for pid in $pids; do
        kill -KILL "$pid" 2>/dev/null || true
    done
}

# ============================================================================
# Build and Install
# ============================================================================

build_and_install() {
    log_step "========================================="
    log_step "Building and Installing FalconFS"
    log_step "========================================="
    
    cd "$PROJECT_DIR"
    
    # Build
    log_step "Building FalconFS..."
    ./build.sh build falcon
    
    # Install
    log_step "Installing FalconFS..."
    sudo ./build.sh install
    
    log_info "Build and install completed"
}

# ============================================================================
# Start Services
# ============================================================================

start_all() {
    log_step "========================================="
    log_step "Starting FalconFS Single-Node Cluster"
    log_step "Architecture: 1 CN + 2 DNs + 2 Clients"
    log_step "========================================="
    
    # 1. Clean old environment
    cleanup
    
    # 2. Check port availability
    log_step "Checking port availability..."
    check_port $CN_PORT || exit 1
    check_port $CN_POOLER_PORT || exit 1
    check_port $DN1_PORT || exit 1
    check_port $DN1_POOLER_PORT || exit 1
    check_port $DN2_PORT || exit 1
    check_port $DN2_POOLER_PORT || exit 1
    if kv_three_dns_enabled; then
        check_port $DN3_PORT || exit 1
        check_port $DN3_POOLER_PORT || exit 1
    fi
    check_port $CLIENT1_RPC_PORT || exit 1
    check_port $CLIENT2_RPC_PORT || exit 1
    log_info "All ports are available"
    
    # 3. Set environment variables
    export FALCONFS_INSTALL_DIR="$INSTALL_DIR"
    export PATH="$INSTALL_DIR/falcon_client/bin:$(pg_config --bindir):${PATH:-}"
    export LD_LIBRARY_PATH="$INSTALL_DIR/falcon_client/lib:$INSTALL_DIR/falcon_meta/lib:$(pg_config --libdir):${LD_LIBRARY_PATH:-}"
    export CONFIG_FILE="$INSTALL_DIR/falcon_client/config/config.json"
    # DN ``KVMetadataEngine`` and ``falcon_kv_store`` must use the same logical
    # KV block size so ``RegisterStoreRegion`` succeeds (see ``ResolveDnKvBlockSize``).
    export FALCON_KV_STORE_BLOCK_SIZE="${FALCON_KV_STORE_BLOCK_SIZE:-2097152}"

    local FALCON_CLIENT_BIN="$INSTALL_DIR/falcon_client/bin/falcon_client"
    if [ ! -x "$FALCON_CLIENT_BIN" ]; then
        log_error "falcon_client not found or not executable: $FALCON_CLIENT_BIN"
        log_error "Install the client (e.g. sudo ./build.sh install) or set FALCONFS_INSTALL_DIR to a prefix that contains falcon_client/bin/falcon_client"
        exit 1
    fi
    
    # 4. Install Falcon extension to PostgreSQL
    log_step "【1/6】Installing Falcon extension to PostgreSQL..."
    local pg_ext_dir="$(pg_config --sharedir)/extension"
    local pg_lib_dir="$(pg_config --pkglibdir)"
    sudo mkdir -p "$pg_ext_dir" "$pg_lib_dir"
    sudo cp -f "$INSTALL_DIR/falcon_meta/share/extension"/falcon* "$pg_ext_dir/" 2>/dev/null || true
    sudo cp -f "$INSTALL_DIR/falcon_meta/lib/postgresql"/falcon*.so "$pg_lib_dir/" 2>/dev/null || true
    sudo cp -f "$INSTALL_DIR/falcon_meta/lib/postgresql"/libbrpcplugin.so "$pg_lib_dir/" 2>/dev/null || true
    log_info "Falcon extension installed"
    
    # 5. Initialize and start CN (Coordinator - server_id=0)
    log_step "【2/6】Starting CN Coordinator (port: $CN_PORT)..."
    local cn_path="$META_DB_DIR/coordinator0"
    mkdir -p "$cn_path"
    
    if [ ! -d "$cn_path/PG_VERSION" ]; then
        initdb -D "$cn_path" --username=$USER
    fi
    
    local comm_plugin_path="$INSTALL_DIR/falcon_meta/lib/postgresql/libbrpcplugin.so"
    cat >"$cn_path/postgresql.conf" <<EOF
shared_preload_libraries = 'falcon'
port=$CN_PORT
listen_addresses = '*'
wal_level = logical
max_prepared_transactions = 100
max_replication_slots = 8
max_wal_senders = 8
falcon_connection_pool.port = $CN_POOLER_PORT
falcon_connection_pool.pool_size = 32
falcon_connection_pool.shmem_size = $FALCON_POOL_SHMEM_MB
falcon_connection_pool.batch_size = 1024
falcon_connection_pool.wait_adjust = 1
falcon_connection_pool.wait_min = 1
falcon_connection_pool.wait_max = 500
falcon_communication.plugin_path = '$comm_plugin_path'
falcon_communication.server_ip = '127.0.0.1'
falcon_plugin.directory = '$INSTALL_DIR/plugins'
falcon.local_ip = '127.0.0.1'
falcon.perf_enabled = on
EOF
    echo "host all all 0.0.0.0/0 trust" >>"$cn_path/pg_hba.conf"
    
    pg_ctl start -l "/tmp/falcon_cn.log" -D "$cn_path" -c
    sleep 3
    
    # Verify CN is running
    if ! pg_ctl status -D "$cn_path" >/dev/null 2>&1; then
        log_error "CN failed to start. Check log: /tmp/falcon_cn.log"
        tail -30 /tmp/falcon_cn.log
        exit 1
    fi
    
    psql -d postgres -h 127.0.0.1 -p $CN_PORT -c "CREATE EXTENSION IF NOT EXISTS falcon;"
    log_info "CN started successfully"
    
    # 6. Initialize and start DN1 (Worker - server_id=1)
    log_step "【3/8】Starting DN1 Worker (port: $DN1_PORT)..."
    local dn1_path="$META_DB_DIR/worker0"
    mkdir -p "$dn1_path"
    
    if [ ! -d "$dn1_path/PG_VERSION" ]; then
        initdb -D "$dn1_path" --username=$USER
    fi
    
    cat >"$dn1_path/postgresql.conf" <<EOF
shared_preload_libraries = 'falcon'
port=$DN1_PORT
listen_addresses = '*'
wal_level = logical
max_prepared_transactions = 100
max_replication_slots = 8
max_wal_senders = 8
falcon_connection_pool.port = $DN1_POOLER_PORT
falcon_connection_pool.pool_size = 32
falcon_connection_pool.shmem_size = $FALCON_POOL_SHMEM_MB
falcon_connection_pool.batch_size = 1024
falcon_connection_pool.wait_adjust = 1
falcon_connection_pool.wait_min = 1
falcon_connection_pool.wait_max = 500
falcon_communication.plugin_path = '$comm_plugin_path'
falcon_communication.server_ip = '127.0.0.1'
falcon_plugin.directory = '$INSTALL_DIR/plugins'
falcon.local_ip = '127.0.0.1'
falcon.perf_enabled = on
falcon_kv.store_spill_endpoint = '127.0.0.1:${KV_STORE_BRPC_PORT}'
EOF
    echo "host all all 0.0.0.0/0 trust" >>"$dn1_path/pg_hba.conf"
    
    pg_ctl start -l "/tmp/falcon_dn1.log" -D "$dn1_path" -c
    sleep 3
    
    # Verify DN1 is running
    if ! pg_ctl status -D "$dn1_path" >/dev/null 2>&1; then
        log_error "DN1 failed to start. Check log: /tmp/falcon_dn1.log"
        tail -30 /tmp/falcon_dn1.log
        exit 1
    fi
    
    psql -d postgres -h 127.0.0.1 -p $DN1_PORT -c "CREATE EXTENSION IF NOT EXISTS falcon;"
    log_info "DN1 started successfully"
    
    # 7. Initialize and start DN2 (Worker - server_id=2)
    log_step "【4/8】Starting DN2 Worker (port: $DN2_PORT)..."
    local dn2_path="$META_DB_DIR/worker1"
    mkdir -p "$dn2_path"
    
    if [ ! -d "$dn2_path/PG_VERSION" ]; then
        initdb -D "$dn2_path" --username=$USER
    fi
    
    cat >"$dn2_path/postgresql.conf" <<EOF
shared_preload_libraries = 'falcon'
port=$DN2_PORT
listen_addresses = '*'
wal_level = logical
max_prepared_transactions = 100
max_replication_slots = 8
max_wal_senders = 8
falcon_connection_pool.port = $DN2_POOLER_PORT
falcon_connection_pool.pool_size = 32
falcon_connection_pool.shmem_size = $FALCON_POOL_SHMEM_MB
falcon_connection_pool.batch_size = 1024
falcon_connection_pool.wait_adjust = 1
falcon_connection_pool.wait_min = 1
falcon_connection_pool.wait_max = 500
falcon_communication.plugin_path = '$comm_plugin_path'
falcon_communication.server_ip = '127.0.0.1'
falcon_plugin.directory = '$INSTALL_DIR/plugins'
falcon.local_ip = '127.0.0.1'
falcon.perf_enabled = on
falcon_kv.store_spill_endpoint = '127.0.0.1:${KV_STORE_BRPC_PORT}'
EOF
    echo "host all all 0.0.0.0/0 trust" >>"$dn2_path/pg_hba.conf"
    
    pg_ctl start -l "/tmp/falcon_dn2.log" -D "$dn2_path" -c
    sleep 3
    
    # Verify DN2 is running
    if ! pg_ctl status -D "$dn2_path" >/dev/null 2>&1; then
        log_error "DN2 failed to start. Check log: /tmp/falcon_dn2.log"
        tail -30 /tmp/falcon_dn2.log
        exit 1
    fi
    
    psql -d postgres -h 127.0.0.1 -p $DN2_PORT -c "CREATE EXTENSION IF NOT EXISTS falcon;"
    log_info "DN2 started successfully"

    if kv_three_dns_enabled; then
        log_step "【4b/8】Starting DN3 Worker (port: $DN3_PORT)..."
        local dn3_path="$META_DB_DIR/worker2"
        mkdir -p "$dn3_path"

        if [ ! -d "$dn3_path/PG_VERSION" ]; then
            initdb -D "$dn3_path" --username=$USER
        fi

        cat >"$dn3_path/postgresql.conf" <<EOF
shared_preload_libraries = 'falcon'
port=$DN3_PORT
listen_addresses = '*'
wal_level = logical
max_prepared_transactions = 100
max_replication_slots = 8
max_wal_senders = 8
falcon_connection_pool.port = $DN3_POOLER_PORT
falcon_connection_pool.pool_size = 32
falcon_connection_pool.shmem_size = $FALCON_POOL_SHMEM_MB
falcon_connection_pool.batch_size = 1024
falcon_connection_pool.wait_adjust = 1
falcon_connection_pool.wait_min = 1
falcon_connection_pool.wait_max = 500
falcon_communication.plugin_path = '$comm_plugin_path'
falcon_communication.server_ip = '127.0.0.1'
falcon_plugin.directory = '$INSTALL_DIR/plugins'
falcon.local_ip = '127.0.0.1'
falcon.perf_enabled = on
falcon_kv.store_spill_endpoint = '127.0.0.1:${KV_STORE_BRPC_PORT}'
EOF
        echo "host all all 0.0.0.0/0 trust" >>"$dn3_path/pg_hba.conf"

        pg_ctl start -l "/tmp/falcon_dn3.log" -D "$dn3_path" -c
        sleep 3

        if ! pg_ctl status -D "$dn3_path" >/dev/null 2>&1; then
            log_error "DN3 failed to start. Check log: /tmp/falcon_dn3.log"
            tail -30 /tmp/falcon_dn3.log
            exit 1
        fi

        psql -d postgres -h 127.0.0.1 -p $DN3_PORT -c "CREATE EXTENSION IF NOT EXISTS falcon;"
        log_info "DN3 started successfully"
    fi
    
    # 8. Register servers in the cluster
    # IMPORTANT: Register on ALL nodes so they all know each other
    log_step "【5/8】Registering servers to cluster..."

    local node_ports=( "$CN_PORT" "$DN1_PORT" "$DN2_PORT" )
    if kv_three_dns_enabled; then
        node_ports+=( "$DN3_PORT" )
    fi

    for target_port in "${node_ports[@]}"; do
        local cn_local=false
        local dn1_local=false
        local dn2_local=false
        local dn3_local=false

        if [ "$target_port" -eq "$CN_PORT" ]; then
            cn_local=true
        elif [ "$target_port" -eq "$DN1_PORT" ]; then
            dn1_local=true
        elif [ "$target_port" -eq "$DN2_PORT" ]; then
            dn2_local=true
        elif kv_three_dns_enabled && [ "$target_port" -eq "$DN3_PORT" ]; then
            dn3_local=true
        fi

        psql -d postgres -h 127.0.0.1 -p "$target_port" \
            -c "SELECT falcon_insert_foreign_server(0, 'cn0', '127.0.0.1', $CN_PORT, $cn_local, '$USER');"
        psql -d postgres -h 127.0.0.1 -p "$target_port" \
            -c "SELECT falcon_insert_foreign_server(1, 'worker0', '127.0.0.1', $DN1_PORT, $dn1_local, '$USER');"
        psql -d postgres -h 127.0.0.1 -p "$target_port" \
            -c "SELECT falcon_insert_foreign_server(2, 'worker1', '127.0.0.1', $DN2_PORT, $dn2_local, '$USER');"
        if kv_three_dns_enabled; then
            psql -d postgres -h 127.0.0.1 -p "$target_port" \
                -c "SELECT falcon_insert_foreign_server(3, 'worker2', '127.0.0.1', $DN3_PORT, $dn3_local, '$USER');"
        fi
    done
    
    log_info "Servers registered"
    
    # 9. Build shard table and initialize services
    log_step "【6/8】Building shard table and initializing services..."
    
    # Build shard table on all nodes (distributes shards across workers)
    psql -d postgres -h 127.0.0.1 -p $CN_PORT -c "SELECT falcon_build_shard_table(50);"
    psql -d postgres -h 127.0.0.1 -p $DN1_PORT -c "SELECT falcon_build_shard_table(50);"
    psql -d postgres -h 127.0.0.1 -p $DN2_PORT -c "SELECT falcon_build_shard_table(50);"
    if kv_three_dns_enabled; then
        psql -d postgres -h 127.0.0.1 -p $DN3_PORT -c "SELECT falcon_build_shard_table(50);"
    fi
    
    # Create tables and start background services on all nodes.
    # Membership tables MUST exist before falcon_start_background_service: DN BRPC
    # self-registration inserts into pg_catalog.falcon_dn_node at startup.
    local init_ports="$CN_PORT $DN1_PORT $DN2_PORT"
    if kv_three_dns_enabled; then
        init_ports="$init_ports $DN3_PORT"
    fi
    for port in $init_ports; do
        psql -d postgres -h 127.0.0.1 -p $port <<EOF
SELECT falcon_create_distributed_data_table();
SELECT falcon_create_slice_table();
SELECT falcon_create_kvmeta_table();
SELECT pg_catalog.falcon_create_kv_membership_tables();
SELECT falcon_start_background_service();
EOF
    done

    sleep 3

    local pooler_ports=("$CN_POOLER_PORT" "$DN1_POOLER_PORT" "$DN2_POOLER_PORT")
    if kv_three_dns_enabled; then
        pooler_ports+=("$DN3_POOLER_PORT")
    fi
    for pooler_port in "${pooler_ports[@]}"; do
        if ! wait_for_listen_tcp "$pooler_port" 45; then
            log_error "Falcon BRPC pooler did not listen on TCP port $pooler_port within 45s"
            exit 1
        fi
    done

    mirror_kv_dn_membership_rows_to_cn || {
        log_error "CN falcon_dn_node seed failed"
        exit 1
    }

    # Create root directory on CN only
    psql -d postgres -h 127.0.0.1 -p $CN_PORT -c "SELECT falcon_plain_mkdir('/');"
    
    log_info "Cluster initialization completed"
    sleep 2

    # 9b. v6.5 P2: standalone KV store daemon — DRAM + KVDataService; DNs are metadata-only.
    KV_STORE_BRPC_PORT="${KV_STORE_BRPC_PORT:-18765}"
    STORE_COUNT="${STORE_COUNT:-1}"
    rm -f /tmp/falcon_kv_store.pids /tmp/falcon_kv_store_env.sh
    if [ "${STORE_COUNT}" -gt 0 ]; then
        local kv_store_bin="$PROJECT_DIR/build/vllm_kv_cache/falcon_kv_store"
        if [ ! -x "$kv_store_bin" ]; then
            log_step "falcon_kv_store binary missing; building..."
            (cd "$PROJECT_DIR/build" && ninja falcon_kv_store)
        fi
        for ((si=0; si<STORE_COUNT; si++)); do
            local nid=$((si + 1))
            mkdir -p "/tmp/falcon_kv_store_runtime/n${nid}" 2>/dev/null || true
        done
        local store_poolers
        store_poolers="$(kv_store_poolers_csv)"
        if [ -z "${FALCON_KV_STORE_DRAM_BYTES:-}" ]; then
            local __blk="${FALCON_KV_STORE_BLOCK_SIZE:-2097152}"
            local __min_slots="${FALCON_KV_STORE_MIN_LOGICAL_SLOTS:-2048}"
            export FALCON_KV_STORE_DRAM_BYTES=$(( __blk * __min_slots ))
            log_info "FALCON_KV_STORE_DRAM_BYTES unset → ${__min_slots} logical slots × ${__blk} B = ${FALCON_KV_STORE_DRAM_BYTES} B (set FALCON_KV_STORE_DRAM_BYTES or FALCON_KV_STORE_MIN_LOGICAL_SLOTS to override)"
        fi
        # Start one store at a time: each process registers on the CN then issues
        # RegisterStoreRegion to every DN. Parallel startups race on DN-side meta
        # and can cause later stores to unregister (leaving a single falcon_store_node row).
        for ((si=0; si<STORE_COUNT; si++)); do
            local p=$((KV_STORE_BRPC_PORT + si))
            local node_id=$((si + 1))
            local env_prefix=()
            if kv_three_dns_enabled; then
                env_prefix+=(NODE_NAME="v65mix${si}")
            fi
            nohup env "${env_prefix[@]}" FALCON_KV_STORE_CN_PGPORT="$CN_PORT" \
                FALCON_KV_STORE_BRPC_PORT="$p" \
                FALCON_KV_STORE_DN_POOLERS="$store_poolers" \
                FALCON_KV_STORE_NODE_ID="$node_id" \
                FALCON_KV_STORE_BLOCK_SIZE="${FALCON_KV_STORE_BLOCK_SIZE:-2097152}" \
                FALCON_KV_STORE_DRAM_BYTES="${FALCON_KV_STORE_DRAM_BYTES:-}" \
                FALCON_KV_STORE_ADVERTISE_HOST="127.0.0.1" \
                FALCON_KV_STORE_SHM_NAME="falcon_kv_store_heap_${node_id}" \
                FALCON_KV_STORE_RUNTIME_DIR="/tmp/falcon_kv_store_runtime/n${node_id}" \
                "$kv_store_bin" >>"/tmp/falcon_kv_store_${si}.log" 2>&1 &
            echo $! >> /tmp/falcon_kv_store.pids
            if ! wait_for_listen_tcp "$p" 45; then
                log_error "falcon_kv_store did not listen on TCP port $p within 45s"
                exit 1
            fi
            # falcon_kv_store registers with CN then each DN after Listen; brief pause
            # before spawning the next store avoids BRPC/meta contention on cold DNs.
            sleep 1
        done
        echo "export FALCON_KV_STORE_BRPC_ENDPOINT=127.0.0.1:${KV_STORE_BRPC_PORT}" > /tmp/falcon_kv_store_env.sh
        log_info "Started STORE_COUNT=${STORE_COUNT} falcon_kv_store (FALCON_KV_STORE_BRPC_ENDPOINT=127.0.0.1:${KV_STORE_BRPC_PORT})"
    fi
    
    # 10. Start Client1 and Client2 with full cluster view
    # Note: Both clients will attempt to connect to each other in cluster_view
    # Connection failures are acceptable during initial startup
    log_step "【7/8】Starting Client1 (mount: $CLIENT1_MNT, RPC: $CLIENT1_RPC_PORT)..."
    mkdir -p "$CLIENT1_MNT" "$CLIENT1_CACHE"
    for i in {0..100}; do mkdir -p "$CLIENT1_CACHE/$i" 2>/dev/null || true; done
    
    # Create client-specific config with full cluster view
    cat > /tmp/falcon_client1_config.json << EOF
{
    "main": {
        "falcon_log_dir": "/tmp",
        "falcon_log_level": "INFO",
        "falcon_log_max_size_mb": 10,
        "falcon_cache_root": "$CLIENT1_CACHE",
        "falcon_dir_num": 101,
        "falcon_block_size": 524288,
        "falcon_read_big_file_size": 2097152,
        "falcon_preblock_num": 1000,
        "falcon_max_open_num": 0,
        "falcon_node_id": 0,
        "falcon_cluster_view": ["127.0.0.1:$CLIENT1_RPC_PORT", "127.0.0.1:$CLIENT2_RPC_PORT"],
        "falcon_thread_num": 50,
        "falcon_server_ip": "127.0.0.1",
        "falcon_server_port": "$CN_POOLER_PORT",
        "falcon_async": false,
        "falcon_persist": false,
        "falcon_eviction": 0.1,
        "falcon_is_inference": false,
        "falcon_mount_path": "$CLIENT1_MNT",
        "falcon_to_local": true,
        "falcon_log_reserved_num": 3,
        "falcon_log_reserved_time": 1,
        "falcon_stat_max": true,
        "falcon_use_prometheus": false,
        "falcon_prometheus_port": "50040"
    }
}
EOF
    
    nohup env CONFIG_FILE=/tmp/falcon_client1_config.json "$FALCON_CLIENT_BIN" "$CLIENT1_MNT" -f -o direct_io -o attr_timeout=200 -o entry_timeout=200 \
        -brpc true -rpc_endpoint="0.0.0.0:${CLIENT1_RPC_PORT}" \
        -socket_max_unwritten_bytes=268435456 > "$CLIENT1_LOG" 2>&1 &
    CLIENT1_PID=$!
    sleep 5
    
    # 11. Start Client2  
    log_step "【8/8】Starting Client2 (mount: $CLIENT2_MNT, RPC: $CLIENT2_RPC_PORT)..."
    mkdir -p "$CLIENT2_MNT" "$CLIENT2_CACHE"
    for i in {0..100}; do mkdir -p "$CLIENT2_CACHE/$i" 2>/dev/null || true; done
    
    # Create client-specific config with full cluster view
    cat > /tmp/falcon_client2_config.json << EOF
{
    "main": {
        "falcon_log_dir": "/tmp",
        "falcon_log_level": "INFO",
        "falcon_log_max_size_mb": 10,
        "falcon_cache_root": "$CLIENT2_CACHE",
        "falcon_dir_num": 101,
        "falcon_block_size": 524288,
        "falcon_read_big_file_size": 2097152,
        "falcon_preblock_num": 1000,
        "falcon_max_open_num": 0,
        "falcon_node_id": 1,
        "falcon_cluster_view": ["127.0.0.1:$CLIENT1_RPC_PORT", "127.0.0.1:$CLIENT2_RPC_PORT"],
        "falcon_thread_num": 50,
        "falcon_server_ip": "127.0.0.1",
        "falcon_server_port": "$CN_POOLER_PORT",
        "falcon_async": false,
        "falcon_persist": false,
        "falcon_eviction": 0.1,
        "falcon_is_inference": false,
        "falcon_mount_path": "$CLIENT2_MNT",
        "falcon_to_local": true,
        "falcon_log_reserved_num": 3,
        "falcon_log_reserved_time": 1,
        "falcon_stat_max": true,
        "falcon_use_prometheus": false,
        "falcon_prometheus_port": "50041"
    }
}
EOF
    
    # Wait before starting Client2
    sleep 2
    
    nohup env CONFIG_FILE=/tmp/falcon_client2_config.json "$FALCON_CLIENT_BIN" "$CLIENT2_MNT" -f -o direct_io -o attr_timeout=200 -o entry_timeout=200 \
        -brpc true -rpc_endpoint="0.0.0.0:${CLIENT2_RPC_PORT}" \
        -socket_max_unwritten_bytes=268435456 > "$CLIENT2_LOG" 2>&1 &
    CLIENT2_PID=$!
    sleep 8

    if ! wait_for_fuse_mount "$CLIENT1_MNT" 90; then
        log_error "Client1 FUSE mount did not appear at $CLIENT1_MNT within 90s (pid=$CLIENT1_PID)"
        tail -80 "$CLIENT1_LOG" 2>/dev/null || true
        exit 1
    fi
    if ! wait_for_fuse_mount "$CLIENT2_MNT" 90; then
        log_error "Client2 FUSE mount did not appear at $CLIENT2_MNT within 90s (pid=$CLIENT2_PID)"
        tail -80 "$CLIENT2_LOG" 2>/dev/null || true
        exit 1
    fi
    log_info "FUSE mounts verified: $CLIENT1_MNT $CLIENT2_MNT"
    
    # 12. Verify cluster status
    echo ""
    log_step "========================================="
    log_info "✅ FalconFS cluster started successfully!"
    log_step "========================================="
    echo ""
    log_info "Cluster Topology:"
    log_info "  CN (Coordinator): 127.0.0.1:$CN_PORT (Pooler: $CN_POOLER_PORT)"
    log_info "  DN1 (Worker):     127.0.0.1:$DN1_PORT (Pooler: $DN1_POOLER_PORT)"
    log_info "  DN2 (Worker):     127.0.0.1:$DN2_PORT (Pooler: $DN2_POOLER_PORT)"
    if kv_three_dns_enabled; then
        log_info "  DN3 (Worker):     127.0.0.1:$DN3_PORT (Pooler: $DN3_POOLER_PORT)"
    fi
    log_info "  Client1:          Mount: $CLIENT1_MNT (RPC: $CLIENT1_RPC_PORT)"
    log_info "  Client2:          Mount: $CLIENT2_MNT (RPC: $CLIENT2_RPC_PORT)"
    echo ""
    log_info "Test Commands:"
    log_info "  # Test Client1"
    log_info "  echo 'test from client1' > $CLIENT1_MNT/test1.txt"
    log_info "  cat $CLIENT1_MNT/test1.txt"
    echo ""
    log_info "  # Test Client2"
    log_info "  echo 'test from client2' > $CLIENT2_MNT/test2.txt"
    log_info "  cat $CLIENT2_MNT/test2.txt"
    echo ""
    log_info "  # Cross-client access (verify distributed consistency)"
    log_info "  ls -la $CLIENT1_MNT/"
    log_info "  ls -la $CLIENT2_MNT/"
    echo ""
    log_info "Stop Command:"
    log_info "  $0 stop"
}

# ============================================================================
# Stop Services
# ============================================================================

stop_all() {
    log_step "Stopping FalconFS cluster..."

    if [ -f /tmp/falcon_kv_store.pids ]; then
        log_step "Stopping falcon_kv_store daemon(s)..."
        while read -r pid; do
            if [ -n "${pid}" ] && kill -0 "${pid}" 2>/dev/null; then
                kill -TERM "${pid}" 2>/dev/null || true
            fi
        done < /tmp/falcon_kv_store.pids
        sleep 1
        while read -r pid; do
            if [ -n "${pid}" ] && kill -0 "${pid}" 2>/dev/null; then
                kill -KILL "${pid}" 2>/dev/null || true
            fi
        done < /tmp/falcon_kv_store.pids
        rm -f /tmp/falcon_kv_store.pids
    fi
    pkill -TERM falcon_kv_store 2>/dev/null || true
    sleep 1
    pkill -KILL falcon_kv_store 2>/dev/null || true
    KV_STORE_BRPC_PORT="${KV_STORE_BRPC_PORT:-18765}"
    STORE_COUNT="${STORE_COUNT:-1}"
    for ((si=0; si<STORE_COUNT; si++)); do
        kill_listener_on_port "$((KV_STORE_BRPC_PORT + si))" "KV store BRPC" || true
    done
    rm -f /tmp/falcon_kv_store_env.sh
    
    # Stop Clients (unmount FUSE)
    for mnt in "$CLIENT1_MNT" "$CLIENT2_MNT"; do
        if mount | grep -q "$mnt"; then
            sudo umount -l "$mnt" 2>/dev/null || true
            log_info "Unmounted $mnt"
        fi
    done
    
    pkill -9 falcon_client 2>/dev/null || true
    
    # Stop Meta (PostgreSQL)
    stop_pg_node "CN" "$META_DB_DIR/coordinator0" || true
    stop_pg_node "DN1" "$META_DB_DIR/worker0" || true
    stop_pg_node "DN2" "$META_DB_DIR/worker1" || true
    stop_pg_node "DN3" "$META_DB_DIR/worker2" || true
    kill_listener_on_port "$CN_PORT" "CN SQL" || true
    kill_listener_on_port "$CN_POOLER_PORT" "CN pooler" || true
    kill_listener_on_port "$DN1_PORT" "DN1 SQL" || true
    kill_listener_on_port "$DN1_POOLER_PORT" "DN1 pooler" || true
    kill_listener_on_port "$DN2_PORT" "DN2 SQL" || true
    kill_listener_on_port "$DN2_POOLER_PORT" "DN2 pooler" || true
    kill_listener_on_port "$DN3_PORT" "DN3 SQL" || true
    kill_listener_on_port "$DN3_POOLER_PORT" "DN3 pooler" || true
    
    sleep 2
    log_info "All services stopped"
}

# ============================================================================
# Show Status
# ============================================================================

show_status() {
    echo "========================================="
    echo " FalconFS Cluster Status"
    echo "========================================="
    echo ""
    
    # Meta status
    echo "【Meta Services】"
    if pg_ctl status -D "$META_DB_DIR/coordinator0" >/dev/null 2>&1; then
        log_info "CN (Coordinator) running (port: $CN_PORT)"
    else
        log_error "CN not running"
    fi
    
    if pg_ctl status -D "$META_DB_DIR/worker0" >/dev/null 2>&1; then
        log_info "DN1 (Worker) running (port: $DN1_PORT)"
    else
        log_error "DN1 not running"
    fi

    if pg_ctl status -D "$META_DB_DIR/worker1" >/dev/null 2>&1; then
        log_info "DN2 (Worker) running (port: $DN2_PORT)"
    else
        log_error "DN2 not running"
    fi

    if kv_three_dns_enabled; then
        if pg_ctl status -D "$META_DB_DIR/worker2" >/dev/null 2>&1; then
            log_info "DN3 (Worker) running (port: $DN3_PORT)"
        else
            log_error "DN3 not running"
        fi
    fi
    echo ""
    
    # Client status
    echo "【Client Nodes】"
    if mount | grep -q "$CLIENT1_MNT"; then
        log_info "Client1 mounted: $CLIENT1_MNT"
    else
        log_error "Client1 not mounted"
    fi
    
    if mount | grep -q "$CLIENT2_MNT"; then
        log_info "Client2 mounted: $CLIENT2_MNT"
    else
        log_error "Client2 not mounted"
    fi
    echo ""
    
    # Port listening
    echo "【Port Listening】"
    local port_grep="${CN_PORT}|${DN1_PORT}|${DN2_PORT}|${CLIENT1_RPC_PORT}|${CLIENT2_RPC_PORT}"
    if kv_three_dns_enabled; then
        port_grep="${port_grep}|${DN3_PORT}|${DN3_POOLER_PORT}"
    fi
    ss -tuln 2>/dev/null | grep -E "(${port_grep})" || \
        log_warn "Cannot check port status"
}

# ============================================================================
# Run Tests
# ============================================================================

run_test() {
    log_step "Running distributed tests..."
    
    # Check if mounts are ready
    if ! mount | grep -q "$CLIENT1_MNT" || ! mount | grep -q "$CLIENT2_MNT"; then
        log_error "Clients not mounted. Please run '$0 start' first"
        exit 1
    fi
    
    # Test 1: Client1 write
    log_step "【Test 1】Client1 writing file..."
    echo "Write time: $(date)" > "$CLIENT1_MNT/test_client1.txt"
    echo "Test content: Hello from Client1" >> "$CLIENT1_MNT/test_client1.txt"
    log_info "Client1 write successful"
    
    # Test 2: Client2 write
    log_step "【Test 2】Client2 writing file..."
    echo "Write time: $(date)" > "$CLIENT2_MNT/test_client2.txt"
    echo "Test content: Hello from Client2" >> "$CLIENT2_MNT/test_client2.txt"
    log_info "Client2 write successful"
    
    # Test 3: Client1 read its own file
    log_step "【Test 3】Client1 reading own file..."
    cat "$CLIENT1_MNT/test_client1.txt"
    log_info "Client1 read successful"
    
    # Test 4: Client2 read its own file
    log_step "【Test 4】Client2 reading own file..."
    cat "$CLIENT2_MNT/test_client2.txt"
    log_info "Client2 read successful"
    
    # Test 5: Cross-client visibility
    log_step "【Test 5】Checking cross-client file visibility..."
    echo "Client1 mount content:"
    ls -la "$CLIENT1_MNT/" | head -10
    echo ""
    echo "Client2 mount content:"
    ls -la "$CLIENT2_MNT/" | head -10
    
    # Test 6: Large file write
    log_step "【Test 6】Large file write test (10MB)..."
    dd if=/dev/urandom of="$CLIENT1_MNT/large_file.bin" bs=1M count=10 2>&1 | tail -1
    ls -lh "$CLIENT1_MNT/large_file.bin"
    log_info "Large file write successful"
    
    # Test 7: Concurrent write
    log_step "【Test 7】Concurrent write test..."
    for i in {1..5}; do
        echo "Concurrent test $i from Client1" > "$CLIENT1_MNT/concurrent_$i.txt" &
        echo "Concurrent test $i from Client2" > "$CLIENT2_MNT/concurrent_$i.txt" &
    done
    wait
    log_info "Concurrent write completed"
    
    # Cleanup test files
    log_step "Cleaning up test files..."
    rm -f "$CLIENT1_MNT"/test_*.txt "$CLIENT2_MNT"/test_*.txt
    rm -f "$CLIENT1_MNT"/large_file.bin "$CLIENT1_MNT"/concurrent_*.txt
    rm -f "$CLIENT2_MNT"/concurrent_*.txt
    
    echo ""
    log_step "========================================="
    log_info "✅ All tests passed!"
    log_step "========================================="
}

run_kv_test() {
    log_step "Running FalconFS KV cache standalone tests..."

    export PYTHONPATH="$PROJECT_DIR/vllm_kv_cache/python:${PYTHONPATH:-}"

    python3 "$PROJECT_DIR/vllm_kv_cache/test/test_reference_unit.py"
    python3 "$PROJECT_DIR/vllm_kv_cache/test/test_metadata_reference.py"
    python3 "$PROJECT_DIR/vllm_kv_cache/test/test_store_reference.py"
    python3 "$PROJECT_DIR/vllm_kv_cache/test/test_brpc_contract_reference.py"

    log_info "KV standalone tests passed"
}

run_kv_fault_test() {
    log_step "Running FalconFS KV cache fault tests..."

    export PYTHONPATH="$PROJECT_DIR/vllm_kv_cache/python:${PYTHONPATH:-}"
    python3 "$PROJECT_DIR/vllm_kv_cache/test/test_kv_fault_reference.py"

    log_info "KV fault tests passed"
}

# ============================================================================
# KV Metadata End-to-End Stress Test
# ============================================================================
# Drives the FalconFS BRPC metadata service against the running cluster with
# varying concurrent clients and batch sizes to validate system correctness
# (allocation uniqueness, CAS version correctness, lookup/renew/free flow,
# concurrent dedup, and bitmap+catalog cleanup between sweeps).

run_kv_meta_stress_test() {
    log_step "Running FalconFS KV metadata end-to-end stress tests..."

    local stress_bin="$PROJECT_DIR/build/tests/falcon_kv/FalconKVMetadataStressE2E"
    if [ ! -x "$stress_bin" ]; then
        log_step "Stress binary missing; building it now..."
        (cd "$PROJECT_DIR/build" && ninja FalconKVMetadataStressE2E)
    fi

    local endpoints=("127.0.0.1:$DN1_POOLER_PORT" "127.0.0.1:$DN2_POOLER_PORT")
    if kv_three_dns_enabled; then
        endpoints+=("127.0.0.1:$DN3_POOLER_PORT")
    fi
    local rc=0
    for ep in "${endpoints[@]}"; do
        log_step "  KV stress sweep on $ep"
        if ! "$stress_bin" --endpoint "$ep" --iterations 1 --dedup-clients 8; then
            log_step "  KV stress sweep FAILED on $ep"
            rc=1
        fi
    done

    # Verify catalog cleanup on every DN: every kvblock_table row created by
    # the sweep must have been DELETEd by the test's force-free phase. The
    # stress test uses block hashes prefixed with "stress_" so we only count
    # those.
    local cleanup_rc=0
    local stress_ports=( "$DN1_PORT" "$DN2_PORT" )
    if kv_three_dns_enabled; then
        stress_ports+=( "$DN3_PORT" )
    fi
    for port in "${stress_ports[@]}"; do
        local rows
        rows=$(psql -d postgres -h 127.0.0.1 -p "$port" -tAXc \
            "SELECT count(*) FROM pg_catalog.falcon_kvblock_table WHERE block_hash LIKE 'stress_%';" 2>/dev/null \
            || echo "ERR")
        if [ "$rows" != "0" ]; then
            log_step "  Catalog cleanup FAILED on DN port=$port (residual rows=$rows)"
            cleanup_rc=1
        else
            log_info "  Catalog clean on DN port=$port"
        fi
    done

    if [ $rc -ne 0 ] || [ $cleanup_rc -ne 0 ]; then
        log_step "KV metadata stress tests FAILED"
        return 1
    fi
    log_info "KV metadata stress tests passed"
}

# ============================================================================
# KV Cluster Fault Drill (v6 §19.4 #3/#4/#5/#7)
# ============================================================================
# Drives FalconKVClusterFaultE2E through every fault scenario. The DN-restart
# drill (#3) is split across two test invocations with a real
# `pg_ctl restart -m immediate` for DN1 in between.

# Wait for a port to accept TCP connections, with a configurable timeout.
wait_for_port() {
    local port=$1
    local timeout_s=${2:-30}
    local i=0
    while [ "$i" -lt "$timeout_s" ]; do
        if psql -d postgres -h 127.0.0.1 -p "$port" -tAXc 'SELECT 1;' >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
        i=$((i + 1))
    done
    return 1
}

run_kv_cluster_fault_test() {
    log_step "Running FalconFS KV cluster fault drill (§19.4 #3/#4/#5/#7)..."

    local fault_bin="$PROJECT_DIR/build/tests/falcon_kv/FalconKVClusterFaultE2E"
    if [ ! -x "$fault_bin" ]; then
        log_step "Fault binary missing; building it now..."
        (cd "$PROJECT_DIR/build" && ninja FalconKVClusterFaultE2E)
    fi

    local rc=0

    # T7 large batch (§19.4 #7).
    log_step "  [T7] large batch chunking"
    if ! "$fault_bin" --scenario=large-batch --endpoint "127.0.0.1:$DN1_POOLER_PORT"; then
        log_step "  [T7] FAILED"; rc=1
    fi

    # T4 stale store_epoch (§19.4 #4).
    log_step "  [T4] stale store_epoch on RenewLease"
    if ! "$fault_bin" --scenario=stale-store-epoch --endpoint "127.0.0.1:$DN1_POOLER_PORT"; then
        log_step "  [T4] FAILED"; rc=1
    fi

    # T8 partial store write cleanup (v6.5 P8).
    log_step "  [T8] partial store write cleanup"
    if ! "$fault_bin" --scenario=partial-store-write --endpoint "127.0.0.1:$DN1_POOLER_PORT"; then
        log_step "  [T8] FAILED"; rc=1
    fi

    # T5 eviction rollback (§19.4 #5). Spill always fails (no SSD root
    # configured); the eviction worker must roll the row back to STORED.
    log_step "  [T5] eviction rollback"
    if ! "$fault_bin" --scenario=eviction-rollback --endpoint "127.0.0.1:$DN1_POOLER_PORT" \
         --wait-ms 2000; then
        log_step "  [T5] FAILED"; rc=1
    fi

    # T3 DN restart drill (§19.4 #3).
    log_step "  [T3] DN1 restart drill — phase 1 (alloc + dump state)"
    local state_file="/tmp/falcon_kv_cluster_fault_state.$$.txt"
    if ! "$fault_bin" --scenario=dn-restart-phase1 --endpoint "127.0.0.1:$DN1_POOLER_PORT" \
         --state-file "$state_file"; then
        log_step "  [T3] FAILED at phase 1"; rc=1; rm -f "$state_file"
    else
        log_step "  [T3] restarting DN1 PG (immediate mode, then poll)"
        # `pg_ctl -m immediate -w` can hang waiting on Unix-socket
        # readiness probe in TCP-only setups; use -W and poll the TCP port.
        pg_ctl restart -D "$META_DB_DIR/worker0" -m immediate -W >/dev/null 2>&1 || true
        if ! wait_for_port "$DN1_PORT" 60; then
            log_step "  [T3] DN1 did not come back up within 60s"; rc=1
        else
            log_step "  [T3] DN1 restarted; phase 2 (verify recovery + epoch fence)"
            # Give the in-plugin recovery runner a moment to bump dn_epoch
            # and rehydrate the engine before BRPC clients hit it.
            sleep 3
            # v6 §15.1: DN restart recovery assumes Store DRAM is intact. The
            # live Store daemon periodically re-issues RegisterStoreRegion; wait
            # for that re-registration instead of restarting the Store (which
            # would turn this into the §15.2 Store-restart/lost-DRAM case).
            if [ "${STORE_COUNT:-1}" -gt 0 ]; then
                log_step "  [T3] waiting for live falcon_kv_store region re-registration"
                sleep 12
            fi
            if ! "$fault_bin" --scenario=dn-restart-phase2 --endpoint "127.0.0.1:$DN1_POOLER_PORT" \
                 --state-file "$state_file"; then
                log_step "  [T3] FAILED at phase 2"; rc=1
            fi
        fi
        rm -f "$state_file"
    fi

    # Store restart reconciliation (§15.2). Run late because it registers a
    # synthetic Store region on the DN to isolate catalog cleanup behavior.
    log_step "  [T9] Store restart catalog reconcile"
    if ! "$fault_bin" --scenario=store-restart-reconcile --endpoint "127.0.0.1:$DN1_POOLER_PORT" \
         --pg-port "$DN1_PORT"; then
        log_step "  [T9] FAILED"; rc=1
    fi

    # Best-effort post-drill catalog cleanup (some tests intentionally leak
    # rows that they then force-free; eviction-rollback's cleanup may also
    # have raced an eviction cycle on the same row).
    local fault_del_ports=( "$DN1_PORT" "$DN2_PORT" )
    if kv_three_dns_enabled; then
        fault_del_ports+=( "$DN3_PORT" )
    fi
    for port in "${fault_del_ports[@]}"; do
        psql -d postgres -h 127.0.0.1 -p "$port" \
            -c "DELETE FROM pg_catalog.falcon_kvblock_table WHERE block_hash::text LIKE '\\\\x6661756c745f%';" \
            >/dev/null 2>&1 || true
    done

    if ! mirror_kv_dn_membership_rows_to_cn; then
        log_step "CN falcon_dn_node re-mirror after fault drill failed"
        rc=1
    fi

    if [ "$rc" -ne 0 ]; then
        log_step "KV cluster fault tests FAILED"
        return 1
    fi
    log_info "KV cluster fault tests passed"
}

# ============================================================================
# KV Cluster Failover Drill (v6.5 P6)
# ============================================================================
# Exercises CM-style endpoint switchover SQL and verifies fault drill still
# passes after endpoint rewrite + restore.
run_kv_cluster_failover_test() {
    log_step "Running FalconFS KV cluster failover drill (foreign_server + dn endpoint update)..."

    local rc=0

    # Save current DN1 endpoint.
    local old_host old_port old_kv_port
    old_host=$(psql -d postgres -h 127.0.0.1 -p "$CN_PORT" -tAXc \
        "SELECT host FROM pg_catalog.falcon_foreign_server WHERE server_id=1;" 2>/dev/null || echo "127.0.0.1")
    old_port=$(psql -d postgres -h 127.0.0.1 -p "$CN_PORT" -tAXc \
        "SELECT port FROM pg_catalog.falcon_foreign_server WHERE server_id=1;" 2>/dev/null || echo "$DN1_PORT")
    old_kv_port=$(psql -d postgres -h 127.0.0.1 -p "$CN_PORT" -tAXc \
        "SELECT kv_brpc_port FROM pg_catalog.falcon_dn_node WHERE server_id=1;" 2>/dev/null || echo "$DN1_POOLER_PORT")

    log_step "  failover step: DN1 -> DN2 endpoint"
    if ! psql -v ON_ERROR_STOP=1 -d postgres -h 127.0.0.1 -p "$CN_PORT" -c \
        "SELECT pg_catalog.falcon_update_foreign_server(1, '127.0.0.1', $DN2_PORT);" >/dev/null; then
        log_step "  failover SQL failed: falcon_update_foreign_server"; rc=1
    fi
    if ! psql -v ON_ERROR_STOP=1 -d postgres -h 127.0.0.1 -p "$CN_PORT" -c \
        "SELECT pg_catalog.falcon_dn_node_update_endpoint(1, 'worker0', '127.0.0.1', $DN2_PORT, $DN2_POOLER_PORT);" >/dev/null; then
        log_step "  failover SQL failed: falcon_dn_node_update_endpoint"; rc=1
    fi

    # Restore to original endpoint.
    log_step "  restore step: DN1 endpoint rollback"
    if ! psql -v ON_ERROR_STOP=1 -d postgres -h 127.0.0.1 -p "$CN_PORT" -c \
        "SELECT pg_catalog.falcon_update_foreign_server(1, '$old_host', $old_port);" >/dev/null; then
        log_step "  restore SQL failed: falcon_update_foreign_server"; rc=1
    fi
    if ! psql -v ON_ERROR_STOP=1 -d postgres -h 127.0.0.1 -p "$CN_PORT" -c \
        "SELECT pg_catalog.falcon_dn_node_update_endpoint(1, 'worker0', '$old_host', $old_port, $old_kv_port);" >/dev/null; then
        log_step "  restore SQL failed: falcon_dn_node_update_endpoint"; rc=1
    fi

    # Reuse the existing cluster fault drill as post-failover retry validation.
    if ! run_kv_cluster_fault_test; then
        rc=1
    fi

    if [ "$rc" -ne 0 ]; then
        log_step "KV cluster failover drill FAILED"
        return 1
    fi
    log_info "KV cluster failover drill passed"
}

# ============================================================================
# KV Distributed Cluster Test (v6 §19.4 / M4)
# ============================================================================
# Drives FalconKVClusterE2E against both DN BRPC endpoints (and optionally CN)
# to validate routing + per-DN traffic + alloc/update/lookup/renew/free
# end-to-end. Ensures both DNs receive a non-zero share of work.

run_kv_cluster_test() {
    log_step "Running FalconFS KV distributed cluster test (§19.4 / M4)..."

    local smoke_bin="$PROJECT_DIR/build/tests/falcon_kv/FalconKVP2SmokeE2E"
    if [ ! -x "$smoke_bin" ]; then
        log_step "P2 smoke binary missing; building it now..."
        (cd "$PROJECT_DIR/build" && ninja FalconKVP2SmokeE2E)
    fi

    local cluster_bin="$PROJECT_DIR/build/tests/falcon_kv/FalconKVClusterE2E"
    if [ ! -x "$cluster_bin" ]; then
        log_step "Cluster E2E binary missing; building it now..."
        (cd "$PROJECT_DIR/build" && ninja FalconKVClusterE2E)
    fi

    local rc=0

    log_step "  P2 smoke: DN must not host KVDataService"
    local smoke_dns=( "--dn" "127.0.0.1:$DN1_POOLER_PORT" "--dn" "127.0.0.1:$DN2_POOLER_PORT" )
    local cluster_dns=( "--dn" "127.0.0.1:$DN1_POOLER_PORT" "--dn" "127.0.0.1:$DN2_POOLER_PORT" )
    if kv_three_dns_enabled; then
        smoke_dns+=( "--dn" "127.0.0.1:$DN3_POOLER_PORT" )
        cluster_dns+=( "--dn" "127.0.0.1:$DN3_POOLER_PORT" )
    fi
    if ! "$smoke_bin" --mode=dn-no-kvdata "${smoke_dns[@]}"; then
        log_step "  P2 smoke (dn-no-kvdata) FAILED"; rc=1
    fi

    local store_ep="${FALCON_KV_STORE_BRPC_ENDPOINT:-127.0.0.1:${KV_STORE_BRPC_PORT:-18765}}"
    log_step "  P2 smoke: store round-trip (meta=DN1 pooler store=$store_ep)"
    if ! "$smoke_bin" --mode=store-smoke \
            --meta "127.0.0.1:$DN1_POOLER_PORT" \
            --store "$store_ep"; then
        log_step "  P2 smoke (store-smoke) FAILED"; rc=1
    fi

    log_step "  multi-DN sweep (keys=64, iterations=2)"
    if ! "$cluster_bin" "${cluster_dns[@]}" --keys 64 --iterations 2; then
        log_step "  multi-DN sweep FAILED"; rc=1
    fi

    # Verify per-DN catalog cleanup: every test row has the prefix `cluster_`.
    local cleanup_rc=0
    local catalog_ports=( "$DN1_PORT" "$DN2_PORT" )
    if kv_three_dns_enabled; then
        catalog_ports+=( "$DN3_PORT" )
    fi
    for port in "${catalog_ports[@]}"; do
        local rows
        rows=$(psql -d postgres -h 127.0.0.1 -p "$port" -tAXc \
            "SELECT count(*) FROM pg_catalog.falcon_kvblock_table WHERE encode(block_hash, 'escape') LIKE 'cluster_%';" \
            2>/dev/null || echo "ERR")
        if [ "$rows" != "0" ]; then
            log_step "  catalog cleanup FAILED on DN port=$port (residual cluster_* rows=$rows)"
            cleanup_rc=1
        else
            log_info "  catalog clean on DN port=$port"
        fi
    done

    if [ "$rc" -ne 0 ] || [ "$cleanup_rc" -ne 0 ]; then
        log_step "KV distributed cluster test FAILED"
        return 1
    fi
    log_info "KV distributed cluster test passed"
}

# ============================================================================
# KV mixed colocation drill (v6.5 §19.4.1 #4 — 1 CN + 3 DNs + 4 Stores + 4 Clients)
# ============================================================================
# Requires cluster started with KV_THREE_DNS=1 and STORE_COUNT>=4 (harness sets
# NODE_NAME=v65mix{0..3} per store and three DN poolers per store process).
run_kv_mixed_colocation_test() {
    log_step "Running FalconFS KV mixed-colocation drill (v6.5 §19.4.1 #4)..."

    if ! kv_three_dns_enabled; then
        log_error "KV_THREE_DNS=1 is required (third DN + 3-region stores)"
        return 1
    fi
    local count="${STORE_COUNT:-1}"
    if [ "$count" -lt 4 ]; then
        log_error "STORE_COUNT=$count; mixed drill needs STORE_COUNT>=4"
        return 1
    fi

    local mix_bin="$PROJECT_DIR/build/tests/falcon_kv/FalconKVMixedColocationE2E"
    if [ ! -x "$mix_bin" ]; then
        log_step "Mixed-colocation E2E binary missing; building..."
        (cd "$PROJECT_DIR/build" && ninja FalconKVMixedColocationE2E)
    fi

    local pguser="${USER:-postgres}"
    local cninfo="hostaddr=127.0.0.1 port=${CN_PORT} user=${pguser} dbname=postgres application_name=FalconKVMixedColocationE2E"

    if ! "$mix_bin" --cn-conninfo "$cninfo" \
        --dn "127.0.0.1:$DN1_POOLER_PORT" \
        --dn "127.0.0.1:$DN2_POOLER_PORT" \
        --dn "127.0.0.1:$DN3_POOLER_PORT" \
        --keys-per-phase 220 --timeout-ms 45000; then
        log_step "KV mixed colocation drill FAILED"
        return 1
    fi
    log_info "KV mixed colocation drill passed"
}

run_kv_topology_test() {
    log_step "Running FalconFS KV topology test (multi-store daemon topology)..."
    local count="${STORE_COUNT:-1}"
    if [ "$count" -lt 2 ]; then
        log_warn "STORE_COUNT=$count; set STORE_COUNT>=2 for full topology coverage"
    fi

    local port_base="${KV_STORE_BRPC_PORT:-18765}"
    for ((si=0; si<count; si++)); do
        local p=$((port_base + si))
        if ! wait_for_listen_tcp "$p" 3; then
            log_step "  topology check failed: store daemon not listening on $p"
            return 1
        fi
    done
    run_kv_cluster_test
}

run_kv_cluster_promote_test() {
    log_step "Running FalconFS KV promote-on-read tests..."
    export PYTHONPATH="$PROJECT_DIR/vllm_kv_cache/python:${PYTHONPATH:-}"
    python3 "$PROJECT_DIR/vllm_kv_cache/test/test_promote_on_read.py"

    local ut_bin="$PROJECT_DIR/build/tests/falcon_kv/FalconKVPrimitivesUT"
    if [ ! -x "$ut_bin" ]; then
        log_step "KV unit binary missing; building it now..."
        (cd "$PROJECT_DIR/build" && ninja FalconKVPrimitivesUT)
    fi
    "$ut_bin" --gtest_filter=KvPromoteFromEvicted.*
    log_info "KV promote-on-read tests passed"
}

# ============================================================================
# Main Entry
# ============================================================================

usage() {
    echo "Usage: $0 {build|start|stop|restart|status|test|kv-test|kv-fault-test|kv-meta-stress-test|kv-cluster-test|kv-cluster-fault-test|kv-cluster-failover-test|kv-mixed-colocation-test|kv-topology-test|kv-cluster-promote-test}"
    echo ""
    echo "Commands:"
    echo "  build   - Build and install FalconFS"
    echo "  start   - Start single-node cluster (1 CN + 2 DNs + 2 Clients)"
    echo "  stop    - Stop all services"
    echo "  restart - Restart all services"
    echo "  status  - Show service status"
    echo "  test    - Run distributed tests"
    echo "  kv-test - Run KV cache standalone tests (no vLLM required)"
    echo "  kv-fault-test - Run KV cache fault tests (no vLLM required)"
    echo "  kv-meta-stress-test - Run KV metadata end-to-end stress tests against the running cluster"
    echo "  kv-cluster-test - Run KV multi-DN distributed test (allocate/write/update/lookup/free across DN1+DN2)"
    echo "  kv-cluster-fault-test - Run KV cluster fault drill (DN restart, eviction rollback, large batch, stale store_epoch)"
    echo "  kv-cluster-failover-test - Run failover SQL drill + fault regression"
    echo "  kv-mixed-colocation-test - v6.5 §19.4.1 #4 affinity+fallback (needs KV_THREE_DNS=1, STORE_COUNT>=4)"
    echo "  kv-topology-test - Run KV topology checks for multi-store daemon setup"
    echo "  kv-cluster-promote-test - Run promote-from-evicted unit/integration checks"
    exit 1
}

case "${1:-}" in
    build)
        build_and_install
        ;;
    start)
        start_all
        ;;
    stop)
        stop_all
        ;;
    restart)
        stop_all
        sleep 2
        start_all
        ;;
    status)
        show_status
        ;;
    test)
        run_test
        ;;
    kv-test)
        run_kv_test
        ;;
    kv-meta-stress-test)
        run_kv_meta_stress_test
        ;;
    kv-cluster-test)
        run_kv_cluster_test
        ;;
    kv-cluster-fault-test)
        run_kv_cluster_fault_test
        ;;
    kv-cluster-failover-test)
        run_kv_cluster_failover_test
        ;;
    kv-topology-test)
        run_kv_topology_test
        ;;
    kv-mixed-colocation-test)
        run_kv_mixed_colocation_test
        ;;
    kv-cluster-promote-test)
        run_kv_cluster_promote_test
        ;;
    kv-fault-test)
        run_kv_fault_test
        ;;
    *)
        usage
        ;;
esac
