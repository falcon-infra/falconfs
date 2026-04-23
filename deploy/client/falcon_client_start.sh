#!/bin/bash
set -euo pipefail

DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
source "$DIR/falcon_client_config.sh"

is_mounted() {
    if command -v mountpoint >/dev/null 2>&1; then
        mountpoint -q "$MNT_PATH" 2>/dev/null
    else
        mount | grep -q " $MNT_PATH "
    fi
}

get_client_pids() {
    pgrep -x falcon_client 2>/dev/null || true
}

stop_stale_clients() {
    local pids
    local remaining

    pids="$(get_client_pids)"
    [ -z "$pids" ] && return 0

    echo "Stopping stale falcon_client process(es): $pids"
    for pid in $pids; do
        kill -TERM "$pid" 2>/dev/null || true
    done

    for _ in {1..5}; do
        sleep 1
        remaining="$(get_client_pids)"
        [ -z "$remaining" ] && return 0
    done

    echo "Force killing stale falcon_client process(es): $remaining"
    for pid in $remaining; do
        kill -KILL "$pid" 2>/dev/null || true
    done
}

ensure_mountpoint_writable() {
    mkdir -p "$MNT_PATH" 2>/dev/null || true
    if [ -w "$MNT_PATH" ]; then
        return 0
    fi

    echo "Error: mountpoint $MNT_PATH is not writable. Please run: sudo chown $USER:$(id -gn) $MNT_PATH" >&2
    exit 1
}

# already healthy => no-op
if is_mounted && [ -n "$(get_client_pids)" ]; then
    echo "falcon_client is already running and mounted at $MNT_PATH"
    exit 0
fi

# stale mount without process
if is_mounted && [ -z "$(get_client_pids)" ]; then
    echo "Found stale mount at $MNT_PATH without falcon_client process, unmounting..."
    if ! umount "$MNT_PATH" 2>/dev/null; then
        umount -l "$MNT_PATH"
    fi
fi

# stale process without mount
if ! is_mounted && [ -n "$(get_client_pids)" ]; then
    stop_stale_clients
fi

ensure_mountpoint_writable

# 清理并创建缓存目录
[ -d "$CACHE_PATH" ] && rm -rf "$CACHE_PATH"
mkdir -p "$CACHE_PATH" || {
    echo "Error: Failed to create cache directory" >&2
    exit 1
}

for i in {0..100}; do
    mkdir -p "$CACHE_PATH/$i" 2>/dev/null || true
done

CLIENT_OPTIONS=(
    "$MNT_PATH"
    -f
    -o direct_io
    -o attr_timeout=200
    -o entry_timeout=200
    -brpc true
    -rpc_endpoint="0.0.0.0:56039"
    -socket_max_unwritten_bytes=268435456
)

nohup falcon_client "${CLIENT_OPTIONS[@]}" >"${DIR}/falcon_client.log" 2>&1 &
client_pid=$!

for _ in {1..5}; do
    sleep 1
    if ! kill -0 "$client_pid" 2>/dev/null; then
        echo "Error: falcon_client exited during startup" >&2
        [ -f "${DIR}/falcon_client.log" ] && tail -n 50 "${DIR}/falcon_client.log" || true
        exit 1
    fi
done

echo "falcon_client started successfully (PID: $client_pid)"
exit 0
