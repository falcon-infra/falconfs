#!/bin/bash
set -euo pipefail

DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
source "$DIR/falcon_client_config.sh"

# 检查是否已挂载
if mount | grep -q "$MNT_PATH"; then
    echo "$MNT_PATH is already mounted"
else
    mkdir -p "$MNT_PATH" 2>/dev/null || true
fi

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

nohup falcon_client "${CLIENT_OPTIONS[@]}" >falcon_client.log 2>&1 &

sleep 1
if ! pgrep -f "falcon_client" >/dev/null; then
    echo "Error: Failed to start falcon_client" >&2
    exit 1
fi

echo "falcon_client started successfully (PID: $(pgrep -f "^\falcon_client"))"
exit 0
