#!/bin/bash

set -euo pipefail

DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
source "$DIR/falcon_client_config.sh"

SUDO=""
if [ "$EUID" -ne 0 ]; then
    if ! command -v sudo >/dev/null 2>&1; then
        echo "Error: sudo not found but required" >&2
        exit 1
    fi
    SUDO="sudo"
fi

# 1. Unmount first (will stop the mounted instance)
if mount | grep -q "$MNT_PATH"; then
    $SUDO fusermount -u "$MNT_PATH"
    echo "Unmounted $MNT_PATH and stopped associated falcon_client"
else
    echo "$MNT_PATH is not mounted"
fi

# 2. Kill any remaining falcon_client processes (for unmounted instances)
sleep 1
pids=$(pgrep -f "^\falcon_client" | grep -v $$ | grep -v grep || true)
if [ -n "$pids" ]; then
    for pid in $pids; do
        if ps -p "$pid" >/dev/null; then
            $SUDO kill -9 "$pid" && echo "Stopped orphaned falcon_client (PID: $pid)"
        fi
    done
else
    echo "No additional falcon_client processes found"
fi

# 3. Clean cache (idempotent)
[ -d "$CACHE_PATH" ] && rm -rf "$CACHE_PATH"

# 4. Clean log (idempotent)
[ -f "${FALCONFS_HOME}/build/falcon_client.log" ] && rm -f "${FALCONFS_HOME}/build/falcon_client.log"

exit 0
