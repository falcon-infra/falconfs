#!/bin/bash

set -euo pipefail

DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
source "$DIR/falcon_client_config.sh"

CLIENT_LOG_DIR=${FALCON_CLIENT_LOG_DIR:-$DIR}
CLIENT_LOG_FILE="${CLIENT_LOG_DIR}/falcon_client.log"

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
    $SUDO umount -l "$MNT_PATH"
    echo "Unmounted $MNT_PATH and stopped associated falcon_client"
else
    echo "$MNT_PATH is not mounted"
fi

# 2. Kill any remaining falcon_client processes (for unmounted instances)
sleep 1
pids=$(pgrep -x falcon_client || true)
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
[ -f "${CLIENT_LOG_FILE}" ] && rm -f "${CLIENT_LOG_FILE}"

exit 0
