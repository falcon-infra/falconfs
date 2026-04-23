#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/../.." && pwd)

: "${LD_LIBRARY_PATH:=}"
source "$REPO_ROOT/deploy/falcon_env.sh"
source "$REPO_ROOT/deploy/meta/falcon_meta_config.sh"

SERVER_IP="${SERVER_IP:-${FALCON_METADATA_UT_SERVER_IP:-$cnIp}}"
SERVER_PORT="${SERVER_PORT:-${FALCON_METADATA_UT_SERVER_PORT:-${cnPoolerPortPrefix}0}}"
CLIENT_NUM="${FALCON_METADATA_UT_CLIENT_NUM:-4}"
TEST_BIN="${FALCON_METADATA_UT_BIN:-$REPO_ROOT/build/tests/metadata_ut/FalconMetadataUT}"
MANAGE_SERVER="${FALCON_METADATA_UT_MANAGE_SERVER:-auto}"

port_ready() {
    (echo >/dev/tcp/"$SERVER_IP"/"$SERVER_PORT") >/dev/null 2>&1
}

started_server=0
need_start=0

if [[ "$MANAGE_SERVER" == "1" ]]; then
    need_start=1
elif [[ "$MANAGE_SERVER" == "auto" ]]; then
    if ! port_ready; then
        need_start=1
    fi
fi

if [[ "$need_start" == "1" ]]; then
    "$REPO_ROOT/deploy/meta/falcon_meta_stop.sh" || true
    "$REPO_ROOT/deploy/meta/falcon_meta_start.sh"
    started_server=1
fi

cleanup() {
    if [[ "$started_server" == "1" ]]; then
        "$REPO_ROOT/deploy/meta/falcon_meta_stop.sh" || true
    fi
}
trap cleanup EXIT

for _ in $(seq 1 60); do
    if port_ready; then
        break
    fi
    sleep 1
done

if ! port_ready; then
    echo "Falcon metadata server is not ready at $SERVER_IP:$SERVER_PORT" >&2
    exit 1
fi

if [[ ! -x "$TEST_BIN" ]]; then
    echo "Metadata UT binary is not executable: $TEST_BIN" >&2
    exit 1
fi

SERVER_IP="$SERVER_IP" SERVER_PORT="$SERVER_PORT" FALCON_METADATA_UT_CLIENT_NUM="$CLIENT_NUM" "$TEST_BIN" "$@"
