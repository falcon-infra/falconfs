#!/bin/bash
set -euo pipefail

CURDIR=$(pwd)
source "$CURDIR/deploy/falcon_env.sh"
source "$CURDIR/deploy/client/falcon_client_config.sh"

cleanup() {
    "$CURDIR"/deploy/falcon_stop.sh || true
    "$CURDIR"/build.sh clean || true
}
trap cleanup EXIT

"$CURDIR"/deploy/falcon_stop.sh || true
"$CURDIR"/build.sh test
"$CURDIR"/deploy/falcon_start.sh
"$CURDIR"/.github/workflows/smoke_test.sh "$MNT_PATH"
