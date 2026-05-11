#!/bin/bash
set -euo pipefail

CURDIR=$(pwd)
source "$CURDIR/deploy/falcon_env.sh"
source "$CURDIR/deploy/client/falcon_client_config.sh"
ARTIFACT_DIR="$CURDIR/ci-artifacts"

collect_logs() {
    local exit_code=$1

    echo "Collecting failure diagnostics into $ARTIFACT_DIR"
    rm -rf ci-artifacts
    mkdir -p "$ARTIFACT_DIR/meta" "$ARTIFACT_DIR/client"

    {
        echo "failure_exit_code=$exit_code"
        echo "collected_at=$(date -Is)"
        echo "workspace=$CURDIR"
        echo "user=$(id -un)"
        echo "hostname=$(hostname)"
        echo "mount_path=$MNT_PATH"
    } >"$ARTIFACT_DIR/summary.txt" 2>&1 || true

    pgrep -af 'falcon_client|postgres.*metadata|falcon_.*process|mdtest|mpirun|fio' \
        >"$ARTIFACT_DIR/process_status.txt" 2>&1 || true
    {
        findmnt -T "$MNT_PATH" || true
        findmnt -rn -t fuse.falcon_client -o TARGET,SOURCE,FSTYPE,OPTIONS || true
    } >"$ARTIFACT_DIR/mount_status.txt" 2>&1 || true

    {
        pg_ctl status -D "$HOME/metadata/coordinator0" || true
        pg_ctl status -D "$HOME/metadata/worker0" || true
    } >"$ARTIFACT_DIR/pg_status.txt" 2>&1 || true

    {
        echo "== /usr/local/pgsql/lib/falcon.so =="
        strings /usr/local/pgsql/lib/falcon.so 2>/dev/null | grep '/home/.*/falconfs' || true
        strings /usr/local/pgsql/lib/falcon.so 2>/dev/null | grep '\.gcda' || true
        echo
        echo "== $FALCONFS_INSTALL_DIR/falcon_meta/lib/postgresql/falcon.so =="
        strings "$FALCONFS_INSTALL_DIR/falcon_meta/lib/postgresql/falcon.so" 2>/dev/null | grep '/home/.*/falconfs' || true
        strings "$FALCONFS_INSTALL_DIR/falcon_meta/lib/postgresql/falcon.so" 2>/dev/null | grep '\.gcda' || true
    } >"$ARTIFACT_DIR/binary_origin.txt" 2>&1 || true

    dmesg >"$ARTIFACT_DIR/dmesg.txt" 2>&1 || true

    cp -f "$CURDIR"/deploy/meta/cnlogfile*.log "$ARTIFACT_DIR/meta/" 2>/dev/null || true
    cp -f "$CURDIR"/deploy/meta/workerlogfile*.log "$ARTIFACT_DIR/meta/" 2>/dev/null || true
    cp -f "$CURDIR"/deploy/client/falcon_client.log "$ARTIFACT_DIR/client/" 2>/dev/null || true

    find "$ARTIFACT_DIR" -maxdepth 3 -type f -print | sort >"$ARTIFACT_DIR/manifest.txt" 2>/dev/null || true
    echo "Failure diagnostics collected. Files are listed in $ARTIFACT_DIR/manifest.txt"
}

cleanup() {
    local exit_code=$?

    if [ "$exit_code" -ne 0 ]; then
        collect_logs "$exit_code" || true
    fi

    "$CURDIR"/deploy/falcon_stop.sh || true
    "$CURDIR"/build.sh clean || true
    exit "$exit_code"
}
trap cleanup EXIT

rm -rf ci-artifacts
"$CURDIR"/deploy/falcon_stop.sh || true
"$CURDIR"/build.sh test
"$CURDIR"/deploy/falcon_start.sh
"$CURDIR"/.github/workflows/smoke_test.sh "$MNT_PATH"
