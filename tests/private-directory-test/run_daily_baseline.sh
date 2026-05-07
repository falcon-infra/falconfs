#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
REPO_DIR="${FALCONFS_BASELINE_REPO_DIR:-$(cd "$SCRIPT_DIR/../.." && pwd -P)}"
RESULT_ROOT="${FALCONFS_BASELINE_RESULT_ROOT:-$HOME/falconfs-baseline-results}"
RUN_ID="${FALCONFS_BASELINE_RUN_ID:-$(date +%Y%m%d_%H%M%S)}"
RESULT_DIR="${FALCONFS_BASELINE_RESULT_DIR:-$RESULT_ROOT/$RUN_ID}"
LOCK_DIR="${FALCONFS_BASELINE_LOCK_DIR:-/tmp/falconfs_daily_baseline_$(id -un).lockdir}"

BASELINE_BRANCH="${FALCONFS_BASELINE_BRANCH:-baseline/upstream-main}"
BASELINE_REMOTE="${FALCONFS_BASELINE_REMOTE:-upstream}"
BASELINE_REF="${FALCONFS_BASELINE_REF:-$BASELINE_REMOTE/main}"
UPDATE_GIT="${FALCONFS_BASELINE_UPDATE_GIT:-1}"
ALLOW_BRANCH_RESET="${FALCONFS_BASELINE_ALLOW_BRANCH_RESET:-0}"
RUN_BUILD="${FALCONFS_BASELINE_BUILD:-1}"
RUN_DEPLOY="${FALCONFS_BASELINE_DEPLOY:-1}"
INSTALL_DIR="${FALCONFS_INSTALL_DIR:-/usr/local/falconfs}"
TEST_DIR="$INSTALL_DIR/private-directory-test"
ROUNDS="${FALCONFS_BASELINE_ROUNDS:-0 1 2 3}"
SUDO_CMD_TEXT="${FALCONFS_BASELINE_SUDO_CMD:-sudo -n}"
REPORT_ENABLED="${FALCONFS_BASELINE_REPORT:-1}"
CONFIRM_ON_DEGRADATION="${FALCONFS_BASELINE_CONFIRM_ON_DEGRADATION:-1}"
FAIL_ON_ALERT="${FALCONFS_BASELINE_FAIL_ON_ALERT:-0}"
REPORT_SCRIPT="${FALCONFS_BASELINE_REPORT_SCRIPT:-$TEST_DIR/baseline_report.py}"

read -r -a SUDO_CMD <<< "$SUDO_CMD_TEXT"

log() {
    printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

resolve_meta_server_ip() {
    local meta_config="$REPO_DIR/deploy/meta/falcon_meta_config.sh"
    if [ -f "$meta_config" ]; then
        bash -lc "source '$meta_config' >/dev/null 2>&1; printf '%s' \"\${cnIp:-127.0.0.1}\""
    else
        printf '127.0.0.1'
    fi
}

resolve_meta_server_port() {
    local meta_config="$REPO_DIR/deploy/meta/falcon_meta_config.sh"
    if [ -f "$meta_config" ]; then
        bash -lc "source '$meta_config' >/dev/null 2>&1; printf '%s0' \"\${cnPoolerPortPrefix:-5551}\""
    else
        printf '55510'
    fi
}

run_private_directory_baseline() {
    local output_dir="$1"
    local run_id="$2"

    BIN_DIR="$TEST_DIR/bin" \
    TEST_PROGRAM="${FALCONFS_BASELINE_TEST_PROGRAM:-test_falcon}" \
    MOUNT_DIR="${FALCONFS_BASELINE_MOUNT_DIR:-/}" \
    FILE_PER_THREAD="${FALCONFS_BASELINE_FILE_PER_THREAD:-1000}" \
    THREAD_NUM_PER_CLIENT="${FALCONFS_BASELINE_THREAD_NUM_PER_CLIENT:-2000}" \
    ROUND_INDEX="$ROUNDS" \
    PORT="${FALCONFS_BASELINE_WAIT_PORT:-1111}" \
    FILE_SIZE="${FALCONFS_BASELINE_FILE_SIZE:-1572864}" \
    CLIENT_NUM="${FALCONFS_BASELINE_CLIENT_NUM:-1}" \
    CLIENT_ID="${FALCONFS_BASELINE_CLIENT_ID:-0}" \
    MOUNT_PER_CLIENT="${FALCONFS_BASELINE_MOUNT_PER_CLIENT:-1}" \
    CLIENT_CACHE_SIZE="${FALCONFS_BASELINE_CLIENT_CACHE_SIZE:-16384}" \
    META_SERVER_IP="$meta_server_ip" \
    META_SERVER_PORT="$meta_server_port" \
    RESULT_DIR="$output_dir" \
    RUN_ID="$run_id" \
    bash "$TEST_DIR/local-run.sh"
}

run_baseline_report() {
    local current_dir="$1"
    local confirm_dir="${2:-}"
    local script_path="$REPORT_SCRIPT"

    if [ ! -x "$script_path" ]; then
        script_path="$SCRIPT_DIR/baseline_report.py"
    fi
    if [ ! -x "$script_path" ]; then
        log "Baseline report script not found, skip report"
        return 0
    fi

    local report_args=(
        "$script_path"
        --result-root "$RESULT_ROOT"
        --current-dir "$current_dir"
    )
    if [ -n "$confirm_dir" ]; then
        report_args+=(--confirm-dir "$confirm_dir")
    fi

    set +e
    python3 "${report_args[@]}"
    local status=$?
    set -e
    return "$status"
}

mkdir -p "$RESULT_DIR"
if ! mkdir "$LOCK_DIR" 2>/dev/null; then
    echo "Another FalconFS daily baseline run is already active: $LOCK_DIR" >&2
    exit 1
fi
trap 'rmdir "$LOCK_DIR" 2>/dev/null || true' EXIT

cd "$REPO_DIR"

if [ "$UPDATE_GIT" = "1" ]; then
    current_branch=$(git branch --show-current)
    if [ "$current_branch" != "$BASELINE_BRANCH" ] && [ "$ALLOW_BRANCH_RESET" != "1" ]; then
        cat >&2 <<EOF
Refusing to reset branch '$current_branch'.
Run this script from the dedicated '$BASELINE_BRANCH' worktree, or set FALCONFS_BASELINE_ALLOW_BRANCH_RESET=1.
EOF
        exit 1
    fi

    log "Fetching $BASELINE_REMOTE"
    git fetch "$BASELINE_REMOTE"
    log "Resetting $current_branch to $BASELINE_REF"
    git reset --hard "$BASELINE_REF"
fi

commit_sha=$(git rev-parse HEAD)
commit_subject=$(git log -1 --format=%s)
git_status_file="$RESULT_DIR/git_status.txt"
git status --short --branch > "$git_status_file"
git_dirty=0
if [ -n "$(git status --porcelain)" ]; then
    git_dirty=1
fi

cat > "$RESULT_DIR/run_info.env" <<EOF
run_id=$RUN_ID
repo_dir=$REPO_DIR
commit_sha=$commit_sha
commit_subject=$commit_subject
git_dirty=$git_dirty
git_status_file=$git_status_file
baseline_ref=$BASELINE_REF
result_dir=$RESULT_DIR
EOF

if [ "$RUN_BUILD" = "1" ]; then
    log "Cleaning FalconFS build"
    ./build.sh clean falcon
    log "Building FalconFS"
    ./build.sh build falcon
    log "Installing FalconFS to $INSTALL_DIR"
    "${SUDO_CMD[@]}" ./build.sh install falcon
fi

if [ "$RUN_DEPLOY" = "1" ]; then
    log "Stopping FalconFS"
    ./deploy/falcon_stop.sh
    log "Starting FalconFS"
    ./deploy/falcon_start.sh
fi

meta_server_ip="${FALCONFS_BASELINE_META_SERVER_IP:-$(resolve_meta_server_ip)}"
meta_server_port="${FALCONFS_BASELINE_META_SERVER_PORT:-$(resolve_meta_server_port)}"

log "Running private-directory baseline rounds: $ROUNDS"
run_private_directory_baseline "$RESULT_DIR" "$RUN_ID"

if [ "$REPORT_ENABLED" = "1" ]; then
    log "Generating baseline report"
    set +e
    run_baseline_report "$RESULT_DIR"
    report_status=$?
    set -e
    if [ "$report_status" -ne 0 ] && [ "$report_status" -ne 10 ]; then
        exit "$report_status"
    fi
    if [ "$report_status" -eq 10 ] && [ "$CONFIRM_ON_DEGRADATION" = "1" ]; then
        CONFIRM_RUN_ID="${RUN_ID}_confirm"
        CONFIRM_RESULT_DIR="${RESULT_DIR}_confirm"
        log "Suspected degradation detected; rerunning benchmark only for confirmation"
        run_private_directory_baseline "$CONFIRM_RESULT_DIR" "$CONFIRM_RUN_ID"
        cat >> "$CONFIRM_RESULT_DIR/run_info.env" <<EOF
confirm_of=$RUN_ID
confirm_of_result_dir=$RESULT_DIR
EOF
        set +e
        run_baseline_report "$RESULT_DIR" "$CONFIRM_RESULT_DIR"
        final_report_status=$?
        set -e
        if [ "$final_report_status" -ne 0 ] && [ "$final_report_status" -ne 10 ]; then
            exit "$final_report_status"
        fi
        if [ "$final_report_status" -eq 10 ]; then
            log "Confirmed baseline degradation"
            if [ "$FAIL_ON_ALERT" = "1" ]; then
                exit 10
            fi
        fi
    elif [ "$report_status" -eq 10 ] && [ "$FAIL_ON_ALERT" = "1" ]; then
        exit 10
    fi
fi

log "Daily baseline completed: $RESULT_DIR"
