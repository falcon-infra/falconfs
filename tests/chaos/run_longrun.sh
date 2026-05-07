#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
REPO_ROOT="$(realpath "${SCRIPT_DIR}/../..")"
CONFIG_FILE="${SCRIPT_DIR}/longrun_config.sh"
DRY_RUN=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run)
            DRY_RUN=1
            shift
            ;;
        -h|--help)
            echo "Usage: tests/chaos/run_longrun.sh [--dry-run]"
            echo "Reads tests/chaos/longrun_config.sh and runs chaos_suite.sh"
            exit 0
            ;;
        *)
            echo "unknown argument: $1"
            echo "Usage: tests/chaos/run_longrun.sh [--dry-run]"
            exit 1
            ;;
    esac
done

if [[ ! -f "${CONFIG_FILE}" ]]; then
    echo "config file not found: ${CONFIG_FILE}"
    exit 1
fi

# shellcheck source=/dev/null
source "${CONFIG_FILE}"

resolve_path() {
    local p="$1"
    if [[ -z "$p" ]]; then
        echo ""
    elif [[ "$p" = /* ]]; then
        echo "$p"
    else
        echo "${REPO_ROOT}/${p}"
    fi
}

bool_is_one() {
    [[ "${1:-0}" == "1" ]]
}

data_path="${LONGRUN_DATA_PATH:-}"
if [[ -z "$data_path" ]]; then
    data_path="tests/chaos/accept_longrun_$(date +%Y-%m-%d_%H%M%S)"
fi

matrix_file="$(resolve_path "${LONGRUN_MATRIX_FILE:-}")"
core_once_dir="$(resolve_path "${LONGRUN_CORE_ONCE_DIR:-}")"
data_path="$(resolve_path "$data_path")"

export FALCON_FULL_IMAGE="${LONGRUN_IMAGE}"
export SKIP_IMAGE_BUILD="${LONGRUN_SKIP_IMAGE_BUILD:-1}"

cmd=(bash "${SCRIPT_DIR}/chaos_suite.sh"
    --data-path "$data_path"
    --single-duration-min "${LONGRUN_SINGLE_DURATION_MIN:-60}"
    --dual-duration-min "${LONGRUN_DUAL_DURATION_MIN:-120}"
    --triple-duration-min "${LONGRUN_TRIPLE_DURATION_MIN:-120}"
    --interval-sec "${LONGRUN_INTERVAL_SEC:-300}"
    --health-timeout-sec "${LONGRUN_HEALTH_TIMEOUT_SEC:-300}"
    --startup-timeout-sec "${LONGRUN_STARTUP_TIMEOUT_SEC:-600}"
    --post-action-grace-sec "${LONGRUN_POST_ACTION_GRACE_SEC:-15}"
    --stable-window-sec "${LONGRUN_STABLE_WINDOW_SEC:-30}"
    --reliability-timeout-sec "${LONGRUN_RELIABILITY_TIMEOUT_SEC:-900}"
    --probe-mode "${LONGRUN_PROBE_MODE:-meta}"
    --actions-single "${LONGRUN_ACTIONS_SINGLE:-S01,S03,S04,S05}"
    --actions-dual "${LONGRUN_ACTIONS_DUAL:-S01,S02,S03,S04,S05,S06,S07,S08}"
    --actions-triple "${LONGRUN_ACTIONS_TRIPLE:-S01,S02,S03,S04,S05,S06,S07,S08}"
    --stages "${LONGRUN_STAGES:-single,dual,triple}"
    --seed "${LONGRUN_SEED:-20260305}")

if [[ -n "$matrix_file" ]]; then
    cmd+=(--case-matrix-file "$matrix_file")
fi

if bool_is_one "${LONGRUN_STRICT_COVERAGE:-1}"; then
    cmd+=(--strict-coverage)
fi

if [[ -n "${LONGRUN_ACTION_HOLD_SECS:-}" ]]; then
    cmd+=(--action-hold-secs "${LONGRUN_ACTION_HOLD_SECS}")
fi

if bool_is_one "${LONGRUN_FAIL_FAST:-0}"; then
    cmd+=(--fail-fast)
fi

if ! bool_is_one "${LONGRUN_FRESH_START:-1}"; then
    cmd+=(--no-fresh-start)
fi

if bool_is_one "${LONGRUN_CORE_ONCE_ENABLE:-0}"; then
    cmd+=(--core-once-enable --core-limit-kb "${LONGRUN_CORE_LIMIT_KB:-1048576}")
    if [[ -n "$core_once_dir" ]]; then
        cmd+=(--core-once-dir "$core_once_dir")
    fi
fi

echo "[run_longrun] image: ${FALCON_FULL_IMAGE}"
echo "[run_longrun] data path: ${data_path}"
echo "[run_longrun] command: ${cmd[*]}"

if (( DRY_RUN == 1 )); then
    exit 0
fi

"${cmd[@]}"
