#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
REPO_ROOT="$(realpath "${SCRIPT_DIR}/../..")"

DATA_PATH=""

SINGLE_DURATION_MIN=60
DUAL_DURATION_MIN=120
TRIPLE_DURATION_MIN=120

INTERVAL_SEC=300
HEALTH_TIMEOUT_SEC=300
STARTUP_TIMEOUT_SEC=600
POST_ACTION_GRACE_SEC=15
STABLE_WINDOW_SEC=30
RELIABILITY_TIMEOUT_SEC=900
PROBE_MODE="meta"

ACTIONS_SINGLE="S01,S03,S04,S05"
ACTIONS_DUAL="S01,S02,S03,S04,S05,S06,S07"
ACTIONS_TRIPLE="S01,S02,S03,S04,S05,S06,S07"

SEED=20260305
FRESH_START=1
FAIL_FAST=0
CORE_ONCE_ENABLE=0
CORE_LIMIT_KB=1048576
CORE_ONCE_DIR=""

usage() {
    cat <<'EOF'
Usage:
  tests/regress/chaos_suite.sh --data-path <path> [options]

Options:
  --single-duration-min <minutes>       default: 60
  --dual-duration-min <minutes>         default: 120
  --triple-duration-min <minutes>       default: 120
  --interval-sec <seconds>              default: 300
  --health-timeout-sec <seconds>        default: 300
  --startup-timeout-sec <seconds>       default: 600
  --post-action-grace-sec <seconds>     default: 15
  --stable-window-sec <seconds>         default: 30
  --reliability-timeout-sec <seconds>   default: 900
  --probe-mode <mode>                   meta|meta+fuse, default: meta
  --actions-single <csv>                default: S01,S03,S04,S05
  --actions-dual <csv>                  default: S01,S02,S03,S04,S05,S06,S07
  --actions-triple <csv>                default: S01,S02,S03,S04,S05,S06,S07
  --seed <int>                          default: 20260305
  --no-fresh-start                      do not run fresh-start per stage
  --fail-fast                           stop suite on first failed stage
  --core-once-enable                    pass through core-once mode to each stage
  --core-limit-kb <kb>                  default: 1048576 (effective with core-once)
  --core-once-dir <path>                base dir for per-stage core files
  -h, --help

Output:
  <data-path>/single
  <data-path>/dual
  <data-path>/triple
  <data-path>/suite_summary.json
EOF
}

resolve_path() {
    local p="$1"
    if [[ "$p" = /* ]]; then
        echo "$p"
    else
        echo "${REPO_ROOT}/${p}"
    fi
}

log() {
    echo "[$(date '+%F %T')] $*"
}

parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --data-path)
                DATA_PATH="$2"
                shift 2
                ;;
            --single-duration-min)
                SINGLE_DURATION_MIN="$2"
                shift 2
                ;;
            --dual-duration-min)
                DUAL_DURATION_MIN="$2"
                shift 2
                ;;
            --triple-duration-min)
                TRIPLE_DURATION_MIN="$2"
                shift 2
                ;;
            --interval-sec)
                INTERVAL_SEC="$2"
                shift 2
                ;;
            --health-timeout-sec)
                HEALTH_TIMEOUT_SEC="$2"
                shift 2
                ;;
            --startup-timeout-sec)
                STARTUP_TIMEOUT_SEC="$2"
                shift 2
                ;;
            --post-action-grace-sec)
                POST_ACTION_GRACE_SEC="$2"
                shift 2
                ;;
            --stable-window-sec)
                STABLE_WINDOW_SEC="$2"
                shift 2
                ;;
            --reliability-timeout-sec)
                RELIABILITY_TIMEOUT_SEC="$2"
                shift 2
                ;;
            --probe-mode)
                PROBE_MODE="$2"
                shift 2
                ;;
            --actions-single)
                ACTIONS_SINGLE="$2"
                shift 2
                ;;
            --actions-dual)
                ACTIONS_DUAL="$2"
                shift 2
                ;;
            --actions-triple)
                ACTIONS_TRIPLE="$2"
                shift 2
                ;;
            --seed)
                SEED="$2"
                shift 2
                ;;
            --no-fresh-start)
                FRESH_START=0
                shift
                ;;
            --fail-fast)
                FAIL_FAST=1
                shift
                ;;
            --core-once-enable)
                CORE_ONCE_ENABLE=1
                shift
                ;;
            --core-limit-kb)
                CORE_LIMIT_KB="$2"
                shift 2
                ;;
            --core-once-dir)
                CORE_ONCE_DIR="$2"
                shift 2
                ;;
            -h|--help)
                usage
                exit 0
                ;;
            *)
                echo "unknown argument: $1"
                usage
                exit 1
                ;;
        esac
    done
}

run_stage() {
    local stage="$1"
    local compose_file="$2"
    local duration="$3"
    local actions_csv="$4"
    local stage_data="${DATA_PATH}/${stage}"
    local cmd=(
        bash "${REPO_ROOT}/tests/regress/chaos_run.sh"
        --data-path "$stage_data"
        --compose-file "$compose_file"
        --duration-min "$duration"
        --interval-sec "$INTERVAL_SEC"
        --health-timeout-sec "$HEALTH_TIMEOUT_SEC"
        --startup-timeout-sec "$STARTUP_TIMEOUT_SEC"
        --post-action-grace-sec "$POST_ACTION_GRACE_SEC"
        --stable-window-sec "$STABLE_WINDOW_SEC"
        --reliability-timeout-sec "$RELIABILITY_TIMEOUT_SEC"
        --probe-mode "$PROBE_MODE"
        --topology auto
        --actions "$actions_csv"
        --seed "$SEED"
        --teardown
    )

    if [[ "$FRESH_START" == "1" ]]; then
        cmd+=(--fresh-start)
    fi

    if [[ "$CORE_ONCE_ENABLE" == "1" ]]; then
        cmd+=(--core-once-enable --core-limit-kb "$CORE_LIMIT_KB")
        if [[ -n "$CORE_ONCE_DIR" ]]; then
            cmd+=(--core-once-dir "${CORE_ONCE_DIR}/${stage}")
        fi
    fi

    log "start stage=${stage}, compose=${compose_file}, duration=${duration}min, actions=${actions_csv}"
    if ! "${cmd[@]}"; then
        log "stage failed: ${stage}"
        return 1
    fi
    log "finish stage=${stage}"
}

main() {
    parse_args "$@"
    if [[ -z "$DATA_PATH" ]]; then
        usage
        exit 1
    fi

    DATA_PATH="$(resolve_path "$DATA_PATH")"
    mkdir -p "$DATA_PATH"
    if [[ -n "$CORE_ONCE_DIR" ]]; then
        CORE_ONCE_DIR="$(resolve_path "$CORE_ONCE_DIR")"
        mkdir -p "$CORE_ONCE_DIR"
    fi

    local failed=0

    run_stage "single" "tests/regress/docker-compose-single.yaml" "$SINGLE_DURATION_MIN" "$ACTIONS_SINGLE" || failed=1
    if [[ "$failed" == "1" && "$FAIL_FAST" == "1" ]]; then
        exit 1
    fi

    run_stage "dual" "tests/regress/docker-compose-dual.yaml" "$DUAL_DURATION_MIN" "$ACTIONS_DUAL" || failed=1
    if [[ "$failed" == "1" && "$FAIL_FAST" == "1" ]]; then
        exit 1
    fi

    run_stage "triple" "tests/regress/docker-compose-triple.yaml" "$TRIPLE_DURATION_MIN" "$ACTIONS_TRIPLE" || failed=1

    python3 - "$DATA_PATH" <<'PY'
import json
import os
import sys

root = sys.argv[1]
stages = ["single", "dual", "triple"]
summary = {"type": "suite_summary", "stages": {}}
all_passed = True

for stage in stages:
    report = os.path.join(root, stage, "chaos_report.jsonl")
    data = {"report": report, "exists": os.path.exists(report), "summary": None}
    if os.path.exists(report):
        with open(report, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                event = json.loads(line)
                if event.get("type") == "summary":
                    data["summary"] = event
        if not data["summary"] or data["summary"].get("failed", 1) != 0:
            all_passed = False
    else:
        all_passed = False
    summary["stages"][stage] = data

summary["passed"] = all_passed
out_file = os.path.join(root, "suite_summary.json")
with open(out_file, "w", encoding="utf-8") as f:
    json.dump(summary, f, ensure_ascii=True, indent=2)
print(json.dumps(summary, ensure_ascii=True))
PY

    if [[ "$failed" == "1" ]]; then
        exit 1
    fi
}

main "$@"
