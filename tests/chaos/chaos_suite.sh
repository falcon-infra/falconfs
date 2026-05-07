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
CASE_MATRIX_FILE=""
STRICT_COVERAGE=0
ACTION_HOLD_SECS_CSV=""
STAGES_CSV="single,dual,triple"

ACTIONS_SINGLE="S01,S03,S04,S05"
ACTIONS_DUAL="S01,S02,S03,S04,S05,S06,S07,S08"
ACTIONS_TRIPLE="S01,S02,S03,S04,S05,S06,S07,S08"

SEED=20260305
FRESH_START=1
FAIL_FAST=0
CORE_ONCE_ENABLE=0
CORE_LIMIT_KB=1048576
CORE_ONCE_DIR=""

declare -A MATRIX_COMPOSE=()
declare -A MATRIX_TOPOLOGY=()
declare -A MATRIX_DURATION=()
declare -A MATRIX_ACTION_PLAN=()
declare -A MATRIX_REQUIRE_ACTIONS=()

usage() {
    cat <<'EOF'
Usage:
  tests/chaos/chaos_suite.sh --data-path <path> [options]

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
  --actions-dual <csv>                  default: S01,S02,S03,S04,S05,S06,S07,S08
  --actions-triple <csv>                default: S01,S02,S03,S04,S05,S06,S07,S08
  --case-matrix-file <path>             deterministic stage matrix file
  --strict-coverage                     fail if any single/dual/triple matrix stage missing
  --action-hold-secs <csv>              pass through hold overrides, e.g. S06=60,S07=60,S08=60
  --stages <csv>                        stages to run, default: single,dual,triple
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
            --case-matrix-file)
                CASE_MATRIX_FILE="$2"
                shift 2
                ;;
            --strict-coverage)
                STRICT_COVERAGE=1
                shift
                ;;
            --action-hold-secs)
                ACTION_HOLD_SECS_CSV="$2"
                shift 2
                ;;
            --stages)
                STAGES_CSV="$2"
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

trim_ws() {
    local text="$1"
    text="${text#"${text%%[![:space:]]*}"}"
    text="${text%"${text##*[![:space:]]}"}"
    printf '%s' "$text"
}

stage_selected() {
    local stage="$1"
    local item
    IFS=',' read -r -a selected_stages <<<"$STAGES_CSV"
    for item in "${selected_stages[@]}"; do
        item="$(trim_ws "$item")"
        if [[ "$item" == "$stage" ]]; then
            return 0
        fi
    done
    return 1
}

validate_stages() {
    local item seen=0
    IFS=',' read -r -a selected_stages <<<"$STAGES_CSV"
    for item in "${selected_stages[@]}"; do
        item="$(trim_ws "$item")"
        [[ -z "$item" ]] && continue
        case "$item" in
            single|dual|triple) ;;
            *) echo "invalid stage in --stages: $item"; exit 1 ;;
        esac
        seen=1
    done
    if (( seen == 0 )); then
        echo "--stages has no executable stages"
        exit 1
    fi
}

load_case_matrix() {
    local raw line stage compose_file topology duration action_plan required
    [[ -z "$CASE_MATRIX_FILE" ]] && return 0
    CASE_MATRIX_FILE="$(resolve_path "$CASE_MATRIX_FILE")"
    [[ ! -f "$CASE_MATRIX_FILE" ]] && { echo "matrix file not found: $CASE_MATRIX_FILE"; exit 1; }

    while IFS= read -r raw || [[ -n "$raw" ]]; do
        line="${raw%%#*}"
        line="$(trim_ws "$line")"
        [[ -z "$line" ]] && continue
        IFS='|' read -r stage compose_file topology duration action_plan required <<<"$line"
        stage="$(trim_ws "$stage")"
        compose_file="$(trim_ws "$compose_file")"
        topology="$(trim_ws "$topology")"
        duration="$(trim_ws "$duration")"
        action_plan="$(trim_ws "$action_plan")"
        required="$(trim_ws "$required")"

        case "$stage" in
            single|dual|triple) ;;
            *) echo "invalid matrix stage: $stage"; exit 1 ;;
        esac
        case "$topology" in
            auto|single|dual|triple) ;;
            *) echo "invalid matrix topology: $topology"; exit 1 ;;
        esac
        [[ -z "$compose_file" || -z "$duration" || -z "$action_plan" ]] && { echo "invalid matrix line: $raw"; exit 1; }

        MATRIX_COMPOSE["$stage"]="$compose_file"
        MATRIX_TOPOLOGY["$stage"]="$topology"
        MATRIX_DURATION["$stage"]="$duration"
        MATRIX_ACTION_PLAN["$stage"]="$action_plan"
        MATRIX_REQUIRE_ACTIONS["$stage"]="$required"
    done <"$CASE_MATRIX_FILE"

    if [[ "$STRICT_COVERAGE" == "1" ]]; then
        local stage
        for stage in single dual triple; do
            if ! stage_selected "$stage"; then
                continue
            fi
            if [[ -z "${MATRIX_COMPOSE[$stage]:-}" ]]; then
                echo "strict coverage failed: matrix missing stage '${stage}'"
                exit 1
            fi
        done
    fi
}

run_stage() {
    local stage="$1"
    local compose_file="$2"
    local duration="$3"
    local actions_csv="$4"
    local topology_mode="${5:-auto}"
    local action_plan_file="${6:-}"
    local require_actions_csv="${7:-}"
    local stage_data="${DATA_PATH}/${stage}"
    local cmd=(
        bash "${REPO_ROOT}/tests/chaos/chaos_run.sh"
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
        --topology "$topology_mode"
        --actions "$actions_csv"
        --seed "$SEED"
        --teardown
    )

    if [[ -n "$action_plan_file" ]]; then
        cmd+=(--action-plan-file "$action_plan_file")
    fi
    if [[ -n "$require_actions_csv" ]]; then
        cmd+=(--require-actions "$require_actions_csv")
    fi
    if [[ -n "$ACTION_HOLD_SECS_CSV" ]]; then
        cmd+=(--action-hold-secs "$ACTION_HOLD_SECS_CSV")
    fi

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
    validate_stages

    DATA_PATH="$(resolve_path "$DATA_PATH")"
    mkdir -p "$DATA_PATH"
    load_case_matrix
    if [[ -n "$CORE_ONCE_DIR" ]]; then
        CORE_ONCE_DIR="$(resolve_path "$CORE_ONCE_DIR")"
        mkdir -p "$CORE_ONCE_DIR"
    fi

    local failed=0

    local single_compose="tests/regress/docker-compose-single.yaml"
    local dual_compose="tests/regress/docker-compose-dual.yaml"
    local triple_compose="tests/regress/docker-compose-triple.yaml"
    local single_topology="auto"
    local dual_topology="auto"
    local triple_topology="auto"
    local single_duration="$SINGLE_DURATION_MIN"
    local dual_duration="$DUAL_DURATION_MIN"
    local triple_duration="$TRIPLE_DURATION_MIN"
    local single_plan=""
    local dual_plan=""
    local triple_plan=""
    local single_req=""
    local dual_req=""
    local triple_req=""

    if [[ -n "$CASE_MATRIX_FILE" ]]; then
        single_compose="${MATRIX_COMPOSE[single]:-$single_compose}"
        dual_compose="${MATRIX_COMPOSE[dual]:-$dual_compose}"
        triple_compose="${MATRIX_COMPOSE[triple]:-$triple_compose}"
        single_topology="${MATRIX_TOPOLOGY[single]:-$single_topology}"
        dual_topology="${MATRIX_TOPOLOGY[dual]:-$dual_topology}"
        triple_topology="${MATRIX_TOPOLOGY[triple]:-$triple_topology}"
        single_duration="${MATRIX_DURATION[single]:-$single_duration}"
        dual_duration="${MATRIX_DURATION[dual]:-$dual_duration}"
        triple_duration="${MATRIX_DURATION[triple]:-$triple_duration}"
        single_plan="${MATRIX_ACTION_PLAN[single]:-}"
        dual_plan="${MATRIX_ACTION_PLAN[dual]:-}"
        triple_plan="${MATRIX_ACTION_PLAN[triple]:-}"
        single_req="${MATRIX_REQUIRE_ACTIONS[single]:-}"
        dual_req="${MATRIX_REQUIRE_ACTIONS[dual]:-}"
        triple_req="${MATRIX_REQUIRE_ACTIONS[triple]:-}"
    fi

    if stage_selected "single"; then
        run_stage "single" "$single_compose" "$single_duration" "$ACTIONS_SINGLE" "$single_topology" "$single_plan" "$single_req" || failed=1
        if [[ "$failed" == "1" && "$FAIL_FAST" == "1" ]]; then
            exit 1
        fi
    fi

    if stage_selected "dual"; then
        run_stage "dual" "$dual_compose" "$dual_duration" "$ACTIONS_DUAL" "$dual_topology" "$dual_plan" "$dual_req" || failed=1
        if [[ "$failed" == "1" && "$FAIL_FAST" == "1" ]]; then
            exit 1
        fi
    fi

    if stage_selected "triple"; then
        run_stage "triple" "$triple_compose" "$triple_duration" "$ACTIONS_TRIPLE" "$triple_topology" "$triple_plan" "$triple_req" || failed=1
    fi

    python3 - "$DATA_PATH" "$STAGES_CSV" <<'PY'
import json
import os
import sys

root = sys.argv[1]
stages = [stage.strip() for stage in sys.argv[2].split(",") if stage.strip()]
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
