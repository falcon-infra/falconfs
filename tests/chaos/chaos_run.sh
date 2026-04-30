#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
REPO_ROOT="$(realpath "${SCRIPT_DIR}/../..")"

DEFAULT_IMAGE="localhost:5000/falconfs-full-ubuntu24.04:v0.1.2-chaosmatrix"
DATA_PATH=""
COMPOSE_FILE="tests/regress/docker-compose-dual.yaml"
DURATION_MIN=120
INTERVAL_SEC=300
HEALTH_TIMEOUT_SEC=300
HEALTH_CHECK_INTERVAL_SEC=5
STARTUP_TIMEOUT_SEC=600
POST_ACTION_GRACE_SEC=15
STABLE_WINDOW_SEC=30
REPORT_FILE=""
RUN_LOG_FILE=""
DIAG_DIR=""
ACTIONS_CSV="S01,S02,S03,S04,S05"
ACTION_PLAN_FILE=""
REQUIRE_ACTIONS_CSV=""
ACTION_HOLD_SECS_CSV=""
SEED=20260305
SKIP_START=0
FRESH_START=0
TEARDOWN=0
FAIL_FAST=0
NO_AUTO_LOG=0
SKIP_PG_IP_CHECK=0
TOPOLOGY="auto"
TOPOLOGY_RESOLVED=""
REPLICA_TARGET=0
PROBE_MODE="meta"
PROBE_TIMEOUT_SEC=8
RELIABILITY_TIMEOUT_SEC=900
SUPPLEMENT_HOLD_SEC=0
STRICT_DUAL_RO_CHECK=0

SERVICE_EXPECTED=""
SERVICE_OBSERVED=""
SERVICE_READ_OK=0
SERVICE_WRITE_OK=0
SERVICE_PROBE_ERROR=""
SERVICE_FUSE_OK=1
SERVICE_PG_IN_RECOVERY=0
SERVICE_TX_READ_ONLY="unknown"
SERVICE_PROBE_REASON=""
DUAL_RO_SEEN_ACTION=0
DEGRADED_REQUIRED=0
DEGRADED_OBSERVED=0
NEED_SUPPLEMENT_SEEN=0
RELIABILITY_RESTORED=0
RELIABILITY_ERROR_MSG=""
STAGE_FAILED=""
RECOVERY_PATH="unknown"

RECOVER_SECONDS=0
RECOVERY_ERROR=""
CORE_ONCE_ENABLE=0
CORE_LIMIT_KB=1048576
CORE_ONCE_DIR=""
CORE_GUARD_PID=""

CHAOS_ALERT_ENABLE=0
CHAOS_SMTP_HOST=""
CHAOS_SMTP_PORT=587
CHAOS_SMTP_USER=""
CHAOS_SMTP_PASS=""
CHAOS_ALERT_EMAIL_FROM=""
CHAOS_ALERT_EMAIL_TO=""
CHAOS_SMTP_TLS=1

LAST_ALERT_KEY=""

HEALTH_ERROR_MSG=""
ACTION_TARGET=""

ALL_ACTION_IDS=(S01 S02 S03 S04 S05 S06 S07 S08)
declare -A ACTION_EXEC_COUNTS=()
declare -A ACTION_HOLD_SECS=()
declare -a ACTION_IDS=()
declare -a ACTION_PLAN_IDS=()
declare -a REQUIRED_ACTION_IDS=()

# Shared helper library for action mapping, plan parsing and coverage/report helpers.
# Keep this near the top so function availability is explicit.
# shellcheck source=/dev/null
source "${SCRIPT_DIR}/chaos_lib.sh"

usage() {
    cat <<'EOF'
Usage:
  tests/chaos/chaos_run.sh --data-path <path> [options]

Options:
  --compose-file <path>                default: tests/regress/docker-compose-dual.yaml
  --duration-min <minutes>             default: 120
  --interval-sec <seconds>             default: 300
  --health-timeout-sec <seconds>       default: 300
  --health-check-interval-sec <sec>    default: 5
  --startup-timeout-sec <seconds>      default: 600
  --post-action-grace-sec <seconds>    default: 15
  --stable-window-sec <seconds>        default: 30
  --actions <csv>                      default: S01,S02,S03,S04,S05
  --action-plan-file <path>            deterministic action order, one action-id per line
  --require-actions <csv>              fail coverage check if any required action count is 0
  --action-hold-secs <csv>             override hold sec by action id, e.g. S06=20,S07=90,S08=90
  --report-file <path>                 default: <data-path>/chaos_report.jsonl
  --run-log-file <path>                default: <data-path>/chaos_run.log
  --diag-dir <path>                    default: <data-path>/chaos_diag
  --no-auto-log                        disable auto tee logging to run log file
  --skip-pg-ip-check                   skip postgresql.conf pod-ip consistency check
  --topology <mode>                    auto|single|dual|triple, default: auto
  --probe-mode <mode>                  meta|meta+fuse, default: meta
  --probe-timeout-sec <seconds>        timeout for metadata probe command, default: 8
  --reliability-timeout-sec <seconds>  default: 900
  --supplement-hold-sec <seconds>      default: auto(wait_replica_time+30)
  --strict-dual-ro-check               require degraded-state observation for dual metadata actions
  --core-once-enable                   allow core dump and keep only first core
  --core-limit-kb <kb>                 core size limit in KB when core-once enabled (default: 1048576)
  --core-once-dir <path>               default: <data-path>/core_once
  --seed <int>                         default: 20260305
  --skip-start                         do not run docker-compose up -d
  --fresh-start                        run docker-compose down before start
  --teardown                           run docker-compose down on exit
  --fail-fast                          stop on first failed action
  -h, --help

Scenarios:
  S01 restart cn leader
  S02 restart one dn leader
  S03 restart random dn (prefer follower)
  S04 restart store container
  S05 kill falcon_client process in store
  S06 stop cn leader then start after hold duration
  S07 stop random dn then start after hold duration
  S08 stop cn leader, wait new leader, stop new leader, then recover both
EOF
}

parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --data-path)
                DATA_PATH="$2"
                shift 2
                ;;
            --compose-file)
                COMPOSE_FILE="$2"
                shift 2
                ;;
            --duration-min)
                DURATION_MIN="$2"
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
            --health-check-interval-sec)
                HEALTH_CHECK_INTERVAL_SEC="$2"
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
            --actions)
                ACTIONS_CSV="$2"
                shift 2
                ;;
            --action-plan-file)
                ACTION_PLAN_FILE="$2"
                shift 2
                ;;
            --require-actions)
                REQUIRE_ACTIONS_CSV="$2"
                shift 2
                ;;
            --action-hold-secs)
                ACTION_HOLD_SECS_CSV="$2"
                shift 2
                ;;
            --report-file)
                REPORT_FILE="$2"
                shift 2
                ;;
            --run-log-file)
                RUN_LOG_FILE="$2"
                shift 2
                ;;
            --diag-dir)
                DIAG_DIR="$2"
                shift 2
                ;;
            --seed)
                SEED="$2"
                shift 2
                ;;
            --no-auto-log)
                NO_AUTO_LOG=1
                shift
                ;;
            --skip-pg-ip-check)
                SKIP_PG_IP_CHECK=1
                shift
                ;;
            --topology)
                TOPOLOGY="$2"
                shift 2
                ;;
            --probe-mode)
                PROBE_MODE="$2"
                shift 2
                ;;
            --probe-timeout-sec)
                PROBE_TIMEOUT_SEC="$2"
                shift 2
                ;;
            --reliability-timeout-sec)
                RELIABILITY_TIMEOUT_SEC="$2"
                shift 2
                ;;
            --supplement-hold-sec)
                SUPPLEMENT_HOLD_SEC="$2"
                shift 2
                ;;
            --strict-dual-ro-check)
                STRICT_DUAL_RO_CHECK=1
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
            --skip-start)
                SKIP_START=1
                shift
                ;;
            --fresh-start)
                FRESH_START=1
                shift
                ;;
            --teardown)
                TEARDOWN=1
                shift
                ;;
            --fail-fast)
                FAIL_FAST=1
                shift
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

main() {
    parse_args "$@"
    if [[ -z "$DATA_PATH" ]]; then
        usage
        exit 1
    fi
    setup_runtime_and_validation
    log_runtime_configuration
    startup_stack_and_initial_checks
    run_action_loop_and_write_summary

    if [[ "$TEARDOWN" == "1" ]]; then
        log "teardown enabled, running docker-compose down"
        compose down --remove-orphans || true
    fi

    log "chaos run finished"
}

main "$@"
