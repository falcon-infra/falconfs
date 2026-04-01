#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
REPO_ROOT="$(realpath "${SCRIPT_DIR}/../..")"

DEFAULT_IMAGE="localhost:5000/falconfs-full-ubuntu24.04:v0.1.0"
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
NEED_SUPPLEMENT_SEEN=0
RELIABILITY_RESTORED=0
RELIABILITY_ERROR_MSG=""
STAGE_FAILED=""

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

usage() {
    cat <<'EOF'
Usage:
  tests/regress/chaos_run.sh --data-path <path> [options]

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
  S01C2 restart cn-2 (fixed target)
  S02 restart one dn leader
  S03 restart random dn (prefer follower)
  S04 restart store container
  S05 kill falcon_client process in store
  S06 stop cn leader then start after hold duration
  S07 stop random dn then start after hold duration
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

is_nonneg_integer() {
    local v="$1"
    [[ "$v" =~ ^[0-9]+$ ]]
}

compose_replica_server_num() {
    local line
    while IFS= read -r line; do
        if [[ "$line" =~ replica_server_num:[[:space:]]*\"?([0-9]+)\"? ]]; then
            echo "${BASH_REMATCH[1]}"
            return 0
        fi
    done <"$COMPOSE_FILE"
    return 1
}

runtime_env_value() {
    local container="$1"
    local key="$2"
    docker exec "$container" bash -lc "printenv '$key' 2>/dev/null | tr -d '\n'" 2>/dev/null || true
}

pick_running_cn_container() {
    docker ps --format '{{.Names}}' | grep '^falcon-cn-' | head -n 1
}

resolve_topology() {
    local replica_num=""

    case "$TOPOLOGY" in
        single|dual|triple)
            TOPOLOGY_RESOLVED="$TOPOLOGY"
            ;;
        auto)
            replica_num="$(runtime_env_value falcon-cn-1 replica_server_num)"
            if ! is_nonneg_integer "$replica_num"; then
                replica_num="$(compose_replica_server_num || true)"
            fi
            if ! is_nonneg_integer "$replica_num"; then
                log "ERROR: failed to resolve topology in auto mode"
                log "Please pass --topology single|dual|triple explicitly"
                return 1
            fi
            case "$replica_num" in
                0) TOPOLOGY_RESOLVED="single" ;;
                1) TOPOLOGY_RESOLVED="dual" ;;
                2) TOPOLOGY_RESOLVED="triple" ;;
                *)
                    log "ERROR: unsupported replica_server_num=${replica_num}"
                    return 1
                    ;;
            esac
            ;;
        *)
            log "ERROR: invalid topology: ${TOPOLOGY}"
            return 1
            ;;
    esac

    case "$TOPOLOGY_RESOLVED" in
        single) REPLICA_TARGET=0 ;;
        dual) REPLICA_TARGET=1 ;;
        triple) REPLICA_TARGET=2 ;;
        *)
            log "ERROR: unresolved topology: ${TOPOLOGY_RESOLVED}"
            return 1
            ;;
    esac
    return 0
}

resolve_supplement_hold_sec() {
    local wait_replica
    if (( SUPPLEMENT_HOLD_SEC > 0 )); then
        return 0
    fi
    wait_replica="$(runtime_env_value falcon-cn-1 wait_replica_time)"
    if ! is_nonneg_integer "$wait_replica"; then
        wait_replica="600"
    fi
    SUPPLEMENT_HOLD_SEC=$((wait_replica + 30))
}

validate_runtime_options() {
    case "$TOPOLOGY" in
        auto|single|dual|triple) ;;
        *)
            echo "invalid --topology: ${TOPOLOGY}"
            exit 1
            ;;
    esac

    case "$PROBE_MODE" in
        meta|meta+fuse) ;;
        *)
            echo "invalid --probe-mode: ${PROBE_MODE}"
            exit 1
            ;;
    esac

    if ! is_nonneg_integer "$PROBE_TIMEOUT_SEC" || (( PROBE_TIMEOUT_SEC < 1 )); then
        echo "invalid --probe-timeout-sec: ${PROBE_TIMEOUT_SEC}"
        exit 1
    fi

    if ! is_nonneg_integer "$RELIABILITY_TIMEOUT_SEC"; then
        echo "invalid --reliability-timeout-sec: ${RELIABILITY_TIMEOUT_SEC}"
        exit 1
    fi

    if ! is_nonneg_integer "$SUPPLEMENT_HOLD_SEC"; then
        echo "invalid --supplement-hold-sec: ${SUPPLEMENT_HOLD_SEC}"
        exit 1
    fi
}

log() {
    echo "[$(date '+%F %T')] $*"
}

alert_enabled() {
    [[ "${CHAOS_ALERT_ENABLE}" == "1" ]]
}

send_email_alert() {
    local subject="$1"
    local body="$2"
    local dedup_key="${3:-}"
    local missing=()

    if ! alert_enabled; then
        return 0
    fi

    if [[ -n "$dedup_key" && "$dedup_key" == "$LAST_ALERT_KEY" ]]; then
        log "skip duplicate alert: ${dedup_key}"
        return 0
    fi

    [[ -z "${CHAOS_SMTP_HOST}" ]] && missing+=("CHAOS_SMTP_HOST")
    [[ -z "${CHAOS_SMTP_PORT}" ]] && missing+=("CHAOS_SMTP_PORT")
    [[ -z "${CHAOS_SMTP_USER}" ]] && missing+=("CHAOS_SMTP_USER")
    [[ -z "${CHAOS_SMTP_PASS}" ]] && missing+=("CHAOS_SMTP_PASS")
    [[ -z "${CHAOS_ALERT_EMAIL_FROM}" ]] && missing+=("CHAOS_ALERT_EMAIL_FROM")
    [[ -z "${CHAOS_ALERT_EMAIL_TO}" ]] && missing+=("CHAOS_ALERT_EMAIL_TO")

    if (( ${#missing[@]} > 0 )); then
        log "WARNING: alert enabled but missing SMTP settings: ${missing[*]}"
        return 1
    fi

    if python3 - "$subject" "$body" <<'PY'
import os
import smtplib
import ssl
import sys
from email.message import EmailMessage

subject = sys.argv[1]
body = sys.argv[2]

host = os.environ["CHAOS_SMTP_HOST"]
port = int(os.environ.get("CHAOS_SMTP_PORT", "587"))
user = os.environ.get("CHAOS_SMTP_USER", "")
password = os.environ.get("CHAOS_SMTP_PASS", "")
from_addr = os.environ["CHAOS_ALERT_EMAIL_FROM"]
to_addr_raw = os.environ["CHAOS_ALERT_EMAIL_TO"]
use_tls = os.environ.get("CHAOS_SMTP_TLS", "1") == "1"

to_addrs = [item.strip() for item in to_addr_raw.split(",") if item.strip()]
if not to_addrs:
    raise SystemExit("empty recipient list")

msg = EmailMessage()
msg["Subject"] = subject
msg["From"] = from_addr
msg["To"] = ", ".join(to_addrs)
msg.set_content(body)

if port == 465 and use_tls:
    smtp = smtplib.SMTP_SSL(host, port, timeout=20)
else:
    smtp = smtplib.SMTP(host, port, timeout=20)

try:
    smtp.ehlo()
    if use_tls and port != 465:
        smtp.starttls(context=ssl.create_default_context())
        smtp.ehlo()
    if user:
        smtp.login(user, password)
    smtp.send_message(msg)
finally:
    try:
        smtp.quit()
    except Exception:
        pass
PY
    then
        [[ -n "$dedup_key" ]] && LAST_ALERT_KEY="$dedup_key"
        log "alert email sent: ${subject}"
        return 0
    fi

    log "WARNING: failed to send alert email: ${subject}"
    return 1
}

send_core_alert() {
    local reason="$1"
    local idx="$2"
    local detail="${3:-}"
    local host short_host ts subject body dedup_key

    if ! alert_enabled; then
        return 0
    fi

    host="$(hostname -f 2>/dev/null || hostname)"
    short_host="$(hostname 2>/dev/null || echo unknown-host)"
    ts="$(date '+%F %T')"
    subject="[FalconFS Chaos][CORE][${short_host}] ${reason}"
    dedup_key="core:${reason}:${idx}:${detail}"

    body="time=${ts}
host=${host}
reason=${reason}
index=${idx}
detail=${detail}
compose_file=${COMPOSE_FILE}
data_path=${DATA_PATH}
run_log_file=${RUN_LOG_FILE}
report_file=${REPORT_FILE}
diag_dir=${DIAG_DIR}
core_once_enable=${CORE_ONCE_ENABLE}
core_limit_kb=${CORE_LIMIT_KB}"

    send_email_alert "$subject" "$body" "$dedup_key"
}

setup_auto_logging() {
    if [[ "$NO_AUTO_LOG" == "1" ]]; then
        return
    fi
    mkdir -p "$(dirname "$RUN_LOG_FILE")"
    : >"$RUN_LOG_FILE"
    exec > >(tee -a "$RUN_LOG_FILE") 2>&1
}

copy_file_with_sudo() {
    local src="$1"
    local dst="$2"
    if sudo test -f "$src" >/dev/null 2>&1; then
        sudo cp "$src" "$dst" >/dev/null 2>&1 || true
    fi
}

collect_failure_snapshot() {
    local reason="$1"
    local idx="$2"
    local ts out_dir container latest_log node
    ts="$(date +%Y%m%d_%H%M%S)"
    out_dir="${DIAG_DIR}/fail_${idx}_${ts}"
    mkdir -p "$out_dir"

    log "collecting diagnostics to ${out_dir} (reason: ${reason})"

    docker ps -a --format '{{.Names}}\t{{.Status}}\t{{.Image}}' >"${out_dir}/docker_ps.txt" 2>&1 || true
    docker network ls >"${out_dir}/docker_network_ls.txt" 2>&1 || true
    docker exec falcon-zk-1 zkCli.sh -server localhost:2181 ls /falcon >"${out_dir}/zk_falcon_ls.txt" 2>&1 || true
    docker exec falcon-zk-1 zkCli.sh -server localhost:2181 ls /falcon/need_supplement >"${out_dir}/zk_need_supplement_ls.txt" 2>&1 || true
    docker exec falcon-zk-1 zkCli.sh -server localhost:2181 ls /falcon/falcon_clusters >"${out_dir}/zk_clusters_ls.txt" 2>&1 || true

    local clusters cluster
    local -a cluster_names
    clusters="$(zk_ls "/falcon/falcon_clusters")"
    IFS=',' read -r -a cluster_names <<<"$clusters"
    for cluster in "${cluster_names[@]}"; do
        [[ -z "$cluster" ]] && continue
        docker exec falcon-zk-1 zkCli.sh -server localhost:2181 ls "/falcon/falcon_clusters/${cluster}/replicas" >"${out_dir}/zk_${cluster}_replicas_ls.txt" 2>&1 || true
    done

    while read -r container; do
        [[ -z "$container" ]] && continue
        docker logs --tail 500 "$container" >"${out_dir}/${container}.docker.log" 2>&1 || true
    done < <(docker ps -a --format '{{.Names}}' | grep '^falcon-' || true)

    for node in cn-1 cn-2 cn-3 dn-1 dn-2 dn-3; do
        copy_file_with_sudo "${DATA_PATH}/falcon-data/${node}/data/start.log" "${out_dir}/${node}.start.log"
        copy_file_with_sudo "${DATA_PATH}/falcon-data/${node}/data/cmlog" "${out_dir}/${node}.cmlog"
        copy_file_with_sudo "${DATA_PATH}/falcon-data/${node}/data/metadata/postgresql.conf" "${out_dir}/${node}.postgresql.conf"
        copy_file_with_sudo "${DATA_PATH}/falcon-data/${node}/data/metadata/postgresql.auto.conf" "${out_dir}/${node}.postgresql.auto.conf"

        latest_log="$(sudo bash -lc "ls -1t '${DATA_PATH}/falcon-data/${node}/data/metadata/log'/postgresql-*.log 2>/dev/null | head -n 1" || true)"
        if [[ -n "$latest_log" ]]; then
            sudo tail -n 500 "$latest_log" >"${out_dir}/${node}.postgresql.tail.log" 2>/dev/null || true
        fi
    done

    # Make diagnostics readable for current user.
    sudo chown -R "$(id -u):$(id -g)" "${out_dir}" >/dev/null 2>&1 || true
    chmod -R u+rwX,go+rX "${out_dir}" >/dev/null 2>&1 || true

    log "diagnostics collected: ${out_dir}"
}

log_core_ulimit() {
    local cn_core dn_core
    cn_core="$(docker exec falcon-cn-1 bash -lc 'ulimit -c' 2>/dev/null || true)"
    dn_core="$(docker exec falcon-dn-1 bash -lc 'ulimit -c' 2>/dev/null || true)"
    log "core ulimit inside containers: cn1=${cn_core:-unknown}, dn1=${dn_core:-unknown}"
    if [[ "$CORE_ONCE_ENABLE" == "1" ]]; then
        if [[ "${cn_core}" == "0" || "${dn_core}" == "0" ]]; then
            log "ERROR: core-once enabled but core ulimit is 0"
            return 1
        fi
    else
        if [[ "${cn_core}" != "0" || "${dn_core}" != "0" ]]; then
            log "ERROR: core ulimit is not 0, aborting to avoid disk blowup"
            return 1
        fi
    fi
    return 0
}

disable_core_dump_runtime() {
    local container
    while read -r container; do
        [[ -z "$container" ]] && continue
        docker exec "$container" bash -lc 'for p in $(pgrep -x postgres); do prlimit --pid "$p" --core=0:0 >/dev/null 2>&1 || true; done' >/dev/null 2>&1 || true
    done < <(docker ps --format '{{.Names}}' | grep -E '^falcon-(cn|dn)-' || true)
}

enable_core_dump_runtime() {
    local container
    while read -r container; do
        [[ -z "$container" ]] && continue
        docker exec "$container" bash -lc "for p in \$(pgrep -x postgres); do prlimit --pid \"\$p\" --core=${CORE_LIMIT_KB}:${CORE_LIMIT_KB} >/dev/null 2>&1 || true; done" >/dev/null 2>&1 || true
    done < <(docker ps --format '{{.Names}}' | grep -E '^falcon-(cn|dn)-' || true)
}

stop_core_once_guard() {
    if [[ -n "${CORE_GUARD_PID}" ]]; then
        kill "${CORE_GUARD_PID}" >/dev/null 2>&1 || true
        wait "${CORE_GUARD_PID}" >/dev/null 2>&1 || true
        CORE_GUARD_PID=""
    fi
}

start_core_once_guard() {
    if [[ "$CORE_ONCE_ENABLE" != "1" ]]; then
        return
    fi

    if [[ -z "$CORE_ONCE_DIR" ]]; then
        CORE_ONCE_DIR="${DATA_PATH}/core_once"
    else
        CORE_ONCE_DIR="$(resolve_path "$CORE_ONCE_DIR")"
    fi
    mkdir -p "$CORE_ONCE_DIR"

    sudo rm -f /tmp/core-postgres-* >/dev/null 2>&1 || true
    log "core-once mode enabled, waiting for first core in /tmp/core-postgres-*"
    log "host core_pattern: $(cat /proc/sys/kernel/core_pattern 2>/dev/null || echo unknown)"

    (
        while true; do
            core_file="$(sudo bash -lc 'ls -1t /tmp/core-postgres-* 2>/dev/null | head -n 1' || true)"
            if [[ -n "$core_file" ]]; then
                ts="$(date +%Y%m%d_%H%M%S)"
                dst="${CORE_ONCE_DIR}/$(basename "$core_file").${ts}"
                sudo cp "$core_file" "$dst" >/dev/null 2>&1 || true
                sudo chown "$(id -u):$(id -g)" "$dst" >/dev/null 2>&1 || true
                chmod u+rw,go+r "$dst" >/dev/null 2>&1 || true
                echo "[$(date '+%F %T')] core-once captured: ${dst}" >>"${RUN_LOG_FILE}"
                send_core_alert "core_once_captured" "-1" "core_file=${dst}" || true
                disable_core_dump_runtime
                echo "[$(date '+%F %T')] core-once guard disabled further core dumps" >>"${RUN_LOG_FILE}"
                break
            fi
            sleep 2
        done
    ) &
    CORE_GUARD_PID=$!
    log "core-once guard pid: ${CORE_GUARD_PID}"
}

ensure_sudo_ready() {
    if sudo -n true >/dev/null 2>&1; then
        return 0
    fi
    log "ERROR: passwordless sudo is required for mount/data cleanup"
    log "Please configure NOPASSWD in /etc/sudoers, then rerun"
    exit 1
}

compose() {
    local default_core_limit="0"
    if [[ "$CORE_ONCE_ENABLE" == "1" ]]; then
        default_core_limit="$CORE_LIMIT_KB"
    fi

    FALCON_DATA_PATH="${DATA_PATH}" \
    FALCON_CODE_PATH="${REPO_ROOT}" \
    FALCON_FULL_IMAGE="${FALCON_FULL_IMAGE:-$DEFAULT_IMAGE}" \
    FALCON_ALLOW_CORE_DUMP="${FALCON_ALLOW_CORE_DUMP:-$CORE_ONCE_ENABLE}" \
    FALCON_CORE_LIMIT="${FALCON_CORE_LIMIT:-$default_core_limit}" \
    docker-compose -f "${COMPOSE_FILE}" "$@"
}

zk_ls() {
    local path="$1"
    local output line list
    output="$(docker exec falcon-zk-1 zkCli.sh -server localhost:2181 ls "$path" 2>&1 || true)"
    line="$(printf '%s\n' "$output" | grep -E '^\[[^][]*\]$' | tail -n 1 || true)"
    list="$(printf '%s\n' "$line" | sed -n 's/.*\[\(.*\)\].*/\1/p' | tr -d ' ')"
    printf '%s' "$list"
}

zk_has_child() {
    local path="$1"
    local child="$2"
    local list
    list="$(zk_ls "$path")"
    [[ ",${list}," == *",${child},"* ]]
}

zk_get_ip() {
    local path="$1"
    local output
    output="$(docker exec falcon-zk-1 zkCli.sh -server localhost:2181 get "$path" 2>&1 || true)"
    printf '%s\n' "$output" | grep -Eo '([0-9]{1,3}\.){3}[0-9]{1,3}:5432' | tail -n 1 | sed 's/:5432$//' || true
}

get_cn_leader_ip() {
    zk_get_ip "/falcon/leaders/cn"
}

get_dn_leader_map() {
    local leaders name ip
    leaders="$(zk_ls "/falcon/leaders")"
    IFS=',' read -r -a names <<<"$leaders"
    for name in "${names[@]}"; do
        if [[ "$name" == dn* ]]; then
            ip="$(zk_get_ip "/falcon/leaders/${name}")"
            if [[ -n "$ip" ]]; then
                echo "${name}=${ip}"
            fi
        fi
    done | sort
}

dn_leader_snapshot() {
    local lines
    lines="$(get_dn_leader_map | tr '\n' ',' | sed 's/,$//')"
    printf '%s' "$lines"
}

container_ip() {
    local container="$1"
    docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$container" 2>/dev/null || true
}

list_containers() {
    local prefix="$1"
    docker ps --format '{{.Names}}' | grep -E "^${prefix}" || true
}

find_container_by_ip() {
    local prefix="$1"
    local ip="$2"
    local name node_ip
    while read -r name; do
        [[ -z "$name" ]] && continue
        node_ip="$(container_ip "$name")"
        if [[ "$node_ip" == "$ip" ]]; then
            echo "$name"
            return 0
        fi
    done < <(list_containers "$prefix")
    return 1
}

check_liveness() {
    local container="$1"
    local script=""
    if [[ "$container" == falcon-cn-* ]]; then
        script="/usr/local/falconfs/falcon_cn/check_liveness.sh"
    elif [[ "$container" == falcon-dn-* ]]; then
        script="/usr/local/falconfs/falcon_dn/check_liveness.sh"
    elif [[ "$container" == "falcon-store-1" ]]; then
        script="/usr/local/falconfs/falcon_store/check_liveness.sh"
    else
        return 0
    fi

    docker exec "$container" bash "$script" >/dev/null 2>&1
}

check_pg_ip_consistency() {
    local container="$1"
    local result
    result="$(docker exec "$container" bash -lc '
        conf=/usr/local/falconfs/data/metadata/postgresql.conf
        if [ ! -f "$conf" ]; then
            exit 0
        fi
        pod="$POD_IP"
        if [ -z "$pod" ]; then
            exit 0
        fi
        local_ip=$(grep -E "^falcon\.local_ip" "$conf" | awk -F"'\''" "{print \$2}" | tail -n 1)
        server_ip=$(grep -E "^falcon_communication\.server_ip" "$conf" | awk -F"'\''" "{print \$2}" | tail -n 1)
        if [ -n "$local_ip" ] && [ "$local_ip" != "$pod" ]; then
            echo "pod_ip=$pod local_ip=$local_ip server_ip=$server_ip"
            exit 2
        fi
        if [ -n "$server_ip" ] && [ "$server_ip" != "$pod" ]; then
            echo "pod_ip=$pod local_ip=$local_ip server_ip=$server_ip"
            exit 2
        fi
    ' 2>/dev/null || true)"

    if [[ -n "$result" ]]; then
        echo "$result"
        return 1
    fi
    return 0
}

mounted_fuse_count() {
    local host_target="${DATA_PATH}/falcon-data/store-1/data"
    docker exec falcon-store-1 bash -lc "mount -t fuse.falcon_client | awk '\$3==\"/mnt/data\" || \$3==\"${host_target}\" {n++} END {print n+0}'" 2>/dev/null | tr -d ' '
}

start_store_client() {
    docker exec falcon-store-1 bash -lc '
        has_proc=0
        has_mount=0
        if pgrep -x falcon_client >/dev/null 2>&1; then
            has_proc=1
        fi
        if mount -t fuse.falcon_client | awk "\$3==\"/mnt/data\" {found=1} END {exit found?0:1}" >/dev/null 2>&1; then
            has_mount=1
        fi

        if [ "$has_proc" -eq 1 ] && [ "$has_mount" -eq 1 ]; then
            exit 0
        fi

        if [ "$has_proc" -eq 1 ] && [ "$has_mount" -eq 0 ]; then
            pkill -x falcon_client >/dev/null 2>&1 || true
            sleep 1
        fi

        nohup /usr/local/falconfs/falcon_store/start.sh >/dev/null 2>&1 &
    ' >/dev/null 2>&1 || true
}

wait_cluster_ready() {
    local timeout="$1"
    local deadline now waited
    deadline=$(( $(date +%s) + timeout ))
    waited=0
    while true; do
        now=$(date +%s)
        if (( now >= deadline )); then
            return 1
        fi
        if zk_has_child "/" "falcon" && zk_has_child "/falcon" "ready"; then
            return 0
        fi
        log "cluster not ready yet, waited ${waited}s/${timeout}s"
        sleep 5
        waited=$((waited + 5))
    done
}

wait_store_mount() {
    local timeout="$1"
    local deadline now waited
    deadline=$(( $(date +%s) + timeout ))
    waited=0
    while true; do
        now=$(date +%s)
        if (( now >= deadline )); then
            return 1
        fi
        if [[ "$(mounted_fuse_count)" -ge 1 ]]; then
            return 0
        fi
        start_store_client
        log "store fuse mount not ready yet, waited ${waited}s/${timeout}s"
        sleep 5
        waited=$((waited + 5))
    done
}

join_errors() {
    local -n errs_ref=$1
    local out=""
    local e
    for e in "${errs_ref[@]}"; do
        if [[ -z "$out" ]]; then
            out="$e"
        else
            out="${out}; $e"
        fi
    done
    printf '%s' "$out"
}

health_check_core() {
    local errors=()
    local name
    HEALTH_ERROR_MSG=""

    if ! zk_has_child "/falcon" "ready"; then
        errors+=("/falcon/ready missing")
    fi

    while read -r name; do
        local ip_msg
        [[ -z "$name" ]] && continue
        if ! check_liveness "$name"; then
            errors+=("${name} liveness failed")
        fi
        if [[ "$SKIP_PG_IP_CHECK" != "1" ]]; then
            if ! ip_msg="$(check_pg_ip_consistency "$name")"; then
                errors+=("${name} pg ip mismatch (${ip_msg})")
            fi
        fi
    done < <(list_containers "falcon-cn-")

    while read -r name; do
        local ip_msg
        [[ -z "$name" ]] && continue
        if ! check_liveness "$name"; then
            errors+=("${name} liveness failed")
        fi
        if [[ "$SKIP_PG_IP_CHECK" != "1" ]]; then
            if ! ip_msg="$(check_pg_ip_consistency "$name")"; then
                errors+=("${name} pg ip mismatch (${ip_msg})")
            fi
        fi
    done < <(list_containers "falcon-dn-")

    if ! check_liveness "falcon-store-1"; then
        errors+=("falcon-store-1 liveness failed")
    fi

    if [[ "$(mounted_fuse_count)" -lt 1 ]]; then
        start_store_client
        sleep 2
        if [[ "$(mounted_fuse_count)" -lt 1 ]]; then
            errors+=("fuse mount missing")
        fi
    fi

    if (( ${#errors[@]} > 0 )); then
        HEALTH_ERROR_MSG="$(join_errors errors)"
        return 1
    fi
    return 0
}

meta_rw_probe() {
    local ip output line probe_rc probe_container
    SERVICE_OBSERVED="UNAVAILABLE"
    SERVICE_READ_OK=0
    SERVICE_WRITE_OK=0
    SERVICE_PROBE_ERROR=""
    SERVICE_PG_IN_RECOVERY=0
    SERVICE_TX_READ_ONLY="unknown"
    SERVICE_PROBE_REASON=""

    ip="$(get_cn_leader_ip)"
    if [[ -z "$ip" ]]; then
        SERVICE_PROBE_ERROR="cn leader ip missing"
        SERVICE_PROBE_REASON="leader_missing"
        return 1
    fi

    probe_container="$(pick_running_cn_container || true)"
    if [[ -z "$probe_container" ]]; then
        SERVICE_PROBE_ERROR="no running cn container for probe"
        SERVICE_PROBE_REASON="probe_container_missing"
        SERVICE_OBSERVED="UNAVAILABLE"
        return 1
    fi

    set +e
    output="$(timeout "${PROBE_TIMEOUT_SEC}s" docker exec -i "$probe_container" python3 - "$ip" <<'PY' 2>/dev/null
import sys
import time
import psycopg2

host = sys.argv[1]
state = "UNAVAILABLE"
read_ok = 0
write_ok = 0
error = ""
pg_in_recovery = 0
tx_read_only = "unknown"
reason = ""

def set_error(ex):
    msg = str(ex).replace("\n", " ").strip()
    return msg[:300]

try:
    conn = psycopg2.connect(host=host, port=5432, dbname="postgres", user="falconMeta", connect_timeout=3)
    conn.autocommit = False
    cur = conn.cursor()
    cur.execute("SET statement_timeout TO '3000ms'")
    cur.execute("SELECT 1")
    cur.fetchone()
    read_ok = 1

    cur.execute("SELECT pg_is_in_recovery()")
    rec = cur.fetchone()
    pg_in_recovery = 1 if rec and rec[0] else 0

    cur.execute("SHOW transaction_read_only")
    tx_row = cur.fetchone()
    if tx_row and tx_row[0] is not None:
        tx_read_only = str(tx_row[0]).strip().lower()

    probe_id = f"probe_{int(time.time()*1000)}"
    try:
        cur.execute("CREATE TABLE IF NOT EXISTS public.chaos_probe_rw(id text PRIMARY KEY, touched_at timestamptz NOT NULL)")
        cur.execute("INSERT INTO public.chaos_probe_rw(id, touched_at) VALUES (%s, now()) ON CONFLICT (id) DO UPDATE SET touched_at=EXCLUDED.touched_at", (probe_id,))
        conn.commit()
        write_ok = 1
        state = "RW"
        reason = "write_commit_ok"
        try:
            cur.execute("DELETE FROM public.chaos_probe_rw WHERE id=%s", (probe_id,))
            conn.commit()
        except Exception:
            conn.rollback()
    except Exception as ex:
        conn.rollback()
        error = set_error(ex)
        lowered = error.lower()
        if "in recovery mode" in lowered or "not yet accepting connections" in lowered:
            state = "RECOVERY"
            reason = "write_in_recovery"
        elif tx_read_only in ("on", "true", "1") or "read-only" in lowered or "read only" in lowered:
            state = "RO"
            reason = "write_read_only"
        elif "statement timeout" in lowered or "canceling statement due to statement timeout" in lowered or "synchronous" in lowered:
            state = "WRITE_BLOCKED"
            reason = "write_blocked"
        else:
            state = "WRITE_ERR"
            reason = "write_error"

    if state == "RW" and (pg_in_recovery == 1 or tx_read_only in ("on", "true", "1")):
        state = "RO"
        reason = "read_only_flag"

    cur.close()
    conn.close()
except Exception as ex:
    error = set_error(ex)
    lowered = error.lower()
    if "in recovery mode" in lowered or "not yet accepting connections" in lowered:
        state = "RECOVERY"
        reason = "connect_in_recovery"
    else:
        state = "UNAVAILABLE"
        reason = "connect_failed"

if not reason:
    if state == "RO":
        reason = "read_only"
    elif state == "RECOVERY":
        reason = "in_recovery"
    elif state == "RW":
        reason = "rw"
    elif state == "WRITE_BLOCKED":
        reason = "write_blocked"
    elif state == "WRITE_ERR":
        reason = "write_error"
    else:
        reason = "unavailable"

print(f"STATE={state}")
print(f"READ_OK={read_ok}")
print(f"WRITE_OK={write_ok}")
print(f"PG_IN_RECOVERY={pg_in_recovery}")
print(f"TX_READ_ONLY={tx_read_only}")
print(f"REASON={reason}")
print(f"ERROR={error}")
PY
)"
    probe_rc=$?
    set -e

    if (( probe_rc == 124 || probe_rc == 137 )); then
        SERVICE_OBSERVED="WRITE_BLOCKED"
        SERVICE_READ_OK=0
        SERVICE_WRITE_OK=0
        SERVICE_PG_IN_RECOVERY=0
        SERVICE_TX_READ_ONLY="unknown"
        SERVICE_PROBE_REASON="probe_timeout"
        SERVICE_PROBE_ERROR="probe timeout after ${PROBE_TIMEOUT_SEC}s (likely sync commit wait)"
        return 0
    fi

    if (( probe_rc != 0 )) && [[ -z "$output" ]]; then
        SERVICE_OBSERVED="UNAVAILABLE"
        SERVICE_PROBE_REASON="probe_exec_failed"
        SERVICE_PROBE_ERROR="probe command failed rc=${probe_rc}"
        return 1
    fi

    while IFS= read -r line; do
        case "$line" in
            STATE=*) SERVICE_OBSERVED="${line#STATE=}" ;;
            READ_OK=*) SERVICE_READ_OK="${line#READ_OK=}" ;;
            WRITE_OK=*) SERVICE_WRITE_OK="${line#WRITE_OK=}" ;;
            PG_IN_RECOVERY=*) SERVICE_PG_IN_RECOVERY="${line#PG_IN_RECOVERY=}" ;;
            TX_READ_ONLY=*) SERVICE_TX_READ_ONLY="${line#TX_READ_ONLY=}" ;;
            REASON=*) SERVICE_PROBE_REASON="${line#REASON=}" ;;
            ERROR=*) SERVICE_PROBE_ERROR="${line#ERROR=}" ;;
        esac
    done <<<"$output"

    [[ "$SERVICE_OBSERVED" == "RW" || "$SERVICE_OBSERVED" == "RO" || "$SERVICE_OBSERVED" == "RECOVERY" || "$SERVICE_OBSERVED" == "WRITE_BLOCKED" ]]
}

fuse_rw_probe() {
    if docker exec falcon-store-1 bash -lc 'set -e; p=/mnt/data/.chaos_probe_$$; printf "ok" > "$p"; cat "$p" >/dev/null; rm -f "$p"' >/dev/null 2>&1; then
        SERVICE_FUSE_OK=1
        return 0
    fi
    SERVICE_FUSE_OK=0
    return 1
}

check_service_state() {
    local rc=0
    if ! meta_rw_probe; then
        rc=1
    fi

    if [[ "$PROBE_MODE" == "meta+fuse" ]]; then
        if ! fuse_rw_probe; then
            if [[ -z "$SERVICE_PROBE_ERROR" ]]; then
                SERVICE_PROBE_ERROR="fuse probe failed"
            else
                SERVICE_PROBE_ERROR="${SERVICE_PROBE_ERROR}; fuse probe failed"
            fi
            rc=1
        fi
    fi

    return $rc
}

zk_child_count() {
    local path="$1"
    local list count
    local -a items
    list="$(zk_ls "$path")"
    if [[ -z "$list" ]]; then
        echo 0
        return 0
    fi
    IFS=',' read -r -a items <<<"$list"
    count=0
    for item in "${items[@]}"; do
        [[ -n "$item" ]] && count=$((count + 1))
    done
    echo "$count"
}

list_cluster_names() {
    local list name
    local -a names
    list="$(zk_ls "/falcon/falcon_clusters")"
    IFS=',' read -r -a names <<<"$list"
    for name in "${names[@]}"; do
        [[ -n "$name" ]] && echo "$name"
    done
}

check_reliability_restored() {
    local errors=()
    local cluster count
    local seen_cluster=0
    RELIABILITY_ERROR_MSG=""

    if [[ "$TOPOLOGY_RESOLVED" == "single" ]]; then
        RELIABILITY_RESTORED=1
        return 0
    fi

    if zk_has_child "/falcon" "need_supplement"; then
        if [[ -n "$(zk_ls "/falcon/need_supplement")" ]]; then
            NEED_SUPPLEMENT_SEEN=1
        fi
    fi

    while read -r cluster; do
        [[ -z "$cluster" ]] && continue
        seen_cluster=1
        count="$(zk_child_count "/falcon/falcon_clusters/${cluster}/replicas")"
        if ! is_nonneg_integer "$count"; then
            errors+=("${cluster} replicas count invalid")
            continue
        fi
        if (( count < REPLICA_TARGET )); then
            errors+=("${cluster} replicas ${count}/${REPLICA_TARGET}")
        fi
    done < <(list_cluster_names)

    if (( seen_cluster == 0 )); then
        errors+=("no clusters found under /falcon/falcon_clusters")
    fi

    if (( ${#errors[@]} > 0 )); then
        RELIABILITY_RESTORED=0
        RELIABILITY_ERROR_MSG="$(join_errors errors)"
        return 1
    fi

    RELIABILITY_RESTORED=1
    return 0
}

health_check() {
    if ! health_check_core; then
        return 1
    fi
    if ! check_service_state; then
        HEALTH_ERROR_MSG="service probe failed: state=${SERVICE_OBSERVED}, reason=${SERVICE_PROBE_REASON}, error=${SERVICE_PROBE_ERROR}"
        return 1
    fi
    if [[ "$SERVICE_OBSERVED" != "RW" ]]; then
        HEALTH_ERROR_MSG="service not RW: ${SERVICE_OBSERVED}, reason=${SERVICE_PROBE_REASON}, tx_read_only=${SERVICE_TX_READ_ONLY}, in_recovery=${SERVICE_PG_IN_RECOVERY}, error=${SERVICE_PROBE_ERROR}"
        return 1
    fi
    if ! check_reliability_restored; then
        HEALTH_ERROR_MSG="reliability not restored: ${RELIABILITY_ERROR_MSG}"
        return 1
    fi
    return 0
}

prepare_data_dirs() {
    local idx
    sudo umount -l "${DATA_PATH}/falcon-data/store-1/data" >/dev/null 2>&1 || true

    for idx in 1 2 3; do
        mkdir -p "${DATA_PATH}/falcon-data/zk-${idx}/data"
    done

    for idx in 1 2 3; do
        mkdir -p "${DATA_PATH}/falcon-data/cn-${idx}/data"
    done

    for idx in 1 2 3; do
        mkdir -p "${DATA_PATH}/falcon-data/dn-${idx}/data"
    done

    mkdir -p "${DATA_PATH}/falcon-data/store-1/cache"
    mkdir -p "${DATA_PATH}/falcon-data/store-1/log"
    mkdir -p "${DATA_PATH}/falcon-data/store-1/data"
}

clean_data_dirs() {
    local idx
    sudo umount -l "${DATA_PATH}/falcon-data/store-1/data" >/dev/null 2>&1 || true

    for idx in 1 2 3; do
        sudo rm -rf "${DATA_PATH}/falcon-data/zk-${idx}/data"/* >/dev/null 2>&1 || true
    done

    for idx in 1 2 3; do
        sudo rm -rf "${DATA_PATH}/falcon-data/cn-${idx}/data"/* >/dev/null 2>&1 || true
    done

    for idx in 1 2 3; do
        sudo rm -rf "${DATA_PATH}/falcon-data/dn-${idx}/data"/* >/dev/null 2>&1 || true
    done

    sudo rm -rf "${DATA_PATH}/falcon-data/store-1/cache"/* >/dev/null 2>&1 || true
    sudo rm -rf "${DATA_PATH}/falcon-data/store-1/log"/* >/dev/null 2>&1 || true
    sudo rm -rf "${DATA_PATH}/falcon-data/store-1/data"/* >/dev/null 2>&1 || true
}

restart_container() {
    local name="$1"
    docker restart "$name" >/dev/null
}

reset_store_mountpoint() {
    sudo umount -l "${DATA_PATH}/falcon-data/store-1/data" >/dev/null 2>&1 || true
    sudo mkdir -p "${DATA_PATH}/falcon-data/store-1/data" >/dev/null 2>&1 || true
}

ACTION_TARGET=""

action_s01() {
    local ip target
    ip="$(get_cn_leader_ip)"
    [[ -z "$ip" ]] && return 1
    target="$(find_container_by_ip "falcon-cn-" "$ip" || true)"
    [[ -z "$target" ]] && return 1
    restart_container "$target"
    ACTION_TARGET="$target"
}

action_s01c2() {
    restart_container "falcon-cn-2"
    ACTION_TARGET="falcon-cn-2"
}

action_s02() {
    local entries count idx entry leader_name ip target
    mapfile -t entries < <(get_dn_leader_map)
    count=${#entries[@]}
    (( count == 0 )) && return 1
    idx=$(( RANDOM % count ))
    entry="${entries[$idx]}"
    leader_name="${entry%%=*}"
    ip="${entry#*=}"
    target="$(find_container_by_ip "falcon-dn-" "$ip" || true)"
    [[ -z "$target" ]] && return 1
    restart_container "$target"
    ACTION_TARGET="${leader_name}:${target}"
}

action_s03() {
    local -a dn_nodes followers leader_ips
    local name ip target count idx

    mapfile -t dn_nodes < <(list_containers "falcon-dn-")
    (( ${#dn_nodes[@]} == 0 )) && return 1

    while read -r line; do
        [[ -z "$line" ]] && continue
        leader_ips+=("${line#*=}")
    done < <(get_dn_leader_map)

    for name in "${dn_nodes[@]}"; do
        ip="$(container_ip "$name")"
        if [[ -n "$ip" ]]; then
            local is_leader=0
            local lip
            for lip in "${leader_ips[@]:-}"; do
                if [[ "$lip" == "$ip" ]]; then
                    is_leader=1
                    break
                fi
            done
            if (( is_leader == 0 )); then
                followers+=("$name")
            fi
        fi
    done

    if (( ${#followers[@]} > 0 )); then
        count=${#followers[@]}
        idx=$(( RANDOM % count ))
        target="${followers[$idx]}"
    else
        count=${#dn_nodes[@]}
        idx=$(( RANDOM % count ))
        target="${dn_nodes[$idx]}"
    fi

    restart_container "$target"
    ACTION_TARGET="$target"
}

action_s04() {
    reset_store_mountpoint
    if ! restart_container "falcon-store-1"; then
        reset_store_mountpoint
        compose up -d store1
    fi
    sleep 2
    start_store_client
    ACTION_TARGET="falcon-store-1"
}

action_s05() {
    docker exec falcon-store-1 bash -lc 'pkill -f /usr/local/falconfs/falcon_client/bin/falcon_client || true' >/dev/null 2>&1
    ACTION_TARGET="falcon-store-1"
}

hold_and_observe_dual_ro() {
    local action_id="$1"
    local started now remain step
    DUAL_RO_SEEN_ACTION=0
    started=$(date +%s)
    while true; do
        now=$(date +%s)
        remain=$((SUPPLEMENT_HOLD_SEC - (now - started)))
        if (( remain <= 0 )); then
            break
        fi

        if [[ "$TOPOLOGY_RESOLVED" == "dual" ]] && is_metadata_action "$action_id"; then
            check_service_state || true
            if service_state_is_degraded "$SERVICE_OBSERVED"; then
                if (( DUAL_RO_SEEN_ACTION == 0 )); then
                    log "dual degraded state observed during ${action_id}: state=${SERVICE_OBSERVED}, reason=${SERVICE_PROBE_REASON}, error=${SERVICE_PROBE_ERROR}"
                fi
                DUAL_RO_SEEN_ACTION=1
            fi
        fi
        if (( remain < HEALTH_CHECK_INTERVAL_SEC )); then
            step=$remain
        else
            step=$HEALTH_CHECK_INTERVAL_SEC
        fi
        sleep "$step"
    done
}

service_state_is_degraded() {
    case "$1" in
        RO|RECOVERY|WRITE_BLOCKED) return 0 ;;
        *) return 1 ;;
    esac
}

action_s06() {
    local ip target
    ip="$(get_cn_leader_ip)"
    [[ -z "$ip" ]] && return 1
    target="$(find_container_by_ip "falcon-cn-" "$ip" || true)"
    [[ -z "$target" ]] && return 1

    docker stop "$target" >/dev/null
    log "${target} stopped, hold ${SUPPLEMENT_HOLD_SEC}s for supplement observation"
    hold_and_observe_dual_ro "S06"
    docker start "$target" >/dev/null
    ACTION_TARGET="${target}:hold=${SUPPLEMENT_HOLD_SEC}s"
}

action_s07() {
    local -a dn_nodes followers leader_ips
    local name ip target count idx

    mapfile -t dn_nodes < <(list_containers "falcon-dn-")
    (( ${#dn_nodes[@]} == 0 )) && return 1

    while read -r line; do
        [[ -z "$line" ]] && continue
        leader_ips+=("${line#*=}")
    done < <(get_dn_leader_map)

    for name in "${dn_nodes[@]}"; do
        ip="$(container_ip "$name")"
        if [[ -n "$ip" ]]; then
            local is_leader=0
            local lip
            for lip in "${leader_ips[@]:-}"; do
                if [[ "$lip" == "$ip" ]]; then
                    is_leader=1
                    break
                fi
            done
            if (( is_leader == 0 )); then
                followers+=("$name")
            fi
        fi
    done

    if (( ${#followers[@]} > 0 )); then
        count=${#followers[@]}
        idx=$(( RANDOM % count ))
        target="${followers[$idx]}"
    else
        count=${#dn_nodes[@]}
        idx=$(( RANDOM % count ))
        target="${dn_nodes[$idx]}"
    fi

    docker stop "$target" >/dev/null
    log "${target} stopped, hold ${SUPPLEMENT_HOLD_SEC}s for supplement observation"
    hold_and_observe_dual_ro "S07"
    docker start "$target" >/dev/null
    ACTION_TARGET="${target}:hold=${SUPPLEMENT_HOLD_SEC}s"
}

is_metadata_action() {
    case "$1" in
        S01|S01C2|S02|S03|S06|S07) return 0 ;;
        *) return 1 ;;
    esac
}

action_requires_reliability_restore() {
    local action_id="$1"
    if [[ "$TOPOLOGY_RESOLVED" == "single" ]]; then
        return 1
    fi
    is_metadata_action "$action_id"
}

expect_dual_ro_observation() {
    local action_id="$1"
    if [[ "$TOPOLOGY_RESOLVED" != "dual" ]]; then
        return 1
    fi
    if ! is_metadata_action "$action_id"; then
        return 1
    fi
    if [[ "$action_id" == "S06" || "$action_id" == "S07" || "$STRICT_DUAL_RO_CHECK" == "1" ]]; then
        return 0
    fi
    return 1
}

wait_recovery() {
    local action_id="$1"
    local started deadline now elapsed healthy_since
    local service_deadline service_healthy_since
    local require_ro_observed saw_ro required_reliability

    started=$(date +%s)
    deadline=$(( started + HEALTH_TIMEOUT_SEC ))
    RECOVER_SECONDS=0
    RECOVERY_ERROR=""
    STAGE_FAILED=""
    healthy_since=0
    service_healthy_since=0
    require_ro_observed=0
    saw_ro=$DUAL_RO_SEEN_ACTION
    required_reliability=0

    SERVICE_EXPECTED="RW"
    SERVICE_OBSERVED=""
    SERVICE_PROBE_ERROR=""
    SERVICE_PG_IN_RECOVERY=0
    SERVICE_TX_READ_ONLY="unknown"
    SERVICE_PROBE_REASON=""
    RELIABILITY_RESTORED=0
    RELIABILITY_ERROR_MSG=""

    if expect_dual_ro_observation "$action_id"; then
        require_ro_observed=1
        SERVICE_EXPECTED="DEGRADED->RW"
    elif [[ "$TOPOLOGY_RESOLVED" == "dual" ]] && is_metadata_action "$action_id"; then
        SERVICE_EXPECTED="RW(degraded may appear)"
    fi

    if action_requires_reliability_restore "$action_id"; then
        required_reliability=1
    fi

    if (( POST_ACTION_GRACE_SEC > 0 )); then
        log "post-action grace sleep ${POST_ACTION_GRACE_SEC}s for ${action_id}"
        sleep "$POST_ACTION_GRACE_SEC"
    fi

    while true; do
        now=$(date +%s)
        elapsed=$(( now - started ))
        if (( now >= deadline )); then
            STAGE_FAILED="core"
            RECOVER_SECONDS=$HEALTH_TIMEOUT_SEC
            RECOVERY_ERROR="$HEALTH_ERROR_MSG"
            return 1
        fi
        if health_check_core; then
            if (( healthy_since == 0 )); then
                healthy_since=$now
                log "core health first-pass for ${action_id}, waiting stable window ${STABLE_WINDOW_SEC}s"
            fi
            if (( now - healthy_since >= STABLE_WINDOW_SEC )); then
                break
            fi
            sleep "$HEALTH_CHECK_INTERVAL_SEC"
            continue
        fi
        healthy_since=0
        RECOVERY_ERROR="$HEALTH_ERROR_MSG"
        log "core recovery pending for ${action_id}, ${elapsed}s/${HEALTH_TIMEOUT_SEC}s: ${RECOVERY_ERROR}"
        if [[ "$action_id" == "S04" || "$action_id" == "S05" ]]; then
            start_store_client
        fi
        sleep "$HEALTH_CHECK_INTERVAL_SEC"
    done

    service_deadline=$(( $(date +%s) + HEALTH_TIMEOUT_SEC ))
    while true; do
        now=$(date +%s)
        if (( now >= service_deadline )); then
            STAGE_FAILED="service"
            RECOVERY_ERROR="service probe timeout: expected=${SERVICE_EXPECTED}, observed=${SERVICE_OBSERVED}, reason=${SERVICE_PROBE_REASON}, tx_read_only=${SERVICE_TX_READ_ONLY}, in_recovery=${SERVICE_PG_IN_RECOVERY}, error=${SERVICE_PROBE_ERROR}"
            RECOVER_SECONDS=$(( now - started ))
            return 1
        fi

        check_service_state || true
        if service_state_is_degraded "$SERVICE_OBSERVED"; then
            saw_ro=1
        fi

        if [[ "$SERVICE_OBSERVED" == "RW" ]]; then
            if (( service_healthy_since == 0 )); then
                service_healthy_since=$now
                log "service reached RW for ${action_id}, waiting stable window ${STABLE_WINDOW_SEC}s"
            fi
            if (( now - service_healthy_since >= STABLE_WINDOW_SEC )); then
                break
            fi
        else
            service_healthy_since=0
            log "service recovery pending for ${action_id}: observed=${SERVICE_OBSERVED}, expected=${SERVICE_EXPECTED}, reason=${SERVICE_PROBE_REASON}, tx_read_only=${SERVICE_TX_READ_ONLY}, in_recovery=${SERVICE_PG_IN_RECOVERY}, error=${SERVICE_PROBE_ERROR}"
        fi

        if [[ "$action_id" == "S04" || "$action_id" == "S05" ]]; then
            start_store_client
        fi
        sleep "$HEALTH_CHECK_INTERVAL_SEC"
    done

    if (( require_ro_observed == 1 )) && (( saw_ro == 0 )); then
        STAGE_FAILED="service"
        RECOVERY_ERROR="dual mode expected degraded state (RO/RECOVERY/WRITE_BLOCKED) before RW, but none observed"
        RECOVER_SECONDS=$(( $(date +%s) - started ))
        return 1
    fi

    if (( required_reliability == 1 )); then
        deadline=$(( $(date +%s) + RELIABILITY_TIMEOUT_SEC ))
        while true; do
            now=$(date +%s)
            if (( now >= deadline )); then
                STAGE_FAILED="reliability"
                RECOVERY_ERROR="reliability timeout: ${RELIABILITY_ERROR_MSG}"
                RECOVER_SECONDS=$(( now - started ))
                return 1
            fi

            if check_reliability_restored; then
                RELIABILITY_RESTORED=1
                break
            fi
            log "reliability recovery pending for ${action_id}: ${RELIABILITY_ERROR_MSG}"
            sleep "$HEALTH_CHECK_INTERVAL_SEC"
        done
    else
        RELIABILITY_RESTORED=1
    fi

    RECOVER_SECONDS=$(( $(date +%s) - started ))
    RECOVERY_ERROR=""
    return 0
}

wait_initial_state() {
    local deadline now started last_log elapsed
    local core_ok service_ok reliability_ok
    started=$(date +%s)
    deadline=$(( started + STARTUP_TIMEOUT_SEC ))
    last_log=$started
    while true; do
        now=$(date +%s)
        if (( now >= deadline )); then
            return 1
        fi

        core_ok=0
        service_ok=0
        reliability_ok=0

        if health_check_core; then
            core_ok=1
        fi
        if check_service_state && [[ "$SERVICE_OBSERVED" == "RW" ]]; then
            service_ok=1
        fi
        if check_reliability_restored; then
            reliability_ok=1
        fi

        if (( core_ok == 1 && service_ok == 1 && reliability_ok == 1 )); then
            return 0
        fi

        HEALTH_ERROR_MSG="core_ok=${core_ok} service=${SERVICE_OBSERVED} reason=${SERVICE_PROBE_REASON} tx_read_only=${SERVICE_TX_READ_ONLY} in_recovery=${SERVICE_PG_IN_RECOVERY} reliability_ok=${reliability_ok} core_err=${HEALTH_ERROR_MSG} probe_err=${SERVICE_PROBE_ERROR} reliability_err=${RELIABILITY_ERROR_MSG}"
        elapsed=$((now - started))
        if (( now - last_log >= 30 )); then
            log "initial state pending ${elapsed}s/${STARTUP_TIMEOUT_SEC}s: ${HEALTH_ERROR_MSG}"
            last_log=$now
        fi
        sleep "$HEALTH_CHECK_INTERVAL_SEC"
    done
}

write_event_json() {
    local ts="$1"
    local idx="$2"
    local action_id="$3"
    local action_name="$4"
    local target="$5"
    local injected="$6"
    local recovered="$7"
    local recover_seconds="$8"
    local before_cn="$9"
    local before_dn="${10}"
    local after_cn="${11}"
    local after_dn="${12}"
    local error_msg="${13}"
    local topology="${14}"
    local service_expected="${15}"
    local service_observed="${16}"
    local read_ok="${17}"
    local write_ok="${18}"
    local probe_error="${19}"
    local reliability_restored="${20}"
    local need_supplement_seen="${21}"
    local stage_failed="${22}"
    local probe_mode="${23}"
    local fuse_ok="${24}"
    local pg_in_recovery="${25}"
    local tx_read_only="${26}"
    local probe_reason="${27}"

    python3 - "$REPORT_FILE" "$ts" "$idx" "$action_id" "$action_name" "$target" "$injected" "$recovered" "$recover_seconds" "$before_cn" "$before_dn" "$after_cn" "$after_dn" "$error_msg" "$topology" "$service_expected" "$service_observed" "$read_ok" "$write_ok" "$probe_error" "$reliability_restored" "$need_supplement_seen" "$stage_failed" "$probe_mode" "$fuse_ok" "$pg_in_recovery" "$tx_read_only" "$probe_reason" <<'PY'
import json
import sys

report = sys.argv[1]
event = {
    "ts": int(sys.argv[2]),
    "index": int(sys.argv[3]),
    "action_id": sys.argv[4],
    "action": sys.argv[5],
    "target": sys.argv[6],
    "injected": True if sys.argv[7] == "1" else False,
    "recovered": True if sys.argv[8] == "1" else False,
    "recover_seconds": int(sys.argv[9]),
    "leader_before": {
        "cn": sys.argv[10],
        "dn": sys.argv[11],
    },
    "leader_after": {
        "cn": sys.argv[12],
        "dn": sys.argv[13],
    },
    "error": sys.argv[14],
    "topology": sys.argv[15],
    "service_expected": sys.argv[16],
    "service_observed": sys.argv[17],
    "read_ok": True if sys.argv[18] == "1" else False,
    "write_ok": True if sys.argv[19] == "1" else False,
    "probe_error": sys.argv[20],
    "reliability_restored": True if sys.argv[21] == "1" else False,
    "need_supplement_seen": True if sys.argv[22] == "1" else False,
    "stage_failed": sys.argv[23],
    "probe_mode": sys.argv[24],
    "fuse_ok": True if sys.argv[25] == "1" else False,
    "pg_in_recovery": True if sys.argv[26] == "1" else False,
    "tx_read_only": sys.argv[27],
    "probe_reason": sys.argv[28],
}
with open(report, "a", encoding="utf-8") as f:
    f.write(json.dumps(event, ensure_ascii=True) + "\n")
PY
}

write_summary_json() {
    local passed="$1"
    local failed="$2"
    local failed_core="$3"
    local failed_service="$4"
    local failed_reliability="$5"
    python3 - "$REPORT_FILE" "$passed" "$failed" "$failed_core" "$failed_service" "$failed_reliability" "$TOPOLOGY_RESOLVED" <<'PY'
import json
import sys
import time

report = sys.argv[1]
passed = int(sys.argv[2])
failed = int(sys.argv[3])
failed_core = int(sys.argv[4])
failed_service = int(sys.argv[5])
failed_reliability = int(sys.argv[6])
topology = sys.argv[7]
event = {
    "ts": int(time.time()),
    "type": "summary",
    "topology": topology,
    "passed": passed,
    "failed": failed,
    "failed_core": failed_core,
    "failed_service": failed_service,
    "failed_reliability": failed_reliability,
    "total": passed + failed,
}
with open(report, "a", encoding="utf-8") as f:
    f.write(json.dumps(event, ensure_ascii=True) + "\n")
print(json.dumps(event, ensure_ascii=True))
PY
}

action_name() {
    case "$1" in
        S01) echo "restart cn leader" ;;
        S01C2) echo "restart cn-2 (fixed target)" ;;
        S02) echo "restart one dn leader" ;;
        S03) echo "restart random dn (prefer follower)" ;;
        S04) echo "restart store container" ;;
        S05) echo "kill falcon_client process in store" ;;
        S06) echo "stop cn leader then start after hold" ;;
        S07) echo "stop random dn then start after hold" ;;
        *) echo "unknown" ;;
    esac
}

run_action() {
    case "$1" in
        S01) action_s01 ;;
        S01C2) action_s01c2 ;;
        S02) action_s02 ;;
        S03) action_s03 ;;
        S04) action_s04 ;;
        S05) action_s05 ;;
        S06) action_s06 ;;
        S07) action_s07 ;;
        *) return 1 ;;
    esac
}

validate_actions() {
    local action
    IFS=',' read -r -a ACTION_IDS <<<"$ACTIONS_CSV"
    for action in "${ACTION_IDS[@]}"; do
        case "$action" in
            S01|S01C2|S02|S03|S04|S05|S06|S07) ;;
            *)
                echo "invalid action id: $action"
                exit 1
                ;;
        esac
    done
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

    validate_runtime_options

    COMPOSE_FILE="$(resolve_path "$COMPOSE_FILE")"
    if [[ ! -f "$COMPOSE_FILE" ]]; then
        log "ERROR: compose file not found: ${COMPOSE_FILE}"
        exit 1
    fi
    DATA_PATH="$(resolve_path "$DATA_PATH")"
    mkdir -p "$DATA_PATH"
    if [[ -z "$REPORT_FILE" ]]; then
        REPORT_FILE="${DATA_PATH}/chaos_report.jsonl"
    else
        REPORT_FILE="$(resolve_path "$REPORT_FILE")"
    fi
    if [[ -z "$RUN_LOG_FILE" ]]; then
        RUN_LOG_FILE="${DATA_PATH}/chaos_run.log"
    else
        RUN_LOG_FILE="$(resolve_path "$RUN_LOG_FILE")"
    fi
    if [[ -z "$DIAG_DIR" ]]; then
        DIAG_DIR="${DATA_PATH}/chaos_diag"
    else
        DIAG_DIR="$(resolve_path "$DIAG_DIR")"
    fi
    mkdir -p "$(dirname "$REPORT_FILE")"
    mkdir -p "$DIAG_DIR"

    setup_auto_logging
    trap stop_core_once_guard EXIT

    RANDOM=$(( SEED % 32767 ))
    validate_actions

    log "compose file: ${COMPOSE_FILE}"
    log "data path: ${DATA_PATH}"
    log "report file: ${REPORT_FILE}"
    log "run log file: ${RUN_LOG_FILE}"
    log "diag dir: ${DIAG_DIR}"
    log "actions: ${ACTIONS_CSV}"
    log "recovery policy: grace=${POST_ACTION_GRACE_SEC}s, stable_window=${STABLE_WINDOW_SEC}s, timeout=${HEALTH_TIMEOUT_SEC}s"
    log "reliability timeout: ${RELIABILITY_TIMEOUT_SEC}s"
    log "pg ip consistency check: $([[ "$SKIP_PG_IP_CHECK" == "1" ]] && echo disabled || echo enabled)"
    log "topology mode: ${TOPOLOGY}"
    log "probe mode: ${PROBE_MODE}"
    log "probe timeout: ${PROBE_TIMEOUT_SEC}s"
    log "strict dual ro check: $([[ "$STRICT_DUAL_RO_CHECK" == "1" ]] && echo enabled || echo disabled)"
    log "core-once mode: $([[ "$CORE_ONCE_ENABLE" == "1" ]] && echo enabled || echo disabled), core-limit-kb=${CORE_LIMIT_KB}"
    log "smtp core alert: $([[ "$CHAOS_ALERT_ENABLE" == "1" ]] && echo enabled || echo disabled)"

    ensure_sudo_ready

    prepare_data_dirs

    if [[ "$FRESH_START" == "1" ]]; then
        log "running docker-compose down before startup"
        compose down --remove-orphans || true
        log "fresh-start enabled, cleaning existing data directories"
        clean_data_dirs
    fi

    if [[ "$SKIP_START" == "0" ]]; then
        log "starting stack with docker-compose up -d --force-recreate"
        compose up -d --force-recreate
    fi

    if [[ "$CORE_ONCE_ENABLE" == "1" ]]; then
        log "applying runtime core limit ${CORE_LIMIT_KB}KB to postgres processes"
        enable_core_dump_runtime
    fi

    if ! log_core_ulimit; then
        send_core_alert "core_ulimit_check_failed" "0" "core_ulimit_not_zero" || true
        collect_failure_snapshot "core_ulimit_check_failed" 0
        exit 1
    fi

    start_core_once_guard

    log "waiting cluster ready"
    if ! wait_cluster_ready "$STARTUP_TIMEOUT_SEC"; then
        log "ERROR: cluster did not become ready"
        collect_failure_snapshot "cluster_not_ready" 0
        exit 1
    fi
    log "cluster ready"

    start_store_client
    log "waiting store fuse mount ready"
    if ! wait_store_mount "$STARTUP_TIMEOUT_SEC"; then
        log "ERROR: fuse.falcon_client mount not ready"
        collect_failure_snapshot "mount_not_ready" 0
        exit 1
    fi
    log "store fuse mount ready"

    if ! resolve_topology; then
        collect_failure_snapshot "resolve_topology_failed" 0
        exit 1
    fi
    resolve_supplement_hold_sec
    log "resolved topology: ${TOPOLOGY_RESOLVED} (replica_target=${REPLICA_TARGET})"
    log "supplement hold seconds: ${SUPPLEMENT_HOLD_SEC}"

    if ! wait_initial_state; then
        log "ERROR: initial health check failed: ${HEALTH_ERROR_MSG}"
        collect_failure_snapshot "initial_health_failed" 0
        exit 1
    fi
    log "initial health check passed"

    local end_ts now remain idx passed failed
    local failed_core failed_service failed_reliability
    local action_id action_desc before_cn before_dn after_cn after_dn
    local injected recovered inject_error error_msg
    end_ts=$(( $(date +%s) + DURATION_MIN * 60 ))
    idx=0
    passed=0
    failed=0
    failed_core=0
    failed_service=0
    failed_reliability=0

    while true; do
        now=$(date +%s)
        if (( now >= end_ts )); then
            break
        fi

        idx=$((idx + 1))
        action_id="${ACTION_IDS[$((RANDOM % ${#ACTION_IDS[@]}))]}"
        action_desc="$(action_name "$action_id")"
        before_cn="$(get_cn_leader_ip)"
        before_dn="$(dn_leader_snapshot)"

        log "[${idx}] inject ${action_id} ${action_desc}"

        injected=1
        recovered=0
        ACTION_TARGET=""
        inject_error=""
        error_msg=""
        STAGE_FAILED=""
        NEED_SUPPLEMENT_SEEN=0
        RELIABILITY_RESTORED=0
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

        if ! run_action "$action_id"; then
            injected=0
            inject_error="inject action failed"
            STAGE_FAILED="inject"
        fi

        if [[ "$injected" == "1" ]]; then
            if wait_recovery "$action_id"; then
                recovered=1
                error_msg=""
            else
                recovered=0
                error_msg="$RECOVERY_ERROR"
            fi
        else
            RECOVER_SECONDS=0
            error_msg="$inject_error"
        fi

        after_cn="$(get_cn_leader_ip)"
        after_dn="$(dn_leader_snapshot)"

        write_event_json \
            "$(date +%s)" \
            "$idx" \
            "$action_id" \
            "$action_desc" \
            "$ACTION_TARGET" \
            "$injected" \
            "$recovered" \
            "$RECOVER_SECONDS" \
            "$before_cn" \
            "$before_dn" \
            "$after_cn" \
            "$after_dn" \
            "$error_msg" \
            "$TOPOLOGY_RESOLVED" \
            "$SERVICE_EXPECTED" \
            "$SERVICE_OBSERVED" \
            "$SERVICE_READ_OK" \
            "$SERVICE_WRITE_OK" \
            "$SERVICE_PROBE_ERROR" \
            "$RELIABILITY_RESTORED" \
            "$NEED_SUPPLEMENT_SEEN" \
            "$STAGE_FAILED" \
            "$PROBE_MODE" \
            "$SERVICE_FUSE_OK" \
            "$SERVICE_PG_IN_RECOVERY" \
            "$SERVICE_TX_READ_ONLY" \
            "$SERVICE_PROBE_REASON"

        if [[ "$injected" == "1" && "$recovered" == "1" ]]; then
            passed=$((passed + 1))
            log "[${idx}] PASS in ${RECOVER_SECONDS}s"
        else
            failed=$((failed + 1))
            log "[${idx}] FAIL: ${error_msg}"
            case "$STAGE_FAILED" in
                core) failed_core=$((failed_core + 1)) ;;
                service) failed_service=$((failed_service + 1)) ;;
                reliability) failed_reliability=$((failed_reliability + 1)) ;;
            esac
            collect_failure_snapshot "action_${action_id}_failed" "$idx"
            if [[ "$FAIL_FAST" == "1" ]]; then
                break
            fi
        fi

        now=$(date +%s)
        remain=$((end_ts - now))
        if (( remain <= 0 )); then
            break
        fi
        if (( INTERVAL_SEC < remain )); then
            log "sleep ${INTERVAL_SEC}s before next action"
        else
            log "sleep ${remain}s before next action"
        fi
        if (( INTERVAL_SEC < remain )); then
            sleep "$INTERVAL_SEC"
        else
            sleep "$remain"
        fi
    done

    write_summary_json "$passed" "$failed" "$failed_core" "$failed_service" "$failed_reliability"

    if [[ "$TEARDOWN" == "1" ]]; then
        log "teardown enabled, running docker-compose down"
        compose down --remove-orphans || true
    fi

    log "chaos run finished"
}

main "$@"
