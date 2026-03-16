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
CORE_ONCE_ENABLE=0
CORE_LIMIT_KB=1048576
CORE_ONCE_DIR=""
CORE_GUARD_PID=""

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

check_replication() {
    docker exec -i falcon-cn-1 bash -lc 'cd /usr/local/falconfs/falcon_cm && python3 -' >/dev/null 2>&1 <<'PY'
from tool.check_replication import check_replication_status
ok, _, _ = check_replication_status()
raise SystemExit(0 if ok else 1)
PY
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

health_check() {
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

    if ! check_replication; then
        errors+=("check_replication_status failed")
    fi

    if (( ${#errors[@]} > 0 )); then
        local e
        for e in "${errors[@]}"; do
            if [[ -z "$HEALTH_ERROR_MSG" ]]; then
                HEALTH_ERROR_MSG="$e"
            else
                HEALTH_ERROR_MSG="${HEALTH_ERROR_MSG}; $e"
            fi
        done
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

wait_recovery() {
    local action_id="$1"
    local started deadline now elapsed healthy_since
    started=$(date +%s)
    deadline=$(( started + HEALTH_TIMEOUT_SEC ))
    RECOVER_SECONDS=0
    RECOVERY_ERROR=""
    healthy_since=0

    if (( POST_ACTION_GRACE_SEC > 0 )); then
        log "post-action grace sleep ${POST_ACTION_GRACE_SEC}s for ${action_id}"
        sleep "$POST_ACTION_GRACE_SEC"
    fi

    while true; do
        now=$(date +%s)
        elapsed=$(( now - started ))
        if (( now >= deadline )); then
            RECOVER_SECONDS=$HEALTH_TIMEOUT_SEC
            RECOVERY_ERROR="$HEALTH_ERROR_MSG"
            return 1
        fi
        if health_check; then
            if (( healthy_since == 0 )); then
                healthy_since=$now
                log "recovery health first-pass for ${action_id}, waiting stable window ${STABLE_WINDOW_SEC}s"
            fi
            if (( now - healthy_since >= STABLE_WINDOW_SEC )); then
                RECOVER_SECONDS=$(( now - started ))
                RECOVERY_ERROR=""
                return 0
            fi
            sleep "$HEALTH_CHECK_INTERVAL_SEC"
            continue
        fi
        healthy_since=0
        RECOVERY_ERROR="$HEALTH_ERROR_MSG"
        log "recovery pending for ${action_id}, ${elapsed}s/${HEALTH_TIMEOUT_SEC}s: ${RECOVERY_ERROR}"
        if [[ "$action_id" == "S04" || "$action_id" == "S05" ]]; then
            start_store_client
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

    python3 - "$REPORT_FILE" "$ts" "$idx" "$action_id" "$action_name" "$target" "$injected" "$recovered" "$recover_seconds" "$before_cn" "$before_dn" "$after_cn" "$after_dn" "$error_msg" <<'PY'
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
}
with open(report, "a", encoding="utf-8") as f:
    f.write(json.dumps(event, ensure_ascii=True) + "\n")
PY
}

write_summary_json() {
    local passed="$1"
    local failed="$2"
    python3 - "$REPORT_FILE" "$passed" "$failed" <<'PY'
import json
import sys
import time

report = sys.argv[1]
passed = int(sys.argv[2])
failed = int(sys.argv[3])
event = {
    "ts": int(time.time()),
    "type": "summary",
    "passed": passed,
    "failed": failed,
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
        *) return 1 ;;
    esac
}

validate_actions() {
    local action
    IFS=',' read -r -a ACTION_IDS <<<"$ACTIONS_CSV"
    for action in "${ACTION_IDS[@]}"; do
        case "$action" in
            S01|S01C2|S02|S03|S04|S05) ;;
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

    COMPOSE_FILE="$(resolve_path "$COMPOSE_FILE")"
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
    log "pg ip consistency check: $([[ "$SKIP_PG_IP_CHECK" == "1" ]] && echo disabled || echo enabled)"
    log "core-once mode: $([[ "$CORE_ONCE_ENABLE" == "1" ]] && echo enabled || echo disabled), core-limit-kb=${CORE_LIMIT_KB}"

    ensure_sudo_ready

    prepare_data_dirs

    if [[ "$FRESH_START" == "1" ]]; then
        log "fresh-start enabled, cleaning existing data directories"
        clean_data_dirs
    fi

    if [[ "$FRESH_START" == "1" ]]; then
        log "running docker-compose down before startup"
        compose down || true
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

    if ! health_check; then
        log "ERROR: initial health check failed: ${HEALTH_ERROR_MSG}"
        collect_failure_snapshot "initial_health_failed" 0
        exit 1
    fi
    log "initial health check passed"

    local end_ts now remain idx passed failed
    local action_id action_desc before_cn before_dn after_cn after_dn
    local injected recovered inject_error error_msg
    end_ts=$(( $(date +%s) + DURATION_MIN * 60 ))
    idx=0
    passed=0
    failed=0

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

        if ! run_action "$action_id"; then
            injected=0
            inject_error="inject action failed"
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
            "$error_msg"

        if [[ "$injected" == "1" && "$recovered" == "1" ]]; then
            passed=$((passed + 1))
            log "[${idx}] PASS in ${RECOVER_SECONDS}s"
        else
            failed=$((failed + 1))
            log "[${idx}] FAIL: ${error_msg}"
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

    write_summary_json "$passed" "$failed"

    if [[ "$TEARDOWN" == "1" ]]; then
        log "teardown enabled, running docker-compose down"
        compose down || true
    fi

    log "chaos run finished"
}

main "$@"
