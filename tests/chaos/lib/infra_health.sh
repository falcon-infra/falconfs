#!/bin/bash

# Docker, ZooKeeper, liveness, service probe, and reliability helpers.

# Docker/ZK and container infrastructure helpers.
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

mark_need_supplement_seen() {
    if [[ "$TOPOLOGY_RESOLVED" == "single" ]]; then
        return 0
    fi
    if zk_has_child "/falcon" "need_supplement"; then
        if [[ -n "$(zk_ls "/falcon/need_supplement")" ]]; then
            NEED_SUPPLEMENT_SEEN=1
        fi
    fi
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

    mark_need_supplement_seen

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
