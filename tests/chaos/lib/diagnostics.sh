#!/bin/bash

# Alerting, diagnostics, auto logging, and core-once handling.

# Alerting, diagnostics and core-once helpers.
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
