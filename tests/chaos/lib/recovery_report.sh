#!/bin/bash

# Recovery orchestration, JSON reporting, and action-loop control.

# Recovery and service-state evaluation.
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
    DEGRADED_REQUIRED=0
    DEGRADED_OBSERVED=0

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
        DEGRADED_REQUIRED=1
        SERVICE_EXPECTED="DEGRADED->RW"
    elif [[ "$TOPOLOGY_RESOLVED" == "dual" ]] && is_metadata_action "$action_id"; then
        SERVICE_EXPECTED="RW(degraded optional)"
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

    DEGRADED_OBSERVED=$saw_ro

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

# JSONL report writers.
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
    local degraded_required="${28}"
    local degraded_observed="${29}"
    local recovery_path="${30}"

    python3 - "$REPORT_FILE" "$ts" "$idx" "$action_id" "$action_name" "$target" "$injected" "$recovered" "$recover_seconds" "$before_cn" "$before_dn" "$after_cn" "$after_dn" "$error_msg" "$topology" "$service_expected" "$service_observed" "$read_ok" "$write_ok" "$probe_error" "$reliability_restored" "$need_supplement_seen" "$stage_failed" "$probe_mode" "$fuse_ok" "$pg_in_recovery" "$tx_read_only" "$probe_reason" "$degraded_required" "$degraded_observed" "$recovery_path" <<'PY'
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
    "degraded_required": True if sys.argv[29] == "1" else False,
    "degraded_observed": True if sys.argv[30] == "1" else False,
    "recovery_path": sys.argv[31],
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
    local failed_coverage="$6"
    python3 - "$REPORT_FILE" "$passed" "$failed" "$failed_core" "$failed_service" "$failed_reliability" "$failed_coverage" "$TOPOLOGY_RESOLVED" <<'PY'
import json
import sys
import time

report = sys.argv[1]
passed = int(sys.argv[2])
failed = int(sys.argv[3])
failed_core = int(sys.argv[4])
failed_service = int(sys.argv[5])
failed_reliability = int(sys.argv[6])
failed_coverage = int(sys.argv[7])
topology = sys.argv[8]
event = {
    "ts": int(time.time()),
    "type": "summary",
    "topology": topology,
    "passed": passed,
    "failed": failed,
    "failed_core": failed_core,
    "failed_service": failed_service,
    "failed_reliability": failed_reliability,
    "failed_coverage": failed_coverage,
    "total": passed + failed,
}
with open(report, "a", encoding="utf-8") as f:
    f.write(json.dumps(event, ensure_ascii=True) + "\n")
print(json.dumps(event, ensure_ascii=True))
PY
}

# Top-level orchestration phases used by chaos_run.sh main().
setup_runtime_and_validation() {
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
}

log_runtime_configuration() {
    log "compose file: ${COMPOSE_FILE}"
    log "data path: ${DATA_PATH}"
    log "report file: ${REPORT_FILE}"
    log "run log file: ${RUN_LOG_FILE}"
    log "diag dir: ${DIAG_DIR}"
    log "actions: ${ACTIONS_CSV}"
    if [[ -n "$ACTION_PLAN_FILE" ]]; then
        log "action plan file: ${ACTION_PLAN_FILE}"
    fi
    if [[ -n "$REQUIRE_ACTIONS_CSV" ]]; then
        log "required actions: ${REQUIRE_ACTIONS_CSV}"
    fi
    if [[ -n "$ACTION_HOLD_SECS_CSV" ]]; then
        log "action hold overrides: ${ACTION_HOLD_SECS_CSV}"
    fi
    log "recovery policy: grace=${POST_ACTION_GRACE_SEC}s, stable_window=${STABLE_WINDOW_SEC}s, timeout=${HEALTH_TIMEOUT_SEC}s"
    log "reliability timeout: ${RELIABILITY_TIMEOUT_SEC}s"
    log "pg ip consistency check: $([[ "$SKIP_PG_IP_CHECK" == "1" ]] && echo disabled || echo enabled)"
    log "topology mode: ${TOPOLOGY}"
    log "probe mode: ${PROBE_MODE}"
    log "probe timeout: ${PROBE_TIMEOUT_SEC}s"
    log "strict dual ro check: $([[ "$STRICT_DUAL_RO_CHECK" == "1" ]] && echo enabled || echo disabled)"
    log "core-once mode: $([[ "$CORE_ONCE_ENABLE" == "1" ]] && echo enabled || echo disabled), core-limit-kb=${CORE_LIMIT_KB}"
    log "smtp core alert: $([[ "$CHAOS_ALERT_ENABLE" == "1" ]] && echo enabled || echo disabled)"
}

startup_stack_and_initial_checks() {
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
    if (( ${#ACTION_HOLD_SECS[@]} > 0 )); then
        local action_id
        for action_id in "${ALL_ACTION_IDS[@]}"; do
            if [[ -n "${ACTION_HOLD_SECS[$action_id]:-}" ]]; then
                log "resolved hold ${action_id}=${ACTION_HOLD_SECS[$action_id]}s"
            fi
        done
    fi

    if ! wait_initial_state; then
        log "ERROR: initial health check failed: ${HEALTH_ERROR_MSG}"
        collect_failure_snapshot "initial_health_failed" 0
        exit 1
    fi
    log "initial health check passed"
}

pick_action_id_for_iteration() {
    local idx="$1"
    local plan_mode="$2"
    if (( plan_mode == 1 )); then
        printf '%s' "${ACTION_PLAN_IDS[$((idx - 1))]}"
    else
        printf '%s' "${ACTION_IDS[$((RANDOM % ${#ACTION_IDS[@]}))]}"
    fi
}

reset_iteration_runtime_state() {
    ACTION_TARGET=""
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
    DEGRADED_REQUIRED=0
    DEGRADED_OBSERVED=0
    RECOVERY_PATH="unknown"
}

sleep_before_next_iteration() {
    local plan_mode="$1"
    local idx="$2"
    local plan_count="$3"
    local end_ts="$4"
    local now remain

    if (( plan_mode == 1 )); then
        if (( idx < plan_count )); then
            log "sleep ${INTERVAL_SEC}s before next action"
            sleep "$INTERVAL_SEC"
        fi
        return 0
    fi

    now=$(date +%s)
    remain=$((end_ts - now))
    if (( remain <= 0 )); then
        return 1
    fi
    if (( INTERVAL_SEC < remain )); then
        log "sleep ${INTERVAL_SEC}s before next action"
        sleep "$INTERVAL_SEC"
    else
        log "sleep ${remain}s before next action"
        sleep "$remain"
    fi
    return 0
}

should_continue_action_loop() {
    local now="$1"
    local end_ts="$2"
    local plan_mode="$3"
    local idx="$4"
    local plan_count="$5"

    if (( plan_mode == 1 )); then
        (( idx < plan_count ))
        return $?
    fi

    (( now < end_ts ))
}

run_action_loop_and_write_summary() {
    local end_ts now idx passed failed
    local failed_core failed_service failed_reliability failed_coverage
    local action_id action_desc before_cn before_dn after_cn after_dn
    local injected recovered inject_error error_msg
    local plan_mode=0
    local plan_count=0

    end_ts=$(( $(date +%s) + DURATION_MIN * 60 ))
    idx=0
    passed=0
    failed=0
    failed_core=0
    failed_service=0
    failed_reliability=0
    failed_coverage=0

    if (( ${#ACTION_PLAN_IDS[@]} > 0 )); then
        plan_mode=1
        plan_count=${#ACTION_PLAN_IDS[@]}
    fi

    while true; do
        now=$(date +%s)
        if ! should_continue_action_loop "$now" "$end_ts" "$plan_mode" "$idx" "$plan_count"; then
            break
        fi

        idx=$((idx + 1))
        action_id="$(pick_action_id_for_iteration "$idx" "$plan_mode")"
        action_desc="$(action_name "$action_id")"
        before_cn="$(get_cn_leader_ip)"
        before_dn="$(dn_leader_snapshot)"

        log "[${idx}] inject ${action_id} ${action_desc}"

        injected=1
        recovered=0
        inject_error=""
        error_msg=""
        reset_iteration_runtime_state

        record_action_execution "$action_id"

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

        RECOVERY_PATH="$(classify_recovery_path "$action_id")"

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
            "$SERVICE_PROBE_REASON" \
            "$DEGRADED_REQUIRED" \
            "$DEGRADED_OBSERVED" \
            "$RECOVERY_PATH"

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

        if ! sleep_before_next_iteration "$plan_mode" "$idx" "$plan_count" "$end_ts"; then
            break
        fi
    done

    write_coverage_json
    if ! validate_required_actions_coverage; then
        failed=$((failed + 1))
        failed_coverage=$((failed_coverage + 1))
        if [[ -z "$RECOVERY_ERROR" ]]; then
            RECOVERY_ERROR="required actions coverage check failed"
        fi
    fi

    write_summary_json "$passed" "$failed" "$failed_core" "$failed_service" "$failed_reliability" "$failed_coverage"
}
