#!/bin/bash

# Action metadata, plan parsing, coverage bookkeeping, and action implementations.

action_name() {
    case "$1" in
        S01) echo "restart cn leader" ;;
        S02) echo "restart one dn leader" ;;
        S03) echo "restart random dn (prefer follower)" ;;
        S04) echo "restart store container" ;;
        S05) echo "kill falcon_client process in store" ;;
        S06) echo "stop cn leader then start after hold" ;;
        S07) echo "stop random dn then start after hold" ;;
        S08) echo "double cn leader churn then recover" ;;
        *) echo "unknown" ;;
    esac
}
# Action plan parsing and coverage bookkeeping.
run_action() {
    case "$1" in
        S01) action_s01 ;;
        S02) action_s02 ;;
        S03) action_s03 ;;
        S04) action_s04 ;;
        S05) action_s05 ;;
        S06) action_s06 ;;
        S07) action_s07 ;;
        S08) action_s08 ;;
        *) return 1 ;;
    esac
}

trim_whitespace() {
    local text="$1"
    text="${text#"${text%%[![:space:]]*}"}"
    text="${text%"${text##*[![:space:]]}"}"
    printf '%s' "$text"
}

load_action_plan() {
    local line action
    ACTION_PLAN_IDS=()
    [[ -z "$ACTION_PLAN_FILE" ]] && return 0
    ACTION_PLAN_FILE="$(resolve_path "$ACTION_PLAN_FILE")"
    if [[ ! -f "$ACTION_PLAN_FILE" ]]; then
        echo "action plan file not found: $ACTION_PLAN_FILE"
        exit 1
    fi

    while IFS= read -r line || [[ -n "$line" ]]; do
        line="${line%%#*}"
        action="$(trim_whitespace "$line")"
        [[ -z "$action" ]] && continue
        if ! is_valid_action_id "$action"; then
            echo "invalid action id in action plan: $action"
            exit 1
        fi
        ACTION_PLAN_IDS+=("$action")
    done <"$ACTION_PLAN_FILE"

    if (( ${#ACTION_PLAN_IDS[@]} == 0 )); then
        echo "action plan file has no executable actions: $ACTION_PLAN_FILE"
        exit 1
    fi
}

parse_required_actions() {
    local action
    REQUIRED_ACTION_IDS=()
    [[ -z "$REQUIRE_ACTIONS_CSV" ]] && return 0
    IFS=',' read -r -a REQUIRED_ACTION_IDS <<<"$REQUIRE_ACTIONS_CSV"
    for action in "${REQUIRED_ACTION_IDS[@]}"; do
        action="$(trim_whitespace "$action")"
        [[ -z "$action" ]] && continue
        if ! is_valid_action_id "$action"; then
            echo "invalid required action id: $action"
            exit 1
        fi
    done
}

parse_action_hold_secs() {
    local pair key value
    local -a pairs
    ACTION_HOLD_SECS=()
    [[ -z "$ACTION_HOLD_SECS_CSV" ]] && return 0
    IFS=',' read -r -a pairs <<<"$ACTION_HOLD_SECS_CSV"
    for pair in "${pairs[@]}"; do
        pair="$(trim_whitespace "$pair")"
        [[ -z "$pair" ]] && continue
        if [[ "$pair" != *=* ]]; then
            echo "invalid action hold entry: $pair"
            exit 1
        fi
        key="$(trim_whitespace "${pair%%=*}")"
        value="$(trim_whitespace "${pair#*=}")"
        if ! is_valid_action_id "$key"; then
            echo "invalid action id in --action-hold-secs: $key"
            exit 1
        fi
        if ! action_supports_hold "$key"; then
            echo "action does not support hold override: $key"
            exit 1
        fi
        if ! is_nonneg_integer "$value" || (( value <= 0 )); then
            echo "invalid hold seconds for ${key}: ${value}"
            exit 1
        fi
        ACTION_HOLD_SECS["$key"]="$value"
    done
    return 0
}

record_action_execution() {
    local action_id="$1"
    local cur="${ACTION_EXEC_COUNTS[$action_id]:-0}"
    ACTION_EXEC_COUNTS["$action_id"]=$((cur + 1))
}

write_coverage_json() {
    local coverage_file="$DATA_PATH/chaos_coverage.json"
    local id value first=1
    {
        printf '{\n  "actions": {'
        for id in "${ALL_ACTION_IDS[@]}"; do
            value="${ACTION_EXEC_COUNTS[$id]:-0}"
            if (( first == 1 )); then
                printf '\n    "%s": %s' "$id" "$value"
                first=0
            else
                printf ',\n    "%s": %s' "$id" "$value"
            fi
        done
        printf '\n  }\n}\n'
    } >"$coverage_file"
}

validate_required_actions_coverage() {
    local action count
    local -a missing=()
    for action in "${REQUIRED_ACTION_IDS[@]}"; do
        action="$(trim_whitespace "$action")"
        [[ -z "$action" ]] && continue
        count="${ACTION_EXEC_COUNTS[$action]:-0}"
        if (( count == 0 )); then
            missing+=("$action")
        fi
    done

    if (( ${#missing[@]} > 0 )); then
        log "ERROR: required actions not executed: ${missing[*]}"
        return 1
    fi
    return 0
}

validate_actions() {
    local action
    IFS=',' read -r -a ACTION_IDS <<<"$ACTIONS_CSV"
    for action in "${ACTION_IDS[@]}"; do
        if ! is_valid_action_id "$action"; then
            echo "invalid action id: $action"
            exit 1
        fi
    done

    load_action_plan
    parse_required_actions
    parse_action_hold_secs
}

# Chaos action implementations (S01..S08).
action_s01() {
    local ip target
    ip="$(get_cn_leader_ip)"
    [[ -z "$ip" ]] && return 1
    target="$(find_container_by_ip "falcon-cn-" "$ip" || true)"
    [[ -z "$target" ]] && return 1
    restart_container "$target"
    ACTION_TARGET="$target"
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

    dn_nodes=()
    followers=()
    leader_ips=()

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

hold_and_observe_recovery_signals() {
    local action_id="$1"
    local hold_sec="$2"
    local started now remain step
    DUAL_RO_SEEN_ACTION=0
    started=$(date +%s)
    while true; do
        now=$(date +%s)
        remain=$((hold_sec - (now - started)))
        if (( remain <= 0 )); then
            break
        fi

        if [[ "$TOPOLOGY_RESOLVED" != "single" ]] && is_metadata_action "$action_id"; then
            mark_need_supplement_seen || true
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
    local ip target hold_sec
    ip="$(get_cn_leader_ip)"
    [[ -z "$ip" ]] && return 1
    target="$(find_container_by_ip "falcon-cn-" "$ip" || true)"
    [[ -z "$target" ]] && return 1
    hold_sec="$(action_hold_sec "S06")"

    docker stop "$target" >/dev/null
    log "${target} stopped, hold ${hold_sec}s for supplement observation"
    hold_and_observe_recovery_signals "S06" "$hold_sec"
    docker start "$target" >/dev/null
    ACTION_TARGET="${target}:hold=${hold_sec}s"
}

action_s07() {
    local -a dn_nodes followers leader_ips
    local name ip target count idx hold_sec

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

    hold_sec="$(action_hold_sec "S07")"
    docker stop "$target" >/dev/null
    log "${target} stopped, hold ${hold_sec}s for supplement observation"
    hold_and_observe_recovery_signals "S07" "$hold_sec"
    docker start "$target" >/dev/null
    ACTION_TARGET="${target}:hold=${hold_sec}s"
}

wait_new_cn_leader_ip() {
    local old_ip="$1"
    local timeout_sec="$2"
    local start now ip
    start=$(date +%s)
    while true; do
        now=$(date +%s)
        if (( now - start >= timeout_sec )); then
            return 1
        fi
        ip="$(get_cn_leader_ip)"
        if [[ -n "$ip" && "$ip" != "$old_ip" ]]; then
            printf '%s' "$ip"
            return 0
        fi
        sleep 1
    done
}

action_s08() {
    local l1_ip l1_name l2_ip l2_name hold_sec
    l1_ip="$(get_cn_leader_ip)"
    [[ -z "$l1_ip" ]] && return 1
    l1_name="$(find_container_by_ip "falcon-cn-" "$l1_ip" || true)"
    [[ -z "$l1_name" ]] && return 1
    hold_sec="$(action_hold_sec "S08")"

    docker stop "$l1_name" >/dev/null
    l2_ip="$(wait_new_cn_leader_ip "$l1_ip" 120 || true)"
    if [[ -z "$l2_ip" ]]; then
        docker start "$l1_name" >/dev/null || true
        return 1
    fi
    l2_name="$(find_container_by_ip "falcon-cn-" "$l2_ip" || true)"
    if [[ -z "$l2_name" || "$l2_name" == "$l1_name" ]]; then
        docker start "$l1_name" >/dev/null || true
        return 1
    fi

    docker stop "$l2_name" >/dev/null
    log "${l1_name}->${l2_name} stopped, hold ${hold_sec}s for epoch churn observation"
    hold_and_observe_recovery_signals "S08" "$hold_sec"
    docker start "$l2_name" >/dev/null
    docker start "$l1_name" >/dev/null
    ACTION_TARGET="${l1_name}->${l2_name}:hold=${hold_sec}s"
}

action_supports_hold() {
    case "$1" in
        S06|S07|S08) return 0 ;;
        *) return 1 ;;
    esac
}

action_hold_sec() {
    local action_id="$1"
    local overridden="${ACTION_HOLD_SECS[$action_id]:-}"
    if [[ -n "$overridden" ]]; then
        echo "$overridden"
    else
        echo "$SUPPLEMENT_HOLD_SEC"
    fi
}

classify_recovery_path() {
    local action_id="$1"
    case "$action_id" in
        S06|S07)
            if (( NEED_SUPPLEMENT_SEEN == 1 )); then
                echo "supplement"
            else
                echo "self_recover"
            fi
            ;;
        S08)
            if (( NEED_SUPPLEMENT_SEEN == 1 )); then
                echo "supplement"
            else
                echo "leader_churn"
            fi
            ;;
        *)
            echo "n/a"
            ;;
    esac
}

is_metadata_action() {
    case "$1" in
        S01|S02|S03|S06|S07|S08) return 0 ;;
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
    if [[ "$STRICT_DUAL_RO_CHECK" == "1" ]]; then
        return 0
    fi
    return 1
}
