#!/bin/bash

# Runtime primitives, topology resolution, and option validation.

# Shared runtime primitives used by entry and library flows.

log() {
    echo "[$(date '+%F %T')] $*"
}
is_valid_action_id() {
    case "$1" in
        S01|S02|S03|S04|S05|S06|S07|S08) return 0 ;;
        *) return 1 ;;
    esac
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
