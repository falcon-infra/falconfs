#!/usr/bin/env bash
# ============================================================================
# FalconFS KV cache regression harness (v6.5)
# ============================================================================
# Single fail-fast wrapper that runs the entire KV-cache regression in one
# command:
#
#   1. Build (calls scripts/falcon_distributed_test.sh build)
#   2. ctest (FalconKVPrimitivesUT, label kv-unit)
#   3. Cluster up (1 CN + 2 DNs + 2 Clients)
#   4. FalconFS distributed file ops — `falcon_distributed_test.sh test` (client
#      mounts: read/write/cross-visibility/large file/concurrent writes)
#   5. C++ E2E suites (same driver):
#         kv-test, kv-meta-stress-test, kv-cluster-test,
#         kv-fault-test, kv-cluster-fault-test
#   6. When REGRESSION_PROFILE=full only (P8 final gate):
#         kv-cluster-failover-test, kv-topology-test, kv-mixed-colocation-test,
#         kv-cluster-promote-test
#   7. Python unittest under vllm_kv_cache/test/ (excluding vllm/ subfolder), including
#         ``test_offloading_manager_cluster_mixed_e2e`` (vLLM OffloadingManager meta+data
#         BRPC against 3 DNs + 4 stores). Full profile fails fast if that topology is down.
#   8. Cluster down
#   9. Coloured summary
#
# Knobs (all env-driven, all optional):
#   REGRESSION_PROFILE   - smoke (default) | full (adds failover/topology/mixed-
#                          colocation/promote suites; defaults KV_THREE_DNS=1 and
#                          STORE_COUNT=4 for 3-DN + 4-store topology)
#   KEEP_LOGS            - 1 to keep logs even on success (default: 0)
#   STOP_ON_FAILURE      - 0 to keep cluster up after first failure for debug
#                          (default: 1; cleanup runs in trap)
#   SKIP_BUILD           - 1 to skip the build step (use when iterating)
#   SKIP_CLUSTER         - 1 to skip cluster start/stop (e.g. cluster already up)
#   PYTHON_BIN           - override python interpreter (default: python3)
#
# Exit codes:
#   0  - all suites passed
#   1+ - first failed suite's order (see main() in this file). Logs:
#        logs/regression/<ts>/
# ============================================================================

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
DRIVER="$SCRIPT_DIR/falcon_distributed_test.sh"
BUILD_DIR="${BUILD_DIR:-$PROJECT_DIR/build}"

PROFILE="${REGRESSION_PROFILE:-smoke}"
KEEP_LOGS="${KEEP_LOGS:-0}"
STOP_ON_FAILURE="${STOP_ON_FAILURE:-1}"
SKIP_BUILD="${SKIP_BUILD:-0}"
SKIP_CLUSTER="${SKIP_CLUSTER:-0}"
PYTHON_BIN="${PYTHON_BIN:-python3}"

TS="$(date +%Y%m%d_%H%M%S)"
LOG_ROOT="${REGRESSION_LOG_ROOT:-$PROJECT_DIR/logs/regression}"
LOG_DIR="$LOG_ROOT/$TS"
mkdir -p "$LOG_DIR"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m'

log_step() { echo -e "${BLUE}${BOLD}[STEP]${NC} $*"; }
log_info() { echo -e "${GREEN}[ OK ]${NC} $*"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_err()  { echo -e "${RED}${BOLD}[FAIL]${NC} $*"; }

collect_runtime_logs() {
    local dst="$LOG_DIR/runtime"
    mkdir -p "$dst"
    local copied=0
    local f
    for f in /tmp/falcon_*.log /tmp/falcon_kv_store_*.log; do
        if [ -f "$f" ]; then
            cp -f "$f" "$dst/" 2>/dev/null && copied=1
        fi
    done
    if [ -f /tmp/falcon_kv_store.pids ]; then
        cp -f /tmp/falcon_kv_store.pids "$dst/" 2>/dev/null || true
    fi
    if [ "$copied" = "1" ]; then
        log_info "Runtime logs copied to $dst"
    fi
}

# Suite results: NAME|STATUS|DURATION_S|LOG
SUITE_RESULTS=()
SUITE_FAILED=""
SUITE_FAILED_ORDER=0
START_EPOCH="$(date +%s)"

cleanup() {
    local rc=$?
    collect_runtime_logs
    if [ -n "$SUITE_FAILED" ] && [ "$STOP_ON_FAILURE" = "0" ]; then
        log_warn "Failure detected ($SUITE_FAILED); STOP_ON_FAILURE=0 keeping cluster up"
    elif [ "$SKIP_CLUSTER" = "0" ]; then
        # Best-effort stop, ignore exit code since we're already in cleanup.
        if bash "$DRIVER" stop >>"$LOG_DIR/zz_cleanup_stop.log" 2>&1; then
            :
        fi
    fi
    print_summary
    exit $rc
}
trap cleanup EXIT
trap 'echo; log_warn "Interrupted"; exit 130' INT TERM

run_suite() {
    local order="$1"
    local name="$2"
    shift 2
    local logfile="$LOG_DIR/${order}_${name}.log"
    local t0 t1 dur rc
    log_step "[$order] $name"
    t0="$(date +%s)"
    if "$@" >>"$logfile" 2>&1; then
        rc=0
    else
        rc=$?
    fi
    t1="$(date +%s)"
    dur=$((t1 - t0))
    if [ $rc -eq 0 ]; then
        SUITE_RESULTS+=("$name|PASS|${dur}|$logfile")
        log_info "[$order] $name passed in ${dur}s"
    else
        SUITE_RESULTS+=("$name|FAIL|${dur}|$logfile")
        SUITE_FAILED="$name"
        SUITE_FAILED_ORDER="$order"
        log_err "[$order] $name failed in ${dur}s (rc=$rc); see $logfile"
        # Echo last 30 lines of the log for quick triage.
        echo "----- tail $logfile -----"
        tail -n 30 "$logfile" || true
        echo "----- end tail -----"
        return $rc
    fi
    return 0
}

print_summary() {
    local end_epoch total_dur
    end_epoch="$(date +%s)"
    total_dur=$((end_epoch - START_EPOCH))
    echo
    echo "============================================================"
    echo " FalconFS KV regression summary  (profile: $PROFILE)"
    echo " logs:  $LOG_DIR"
    echo " total: ${total_dur}s"
    echo "============================================================"
    if [ ${#SUITE_RESULTS[@]} -eq 0 ]; then
        log_warn "No suites ran"
        return
    fi
    local pass_count=0 fail_count=0
    for entry in "${SUITE_RESULTS[@]}"; do
        local name="${entry%%|*}"
        local rest="${entry#*|}"
        local status="${rest%%|*}"
        rest="${rest#*|}"
        local dur="${rest%%|*}"
        local logf="${rest#*|}"
        if [ "$status" = "PASS" ]; then
            printf " ${GREEN}PASS${NC}  %-30s %5ss  %s\n" "$name" "$dur" "$logf"
            pass_count=$((pass_count + 1))
        else
            printf " ${RED}FAIL${NC}  %-30s %5ss  %s\n" "$name" "$dur" "$logf"
            fail_count=$((fail_count + 1))
        fi
    done
    echo "------------------------------------------------------------"
    printf " %d passed, %d failed\n" "$pass_count" "$fail_count"
    echo "============================================================"
    if [ "$fail_count" -eq 0 ] && [ "$KEEP_LOGS" = "0" ]; then
        # Trim noise on the happy path: keep the directory but remove
        # individual log files so the next run starts clean.
        find "$LOG_DIR" -maxdepth 1 -name '*.log' -delete 2>/dev/null || true
    fi
}

# ============================================================================
# Suite definitions
# ============================================================================

suite_build() {
    bash "$DRIVER" build
}

suite_ctest_unit() {
    if [ ! -d "$BUILD_DIR/tests/falcon_kv" ]; then
        echo "BUILD_DIR/tests/falcon_kv missing: $BUILD_DIR/tests/falcon_kv"
        return 1
    fi
    cd "$BUILD_DIR/tests/falcon_kv"
    ctest --output-on-failure -L kv-unit -j"$(nproc)"
}

suite_cluster_up() {
    if [ "$PROFILE" = "full" ]; then
        export KV_THREE_DNS="${KV_THREE_DNS:-1}"
        export STORE_COUNT="${STORE_COUNT:-4}"
        log_step "full profile: KV_THREE_DNS=${KV_THREE_DNS} STORE_COUNT=${STORE_COUNT}"
    fi
    bash "$DRIVER" start
}

# `falcon_distributed_test.sh test` — exercises FalconFS through the two client
# FUSE mounts (not KV-specific). Must pass before KV E2E assumes a healthy cluster.
suite_distributed_fs_test() {
    bash "$DRIVER" test
}

suite_kv_test() {
    bash "$DRIVER" kv-test
}

suite_kv_meta_stress_test() {
    # Smoke profile keeps the existing default (1 iteration). Full profile
    # bumps iterations to exercise more concurrency permutations.
    if [ "$PROFILE" = "full" ]; then
        export FALCON_KV_STRESS_ITERATIONS=4
    fi
    bash "$DRIVER" kv-meta-stress-test
}

suite_kv_cluster_test() {
    bash "$DRIVER" kv-cluster-test
}

suite_kv_fault_test() {
    bash "$DRIVER" kv-fault-test
}

suite_kv_cluster_fault_test() {
    bash "$DRIVER" kv-cluster-fault-test
}

suite_kv_cluster_failover_test() {
    bash "$DRIVER" kv-cluster-failover-test
}

suite_kv_topology_test() {
    bash "$DRIVER" kv-topology-test
}

suite_kv_mixed_colocation_test() {
    bash "$DRIVER" kv-mixed-colocation-test
}

suite_kv_cluster_promote_test() {
    bash "$DRIVER" kv-cluster-promote-test
}

suite_python_unittest() {
    cd "$PROJECT_DIR"
    if [ -x /usr/local/pgsql/bin/pg_config ]; then
        export PATH="/usr/local/pgsql/bin:${PATH:-}"
    fi
    export PYTHONPATH="$PROJECT_DIR/vllm_kv_cache/python:$PROJECT_DIR/vllm_kv_cache/test:${PYTHONPATH:-}"
    # Keep Python OffloadingManager block sizing aligned with the running store.
    # Regression runs often lower FALCON_KV_STORE_BLOCK_SIZE to fit local DRAM.
    if [ -z "${FALCON_MIX_KV_BLOCK_BYTES:-}" ] && [ -n "${FALCON_KV_STORE_BLOCK_SIZE:-}" ]; then
        export FALCON_MIX_KV_BLOCK_BYTES="$FALCON_KV_STORE_BLOCK_SIZE"
        log_step "  python profile: FALCON_MIX_KV_BLOCK_BYTES=${FALCON_MIX_KV_BLOCK_BYTES} (from FALCON_KV_STORE_BLOCK_SIZE)"
    fi
    if [ "$PROFILE" = "full" ]; then
        log_step "  full profile: require vLLM mixed-cluster topology (3 DNs + 4 stores) for Python E2E"
        if ! $PYTHON_BIN -c "
import sys
sys.path.insert(0, 'vllm_kv_cache/python')
sys.path.insert(0, 'vllm_kv_cache/test')
import test_offloading_manager_cluster_mixed_e2e as m
if not m.mixed_topology_available():
    print(
        'REGRESSION_FAIL: mixed topology unreachable (need KV_THREE_DNS=1, STORE_COUNT=4, '
        'CN + DN1/2/3 poolers + four falcon_kv_store BRPC ports). '
        'See vllm_kv_cache/test/test_offloading_manager_cluster_mixed_e2e.py',
        file=sys.stderr,
    )
    sys.exit(1)
"; then
            return 1
        fi
        export FALCON_MIX_E2E_QUICK="${FALCON_MIX_E2E_QUICK:-0}"
        # Keep the full Python mixed E2E bounded for the documented local 1 MiB /
        # 512-slot profile. Callers with larger pools can override these.
        export FALCON_MIX_THROUGHPUT_KEYS="${FALCON_MIX_THROUGHPUT_KEYS:-384}"
        export FALCON_MIX_PHASE_KEYS="${FALCON_MIX_PHASE_KEYS:-512}"
        export FALCON_MIX_PHASE_CHUNK_KEYS="${FALCON_MIX_PHASE_CHUNK_KEYS:-128}"
        log_step "  full profile: FALCON_MIX_E2E_QUICK=${FALCON_MIX_E2E_QUICK} phase_keys=${FALCON_MIX_PHASE_KEYS} throughput_keys=${FALCON_MIX_THROUGHPUT_KEYS}"
    fi
    # All Python tests directly under vllm_kv_cache/test/ are
    # unittest.TestCase-based; avoid a pytest dependency. The vllm/
    # subfolder requires vLLM at import time and is opt-in.
    # The cluster_brpc test self-skips if BRPC endpoints are unreachable.
    local modules=()
    while IFS= read -r f; do
        local base
        base="$(basename "$f" .py)"
        modules+=("$base")
    done < <(find "$PROJECT_DIR/vllm_kv_cache/test" -maxdepth 1 -type f -name 'test_*.py' | sort)
    if [ ${#modules[@]} -eq 0 ]; then
        echo "No Python test modules found"
        return 1
    fi
    echo "Running unittest modules: ${modules[*]}"
    $PYTHON_BIN -m unittest -v "${modules[@]}"
}

suite_cluster_restart_for_python() {
    bash "$DRIVER" restart
}

suite_kv_store_microbench_info() {
    cd "$PROJECT_DIR"
    export PYTHONPATH="$PROJECT_DIR/vllm_kv_cache/python:$PROJECT_DIR/vllm_kv_cache/test:${PYTHONPATH:-}"
    local block_bytes="${FALCON_KV_MICRO_BLOCK_BYTES:-${FALCON_MIX_KV_BLOCK_BYTES:-${FALCON_KV_STORE_BLOCK_SIZE:-1048576}}}"
    local blocks="${FALCON_KV_MICRO_BLOCKS:-32}"
    local par_default=$(( $(nproc) * 2 ))
    if [ "$par_default" -gt 64 ]; then par_default=64; fi
    local parallelism="${FALCON_KV_MICRO_PARALLELISM:-$par_default}"
    local copy_iters="${FALCON_KV_MICRO_COPY_BOUND_ITERS:-64}"
    if [ -z "${FALCON_KV_MICRO_STORES:-}" ]; then
        local count="${STORE_COUNT:-2}"
        local stores=""
        local i
        # Build the list without relying on seq availability in minimal shells.
        for ((i=1; i<=count; ++i)); do
            if [ -n "$stores" ]; then stores="$stores,$i"; else stores="$i"; fi
        done
        export FALCON_KV_MICRO_STORES="$stores"
    fi

    mkdir -p "$PROJECT_DIR/logs"
    local latest_json="${FALCON_KV_MICRO_JSON:-$PROJECT_DIR/logs/falcon_kv_store_microbench_latest.json}"
    local native_json="${FALCON_KV_MICRO_NATIVE_JSON:-$PROJECT_DIR/logs/falcon_kv_store_microbench_native_latest.json}"
    local local_read_json="${FALCON_KV_MICRO_LOCAL_READ_JSON:-$PROJECT_DIR/logs/falcon_kv_store_microbench_local_read_latest.json}"
    local local_write_json="${FALCON_KV_MICRO_LOCAL_WRITE_JSON:-$PROJECT_DIR/logs/falcon_kv_store_microbench_local_write_latest.json}"
    local remote_json="${FALCON_KV_MICRO_REMOTE_JSON:-$PROJECT_DIR/logs/falcon_kv_store_microbench_remote_latest.json}"
    local local_store="${FALCON_KV_MICRO_LOCAL_STORE_ID:-1}"
    local node_name="${FALCON_KV_MICRO_NODE_NAME:-${FALCON_MIX_FACADE_NODE_NAME:-${NODE_NAME:-v65mix0}}}"
    local remote_stores=""
    local store
    IFS=',' read -ra _micro_store_ids <<< "$FALCON_KV_MICRO_STORES"
    for store in "${_micro_store_ids[@]}"; do
        if [ "$store" != "$local_store" ]; then
            if [ -n "$remote_stores" ]; then remote_stores="$remote_stores,$store"; else remote_stores="$store"; fi
        fi
    done

    export FALCON_MIX_MICROBENCH_JSONS="$native_json:$local_read_json:$local_write_json:$remote_json:$latest_json"
    export FALCON_MIX_MICROBENCH_JSON="$latest_json"
    log_step "  microbench info: stores=${FALCON_KV_MICRO_STORES} local_store=${local_store} node=${node_name} blocks=${blocks} block_bytes=${block_bytes} parallelism=${parallelism}"

    if ! FALCON_KV_MICRO_JSON="$native_json" "$PYTHON_BIN" vllm_kv_cache/test/kv_store_microbench.py \
        --mode facade --allocation fixed --stores "$local_store" --path-mode pure-native-memcpy \
        --verify none --blocks "$blocks" --block-bytes "$block_bytes" \
        --parallelism "$parallelism" --copy-bound-iters "$copy_iters"; then
        log_warn "kv_store_microbench native memcpy bound failed; continuing"
    fi

    if ! NODE_NAME="$node_name" FALCON_KV_MICRO_REQUIRE_LOCAL=1 FALCON_KV_MICRO_JSON="$local_read_json" "$PYTHON_BIN" vllm_kv_cache/test/kv_store_microbench.py \
        --mode facade --allocation metadata --stores "$local_store" --path-mode local-shm-zero-copy-read \
        --require-local-store "$local_store" --verify none --blocks "$blocks" \
        --block-bytes "$block_bytes" --parallelism "$parallelism" --copy-bound-iters "$copy_iters"; then
        if [ "${FALCON_KV_MICRO_REQUIRE_LOCAL_BOUNDS:-1}" = "1" ]; then
            log_err "kv_store_microbench local SHM zero-copy read bound failed"
            return 1
        fi
        log_warn "kv_store_microbench local SHM zero-copy read bound failed; continuing"
    fi

    if ! NODE_NAME="$node_name" FALCON_KV_MICRO_REQUIRE_LOCAL=1 FALCON_KV_MICRO_JSON="$local_write_json" "$PYTHON_BIN" vllm_kv_cache/test/kv_store_microbench.py \
        --mode facade --allocation metadata --stores "$local_store" --path-mode local-shm-prealloc-write \
        --require-local-store "$local_store" --verify none --blocks "$blocks" \
        --block-bytes "$block_bytes" --parallelism "$parallelism" --copy-bound-iters "$copy_iters"; then
        if [ "${FALCON_KV_MICRO_REQUIRE_LOCAL_BOUNDS:-1}" = "1" ]; then
            log_err "kv_store_microbench local SHM preallocated write bound failed"
            return 1
        fi
        log_warn "kv_store_microbench local SHM preallocated write bound failed; continuing"
    fi

    if [ -n "$remote_stores" ]; then
        if ! NODE_NAME="$node_name" FALCON_KV_MICRO_JSON="$remote_json" "$PYTHON_BIN" vllm_kv_cache/test/kv_store_microbench.py \
            --mode facade --allocation metadata --stores "$remote_stores" --path-mode remote-brpc-attachment \
            --verify none --blocks "$blocks" --block-bytes "$block_bytes" \
            --parallelism "$parallelism" --copy-bound-iters "$copy_iters"; then
            log_warn "kv_store_microbench remote BRPC attachment bound failed; continuing without remote upper bound"
        fi
    else
        log_warn "kv_store_microbench remote BRPC attachment bound skipped; no remote stores in ${FALCON_KV_MICRO_STORES}"
    fi

    if ! NODE_NAME="$node_name" FALCON_KV_MICRO_JSON="$latest_json" "$PYTHON_BIN" vllm_kv_cache/test/kv_store_microbench.py \
        --mode facade --allocation metadata --blocks "$blocks" --block-bytes "$block_bytes" \
        --parallelism "$parallelism" --copy-bound-iters "$copy_iters"; then
        log_warn "kv_store_microbench compatibility mixed/facade run failed; continuing without legacy latest JSON"
    fi
    return 0
}

suite_cluster_down() {
    bash "$DRIVER" stop
}

# ============================================================================
# Driver
# ============================================================================

main() {
    log_step "FalconFS KV regression harness  (profile: $PROFILE)"
    log_step "Logs: $LOG_DIR"

    if [ "$SKIP_BUILD" = "1" ]; then
        log_warn "[1] build SKIPPED (SKIP_BUILD=1)"
    else
        run_suite 1 build suite_build || exit 1
    fi

    run_suite 2 ctest_unit suite_ctest_unit || exit 2

    if [ "$SKIP_CLUSTER" = "1" ]; then
        log_warn "[3] cluster_up SKIPPED (SKIP_CLUSTER=1)"
    else
        run_suite 3 cluster_up suite_cluster_up || exit 3
        if [ -f /tmp/falcon_kv_store_env.sh ]; then
            set -a
            # shellcheck disable=SC1090
            . /tmp/falcon_kv_store_env.sh
            set +a
        fi
    fi

    if [ "$SKIP_CLUSTER" = "1" ]; then
        log_warn "[4] distributed_fs_test SKIPPED (SKIP_CLUSTER=1)"
    else
        run_suite 4 distributed_fs_test suite_distributed_fs_test || exit 4
    fi

    run_suite 5 kv_test               suite_kv_test               || exit 5
    run_suite 6 kv_meta_stress_test   suite_kv_meta_stress_test   || exit 6
    run_suite 7 kv_cluster_test       suite_kv_cluster_test       || exit 7
    run_suite 8 kv_fault_test         suite_kv_fault_test         || exit 8
    run_suite 9 kv_cluster_fault_test suite_kv_cluster_fault_test || exit 9

    local restart_order=10
    local microbench_order=11
    local py_order=12
    if [ "$PROFILE" = "full" ]; then
        run_suite 10 kv_cluster_failover_test suite_kv_cluster_failover_test || exit 10
        run_suite 11 kv_topology_test suite_kv_topology_test || exit 11
        run_suite 12 kv_mixed_colocation_test suite_kv_mixed_colocation_test || exit 12
        run_suite 13 kv_cluster_promote_test suite_kv_cluster_promote_test || exit 13
        restart_order=14
        microbench_order=15
        py_order=16
    fi

    # Fault/failover/promote drills intentionally perturb epochs and live-region
    # state. Python BRPC/offloading tests assume a clean topology, so refresh the
    # harness cluster before entering those end-to-end tests.
    if [ "$SKIP_CLUSTER" = "1" ]; then
        log_warn "[${restart_order}] cluster_restart_for_python SKIPPED (SKIP_CLUSTER=1)"
        log_warn "[${microbench_order}] kv_store_microbench_info SKIPPED (SKIP_CLUSTER=1)"
    else
        run_suite "$restart_order" cluster_restart_for_python suite_cluster_restart_for_python || exit "$restart_order"
        if [ -f /tmp/falcon_kv_store_env.sh ]; then
            set -a
            # shellcheck disable=SC1090
            . /tmp/falcon_kv_store_env.sh
            set +a
        fi
        run_suite "$microbench_order" kv_store_microbench_info suite_kv_store_microbench_info || exit "$microbench_order"
    fi

    run_suite "$py_order" python_unittest suite_python_unittest || exit "$py_order"

    local down_order=$((py_order + 1))
    if [ "$SKIP_CLUSTER" = "1" ]; then
        log_warn "[${down_order}] cluster_down SKIPPED (SKIP_CLUSTER=1)"
    else
        run_suite "$down_order" cluster_down suite_cluster_down || exit "$down_order"
    fi
}

main "$@"
