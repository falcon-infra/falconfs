#!/bin/bash
set -eo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)

BIN_DIR="${BIN_DIR:-/root/falconfs/build/tests/private-directory-test}"
TEST_PROGRAM="${TEST_PROGRAM:-test_falcon}" # test_falcon / test_posix
MOUNT_DIR="${MOUNT_DIR:-/test/}" # meta directory / falconfs mount path, end with /
FILE_PER_THREAD="${FILE_PER_THREAD:-1000}"
PORT="${PORT:-1111}"
FILE_SIZE="${FILE_SIZE:-1572864}"
CLIENT_NUM="${CLIENT_NUM:-1}"
THREAD_NUM_PER_CLIENT="${THREAD_NUM_PER_CLIENT:-2000}"
ROUND_INDEX="${ROUND_INDEX:-0 1 2 3}"
ROUND_NAME=("workload_init" "workload_create" "workload_stat" "workload_open" "workload_close" "workload_delete" "workload_mkdir" "workload_rmdir" "workload_open_write_close" "workload_open_write_close_nocreate" "workload_open_read_close" "workload_kv_put" "workload_kv_get" "workload_kv_del" "workload_slice_put" "workload_slice_get" "workload_slice_del" "workload_uninit")
CLIENT_ID="${CLIENT_ID:-0}"
MOUNT_PER_CLIENT="${MOUNT_PER_CLIENT:-1}"
CLIENT_CACHE_SIZE="${CLIENT_CACHE_SIZE:-16384}"
META_SERVER_IP="${META_SERVER_IP:-127.0.0.1}" # meta cn ip
META_SERVER_PORT="${META_SERVER_PORT:-58610}" # meta cn port
SIGNAL_IP="${SIGNAL_IP:-127.0.0.1}"
POLL_INTERVAL_SEC="${POLL_INTERVAL_SEC:-3}"
ROUND_TIMEOUT_SEC="${ROUND_TIMEOUT_SEC:-0}"
RUN_ID="${RUN_ID:-$(date +%Y%m%d_%H%M%S)}"
RESULT_ROOT="${RESULT_ROOT:-$SCRIPT_DIR/results}"
RESULT_DIR="${RESULT_DIR:-$RESULT_ROOT/$RUN_ID}"
SUMMARY_FILE="${SUMMARY_FILE:-$RESULT_DIR/summary.csv}"

read -r -a ROUND_INDEX_ARRAY <<< "$ROUND_INDEX"

extract_finish_field() {
    local line="$1"
    local field="$2"

    printf '%s\n' "$line" | awk -v key="$field" -F', ' '{
        prefix = key " "
        for (i = 1; i <= NF; i++) {
            if (index($i, prefix) == 1) {
                print substr($i, length(prefix) + 1)
                exit
            }
        }
    }'
}

program_path="$BIN_DIR/$TEST_PROGRAM"
if [ ! -x "$program_path" ]; then
    echo "Error: test program is not executable: $program_path" >&2
    exit 1
fi

mkdir -p "$RESULT_DIR"
printf 'round_idx,round_name,throughput,avg_latency,ops,time,raw_log\n' > "$SUMMARY_FILE"

echo "Thread Num $((THREAD_NUM_PER_CLIENT * CLIENT_NUM)), Files per Thread $FILE_PER_THREAD"
echo "Result dir: $RESULT_DIR"

for round_idx in "${ROUND_INDEX_ARRAY[@]}"
do
    round_name="${ROUND_NAME[$round_idx]}"
    raw_log="$RESULT_DIR/round_${round_idx}_${round_name}.log"

    SERVER_IP=$META_SERVER_IP \
    SERVER_PORT=$META_SERVER_PORT \
    LD_LIBRARY_PATH="/usr/local/lib64/:${LD_LIBRARY_PATH:-}" \
    "$program_path" "$MOUNT_DIR" "$FILE_PER_THREAD" "$THREAD_NUM_PER_CLIENT" "$round_idx" "$CLIENT_ID" "$MOUNT_PER_CLIENT" "$CLIENT_CACHE_SIZE" "$PORT" "$FILE_SIZE" "$CLIENT_NUM" > "$raw_log" 2>&1 &
    test_pid=$!

    sleep 1
    python3 "$SCRIPT_DIR/send_signal.py" "$SIGNAL_IP" "$PORT"

    elapsed=0
    while true
    do
        sleep "$POLL_INTERVAL_SEC"
        elapsed=$((elapsed + POLL_INTERVAL_SEC))
        if [ -f "$raw_log" ]; then
            last_line=$(awk 'NF{line=$0} END{print line}' "$raw_log")
            if [[ $last_line == *"[FINISH]"* ]]; then
                throughput=$(extract_finish_field "$last_line" "Throughput")
                avg_latency=$(extract_finish_field "$last_line" "Average Latency")
                ops=$(extract_finish_field "$last_line" "OPs")
                time_spent=$(extract_finish_field "$last_line" "Time")
                if [ -z "$throughput" ]; then
                    continue
                fi
                printf '%s,%s,%s,%s,%s,%s,%s\n' "$round_idx" "$round_name" "$throughput" "$avg_latency" "$ops" "$time_spent" "$raw_log" >> "$SUMMARY_FILE"
                break
            fi
        fi

        if ! kill -0 "$test_pid" 2>/dev/null; then
            wait "$test_pid" || true
            echo "Error: round $round_idx ($round_name) exited without a [FINISH] line; see $raw_log" >&2
            exit 1
        fi

        if [ "$ROUND_TIMEOUT_SEC" -gt 0 ] && [ "$elapsed" -ge "$ROUND_TIMEOUT_SEC" ]; then
            kill "$test_pid" 2>/dev/null || true
            wait "$test_pid" 2>/dev/null || true
            echo "Error: round $round_idx ($round_name) timed out after ${ROUND_TIMEOUT_SEC}s; see $raw_log" >&2
            exit 1
        fi
    done

    wait "$test_pid"
    echo "round $round_name done, total throughput = $throughput"
done

echo "Summary file: $SUMMARY_FILE"
