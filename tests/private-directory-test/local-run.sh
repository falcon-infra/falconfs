#!/bin/bash

BIN_DIR="/root/falconfs/build/tests/private-directory-test"
TEST_PROGRAM="test_falcon" # test_falcon / test_posix
MOUNT_DIR="/test/" # meta directory / falconfs mount path, end with /
FILE_PER_THREAD=1000
PORT=1111
FILE_SIZE=1572864
CLIENT_NUM=1
THREAD_NUM_PER_CLIENT=2000
ROUND_INDEX=(0 1 2 3)
ROUND_NAME=("workload_init" "workload_create" "workload_stat" "workload_open" "workload_close" "workload_delete" "workload_mkdir" "workload_rmdir" "workload_open_write_close" "workload_open_write_close_nocreate" "workload_open_read_close" "workload_kv_put" "workload_kv_get" "workload_kv_del" "workload_slice_put" "workload_slice_get" "workload_slice_del" "workload_uninit")
CLIENT_ID=0
MOUNT_PER_CLIENT=1
CLIENT_CACHE_SIZE=16384
META_SERVER_IP="127.0.0.1" # meta cn ip
META_SERVER_PORT="58610" #meta cn port

echo "Thread Num" $(($THREAD_NUM_PER_CLIENT * $CLIENT_NUM))", Files per Thread" $FILE_PER_THREAD

for round_idx in "${ROUND_INDEX[@]}"
do
    SERVER_IP=$META_SERVER_IP SERVER_PORT=$META_SERVER_PORT LD_LIBRARY_PATH=/usr/local/lib64/:$LD_LIBRARY_PATH $BIN_DIR/$TEST_PROGRAM $MOUNT_DIR $FILE_PER_THREAD $THREAD_NUM_PER_CLIENT $round_idx $CLIENT_ID $MOUNT_PER_CLIENT $CLIENT_CACHE_SIZE $PORT $FILE_SIZE $CLIENT_NUM > $BIN_DIR/result_1111 2>&1 &
    sleep 1
    python3 send_signal.py 127.0.0.1 $PORT

    while true
    do
        sleep 3
        total_throughput=0
        if [ -f "$BIN_DIR/result_1111" ]; then
            last_line=$(tail -n 1 "$BIN_DIR/result_1111")
            if [[ $last_line == *"[FINISH]"* ]]; then
                throughput=$(echo "$last_line" | awk -F', ' '{
                    for(i=1;i<=NF;i++){
                        if($i~/^Throughput/){
                            split($i,a," ")
                            print a[2]
                        }
                    }
                }')
                total_throughput=$(echo "$total_throughput + $throughput" | bc)
                break
                rm -f "./result_${SERVER}_${PORT}"
            fi
        fi
    done
    echo "round" ${ROUND_NAME[$round_idx]} "done, total throughput =" $total_throughput
done