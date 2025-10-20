#!/bin/bash

ALL_SERVERS=(
    "127.0.0.1"
)

# test_falcon/test_posix <ROOT DIR (end with /)> <FILES PER DIR> <THREAD NUMBER> <ROUND INDEX> <CLIENT ID> <MOUNT_PER_CLIENT> <CLIENT CACHE SIZE> <WAIT PORT> <FILE SIZE> <CLIENT NUMBER>

MOUNT_DIR="/" # falconfs mount path, end with /
BIN_DIR="/root/falconfs/build/tests/private-directory-test/"
TEST_PROGRAM="test_falcon"
ROUND_NUM=(0 1 2 3)
FILE_PER_THREAD=10
PORT=1111
FILE_SIZE=1572864
CLIENT_NUM=10
THREAD_NUM_PER_CLIENT=12
META_SERVER_IP=""
META_SERVER_PORT=""
ROUND_NAME=("workload_init" "workload_create" "workload_stat" "workload_open" "workload_close" "workload_delete" "workload_mkdir" "workload_rmdir" "workload_open_write_close" "workload_open_write_close_nocreate" "workload_open_read_close" "workload_uninit")


for round_idx in "${ROUND_NUM[@]}"
do
    for ((i=0; i<${#ALL_SERVERS[@]}; i++))
    do
        SERVER=${ALL_SERVERS[$i]}
        ssh $SERVER "SERVER_IP=$META_SERVER_IP SERVER_PORT=$META_SERVER_PORT LD_LIBRARY_PATH=/usr/local/lib64/:$LD_LIBRARY_PATH $BIN_DIR/$TEST_PROGRAM $MOUNT_DIR $FILE_PER_THREAD $THREAD_NUM $round_idx $i 1 16384 $PORT $FILE_SIZE $CLIENT_NUM &> $BIN_DIR/result_1111 &"
    done

    sleep 1
    for server in "${ALL_SERVERS[@]}"; do
        python3 send_signal.py $server $PORT
    done

    while true
    do
        sleep 3
        total_throughput=0
        done_count=0
        for ((i=0; i<${#ALL_SERVERS[@]}; i++))
        do
            SERVER=${ALL_SERVERS[$i]}
            # change to actual user
            scp -q $USER@$SERVER:$BIN_DIR/result_${PORT} ./result_${SERVER}_${PORT}
            if [ -f "./result_${SERVER}_${PORT}" ]; then
                last_line=$(tail -n 1 "./result_${SERVER}_${PORT}")
                if [[ $last_line == *"[FINISH]"* ]]; then
                    # throughput=$(echo "$last_line" | awk '{print $NF}')
                    throughput=$(echo "$last_line" | awk -F', ' '{
                        for(i=1;i<=NF;i++){
                            if($i~/^Throughput/){
                                split($i,a," ")
                                print a[2]
                            }
                        }
                    }')
                    echo $SERVER ${PORT} "throughput:" $throughput
                    total_throughput=$(echo "$total_throughput + $throughput" | bc)
                    ((done_count++))
                    # rm -f "./result_${SERVER}_${PORT}"
                fi
            fi
        done
        if [ $done_count -eq ${#ALL_SERVERS[@]} ]; then
            break
        fi
    done
    echo "round" ${ROUND_NAME[$round_idx]} "done, total throughput = " $total_throughput
done