# !/bin/bash

# 1. Unmount first (will stop the mounted instance)
if mount | grep -q "/mnt/data"; then
    umount -l "/mnt/data"
    echo "Unmounted /mnt/data and stopped associated falcon_client"
else
    echo "/mnt/data is not mounted"
fi

# 2. Kill any remaining falcon_client processes (for unmounted instances)
sleep 1
pids=$(pgrep -f "^\/root/falconfs/bin/falcon_client" | grep -v $$ | grep -v grep || true)
if [ -n "$pids" ]; then
    for pid in $pids; do
        if ps -p "$pid" >/dev/null; then
            kill -9 "$pid" && echo "Stopped orphaned falcon_client (PID: $pid)"
        fi
    done
else
    echo "No additional falcon_client processes found"
fi

for i in {1..2}
do
    exit=0
    for j in {1..10}
    do
        outputFile=/opt/log/falconfs_${i}_${j}.out
        if [ ! -f $outputFile ]; then
            mv /opt/log/falconfs.out $outputFile
            exit=1
            break
        fi
    done
    if [ $exit = 1 ]; then
        break
    fi
done

if [ -f "/opt/log/falconfs_2_10.out" ]; then
    for i in {1..10}
    do
        basePath=/opt/log/falconfs
        rm ${basePath}_1_$i.out
        mv ${basePath}_2_$i.out ${basePath}_1_$i.out
    done
fi