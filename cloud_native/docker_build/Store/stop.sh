# !/bin/bash

# 1. Unmount first (will stop the mounted instance)
if mount | grep -q "/mnt/falcon"; then
    fusermount -u "/mnt/falcon"
    echo "Unmounted /mnt/falcon and stopped associated falcon_client"
else
    echo "/mnt/falcon is not mounted"
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
        outputFile=/opt/output/falconfs_${i}_${j}.out
        if [ ! -f $outputFile ]; then
            mv /opt/output/falconfs.out $outputFile
            exit=1
            break
        fi
    done
    if [ $exit = 1 ]; then
        break
    fi
done

if [ -f "/opt/output/falconfs_2_10.out" ]; then
    for i in {1..10}
    do
        basePath=/opt/output/falconfs
        rm ${basePath}_1_$i.out
        mv ${basePath}_2_$i.out ${basePath}_1_$i.out
    done
fi