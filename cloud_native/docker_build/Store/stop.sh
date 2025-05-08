# !/bin/bash
isProcessAlive=`ps -ef | grep -m 1 falcon_client | grep -v "grep" | wc -l`
if [ "${isProcessAlive}" = "0" ]; then
    echo "no running FuseClient"
else
    falconfsPid=`ps -ef | grep -m 1 falcon_client | awk '{print $2}'`
    kill -9 $falconfsPid
fi

umount -l /mnt/falcon

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