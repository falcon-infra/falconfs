#!/bin/bash

# prepare the directory to store files

CACHE_PATH=/opt/falcon/
if [ ! -d ${CACHE_PATH} ]; then
    mkdir ${CACHE_PATH}
    for i in {0..100}
    do
        mkdir ${CACHE_PATH}/$i
    done
else
    if [ ! -d ${CACHE_PATH}/0 ]; then
        for i in {0..100}
        do
            mkdir ${CACHE_PATH}/$i
        done
    fi
fi

if [ ! -d /mnt/data ]; then
    mkdir -p /mnt/data
else
    rm -rf /mnt/data/*
fi

/root/falconfs/bin/falcon_client /mnt/data -f -o direct_io,allow_other,nonempty -o attr_timeout=20 -o entry_timeout=20 -brpc true -rpc_endpoint=0.0.0.0:50039 -socket_max_unwritten_bytes=268435456 > /opt/log/falconfs.out 2>&1 &
