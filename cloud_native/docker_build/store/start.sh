#!/bin/bash

# 设置默认的安装目录
FALCONFS_INSTALL_DIR=${FALCONFS_INSTALL_DIR:-/usr/local/falconfs}

# prepare the directory to store files
CACHE_PATH=${FALCONFS_INSTALL_DIR}/falcon_store/cache
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

export LD_LIBRARY_PATH=${FALCONFS_INSTALL_DIR}/falcon_client/lib:${LD_LIBRARY_PATH}
${FALCONFS_INSTALL_DIR}/falcon_client/bin/falcon_client /mnt/data -f -o direct_io,allow_other,nonempty -o attr_timeout=20 -o entry_timeout=20 -brpc true -rpc_endpoint=0.0.0.0:50039 -socket_max_unwritten_bytes=268435456 > ${FALCONFS_INSTALL_DIR}/falcon_store/log/falcon_client.log 2>&1 &
