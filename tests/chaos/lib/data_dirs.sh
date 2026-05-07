#!/bin/bash

# Data directory, mountpoint, and container restart helpers.

prepare_data_dirs() {
    local idx
    sudo umount -l "${DATA_PATH}/falcon-data/store-1/data" >/dev/null 2>&1 || true

    for idx in 1 2 3; do
        mkdir -p "${DATA_PATH}/falcon-data/zk-${idx}/data"
    done

    for idx in 1 2 3; do
        mkdir -p "${DATA_PATH}/falcon-data/cn-${idx}/data"
    done

    for idx in 1 2 3; do
        mkdir -p "${DATA_PATH}/falcon-data/dn-${idx}/data"
    done

    mkdir -p "${DATA_PATH}/falcon-data/store-1/cache"
    mkdir -p "${DATA_PATH}/falcon-data/store-1/log"
    mkdir -p "${DATA_PATH}/falcon-data/store-1/data"
}

clean_data_dirs() {
    local idx
    sudo umount -l "${DATA_PATH}/falcon-data/store-1/data" >/dev/null 2>&1 || true

    for idx in 1 2 3; do
        sudo rm -rf "${DATA_PATH}/falcon-data/zk-${idx}/data"/* >/dev/null 2>&1 || true
    done

    for idx in 1 2 3; do
        sudo rm -rf "${DATA_PATH}/falcon-data/cn-${idx}/data"/* >/dev/null 2>&1 || true
    done

    for idx in 1 2 3; do
        sudo rm -rf "${DATA_PATH}/falcon-data/dn-${idx}/data"/* >/dev/null 2>&1 || true
    done

    sudo rm -rf "${DATA_PATH}/falcon-data/store-1/cache"/* >/dev/null 2>&1 || true
    sudo rm -rf "${DATA_PATH}/falcon-data/store-1/log"/* >/dev/null 2>&1 || true
    sudo rm -rf "${DATA_PATH}/falcon-data/store-1/data"/* >/dev/null 2>&1 || true
}

restart_container() {
    local name="$1"
    docker restart "$name" >/dev/null
}

reset_store_mountpoint() {
    sudo umount -l "${DATA_PATH}/falcon-data/store-1/data" >/dev/null 2>&1 || true
    sudo mkdir -p "${DATA_PATH}/falcon-data/store-1/data" >/dev/null 2>&1 || true
}
