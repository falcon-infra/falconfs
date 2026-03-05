#!/bin/bash
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

# director of falcon code, used for gdb in contain
FALCON_CODE_PATH=$(realpath "$DIR/../../")
export FALCON_CODE_PATH
export FALCON_FULL_IMAGE=${FALCON_FULL_IMAGE:-localhost:5000/falconfs-full-ubuntu24.04:v0.1.0}
export SKIP_IMAGE_BUILD=${SKIP_IMAGE_BUILD:-0}

# Change value of FALCON_CN_DN_NUM or FALCON_STORE_NUM need change compose yaml too.
FALCON_CN_NUM=3
FALCON_DN_NUM=3
# used for clean data, using fixed num 3
FALCON_ZK_NUM=3
# for simple now only create one store node.
FALCON_STORE_NUM=1

function uninstall_cluster() {
    cd "${DIR}"
    # unmount fuse filesystem before reinstall
    for ((idx = 1; idx <= FALCON_STORE_NUM; idx++)); do
        # do umount
        mount_flag=$(mount -t fuse.falcon_client | awk "/store-${idx}/" | wc -l)
        if [ "${mount_flag}" -eq "1" ]; then
            sudo umount -l "${FALCON_DATA_PATH}"/falcon-data/store-"${idx}"/data
        fi
    done
    # Stop and remove pre containers resources
    docker-compose -f "${1}" down
}

function clean_run_data() {
    # clear history data of zk node
    for ((idx = 1; idx <= FALCON_ZK_NUM; idx++)); do
        if [ -d "${FALCON_DATA_PATH}"/falcon-data/zk-"${idx}"/data ]; then
            mkdir -p "${FALCON_DATA_PATH}"/falcon-data/zk-"${idx}"/data/
        else
            sudo rm -rf "${FALCON_DATA_PATH}"/falcon-data/zk-"${idx}"/data/*
        fi
    done

    # clear history data of cn node
    for ((idx = 1; idx <= FALCON_CN_NUM; idx++)); do
        if [ -d "${FALCON_DATA_PATH}"/falcon-data/cn-"${idx}"/data ]; then
            sudo rm -rf "${FALCON_DATA_PATH}"/falcon-data/cn-"${idx}"/data/*
        else
            mkdir -p "${FALCON_DATA_PATH}"/falcon-data/cn-"${idx}"/data/
        fi
    done

    # clear history data of dn node
    for ((idx = 1; idx <= FALCON_DN_NUM; idx++)); do
        if [ -d "${FALCON_DATA_PATH}"/falcon-data/dn-"${idx}"/data ]; then
            sudo rm -rf "${FALCON_DATA_PATH}"/falcon-data/dn-"${idx}"/data/*
        else
            mkdir -p "${FALCON_DATA_PATH}"/falcon-data/dn-"${idx}"/data/
        fi
    done

    # clear history data of store node
    for ((idx = 1; idx <= FALCON_STORE_NUM; idx++)); do
        if [ -d "${FALCON_DATA_PATH}"/falcon-data/store-"${idx}" ]; then
            sudo rm -rf "${FALCON_DATA_PATH}"/falcon-data/store-"${idx}"/cache/*
            sudo rm -rf "${FALCON_DATA_PATH}"/falcon-data/store-"${idx}"/log/*
            sudo rm -rf "${FALCON_DATA_PATH}"/falcon-data/store-"${idx}"/data/*
        else
            mkdir -p "${FALCON_DATA_PATH}"/falcon-data/store-"${idx}"/cache/*
            mkdir -p "${FALCON_DATA_PATH}"/falcon-data/store-"${idx}"/log/*
            mkdir -p "${FALCON_DATA_PATH}"/falcon-data/store-"${idx}"/data/*
        fi
    done
}

function build_unified_image() {
    local deb_path="${FALCON_CODE_PATH}/falconfs-deb-full.deb"
    local dockerfile_path="${FALCON_CODE_PATH}/docker/ubuntu24.04-full-runtime-dockerfile"

    if [ "${SKIP_IMAGE_BUILD}" = "1" ]; then
        echo "skip unified image build because SKIP_IMAGE_BUILD=1"
        return 0
    fi

    if [ ! -f "${deb_path}" ]; then
        echo "ERROR: full deb package not found: ${deb_path}"
        echo "Please copy the package first, for example:"
        echo "  cp ../falconfs_0.1.0-1_amd64.deb ${deb_path}"
        exit 1
    fi

    if [ ! -f "${dockerfile_path}" ]; then
        echo "ERROR: dockerfile not found: ${dockerfile_path}"
        exit 1
    fi

    cd "${FALCON_CODE_PATH}"
    docker build --platform linux/amd64 \
        --build-arg CACHE_BUST=$(date +%s) \
        --build-arg DEB_PRECHECK=0 \
        -t "${FALCON_FULL_IMAGE}" \
        -f "${dockerfile_path}" \
        . \
        --push
}

function clear_one_image() {
    if docker image inspect "$1" >/dev/null 2>&1; then
        docker image rm "$1"
    fi
}

function clear_images() {
    if [ "${SKIP_IMAGE_BUILD}" = "1" ]; then
        return 0
    fi
    # clear pre images of docker before rebuild
    clear_one_image "${FALCON_FULL_IMAGE}"
}

function run_regress() {
    cd "${DIR}"
    # restart containers
    docker-compose -f "${1}" up -d

    # wait for falcon created in zk
    falcon_flag=$(docker exec falcon-zk-1 zkCli.sh -server localhost:2181 ls / | awk '/falcon/' | wc -l)
    waited_times=0
    sleep_interval=5
    while [ "${falcon_flag}" -ne "1" ]; do
        echo "falcon cluster not ready, wait ${waited_times} second ... "
        sleep $sleep_interval
        falcon_flag=$(docker exec falcon-zk-1 zkCli.sh -server localhost:2181 ls / | awk '/falcon/' | wc -l)
        ((waited_times += sleep_interval))
    done
    echo "falcon exist now, cost ${waited_times} second."

    # wait for cluster ready
    ready_flag=$(docker exec falcon-zk-1 zkCli.sh -server localhost:2181 ls /falcon | awk '/ready/' | wc -l)
    waited_times=0
    sleep_interval=5
    while [ "${ready_flag}" -ne "1" ]; do
        echo "falcon cluster not ready, wait ${waited_times} second ... "
        sleep $sleep_interval
        ready_flag=$(docker exec falcon-zk-1 zkCli.sh -server localhost:2181 ls /falcon | awk '/ready/' | wc -l)
        ((waited_times += sleep_interval))
    done
    echo "falcon cluster ready now, cost ${waited_times} second."

    # mount fuse.falcon_client filesystem in store container
    for ((idx = 1; idx <= FALCON_STORE_NUM; idx++)); do
        docker exec -d falcon-store-${idx} /usr/local/falconfs/falcon_store/start.sh
    done

    # wait for filesystem of falcon store mounted
    mounted_num=$(df -T | awk '/fuse.falcon_client/' | wc -l)
    waited_times=0
    sleep_interval=5
    while [ "${mounted_num}" -ne "${FALCON_STORE_NUM}" ]; do
        echo "fuse.falcon_client file system not ready, wait ${waited_times} second ... "
        sleep $sleep_interval
        mounted_num=$(df -T | awk '/fuse.falcon_client/' | wc -l)
        ((waited_times += sleep_interval))
    done
    echo "fuse.falcon_client file system ready, cost ${waited_times} second."
    META_SERVER_IP=$(docker exec falcon-zk-1 zkCli.sh -server localhost:2181 get /falcon/leaders/cn | grep ":5432" | sed 's/:5432//')
    docker exec -e META_SERVER_IP="${META_SERVER_IP}" falcon-regress-1 /usr/local/falconfs/falcon_regress/start.sh
}

# check input parameter
if [ $# -eq 1 ]; then
    mkdir -p "$1"
    echo "run regress test:$0 $1"
    export FALCON_DATA_PATH=$1
else
    echo "usage: $0 falcon_data_save_dir"
    exit 1
fi

# build one unified image from full deb package
build_unified_image

# uninstall cluster, avoid clear_images failed
uninstall_cluster docker-compose-single.yaml
clear_images

compose_files=('docker-compose-single.yaml' 'docker-compose-dual.yaml' 'docker-compose-triple.yaml')

for compose_file in "${compose_files[@]}"; do
    echo "start regress test for ${compose_file}..."
    # clear pre run data
    clean_run_data
    # start regress from single replica
    run_regress "${compose_file}"
    echo "end regress test for ${compose_file}..."
    # uninstall cluster
    uninstall_cluster "${compose_file}"
done
