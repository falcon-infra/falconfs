#!/bin/bash
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

# director of falcon code, used for gdb in contain
FALCON_CODE_PATH=$(realpath "$DIR/../../")
export FALCON_CODE_PATH

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

function rebuild_falcon() {
    # rebuild falconfs using "cloud_native/docker_build/docker_build.sh" manually.
    # cd "${FALCON_CODE_PATH}"/third_party/postgres
    # git restore .
    # cd "${FALCON_CODE_PATH}"
    # #git pull --rebase

    # up_flag=$(docker ps -a --filter "name=falcon-dev" --format "{{.Names}}\t{{.Status}}" | awk "/Up/" | wc -l)
    # if [ "$up_flag" -eq 0 ]; then
    #     docker start falcon-dev
    # fi
    # docker exec -e LD_LIBRARY_PATH=/usr/local/obs/lib -e CPLUS_INCLUDE_PATH=/usr/local/obs/include falcon-dev \
    #     /root/code/falconfs/cloud_native/docker_build/docker_build.sh
    # Build FalconFS directly from repository scripts.
    # Some environments do not provide falconenv.sh.
    "${FALCON_CODE_PATH}"/cloud_native/docker_build/docker_build.sh
    return 0
}

function rebuild_images() {
    # rebuild cn image (构建上下文是 docker_build/)
    cd "${FALCON_CODE_PATH}"/cloud_native/docker_build
    docker build --platform linux/amd64 \
        --build-arg CACHE_BUST=$(date +%s) \
        -t localhost:5000/falconfs-cn:ubuntu24.04 \
        -f Dockerfile.cn \
        . \
        --push

    # rebuild dn image
    docker build --platform linux/amd64 \
        --build-arg CACHE_BUST=$(date +%s) \
        -t localhost:5000/falconfs-dn:ubuntu24.04 \
        -f Dockerfile.dn \
        . \
        --push

    # rebuild store image
    docker build --platform linux/amd64 \
        --build-arg CACHE_BUST=$(date +%s) \
        -t localhost:5000/falconfs-store:ubuntu24.04 \
        -f Dockerfile.store \
        . \
        --push

    # rebuild regress image
    cd "${FALCON_CODE_PATH}"/cloud_native/docker_build/
    docker build --platform linux/amd64 \
        --build-arg CACHE_BUST=$(date +%s) \
        -t localhost:5000/falconfs-regress:ubuntu24.04 \
        -f Dockerfile.regress \
        . \
        --push
}

function clear_one_image() {
    rep_info=$(docker images --format " {{.Repository}}" "$1")
    if [ "${rep_info}" == "$1" ]; then
        docker image rm "$1"
    fi
}

function clear_images() {
    # clear pre images of docker before rebuild
    clear_one_image "localhost:5000/falconfs-cn:ubuntu24.04"
    clear_one_image "localhost:5000/falconfs-dn:ubuntu24.04"
    clear_one_image "localhost:5000/falconfs-store:ubuntu24.04"
    clear_one_image "localhost:5000/falconfs-regress:ubuntu24.04"
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
if [ $# -eq 1 ] && [ -d "$1" ]; then
    echo "run regress test:$0 $1"
    export FALCON_DATA_PATH=$1
else
    echo "usage: $0 falcon_data_save_dir"
    exit 1
fi

# down load code and rebuild falcon
rebuild_falcon

# rebuild falcon images
rebuild_images

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
