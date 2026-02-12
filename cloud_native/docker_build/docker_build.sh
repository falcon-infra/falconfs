#! /bin/bash
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
export FALCONFS_INSTALL_DIR=~/falconfs
FALCONFS_DIR=$DIR/../../

gen_config() {
    cp -f $FALCONFS_DIR/config/config.json $DIR/store/
    JSON_DIR=$DIR/store/config.json
    ## modified the content in config.json for container
    jq '.main.falcon_log_dir = "/usr/local/falconfs/falcon_store/log"' $JSON_DIR | sponge $JSON_DIR
    jq '.main.falcon_cache_root = "/usr/local/falconfs/falcon_store/cache"' $JSON_DIR | sponge $JSON_DIR
    jq '.main.falcon_mount_path = "/mnt/data"' $JSON_DIR | sponge $JSON_DIR
    jq '.main.falcon_log_reserved_num = 50' $JSON_DIR | sponge $JSON_DIR
    jq '.main.falcon_log_reserved_time = 168' $JSON_DIR | sponge $JSON_DIR
    jq '.main.falcon_stat_max = true' $JSON_DIR | sponge $JSON_DIR
    jq '.main.falcon_use_prometheus = true' $JSON_DIR | sponge $JSON_DIR
}

gen_config

pushd $FALCONFS_DIR
rm -rf $FALCONFS_INSTALL_DIR
./build.sh clean falcon
./build.sh build falcon --with-zk-init --with-prometheus  --debug
./build.sh install falcon
popd
pushd $DIR

# prepare image data for cn/dn/store
# falcon_meta 目录包含 falcon.so 和 libbrpcplugin.so，需要在容器内使用
# 所有容器都复制完整的安装包到统一路径 /usr/local/falconfs/
# 只复制一份到 docker_build/falconfs/，由各 Dockerfile 引用
rm -rf $FALCONFS_DIR/cloud_native/docker_build/falconfs
cp -rf ~/falconfs $FALCONFS_DIR/cloud_native/docker_build

# 设置文件权限
chmod 777 -R ./falconfs

# 确保脚本有执行权限
chmod +x ./cn/*.sh
chmod +x ./dn/*.sh
chmod +x ./store/*.sh
chmod +x ./regress/*.sh

popd