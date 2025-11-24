#! /bin/bash
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
FALCONFS_INSTALL_DIR=~/metadb
FALCONFS_DIR=$DIR/../../

gen_config() {
    cp -f $FALCONFS_DIR/config/config.json $DIR/store/
    JSON_DIR=$DIR/store/config.json
    ## modified the content in config.json for container
    jq '.main.falcon_log_dir = "/opt/log"' $JSON_DIR | sponge $JSON_DIR
    jq '.main.falcon_cache_root = "/opt/falcon"' $JSON_DIR | sponge $JSON_DIR
    jq '.main.falcon_mount_path = "/mnt/data"' $JSON_DIR | sponge $JSON_DIR
    jq '.main.falcon_log_reserved_num = 50' $JSON_DIR | sponge $JSON_DIR
    jq '.main.falcon_log_reserved_time = 168' $JSON_DIR | sponge $JSON_DIR
    jq '.main.falcon_stat_max = true' $JSON_DIR | sponge $JSON_DIR
    jq '.main.falcon_use_prometheus = true' $JSON_DIR | sponge $JSON_DIR
}

pushd $FALCONFS_DIR
./build.sh clean
./build.sh build pg
./build.sh build falcon --with-zk-init --with-prometheus
./build.sh install
popd
pushd $DIR

cp -rf ~/metadb ./cn/
cp -rf ~/metadb ./dn/
cp -rf $FALCONFS_DIR/cloud_native/falcon_cm ./cn/
cp -rf $FALCONFS_DIR/cloud_native/falcon_cm ./dn/
mkdir -p $DIR/store/falconfs/bin/
mkdir -p $DIR/store/falconfs/lib/
cp -f $FALCONFS_DIR/build/bin/falcon_client $DIR/store/falconfs/bin/

chmod 777 -R ./cn/metadb
chmod 777 -R ./cn/falcon_cm
chmod 777 -R ./dn/metadb
chmod 777 -R ./dn/falcon_cm
chmod 777 -R ./store/falconfs

gen_config
popd