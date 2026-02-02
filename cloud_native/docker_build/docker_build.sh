#! /bin/bash
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
export FALCONFS_INSTALL_DIR=~/metadb
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
rm -rf $FALCONFS_INSTALL_DIR
./build.sh clean pg
./build.sh build pg
./build.sh install pg
./build.sh clean falcon
./build.sh build falcon --with-zk-init --with-prometheus
./build.sh install falcon
popd
pushd $DIR

# prepare image data for store
mkdir -p $DIR/store/falconfs/bin/
mkdir -p $DIR/store/falconfs/lib/
./ldd_copy.sh -b $FALCONFS_DIR/build/bin/falcon_client -t $DIR/store/falconfs/lib/
cp -f $FALCONFS_DIR/build/bin/falcon_client $DIR/store/falconfs/bin/
./ldd_copy.sh -b $FALCONFS_DIR/build/tests/private-directory-test/test_falcon -t $DIR/store/falconfs/lib/
./ldd_copy.sh -b $FALCONFS_DIR/build/tests/private-directory-test/test_posix -t $DIR/store/falconfs/lib/
cp -rf $FALCONFS_DIR/tests/private-directory-test $DIR/store/falconfs/
cp -f $FALCONFS_DIR/build/tests/private-directory-test/test_falcon $DIR/store/falconfs/bin/
cp -f $FALCONFS_DIR/build/tests/private-directory-test/test_posix $DIR/store/falconfs/bin/

# prepare image data for regress
mkdir -p $FALCONFS_DIR/tests/regress/falconfs/bin/
mkdir -p $FALCONFS_DIR/tests/regress/falconfs/lib/
./ldd_copy.sh -b $FALCONFS_DIR/build/tests/private-directory-test/test_falcon -t $FALCONFS_DIR/tests/regress/falconfs/lib/
./ldd_copy.sh -b $FALCONFS_DIR/build/tests/private-directory-test/test_posix -t $FALCONFS_DIR/tests/regress/falconfs/lib/
./ldd_copy.sh -b $FALCONFS_DIR/build/tests/common/FalconCMIT -t $FALCONFS_DIR/tests/regress/falconfs/lib/
cp -f $FALCONFS_DIR/build/tests/private-directory-test/test_falcon  $FALCONFS_DIR/tests/regress/falconfs/bin/
cp -f $FALCONFS_DIR/build/tests/private-directory-test/test_posix $FALCONFS_DIR/tests/regress/falconfs/bin/
cp -f $FALCONFS_DIR/build/tests/common/FalconCMIT $FALCONFS_DIR/tests/regress/falconfs/bin/
cp -f $FALCONFS_DIR/tests/private-directory-test/local-run.sh $FALCONFS_DIR/tests/regress/falconfs/
cp -f $FALCONFS_DIR/tests/private-directory-test/send_signal.py $FALCONFS_DIR/tests/regress/falconfs/
cp -f $FALCONFS_DIR/tests/regress/start.sh $FALCONFS_DIR/tests/regress/falconfs
cp -f $FALCONFS_DIR/tests/regress/stop.sh  $FALCONFS_DIR/tests/regress/falconfs
cp -f $FALCONFS_DIR/tests/regress/docker-entrypoint.sh $FALCONFS_DIR/tests/regress/falconfs

# prepare image data for cn/dn
./ldd_copy.sh -b ~/metadb/lib/postgresql/libbrpcplugin.so -t ~/metadb/lib/
rm -rf ./cn/metadb
cp -rf ~/metadb ./cn/
rm -rf ./dn/metadb
cp -rf ~/metadb ./dn/
rm -rf ./cn/falcon_cm
cp -rf $FALCONFS_DIR/cloud_native/falcon_cm ./cn/
rm -rf ./dn/falcon_cm
cp -rf $FALCONFS_DIR/cloud_native/falcon_cm ./dn/

chmod 777 -R ./cn/metadb
chmod 777 -R ./cn/falcon_cm
chmod 777 -R ./dn/metadb
chmod 777 -R ./dn/falcon_cm
chmod 777 -R ./store/falconfs
chmod 777 -R $FALCONFS_DIR/tests/regress/falconfs

gen_config
popd