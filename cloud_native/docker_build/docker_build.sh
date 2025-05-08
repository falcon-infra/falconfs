#! /bin/bash
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

FALCONFS_DIR=$DIR/../../
pushd $FALCONFS_DIR
./build.sh build pg && ./build.sh install
./build.sh build falcon --with-zk-init
popd
pushd $DIR
./ldd_copy.sh -b ~/metadb/lib/postgresql/falcon.so -t ~/metadb/lib/
./ldd_copy.sh -b $FALCONFS_DIR/build/bin/falcon_client -t $DIR/Store/falconfs/lib/

cp -f $FALCONFS_DIR/build/bin/falcon_client $DIR/Store/falconfs/bin/
cp -rf ~/metadb ./CN/
cp -rf ~/metadb ./DN/
cp -rf $FALCONFS_DIR/cloud_native/falcon_cm ./CN/
cp -rf $FALCONFS_DIR/cloud_native/falcon_cm ./DN/
popd