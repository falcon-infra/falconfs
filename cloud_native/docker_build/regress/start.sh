#!/bin/bash

# 使用统一安装目录
FALCONFS_INSTALL_DIR=/usr/local/falconfs
TEST_DIR=$FALCONFS_INSTALL_DIR/private-directory-test
export LD_LIBRARY_PATH=${FALCONFS_INSTALL_DIR}/private-directory-test/lib:${LD_LIBRARY_PATH}

# add prepare here, change env value of local-run.sh
sed -i 's|BIN_DIR=.*$|BIN_DIR="'$TEST_DIR'/bin/"|' $TEST_DIR/local-run.sh

if [ "$API_MODE" = "FUSE" ]; then
    # using posix api test
    sed -i 's|TEST_PROGRAM=.*$|TEST_PROGRAM="test_posix"|' $TEST_DIR/local-run.sh
    sed -i 's|MOUNT_DIR=.*$|MOUNT_DIR="/mnt/data/"|' $TEST_DIR/local-run.sh
else
    # using falcon api test
    sed -i 's|TEST_PROGRAM=.*$|TEST_PROGRAM="test_falcon"|' $TEST_DIR/local-run.sh
    sed -i 's|MOUNT_DIR=.*$|MOUNT_DIR="/"|' $TEST_DIR/local-run.sh
fi
sed -i 's|FILE_PER_THREAD=.*$|FILE_PER_THREAD=10|' $TEST_DIR/local-run.sh
sed -i 's|THREAD_NUM_PER_CLIENT=.*$|THREAD_NUM_PER_CLIENT=5|' $TEST_DIR/local-run.sh
# shellcheck disable=SC2016
sed -i 's|META_SERVER_IP=.*$|META_SERVER_IP=${META_SERVER_IP}|' $TEST_DIR/local-run.sh
# change to variable after get from "cloud_native/docker_build/cn/start.sh", and falcon client need modify the logic of get server port too.
sed -i 's|META_SERVER_PORT=.*$|META_SERVER_PORT=5442|' $TEST_DIR/local-run.sh

# shellcheck disable=SC2164
cd $TEST_DIR

bash local-run.sh

# run falcon_cm test
export CONFIG_FILE=/opt/conf/config.json
$TEST_DIR/bin/FalconCMIT
