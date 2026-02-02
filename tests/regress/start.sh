#!/bin/bash

# add prepare here, change env value of local-run.sh
sed -i 's|BIN_DIR=.*$|BIN_DIR="/root/falconfs/bin/"|' /root/falconfs/local-run.sh

if [ "$API_MODE" = "FUSE" ]; then
    # using posix api test
    sed -i 's|TEST_PROGRAM=.*$|TEST_PROGRAM="test_posix"|' /root/falconfs/local-run.sh
    sed -i 's|MOUNT_DIR=.*$|MOUNT_DIR="/mnt/data/"|' /root/falconfs/local-run.sh
else
    # using falcon api test
    sed -i 's|TEST_PROGRAM=.*$|TEST_PROGRAM="test_falcon"|' /root/falconfs/local-run.sh
    sed -i 's|MOUNT_DIR=.*$|MOUNT_DIR="/"|' /root/falconfs/local-run.sh
fi
sed -i 's|FILE_PER_THREAD=.*$|FILE_PER_THREAD=10|' /root/falconfs/local-run.sh
sed -i 's|THREAD_NUM_PER_CLIENT=.*$|THREAD_NUM_PER_CLIENT=5|' /root/falconfs/local-run.sh
# shellcheck disable=SC2016
sed -i 's|META_SERVER_IP=.*$|META_SERVER_IP=${META_SERVER_IP}|' /root/falconfs/local-run.sh
# change to variable after get from "cloud_native/docker_build/cn/start.sh", and falcon client need modify the logic of get server port too.
sed -i 's|META_SERVER_PORT=.*$|META_SERVER_PORT=5442|' /root/falconfs/local-run.sh

# shellcheck disable=SC2164
cd /root/falconfs/

bash local-run.sh

# run falcon_cm test
echo "----------------------------------------------Running CM IT----------------------------------------------"
export CONFIG_FILE=/opt/conf/config.json
./bin/FalconCMIT