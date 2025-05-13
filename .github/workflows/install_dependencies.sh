#!/bin/bash

set -euo pipefail

sudo apt-get update
echo -e "Asia\nShanghai" | sudo apt-get install -y tzdata
apt-get install -y locales && locale-gen en_US.UTF-8
export LANG=en_US.UTF-8
export LC_ALL=en_US.UTF-8
sudo apt-get install -y gcc-14 g++-14
sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-14 60
sudo update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-14 60
sudo update-alternatives --set gcc /usr/bin/gcc-14
sudo update-alternatives --set g++ /usr/bin/g++-14
sudo ln -sf /usr/lib/x86_64-linux-gnu/libstdc++.so.6.0.33 /usr/lib/libstdc++.so.6
sudo ln -sf /usr/lib/libstdc++.so.6 /usr/lib/libstdc++.so
sudo apt-get install -y tar wget make cmake ninja-build libreadline-dev liblz4-dev libzstd-dev libpython3-dev bison flex m4 autoconf automake pkg-config libssl-dev fuse libfuse-dev libtool libflatbuffers-dev flatbuffers-compiler libprotoc-dev libprotobuf-dev protobuf-compiler libgflags-dev libjsoncpp-dev libleveldb-dev libfmt-dev libgtest-dev libgmock-dev libgoogle-glog-dev libzookeeper-mt-dev libibverbs-dev
sudo mv /usr/include/jsoncpp/json /usr/include/json && sudo rm -rf /usr/include/jsoncpp

export BRPC_VERSION=1.12.1
export BRPC_DOWNLOAD_URL=https://github.com/apache/brpc/archive/refs/tags/${BRPC_VERSION}.tar.gz
wget -O- "${BRPC_DOWNLOAD_URL}" | tar -xzvf - -C /tmp &&
    cd "/tmp/brpc-${BRPC_VERSION}" &&
    mkdir build && cd build &&
    cmake -GNinja -DWITH_GLOG=ON -DWITH_RDMA=ON .. &&
    ninja && sudo ninja install &&
    rm -rf "/tmp/brpc-${BRPC_VERSION}"

export OBS_VERSION=v3.24.12
export OBS_DOWNLOAD_URL=https://github.com/huaweicloud/huaweicloud-sdk-c-obs/archive/refs/tags/${OBS_VERSION}.tar.gz
export OBS_INSTALL_PREFIX=/usr/local/obs
wget -O- "${OBS_DOWNLOAD_URL}" | tar -xzvf - -C /tmp &&
    cd "/tmp/huaweicloud-sdk-c-obs-${OBS_VERSION#v}" &&
    sed -i '/if(NOT DEFINED OPENSSL_INC_DIR)/,+5d' CMakeLists.txt &&
    sed -i '/OPENSSL/d' CMakeLists.txt &&
    cd source/eSDK_OBS_API/eSDK_OBS_API_C++ &&
    export SPDLOG_VERSION=spdlog-1.12.0 && bash build.sh sdk &&
    sudo mkdir -p "${OBS_INSTALL_PREFIX}" &&
    sudo tar zxvf sdk.tgz -C "${OBS_INSTALL_PREFIX}" &&
    sudo rm /usr/local/obs/lib/libcurl.so* &&
    sudo rm /usr/local/obs/lib/libssl.so* &&
    sudo rm /usr/local/obs/lib/libcrypto.so* &&
    sudo rm -rf "/tmp/huaweicloud-sdk-c-obs-${OBS_VERSION#v}"

sudo apt-get install -y fio
