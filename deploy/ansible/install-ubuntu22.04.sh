#!/usr/bin/env bash
#
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

GCC_VERSION=13
apt update && apt install -y build-essential software-properties-common

# gcc-13
add-apt-repository -y ppa:ubuntu-toolchain-r/test
apt update
apt -y install gcc-$GCC_VERSION g++-$GCC_VERSION
update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-$GCC_VERSION 60 --slave /usr/bin/g++ g++ /usr/bin/g++-$GCC_VERSION
update-alternatives --set gcc /usr/bin/gcc-$GCC_VERSION

apt install -y tar wget make cmake ninja-build libreadline-dev liblz4-dev libzstd-dev libpython3-dev bison flex m4 autoconf automake pkg-config libssl-dev fuse libfuse-dev libtool libflatbuffers-dev flatbuffers-compiler libprotoc-dev libprotobuf-dev protobuf-compiler libgflags-dev libleveldb-dev libfmt-dev libgtest-dev libgmock-dev libgoogle-glog-dev libzookeeper-mt-dev libibverbs-dev

./build-gcc-14.sh

# third_party

# jsoncpp
JSONCPP_VERSION=1.9.6
cd /tmp
wget https://github.com/open-source-parsers/jsoncpp/archive/refs/tags/$JSONCPP_VERSION.tar.gz
tar -xzvf $JSONCPP_VERSION.tar.gz
cd jsoncpp-$JSONCPP_VERSION
sed -i 's/set(CMAKE_CXX_STANDARD 11)/set(CMAKE_CXX_STANDARD 17)/' CMakeLists.txt
mkdir build && cd build
cmake ..
make -j$(nproc)
make install
rm /tmp/$JSONCPP_VERSION.tar.gz

# brpc
BRPC_VERSION=1.12.1
cd /tmp
wget https://github.com/apache/brpc/archive/refs/tags/$BRPC_VERSION.tar.gz
tar -zxvf $BRPC_VERSION.tar.gz
cd brpc-$BRPC_VERSION && mkdir build && cd build
cmake -DWITH_GLOG=ON -DWITH_RDMA=ON ..
make -j$(nproc)
make install
rm /tmp/$BRPC_VERSION.tar.gz

# huaweicloud-sdk-c-obs
OBS_VERSION=3.24.12
cd /tmp
wget https://github.com/huaweicloud/huaweicloud-sdk-c-obs/archive/refs/tags/v$OBS_VERSION.tar.gz
tar -zxvf v$OBS_VERSION.tar.gz
cd huaweicloud-sdk-c-obs-$OBS_VERSION
sed -i '/if(NOT DEFINED OPENSSL_INC_DIR)/,+5d' CMakeLists.txt
sed -i '/OPENSSL/d' CMakeLists.txt
cd source/eSDK_OBS_API/eSDK_OBS_API_C++ &&
    export SPDLOG_VERSION=spdlog-1.12.0 && bash build.sh sdk &&
    mkdir -p /usr/local/obs &&
    tar zxvf sdk.tgz -C /usr/local/obs &&
    rm /usr/local/obs/lib/libcurl.so* &&
    rm /usr/local/obs/lib/libssl.so* &&
    rm /usr/local/obs/lib/libcrypto.so*
rm /tmp/v3.24.12.tar.gz
ln -s /usr/local/obs/lib/libiconv.so /usr/local/obs/lib/libiconv.so.0
