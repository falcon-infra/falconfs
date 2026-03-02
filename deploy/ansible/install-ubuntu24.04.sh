#!/usr/bin/env bash

apt-get update
apt-get install -y locales
locale-gen en_US.UTF-8
apt-get install -y gcc-14 g++-14
update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-14 60
update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-14 60
update-alternatives --set gcc /usr/bin/gcc-14
update-alternatives --set g++ /usr/bin/g++-14

try_download() {
	local url=$1
	local output=$2
	wget --timeout=120 --tries=3 -O "$output" "$url"
}

apt-get install -y tar wget make cmake ninja-build build-essential libreadline-dev liblz4-dev libzstd-dev libpython3-dev bison flex m4 autoconf automake pkg-config libssl-dev fuse libfuse-dev libtool libflatbuffers-dev flatbuffers-compiler libprotoc-dev libprotobuf-dev protobuf-compiler libgflags-dev libleveldb-dev libfmt-dev libgtest-dev libgmock-dev libgoogle-glog-dev libzookeeper-mt-dev libibverbs-dev libboost-dev libboost-system-dev libboost-thread-dev libboost-filesystem-dev libboost-program-options-dev git

# Configure git to handle owned directories
git config --global --add safe.directory '*'

# 从源码编译安装 PostgreSQL 17
PG_VERSION=17.2
PG_DOWNLOAD_URL=https://ftp.postgresql.org/pub/source/v${PG_VERSION}/postgresql-${PG_VERSION}.tar.gz
cd /tmp
try_download "${PG_DOWNLOAD_URL}" "postgresql-${PG_VERSION}.tar.gz"
tar -xzvf postgresql-${PG_VERSION}.tar.gz
rm postgresql-${PG_VERSION}.tar.gz
cd postgresql-${PG_VERSION}
./configure --prefix=/usr/local/pgsql --without-icu --enable-debug
make -j$(nproc)
make install
cd /tmp
rm -rf postgresql-${PG_VERSION}

# 设置 PostgreSQL 环境变量
export PATH=/usr/local/pgsql/bin:$PATH
export LD_LIBRARY_PATH=/usr/local/pgsql/lib:$LD_LIBRARY_PATH
echo "export PATH=/usr/local/pgsql/bin:\$PATH" >>/etc/profile
echo "export LD_LIBRARY_PATH=/usr/local/pgsql/lib:\$LD_LIBRARY_PATH" >>/etc/profile
pg_config --version

# jsoncpp
JSONCPP_VERSION=1.9.6
cd /tmp
try_download "https://github.com/open-source-parsers/jsoncpp/archive/refs/tags/$JSONCPP_VERSION.tar.gz" "$JSONCPP_VERSION.tar.gz"
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
try_download "https://github.com/apache/brpc/archive/refs/tags/$BRPC_VERSION.tar.gz" "$BRPC_VERSION.tar.gz"
tar -zxvf $BRPC_VERSION.tar.gz
cd brpc-$BRPC_VERSION
mkdir build
cd build
cmake -DWITH_GLOG=ON -DWITH_RDMA=ON ..
make -j$nproc
make install
rm /tmp/$BRPC_VERSION.tar.gz

# huaweicloud-sdk-c-obs
OBS_VERSION=3.24.12
cd /tmp
try_download "https://github.com/huaweicloud/huaweicloud-sdk-c-obs/archive/refs/tags/v$OBS_VERSION.tar.gz" "v$OBS_VERSION.tar.gz"
tar -zxvf v$OBS_VERSION.tar.gz
cd huaweicloud-sdk-c-obs-$OBS_VERSION
sed -i '/if(NOT DEFINED OPENSSL_INC_DIR)/,+5d' CMakeLists.txt
sed -i '/OPENSSL/d' CMakeLists.txt
cd source/eSDK_OBS_API/eSDK_OBS_API_C++
export SPDLOG_VERSION=spdlog-1.12.0
bash build.sh sdk
mkdir -p /usr/local/obs
tar zxvf sdk.tgz -C /usr/local/obs
rm /usr/local/obs/lib/libcurl.so*
rm /usr/local/obs/lib/libssl.so*
rm /usr/local/obs/lib/libcrypto.so*
rm /tmp/v3.24.12.tar.gz

apt install -y libcurl4-openssl-dev
export PROMETHEUS_CPP_VERSION=1.3.0
cd /tmp
try_download "https://github.com/jupp0r/prometheus-cpp/releases/download/v${PROMETHEUS_CPP_VERSION}/prometheus-cpp-with-submodules.tar.gz" "prometheus-cpp.tar.gz"
tar -xzvf prometheus-cpp.tar.gz
cd "/tmp/prometheus-cpp-with-submodules"
mkdir build
cd build
cmake .. -DBUILD_SHARED_LIBS=ON -DENABLE_PULL=ON -DENABLE_COMPRESSION=OFF
make -j$(nproc)
make install
rm -rf "/tmp/prometheus-cpp-with-submodule" /tmp/prometheus-cpp.tar.gz

# Clean up to reduce image size
apt-get clean
rm -rf /var/lib/apt/lists/*
