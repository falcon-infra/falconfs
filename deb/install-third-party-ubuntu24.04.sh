#!/usr/bin/env bash

set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

sudo apt-get update
sudo apt-get install -y \
    ca-certificates \
    tzdata \
    locales \
    python3 \
    python3-pip \
    python3-requests \
    python3-psycopg2 \
    python3-kazoo \
    gcc-14 \
    g++-14 \
    tar \
    wget \
    make \
    cmake \
    ninja-build \
    libreadline-dev \
    liblz4-dev \
    libzstd-dev \
    libpython3-dev \
    bison \
    flex \
    m4 \
    autoconf \
    automake \
    pkg-config \
    libssl-dev \
    fuse \
    libfuse-dev \
    libtool \
    libflatbuffers-dev \
    flatbuffers-compiler \
    libprotoc-dev \
    libprotobuf-dev \
    protobuf-compiler \
    libgflags-dev \
    libjsoncpp-dev \
    libleveldb-dev \
    libfmt-dev \
    libboost-thread-dev \
    libboost-system-dev \
    libgtest-dev \
    libgmock-dev \
    libgoogle-glog-dev \
    libzookeeper-mt-dev \
    libibverbs-dev \
    libcurl4-openssl-dev \
    jq \
    moreutils \
    git \
    rsync \
    default-jdk \
    maven

sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-14 60
sudo update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-14 60
sudo update-alternatives --set gcc /usr/bin/gcc-14
sudo update-alternatives --set g++ /usr/bin/g++-14

sudo locale-gen en_US.UTF-8

# PostgreSQL 17 to /usr/local/pgsql
PG_VERSION="17.7"
PG_DOWNLOAD_URL="https://ftp.postgresql.org/pub/source/v${PG_VERSION}/postgresql-${PG_VERSION}.tar.gz"
wget -O- "${PG_DOWNLOAD_URL}" | tar -xz -C /tmp
pushd "/tmp/postgresql-${PG_VERSION}" >/dev/null
./configure --prefix=/usr/local/pgsql --without-icu --enable-debug
make -j"$(nproc)"
sudo make install
popd >/dev/null
sudo rm -rf "/tmp/postgresql-${PG_VERSION}"

# brpc
BRPC_VERSION="1.12.1"
wget -O- "https://github.com/apache/brpc/archive/refs/tags/${BRPC_VERSION}.tar.gz" | tar -xz -C /tmp
pushd "/tmp/brpc-${BRPC_VERSION}" >/dev/null
mkdir -p build
pushd build >/dev/null
cmake -GNinja -DWITH_GLOG=ON -DWITH_RDMA=ON ..
ninja
sudo ninja install
popd >/dev/null
popd >/dev/null
sudo rm -rf "/tmp/brpc-${BRPC_VERSION}"

# prometheus-cpp
PROMETHEUS_CPP_VERSION="1.3.0"
PROMETHEUS_DOWNLOAD_URL="https://github.com/jupp0r/prometheus-cpp/releases/download/v${PROMETHEUS_CPP_VERSION}/prometheus-cpp-with-submodules.tar.gz"
wget -O- "${PROMETHEUS_DOWNLOAD_URL}" | tar -xz -C /tmp
pushd /tmp/prometheus-cpp-with-submodules >/dev/null
mkdir -p build
pushd build >/dev/null
cmake .. -DBUILD_SHARED_LIBS=ON -DENABLE_PULL=ON -DENABLE_COMPRESSION=OFF
make -j"$(nproc)"
sudo make install
popd >/dev/null
popd >/dev/null
sudo rm -rf /tmp/prometheus-cpp-with-submodules

echo "Third-party dependencies installed."
echo "Verify pg_config: /usr/local/pgsql/bin/pg_config --version"
echo "OBS SDK is optional now; install it only when building with --with-obs-storage."
