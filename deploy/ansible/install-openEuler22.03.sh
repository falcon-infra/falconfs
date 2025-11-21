#!/bin/bash
set -euo pipefail

PREFIX=${1:-/usr/local/falconfs}
SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]:-${(%):-%N}}")")" && pwd)"

INSTALL_GCC=true
INSTALL_CMAKE=true
INSTALL_ABSL=true
INSTALL_PROTOBUF=true
INSTALL_FLATBUFFER=true
INSTALL_BRPC=true
INSTALL_ZOOKEEPER=true
INSTALL_OBS=true
INSTALL_JSONCPP=true
INSTALL_GTEST=true
INSTALL_PYTHON=true

while [[ $# -gt 0 ]]; do
    case $1 in
        --no-gcc) INSTALL_GCC=false;;
        --no-cmake) INSTALL_CMAKE=false;;
        --no-absl) INSTALL_ABSL=false;;
        --no-protobuf) INSTALL_PROTOBUF=false;;
        --no-flatbuffer) INSTALL_FLATBUFFER=false;;
        --no-brpc) INSTALL_BRPC=false;;
        --no-zookeeper) INSTALL_ZOOKEEPER=false;;
        --no-obs) INSTALL_OBS=false;;
        --no-jsoncpp) INSTALL_JSONCPP=false;;
        --no-gtest) INSTALL_GTEST=false;;
        --no-python) INSTALL_PYTHON=false;;
        *) ;;  # ignore unknown flags
    esac
    shift
done

echo "Using prefix: $PREFIX"
echo "Script directory: $SCRIPT_DIR"

export PATH="$PREFIX/bin":${PATH:-}
export LD_LIBRARY_PATH="$PREFIX/lib64:$PREFIX/lib":${LD_LIBRARY_PATH:-}
export CMAKE_PREFIX_PATH="$PREFIX":${CMAKE_PREFIX_PATH:-}

#echo "$PREFIX/lib64" | tee /etc/ld.so.conf.d/00-falconfs.conf
#echo "$PREFIX/lib" | tee -a /etc/ld.so.conf.d/00-falconfs.conf

install_gcc() {
    pushd .

    GCC_VERSION=14.2.0
    yum install -y gcc gcc-c++ glibc-static glibc-devel libmpc-devel tmux flex bison 
    wget https://github.com/gcc-mirror/gcc/archive/refs/tags/releases/gcc-$GCC_VERSION.tar.gz
    tar -zxvf gcc-$GCC_VERSION.tar.gz
    mv gcc-releases-gcc-$GCC_VERSION gcc-$GCC_VERSION
    cd ./gcc-$GCC_VERSION/ && ./contrib/download_prerequisites
    mkdir build && cd build
    ../configure --prefix=$PREFIX/third_party/gcc-$GCC_VERSION --enable-bootstrap --enable-threads=posix --enable--long-long --enable-checking=release --enable-language=c,c++ --disable-multilib
    make -j$(nproc)
    make install
    ln -s gcc $PREFIX/third_party/gcc-$GCC_VERSION/bin/cc

    bash "$SCRIPT_DIR/link_third_party_to.sh" $PREFIX/third_party/gcc-$GCC_VERSION $PREFIX

    ldconfig

    popd
}

install_cmake() {
    pushd .

    CMAKE_VERSION=3.28.6
    yum install -y openssl-devel
    wget https://cmake.org/files/v3.28/cmake-$CMAKE_VERSION.tar.gz
    tar -xvf cmake-$CMAKE_VERSION.tar.gz
    cd cmake-$CMAKE_VERSION
    ./bootstrap --prefix=$PREFIX/third_party/cmake-$CMAKE_VERSION \
        -- \
        -DCMAKE_INSTALL_RPATH=$PREFIX/lib64:$PREFIX/lib \
        -DCMAKE_BUILD_WITH_INSTALL_RPATH=ON
    make -j$(nproc)
    make install

    bash "$SCRIPT_DIR/link_third_party_to.sh" $PREFIX/third_party/cmake-$CMAKE_VERSION $PREFIX

    popd 
}

install_absl() {
    pushd .

    ABSL_VERSION=20250512.1
    wget https://github.com/abseil/abseil-cpp/releases/download/$ABSL_VERSION/abseil-cpp-$ABSL_VERSION.tar.gz
    tar -zxvf abseil-cpp-$ABSL_VERSION.tar.gz
    cd abseil-cpp-$ABSL_VERSION
    cmake -B build -DCMAKE_INSTALL_PREFIX=$PREFIX/third_party/absl-$ABSL_VERSION -DBUILD_TESTING=ON
    cmake --build build -j $(nproc)
    cmake --install build

    bash "$SCRIPT_DIR/link_third_party_to.sh" $PREFIX/third_party/absl-$ABSL_VERSION $PREFIX

    popd
}

install_protobuf() {
    pushd .

    PROTOBUF_VERSION=3.17.3
    wget https://github.com/protocolbuffers/protobuf/releases/download/v$PROTOBUF_VERSION/protobuf-cpp-$PROTOBUF_VERSION.tar.gz
    tar -zxvf protobuf-cpp-$PROTOBUF_VERSION.tar.gz
    cd protobuf-$PROTOBUF_VERSION
    ./configure --prefix=$PREFIX/third_party/protobuf-$PROTOBUF_VERSION
    make -j$(nproc)
    make install

    bash "$SCRIPT_DIR/link_third_party_to.sh" $PREFIX/third_party/protobuf-$PROTOBUF_VERSION $PREFIX

    popd
}

install_flatbuffer() {
    pushd .

    FLATBUFFER_VERSION=25.2.10
    wget https://github.com/google/flatbuffers/archive/refs/tags/v$FLATBUFFER_VERSION.tar.gz
    tar -xvf v$FLATBUFFER_VERSION.tar.gz
    cd flatbuffers-$FLATBUFFER_VERSION
    cmake -B build -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=$PREFIX/third_party/flatbuffers-$FLATBUFFER_VERSION
    cmake --build build -j $(nproc)
    cmake --install build

    bash "$SCRIPT_DIR/link_third_party_to.sh" $PREFIX/third_party/flatbuffers-$FLATBUFFER_VERSION $PREFIX

    popd
}

install_brpc() {
    pushd .

    BRPC_VERSION=1.12.1
    yum install -y gflags-devel leveldb leveldb-devel glog glog-devel libibverbs libibverbs-utils libibverbs-devel
    wget https://github.com/apache/brpc/archive/refs/tags/$BRPC_VERSION.tar.gz
    tar -zxvf $BRPC_VERSION.tar.gz
    cd brpc-$BRPC_VERSION && mkdir build && cd build
    cmake .. \
        -DWITH_GLOG=ON -DWITH_RDMA=ON \
        -DCMAKE_INSTALL_PREFIX=$PREFIX/third_party/brpc-$BRPC_VERSION \
        -DCMAKE_PREFIX_PATH=$PREFIX
    make -j$(nproc)
    make install

    bash "$SCRIPT_DIR/link_third_party_to.sh" $PREFIX/third_party/brpc-$BRPC_VERSION $PREFIX

    popd
}

install_zookeeper() {
    pushd .

    ZOOKEEPER_VERSION=3.9.3
    yum install -y autoconf automake libtool libtool-ltdl-devel cppunit-devel maven java-1.8.0-openjdk-devel
    wget https://github.com/apache/zookeeper/archive/refs/tags/release-$ZOOKEEPER_VERSION.tar.gz
    tar -xvf release-$ZOOKEEPER_VERSION.tar.gz
    cd zookeeper-release-$ZOOKEEPER_VERSION/zookeeper-jute
    mvn clean install -DskipTests
    cd ../zookeeper-client/zookeeper-client-c
    mvn clean install -DskipTests
    mkdir -p $PREFIX/third_party/zookeeper-$ZOOKEEPER_VERSION/include/zookeeper $PREFIX/third_party/zookeeper-$ZOOKEEPER_VERSION/lib64
    cp -r generated/*.h $PREFIX/third_party/zookeeper-$ZOOKEEPER_VERSION/include/zookeeper
    cp -r include/*.h $PREFIX/third_party/zookeeper-$ZOOKEEPER_VERSION/include/zookeeper
    cp -d target/c/lib/libzookeeper* $PREFIX/third_party/zookeeper-$ZOOKEEPER_VERSION/lib64

    bash "$SCRIPT_DIR/link_third_party_to.sh" $PREFIX/third_party/zookeeper-$ZOOKEEPER_VERSION $PREFIX

    popd
}

install_obs() {
    pushd .

    OBS_VERSION=3.24.12
    wget https://github.com/huaweicloud/huaweicloud-sdk-c-obs/archive/refs/tags/v$OBS_VERSION.tar.gz
    tar -zxvf v$OBS_VERSION.tar.gz
    cd huaweicloud-sdk-c-obs-$OBS_VERSION
    sed -i '/if(NOT DEFINED OPENSSL_INC_DIR)/,+5d' CMakeLists.txt
    sed -i '/OPENSSL/d' CMakeLists.txt
    sed -i "s|SET(CMAKE_SKIP_RPATH TRUE)|SET(CMAKE_BUILD_RPATH $PREFIX/lib64:$PREFIX/lib)|g" CMakeLists.txt
    if [ "$(uname -m)" = "aarch64" ]; then
        OSB_BUILD_SCRIPT="build_aarch.sh"
    else
        OSB_BUILD_SCRIPT="build.sh"
    fi
    cd source/eSDK_OBS_API/eSDK_OBS_API_C++ &&
        export SPDLOG_VERSION=spdlog-1.12.0 && \
        bash $OSB_BUILD_SCRIPT sdk &&
        mkdir -p $PREFIX/third_party/obs-$OBS_VERSION &&
        tar zxvf sdk.tgz -C $PREFIX/third_party/obs-$OBS_VERSION &&
        rm -f $PREFIX/third_party/obs-$OBS_VERSION/lib/libcurl.so* &&
        rm -f $PREFIX/third_party/obs-$OBS_VERSION/lib/libssl.so* &&
        rm -f $PREFIX/third_party/obs-$OBS_VERSION/lib/libcrypto.so* &&
        rm -rf $PREFIX/third_party/obs-$OBS_VERSION/demo &&
        rm -f $PREFIX/third_party/obs-$OBS_VERSION/readme.txt
    ln -s libiconv.so $PREFIX/third_party/obs-$OBS_VERSION/lib/libiconv.so.0

    bash "$SCRIPT_DIR/link_third_party_to.sh" $PREFIX/third_party/obs-$OBS_VERSION $PREFIX
    # compatible with legacy code
    bash "$SCRIPT_DIR/link_third_party_to.sh" $PREFIX/third_party/obs-$OBS_VERSION /usr/local/obs

    popd
}

install_jsoncpp() {
    pushd .

    JSONCPP_VERSION=1.9.6
    wget https://github.com/open-source-parsers/jsoncpp/archive/refs/tags/$JSONCPP_VERSION.tar.gz
    tar -xzvf $JSONCPP_VERSION.tar.gz
    cd jsoncpp-$JSONCPP_VERSION
    sed -i 's/set(CMAKE_CXX_STANDARD 11)/set(CMAKE_CXX_STANDARD 17)/' CMakeLists.txt
    mkdir build && cd build
    cmake .. -DCMAKE_INSTALL_PREFIX=$PREFIX/third_party/jsoncpp-$JSONCPP_VERSION \
        -DCMAKE_INSTALL_RPATH=$PREFIX/lib64:$PREFIX/lib
    make -j$(nproc)
    make install

    bash "$SCRIPT_DIR/link_third_party_to.sh" $PREFIX/third_party/jsoncpp-$JSONCPP_VERSION $PREFIX

    popd
}

install_gtest() {
    pushd .

    GTEST_VERSION=1.17.0
    wget https://github.com/google/googletest/releases/download/v$GTEST_VERSION/googletest-$GTEST_VERSION.tar.gz
    tar -xvf googletest-$GTEST_VERSION.tar.gz
    cd googletest-$GTEST_VERSION
    mkdir build && cd build
    cmake .. -DCMAKE_INSTALL_PREFIX=$PREFIX/third_party/googletest-$GTEST_VERSION
    make -j$(nproc)
    make install

    bash "$SCRIPT_DIR/link_third_party_to.sh" $PREFIX/third_party/googletest-$GTEST_VERSION $PREFIX

    popd
}

install_python() {
    pushd .

    PYTHON_VERSION=3.11.13
    yum install -y libffi-devel
    wget https://www.python.org/ftp/python/$PYTHON_VERSION/Python-$PYTHON_VERSION.tar.xz
    tar -xvf Python-$PYTHON_VERSION.tar.xz
    cd Python-$PYTHON_VERSION
    ./configure --prefix=$PREFIX/third_party/python-$PYTHON_VERSION --enable-shared --with-pydebug --enable-optimizations
    make -j$(nproc)
    make install

    # We avoid linking Python to $PREFIX/lib to prevent rpath from forcing the use of an
    # unintended libpython.so, which may conflict with the user's Python environment. At build
    # time, this Python is used, but it is not required at runtime. The user should ensure that 
    # its version matches the one used at runtime.
    bash "$SCRIPT_DIR/link_third_party_to.sh" $PREFIX/third_party/python-$PYTHON_VERSION $PREFIX/python

    popd
}

mkdir -p tmp
cd tmp
if [ "$INSTALL_GCC" = true ]; then
    install_gcc
fi
if [ "$INSTALL_CMAKE" = true ]; then
    install_cmake
fi
if [ "$INSTALL_ABSL" = true ]; then
    install_absl
fi
if [ "$INSTALL_PROTOBUF" = true ]; then
    install_protobuf
fi
if [ "$INSTALL_FLATBUFFER" = true ]; then
    install_flatbuffer
fi
if [ "$INSTALL_BRPC" = true ]; then
    install_brpc
fi
if [ "$INSTALL_ZOOKEEPER" = true ]; then
    install_zookeeper
fi
if [ "$INSTALL_OBS" = true ]; then
    install_obs
fi
if [ "$INSTALL_JSONCPP" = true ]; then
    install_jsoncpp
fi
if [ "$INSTALL_GTEST" = true ]; then
    install_gtest
fi
if [ "$INSTALL_PYTHON" = true ]; then
    install_python
fi

# other packages for falconfs
yum install -y ninja-build readline-devel fuse fuse-devel fmt-devel rsync ansible
ldconfig

cat <<EOF
-------------------------------------------------------------
Please export environment variables when building:

export FALCONFS_INSTALL_DIR=$PREFIX

If you plan to copy the compiled binaries to another machine, please make sure to install the following system dependencies on the target machine:

yum install -y gcc gcc-c++ glibc-static glibc-devel libmpc-devel tmux flex bison openssl-devel gflags-devel leveldb leveldb-devel glog glog-devel libibverbs libibverbs-utils libibverbs-devel autoconf automake libtool libtool-ltdl-devel cppunit-devel maven java-1.8.0-openjdk-devel ninja-build readline-devel fuse fuse-devel fmt-devel ansible libffi-devel

-------------------------------------------------------------
EOF
