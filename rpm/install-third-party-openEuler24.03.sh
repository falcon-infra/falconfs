#!/usr/bin/env bash

set -euo pipefail

PG_VERSION="${PG_VERSION:-17.7}"
BRPC_VERSION="${BRPC_VERSION:-1.14.1}"
PROMETHEUS_VERSION="${PROMETHEUS_VERSION:-1.3.0}"
ZK_VERSION="${ZK_VERSION:-3.9.1}"

PG_PREFIX="${PG_PREFIX:-/usr/local/pgsql}"
INSTALL_PREFIX="${INSTALL_PREFIX:-/usr/local}"
SOURCE_DIR="${SOURCE_DIR:-/tmp/falconfs-third-party-src}"

if [[ "${EUID}" -eq 0 ]]; then
    SUDO=""
else
    if ! command -v sudo >/dev/null 2>&1; then
        echo "Error: sudo is required when running as a non-root user." >&2
        exit 1
    fi
    SUDO="sudo"
fi

if command -v dnf >/dev/null 2>&1; then
    PKG_MANAGER="dnf"
elif command -v yum >/dev/null 2>&1; then
    PKG_MANAGER="yum"
else
    echo "Error: dnf or yum is required on openEuler." >&2
    exit 1
fi

retry_download() {
    local url="$1"
    local output="$2"

    for attempt in 1 2 3; do
        if wget --no-check-certificate --timeout=120 --tries=1 -O "$output" "$url"; then
            return 0
        fi
        echo "Download failed (attempt ${attempt}/3): ${url}" >&2
        sleep $((attempt * 2))
    done

    echo "Error: failed to download ${url}" >&2
    return 1
}

install_system_dependencies() {
    ${SUDO} "$PKG_MANAGER" clean all || true
    ${SUDO} "$PKG_MANAGER" makecache
    ${SUDO} "$PKG_MANAGER" groupinstall -y "Development Tools" || true
    ${SUDO} "$PKG_MANAGER" reinstall -y glibc-common || true
    ${SUDO} "$PKG_MANAGER" install -y \
        bash \
        sudo \
        git \
        findutils \
        shadow \
        util-linux \
        glibc-langpack-en \
        glibc-all-langpacks \
        gcc \
        gcc-c++ \
        make \
        cmake \
        ninja-build \
        autoconf \
        automake \
        libtool \
        bison \
        flex \
        readline-devel \
        openssl-devel \
        gflags-devel \
        glog-devel \
        leveldb-devel \
        snappy-devel \
        fmt-devel \
        gperftools-devel \
        libunwind-devel \
        rdma-core-devel \
        fuse-devel \
        libcurl-devel \
        jansson-devel \
        libffi-devel \
        libzstd-devel \
        xz-devel \
        expat-devel \
        libxml2-devel \
        systemd-devel \
        protobuf-devel \
        protobuf-compiler \
        flatbuffers-devel \
        flatbuffers-compiler \
        jsoncpp-devel \
        thrift-devel \
        cppunit-devel \
        gtest-devel \
        gmock-devel \
        python3 \
        python3-devel \
        python3-requests \
        python3-psycopg2 \
        python3-kazoo \
        wget \
        tar \
        rsync \
        libstdc++-static \
        zstd-devel \
        perl \
        java-11-openjdk-devel \
        maven \
        hostname \
        jq
}

install_postgresql() {
    if [[ -x "${PG_PREFIX}/bin/pg_config" ]] && "${PG_PREFIX}/bin/pg_config" --version | grep -q "${PG_VERSION}"; then
        echo "PostgreSQL ${PG_VERSION} already installed at ${PG_PREFIX}."
        return 0
    fi

    local archive="${SOURCE_DIR}/postgresql-${PG_VERSION}.tar.gz"
    local src_dir="${SOURCE_DIR}/postgresql-${PG_VERSION}"
    local url="https://ftp.postgresql.org/pub/source/v${PG_VERSION}/postgresql-${PG_VERSION}.tar.gz"

    rm -rf "$src_dir"
    retry_download "$url" "$archive"
    tar -xzf "$archive" -C "$SOURCE_DIR"

    pushd "$src_dir" >/dev/null
    ./configure --prefix="$PG_PREFIX" --without-icu --enable-debug
    make -j"$(nproc)"
    ${SUDO} make install
    popd >/dev/null
}

install_brpc() {
    if ldconfig -p 2>/dev/null | grep -q 'libbrpc\.so'; then
        echo "brpc already installed."
        return 0
    fi

    local archive="${SOURCE_DIR}/brpc-${BRPC_VERSION}.tar.gz"
    local src_dir="${SOURCE_DIR}/brpc-${BRPC_VERSION}"
    local url="https://github.com/apache/brpc/archive/refs/tags/${BRPC_VERSION}.tar.gz"

    rm -rf "$src_dir"
    retry_download "$url" "$archive"
    tar -xzf "$archive" -C "$SOURCE_DIR"

    cmake -S "$src_dir" -B "$src_dir/build" -GNinja \
        -DWITH_GLOG=ON \
        -DWITH_RDMA=ON \
        -DCMAKE_INSTALL_PREFIX="$INSTALL_PREFIX" \
        -DCMAKE_PREFIX_PATH="$INSTALL_PREFIX" \
        -DCMAKE_CXX_FLAGS="-Wno-error -Wno-deprecated-declarations" \
        -DCMAKE_BUILD_TYPE=Release
    ninja -C "$src_dir/build"
    ${SUDO} ninja -C "$src_dir/build" install
}

install_prometheus_cpp() {
    if ldconfig -p 2>/dev/null | grep -q 'libprometheus-cpp-core\.so'; then
        echo "prometheus-cpp already installed."
        return 0
    fi

    local archive="${SOURCE_DIR}/prometheus-cpp-with-submodules-${PROMETHEUS_VERSION}.tar.gz"
    local src_dir="${SOURCE_DIR}/prometheus-cpp-with-submodules"
    local url="https://github.com/jupp0r/prometheus-cpp/releases/download/v${PROMETHEUS_VERSION}/prometheus-cpp-with-submodules.tar.gz"

    rm -rf "$src_dir"
    retry_download "$url" "$archive"
    tar -xzf "$archive" -C "$SOURCE_DIR"

    cmake -S "$src_dir" -B "$src_dir/build" \
        -DBUILD_SHARED_LIBS=ON \
        -DENABLE_PULL=ON \
        -DENABLE_COMPRESSION=OFF \
        -DCMAKE_INSTALL_PREFIX="$INSTALL_PREFIX" \
        -DCMAKE_BUILD_TYPE=Release
    make -C "$src_dir/build" -j"$(nproc)"
    ${SUDO} make -C "$src_dir/build" install
}

install_zookeeper_c_client() {
    if ldconfig -p 2>/dev/null | grep -q 'libzookeeper_mt\.so'; then
        echo "ZooKeeper C client already installed."
        return 0
    fi

    local archive="${SOURCE_DIR}/apache-zookeeper-${ZK_VERSION}.tar.gz"
    local src_dir="${SOURCE_DIR}/apache-zookeeper-${ZK_VERSION}"
    local url="https://archive.apache.org/dist/zookeeper/zookeeper-${ZK_VERSION}/apache-zookeeper-${ZK_VERSION}.tar.gz"

    rm -rf "$src_dir"
    retry_download "$url" "$archive"
    tar -xzf "$archive" -C "$SOURCE_DIR"

    pushd "$src_dir" >/dev/null
    mvn compile -DskipTests -pl zookeeper-jute -T 1C
    cd zookeeper-client/zookeeper-client-c
    autoreconf -if
    ./configure --prefix="$INSTALL_PREFIX"
    make -j"$(nproc)"
    ${SUDO} make install
    popd >/dev/null
}

configure_environment() {
    ${SUDO} tee /etc/ld.so.conf.d/falconfs-third-party.conf >/dev/null <<EOF
${INSTALL_PREFIX}/lib
${INSTALL_PREFIX}/lib64
${PG_PREFIX}/lib
EOF
    ${SUDO} ldconfig

    ${SUDO} ln -sf "${PG_PREFIX}/bin/pg_config" /usr/local/bin/pg_config

    ${SUDO} tee /etc/profile.d/falconfs-third-party.sh >/dev/null <<EOF
export PATH=${PG_PREFIX}/bin:${INSTALL_PREFIX}/bin:\$PATH
export LD_LIBRARY_PATH=${PG_PREFIX}/lib:${INSTALL_PREFIX}/lib:${INSTALL_PREFIX}/lib64:\${LD_LIBRARY_PATH:-}
EOF
}

verify_installation() {
    export PATH="${PG_PREFIX}/bin:${INSTALL_PREFIX}/bin:${PATH}"
    export LD_LIBRARY_PATH="${PG_PREFIX}/lib:${INSTALL_PREFIX}/lib:${INSTALL_PREFIX}/lib64:${LD_LIBRARY_PATH:-}"

    if ! command -v pg_config >/dev/null 2>&1; then
        echo "Error: pg_config not found" >&2
        exit 1
    fi

    pg_version="$(pg_config --version)"
    if [[ "$pg_version" != *"$PG_VERSION"* ]]; then
        echo "Error: unexpected PostgreSQL version: $pg_version (expected $PG_VERSION)" >&2
        exit 1
    fi

    if ! ldconfig -p | grep -q 'libbrpc\.so'; then
        echo "Error: libbrpc not found in ldconfig cache" >&2
        exit 1
    fi

    if ! ldconfig -p | grep -q 'libprometheus-cpp-core\.so'; then
        echo "Error: libprometheus-cpp-core not found in ldconfig cache" >&2
        exit 1
    fi

    if ! ldconfig -p | grep -q 'libzookeeper_mt\.so'; then
        echo "Error: libzookeeper_mt not found in ldconfig cache" >&2
        exit 1
    fi

    echo "Verified PostgreSQL: $pg_version"
    echo "Verified brpc, prometheus-cpp, and ZooKeeper C client libraries."
}

main() {
    mkdir -p "$SOURCE_DIR"
    install_system_dependencies
    install_postgresql
    install_brpc
    install_prometheus_cpp
    install_zookeeper_c_client
    configure_environment
    verify_installation

    echo "Third-party dependencies installed for openEuler 24.03."
    echo "OBS SDK is not installed; install it separately only when building with --with-obs-storage."
}

main "$@"
