#!/usr/bin/env bash

set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

PG_VERSION="${PG_VERSION:-17.7}"
BRPC_VERSION="${BRPC_VERSION:-1.14.1}"
PROMETHEUS_CPP_VERSION="${PROMETHEUS_CPP_VERSION:-1.3.0}"

PG_PREFIX="${PG_PREFIX:-/usr/local/pgsql}"
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

retry_download() {
    local url="$1"
    local output="$2"

    for attempt in 1 2 3; do
        if wget --timeout=120 --tries=1 -O "$output" "$url"; then
            return 0
        fi
        echo "Download failed (attempt ${attempt}/3): ${url}" >&2
        sleep $((attempt * 2))
    done

    echo "Error: failed to download ${url}" >&2
    return 1
}

install_apt_dependencies() {
    ${SUDO} apt-get update
    ${SUDO} apt-get install -y \
        ca-certificates \
        tzdata \
        locales \
        sudo \
        git \
        rsync \
        tar \
        wget \
        curl \
        make \
        cmake \
        ninja-build \
        build-essential \
        gcc-14 \
        g++-14 \
        bison \
        flex \
        m4 \
        autoconf \
        automake \
        pkg-config \
        libtool \
        libreadline-dev \
        liblz4-dev \
        libzstd-dev \
        zstd \
        libssl-dev \
        fuse \
        libfuse-dev \
        libflatbuffers-dev \
        flatbuffers-compiler \
        libprotoc-dev \
        libprotobuf-dev \
        protobuf-compiler \
        libgflags-dev \
        libjsoncpp-dev \
        libleveldb-dev \
        libsnappy-dev \
        libfmt-dev \
        libboost-thread-dev \
        libboost-system-dev \
        libboost-filesystem-dev \
        libboost-program-options-dev \
        libgtest-dev \
        libgmock-dev \
        libgoogle-glog-dev \
        libzookeeper-mt-dev \
        libibverbs-dev \
        rdma-core \
        libcurl4-openssl-dev \
        libunwind-dev \
        libjansson-dev \
        libffi-dev \
        libxml2-dev \
        libsystemd-dev \
        libthrift-dev \
        libcppunit-dev \
        python3 \
        python3-dev \
        python3-pip \
        python3-requests \
        python3-psycopg2 \
        python3-kazoo \
        jq \
        moreutils \
        iputils-ping \
        net-tools

    ${SUDO} update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-14 60
    ${SUDO} update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-14 60
    ${SUDO} update-alternatives --set gcc /usr/bin/gcc-14
    ${SUDO} update-alternatives --set g++ /usr/bin/g++-14

    if [[ ! -e /usr/include/json && -d /usr/include/jsoncpp/json ]]; then
        ${SUDO} ln -s /usr/include/jsoncpp/json /usr/include/json
    fi

    ${SUDO} locale-gen en_US.UTF-8
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
        -DCMAKE_BUILD_TYPE=Release
    ninja -C "$src_dir/build"
    ${SUDO} ninja -C "$src_dir/build" install
}

install_prometheus_cpp() {
    if ldconfig -p 2>/dev/null | grep -q 'libprometheus-cpp-core\.so'; then
        echo "prometheus-cpp already installed."
        return 0
    fi

    local archive="${SOURCE_DIR}/prometheus-cpp-with-submodules-${PROMETHEUS_CPP_VERSION}.tar.gz"
    local src_dir="${SOURCE_DIR}/prometheus-cpp-with-submodules"
    local url="https://github.com/jupp0r/prometheus-cpp/releases/download/v${PROMETHEUS_CPP_VERSION}/prometheus-cpp-with-submodules.tar.gz"

    rm -rf "$src_dir"
    retry_download "$url" "$archive"
    tar -xzf "$archive" -C "$SOURCE_DIR"

    cmake -S "$src_dir" -B "$src_dir/build" \
        -DBUILD_SHARED_LIBS=ON \
        -DENABLE_PULL=ON \
        -DENABLE_COMPRESSION=OFF \
        -DCMAKE_BUILD_TYPE=Release
    make -C "$src_dir/build" -j"$(nproc)"
    ${SUDO} make -C "$src_dir/build" install
}

configure_environment() {
    ${SUDO} tee /etc/ld.so.conf.d/falconfs-third-party.conf >/dev/null <<EOF
/usr/local/lib
/usr/local/lib64
${PG_PREFIX}/lib
EOF
    ${SUDO} ldconfig

    ${SUDO} ln -sf "${PG_PREFIX}/bin/pg_config" /usr/local/bin/pg_config

    ${SUDO} tee /etc/profile.d/falconfs-third-party.sh >/dev/null <<EOF
export PATH=${PG_PREFIX}/bin:/usr/local/bin:\$PATH
export LD_LIBRARY_PATH=${PG_PREFIX}/lib:/usr/local/lib:/usr/local/lib64:\${LD_LIBRARY_PATH:-}
EOF
}

verify_installation() {
    export PATH="${PG_PREFIX}/bin:/usr/local/bin:${PATH}"
    export LD_LIBRARY_PATH="${PG_PREFIX}/lib:/usr/local/lib:/usr/local/lib64:${LD_LIBRARY_PATH:-}"

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
    install_apt_dependencies
    install_postgresql
    install_brpc
    install_prometheus_cpp
    configure_environment
    verify_installation

    echo "Third-party dependencies installed."
    echo "pg_config is linked into /usr/local/bin for immediate build use."
    echo "OBS SDK is optional; install it only when building with --with-obs-storage."
}

main "$@"
