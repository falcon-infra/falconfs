#!/usr/bin/env bash

set -euo pipefail

BUILD_TYPE="Debug"
BUILD_TEST=true
WITH_FUSE_OPT=false
WITH_ZK_INIT=false
WITH_RDMA=false
WITH_PROMETHEUS=false
CREATE_SOFT_LINK=true

FALCONFS_INSTALL_DIR="${FALCONFS_INSTALL_DIR:-/usr/local/falconfs}"
export FALCONFS_INSTALL_DIR=$FALCONFS_INSTALL_DIR
export PATH=$FALCONFS_INSTALL_DIR/bin:$FALCONFS_INSTALL_DIR/python/bin:${PATH:-}
export LD_LIBRARY_PATH=$FALCONFS_INSTALL_DIR/lib64:$FALCONFS_INSTALL_DIR/lib:$FALCONFS_INSTALL_DIR/python/lib:${LD_LIBRARY_PATH:-}

# Default command is build
COMMAND=${1:-build}

# Get source directory
FALCONFS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
POSTGRES_SRC_DIR="$FALCONFS_DIR/third_party/postgres"
export CONFIG_FILE="$FALCONFS_DIR/config/config.json"

# Set build directory
BUILD_DIR="${BUILD_DIR:-$FALCONFS_DIR/build}"

# Set default install directory
PG_INSTALL_DIR="$FALCONFS_INSTALL_DIR/falcon_metadb"
FALCON_CLIENT_INSTALL_DIR="$FALCONFS_INSTALL_DIR/falcon_client"
PYTHON_SDK_INSTALL_DIR="$FALCONFS_INSTALL_DIR/falcon_python_interface"

gen_proto() {
    mkdir -p "$BUILD_DIR"
    echo "Generating Protobuf files..."
    protoc --cpp_out="$BUILD_DIR" \
        --proto_path="$FALCONFS_DIR/remote_connection_def/proto" \
        falcon_meta_rpc.proto brpc_io.proto
    echo "Protobuf files generated."
}

build_pg() {
    local BLD_OPT=${1:-"deploy"}
    [[ "$BUILD_TYPE" == "Debug" ]] && BLD_OPT="debug"
    local CONFIGURE_OPTS=(--without-icu)
    local PG_CFLAGS=""

    echo "Building PostgreSQL (mode: $BLD_OPT) ..."

    # 设置构建选项
    if [[ "$BLD_OPT" == "debug" ]]; then
        CONFIGURE_OPTS+=(--enable-debug)
        PG_CFLAGS="-ggdb -O0 -g3 -Wall -fno-omit-frame-pointer"
    fi

    # 进入源码目录
    cd "$POSTGRES_SRC_DIR" || exit 1

    # 清理旧配置
    if [[ -f "config.status" ]]; then
        make distclean || true
    fi

    # 生成配置并构建
    CFLAGS="$PG_CFLAGS" ./configure --prefix=${PG_INSTALL_DIR} "${CONFIGURE_OPTS[@]}" \
        --enable-rpath LDFLAGS="-Wl,-rpath,$FALCONFS_INSTALL_DIR/lib64:$FALCONFS_INSTALL_DIR/lib" &&
        make -j$(nproc) &&
        cd "$POSTGRES_SRC_DIR/contrib" && make -j
    echo "PostgreSQL build complete."
}

clean_pg() {
    echo "Cleaning PostgreSQL..."
    if [[ -d "$POSTGRES_SRC_DIR/contrib/falcon" ]]; then
        cd "$POSTGRES_SRC_DIR" &&
            [ -f "Makefile" ] && make clean || true
        rm -rf "$POSTGRES_SRC_DIR/contrib/falcon"
    fi
    echo "PostgreSQL clean complete."
}

# 构建 FalconFS
build_falconfs() {
    gen_proto

    PG_CFLAGS=""
    if [[ "$BUILD_TYPE" == "Debug" ]]; then
        CONFIGURE_OPTS+=(--enable-debug)
        PG_CFLAGS="-ggdb -O0 -g3 -Wall -fno-omit-frame-pointer"
    fi
    echo "Building FalconFS Meta (mode: $BUILD_TYPE)..."
    cd $FALCONFS_DIR/falcon
    make USE_PGXS=1 CFLAGS="-Wno-shadow $PG_CFLAGS" CXXFLAGS="-Wno-shadow $PG_CFLAGS"

    echo "Building FalconFS Client (mode: $BUILD_TYPE)..."
    cmake -B "$BUILD_DIR" -GNinja "$FALCONFS_DIR" \
        -DCMAKE_INSTALL_PREFIX=$FALCON_CLIENT_INSTALL_DIR \
        -DCMAKE_EXPORT_COMPILE_COMMANDS=1 \
        -DCMAKE_BUILD_TYPE=${BUILD_TYPE} \
        -DPOSTGRES_SRC_DIR="$POSTGRES_SRC_DIR" \
        -DWITH_FUSE_OPT="$WITH_FUSE_OPT" \
        -DWITH_ZK_INIT="$WITH_ZK_INIT" \
        -DWITH_RDMA="$WITH_RDMA" \
        -DWITH_PROMETHEUS="$WITH_PROMETHEUS" \
        -DBUILD_TEST=$BUILD_TEST &&
        cd "$BUILD_DIR" && ninja
    echo "FalconFS build complete."
}

# 清理 FalconFS
clean_falconfs() {
    echo "Cleaning FalconFS Meta"
    cd $FALCONFS_DIR/falcon
    make USE_PGXS=1 clean
    rm -rf $FALCONFS_DIR/falcon/connection_pool/fbs
    rm -rf $FALCONFS_DIR/falcon/connection_pool/proto

    echo "Cleaning FalconFS Client..."
    rm -rf "$BUILD_DIR"
    echo "FalconFS clean complete."
}

clean_tests() {
    echo "Cleaning FalconFS tests..."
    rm -rf "$BUILD_DIR/tests"
    echo "FalconFS tests clean complete."
}

install_pg() {
    echo "Installing PostgreSQL to $PG_INSTALL_DIR..."
    cd "$POSTGRES_SRC_DIR" &&
        make install
    cd "$POSTGRES_SRC_DIR/contrib" && make install
    if [[ "$CREATE_SOFT_LINK" == "true" ]]; then
        bash $FALCONFS_DIR/deploy/ansible/link_third_party_to.sh $PG_INSTALL_DIR $FALCONFS_INSTALL_DIR
    fi
    echo "PostgreSQL installed to $PG_INSTALL_DIR"
}

install_falcon_meta() {
    echo "Installing FalconFS meta ..."
    cd "$FALCONFS_DIR/falcon" && make USE_PGXS=1 install
    echo "FalconFS meta installed"
}

install_falcon_client() {
    echo "Installing FalconFS client to $FALCON_CLIENT_INSTALL_DIR..."
    cd "$BUILD_DIR" && ninja install
    if [[ "$CREATE_SOFT_LINK" == "true" ]]; then
        bash $FALCONFS_DIR/deploy/ansible/link_third_party_to.sh $FALCON_CLIENT_INSTALL_DIR $FALCONFS_INSTALL_DIR
    fi

    echo "FalconFS client installed to $FALCON_CLIENT_INSTALL_DIR"
}

install_falcon_python_sdk() {
    echo "Installing FalconFS python sdk to $PYTHON_SDK_INSTALL_DIR..."
    rm -rf "$PYTHON_SDK_INSTALL_DIR"
    cp -r "$FALCONFS_DIR/python_interface" "$PYTHON_SDK_INSTALL_DIR"
    echo "FalconFS python sdk installed to $PYTHON_SDK_INSTALL_DIR"
}

install_deploy_scripts() {
    echo "Installing deploy scripts to $FALCONFS_INSTALL_DIR..."
    rm -rf "$FALCONFS_INSTALL_DIR/deploy"
    rm -rf "$FALCONFS_INSTALL_DIR/config"
    rsync -av --exclude='tmp' "$FALCONFS_DIR/deploy" "$FALCONFS_INSTALL_DIR"
    rsync -av --exclude='tmp' "$FALCONFS_DIR/config" "$FALCONFS_INSTALL_DIR"
    echo "deploy scripts installed to $FALCONFS_INSTALL_DIR"
}

clean_dist() {
    echo "Removing installed PostgreSQL from $PG_INSTALL_DIR..."
    rm -rf "$PG_INSTALL_DIR"
    echo "PostgreSQL installation removed from $PG_INSTALL_DIR"
}

print_help() {
    case "$1" in
    build)
        echo "Usage: $0 build [subcommand] [options]"
        echo ""
        echo "Build All Components of FalconFS"
        echo ""
        echo "Subcommands:"
        echo "  pg               Build only PostgreSQL"
        echo "  falcon           Build only FalconFS"
        echo ""
        echo "Options:"
        echo "  --debug          Build debug versions"
        echo "  --release        Build release versions (default)"
        echo "  -h, --help       Show this help message"
        echo ""
        echo "Examples:"
        echo "  $0 build --debug     # Build everything in debug mode"
        ;;
    clean)
        echo "Usage: $0 clean [target] [options]"
        echo ""
        echo "Clean build artifacts and installations"
        echo ""
        echo "Targets:"
        echo "  pg       Clean PostgreSQL build artifacts"
        echo "  falcon   Clean FalconFS build artifacts"
        echo "  test     Clean test binaries"
        echo "  dist     Remove installed PostgreSQL from $PG_INSTALL_DIR"
        echo ""
        echo "Options:"
        echo "  -h, --help  Show this help message"
        echo ""
        echo "Examples:"
        echo "  $0 clean           # Clean everything except installed PostgreSQL"
        echo "  $0 clean pg       # Clean only PostgreSQL"
        echo "  $0 clean dist     # Remove installed PostgreSQL"
        ;;
    *)
        # 主帮助信息
        echo "Usage: $0 <command> [subcommand] [options]"
        echo ""
        echo "Commands:"
        echo "  build     Build components"
        echo "  clean     Clean artifacts"
        echo "  test      Run tests"
        echo "  install   Install PostgreSQL"
        echo ""
        echo "Run '$0 <command> --help' for more information on a specific command"
        ;;
    esac
}

# 命令分发
case "$COMMAND" in
build)
    # Process shared build options (only debug/deploy allowed for combined build)
    while [[ $# -ge 2 ]]; do
        case "$2" in
        --debug)
            BUILD_TYPE="Debug"
            shift
            ;;
        --deploy | --release)
            BUILD_TYPE="Release"
            shift
            ;;
        --help | -h)
            print_help "build"
            exit 0
            ;;
        *)
            # Only break if this isn't the combined build case
            [[ -z "${2:-}" || "$2" == "pg" || "$2" == "falcon" ]] && break
            echo "Error: Combined build only supports --debug or --deploy" >&2
            exit 1
            ;;
        esac
    done

    case "${2:-}" in
    pg)
        for arg in "${@:3}"; do
            if [[ "$arg" == "--help" || "$arg" == "-h" ]]; then
                echo "Usage: $0 build pg [options]"
                echo ""
                echo "Build PostgreSQL Components"
                echo ""
                echo "Options:"
                echo "  --debug    Build in debug mode"
                echo "  --deploy   Build in deploy mode"
                exit 0
            fi
        done
        build_pg "${@:3}"
        ;;
    falcon)
        shift 2
        while [[ $# -gt 0 ]]; do
            case "$1" in
            --debug)
                BUILD_TYPE="Debug"
                ;;
            --release | --deploy)
                BUILD_TYPE="Release"
                ;;
            --relwithdebinfo)
                BUILD_TYPE="RelWithDebInfo"
                ;;
            --with-fuse-opt)
                WITH_FUSE_OPT=true
                ;;
            --with-zk-init)
                WITH_ZK_INIT=true
                ;;
            --with-rdma)
                WITH_RDMA=true
                ;;
            --with-prometheus)
                WITH_PROMETHEUS=true
                ;;
            --help | -h)
                echo "Usage: $0 build falcon [options]"
                echo ""
                echo "Build FalconFS Components"
                echo ""
                echo "Options:"
                echo "  --debug         Build in debug mode"
                echo "  --release       Build in release mode"
                echo "  --relwithdebinfo Build with debug symbols"
                echo "  --with-fuse-opt Enable FUSE optimizations"
                echo "  --with-zk-init Enable Zookeeper initialization for containerized deployment"
                echo "  --with-rdma     Enable RDMA support"
                echo "  --with-prometheus Enable Prometheus metrics"
                exit 0
                ;;
            *)
                echo "Unknown option: $1"
                exit 1
                ;;
            esac
            shift
        done
        build_falconfs
        ;;
    *)
        build_pg "${@:2}" && build_falconfs
        ;;
    esac
    ;;
clean)
    case "${2:-}" in
    pg)
        # Check for --help in clean pg
        for arg in "${@:3}"; do
            if [[ "$arg" == "--help" || "$arg" == "-h" ]]; then
                echo "Usage: $0 clean pg"
                echo "Clean PostgreSQL build artifacts"
                exit 0
            fi
        done
        clean_pg
        ;;
    falcon)
        # Check for --help in clean falcon
        for arg in "${@:3}"; do
            if [[ "$arg" == "--help" || "$arg" == "-h" ]]; then
                echo "Usage: $0 clean falcon"
                echo "Clean FalconFS build artifacts"
                exit 0
            fi
        done
        clean_falconfs
        ;;
    test)
        # Check for --help in clean test
        for arg in "${@:3}"; do
            if [[ "$arg" == "--help" || "$arg" == "-h" ]]; then
                echo "Usage: $0 clean test"
                echo "Clean test binaries"
                exit 0
            fi
        done
        clean_tests
        ;;
    dist)
        # Check for --help in clean dist
        for arg in "${@:3}"; do
            if [[ "$arg" == "--help" || "$arg" == "-h" ]]; then
                echo "Usage: $0 clean dist"
                echo "Remove installed PostgreSQL from $PG_INSTALL_DIR"
                exit 0
            fi
        done
        clean_dist
        ;;
    *)
        # Main clean command options
        while true; do
            case "${2:-}" in
            --help | -h)
                print_help "clean"
                exit 0
                ;;
            *) break ;;
            esac
        done
        clean_pg && clean_falconfs
        ;;
    esac
    ;;
test)
    TARGET_DIR="$FALCONFS_DIR/build/tests/falcon_store/"
    # Find executable files directly in the test directory (not in subdirectories)
    # Exclude .cmake files and anything in CMakeFiles/
    find "$TARGET_DIR" -maxdepth 1 -type f -executable -not -name "*.cmake" -not -path "*/CMakeFiles/*" | while read -r executable_file; do
        echo "Executing: $executable_file"
        "$executable_file"
        echo "---------------------------------------------------------------------------------------"
    done
    echo "All unit tests passed."
    ;;
install)
    case "${2:-}" in
    pg)
        install_pg
        ;;
    falcon)
        install_falcon_meta
        install_falcon_client
        install_falcon_python_sdk
        install_deploy_scripts
        ;;
    *)
        install_pg
        install_falcon_meta
        install_falcon_client
        install_falcon_python_sdk
        install_deploy_scripts
    esac
    ;;
*)
    print_help "build"
    exit 1
    ;;
esac
