#!/usr/bin/env bash

set -euo pipefail

BUILD_TYPE="Release"
BUILD_TEST=true
WITH_FUSE_OPT=false
WITH_ZK_INIT=false
WITH_RDMA=false
WITH_PROMETHEUS=false
COMM_PLUGIN="brpc"

FALCONFS_INSTALL_DIR="${FALCONFS_INSTALL_DIR:-/usr/local/falconfs}"
export FALCONFS_INSTALL_DIR=$FALCONFS_INSTALL_DIR
export PATH=$FALCONFS_INSTALL_DIR/bin:$FALCONFS_INSTALL_DIR/python/bin:${PATH:-}
export LD_LIBRARY_PATH=$FALCONFS_INSTALL_DIR/lib64:$FALCONFS_INSTALL_DIR/lib:$FALCONFS_INSTALL_DIR/python/lib:${LD_LIBRARY_PATH:-}

# Default command is build
COMMAND=${1:-build}

# Get source directory
FALCONFS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
POSTGRES_INCLUDE_DIR="$(pg_config --includedir)"
POSTGRES_LIB_DIR="$(pg_config --libdir)"
PG_PKGLIBDIR="$(pg_config --pkglibdir)"
export CONFIG_FILE="$FALCONFS_DIR/config/config.json"

# Set build directory
BUILD_DIR="${BUILD_DIR:-$FALCONFS_DIR/build}"

# Set default install directory
FALCON_META_INSTALL_DIR="$FALCONFS_INSTALL_DIR/falcon_meta"
FALCON_CM_INSTALL_DIR="$FALCONFS_INSTALL_DIR/falcon_cm"
FALCON_CN_INSTALL_DIR="$FALCONFS_INSTALL_DIR/falcon_cn"
FALCON_DN_INSTALL_DIR="$FALCONFS_INSTALL_DIR/falcon_dn"
FALCON_STORE_INSTALL_DIR="$FALCONFS_INSTALL_DIR/falcon_store"
FALCON_REGRESS_INSTALL_DIR="$FALCONFS_INSTALL_DIR/falcon_regress"
FALCON_CLIENT_INSTALL_DIR="$FALCONFS_INSTALL_DIR/falcon_client"
PYTHON_SDK_INSTALL_DIR="$FALCONFS_INSTALL_DIR/falcon_python_interface"
PRIVATE_DIRECTORY_TEST_INSTALL_DIR="$FALCONFS_INSTALL_DIR/private-directory-test"

set_comm_plugin() {
    local plugin="${1,,}"
    case "$plugin" in
    brpc | hcom)
        COMM_PLUGIN="$plugin"
        ;;
    *)
        echo "Error: Unknown communication plugin '$1' (choose brpc|hcom)" >&2
        exit 1
        ;;
    esac
}

parse_comm_plugin_option() {
    local args=("$@")
    local count=${#args[@]}
    for ((i = 0; i < count; i++)); do
        case "${args[i]}" in
        --comm-plugin=*)
            set_comm_plugin "${args[i]#*=}"
            ;;
        --comm-plugin)
            if ((i + 1 < count)); then
                set_comm_plugin "${args[i + 1]}"
            else
                echo "Error: --comm-plugin requires a value (brpc|hcom)" >&2
                exit 1
            fi
            ;;
        esac
    done
}

parse_comm_plugin_option "$@"

gen_proto() {
    mkdir -p "$BUILD_DIR"
    echo "Generating Protobuf files..."
    protoc --cpp_out="$BUILD_DIR" \
        --proto_path="$FALCONFS_DIR/remote_connection_def/proto" \
        falcon_meta_rpc.proto brpc_io.proto
    echo "Protobuf files generated."
}

build_comm_plugin() {
    case "$COMM_PLUGIN" in
    brpc)
        echo "Building brpc communication plugin..."
        cd "$FALCONFS_DIR/falcon" && make -f MakefilePlugin.brpc
        echo "brpc communication plugin build complete."
        ;;
    hcom)
        echo "Building hcom communication plugin..."
        cd "$FALCONFS_DIR/falcon" && make -f MakefilePlugin.hcom
        echo "hcom communication plugin build complete."

        # Copy test plugins to plugins directory for hcom
        local test_plugin_src="$BUILD_DIR/test_plugins"
        local plugins_dest="$FALCONFS_DIR/plugins"
        if [[ -d "$test_plugin_src" ]]; then
            echo "Copying test plugins to $plugins_dest..."
            mkdir -p "$plugins_dest"
            cp -f "$test_plugin_src"/*.so "$plugins_dest/" 2>/dev/null || true
            echo "Test plugins copied."
        fi
	;;
    esac
}

# build_falconfs
build_falconfs() {
    gen_proto

    PG_CFLAGS=""
    if [[ "$BUILD_TYPE" == "Debug" ]]; then
        CONFIGURE_OPTS+=(--enable-debug)
        PG_CFLAGS="-ggdb -O0 -g3 -Wall -fno-omit-frame-pointer"
    else
        PG_CFLAGS="-O0 -g"
    fi
    echo "Building FalconFS Meta (mode: $BUILD_TYPE)..."
    cd $FALCONFS_DIR/falcon
    make USE_PGXS=1 CFLAGS="-Wno-shadow $PG_CFLAGS" CXXFLAGS="-Wno-shadow $PG_CFLAGS" \
        FALCONFS_INSTALL_DIR="$FALCONFS_INSTALL_DIR"

    echo "Building FalconFS Client (mode: $BUILD_TYPE)..."
    cmake -B "$BUILD_DIR" -GNinja "$FALCONFS_DIR" \
        -DCMAKE_INSTALL_PREFIX=$FALCON_CLIENT_INSTALL_DIR \
        -DCMAKE_EXPORT_COMPILE_COMMANDS=1 \
        -DCMAKE_BUILD_TYPE=${BUILD_TYPE} \
        -DPOSTGRES_INCLUDE_DIR="$POSTGRES_INCLUDE_DIR" \
        -DPOSTGRES_LIB_DIR="$POSTGRES_LIB_DIR" \
        -DPG_PKGLIBDIR="$PG_PKGLIBDIR" \
        -DWITH_FUSE_OPT="$WITH_FUSE_OPT" \
        -DWITH_ZK_INIT="$WITH_ZK_INIT" \
        -DWITH_RDMA="$WITH_RDMA" \
        -DWITH_PROMETHEUS="$WITH_PROMETHEUS" \
        -DBUILD_TEST=$BUILD_TEST &&
        cd "$BUILD_DIR" && ninja

    build_comm_plugin

    echo "FalconFS build complete."
}

# clean_falconfs
clean_falconfs() {
    echo "Cleaning FalconFS Meta"
    cd $FALCONFS_DIR/falcon
    make USE_PGXS=1 clean
    rm -rf $FALCONFS_DIR/falcon/connection_pool/fbs
    rm -rf $FALCONFS_DIR/falcon/brpc_comm_adapter/proto
    make -f MakefilePlugin.brpc clean || true
    make -f MakefilePlugin.hcom clean || true

    echo "Cleaning FalconFS Client..."
    rm -rf "$BUILD_DIR"
    echo "FalconFS clean complete."
}

clean_tests() {
    echo "Cleaning FalconFS tests..."
    rm -rf "$BUILD_DIR/tests"
    echo "FalconFS tests clean complete."
}

install_falcon_meta() {
    echo "Installing FalconFS meta ..."
    cd "$FALCONFS_DIR/falcon" && make USE_PGXS=1 install-falconfs \
        FALCONFS_INSTALL_DIR="$FALCONFS_INSTALL_DIR"
    echo "FalconFS meta installed"

    local plugin_src=""
    case "$COMM_PLUGIN" in
    brpc)
        plugin_src="$FALCONFS_DIR/falcon/libbrpcplugin.so"
        ;;
    hcom)
        plugin_src="$FALCONFS_DIR/falcon/libhcomplugin.so"
        ;;
    esac

    if [[ ! -f "$plugin_src" ]]; then
        echo "Error: communication plugin ($COMM_PLUGIN) not built at $plugin_src" >&2
        exit 1
    fi
    echo "copy ${COMM_PLUGIN} communication plugin to $FALCON_META_INSTALL_DIR/lib/postgresql..."
    cp "$plugin_src" "$FALCON_META_INSTALL_DIR/lib/postgresql/"
    # 复制依赖库
    if [[ -f "$FALCONFS_DIR/cloud_native/docker_build/ldd_copy.sh" ]]; then
        "$FALCONFS_DIR/cloud_native/docker_build/ldd_copy.sh" \
            -b "$plugin_src" \
            -t "$FALCON_META_INSTALL_DIR/lib"
    fi

    echo "${COMM_PLUGIN} communication plugin copied."

    # 安装测试插件 (如果存在)
    if [[ -f "$FALCONFS_DIR/falcon/libfalcon_meta_service_test_plugin.so" ]]; then
        cp "$FALCONFS_DIR/falcon/libfalcon_meta_service_test_plugin.so" \
           "$FALCON_META_INSTALL_DIR/lib/postgresql/"
        echo "test plugin copied."
    fi
}

install_falcon_client() {
    echo "Installing FalconFS client to $FALCON_CLIENT_INSTALL_DIR..."

    cd "$BUILD_DIR" && ninja install

    # 复制配置文件
    mkdir -p "$FALCON_CLIENT_INSTALL_DIR/config"
    cp -r "$FALCONFS_DIR/config"/* "$FALCON_CLIENT_INSTALL_DIR/config/"

    # 复制依赖库
    if [[ -f "$FALCONFS_DIR/cloud_native/docker_build/ldd_copy.sh" ]]; then
        "$FALCONFS_DIR/cloud_native/docker_build/ldd_copy.sh" \
            -b "$FALCON_CLIENT_INSTALL_DIR/bin/falcon_client" \
            -t "$FALCON_CLIENT_INSTALL_DIR/lib"
    fi

    echo "FalconFS client installed to $FALCON_CLIENT_INSTALL_DIR"
}

install_falcon_python_sdk() {
    echo "Installing FalconFS python sdk to $PYTHON_SDK_INSTALL_DIR..."
    rm -rf "$PYTHON_SDK_INSTALL_DIR"
    mkdir -p "$PYTHON_SDK_INSTALL_DIR"

    # 复制 python_interface 目录内容，排除 _pyfalconfs_internal
    for item in "$FALCONFS_DIR/python_interface"/*; do
        base=$(basename "$item")
        if [[ "$base" != "_pyfalconfs_internal" ]]; then
            cp -r "$item" "$PYTHON_SDK_INSTALL_DIR/"
        fi
    done

    echo "FalconFS python sdk installed to $PYTHON_SDK_INSTALL_DIR"
}

install_falcon_cm() {
    echo "Installing FalconFS cluster management scripts..."
    rm -rf "$FALCON_CM_INSTALL_DIR"
    mkdir -p "$FALCON_CM_INSTALL_DIR"

    # 从 cloud_native/falcon_cm/ 复制集群管理脚本
    cp -r "$FALCONFS_DIR/cloud_native/falcon_cm"/* "$FALCON_CM_INSTALL_DIR/"

    echo "FalconFS cluster management scripts installed"
}

install_falcon_cn() {
    echo "Installing FalconFS CN scripts..."
    rm -rf "$FALCON_CN_INSTALL_DIR"
    mkdir -p "$FALCON_CN_INSTALL_DIR"

    # 从 cloud_native/docker_build/cn/ 复制，排除 Dockerfile
    for file in "$FALCONFS_DIR/cloud_native/docker_build/cn"/*; do
        if [[ "$(basename "$file")" != "Dockerfile" ]]; then
            cp -r "$file" "$FALCON_CN_INSTALL_DIR/"
        fi
    done

    echo "FalconFS CN scripts installed"
}

install_falcon_dn() {
    echo "Installing FalconFS DN scripts..."
    rm -rf "$FALCON_DN_INSTALL_DIR"
    mkdir -p "$FALCON_DN_INSTALL_DIR"

    # 从 cloud_native/docker_build/dn/ 复制，排除 Dockerfile
    for file in "$FALCONFS_DIR/cloud_native/docker_build/dn"/*; do
        if [[ "$(basename "$file")" != "Dockerfile" ]]; then
            cp -r "$file" "$FALCON_DN_INSTALL_DIR/"
        fi
    done

    echo "FalconFS DN scripts installed"
}

install_falcon_store() {
    echo "Installing FalconFS Store scripts..."
    rm -rf "$FALCON_STORE_INSTALL_DIR"
    mkdir -p "$FALCON_STORE_INSTALL_DIR"

    # 只复制脚本文件，排除 falconfs 子目录和 Dockerfile
    for file in "$FALCONFS_DIR/cloud_native/docker_build/store"/*; do
        base=$(basename "$file")
        if [[ "$base" != "Dockerfile" && "$base" != "falconfs" ]]; then
            cp -r "$file" "$FALCON_STORE_INSTALL_DIR/"
        fi
    done

    echo "FalconFS Store scripts installed"
}

install_falcon_regress() {
    echo "Installing FalconFS Regress scripts..."
    rm -rf "$FALCON_REGRESS_INSTALL_DIR"
    mkdir -p "$FALCON_REGRESS_INSTALL_DIR"

    # 复制 regress 脚本
    for file in "$FALCONFS_DIR/cloud_native/docker_build/regress"/*; do
        base=$(basename "$file")
        if [[ "$base" != "Dockerfile" ]]; then
            cp -r "$file" "$FALCON_REGRESS_INSTALL_DIR/"
        fi
    done

    echo "FalconFS Regress scripts installed"
}

install_private_directory_test() {
    echo "Installing private-directory-test..."

    # 创建目录结构
    rm -rf "$PRIVATE_DIRECTORY_TEST_INSTALL_DIR"
    mkdir -p "$PRIVATE_DIRECTORY_TEST_INSTALL_DIR/bin"
    mkdir -p "$PRIVATE_DIRECTORY_TEST_INSTALL_DIR/lib"

    # 复制可执行文件
    if [[ -f "$FALCONFS_DIR/build/tests/private-directory-test/test_falcon" ]]; then
        cp "$FALCONFS_DIR/build/tests/private-directory-test/test_falcon" \
           "$PRIVATE_DIRECTORY_TEST_INSTALL_DIR/bin/"
    fi
    if [[ -f "$FALCONFS_DIR/build/tests/private-directory-test/test_posix" ]]; then
        cp "$FALCONFS_DIR/build/tests/private-directory-test/test_posix" \
           "$PRIVATE_DIRECTORY_TEST_INSTALL_DIR/bin/"
    fi

    # 复制 FalconCMIT
    if [[ -f "$FALCONFS_DIR/build/tests/common/FalconCMIT" ]]; then
        cp "$FALCONFS_DIR/build/tests/common/FalconCMIT" \
           "$PRIVATE_DIRECTORY_TEST_INSTALL_DIR/bin/"
    fi

    # 复制依赖库
    if [[ -f "$FALCONFS_DIR/cloud_native/docker_build/ldd_copy.sh" ]]; then
        "$FALCONFS_DIR/cloud_native/docker_build/ldd_copy.sh" \
            -b "$PRIVATE_DIRECTORY_TEST_INSTALL_DIR/bin/test_falcon" \
            -t "$PRIVATE_DIRECTORY_TEST_INSTALL_DIR/lib"
        "$FALCONFS_DIR/cloud_native/docker_build/ldd_copy.sh" \
            -b "$PRIVATE_DIRECTORY_TEST_INSTALL_DIR/bin/test_posix" \
            -t "$PRIVATE_DIRECTORY_TEST_INSTALL_DIR/lib"
        # 复制 FalconCMIT 依赖
        if [[ -f "$PRIVATE_DIRECTORY_TEST_INSTALL_DIR/bin/FalconCMIT" ]]; then
            "$FALCONFS_DIR/cloud_native/docker_build/ldd_copy.sh" \
                -b "$PRIVATE_DIRECTORY_TEST_INSTALL_DIR/bin/FalconCMIT" \
                -t "$PRIVATE_DIRECTORY_TEST_INSTALL_DIR/lib"
        fi
    fi

    # 复制脚本文件（排除 C++ 源码）
    for file in "$FALCONFS_DIR/tests/private-directory-test"/*; do
        base=$(basename "$file")
        case "$base" in
            *.sh|*.py)
                cp "$file" "$PRIVATE_DIRECTORY_TEST_INSTALL_DIR/"
                ;;
        esac
    done

    # 复制 README（如果存在）
    if [[ -f "$FALCONFS_DIR/tests/private-directory-test/README.md" ]]; then
        cp "$FALCONFS_DIR/tests/private-directory-test/README.md" \
           "$PRIVATE_DIRECTORY_TEST_INSTALL_DIR/"
    fi

    echo "private-directory-test installed"
}

install_deploy_scripts() {
    echo "Installing deploy scripts to $FALCONFS_INSTALL_DIR/deploy..."
    rm -rf "$FALCONFS_INSTALL_DIR/deploy"
    mkdir -p "$FALCONFS_INSTALL_DIR/deploy"
    # 复制 deploy 目录内容，排除 tmp 目录
    rsync -av --exclude='tmp' "$FALCONFS_DIR/deploy/" "$FALCONFS_INSTALL_DIR/deploy/"
    echo "deploy scripts installed to $FALCONFS_INSTALL_DIR/deploy"
}

print_help() {
    case "$1" in
    build)
        echo "Usage: $0 build [subcommand] [options]"
        echo ""
        echo "Build All Components of FalconFS"
        echo ""
        echo "Subcommands:"
        echo "  falcon           Build only FalconFS"
        echo ""
        echo "Options:"
        echo "  --debug              Build debug versions"
        echo "  --release            Build release versions (default)"
        echo "  --comm-plugin=PLUGIN Communication plugin: brpc (default) or hcom"
        echo "  -h, --help           Show this help message"
        echo ""
        echo "Examples:"
        echo "  $0 build --debug                 # Build everything in debug mode"
        echo "  $0 build --comm-plugin=hcom      # Build with hcom communication plugin"
        ;;
    clean)
        echo "Usage: $0 clean [target] [options]"
        echo ""
        echo "Clean build artifacts and installations"
        echo ""
        echo "Targets:"
        echo "  falcon   Clean FalconFS build artifacts"
        echo "  test     Clean test binaries"
        echo ""
        echo "Options:"
        echo "  -h, --help  Show this help message"
        echo ""
        echo "Examples:"
        echo "  $0 clean           # Clean everything"
        echo "  $0 clean falcon    # Clean only FalconFS"
        ;;
    *)
        # General help information
        echo "Usage: $0 <command> [subcommand] [options]"
        echo ""
        echo "Commands:"
        echo "  build     Build components"
        echo "  clean     Clean artifacts"
        echo "  test      Run tests"
        echo "  install   Install components"
        echo ""
        echo "Run '$0 <command> --help' for more information on a specific command"
        ;;
    esac
}

# Dispatch commands
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
        --comm-plugin)
            if [[ -z "${3:-}" ]]; then
                echo "Error: --comm-plugin requires a value (brpc|hcom)" >&2
                exit 1
            fi
            set_comm_plugin "$3"
            shift 2
            ;;
        --comm-plugin=*)
            set_comm_plugin "${2#*=}"
            shift
            ;;
        *)
            # Only break if this isn't the combined build case
            [[ -z "${2:-}" || "$2" == "pg" || "$2" == "falcon" ]] && break
            echo "Error: Combined build only supports --debug, --deploy or --comm-plugin" >&2
            exit 1
            ;;
        esac
    done

    case "${2:-}" in
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
            --comm-plugin)
                if [[ -z "${2:-}" ]]; then
                    echo "Error: --comm-plugin requires a value (brpc|hcom)" >&2
                    exit 1
                fi
                set_comm_plugin "$2"
                shift
                ;;
            --comm-plugin=*)
                set_comm_plugin "${1#*=}"
                ;;
            --help | -h)
                echo "Usage: $0 build falcon [options]"
                echo ""
                echo "Build FalconFS Components"
                echo ""
                echo "Options:"
                echo "  --debug              Build in debug mode"
                echo "  --release            Build in release mode"
                echo "  --relwithdebinfo     Build with debug symbols"
                echo "  --comm-plugin=PLUGIN Communication plugin: brpc (default) or hcom"
                echo "  --with-fuse-opt      Enable FUSE optimizations"
                echo "  --with-zk-init       Enable Zookeeper initialization for containerized deployment"
                echo "  --with-rdma          Enable RDMA support"
                echo "  --with-prometheus    Enable Prometheus metrics"
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
        build_falconfs
        ;;
    esac
    ;;
clean)
    case "${2:-}" in
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
        clean_falconfs
        ;;
    esac
    ;;
test)
    TARGET_DIRS=("$FALCONFS_DIR/build/tests/falcon_store/" "$FALCONFS_DIR/build/tests/falcon_plugin/")

    for TARGET_DIR in "${TARGET_DIRS[@]}"; do
        if [ -d "$TARGET_DIR" ]; then
            echo "Running tests in: $TARGET_DIR"
            find "$TARGET_DIR" -type f -executable -name "*UT" | while read -r executable_file; do
                echo "Executing: $executable_file"
                "$executable_file"
                echo "---------------------------------------------------------------------------------------"
            done
        else
            echo "Test directory not found: $TARGET_DIR"
        fi
    done
    TARGET_DIR="$FALCONFS_DIR/build/tests/falcon/"
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
    falcon)
        install_falcon_meta
        install_falcon_client
        install_falcon_python_sdk
        install_falcon_cm
        install_falcon_cn
        install_falcon_dn
        install_falcon_store
        install_falcon_regress
        install_private_directory_test
        install_deploy_scripts
        ;;
    *)
        install_falcon_meta
        install_falcon_client
        install_falcon_python_sdk
        install_falcon_cm
        install_falcon_cn
        install_falcon_dn
        install_falcon_store
        install_falcon_regress
        install_private_directory_test
        install_deploy_scripts
    esac
    ;;
*)
    print_help "build"
    exit 1
    ;;
esac
