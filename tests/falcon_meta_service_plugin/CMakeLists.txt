# FalconMetaService E2E Test Plugin
#
# 构建命令:
#   cd build && cmake .. && make falcon_meta_service_test_plugin
#
# 部署:
#   cp libfalcon_meta_service_test_plugin.so $FALCON_PLUGIN_DIR/
#
# 运行:
#   设置 GUC: falcon.plugin_directory = '$FALCON_PLUGIN_DIR'
#   启动 PostgreSQL，查看日志获取测试结果

cmake_minimum_required(VERSION 3.10)
project(falcon_meta_service_test_plugin)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# PostgreSQL 配置
execute_process(
    COMMAND pg_config --includedir-server
    OUTPUT_VARIABLE PG_INCLUDEDIR_SERVER
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

execute_process(
    COMMAND pg_config --includedir
    OUTPUT_VARIABLE PG_INCLUDEDIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

execute_process(
    COMMAND pg_config --libdir
    OUTPUT_VARIABLE PG_LIBDIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

# Protobuf
find_package(Protobuf REQUIRED)

# 包含目录
include_directories(
    ${CMAKE_SOURCE_DIR}/falcon/include
    ${CMAKE_SOURCE_DIR}/falcon/connection_pool/proto
    ${CMAKE_SOURCE_DIR}/falcon/connection_pool/fbs
    ${CMAKE_SOURCE_DIR}/remote_connection_def
    ${PG_INCLUDEDIR_SERVER}
    ${PG_INCLUDEDIR}
    ${PROTOBUF_INCLUDE_DIRS}
    /usr/local/include
)

# 链接目录
link_directories(
    ${CMAKE_SOURCE_DIR}/build/lib
    ${PG_LIBDIR}
    /usr/local/lib
)

# 插件共享库
add_library(falcon_meta_service_test_plugin SHARED
    falcon_meta_service_test_plugin.cpp
)

# 编译选项
target_compile_options(falcon_meta_service_test_plugin PRIVATE
    -fPIC
    -Wall
    -Wno-unused-variable
)

# 链接库
# 注意: brpc/gflags/glog 已在 falcon.so 中链接，这里不再链接，避免 gflags 重复注册
# 使用 -undefined dynamic_lookup 让符号在运行时解析
target_link_libraries(falcon_meta_service_test_plugin
    pthread
)

# 设置输出目录和链接选项
# Linux 下允许未定义符号，运行时从 falcon.so 解析
set_target_properties(falcon_meta_service_test_plugin PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/test_plugins
    PREFIX "lib"
    LINK_FLAGS "-Wl,--allow-shlib-undefined"
)

# 安装规则（可选）
install(TARGETS falcon_meta_service_test_plugin
    LIBRARY DESTINATION lib/falcon/plugins
)
