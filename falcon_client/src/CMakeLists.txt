cmake_minimum_required(VERSION 3.17)
project(kvserver)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(
    SOURCES
    falcon_kv_server.cpp
    slice_key_gen.cpp
)

add_library(my_local_kvserver STATIC ${SOURCES}) # 创建库（为了触发编译），用STATIC LIBRARY 避免需要main函数


target_include_directories(my_local_kvserver PRIVATE
    include
    mock_include
    ${CMAKE_CURRENT_SOURCE_DIR}/../../common/src/include
)
