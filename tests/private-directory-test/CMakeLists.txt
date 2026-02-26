cmake_minimum_required(VERSION 3.15)

project(MyProject)

add_definitions(-D_FILE_OFFSET_BITS=64)
set(CMAKE_CXX_FLAGS "$ENV{CXXFLAGS} -O0 -Wall -g -ggdb")
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(test_posix main.cc workloads.cc posix.cc
    ${CMAKE_SOURCE_DIR}/common/src/log/logging.cpp
)

target_link_libraries(test_posix 
    PUBLIC pthread
    PUBLIC fmt
    PUBLIC ${DYNAMIC_LIB}
)

link_directories(
    ${POSTGRES_LIB_DIR}
)
include_directories(
    ${POSTGRES_INCLUDE_DIR}
    ${CMAKE_SOURCE_DIR}/falcon_store/src/include
    ${CMAKE_SOURCE_DIR}/falcon_client/src/include
    ${CMAKE_SOURCE_DIR}/common/src/include
)

add_executable(test_falcon main.cc workloads.cc falconfs.cc
    ${CMAKE_SOURCE_DIR}/common/src/log/logging.cpp
)

target_link_libraries(test_falcon
    FalconClient
    FalconStore
    zookeeper_mt
    glog
    jsoncpp
    gtest
    pq
    ${BRPC_LIBRARIES}
    ${DYNAMIC_LIB}
)
