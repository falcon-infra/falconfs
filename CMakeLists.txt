cmake_minimum_required(VERSION 3.12)
project(FalconFS VERSION 0.1.0 LANGUAGES C CXX)

# ==================== Compiler Flags =================
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_CXX_FLAGS "-D_FILE_OFFSET_BITS=64 -DTHREADED -DWITH_FUSE_OPT=${WITH_FUSE_OPT}")
set(CMAKE_BUILD_TYPE Debug)
set(CMAKE_CXX_FLAGS_DEBUG "$ENV{CXXFLAGS} -O0 -Wall -g -ggdb")
set(CMAKE_CXX_FLAGS_RELEASE "$ENV{CXXFLAGS} -O2 -Wall")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "$ENV{CXXFLAGS} -O2 -g -Wall")
set(CMAKE_CONFIGURATION_TYPES "Debug;Release;RelWithDebInfo" CACHE STRING "Build types" FORCE)
option(WITH_ZK_INIT "Enable Zookeeper initialization for containerized deployment" OFF)
if(WITH_ZK_INIT)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DZK_INIT")
endif()

option(WITH_RDMA "Enable storage connection use RDMA" OFF)
if(WITH_RDMA)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DUSE_RDMA")
endif()

option(WITH_PROMETHEUS "Enable prometheus monitor" OFF)
if(WITH_PROMETHEUS)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DWITH_PROMETHEUS")
endif()

option(FALCON_ENABLE_PLUGINS "Enable plugin support" ON)

if(FALCON_ENABLE_PLUGINS)
    find_library(DL_LIBRARY dl)
    if(NOT DL_LIBRARY)
        message(FATAL_ERROR "dlopen library not found, required for plugin support")
    endif()

    include(CheckCXXSourceCompiles)
    check_cxx_source_compiles("
        #include <filesystem>
        int main() { std::filesystem::exists(\"/tmp\"); return 0; }
    " HAS_FILESYSTEM)

    if(NOT HAS_FILESYSTEM)
        message(FATAL_ERROR "C++17 filesystem support required for plugin system")
    endif()

    add_definitions(-DFALCON_ENABLE_PLUGINS)
endif()

# ==================== Dependencies =================
list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake")
find_package(BRPC REQUIRED)

# Find required packages
find_package(Threads REQUIRED)
find_package(Protobuf REQUIRED)
find_package(BRPC REQUIRED)
find_package(GFlags REQUIRED)
find_package(LevelDB REQUIRED)
find_package(OpenSSL REQUIRED)

# Compiler flags with GFlags namespace
set(GFLAGS_NS "gflags")
set(CMAKE_CPP_FLAGS "${CMAKE_CPP_FLAGS} -DGFLAGS_NS=${GFLAGS_NS} -faligned-new")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_CPP_FLAGS} -O2 -D__const__=__unused__ -pipe -W -Wall -Wno-unused-parameter -fPIE -fPIC -fno-omit-frame-pointer")

# Dynamic libraries
set(DYNAMIC_LIB
    ${CMAKE_THREAD_LIBS_INIT}
    ${GFLAGS_LIBRARIES}
    ${Protobuf_LIBRARIES}
    ${LevelDB_LIBRARIES}
    ${OPENSSL_CRYPTO_LIBRARIES}
    ${OPENSSL_SSL_LIBRARIES}
    dl
    z
    glog
)

if(WITH_PROMETHEUS)
    list(APPEND DYNAMIC_LIB "prometheus-cpp-pull" "prometheus-cpp-core")
endif()

# protobuf generated
set(PROTO_SRC ${CMAKE_BINARY_DIR}/falcon_meta_rpc.pb.cc)
set(PROTO_HEADER ${CMAKE_BINARY_DIR}/falcon_meta_rpc.pb.h)
set(BRPC_SRC ${CMAKE_BINARY_DIR}/brpc_io.pb.cc)
set(BRPC_HEADER ${CMAKE_BINARY_DIR}/brpc_io.pb.h)

include(${CMAKE_SOURCE_DIR}/cmake/Flatbuffers.cmake)
include_directories(${FLATBUFFERS_GENERATED_DIR} ${CMAKE_BINARY_DIR} /usr/local/obs/include)
include_directories(${PROJECT_SOURCE_DIR}/remote_connection_def)

# ==================== Common Sources =================
set(COMMON_SRC_PATH "${PROJECT_SOURCE_DIR}/common/src")
file(GLOB_RECURSE common_src "${COMMON_SRC_PATH}/*.cpp" "${COMMON_SRC_PATH}/*.c")

add_subdirectory(falcon_store)
add_subdirectory(falcon_client)

if(NOT DEFINED POSTGRES_SRC_DIR)
    message(FATAL_ERROR "POSTGRES_SRC_DIR must be defined")
endif()


# ==================== falcon client  =================
add_executable(falcon_client ${PROJECT_SOURCE_DIR}/falcon_client/fuse_main.cpp)
set_target_properties(falcon_client PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
)
target_link_libraries(falcon_client
    FalconStore
    FalconClient
    pthread
    pq
    fuse
    zookeeper_mt
    glog
    jsoncpp
    ${BRPC_LIBRARIES}
    ${DYNAMIC_LIB}
)

target_link_directories(falcon_client PUBLIC
    ${POSTGRES_SRC_DIR}/src/interfaces/libpq
)

target_include_directories(falcon_client PUBLIC
    ${POSTGRES_SRC_DIR}/src/include
    ${POSTGRES_SRC_DIR}/src/interfaces/libpq
    ${PROJECT_SOURCE_DIR}/falcon_store/src/include
    ${PROJECT_SOURCE_DIR}/common/src/include
)

# ==================== Install Targets =================
install(TARGETS FalconStore FalconClient falcon_client
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
)

if (BUILD_TEST)
    message("Building tests...")
    add_subdirectory(tests)
endif()
