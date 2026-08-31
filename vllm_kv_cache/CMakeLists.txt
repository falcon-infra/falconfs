# FalconFS KV cache native primitives.
#
# Currently builds the metadata-side data structures (bitmap allocator, lease
# manager, LRU manager) as a header-and-source library
# `FalconKVCommon`. The library has no PostgreSQL or BRPC dependencies; those
# come in later milestones (M1 PG-extension wiring and M2 Store data path).

set(FALCON_KV_INCLUDES
    ${PROJECT_SOURCE_DIR}
    ${PROJECT_SOURCE_DIR}/falcon/include
)

set(FALCON_KV_SOURCES
    ${PROJECT_SOURCE_DIR}/vllm_kv_cache/src/metadata/bitmap_allocator.cpp
    ${PROJECT_SOURCE_DIR}/vllm_kv_cache/src/metadata/lease_manager.cpp
    ${PROJECT_SOURCE_DIR}/vllm_kv_cache/src/metadata/lru_manager.cpp
)

add_library(FalconKVCommon STATIC ${FALCON_KV_SOURCES})

target_include_directories(FalconKVCommon PUBLIC
    ${FALCON_KV_INCLUDES}
)

# ==================== Proto codegen (v6 KV contracts) ====================
set(FALCON_KV_PROTO_DIR ${PROJECT_SOURCE_DIR}/vllm_kv_cache/proto)
set(FALCON_KV_PROTO_GEN_DIR ${CMAKE_BINARY_DIR}/vllm_kv_cache/generated)
file(MAKE_DIRECTORY ${FALCON_KV_PROTO_GEN_DIR})

# Propagate include paths so dependents (e.g. tests/falcon_kv) pick up both the
# source-tree headers AND the proto-generated headers (kv_common.pb.h ...).
set(FALCON_KV_INCLUDE_DIRS ${FALCON_KV_INCLUDES} ${FALCON_KV_PROTO_GEN_DIR}
    CACHE INTERNAL "FalconKV include dirs")

set(FALCON_KV_PROTO_FILES
    ${FALCON_KV_PROTO_DIR}/kv_common.proto
    ${FALCON_KV_PROTO_DIR}/kv_metadata_service.proto
    ${FALCON_KV_PROTO_DIR}/kv_data_service.proto
    ${FALCON_KV_PROTO_DIR}/kv_store_admin_service.proto
)

set(FALCON_KV_PROTO_SRCS
    ${FALCON_KV_PROTO_GEN_DIR}/kv_common.pb.cc
    ${FALCON_KV_PROTO_GEN_DIR}/kv_metadata_service.pb.cc
    ${FALCON_KV_PROTO_GEN_DIR}/kv_data_service.pb.cc
    ${FALCON_KV_PROTO_GEN_DIR}/kv_store_admin_service.pb.cc
)
set(FALCON_KV_PROTO_HDRS
    ${FALCON_KV_PROTO_GEN_DIR}/kv_common.pb.h
    ${FALCON_KV_PROTO_GEN_DIR}/kv_metadata_service.pb.h
    ${FALCON_KV_PROTO_GEN_DIR}/kv_data_service.pb.h
    ${FALCON_KV_PROTO_GEN_DIR}/kv_store_admin_service.pb.h
)

add_custom_command(
    OUTPUT ${FALCON_KV_PROTO_SRCS} ${FALCON_KV_PROTO_HDRS}
    COMMAND ${Protobuf_PROTOC_EXECUTABLE}
            --cpp_out=${FALCON_KV_PROTO_GEN_DIR}
            --proto_path=${FALCON_KV_PROTO_DIR}
            ${FALCON_KV_PROTO_FILES}
    DEPENDS ${FALCON_KV_PROTO_FILES}
    COMMENT "Generating Falcon KV protobuf sources"
    VERBATIM
)

add_library(FalconKVProto STATIC ${FALCON_KV_PROTO_SRCS})
target_include_directories(FalconKVProto PUBLIC
    ${FALCON_KV_PROTO_GEN_DIR}
    ${FALCON_KV_INCLUDES}
)
target_link_libraries(FalconKVProto PUBLIC ${Protobuf_LIBRARIES})

execute_process(
    COMMAND pg_config --libdir
    OUTPUT_VARIABLE FALCON_KV_PG_LIBDIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_QUIET)
if(NOT FALCON_KV_PG_LIBDIR)
    set(FALCON_KV_PG_LIBDIR "")
endif()
execute_process(
    COMMAND pg_config --includedir
    OUTPUT_VARIABLE FALCON_KV_PG_INCLUDEDIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_QUIET)
if(NOT FALCON_KV_PG_INCLUDEDIR)
    set(FALCON_KV_PG_INCLUDEDIR "")
endif()

# ==================== Metadata service skeleton ====================
add_library(FalconKVMetadataService STATIC
    ${PROJECT_SOURCE_DIR}/vllm_kv_cache/src/metadata/kv_metadata_engine.cpp
    ${PROJECT_SOURCE_DIR}/vllm_kv_cache/src/metadata/kv_shmem_runtime.cpp
    ${PROJECT_SOURCE_DIR}/vllm_kv_cache/src/metadata/kv_metadata_recovery.cpp
    ${PROJECT_SOURCE_DIR}/vllm_kv_cache/src/metadata/kv_metadata_recovery_scan_apply.cpp
    ${PROJECT_SOURCE_DIR}/vllm_kv_cache/src/metadata/kv_metadata_service_impl.cpp
    ${PROJECT_SOURCE_DIR}/vllm_kv_cache/src/metadata/kv_meta_table_accessor.cpp
    ${PROJECT_SOURCE_DIR}/vllm_kv_cache/src/metadata/eviction_coordinator.cpp
)
target_include_directories(FalconKVMetadataService PUBLIC
    ${FALCON_KV_INCLUDES}
    ${FALCON_KV_PROTO_GEN_DIR}
)
target_link_libraries(FalconKVMetadataService
    PUBLIC FalconKVCommon
    PUBLIC FalconKVProto
)

# ==================== Data service skeleton ====================
add_library(FalconKVDataService STATIC
    ${PROJECT_SOURCE_DIR}/vllm_kv_cache/src/store/dram_pool.cpp
    ${PROJECT_SOURCE_DIR}/vllm_kv_cache/src/store/store_region_registry.cpp
    ${PROJECT_SOURCE_DIR}/vllm_kv_cache/src/store/ssd_spill_manager.cpp
    ${PROJECT_SOURCE_DIR}/vllm_kv_cache/src/store/kv_store_engine.cpp
    ${PROJECT_SOURCE_DIR}/vllm_kv_cache/src/store/kv_data_service_impl.cpp
)
target_include_directories(FalconKVDataService PUBLIC
    ${FALCON_KV_INCLUDES}
    ${FALCON_KV_PROTO_GEN_DIR}
)
target_link_libraries(FalconKVDataService
    FalconKVProto
)

# ==================== brpc-compatible service adapters ====================
# These derive from the protobuf-generated abstract Service classes (enabled
# by `option cc_generic_services = true;` in the .proto files) and forward
# RPC method calls to KV*ServiceImpl. They depend only on protobuf, so they
# can be linked into both standalone gtest binaries and the brpc-server-
# hosted PG extension process.
add_library(FalconKVBrpcServiceAdapters STATIC
    ${PROJECT_SOURCE_DIR}/vllm_kv_cache/src/service/kv_metadata_brpc_service.cpp
    ${PROJECT_SOURCE_DIR}/vllm_kv_cache/src/service/kv_data_brpc_service.cpp
    ${PROJECT_SOURCE_DIR}/vllm_kv_cache/src/service/kv_store_admin_brpc_service.cpp
    ${PROJECT_SOURCE_DIR}/vllm_kv_cache/src/store/kv_store_facade.cpp
)
target_include_directories(FalconKVBrpcServiceAdapters PUBLIC
    ${FALCON_KV_INCLUDES}
    ${FALCON_KV_PROTO_GEN_DIR}
)
if(FALCON_KV_PG_INCLUDEDIR)
    target_include_directories(FalconKVBrpcServiceAdapters PRIVATE "${FALCON_KV_PG_INCLUDEDIR}")
endif()
if(FALCON_KV_PG_LIBDIR)
    target_link_directories(FalconKVBrpcServiceAdapters PRIVATE "${FALCON_KV_PG_LIBDIR}")
endif()
target_link_libraries(FalconKVBrpcServiceAdapters
    PUBLIC FalconKVMetadataService
    PUBLIC FalconKVDataService
    PUBLIC FalconKVProto
    PRIVATE pq
)

# ==================== Python protobuf bindings (v6 §16 / M3) ====================
# Generate `kv_*_pb2.py` modules into the in-tree falconfs_kv package so the
# Python OffloadingManager (mode="cluster") can build/parse the proto messages
# without a separate install step. Tests use `PYTHONPATH=vllm_kv_cache/python`.
set(FALCON_KV_PYTHON_PKG_DIR ${PROJECT_SOURCE_DIR}/vllm_kv_cache/python/falconfs_kv)
set(FALCON_KV_PYTHON_PROTO_OUTPUTS
    ${FALCON_KV_PYTHON_PKG_DIR}/kv_common_pb2.py
    ${FALCON_KV_PYTHON_PKG_DIR}/kv_metadata_service_pb2.py
    ${FALCON_KV_PYTHON_PKG_DIR}/kv_data_service_pb2.py
    ${FALCON_KV_PYTHON_PKG_DIR}/kv_store_admin_service_pb2.py
)
add_custom_command(
    OUTPUT ${FALCON_KV_PYTHON_PROTO_OUTPUTS}
    COMMAND ${Protobuf_PROTOC_EXECUTABLE}
            --python_out=${FALCON_KV_PYTHON_PKG_DIR}
            --proto_path=${FALCON_KV_PROTO_DIR}
            ${FALCON_KV_PROTO_FILES}
    # protoc generates `import kv_common_pb2` (top-level) but we drop the
    # files into the `falconfs_kv` package, where the import has to be
    # relative. Post-process to use `from . import ...`.
    COMMAND sed -i -E "s/^import (kv_[a-z_]+_pb2)/from . import \\1/g"
            ${FALCON_KV_PYTHON_PROTO_OUTPUTS}
    DEPENDS ${FALCON_KV_PROTO_FILES}
    COMMENT "Generating Falcon KV Python protobuf bindings"
    VERBATIM
)
add_custom_target(FalconKVPythonProto ALL
    DEPENDS ${FALCON_KV_PYTHON_PROTO_OUTPUTS}
)

# ==================== Python BRPC bridge extension (v6 §16 / M3) ====================
# Single-file CPython extension wrapping `brpc::Channel` + the KV stubs. The
# Python OffloadingManager and dn_client/store_client wrappers import it as
# `from falconfs_kv import falconfs_kv_brpc`.
add_library(falconfs_kv_brpc MODULE
    ${PROJECT_SOURCE_DIR}/vllm_kv_cache/python_ext/falconfs_kv_brpc.cpp
)
add_dependencies(falconfs_kv_brpc FalconKVProto)
set_target_properties(falconfs_kv_brpc PROPERTIES
    PREFIX ""
    SUFFIX ".so"
)
target_include_directories(falconfs_kv_brpc PRIVATE
    ${Python3_INCLUDE_DIRS}
    ${FALCON_KV_INCLUDES}
    ${FALCON_KV_PROTO_GEN_DIR}
)
if(FALCON_KV_PG_LIBDIR)
    target_link_directories(falconfs_kv_brpc PRIVATE "${FALCON_KV_PG_LIBDIR}")
endif()
target_link_libraries(falconfs_kv_brpc PRIVATE
    ${Python3_LIBRARIES}
    FalconKVBrpcServiceAdapters
    FalconKVProto
    ${BRPC_LIBRARIES}
    ${Protobuf_LIBRARIES}
    pq
    ${DYNAMIC_LIB}
    pthread
    dl
)
# Drop the shared object directly into the in-tree falconfs_kv package so
# `import falconfs_kv.falconfs_kv_brpc` resolves once tests put
# `vllm_kv_cache/python` on PYTHONPATH.
set_target_properties(falconfs_kv_brpc PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY ${FALCON_KV_PYTHON_PKG_DIR}
)

# ==================== falcon_kv_store daemon (v6.5 P2) ====================
add_executable(falcon_kv_store ${PROJECT_SOURCE_DIR}/falcon_kv_store/main.cpp)
target_include_directories(falcon_kv_store PRIVATE ${FALCON_KV_INCLUDE_DIRS})
if(FALCON_KV_PG_INCLUDEDIR)
    target_include_directories(falcon_kv_store PRIVATE "${FALCON_KV_PG_INCLUDEDIR}")
endif()
if(FALCON_KV_PG_LIBDIR)
    target_link_directories(falcon_kv_store PRIVATE "${FALCON_KV_PG_LIBDIR}")
endif()
target_link_libraries(falcon_kv_store PRIVATE
    FalconKVBrpcServiceAdapters
    FalconKVProto
    ${BRPC_LIBRARIES}
    ${Protobuf_LIBRARIES}
    ${DYNAMIC_LIB}
    pq
    pthread
    dl
)
