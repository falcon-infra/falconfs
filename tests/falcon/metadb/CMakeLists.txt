add_executable(MetadbCoverageUT
    test_metadb_coverage_ut.cpp
    test_metadb_coverage_common.cpp
    test_metadb_dfs_flows_ut.cpp
    test_metadb_helper_ut.cpp
    test_metadb_sql_serialized_ut.cpp
    ${PROJECT_SOURCE_DIR}/falcon/metadb/meta_process_info.c
    ${PROJECT_SOURCE_DIR}/falcon/metadb/meta_serialize_interface_helper.cpp
    ${PROJECT_SOURCE_DIR}/falcon/utils/utils_standalone.c
    ${PROJECT_SOURCE_DIR}/tests/private-directory-test/workloads.cc
    ${PROJECT_SOURCE_DIR}/tests/private-directory-test/falconfs.cc
)

target_include_directories(MetadbCoverageUT PUBLIC
    ${PROJECT_SOURCE_DIR}/tests/private-directory-test
    ${PROJECT_SOURCE_DIR}/falcon_store/src/include
    ${PROJECT_SOURCE_DIR}/falcon_client/src/include
    ${PROJECT_SOURCE_DIR}/common/src/include
    ${PROJECT_SOURCE_DIR}/falcon/include
    ${PROJECT_BINARY_DIR}/generated
    ${POSTGRES_INCLUDE_DIR}
)

add_dependencies(MetadbCoverageUT GeneratedFlatBuffers)

target_link_libraries(MetadbCoverageUT
    FalconStore
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
