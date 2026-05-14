include(GoogleTest)

enable_testing()

# ==================== FalconClientCoverageUT =================
add_executable(FalconClientCoverageUT
    ${PROJECT_SOURCE_DIR}/tests/falcon_client/test_falcon_client_coverage.cpp
    ${PROJECT_SOURCE_DIR}/falcon/brpc_comm_adapter/brpc_meta_service_imp.cpp
    ${PROJECT_SOURCE_DIR}/falcon/brpc_comm_adapter/brpc_meta_service_job.cpp
    ${PROJECT_SOURCE_DIR}/falcon/brpc_comm_adapter/falcon_brpc_server.cpp
)

target_link_libraries(FalconClientCoverageUT
    FalconClient
    FalconStore
    zookeeper_mt
    glog
    jsoncpp
    gtest
    pq
    gcov
    ${BRPC_LIBRARIES}
    ${DYNAMIC_LIB}
)

target_include_directories(FalconClientCoverageUT PUBLIC
    ${PROJECT_SOURCE_DIR}/falcon/include
    ${PROJECT_SOURCE_DIR}/falcon_client/src/include
    ${PROJECT_SOURCE_DIR}/falcon_store/src/include
    ${PROJECT_SOURCE_DIR}/common/src/include
    ${PROJECT_SOURCE_DIR}/remote_connection_def
)

gtest_discover_tests(FalconClientCoverageUT)
