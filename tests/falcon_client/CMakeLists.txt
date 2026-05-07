include(GoogleTest)

enable_testing()

# ==================== FalconClientCoverageUT =================
add_executable(FalconClientCoverageUT
    ${PROJECT_SOURCE_DIR}/tests/falcon_client/test_falcon_client_coverage.cpp
)

target_link_libraries(FalconClientCoverageUT
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

target_include_directories(FalconClientCoverageUT PUBLIC
    ${PROJECT_SOURCE_DIR}/falcon_client/src/include
    ${PROJECT_SOURCE_DIR}/falcon_store/src/include
    ${PROJECT_SOURCE_DIR}/common/src/include
    ${PROJECT_SOURCE_DIR}/remote_connection_def
)

gtest_discover_tests(FalconClientCoverageUT)
