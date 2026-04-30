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

# ==================== FalconClientFuseMainMockUT =================
add_executable(FalconClientFuseMainMockUT
    ${PROJECT_SOURCE_DIR}/tests/falcon_client/test_fuse_main_mock.cpp
    ${PROJECT_SOURCE_DIR}/common/src/log/logging.cpp
    ${PROJECT_SOURCE_DIR}/common/src/conf/property_key.cpp
    ${PROJECT_SOURCE_DIR}/falcon_client/src/serialized_data.c
)

target_link_libraries(FalconClientFuseMainMockUT
    gtest
    gtest_main
    gflags
    fuse
    pthread
    glog
    jsoncpp
    pq
    ${BRPC_LIBRARIES}
    ${DYNAMIC_LIB}
)

target_link_directories(FalconClientFuseMainMockUT PUBLIC
    ${POSTGRES_LIB_DIR}
)

target_include_directories(FalconClientFuseMainMockUT PUBLIC
    ${PROJECT_SOURCE_DIR}
    ${PROJECT_SOURCE_DIR}/falcon_client/src/include
    ${PROJECT_SOURCE_DIR}/falcon_store/src/include
    ${PROJECT_SOURCE_DIR}/common/src/include
    ${PROJECT_SOURCE_DIR}/remote_connection_def
)

gtest_discover_tests(FalconClientFuseMainMockUT)
