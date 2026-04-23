include(CTest)

link_directories(${POSTGRES_LIB_DIR})

add_executable(FalconMetadataUT
    ${PROJECT_SOURCE_DIR}/tests/metadata_ut/metadata_ut_client.cpp
    ${PROJECT_SOURCE_DIR}/tests/metadata_ut/test_metadata_ut.cpp
    ${common_src}
)

target_include_directories(FalconMetadataUT PUBLIC
    ${PROJECT_SOURCE_DIR}/falcon_store/src/include
    ${PROJECT_SOURCE_DIR}/falcon_client/src/include
    ${PROJECT_SOURCE_DIR}/common/src/include
    ${PROJECT_SOURCE_DIR}/falcon/include
    ${POSTGRES_INCLUDE_DIR}
)

target_link_libraries(FalconMetadataUT
    FalconStore
    FalconClient
    zookeeper_mt
    glog
    jsoncpp
    gtest
    pq
    ${BRPC_LIBRARIES}
    ${DYNAMIC_LIB}
)

add_test(
    NAME FalconMetadataUT
    COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/run_metadata_ut.sh
)

set_tests_properties(FalconMetadataUT PROPERTIES
    LABELS "ut;metadata;e2e"
    TIMEOUT 300
)
