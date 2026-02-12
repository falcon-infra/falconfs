include(GoogleTest)

enable_testing()
link_directories(${POSTGRES_LIB_DIR})

# ==================== FalconCMIT =================
add_executable(FalconCMIT
    ${PROJECT_SOURCE_DIR}/tests/common/test_falcon_cm.cpp
    ${common_src}
)
target_link_libraries(FalconCMIT
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

gtest_discover_tests(FalconCMIT)
