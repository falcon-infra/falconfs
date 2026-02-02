include(GoogleTest)

enable_testing()
link_directories(${POSTGRES_SRC_DIR}/src/interfaces/libpq)

# ==================== FalconQueueUT =================
add_executable(FalconQueueUT
    ${PROJECT_SOURCE_DIR}/tests/falcon/test_falcon_concurrent_queue.cpp
)
target_link_libraries(FalconQueueUT
    boost_system
    boost_thread
    gtest
)

target_include_directories(FalconQueueUT PUBLIC
    ${PROJECT_SOURCE_DIR}/falcon_store/src/include
    ${PROJECT_SOURCE_DIR}/falcon_client/src/include
    ${PROJECT_SOURCE_DIR}/common/src/include
    ${PROJECT_SOURCE_DIR}/falcon/include
    ${POSTGRES_SRC_DIR}/src/include
    ${POSTGRES_SRC_DIR}/src/interfaces/libpq
)
