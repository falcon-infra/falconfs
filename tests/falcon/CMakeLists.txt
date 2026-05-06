include(GoogleTest)

enable_testing()
link_directories(${POSTGRES_SRC_DIR}/src/interfaces/libpq)

add_subdirectory(metadb)

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
    ${POSTGRES_INCLUDE_DIR}
)

gtest_discover_tests(FalconQueueUT)

# ==================== ConnectionPoolCoverageUT =================
add_executable(ConnectionPoolCoverageUT
    ${PROJECT_SOURCE_DIR}/tests/falcon/test_connection_pool_coverage.cpp
    ${PROJECT_SOURCE_DIR}/falcon/utils/falcon_shmem_allocator.c
    ${PROJECT_SOURCE_DIR}/falcon/utils/serialized_data.c
    ${PROJECT_SOURCE_DIR}/falcon/utils/utils_standalone.c
)
target_link_libraries(ConnectionPoolCoverageUT
    boost_system
    boost_thread
    gtest
    gtest_main
)

target_include_directories(ConnectionPoolCoverageUT PUBLIC
    ${PROJECT_SOURCE_DIR}/falcon_store/src/include
    ${PROJECT_SOURCE_DIR}/falcon_client/src/include
    ${PROJECT_SOURCE_DIR}/common/src/include
    ${PROJECT_SOURCE_DIR}/falcon/include
    ${PROJECT_SOURCE_DIR}/falcon/connection_pool/fbs
    ${POSTGRES_SRC_DIR}/src/include
    ${POSTGRES_SRC_DIR}/src/interfaces/libpq
    ${POSTGRES_INCLUDE_DIR}
)

gtest_discover_tests(ConnectionPoolCoverageUT)

# ==================== PGConnectionCoverageUT =================
add_executable(PGConnectionCoverageUT
    ${PROJECT_SOURCE_DIR}/tests/falcon/test_pg_connection_coverage.cpp
    ${PROJECT_SOURCE_DIR}/falcon/utils/serialized_data.c
)
target_link_libraries(PGConnectionCoverageUT
    boost_system
    boost_thread
    gtest
    gtest_main
)

target_include_directories(PGConnectionCoverageUT PUBLIC
    ${PROJECT_SOURCE_DIR}/falcon/include
    ${PROJECT_SOURCE_DIR}/falcon/connection_pool/fbs
    ${POSTGRES_SRC_DIR}/src/include
    ${POSTGRES_SRC_DIR}/src/interfaces/libpq
    ${POSTGRES_INCLUDE_DIR}
)

gtest_discover_tests(PGConnectionCoverageUT)

# ==================== PerfCounterCoverageUT =================
add_executable(PerfCounterCoverageUT
    ${PROJECT_SOURCE_DIR}/tests/falcon/test_perf_counter_coverage.cpp
    ${PROJECT_SOURCE_DIR}/falcon/perf_counter/falcon_per_request_stat.c
)
target_link_libraries(PerfCounterCoverageUT
    gtest
    gtest_main
)

target_include_directories(PerfCounterCoverageUT PUBLIC
    ${PROJECT_SOURCE_DIR}/falcon/include
    ${PROJECT_SOURCE_DIR}/third_party/postgres/src/include
    ${POSTGRES_SRC_DIR}/src/include
    ${POSTGRES_INCLUDE_DIR}
)

gtest_discover_tests(PerfCounterCoverageUT)

# ==================== FalconConnectionPoolCCoverageUT =================
add_library(test_comm_plugin_coverage SHARED
    ${PROJECT_SOURCE_DIR}/tests/falcon/test_comm_plugin_coverage.c
)
set_target_properties(test_comm_plugin_coverage PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/test_comm_plugins
)
target_include_directories(test_comm_plugin_coverage PUBLIC
    ${PROJECT_SOURCE_DIR}/falcon/include
)

add_executable(FalconConnectionPoolCCoverageUT
    ${PROJECT_SOURCE_DIR}/tests/falcon/test_falcon_connection_pool_c_coverage.cpp
    ${PROJECT_SOURCE_DIR}/falcon/connection_pool/falcon_connection_pool.c
    ${PROJECT_SOURCE_DIR}/falcon/utils/falcon_shmem_allocator.c
)
add_dependencies(FalconConnectionPoolCCoverageUT test_comm_plugin_coverage)
target_compile_definitions(FalconConnectionPoolCCoverageUT PRIVATE _GNU_SOURCE)
target_link_libraries(FalconConnectionPoolCCoverageUT
    gtest
    gtest_main
    dl
)

target_include_directories(FalconConnectionPoolCCoverageUT PUBLIC
    ${PROJECT_SOURCE_DIR}/falcon/include
    ${PROJECT_SOURCE_DIR}/third_party/postgres/src/include
    ${PROJECT_SOURCE_DIR}/common/src/include
    ${POSTGRES_INCLUDE_DIR}
)

gtest_discover_tests(FalconConnectionPoolCCoverageUT)
