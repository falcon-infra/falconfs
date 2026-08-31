include(GoogleTest)

enable_testing()

execute_process(
    COMMAND pg_config --libdir
    OUTPUT_VARIABLE FALCON_KV_TEST_PG_LIBDIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_QUIET)

# ==================== FalconKVPrimitivesUT =================
add_executable(FalconKVPrimitivesUT
    ${PROJECT_SOURCE_DIR}/tests/falcon_kv/test_bitmap_allocator.cpp
    ${PROJECT_SOURCE_DIR}/tests/falcon_kv/test_lease_manager.cpp
    ${PROJECT_SOURCE_DIR}/tests/falcon_kv/test_lru_manager.cpp
    ${PROJECT_SOURCE_DIR}/tests/falcon_kv/test_kv_metadata_engine.cpp
    ${PROJECT_SOURCE_DIR}/tests/falcon_kv/test_kv_metadata_service_impl.cpp
    ${PROJECT_SOURCE_DIR}/tests/falcon_kv/test_dram_pool.cpp
    ${PROJECT_SOURCE_DIR}/tests/falcon_kv/test_store_region_registry.cpp
    ${PROJECT_SOURCE_DIR}/tests/falcon_kv/test_ssd_spill_manager.cpp
    ${PROJECT_SOURCE_DIR}/tests/falcon_kv/test_kv_store_engine.cpp
    ${PROJECT_SOURCE_DIR}/tests/falcon_kv/test_kv_data_service_impl.cpp
    ${PROJECT_SOURCE_DIR}/tests/falcon_kv/test_kv_brpc_adapters.cpp
    ${PROJECT_SOURCE_DIR}/tests/falcon_kv/test_throttle_admission.cpp
    ${PROJECT_SOURCE_DIR}/tests/falcon_kv/test_kv_subtx.cpp
    ${PROJECT_SOURCE_DIR}/tests/falcon_kv/test_kv_meta_table_accessor.cpp
    ${PROJECT_SOURCE_DIR}/tests/falcon_kv/test_kv_metadata_recovery.cpp
    ${PROJECT_SOURCE_DIR}/tests/falcon_kv/test_eviction_coordinator.cpp
    ${PROJECT_SOURCE_DIR}/tests/falcon_kv/test_dn_node_register.cpp
    ${PROJECT_SOURCE_DIR}/tests/falcon_kv/test_falcon_kv_store_smoke.cpp
    ${PROJECT_SOURCE_DIR}/tests/falcon_kv/test_dn_metadata_only.cpp
    ${PROJECT_SOURCE_DIR}/tests/falcon_kv/test_kv_eviction_worker_brpc.cpp
    ${PROJECT_SOURCE_DIR}/tests/falcon_kv/test_kv_watchdog.cpp
    ${PROJECT_SOURCE_DIR}/tests/falcon_kv/test_kv_store_facade.cpp
    ${PROJECT_SOURCE_DIR}/tests/falcon_kv/test_membership_refresh_diff.cpp
    ${PROJECT_SOURCE_DIR}/tests/falcon_kv/test_membership_refresh_rate_limit.cpp
    ${PROJECT_SOURCE_DIR}/tests/falcon_kv/test_dn_failover_retry_policy.cpp
    ${PROJECT_SOURCE_DIR}/tests/falcon_kv/test_kv_promote_from_evicted.cpp
)

target_include_directories(FalconKVPrimitivesUT PRIVATE
    ${FALCON_KV_INCLUDE_DIRS}
)
if(FALCON_KV_TEST_PG_LIBDIR)
    target_link_directories(FalconKVPrimitivesUT PRIVATE "${FALCON_KV_TEST_PG_LIBDIR}")
endif()

target_link_libraries(FalconKVPrimitivesUT
    FalconKVCommon
    FalconKVMetadataService
    FalconKVDataService
    FalconKVBrpcServiceAdapters
    FalconKVProto
    ${BRPC_LIBRARIES}
    ${DYNAMIC_LIB}
    pq
    gtest
    gtest_main
    pthread
)

gtest_discover_tests(FalconKVPrimitivesUT
    PROPERTIES LABELS "kv-unit"
)

# ==================== FalconKVStoreNativeMicrobench =================
add_executable(FalconKVStoreNativeMicrobench
    ${PROJECT_SOURCE_DIR}/tests/falcon_kv/kv_store_native_microbench.cpp
)

target_include_directories(FalconKVStoreNativeMicrobench PRIVATE
    ${FALCON_KV_INCLUDE_DIRS}
)

target_link_libraries(FalconKVStoreNativeMicrobench
    FalconKVProto
    ${BRPC_LIBRARIES}
    ${DYNAMIC_LIB}
    pthread
)

# ==================== FalconKVMetadataBrpcE2E =================
add_executable(FalconKVMetadataBrpcE2E
    ${PROJECT_SOURCE_DIR}/tests/falcon_kv/kv_metadata_brpc_e2e_client.cpp
)

target_include_directories(FalconKVMetadataBrpcE2E PRIVATE
    ${FALCON_KV_INCLUDE_DIRS}
)

target_link_libraries(FalconKVMetadataBrpcE2E
    FalconKVProto
    ${BRPC_LIBRARIES}
    ${DYNAMIC_LIB}
    pthread
)

# ==================== FalconKVMetaEngineConcurrencyE2E =================
add_executable(FalconKVMetaEngineConcurrencyE2E
    ${PROJECT_SOURCE_DIR}/tests/falcon_kv/kv_metadata_meta_engine_concurrency_e2e.cpp
)

target_include_directories(FalconKVMetaEngineConcurrencyE2E PRIVATE
    ${FALCON_KV_INCLUDE_DIRS}
)

target_link_libraries(FalconKVMetaEngineConcurrencyE2E
    FalconKVProto
    ${BRPC_LIBRARIES}
    ${DYNAMIC_LIB}
    pthread
)

# ==================== FalconKVMetadataStressE2E =================
# End-to-end KV metadata stress test that sweeps a (threads, batch_size, waves)
# matrix to exercise BatchAllocate / BatchUpdate / BatchLookup / BatchRenew /
# BatchFreeAllocated under varying concurrent BRPC clients, validates
# correctness on every surface, and verifies that the engine bitmap and the
# persistent catalog return to a clean state between sweeps.
add_executable(FalconKVMetadataStressE2E
    ${PROJECT_SOURCE_DIR}/tests/falcon_kv/kv_metadata_stress_e2e.cpp
)

target_include_directories(FalconKVMetadataStressE2E PRIVATE
    ${FALCON_KV_INCLUDE_DIRS}
)

target_link_libraries(FalconKVMetadataStressE2E
    FalconKVProto
    ${BRPC_LIBRARIES}
    ${DYNAMIC_LIB}
    pthread
)

# ==================== FalconKVClusterFaultE2E =================
# v6 §19.4 cluster fault drill harness. Single binary, multiple --scenario
# subcommands; a shell wrapper coordinates DN restart phases.
add_executable(FalconKVClusterFaultE2E
    ${PROJECT_SOURCE_DIR}/tests/falcon_kv/kv_cluster_fault_e2e.cpp
)

target_include_directories(FalconKVClusterFaultE2E PRIVATE
    ${FALCON_KV_INCLUDE_DIRS}
)

target_link_libraries(FalconKVClusterFaultE2E
    FalconKVProto
    ${BRPC_LIBRARIES}
    ${DYNAMIC_LIB}
    pthread
)

# ==================== FalconKVClusterE2E =================
# v6 §19.4 / M4 distributed multi-DN sweep. Hashes keys across one or more
# DN BRPC endpoints, drives alloc -> update -> lookup -> renew -> free
# end-to-end on every endpoint, and asserts both DNs see traffic.
add_executable(FalconKVClusterE2E
    ${PROJECT_SOURCE_DIR}/tests/falcon_kv/kv_cluster_e2e.cpp
)

target_include_directories(FalconKVClusterE2E PRIVATE
    ${FALCON_KV_INCLUDE_DIRS}
)

target_link_libraries(FalconKVClusterE2E
    FalconKVProto
    ${BRPC_LIBRARIES}
    ${DYNAMIC_LIB}
    pthread
)

# ==================== FalconKVP2SmokeE2E (v6.5 P2 cluster gate) ====================
add_executable(FalconKVP2SmokeE2E
    ${PROJECT_SOURCE_DIR}/tests/falcon_kv/kv_p2_smoke_e2e.cpp
)

target_include_directories(FalconKVP2SmokeE2E PRIVATE
    ${FALCON_KV_INCLUDE_DIRS}
)

target_link_libraries(FalconKVP2SmokeE2E
    FalconKVProto
    ${BRPC_LIBRARIES}
    ${DYNAMIC_LIB}
    pthread
)

# ==================== FalconKVMixedColocationE2E (v6.5 §19.4.1 drill #4) =================
# Four logical clients (NODE_NAME v65mix0..3), allocator affinity vs preferred_store_id,
# fallback, and multi-DN routing. Requires harness: KV_THREE_DNS=1, STORE_COUNT=4.
add_executable(FalconKVMixedColocationE2E
    ${PROJECT_SOURCE_DIR}/tests/falcon_kv/kv_mixed_colocation_e2e.cpp
)

target_include_directories(FalconKVMixedColocationE2E PRIVATE
    ${FALCON_KV_INCLUDE_DIRS}
)

if(FALCON_KV_TEST_PG_LIBDIR)
    target_link_directories(FalconKVMixedColocationE2E PRIVATE "${FALCON_KV_TEST_PG_LIBDIR}")
endif()

target_link_libraries(FalconKVMixedColocationE2E
    FalconKVBrpcServiceAdapters
    FalconKVProto
    ${BRPC_LIBRARIES}
    ${DYNAMIC_LIB}
    pq
    pthread
)
