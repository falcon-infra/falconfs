# ==================== FalconClient =================
file(GLOB_RECURSE FALCON_SRC "${PROJECT_SOURCE_DIR}/falcon_client/src/*.cpp" "${PROJECT_SOURCE_DIR}/falcon_client/src/*.c")

add_library(FalconClient
    ${FALCON_SRC}
    ${PROTO_SRC}
)
set_target_properties(FalconClient PROPERTIES POSITION_INDEPENDENT_CODE ON)

target_link_libraries(FalconClient PUBLIC
    ${BRPC_LIBRARIES}
    ${DYNAMIC_LIB}
    pthread
    pq
    fuse
)
target_link_directories(FalconClient PUBLIC ${POSTGRES_LIB_DIR})
target_include_directories(FalconClient PUBLIC
    ${POSTGRES_INCLUDE_DIR}
    ${PROJECT_SOURCE_DIR}/falcon_client/src/include
    ${PROJECT_SOURCE_DIR}/falcon_store/src/include
    ${PROJECT_SOURCE_DIR}/common/src/include
)
