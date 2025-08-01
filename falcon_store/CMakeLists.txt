# ==================== FalconStore =================
file(GLOB_RECURSE store_src "${PROJECT_SOURCE_DIR}/falcon_store/src/*.cpp" "${PROJECT_SOURCE_DIR}/falcon_store/src/*.c")

add_library(FalconStore STATIC
    ${common_src}
    ${store_src}
    ${PROTO_SRC}
    ${PROTO_HEADER}
    ${BRPC_SRC}
    ${BRPC_HEADER}
)
set_target_properties(FalconStore PROPERTIES POSITION_INDEPENDENT_CODE ON)
add_dependencies(FalconStore GeneratedFlatBuffers)

target_include_directories(FalconStore PUBLIC
    ${PROJECT_SOURCE_DIR}/falcon_store/src/include/
    ${COMMON_SRC_PATH}/include/
    ${POSTGRES_SRC_DIR}/src/include
    ${POSTGRES_SRC_DIR}/src/interfaces/libpq
    /usr/local/obs/include
    ${PROJECT_SOURCE_DIR}/falcon_client/src/include/
)

set(OBS_LIB_DIR "/usr/local/obs/lib/")
file(GLOB OBS_LIBS "${OBS_LIB_DIR}/*.so")

target_link_libraries(FalconStore PUBLIC
    pthread
    ${OBS_LIBS}
    ${BRPC_LIBRARIES}
    ${DYNAMIC_LIB}
    fmt
    jsoncpp
)
