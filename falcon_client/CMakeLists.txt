# ==================== FalconClient =================
file(GLOB_RECURSE FALCON_SRC "${PROJECT_SOURCE_DIR}/falcon_client/src/*.cpp" "${PROJECT_SOURCE_DIR}/falcon_client/src/*.c")

add_library(FalconClient
    ${FALCON_SRC}
    ${PROTO_SRC}
)

# add_library(bio_sdk SHARED IMPORTED)
# set_target_properties(bio_sdk PROPERTIES IMPORTED_LOCATION /opt/boostio/lib/libbio_sdk.so)

target_link_libraries(FalconClient PUBLIC
    ${BRPC_LIBRARIES}
    ${DYNAMIC_LIB}
    pthread
    pq
    fuse
    # bio_sdk # added
)
target_link_directories(FalconClient PUBLIC ${PG_INSTALL_PATH}/lib)
target_include_directories(FalconClient PUBLIC
    ${POSTGRES_SRC_DIR}/src/include
    ${POSTGRES_SRC_DIR}/src/interfaces/libpq
    ${PROJECT_SOURCE_DIR}/falcon_client/src/include
    ${PROJECT_SOURCE_DIR}/falcon_store/src/include
    ${PROJECT_SOURCE_DIR}/common/src/include
    ${PROJECT_SOURCE_DIR}/falcon_client/src/mock_include # 临时mock的接口，后面去掉
    /opt/boostio/include # added
)
