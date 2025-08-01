# ==================== _pyfalconfs_internal  =================
add_library(_pyfalconfs_internal MODULE ${PROJECT_SOURCE_DIR}/python_interface/_pyfalconfs_internal/_pyfalconfs_internal.cpp)
set_target_properties(_pyfalconfs_internal PROPERTIES
    PREFIX ""
    SUFFIX ".so"
)
target_include_directories(_pyfalconfs_internal PUBLIC 
    ${Python3_INCLUDE_DIRS}
    ${POSTGRES_SRC_DIR}/src/include
    ${POSTGRES_SRC_DIR}/src/interfaces/libpq
    ${PROJECT_SOURCE_DIR}/falcon_store/src/include
    ${PROJECT_SOURCE_DIR}/common/src/include
)
target_link_options(_pyfalconfs_internal PRIVATE -static-libstdc++ -static-libgcc)
target_link_libraries(_pyfalconfs_internal PUBLIC 
    ${Python3_LIBRARIES}
    FalconClient
    FalconStore
    pthread
    pq
    fuse
    zookeeper_mt
    glog
    jsoncpp
    ${BRPC_LIBRARIES}
    ${DYNAMIC_LIB}
    dl
)
target_link_directories(_pyfalconfs_internal PUBLIC
    ${POSTGRES_SRC_DIR}/src/interfaces/libpq
)

install(TARGETS _pyfalconfs_internal
    LIBRARY DESTINATION ${PROJECT_SOURCE_DIR}/python_interface/pyfalconfs
)

# ==================== throw_hook  =================
add_library(throw_hook MODULE ${PROJECT_SOURCE_DIR}/python_interface/_pyfalconfs_internal/throw_hook.cpp)
set_target_properties(throw_hook PROPERTIES
    PREFIX ""
    SUFFIX ".so"
)

install(TARGETS throw_hook
    LIBRARY DESTINATION ${PROJECT_SOURCE_DIR}/python_interface/pyfalconfs
)