# 查找 BRPC 的头文件和库文件

if(DEFINED ENV{FALCONFS_INSTALL_DIR})
    list(APPEND EXTRA_INCLUDE_DIRS "$ENV{FALCONFS_INSTALL_DIR}/include")
    list(APPEND EXTRA_LIBRARY_DIRS "$ENV{FALCONFS_INSTALL_DIR}/lib")
    list(APPEND EXTRA_LIBRARY_DIRS "$ENV{FALCONFS_INSTALL_DIR}/lib64")
endif()

find_path(BRPC_INCLUDE_DIRS
  NAMES brpc/server.h
  PATHS
    ${EXTRA_INCLUDE_DIRS}
    /usr/local/include
    /usr/include
)

find_library(BRPC_LIBRARIES
  NAMES brpc
  PATHS
    ${EXTRA_LIBRARY_DIRS}
    /usr/local/lib64
    /usr/local/lib
    /usr/lib64
    /usr/lib
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(BRPC DEFAULT_MSG BRPC_INCLUDE_DIRS BRPC_LIBRARIES)
