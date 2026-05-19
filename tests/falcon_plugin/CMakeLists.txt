include(GoogleTest)

enable_testing()

# Get PostgreSQL server headers from the system development package.
find_program(PG_CONFIG_EXECUTABLE pg_config)
if(NOT PG_CONFIG_EXECUTABLE)
    message(FATAL_ERROR "pg_config not found, please install PostgreSQL development package")
endif()

execute_process(
    COMMAND ${PG_CONFIG_EXECUTABLE} --includedir-server
    OUTPUT_VARIABLE PostgreSQL_INCLUDE_DIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

set(PostgreSQL_TYPE_INCLUDE_DIR ${PostgreSQL_INCLUDE_DIR})

# Create test plugins directory
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/test_plugins)

# Include plugin framework headers
include_directories(
    ${PROJECT_SOURCE_DIR}/falcon/include
)

# Test plugins
add_library(test_plugin_inline SHARED test_plugin_inline.c)
set_target_properties(test_plugin_inline PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/test_plugins
)

add_library(test_plugin_background SHARED test_plugin_background.c)
set_target_properties(test_plugin_background PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/test_plugins
)

add_library(test_plugin_invalid SHARED test_plugin_invalid.c)
set_target_properties(test_plugin_invalid PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/test_plugins
)

add_library(test_plugin_inline_fail SHARED test_plugin_inline_fail.c)
set_target_properties(test_plugin_inline_fail PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/test_plugins
)

add_library(test_plugin_background_fail SHARED test_plugin_background_fail.c)
set_target_properties(test_plugin_background_fail PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/test_plugins
)

# ==================== PluginFrameworkUT =================
add_executable(PluginFrameworkUT
    ${PROJECT_SOURCE_DIR}/tests/falcon_plugin/test_plugin_framework.cpp
)
target_link_libraries(PluginFrameworkUT
    gtest
    dl
)

gtest_discover_tests(PluginFrameworkUT)

# ==================== PluginLoaderUT =================
add_executable(PluginLoaderUT
    ${PROJECT_SOURCE_DIR}/tests/falcon_plugin/test_plugin_loader.cpp
)
target_link_libraries(PluginLoaderUT
    gtest
    dl
)

gtest_discover_tests(PluginLoaderUT)

# ==================== PluginLoaderCoverageUT =================
add_executable(PluginLoaderCoverageUT
    ${PROJECT_SOURCE_DIR}/tests/falcon_plugin/test_plugin_loader_coverage.cpp
    ${PROJECT_SOURCE_DIR}/falcon/plugin/falcon_plugin_loader.c
)
add_dependencies(PluginLoaderCoverageUT
    test_plugin_inline
    test_plugin_background
    test_plugin_invalid
    test_plugin_inline_fail
    test_plugin_background_fail
)
target_include_directories(PluginLoaderCoverageUT PUBLIC
    ${PROJECT_SOURCE_DIR}/falcon/include
    ${PostgreSQL_TYPE_INCLUDE_DIR}
    ${POSTGRES_INCLUDE_DIR}
)
target_link_libraries(PluginLoaderCoverageUT
    gtest
    gtest_main
    dl
)

gtest_discover_tests(PluginLoaderCoverageUT)
