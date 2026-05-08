include(GoogleTest)

enable_testing()

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
)
target_include_directories(PluginLoaderCoverageUT PUBLIC
    ${PROJECT_SOURCE_DIR}/falcon/include
    ${PROJECT_SOURCE_DIR}/third_party/postgres/src/include
    ${POSTGRES_INCLUDE_DIR}
)
target_link_libraries(PluginLoaderCoverageUT
    gtest
    gtest_main
    dl
)

gtest_discover_tests(PluginLoaderCoverageUT)
