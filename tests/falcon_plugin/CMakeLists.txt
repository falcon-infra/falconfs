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