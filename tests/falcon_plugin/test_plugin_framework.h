#pragma once

#include <gtest/gtest.h>
#include <dlfcn.h>
#include <string>
#include <vector>
#include <filesystem>

#include "log/logging.h"

extern "C" {
#include "plugin/falcon_plugin_framework.h"
}

class PluginFrameworkUT : public testing::Test {
public:
    static void SetUpTestSuite()
    {
        FALCON_LOG(LOG_INFO) << "Calling PluginFrameworkUT SetUpTestSuite!";

        // Initialize test plugin paths - try multiple possible locations
        std::vector<std::string> possible_paths = {
            "./test_plugins/",  // When run from test directory
            "build/tests/falcon_plugin/test_plugins/",  // When run from project root
            "tests/falcon_plugin/test_plugins/"  // Alternative location
        };

        std::string plugin_dir;
        for (const auto& path : possible_paths) {
            if (std::filesystem::exists(path)) {
                plugin_dir = path;
                break;
            }
        }

        inline_plugin_path = plugin_dir + "libtest_plugin_inline.so";
        background_plugin_path = plugin_dir + "libtest_plugin_background.so";
        invalid_plugin_path = plugin_dir + "libtest_plugin_invalid.so";
    }

    static void TearDownTestSuite()
    {
        FALCON_LOG(LOG_INFO) << "Calling PluginFrameworkUT TearDownTestSuite!";
    }

    void SetUp() override {}
    void TearDown() override {}

    // Helper function to load plugin and get function pointers
    bool LoadPlugin(const std::string& path, void*& dl_handle,
                   falcon_plugin_get_type_func_t& get_type_func,
                   falcon_plugin_work_func_t& work_func)
    {
        dl_handle = dlopen(path.c_str(), RTLD_LAZY);
        if (!dl_handle) {
            return false;
        }

        get_type_func = (falcon_plugin_get_type_func_t)dlsym(dl_handle, FALCON_PLUGIN_GET_TYPE_FUNC_NAME);
        work_func = (falcon_plugin_work_func_t)dlsym(dl_handle, FALCON_PLUGIN_WORK_FUNC_NAME);

        return true;
    }

    // Helper function to load plugin with init and cleanup functions
    bool LoadPluginFull(const std::string& path, void*& dl_handle,
                       falcon_plugin_init_func_t& init_func,
                       falcon_plugin_get_type_func_t& get_type_func,
                       falcon_plugin_work_func_t& work_func,
                       falcon_plugin_cleanup_func_t& cleanup_func)
    {
        dl_handle = dlopen(path.c_str(), RTLD_LAZY);
        if (!dl_handle) {
            return false;
        }

        init_func = (falcon_plugin_init_func_t)dlsym(dl_handle, FALCON_PLUGIN_INIT_FUNC_NAME);
        get_type_func = (falcon_plugin_get_type_func_t)dlsym(dl_handle, FALCON_PLUGIN_GET_TYPE_FUNC_NAME);
        work_func = (falcon_plugin_work_func_t)dlsym(dl_handle, FALCON_PLUGIN_WORK_FUNC_NAME);
        cleanup_func = (falcon_plugin_cleanup_func_t)dlsym(dl_handle, FALCON_PLUGIN_CLEANUP_FUNC_NAME);

        return true;
    }

    static std::string inline_plugin_path;
    static std::string background_plugin_path;
    static std::string invalid_plugin_path;
};

// Static member declarations only
