#include "test_plugin_framework.h"
#include <cstring>

std::string PluginFrameworkUT::inline_plugin_path = "";
std::string PluginFrameworkUT::background_plugin_path = "";
std::string PluginFrameworkUT::invalid_plugin_path = "";

/* ------------------------------------------- Plugin Loading Tests -------------------------------------------*/

TEST_F(PluginFrameworkUT, LoadInlinePluginSuccess)
{
    void* dl_handle = nullptr;
    falcon_plugin_get_type_func_t get_type_func = nullptr;
    falcon_plugin_work_func_t work_func = nullptr;

    bool result = LoadPlugin(inline_plugin_path, dl_handle, get_type_func, work_func);
    EXPECT_TRUE(result);
    EXPECT_NE(dl_handle, nullptr);
    EXPECT_NE(get_type_func, nullptr);
    EXPECT_NE(work_func, nullptr);

    if (dl_handle) {
        dlclose(dl_handle);
    }
}

TEST_F(PluginFrameworkUT, LoadBackgroundPluginSuccess)
{
    void* dl_handle = nullptr;
    falcon_plugin_get_type_func_t get_type_func = nullptr;
    falcon_plugin_work_func_t work_func = nullptr;

    bool result = LoadPlugin(background_plugin_path, dl_handle, get_type_func, work_func);
    EXPECT_TRUE(result);
    EXPECT_NE(dl_handle, nullptr);
    EXPECT_NE(get_type_func, nullptr);
    EXPECT_NE(work_func, nullptr);

    if (dl_handle) {
        dlclose(dl_handle);
    }
}

TEST_F(PluginFrameworkUT, LoadNonExistentPlugin)
{
    void* dl_handle = nullptr;
    falcon_plugin_get_type_func_t get_type_func = nullptr;
    falcon_plugin_work_func_t work_func = nullptr;

    std::string nonexistent_path = "./test_plugins/libnonexistent.so";
    bool result = LoadPlugin(nonexistent_path, dl_handle, get_type_func, work_func);
    EXPECT_FALSE(result);
    EXPECT_EQ(dl_handle, nullptr);
}

/* ------------------------------------------- Plugin Function Tests -------------------------------------------*/

TEST_F(PluginFrameworkUT, InlinePluginFunctionCalls)
{
    void* dl_handle = nullptr;
    falcon_plugin_get_type_func_t get_type_func = nullptr;
    falcon_plugin_work_func_t work_func = nullptr;

    ASSERT_TRUE(LoadPlugin(inline_plugin_path, dl_handle, get_type_func, work_func));
    ASSERT_NE(get_type_func, nullptr);
    ASSERT_NE(work_func, nullptr);

    FalconPluginWorkType type = get_type_func();
    EXPECT_EQ(type, FALCON_PLUGIN_TYPE_INLINE);

    // Create test shared data
    FalconPluginData test_data;
    memset(&test_data, 0, sizeof(test_data));
    strcpy(test_data.plugin_name, "test_inline");
    strcpy(test_data.plugin_path, inline_plugin_path.c_str());
    test_data.main_pid = getpid();

    int work_result = work_func(&test_data);
    EXPECT_EQ(work_result, 0);

    dlclose(dl_handle);
}

TEST_F(PluginFrameworkUT, BackgroundPluginFunctionCalls)
{
    void* dl_handle = nullptr;
    falcon_plugin_get_type_func_t get_type_func = nullptr;
    falcon_plugin_work_func_t work_func = nullptr;

    ASSERT_TRUE(LoadPlugin(background_plugin_path, dl_handle, get_type_func, work_func));
    ASSERT_NE(get_type_func, nullptr);
    ASSERT_NE(work_func, nullptr);

    FalconPluginWorkType type = get_type_func();
    EXPECT_EQ(type, FALCON_PLUGIN_TYPE_BACKGROUND);

    // Create test shared data
    FalconPluginData test_data;
    memset(&test_data, 0, sizeof(test_data));
    strcpy(test_data.plugin_name, "test_background");
    strcpy(test_data.plugin_path, background_plugin_path.c_str());
    test_data.main_pid = getpid();

    // Test plugin work function
    int work_result = work_func(&test_data);
    EXPECT_EQ(work_result, 0);

    dlclose(dl_handle);
}

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}