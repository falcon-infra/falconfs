#include "test_plugin_framework.h"

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

TEST_F(PluginFrameworkUT, LoadInvalidPluginPartialFunctions)
{
    void* dl_handle = nullptr;
    falcon_plugin_get_type_func_t get_type_func = nullptr;
    falcon_plugin_work_func_t work_func = nullptr;

    bool result = LoadPlugin(invalid_plugin_path, dl_handle, get_type_func, work_func);
    EXPECT_TRUE(result); // Loading succeeds, but functions are missing
    EXPECT_NE(dl_handle, nullptr);
    EXPECT_EQ(get_type_func, nullptr); // Missing function
    EXPECT_EQ(work_func, nullptr); // Missing function

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
    FalconPluginSharedData test_data;
    memset(&test_data, 0, sizeof(test_data));
    strcpy(test_data.plugin_name, "test_inline");
    strcpy(test_data.plugin_path, inline_plugin_path.c_str());
    strcpy(test_data.custom_config, "{\"test\": \"config\"}");

    int work_result = work_func(&test_data);
    EXPECT_EQ(work_result, 1);

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
    FalconPluginSharedData test_data;
    memset(&test_data, 0, sizeof(test_data));
    strcpy(test_data.plugin_name, "test_background");
    strcpy(test_data.plugin_path, background_plugin_path.c_str());
    strcpy(test_data.custom_config, "{\"test\": \"background_config\"}");

    // Test plugin work function multiple times
    // Background plugin has work limit of 3, so first 2 calls return 0, 3rd call returns 1
    int work_result;
    for (int i = 0; i < 2; i++) {
        work_result = work_func(&test_data);
        EXPECT_EQ(work_result, 0);
    }

    // Background plugin now always returns 0 for success (simplified design)
    work_result = work_func(&test_data);
    EXPECT_EQ(work_result, 0);

    dlclose(dl_handle);
}

/* ------------------------------------------- Plugin State Tests -------------------------------------------*/

TEST_F(PluginFrameworkUT, InlinePluginStateTracking)
{
    void* dl_handle = nullptr;
    falcon_plugin_get_type_func_t get_type_func = nullptr;
    falcon_plugin_work_func_t work_func = nullptr;

    ASSERT_TRUE(LoadPlugin(inline_plugin_path, dl_handle, get_type_func, work_func));

    int (*test_get_work_count)() = (int (*)())dlsym(dl_handle, "test_get_work_count");
    void (*test_reset_counters)() = (void (*)())dlsym(dl_handle, "test_reset_counters");

    ASSERT_NE(test_get_work_count, nullptr);
    ASSERT_NE(test_reset_counters, nullptr);

    test_reset_counters();

    EXPECT_EQ(test_get_work_count(), 0);

    FalconPluginSharedData test_data;
    memset(&test_data, 0, sizeof(test_data));
    work_func(&test_data);
    EXPECT_EQ(test_get_work_count(), 1);

    dlclose(dl_handle);
}

TEST_F(PluginFrameworkUT, BackgroundPluginStateTracking)
{
    void* dl_handle = nullptr;
    falcon_plugin_get_type_func_t get_type_func = nullptr;
    falcon_plugin_work_func_t work_func = nullptr;

    ASSERT_TRUE(LoadPlugin(background_plugin_path, dl_handle, get_type_func, work_func));

    int (*test_get_work_count)() = (int (*)())dlsym(dl_handle, "test_get_work_count");
    void (*test_reset_counters)() = (void (*)())dlsym(dl_handle, "test_reset_counters");
    void (*test_set_work_limit)(int) = (void (*)(int))dlsym(dl_handle, "test_set_work_limit");

    ASSERT_NE(test_get_work_count, nullptr);
    ASSERT_NE(test_reset_counters, nullptr);
    ASSERT_NE(test_set_work_limit, nullptr);

    test_reset_counters();
    test_set_work_limit(5);

    // Test initial state
    EXPECT_EQ(test_get_work_count(), 0);

    // Test work function calls
    FalconPluginSharedData test_data;
    memset(&test_data, 0, sizeof(test_data));

    for (int i = 1; i <= 5; i++) {
        int result = work_func(&test_data);
        EXPECT_EQ(test_get_work_count(), i);
        // Background plugin now always returns 0 for success (simplified design)
        EXPECT_EQ(result, 0);
    }

    dlclose(dl_handle);
}

/* ------------------------------------------- Function Name Constants Tests -------------------------------------------*/

TEST_F(PluginFrameworkUT, VerifyFunctionNameConstants)
{
    // Verify that the function name constants match expected values
    EXPECT_STREQ(FALCON_PLUGIN_GET_TYPE_FUNC_NAME, "plugin_get_type");
    EXPECT_STREQ(FALCON_PLUGIN_WORK_FUNC_NAME, "plugin_work");
}

TEST_F(PluginFrameworkUT, VerifyPluginTypeEnum)
{
    // Verify that the plugin type enumeration has expected values
    EXPECT_EQ(FALCON_PLUGIN_TYPE_INLINE, 0);
    EXPECT_EQ(FALCON_PLUGIN_TYPE_BACKGROUND, 1);
}

TEST_F(PluginFrameworkUT, VerifySharedDataStructure)
{
    // Test shared data structure
    FalconPluginSharedData data;
    memset(&data, 0, sizeof(data));

    strcpy(data.plugin_name, "test_plugin");
    strcpy(data.plugin_path, "/path/to/plugin.so");
    strcpy(data.custom_config, "{\"key\": \"value\"}");
    data.main_pid = 1234;

    EXPECT_STREQ(data.plugin_name, "test_plugin");
    EXPECT_STREQ(data.plugin_path, "/path/to/plugin.so");
    EXPECT_STREQ(data.custom_config, "{\"key\": \"value\"}");
    EXPECT_EQ(data.main_pid, 1234);
}

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}