#include "test_plugin_framework.h"

std::string PluginFrameworkUT::inline_plugin_path = "";
std::string PluginFrameworkUT::background_plugin_path = "";
std::string PluginFrameworkUT::invalid_plugin_path = "";

/* ------------------------------------------- Plugin Loading Tests -------------------------------------------*/

TEST_F(PluginFrameworkUT, LoadInlinePluginSuccess)
{
    void* dl_handle = nullptr;
    falcon_plugin_init_func_t init_func = nullptr;
    falcon_plugin_get_type_func_t get_type_func = nullptr;
    falcon_plugin_work_func_t work_func = nullptr;
    falcon_plugin_cleanup_func_t cleanup_func = nullptr;

    bool result = LoadPlugin(inline_plugin_path, dl_handle, init_func, get_type_func, work_func, cleanup_func);
    EXPECT_TRUE(result);
    EXPECT_NE(dl_handle, nullptr);
    EXPECT_NE(init_func, nullptr);
    EXPECT_NE(get_type_func, nullptr);
    EXPECT_NE(work_func, nullptr);
    EXPECT_NE(cleanup_func, nullptr);

    if (dl_handle) {
        dlclose(dl_handle);
    }
}

TEST_F(PluginFrameworkUT, LoadBackgroundPluginSuccess)
{
    void* dl_handle = nullptr;
    falcon_plugin_init_func_t init_func = nullptr;
    falcon_plugin_get_type_func_t get_type_func = nullptr;
    falcon_plugin_work_func_t work_func = nullptr;
    falcon_plugin_cleanup_func_t cleanup_func = nullptr;

    bool result = LoadPlugin(background_plugin_path, dl_handle, init_func, get_type_func, work_func, cleanup_func);
    EXPECT_TRUE(result);
    EXPECT_NE(dl_handle, nullptr);
    EXPECT_NE(init_func, nullptr);
    EXPECT_NE(get_type_func, nullptr);
    EXPECT_NE(work_func, nullptr);
    EXPECT_NE(cleanup_func, nullptr);

    if (dl_handle) {
        dlclose(dl_handle);
    }
}

TEST_F(PluginFrameworkUT, LoadInvalidPluginPartialFunctions)
{
    void* dl_handle = nullptr;
    falcon_plugin_init_func_t init_func = nullptr;
    falcon_plugin_get_type_func_t get_type_func = nullptr;
    falcon_plugin_work_func_t work_func = nullptr;
    falcon_plugin_cleanup_func_t cleanup_func = nullptr;

    bool result = LoadPlugin(invalid_plugin_path, dl_handle, init_func, get_type_func, work_func, cleanup_func);
    EXPECT_TRUE(result); // Loading succeeds, but some functions are missing
    EXPECT_NE(dl_handle, nullptr);
    EXPECT_NE(init_func, nullptr);
    EXPECT_EQ(get_type_func, nullptr); // Missing function
    EXPECT_EQ(work_func, nullptr); // Missing function
    EXPECT_NE(cleanup_func, nullptr);

    if (dl_handle) {
        dlclose(dl_handle);
    }
}

TEST_F(PluginFrameworkUT, LoadNonExistentPlugin)
{
    void* dl_handle = nullptr;
    falcon_plugin_init_func_t init_func = nullptr;
    falcon_plugin_get_type_func_t get_type_func = nullptr;
    falcon_plugin_work_func_t work_func = nullptr;
    falcon_plugin_cleanup_func_t cleanup_func = nullptr;

    std::string nonexistent_path = "./test_plugins/libnonexistent.so";
    bool result = LoadPlugin(nonexistent_path, dl_handle, init_func, get_type_func, work_func, cleanup_func);
    EXPECT_FALSE(result);
    EXPECT_EQ(dl_handle, nullptr);
}

/* ------------------------------------------- Plugin Function Tests -------------------------------------------*/

TEST_F(PluginFrameworkUT, InlinePluginFunctionCalls)
{
    void* dl_handle = nullptr;
    falcon_plugin_init_func_t init_func = nullptr;
    falcon_plugin_get_type_func_t get_type_func = nullptr;
    falcon_plugin_work_func_t work_func = nullptr;
    falcon_plugin_cleanup_func_t cleanup_func = nullptr;

    ASSERT_TRUE(LoadPlugin(inline_plugin_path, dl_handle, init_func, get_type_func, work_func, cleanup_func));
    ASSERT_NE(init_func, nullptr);
    ASSERT_NE(get_type_func, nullptr);
    ASSERT_NE(work_func, nullptr);

    int init_result = init_func();
    EXPECT_EQ(init_result, 0);

    FalconPluginWorkType type = get_type_func();
    EXPECT_EQ(type, FALCON_PLUGIN_TYPE_INLINE);

    int work_result = work_func();
    EXPECT_EQ(work_result, 1);

    cleanup_func();

    dlclose(dl_handle);
}

TEST_F(PluginFrameworkUT, BackgroundPluginFunctionCalls)
{
    void* dl_handle = nullptr;
    falcon_plugin_init_func_t init_func = nullptr;
    falcon_plugin_get_type_func_t get_type_func = nullptr;
    falcon_plugin_work_func_t work_func = nullptr;
    falcon_plugin_cleanup_func_t cleanup_func = nullptr;

    ASSERT_TRUE(LoadPlugin(background_plugin_path, dl_handle, init_func, get_type_func, work_func, cleanup_func));
    ASSERT_NE(init_func, nullptr);
    ASSERT_NE(get_type_func, nullptr);
    ASSERT_NE(work_func, nullptr);

    int init_result = init_func();
    EXPECT_EQ(init_result, 0);

    FalconPluginWorkType type = get_type_func();
    EXPECT_EQ(type, FALCON_PLUGIN_TYPE_BACKGROUND);

    // Test plugin work function multiple times
    // Background plugin has work limit of 3, so first 2 calls return 0, 3rd call returns 1
    int work_result;
    for (int i = 0; i < 2; i++) {
        work_result = work_func();
        EXPECT_EQ(work_result, 0);
    }

    // Third call should return non-zero to stop (based on work limit in plugin)
    work_result = work_func();
    EXPECT_EQ(work_result, 1);

    cleanup_func();

    dlclose(dl_handle);
}

/* ------------------------------------------- Plugin State Tests -------------------------------------------*/

TEST_F(PluginFrameworkUT, InlinePluginStateTracking)
{
    void* dl_handle = nullptr;
    falcon_plugin_init_func_t init_func = nullptr;
    falcon_plugin_get_type_func_t get_type_func = nullptr;
    falcon_plugin_work_func_t work_func = nullptr;
    falcon_plugin_cleanup_func_t cleanup_func = nullptr;

    ASSERT_TRUE(LoadPlugin(inline_plugin_path, dl_handle, init_func, get_type_func, work_func, cleanup_func));

    // Get test helper functions
    int (*test_get_init_count)() = (int (*)())dlsym(dl_handle, "test_get_init_count");
    int (*test_get_work_count)() = (int (*)())dlsym(dl_handle, "test_get_work_count");
    int (*test_get_cleanup_count)() = (int (*)())dlsym(dl_handle, "test_get_cleanup_count");
    void (*test_reset_counters)() = (void (*)())dlsym(dl_handle, "test_reset_counters");

    ASSERT_NE(test_get_init_count, nullptr);
    ASSERT_NE(test_get_work_count, nullptr);
    ASSERT_NE(test_get_cleanup_count, nullptr);
    ASSERT_NE(test_reset_counters, nullptr);

    // Reset counters
    test_reset_counters();

    // Test initial state
    EXPECT_EQ(test_get_init_count(), 0);
    EXPECT_EQ(test_get_work_count(), 0);
    EXPECT_EQ(test_get_cleanup_count(), 0);

    // Test after initialization
    init_func();
    EXPECT_EQ(test_get_init_count(), 1);
    EXPECT_EQ(test_get_work_count(), 0);
    EXPECT_EQ(test_get_cleanup_count(), 0);

    // Test after work
    work_func();
    EXPECT_EQ(test_get_init_count(), 1);
    EXPECT_EQ(test_get_work_count(), 1);
    EXPECT_EQ(test_get_cleanup_count(), 0);

    // Test after cleanup
    cleanup_func();
    EXPECT_EQ(test_get_init_count(), 1);
    EXPECT_EQ(test_get_work_count(), 1);
    EXPECT_EQ(test_get_cleanup_count(), 1);

    dlclose(dl_handle);
}

TEST_F(PluginFrameworkUT, BackgroundPluginStateTracking)
{
    void* dl_handle = nullptr;
    falcon_plugin_init_func_t init_func = nullptr;
    falcon_plugin_get_type_func_t get_type_func = nullptr;
    falcon_plugin_work_func_t work_func = nullptr;
    falcon_plugin_cleanup_func_t cleanup_func = nullptr;

    ASSERT_TRUE(LoadPlugin(background_plugin_path, dl_handle, init_func, get_type_func, work_func, cleanup_func));

    // Get test helper functions
    int (*test_get_init_count)() = (int (*)())dlsym(dl_handle, "test_get_init_count");
    int (*test_get_work_count)() = (int (*)())dlsym(dl_handle, "test_get_work_count");
    int (*test_get_cleanup_count)() = (int (*)())dlsym(dl_handle, "test_get_cleanup_count");
    void (*test_reset_counters)() = (void (*)())dlsym(dl_handle, "test_reset_counters");
    void (*test_set_work_limit)(int) = (void (*)(int))dlsym(dl_handle, "test_set_work_limit");

    ASSERT_NE(test_get_init_count, nullptr);
    ASSERT_NE(test_get_work_count, nullptr);
    ASSERT_NE(test_get_cleanup_count, nullptr);
    ASSERT_NE(test_reset_counters, nullptr);
    ASSERT_NE(test_set_work_limit, nullptr);

    // Reset counters and set work limit
    test_reset_counters();
    test_set_work_limit(5);

    // Test initial state
    EXPECT_EQ(test_get_init_count(), 0);
    EXPECT_EQ(test_get_work_count(), 0);
    EXPECT_EQ(test_get_cleanup_count(), 0);

    // Test after initialization
    init_func();
    EXPECT_EQ(test_get_init_count(), 1);
    EXPECT_EQ(test_get_work_count(), 0);
    EXPECT_EQ(test_get_cleanup_count(), 0);

    // Test work function calls
    for (int i = 1; i <= 5; i++) {
        int result = work_func();
        EXPECT_EQ(test_get_work_count(), i);
        if (i < 5) {
            EXPECT_EQ(result, 0); // Continue working
        } else {
            EXPECT_EQ(result, 1); // Stop working
        }
    }

    // Test after cleanup
    cleanup_func();
    EXPECT_EQ(test_get_init_count(), 1);
    EXPECT_EQ(test_get_work_count(), 5);
    EXPECT_EQ(test_get_cleanup_count(), 1);

    dlclose(dl_handle);
}

/* ------------------------------------------- Function Name Constants Tests -------------------------------------------*/

TEST_F(PluginFrameworkUT, VerifyFunctionNameConstants)
{
    // Verify that the function name constants match expected values
    EXPECT_STREQ(FALCON_PLUGIN_INIT_FUNC_NAME, "plugin_init");
    EXPECT_STREQ(FALCON_PLUGIN_GET_TYPE_FUNC_NAME, "plugin_get_type");
    EXPECT_STREQ(FALCON_PLUGIN_WORK_FUNC_NAME, "plugin_work");
    EXPECT_STREQ(FALCON_PLUGIN_CLEANUP_FUNC_NAME, "plugin_cleanup");
}

TEST_F(PluginFrameworkUT, VerifyPluginTypeEnum)
{
    // Verify that the plugin type enumeration has expected values
    EXPECT_EQ(FALCON_PLUGIN_TYPE_INLINE, 0);
    EXPECT_EQ(FALCON_PLUGIN_TYPE_BACKGROUND, 1);
}

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}