#include "buffer/base64.h"
#include "conf/falcon_config.h"
#include "conf/falcon_property_key.h"
#include "stats/falcon_stats.h"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

namespace {

std::filesystem::path MakeTempFile(const std::string &name, const std::string &content)
{
    auto path = std::filesystem::temp_directory_path() / name;
    std::ofstream out(path);
    out << content;
    out.close();
    return path;
}

std::string FullFalconConfigJson()
{
    return R"json({
      "main": {
        "falcon_log_dir": "/tmp/falcon_common_cov_log",
        "falcon_log_level": "INFO",
        "falcon_log_max_size_mb": 32,
        "falcon_thread_num": 4,
        "falcon_node_id": 7,
        "falcon_cache_root": "/tmp/falcon_common_cov_cache",
        "falcon_dir_num": 16,
        "falcon_block_size": 4096,
        "falcon_read_big_file_size": 8192,
        "falcon_cluster_view": ["127.0.0.1:56039", "127.0.0.2:56040"],
        "falcon_server_ip": "127.0.0.1",
        "falcon_server_port": "55500",
        "falcon_async": true,
        "falcon_persist": false,
        "falcon_max_open_num": 64,
        "falcon_preblock_num": 2,
        "falcon_eviction": 0.25,
        "falcon_is_inference": false,
        "falcon_mount_path": "/tmp/falcon_mnt",
        "falcon_to_local": true,
        "falcon_log_reserved_num": 3,
        "falcon_log_reserved_time": 5,
        "falcon_stat_max": true,
        "falcon_use_prometheus": false,
        "falcon_prometheus_port": "19090"
      },
      "runtime": {}
    })json";
}

void ResetStats()
{
    auto &stats = FalconStats::GetInstance();
    for (int i = 0; i < STATS_END; ++i) {
        stats.stats[i].store(0);
        stats.storedStats[i].store(0);
    }
}

}  // namespace

TEST(CommonBase64UT, EncodeDecodePaddingAndBinaryData)
{
    const std::vector<unsigned char> input = {'f', 'a', 'l', 'c', 'o', 'n', '\0', 'f', 's'};
    std::vector<char> encoded(BASE64_ENCODE_OUT_SIZE(input.size()));
    unsigned int encoded_size = base64_encode(input.data(), input.size(), encoded.data());
    EXPECT_EQ(std::string(encoded.data(), encoded_size), "ZmFsY29uAGZz");

    std::vector<unsigned char> decoded(BASE64_DECODE_OUT_SIZE(encoded_size));
    unsigned int decoded_size = base64_decode(encoded.data(), encoded_size, decoded.data());
    ASSERT_EQ(decoded_size, input.size());
    EXPECT_EQ(std::vector<unsigned char>(decoded.begin(), decoded.begin() + decoded_size), input);

    char one_byte[BASE64_ENCODE_OUT_SIZE(1)] = {};
    EXPECT_EQ(base64_encode(reinterpret_cast<const unsigned char *>("f"), 1, one_byte), 4U);
    EXPECT_STREQ(one_byte, "Zg==");

    char two_bytes[BASE64_ENCODE_OUT_SIZE(2)] = {};
    EXPECT_EQ(base64_encode(reinterpret_cast<const unsigned char *>("fo"), 2, two_bytes), 4U);
    EXPECT_STREQ(two_bytes, "Zm8=");
}

TEST(CommonBase64UT, DecodeRejectsMalformedInput)
{
    unsigned char out[8] = {};
    EXPECT_EQ(base64_decode("abc", 3, out), UINT32_MAX);
    EXPECT_EQ(base64_decode("@@@@", 4, out), 0U);
    EXPECT_EQ(base64_decode("!!!!", 4, out), 0U);
}

TEST(CommonFalconConfigUT, LoadsTypedPropertiesAndArrays)
{
    auto config_path = MakeTempFile("falcon_common_config_full.json", FullFalconConfigJson());
    FalconConfig config;
    ASSERT_EQ(config.InitConf(config_path.string()), 0);

    EXPECT_EQ(config.GetString(FalconPropertyKey::FALCON_LOG_DIR), "/tmp/falcon_common_cov_log");
    EXPECT_EQ(config.GetString(FalconPropertyKey::FALCON_SERVER_IP), "127.0.0.1");
    EXPECT_EQ(config.GetUint32(FalconPropertyKey::FALCON_THREAD_NUM), 4U);
    EXPECT_EQ(config.GetUint32(FalconPropertyKey::FALCON_MAX_OPEN_NUM), 64U);
    EXPECT_TRUE(config.GetBool(FalconPropertyKey::FALCON_ASYNC));
    EXPECT_FALSE(config.GetBool(FalconPropertyKey::FALCON_PERSIST));
    EXPECT_DOUBLE_EQ(config.GetDouble(FalconPropertyKey::FALCON_EVICTION), 0.25);
    EXPECT_EQ(config.GetArray(FalconPropertyKey::FALCON_CLUSTER_VIEW), "127.0.0.1:56039,127.0.0.2:56040");

    std::filesystem::remove(config_path);
}

TEST(CommonFalconConfigUT, ReportsInvalidFilesAndTypes)
{
    FalconConfig config;
    EXPECT_NE(config.InitConf(""), 0);
    EXPECT_NE(config.InitConf("/tmp/falcon_config_file_not_exist.json"), 0);

    auto invalid_json = MakeTempFile("falcon_common_config_invalid.json", "{ invalid json");
    EXPECT_NE(config.InitConf(invalid_json.string()), 0);
    std::filesystem::remove(invalid_json);

    std::string wrong_type = FullFalconConfigJson();
    const std::string from = "\"falcon_thread_num\": 4";
    const std::string to = "\"falcon_thread_num\": \"bad\"";
    wrong_type.replace(wrong_type.find(from), from.size(), to);
    auto wrong_type_path = MakeTempFile("falcon_common_config_wrong_type.json", wrong_type);
    EXPECT_NE(config.InitConf(wrong_type_path.string()), 0);
    std::filesystem::remove(wrong_type_path);
}

TEST(CommonFalconConfigUT, FormatUtilConvertsSupportedTypes)
{
    Json::Value string_value("falcon");
    EXPECT_EQ(std::any_cast<std::string>(FormatUtil::JsonToAny(string_value, FALCON_STRING)), "falcon");

    Json::Value array_value(Json::arrayValue);
    array_value.append("a");
    array_value.append("b");
    EXPECT_EQ(std::any_cast<std::string>(FormatUtil::JsonToAny(array_value, FALCON_ARRAY)), "a,b");

    EXPECT_TRUE(FormatUtil::StringToAny("12", FALCON_UINT).has_value());
    EXPECT_TRUE(FormatUtil::StringToAny("34", FALCON_UINT64).has_value());
    EXPECT_TRUE(FormatUtil::StringToAny("true", FALCON_BOOL).has_value());
    EXPECT_EQ(FormatUtil::AnyToString(std::any(uint32_t{56}), FALCON_UINT), "56");
    EXPECT_EQ(FormatUtil::AnyToString(std::any(true), FALCON_BOOL), "1");
    EXPECT_FALSE(FormatUtil::JsonToAny(string_value, static_cast<DataType>(999)).has_value());
}

TEST(CommonFalconStatsUT, FormatHelpersCoverUnitsAndRounding)
{
    EXPECT_EQ(formatU64(0), "0");
    EXPECT_EQ(formatU64(9999), "9999B");
    EXPECT_EQ(formatU64(10000), "9K");
    EXPECT_EQ(formatU64(10ULL * 1024 * 1024), "10M");

    EXPECT_EQ(formatOp(0), "0");
    EXPECT_EQ(formatOp(9999), "9999");
    EXPECT_EQ(formatOp(10000), "9K");
    EXPECT_EQ(formatOp(10ULL * 1024 * 1024), "10M");

    EXPECT_DOUBLE_EQ(formatTimeDouble(1234, 0), 1234.0);
    EXPECT_EQ(formatTime(1234, 1), "1.234");
    EXPECT_EQ(formatTime(1, 10000), "0");
}

TEST(CommonFalconStatsUT, ConvertsStatsVectorToDisplayStrings)
{
    std::vector<size_t> stats(STATS_END, 0);
    stats[FUSE_OPS] = 2;
    stats[FUSE_LAT] = 4000;
    stats[FUSE_LAT_MAX] = 3000;
    stats[FUSE_READ] = 10000;
    stats[FUSE_READ_OPS] = 1;
    stats[FUSE_READ_LAT] = 2000;
    stats[FUSE_READ_LAT_MAX] = 2500;
    stats[FUSE_WRITE] = 2048;
    stats[FUSE_WRITE_OPS] = 2;
    stats[FUSE_WRITE_LAT] = 6000;
    stats[FUSE_WRITE_LAT_MAX] = 4000;
    stats[META_OPS] = 3;
    stats[META_LAT] = 9000;
    stats[META_LAT_MAX] = 7000;
    stats[META_OPEN] = 1;
    stats[META_OPEN_LAT] = 1000;
    stats[META_OPEN_LAT_MAX] = 1500;
    stats[META_STAT] = 1;
    stats[META_LOOKUP] = 2;
    stats[META_STAT_LAT] = 6000;
    stats[META_STAT_LAT_MAX] = 5000;
    stats[META_RELEASE] = 1;
    stats[META_RELEASE_LAT] = 3000;
    stats[META_RELEASE_LAT_MAX] = 3500;
    stats[META_CREATE] = 1;
    stats[META_CREATE_LAT] = 4000;
    stats[META_CREATE_LAT_MAX] = 4500;
    stats[BLOCKCACHE_READ] = 10000;
    stats[BLOCKCACHE_WRITE] = 4096;
    stats[OBJ_GET] = 7;
    stats[OBJ_PUT] = 8;

    auto string_stats = convertStatstoString(stats);
    EXPECT_EQ(string_stats[FUSE_OPS], "2");
    EXPECT_EQ(string_stats[FUSE_LAT], "2");
    EXPECT_EQ(string_stats[FUSE_LAT_MAX], "3");
    EXPECT_EQ(string_stats[FUSE_READ], "9K");
    EXPECT_EQ(string_stats[FUSE_WRITE], "2048B");
    EXPECT_EQ(string_stats[META_STAT], "3");
    EXPECT_EQ(string_stats[META_STAT_LAT], "2");
    EXPECT_EQ(string_stats[BLOCKCACHE_READ], "9K");
    EXPECT_EQ(string_stats[OBJ_PUT], "8B");

    testing::internal::CaptureStdout();
    printStatsHeader();
    printStatsVector(string_stats);
    std::string output = testing::internal::GetCapturedStdout();
    EXPECT_NE(output.find("fuse"), std::string::npos);
    EXPECT_NE(output.find("meta"), std::string::npos);
}

TEST(CommonFalconStatsUT, TimerUpdatesLatencyAndMaxCounters)
{
    ResetStats();
    setStatMax(false);
    {
        StatFuseTimer timer(FUSE_LAT, META_LAT);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_GT(FalconStats::GetInstance().stats[FUSE_LAT].load(), 0U);
    EXPECT_GT(FalconStats::GetInstance().stats[META_LAT].load(), 0U);
    EXPECT_EQ(FalconStats::GetInstance().stats[META_LAT_MAX].load(), 0U);

    setStatMax(true);
    {
        StatFuseTimer timer(FUSE_READ_LAT);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_GT(FalconStats::GetInstance().stats[FUSE_READ_LAT_MAX].load(), 0U);
    setStatMax(false);
}

TEST(CommonFalconStatsUT, StoreAndPrintStatsFlow)
{
    ResetStats();
    auto &stats = FalconStats::GetInstance();
    stats.stats[FUSE_READ_OPS].store(2);
    stats.stats[FUSE_WRITE_OPS].store(3);
    stats.stats[META_OPEN].store(4);
    stats.stats[META_STAT].store(5);
    stats.stats[FUSE_LAT].store(6000);
    stats.stats[META_LAT].store(7000);
    stats.stats[META_OPEN_LAT_MAX].store(11);
    stats.stats[META_CREATE_LAT_MAX].store(13);

    std::jthread store_thread([](std::stop_token token) { FalconStats::GetInstance().storeStatforGet(token); });
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    store_thread.request_stop();
    store_thread.join();

    EXPECT_EQ(stats.storedStats[META_OPS].load(), 9U);
    EXPECT_EQ(stats.storedStats[FUSE_OPS].load(), 14U);
    EXPECT_EQ(stats.storedStats[FUSE_LAT].load(), 13000U);
    EXPECT_EQ(stats.storedStats[META_LAT_MAX].load(), 13U);

    ResetStats();
    auto temp_root = std::filesystem::temp_directory_path() / "falcon_stats_cov";
    auto mount_path = temp_root / "mnt";
    std::filesystem::remove_all(temp_root);
    std::filesystem::create_directories(mount_path);
    stats.stats[FUSE_OPS].store(1);
    stats.stats[FUSE_READ].store(4096);
    stats.stats[META_CREATE].store(2);
    stats.stats[OBJ_GET].store(3);

    std::jthread print_thread([&](std::stop_token token) { PrintStats(mount_path.string(), token); });
    auto stat_path = temp_root / "stats.out";
    for (int i = 0; i < 30 && !std::filesystem::exists(stat_path); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    print_thread.request_stop();
    print_thread.join();

    ASSERT_TRUE(std::filesystem::exists(stat_path));
    std::ifstream in(stat_path);
    std::string content((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    EXPECT_NE(content.find("Falcon File System Statistics"), std::string::npos);
    EXPECT_NE(content.find("Metadata Operations"), std::string::npos);
    EXPECT_NE(content.find("Object Operations"), std::string::npos);

    std::filesystem::remove_all(temp_root);
}

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
