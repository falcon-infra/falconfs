#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <string>

#include "vllm_kv_cache/src/store/ssd_spill_manager.h"

namespace falconfs::kv {

namespace {

std::string MakeTempDir() {
    namespace fs = std::filesystem;
    fs::path base = fs::temp_directory_path() / "falconfs_kv_ssd_test";
    fs::create_directories(base);
    fs::path unique = base / std::to_string(::getpid());
    fs::create_directories(unique);
    return unique.string();
}

}  // namespace

TEST(SSDSpillManager, ComputePathLayoutMatchesV6) {
    SSDSpillManager mgr("/tmp/falcon_kv_ssd_root");
    // block_hash = "ab" -> hex "6162", prefix "6162"
    std::string path = mgr.ComputePath(/*store_node_id=*/3, /*block_hash=*/"ab", /*version=*/9);
    EXPECT_EQ(path, "/tmp/falcon_kv_ssd_root/3/6162/6162.9.kv");
    EXPECT_TRUE(mgr.ValidatePath(path));
}

TEST(SSDSpillManager, RejectsTraversalAndNonRootedPaths) {
    SSDSpillManager mgr("/tmp/falcon_kv_ssd_root");
    EXPECT_FALSE(mgr.ValidatePath("relative/path"));
    EXPECT_FALSE(mgr.ValidatePath("/elsewhere/foo.kv"));
    EXPECT_FALSE(mgr.ValidatePath("/tmp/falcon_kv_ssd_root/../escape"));
    EXPECT_FALSE(mgr.ValidatePath("/tmp/falcon_kv_ssd_root"));  // exact root not a file
}

TEST(SSDSpillManager, SpillThenRoundtrip) {
    const std::string root = MakeTempDir();
    SSDSpillManager mgr(root);
    SSDSpillResult sr = mgr.Spill(/*store_node_id=*/1, /*block_hash=*/std::string("\x10\x20", 2),
                                  /*version=*/1, /*payload=*/"hello-ssd");
    ASSERT_TRUE(sr.ok) << sr.error_message;
    EXPECT_TRUE(mgr.ValidatePath(sr.evicted_path));

    SSDSpillReadResult rr = mgr.Read(sr.evicted_path);
    ASSERT_TRUE(rr.ok) << rr.error_message;
    EXPECT_EQ(rr.payload, "hello-ssd");
}

TEST(SSDSpillManager, ReadRejectsBadPath) {
    SSDSpillManager mgr("/tmp/falcon_kv_ssd_root");
    SSDSpillReadResult rr = mgr.Read("/etc/passwd");
    EXPECT_FALSE(rr.ok);
}

}  // namespace falconfs::kv
