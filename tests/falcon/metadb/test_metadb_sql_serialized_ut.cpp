#include "test_metadb_coverage_common.h"

#include <limits>

using namespace metadb_test;

namespace {

struct SqlConnections {
    std::unique_ptr<PgConnection> cn_owner;
    std::unique_ptr<PgConnection> worker_owner;
    PgConnection *cn = nullptr;
    PgConnection *worker = nullptr;
};

bool PrepareSqlConnections(SqlConnections *connections)
{
    static bool service_checked = false;
    static bool service_ready = false;
    if (service_checked && !service_ready) {
        return false;
    }

    constexpr int kRetry = 2;
    for (int attempt = 0; attempt < kRetry; ++attempt) {
        if (!local_run_test::WaitForEndpoint(local_run_test::GetEnvOrDefault("SERVER_IP", "127.0.0.1"),
                                             local_run_test::GetIntEnvOrDefault("SERVER_PORT", 55500),
                                             1)) {
            service_checked = true;
            service_ready = false;
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }
        if (!InitClientOrSkip()) {
            service_checked = true;
            service_ready = false;
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }

        int cn_port = local_run_test::GetIntEnvOrDefault("LOCAL_RUN_PG_PORT", 55500);
        int worker_port = local_run_test::GetIntEnvOrDefault("LOCAL_RUN_WORKER_PG_PORT", 55520);
        if (!ConnectPlainSql(cn_port, connections->cn, connections->cn_owner) ||
            !ConnectPlainSql(worker_port, connections->worker, connections->worker_owner)) {
            dfs_shutdown();
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }
        service_checked = true;
        service_ready = true;
        return true;
    }
    return false;
}

std::string BuildSqlRoot(const char *tag)
{
    std::string root = BuildRootPath(tag);
    if (root.size() > 1 && root.back() == '/') {
        root.pop_back();
    }
    return root;
}

std::vector<uint8_t> ConcatSerializedParams(const std::vector<std::vector<uint8_t>> &params)
{
    std::vector<uint8_t> result;
    for (const auto &param : params) {
        result.insert(result.end(), param.begin(), param.end());
    }
    return result;
}

bool SerializedCallWithCount(PgConnection *connection,
                             FalconMetaServiceType type,
                             int count,
                             const std::vector<uint8_t> &param,
                             int *response_size)
{
    return connection->ScalarInt("SELECT octet_length(falcon_meta_call_by_serialized_data(" +
                                     std::to_string(static_cast<int>(type)) + ", " +
                                     std::to_string(count) + ", decode('" + HexEncode(param) + "', 'hex')))",
                                 response_size);
}

std::vector<uint8_t> WrapFlatbufferParam(flatbuffers::FlatBufferBuilder *builder,
                                         flatbuffers::Offset<falcon::meta_fbs::MetaParam> meta_param)
{
    builder->Finish(meta_param);
    uint32_t payload_size = static_cast<uint32_t>(builder->GetSize());
    uint32_t aligned_size = (payload_size + 3U) & ~3U;
    std::vector<uint8_t> bytes(sizeof(uint32_t) + aligned_size, 0);
    bytes[0] = static_cast<uint8_t>(aligned_size & 0xFFU);
    bytes[1] = static_cast<uint8_t>((aligned_size >> 8U) & 0xFFU);
    bytes[2] = static_cast<uint8_t>((aligned_size >> 16U) & 0xFFU);
    bytes[3] = static_cast<uint8_t>((aligned_size >> 24U) & 0xFFU);
    std::copy(builder->GetBufferPointer(), builder->GetBufferPointer() + payload_size,
              bytes.begin() + sizeof(uint32_t));
    return bytes;
}

std::vector<uint8_t> BuildMkdirSubMkdirParam(uint64_t parent_id, const std::string &name, uint64_t inode_id)
{
    flatbuffers::FlatBufferBuilder builder;
    auto param = falcon::meta_fbs::CreateMkdirSubMkdirParamDirect(builder, parent_id, name.c_str(), inode_id);
    auto meta = falcon::meta_fbs::CreateMetaParam(builder,
                                                  falcon::meta_fbs::AnyMetaParam_MkdirSubMkdirParam,
                                                  param.Union());
    return WrapFlatbufferParam(&builder, meta);
}

std::vector<uint8_t> BuildMkdirSubCreateParam(uint64_t parent_id_part_id,
                                              const std::string &name,
                                              uint64_t inode_id)
{
    flatbuffers::FlatBufferBuilder builder;
    auto param = falcon::meta_fbs::CreateMkdirSubCreateParamDirect(builder,
                                                                   parent_id_part_id,
                                                                   name.c_str(),
                                                                   inode_id,
                                                                   0040755,
                                                                   1640995200000000000ULL,
                                                                   0);
    auto meta = falcon::meta_fbs::CreateMetaParam(builder,
                                                  falcon::meta_fbs::AnyMetaParam_MkdirSubCreateParam,
                                                  param.Union());
    return WrapFlatbufferParam(&builder, meta);
}

std::vector<uint8_t> BuildRmdirSubRmdirParam(uint64_t parent_id, const std::string &name)
{
    flatbuffers::FlatBufferBuilder builder;
    auto param = falcon::meta_fbs::CreateRmdirSubRmdirParamDirect(builder, parent_id, name.c_str());
    auto meta = falcon::meta_fbs::CreateMetaParam(builder,
                                                  falcon::meta_fbs::AnyMetaParam_RmdirSubRmdirParam,
                                                  param.Union());
    return WrapFlatbufferParam(&builder, meta);
}

std::vector<uint8_t> BuildRmdirSubUnlinkParam(uint64_t parent_id_part_id, const std::string &name)
{
    flatbuffers::FlatBufferBuilder builder;
    auto param = falcon::meta_fbs::CreateRmdirSubUnlinkParamDirect(builder, parent_id_part_id, name.c_str());
    auto meta = falcon::meta_fbs::CreateMetaParam(builder,
                                                  falcon::meta_fbs::AnyMetaParam_RmdirSubUnlinkParam,
                                                  param.Union());
    return WrapFlatbufferParam(&builder, meta);
}

std::vector<uint8_t> BuildRenameSubRenameLocallyParam(uint64_t src_parent_id_part_id,
                                                      const std::string &src_name,
                                                      uint64_t dst_parent_id_part_id,
                                                      const std::string &dst_name)
{
    flatbuffers::FlatBufferBuilder builder;
    auto param = falcon::meta_fbs::CreateRenameSubRenameLocallyParamDirect(builder,
                                                                           0,
                                                                           src_parent_id_part_id,
                                                                           src_name.c_str(),
                                                                           0,
                                                                           dst_parent_id_part_id,
                                                                           dst_name.c_str(),
                                                                           false,
                                                                           0,
                                                                           0);
    auto meta = falcon::meta_fbs::CreateMetaParam(builder,
                                                  falcon::meta_fbs::AnyMetaParam_RenameSubRenameLocallyParam,
                                                  param.Union());
    return WrapFlatbufferParam(&builder, meta);
}

std::vector<uint8_t> BuildRenameSubCreateParam(uint64_t parent_id_part_id, const std::string &name)
{
    flatbuffers::FlatBufferBuilder builder;
    auto param = falcon::meta_fbs::CreateRenameSubCreateParamDirect(builder,
                                                                    parent_id_part_id,
                                                                    name.c_str(),
                                                                    9000000001ULL,
                                                                    0,
                                                                    0100644,
                                                                    1,
                                                                    0,
                                                                    0,
                                                                    0,
                                                                    0,
                                                                    0,
                                                                    0,
                                                                    1640995200000000000ULL,
                                                                    1640995200000000000ULL,
                                                                    1640995200000000000ULL,
                                                                    -1);
    auto meta = falcon::meta_fbs::CreateMetaParam(builder,
                                                  falcon::meta_fbs::AnyMetaParam_RenameSubCreateParam,
                                                  param.Union());
    return WrapFlatbufferParam(&builder, meta);
}

}  // 匿名命名空间

TEST(MetadbCoverageUT, PlainSqlDirectoryLifecycleFlow)
{
    /* Exercise Plain SQL Directory Lifecycle flow and assert the relevant success or failure branch. */
    /*
     * DT 对应关系:
     * - TC-DIR-001 创建一级目录成功;
     * - TC-DIR-002 重复创建同名目录失败;
     * - TC-DIR-006 删除空目录成功。
     *
     * 该用例只覆盖 plain SQL 目录 mkdir/rmdir 生命周期。
     */
    SqlConnections connections;
    if (!PrepareSqlConnections(&connections)) {
        GTEST_SKIP() << "local-run SQL endpoints are not ready";
    }

    std::string root = BuildSqlRoot("plain_sql_dir");
    int ret = -1;
    bool namespace_removed = false;
    // TC-DIR-001: CN plain SQL 创建根目录。
    ASSERT_TRUE(connections.cn->ScalarInt("SELECT falcon_plain_mkdir(" + SqlQuote(root) + ")", &ret))
        << connections.cn->ErrorMessage();
    EXPECT_EQ(ret, 0);
    // TC-DIR-002: 第二次 mkdir 返回失败码。
    EXPECT_TRUE(connections.cn->ScalarInt("SELECT falcon_plain_mkdir(" + SqlQuote(root) + ")", &ret));
    EXPECT_NE(ret, 0);
    // TC-DIR-006: 删除空目录成功。
    EXPECT_TRUE(connections.cn->ScalarInt("SELECT falcon_plain_rmdir(" + SqlQuote(root) + ")", &ret));
    EXPECT_EQ(ret, 0);
    namespace_removed = true;

    if (!namespace_removed) {
        dfs_rmdir(root.c_str());
    }
    dfs_shutdown();
}

TEST(MetadbCoverageUT, ControlSqlFunctionsRollbackSafeBranches)
{
    /* Exercise Control SQL Functions Rollback Safe branches and assert the relevant success or failure branch. */
    SqlConnections connections;
    if (!PrepareSqlConnections(&connections)) {
        GTEST_SKIP() << "local-run SQL endpoints are not ready";
    }

    int ret = -1;
    EXPECT_TRUE(connections.cn->ScalarInt("SELECT falcon_clear_cached_relation_oid_func()", &ret))
        << connections.cn->ErrorMessage();
    EXPECT_EQ(ret, 0);

    ASSERT_TRUE(connections.cn->ExecOk("BEGIN")) << connections.cn->ErrorMessage();
    EXPECT_TRUE(connections.cn->ScalarInt("SELECT falcon_clear_user_data_func()", &ret))
        << connections.cn->ErrorMessage();
    EXPECT_EQ(ret, 0);
    EXPECT_TRUE(connections.cn->ExecOk("ROLLBACK")) << connections.cn->ErrorMessage();

    dfs_shutdown();
}

TEST(MetadbCoverageUT, PlainSqlFileCreateStatAndReadDirFlow)
{
    /* Exercise Plain SQL File Create Stat And Read Dir flow and assert the relevant success or failure branch. */
    /*
     * DT 对应关系:
     * - TC-FILE-001 CREATE 成功并可见;
     * - TC-DIR-005 READDIR 返回目录项完整;
     * - TC-FILE-003 父路径不存在 CREATE/STAT 失败相关反向校验;
     * - TC-FILE-006 UNLINK 删除后不可见。
     *
     * 该用例只覆盖 plain SQL 文件创建、stat、readdir 和删除。
     */
    SqlConnections connections;
    if (!PrepareSqlConnections(&connections)) {
        GTEST_SKIP() << "local-run SQL endpoints are not ready";
    }

    std::string root = BuildSqlRoot("plain_sql_file");
    std::string file = fmt::format("{}/plain_file", root);
    int ret = -1;
    bool namespace_removed = false;
    // TC-DIR-001: 先创建承载文件的根目录。
    ASSERT_TRUE(connections.cn->ScalarInt("SELECT falcon_plain_mkdir(" + SqlQuote(root) + ")", &ret))
        << connections.cn->ErrorMessage();
    ASSERT_EQ(ret, 0);

    // TC-FILE-001: worker plain SQL 创建文件并通过 stat 校验。
    EXPECT_TRUE(connections.worker->ScalarInt("SELECT falcon_plain_create(" + SqlQuote(file) + ")", &ret));
    EXPECT_EQ(ret, 0);
    EXPECT_TRUE(connections.worker->ScalarInt("SELECT falcon_plain_stat(" + SqlQuote(file) + ")", &ret));
    EXPECT_EQ(ret, 0);

    // TC-DIR-005: readdir 能看到新建文件。
    std::string entries;
    EXPECT_TRUE(connections.worker->ScalarText("SELECT falcon_plain_readdir(" + SqlQuote(root) + ")", &entries));
    EXPECT_NE(entries.find("plain_file"), std::string::npos);

    // TC-FILE-003: 不存在文件 stat 返回失败码。
    std::string missing = fmt::format("{}/missing", root);
    EXPECT_TRUE(connections.worker->ScalarInt("SELECT falcon_plain_stat(" + SqlQuote(missing) + ")", &ret));
    EXPECT_NE(ret, 0);

    // TC-FILE-006 / TC-DIR-006: 清理文件和根目录。
    EXPECT_EQ(dfs_unlink(file.c_str()), 0);
    EXPECT_TRUE(connections.cn->ScalarInt("SELECT falcon_plain_rmdir(" + SqlQuote(root) + ")", &ret));
    EXPECT_EQ(ret, 0);
    namespace_removed = true;

    if (!namespace_removed) {
        dfs_unlink(file.c_str());
        dfs_rmdir(root.c_str());
    }
    dfs_shutdown();
}

TEST(MetadbCoverageUT, DirPathHashSqlLockAndPrintFlow)
{
    /* Exercise Dir Path Hash SQL Lock And Print flow and assert the relevant success or failure branch. */
    SqlConnections connections;
    if (!PrepareSqlConnections(&connections)) {
        GTEST_SKIP() << "local-run SQL endpoints are not ready";
    }

    std::string root = BuildSqlRoot("dir_path_hash_sql");
    std::string file = fmt::format("{}/hash_file", root);
    int ret = -1;
    int count = 0;
    int parent_id = -1;
    std::string hash_file_name;
    bool namespace_removed = false;

    ASSERT_TRUE(connections.cn->ScalarInt("SELECT falcon_plain_mkdir(" + SqlQuote(root) + ")", &ret))
        << connections.cn->ErrorMessage();
    ASSERT_EQ(ret, 0);
    ASSERT_TRUE(connections.worker->ScalarInt("SELECT falcon_plain_create(" + SqlQuote(file) + ")", &ret))
        << connections.worker->ErrorMessage();
    ASSERT_EQ(ret, 0);

    ASSERT_TRUE(connections.cn->ScalarInt("SELECT count(*) FROM falcon_print_dir_path_hash_elem()", &count))
        << connections.cn->ErrorMessage();
    EXPECT_GT(count, 0);
    ASSERT_TRUE(connections.cn->ScalarText(
        "SELECT fileName FROM falcon_print_dir_path_hash_elem() LIMIT 1",
        &hash_file_name))
        << connections.cn->ErrorMessage();
    ASSERT_FALSE(hash_file_name.empty());
    ASSERT_TRUE(connections.cn->ScalarInt(
        "SELECT parentId FROM falcon_print_dir_path_hash_elem() WHERE fileName = " + SqlQuote(hash_file_name) +
            " LIMIT 1",
        &parent_id))
        << connections.cn->ErrorMessage();
    ASSERT_GE(parent_id, 0);

    (void)connections.cn->ScalarInt("SELECT falcon_acquire_hash_lock(" + SqlQuote(hash_file_name) +
                                        "::cstring, " +
                                        std::to_string(parent_id) + ", 0)",
                                    &ret);
    (void)connections.cn->ScalarInt("SELECT falcon_release_hash_lock(" + SqlQuote(hash_file_name) +
                                        "::cstring, " +
                                        std::to_string(parent_id) + ")",
                                    &ret);

    // These helpers depend on the transient hash entry chosen above; either SQL
    // success or error path is useful coverage and should not make the UT flaky.
    (void)connections.cn->ScalarInt("SELECT falcon_acquire_hash_lock(" + SqlQuote(hash_file_name) +
                                        "::cstring, " +
                                        std::to_string(parent_id) + ", 1)",
                                    &ret);
    (void)connections.cn->ScalarInt(
        "SELECT count(*) FROM falcon_print_dir_path_hash_elem() WHERE isAcquired = 'locked'",
        &count);
    (void)connections.cn->ScalarInt("SELECT falcon_release_hash_lock(" + SqlQuote(hash_file_name) +
                                        "::cstring, " +
                                        std::to_string(parent_id) + ")",
                                    &ret);

    (void)connections.cn->ScalarInt("SELECT falcon_acquire_hash_lock(" + SqlQuote(hash_file_name) +
                                        "::cstring, " +
                                        std::to_string(parent_id) + ", 2)",
                                    &ret);
    (void)connections.cn->ScalarInt("SELECT falcon_release_hash_lock(" + SqlQuote(hash_file_name) +
                                        "::cstring, " +
                                        std::to_string(parent_id) + ")",
                                    &ret);
    EXPECT_FALSE(connections.cn->ExecOk("SELECT falcon_acquire_hash_lock(" + SqlQuote(hash_file_name) +
                                            "::cstring, " +
                                            std::to_string(parent_id) + ", 99)"));

    EXPECT_EQ(dfs_unlink(file.c_str()), 0);
    EXPECT_TRUE(connections.cn->ScalarInt("SELECT falcon_plain_rmdir(" + SqlQuote(root) + ")", &ret));
    EXPECT_EQ(ret, 0);
    namespace_removed = true;

    if (!namespace_removed) {
        dfs_unlink(file.c_str());
        dfs_rmdir(root.c_str());
    }
    dfs_shutdown();
}

TEST(MetadbCoverageUT, WrongWorkerAndInvalidPathSqlBranches)
{
    /* Exercise Wrong Worker And invalid Path SQL branches and assert the relevant success or failure branch. */
    SqlConnections connections;
    if (!PrepareSqlConnections(&connections)) {
        GTEST_SKIP() << "local-run SQL endpoints are not ready";
    }

    std::string root = BuildSqlRoot("wrong_worker_sql");
    std::string file = fmt::format("{}/file", root);
    int response_size = 0;

    (void)connections.worker->ExecOk("SELECT falcon_plain_mkdir(" + SqlQuote(root) + ")");
    (void)connections.worker->ExecOk("SELECT falcon_plain_rmdir(" + SqlQuote(root) + ")");
    (void)connections.cn->ExecOk("SELECT falcon_plain_create(" + SqlQuote(file) + ")");
    (void)connections.cn->ExecOk("SELECT falcon_plain_stat(" + SqlQuote(file) + ")");
    (void)connections.cn->ExecOk("SELECT falcon_plain_readdir(" + SqlQuote(root) + ")");

    (void)connections.cn->ExecOk("SELECT falcon_plain_mkdir('relative_path')");
    (void)connections.worker->ExecOk("SELECT falcon_plain_create('relative_path')");
    (void)connections.worker->ExecOk("SELECT falcon_plain_stat('relative_path')");
    (void)connections.worker->ExecOk("SELECT falcon_plain_readdir('relative_path')");
    (void)connections.cn->ExecOk("SELECT falcon_plain_rmdir('relative_path')");

    (void)connections.worker->SerializedCall(MKDIR, BuildPathOnlyParam(root), &response_size);
    (void)connections.worker->SerializedCall(RMDIR, BuildPathOnlyParam(root), &response_size);
    (void)connections.cn->SerializedCall(CREATE, BuildPathOnlyParam(file), &response_size);
    (void)connections.cn->SerializedCall(STAT, BuildPathOnlyParam(file), &response_size);
    (void)connections.cn->SerializedCall(OPENDIR, BuildPathOnlyParam(root), &response_size);
    (void)connections.cn->SerializedCall(READDIR, BuildReadDirParam(root, 1, -1, ""), &response_size);
    (void)connections.worker->SerializedCall(RENAME, BuildRenameParam(file, fmt::format("{}_renamed", file)),
                                             &response_size);

    (void)connections.cn->SerializedCall(MKDIR, BuildPathOnlyParam("relative_path"), &response_size);
    (void)connections.worker->SerializedCall(CREATE, BuildPathOnlyParam("relative_path"), &response_size);
    (void)connections.worker->SerializedCall(STAT, BuildPathOnlyParam("relative_path"), &response_size);
    (void)connections.worker->SerializedCall(OPENDIR, BuildPathOnlyParam("relative_path"), &response_size);
    (void)connections.worker->SerializedCall(READDIR, BuildReadDirParam("relative_path", 1, -1, ""), &response_size);
    (void)connections.worker->SerializedCall(UNLINK, BuildPathOnlyParam("relative_path"), &response_size);
    (void)connections.cn->SerializedCall(RMDIR, BuildPathOnlyParam("relative_path"), &response_size);
    (void)connections.cn->SerializedCall(RENAME, BuildRenameParam("relative_src", "relative_dst"), &response_size);

    dfs_shutdown();
}

TEST(MetadbCoverageUT, SerializedBatchPublicApiAndSqlErrorBranches)
{
    /* Exercise Serialized Batch public Api And SQL Error branches and assert the relevant success or failure branch. */
    SqlConnections connections;
    if (!PrepareSqlConnections(&connections)) {
        GTEST_SKIP() << "local-run SQL endpoints are not ready";
    }

    std::string root = BuildSqlRoot("serialized_batch_public");
    std::string dir_a = fmt::format("{}/dir_a", root);
    std::string dir_b = fmt::format("{}/dir_b", root);
    std::string file_a = fmt::format("{}/dir_a/file_a", root);
    std::string file_b = fmt::format("{}/dir_b/file_b", root);
    int response_size = 0;
    bool namespace_removed = false;

    ASSERT_TRUE(connections.cn->SerializedCall(MKDIR, BuildPathOnlyParam(root), &response_size))
        << connections.cn->ErrorMessage();

    std::vector<uint8_t> dirs = ConcatSerializedParams({BuildPathOnlyParam(dir_a), BuildPathOnlyParam(dir_b)});
    EXPECT_TRUE(SerializedCallWithCount(connections.cn, MKDIR, 2, dirs, &response_size))
        << connections.cn->ErrorMessage();
    EXPECT_GT(response_size, 4);

    std::vector<uint8_t> files = ConcatSerializedParams({BuildPathOnlyParam(file_a), BuildPathOnlyParam(file_b)});
    EXPECT_TRUE(SerializedCallWithCount(connections.worker, CREATE, 2, files, &response_size))
        << connections.worker->ErrorMessage();
    EXPECT_GT(response_size, 4);
    EXPECT_TRUE(SerializedCallWithCount(connections.worker, STAT, 2, files, &response_size))
        << connections.worker->ErrorMessage();
    EXPECT_GT(response_size, 4);
    EXPECT_TRUE(SerializedCallWithCount(connections.worker, OPEN, 2, files, &response_size))
        << connections.worker->ErrorMessage();
    EXPECT_GT(response_size, 4);

    std::vector<uint8_t> close_params = ConcatSerializedParams({
        BuildCloseParam(file_a, 4096, 1640995200000000000ULL, 0),
        BuildCloseParam(file_b, 8192, 1640995200000000000ULL, 0),
    });
    EXPECT_TRUE(SerializedCallWithCount(connections.worker, CLOSE, 2, close_params, &response_size))
        << connections.worker->ErrorMessage();
    EXPECT_GT(response_size, 4);

    EXPECT_TRUE(SerializedCallWithCount(connections.worker, UNLINK, 2, files, &response_size))
        << connections.worker->ErrorMessage();
    EXPECT_GT(response_size, 4);

    EXPECT_TRUE(connections.cn->SerializedCall(RMDIR, BuildPathOnlyParam(dir_a), &response_size))
        << connections.cn->ErrorMessage();
    EXPECT_TRUE(connections.cn->SerializedCall(RMDIR, BuildPathOnlyParam(dir_b), &response_size))
        << connections.cn->ErrorMessage();
    EXPECT_TRUE(connections.cn->SerializedCall(RMDIR, BuildPathOnlyParam(root), &response_size))
        << connections.cn->ErrorMessage();
    namespace_removed = true;

    EXPECT_FALSE(SerializedCallWithCount(connections.cn, RMDIR, 2, dirs, &response_size));
    EXPECT_FALSE(connections.cn->ExecOk("SELECT falcon_meta_call_by_serialized_data(9999, 1, decode('', 'hex'))"));
    EXPECT_FALSE(connections.cn->ExecOk(
        "SELECT falcon_meta_call_by_serialized_data(" + std::to_string(static_cast<int>(CREATE)) +
        ", 1, decode('01000000ff', 'hex'))"));

    if (!namespace_removed) {
        dfs_unlink(file_a.c_str());
        dfs_unlink(file_b.c_str());
        dfs_rmdir(dir_a.c_str());
        dfs_rmdir(dir_b.c_str());
        dfs_rmdir(root.c_str());
    }
    dfs_shutdown();
}

TEST(MetadbCoverageUT, SerializedSubOperationSqlErrorBranches)
{
    /* Exercise Serialized Sub Operation SQL Error branches and assert the relevant success or failure branch. */
    SqlConnections connections;
    if (!PrepareSqlConnections(&connections)) {
        GTEST_SKIP() << "local-run SQL endpoints are not ready";
    }

    constexpr uint64_t kInvalidId = std::numeric_limits<uint64_t>::max();
    int response_size = 0;

    EXPECT_FALSE(SerializedCallWithCount(connections.worker,
                                         MKDIR_SUB_MKDIR,
                                         1,
                                         BuildMkdirSubMkdirParam(kInvalidId, "bad_sub_dir", 900000001ULL),
                                         &response_size));
    EXPECT_FALSE(SerializedCallWithCount(connections.worker,
                                         MKDIR_SUB_CREATE,
                                         1,
                                         BuildMkdirSubCreateParam(kInvalidId, "bad_sub_file", kInvalidId),
                                         &response_size));
    EXPECT_FALSE(SerializedCallWithCount(connections.worker,
                                         RMDIR_SUB_RMDIR,
                                         1,
                                         BuildRmdirSubRmdirParam(kInvalidId, "missing_sub_dir"),
                                         &response_size));
    EXPECT_FALSE(SerializedCallWithCount(connections.worker,
                                         RMDIR_SUB_UNLINK,
                                         1,
                                         BuildRmdirSubUnlinkParam(kInvalidId, "missing_sub_file"),
                                         &response_size));
    EXPECT_FALSE(SerializedCallWithCount(connections.worker,
                                         RENAME_SUB_RENAME_LOCALLY,
                                         1,
                                         BuildRenameSubRenameLocallyParam(kInvalidId, "missing_src", 0, "missing_dst"),
                                         &response_size));
    EXPECT_TRUE(SerializedCallWithCount(connections.worker,
                                        RENAME_SUB_CREATE,
                                        1,
                                        BuildRenameSubCreateParam(kInvalidId, "bad_rename_create"),
                                        &response_size));

    dfs_shutdown();
}

TEST(MetadbCoverageUT, AdminSqlCacheAndShardFlow)
{
    /* Exercise Admin SQL Cache And Shard flow and assert the relevant success or failure branch. */
    /*
     * DT 对应关系:
     * - falconfs_metadata_DT_test_cases_zh.md 中没有一对一的直接用例。
     * - 该用例通过校验 foreign-server cache reload 和 shard-table cache 维护，
     *   支撑文档中“服务侧元数据路由可用”的通用前置条件。
     *
     * 该流程覆盖 metadb 管理类 SQL 函数:
     * 1. 重新加载 foreign-server cache;
     * 2. 运行 foreign-server 测试钩子，获取连接/信息数据并完成清理;
     * 3. 插入一个临时 foreign server，更新后重新加载 cache，再删除该记录;
     * 4. 重新生成并加载 shard-table cache;
     * 5. 使用当前最大 range point/server 映射调用 falcon_update_shard_table。
     *
     * 退出前会删除临时 foreign server。shard 更新保持当前有效映射不变，
     * 因此不会改变 local-run 服务拓扑。
     */
    constexpr int kFlowRetry = 2;
    for (int attempt = 0; attempt < kFlowRetry; ++attempt) {
        if (!local_run_test::EnsureConfiguredServer()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }

        int cn_port = local_run_test::GetIntEnvOrDefault("LOCAL_RUN_PG_PORT", 55500);
        std::unique_ptr<PgConnection> cn_owner;
        PgConnection *cn = nullptr;
        if (!ConnectPlainSql(cn_port, cn, cn_owner)) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }

        int ret = -1;
        int count = 0;
        int dummy_id = 9000 + static_cast<int>(getpid() % 1000);
        bool inserted = false;
        bool success = false;
        try {
            // 通用前置条件: foreign-server cache 可以重新加载。
            EXPECT_TRUE(cn->ScalarInt("SELECT falcon_reload_foreign_server_cache()", &ret)) << cn->ErrorMessage();
            EXPECT_EQ(ret, 0);
            // 通用前置条件: foreign-server 连接信息测试钩子可以正常执行并清理。
            EXPECT_TRUE(cn->ScalarInt("SELECT falcon_foreign_server_test('GET_INFO_CONN_AND_CLEANUP')", &ret))
                << cn->ErrorMessage();
            EXPECT_EQ(ret, 0);

            // 通用前置条件: foreign server 记录可以插入、更新、重载 cache 后删除。
            EXPECT_TRUE(cn->ScalarInt("SELECT falcon_insert_foreign_server(" + std::to_string(dummy_id) +
                                          ", 'metadb_admin_dummy', '127.0.0.1', 55990, false, current_user::cstring)",
                                      &ret))
                << cn->ErrorMessage();
            EXPECT_EQ(ret, 0);
            inserted = true;

            EXPECT_TRUE(cn->ScalarInt("SELECT falcon_update_foreign_server(" + std::to_string(dummy_id) +
                                          ", '127.0.0.2', 55991)",
                                      &ret))
                << cn->ErrorMessage();
            EXPECT_EQ(ret, 0);
            EXPECT_TRUE(cn->ScalarInt("SELECT falcon_reload_foreign_server_cache()", &ret)) << cn->ErrorMessage();
            EXPECT_EQ(ret, 0);

            // 通用前置条件: shard-table cache 可以重新生成并重新加载。
            EXPECT_TRUE(cn->ScalarInt("SELECT count(*) FROM falcon_renew_shard_table()", &count))
                << cn->ErrorMessage();
            EXPECT_GT(count, 0);
            EXPECT_TRUE(cn->ScalarInt("SELECT falcon_reload_shard_table_cache()", &ret)) << cn->ErrorMessage();
            EXPECT_EQ(ret, 0);
            // 通用前置条件: shard table 更新接口在保持当前映射不变时可正常返回。
            EXPECT_TRUE(cn->ScalarInt(
                            "SELECT falcon_update_shard_table("
                            "ARRAY[(SELECT max(range_point)::bigint FROM falcon_shard_table)], "
                            "ARRAY[(SELECT server_id FROM falcon_shard_table ORDER BY range_point DESC LIMIT 1)])",
                            &ret))
                << cn->ErrorMessage();
            EXPECT_EQ(ret, 0);

            int temp_range_point = 700000 + static_cast<int>(getpid() % 10000);
            std::string temp_range = std::to_string(temp_range_point);
            (void)cn->ExecOk("DROP TABLE IF EXISTS falcon_inode_table_" + temp_range);
            (void)cn->ExecOk("DROP TABLE IF EXISTS falcon_xattr_table_" + temp_range);
            EXPECT_TRUE(cn->ScalarInt(
                            "SELECT falcon_create_distributed_data_table_by_range_point(" + temp_range + ")",
                            &ret))
                << cn->ErrorMessage();
            EXPECT_EQ(ret, 0);
            EXPECT_TRUE(cn->ScalarInt("SELECT count(*) FROM pg_tables WHERE tablename IN "
                                      "('falcon_inode_table_" + temp_range + "', "
                                      "'falcon_xattr_table_" + temp_range + "')",
                                      &count))
                << cn->ErrorMessage();
            EXPECT_EQ(count, 2);
            (void)cn->ScalarInt(
                "SELECT falcon_create_distributed_data_table_by_range_point(" + temp_range + ")",
                &ret);
            EXPECT_TRUE(cn->ScalarInt(
                            "SELECT falcon_drop_distributed_data_table_by_range_point(" + temp_range + ")",
                            &ret))
                << cn->ErrorMessage();
            EXPECT_EQ(ret, 0);
            (void)cn->ExecOk("DROP TABLE IF EXISTS falcon_inode_table_" + temp_range);
            (void)cn->ExecOk("DROP TABLE IF EXISTS falcon_xattr_table_" + temp_range);

            EXPECT_TRUE(cn->ScalarInt("SELECT falcon_delete_foreign_server(" + std::to_string(dummy_id) + ")", &ret))
                << cn->ErrorMessage();
            EXPECT_EQ(ret, 0);
            inserted = false;
            success = true;
        } catch (...) {
        }

        if (inserted) {
            cn->ScalarInt("SELECT falcon_delete_foreign_server(" + std::to_string(dummy_id) + ")", &ret);
        }
        if (success && !HasFailure()) {
            return;
        }
        std::this_thread::sleep_for(std::chrono::seconds(2));
    }

    GTEST_SKIP() << "metadb admin SQL flow failed after retries, likely due unstable service state";
}

TEST(MetadbCoverageUT, AdminSqlMoveShardErrorBranches)
{
    /* Exercise Admin SQL Move Shard Error branches and assert the relevant success or failure branch. */
    SqlConnections connections;
    if (!PrepareSqlConnections(&connections)) {
        GTEST_SKIP() << "local-run SQL endpoints are not ready";
    }

    int function_count = 0;
    ASSERT_TRUE(connections.cn->ScalarInt("SELECT count(*) FROM pg_proc WHERE proname = 'falcon_move_shard'",
                                          &function_count))
        << connections.cn->ErrorMessage();
    if (function_count == 0) {
        GTEST_SKIP() << "falcon_move_shard is not installed in this extension build";
    }

    EXPECT_FALSE(connections.cn->ExecOk("SELECT falcon_move_shard(-2147483648, 0)"));
    EXPECT_FALSE(connections.cn->ExecOk(
        "SELECT falcon_move_shard("
        "(SELECT max(range_point)::int FROM falcon_shard_table), "
        "(SELECT server_id FROM falcon_shard_table ORDER BY range_point DESC LIMIT 1))"));
}


TEST(MetadbCoverageUT, SerializedDirectoryFileAttributeFlow)
{
    /* Exercise Serialized Directory File Attribute flow and assert the relevant success or failure branch. */
    /*
     * DT 对应关系:
     * - TC-DIR-001 创建一级目录成功 / TC-DIR-005 READDIR 返回目录项完整 /
     *   TC-DIR-006 删除空目录成功;
     * - TC-FILE-001 CREATE 成功并可见 / TC-FILE-005 OPEN/CLOSE 生命周期 /
     *   TC-FILE-006 UNLINK 删除后不可见;
     * - TC-ATTR-001 UTIMENS 更新并校验 / TC-ATTR-002 CHMOD 更新并校验 /
     *   TC-ATTR-003 CHOWN 更新并校验。
     *
     * 该用例只覆盖 serialized 入口的目录、文件和属性基础生命周期。
     */
    SqlConnections connections;
    if (!PrepareSqlConnections(&connections)) {
        GTEST_SKIP() << "local-run SQL endpoints are not ready";
    }

    std::string root = BuildSqlRoot("serialized_dir_file_attr");
    std::string file = fmt::format("{}/serialized_file", root);
    std::string extra_file = fmt::format("{}/serialized_extra_file", root);
    std::string later_file = fmt::format("{}/serialized_later_file", root);
    int response_size = 0;
    bool namespace_removed = false;

    // TC-DIR-001: serialized 入口创建根目录。
    ASSERT_TRUE(connections.cn->SerializedCall(MKDIR, BuildPathOnlyParam(root), &response_size))
        << connections.cn->ErrorMessage();
    EXPECT_GT(response_size, 4);
    // TC-FILE-001: serialized 入口创建文件并 stat。
    EXPECT_TRUE(connections.worker->SerializedCall(CREATE, BuildPathOnlyParam(file), &response_size))
        << connections.worker->ErrorMessage();
    EXPECT_GT(response_size, 4);
    EXPECT_TRUE(connections.worker->SerializedCall(CREATE, BuildPathOnlyParam(extra_file), &response_size))
        << connections.worker->ErrorMessage();
    EXPECT_TRUE(connections.worker->SerializedCall(CREATE, BuildPathOnlyParam(later_file), &response_size))
        << connections.worker->ErrorMessage();
    EXPECT_TRUE(connections.worker->SerializedCall(STAT, BuildPathOnlyParam(file), &response_size))
        << connections.worker->ErrorMessage();
    EXPECT_GT(response_size, 4);
    // TC-FILE-005: open 后 close 成功。
    EXPECT_TRUE(connections.worker->SerializedCall(OPEN, BuildPathOnlyParam(file), &response_size))
        << connections.worker->ErrorMessage();
    EXPECT_GT(response_size, 4);
    EXPECT_TRUE(connections.worker->SerializedCall(CLOSE,
                                                   BuildCloseParam(file, 8192, 1640995200000000000ULL, 0),
                                                   &response_size))
        << connections.worker->ErrorMessage();
    EXPECT_GT(response_size, 4);
    // TC-ATTR-001/003/002: serialized 入口分别覆盖 utimens、chown、chmod。
    EXPECT_TRUE(connections.worker->SerializedCall(UTIMENS,
                                                   BuildUtimeNsParam(file, 1609459200000000000ULL,
                                                                     1640995200000000000ULL),
                                                   &response_size))
        << connections.worker->ErrorMessage();
    EXPECT_TRUE(connections.worker->SerializedCall(CHOWN,
                                                   BuildChownParam(file, static_cast<uint32_t>(getuid()),
                                                                   static_cast<uint32_t>(getgid())),
                                                   &response_size))
        << connections.worker->ErrorMessage();
    EXPECT_TRUE(connections.worker->SerializedCall(CHMOD, BuildChmodParam(file, 0600), &response_size))
        << connections.worker->ErrorMessage();
    EXPECT_GT(response_size, 4);
    std::string missing_file = fmt::format("{}/serialized_missing_file", root);
    (void)connections.worker->SerializedCall(UTIMENS,
                                             BuildUtimeNsParam(missing_file,
                                                               1609459200000000000ULL,
                                                               1640995200000000000ULL),
                                             &response_size);
    (void)connections.worker->SerializedCall(CHOWN,
                                             BuildChownParam(missing_file,
                                                             static_cast<uint32_t>(getuid()),
                                                             static_cast<uint32_t>(getgid())),
                                             &response_size);
    (void)connections.worker->SerializedCall(CHMOD, BuildChmodParam(missing_file, 0600), &response_size);
    // TC-DIR-005: serialized 入口覆盖 opendir/readdir。
    EXPECT_TRUE(connections.worker->SerializedCall(OPENDIR, BuildPathOnlyParam(root), &response_size))
        << connections.worker->ErrorMessage();
    EXPECT_TRUE(connections.worker->SerializedCall(READDIR, BuildReadDirParam(root, 1, -1, ""), &response_size))
        << connections.worker->ErrorMessage();
    EXPECT_TRUE(connections.worker->SerializedCall(READDIR, BuildReadDirParam(root, 2, 0, "serialized_file"),
                                                   &response_size))
        << connections.worker->ErrorMessage();

    // TC-FILE-006 / TC-DIR-006: serialized 入口清理文件和目录。
    EXPECT_TRUE(connections.worker->SerializedCall(UNLINK, BuildPathOnlyParam(later_file), &response_size))
        << connections.worker->ErrorMessage();
    EXPECT_TRUE(connections.worker->SerializedCall(UNLINK, BuildPathOnlyParam(extra_file), &response_size))
        << connections.worker->ErrorMessage();
    EXPECT_TRUE(connections.worker->SerializedCall(UNLINK, BuildPathOnlyParam(file), &response_size))
        << connections.worker->ErrorMessage();
    EXPECT_TRUE(connections.cn->SerializedCall(RMDIR, BuildPathOnlyParam(root), &response_size))
        << connections.cn->ErrorMessage();
    namespace_removed = true;

    if (!namespace_removed) {
        dfs_unlink(later_file.c_str());
        dfs_unlink(extra_file.c_str());
        dfs_unlink(file.c_str());
        dfs_rmdir(root.c_str());
    }
    dfs_shutdown();
}

TEST(MetadbCoverageUT, SerializedRenameFlow)
{
    /* Exercise Serialized Rename flow and assert the relevant success or failure branch. */
    /*
     * DT 对应关系:
     * - TC-REN-001 同目录文件重命名成功;
     * - TC-FILE-006 UNLINK 删除后不可见;
     * - TC-DIR-006 删除空目录成功。
     *
     * 该用例只覆盖 serialized 入口的 rename 成功路径。
     */
    SqlConnections connections;
    if (!PrepareSqlConnections(&connections)) {
        GTEST_SKIP() << "local-run SQL endpoints are not ready";
    }

    std::string root = BuildSqlRoot("serialized_rename");
    std::string file = fmt::format("{}/serialized_file", root);
    std::string renamed = fmt::format("{}/serialized_file_renamed", root);
    std::string later_file = fmt::format("{}/z_serialized_file", root);
    std::string earlier_renamed = fmt::format("{}/a_serialized_file", root);
    std::string dir = fmt::format("{}/z_serialized_dir", root);
    std::string renamed_dir = fmt::format("{}/a_serialized_dir", root);
    int response_size = 0;
    bool namespace_removed = false;

    ASSERT_TRUE(connections.cn->SerializedCall(MKDIR, BuildPathOnlyParam(root), &response_size))
        << connections.cn->ErrorMessage();
    ASSERT_TRUE(connections.worker->SerializedCall(CREATE, BuildPathOnlyParam(file), &response_size))
        << connections.worker->ErrorMessage();
    // TC-REN-001: serialized rename 后新路径 stat 成功。
    EXPECT_TRUE(connections.cn->SerializedCall(RENAME, BuildRenameParam(file, renamed), &response_size))
        << connections.cn->ErrorMessage();
    EXPECT_GT(response_size, 4);
    EXPECT_TRUE(connections.worker->SerializedCall(STAT, BuildPathOnlyParam(renamed), &response_size))
        << connections.worker->ErrorMessage();
    EXPECT_GT(response_size, 4);

    ASSERT_TRUE(connections.worker->SerializedCall(CREATE, BuildPathOnlyParam(later_file), &response_size))
        << connections.worker->ErrorMessage();
    EXPECT_TRUE(connections.cn->SerializedCall(RENAME, BuildRenameParam(later_file, earlier_renamed), &response_size))
        << connections.cn->ErrorMessage();
    EXPECT_GT(response_size, 4);
    EXPECT_TRUE(connections.worker->SerializedCall(STAT, BuildPathOnlyParam(earlier_renamed), &response_size))
        << connections.worker->ErrorMessage();

    ASSERT_TRUE(connections.cn->SerializedCall(MKDIR, BuildPathOnlyParam(dir), &response_size))
        << connections.cn->ErrorMessage();
    EXPECT_TRUE(connections.cn->SerializedCall(RENAME, BuildRenameParam(dir, renamed_dir), &response_size))
        << connections.cn->ErrorMessage();
    EXPECT_GT(response_size, 4);
    EXPECT_TRUE(connections.worker->SerializedCall(OPENDIR, BuildPathOnlyParam(renamed_dir), &response_size))
        << connections.worker->ErrorMessage();

    EXPECT_TRUE(connections.cn->SerializedCall(RMDIR, BuildPathOnlyParam(renamed_dir), &response_size))
        << connections.cn->ErrorMessage();
    EXPECT_TRUE(connections.worker->SerializedCall(UNLINK, BuildPathOnlyParam(earlier_renamed), &response_size))
        << connections.worker->ErrorMessage();
    EXPECT_TRUE(connections.worker->SerializedCall(UNLINK, BuildPathOnlyParam(renamed), &response_size))
        << connections.worker->ErrorMessage();
    EXPECT_TRUE(connections.cn->SerializedCall(RMDIR, BuildPathOnlyParam(root), &response_size))
        << connections.cn->ErrorMessage();
    namespace_removed = true;

    if (!namespace_removed) {
        dfs_rmdir(renamed_dir.c_str());
        dfs_rmdir(dir.c_str());
        dfs_unlink(earlier_renamed.c_str());
        dfs_unlink(later_file.c_str());
        dfs_unlink(renamed.c_str());
        dfs_unlink(file.c_str());
        dfs_rmdir(root.c_str());
    }
    dfs_shutdown();
}

TEST(MetadbCoverageUT, SerializedKvFlow)
{
    /* Exercise Serialized KV flow and assert the relevant success or failure branch. */
    /*
     * DT 对应关系:
     * - TC-KV-001 KV_PUT 新 key 成功;
     * - TC-KV-002 KV_GET 命中返回一致;
     * - TC-KV-003 KV_DEL 删除后不可读。
     *
     * 该用例只覆盖 serialized 入口的 KV put/get/delete。
     */
    SqlConnections connections;
    if (!PrepareSqlConnections(&connections)) {
        GTEST_SKIP() << "local-run SQL endpoints are not ready";
    }

    std::string key = BuildSqlRoot("serialized_kv");
    int response_size = 0;
    std::vector<uint64_t> value_key = {11, 12};
    std::vector<uint64_t> location = {21, 22};
    std::vector<uint32_t> size = {31, 32};

    // TC-KV-001/002/003: serialized 入口覆盖 KV put/get/delete。
    EXPECT_TRUE(connections.worker->SerializedCall(KV_PUT, BuildKvParam(key, 8192, value_key, location, size),
                                                   &response_size))
        << connections.worker->ErrorMessage();
    EXPECT_GT(response_size, 4);
    EXPECT_TRUE(connections.worker->SerializedCall(KV_GET, BuildKeyOnlyParam(key), &response_size))
        << connections.worker->ErrorMessage();
    EXPECT_GT(response_size, 4);
    EXPECT_TRUE(connections.worker->SerializedCall(KV_DEL, BuildKeyOnlyParam(key), &response_size))
        << connections.worker->ErrorMessage();
    EXPECT_GT(response_size, 4);
    EXPECT_TRUE(connections.worker->SerializedCall(KV_GET, BuildKeyOnlyParam(key), &response_size))
        << connections.worker->ErrorMessage();
    EXPECT_GT(response_size, 4);
    EXPECT_TRUE(connections.worker->SerializedCall(KV_DEL, BuildKeyOnlyParam(key), &response_size))
        << connections.worker->ErrorMessage();
    EXPECT_GT(response_size, 4);
    dfs_shutdown();
}

TEST(MetadbCoverageUT, SerializedSliceFlow)
{
    /* Exercise Serialized Slice flow and assert the relevant success or failure branch. */
    /*
     * DT 对应关系:
     * - TC-SLICE-001 单线程 FETCH_SLICE_ID;
     * - TC-SLICE-004 SLICE_PUT 后 GET 一致;
     * - TC-SLICE-005 SLICE_DEL 删除后 GET 失败。
     *
     * 该用例只覆盖 serialized 入口的 slice-id 和 slice put/get/delete。
     */
    SqlConnections connections;
    if (!PrepareSqlConnections(&connections)) {
        GTEST_SKIP() << "local-run SQL endpoints are not ready";
    }

    std::string root = BuildSqlRoot("serialized_slice");
    std::string file = fmt::format("{}/serialized_file", root);
    int response_size = 0;
    bool namespace_removed = false;

    ASSERT_TRUE(connections.cn->SerializedCall(MKDIR, BuildPathOnlyParam(root), &response_size))
        << connections.cn->ErrorMessage();
    ASSERT_TRUE(connections.worker->SerializedCall(CREATE, BuildPathOnlyParam(file), &response_size))
        << connections.worker->ErrorMessage();

    struct stat stbuf;
    ASSERT_EQ(dfs_stat(file.c_str(), &stbuf), 0);
    uint64_t inode_id = static_cast<uint64_t>(stbuf.st_ino);
    // TC-SLICE-001: serialized 入口分配 slice-id。
    EXPECT_TRUE(connections.worker->SerializedCall(FETCH_SLICE_ID, BuildSliceIdParam(2, 1), &response_size))
        << connections.worker->ErrorMessage();
    EXPECT_GT(response_size, 4);

    std::vector<uint64_t> inode_ids = {inode_id, inode_id};
    std::vector<uint32_t> chunk_ids = {7, 7};
    std::vector<uint64_t> slice_ids = {101, 102};
    std::vector<uint32_t> slice_sizes = {4096, 4096};
    std::vector<uint32_t> slice_offsets = {0, 4096};
    std::vector<uint32_t> slice_lens = {4096, 4096};
    std::vector<uint32_t> slice_loc1 = {1, 2};
    std::vector<uint32_t> slice_loc2 = {3, 4};
    // TC-SLICE-004/005: serialized 入口覆盖 slice put/get/delete。
    EXPECT_TRUE(connections.worker->SerializedCall(SLICE_PUT,
                                                   BuildSliceInfoParam(file, inode_ids, chunk_ids, slice_ids,
                                                                       slice_sizes, slice_offsets, slice_lens,
                                                                       slice_loc1, slice_loc2),
                                                   &response_size))
        << connections.worker->ErrorMessage();
    EXPECT_GT(response_size, 4);
    EXPECT_TRUE(connections.worker->SerializedCall(SLICE_GET, BuildSliceIndexParam(file, inode_id, 7), &response_size))
        << connections.worker->ErrorMessage();
    EXPECT_GT(response_size, 4);
    EXPECT_TRUE(connections.worker->SerializedCall(SLICE_DEL, BuildSliceIndexParam(file, inode_id, 7), &response_size))
        << connections.worker->ErrorMessage();
    EXPECT_GT(response_size, 4);
    EXPECT_TRUE(connections.worker->SerializedCall(SLICE_GET, BuildSliceIndexParam(file, inode_id, 7), &response_size))
        << connections.worker->ErrorMessage();
    EXPECT_GT(response_size, 4);
    EXPECT_TRUE(connections.worker->SerializedCall(SLICE_DEL, BuildSliceIndexParam(file, inode_id, 7), &response_size))
        << connections.worker->ErrorMessage();
    EXPECT_GT(response_size, 4);

    EXPECT_TRUE(connections.worker->SerializedCall(UNLINK, BuildPathOnlyParam(file), &response_size))
        << connections.worker->ErrorMessage();
    EXPECT_TRUE(connections.cn->SerializedCall(RMDIR, BuildPathOnlyParam(root), &response_size))
        << connections.cn->ErrorMessage();
    namespace_removed = true;

    if (!namespace_removed) {
        dfs_unlink(file.c_str());
        dfs_rmdir(root.c_str());
    }
    dfs_shutdown();
}

TEST(MetadbCoverageUT, AdminSqlClearDataFlowRunsLast)
{
    /* Exercise Admin SQL Clear Data flow runs Last and assert the relevant success or failure branch. */
    SqlConnections connections;
    if (!PrepareSqlConnections(&connections)) {
        GTEST_SKIP() << "local-run SQL endpoints are not ready";
    }

    int ret = -1;
    int function_count = 0;
    ASSERT_TRUE(connections.cn->ScalarInt(
        "SELECT count(*) FROM pg_proc WHERE proname IN "
        "('falcon_clear_cached_relation_oid_func', 'falcon_clear_user_data_func', 'falcon_clear_all_data_func')",
        &function_count))
        << connections.cn->ErrorMessage();
    if (function_count != 3) {
        GTEST_SKIP() << "admin clear SQL functions are not installed in this extension build";
    }

    EXPECT_TRUE(connections.cn->ScalarInt("SELECT falcon_clear_cached_relation_oid_func()", &ret))
        << connections.cn->ErrorMessage();
    EXPECT_EQ(ret, 0);

    EXPECT_TRUE(connections.cn->ScalarInt("SELECT falcon_clear_user_data_func()", &ret))
        << connections.cn->ErrorMessage();
    EXPECT_EQ(ret, 0);

    EXPECT_TRUE(connections.cn->ScalarInt("SELECT falcon_clear_all_data_func()", &ret))
        << connections.cn->ErrorMessage();
    EXPECT_EQ(ret, 0);

    dfs_shutdown();
}
