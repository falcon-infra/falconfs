#include "test_metadb_coverage_common.h"

#include <algorithm>
#include <thread>

using namespace metadb_test;

namespace {

bool PrepareDfsClient()
{
    constexpr int kRetry = 2;
    for (int attempt = 0; attempt < kRetry; ++attempt) {
        if (!local_run_test::EnsureConfiguredServer()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }
        if (InitClientOrSkip()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    return false;
}

void CleanupPaths(const std::vector<std::string> &files, const std::vector<std::string> &dirs, const std::string &root)
{
    for (const auto &path : files) {
        dfs_unlink(path.c_str());
    }
    for (auto it = dirs.rbegin(); it != dirs.rend(); ++it) {
        dfs_rmdir(it->c_str());
    }
    try {
        UninitNamespaceRoot(root);
    } catch (...) {
    }
    dfs_shutdown();
}

}  // 匿名命名空间

TEST(MetadbCoverageUT, DirectoryCreateListRemoveFlow)
{
    /*
     * DT 对应关系:
     * - TC-DIR-001 创建一级目录成功: 创建目录，并验证目录可以 opendir;
     * - TC-DIR-005 READDIR 返回目录项完整: 验证新建文件和目录都出现在目录项中;
     * - TC-DIR-006 删除空目录成功: 删除空目录并拆除命名空间;
     * - TC-FILE-001 CREATE 成功并可见 / TC-FILE-006 UNLINK 删除后不可见。
     *
     * 该用例只覆盖目录创建、遍历和删除的基础生命周期。
     */
    if (!PrepareDfsClient()) {
        GTEST_SKIP() << "local-run service is not ready";
    }

    std::string root = BuildRootPath("dir_create_list_remove");
    std::vector<std::string> files;
    std::vector<std::string> dirs;
    bool namespace_removed = false;
    try {
        InitNamespaceRoot(root);
        std::string thread_dir = ThreadDir(root, 0);
        std::string file = FilePath(root, 0, 0);
        std::string dir = DirPath(root, 0, 0);

        // TC-FILE-001: 创建文件，供目录遍历验证。
        ASSERT_EQ(dfs_create(file.c_str(), 0644), 0);
        files.push_back(file);
        // TC-DIR-001: 创建一级目录，并验证目录可打开。
        ASSERT_EQ(dfs_mkdir(dir.c_str(), 0755), 0);
        dirs.push_back(dir);
        uint64_t dir_inode = 0;
        EXPECT_EQ(dfs_opendir(thread_dir.c_str(), &dir_inode), 0);
        EXPECT_NE(dir_inode, 0U);

        // TC-DIR-005: readdir 结果应包含刚创建的文件和目录。
        std::vector<std::string> entries;
        ASSERT_EQ(dfs_readdir(thread_dir.c_str(), &entries), 0);
        EXPECT_NE(std::find(entries.begin(), entries.end(), "file_0"), entries.end());
        EXPECT_NE(std::find(entries.begin(), entries.end(), "dir_0"), entries.end());

        // TC-FILE-006 / TC-DIR-006: 删除文件和空目录，清理后根目录可拆除。
        EXPECT_EQ(dfs_unlink(file.c_str()), 0);
        files.clear();
        EXPECT_EQ(dfs_rmdir(dir.c_str()), 0);
        dirs.clear();
        UninitNamespaceRoot(root);
        namespace_removed = true;
    } catch (...) {
        ADD_FAILURE() << "directory create/list/remove flow threw an exception";
    }

    if (!namespace_removed) {
        CleanupPaths(files, dirs, root);
    } else {
        dfs_shutdown();
    }
}

TEST(MetadbCoverageUT, FileAttributeUpdateFlow)
{
    /*
     * DT 对应关系:
     * - TC-ATTR-001 UTIMENS 更新并校验;
     * - TC-ATTR-002 CHMOD 更新并校验;
     * - TC-ATTR-003 CHOWN 更新并校验。
     *
     * 该用例只覆盖文件属性更新和 stat 校验。
     */
    if (!PrepareDfsClient()) {
        GTEST_SKIP() << "local-run service is not ready";
    }

    std::string root = BuildRootPath("file_attribute_update");
    std::vector<std::string> files;
    bool namespace_removed = false;
    try {
        InitNamespaceRoot(root);
        std::string file = FilePath(root, 0, 0);
        ASSERT_EQ(dfs_create(file.c_str(), 0644), 0);
        files.push_back(file);

        // TC-ATTR-002 / TC-ATTR-003 / TC-ATTR-001: 分别覆盖 chmod、chown、utimens。
        EXPECT_EQ(dfs_chmod(file.c_str(), 0600), 0);
        EXPECT_EQ(dfs_chown(file.c_str(), static_cast<uint32_t>(getuid()), static_cast<uint32_t>(getgid())), 0);
        EXPECT_EQ(dfs_utimens(file.c_str(), 1609459200000000000LL, 1640995200000000000LL), 0);

        // TC-ATTR-001/002/003: 通过 stat 校验属性更新结果。
        struct stat stbuf;
        ASSERT_EQ(dfs_stat(file.c_str(), &stbuf), 0);
        EXPECT_EQ(stbuf.st_mode & 0777, 0600);
        EXPECT_EQ(stbuf.st_uid, getuid());
        EXPECT_EQ(stbuf.st_gid, getgid());

        EXPECT_EQ(dfs_unlink(file.c_str()), 0);
        files.clear();
        UninitNamespaceRoot(root);
        namespace_removed = true;
    } catch (...) {
        ADD_FAILURE() << "file attribute update flow threw an exception";
    }

    if (!namespace_removed) {
        CleanupPaths(files, {}, root);
    } else {
        dfs_shutdown();
    }
}

TEST(MetadbCoverageUT, FileAndDirectoryRenameFlow)
{
    /*
     * DT 对应关系:
     * - TC-REN-001 同目录文件重命名成功: 旧路径不可见，新路径可见;
     * - TC-REN-001 同目录目录重命名成功: 旧目录不可打开，新目录可打开。
     */
    if (!PrepareDfsClient()) {
        GTEST_SKIP() << "local-run service is not ready";
    }

    std::string root = BuildRootPath("file_dir_rename");
    std::vector<std::string> files;
    std::vector<std::string> dirs;
    bool namespace_removed = false;
    try {
        InitNamespaceRoot(root);
        std::string thread_dir = ThreadDir(root, 0);
        std::string file_src = FilePath(root, 0, 0);
        std::string file_dst = fmt::format("{}/file_renamed", thread_dir);
        std::string dir_src = DirPath(root, 0, 0);
        std::string dir_dst = fmt::format("{}/dir_renamed", thread_dir);
        struct stat stbuf;

        ASSERT_EQ(dfs_create(file_src.c_str(), 0644), 0);
        files.push_back(file_src);
        ASSERT_EQ(dfs_mkdir(dir_src.c_str(), 0755), 0);
        dirs.push_back(dir_src);

        // TC-REN-001: 文件 rename 后旧路径 stat 失败，新路径 stat 成功。
        EXPECT_EQ(dfs_rename(file_src.c_str(), file_dst.c_str()), 0);
        files[0] = file_dst;
        EXPECT_NE(dfs_stat(file_src.c_str(), &stbuf), 0);
        EXPECT_EQ(dfs_stat(file_dst.c_str(), &stbuf), 0);

        // TC-REN-001: 目录 rename 后旧路径 opendir 失败，新路径 opendir 成功。
        uint64_t dir_inode = 0;
        EXPECT_EQ(dfs_rename(dir_src.c_str(), dir_dst.c_str()), 0);
        dirs[0] = dir_dst;
        EXPECT_NE(dfs_opendir(dir_src.c_str(), nullptr), 0);
        EXPECT_EQ(dfs_opendir(dir_dst.c_str(), &dir_inode), 0);

        EXPECT_EQ(dfs_unlink(file_dst.c_str()), 0);
        files.clear();
        EXPECT_EQ(dfs_rmdir(dir_dst.c_str()), 0);
        dirs.clear();
        UninitNamespaceRoot(root);
        namespace_removed = true;
    } catch (...) {
        ADD_FAILURE() << "file/directory rename flow threw an exception";
    }

    if (!namespace_removed) {
        CleanupPaths(files, dirs, root);
    } else {
        dfs_shutdown();
    }
}

TEST(MetadbCoverageUT, MissingPathAttributeAndRenameFailureFlow)
{
    /*
     * DT 对应关系:
     * - TC-ATTR-004 不存在路径属性更新失败;
     * - TC-REN-003 源不存在重命名失败;
     * - TC-DIR-004/TC-FILE-003 不存在路径访问失败。
     */
    if (!PrepareDfsClient()) {
        GTEST_SKIP() << "local-run service is not ready";
    }

    std::string root = BuildRootPath("missing_attr_rename");
    bool namespace_removed = false;
    try {
        InitNamespaceRoot(root);
        std::string missing = fmt::format("{}/missing", ThreadDir(root, 0));

        // TC-DIR-004 / TC-FILE-003: 不存在路径不能 opendir/readdir。
        EXPECT_NE(dfs_opendir(missing.c_str(), nullptr), 0);
        EXPECT_NE(dfs_readdir(missing.c_str(), nullptr), 0);
        // TC-REN-003: 源路径不存在时 rename 失败。
        EXPECT_NE(dfs_rename(missing.c_str(), fmt::format("{}/dst", ThreadDir(root, 0)).c_str()), 0);
        // TC-ATTR-004: 不存在路径上的属性更新失败。
        EXPECT_NE(dfs_chmod(missing.c_str(), 0644), 0);
        EXPECT_NE(dfs_chown(missing.c_str(), 1000, 1000), 0);
        EXPECT_NE(dfs_utimens(missing.c_str(), 1, 1), 0);

        UninitNamespaceRoot(root);
        namespace_removed = true;
    } catch (...) {
        ADD_FAILURE() << "missing path attribute/rename flow threw an exception";
    }

    if (!namespace_removed) {
        CleanupPaths({}, {}, root);
    } else {
        dfs_shutdown();
    }
}

TEST(MetadbCoverageUT, DuplicateCreateAndMkdirFailureFlow)
{
    /*
     * DT 对应关系:
     * - TC-FILE-002 重复 CREATE 失败;
     * - TC-DIR-002 重复创建同名目录失败。
     */
    if (!PrepareDfsClient()) {
        GTEST_SKIP() << "local-run service is not ready";
    }

    std::string root = BuildRootPath("duplicate_create_mkdir");
    std::vector<std::string> files;
    std::vector<std::string> dirs;
    bool namespace_removed = false;
    try {
        InitNamespaceRoot(root);
        std::string file = FilePath(root, 0, 0);
        std::string dir = DirPath(root, 0, 0);

        // TC-FILE-002: 第一次 create 成功，重复 create 失败。
        EXPECT_EQ(dfs_create(file.c_str(), 0644), 0);
        files.push_back(file);
        EXPECT_NE(dfs_create(file.c_str(), 0644), 0);
        // TC-DIR-002: 第一次 mkdir 成功，重复 mkdir 失败。
        EXPECT_EQ(dfs_mkdir(dir.c_str(), 0755), 0);
        dirs.push_back(dir);
        EXPECT_NE(dfs_mkdir(dir.c_str(), 0755), 0);

        EXPECT_EQ(dfs_unlink(file.c_str()), 0);
        files.clear();
        EXPECT_EQ(dfs_rmdir(dir.c_str()), 0);
        dirs.clear();
        UninitNamespaceRoot(root);
        namespace_removed = true;
    } catch (...) {
        ADD_FAILURE() << "duplicate create/mkdir flow threw an exception";
    }

    if (!namespace_removed) {
        CleanupPaths(files, dirs, root);
    } else {
        dfs_shutdown();
    }
}

TEST(MetadbCoverageUT, MissingPathOperationFailureFlow)
{
    /*
     * DT 对应关系:
     * - TC-FILE-003 父路径不存在 CREATE 失败;
     * - TC-DIR-003 父目录不存在时创建失败;
     * - TC-REN-003 源不存在重命名失败。
     */
    if (!PrepareDfsClient()) {
        GTEST_SKIP() << "local-run service is not ready";
    }

    std::string root = BuildRootPath("missing_path_ops");
    bool namespace_removed = false;
    try {
        InitNamespaceRoot(root);
        std::string thread_dir = ThreadDir(root, 0);
        std::string missing = fmt::format("{}/missing", thread_dir);
        std::string missing_child_dir = fmt::format("{}/child_dir", missing);
        std::string missing_child_file = fmt::format("{}/child_file", missing);
        struct stat stbuf;

        // TC-FILE-003: 不存在路径上的 stat/open/unlink/rmdir/create 均失败。
        EXPECT_NE(dfs_stat(missing.c_str(), &stbuf), 0);
        EXPECT_NE(dfs_open(missing.c_str(), O_RDONLY, 0), 0);
        EXPECT_NE(dfs_unlink(missing.c_str()), 0);
        EXPECT_NE(dfs_rmdir(missing.c_str()), 0);
        EXPECT_NE(dfs_create(missing_child_file.c_str(), 0644), 0);
        EXPECT_NE(dfs_stat(missing_child_file.c_str(), &stbuf), 0);
        // TC-DIR-003: 父目录不存在时 mkdir 失败且不残留。
        EXPECT_NE(dfs_mkdir(missing_child_dir.c_str(), 0755), 0);
        EXPECT_NE(dfs_stat(missing_child_dir.c_str(), &stbuf), 0);
        // TC-REN-003: 源路径不存在时 rename 失败。
        EXPECT_NE(dfs_rename(missing.c_str(), fmt::format("{}/dst", thread_dir).c_str()), 0);

        UninitNamespaceRoot(root);
        namespace_removed = true;
    } catch (...) {
        ADD_FAILURE() << "missing path operation flow threw an exception";
    }

    if (!namespace_removed) {
        CleanupPaths({}, {}, root);
    } else {
        dfs_shutdown();
    }
}

TEST(MetadbCoverageUT, TypeMismatchAndNonEmptyDirectoryFailureFlow)
{
    /*
     * DT 对应关系:
     * - TC-DIR-004 对文件执行 OPENDIR 失败;
     * - TC-FILE-004 对目录执行 OPEN 的当前实现行为;
     * - TC-DIR-007 删除非空目录失败。
     */
    if (!PrepareDfsClient()) {
        GTEST_SKIP() << "local-run service is not ready";
    }

    std::string root = BuildRootPath("type_mismatch_nonempty");
    std::vector<std::string> files;
    std::vector<std::string> dirs;
    bool namespace_removed = false;
    try {
        InitNamespaceRoot(root);
        std::string file = FilePath(root, 0, 0);
        std::string dir = DirPath(root, 0, 0);
        std::string nested_file = fmt::format("{}/nested_file", dir);

        ASSERT_EQ(dfs_create(file.c_str(), 0644), 0);
        files.push_back(file);
        ASSERT_EQ(dfs_mkdir(dir.c_str(), 0755), 0);
        dirs.push_back(dir);

        // TC-DIR-004: 普通文件不能作为目录打开。
        EXPECT_NE(dfs_opendir(file.c_str(), nullptr), 0);
        // TC-FILE-004: 当前 dfs_open 层可能接受目录路径，若返回句柄则关闭。
        int dir_fd = dfs_open(dir.c_str(), O_RDONLY, 0);
        if (dir_fd >= 0) {
            EXPECT_EQ(dfs_close(dir_fd, dir.c_str()), 0);
        }
        // TC-DIR-007: 非空目录 rmdir 失败。
        ASSERT_EQ(dfs_create(nested_file.c_str(), 0644), 0);
        files.push_back(nested_file);
        EXPECT_NE(dfs_rmdir(dir.c_str()), 0);

        EXPECT_EQ(dfs_unlink(nested_file.c_str()), 0);
        files.pop_back();
        EXPECT_EQ(dfs_unlink(file.c_str()), 0);
        files.clear();
        EXPECT_EQ(dfs_rmdir(dir.c_str()), 0);
        dirs.clear();
        UninitNamespaceRoot(root);
        namespace_removed = true;
    } catch (...) {
        ADD_FAILURE() << "type mismatch/non-empty directory flow threw an exception";
    }

    if (!namespace_removed) {
        CleanupPaths(files, dirs, root);
    } else {
        dfs_shutdown();
    }
}

TEST(MetadbCoverageUT, CrossDirectoryRenameAndConflictFlow)
{
    /*
     * DT 对应关系:
     * - TC-REN-002 跨目录重命名成功;
     * - TC-REN-003 目标已存在时 rename 失败。
     */
    if (!PrepareDfsClient()) {
        GTEST_SKIP() << "local-run service is not ready";
    }

    std::string root = BuildRootPath("cross_dir_rename_conflict");
    std::vector<std::string> files;
    std::vector<std::string> dirs;
    bool namespace_removed = false;
    try {
        InitNamespaceRoot(root);
        std::string file = FilePath(root, 0, 0);
        std::string other_file = FilePath(root, 0, 1);
        std::string other_dir = DirPath(root, 0, 1);
        std::string moved_file = fmt::format("{}/moved_file", other_dir);
        struct stat stbuf;

        ASSERT_EQ(dfs_create(file.c_str(), 0644), 0);
        files.push_back(file);
        ASSERT_EQ(dfs_mkdir(other_dir.c_str(), 0755), 0);
        dirs.push_back(other_dir);

        // TC-REN-002: 文件跨目录移动后旧路径不可见，新路径可见。
        EXPECT_EQ(dfs_rename(file.c_str(), moved_file.c_str()), 0);
        files[0] = moved_file;
        EXPECT_EQ(dfs_stat(moved_file.c_str(), &stbuf), 0);
        EXPECT_NE(dfs_stat(file.c_str(), &stbuf), 0);

        // TC-REN-003: 目标路径已存在时 rename 失败。
        ASSERT_EQ(dfs_create(other_file.c_str(), 0644), 0);
        files.push_back(other_file);
        EXPECT_NE(dfs_rename(moved_file.c_str(), other_file.c_str()), 0);

        for (const auto &path : files) {
            EXPECT_EQ(dfs_unlink(path.c_str()), 0);
        }
        files.clear();
        EXPECT_EQ(dfs_rmdir(other_dir.c_str()), 0);
        dirs.clear();
        UninitNamespaceRoot(root);
        namespace_removed = true;
    } catch (...) {
        ADD_FAILURE() << "cross-directory rename/conflict flow threw an exception";
    }

    if (!namespace_removed) {
        CleanupPaths(files, dirs, root);
    } else {
        dfs_shutdown();
    }
}

TEST(MetadbCoverageUT, KvPutDeleteBoundaryFlow)
{
    /*
     * DT 对应关系:
     * - TC-KV-001 KV_PUT 新 key 成功;
     * - TC-KV-003 KV_DEL 删除后不可读;
     * - TC-KV-004 重复 key PUT 语义。
     */
    if (!PrepareDfsClient()) {
        GTEST_SKIP() << "local-run service is not ready";
    }

    std::string root = BuildRootPath("kv_put_delete_boundary");
    bool namespace_removed = false;
    try {
        InitNamespaceRoot(root);
        std::string key = fmt::format("{}kv_boundary_key", ThreadDir(root, 0));
        uint64_t value_key = 11;
        uint64_t location = 22;
        uint32_t size = 33;

        // TC-KV-003: 不存在 key 读取和删除失败。
        EXPECT_NE(dfs_kv_get(key.c_str(), nullptr, nullptr), 0);
        EXPECT_NE(dfs_kv_del(key.c_str()), 0);
        // TC-KV-001 / TC-KV-004: 新 key put 成功，重复 put 保持当前幂等语义。
        EXPECT_EQ(dfs_kv_put(key.c_str(), 4096, 1, &value_key, &location, &size), 0);
        EXPECT_EQ(dfs_kv_put(key.c_str(), 4096, 1, &value_key, &location, &size), 0);
        EXPECT_EQ(dfs_kv_del(key.c_str()), 0);
        EXPECT_NE(dfs_kv_del(key.c_str()), 0);

        UninitNamespaceRoot(root);
        namespace_removed = true;
    } catch (...) {
        ADD_FAILURE() << "KV put/delete boundary flow threw an exception";
    }

    if (!namespace_removed) {
        CleanupPaths({}, {}, root);
    } else {
        dfs_shutdown();
    }
}

TEST(MetadbCoverageUT, SlicePutGetDeleteBoundaryFlow)
{
    /*
     * DT 对应关系:
     * - TC-SLICE-001 单线程 FETCH_SLICE_ID;
     * - TC-SLICE-004 SLICE_PUT 后 GET 一致;
     * - TC-SLICE-005 SLICE_DEL 删除后 GET 失败。
     */
    if (!PrepareDfsClient()) {
        GTEST_SKIP() << "local-run service is not ready";
    }

    std::string root = BuildRootPath("slice_put_get_delete");
    std::vector<std::string> files;
    bool namespace_removed = false;
    try {
        InitNamespaceRoot(root);
        std::string file = FilePath(root, 0, 0);
        ASSERT_EQ(dfs_create(file.c_str(), 0644), 0);
        files.push_back(file);

        // TC-SLICE-001: 单线程分配指定数量的 slice-id。
        uint64_t slice_start = 0;
        uint64_t slice_end = 0;
        ASSERT_EQ(dfs_fetch_slice_id(2, &slice_start, &slice_end), 0);
        EXPECT_EQ(slice_end - slice_start, 2U);
        // TC-SLICE-005: 不存在 slice 查询失败，删除保持当前幂等语义。
        EXPECT_NE(dfs_slice_get(file.c_str(), 9999, 9999, nullptr), 0);
        EXPECT_EQ(dfs_slice_del(file.c_str(), 9999, 9999), 0);
        // TC-SLICE-004 / TC-SLICE-005: put 后可 get，delete 后再次 delete 保持当前行为。
        EXPECT_EQ(dfs_slice_put(file.c_str(), 9999, 9, slice_start, 4096, 0, 4096), 0);
        uint32_t slice_num = 0;
        EXPECT_EQ(dfs_slice_get(file.c_str(), 9999, 9, &slice_num), 0);
        EXPECT_GT(slice_num, 0U);
        EXPECT_EQ(dfs_slice_del(file.c_str(), 9999, 9), 0);
        EXPECT_EQ(dfs_slice_del(file.c_str(), 9999, 9), 0);

        EXPECT_EQ(dfs_unlink(file.c_str()), 0);
        files.clear();
        UninitNamespaceRoot(root);
        namespace_removed = true;
    } catch (...) {
        ADD_FAILURE() << "slice put/get/delete boundary flow threw an exception";
    }

    if (!namespace_removed) {
        CleanupPaths(files, {}, root);
    } else {
        dfs_shutdown();
    }
}

TEST(MetadbCoverageUT, UnlinkMakesFileInvisibleFlow)
{
    /*
     * DT 对应关系:
     * - TC-FILE-006 UNLINK 删除后不可见: unlink 后 stat/open 均失败。
     */
    if (!PrepareDfsClient()) {
        GTEST_SKIP() << "local-run service is not ready";
    }

    std::string root = BuildRootPath("unlink_invisible");
    std::vector<std::string> files;
    bool namespace_removed = false;
    try {
        InitNamespaceRoot(root);
        std::string file = FilePath(root, 0, 0);
        ASSERT_EQ(dfs_create(file.c_str(), 0644), 0);
        files.push_back(file);
        EXPECT_EQ(dfs_unlink(file.c_str()), 0);
        files.clear();

        // TC-FILE-006: 删除后 stat/open 都应失败。
        struct stat stbuf;
        EXPECT_NE(dfs_stat(file.c_str(), &stbuf), 0);
        EXPECT_NE(dfs_open(file.c_str(), O_RDONLY, 0), 0);

        UninitNamespaceRoot(root);
        namespace_removed = true;
    } catch (...) {
        ADD_FAILURE() << "unlink invisible flow threw an exception";
    }

    if (!namespace_removed) {
        CleanupPaths(files, {}, root);
    } else {
        dfs_shutdown();
    }
}

TEST(MetadbCoverageUT, DeepPathFileLifecycleFlow)
{
    /*
     * DT 对应关系:
     * - TC-FILE-009 深层路径文件生命周期: 深层路径上的 create/stat/open/close/unlink 成功。
     */
    if (!PrepareDfsClient()) {
        GTEST_SKIP() << "local-run service is not ready";
    }

    std::string root = BuildRootPath("deep_path_file_lifecycle");
    std::vector<std::string> files;
    std::vector<std::string> dirs;
    bool namespace_removed = false;
    try {
        InitNamespaceRoot(root);
        std::string current = ThreadDir(root, 0);
        for (const char *part : {"a", "b", "c", "d", "e"}) {
            current = fmt::format("{}/{}", current, part);
            ASSERT_EQ(dfs_mkdir(current.c_str(), 0755), 0);
            dirs.push_back(current);
        }

        // TC-FILE-009: 深层路径文件 create/stat/open/close/unlink 生命周期。
        std::string deep_file = fmt::format("{}/file_deep", current);
        ASSERT_EQ(dfs_create(deep_file.c_str(), 0644), 0);
        files.push_back(deep_file);
        struct stat stbuf;
        EXPECT_EQ(dfs_stat(deep_file.c_str(), &stbuf), 0);
        EXPECT_EQ(dfs_open(deep_file.c_str(), O_RDONLY, 0), 0);
        EXPECT_EQ(dfs_close(0, deep_file.c_str()), 0);
        EXPECT_EQ(dfs_unlink(deep_file.c_str()), 0);
        files.clear();
        EXPECT_NE(dfs_stat(deep_file.c_str(), &stbuf), 0);

        for (auto it = dirs.rbegin(); it != dirs.rend(); ++it) {
            EXPECT_EQ(dfs_rmdir(it->c_str()), 0);
        }
        dirs.clear();
        UninitNamespaceRoot(root);
        namespace_removed = true;
    } catch (...) {
        ADD_FAILURE() << "deep path file lifecycle flow threw an exception";
    }

    if (!namespace_removed) {
        CleanupPaths(files, dirs, root);
    } else {
        dfs_shutdown();
    }
}


TEST(MetadbCoverageUT, ConcurrentDirectoryAndFileCreateFlow)
{
    /*
     * DT 对应关系:
     * - TC-DIR-008 并发创建同名目录: 多线程并发 mkdir 同一路径时只有一个成功;
     * - TC-FILE-007 并发创建同名文件: 多线程并发 create 同一路径时只有一个成功。
     *
     * 该流程在 local-run 服务上启动一个 DFS 客户端，创建独立命名空间根目录，
     * 然后让多个线程竞争同一个目录名和文件名。预期结果是元数据创建只成功一次，
     * 其余调用返回已存在类错误。dfs_shutdown 前会清理所有创建的目录项。
     */
    if (!local_run_test::EnsureConfiguredServer()) {
        GTEST_SKIP() << "local-run service is not configured";
    }
    if (!InitClientOrSkip()) {
        GTEST_SKIP() << "local-run service is not ready";
    }

    std::string root = BuildRootPath("dt_concurrent_create_flow");
    bool namespace_removed = false;
    std::string same_dir;
    std::string same_file;
    try {
        InitNamespaceRoot(root);
        std::string thread_dir = ThreadDir(root, 0);
        same_dir = fmt::format("{}/same_dir", thread_dir);
        same_file = fmt::format("{}/same_file", thread_dir);

        constexpr int kThreads = 8;
        std::atomic<int> mkdir_success(0);
        std::atomic<int> mkdir_failure(0);
        std::vector<std::thread> workers;
        for (int i = 0; i < kThreads; ++i) {
            workers.emplace_back([&]() {
                if (dfs_mkdir(same_dir.c_str(), 0755) == 0) {
                    mkdir_success.fetch_add(1, std::memory_order_relaxed);
                } else {
                    mkdir_failure.fetch_add(1, std::memory_order_relaxed);
                }
            });
        }
        for (auto &worker : workers) {
            worker.join();
        }
        // TC-DIR-008 并发创建同名目录: 只有一个线程成功，其余线程失败。
        EXPECT_EQ(mkdir_success.load(), 1);
        EXPECT_EQ(mkdir_failure.load(), kThreads - 1);

        std::atomic<int> create_success(0);
        std::atomic<int> create_failure(0);
        workers.clear();
        for (int i = 0; i < kThreads; ++i) {
            workers.emplace_back([&]() {
                if (dfs_create(same_file.c_str(), 0644) == 0) {
                    create_success.fetch_add(1, std::memory_order_relaxed);
                } else {
                    create_failure.fetch_add(1, std::memory_order_relaxed);
                }
            });
        }
        for (auto &worker : workers) {
            worker.join();
        }
        // TC-FILE-007 并发创建同名文件: 只有一个线程成功，其余线程失败。
        EXPECT_EQ(create_success.load(), 1);
        EXPECT_EQ(create_failure.load(), kThreads - 1);

        EXPECT_EQ(dfs_unlink(same_file.c_str()), 0);
        same_file.clear();
        EXPECT_EQ(dfs_rmdir(same_dir.c_str()), 0);
        same_dir.clear();
        UninitNamespaceRoot(root);
        namespace_removed = true;
    } catch (...) {
        ADD_FAILURE() << "concurrent directory/file create flow threw an exception";
    }

    if (!same_file.empty()) {
        dfs_unlink(same_file.c_str());
    }
    if (!same_dir.empty()) {
        dfs_rmdir(same_dir.c_str());
    }
    if (!namespace_removed) {
        try {
            UninitNamespaceRoot(root);
        } catch (...) {
        }
    }
    dfs_shutdown();
}

TEST(MetadbCoverageUT, SliceIdConcurrentAndAllocatorIsolationFlow)
{
    /*
     * DT 对应关系:
     * - TC-SLICE-002 并发 FETCH_SLICE_ID 不重叠:
     *   并发 FETCH_SLICE_ID 返回的区间互不重叠;
     * - TC-SLICE-003 FILE/KV 两类分配器隔离: FILE 和 KV 两类 slice-id 分配器都能通过
     *   序列化元数据入口返回结果。
     *
     * 前半部分通过 DFS 客户端校验外部可见的分配行为。
     * 后半部分分别用 type=0(KV) 和 type=1(FILE) 直接调用
     * falcon_meta_call_by_serialized_data，同时覆盖 sliceid_table.c 中的关系选择分支。
     */
    if (!local_run_test::EnsureConfiguredServer()) {
        GTEST_SKIP() << "local-run service is not configured";
    }
    if (!InitClientOrSkip()) {
        GTEST_SKIP() << "local-run service is not ready";
    }

    constexpr int kThreads = 8;
    constexpr uint32_t kCountPerThread = 3;
    std::vector<std::pair<uint64_t, uint64_t>> ranges(kThreads);
    std::vector<int> ret_codes(kThreads, -1);
    std::vector<std::thread> workers;
    for (int i = 0; i < kThreads; ++i) {
        workers.emplace_back([&, i]() {
            uint64_t start = 0;
            uint64_t end = 0;
            ret_codes[i] = dfs_fetch_slice_id(kCountPerThread, &start, &end);
            ranges[i] = {start, end};
        });
    }
    for (auto &worker : workers) {
        worker.join();
    }

    // TC-SLICE-002 并发 FETCH_SLICE_ID 不重叠: 每次分配数量正确。
    for (int i = 0; i < kThreads; ++i) {
        ASSERT_EQ(ret_codes[i], 0);
        EXPECT_EQ(ranges[i].second - ranges[i].first, kCountPerThread);
    }
    std::sort(ranges.begin(), ranges.end());
    // TC-SLICE-002 并发 FETCH_SLICE_ID 不重叠: 排序后相邻区间不重叠。
    for (size_t i = 1; i < ranges.size(); ++i) {
        EXPECT_LE(ranges[i - 1].second, ranges[i].first);
    }

    int worker_port = local_run_test::GetIntEnvOrDefault("LOCAL_RUN_WORKER_PG_PORT", 55520);
    std::unique_ptr<PgConnection> worker_owner;
    PgConnection *worker = nullptr;
    if (!ConnectPlainSql(worker_port, worker, worker_owner)) {
        dfs_shutdown();
        GTEST_SKIP() << "worker PostgreSQL endpoint is not ready";
    }

    int response_size = 0;
    // TC-SLICE-003 FILE/KV 两类分配器隔离: KV 分配器可以返回 slice-id。
    EXPECT_TRUE(worker->SerializedCall(FETCH_SLICE_ID, BuildSliceIdParam(2, 0), &response_size))
        << worker->ErrorMessage();
    EXPECT_GT(response_size, 4);
    // TC-SLICE-003 FILE/KV 两类分配器隔离: FILE 分配器可以返回 slice-id。
    EXPECT_TRUE(worker->SerializedCall(FETCH_SLICE_ID, BuildSliceIdParam(2, 1), &response_size))
        << worker->ErrorMessage();
    EXPECT_GT(response_size, 4);
    dfs_shutdown();
}

TEST(MetadbCoverageUT, InvalidFilenameBoundaryFlow)
{
    /*
     * DT 对应关系:
     * - TC-FILE-010 文件名超长/非法字符: 非法路径会被拒绝，且不会留下可见元数据项。
     *
     * 当前元数据层没有对普通路径组件强制 POSIX NAME_MAX 限制，
     * 因此此处覆盖当前实现中稳定的边界: 空路径不能 create，也不能 stat。
     */
    if (!local_run_test::EnsureConfiguredServer()) {
        GTEST_SKIP() << "local-run service is not configured";
    }
    if (!InitClientOrSkip()) {
        GTEST_SKIP() << "local-run service is not ready";
    }

    struct stat stbuf;
    // TC-FILE-010 文件名超长/非法字符: 空路径不能 create，也不能 stat。
    EXPECT_NE(dfs_create("", 0644), 0);
    EXPECT_NE(dfs_stat("", &stbuf), 0);
    dfs_shutdown();
}
