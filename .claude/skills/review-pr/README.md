# FalconFS Skills

本目录包含 FalconFS 项目专用的 Claude Code skills。

## review-pr.py

自动 review GitHub Pull Request 的 skill。

### 功能特性

1. **PR 信息获取**
   - 使用 `gh` CLI 获取 PR 的详细信息（标题、描述、作者、分支、变更统计）
   - 支持多种输入格式：
     - PR 编号: `123`
     - PR URL: `https://github.com/falcon-infra/falconfs/pull/123`
     - 仓库+编号: `falcon-infra/falconfs#123`

2. **代码分析**
   - 解析 PR diff
   - 统计文件变更（增加/删除行数）
   - 识别修改的文件类型

3. **FalconFS 项目特定规则检查**
   - **错误码处理**: 检查是否直接返回内部错误码，应使用 `ErrorCodeToErrno()` 转换
   - **日志输出**: 建议使用 glog 而非 printf/cout
   - **内存管理**: 建议使用智能指针而非裸 new
   - **Protobuf 变更**: 提醒需要重新生成 .pb.cc/.h 文件
   - **PostgreSQL 插件**: 提醒 falcon/ 目录的文件会被复制到 third_party
   - **CMake 变更**: 提醒新依赖需要 Find*.cmake 脚本

4. **行级评论（v1.5.0 更新）**
   - **使用 `line` + `side: "RIGHT"` 定位**：直接使用文件绝对行号，避免多 hunk 歧义
   - **基于内容定位行号**：通过 GitHub API 读取文件内容，用 `grep -n` 定位真实行号
   - 支持提交前验证评论位置

5. **Review 输出**
   - 终端彩色输出（按严重程度分级: error/warning/info）
   - 生成 Markdown 格式的 review 评论
   - 支持发布评论到 GitHub（`--post` 参数）
   - 支持行级评论（`--line-comments` 参数）
   - 支持 JSON 输出（`--json` 参数）

### 使用方式

#### 作为 Claude Code Skill
```
/review-pr 123
/review-pr https://github.com/falcon-infra/falconfs/pull/123
/review-pr falcon-infra/falconfs 123
```

#### 直接运行脚本
```bash
# Review PR（仅显示）
python3 .claude/skills/review-pr.py 123

# 发布整体评论到 GitHub
python3 .claude/skills/review-pr.py 123 --post

# 发布行级评论（推荐）
python3 .claude/skills/review-pr.py 123 --line-comments

# 行级评论 + 批准 PR
python3 .claude/skills/review-pr.py 123 --line-comments --approve

# 行级评论 + 请求修改
python3 .claude/skills/review-pr.py 123 --line-comments --request-changes

# 验证行级评论位置（不提交）
python3 .claude/skills/review-pr.py 123 --verify-lines

# JSON 输出
python3 .claude/skills/review-pr.py 123 --json
```

### 依赖项

- Python 3
- gh (GitHub CLI) - 需要先运行 `gh auth login` 进行认证
- git

### ⚠️ 行号定位最佳实践

**必须使用 `line` + `side: "RIGHT"` 提交行级评论，禁止使用 `position` 参数。**

`position` 是 diff hunk 内的相对偏移量，同一文件有多个 hunk 时会产生歧义（不同 hunk 可能存在相同的 position 值），GitHub 会选择第一个匹配的 hunk 导致评论关联到错误行。

```bash
# 1. 获取 PR head SHA
HEAD_SHA=$(gh pr view 123 --repo falcon-infra/falconfs --json headRefOid --jq '.headRefOid')

# 2. 通过内容查找行号
gh api repos/falcon-infra/falconfs/contents/<FILE>?ref=$HEAD_SHA --jq '.content' | base64 -d | grep -n "关键代码"

# 3. 验证行号
gh api repos/falcon-infra/falconfs/contents/<FILE>?ref=$HEAD_SHA --jq '.content' | base64 -d | sed -n '<LINE-2>,<LINE+2>p'

# 4. 提交时使用 line + side: "RIGHT"
# comments JSON 中使用:
# {"path": "<FILE>", "line": <LINE_NUM>, "side": "RIGHT", "body": "..."}
```

### 示例输出

```
================================================================================
  PR 摘要
================================================================================

  标题: Add new feature for metadata caching
  作者: username
  状态: OPEN
  分支: feature/cache → main
  变更: +150 -50 (5 个文件)

修改的文件:
  - falcon/metadb/meta_handle.cpp +100 -20
  - falcon/metadb/meta_handle.h +30 -5
  - CMakeLists.txt +5 -2
  - tests/metadb/test_cache.cpp +15 -23

正在分析代码...

⚠️  WARNING (2 条)

  falcon/metadb/meta_handle.cpp:125
    直接返回错误码: 直接返回内部错误代码，应该使用 ErrorCodeToErrno() 转换
    💡 建议: 参考 fuse_main.cpp: return ret > 0 ? -ErrorCodeToErrno(ret) : ret;

  falcon/metadb/meta_handle.cpp:200
    内存管理: 使用 new 分配内存
    💡 建议: 考虑使用智能指针 (std::unique_ptr/std::shared_ptr) 避免内存泄漏

ℹ️  INFO (1 条)

  CMakeLists.txt
    CMake 配置变更: 修改了 CMake 配置，如果引入新依赖请确保在 cmake/ 目录下有对应的 Find*.cmake
    💡 建议: 参考现有的 FindBRPC.cmake, Protobuf.cmake 等
```
