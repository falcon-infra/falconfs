# Commit Description Generator

自动生成 Git commit 描述的 Claude Code skill。

## 功能特性

1. **自动分析代码变更**
   - 自动运行 `git diff` 分析未提交变更
   - 自动运行 `git show HEAD` 分析已提交commit
   - 智能推断变更意图

2. **多种分析场景**
   - 未提交的变更（工作区/暂存区）
   - 最近一个已提交的commit
   - 最近N个commits
   - 与远程分支的差异

3. **标准化输出格式**
   - 总述：一句话概括核心修改
   - 逐项介绍：每行一个功能点
   - 支持中英文描述

## 使用方式

### 作为 Claude Code Skill

```
帮我生成commit描述
生成最近一个已提交commit的描述
分析最近3个commits
```

### 触发关键词

- `commit message` / `commit描述`
- `write commit` / `modify commit` / `edit commit`
- `generate commit` / `analyze commit`
- `生成commit` / `分析commit`

## 分析场景

| 场景 | 使用的Git命令 |
|------|--------------|
| 未提交变更 | `git diff`, `git diff --staged` |
| 最近commit | `git show HEAD`, `git diff HEAD~1 HEAD` |
| 最近N个commits | `git diff HEAD~N HEAD` |
| 与远程差异 | `git diff origin/main` |

## 输出格式

```
总述：一句话概括核心修改

逐项介绍：
- 修改功能点1
- 修改功能点2
- 修改功能点3
```

## 示例

### 示例1 - 未提交变更

**输入**: "帮我生成commit描述"

**自动分析**:
```bash
$ git diff --stat
 src/user/user.service.ts   |  20 +++++++
 src/user/user.controller.ts|  15 +++---
 2 files changed, 35 insertions(+), 10 deletions(-)
```

**输出**:
```
添加用户登录功能

新增UserService.login()方法
更新UserController登录路由
```

### 示例2 - 已提交commit

**输入**: "生成最近一个已提交commit的描述"

**自动分析**:
```bash
$ git show --stat HEAD
commit abc123
Author: Zhang San <zhangsan@example.com>

    添加用户登录功能

 src/user/user.service.ts   |  50 +++++++++++++++++++++++++
 src/user/user.controller.ts|  30 +++++++-------
 2 files changed, 80 insertions(+), 10 deletions(-)
```

**输出**:
```
添加用户登录功能

新增UserService.login()方法
更新UserController登录路由
集成JWT令牌生成
```

### 示例3 - 工作区干净时

**输入**: "帮我生成commit描述"

**自动分析**:
```bash
$ git status
On branch main
nothing to commit, working tree clean

# 自动分析最近commit
$ git diff HEAD~1 HEAD --stat
 src/auth/jwt.ts           |  20 +++++++
 src/auth/login.ts         |  15 +++---
 2 files changed, 35 insertions(+), 10 deletions(-)
```

**输出**:
```
实现JWT认证功能

新增JWT令牌生成和验证逻辑
更新登录接口返回token
```

## 智能推断

根据文件变更模式自动推断修改意图：

| 文件变更模式 | 推断意图 |
|-------------|----------|
| 新增文件 | 新增功能 |
| 删除文件 | 移除功能 |
| 修改配置文件 | 配置变更 |
| 修改测试文件 | 添加/修复测试 |
| 修改文档 | 文档更新 |
| 重构同类型文件 | 代码重构 |
| 修复bug相关文件 | Bug修复 |

## 注意事项

1. 始终自动分析实际代码变更，不凭空编造
2. 如果工作区干净，自动分析最近commit
3. 支持任意数量功能点的逐项介绍
4. 可与 git-master 技能配合完成实际提交
