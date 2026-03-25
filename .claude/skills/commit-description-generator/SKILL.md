---
name: commit-description-generator
description: "Use this skill whenever the user wants to generate, create, improve, or modify a commit message/description. This includes: helping write new commit messages from staged changes, editing/refining existing commit messages, converting git diff into commit descriptions, formatting commit messages to follow a specific style, analyzing git diff automatically to generate descriptions, or reviewing commit messages for clarity. Trigger when user mentions 'commit message', 'commit描述', 'write commit', 'modify commit', 'edit commit', 'generate commit', 'analyze commit', or similar requests. ALWAYS automatically run git diff to analyze actual code changes when user asks for commit description generation."
---

# Commit Description Generator

## Overview

This skill helps you generate or modify git commit descriptions following a standardized format. It can automatically analyze git diff to extract change information.

## Output Format

Commit描述必须遵循以下格式：

```
commit修改总述, 一句话概括核心修改, 独占一行

commit修改功能逐项介绍, 每行一个修改功能点介绍
```

### Format Rules

1. **第一行（总述）**：
   - 一句话概括本次commit的核心修改
   - 简洁明确，突出主要功能或修复的问题
   - 独占一行，无空行分隔

2. **逐项介绍**：
   - 每行一个修改功能点
   - 使用简洁的技术语言
   - 可以包含文件名、函数名等具体信息

## Auto Analysis Process (MUST DO)

### 步骤1：自动获取变更信息

当用户请求生成commit描述时，**必须自动执行以下命令**获取变更信息：

```bash
# 场景A：分析未提交变更（工作区/暂存区）
# 获取未暂存的变更
git diff --stat

# 获取已暂存的变更
git diff --staged --stat

# 获取完整的diff内容（用于分析具体修改）
git diff

# 获取已暂存的完整diff
git diff --staged

# 场景B：分析已提交的commit
# 获取最近一个commit的diff（与父提交的差异）
git show --stat HEAD

# 获取最近一个commit的详细diff
git show HEAD

# 获取最近N个commit的diff
git diff HEAD~1 HEAD --stat   # 最近1个commit
git diff HEAD~3 HEAD --stat   # 最近3个commits

# 获取commit的完整信息（包含消息）
git log -1 --format="%H %s %b"
```

### 场景区分

根据用户需求判断分析目标：

| 用户需求 | 使用的Git命令 |
|---------|--------------|
| 生成未提交变更的描述 | `git diff`, `git diff --staged` |
| 生成最近一个已提交commit的描述 | `git show HEAD`, `git diff HEAD~1 HEAD` |
| 生成最近N个commits的描述 | `git diff HEAD~N HEAD` |
| 获取commit元信息 | `git log -1 --format="%H %s %b"` |

### 步骤2：分析已提交的Commit

当用户请求生成**已提交commit**的描述时，执行：

```bash
# 获取最近一个commit的摘要信息
git show --stat HEAD

# 获取完整diff（用于分析具体修改内容）
git show HEAD

# 或者使用diff方式
git diff HEAD~1 HEAD
```

**分析要点**：
- 从 `git show HEAD` 提取：修改的文件列表、变更行数、具体修改的函数/逻辑
- 注意：已提交的commit可能包含多个文件的修改，需要综合分析所有变更
- 推断修改意图时，考虑文件类型和修改内容的组合

### 步骤3：智能推断变更意图

根据文件变更模式推断修改目的：

| 文件变更模式 | 可能的修改意图 |
|-------------|---------------|
| 新增文件 | 新增功能 |
| 删除文件 | 移除功能 |
| 修改配置文件 | 配置变更 |
| 修改测试文件 | 添加/修复测试 |
| 修改文档 | 文档更新 |
| 重构同类型文件 | 代码重构 |
| 修复bug相关文件 | Bug修复 |

### 步骤4：生成描述

根据分析结果生成：

1. **总述**
   - 使用简洁的语言概括核心变更
   - 突出主要功能或修复的问题
   - 可以适当详细，但不要冗长

2. **逐项介绍**（每行一个功能点）
   - 每个功能点使用动词开头
   - 保持语言简洁明了

## Input Processing

### 用户输入场景

1. **用户说"帮我生成commit描述"或"分析最近一个commit"**：
   - 首先检查是否有未提交的变更
   - 如果有未提交变更，分析未提交变更
   - 如果没有未提交变更（工作区干净），自动分析最近一个已提交的commit
   - 使用 `git diff HEAD~1 HEAD` 或 `git show HEAD` 获取变更

2. **用户明确说"生成已提交commit的描述"、"分析最近commit"**：
   - 必须运行 `git show HEAD` 或 `git diff HEAD~1 HEAD` 分析最近一个commit
   - 基于实际代码变更生成描述

3. **用户提供了变更信息**：
   - 基于用户提供的变更信息生成描述
   - 也可以运行 git diff 辅助确认

4. **用户指定获取特定范围的变更**：
   - `git diff HEAD~1` - 上一个commit（简写）
   - `git diff HEAD~1 HEAD` - 上一个commit（显式）
   - `git diff HEAD~3 HEAD` - 最近3个commits
   - `git diff origin/main` - 与远程的差异

## Quality Guidelines

### 必须遵守

- 总述简洁明确，概括核心变更
- 逐项介绍使用简洁的动宾短语
- 避免冗余信息，每个功能点一行讲完
- 使用技术术语但保持简洁
- **自动执行git diff分析实际变更**

### 避免

- 模糊的功能描述如"修改了一些代码"
- 与技术实现无关的描述
- 标点符号（总述不需要句号）

## Examples

### Example 1 - 自动分析

**User Input**: "帮我生成commit描述"

**Auto Analysis**:
```bash
$ git diff --stat
 src/user/user.service.ts   |  20 +++++++
 src/user/user.controller.ts|  15 +++---
 2 files changed, 35 insertions(+), 10 deletions(-)
```

**Output**:
```
添加用户登录功能

新增UserService.login()方法
更新UserController登录路由
```

### Example 2 - 删除文件

**User Input**: "帮我生成commit描述"

**Auto Analysis**:
```bash
$ git diff --stat
 docs/old-guide.md         | 200 ------------
 README.md                 |  10 ++--
 2 files changed, 10 insertions(+), 200 deletions(-)
```

**Output**:
```
清理废弃文档

删除过时的旧指南文档
更新README相关说明
```

### Example 3 - Bug修复

**User Input**: "帮我生成commit描述"

**Auto Analysis**:
```bash
$ git diff
-  // 缺少空值检查
+  if (user == null) return null;
+  // 添加空值检查
```

**Output**:
```
修复空指针异常

添加用户对象空值检查
优化错误处理逻辑
```

### Example 4 - 重构

**User Input**: "帮我生成commit描述"

**Auto Analysis**:
```bash
$ git diff --stat
 src/utils/helper.ts       |  50 ++++++------
 src/services/parser.ts   | 100 +++++++++++++-------
 2 files changed, 80 insertions(+), 70 deletions(-)
```

**Output**:
```
重构工具类代码

提取公共方法到Helper类
优化解析器逻辑
```

### Example 5 - 分析已提交的commit

**User Input**: "生成最近一个已提交commit的描述"

**Auto Analysis**:
```bash
$ git show --stat HEAD
commit abc1234567890
Author: Zhang San <zhangsan@example.com>
Date:   Mon Mar 2 10:30:00 2026 +0800

    添加用户登录功能

 src/user/user.service.ts   |  50 +++++++++++++++++++++++++
 src/user/user.controller.ts|  30 +++++++-------
 2 files changed, 80 insertions(+), 10 deletions(-)

$ git show HEAD
// 详细的代码变更diff
```

**Output**:
```
添加用户登录功能

新增UserService.login()方法
更新UserController登录路由
集成JWT令牌生成
```

### Example 6 - 工作区干净时自动分析最近commit

**User Input**: "帮我生成commit描述"

**Auto Analysis**:
```bash
$ git status
On branch main
nothing to commit, working tree clean

# 工作区干净，自动分析最近commit
$ git diff HEAD~1 HEAD --stat
 src/auth/jwt.ts           |  20 +++++++
 src/auth/login.ts         |  15 +++---
 2 files changed, 35 insertions(+), 10 deletions(-)
```

**Output**:
```
实现JWT认证功能

新增JWT令牌生成和验证逻辑
更新登录接口返回token
```

## Output Delivery

完成commit描述后，直接输出描述内容，无需额外说明。如果需要修改现有描述，请先展示修改后的完整描述，询问用户是否满意。

如果需要实际执行git commit，可以使用git-master技能协助完成提交。

## Summary

**关键点**：
1. 用户请求commit描述时，必须自动运行 `git diff --stat` 和 `git diff` 分析变更
2. 如果工作区干净（无未提交变更），自动分析最近一个已提交的commit（使用 `git show HEAD` 或 `git diff HEAD~1 HEAD`）
3. 根据实际代码变更生成描述，而不是凭空编造
4. 格式必须符合要求：总述概括核心变更，逐项介绍每行一个功能点
5. 如果git diff为空或无法获取变更，再询问用户具体修改内容
