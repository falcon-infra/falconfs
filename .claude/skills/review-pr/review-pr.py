#!/usr/bin/env python3
"""
GitHub PR 自动 Review Skill

功能：
- 自动获取 PR 的详细信息（标题、描述、修改的文件）
- 分析代码变更（diff）
- 根据 FalconFS 项目规范提供 review 意见
- 基于内容定位行号，避免行号错乱

依赖：
- gh (GitHub CLI)
- git

使用方式：
- /review-pr <PR_NUMBER>           # review 当前仓库的 PR
- /review-pr <PR_URL>              # review 指定 URL 的 PR
- /review-pr <REPO> <PR_NUMBER>    # review 其他仓库的 PR
"""

import os
import sys
import json
import subprocess
import argparse
import base64
import re
from pathlib import Path
from typing import Dict, List, Tuple, Optional


class Colors:
    """终端颜色"""
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


def print_colored(text: str, color: str = Colors.ENDC):
    """打印带颜色的文本"""
    print(f"{color}{text}{Colors.ENDC}")


def run_command(cmd: List[str], check: bool = True) -> Tuple[int, str, str]:
    """运行命令并返回结果"""
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=check
    )
    return result.returncode, result.stdout.strip(), result.stderr.strip()


def parse_pr_input(input_str: str) -> Tuple[str, int]:
    """解析 PR 输入，返回 (owner/repo, pr_number)"""
    # 检查是否是 URL
    if "github.com/" in input_str:
        # 解析 URL: https://github.com/owner/repo/pull/123
        parts = input_str.split("github.com/")[-1].split("/")
        if len(parts) >= 4 and parts[2] == "pull":
            return f"{parts[0]}/{parts[1]}", int(parts[3])

    # 检查是否是 owner/repo#123 格式
    if "#" in input_str:
        repo, pr_num = input_str.split("#")
        return repo, int(pr_num)

    # 检查是否是纯数字
    if input_str.isdigit():
        # 获取当前仓库
        _, origin_url, _ = run_command(["git", "config", "--get", "remote.origin.url"])
        if "github.com" in origin_url:
            # 解析仓库 URL
            repo_path = origin_url.split("github.com:")[-1].split("github.com/")[-1]
            repo = repo_path.replace(".git", "")
            return repo, int(input_str)

    raise ValueError(f"无法解析 PR 输入: {input_str}")


def get_pr_info(repo: str, pr_number: int) -> Dict:
    """获取 PR 的详细信息"""
    print_colored(f"\n正在获取 {repo} PR #{pr_number} 的信息...", Colors.OKBLUE)

    # 获取 PR 基本信息
    returncode, stdout, stderr = run_command([
        "gh", "pr", "view", str(pr_number),
        "--repo", repo,
        "--json", "title,body,author,state,headRefName,baseRefName,additions,deletions,changedFiles,labels"
    ])

    if returncode != 0:
        print_colored(f"获取 PR 信息失败: {stderr}", Colors.FAIL)
        sys.exit(1)

    return json.loads(stdout)


def get_pr_diff(repo: str, pr_number: int) -> str:
    """获取 PR 的 diff"""
    returncode, stdout, stderr = run_command([
        "gh", "pr", "diff", str(pr_number),
        "--repo", repo
    ])

    if returncode != 0:
        print_colored(f"获取 PR diff 失败: {stderr}", Colors.FAIL)
        return ""

    return stdout


def get_pr_files(repo: str, pr_number: int) -> List[Dict]:
    """获取 PR 修改的文件列表"""
    returncode, stdout, stderr = run_command([
        "gh", "pr", "diff", str(pr_number),
        "--repo", repo,
        "--name-only"
    ])

    if returncode != 0:
        return []

    files = stdout.split("\n") if stdout else []
    return [{"path": f} for f in files if f]


def get_pr_head_sha(repo: str, pr_number: int) -> str:
    """获取 PR 的 head commit SHA"""
    returncode, stdout, stderr = run_command([
        "gh", "pr", "view", str(pr_number),
        "--repo", repo,
        "--json", "headRefOid",
        "--jq", ".headRefOid"
    ])

    if returncode != 0:
        return ""
    return stdout.strip()


def get_file_content_from_github(repo: str, file_path: str, ref: str) -> Optional[str]:
    """
    通过 GitHub API 获取文件内容
    
    Args:
        repo: 仓库 (owner/repo)
        file_path: 文件路径
        ref: Git ref (commit SHA, branch, etc.)
    
    Returns:
        文件内容字符串，失败返回 None
    """
    returncode, stdout, stderr = run_command([
        "gh", "api",
        f"repos/{repo}/contents/{file_path}?ref={ref}",
        "--jq", ".content"
    ], check=False)

    if returncode != 0 or not stdout.strip():
        return None

    try:
        # Base64 解码
        return base64.b64decode(stdout.strip()).decode('utf-8')
    except Exception:
        return None


def find_line_by_content(file_content: str, search_pattern: str) -> Tuple[Optional[int], Optional[str]]:
    """
    通过内容模式查找行号
    
    Args:
        file_content: 文件内容
        search_pattern: 要搜索的内容（子字符串匹配）
    
    Returns:
        (行号, 行内容)，未找到返回 (None, None)
    """
    if not file_content:
        return None, None
    
    for i, line in enumerate(file_content.split('\n'), 1):
        if search_pattern in line:
            return i, line
    
    return None, None


def find_all_lines_by_content(file_content: str, search_pattern: str) -> List[Tuple[int, str]]:
    """
    查找所有匹配的行
    
    Args:
        file_content: 文件内容
        search_pattern: 要搜索的内容
    
    Returns:
        [(行号, 行内容), ...]
    """
    results = []
    if not file_content:
        return results
    
    for i, line in enumerate(file_content.split('\n'), 1):
        if search_pattern in line:
            results.append((i, line))
    
    return results


def verify_line_content(file_content: str, line_num: int, expected_content: str) -> bool:
    """
    验证指定行是否包含期望的内容
    
    Args:
        file_content: 文件内容
        line_num: 行号 (1-indexed)
        expected_content: 期望的内容
    
    Returns:
        True 如果匹配，False 否则
    """
    if not file_content:
        return False
    
    lines = file_content.split('\n')
    if line_num < 1 or line_num > len(lines):
        return False
    
    return expected_content in lines[line_num - 1]


class LineCommentBuilder:
    """
    行级评论构建器
    
    使用基于内容的方式定位行号，避免行号错乱
    """
    
    def __init__(self, repo: str, head_sha: str):
        self.repo = repo
        self.head_sha = head_sha
        self.comments = []
        self._file_cache = {}  # 缓存已获取的文件内容
    
    def get_file_content(self, file_path: str) -> Optional[str]:
        """获取文件内容（带缓存）"""
        if file_path not in self._file_cache:
            self._file_cache[file_path] = get_file_content_from_github(
                self.repo, file_path, self.head_sha
            )
        return self._file_cache[file_path]
    
    def add_comment_by_content(
        self, 
        file_path: str, 
        search_content: str, 
        body: str,
        severity: str = "info"
    ) -> bool:
        """
        通过内容添加行级评论
        
        Args:
            file_path: 文件路径
            search_content: 用于定位的内容片段
            body: 评论内容
            severity: 严重程度 (error/warning/info)
        
        Returns:
            True 如果成功添加，False 如果未找到内容
        """
        file_content = self.get_file_content(file_path)
        if not file_content:
            print_colored(f"  ⚠️ 无法获取文件: {file_path}", Colors.WARNING)
            return False
        
        line_num, line_content = find_line_by_content(file_content, search_content)
        if line_num is None:
            print_colored(f"  ⚠️ 未找到内容 '{search_content[:50]}...' in {file_path}", Colors.WARNING)
            return False
        
        # 验证并添加
        self.comments.append({
            "path": file_path,
            "position": line_num,
            "body": body,
            "_verified_content": line_content.strip()[:60],  # 用于调试
            "_severity": severity
        })
        
        print_colored(f"  ✓ {file_path}:{line_num} - {line_content.strip()[:50]}...", Colors.OKGREEN)
        return True
    
    def add_comment_at_line(self, file_path: str, line_num: int, body: str, verify_content: str = None) -> bool:
        """
        在指定行添加评论（可选验证内容）
        
        Args:
            file_path: 文件路径
            line_num: 行号
            body: 评论内容
            verify_content: 可选，用于验证该行确实包含此内容
        
        Returns:
            True 如果成功添加
        """
        if verify_content:
            file_content = self.get_file_content(file_path)
            if file_content and not verify_line_content(file_content, line_num, verify_content):
                print_colored(f"  ⚠️ 行号验证失败: {file_path}:{line_num} 不包含 '{verify_content[:30]}'", Colors.WARNING)
                return False
        
        self.comments.append({
            "path": file_path,
            "position": line_num,
            "body": body
        })
        return True
    
    def build_payload(self, review_body: str, event: str = "COMMENT") -> Dict:
        """构建 GitHub API 请求 payload"""
        # 过滤内部字段
        clean_comments = []
        for c in self.comments:
            clean_comments.append({
                "path": c["path"],
                "position": c["position"],
                "body": c["body"]
            })
        
        return {
            "commit_id": self.head_sha,
            "body": review_body,
            "event": event,
            "comments": clean_comments
        }
    
    def print_summary(self):
        """打印评论摘要"""
        if not self.comments:
            print_colored("\n没有行级评论", Colors.WARNING)
            return
        
        print_colored(f"\n行级评论 ({len(self.comments)} 条):", Colors.BOLD + Colors.OKCYAN)
        for c in self.comments:
            verified = c.get("_verified_content", "")
            severity = c.get("_severity", "info")
            icon = {"error": "🔴", "warning": "⚠️", "info": "ℹ️"}.get(severity, "ℹ️")
            print(f"  {icon} {c['path']}:{c['position']}")
            if verified:
                print(f"     内容: {verified}...")


def analyze_file_changes(diff: str) -> List[Dict]:
    """分析代码变更"""
    changes = []
    current_file = None
    additions = 0
    deletions = 0

    for line in diff.split("\n"):
        if line.startswith("diff --git"):
            if current_file:
                changes.append({
                    "file": current_file,
                    "additions": additions,
                    "deletions": deletions
                })
            current_file = line.split("b/")[-1] if " b/" in line else line.split(" a/")[-1]
            additions = 0
            deletions = 0
        elif line.startswith("+") and not line.startswith("+++"):
            additions += 1
        elif line.startswith("-") and not line.startswith("---"):
            deletions += 1

    if current_file:
        changes.append({
            "file": current_file,
            "additions": additions,
            "deletions": deletions
        })

    return changes


# FalconFS 项目特定的 review 规则
FALCONFS_RULES = {
    "c_cpp": {
        "name": "C/C++ 代码规范",
        "patterns": [
            {
                "pattern": r"return\s+-?\d+;",
                "description": "直接返回内部错误代码",
                "severity": "error",
                "fix": "使用 ErrorCodeToErrno() 将内部错误码转换为 POSIX errno",
                "reference": "falcon_client/fuse_main.cpp"
            },
            {
                "pattern": r"new\s+\w+",
                "description": "使用 new 分配内存",
                "severity": "warning",
                "fix": "考虑使用智能指针 (std::unique_ptr/std::shared_ptr) 避免内存泄漏"
            },
            {
                "pattern": r"printf\(",
                "description": "使用 printf 输出日志",
                "severity": "info",
                "fix": "使用 glog (LOG(INFO)/LOG(ERROR)) 统一日志输出"
            },
            {
                "pattern": r"std::cout",
                "description": "使用 std::cout 输出",
                "severity": "info",
                "fix": "使用 glog 统一日志输出"
            }
        ]
    },
    "proto": {
        "name": "Protobuf 定义",
        "patterns": [
            {
                "pattern": r"\.proto$",
                "description": "修改了 proto 定义文件",
                "severity": "info",
                "fix": "确保运行 build.sh 重新生成 .pb.cc/.h 文件"
            }
        ]
    },
    "postgres": {
        "name": "PostgreSQL 插件",
        "patterns": [
            {
                "pattern": r"falcon/.*\.c",
                "description": "修改 falcon/ 目录下的 C 文件",
                "severity": "info",
                "fix": "这些文件会被复制到 third_party/postgres/contrib/falcon，不要直接修改 third_party/postgres 下的文件"
            }
        ]
    },
    "cmake": {
        "name": "CMake 配置",
        "patterns": [
            {
                "pattern": r"CMakeLists\.txt",
                "description": "修改了 CMakeLists.txt",
                "severity": "info",
                "fix": "确保新依赖的库在 cmake/ 目录下有对应的 Find*.cmake 脚本"
            }
        ]
    },
    "tests": {
        "name": "测试覆盖",
        "patterns": [
            {
                "pattern": r"(additions|deletions) > 100",
                "description": "较大代码变更",
                "severity": "warning",
                "fix": "建议添加相应的单元测试，测试文件位于 tests/ 目录"
            }
        ]
    }
}


def review_code(pr_info: Dict, diff: str, file_changes: List[Dict]) -> List[Dict]:
    """根据项目规范 review 代码"""
    reviews = []
    files = [change["file"] for change in file_changes]

    # 检查是否修改了关键文件
    for file_path in files:
        # 检查 proto 文件
        if file_path.endswith(".proto"):
            reviews.append({
                "file": file_path,
                "line": None,
                "severity": "info",
                "title": "Protobuf 定义变更",
                "message": "修改了 protobuf 定义文件，请确保运行 build.sh 重新生成 .pb.cc/.h 文件",
                "suggestion": "build.sh 会在构建前自动调用 protoc 生成代码"
            })

        # 检查 CMakeLists.txt
        if file_path.endswith("CMakeLists.txt"):
            reviews.append({
                "file": file_path,
                "line": None,
                "severity": "info",
                "title": "CMake 配置变更",
                "message": "修改了 CMake 配置，如果引入新依赖请确保在 cmake/ 目录下有对应的 Find*.cmake",
                "suggestion": "参考现有的 FindBRPC.cmake, Protobuf.cmake 等"
            })

        # 检查 PostgreSQL 插件文件
        if file_path.startswith("falcon/") and file_path.endswith(".c"):
            reviews.append({
                "file": file_path,
                "line": None,
                "severity": "info",
                "title": "PostgreSQL 插件变更",
                "message": "falcon/ 目录的文件会被复制到 third_party/postgres/contrib/falcon",
                "suggestion": "不要直接修改 third_party/postgres 下的文件，它们会被覆盖"
            })

    # 分析 diff 内容
    current_file = None
    for i, line in enumerate(diff.split("\n")):
        if line.startswith("diff --git"):
            current_file = line.split("b/")[-1] if " b/" in line else line.split(" a/")[-1]
            continue

        if line.startswith("+") and current_file:
            # 获取新增的代码行
            code_line = line[1:]
            line_num = i + 1

            # 检查错误码返回
            if "return -" in code_line and ("E" in code_line or "return -" in code_line):
                # 检查是否是直接返回数字
                import re
                if re.search(r"return\s+-\d+", code_line):
                    reviews.append({
                        "file": current_file,
                        "line": line_num,
                        "severity": "warning",
                        "title": "直接返回错误码",
                        "message": "直接返回内部错误代码，应该使用 ErrorCodeToErrno() 转换",
                        "suggestion": "参考 fuse_main.cpp: return ret > 0 ? -ErrorCodeToErrno(ret) : ret;"
                    })

            # 检查 printf/cout
            if "printf(" in code_line or "std::cout" in code_line:
                reviews.append({
                    "file": current_file,
                    "line": line_num,
                    "severity": "info",
                    "title": "日志输出方式",
                    "message": "使用 printf/cout 输出日志",
                    "suggestion": "建议使用 glog: LOG(INFO) << ... 或 LOG(ERROR) << ..."
                })

            # 检查裸 new
            if re.search(r"\bnew\s+\w+", code_line):
                reviews.append({
                    "file": current_file,
                    "line": line_num,
                    "severity": "info",
                    "title": "内存管理",
                    "message": "使用 new 分配内存",
                    "suggestion": "考虑使用智能指针 (std::unique_ptr/std::shared_ptr) 避免内存泄漏"
                })

    return reviews


def print_pr_summary(pr_info: Dict, file_changes: List[Dict]):
    """打印 PR 摘要"""
    print_colored("\n" + "=" * 80, Colors.BOLD)
    print_colored(f"  PR 摘要", Colors.BOLD + Colors.HEADER)
    print_colored("=" * 80, Colors.BOLD)

    print(f"\n  {Colors.BOLD}标题:{Colors.ENDC} {pr_info['title']}")
    print(f"  {Colors.BOLD}作者:{Colors.ENDC} {pr_info['author']['login']}")
    print(f"  {Colors.BOLD}状态:{Colors.ENDC} {pr_info['state']}")
    print(f"  {Colors.BOLD}分支:{Colors.ENDC} {pr_info['headRefName']} → {pr_info['baseRefName']}")
    print(f"  {Colors.BOLD}变更:{Colors.ENDC} +{pr_info['additions']} -{pr_info['deletions']} ({pr_info['changedFiles']} 个文件)")

    if pr_info.get('labels'):
        labels = [label['name'] for label in pr_info['labels']]
        print(f"  {Colors.BOLD}标签:{Colors.ENDC} {', '.join(labels)}")

    if pr_info.get('body'):
        print(f"\n  {Colors.BOLD}描述:{Colors.ENDC}")
        for line in pr_info['body'].split('\n')[:10]:  # 只显示前10行
            print(f"    {line}")

    print()


def print_file_changes(file_changes: List[Dict]):
    """打印文件变更"""
    print_colored("\n修改的文件:", Colors.BOLD + Colors.OKCYAN)
    for change in file_changes:
        additions = f"+{change['additions']}" if change['additions'] > 0 else ""
        deletions = f"-{change['deletions']}" if change['deletions'] > 0 else ""
        stats = " ".join([a for a in [additions, deletions] if a])
        print(f"  - {change['file']}{Colors.OKGREEN} {stats}{Colors.ENDC}" if stats else f"  - {change['file']}")


def print_reviews(reviews: List[Dict]):
    """打印 review 意见"""
    if not reviews:
        print_colored("\n✓ 没有发现明显的问题！", Colors.OKGREEN + Colors.BOLD)
        return

    # 按严重程度分组
    by_severity = {
        "error": [],
        "warning": [],
        "info": []
    }

    for review in reviews:
        by_severity[review["severity"]].append(review)

    severity_colors = {
        "error": Colors.FAIL,
        "warning": Colors.WARNING,
        "info": Colors.OKCYAN
    }

    severity_icons = {
        "error": "❌",
        "warning": "⚠️ ",
        "info": "ℹ️ "
    }

    for severity in ["error", "warning", "info"]:
        items = by_severity[severity]
        if not items:
            continue

        color = severity_colors[severity]
        icon = severity_icons[severity]

        print_colored(f"\n{icon} {severity.upper()} ({len(items)} 条)", color + Colors.BOLD)

        for review in items:
            location = f"{review['file']}"
            if review.get('line'):
                location += f":{review['line']}"

            print(f"\n  {Colors.UNDERLINE}{location}{Colors.ENDC}")
            print(f"    {color}{review['title']}{Colors.ENDC}: {review['message']}")
            if review.get('suggestion'):
                print(f"    💡 {Colors.OKCYAN}建议:{Colors.ENDC} {review['suggestion']}")


def generate_review_comment(pr_info: Dict, file_changes: List[Dict], reviews: List[Dict]) -> str:
    """生成 review 评论（用于发布到 GitHub）"""
    comment = f"## 🤖 自动代码 Review\n\n"
    comment += f"**PR:** {pr_info['title']}\n"
    comment += f"**作者:** @{pr_info['author']['login']}\n"
    comment += f"**变更:** +{pr_info['additions']} -{pr_info['deletions']} ({pr_info['changedFiles']} 个文件)\n\n"

    if not reviews:
        comment += "✅ 没有发现明显的问题！\n"
    else:
        comment += f"### 发现 {len(reviews)} 条建议\n\n"

        # 按严重程度分组
        by_severity = {"error": [], "warning": [], "info": []}
        for review in reviews:
            by_severity[review["severity"]].append(review)

        for severity, icon in [("error", "❌"), ("warning", "⚠️"), ("info", "ℹ️")]:
            items = by_severity[severity]
            if not items:
                continue

            comment += f"#### {icon} {severity.upper()} ({len(items)} 条)\n\n"
            for review in items:
                location = review['file']
                if review.get('line'):
                    location += f":{review['line']}"
                comment += f"- **{location}**: {review['title']} - {review['message']}\n"
                if review.get('suggestion'):
                    comment += f"  - 💡 建议: {review['suggestion']}\n"
            comment += "\n"

    comment += "\n---\n*此 review 由 Claude Code 自动生成*"
    return comment


def main():
    parser = argparse.ArgumentParser(description="GitHub PR 自动 Review")
    parser.add_argument("input", nargs="?", help="PR 编号、URL 或 owner/repo#pr")
    parser.add_argument("--repo", help="仓库 (owner/repo)")
    parser.add_argument("--post", action="store_true", help="将评论发布到 GitHub")
    parser.add_argument("--line-comments", action="store_true", help="提交行级评论（关联代码行）")
    parser.add_argument("--approve", action="store_true", help="批准 PR")
    parser.add_argument("--request-changes", action="store_true", help="请求修改")
    parser.add_argument("--json", action="store_true", help="输出 JSON 格式")
    parser.add_argument("--verify-lines", action="store_true", help="验证行级评论位置（不提交）")

    args = parser.parse_args()

    # 解析 PR 输入
    if not args.input:
        # 尝试从当前分支获取 PR
        returncode, stdout, _ = run_command(["git", "branch", "--show-current"], check=False)
        if returncode == 0:
            branch = stdout
            returncode, stdout, _ = run_command([
                "gh", "pr", "list", "--head", branch, "--json", "number"
            ], check=False)
            if returncode == 0 and stdout.strip():
                pr_data = json.loads(stdout)
                if pr_data:
                    args.input = str(pr_data[0]["number"])

        if not args.input:
            print_colored("错误: 请指定 PR 编号、URL 或 owner/repo#pr", Colors.FAIL)
            sys.exit(1)

    try:
        if args.repo:
            # 格式: --repo owner/repo 123
            repo = args.repo
            pr_number = int(args.input)
        else:
            repo, pr_number = parse_pr_input(args.input)
    except (ValueError, IndexError) as e:
        print_colored(f"错误: {e}", Colors.FAIL)
        sys.exit(1)

    # 获取 PR 信息
    pr_info = get_pr_info(repo, pr_number)

    # 获取 diff
    diff = get_pr_diff(repo, pr_number)

    # 分析文件变更
    file_changes = analyze_file_changes(diff)

    # 打印摘要
    print_pr_summary(pr_info, file_changes)
    print_file_changes(file_changes)

    # Review 代码
    print_colored("\n正在分析代码...", Colors.OKBLUE)
    reviews = review_code(pr_info, diff, file_changes)

    # 打印 review 结果
    print_reviews(reviews)

    # 获取 head SHA 用于行级评论
    head_sha = get_pr_head_sha(repo, pr_number)

    # 构建行级评论（如果启用）
    line_comment_builder = None
    if args.line_comments or args.verify_lines:
        print_colored("\n正在构建行级评论（基于内容定位）...", Colors.OKBLUE)
        line_comment_builder = LineCommentBuilder(repo, head_sha)
        
        # 将 review 结果转换为行级评论
        for review in reviews:
            if review.get('line') and review.get('file'):
                # 对于已有行号的 review，尝试验证
                file_content = line_comment_builder.get_file_content(review['file'])
                if file_content:
                    # 验证该行是否包含问题代码
                    lines = file_content.split('\n')
                    line_idx = review['line'] - 1
                    if 0 <= line_idx < len(lines):
                        line_content = lines[line_idx]
                        line_comment_builder.comments.append({
                            "path": review['file'],
                            "position": review['line'],
                            "body": f"{review['title']}: {review['message']}\n\n💡 建议: {review.get('suggestion', 'N/A')}",
                            "_verified_content": line_content.strip()[:60],
                            "_severity": review['severity']
                        })
        
        line_comment_builder.print_summary()
        
        if args.verify_lines:
            print_colored("\n✓ 行级评论位置验证完成", Colors.OKGREEN)
            return

    # 生成评论
    comment = generate_review_comment(pr_info, file_changes, reviews)

    # 保存评论到文件
    comment_file = Path("/tmp/pr_review_comment.md")
    comment_file.write_text(comment)
    print_colored(f"\n💾 Review 评论已保存到: {comment_file}", Colors.OKBLUE)

    # 发布到 GitHub
    if args.post:
        print_colored("\n正在发布评论到 GitHub...", Colors.OKBLUE)
        returncode, stdout, stderr = run_command([
            "gh", "pr", "comment", str(pr_number),
            "--repo", repo,
            "--body-file", str(comment_file)
        ], check=False)

        if returncode == 0:
            print_colored("✓ 评论已发布到 GitHub", Colors.OKGREEN)
        else:
            print_colored(f"发布失败: {stderr}", Colors.FAIL)
    elif args.line_comments and line_comment_builder and line_comment_builder.comments:
        # 提交行级评论
        print_colored("\n正在提交行级评论到 GitHub...", Colors.OKBLUE)
        
        # 确定 event 类型
        event = "COMMENT"
        if args.approve:
            event = "APPROVE"
        elif args.request_changes:
            event = "REQUEST_CHANGES"
        
        # 构建 payload
        payload = line_comment_builder.build_payload(comment, event)
        
        # 保存 payload 用于调试
        payload_file = Path("/tmp/pr_review_payload.json")
        payload_file.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
        print_colored(f"💾 Payload 已保存到: {payload_file}", Colors.OKBLUE)
        
        # 提交到 GitHub
        returncode, stdout, stderr = run_command([
            "gh", "api",
            "--method", "POST",
            "-H", "Accept: application/vnd.github+json",
            f"repos/{repo}/pulls/{pr_number}/reviews",
            "--input", str(payload_file)
        ], check=False)
        
        if returncode == 0:
            result = json.loads(stdout) if stdout else {}
            review_url = result.get('html_url', f'https://github.com/{repo}/pull/{pr_number}')
            print_colored(f"✓ 行级评论已发布: {review_url}", Colors.OKGREEN)
        else:
            print_colored(f"发布失败: {stderr}", Colors.FAIL)
    else:
        print_colored("\n提示:", Colors.OKCYAN)
        print("  --post           发布整体评论")
        print("  --line-comments  发布行级评论（推荐）")
        print("  --approve        批准 PR（可与 --line-comments 组合）")
        print("  --request-changes 请求修改（可与 --line-comments 组合）")

    # JSON 输出
    if args.json:
        print("\n" + json.dumps({
            "pr_info": pr_info,
            "file_changes": file_changes,
            "reviews": reviews
        }, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
