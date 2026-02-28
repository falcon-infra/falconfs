# Ansible 部署验证 Skill

## 概述

此 skill 用于验证 FalconFS 的 Ansible 部署方案。通过 Docker 容器模拟多节点环境（1 CN + 2 DN），执行完整的部署流程测试。

**版本**: v2.8.0

## 部署架构

- **CN (Compute Node)**: 1 个节点 - 负责编译和协调
- **DN (Data Node)**: 2 个节点 - 数据存储
- **Client**: 3 个节点（与 CN/DN 共用）

## ⚠️ 严重警告

**禁止复制本地源码到容器！**

```bash
# ❌ 绝对不要这样做！
docker cp /path/to/local/falconfs falcon-ansible-test-cn:/home/falcon/code/

# ✅ 必须使用 Ansible build tag 从 Git 克隆源码
docker exec falcon-ansible-test-cn bash -c "su - falcon -c 'cd ~/code/ansible && ansible-playbook falcontest.yml --tags build'"
```

**原因**: 本地编译的 `libbrpcplugin.so` 会链接本地环境的 `libprotobuf.so.XX`，但容器内的 protobuf 版本可能不同，导致运行时错误：
```
libprotobuf.so.XX: cannot open shared object file
ERROR: could not load library "falcon.so"
```

**验证方法**:
```bash
# 检查 libbrpcplugin.so 链接的 protobuf 版本
docker exec falcon-ansible-test-cn ldd /usr/local/falconfs/falcon_meta/lib/postgresql/libbrpcplugin.so | grep protobuf

# 正确输出（链接到容器内的版本）:
#   libprotobuf.so.32 => /usr/local/lib/libprotobuf.so.32

# 错误输出（链接到本地版本，容器内不存在）:
#   libprotobuf.so.23 => not found
```

## 构建模式

| 模式 | 说明 | 适用场景 |
|------|------|----------|
| distributed | 所有节点并发编译 | 生产环境（默认） |
| centralized | 仅 CN 编译，然后分发 | 容器测试 |

## 前置要求

1. **Docker 环境** - 确保 Docker 正常运行
2. **基础镜像** - `127.0.0.1:5000/ubuntu:24.04`
3. **中间镜像** - `127.0.0.1:5000/falconfs-ansible-test:latest`
4. **端口可用** - 2221-2223, 5442-5444, 5550-5552, 56039
5. **宿主机 FUSE 支持** - 宿主机需加载 fuse 内核模块（`modprobe fuse`）

**重要**: 容器必须以 `--privileged` 模式运行，否则 FUSE 文件系统挂载会失败。

## 快速开始：使用执行脚本

推荐使用自动化执行脚本 `run-ansible-test.sh` 来简化测试流程：

```bash
cd /path/to/falconfs/.claude/skills/test-ansible-deploy

# 完整流程（自动执行所有步骤）
./run-ansible-test.sh full

# 跳过 install-deps（使用中间镜像时）
./run-ansible-test.sh full --skip-deps

# 仅执行特定步骤
./run-ansible-test.sh clean              # 清理环境
./run-ansible-test.sh build              # 构建中间镜像
./run-ansible-test.sh start              # 启动容器并配置
./run-ansible-test.sh deploy --tags="build,package,distribute"  # 执行部署
./run-ansible-test.sh verify             # 验证状态

# rebuild = 清理 + 完整流程
./run-ansible-test.sh rebuild --skip-deps
```

**脚本支持的模式：**
| 模式 | 说明 |
|------|------|
| `full` | 完整流程: 清理 → 启动容器 → 部署 → 验证 |
| `build` | 仅构建中间镜像 |
| `start` | 启动容器并配置 (不执行部署) |
| `deploy` | 执行部署 (假设容器已启动) |
| `verify` | 仅验证部署状态 |
| `clean` | 仅清理环境 |
| `rebuild` | 清理 + 完整流程 |

**脚本支持的选项：**
| 选项 | 说明 |
|------|------|
| `--tags=TAGS` | 执行特定的 ansible tags |
| `--skip-deps` | 跳过 install-deps 步骤 |
| `--mode=MODE` | 构建模式: centralized (默认) 或 distributed |
| `--no-verify` | 执行部署后不自动验证 |
| `--verbose` | 显示详细输出 |

---

## 手动测试步骤

### 步骤 1：构建中间镜像（首次需要）

```bash
# 方法 A: 从 Dockerfile 构建（约 15-20 分钟）
docker build -f .claude/skills/test-ansible-deploy/Dockerfile.intermediate \
  -t 127.0.0.1:5000/falconfs-ansible-test:latest .

# 方法 B: 从已配置的 CN 容器创建（更快）
docker commit falcon-ansible-test-cn 127.0.0.1:5000/falconfs-ansible-test:latest
docker push 127.0.0.1:5000/falconfs-ansible-test:latest
```

### 步骤 2：启动容器

```bash
# 清理旧环境
docker stop falcon-ansible-test-cn falcon-ansible-test-dn0 falcon-ansible-test-dn1 2>/dev/null
docker rm falcon-ansible-test-cn falcon-ansible-test-dn0 falcon-ansible-test-dn1 2>/dev/null
docker network rm falcon-ansible-test-net 2>/dev/null

# 创建网络
docker network create falcon-ansible-test-net

# 启动容器（使用中间镜像）
docker run -d --name falcon-ansible-test-cn --network falcon-ansible-test-net \
  -p 2221:22 -p 5442:5442 -p 5550:5550 --privileged \
  127.0.0.1:5000/falconfs-ansible-test:latest

docker run -d --name falcon-ansible-test-dn0 --network falcon-ansible-test-net \
  -p 2222:22 -p 5443:5442 -p 5551:5551 --privileged \
  127.0.0.1:5000/falconfs-ansible-test:latest

docker run -d --name falcon-ansible-test-dn1 --network falcon-ansible-test-net \
  -p 2223:22 -p 5444:5442 -p 5552:5552 --privileged \
  127.0.0.1:5000/falconfs-ansible-test:latest
```

### 步骤 3：配置 SSH 和 Inventory

```bash
# 生成 SSH 密钥
ssh-keygen -t rsa -f /tmp/falcon_ansible_test_key -N "" -q
PUB_KEY=$(cat /tmp/falcon_ansible_test_key.pub)

# 配置 SSH
for c in cn dn0 dn1; do
  docker exec falcon-ansible-test-$c bash -c "
    echo '$PUB_KEY' > /home/falcon/.ssh/authorized_keys && \
    chown -R falcon:falcon /home/falcon/.ssh && \
    chmod 600 /home/falcon/.ssh/authorized_keys
  "
done

# 获取容器 IP
CN_IP=$(docker inspect falcon-ansible-test-cn | grep '"IPAddress"' | head -1 | awk -F'"' '{print $4}')
DN0_IP=$(docker inspect falcon-ansible-test-dn0 | grep '"IPAddress"' | head -1 | awk -F'"' '{print $4}')
DN1_IP=$(docker inspect falcon-ansible-test-dn1 | grep '"IPAddress"' | head -1 | awk -F'"' '{print $4}')

# 创建 Inventory
cat > /tmp/test_inventory << EOF
[falconcn]
cn ansible_host=$CN_IP ansible_user=falcon localip="$CN_IP"

[falcondn]
dn0 ansible_host=$DN0_IP ansible_user=falcon localip="$DN0_IP"
dn1 ansible_host=$DN1_IP ansible_user=falcon localip="$DN1_IP"

[falconclient]
client0 ansible_host=$CN_IP ansible_user=falcon localip="$CN_IP"
client1 ansible_host=$DN0_IP ansible_user=falcon localip="$DN0_IP"
client2 ansible_host=$DN1_IP ansible_user=falcon localip="$DN1_IP"

[falconmeta:children]
falconcn
falcondn

[falconall:children]
falconmeta
falconclient

[falconall:vars]
ansible_become_password="falcon"
extra_ld_library_path="/usr/local/pgsql/lib:/usr/local/lib:/usr/local/obs/lib:/usr/local/lib64"
extra_path="/usr/local/pgsql/bin:/home/falcon/metadb/bin"
falcon_repo_url="https://github.com/falcon-infra/falconfs.git"
falcon_repo_version="main"
remote_src_dir="/home/falcon/code/falconfs"
falconfs_install_dir="/usr/local/falconfs"
falcon_build_mode="centralized"
falcon_server_port_prefix="555"
falcon_client_port="56039"
falcon_client_log_path="/tmp"
falcon_client_cache_path="/tmp/falcon_cache"
mount_path="/home/falcon/mnt"
EOF
```

### 步骤 4：执行部署（centralized 模式）

```bash
# 安装 Ansible
docker exec falcon-ansible-test-cn bash -c "
  apt-get update -qq
  apt-get install -y -qq ansible curl sshpass > /dev/null 2>&1
  mkdir -p /home/falcon/code/ansible /home/falcon/.ansible
"

# 复制部署文件
docker cp /tmp/test_inventory falcon-ansible-test-cn:/home/falcon/code/ansible/inventory
docker cp deploy/ansible/falcontest.yml falcon-ansible-test-cn:/home/falcon/code/ansible/
docker cp /tmp/falcon_ansible_test_key falcon-ansible-test-cn:/home/falcon/.ssh/id_rsa
docker exec falcon-ansible-test-cn chown -R falcon:falcon /home/falcon/code /home/falcon/.ssh

# 一键执行部署
docker exec falcon-ansible-test-cn bash -c "su - falcon -c 'cd ~/code/ansible && ansible-playbook falcontest.yml --tags \"install-deps,build,package,distribute,start,verify\"'"
```

## 关键目录

| 目录 | 说明 |
|------|------|
| `remote_src_dir` | 源码目录，默认 `/home/falcon/code/falconfs` |
| `FALCONFS_INSTALL_DIR` | 安装目录，默认 `/usr/local/falconfs` |

## 验证检查项

- [ ] CN 节点: PostgreSQL coordinator 进程运行，端口 55500/55510 监听
- [ ] DN 节点: PostgreSQL worker 进程运行，端口 55520 监听
- [ ] Client: falcon_client 进程运行（如启用）

## 清理测试环境

```bash
docker stop falcon-ansible-test-cn falcon-ansible-test-dn0 falcon-ansible-test-dn1
docker rm falcon-ansible-test-cn falcon-ansible-test-dn0 falcon-ansible-test-dn1
docker network rm falcon-ansible-test-net
rm -f /tmp/falcon_ansible_test_key* /tmp/test_inventory /tmp/falconfs-*.tar.gz /tmp/pgsql.tar.gz
```

## 文件结构

```
.claude/skills/test-ansible-deploy/
├── SKILL.md                    # 详细测试指南（完整版）
├── README.md                   # 本文件（简要说明）
├── Dockerfile.intermediate     # 中间镜像构建文件
└── run-ansible-test.sh        # 自动化执行脚本
```

## 版本历史

- **v2.9.0** - 新增: 自动化执行脚本 run-ansible-test.sh，降低 prompt 调用次数
- **v2.8.0** - 更新: 使用中间镜像时可跳过 install-deps 步骤
- **v2.7.0** - 更新: Ansible distribute tag 已验证可用，移除手动分发步骤
- **v2.6.0** - 【严重警告】添加禁止复制本地源码的说明；强调必须从 Git 克隆源码
- **v2.5.0** - 优化: 推荐手动分发替代 Ansible synchronize
- **v2.4.0** - 新增: FUSE 挂载问题解决方案，强调 --privileged 权限的重要性
- **v2.3.0** - 新增: 中间镜像构建步骤，确保 DN 节点有运行时依赖
- **v2.2.0** - 新增: 两种构建模式（distributed/centralized）
- **v2.1.0** - 修复: 添加 build 步骤、修正分发目录
- **v2.0.0** - 更新为手动测试指南
- **v1.0.0** - 初始版本
