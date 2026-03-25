---
name: test-ansible-deploy
description: This skill should be used when the user asks to "test ansible deployment", "verify ansible deploy", "/test-ansible", or needs to validate the deploy/ansible deployment solution for FalconFS. The skill automates testing the Ansible deployment by setting up Docker containers and running the deployment playbooks.
version: 2.9.0
---

# Ansible 部署验证 Skill

## 概述

此 skill 用于验证 FalconFS 的 Ansible 部署方案。通过 Docker 容器模拟多节点环境，执行完整的部署流程测试。

**部署架构：**
- **CN (Compute Node)**: 1 个节点 - 负责编译和协调
- **DN (Data Node)**: 2 个节点 - 数据存储
- **Client**: 3 个节点（与 CN/DN 共用）

**关键目录：**
- `remote_src_dir`: 源码目录，默认 `/home/falcon/code/falconfs`
- `FALCONFS_INSTALL_DIR`: 安装目录，默认 `/usr/local/falconfs`

## 构建模式

Playbook 支持两种构建模式，通过 `falcon_build_mode` 变量控制：

### 模式 1: distributed（生产环境，默认）

```
┌─────────────────────────────────────────────────────────────────┐
│                    distributed 模式                              │
├─────────────────────────────────────────────────────────────────┤
│  install-deps  →  所有节点（CN/DN）并发安装依赖                    │
│       ↓                                                         │
│  build         →  所有节点（CN/DN）并发编译安装                    │
│       ↓                                                         │
│  start         →  直接启动（跳过 package/distribute）             │
│       ↓                                                         │
│  verify        →  验证状态                                       │
└─────────────────────────────────────────────────────────────────┘

使用命令:
  ansible-playbook falcontest.yml --tags "install-deps,build,start,verify"
```

### 模式 2: centralized（容器测试环境，资源受限环境）

```
┌─────────────────────────────────────────────────────────────────┐
│                    centralized 模式                              │
├─────────────────────────────────────────────────────────────────┤
│  install-deps  →  仅 CN 节点安装依赖                              │
│       ↓                                                         │
│  build         →  仅 CN 节点编译安装                              │
│       ↓                                                         │
│  package       →  CN 打包 /usr/local/falconfs                    │
│       ↓                                                         │
│  distribute    →  CN 分发到 DN 节点的 /usr/local/falconfs        │
│       ↓                                                         │
│  start         →  启动服务                                       │
│       ↓                                                         │
│  verify        →  验证状态                                       │
└─────────────────────────────────────────────────────────────────┘

使用命令:
  # 设置 inventory 中 falcon_build_mode="centralized"
  ansible-playbook falcontest.yml --tags "install-deps,build,package,distribute,start,verify"
```

**重要**: centralized 模式下，DN 节点仍需要 PostgreSQL 和 boost 等运行时依赖！
使用中间镜像可以确保所有节点都有必要的依赖。

**模式选择建议：**
| 场景 | 推荐模式 | 原因 |
|------|----------|------|
| 生产环境多主机 | distributed | 充分利用多主机资源，无单点瓶颈 |
| 容器测试环境 | centralized | 避免主机资源耗尽，节省编译时间 |
| CI/CD 流水线 | centralized | 资源可控，可重复性高 |

## 前置要求

1. **Docker 环境** - 确保 Docker 正常运行
2. **基础镜像** - `127.0.0.1:5000/ubuntu:24.04`
3. **中间镜像** - `127.0.0.1:5000/falconfs-ansible-test:latest`（包含 PostgreSQL 17.2, boost 等）
4. **端口可用** - 2221-2223, 5442-5444, 5550-5552, 56039
5. **网络访问** - 能访问 GitHub 和 PostgreSQL 官方下载站点
6. **宿主机 FUSE 支持** - 宿主机需加载 fuse 内核模块（`modprobe fuse`）

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
./run-ansible-test.sh verify            # 验证状态

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

## 测试步骤

### 步骤 1：构建中间镜像（首次或依赖更新时）

**重要**: 中间镜像包含 PostgreSQL 17.2、boost、brpc 等所有编译和运行依赖。
使用中间镜像可以大幅加速测试，并确保 DN 节点有必要的运行时依赖。

```bash
# 从项目根目录构建中间镜像
cd /path/to/falconfs

# 构建中间镜像（约 15-20 分钟）
docker build -f .claude/skills/test-ansible-deploy/Dockerfile.intermediate \
  -t 127.0.0.1:5000/falconfs-ansible-test:latest .

# 推送到本地仓库（可选）
docker push 127.0.0.1:5000/falconfs-ansible-test:latest

# 验证镜像
docker images | grep falconfs-ansible-test
```

**中间镜像包含的依赖：**
- PostgreSQL 17.2（编译安装到 /usr/local/pgsql）
- GCC/G++ 14
- boost 库（system, thread, filesystem, program-options）
- brpc, glog, protobuf, leveldb
- OBS SDK, prometheus-cpp
- SSH 服务器，已配置 falcon 用户

### 步骤 2：环境检查

```bash
# 检查 Docker
docker info > /dev/null 2>&1 && echo "Docker 正常" || echo "Docker 未运行"

# 检查中间镜像
docker images | grep "127.0.0.1:5000/falconfs-ansible-test" || echo "⚠️ 中间镜像不存在，请先执行步骤 1"

# 检查端口
ss -tuln | grep -E ":(2221|2222|2223|5442|5550|5551)" || echo "端口可用"
```

### 步骤 3：清理旧环境

```bash
docker stop falcon-ansible-test-cn falcon-ansible-test-dn0 falcon-ansible-test-dn1 2>/dev/null
docker rm falcon-ansible-test-cn falcon-ansible-test-dn0 falcon-ansible-test-dn1 2>/dev/null
docker network rm falcon-ansible-test-net 2>/dev/null
rm -f /tmp/falcon_ansible_test_key* /tmp/test_inventory /tmp/falconfs-*.tar.gz /tmp/pgsql.tar.gz
```

### 步骤 4：创建网络和启动容器

```bash
# 创建网络
docker network create falcon-ansible-test-net

# 启动 CN 容器（使用中间镜像）
docker run -d --name falcon-ansible-test-cn --network falcon-ansible-test-net \
  -p 2221:22 -p 5442:5442 -p 5550:5550 --privileged \
  127.0.0.1:5000/falconfs-ansible-test:latest

# 启动 DN0 容器（使用中间镜像）
docker run -d --name falcon-ansible-test-dn0 --network falcon-ansible-test-net \
  -p 2222:22 -p 5443:5442 -p 5551:5551 --privileged \
  127.0.0.1:5000/falconfs-ansible-test:latest

# 启动 DN1 容器（使用中间镜像）
docker run -d --name falcon-ansible-test-dn1 --network falcon-ansible-test-net \
  -p 2223:22 -p 5444:5442 -p 5552:5552 --privileged \
  127.0.0.1:5000/falconfs-ansible-test:latest

# 获取容器 IP
CN_IP=$(docker inspect falcon-ansible-test-cn | grep '"IPAddress"' | head -1 | awk -F'"' '{print $4}')
DN0_IP=$(docker inspect falcon-ansible-test-dn0 | grep '"IPAddress"' | head -1 | awk -F'"' '{print $4}')
DN1_IP=$(docker inspect falcon-ansible-test-dn1 | grep '"IPAddress"' | head -1 | awk -F'"' '{print $4}')

echo "CN_IP=$CN_IP, DN0_IP=$DN0_IP, DN1_IP=$DN1_IP"
```

### 步骤 5：配置 SSH 密钥

```bash
# 生成 SSH 密钥
ssh-keygen -t rsa -f /tmp/falcon_ansible_test_key -N "" -q
PUB_KEY=$(cat /tmp/falcon_ansible_test_key.pub)

# 配置所有容器的 SSH
for c in cn dn0 dn1; do
  docker exec falcon-ansible-test-$c bash -c "
    echo '$PUB_KEY' > /home/falcon/.ssh/authorized_keys && \
    chown -R falcon:falcon /home/falcon/.ssh && \
    chmod 600 /home/falcon/.ssh/authorized_keys
  "
done

echo "✅ SSH 密钥配置完成"
```

### 步骤 6：准备 Inventory 文件

创建 `/tmp/test_inventory`（**容器测试必须使用 centralized 模式**）：

```bash
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

echo "✅ Inventory 创建完成 (falcon_build_mode=centralized)"
```

### 步骤 7：安装 Ansible 和复制部署文件

```bash
# 在 CN 容器中安装 Ansible
docker exec falcon-ansible-test-cn bash -c "
  apt-get update -qq
  apt-get install -y -qq ansible curl sshpass > /dev/null 2>&1
  mkdir -p /home/falcon/code/ansible /home/falcon/.ansible
"

# 复制部署文件到 CN 容器
docker cp /tmp/test_inventory falcon-ansible-test-cn:/home/falcon/code/ansible/inventory
docker cp deploy/ansible/falcontest.yml falcon-ansible-test-cn:/home/falcon/code/ansible/
docker cp deploy/ansible/install-ubuntu24.04.sh falcon-ansible-test-cn:/home/falcon/code/ansible/
docker cp deploy/ansible/install-ubuntu22.04.sh falcon-ansible-test-cn:/home/falcon/code/ansible/
docker cp /tmp/falcon_ansible_test_key falcon-ansible-test-cn:/home/falcon/.ssh/id_rsa
docker cp /tmp/falcon_ansible_test_key.pub falcon-ansible-test-cn:/home/falcon/.ssh/id_rsa.pub

# 配置权限和 ansible.cfg
docker exec falcon-ansible-test-cn bash -c "
  chown -R falcon:falcon /home/falcon/code /home/falcon/.ssh /home/falcon/.ansible
  chmod 600 /home/falcon/.ssh/id_rsa
  chmod 644 /home/falcon/.ssh/id_rsa.pub
  cat > /home/falcon/.ansible.cfg << 'EOF'
[defaults]
inventory = ~/code/ansible/inventory
log_path = ~/code/ansible/ansible.log
host_key_checking = False
private_key_file = ~/.ssh/id_rsa
EOF
  chown falcon:falcon /home/falcon/.ansible.cfg
"

# 测试连接
docker exec falcon-ansible-test-cn bash -c "su - falcon -c 'ansible all -m ping'"

echo "✅ 部署文件准备完成"
```

### 步骤 8：执行 Ansible 部署（centralized 模式）

**容器测试环境必须按以下顺序执行**：

```bash
# 1. 编译和安装（仅在 CN 节点，从 GitHub 克隆源码）
#    使用中间镜像时，可以跳过 install-deps（中间镜像已包含所有依赖）
#    如需执行: ansible-playbook falcontest.yml --tags install-deps
docker exec falcon-ansible-test-cn bash -c "su - falcon -c 'cd ~/code/ansible && ansible-playbook falcontest.yml --tags build'"

# 2. 打包构建产物
docker exec falcon-ansible-test-cn bash -c "su - falcon -c 'cd ~/code/ansible && ansible-playbook falcontest.yml --tags package'"

# 3. 分发到 DN 节点（如步骤 9 所述）
docker exec falcon-ansible-test-cn bash -c "su - falcon -c 'cd ~/code/ansible && ansible-playbook falcontest.yml --tags distribute'"
```

**⚡ 提示：使用中间镜像时跳过 install-deps**

中间镜像 `127.0.0.1:5000/falconfs-ansible-test:latest` 已包含所有运行时依赖（PostgreSQL, boost, brpc 等），因此在容器测试环境中可以跳过 `install-deps` 步骤，大幅节省时间。

如果网络无法访问 GitHub（install-deps 需要下载依赖），**必须跳过此步骤**。

```bash
# 完整命令（跳过 install-deps）
docker exec falcon-ansible-test-cn bash -c "su - falcon -c 'cd ~/code/ansible && ansible-playbook falcontest.yml --tags build,package,distribute'"
```

**⚠️ 严重警告：禁止复制本地源码到容器**

```bash
# ❌ 错误做法 - 绝对不要这样做！
docker cp /path/to/local/falconfs falcon-ansible-test-cn:/home/falcon/code/
# 或者
tar -czf falconfs-src.tar.gz /path/to/local/falconfs
docker cp falconfs-src.tar.gz falcon-ansible-test-cn:/tmp/

# ✅ 正确做法 - 必须使用 Ansible build tag 从 Git 克隆源码
docker exec falcon-ansible-test-cn bash -c "su - falcon -c 'cd ~/code/ansible && ansible-playbook falcontest.yml --tags build'"
```

**原因**：本地编译的 `libbrpcplugin.so` 会链接本地环境的 `libprotobuf.so.X`，但容器内的 protobuf 版本可能不同，导致运行时错误：`libprotobuf.so.XX: cannot open shared object file`

**playbook 已包含 clean 步骤**：`falcontest.yml` 的 build task 会在编译前自动执行 `./build.sh clean`，确保干净的编译环境。

### 步骤 9：分发到 DN 节点

**使用 Ansible distribute tag（已验证可正常工作）**：

```bash
# 使用 Ansible distribute tag 分发包到 DN 节点
docker exec falcon-ansible-test-cn bash -c "su - falcon -c 'cd ~/code/ansible && ansible-playbook falcontest.yml --tags distribute'"

# ⚠️ 注意：如果 DN 节点上已存在空的 /usr/local/falconfs 目录（来自之前运行），需要先清理：
# docker exec -u root falcon-ansible-test-dn0 rm -rf /usr/local/falconfs
# docker exec -u root falcon-ansible-test-dn1 rm -rf /usr/local/falconfs
```

### 步骤 10：启动服务

```bash
# 启动服务
docker exec falcon-ansible-test-cn bash -c "su - falcon -c 'cd ~/code/ansible && ansible-playbook falcontest.yml --tags start'"
```

### 步骤 11：验证部署状态

```bash
# 验证所有节点
for node in cn dn0 dn1; do
  echo "=== $node ==="
  docker exec falcon-ansible-test-$node bash -c "
    echo 'PostgreSQL 进程:' && ps aux | grep 'postgres.*-D' | grep -v grep | head -2
    echo 'Client 进程:' && ps aux | grep falcon_client | grep -v grep | head -1
    echo 'FUSE 挂载:' && mount | grep fuse || echo '  无 FUSE 挂载'
  "
done

# 测试 PostgreSQL 连接
echo ""
echo "=== PostgreSQL 连接测试 ==="
docker exec falcon-ansible-test-dn0 /usr/local/pgsql/bin/pg_isready -h $CN_IP -p 55500
docker exec falcon-ansible-test-dn1 /usr/local/pgsql/bin/pg_isready -h $CN_IP -p 55500
docker exec falcon-ansible-test-cn /usr/local/pgsql/bin/pg_isready -h $DN0_IP -p 55520
docker exec falcon-ansible-test-cn /usr/local/pgsql/bin/pg_isready -h $DN1_IP -p 55520
```

### 步骤 12：清理

```bash
docker stop falcon-ansible-test-cn falcon-ansible-test-dn0 falcon-ansible-test-dn1
docker rm falcon-ansible-test-cn falcon-ansible-test-dn0 falcon-ansible-test-dn1
docker network rm falcon-ansible-test-net
rm -f /tmp/falcon_ansible_test_key* /tmp/test_inventory /tmp/falconfs-*.tar.gz /tmp/pgsql.tar.gz
```

## FALCONFS_INSTALL_DIR 目录结构

`build.sh install` 会将以下内容安装到 `/usr/local/falconfs`:

```
/usr/local/falconfs/
├── falcon_meta/          # PostgreSQL 扩展（falcon.so）
│   ├── lib/postgresql/
│   └── share/extension/
├── falcon_client/        # 客户端二进制和配置
│   ├── bin/falcon_client
│   └── config/config.json
├── falcon_store/         # 存储组件
├── deploy/               # 部署脚本（关键！）
│   ├── falcon_env.sh     # 环境变量配置
│   ├── meta/
│   │   ├── falcon_meta_config.sh
│   │   ├── falcon_meta_start.sh
│   │   └── falcon_meta_stop.sh
│   └── client/
│       ├── falcon_client_start.sh
│       └── falcon_client_stop.sh
├── bin/                  # 通用二进制
└── lib/                  # 共享库
```

## 已知问题和解决方案

### 1. 中间镜像未构建

**问题**: DN 节点缺少 PostgreSQL 或 boost 库

**解决方案**: 必须先构建中间镜像（步骤 1），中间镜像包含所有运行时依赖

### 2. /usr/local/falconfs 目录已存在导致分发失败

**问题**: Ansible distribute tag 执行后，DN 节点 Extraction 步骤被跳过

**原因**: playbook 的 unarchive 任务使用 `creates: "{{ falconfs_install_dir }}"` 判断是否需要解压。如果 `/usr/local/falconfs` 目录已存在（即使是空目录），则会跳过解压步骤。中间镜像或之前运行可能已创建此目录。

**解决方案**: 在执行 distribute tag 前清理 DN 节点上的目录：
```bash
# 清理 DN 节点上的旧目录
docker exec -u root falcon-ansible-test-dn0 rm -rf /usr/local/falconfs
docker exec -u root falcon-ansible-test-dn1 rm -rf /usr/local/falconfs

# 然后重新执行 distribute
docker exec falcon-ansible-test-cn bash -c "su - falcon -c 'cd ~/code/ansible && ansible-playbook falcontest.yml --tags distribute'"
```

### 3. 网络无法访问 GitHub（install-deps 超时）

**问题**: 容器内无法访问 GitHub 或 PostgreSQL 官方源，导致 `install-deps` 步骤超时

**原因**: Docker 容器网络隔离，可能无法访问外网

**解决方案**: 使用中间镜像时跳过 `install-deps` 步骤（中间镜像已包含所有依赖）

```bash
# 跳过 install-deps，直接执行后续步骤
docker exec falcon-ansible-test-cn bash -c "su - falcon -c 'cd ~/code/ansible && ansible-playbook falcontest.yml --tags build,package,distribute'"
```

**验证网络连通性**:
```bash
docker exec falcon-ansible-test-cn curl -I --connect-timeout 10 https://github.com
# 如果超时，需要跳过 install-deps
```

### 3. locale 问题

**问题**: PostgreSQL 启动需要 en_US.UTF-8 locale

**解决方案**: 中间镜像已包含 locale 配置，无需额外处理

### 4. 容器启动命令

**问题**: Ubuntu 24.04 基础镜像不支持 `/sbin/init`

**解决方案**: 中间镜像已配置 sshd 作为默认启动命令

### 5. FUSE 挂载失败

**问题**: Client 启动后 FUSE 挂载失败，文件操作报错 "Permission denied" 或 "Operation not permitted"

**原因**: 容器需要 `--privileged` 权限才能挂载 FUSE 文件系统

**解决方案**: 
```bash
# 确保容器以 --privileged 模式启动
docker run -d --name falcon-ansible-test-cn --privileged ...

# 验证 FUSE 设备可用
docker exec falcon-ansible-test-cn ls -la /dev/fuse
# 应该显示: crw-rw-rw- 1 root root 10, 229 ... /dev/fuse

# 验证 FUSE 文件系统支持
docker exec falcon-ansible-test-cn cat /proc/filesystems | grep fuse
# 应该显示:
#   fuseblk
#   nodev	fuse
#   nodev	fusectl
```

**注意**: 如果宿主机未加载 fuse 模块，即使容器有 `--privileged` 权限也无法挂载 FUSE：
```bash
# 在宿主机上执行
sudo modprobe fuse
```

### 6. PostgreSQL 插件加载失败（libprotobuf 版本不匹配）

**问题**: 日志显示 `libprotobuf.so.23: cannot open shared object file`

**原因**: 中间镜像的 libprotobuf 版本与编译时使用的版本不匹配

**解决方案**: 
1. 确保使用相同的中间镜像进行编译和运行
2. 中间镜像包含的 protobuf 版本会与编译产物自动匹配
3. 不要单独升级中间镜像中的 protobuf

### 7. 【严重】禁止复制本地源码到容器（libbrpcplugin.so 版本不匹配）

**问题**: 复制本地已编译的源码到容器后，PostgreSQL 启动失败，日志显示：
```
libprotobuf.so.XX: cannot open shared object file
ERROR: could not load library "/usr/local/pgsql/lib/falcon.so": libbrpcplugin.so: cannot open shared object file
```

**根本原因**: 
- 本地编译的 `libbrpcplugin.so` 链接了本地环境的 `libprotobuf.so.XX`
- 容器内的 protobuf 版本可能与本地不同（如本地是 libprotobuf.so.23，容器是 libprotobuf.so.32）
- 复制本地源码时，已编译的二进制文件（.so 文件）会保留对本地库的依赖

**验证方法**:
```bash
# 检查 libbrpcplugin.so 链接的 protobuf 版本
docker exec falcon-ansible-test-cn ldd /usr/local/falconfs/falcon_meta/lib/postgresql/libbrpcplugin.so | grep protobuf

# 正确输出（链接到容器内的版本）:
#   libprotobuf.so.32 => /usr/local/lib/libprotobuf.so.32

# 错误输出（链接到本地版本，容器内不存在）:
#   libprotobuf.so.23 => not found
```

**解决方案**: 
1. ✅ **必须使用 Ansible build tag 从 Git 克隆源码并编译**
2. ✅ **Playbook 会在编译前自动执行 `./build.sh clean`**
3. ❌ **绝对禁止复制本地源码目录到容器**
4. ❌ **禁止复制本地已编译的 build 目录**

**正确做法**:
```bash
# 使用 Ansible build tag（会从 Git 克隆源码）
docker exec falcon-ansible-test-cn bash -c "su - falcon -c 'cd ~/code/ansible && ansible-playbook falcontest.yml --tags build'"
```

**错误做法**:
```bash
# ❌ 绝对不要这样做！
docker cp /path/to/local/falconfs falcon-ansible-test-cn:/home/falcon/code/
# 或者
tar -czf -C /path/to/local falconfs | docker exec -i falcon-ansible-test-cn tar -xzf - -C /home/falcon/code/
```

**如果已经错误地复制了本地源码，必须清理后重新编译**:
```bash
# 清理错误的源码和安装
for c in cn dn0 dn1; do
  docker exec falcon-ansible-test-$c bash -c "
    rm -rf /home/falcon/code/falconfs
    rm -rf /usr/local/falconfs
    rm -rf /home/falcon/metadata
  "
done

# 重新执行 Ansible build tag（从 Git 克隆源码）
docker exec falcon-ansible-test-cn bash -c "su - falcon -c 'cd ~/code/ansible && ansible-playbook falcontest.yml --tags build'"
```

## Inventory 关键配置

```ini
# 必须包含 PostgreSQL 路径
extra_ld_library_path="/usr/local/pgsql/lib:/usr/local/lib:/usr/local/obs/lib:/usr/local/lib64"
extra_path="/usr/local/pgsql/bin:/home/falcon/metadb/bin"

# 构建模式选择
# distributed: 所有节点并发编译（生产环境）
# centralized: 仅 CN 编译后分发（容器测试）
falcon_build_mode="centralized"
```

## 文件结构

```
.claude/skills/test-ansible-deploy/
├── SKILL.md                    # Skill 定义文件（本文件）
├── README.md                   # 简要使用说明
├── Dockerfile.intermediate     # 中间镜像构建文件
└── run-ansible-test.sh        # 自动化执行脚本
```

## 版本历史

- **v2.9.0** - 新增: 自动化执行脚本 run-ansible-test.sh，降低 prompt 调用次数
- **v2.8.0** - 更新: 使用中间镜像时可跳过 install-deps 步骤；新增问题说明：网络无法访问 GitHub 时需跳过 install-deps
- **v2.7.0** - 更新: Ansible distribute tag 已验证可用，移除手动分发步骤；新增问题说明：/usr/local/falconfs 目录已存在导致分发失败
- **v2.6.0** - 【严重警告】添加禁止复制本地源码的说明；强调必须从 Git 克隆源码并 clean 后编译；新增问题 #7 说明 libbrpcplugin.so 版本不匹配的根本原因和解决方案
- **v2.5.0** - 优化: 推荐手动分发替代 Ansible synchronize；更新验证步骤；添加 PostgreSQL 插件加载问题说明
- **v2.4.0** - 新增: FUSE 挂载问题解决方案，强调 --privileged 权限的重要性
- **v2.3.0** - 新增: 中间镜像构建步骤，确保 DN 节点有运行时依赖
- **v2.2.0** - 新增: 两种构建模式（distributed/centralized）
- **v2.1.0** - 修复: 添加 build 步骤、修正分发目录
- **v2.0.0** - 更新为手动测试指南
- **v1.0.0** - 初始版本
