# FalconFS RPM 打包说明（source spec）

本文档说明如何基于 `rpm/falconfs.source.spec` 打包 FalconFS。

该 spec 目前支持两种包形态：

- `falconfs`（完整包，默认）
- `falconfs-release`（发布精简包，通过 `--define 'release_pkg 1'` 开启）

## 1. 前置环境

### 1.1 基础工具

```bash
sudo dnf install -y rpmdevtools dnf-plugins-core
```

### 1.2 构建依赖

推荐直接按 spec 安装 BuildRequires：

```bash
sudo dnf builddep -y rpm/falconfs.source.spec
```

### 1.3 外部预编译依赖（必须）

本 spec 不负责编译这些第三方组件，需提前在构建机准备好：

- PostgreSQL（含 `pg_config`，路径为 `/usr/local/pgsql/bin/pg_config`）
- brpc `1.14.1`
- spdlog `1.12.0`
- OBS SDK（路径为 `/usr/local/obs`）
- ZooKeeper（建议 `3.9.x`，用于集群/回归验证）

如果你是按“预编译依赖 + source spec”方案打包，上述组件都应先安装完成。

快速检查：

```bash
test -x /usr/local/pgsql/bin/pg_config
ls -ld /usr/local/obs /usr/local/obs/include /usr/local/obs/lib
ldconfig -p | grep -E 'libbrpc|libspdlog' || true
```

## 2. 准备 SOURCES

当前 spec 只需要一个源码包：

- `falconfs-0.1.0.tar.gz`

在仓库根目录执行：

```bash
rpmdev-setuptree
VERSION=0.1.0
git archive --format=tar.gz --prefix="falconfs-${VERSION}/" -o "$HOME/rpmbuild/SOURCES/falconfs-${VERSION}.tar.gz" HEAD
cp rpm/falconfs.source.spec "$HOME/rpmbuild/SPECS/"
```

## 3. 构建 RPM

### 3.1 完整包（默认）

```bash
rpmbuild -ba "$HOME/rpmbuild/SPECS/falconfs.source.spec"
ls -alh "$HOME"/rpmbuild/RPMS/*/falconfs-0.1.0-1*.rpm
```

### 3.2 发布精简包（release）

```bash
rpmbuild -ba --define 'release_pkg 1' "$HOME/rpmbuild/SPECS/falconfs.source.spec"
ls -alh "$HOME"/rpmbuild/RPMS/*/falconfs-release-0.1.0-1*.rpm
```

说明：

- `release_pkg=1` 时，包名为 `falconfs-release`，仅保留 `falcon_meta/falcon_cm/falcon_cn/falcon_dn` 目录。
- 默认构建为完整包 `falconfs`。

## 4. 安装与验证

### 4.1 安装

```bash
sudo dnf install -y "$HOME"/rpmbuild/RPMS/*/falconfs-0.1.0-1*.rpm
# 或 release 包
# sudo dnf install -y "$HOME"/rpmbuild/RPMS/*/falconfs-release-0.1.0-1*.rpm
```

### 4.2 环境变量

安装后会生成 `/etc/profile.d/falconfs.sh`，关键变量包括：

- `FALCONFS_INSTALL_DIR=/usr/local/falconfs`
- `PATH` 追加 `/usr/local/pgsql/bin` 与 falcon client bin
- `LD_LIBRARY_PATH` 追加 falcon/obs 相关 lib 路径

执行以下命令使当前 shell 生效：

```bash
source /etc/profile.d/falconfs.sh
```

说明：

- 新开终端一般会自动加载 `/etc/profile.d/falconfs.sh`。
- 当前终端若不 `source`，可能出现 `pg_config` 或动态库路径找不到的问题。

### 4.3 本地冒烟（完整包）

```bash
/usr/local/falconfs/deploy/falcon_start.sh
mountpoint -q /tmp/falcon_mnt && echo mounted
/usr/local/falconfs/deploy/falcon_stop.sh
```

### 4.4 权限提示（开发机）

如果当前用户需要直接执行 `/usr/local/falconfs/deploy/falcon_start.sh`，建议确保有 sudo 权限。

开发机可选（非生产推荐）做法：

```bash
sudo chown -R "$USER":"$USER" /usr/local/falconfs
```

说明：生产环境通常保持 `/usr/local/falconfs` 由 root 管理，不建议随意更改归属。

## 5. 常见问题

- `pg_config: command not found`
  - 确认 `/usr/local/pgsql/bin/pg_config` 存在并在 PATH。

- 构建期提示缺少 OBS 相关库
  - 确认 `/usr/local/obs/lib` 存在，且构建环境可访问。

- release 包用于容器部署
  - 建议配合 `tests/regress/docker-compose-release-openeuler.yaml` 做集群启动验证。

- ZooKeeper 集群编号
  - 若手工部署 ZK 集群，`myid` 建议从 `1` 开始（与 compose 中 `zk1/zk2/...` 约定一致）。
