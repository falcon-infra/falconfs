# FalconFS Debian 打包说明（Ubuntu 24.04）

本文档说明如何基于 `debian/` 目录构建 FalconFS 的 `.deb` 安装包。

当前输出两个二进制包：

- `falconfs`（完整包）
- `falconfs-release`（精简运行时包）

## 1. 空白 Ubuntu 24.04 机器准备

在空白 Ubuntu 24.04 机器上，先安装最小拉代码工具：

```bash
sudo apt-get update
sudo apt-get install -y git ca-certificates sudo
```

拉取代码：

```bash
git clone <falconfs-repo-url> falconfs
cd falconfs
```

安装 FalconFS 编译、打包和本地部署所需依赖：

```bash
bash deb/install-third-party-ubuntu24.04.sh
```

脚本默认准备：

- Ubuntu apt 构建依赖，按 `debian/control` 和 `rpm/falconfs.source.spec` 对齐
- PostgreSQL 17（默认 `/usr/local/pgsql`，提供 `pg_config`）
- brpc（源码安装到 `/usr/local`）
- prometheus-cpp（源码安装到 `/usr/local`）
- ZooKeeper C client（使用 Ubuntu 包 `libzookeeper-mt-dev`，不源码编译）

脚本会把 `pg_config` 软链到 `/usr/local/bin`，运行结束后当前 shell 可直接编译；其他 PostgreSQL 命令由 `deploy/falcon_env.sh` 通过 `pg_config --bindir` 加入 `PATH`。脚本也会写入 `/etc/profile.d/falconfs-third-party.sh` 供新 shell 自动加载环境。

默认构建路径已解耦 OBS；仅在显式启用 `--with-obs-storage` 时才需要 OBS SDK。

## 2. 源码编译与本地部署验证

```bash
./build.sh clean falcon
./build.sh build falcon
sudo -E ./build.sh install falcon
source deploy/falcon_env.sh
./deploy/falcon_start.sh
./deploy/falcon_stop.sh
```

说明：本地启动脚本会使用默认 `brpc` 通信插件；若要验证 `hcom` 插件，需要先执行 `./build.sh build falcon --comm-plugin=hcom` 并按对应插件安装。

## 3. Debian 打包

### 3.1 第三方依赖（独立预装）

先执行：

```bash
bash deb/install-third-party-ubuntu24.04.sh
```

该脚本会准备 FalconFS 当前打包链路依赖的外部组件：

- PostgreSQL 17（`/usr/local/pgsql`）
- brpc（源码安装）
- prometheus-cpp（源码安装）
- ZooKeeper C client（Ubuntu 包）

构建 Debian 包还需要额外安装打包工具：

```bash
sudo apt-get install -y dpkg-dev debhelper devscripts fakeroot chrpath
```

### 3.2 构建 deb 包

在仓库根目录执行：

```bash
dpkg-buildpackage -b -us -uc
```

产物位于仓库上级目录：

- `../falconfs_0.1.0-1_*.deb`
- `../falconfs-release_0.1.0-1_*.deb`

## 4. 安装与验证

### 4.1 安装

```bash
sudo apt-get install -y ../falconfs_0.1.0-1_*.deb
# 或
# sudo apt-get install -y ../falconfs-release_0.1.0-1_*.deb
```

### 4.2 环境变量

安装后会生成：`/etc/profile.d/falconfs.sh`

主要包含：

- `FALCONFS_INSTALL_DIR=/usr/local/falconfs`
- `PATH` 追加 `/usr/local/pgsql/bin` 与 falcon client bin

FalconFS 运行时动态库路径由启动脚本按进程设置，不在 profile 中全局导出。

```bash
source /etc/profile.d/falconfs.sh
```

### 4.3 本地冒烟（完整包）

```bash
/usr/local/falconfs/deploy/falcon_start.sh
/usr/local/falconfs/deploy/falcon_stop.sh
```

### 4.4 可选日志目录配置

默认情况下日志路径保持历史行为：

- metadata 启动日志：`/usr/local/falconfs/deploy/meta/`
- client 日志：`/usr/local/falconfs/deploy/client/`
- 容器 CN/DN `start.log`：`${FALCONFS_INSTALL_DIR}/data/`

如需自定义，可在启动前设置：

```bash
export FALCON_META_LOG_DIR=/path/to/meta/logs
export FALCON_CLIENT_LOG_DIR=/path/to/client/logs
export FALCON_CN_DN_START_LOG_DIR=/path/to/cn-dn-start-logs
```

说明：

- 不设置上述变量时，仍使用默认路径。

## 5. 包内容说明

- `falconfs`：完整安装目录 `/usr/local/falconfs`
- `falconfs-release`：仅保留
  - `/usr/local/falconfs/falcon_meta`
  - `/usr/local/falconfs/falcon_cm`
  - `/usr/local/falconfs/falcon_cn`
  - `/usr/local/falconfs/falcon_dn`

两个包互斥安装，不建议同时安装。

## 6. 空白容器验证参考

本机已有 `ubuntu:24.04` 镜像时，可用一次性容器模拟空白机器。该验证会在容器内重新 clone 当前工作区内容并运行安装脚本：

```bash
docker run --rm --privileged -v "$PWD":/src:ro ubuntu:24.04 bash -lc '
  apt-get update
  apt-get install -y git ca-certificates sudo
  git clone /src /work/falconfs
  cd /work/falconfs
  bash deb/install-third-party-ubuntu24.04.sh
  ./build.sh clean falcon
  ./build.sh build falcon
  sudo -E ./build.sh install falcon
  source deploy/falcon_env.sh
  ./deploy/falcon_start.sh
  ./deploy/falcon_stop.sh
'
```

## 7. Release 容器编排验证（docker-compose）

### 7.1 构建 Ubuntu release 运行时镜像

将生成的 release deb 放到仓库根目录并重命名：

```bash
cp ../falconfs-release_0.1.0-1_amd64.deb ./falconfs-deb-release.deb
```

构建镜像：

```bash
docker build \
  -f docker/ubuntu24.04-release-runtime-dockerfile \
  -t falconfs-release-ubuntu24.04:v0.1.0 \
  .
```

### 7.2 用 compose 拉起 release 集群

```bash
export FALCON_RELEASE_IMAGE=falconfs-release-ubuntu24.04:v0.1.0
export FALCON_DATA_PATH=$PWD/tests/regress/verify_data_release_ubuntu
mkdir -p "$FALCON_DATA_PATH"

docker-compose -f tests/regress/docker-compose-release-ubuntu.yaml up -d
docker-compose -f tests/regress/docker-compose-release-ubuntu.yaml ps
docker-compose -f tests/regress/docker-compose-release-ubuntu.yaml down
```

说明：

- `tests/regress/docker-compose-release-ubuntu.yaml` 默认使用镜像 `falconfs-release-ubuntu24.04:v0.1.0`。
