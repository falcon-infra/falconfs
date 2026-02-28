# FalconFS Debian 打包说明（Ubuntu 24.04）

本文档说明如何基于 `debian/` 目录构建 FalconFS 的 `.deb` 安装包。

当前输出两个二进制包：

- `falconfs`（完整包）
- `falconfs-release`（精简运行时包）

## 1. 前置环境

### 1.1 第三方依赖（独立预装）

先执行：

```bash
bash deb/install-third-party-ubuntu24.04.sh
```

该脚本会准备 FalconFS 当前打包链路依赖的外部组件：

- PostgreSQL 17（`/usr/local/pgsql`）
- brpc（源码安装）
- prometheus-cpp（源码安装）

默认构建路径已解耦 OBS；仅在显式启用 `--with-obs-storage` 时才需要 OBS SDK。

### 1.2 Debian 打包工具

```bash
sudo apt-get update
sudo apt-get install -y dpkg-dev debhelper devscripts fakeroot
```

## 2. 构建 deb 包

在仓库根目录执行：

```bash
dpkg-buildpackage -b -us -uc
```

产物位于仓库上级目录：

- `../falconfs_0.1.0-1_*.deb`
- `../falconfs-release_0.1.0-1_*.deb`

## 3. 安装与验证

### 3.1 安装

```bash
sudo apt-get install -y ../falconfs_0.1.0-1_*.deb
# 或
# sudo apt-get install -y ../falconfs-release_0.1.0-1_*.deb
```

### 3.2 环境变量

安装后会生成：`/etc/profile.d/falconfs.sh`

```bash
source /etc/profile.d/falconfs.sh
```

### 3.3 本地冒烟（完整包）

```bash
/usr/local/falconfs/deploy/falcon_start.sh
/usr/local/falconfs/deploy/falcon_stop.sh
```

## 4. 包内容说明

- `falconfs`：完整安装目录 `/usr/local/falconfs`
- `falconfs-release`：仅保留
  - `/usr/local/falconfs/falcon_meta`
  - `/usr/local/falconfs/falcon_cm`
  - `/usr/local/falconfs/falcon_cn`
  - `/usr/local/falconfs/falcon_dn`

两个包互斥安装，不建议同时安装。

## 5. Release 容器编排验证（docker-compose）

### 5.1 构建 Ubuntu release 运行时镜像

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

### 5.2 用 compose 拉起 release 集群

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
