#!/bin/bash
# 获取脚本所在的目录 (即 ~/code/falconfs/docker)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# 切换到脚本的上一级目录 (即项目根目录 ~/code/falconfs)
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "正在切换工作目录到项目根目录: $PROJECT_ROOT"
cd "$PROJECT_ROOT"

# ==============================================================================
# 配置区域
# ==============================================================================
REGISTRY="127.0.0.1:5000"
# 默认 Git 用户
DEFAULT_GIT_USER="liuwei00960908"
# 默认分支
BRANCH_TAG="main"

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m' 
NC='\033[0m' # No Color

# ==============================================================================
# 参数检查
# ==============================================================================
if [ "$#" -lt 2 ]; then
    echo -e "${RED}错误: 参数不足${NC}"
    echo -e "用法: ./build_falcon_release.sh <镜像版本Tag> <GitCode_Token> [Git用户名]"
    echo -e "示例: ./build_falcon_release.sh v2.1-release dXJUD1ZjHmizYzNwDgC-VeWu"
    exit 1
fi

TARGET_TAG=$1
GIT_TOKEN=$2
GIT_USER=${3:-$DEFAULT_GIT_USER}

BASE_IMAGE="${REGISTRY}/falconfs-base-builder:${TARGET_TAG}"
CN_IMAGE="${REGISTRY}/falconfs-cn:${TARGET_TAG}"
DN_IMAGE="${REGISTRY}/falconfs-dn:${TARGET_TAG}"

# 遇到错误立即停止
set -e

echo -e "${GREEN}============================================================${NC}"
echo -e "${GREEN}开始构建 FalconFS 双架构镜像 (x86 + ARM64)${NC}"
echo -e "${GREEN}============================================================${NC}"
echo -e "目标版本: ${YELLOW}${TARGET_TAG}${NC}"
echo -e "Git 用户: ${YELLOW}${GIT_USER}${NC}"
echo -e "工作目录: ${YELLOW}$(pwd)${NC}"
echo ""

# ==============================================================================
# 0. 检查 Registry
# ==============================================================================
if ! docker ps | grep -q "registry"; then
    echo -e "${RED}错误: 本地 Registry 未启动！${NC}"
    echo "请执行: sudo docker run -d -p 5000:5000 --restart=always --name registry registry:2"
    exit 1
fi

# ==============================================================================
# 1. 构建 Base Builder
# ==============================================================================
echo -e "${YELLOW}[1/3] 构建 Base Builder...${NC}"
# 注意：这里路径保持 docker/xxx，因为我们已经 cd 到了根目录
sudo docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -t "${BASE_IMAGE}" \
  -f docker/ubuntu24.04-base-builder-dockerfile \
  --build-arg FALCONFS_TAG="${BRANCH_TAG}" \
  --build-arg GIT_USERNAME="${GIT_USER}" \
  --build-arg GIT_TOKEN="${GIT_TOKEN}" \
  --pull \
  --no-cache \
  . \
  --push

echo -e "${GREEN}✔ Base Builder 完成${NC}"

# ==============================================================================
# 2. 自动修改 Dockerfile 依赖
# ==============================================================================
echo -e "${YELLOW}[INFO] 更新 CN/DN Dockerfile 指向: ${BASE_IMAGE}${NC}"

sed -i "s|FROM .* AS builder|FROM ${BASE_IMAGE} AS builder|g" docker/ubuntu24.04-cn-dockerfile
sed -i "s|FROM .* AS builder|FROM ${BASE_IMAGE} AS builder|g" docker/ubuntu24.04-dn-dockerfile

# ==============================================================================
# 3. 构建 CN 和 DN
# ==============================================================================
echo -e "${YELLOW}[2/3] 构建 CN...${NC}"
sudo docker buildx build \
  --platform linux/amd64,linux/arm64 \ 
  -t "${CN_IMAGE}" \  
  -f docker/ubuntu24.04-cn-dockerfile \
  --pull \
  --no-cache \
  . \
  --push

echo -e "${YELLOW}[3/3] 构建 DN...${NC}"
sudo docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -t "${DN_IMAGE}" \
  -f docker/ubuntu24.04-dn-dockerfile \
  --pull \
  --no-cache \
  . \
  --push

echo -e "${GREEN}============================================================${NC}"
echo -e "${GREEN}所有构建完成！${NC}"
echo -e "${GREEN}============================================================${NC}"   
