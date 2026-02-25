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

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# ==============================================================================
# 参数检查
# ==============================================================================
if [ "$#" -lt 1 ]; then
    echo -e "${RED}错误: 参数不足${NC}"
    echo -e "用法: ./build_falcon_docker.sh <镜像版本Tag> [platform] [Git用户名]"
    echo -e "示例: ./build_falcon_docker.sh v0.3.0 linux/amd64"
    echo -e "      ./build_falcon_docker.sh v0.3.0 linux/amd64,linux/arm64"
    echo -e "      ./build_falcon_docker.sh v0.3.0 linux/amd64 user"
    exit 1
fi

TARGET_TAG=$1
PLATFORM=${2:-linux/amd64}
GIT_USER=${3:-$DEFAULT_GIT_USER}

BASE_IMAGE="${REGISTRY}/falconfs-base-builder:${TARGET_TAG}"
CN_IMAGE="${REGISTRY}/falconfs-cn:${TARGET_TAG}"
DN_IMAGE="${REGISTRY}/falconfs-dn:${TARGET_TAG}"
STORE_IMAGE="${REGISTRY}/falconfs-store:${TARGET_TAG}"
DEV_IMAGE="${REGISTRY}/falconfs-dev:${TARGET_TAG}"

# 遇到错误立即停止
set -e

echo -e "${GREEN}============================================================${NC}"
echo -e "${GREEN}开始构建 FalconFS 镜像${NC}"
echo -e "${GREEN}============================================================${NC}"
echo -e "目标版本: ${YELLOW}${TARGET_TAG}${NC}"
echo -e "目标平台: ${YELLOW}${PLATFORM}${NC}"
echo -e "工作目录: ${YELLOW}$(pwd)${NC}"
echo ""

# ==============================================================================
# 0. 检查 Registry
# ==============================================================================
if ! docker ps | grep -q "registry"; then
    echo -e "${RED}错误: 本地 Registry 未启动！${NC}"
    echo "请执行: docker run -d -p 5000:5000 --restart=always --name registry registry:2"
    exit 1
fi

# ==============================================================================
# 1. 构建 Base Builder
# ==============================================================================
echo -e "${YELLOW}[1/5] 构建 Base Builder (含 PostgreSQL 源码编译)...${NC}"
docker buildx build \
  --platform ${PLATFORM} \
  -t "${BASE_IMAGE}" \
  -f docker/ubuntu24.04-base-builder-dockerfile \
  --no-cache \
  . \
  --push

echo -e "${GREEN}✔ Base Builder 完成${NC}"

# ==============================================================================
# 2. 构建 Dev
# ==============================================================================
echo -e "${YELLOW}[2/5] 构建 Dev (含 PostgreSQL 源码编译)...${NC}"
docker buildx build \
  --platform ${PLATFORM} \
  -t "${DEV_IMAGE}" \
  -f docker/ubuntu24.04-dev-dockerfile \
  --no-cache \
  . \
  --push

echo -e "${GREEN}✔ Dev 完成${NC}"

# ==============================================================================
# 3. 构建 CN
# ==============================================================================
echo -e "${YELLOW}[3/5] 构建 CN...${NC}"
docker buildx build \
  --platform ${PLATFORM} \
  --build-arg BASE_BUILDER_IMAGE="${BASE_IMAGE}" \
  -t "${CN_IMAGE}" \
  -f docker/ubuntu24.04-cn-dockerfile \
  --no-cache \
  . \
  --push

echo -e "${GREEN}✔ CN 完成${NC}"

# ==============================================================================
# 4. 构建 DN
# ==============================================================================
echo -e "${YELLOW}[4/5] 构建 DN...${NC}"
docker buildx build \
  --platform ${PLATFORM} \
  --build-arg BASE_BUILDER_IMAGE="${BASE_IMAGE}" \
  -t "${DN_IMAGE}" \
  -f docker/ubuntu24.04-dn-dockerfile \
  --no-cache \
  . \
  --push

echo -e "${GREEN}✔ DN 完成${NC}"

# ==============================================================================
# 5. 构建 Store
# ==============================================================================
echo -e "${YELLOW}[5/5] 构建 Store...${NC}"
docker buildx build \
  --platform ${PLATFORM} \
  --build-arg BASE_BUILDER_IMAGE="${BASE_IMAGE}" \
  -t "${STORE_IMAGE}" \
  -f docker/ubuntu24.04-store-dockerfile \
  --no-cache \
  . \
  --push

echo -e "${GREEN}✔ Store 完成${NC}"

echo -e "${GREEN}============================================================${NC}"
echo -e "${GREEN}所有构建完成！${NC}"
echo -e "${GREEN}============================================================${NC}"
echo -e "Base Builder: ${YELLOW}${BASE_IMAGE}${NC}"
echo -e "Dev 镜像:   ${YELLOW}${DEV_IMAGE}${NC}"
echo -e "CN 镜像:    ${YELLOW}${CN_IMAGE}${NC}"
echo -e "DN 镜像:    ${YELLOW}${DN_IMAGE}${NC}"
echo -e "Store 镜像: ${YELLOW}${STORE_IMAGE}${NC}"
echo ""
echo -e "${GREEN}运行示例：${NC}"
echo -e "  docker run -it --rm ${DEV_IMAGE} /bin/bash"
echo -e "  docker run -it --rm ${CN_IMAGE} /bin/bash"
echo -e "  docker run -it --rm ${DN_IMAGE} /bin/bash"
echo -e "  docker run -it --rm ${STORE_IMAGE} /bin/bash"
