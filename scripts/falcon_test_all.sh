#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEPLOY_DIR="$PROJECT_DIR/deploy"
TIMEOUT=120

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

run_with_timeout() {
    local desc="$1"
    local timeout="$2"
    shift 2
    local start_time=$(date +%s)
    
    echo ""
    echo "▶ $desc"
    "$@" 2>&1
    local rc=$?
    local end_time=$(date +%s)
    local elapsed=$((end_time - start_time))
    
    if [ $rc -eq 0 ]; then
        log_info "$desc 完成 (${elapsed}秒)"
    else
        log_error "$desc 失败 (rc=$rc, ${elapsed}秒)"
        exit 1
    fi
}

echo "========================================="
echo " FalconFS 完整测试流程"
echo "========================================="

# 步骤1：停止旧服务
echo ""
echo "【1/5】停止旧服务"
echo "========================================="
cd "$DEPLOY_DIR"
./falcon_stop.sh 2>&1 | tail -5
log_info "旧服务已停止"

# 步骤2：重新编译
echo ""
echo "【2/5】编译 FalconFS"
echo "========================================="
cd "$PROJECT_DIR"
[ -d build ] && sudo rm -rf build
run_with_timeout "编译" $TIMEOUT ./build.sh build falcon --debug --comm-plugin=brpc

# 步骤3：安装部署
echo ""
echo "【3/5】安装部署"
echo "========================================="
run_with_timeout "安装" $TIMEOUT sudo ./build.sh install falcon

# 步骤4：启动服务
echo ""
echo "【4/5】启动服务"
echo "========================================="
cd "$DEPLOY_DIR"
run_with_timeout "启动" $TIMEOUT ./falcon_start.sh --comm-plugin=brpc 2>&1 | tail -20

echo ""
log_info "等待服务完全启动..."
sleep 3

# 步骤5：验证测试
echo ""
echo "【5/5】验证测试"
echo "========================================="

echo "检查挂载..."
if mount | grep -q falcon_mnt; then
    log_info "挂载成功"
else
    log_error "挂载失败"
    exit 1
fi

echo "测试读写..."
echo "FalconFS test $(date)" > /tmp/falcon_mnt/test.txt
cat /tmp/falcon_mnt/test.txt
rm /tmp/falcon_mnt/test.txt
log_info "读写测试通过"

echo ""
echo "========================================="
echo " ✅ 全部测试通过！"
echo "========================================="
echo ""
echo "如需停止服务，执行："
echo "  cd $DEPLOY_DIR && ./falcon_stop.sh"
