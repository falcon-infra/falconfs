#!/bin/bash
#
# FalconFS Ansible 部署测试执行脚本
# 用于自动化测试 FalconFS 的 Ansible 部署方案
#
# 用法:
#   ./run-ansible-test.sh [MODE] [OPTIONS]
#
# MODE (模式):
#   full         完整流程: 清理 -> 启动容器 -> 部署 -> 验证 (默认)
#   build        仅构建中间镜像
#   start        启动容器并配置 (不执行部署)
#   deploy       执行部署 (假设容器已启动)
#   verify       仅验证部署状态
#   clean        仅清理环境
#   rebuild      清理 + 完整流程
#
# OPTIONS (选项):
#   --tags=TAGS      执行特定的 ansible tags (如 "build,package,distribute")
#   --skip-deps     跳过 install-deps 步骤 (使用中间镜像时)
#   --mode=MODE     构建模式: centralized (默认) 或 distributed
#   --no-verify     执行部署后不自动验证
#   --verbose       显示详细输出
#   -h, --help      显示帮助信息
#
# 示例:
#   ./run-ansible-test.sh full                    # 完整流程
#   ./run-ansible-test.sh full --skip-deps        # 跳过 install-deps
#   ./run-ansible-test.sh deploy --tags="build"    # 仅执行 build
#   ./run-ansible-test.sh rebuild --tags="build,package,distribute,start,verify"
#   ./run-ansible-test.sh clean                    # 仅清理
#

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 配置变量
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
SKILL_DIR="$SCRIPT_DIR"

# 容器配置
CONTAINER_NETWORK="falcon-ansible-test-net"
CONTAINER_NAME_CN="falcon-ansible-test-cn"
CONTAINER_NAME_DN0="falcon-ansible-test-dn0"
CONTAINER_NAME_DN1="falcon-ansible-test-dn1"
BASE_IMAGE="127.0.0.1:5000/ubuntu:24.04"
INTERMEDIATE_IMAGE="127.0.0.1:5000/falconfs-ansible-test:latest"

# SSH 密钥和临时文件
SSH_KEY="/tmp/falcon_ansible_test_key"
INVENTORY_FILE="/tmp/test_inventory"

# 部署配置
FALCON_BUILD_MODE="centralized"
ANSIBLE_TAGS="install-deps,build,package,distribute,start,verify"
SKIP_DEPS=false
AUTO_VERIFY=true
VERBOSE=false

# 显示带颜色的消息
log_info() {
	echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
	echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warn() {
	echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
	echo -e "${RED}[ERROR]${NC} $1"
}

# 显示帮助信息
show_help() {
	head -30 "$0" | tail -25
}

# 解析命令行参数
parse_args() {
	MODE="full"

	for arg in "$@"; do
		case $arg in
		full | build | start | deploy | verify | clean | rebuild)
			MODE="$arg"
			;;
		--tags=*)
			ANSIBLE_TAGS="${arg#*=}"
			;;
		--skip-deps)
			SKIP_DEPS=true
			;;
		--mode=*)
			FALCON_BUILD_MODE="${arg#*=}"
			;;
		--no-verify)
			AUTO_VERIFY=false
			;;
		--verbose)
			VERBOSE=true
			;;
		-h | --help)
			show_help
			exit 0
			;;
		*)
			log_error "未知参数: $arg"
			show_help
			exit 1
			;;
		esac
	done

	# 如果跳过了 deps，从 tags 中移除 install-deps
	if [ "$SKIP_DEPS" = true ]; then
		ANSIBLE_TAGS="${ANSIBLE_TAGS//install-deps,/}"
		ANSIBLE_TAGS="${ANSIBLE_TAGS//install-deps/}"
	fi

	# 移除可能的逗号开头
	ANSIBLE_TAGS="${ANSIBLE_TAGS#,}"
}

# 检查前置条件
check_prerequisites() {
	log_info "检查前置条件..."

	# 检查 Docker
	if ! docker info >/dev/null 2>&1; then
		log_error "Docker 未运行，请先启动 Docker"
		exit 1
	fi
	log_success "Docker 正常运行"

	# 检查中间镜像
	if ! docker images | grep -q "falconfs-ansible-test"; then
		log_warn "中间镜像不存在，需要先构建"
		return 1
	fi
	log_success "中间镜像存在: $INTERMEDIATE_IMAGE"

	# 检查 FUSE 支持
	if ! lsmod | grep -q fuse; then
		log_warn "FUSE 内核模块未加载，可能影响 FUSE 挂载测试"
		log_warn "如需测试 FUSE 挂载，请运行: sudo modprobe fuse"
	fi

	return 0
}

# 构建中间镜像
build_intermediate_image() {
	log_info "构建中间镜像 (约 15-20 分钟)..."

	cd "$PROJECT_ROOT"

	if docker build -f "$SKILL_DIR/Dockerfile.intermediate" \
		-t "$INTERMEDIATE_IMAGE" .; then
		log_success "中间镜像构建完成: $INTERMEDIATE_IMAGE"
	else
		log_error "中间镜像构建失败"
		exit 1
	fi
}

# 清理环境
cleanup_environment() {
	log_info "清理测试环境..."

	# 停止并删除容器
	for container in $CONTAINER_NAME_CN $CONTAINER_NAME_DN0 $CONTAINER_NAME_DN1; do
		if docker ps -a | grep -q "$container"; then
			docker stop "$container" 2>/dev/null || true
			docker rm "$container" 2>/dev/null || true
			log_info "已删除容器: $container"
		fi
	done

	# 删除网络
	if docker network ls | grep -q "$CONTAINER_NETWORK"; then
		docker network rm "$CONTAINER_NETWORK" 2>/dev/null || true
		log_info "已删除网络: $CONTAINER_NETWORK"
	fi

	# 清理临时文件
	rm -f "$SSH_KEY" "$SSH_KEY.pub" "$INVENTORY_FILE"
	rm -f /tmp/falconfs-*.tar.gz /tmp/pgsql.tar.gz
	log_info "已清理临时文件"

	log_success "环境清理完成"
}

# 启动容器
start_containers() {
	log_info "启动测试容器..."

	# 创建网络
	if ! docker network ls | grep -q "$CONTAINER_NETWORK"; then
		docker network create "$CONTAINER_NETWORK"
		log_info "已创建网络: $CONTAINER_NETWORK"
	fi

	# 启动 CN 容器
	if ! docker ps | grep -q "$CONTAINER_NAME_CN"; then
		docker run -d --name "$CONTAINER_NAME_CN" --network "$CONTAINER_NETWORK" \
			-p 2221:22 -p 5442:5442 -p 5550:5550 --privileged \
			"$INTERMEDIATE_IMAGE"
		log_info "已启动 CN 容器"
	fi

	# 启动 DN0 容器
	if ! docker ps | grep -q "$CONTAINER_NAME_DN0"; then
		docker run -d --name "$CONTAINER_NAME_DN0" --network "$CONTAINER_NETWORK" \
			-p 2222:22 -p 5443:5442 -p 5551:5551 --privileged \
			"$INTERMEDIATE_IMAGE"
		log_info "已启动 DN0 容器"
	fi

	# 启动 DN1 容器
	if ! docker ps | grep -q "$CONTAINER_NAME_DN1"; then
		docker run -d --name "$CONTAINER_NAME_DN1" --network "$CONTAINER_NETWORK" \
			-p 2223:22 -p 5444:5442 -p 5552:5552 --privileged \
			"$INTERMEDIATE_IMAGE"
		log_info "已启动 DN1 容器"
	fi

	# 等待容器就绪
	sleep 2

	# 获取容器 IP
	CN_IP=$(docker inspect "$CONTAINER_NAME_CN" | grep '"IPAddress"' | head -1 | awk -F'"' '{print $4}')
	DN0_IP=$(docker inspect "$CONTAINER_NAME_DN0" | grep '"IPAddress"' | head -1 | awk -F'"' '{print $4}')
	DN1_IP=$(docker inspect "$CONTAINER_NAME_DN1" | grep '"IPAddress"' | head -1 | awk -F'"' '{print $4}')

	echo ""
	log_info "容器 IP 地址:"
	echo "  CN:  $CN_IP"
	echo "  DN0: $DN0_IP"
	echo "  DN1: $DN1_IP"
	echo ""

	# 导出供后续使用
	export CN_IP DN0_IP DN1_IP
}

# 配置 SSH 和 Inventory
configure_ssh_inventory() {
	log_info "配置 SSH 密钥和 Inventory..."

	# 生成 SSH 密钥
	ssh-keygen -t rsa -f "$SSH_KEY" -N "" -q
	PUB_KEY=$(cat "$SSH_KEY.pub")

	# 配置所有容器的 SSH
	for container in $CONTAINER_NAME_CN $CONTAINER_NAME_DN0 $CONTAINER_NAME_DN1; do
		docker exec "$container" bash -c "
            echo '$PUB_KEY' > /home/falcon/.ssh/authorized_keys && \
            chown -R falcon:falcon /home/falcon/.ssh && \
            chmod 600 /home/falcon/.ssh/authorized_keys
        " 2>/dev/null || true
	done
	log_info "SSH 密钥已配置"

	# 创建 Inventory 文件
	cat >"$INVENTORY_FILE" <<EOF
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
falcon_build_mode="$FALCON_BUILD_MODE"
falcon_server_port_prefix="555"
falcon_client_port="56039"
falcon_client_log_path="/tmp"
falcon_client_cache_path="/tmp/falcon_cache"
mount_path="/home/falcon/mnt"
EOF

	log_success "Inventory 文件已创建 (falcon_build_mode=$FALCON_BUILD_MODE)"
}

# 安装 Ansible 和复制部署文件
prepare_ansible() {
	log_info "准备 Ansible 环境..."

	# 在 CN 容器中安装 Ansible
	docker exec "$CONTAINER_NAME_CN" bash -c "
        apt-get update -qq
        apt-get install -y -qq ansible curl sshpass > /dev/null 2>&1
        mkdir -p /home/falcon/code/ansible /home/falcon/.ansible
    "
	log_info "Ansible 已安装"

	# 复制部署文件到 CN 容器
	docker cp "$INVENTORY_FILE" "$CONTAINER_NAME_CN:/home/falcon/code/ansible/inventory"
	docker cp "$PROJECT_ROOT/deploy/ansible/falcontest.yml" "$CONTAINER_NAME_CN:/home/falcon/code/ansible/"
	docker cp "$PROJECT_ROOT/deploy/ansible/install-ubuntu24.04.sh" "$CONTAINER_NAME_CN:/home/falcon/code/ansible/"
	docker cp "$PROJECT_ROOT/deploy/ansible/install-ubuntu22.04.sh" "$CONTAINER_NAME_CN:/home/falcon/code/ansible/"
	docker cp "$SSH_KEY" "$CONTAINER_NAME_CN:/home/falcon/.ssh/id_rsa"
	docker cp "$SSH_KEY.pub" "$CONTAINER_NAME_CN:/home/falcon/.ssh/id_rsa.pub"

	# 配置权限和 ansible.cfg
	docker exec "$CONTAINER_NAME_CN" bash -c "
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
	if docker exec "$CONTAINER_NAME_CN" bash -c "su - falcon -c 'ansible all -m ping'" >/dev/null 2>&1; then
		log_success "Ansible 连接测试通过"
	else
		log_warn "Ansible 连接测试失败，请检查网络配置"
	fi

	log_success "Ansible 环境准备完成"
}

# 清理 DN 节点上的旧目录
clean_dn_directories() {
	log_info "清理 DN 节点上的旧目录..."

	for container in $CONTAINER_NAME_DN0 $CONTAINER_NAME_DN1; do
		if docker exec "$container" test -d /usr/local/falconfs; then
			docker exec -u root "$container" rm -rf /usr/local/falconfs
			log_info "已清理 $container:/usr/local/falconfs"
		fi
	done
}

# 执行 Ansible 部署
run_ansible_deploy() {
	log_info "执行 Ansible 部署 (tags: $ANSIBLE_TAGS)..."

	# 如果包含 distribute，先清理 DN 目录
	if echo "$ANSIBLE_TAGS" | grep -q "distribute"; then
		clean_dn_directories
	fi

	# 执行 Ansible playbook
	docker exec "$CONTAINER_NAME_CN" bash -c "
        su - falcon -c 'cd ~/code/ansible && ansible-playbook falcontest.yml --tags \"$ANSIBLE_TAGS\"'
    "

	log_success "Ansible 部署完成"
}

# 验证部署状态
verify_deployment() {
	log_info "验证部署状态..."

	echo ""
	echo "=== CN 节点状态 ==="
	docker exec "$CONTAINER_NAME_CN" bash -c "
        echo 'PostgreSQL 进程:'
        ps aux | grep 'postgres.*-D' | grep -v grep | head -2 || echo '  无进程'
        echo 'Client 进程:'
        ps aux | grep falcon_client | grep -v grep | head -1 || echo '  无进程'
        echo 'FUSE 挂载:'
        mount | grep fuse || echo '  无 FUSE 挂载'
    "

	echo ""
	echo "=== DN0 节点状态 ==="
	docker exec "$CONTAINER_NAME_DN0" bash -c "
        echo 'PostgreSQL 进程:'
        ps aux | grep 'postgres.*-D' | grep -v grep | head -2 || echo '  无进程'
        echo 'FUSE 挂载:'
        mount | grep fuse || echo '  无 FUSE 挂载'
    "

	echo ""
	echo "=== DN1 节点状态 ==="
	docker exec "$CONTAINER_NAME_DN1" bash -c "
        echo 'PostgreSQL 进程:'
        ps aux | grep 'postgres.*-D' | grep -v grep | head -2 || echo '  无进程'
        echo 'FUSE 挂载:'
        mount | grep fuse || echo '  无 FUSE 挂载'
    "

	echo ""
	echo "=== PostgreSQL 连接测试 ==="
	docker exec "$CONTAINER_NAME_DN0" /usr/local/pgsql/bin/pg_isready -h "$CN_IP" -p 55500 2>/dev/null || echo "CN->DN0: 连接失败"
	docker exec "$CONTAINER_NAME_DN1" /usr/local/pgsql/bin/pg_isready -h "$CN_IP" -p 55500 2>/dev/null || echo "CN->DN1: 连接失败"
	docker exec "$CONTAINER_NAME_CN" /usr/local/pgsql/bin/pg_isready -h "$DN0_IP" -p 55520 2>/dev/null || echo "DN0->CN: 连接失败"
	docker exec "$CONTAINER_NAME_CN" /usr/local/pgsql/bin/pg_isready -h "$DN1_IP" -p 55520 2>/dev/null || echo "DN1->CN: 连接失败"

	log_success "验证完成"
}

# 主流程
main() {
	parse_args "$@"

	echo ""
	echo "=============================================="
	echo "  FalconFS Ansible 部署测试"
	echo "  模式: $MODE"
	echo "  构建模式: $FALCON_BUILD_MODE"
	echo "  Ansible Tags: $ANSIBLE_TAGS"
	echo "=============================================="
	echo ""

	case $MODE in
	build)
		build_intermediate_image
		;;

	clean)
		cleanup_environment
		;;

	start)
		check_prerequisites || true
		cleanup_environment
		start_containers
		configure_ssh_inventory
		prepare_ansible
		;;

	deploy)
		# 确保容器已启动
		if ! docker ps | grep -q "$CONTAINER_NAME_CN"; then
			log_error "容器未启动，请先运行 start 模式"
			exit 1
		fi
		run_ansible_deploy
		;;

	verify)
		# 确保容器已启动
		if ! docker ps | grep -q "$CONTAINER_NAME_CN"; then
			log_error "容器未启动，请先运行 start 或 full 模式"
			exit 1
		fi
		verify_deployment
		;;

	full)
		check_prerequisites || true
		cleanup_environment
		start_containers
		configure_ssh_inventory
		prepare_ansible
		run_ansible_deploy
		if [ "$AUTO_VERIFY" = true ]; then
			verify_deployment
		fi
		;;

	rebuild)
		check_prerequisites || true
		cleanup_environment
		build_intermediate_image
		start_containers
		configure_ssh_inventory
		prepare_ansible
		run_ansible_deploy
		if [ "$AUTO_VERIFY" = true ]; then
			verify_deployment
		fi
		;;

	*)
		log_error "未知模式: $MODE"
		show_help
		exit 1
		;;
	esac

	echo ""
	echo "=============================================="
	echo "  测试完成!"
	echo "=============================================="
	echo ""
}

# 运行主函数
main "$@"
