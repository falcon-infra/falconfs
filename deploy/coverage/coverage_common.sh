#!/usr/bin/env bash

clean_coverage_data() {
	echo "Cleaning coverage artifacts..."
	find "$BUILD_DIR" "$FALCONFS_DIR/falcon" -type f \( -name "*.gcda" -o -name "*.gcno" -o -name "*.info" \) -delete 2>/dev/null || true
	rm -rf "$BUILD_DIR/coverage"
	rm -rf "$SERVICE_COVERAGE_GCOV_PREFIX"
	echo "Coverage artifacts cleaned."
}

remove_filtered_service_gcda() {
	local service_proto_dir="$SERVICE_COVERAGE_GCOV_PREFIX/falcon/brpc_comm_adapter/proto"
	local service_fbs_dir="$SERVICE_COVERAGE_GCOV_PREFIX/falcon/connection_pool/fbs"

	rm -rf "$service_proto_dir" "$service_fbs_dir"
	find "$SERVICE_COVERAGE_GCOV_PREFIX" -type f \
		\( -name '*.pb.gcda' -o -name '*.pb.gcno' -o -name '*.fbs.h.gcda' -o -name '*.fbs.h.gcno' \) \
		-delete 2>/dev/null || true
}

require_coverage_tools() {
	for tool in lcov genhtml; do
		if ! command -v "$tool" >/dev/null 2>&1; then
			echo "Error: required tool '$tool' not found in PATH" >&2
			exit 1
		fi
	done
}

resolve_gcov_tool() {
	local compiler_major
	compiler_major="$(g++ -dumpversion | cut -d. -f1)"
	if command -v "gcov-$compiler_major" >/dev/null 2>&1; then
		echo "gcov-$compiler_major"
	elif command -v gcov >/dev/null 2>&1; then
		echo "gcov"
	else
		echo "Error: gcov tool not found in PATH" >&2
		exit 1
	fi
}

run_standalone_unit_tests() {
	cd "$FALCONFS_DIR"

	local target_dirs=(
		"$FALCONFS_DIR/build/tests/common/"
		"$FALCONFS_DIR/build/tests/falcon_client/"
		"$FALCONFS_DIR/build/tests/falcon_store/"
		"$FALCONFS_DIR/build/tests/falcon_plugin/"
		"$FALCONFS_DIR/build/tests/private-directory-test/"
	)

	for target_dir in "${target_dirs[@]}"; do
		if [[ -d "$target_dir" ]]; then
			echo "Running tests in: $target_dir"
			find "$target_dir" -type f -executable -name "*UT" | while read -r executable_file; do
				case "$(basename "$executable_file")" in
				FalconCMIT | LocalRunWorkloadUT | MetadbCoverageUT)
					continue
					;;
				esac
				echo "Executing: $executable_file"
				"$executable_file"
				echo "---------------------------------------------------------------------------------------"
			done
		else
			echo "Test directory not found: $target_dir"
		fi
	done

	local target_dir="$FALCONFS_DIR/build/tests/falcon/"
	find "$target_dir" -maxdepth 1 -type f -executable -not -name "*.cmake" -not -path "*/CMakeFiles/*" | while read -r executable_file; do
		echo "Executing: $executable_file"
		"$executable_file"
		echo "---------------------------------------------------------------------------------------"
	done

	local python_internal_dir="$FALCONFS_DIR/build/python_interface/_pyfalconfs_internal"
	local python_internal_test="$FALCONFS_DIR/tests/python_interface/test_pyfalconfs_internal.py"
	if [[ -d "$python_internal_dir" && -f "$python_internal_test" ]]; then
		echo "Executing Python interface coverage test: $python_internal_test"
		PYTHONPATH="$python_internal_dir${PYTHONPATH:+:$PYTHONPATH}" python3 "$python_internal_test"
		echo "---------------------------------------------------------------------------------------"
	fi
}

run_service_dependent_unit_tests() {
	local service_server_ip
	local service_server_port
	service_server_ip="$(resolve_service_test_server_ip)"
	service_server_port="$(resolve_service_test_server_port)"
	local service_uts=(
		"$FALCONFS_DIR/build/tests/private-directory-test/LocalRunWorkloadUT"
		"$FALCONFS_DIR/build/tests/falcon/metadb/MetadbCoverageUT"
	)

	echo "Running service-dependent tests:"
	echo "Service-dependent UT endpoint: ${service_server_ip}:${service_server_port}"
	for service_ut in "${service_uts[@]}"; do
		if [[ ! -x "$service_ut" ]]; then
			continue
		fi
		echo "Executing: $service_ut"
		SERVER_IP="$service_server_ip" \
		SERVER_PORT="$service_server_port" \
		LOCAL_RUN_MOUNT_DIR="${LOCAL_RUN_MOUNT_DIR:-/}" \
		LOCAL_RUN_FILE_PER_THREAD="${LOCAL_RUN_FILE_PER_THREAD:-1}" \
		LOCAL_RUN_THREAD_NUM_PER_CLIENT="${LOCAL_RUN_THREAD_NUM_PER_CLIENT:-1}" \
		LOCAL_RUN_CLIENT_ID="${LOCAL_RUN_CLIENT_ID:-0}" \
		LOCAL_RUN_MOUNT_PER_CLIENT="${LOCAL_RUN_MOUNT_PER_CLIENT:-1}" \
		LOCAL_RUN_CLIENT_CACHE_SIZE="${LOCAL_RUN_CLIENT_CACHE_SIZE:-16384}" \
		LOCAL_RUN_WAIT_PORT="${LOCAL_RUN_WAIT_PORT:-1111}" \
		LOCAL_RUN_FILE_SIZE="${LOCAL_RUN_FILE_SIZE:-4096}" \
		LOCAL_RUN_CLIENT_NUM="${LOCAL_RUN_CLIENT_NUM:-1}" \
		"$service_ut"
		echo "---------------------------------------------------------------------------------------"
	done
}

run_all_unit_tests() {
	run_standalone_unit_tests
	run_service_dependent_unit_tests
	echo "All unit tests passed."
}

wait_for_service_endpoint() {
	local service_ip="$1"
	local service_port="$2"
	local timeout_seconds="${3:-60}"
	local deadline=$((SECONDS + timeout_seconds))

	while ((SECONDS < deadline)); do
		if timeout 1 bash -lc "cat < /dev/null > /dev/tcp/${service_ip}/${service_port}" >/dev/null 2>&1; then
			return 0
		fi
		sleep 1
	done

	return 1
}

wait_for_falcon_meta_ready() {
	local service_ip="$1"
	local service_cn_port="$2"
	local timeout_seconds="${3:-60}"
	local deadline=$((SECONDS + timeout_seconds))

	while ((SECONDS < deadline)); do
		local has_extension
		has_extension="$(psql -d postgres -h "$service_ip" -p "$service_cn_port" -tAc "SELECT 1 FROM pg_extension WHERE extname='falcon';" 2>/dev/null || true)"
		if [[ "$has_extension" == "1" ]]; then
			return 0
		fi
		sleep 1
	done

	return 1
}

resolve_service_test_server_ip() {
	if [[ -n "${LOCAL_RUN_META_SERVER_IP:-}" ]]; then
		echo "$LOCAL_RUN_META_SERVER_IP"
		return 0
	fi

	local meta_config="$FALCONFS_DIR/deploy/meta/falcon_meta_config.sh"
	if [[ -f "$meta_config" ]]; then
		local cn_ip=""
		cn_ip="$(bash -lc "source '$meta_config' >/dev/null 2>&1; printf '%s' \"\${cnIp:-}\"")"
		if [[ -n "$cn_ip" ]]; then
			echo "$cn_ip"
			return 0
		fi
	fi

	echo "127.0.0.1"
}

resolve_service_test_server_port() {
	if [[ -n "${LOCAL_RUN_META_SERVER_PORT:-}" ]]; then
		echo "$LOCAL_RUN_META_SERVER_PORT"
		return 0
	fi

	local meta_config="$FALCONFS_DIR/deploy/meta/falcon_meta_config.sh"
	if [[ -f "$meta_config" ]]; then
		local cn_pooler_port_prefix=""
		cn_pooler_port_prefix="$(bash -lc "source '$meta_config' >/dev/null 2>&1; printf '%s' \"\${cnPoolerPortPrefix:-}\"")"
		if [[ -n "$cn_pooler_port_prefix" ]]; then
			echo "${cn_pooler_port_prefix}0"
			return 0
		fi
	fi

	echo "55510"
}

resolve_service_test_cn_port() {
	if [[ -n "${LOCAL_RUN_META_CN_PORT:-}" ]]; then
		echo "$LOCAL_RUN_META_CN_PORT"
		return 0
	fi

	local meta_config="$FALCONFS_DIR/deploy/meta/falcon_meta_config.sh"
	if [[ -f "$meta_config" ]]; then
		local cn_port_prefix=""
		cn_port_prefix="$(bash -lc "source '$meta_config' >/dev/null 2>&1; printf '%s' \"\${cnPortPrefix:-}\"")"
		if [[ -n "$cn_port_prefix" ]]; then
			echo "${cn_port_prefix}0"
			return 0
		fi
	fi

	echo "55500"
}
