#!/usr/bin/env bash

prepare_coverage_service_artifacts() {
	local meta_lib_dir="$FALCONFS_INSTALL_DIR/falcon_meta/lib/postgresql"
	local meta_ext_dir="$FALCONFS_INSTALL_DIR/falcon_meta/share/extension"
	local plugin_src=""
	local use_sudo=false

	case "$COMM_PLUGIN" in
	brpc)
		plugin_src="$FALCONFS_DIR/falcon/libbrpcplugin.so"
		;;
	hcom)
		plugin_src="$FALCONFS_DIR/falcon/libhcomplugin.so"
		;;
	esac

	if [[ ! -w "$FALCONFS_INSTALL_DIR" ]]; then
		if command -v sudo >/dev/null 2>&1; then
			use_sudo=true
		else
			echo "Error: $FALCONFS_INSTALL_DIR is not writable and sudo is unavailable" >&2
			return 1
		fi
	fi

	if [[ "$use_sudo" == true ]]; then
		sudo mkdir -p "$meta_lib_dir" "$meta_ext_dir"
		sudo cp -f "$FALCONFS_DIR/falcon/falcon.so" "$meta_lib_dir/"
		sudo cp -f "$FALCONFS_DIR/falcon/falcon.control" "$meta_ext_dir/"
		sudo cp -f "$FALCONFS_DIR/falcon/falcon--1.0.sql" "$meta_ext_dir/"
		sudo cp -f "$plugin_src" "$meta_lib_dir/"
		if [[ -f "$BUILD_DIR/test_plugins/libfalcon_meta_service_test_plugin.so" ]]; then
			sudo cp -f "$BUILD_DIR/test_plugins/libfalcon_meta_service_test_plugin.so" "$meta_lib_dir/"
		fi
	else
		mkdir -p "$meta_lib_dir" "$meta_ext_dir"
		cp -f "$FALCONFS_DIR/falcon/falcon.so" "$meta_lib_dir/"
		cp -f "$FALCONFS_DIR/falcon/falcon.control" "$meta_ext_dir/"
		cp -f "$FALCONFS_DIR/falcon/falcon--1.0.sql" "$meta_ext_dir/"
		cp -f "$plugin_src" "$meta_lib_dir/"
		if [[ -f "$BUILD_DIR/test_plugins/libfalcon_meta_service_test_plugin.so" ]]; then
			cp -f "$BUILD_DIR/test_plugins/libfalcon_meta_service_test_plugin.so" "$meta_lib_dir/"
		fi
	fi

	echo "Coverage service artifacts prepared in $FALCONFS_INSTALL_DIR/falcon_meta"
}

start_local_service_for_coverage() {
	echo "Starting local FalconFS service for service-dependent coverage tests..."
	local service_server_ip
	local service_server_port
	local service_cn_port
	service_server_ip="$(resolve_service_test_server_ip)"
	service_server_port="$(resolve_service_test_server_port)"
	service_cn_port="$(resolve_service_test_cn_port)"

	for attempt in 1 2; do
		echo "Service startup attempt ${attempt}/2"
		rm -rf "$SERVICE_COVERAGE_GCOV_PREFIX"
		mkdir -p "$SERVICE_COVERAGE_GCOV_PREFIX"
		local gcov_prefix_strip
		gcov_prefix_strip="$(printf '%s' "$FALCONFS_DIR" | awk -F/ '{print NF-1}')"
		bash -lc "export GCOV_PREFIX='$SERVICE_COVERAGE_GCOV_PREFIX'; export GCOV_PREFIX_STRIP='$gcov_prefix_strip'; source '$FALCONFS_DIR/deploy/falcon_env.sh' && '$FALCONFS_DIR/deploy/falcon_start.sh'"
		if wait_for_service_endpoint "$service_server_ip" "$service_server_port" 90 &&
			wait_for_falcon_meta_ready "$service_server_ip" "$service_cn_port" 90; then
			sleep 2
			return 0
		fi

		echo "Service is not ready on endpoints ${service_server_ip}:${service_server_port} and ${service_server_ip}:${service_cn_port} after startup attempt ${attempt}" >&2
		stop_local_service_for_coverage || true
		sleep 2
	done

	echo "Failed to start local FalconFS service on ${service_server_ip}:${service_server_port}" >&2
	return 1
}

stop_local_service_for_coverage() {
	echo "Stopping local FalconFS service..."
	bash -lc "
		stop_pg_with_timeout() {
			local data_dir=\"\$1\"
			local stop_timeout=\"\${2:-15}\"
			if [[ ! -f \"\$data_dir/postmaster.pid\" ]]; then
				return 0
			fi
			if command -v timeout >/dev/null 2>&1; then
				timeout --foreground \"\${stop_timeout}s\" pg_ctl stop -D \"\$data_dir\" -m fast >/dev/null 2>&1 || true
			else
				pg_ctl stop -D \"\$data_dir\" -m fast >/dev/null 2>&1 || true
			fi
		}

		meta_config='$FALCONFS_DIR/deploy/meta/falcon_meta_config.sh'
		if [[ -f \"\$meta_config\" ]]; then
			source \"\$meta_config\" >/dev/null 2>&1 || true
			if [[ \"\${cnIp:-}\" == \"\${localIp:-}\" ]]; then
				cn_path=\"\${cnPathPrefix}0\"
				stop_pg_with_timeout \"\$cn_path\"
			fi
			for ((n = 0; n < \${#workerIpList[@]}; n++)); do
				worker_ip=\"\${workerIpList[\$n]}\"
				if [[ \"\$worker_ip\" == \"\${localIp:-}\" ]]; then
					for ((i = 0; i < \${workerNumList[\$n]}; i++)); do
						worker_path=\"\${workerPathPrefix}\$i\"
						stop_pg_with_timeout \"\$worker_path\"
					done
				fi
			done
		fi
	"
	bash -lc "source '$FALCONFS_DIR/deploy/falcon_env.sh' && '$FALCONFS_DIR/deploy/falcon_stop.sh'"
}
