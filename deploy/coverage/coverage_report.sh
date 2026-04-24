#!/usr/bin/env bash

generate_coverage_report() {
	require_coverage_tools
	local gcov_tool
	gcov_tool="$(resolve_gcov_tool)"
	local coverage_dir="$BUILD_DIR/coverage"
	local baseline_build_info="$coverage_dir/baseline_build.info"
	local baseline_falcon_info="$coverage_dir/baseline_falcon.info"
	local raw_build_info="$coverage_dir/raw_build.info"
	local raw_falcon_info="$coverage_dir/raw_falcon.info"
	local raw_service_info="$coverage_dir/raw_service.info"
	local merged_info="$coverage_dir/merged.info"
	local filtered_info="$coverage_dir/filtered.info"
	local html_dir="$coverage_dir/html"

	mkdir -p "$coverage_dir"
	lcov --capture --initial --directory "$BUILD_DIR" --gcov-tool "$gcov_tool" --ignore-errors mismatch,negative,empty --output-file "$baseline_build_info"
	lcov --capture --initial --directory "$FALCONFS_DIR/falcon" --gcov-tool "$gcov_tool" --ignore-errors mismatch,negative,empty --output-file "$baseline_falcon_info"
	lcov --capture --directory "$BUILD_DIR" --gcov-tool "$gcov_tool" --ignore-errors mismatch,negative,empty --output-file "$raw_build_info"
	local falcon_gcda_sample
	falcon_gcda_sample="$(find "$FALCONFS_DIR/falcon" -type f -name '*.gcda' -print -quit 2>/dev/null || true)"
	if [[ -n "$falcon_gcda_sample" ]]; then
		lcov --capture --directory "$FALCONFS_DIR/falcon" --gcov-tool "$gcov_tool" --ignore-errors mismatch,negative,empty --output-file "$raw_falcon_info"
	else
		cp "$baseline_falcon_info" "$raw_falcon_info"
	fi
	local service_gcda_sample=""
	service_gcda_sample="$(find "$SERVICE_COVERAGE_GCOV_PREFIX" -type f -name '*.gcda' -print -quit 2>/dev/null || true)"
	if [[ -n "$service_gcda_sample" ]]; then
		remove_filtered_service_gcda
		lcov --capture --directory "$SERVICE_COVERAGE_GCOV_PREFIX" --build-directory "$FALCONFS_DIR" --gcov-tool "$gcov_tool" --ignore-errors mismatch,negative,empty --output-file "$raw_service_info"
		lcov -a "$baseline_build_info" -a "$baseline_falcon_info" -a "$raw_build_info" -a "$raw_falcon_info" -a "$raw_service_info" --output-file "$merged_info"
	else
		lcov -a "$baseline_build_info" -a "$baseline_falcon_info" -a "$raw_build_info" -a "$raw_falcon_info" --output-file "$merged_info"
	fi
	lcov --remove "$merged_info" --ignore-errors unused '/usr/*' '*/third_party/*' '*/tests/*' '*/build/*' '*/cmake/*' '*/CMakeFiles/*' '*/generated/*' '*/build/generated/*' '*.pb.cc' '*.pb.h' '*.pb.c' '*.pb.hpp' '*.pb' '*.fbs.h' '*/brpc_comm_adapter/proto/*' '*/connection_pool/fbs/*' --output-file "$filtered_info"
	genhtml "$filtered_info" --output-directory "$html_dir" --title "FalconFS Coverage"
	echo "Coverage report generated: $html_dir/index.html"
}

run_coverage() {
	BUILD_TYPE="Debug"
	COVERAGE=true
	clean_coverage_data
	clean_falconfs
	build_falconfs
	if [[ "$RUN_LOCAL_SERVICE_FOR_COVERAGE" == true ]]; then
		prepare_coverage_service_artifacts
	fi
	local local_service_started=false
	cleanup_coverage_local_service() {
		if [[ "$local_service_started" == true ]]; then
			stop_local_service_for_coverage
			local_service_started=false
		fi
	}
	trap cleanup_coverage_local_service RETURN

	if [[ "$RUN_LOCAL_SERVICE_FOR_COVERAGE" == true ]]; then
		start_local_service_for_coverage
		local_service_started=true
		run_unit_tests
	else
		run_non_service_unit_tests
		echo "All unit tests passed."
	fi

	if [[ "$local_service_started" == true ]]; then
		stop_local_service_for_coverage
		local_service_started=false
	fi
	generate_coverage_report
}
