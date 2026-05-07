#!/bin/bash

# Long-run entry configuration.
#
# Usage:
#   1) Edit values in this file.
#   2) Run: bash tests/chaos/run_longrun.sh

# Image/runtime
export LONGRUN_IMAGE="localhost:5000/falconfs-full-ubuntu24.04:v0.1.2-chaosmatrix"
export LONGRUN_SKIP_IMAGE_BUILD=1

# Output path (relative path is resolved from repo root).
# Leave empty to auto-generate timestamped path.
export LONGRUN_DATA_PATH=""

# Suite-level controls
export LONGRUN_MATRIX_FILE="tests/chaos/chaos_case_matrix_default.txt"
export LONGRUN_STRICT_COVERAGE=1
export LONGRUN_FAIL_FAST=0
export LONGRUN_FRESH_START=1
export LONGRUN_SEED=20260305
export LONGRUN_STAGES="${LONGRUN_STAGES:-single,dual,triple}"

# Duration/profile
export LONGRUN_SINGLE_DURATION_MIN=60
export LONGRUN_DUAL_DURATION_MIN=120
export LONGRUN_TRIPLE_DURATION_MIN=120
export LONGRUN_INTERVAL_SEC=300
export LONGRUN_HEALTH_TIMEOUT_SEC=300
export LONGRUN_STARTUP_TIMEOUT_SEC=600
export LONGRUN_POST_ACTION_GRACE_SEC=15
export LONGRUN_STABLE_WINDOW_SEC=30
export LONGRUN_RELIABILITY_TIMEOUT_SEC=900
export LONGRUN_PROBE_MODE="meta"

# Action sets per topology
export LONGRUN_ACTIONS_SINGLE="S01,S03,S04,S05"
export LONGRUN_ACTIONS_DUAL="S01,S02,S03,S04,S05,S06,S07,S08"
export LONGRUN_ACTIONS_TRIPLE="S01,S02,S03,S04,S05,S06,S07,S08"

# Optional per-action hold overrides for quick validation.
# Leave empty to use wait_replica_time+30 for S06/S07/S08.
export LONGRUN_ACTION_HOLD_SECS="${LONGRUN_ACTION_HOLD_SECS:-}"

# Core-once options
export LONGRUN_CORE_ONCE_ENABLE=1
export LONGRUN_CORE_LIMIT_KB=1048576
export LONGRUN_CORE_ONCE_DIR=""
