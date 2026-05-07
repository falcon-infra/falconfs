#!/bin/bash

# Compatibility entrypoint for the chaos helper library.
# Keep source order explicit so the functional areas are easy to navigate.

CHAOS_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)/lib"

# shellcheck source=/dev/null
source "${CHAOS_LIB_DIR}/runtime.sh"
# shellcheck source=/dev/null
source "${CHAOS_LIB_DIR}/actions.sh"
# shellcheck source=/dev/null
source "${CHAOS_LIB_DIR}/recovery_report.sh"
# shellcheck source=/dev/null
source "${CHAOS_LIB_DIR}/diagnostics.sh"
# shellcheck source=/dev/null
source "${CHAOS_LIB_DIR}/infra_health.sh"
# shellcheck source=/dev/null
source "${CHAOS_LIB_DIR}/data_dirs.sh"
