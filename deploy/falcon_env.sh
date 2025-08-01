#!/usr/bin/env bash

SCRIPT_DIR="$(readlink -f "$(dirname "${BASH_SOURCE[0]:-${(%):-%x}}")")"

export FALCONFS_HOME="${SCRIPT_DIR}/.."
echo "Setting FALCONFS_HOME to ${FALCONFS_HOME}"
export CONFIG_FILE=$FALCONFS_HOME/config/config.json
export FALCONFS_INSTALL_DIR=${FALCONFS_INSTALL_DIR:-/usr/local/falconfs}
