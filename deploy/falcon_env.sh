#!/usr/bin/env bash

export FALCONFS_INSTALL_DIR="${FALCONFS_INSTALL_DIR:-/usr/local/falconfs}"
echo "Setting FALCONFS_INSTALL_DIR to ${FALCONFS_INSTALL_DIR}"
export CONFIG_FILE=$FALCONFS_INSTALL_DIR/falcon_client/config/config.json
export PATH=$FALCONFS_INSTALL_DIR/falcon_client/bin:"$(pg_config --bindir)":$PATH
export LD_LIBRARY_PATH=/usr/local/obs/lib:"$(pg_config --libdir)":$LD_LIBRARY_PATH
