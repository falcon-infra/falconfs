#! /bin/bash

FALCONFS_INSTALL_DIR=${FALCONFS_INSTALL_DIR:-/usr/local/falconfs}
DATA_DIR=${FALCONFS_INSTALL_DIR}/data
METADATA_DIR=${DATA_DIR}/metadata

killall python3 || true
psql -d postgres -c "CHECKPOINT;" || true
sleep 1

if [ -f "${METADATA_DIR}/postmaster.pid" ]; then
    pg_ctl stop -m immediate -D "${METADATA_DIR}" || true
fi
