#!/bin/bash
# 设置默认的安装目录
FALCONFS_INSTALL_DIR=${FALCONFS_INSTALL_DIR:-/usr/local/falconfs}
DATA_DIR=${FALCONFS_INSTALL_DIR}/data
METADATA_DIR=${DATA_DIR}/metadata
STARTUP_SCRIPT=${FALCONFS_INSTALL_DIR}/falcon_dn/start.sh

if [ ! -d "${METADATA_DIR}" ]; then
    chown falconMeta:falconMeta ${DATA_DIR}
    chmod 777 ${DATA_DIR}
    mkdir -p ${METADATA_DIR}
    chown falconMeta:falconMeta ${METADATA_DIR}
    chmod 777 ${METADATA_DIR}

    # 安装扩展文件到 PostgreSQL 系统目录
    # PostgreSQL 在编译时确定扩展文件 (.control, .sql) 的查找位置，无法通过配置修改
    # 因此需要将扩展文件安装到系统目录
    PG_EXT_DIR="$(pg_config --sharedir)/extension"
    PG_LIB_DIR="$(pg_config --pkglibdir)"
    echo "Installing Falcon extension files to PostgreSQL system directories..."
    cp -f ${FALCONFS_INSTALL_DIR}/falcon_meta/share/extension/falcon* "$PG_EXT_DIR/" 2>/dev/null || true
    cp -f ${FALCONFS_INSTALL_DIR}/falcon_meta/lib/postgresql/falcon*.so "$PG_LIB_DIR/" 2>/dev/null || true
fi

exec su falconMeta -c "bash ${STARTUP_SCRIPT} >${DATA_DIR}/start.log 2>&1"