#!/bin/bash
# 设置默认的安装目录
FALCONFS_INSTALL_DIR=${FALCONFS_INSTALL_DIR:-/usr/local/falconfs}
DATA_DIR=${FALCONFS_INSTALL_DIR}/data
METADATA_DIR=${DATA_DIR}/metadata
START_LOG_DIR=${FALCON_CN_DN_START_LOG_DIR:-${DATA_DIR}}
START_LOG_FILE=${START_LOG_DIR}/start.log
STARTUP_SCRIPT=${FALCONFS_INSTALL_DIR}/falcon_cn/start.sh

resolve_postgres_extension_paths() {
    PG_SHARE_DIR=${PG_SHAREDIR:-}
    PG_PKG_LIB_DIR=${PG_PKGLIBDIR:-}

    if [ -z "${PG_SHARE_DIR}" ] || [ -z "${PG_PKG_LIB_DIR}" ]; then
        if command -v pg_config >/dev/null 2>&1; then
            [ -z "${PG_SHARE_DIR}" ] && PG_SHARE_DIR="$(pg_config --sharedir)"
            [ -z "${PG_PKG_LIB_DIR}" ] && PG_PKG_LIB_DIR="$(pg_config --pkglibdir)"
        fi
    fi

    missing_pg_vars=()
    [ -z "${PG_SHARE_DIR}" ] && missing_pg_vars+=("PG_SHAREDIR")
    [ -z "${PG_PKG_LIB_DIR}" ] && missing_pg_vars+=("PG_PKGLIBDIR")
    if [ "${#missing_pg_vars[@]}" -gt 0 ]; then
        echo "ERROR: PostgreSQL extension paths are incomplete and pg_config is unavailable." >&2
        echo "Missing: ${missing_pg_vars[*]}" >&2
        echo "Install the PostgreSQL devel package that provides pg_config, or set:" >&2
        echo "  export PG_SHAREDIR=/path/to/postgresql/share" >&2
        echo "  export PG_PKGLIBDIR=/path/to/postgresql/pkglib" >&2
        exit 1
    fi
}

mkdir -p "${START_LOG_DIR}"
chown falconMeta:falconMeta "${START_LOG_DIR}"
chmod 775 "${START_LOG_DIR}"
touch "${START_LOG_FILE}"
chown falconMeta:falconMeta "${START_LOG_FILE}"

if [ ! -d "${METADATA_DIR}" ]; then
    chown falconMeta:falconMeta "${DATA_DIR}"
    chmod 777 "${DATA_DIR}"
    mkdir -p "${METADATA_DIR}"
    chown falconMeta:falconMeta "${METADATA_DIR}"
    chmod 777 "${METADATA_DIR}"

    # 安装扩展文件到 PostgreSQL 系统目录
    # PostgreSQL 在编译时确定扩展文件 (.control, .sql) 的查找位置，无法通过配置修改
    # 因此需要将扩展文件安装到系统目录
    resolve_postgres_extension_paths
    PG_EXT_DIR="${PG_SHARE_DIR}/extension"
    PG_LIB_DIR="${PG_PKG_LIB_DIR}"
    echo "Installing Falcon extension files to PostgreSQL system directories..."
    cp -f "${FALCONFS_INSTALL_DIR}"/falcon_meta/share/extension/falcon* "${PG_EXT_DIR}/" 2>/dev/null || true
    chmod 644 "${PG_EXT_DIR}"/falcon* 2>/dev/null || true
    cp -f "${FALCONFS_INSTALL_DIR}"/falcon_meta/lib/postgresql/falcon*.so "${PG_LIB_DIR}/" 2>/dev/null || true
    chmod 755 "${PG_LIB_DIR}"/falcon*.so 2>/dev/null || true
    echo "Falcon extension files installed."
fi

exec su -s /bin/bash falconMeta -c "bash ${STARTUP_SCRIPT} >${START_LOG_FILE} 2>&1"
