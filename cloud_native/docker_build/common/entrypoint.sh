#!/bin/bash
set -e

echo "FalconFS Container Entry Point"
echo "NODE_TYPE: $NODE_TYPE"
echo "FALCONFS_INSTALL_DIR: $FALCONFS_INSTALL_DIR"

# 设置默认的安装目录
FALCONFS_INSTALL_DIR=${FALCONFS_INSTALL_DIR:-/usr/local/falconfs}
DATA_DIR=${FALCONFS_INSTALL_DIR}/data
METADATA_DIR=${DATA_DIR}/metadata

# 根据节点类型执行不同的启动逻辑
case "$NODE_TYPE" in
  cn)
        echo "Starting CN node..."
        # CN 的启动逻辑
        if [ ! -d "${METADATA_DIR}" ]; then
            echo "Initializing PostgreSQL metadata..."
            chown falconMeta:falconMeta ${DATA_DIR}
            chmod 777 ${DATA_DIR}
            mkdir -p ${METADATA_DIR}
            chown falconMeta:falconMeta ${METADATA_DIR}
            chmod 777 ${METADATA_DIR}
        fi
        exec su -s /bin/bash falconMeta -c "bash ${FALCONFS_INSTALL_DIR}/falcon_cn/docker-entrypoint.sh"
        ;;

  dn)
        echo "Starting DN node..."
        # DN 的启动逻辑
        if [ ! -d "${METADATA_DIR}" ]; then
            echo "Initializing PostgreSQL metadata..."
            chown falconMeta:falconMeta ${DATA_DIR}
            chmod 777 ${DATA_DIR}
            mkdir -p ${METADATA_DIR}
            chown falconMeta:falconMeta ${METADATA_DIR}
            chmod 777 ${METADATA_DIR}
        fi
        exec su -s /bin/bash falconMeta -c "bash ${FALCONFS_INSTALL_DIR}/falcon_dn/docker-entrypoint.sh"
        ;;

  store)
        echo "Starting Store node..."
        # Store 的启动逻辑
        exec bash ${FALCONFS_INSTALL_DIR}/falcon_store/docker-entrypoint.sh
        ;;

  *)
        echo "ERROR: Unknown NODE_TYPE: $NODE_TYPE"
        echo "Valid values: cn, dn, store"
        exit 1
        ;;
esac
