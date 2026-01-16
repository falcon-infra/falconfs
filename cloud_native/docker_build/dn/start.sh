#!/bin/bash
export PATH=/home/falconMeta/metadb/bin/:$PATH
export LD_LIBRARY_PATH=/home/falconMeta/metadb/lib/
if [ -d "/home/falconMeta/data/metadata/pg_wal" ]; then
    pg_ctl restart -D /home/falconMeta/data/metadata
else
    initdb -D /home/falconMeta/data/metadata
    cp /home/falconMeta/postgresql_falcon.conf /home/falconMeta/data/metadata/postgresql.conf
    echo "host all all 0.0.0.0/0 trust" >>/home/falconMeta/data/metadata/pg_hba.conf
    echo "host replication all 0.0.0.0/0 trust" >>/home/falconMeta/data/metadata/pg_hba.conf
    echo "shared_preload_libraries='falcon'" >>/home/falconMeta/data/metadata/postgresql.conf
    echo "listen_addresses='*'" >>/home/falconMeta/data/metadata/postgresql.conf
    echo "wal_level=logical" >>/home/falconMeta/data/metadata/postgresql.conf
    echo "max_wal_senders=10" >>/home/falconMeta/data/metadata/postgresql.conf
    echo "hot_standby=on" >>/home/falconMeta/data/metadata/postgresql.conf
    echo "synchronous_commit=on" >>/home/falconMeta/data/metadata/postgresql.conf
    echo "falcon_connection_pool.port = ${NODE_PORT:-5442}" >> /home/falconMeta/data/metadata/postgresql.conf
    echo "falcon_plugin.directory = '/FalconFS/plugins'" >> /home/falconMeta/data/metadata/postgresql.conf
    echo "falcon.local_ip = '${NODE_IP:-127.0.0.1}'" >> /home/falconMeta/data/metadata/postgresql.conf
    # default replica_server_num set to 2, compatible to ADS.
    replica_server_num=${replica_server_num:-2}
    sync_replica_num=$(((replica_server_num + 1) / 2))
    if [ "${replica_server_num}" == "0" ]; then
        echo "synchronous_standby_names=''" >>/home/falconMeta/data/metadata/postgresql.conf
    else
        echo "synchronous_standby_names='${sync_replica_num}(*)'" >>/home/falconMeta/data/metadata/postgresql.conf
    fi
    echo "full_page_writes=on" >>/home/falconMeta/data/metadata/postgresql.conf
    echo "wal_log_hints=on" >>/home/falconMeta/data/metadata/postgresql.conf
    echo "logging_collector=on" >>/home/falconMeta/data/metadata/postgresql.conf
    echo "log_filename='postgresql-.%a.log'" >>/home/falconMeta/data/metadata/postgresql.conf
    echo "log_truncate_on_rotation=on" >>/home/falconMeta/data/metadata/postgresql.conf
    echo "log_rotation_age=1440" >>/home/falconMeta/data/metadata/postgresql.conf
    echo "log_rotation_size=1000000" >>/home/falconMeta/data/metadata/postgresql.conf
    echo "falcon_connection_pool.pool_size = 64" >>/home/falconMeta/data/metadata/postgresql.conf
    echo "falcon_connection_pool.shmem_size = 256" >>/home/falconMeta/data/metadata/postgresql.conf
    echo "falcon_connection_pool.batch_size = 1024" >>/home/falconMeta/data/metadata/postgresql.conf
    echo "falcon_connection_pool.wait_adjust = 1" >>/home/falconMeta/data/metadata/postgresql.conf
    echo "falcon_connection_pool.wait_min = 1" >>/home/falconMeta/data/metadata/postgresql.conf
    echo "falcon_connection_pool.wait_max = 500" >>/home/falconMeta/data/metadata/postgresql.conf
    echo "falcon_communication.plugin_path = '/home/falconMeta/metadb/lib/postgresql/libbrpcplugin.so'" >>/home/falconMeta/data/metadata/postgresql.conf
    echo "falcon_communication.server_ip = '${POD_IP}'" >>/home/falconMeta/data/metadata/postgresql.conf
    pg_ctl start -D /home/falconMeta/data/metadata
fi

bash /home/falconMeta/rm_logs.sh &
python3 /home/falconMeta/falcon_cm/falcon_cm_dn.py
