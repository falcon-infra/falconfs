import psycopg2
import logging
import time


def split_ip_port(leader_info):
    split_str = leader_info.split(":")
    return split_str[0], split_str[1]


def init_filesystem(leader_infos, user, replica_server_num):
    logger = logging.getLogger("logger")
    cluster_names = list(leader_infos.keys())
    server_num = len(cluster_names)

    create_extension = "CREATE EXTENSION falcon;"
    stat_replication_sql = "SELECT state from pg_stat_replication;"
    for i in range(server_num):
        name = cluster_names[i]
        ip, port = split_ip_port(leader_infos[name])
        conn = psycopg2.connect(host=ip, port=port, user=user, database="postgres")
        # check if the replication is ready
        with conn:
            cursor = conn.cursor()
            while replica_server_num > 0:
                cursor.execute(stat_replication_sql)
                res = cursor.fetchall()
                if len(res) >= replica_server_num and res[0][0] == "streaming":
                    logger.info("{}--pg_stat_replication results: {}".format(ip, name))
                    break
                time.sleep(0.5)
            cursor.execute(create_extension)
            conn.commit()
    for i in range(server_num):
        name = cluster_names[i]
        ip, port = split_ip_port(leader_infos[name])
        id = 0 if name == "cn" else int(name[2:]) + 1
        for j in range(server_num):
            to_ip, to_port = split_ip_port(leader_infos[cluster_names[j]])
            if i == j:
                is_local = "true"
            else:
                is_local = "false"
            insert_server_sql = "SELECT falcon_insert_foreign_server({}, '{}', '{}', {}, {}, '{}');".format(
                id, name, ip, port, is_local, user
            )
            conn = psycopg2.connect(
                host=to_ip, port=to_port, user=user, database="postgres"
            )
            with conn:
                cursor = conn.cursor()
                cursor.execute(insert_server_sql)
                conn.commit()
    shard_count = 100 * (server_num - 1)
    for i in range(server_num):
        ip, port = split_ip_port(leader_infos[cluster_names[i]])
        build_shard_map_sql = "SELECT falcon_build_shard_table({});SELECT falcon_create_distributed_data_table();SELECT falcon_start_background_service();".format(
            shard_count
        )
        conn = psycopg2.connect(host=ip, port=port, user=user, database="postgres")
        with conn:
            cursor = conn.cursor()
            cursor.execute(build_shard_map_sql)
            conn.commit()
    mkdir_sql = "SELECT * from falcon_plain_mkdir('/');"
    ip, port = split_ip_port(leader_infos["cn"])
    conn = psycopg2.connect(host=ip, port=port, user=user, database="postgres")
    with conn:
        cursor = conn.cursor()
        cursor.execute(mkdir_sql)
        conn.commit()
    logger.info("Init filesystem done")
