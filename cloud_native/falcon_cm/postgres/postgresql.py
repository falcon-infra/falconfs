import logging
import time
import os
import psycopg2
import threading
from postgres import shell
import subprocess


def clear_liveness_file():
    logging.getLogger("logger").info("Clear liveness file")
    shell.exec_cmd("> /home/falconMeta/check_liveness.sh")


def restore_liveness_file():
    content = """#!/bin/bash
pg_isready -d postgres -U falconMeta --timeout=5 --quiet
if [ $? != 0 ]; then
    exit 1;
fi
if [ $? != 0 ]; then
    exit 1;
fi
isMonitor=`ps aux | grep python3 | grep -v grep | wc -l`
if [ "${isMonitor}" = "0" ]; then
    exit 1;
else
    exit 0;
fi
    """
    with open("/home/falconMeta/check_liveness.sh", "w") as f:
        f.write(content)
    logging.getLogger("logger").info("Restore liveness file")


def check_process_by_name(process_name):
    try:
        result = subprocess.run(
            ["ps", "aux"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        if process_name.lower() in result.stdout.lower():
            return True
        else:
            return False
    except Exception as e:
        logging.getLogger("logger").error(f"Error checking process by name: {e}")
        return False


def execute(connection_string, sql):
    """Execute sql statement"""
    conn = None
    res = False
    try:
        conn = psycopg2.connect(dsn=connection_string)
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        cursor.close()
        res = True
    except Exception as e:
        logging.getLogger("logger").error(f"Error executing sql: {e}")
    finally:
        if conn:
            conn.close()
    return res


def send_checkpoint(connection_string):
    try_num = 10
    while not check_process_by_name("pg_basebackup") and try_num >= 0:
        try_num -= 1
        time.sleep(1)
    sql = "checkpoint;"
    execute(connection_string, sql)
    logging.getLogger("logger").info("Send checkpoint")


def try_fetch_one(connection_string, sql):
    """Executes SQL and returns first value if it exists, otherwise returns None."""
    conn = None
    res, err = None, True
    try:
        conn = psycopg2.connect(dsn=connection_string)
        cursor = conn.cursor()
        cursor.execute(sql)
        data = cursor.fetchone()
        res = data[0] if data is not None else data
        err = False
        cursor.close()
        return res, err
    except Exception as e:
        logging.getLogger("logger").error(f"Error fetching one: {e}")
    finally:
        if conn is not None:
            conn.close()
    return res, err


def alter_postgresql_config(connection_string, config_name, val):
    """Update Postgresql config value using ALTER SYSTEM SET command"""
    conn = None
    try:
        conn = psycopg2.connect(dsn=connection_string)
        conn.set_isolation_level(0)
        cursor = conn.cursor()
        sql = "ALTER SYSTEM SET {} TO '{}'".format(config_name, val)
        cursor.execute(sql)
        conn.commit()
        cursor.close()

        cursor = conn.cursor()
        sql = "SELECT pg_reload_conf();"
        cursor.execute(sql)
        fetch_result = cursor.fetchone()
        cursor.close()
        if not fetch_result:
            logging.getLogger("logger").error("Failed to reload postgresql config")
        return fetch_result
    except Exception as e:
        logging.getLogger("logger").error(f"Error altering postgres sql config: {e}")
    finally:
        if conn:
            conn.close()
    return False


def is_standby(data_dir):
    logging.getLogger("logger").info("Check if postgres is standby")
    return os.path.exists(os.path.join(data_dir, "standby.signal"))


def update_node_table(cnhost, cnport, user, nodeid, ip, port):
    nodeid = str(nodeid)
    port = str(port)
    cnport = str(cnport)
    cn_connection_string = "host={} port={} user={} dbname=postgres".format(
        cnhost, cnport, user
    )
    logging.getLogger("logger").info(
        "Update node table: {} {} {} {} {}".format(nodeid, ip, port, cnhost, cnport)
    )
    sql = "SELECT * FROM falcon_update_foreign_server({}, '{}', {});".format(
        nodeid, ip, port
    )
    res = execute(cn_connection_string, sql)
    return res


def update_start_background_service(host, port, user):
    port = str(port)
    connection_string = "host={} port={} user={} dbname=postgres".format(
        host, port, user
    )
    logging.getLogger("logger").info(
        "Update start background service: {} {} {}".format(host, port, user)
    )
    sql = "SELECT * FROM falcon_start_background_service();"
    res = execute(connection_string, sql)
    return res


def reload_foreign_server_cache(cnhost, cnport, user):
    cnport = str(cnport)
    connection_string = "host={} port={} user={} dbname=postgres".format(
        cnhost, cnport, user
    )
    logging.getLogger("logger").info(
        "Reload foreign server cache: {} {} {}".format(cnhost, cnport, user)
    )
    sql = "SELECT * FROM falcon_reload_foreign_server_cache();"
    res = execute(connection_string, sql)
    return res


def is_running(data_dir):
    result = shell.exec_cmd("pg_ctl status -D {}".format(data_dir))
    if "is running" not in result:
        logging.getLogger("logger").info("Server is not running")
        return False
    return True


def pg_initdb(data_dir):
    shell.exec_cmd("initdb -D {}".format(data_dir))
    logging.getLogger("logger").info("Initialize postgres")


def pg_start(data_dir, log_filename):
    pid_path = os.path.join(data_dir, "postmaster.pid")
    if os.path.exists(pid_path):
        os.remove(pid_path)
    shell.exec_cmd("pg_ctl start -D {} -l {}".format(data_dir, log_filename))
    logging.getLogger("logger").info("Start postgres")


def pg_stop(data_dir):
    shell.exec_cmd("pg_ctl stop -D {} -m immediate".format(data_dir))
    logging.getLogger("logger").info("Stop postgres")


def pg_reload(data_dir):
    shell.exec_cmd("pg_ctl reload -D {} -w".format(data_dir))
    logging.getLogger("logger").info("Reload postgres")


def restart(data_dir):
    shell.exec_cmd("pg_ctl restart -D {} -w".format(data_dir))
    logging.getLogger("logger").info("Restart postgres")


def pg_promote(data_dir):
    shell.exec_cmd("pg_ctl promote -D {} -w".format(data_dir))
    logging.getLogger("logger").info("Promote postgres")


def pg_basebackup(data_dir, host, port, user, slot_name):
    port = str(port)
    connection_string = "host={} port={} user={} dbname=postgres".format(
        host, port, user
    )
    logging.getLogger("logger").info("start basebackup")
    shell.exec_cmd(
        "pg_basebackup -D {} -Fp -Pv -Xs -c fast -R -h {} -p {} -U {} --slot={}".format(
            data_dir, host, port, user, slot_name
        )
    )


def create_physical_replication_slot(leader_host, leader_port, user, slot_name):
    leader_port = str(leader_port)
    connection_string = "host={} port={} user={} dbname=postgres".format(
        leader_host, leader_port, user
    )
    sql = "SELECT * FROM pg_drop_replication_slot('{}');".format(slot_name)
    execute(connection_string, sql)
    sql = "SELECT * FROM pg_create_physical_replication_slot('{}');".format(
        slot_name
    )
    execute(connection_string, sql)
    logging.getLogger("logger").info("create physical replication slot: {}".format(slot_name))


def pg_rewind(data_dir, host, port, user, slot_name):
    port = str(port)
    connection_string = "host={} port={} user={} dbname=postgres".format(
        host, port, user
    )
    shell.exec_cmd(
        "pg_rewind -D {} --source-server='{}'".format(data_dir, connection_string)
    )
    shell.exec_cmd("touch {}".format(os.path.join(data_dir, "standby.signal")))
    shell.exec_cmd("> {}".format(os.path.join(data_dir, "postgresql.auto.conf")))
    shell.exec_cmd(
        "echo \"primary_conninfo = '{}'\" >> {}".format(
            connection_string, os.path.join(data_dir, "postgresql.auto.conf")
        )
    )
    shell.exec_cmd(
        "echo \"primary_slot_name = '{}'\" >> {}".format(
            slot_name, os.path.join(data_dir, "postgresql.auto.conf")
        )
    )


def is_standby_ready(connection_string):
    status, err = try_fetch_one(
        connection_string, "SELECT status FROM pg_stat_wal_receiver;"
    )
    if err or status is None or status != "streaming":
        return False
    return True


def clear_replication_slot(host, port, user, slot_host_name):
    port = str(port)
    connection_string = "host={} port={} user={} dbname=postgres".format(
        host, port, user
    )
    slot_name = slot_host_name.replace(".", "_").replace("-", "_")
    sql = "SELECT * FROM pg_drop_replication_slot('{}');".format(slot_name)
    execute(connection_string, sql)
    logging.getLogger("logger").info("Clear replication slot: {}".format(slot_name))


def clear_all_replication_slot(host, port, user):
    port = str(port)
    connection_string = "host={} port={} user={} dbname=postgres".format(
        host, port, user
    )
    sql = "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE NOT active;"
    execute(connection_string, sql)
    logging.getLogger("logger").info("Clear all replication slot")


def do_checkpoint(host, port, user):
    port = str(port)
    connection_string = "host={} port={} user={} dbname=postgres".format(
        host, port, user
    )
    sql = "CHECKPOINT;"
    execute(connection_string, sql)
    logging.getLogger("logger").info("Do checkpoint")


def do_demote(data_dir, host, port, user, local_host, local_port, local_host_node):
    logging.getLogger("logger").info("Demote the DB to standby using pg_rewind")
    clear_liveness_file()
    clear_all_replication_slot(local_host, local_port, user)
    do_checkpoint(local_host, local_port, user)
    pg_stop(data_dir)
    slot_name = local_host_node.replace(".", "_").replace("-", "_")
    create_physical_replication_slot(host, port, user, slot_name)
    pg_rewind(data_dir, host, port, user, slot_name)
    pg_start(data_dir, "~/logfile")
    logging.getLogger("logger").info("Wait for the DB to be ready")
    time.sleep(10)
    logging.getLogger("logger").info("Check if the DB is ready")
    local_port = str(local_port)
    connection_string = "host={} port={} user={} dbname=postgres".format(
        local_host, local_port, user
    )
    ready = is_standby_ready(connection_string)
    if ready:
        logging.getLogger("logger").info("DB is ready")
        restore_liveness_file()
        return
    while not ready:
        logging.getLogger("logger").info(
            "DB is not ready, Demote the DB using pg_basebackup now"
        )
        pg_stop(data_dir)
        shell.exec_cmd("rm -rf {}/*".format(data_dir))
        pg_basebackup(data_dir, host, port, user, slot_name)
        pg_start(data_dir, "~/logfile")
        time.sleep(10)
        ready = is_standby_ready(connection_string)
    logging.getLogger("logger").info("Demote the DB using pg_basebackup successfully.")
    restore_liveness_file()


def do_promote(data_dir, host, port, user):
    port = str(port)
    connection_string = "host={} port={} user={} dbname=postgres".format(
        host, port, user
    )
    logging.getLogger("logger").info("Promote the DB to primary")
    pg_promote(data_dir)
    shell.exec_cmd("> {}".format(os.path.join(data_dir, "postgresql.auto.conf")))
    alter_postgresql_config(connection_string, "synchronous_commit", "on")
    alter_postgresql_config(connection_string, "synchronous_standby_names", "*")
    logging.getLogger("logger").info("Promote the DB to primary successfully.")


def change_following_leader(data_dir, leader_host, leader_port, user, local_host_node):
    logging.getLogger("logger").info("Change following leader")
    slot_name = local_host_node.replace(".", "_").replace("-", "_")
    create_physical_replication_slot(leader_host, leader_port, user, slot_name)
    leader_port = str(leader_port)
    connection_string = "host={} port={} user={} dbname=postgres".format(
        leader_host, leader_port, user
    )
    logging.getLogger("logger").info(
        "Change following leader, the new leader is {}".format(leader_host)
    )
    shell.exec_cmd("> {}".format(os.path.join(data_dir, "postgresql.auto.conf")))
    shell.exec_cmd(
        "echo \"primary_conninfo = '{}'\" >> {}".format(
            connection_string, os.path.join(data_dir, "postgresql.auto.conf")
        )
    )
    shell.exec_cmd(
        "echo \"primary_slot_name = '{}'\" >> {}".format(
            slot_name, os.path.join(data_dir, "postgresql.auto.conf")
        )
    )
    pg_reload(data_dir)
    logging.getLogger("logger").info("Change following leader successfully.")


def lsn_to_num(lsn):
    if not lsn:
        return 0
    log, offset = lsn.split("/")
    num = int(log, 16) << 32 | int(offset, 16)
    return num


def get_receive_lsn(connection_string, sql_string):
    lsn = None
    try:
        conn = psycopg2.connect(dsn=connection_string)
        cursor = conn.cursor()
        cursor.execute(sql_string)
        data = cursor.fetchone()
        lsn = data[0] if data is not None else data
        cursor.close()
    except Exception as e:
        cursor.execute(sql_string)
        data = cursor.fetchone()
        lsn = data[0] if data is not None else data
        cursor.close()
    finally:
        if conn is not None:
            conn.close()
    logging.getLogger("logger").info("Get receive lsn: {}".format(lsn))
    return lsn


def get_lsn(host, port, user):
    sql_get_lsn = "SELECT * FROM pg_last_wal_receive_lsn();"
    sql_get_lsn_falcon = "SELECT * FROM pg_last_wal_receive_lsn_for_falcon();"
    port = str(port)
    connection_string = "host={} port={} user={} dbname=postgres".format(
        host, port, user
    )
    lsn = get_receive_lsn(connection_string, sql_get_lsn)
    lsn_falcon = get_receive_lsn(connection_string, sql_get_lsn_falcon)
    max_lsn = max(lsn_to_num(lsn), lsn_to_num(lsn_falcon))
    return max_lsn


def demote_for_start(data_dir, host, port, user, local_host_node):
    logging.getLogger("logger").info("Demote the DB using pg_basebackup for start")
    clear_liveness_file()
    pg_stop(data_dir)
    slot_name = local_host_node.replace(".", "_").replace("-", "_")
    create_physical_replication_slot(host, port, user, slot_name)
    shell.exec_cmd("rm -rf {}/*".format(data_dir))
    pg_basebackup(data_dir, host, port, user, slot_name)
    pg_start(data_dir, "~/logfile")
    restore_liveness_file()
    logging.getLogger("logger").info(
        "Demote the DB using pg_basebackup for start successfully."
    )


def stop_replication(host, port, user):
    logging.getLogger("logger").info("Stop the replication")
    port = str(port)
    connection_string = "host={} port={} user={} dbname=postgres".format(
        host, port, user
    )
    res1 = alter_postgresql_config(connection_string, "primary_conninfo", "")
    res2 = alter_postgresql_config(connection_string, "primary_slot_name", "")
    res = res1 and res2
    return res


def clean_for_supplement(host, port, user):
    logging.getLogger("logger").info("Clean the DB for supplement")
    res = stop_replication(host, port, user)
    if res:
        logging.getLogger("logger").info("Stop the replication successfully.")
    else:
        logging.getLogger("logger").error("Failed to stop the replication")
