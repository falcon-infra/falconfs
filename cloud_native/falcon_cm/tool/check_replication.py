import logging
from kazoo.client import KazooClient
import psycopg2
import os


def get_zk_nodes(zk_hosts, path):
    zk = KazooClient(hosts=zk_hosts)
    zk.start()

    nodes = {}
    if zk.exists(path):
        children = zk.get_children(path)
        for child in children:
            ip_port, _ = zk.get("{}/{}".format(path, child))
            ip = ip_port.decode('utf-8').split(':', 1)[0]
            nodes[child] = ip

    zk.stop()
    return nodes

def get_db_servers(db_host, db_port, db_name, user):
    conn = psycopg2.connect(host=db_host, port=db_port, dbname=db_name, user=user)
    cursor = conn.cursor()
    cursor.execute("SELECT server_name,host FROM falcon_foreign_server;")
    db_nodes = {str(row[0]):row[1] for row in cursor.fetchall()}

    cursor.close()
    conn.close()
    return db_nodes

def compare_nodes(zk_nodes, db_nodes, isDict=True):
    if isDict:
        zk_set = set(zk_nodes.items())
        db_set = set(db_nodes.items())
    else:
        zk_set = set(zk_nodes)
        db_set = set(db_nodes)
    if zk_set == db_set:
        return True
    else:
        return False

def check_replication(ip, db_port, db_name, user):
    conn = psycopg2.connect(host=ip, port=db_port, dbname=db_name, user=user)
    cursor = conn.cursor()
    cursor.execute("SELECT client_addr,state,sync_state FROM pg_stat_replication;")
    replication_info = cursor.fetchall()

    cursor.close()
    conn.close()
    return replication_info

def check_replication_status():
    zk_endpoint = os.getenv("zk_endpoint")
    cluster_name = os.environ.get("cluster_name", "/falcon")
    nodes_path = "{}/leaders".format(cluster_name)
    db_port = int(os.environ.get("meta_port", "5432"))
    db_name = "postgres"
    user = os.environ.get("user_name", "falconMeta")
    cluster_healthy = True
    error_msg = ""
    error_node = ""
    zk_nodes = get_zk_nodes(zk_endpoint, nodes_path)

    if len(zk_nodes) == 0:
        print("ZK is in error state!")
        error_msg = error_msg + "ZK is in error state!"
        error_node = "ZK"
        return False, error_msg, error_node
    if 'cn' not in zk_nodes.keys():
        print("No CN in ZK!")
        error_msg = error_msg + "No CN in ZK!"
        error_node = "CN"
        return False, error_msg, error_node
    db_host = zk_nodes["cn"]
    db_nodes = get_db_servers(db_host, db_port, db_name, user)
    if compare_nodes(zk_nodes, db_nodes):
        for node, ip in zk_nodes.items():
            replication_info = check_replication(ip, db_port, db_name, user)
            replicas_path = "{}/falcon_clusters/{}/replicas".format(cluster_name, node)
            replicas_info_zk = get_zk_nodes(zk_endpoint, replicas_path)
            if len(replication_info) == 2 and all(state == 'streaming' for _, state, _ in replication_info):
                db_replica_nodes = [client_addr for client_addr, _, _ in replication_info]
                zk_replica_nodes = [ipport.split(":", 1)[0] for ipport in replicas_info_zk.keys()]
                if compare_nodes(db_replica_nodes, zk_replica_nodes, False):
                    print("node {} is OK!".format(node))
                else:
                    print("node {}({}) replicas info in ZK and metaserver is not the same!".format(node, ip))
                    print("replication info is: {}".format(replication_info))
                    print("replication info in zk is: {}".format(replicas_info_zk))
                    error_msg = error_msg + "node {}({}) replicas info in ZK and metaserver is not the same!".format(node, ip)
                    error_node = error_node + node + ":" + ip + ";"
                    cluster_healthy = False
            else:
                print("node {} {} is not OK!".format(node, ip))
                print("replication info is: {}".format(replication_info))
                error_msg = error_msg + "node {} {} is not OK!".format(node, ip)
                error_node = error_node + node + ":" + ip + ";"
    else:
        print("nodes is not the same")
        error_msg = "The node info in ZK is not the same with metaserver"
        error_node = "Meta"
        cluster_healthy = False
    return cluster_healthy, error_msg, error_node

if __name__ == '__main__':
    check_replication_status()
