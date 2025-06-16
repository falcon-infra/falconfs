import os
import time
import logging
from postgres import postgresql
from utils.filesystem import *
from kazoo.client import KazooClient, KazooState
from kazoo.recipe.watchers import ChildrenWatch, DataWatch
from kazoo.protocol.states import WatchedEvent, EventType
from kazoo.exceptions import *
from tool.check_replication import *
from tool.monitor_object import *
import threading
import traceback


class FalconCM:
    def __init__(self, is_cn):
        self.logger = logging.getLogger("logger")
        self.init_falcon_env()
        self._dn_path = self._root_path + "/falcon_dns"
        self._cn_path = self._root_path + "/falcon_cn"
        self._cluster_path = self._root_path + "/falcon_clusters"
        self._leader_path = self._root_path + "/leaders"
        self._cn_supplement_path = self._root_path + "/cn_supplement"
        self._dn_supplement_path = self._root_path + "/dn_supplement"
        self._need_supplement_path = self._root_path + "/need_supplement"
        self._store_node_path = self._root_path + "/StoreNode"

        self._lost_node_time = {}
        self._watch_replica_lock = threading.Lock()
        self._watch_need_supplement_lock = threading.Lock()
        self._replica_change_num = 0
        self._need_supplement_num = 0
        self._thread_running = True
        self._is_cn = is_cn
        self._cluster_name = None
        self._cn_list = []
        self._dn_list = []
        self._cluster_names = []

        self._pgdata_dir = self._data_dir + "/metadata"

    def __del__(self):
        self._thread_running = False

    def init_falcon_env(self):
        self._hosts = os.environ.get("zk_endpoint")
        self._root_path = os.environ.get("cluster_name", "/falcon")
        self._user_name = os.environ.get("user_name", "falconMeta")
        self._pod_ip = os.environ.get("POD_IP")
        self._host_node_name = os.environ.get("NODE_NAME")
        self._meta_port = int(os.environ.get("meta_port", "5432"))
        self._timeout = float(os.environ.get("timeout", "10.0"))
        self._replica_server_num = int(os.environ.get("replica_server_num", "2"))
        self._dn_num = int(os.environ.get("dn_num"))
        self._cn_num = int(os.environ.get("cn_num"))
        self._dn_supplement_num = int(os.environ.get("dn_sup_num"))
        self._cn_supplement_num = int(os.environ.get("cn_sup_num"))
        self._wait_replica_time = int(os.environ.get("wait_replica_time", "600"))
        self._data_dir = os.environ.get("data_dir", "/home/falconMeta/data")
        self._check_meta_period = int(os.environ.get("CHECK_META_PERIOD", "2")) * 3600
        self._send_msg_dst = os.environ.get("REPORT_DST", "None")
        self._use_error_report = int(os.environ.get("USE_ERROR_REPORT", "0"))
    def connect_zk(self):
        try:
            self._zk_client = KazooClient(hosts=self._hosts, timeout=self._timeout)
            self._zk_client.start()
        except Exception as e:
            self.logger.error("Failed to connect to zookeeper: {}".format(e))
            raise e

    def register_node(self):
        node_info = "{}:{}".format(self._pod_ip, self._meta_port)
        if self._is_cn:
            self._zk_client.create(
                self._cn_path + "/" + self._host_node_name,
                value=str.encode(node_info),
                ephemeral=True,
            )
        else:
            self._zk_client.create(
                self._dn_path + "/" + self._host_node_name,
                value=str.encode(node_info),
                ephemeral=True,
            )

    def init_sys(self):
        """init the path for store the original cn and dn"""
        try:
            if self._zk_client.exists(self._root_path):
                if not self._zk_client.exists(self._cn_path):
                    self._zk_client.create(self._cn_path)
                if not self._zk_client.exists(self._dn_path):
                    self._zk_client.create(self._dn_path)
                if not self._zk_client.exists(self._leader_path):
                    self._zk_client.create(self._leader_path)
            else:
                self._zk_client.create(self._root_path)
                self._zk_client.create(self._cn_path)
                self._zk_client.create(self._dn_path)
                self._zk_client.create(self._leader_path)
            self.register_node()
        except NodeExistsError:
            try:
                self.register_node()
            except NodeExistsError:
                time.sleep(10)  # wait for the last ephemeral node to be removed
                self.register_node()
            except Exception as e:
                self.logger.error(
                    "Failed to init the path for store the original cn and dn: {}".format(
                        e
                    )
                )
                raise e

    def init_all_path(self):
        if not self._zk_client.exists(self._cluster_path):
            self._zk_client.create(self._cluster_path)
            self._zk_client.create(self._cn_supplement_path)
            self._zk_client.create(self._dn_supplement_path)
            self._zk_client.create(self._need_supplement_path)
            self._zk_client.create(self._cluster_path + "/cn")
            self._zk_client.create(self._cluster_path + "/cn/lastLeader")
            self._zk_client.create(self._cluster_path + "/cn/hostNodes")
            self._zk_client.create(self._cluster_path + "/cn/replicas")
            self._zk_client.create(self._cluster_path + "/cn/membership")
            self._zk_client.create(self._cluster_path + "/cn/candidates")
            self._zk_client.create(self._store_node_path)
            self._zk_client.create(self._store_node_path + "/Nodes")
            self._zk_client.create(
                self._store_node_path + "/storeNodeStatus", value=str.encode("0")
            )

    def leader_select(self):
        node_info = "{}:{}".format(self._pod_ip, self._meta_port)
        # find the cluster for the node
        self.find_node_cluster()
        leader_path = "{}/{}".format(self._leader_path, self._cluster_name)
        last_leader_path = "{}/{}/lastLeader".format(
            self._cluster_path, self._cluster_name
        )
        if self._zk_client.exists(self._root_path + "/ready"):
            last_leader, _ = self._zk_client.get(last_leader_path)
            last_leader = last_leader.decode("utf-8")
            if last_leader == self._host_node_name:
                time.sleep(10)
                self.watch_leader_and_candidates()
                try:
                    self._zk_client.create(
                        leader_path, value=str.encode(node_info), ephemeral=True
                    )
                    self._zk_client.set(
                        last_leader_path, value=str.encode(self._host_node_name)
                    )
                    self._cluster_leader_ip = self._pod_ip
                    self._is_leader = True
                except NodeExistsError:
                    self._is_leader = False
            else:
                self._is_leader = False
        else:
            if self._zk_client.exists(leader_path):
                self._is_leader = False
            else:
                try:
                    self._zk_client.create(
                        leader_path, value=str.encode(node_info), ephemeral=True
                    )
                    self._cluster_leader_ip = self._pod_ip
                    self._is_leader = True
                    if self._is_cn:
                        self.init_all_path()
                    self._zk_client.set(
                        last_leader_path, value=str.encode(self._host_node_name)
                    )
                except NodeExistsError:
                    self._is_leader = False
                except Exception as e:
                    self.logger.error(
                        "Failed to set the leader for the node: {}".format(e)
                    )
                    self._is_leader = False
        return self._is_leader

    def write_replica(self):
        """write the replica for the node"""
        if self._is_cn:
            found = False
            while not found:
                host_node_path = "{}/{}/hostNodes".format(
                    self._cluster_path, self._cluster_name
                )
                if not self._zk_client.exists(host_node_path):
                    time.sleep(1)
                    continue
                host_nodes_info = self._zk_client.get_children(host_node_path)
                if self._host_node_name in host_nodes_info:
                    self.logger.info(
                        "The node {} is already in the cluster, write the replica".format(
                            self._host_node_name
                        )
                    )
                    found = True
                    break
                else:
                    if len(
                        host_nodes_info
                    ) == self._replica_server_num + 1 and not self._zk_client.exists(
                        self._cn_supplement_path + "/" + self._host_node_name
                    ):
                        # add it to the supplement cluster
                        self._zk_client.create(
                            "{}/{}".format(
                                self._cn_supplement_path, self._host_node_name
                            ),
                            ephemeral=True,
                        )
                        self.logger.info(
                            "The node {} is not in any cluster, add it to the supplement cluster".format(
                                self._host_node_name
                            )
                        )
                        postgresql.clean_for_supplement(
                            self._pod_ip, self._meta_port, self._user_name
                        )
                    time.sleep(30)
        node_info = "{}:{}".format(self._pod_ip, self._meta_port)
        replica_path = "{}/{}/replicas/{}".format(
            self._cluster_path, self._cluster_name, node_info
        )
        membership_path = "{}/{}/membership/{}".format(
            self._cluster_path, self._cluster_name, self._host_node_name
        )
        leader_path = "{}/{}".format(self._leader_path, self._cluster_name)
        while not self._zk_client.exists(leader_path):
            time.sleep(1)
        data, _ = self._zk_client.get(leader_path)
        ip_port = data.decode("utf-8")
        self._cluster_leader_ip, _ = ip_port.split(":", 1)
        if self._zk_client.exists(self._root_path + "/ready"):
            host_path = "{}/{}/hostNodes/{}".format(
                self._cluster_path, self._cluster_name, self._host_node_name
            )
            if self._zk_client.exists(host_path):
                self.logger.info(
                    "The node {} is already in the cluster {}, write the replica".format(
                        self._host_node_name, self._cluster_name
                    )
                )
                flag_data, _ = self._zk_client.get(host_path)
                flag = flag_data.decode("utf-8")
                if flag == "new":
                    self.logger.info(
                        "The node {} is a new node, write the replica".format(
                            self._host_node_name
                        )
                    )
                    postgresql.demote_for_start(
                        self._pgdata_dir,
                        self._cluster_leader_ip,
                        self._meta_port,
                        self._user_name,
                    )
                    self._zk_client.set(host_path, value=str.encode(""))
                    self._zk_client.create(membership_path)
                else:
                    if not postgresql.is_standby(self._pgdata_dir):
                        try:
                            self._zk_client.delete(membership_path)
                        except NoNodeError:
                            pass
                        postgresql.do_demote(
                            self._pgdata_dir,
                            self._cluster_leader_ip,
                            self._meta_port,
                            self._user_name,
                            self._pod_ip,
                            self._meta_port,
                        )
                        self._zk_client.create(membership_path)
                    else:
                        postgresql.change_following_leader(
                            self._pgdata_dir,
                            self._cluster_leader_ip,
                            self._meta_port,
                            self._user_name,
                        )
        else:
            postgresql.demote_for_start(
                self._pgdata_dir,
                self._cluster_leader_ip,
                self._meta_port,
                self._user_name,
            )
        self._zk_client.create(replica_path, ephemeral=True)
        self.watch_leader_and_candidates()

    def find_node_cluster(self):
        """find the cluster for the node"""
        visited_cluster = []
        found_cluster = False
        if self._zk_client.exists(self._root_path + "/ready"):
            # the cluster is ready, find the cluster name
            clusters_info = self._zk_client.get_children(self._cluster_path)
            for cluster in clusters_info:
                host_node_path = "{}/{}/hostNodes".format(self._cluster_path, cluster)
                host_nodes_info = self._zk_client.get_children(host_node_path)
                if self._host_node_name in host_nodes_info:
                    if self._is_cn and cluster == "cn":
                        self._cluster_id = 0
                    elif cluster != "cn" and not self._is_cn:
                        self._cluster_id = int(cluster[2:]) + 1
                    else:
                        continue
                    self._cluster_name = cluster
                    found_cluster = True
                    break
            if not found_cluster:
                # the node is not in any cluster, add it to the supplement cluster
                if self._is_cn:
                    self._zk_client.create(
                        "{}/{}".format(self._cn_supplement_path, self._host_node_name),
                        ephemeral=True,
                    )
                else:
                    self._zk_client.create(
                        "{}/{}".format(self._dn_supplement_path, self._host_node_name),
                        ephemeral=True,
                    )
                postgresql.clean_for_supplement(
                    self._pod_ip, self._meta_port, self._user_name
                )
                self.logger.info(
                    "The node {} is not in any cluster, add it to the supplement cluster".format(
                        self._host_node_name
                    )
                )
        if self._is_cn:
            if not found_cluster:
                self._cluster_name = "cn"
                self._cluster_id = 0
            return
        dn_cluster_num = int((self._dn_num - self._dn_supplement_num) / 3)
        while not found_cluster:
            if not self._zk_client.exists(self._cluster_path):
                time.sleep(1)
                continue
            cluster_infos = self._zk_client.get_children(self._cluster_path)
            for cluster in cluster_infos:
                if cluster == "cn":
                    continue
                if cluster in visited_cluster:
                    continue
                host_node_path = "{}/{}/hostNodes".format(self._cluster_path, cluster)
                host_node_infos = self._zk_client.get_children(host_node_path)
                if self._host_node_name in host_node_infos:
                    self._cluster_name = cluster
                    self._cluster_id = int(cluster[2:]) + 1
                    found_cluster = True
                    break
                if len(host_node_infos) == self._replica_server_num + 1:
                    visited_cluster.append(cluster)
            if found_cluster:
                break
            if len(visited_cluster) > 0 and len(visited_cluster) == dn_cluster_num:
                visited_cluster.clear()
                if not self._zk_client.exists(
                    self._dn_supplement_path + "/" + self._host_node_name
                ):
                    self._zk_client.create(
                        "{}/{}".format(self._dn_supplement_path, self._host_node_name),
                        ephemeral=True,
                    )
                    postgresql.clean_for_supplement(
                        self._pod_ip, self._meta_port, self._user_name
                    )
                    self.logger.info(
                        "The node {} is not in any cluster, add it to the supplement cluster".format(
                            self._host_node_name
                        )
                    )
            time.sleep(10)

    def watch_leader_and_candidates(self):
        leader_path = "{}/{}".format(self._leader_path, self._cluster_name)
        candidates_path = "{}/{}/candidates".format(
            self._cluster_path, self._cluster_name
        )

        @self._zk_client.DataWatch(leader_path)
        def watch_leader(data, state, event):
            # if state == None and event == None:
            #     self.handle_leader_delete_event()
            if event:
                if event.type == EventType.DELETED:
                    self.handle_leader_delete_event()
                elif event.type == EventType.CREATED:
                    self.handle_leader_create_event()

        @self._zk_client.ChildrenWatch(candidates_path)
        def watch_candidates(candidates):
            self.handle_candidate_change_event(candidates)

    def handle_leader_delete_event(self):
        candidate_path = "{}/{}/candidates".format(
            self._cluster_path, self._cluster_name
        )
        if postgresql.is_standby(self._pgdata_dir) and self.is_in_membership():
            candidate_node_path = "{}/{}:{}".format(
                candidate_path, self._pod_ip, self._meta_port
            )
            if postgresql.stop_replication(
                self._pod_ip, self._meta_port, self._user_name
            ):
                self.logger.info("Stop the replication successfully")
            else:
                self.logger.error("Failed to stop the replication")
            lsn = postgresql.get_lsn(self._pod_ip, self._meta_port, self._user_name)
            if not self._zk_client.exists(candidate_node_path):
                self._zk_client.create(candidate_node_path, value=str.encode(str(lsn)))
            else:
                self._zk_client.set(candidate_node_path, value=str.encode(str(lsn)))
        else:
            self.logger.info("The node is not in the membership")

    def get_cn_leader(self):
        cn_leader_path = "{}/cn".format(self._leader_path)
        ret = None
        while not ret:
            ret = self._zk_client.exists(cn_leader_path)
            if ret:
                data, _ = self._zk_client.get(cn_leader_path)
                cn_leader_ip_port = data.decode("utf-8")
                cn_ip, _ = cn_leader_ip_port.split(":", 1)
                return cn_ip
            time.sleep(1)
        return None

    def delete_candidates(self):
        candidate_path = "{}/{}/candidates".format(
            self._cluster_path, self._cluster_name
        )
        candidates = self._zk_client.get_children(candidate_path)
        for candidate in candidates:
            child_path = "{}/{}".format(candidate_path, candidate)
            self._zk_client.delete(child_path)

    def handle_leader_create_event(self):
        leader_path = "{}/{}".format(self._leader_path, self._cluster_name)
        data, _ = self._zk_client.get(leader_path)
        ip_port = data.decode("utf-8")
        leader_ip, _ = ip_port.split(":", 1)
        try:
            self.delete_candidates()
        except Exception:
            pass
        try:
            self._zk_client.delete(
                "{}/{}/replicas/{}".format(
                    self._cluster_path, self._cluster_name, ip_port
                )
            )
        except Exception:
            pass
        self._cluster_leader_ip = leader_ip
        self.logger.info(
            "The leader of the cluster {} is {}".format(self._cluster_name, leader_ip)
        )
        self._is_changing = True
        if leader_ip == self._pod_ip:
            if self._is_cn:
                self.watch_need_supplement()
            self.watch_replicas()
            if postgresql.is_standby(self._pgdata_dir):
                postgresql.do_promote(
                    self._pgdata_dir, self._pod_ip, self._meta_port, self._user_name
                )

            ret = False
            while not ret:
                self.logger.info("--update background service--")
                ret = postgresql.update_start_background_service(
                    self._cluster_leader_ip, self._meta_port, self._user_name
                )
                if ret:
                    break
                time.sleep(1)
            self.logger.info("--update background service successfully--")
            cn_leader_ip = self.get_cn_leader()
            ret = False
            while not ret:
                self.logger.info("--update the node table--")
                ret = postgresql.update_node_table(
                    cn_leader_ip,
                    self._meta_port,
                    self._user_name,
                    self._cluster_id,
                    self._cluster_leader_ip,
                    self._meta_port,
                )
                postgresql.reload_foreign_server_cache(
                    cn_leader_ip, self._meta_port, self._user_name
                )
                if ret:
                    break
                cn_leader_ip = self.get_cn_leader()
                time.sleep(1)
            self.logger.info("--update the node table successfully--")
        else:
            if postgresql.is_standby(self._pgdata_dir):
                postgresql.change_following_leader(
                    self._pgdata_dir,
                    self._cluster_leader_ip,
                    self._meta_port,
                    self._user_name,
                )
            else:
                try:
                    self._zk_client.delete(
                        "{}/{}/membership/{}".format(
                            self._cluster_path, self._cluster_name, self._host_node_name
                        )
                    )
                except NoNodeError:
                    pass
                postgresql.do_demote(
                    self._pgdata_dir,
                    self._cluster_leader_ip,
                    self._meta_port,
                    self._user_name,
                    self._pod_ip,
                    self._meta_port,
                )
                self._zk_client.create(
                    "{}/{}/membership/{}".format(
                        self._cluster_path, self._cluster_name, self._host_node_name
                    )
                )
        self._is_changing = False

    def handle_candidate_change_event(self, candidates):
        leader_path = "{}/{}".format(self._leader_path, self._cluster_name)
        candidate_path = "{}/{}/candidates".format(
            self._cluster_path, self._cluster_name
        )
        self.logger.info(
            "The candidate of the cluster {} is {}".format(
                self._cluster_name, len(candidates)
            )
        )
        if len(candidates) >= self._replica_server_num:
            max_lsn = 0
            new_host_port = ""
            for candidate in candidates:
                node_path = "{}/{}".format(candidate_path, candidate)
                data, _ = self._zk_client.get(node_path)
                lsn = int(data.decode("utf-8"))
                if lsn > max_lsn:
                    max_lsn = lsn
                    new_host_port = candidate
            new_host, _ = new_host_port.split(":", 1)
            if new_host == self._pod_ip:
                try:
                    self._zk_client.create(
                        leader_path, value=str.encode(new_host_port), ephemeral=True
                    )
                    self._zk_client.set(
                        "{}/{}/lastLeader".format(
                            self._cluster_path, self._cluster_name
                        ),
                        value=str.encode(self._host_node_name),
                    )
                    try:
                        self._zk_client.delete(
                            "{}/{}/replicas/{}".format(
                                self._cluster_path, self._cluster_name, new_host_port
                            )
                        )
                    except NoNodeError:
                        pass
                except NodeExistsError:
                    pass
                finally:
                    for candidate in candidates:
                        node_path = "{}/{}".format(candidate_path, candidate)
                        try:
                            self._zk_client.delete(node_path)
                        except NoNodeError:
                            pass

    def watch_conn(self):
        def conn_state_listener(state):
            if state == KazooState.LOST:
                self.logger.critical(
                    "The connection to the zookeeper is lost, please check the zookeeper status"
                )
                postgresql.pg_stop(self._pgdata_dir)

        self._zk_client.add_listener(conn_state_listener)

    def monitor_cn_nodes(self):
        self._cn_list = self._zk_client.get_children(self._cn_path)

        @self._zk_client.ChildrenWatch(self._cn_path)
        def watch_cn_nodes(cn_nodes):
            self._cn_list = cn_nodes

    def monitor_dn_nodes(self):
        self._dn_list = self._zk_client.get_children(self._dn_path)

        @self._zk_client.ChildrenWatch(self._dn_path)
        def watch_dn_nodes(dn_nodes):
            self._dn_list = dn_nodes

    def monitor_nodes(self):
        self.monitor_cn_nodes()
        self.monitor_dn_nodes()
        while len(self._cn_list) != self._cn_num or len(self._dn_list) != self._dn_num:
            time.sleep(1)

    def build_cluster(self):
        self._cn_list = self._zk_client.get_children(self._cn_path)
        live_cn_num = len(self._cn_list)
        self._cluster_names.append("cn")
        try:
            self._zk_client.create(
                "{}/{}/hostNodes/{}".format(
                    self._cluster_path, self._cluster_name, self._host_node_name
                )
            )
        except Exception as ex:
            self.logger.error("Failed to build the cluster: {}".format(ex))
            raise ex
        idx = 0
        for i in range(live_cn_num):
            if self._cn_list[i] == self._host_node_name:
                continue
            else:
                try:
                    self._zk_client.create(
                        "{}/{}/hostNodes/{}".format(
                            self._cluster_path, self._cluster_name, self._cn_list[i]
                        )
                    )
                except Exception as ex:
                    self.logger.error("Failed to build the cluster: {}".format(ex))
                    raise ex
                idx += 1
            if idx == 2:
                break
        self._dn_list = self._zk_client.get_children(self._dn_path)
        live_dn_num = len(self._dn_list)
        cluster_num = int((live_dn_num - self._dn_supplement_num) / 3)
        node_idx = 0
        for dn_idx in range(cluster_num):
            du_cluster_name = "dn{}".format(dn_idx)
            self._cluster_names.append(du_cluster_name)
            dn_cluster_path = "{}/{}".format(self._cluster_path, du_cluster_name)
            try:
                self._zk_client.create(dn_cluster_path)
                self._zk_client.create("{}/hostNodes".format(dn_cluster_path))
                self._zk_client.create("{}/replicas".format(dn_cluster_path))
                self._zk_client.create("{}/membership".format(dn_cluster_path))
                self._zk_client.create("{}/candidates".format(dn_cluster_path))
                self._zk_client.create("{}/lastLeader".format(dn_cluster_path))
            except Exception as ex:
                self.logger.error("Failed to build the cluster: {}".format(ex))
                raise ex
            for i in range(self._replica_server_num + 1):
                try:
                    self._zk_client.create(
                        "{}/hostNodes/{}".format(
                            dn_cluster_path, self._dn_list[node_idx]
                        )
                    )
                    node_idx += 1
                except Exception as ex:
                    self.logger.error("Failed to build the cluster: {}".format(ex))
                    raise ex

    def init_filesystem(self):
        leader_infos = {}
        while len(leader_infos) < len(self._cluster_names):
            for cluster_name in self._cluster_names:
                if cluster_name in leader_infos:
                    continue
                leaders = self._zk_client.get_children(self._leader_path)
                replica_path = "{}/{}/replicas".format(self._cluster_path, cluster_name)
                replica_nodes = self._zk_client.get_children(replica_path)
                if (
                    cluster_name in leaders
                    and len(replica_nodes) == self._replica_server_num
                ):
                    cluster_leader_path = "{}/{}".format(
                        self._leader_path, cluster_name
                    )
                    data = self._zk_client.get(cluster_leader_path)
                    leader_infos[cluster_name] = bytes.decode(data[0])
            time.sleep(1)
        init_filesystem(leader_infos, self._user_name)
        self.build_all_membership()

    def build_all_membership(self):
        self.logger.info("Build all membership")
        for cluster_name in self._cluster_names:
            host_nodes = self._zk_client.get_children(
                "{}/{}/hostNodes".format(self._cluster_path, cluster_name)
            )
            for host_node in host_nodes:
                membership_path = "{}/{}/membership/{}".format(
                    self._cluster_path, cluster_name, host_node
                )
                try:
                    self._zk_client.create(membership_path)
                except NodeExistsError:
                    pass

    def is_in_membership(self):
        node_membership_path = "{}/{}/membership/{}".format(
            self._cluster_path, self._cluster_name, self._host_node_name
        )
        if self._zk_client.exists(node_membership_path):
            return True
        else:
            return False

    def start_watch_replica_thread(self):
        watch_replica_thread = threading.Thread(
            target=FalconCM.handle_replica_change, args=(self,)
        )
        watch_replica_thread.start()
        watch_replica_thread.join()

    def start_watch_replica_need_supplement_thread(self):
        watch_replica_thread = threading.Thread(
            target=FalconCM.handle_replica_change, args=(self,)
        )
        handle_replica_thread = threading.Thread(
            target=FalconCM.handle_need_supplement, args=(self,)
        )
        check_meta_status_thread = threading.Thread(
            target=FalconCM.handle_meta_error_report, args=(self,)
        )
        watch_replica_thread.start()
        handle_replica_thread.start()
        check_meta_status_thread.start()
        watch_replica_thread.join()
        handle_replica_thread.join()
        check_meta_status_thread.join()

    def handle_replica_change(self):
        while self._thread_running:
            self._watch_replica_lock.acquire()
            replica_change_num = self._replica_change_num
            if replica_change_num > 0:
                self._replica_change_num -= 1
            self._watch_replica_lock.release()
            if replica_change_num > 0:
                replica_path = "{}/{}/replicas".format(
                    self._cluster_path, self._cluster_name
                )
                self._replica_list = self._zk_client.get_children(replica_path)
                if len(self._replica_list) < self._replica_server_num:
                    self._isCheckStatus = True
                    retry_num = int(self._wait_replica_time / 10) + 1
                    while self._isCheckStatus:
                        self._replica_list = self._zk_client.get_children(replica_path)
                        if len(self._replica_list) == self._replica_server_num:
                            # the replica list is complete, clear the lost node time
                            self._lost_node_time.clear()
                            self._isCheckStatus = False
                            break
                        self._isCheckStatus = self.check_all_nodes_status(retry_num)
                        retry_num -= 1
                        time.sleep(10)
                if len(self._replica_list) == self._replica_server_num:
                    self._lost_node_time.clear()
                    self._isCheckStatus = False
            else:
                time.sleep(10)

    def check_all_nodes_status(self, retry_num):
        isCheckStatus = True
        host_nodes_path = "{}/{}/hostNodes".format(
            self._cluster_path, self._cluster_name
        )
        host_membership_path = "{}/{}/membership".format(
            self._cluster_path, self._cluster_name
        )
        host_nodes = self._zk_client.get_children(host_nodes_path)
        for name in host_nodes:
            node_path = ""
            if self._is_cn:
                node_path = self._cn_path + "/" + name
            else:
                node_path = self._dn_path + "/" + name
            if not self._zk_client.exists(node_path):
                if name in self._lost_node_time:
                    self._lost_node_time[name] = self._lost_node_time[name] + 10
                else:
                    self._lost_node_time[name] = 0
                self.logger.info('lost time is {}'.format(self._lost_node_time[name]))
                if self._lost_node_time[name] >= self._wait_replica_time - 10:
                    retry_num = 0
            else:
                if name in self._lost_node_time:
                    del self._lost_node_time[name]
        if retry_num == 0:
            self.logger.info('check replica lost')
            for name in host_nodes:
                node_path = ""
                if self._is_cn:
                    node_path = self._cn_path + "/" + name
                else:
                    node_path = self._dn_path + "/" + name
                if self._zk_client.exists(node_path):
                    continue
                if self._lost_node_time[name] >= self._wait_replica_time - 10:
                    self.logger.info(
                        "The node {} is lost, please check the status".format(name)
                    )
                    try:
                        self._zk_client.create(
                            "{}/{}-0".format(
                                self._need_supplement_path, self._cluster_name
                            )
                        )
                    except NodeExistsError:
                        try:
                            self._zk_client.create(
                                "{}/{}-1".format(
                                    self._need_supplement_path, self._cluster_name
                                )
                            )
                        except NodeExistsError:
                            pass
                    self._zk_client.delete(host_nodes_path + "/" + name)
                    self._zk_client.delete(host_membership_path + "/" + name)
                    del self._lost_node_time[name]
            isCheckStatus = False
        return isCheckStatus

    def handle_need_supplement(self):
        while self._thread_running:
            self._watch_need_supplement_lock.acquire()
            need_supplement_num = self._need_supplement_num
            if need_supplement_num > 0:
                self._need_supplement_num -= 1
            self._watch_need_supplement_lock.release()
            if need_supplement_num > 0:
                children = self._zk_client.get_children(self._need_supplement_path)
                for child in children:
                    self.logger.info("The node {} is need to supplement".format(child))
                    cluster_path = self._cluster_path + "/" + child[0:-2]
                    if not self._zk_client.exists(
                        self._need_supplement_path + "/" + child
                    ):
                        continue
                    node_name = ""
                    if child == "cn-0" or child == "cn-1":
                        while True:
                            sup_node_list = self._zk_client.get_children(
                                self._cn_supplement_path
                            )
                            if len(sup_node_list) > 0:
                                node_name = sup_node_list[0]
                                self._zk_client.delete(
                                    self._cn_supplement_path + "/" + node_name
                                )
                                break
                            time.sleep(10)
                    else:
                        while True:
                            sup_node_list = self._zk_client.get_children(
                                self._dn_supplement_path
                            )
                            if len(sup_node_list) > 0:
                                node_name = sup_node_list[0]
                                self._zk_client.delete(
                                    self._dn_supplement_path + "/" + node_name
                                )
                                break
                            time.sleep(10)
                    try:
                        self._zk_client.create(
                            "{}/hostNodes/{}".format(cluster_path, node_name),
                            value=str.encode("new"),
                        )
                        self.logger.info(
                            "The node {} is added to the cluster {}".format(
                                node_name, child[0:-2]
                            )
                        )
                    except NodeExistsError:
                        pass
                    except Exception as ex:
                        pass
                    self._zk_client.delete(self._need_supplement_path + "/" + child)
                    self.logger.info("delete {} in need supplement".format(child))
            else:
                time.sleep(10)

    def handle_meta_error_report(self):
        while self._thread_running and self._use_error_report == 1:
            self.logger.info(f"check meta status")
            cluster_is_healthy = False
            retry_num = 10
            while not cluster_is_healthy and retry_num > 0:
                time.sleep(60)
                cluster_is_healthy, error_message, error_node = check_replication_status()
                if not cluster_is_healthy:
                    self.logger.error("cluster is not healthy!")
                retry_num -= 1
            if not cluster_is_healthy:
                send_message(error_message, error_node, self._send_msg_dst)
            time.sleep(self._check_meta_period)

    def watch_need_supplement(self):
        @self._zk_client.ChildrenWatch(self._need_supplement_path)
        def watch_nodes(children):
            self.logger.info('in need supplement watch')
            self._watch_need_supplement_lock.acquire()
            self._need_supplement_num += 1
            self._watch_need_supplement_lock.release()

    def watch_replicas(self):
        replica_path = "{}/{}/replicas".format(self._cluster_path, self._cluster_name)

        @self._zk_client.ChildrenWatch(replica_path)
        def watch_nodes(children):
            self.logger.info('in watch replicas')
            self._watch_replica_lock.acquire()
            self._replica_change_num += 1
            self._watch_replica_lock.release()

    def init_ready(self):
        try:
            self._zk_client.create(self._root_path + "/ready")
        except NodeExistsError:
            pass

    def is_sys_ready(self):
        ready_path = self._root_path + "/ready"
        if self._zk_client.exists(ready_path):
            return True
        else:
            return False

    def set_store_cluster_ready(self):
        try:
            self._zk_client.set(
                self._store_node_path + "/storeNodeStatus", value=str.encode("1")
            )
        except Exception as e:
            pass

    def monitor_store_node(self):
        self._store_node_deploy = False

        @self._zk_client.ChildrenWatch(self._store_node_path + "/Nodes")
        def watch_nodes(children):
            if len(children) > 0:
                self._store_node_deploy = True

        while not self._store_node_deploy:
            time.sleep(1)
