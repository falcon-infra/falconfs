import os
import time
import logging
from utils import logger
from cm.falcon_cm import *


def main():
    data_dir = os.environ.get("data_dir", "/home/falconMeta/data")
    log_level = os.environ.get("cm_log_level", "INFO")
    has_falcon_stor = bool(os.environ.get("has_falcon_stor", "True"))
    cm_log_data_dir = os.path.join(data_dir, "cmlog")
    logger.logging_init(cm_log_data_dir, log_level)
    logger_ = logging.getLogger("logger")

    falcon_cm = FalconCM(is_cn=True)
    falcon_cm.connect_zk()
    falcon_cm.init_sys()
    is_leader = falcon_cm.leader_select()
    if is_leader:
        logger_.info("This node is cn leader")
        if not falcon_cm.is_sys_ready():
            falcon_cm.watch_leader_and_candidates()
            falcon_cm.monitor_nodes()
            falcon_cm.build_cluster()
            try:
                falcon_cm.init_filesystem()
            except Exception as e:
                logger_.error("Failed to init filesystem: {}".format(e))
                raise e
        falcon_cm.watch_need_supplement()
        falcon_cm.watch_replicas()
        falcon_cm.init_ready()
    else:
        logger_.info("This node is cn follower")
        falcon_cm.write_replica()

    falcon_cm.watch_conn()
    if has_falcon_stor:
        falcon_cm.monitor_store_node()
        time.sleep(30)
        falcon_cm.set_store_cluster_ready()
    falcon_cm.start_watch_replica_need_supplement_thread()


if __name__ == "__main__":
    main()
