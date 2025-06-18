import os
import logging
import requests

def send_message(error_message, error_node, dst="None"):
    if dst == "XIAOLUBAN":
        logging.getLogger("logger").info("luban send message")
        url = os.getenv("LUBAN_URL")
        cluster = os.getenv("MONITOR_CLUSTER_NAME")
        content = f"【任务类型】FalconFS元数据检查 \n" \
                  f"【集群名称】{cluster} \n" \
                  f"【失败原因】{error_message} \n" \
                  f"【问题节点】{error_node}"
        auth = os.getenv("LUBAN_TOKEN")
        receiver = os.getenv("LUBAN_RECEIVER")
        data = {'content': content, 'receiver': receiver, 'auth': auth}
        res = requests.post(url=url, json=data)
        if not res.ok:
            print(res.text)