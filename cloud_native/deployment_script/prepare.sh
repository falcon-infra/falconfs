# /bin/bash
set -euo pipefail

DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
JSON_PATH=$DIR/node.json
ZK_NODES_LEN=$(jq '.nodes.zk | length' $JSON_PATH)
CN_NODES_LEN=$(jq '.nodes.cn | length' $JSON_PATH)
DN_NODES_LEN=$(jq '.nodes.dn | length' $JSON_PATH)

if (( ZK_NODES_LEN != 3 )) || (( CN_NODES_LEN < 3 )) || (( DN_NODES_LEN < 3 )); then
    echo "The number of nodes is wrong: zk !=3 or cn < 3 or dn < 3"
    exit 1
fi 

# ZK_NODES=$(jq -r '.nodes.zk| join(" ")' node.json)
# for ZK_NODE in $ZK_NODES
# do
#     kubectl label node $ZK_NODE falcon-zk=falcon-zk
# done 

# CN_NODES=$(jq -r '.nodes.cn| join(" ")' node.json)
# for CN_NODE in $CN_NODES
# do
#     kubectl label node $CN_NODE falcon-cn=falcon-cn
# done 

# DN_NODES=$(jq -r '.nodes.dn| join(" ")' node.json)
# for DN_NODE in $DN_NODES
# do
#     kubectl label node $DN_NODE falcon-dn=falcon-dn
# done 

#modify the configmap

CONFIGMAP=$DIR/configmap.yaml

CN_SUP_NUM=$(( $CN_NODES_LEN - 3 ))
DN_PRIMARY_NUM=$(( ( $DN_NODES_LEN + 1 ) * 3 / 10 ))
DN_SUP_NUM=$(( $DN_NODES_LEN - ( $DN_PRIMARY_NUM * 3 ) ))

yq eval '.data.cn_num="'"$CN_NODES_LEN"'"' $CONFIGMAP -i
yq eval '.data.cn_sup_num="'"$CN_SUP_NUM"'"' $CONFIGMAP -i
yq eval '.data.dn_num="'"$DN_NODES_LEN"'"' $CONFIGMAP -i
yq eval '.data.dn_sup_num="'"$DN_SUP_NUM"'"' $CONFIGMAP -i

#modify the cn yaml
CN_YAML=$DIR/cn.yaml
yq eval '.spec.replicas='"$CN_NODES_LEN"'' $CN_YAML -i
CN_IMAGES=$(jq -r '.images.cn' $JSON_PATH)
yq eval '.spec.template.spec.containers[0].image="'"$CN_IMAGES"'"' $CN_YAML -i
CN_HOST_PATH=$(jq -r '.hostpath.cn' $JSON_PATH)
yq eval '.spec.template.spec.volumes[0].hostPath.path="'"$CN_HOST_PATH"'"' $CN_YAML -i

#modify the dn yaml
DN_YAML=$DIR/dn.yaml
DN_IMAGES=$(jq -r '.images.dn' $JSON_PATH)
yq eval '.spec.template.spec.containers[0].image="'"$DN_IMAGES"'"' $DN_YAML -i
DN_HOST_PATH=$(jq -r '.hostpath.dn' $JSON_PATH)
yq eval '.spec.template.spec.volumes[0].hostPath.path="'"$DN_HOST_PATH"'"' $DN_YAML -i

#modify the store yaml
STORE_YAML=$DIR/store.yaml
STORE_IMAGES=$(jq -r '.images.store' $JSON_PATH)
yq eval '.spec.template.spec.containers[0].image="'"$STORE_IMAGES"'"' $STORE_YAML -i
STORE_CACHE_HOSTPATH=$(jq -r '.hostpath.store.cache' $JSON_PATH)
yq eval '.spec.template.spec.volumes[0].hostPath.path="'"$STORE_CACHE_HOSTPATH"'"' $STORE_YAML -i

STORE_MNT_HOSTPATH=$(jq -r '.hostpath.store.mnt' $JSON_PATH)
yq eval '.spec.template.spec.volumes[1].hostPath.path="'"$STORE_MNT_HOSTPATH"'"' $STORE_YAML -i

STORE_LOG_HOSTPATH=$(jq -r '.hostpath.store.log' $JSON_PATH)
yq eval '.spec.template.spec.volumes[2].hostPath.path="'"$STORE_LOG_HOSTPATH"'"' $STORE_YAML -i