## Cloud Native Deployment
**Requirement**:

FalconFS needs at least 3 nodes in K8S environment.

-----

Usage:

0. Install ```jq``` and ```yq```
```bash
apt update && apt -y install jq yq
```
1. Set values in ```~/code/cloud_native/deployment_script/requirement/node.json```
- change nodes name in ```[nodes]```to deploy the corresponding modules. The number of ```[zk]``` should be 3, the number of ```[cn]``` should be 3-5, the number of ```[dn]``` should be larger than 5.

- change the ```[images]``` of each module.

- change the ```[hostpath]``` of each module.

2. Prepare the environment
```bash
bash ~/code/cloud_native/deployment_script/requirement/prepare.sh
```

3. Modify the ```[PVC]``` setting in ```~/code/cloud_native/deployment_script/deployment_yaml/zk.yaml```

4. Setup FalconFS
- Setup configmap
```bash
kubectl apply -f configmap.yaml
```

- Setup zookeeper
```bash
kubectl apply -f zk.yaml
```

- Setup FalconFS CN
```bash
kubectl apply -f cn.yaml
```

- Setup FalconFS DN
```bash
kubectl apply -f dn.yaml
```

- Setup FalconFS Store
```bash
kubectl apply -f store.yaml
```