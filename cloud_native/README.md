## Cloud Native Deployment
**Requirement**:

FalconFS needs at least 3 nodes in K8S environment.

-----

Usage:

0. Install ```jq``` and ```yq```
```bash
apt update && apt -y install jq yq
```
1. Set values in ```~/code/cloud_native/deployment_script/node.json```
- change nodes name in ```[nodes]```to deploy the corresponding modules. The number of ```[zk]``` should be 3, the number of ```[cn]``` should be 3-5, the number of ```[dn]``` should be larger than 5.

- change the ```[images]``` of each module. **We have provided images in the json**

- change the ```[hostpath]``` of each module.

2. Prepare the environment
```bash
bash ~/code/cloud_native/deployment_script/prepare.sh
```

3. Modify the ```[PVC]``` setting in ```~/code/cloud_native/deployment_script/zk.yaml```

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

## Docker Image Build
If you need to build the docker images, you can follow:

1. Compile FalconFS

suppose at the `~/code` dir
``` bash
git clone https://github.com/falcon-infra/falconfs.git
cd falconfs
git submodule update --init --recursive # submodule update postresql
./patches/apply.sh

docker run -it --privileged --rm -v `pwd`/..:/root/code -w /root/code/falconfs ghcr.io/falcon-infra/falcon-dockerbuild:0.1.0 /bin/bash

bash cloud_native/docker_build/docker_build.sh
dockerd &
```

2. Build FalconFS images
The dockerfile in the path ```cd cloud_native/docker_build/```

- build the CN image
```
cd CN
docker build -t falcon-cn .
```

- build the DN image
```
cd DN
docker build -t falcon-dn .
```

- build the store iamge
```
cd Store
docker build -t falcon-store .
```

3. Push images to docker registry
```
docker tag falcon-cn [falcon-cn url]
docker tag falcon-dn [falcon-dn url]
docker tag falcon-store [falcon-store url]

docker push [falcon-cn url]
docker push [falcon-dn url]
docker push [falcon-store url]
```
