<details open>
  <summary style="font-size: 2em; font-weight: bold"> FalconFS Cluster Test Setup Guide</summary>

## FalconFS Cluster Test Setup Guide

Usage:

0. Install ansible with apt.

```
apt update && apt install -y ansible sshpass
```

1. Create your user on all nodes. You can do this manually or by ansible with your own playbook.


```
useradd -m -s /bin/bash falcon
passwd falcon
# input your password here
# repeat the password
usermod -aG sudo falcon # add sudo for falcon
```

2. Set up SSH key-based authentication for passwordless login under user falcon

3. Prepare working directory

```bash
mkdir -p ~/code/ansible
cd ~/code/ansible
wget https://raw.githubusercontent.com/falcon-infra/falconfs/main/deploy/ansible/inventory
wget https://raw.githubusercontent.com/falcon-infra/falconfs/main/deploy/ansible/falcontest.yml
wget https://raw.githubusercontent.com/falcon-infra/falconfs/main/deploy/ansible/install-ubuntu24.04.sh
wget https://raw.githubusercontent.com/falcon-infra/falconfs/main/deploy/ansible/install-ubuntu22.04.sh # if ubuntu22.04
```

4. Create .ansible.cfg at your home `~/.ansible.cfg`. The content can be like:

```
[defaults]
inventory = /home/$USER/code/ansible/inventory
log_path = /home/$USER/code/ansible/ansible.log
```

5. Set value in inventory according to the annotation.

- change ips in `[falconcn] [falcondn] [falconclient]`
- fill ansible_become_password

6. Install dependencies.

```
ansible-playbook falcontest.yml --tags install-deps
```

7. Clone code and Build.

```
ansible-playbook falcontest.yml --tags build
```

8. Start.

```
ansible-playbook falcontest.yml --tags start
```

9. Stop.

```
ansible-playbook falcontest.yml --tags stop
```
</details>

<details>
  <summary style="font-size: 2em; font-weight: bold"> Cloud Native Deployment</summary>

## Cloud Native Deployment
**Requirement**:

FalconFS needs at least 3 nodes in K8S environment.

-----

Usage:

0. Install ```jq``` and ```yq```
```bash
apt update && apt -y install jq yq
```
1. Set values in ```$FALCON_PATH/cloud_native/deployment_script/node.json```
- change nodes name in ```[nodes]``` to deploy the corresponding modules. **Important:**
The number of ```[zk]``` should be 3, the number of ```[cn]``` should be 3-5, the number of ```[dn]``` should be larger than 3.

- change the ```[images]``` of each module. **We have provided images in the json**

- change the ```[hostpath]``` of each module.

2. Prepare the environment
```bash
bash $FALCON_PATH/cloud_native/deployment_script/prepare.sh
```

3. Modify the ```[PVC]``` setting in ```$FALCON_PATH/cloud_native/deployment_script/zk.yaml```

4. Set up FalconFS
- Set up configmap
```bash
kubectl apply -f configmap.yaml
```

- Set up zookeeper
```bash
kubectl apply -f zk.yaml # ensure zookeeper is ready
```

- Set up FalconFS CN
```bash
kubectl apply -f cn.yaml
```

- Set up FalconFS DN
```bash
kubectl apply -f dn.yaml
```

- Set up FalconFS Store
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

docker run -it --privileged --rm -v `pwd`/..:/root/code -w /root/code/falconfs ghcr.io/falcon-infra/falcon-dockerbuild:0.1.1 /bin/bash

bash cloud_native/docker_build/docker_build.sh
dockerd &
```

2. Build FalconFS images
The dockerfile in the path ```cd cloud_native/docker_build/```

- build the CN image
```
cd cn
docker build -t falcon-cn .
```

- build the DN image
```
cd dn
docker build -t falcon-dn .
```

- build the store iamge
```
cd store
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

4. Clean the workspace
```bash
bash clean.sh
```
</details>

<details>
  <summary style="font-size: 2em; font-weight: bold"> Debug </summary>

## Debug

### build and start FalconFS in the docker

> **⚠️ Warning**  
> This only for debug mode, do not use no_root_check.patch in production!

no root check debug, suppose at the `~/code` dir
``` bash
docker run --privileged -d -it --name falcon-dev -v `pwd`:/root/code -w /root/code/falconfs ghcr.io/falcon-infra/falconfs-dev:0.1.1
docker exec -it --detach-keys="ctrl-z,z" falcon-dev /bin/zsh
git -C third_party/postgres apply ../../patches/no_root_check.patch
./build.sh clean
./build.sh build --debug && ./build.sh install
source deploy/falcon_env.sh
./deploy/falcon_start.sh
```

### debug falcon meta server

- first login to cn: `psql -d postgres -p $cnport`
- when in the pg cli
``` bash
select pg_backend_pid(); # to get pid, then use gdb to attach the pid
SELECT falcon_plain_mkdir('/test'); # to trigger mkdir meta operation
```

### run some test and stop

``` bash
./.github/workflows/smoke_test.sh /tmp/falcon_mnt
./deploy/falcon_stop.sh
```
</details>

## Copyright
Copyright (c) 2025 Huawei Technologies Co., Ltd.