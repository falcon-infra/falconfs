
# FalconFS Regress Guide

## Regress machine requirement

- Architecture: x84_64
- OS: ubuntu 24.04
- CPU: 32 cores more
- Memory: 32G or more

## Install docker and docker-compose

- not provide here, please refer to the guide of ubuntu.

## Create a local registry for acceleration

- Create container for local registry
   $localDirForRegistry: the storage volume where you want to save images

   ``` bash
   docker run -d \
   -p 5000:5000 \
   --restart=always \
   --name registry \
   -v $localDirForRegistry:/var/lib/registry \
   registry:2
   ```

- Configure HTTP trust by editing Docker's configuration file /etc/docker/daemon.json (if the file does not exist, you can create it directly).

   ``` json
   {
     "insecure-registries": ["localhost:5000", "127.0.0.1:5000"]
   }
   ```

- restart docker service and start docker container `registry`

   ``` bash
   sudo systemctl daemon-reload
   sudo systemctl restart docker
   docker start registry
   ```

## Create dev container for FalconFS

- suppose at the `~/code` dir

   ``` bash
   git clone https://github.com/falcon-infra/falconfs.git
   cd falconfs
   git submodule update --init --recursive # submodule update postresql
   ./patches/apply.sh
   docker run -it --privileged --name falcon-dev -d -v `pwd`/..:/root/code -w /root/code/falconfs ghcr.io/falcon-infra/falconfs-dev:ubuntu24.04 /bin/bash
   ```

## Start FalconFS regerss test

- Configure NOPASSWD privilege for the test account. Add the following content to the end of the /etc/sudoers file

   ``` text
   $USER	ALL=(ALL:ALL)	NOPASSWD: ALL
   ```

- $data_path: the path provided for the regression test, which requires larger space more than 512GB
  
   ``` bash
   cd ~/code/falconfs/tests/regress
   bash start_regress_test.sh $data_path
   ```  
