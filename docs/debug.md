
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

## Copyright
Copyright (c) 2025 Huawei Technologies Co., Ltd.