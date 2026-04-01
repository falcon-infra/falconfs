
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
   $localDirForRegistry: the path where you want to save images

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

- Creating a cross-compilation builder for Docker using the host network.

   ``` bash
   docker buildx create --name multi-platform-builder \
   --driver docker-container \
   --driver-opt network=host \
   --use
   docker buildx inspect --bootstrap
   ```

- Restart docker service and start docker container `registry`

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
   docker run -it --privileged --name falcon-dev -d -v `pwd`/..:/root/code -w /root/code/falconfs ghcr.io/falcon-infra/falconfs-dev:ubuntu24.04 /bin/bash
   ```

## Start FalconFS regress test

- Configure NOPASSWD privilege for the test account. Add the following content to the end of the /etc/sudoers file

   ``` text
   $USER	ALL=(ALL:ALL)	NOPASSWD: ALL
   ```

- Download zookeeper:3.8.3 image and push to local registry for regress test

  ```bash
  docker pull zookeeper:3.8.3
  docker tag zookeeper:3.8.3 localhost:5000/zookeeper:3.8.3
  docker push localhost:5000/zookeeper:3.8.3
  ```

- Prepare full Debian package and place it at repository root:

  ```bash
  cd ~/code/falconfs
  dpkg-buildpackage -b -us -uc
  cp ../falconfs_0.1.0-1_amd64.deb ./falconfs-deb-full.deb
  ```

- The regress script builds one unified image for cn/dn/store/regress:

  ```bash
  export FALCON_FULL_IMAGE=localhost:5000/falconfs-full-ubuntu24.04:v0.1.0
  ```

- Build behavior options of `start_regress_test.sh`:

  1. `SKIP_IMAGE_BUILD=0` (default)
     - The script builds and pushes `${FALCON_FULL_IMAGE}` automatically.
     - Prerequisite: `falconfs-deb-full.deb` must exist at repository root.

  2. `SKIP_IMAGE_BUILD=1`
     - The script skips image build and directly runs `docker-compose up`.
     - Prerequisite: `${FALCON_FULL_IMAGE}` must already exist locally or be pullable from registry.

- Recommended usage when image is already built:

  ```bash
  export FALCON_FULL_IMAGE=localhost:5000/falconfs-full-ubuntu24.04:v0.1.0
  export SKIP_IMAGE_BUILD=1
  bash tests/regress/start_regress_test.sh "$PWD/tests/regress/verify_data_full_unified"
  ```

- Run regress test using the command bellow
  
   ``` bash
   # ${data_path}: the storage path provided for the regression test, which requires larger space more than 512GB
   cd ~/code/falconfs/tests/regress
   bash start_regress_test.sh $data_path
   ```

## Chaos fault injection (P0)

- `tests/regress/chaos_run.sh` provides automated fault injection with recovery checks.
- Current chaos scenarios include `S01~S07`:
  - `S01`: restart cn leader
  - `S01C2`: restart `falcon-cn-2` (fixed target, useful for deterministic repro)
  - `S02`: restart one dn leader
  - `S03`: restart random dn (prefer follower)
  - `S04`: restart store container
  - `S05`: kill `falcon_client` process in store
  - `S06`: stop cn leader, hold for supplement window, then start
  - `S07`: stop random dn, hold for supplement window, then start

- Core checks after each action:
  - `/falcon/ready` exists in ZK
  - liveness scripts of CN/DN/Store pass
  - metadata service probe reaches expected state (`RW` or `DEGRADED->RW` depending on topology/action)
  - `fuse.falcon_client` mount is ready

- Topology and probe options:
  - `--topology auto|single|dual|triple` (default `auto`)
    - `auto` only auto-detects acceptance policy from `replica_server_num`; it does not switch compose topology.
  - `--probe-mode meta|meta+fuse` (default `meta`)
  - `--probe-timeout-sec`: timeout for one metadata probe command (default `8`)
  - `--reliability-timeout-sec`: timeout to wait replica reliability restoration for dual/triple metadata faults
  - `--supplement-hold-sec`: hold duration for `S06/S07`, default is `wait_replica_time+30`
  - `--strict-dual-ro-check`: require degraded-state observation (RO/RECOVERY/WRITE_BLOCKED) for dual metadata actions

- Runtime prerequisites for `chaos_run.sh`:
  - passwordless sudo is required (`sudo -n true` must pass), because the script performs mount cleanup and data cleanup
  - by default, the script requires CN/DN container core ulimit to be `0` to avoid disk blowup

- Core dump behavior and troubleshooting:
  - if startup fails with `core ulimit inside containers: ... unlimited` and `core ulimit is not 0`, use one of these options:
    1. set `ulimits.core.soft=0` and `ulimits.core.hard=0` for CN/DN services in compose
    2. run with `--core-once-enable` (optionally add `--core-limit-kb <kb>`) to allow only first core capture and then auto-disable core dumps

- Optional SMTP alert for core events (default disabled):
  - `CHAOS_ALERT_ENABLE=1`: enable alerting
  - `CHAOS_SMTP_HOST`: SMTP server host
  - `CHAOS_SMTP_PORT`: SMTP port (common: `587` or `465`)
  - `CHAOS_SMTP_USER` / `CHAOS_SMTP_PASS`: SMTP auth user and password (or app token)
  - `CHAOS_ALERT_EMAIL_FROM`: sender email address
  - `CHAOS_ALERT_EMAIL_TO`: recipient list, comma-separated
  - `CHAOS_SMTP_TLS=1`: enable TLS (default `1`)

- SMTP example:

  ```bash
  export CHAOS_ALERT_ENABLE=1
  export CHAOS_SMTP_HOST="smtp.example.com"
  export CHAOS_SMTP_PORT=587
  export CHAOS_SMTP_USER="falcon-alert@example.com"
  export CHAOS_SMTP_PASS="smtp-app-password"
  export CHAOS_ALERT_EMAIL_FROM="falcon-alert@example.com"
  export CHAOS_ALERT_EMAIL_TO="alice@example.com,bob@example.com"
  export CHAOS_SMTP_TLS=1
  ```

- Example command (dual scenario, 2 hours):

  ```bash
  cd ~/code/falconfs
  export FALCON_FULL_IMAGE=localhost:5000/falconfs-full-ubuntu24.04:v0.1.0
  bash tests/regress/chaos_run.sh \
    --data-path tests/regress/verify_data_chaos \
    --compose-file tests/regress/docker-compose-dual.yaml \
    --duration-min 120 \
    --interval-sec 300 \
    --actions S01,S02,S03,S04,S05,S06,S07 \
    --topology auto \
    --fresh-start
  ```

- Run all topologies (single/dual/triple) in one suite:

  ```bash
  cd ~/code/falconfs
  export FALCON_FULL_IMAGE=localhost:5000/falconfs-full-ubuntu24.04:v0.1.0
  bash tests/regress/chaos_suite.sh \
    --data-path tests/regress/verify_data_chaos_suite \
    --single-duration-min 60 \
    --dual-duration-min 120 \
    --triple-duration-min 120
  ```

- Example command when container core ulimit is not `0`:

  ```bash
  cd ~/code/falconfs
  export FALCON_FULL_IMAGE=localhost:5000/falconfs-full-ubuntu24.04:v0.1.0
  bash tests/regress/chaos_run.sh \
    --data-path tests/regress/verify_data_chaos \
    --compose-file tests/regress/docker-compose-dual.yaml \
    --duration-min 120 \
    --interval-sec 300 \
    --actions S01,S02,S03,S04,S05 \
    --fresh-start \
    --core-once-enable
  ```

- Report file:
  - default path: `${data_path}/chaos_report.jsonl`
  - one JSON record per injected action, and one final summary record

- Run log file:
  - default path: `${data_path}/chaos_run.log`
  - includes phase progress logs (startup, wait loop, inject, recovery, sleep)
  - disable auto tee logging with `--no-auto-log`

- Failure diagnostics snapshot:
  - default path: `${data_path}/chaos_diag`
  - on failed action or startup failure, script snapshots docker status, container logs, cmlog, start.log, and PostgreSQL config/log tails
  - custom path can be set with `--diag-dir`

- Recovery timing controls:
  - `--post-action-grace-sec`: wait before first health check after injection
  - `--stable-window-sec`: require continuous healthy window before marking action as recovered

- Optional compatibility switch:
  - `--skip-pg-ip-check`: skip `postgresql.conf` vs `POD_IP` consistency check (use only when comparing old behavior)

- If container core ulimit is not `0`, add suite passthrough options:

  ```bash
  bash tests/regress/chaos_suite.sh \
    --data-path tests/regress/verify_data_chaos_suite \
    --core-once-enable \
    --core-limit-kb 1048576
  ```

- One-core capture mode:
  - `--core-once-enable`: allow core dump and keep only first core
  - `--core-limit-kb`: core size limit in KB when core-once mode is enabled (default 1048576)
  - `--core-once-dir`: output directory for captured first core (default `${data_path}/core_once`)
  - the script automatically disables further core dumps after first core is captured
