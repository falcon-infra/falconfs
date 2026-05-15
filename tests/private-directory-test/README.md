# Private Directory Test

The private directory test drives FalconFS through the LibFS API or POSIX API and prints per-workload IOPS throughput.

## Local Test

Run the local test after FalconFS has been built, installed, and deployed locally.

```bash
bash local-run.sh
```

The default local rounds are `0 1 2 3`:

```text
0 workload_init
1 workload_create
2 workload_stat
3 workload_open
```

`local-run.sh` writes raw logs and a CSV summary under `results/<timestamp>/` by default.

The main local overrides are:

```bash
BIN_DIR=/usr/local/falconfs/private-directory-test/bin \
TEST_PROGRAM=test_falcon \
MOUNT_DIR=/ \
ROUND_INDEX="0 1 2 3" \
META_SERVER_IP=127.0.0.1 \
META_SERVER_PORT=55510 \
RESULT_DIR=/tmp/falconfs-baseline/manual \
bash local-run.sh
```

The summary file format is:

```text
round_idx,round_name,throughput,avg_latency,ops,time,raw_log
```

## Daily Baseline

`run_daily_baseline.sh` is intended for a dedicated local runner account. It fetches the configured Git URL/ref, builds FalconFS, installs FalconFS, restarts the local deployment, and then runs `local-run.sh` for rounds `0 1 2 3`.

Create a runner account and checkout. Replace `<runner-user>` with the account used on the target host:

```bash
sudo useradd -m -s /bin/bash <runner-user>
sudo -iu <runner-user>
git clone https://github.com/falcon-infra/falconfs.git ~/falconfs-baseline-runner
cd ~/falconfs-baseline-runner
git fetch https://github.com/falcon-infra/falconfs.git main
git switch -C baseline/main FETCH_HEAD
```

The default update source is `https://github.com/falcon-infra/falconfs.git` at ref `main`. Override `FALCONFS_BASELINE_GIT_URL` and `FALCONFS_BASELINE_GIT_REF` if the nightly baseline should track another repository or branch.

Prepare the local mountpoint for the runner account:

```bash
sudo mkdir -p /tmp/falcon_mnt
sudo chown <runner-user>:<runner-group> /tmp/falcon_mnt
```

Run one daily baseline manually as the runner account:

```bash
sudo -iu <runner-user>
~/falconfs-baseline-runner/tests/private-directory-test/run_daily_baseline.sh
```

Results are written to `~/falconfs-baseline-results/<timestamp>/` by default. Each run includes `run_info.env`, `git_status.txt`, `host_info.txt`, per-iteration raw logs, aggregated `summary.csv`, `baseline_report.txt`, and `email_preview.txt`.

The wrapper runs three complete iterations by default. Each iteration uses this flow:

```bash
./deploy/falcon_stop.sh
./build.sh clean falcon
./build.sh build falcon
sudo -n ./build.sh install falcon
./deploy/falcon_start.sh
local-run.sh
```

Per-iteration results are written under `iter_1/`, `iter_2/`, and `iter_3/`. The parent `summary.csv` contains the average throughput, average latency, average operation count, and average elapsed time across all iterations.

The wrapper refuses to reset any branch except `baseline/main` unless `FALCONFS_BASELINE_ALLOW_BRANCH_RESET=1` is set. This protects normal development branches from nightly automation.

After each run, the wrapper compares the aggregated daily result against the median of recent successful baseline summaries under `FALCONFS_BASELINE_RESULT_ROOT`. The default history window is 7 runs. The default alert rules are a 10% throughput drop or a 10% average latency increase in any round. Latency is reported in `ns/op`.

The report includes host metadata from `host_info.txt`, including hostname, primary IP, user, repo path, result path, kernel, OS, CPU model, CPU count, memory, iteration count, rounds, and benchmark parameters.

Smoke runs should use a separate result root so they do not pollute daily history:

```bash
FALCONFS_BASELINE_RESULT_ROOT=$HOME/falconfs-baseline-smoke-results
```

Email sending is disabled by default. `email_preview.txt` is always generated so the report can be inspected without SMTP credentials.

Useful wrapper overrides:

```bash
FALCONFS_BASELINE_RESULT_ROOT=$HOME/falconfs-baseline-results
FALCONFS_BASELINE_BRANCH=baseline/main
FALCONFS_BASELINE_GIT_URL=https://github.com/falcon-infra/falconfs.git
FALCONFS_BASELINE_GIT_REF=main
FALCONFS_BASELINE_UPDATE_GIT=1
FALCONFS_BASELINE_BUILD=1
FALCONFS_BASELINE_DEPLOY=1
FALCONFS_BASELINE_ROUNDS="0 1 2 3"
FALCONFS_BASELINE_META_SERVER_IP=127.0.0.1
FALCONFS_BASELINE_META_SERVER_PORT=55510
FALCONFS_BASELINE_SUDO_CMD="sudo -n"
FALCONFS_BASELINE_ITERATIONS=3
FALCONFS_BASELINE_HISTORY_WINDOW=7
FALCONFS_BASELINE_MIN_HISTORY=3
FALCONFS_BASELINE_ALERT_THRESHOLD_PCT=10
FALCONFS_BASELINE_LATENCY_ALERT_THRESHOLD_PCT=10
FALCONFS_BASELINE_LATENCY_ALERT_ENABLED=1
FALCONFS_BASELINE_MAIL_ENABLED=0
```

To enable SMTP email, provide the SMTP settings through environment variables or a systemd environment file. Do not commit credentials to the repository.

```bash
FALCONFS_BASELINE_MAIL_ENABLED=1
FALCONFS_BASELINE_MAIL_FROM=<sender@example.com>
FALCONFS_BASELINE_MAIL_TO=<receiver@example.com>
FALCONFS_BASELINE_SMTP_HOST=<smtp-host>
FALCONFS_BASELINE_SMTP_PORT=465
FALCONFS_BASELINE_SMTP_USER=<smtp-user>
FALCONFS_BASELINE_SMTP_PASSWORD=<smtp-password-or-token>
FALCONFS_BASELINE_SMTP_SSL=1
FALCONFS_BASELINE_SMTP_TLS_VERIFY=0
```

For STARTTLS on port 587, use:

```bash
FALCONFS_BASELINE_SMTP_SSL=0
FALCONFS_BASELINE_SMTP_STARTTLS=1
```

SMTP certificate verification is disabled by default to support internal mail gateways with private CA chains. To enable verification, install the company CA on the host or point the report script at the CA bundle:

```bash
FALCONFS_BASELINE_SMTP_TLS_VERIFY=1
FALCONFS_BASELINE_SMTP_CA_FILE=/path/to/company-ca.pem
```

For systemd, keep secrets outside the unit file, for example:

```ini
EnvironmentFile=/etc/falconfs/baseline-mail.env
```

`sudo -n` requires passwordless sudo for the install and deployment steps used by FalconFS. Configure sudoers for the runner account instead of running the full timer as root.

For example, start with a dedicated sudoers file and narrow it further for the target host:

```bash
sudo visudo -f /etc/sudoers.d/<runner-user>
```

```text
<runner-user> ALL=(ALL) NOPASSWD: /home/<runner-user>/falconfs-baseline-runner/build.sh
<runner-user> ALL=(ALL) NOPASSWD: /usr/bin/mkdir, /usr/bin/cp, /usr/bin/rm, /usr/bin/umount, /usr/bin/kill
```

The deploy scripts call `sudo` internally for PostgreSQL extension installation, extension cleanup, and FUSE unmount cleanup. Adjust command paths if they differ on the host.

## systemd Timer

Use a system timer with `User=<runner-user>` to run the baseline every day at midnight.

`/etc/systemd/system/falconfs-daily-baseline.service`:

```ini
[Unit]
Description=FalconFS daily private-directory baseline

[Service]
Type=oneshot
User=<runner-user>
Group=<runner-group>
Environment=HOME=/home/<runner-user>
Environment=FALCON_META_WORKSPACE=/path/to/fast/local/disk
WorkingDirectory=/home/<runner-user>/falconfs-baseline-runner
ExecStartPre=+/usr/bin/install -d -o <runner-user> -g <runner-group> -m 0755 /path/to/fast/local/disk
ExecStart=/home/<runner-user>/falconfs-baseline-runner/tests/private-directory-test/run_daily_baseline.sh
TimeoutStartSec=4h
```

`FALCON_META_WORKSPACE` is optional. Set it when metadata should live outside `$HOME`, for example on a local NVMe disk. The deployment scripts store metadata under `$FALCON_META_WORKSPACE/metadata`.

`/etc/systemd/system/falconfs-daily-baseline.timer`:

```ini
[Unit]
Description=Run FalconFS daily private-directory baseline

[Timer]
OnCalendar=*-*-* 00:00:00
Persistent=true
Unit=falconfs-daily-baseline.service

[Install]
WantedBy=timers.target
```

Enable the timer:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now falconfs-daily-baseline.timer
systemctl list-timers falconfs-daily-baseline.timer
```

View logs:

```bash
sudo journalctl -u falconfs-daily-baseline.service
```

## Remote Test

Set `ALL_SERVERS` to actual remote test nodes in `distribute.sh`, `kill_test.sh`, and `remote-run.sh`. Enable SSH among all nodes, and append the public key of the client node running `remote-run.sh` to `authorized_keys` on the nodes in `ALL_SERVERS`.

```bash
bash remote-run.sh
```

The IOPS throughput will be printed.
