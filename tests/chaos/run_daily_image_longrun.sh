#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
REPO_ROOT_DEFAULT="$(realpath "${SCRIPT_DIR}/../..")"
REPO_ROOT="${FALCONFS_REPO_ROOT:-${REPO_ROOT_DEFAULT}}"

RUN_ROOT="${DAILY_LONGRUN_ROOT:-/tmp/falconfs-daily-longrun}"
BASE_IMAGE="${DAILY_LONGRUN_BASE_IMAGE:-falconfs-auto-longrun-base:ubuntu24.04}"
IMAGE_REPO="${DAILY_LONGRUN_IMAGE_REPO:-falconfs-auto-longrun}"
LOCK_FILE="${DAILY_LONGRUN_LOCK_FILE:-${RUN_ROOT}/daily.lock}"
LOG_DIR="${DAILY_LONGRUN_LOG_DIR:-${RUN_ROOT}/logs}"
EMAIL_LOG_LINES="${DAILY_LONGRUN_EMAIL_LOG_LINES:-80}"

usage() {
    cat <<'EOF'
Usage:
  tests/chaos/run_daily_image_longrun.sh [options]

Options:
  -h, --help

Environment:
  FALCONFS_REPO_ROOT              default: repo root detected from this script
  DAILY_LONGRUN_ROOT              default: /tmp/falconfs-daily-longrun
  DAILY_LONGRUN_BASE_IMAGE        default: falconfs-auto-longrun-base:ubuntu24.04
  DAILY_LONGRUN_IMAGE_REPO        default: falconfs-auto-longrun
  DAILY_LONGRUN_LOCK_FILE         default: /tmp/falconfs-daily-longrun/daily.lock
  DAILY_LONGRUN_LOG_DIR           default: /tmp/falconfs-daily-longrun/logs
  DAILY_LONGRUN_DATA_PATH         optional long-run data path
  DAILY_LONGRUN_CLEAN_PREVIOUS    default: 1
  DAILY_LONGRUN_EMAIL_LOG_LINES   default: 80

Mail environment uses the existing chaos alert variables:
  CHAOS_ALERT_ENABLE CHAOS_SMTP_HOST CHAOS_SMTP_PORT CHAOS_SMTP_USER
  CHAOS_SMTP_PASS CHAOS_ALERT_EMAIL_FROM CHAOS_ALERT_EMAIL_TO CHAOS_SMTP_TLS
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "unknown argument: $1" >&2
            usage >&2
            exit 1
            ;;
    esac
done

timestamp="$(date +%Y%m%d-%H%M%S)"

log() {
    echo "[$(date '+%F %T')] $*"
}

CHAOS_ALERT_ENABLE="${CHAOS_ALERT_ENABLE:-0}"
CHAOS_SMTP_HOST="${CHAOS_SMTP_HOST:-}"
CHAOS_SMTP_PORT="${CHAOS_SMTP_PORT:-587}"
CHAOS_SMTP_USER="${CHAOS_SMTP_USER:-}"
CHAOS_SMTP_PASS="${CHAOS_SMTP_PASS:-}"
CHAOS_ALERT_EMAIL_FROM="${CHAOS_ALERT_EMAIL_FROM:-}"
CHAOS_ALERT_EMAIL_TO="${CHAOS_ALERT_EMAIL_TO:-}"
CHAOS_SMTP_TLS="${CHAOS_SMTP_TLS:-1}"
LAST_ALERT_KEY=""

# Reuse the chaos harness SMTP implementation; this file defines send_email_alert.
# shellcheck source=/dev/null
source "${SCRIPT_DIR}/lib/diagnostics.sh"

resolve_path() {
    local p="$1"
    if [[ "$p" = /* ]]; then
        echo "$p"
    else
        echo "${REPO_ROOT}/${p}"
    fi
}

tmp_container=""
run_status="RUNNING"
phase="init"
log_file=""
run_data_abs=""
metadata_file=""
daily_image=""
branch=""
commit=""
short_commit=""
commit_subject=""
start_epoch="$(date +%s)"
start_time="$(date '+%F %T')"
finalized=0

cleanup_container() {
    if [[ -n "$tmp_container" ]]; then
        docker rm -f "$tmp_container" >/dev/null 2>&1 || true
    fi
}

cleanup_previous_artifacts() {
    if [[ "${DAILY_LONGRUN_CLEAN_PREVIOUS:-1}" != "1" ]]; then
        return 0
    fi
    rm -rf "${RUN_ROOT}/runs" "${RUN_ROOT}/logs"
}

cleanup_old_daily_images() {
    local image
    docker images "$IMAGE_REPO" --format '{{.Repository}}:{{.Tag}}' | while read -r image; do
        [[ -z "$image" || "$image" == "${IMAGE_REPO}:<none>" || "$image" == "$daily_image" ]] && continue
        docker rmi "$image" >/dev/null 2>&1 || true
    done
}

suite_summary_text() {
    local summary_file="$1"
    if [[ -z "$summary_file" || ! -f "$summary_file" ]]; then
        echo "suite_summary=n/a"
        return 0
    fi

    python3 - "$summary_file" <<'PY' || true
import json
import sys

path = sys.argv[1]
try:
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
except Exception as exc:
    print(f"suite_summary_error={exc}")
    raise SystemExit(0)

print(f"suite_passed={data.get('passed', 'n/a')}")
for stage, item in data.get("stages", {}).items():
    summary = item.get("summary") or {}
    print(
        f"stage_{stage}=passed:{summary.get('passed', 'n/a')} "
        f"failed:{summary.get('failed', 'n/a')} total:{summary.get('total', 'n/a')} "
        f"report:{item.get('report', 'n/a')}"
    )
PY
}

log_tail_text() {
    if [[ -z "$log_file" || ! -f "$log_file" ]]; then
        echo "log_tail=n/a"
        return 0
    fi
    echo "last_${EMAIL_LOG_LINES}_log_lines:"
    tail -n "$EMAIL_LOG_LINES" "$log_file" || true
}

send_daily_result_email() {
    local status="$1"
    local rc="$2"
    local end_epoch end_time duration suite_file short_host host subject body

    end_epoch="$(date +%s)"
    end_time="$(date '+%F %T')"
    duration=$((end_epoch - start_epoch))
    short_host="$(hostname 2>/dev/null || echo unknown-host)"
    host="$(hostname -f 2>/dev/null || hostname 2>/dev/null || echo unknown-host)"
    suite_file=""
    if [[ -n "$run_data_abs" ]]; then
        suite_file="${run_data_abs}/suite_summary.json"
    fi

    subject="[FalconFS Daily Longrun][${status}][${short_host}] ${short_commit:-n/a} ${branch:-n/a}"
    if [[ "$status" == "SKIPPED" ]]; then
        subject="[FalconFS Daily Longrun][SKIPPED][${short_host}] lock held"
    fi

    body="status=${status}
exit_code=${rc}
step=${phase}
start_time=${start_time}
end_time=${end_time}
duration_sec=${duration}
host=${host}
repo=${REPO_ROOT}
branch=${branch:-n/a}
commit=${commit:-n/a}
short_commit=${short_commit:-n/a}
commit_subject=${commit_subject:-n/a}
daily_image=${daily_image:-n/a}
data_path=${run_data_abs:-n/a}
log_file=${log_file:-n/a}
suite_summary=${suite_file:-n/a}
longrun_stages=${LONGRUN_STAGES:-single,dual,triple}
longrun_seed=${LONGRUN_SEED:-20260305}

$(suite_summary_text "$suite_file")

$(log_tail_text)
"

    send_email_alert "$subject" "$body" "daily-longrun:${status}:${timestamp}:${short_commit:-none}" || true
}

finalize() {
    local rc="$1"
    if [[ "$finalized" == "1" ]]; then
        return "$rc"
    fi
    finalized=1

    cleanup_container
    if [[ "$run_status" == "RUNNING" ]]; then
        if [[ "$rc" == "0" ]]; then
            run_status="OK"
        else
            run_status="FAILED"
        fi
    fi
    send_daily_result_email "$run_status" "$rc"
    return "$rc"
}

mkdir -p "$RUN_ROOT" "$(dirname "$LOCK_FILE")"

exec 9>"${LOCK_FILE}"
if ! flock -n 9; then
    run_status="SKIPPED"
    phase="lock"
    log "another daily image long-run is still running, skip this run"
    send_daily_result_email "$run_status" 0
    exit 0
fi

cleanup_previous_artifacts
mkdir -p "${LOG_DIR}" "${RUN_ROOT}/runs"
log_file="${LOG_DIR}/daily_image_longrun_${timestamp}.log"
exec > >(tee -a "${log_file}") 2>&1
trap 'rc=$?; finalize "$rc"; exit "$rc"' EXIT

cd "${REPO_ROOT}"

log "repo root: ${REPO_ROOT}"
log "run root: ${RUN_ROOT}"
log "base image: ${BASE_IMAGE}"
log "image repo: ${IMAGE_REPO}"
log "log file: ${log_file}"

phase="fetch"
log "fetch latest code"
git fetch
log "pull current tracking branch with --ff-only"
git pull --ff-only

branch="$(git rev-parse --abbrev-ref HEAD)"
commit="$(git rev-parse HEAD)"
short_commit="$(git rev-parse --short HEAD)"
commit_subject="$(git log -1 --format=%s)"
daily_image="${IMAGE_REPO}:${timestamp}-${short_commit}"
run_data_path="${DAILY_LONGRUN_DATA_PATH:-${RUN_ROOT}/runs/${timestamp}_${short_commit}}"
run_data_abs="$(resolve_path "${run_data_path}")"
mkdir -p "$run_data_abs"

log "branch: ${branch}"
log "commit: ${commit}"
log "daily image: ${daily_image}"
log "data path: ${run_data_abs}"

metadata_file="${run_data_abs}/daily_image_metadata.env"
{
    printf 'timestamp=%s\n' "$timestamp"
    printf 'branch=%s\n' "$branch"
    printf 'commit=%s\n' "$commit"
    printf 'short_commit=%s\n' "$short_commit"
    printf 'commit_subject=%s\n' "$commit_subject"
    printf 'base_image=%s\n' "$BASE_IMAGE"
    printf 'daily_image=%s\n' "$daily_image"
    printf 'build_mode=source\n'
    printf 'log_file=%s\n' "$log_file"
} >"$metadata_file"
log "metadata: ${metadata_file}"

if ! docker image inspect "${BASE_IMAGE}" >/dev/null 2>&1; then
    phase="build_base_image"
    log "base image not found, build it now: ${BASE_IMAGE}"
    docker build -f tests/chaos/ubuntu24.04-auto-longrun-base-dockerfile -t "${BASE_IMAGE}" .
fi

phase="build_daily_image"
tmp_container="falconfs-daily-source-${short_commit}-$$"
log "create source build container: ${tmp_container}"
docker create --name "$tmp_container" "$BASE_IMAGE" sleep infinity >/dev/null
docker start "$tmp_container" >/dev/null

log "copy HEAD source snapshot into container"
docker exec "$tmp_container" mkdir -p /tmp/falconfs-src
git archive HEAD | docker exec -i "$tmp_container" tar -x -C /tmp/falconfs-src

log "build and install FalconFS from source in container"
docker exec "$tmp_container" bash -lc '
set -euo pipefail
export FALCONFS_INSTALL_DIR=/usr/local/falconfs
export PATH=/usr/local/pgsql/bin:/usr/local/bin:/usr/local/sbin:${PATH}
export LD_LIBRARY_PATH=/usr/local/pgsql/lib:/usr/local/lib:/usr/local/lib64:${LD_LIBRARY_PATH:-}

if ! getent group falconMeta >/dev/null 2>&1; then
    groupadd -r falconMeta
fi
if ! getent passwd falconMeta >/dev/null 2>&1; then
    useradd -r -g falconMeta -d /home/falconMeta -s /usr/sbin/nologin falconMeta
fi
mkdir -p /home/falconMeta /usr/local/falconfs/data
chown -R falconMeta:falconMeta /home/falconMeta /usr/local/falconfs

cd /tmp/falconfs-src
./build.sh clean falcon || true
./build.sh build falcon --with-zk-init --with-prometheus
./build.sh install falcon
chown -R falconMeta:falconMeta /home/falconMeta /usr/local/falconfs/data
rm -rf /tmp/falconfs-src
'

log "commit daily image"
docker commit "$tmp_container" "$daily_image" >/dev/null
docker rm -f "$tmp_container" >/dev/null
tmp_container=""
log "remove old daily images from ${IMAGE_REPO}"
cleanup_old_daily_images

phase="longrun"
log "start long-run with image ${daily_image}"
LONGRUN_IMAGE="$daily_image" \
LONGRUN_DATA_PATH="$run_data_abs" \
LONGRUN_SKIP_IMAGE_BUILD=1 \
bash "${SCRIPT_DIR}/run_longrun.sh"

phase="done"
run_status="OK"
