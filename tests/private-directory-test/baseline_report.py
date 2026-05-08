#!/usr/bin/env python3
import argparse
import csv
import os
import smtplib
import ssl
from email.message import EmailMessage
from pathlib import Path
from statistics import median


ALERT_EXIT_CODE = 10


def parse_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def read_summary(result_dir):
    summary_path = Path(result_dir) / "summary.csv"
    if not summary_path.is_file():
        return None

    rows = {}
    with summary_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row.get("round_name", "").strip()
            throughput = parse_float(row.get("throughput", ""))
            if not name or throughput is None:
                continue
            latency = parse_float(row.get("avg_latency", ""))
            rows[name] = {
                "round_idx": row.get("round_idx", ""),
                "throughput": throughput,
                "avg_latency": latency if latency is not None and latency > 0 else None,
                "ops": parse_float(row.get("ops", "")),
                "time": parse_float(row.get("time", "")),
                "iterations": row.get("iterations", ""),
                "raw_log": row.get("raw_log", ""),
            }

    return rows if rows else None


def read_key_value_file(path):
    info = {}
    path = Path(path)
    if not path.is_file():
        return info
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line or "=" not in line:
                continue
            key, value = line.split("=", 1)
            info[key] = value
    return info


def read_run_info(result_dir):
    return read_key_value_file(Path(result_dir) / "run_info.env")


def read_host_info(result_dir):
    return read_key_value_file(Path(result_dir) / "host_info.txt")


def candidate_history_dirs(result_root, exclude_dirs):
    root = Path(result_root)
    if not root.is_dir():
        return []

    excludes = {Path(p).resolve() for p in exclude_dirs if p}
    candidates = []
    for child in root.iterdir():
        if not child.is_dir():
            continue
        resolved = child.resolve()
        if resolved in excludes:
            continue
        if child.name.endswith("_confirm") or child.name.startswith("iter_"):
            continue
        summary_path = child / "summary.csv"
        if not summary_path.is_file():
            continue
        candidates.append(child)

    candidates.sort(key=lambda p: (p / "summary.csv").stat().st_mtime, reverse=True)
    return candidates


def load_history(result_root, exclude_dirs, window):
    history = []
    for result_dir in candidate_history_dirs(result_root, exclude_dirs):
        summary = read_summary(result_dir)
        if summary:
            history.append({"dir": str(result_dir), "summary": summary})
        if len(history) >= window:
            break
    return history


def build_baseline(history, round_names, min_history):
    baseline = {}
    for name in round_names:
        throughput_values = [
            entry["summary"][name]["throughput"]
            for entry in history
            if name in entry["summary"] and entry["summary"][name]["throughput"] is not None
        ]
        latency_values = [
            entry["summary"][name]["avg_latency"]
            for entry in history
            if name in entry["summary"] and entry["summary"][name]["avg_latency"] is not None
        ]
        baseline[name] = {}
        if len(throughput_values) >= min_history:
            baseline[name]["throughput"] = {
                "median": median(throughput_values),
                "samples": len(throughput_values),
            }
        if len(latency_values) >= min_history:
            baseline[name]["latency"] = {
                "median": median(latency_values),
                "samples": len(latency_values),
            }
    return baseline


def compare_throughput(summary, baseline, threshold):
    results = []
    for name, row in summary.items():
        current = row["throughput"]
        base = baseline.get(name, {}).get("throughput")
        result = compare_value(name, current, base, lower_is_bad=True, threshold=threshold)
        results.append(result)
    return results


def compare_latency(summary, baseline, threshold, enabled):
    results = []
    for name, row in summary.items():
        current = row["avg_latency"]
        base = baseline.get(name, {}).get("latency")
        result = compare_value(name, current, base, lower_is_bad=False, threshold=threshold)
        if not enabled:
            result["degraded"] = False
            result["status"] = "DISABLED"
        results.append(result)
    return results


def compare_value(round_name, current, baseline, lower_is_bad, threshold):
    if current is None:
        return {
            "round_name": round_name,
            "current": None,
            "baseline": None,
            "samples": 0,
            "delta_pct": None,
            "degraded": False,
            "status": "NO_DATA",
        }
    if not baseline:
        return {
            "round_name": round_name,
            "current": current,
            "baseline": None,
            "samples": 0,
            "delta_pct": None,
            "degraded": False,
            "status": "NO_BASELINE",
        }

    base_value = baseline["median"]
    delta_pct = ((current - base_value) / base_value) * 100 if base_value else 0.0
    if lower_is_bad:
        degraded = current < base_value * (1 - threshold)
    else:
        degraded = current > base_value * (1 + threshold)
    return {
        "round_name": round_name,
        "current": current,
        "baseline": base_value,
        "samples": baseline["samples"],
        "delta_pct": delta_pct,
        "degraded": degraded,
        "status": "DEGRADED" if degraded else "OK",
    }


def format_float(value):
    if value is None:
        return "n/a"
    return f"{value:.2f}"


def format_pct(value):
    if value is None:
        return "n/a"
    return f"{value:.2f}%"


def status_from_results(throughput_results, latency_results, history_count):
    if history_count == 0:
        return "NO_HISTORY"
    if any(row["degraded"] for row in throughput_results + latency_results):
        return "ALERT"
    return "OK"


def host_summary_lines(host_info, run_info):
    fields = [
        ("Host", host_info.get("hostname", "n/a")),
        ("Primary IP", host_info.get("primary_ip", "n/a")),
        ("User", host_info.get("user", "n/a")),
        ("Repo", host_info.get("repo_dir", run_info.get("repo_dir", "n/a"))),
        ("Result", host_info.get("result_dir", run_info.get("result_dir", "n/a"))),
        ("Commit", run_info.get("commit_sha", "n/a")),
        ("Subject", run_info.get("commit_subject", "n/a")),
        ("Git dirty", run_info.get("git_dirty", "n/a")),
        ("Kernel", host_info.get("kernel", "n/a")),
        ("OS", host_info.get("os_release", "n/a")),
        ("CPU", host_info.get("cpu_model", "n/a")),
        ("CPU count", host_info.get("cpu_count", "n/a")),
        ("Memory", host_info.get("mem_total", "n/a")),
        ("Iterations", host_info.get("iterations", run_info.get("iterations", "n/a"))),
        ("Rounds", host_info.get("rounds", run_info.get("rounds", "n/a"))),
        ("Files/thread", host_info.get("file_per_thread", run_info.get("file_per_thread", "n/a"))),
        ("Threads/client", host_info.get("thread_num_per_client", run_info.get("thread_num_per_client", "n/a"))),
        ("Client num", host_info.get("client_num", run_info.get("client_num", "n/a"))),
        ("File size", host_info.get("file_size", run_info.get("file_size", "n/a"))),
    ]
    return [f"{key}: {value}" for key, value in fields]


def append_table(lines, title, results):
    lines.append(title)
    lines.append("round,current,median,delta,samples,status")
    for row in results:
        lines.append(
            f"{row['round_name']},{format_float(row['current'])},{format_float(row['baseline'])},"
            f"{format_pct(row['delta_pct'])},{row['samples']},{row['status']}"
        )


def build_report(args, history, throughput_results, latency_results):
    run_info = read_run_info(args.current_dir)
    host_info = read_host_info(args.current_dir)
    history_dirs = [entry["dir"] for entry in history]
    status = status_from_results(throughput_results, latency_results, len(history))

    lines = []
    lines.append(f"FalconFS daily baseline status: {status}")
    lines.append("")
    lines.extend(host_summary_lines(host_info, run_info))
    lines.append("")
    lines.append(f"History samples: {len(history_dirs)} (window={args.history_window}, min={args.min_history})")
    lines.append(f"Throughput alert threshold: -{args.threshold_pct:.1f}%")
    lines.append(f"Latency alert threshold: +{args.latency_threshold_pct:.1f}%")
    lines.append("")

    append_table(lines, "Round throughput comparison:", throughput_results)
    lines.append("")
    append_table(lines, "Round latency comparison (ns/op):", latency_results)

    lines.append("")
    lines.append("History result dirs:")
    if history_dirs:
        lines.extend(history_dirs)
    else:
        lines.append("n/a")

    return status, "\n".join(lines) + "\n"


def mail_subject(status, current_dir, throughput_results, latency_results):
    prefix = os.environ.get("FALCONFS_BASELINE_MAIL_SUBJECT_PREFIX", "[FalconFS baseline]")
    run_name = Path(current_dir).name
    throughput_bad = any(row["degraded"] for row in throughput_results)
    latency_bad = any(row["degraded"] for row in latency_results)
    if status == "ALERT":
        if throughput_bad and latency_bad:
            reason = "throughput/latency regression"
        elif throughput_bad:
            reason = "throughput degraded >10%"
        else:
            reason = "latency regressed >10%"
        return f"{prefix}[ALERT] {reason} {run_name}"
    return f"{prefix}[{status}] {run_name}"


def write_outputs(result_dir, status, report, throughput_results, latency_results):
    result_path = Path(result_dir)
    (result_path / "baseline_report.txt").write_text(report, encoding="utf-8")
    subject = mail_subject(status, result_dir, throughput_results, latency_results)
    preview = f"Subject: {subject}\n\n{report}"
    (result_path / "email_preview.txt").write_text(preview, encoding="utf-8")
    return subject


def send_mail(subject, report):
    if os.environ.get("FALCONFS_BASELINE_MAIL_ENABLED", "0") != "1":
        return False

    host = os.environ["FALCONFS_BASELINE_SMTP_HOST"]
    port = int(os.environ.get("FALCONFS_BASELINE_SMTP_PORT", "465"))
    user = os.environ["FALCONFS_BASELINE_SMTP_USER"]
    password = os.environ["FALCONFS_BASELINE_SMTP_PASSWORD"]
    sender = os.environ["FALCONFS_BASELINE_MAIL_FROM"]
    recipients = [item.strip() for item in os.environ["FALCONFS_BASELINE_MAIL_TO"].split(",") if item.strip()]
    use_ssl = os.environ.get("FALCONFS_BASELINE_SMTP_SSL", "1") == "1"
    use_starttls = os.environ.get("FALCONFS_BASELINE_SMTP_STARTTLS", "0") == "1"
    tls_verify = os.environ.get("FALCONFS_BASELINE_SMTP_TLS_VERIFY", "0") == "1"
    ca_file = os.environ.get("FALCONFS_BASELINE_SMTP_CA_FILE", "")

    if tls_verify:
        context = ssl.create_default_context(cafile=ca_file or None)
    else:
        context = ssl._create_unverified_context()

    msg = EmailMessage()
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = subject
    msg.set_content(report)

    if use_ssl:
        with smtplib.SMTP_SSL(host, port, context=context, timeout=30) as smtp:
            smtp.login(user, password)
            smtp.send_message(msg)
    else:
        with smtplib.SMTP(host, port, timeout=30) as smtp:
            if use_starttls:
                smtp.starttls(context=context)
            smtp.login(user, password)
            smtp.send_message(msg)
    return True


def main():
    parser = argparse.ArgumentParser(description="Generate FalconFS baseline report and optional email.")
    parser.add_argument("--result-root", required=True)
    parser.add_argument("--current-dir", required=True)
    parser.add_argument("--history-window", type=int, default=int(os.environ.get("FALCONFS_BASELINE_HISTORY_WINDOW", "7")))
    parser.add_argument("--min-history", type=int, default=int(os.environ.get("FALCONFS_BASELINE_MIN_HISTORY", "3")))
    parser.add_argument("--threshold-pct", type=float, default=float(os.environ.get("FALCONFS_BASELINE_ALERT_THRESHOLD_PCT", "10")))
    parser.add_argument("--latency-threshold-pct", type=float, default=float(os.environ.get("FALCONFS_BASELINE_LATENCY_ALERT_THRESHOLD_PCT", "10")))
    parser.add_argument("--latency-alert-enabled", default=os.environ.get("FALCONFS_BASELINE_LATENCY_ALERT_ENABLED", "1"))
    args = parser.parse_args()

    args.current_dir = str(Path(args.current_dir).resolve())
    throughput_threshold = args.threshold_pct / 100.0
    latency_threshold = args.latency_threshold_pct / 100.0
    latency_alert_enabled = args.latency_alert_enabled == "1"

    current_summary = read_summary(args.current_dir)
    if not current_summary:
        raise SystemExit(f"missing or empty summary.csv in {args.current_dir}")

    history = load_history(args.result_root, [args.current_dir], args.history_window)
    baseline = build_baseline(history, current_summary.keys(), args.min_history)
    throughput_results = compare_throughput(current_summary, baseline, throughput_threshold)
    latency_results = compare_latency(current_summary, baseline, latency_threshold, latency_alert_enabled)
    status, report = build_report(args, history, throughput_results, latency_results)
    subject = write_outputs(args.current_dir, status, report, throughput_results, latency_results)

    sent = send_mail(subject, report)
    print(f"baseline report status: {status}")
    print(f"baseline report: {Path(args.current_dir) / 'baseline_report.txt'}")
    print(f"email preview: {Path(args.current_dir) / 'email_preview.txt'}")
    if sent:
        print("email sent")
    else:
        print("email not sent (set FALCONFS_BASELINE_MAIL_ENABLED=1 and SMTP env vars to enable)")

    if status == "ALERT":
        raise SystemExit(ALERT_EXIT_CODE)


if __name__ == "__main__":
    main()
