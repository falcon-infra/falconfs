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


def read_summary(result_dir):
    summary_path = Path(result_dir) / "summary.csv"
    if not summary_path.is_file():
        return None

    rows = {}
    with summary_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row.get("round_name", "").strip()
            if not name:
                continue
            try:
                throughput = float(row.get("throughput", ""))
            except ValueError:
                continue
            rows[name] = {
                "round_idx": row.get("round_idx", ""),
                "throughput": throughput,
                "avg_latency": row.get("avg_latency", ""),
                "ops": row.get("ops", ""),
                "time": row.get("time", ""),
                "raw_log": row.get("raw_log", ""),
            }

    return rows if rows else None


def read_run_info(result_dir):
    info = {}
    path = Path(result_dir) / "run_info.env"
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
        if child.name.endswith("_confirm"):
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
        values = [entry["summary"][name]["throughput"] for entry in history if name in entry["summary"]]
        if len(values) >= min_history:
            baseline[name] = {
                "median": median(values),
                "samples": len(values),
            }
    return baseline


def compare(summary, baseline, threshold):
    results = []
    for name, row in summary.items():
        current = row["throughput"]
        base = baseline.get(name)
        if not base:
            results.append({
                "round_name": name,
                "current": current,
                "baseline": None,
                "samples": 0,
                "delta_pct": None,
                "degraded": False,
            })
            continue

        base_value = base["median"]
        delta_pct = ((current - base_value) / base_value) * 100 if base_value else 0.0
        results.append({
            "round_name": name,
            "current": current,
            "baseline": base_value,
            "samples": base["samples"],
            "delta_pct": delta_pct,
            "degraded": current < base_value * (1 - threshold),
        })
    return results


def format_float(value):
    if value is None:
        return "n/a"
    return f"{value:.2f}"


def format_pct(value):
    if value is None:
        return "n/a"
    return f"{value:.2f}%"


def status_from_results(primary_results, confirm_results, history_count):
    primary_degraded = any(row["degraded"] for row in primary_results)
    confirm_degraded = any(row["degraded"] for row in confirm_results) if confirm_results is not None else False

    if history_count == 0:
        return "NO_HISTORY"
    if confirm_results is not None and primary_degraded and not confirm_degraded:
        return "TRANSIENT"
    if confirm_results is not None and confirm_degraded:
        return "ALERT"
    if primary_degraded:
        return "SUSPECT"
    return "OK"


def build_report(args, current_summary, history, baseline, primary_results, confirm_summary, confirm_results):
    info = read_run_info(args.current_dir)
    history_dirs = [entry["dir"] for entry in history]
    status = status_from_results(primary_results, confirm_results, len(history))

    lines = []
    lines.append(f"FalconFS daily baseline status: {status}")
    lines.append("")
    lines.append(f"Current result: {args.current_dir}")
    lines.append(f"Commit: {info.get('commit_sha', 'n/a')}")
    lines.append(f"Subject: {info.get('commit_subject', 'n/a')}")
    lines.append(f"Git dirty: {info.get('git_dirty', 'n/a')}")
    lines.append(f"History samples: {len(history_dirs)} (window={args.history_window}, min={args.min_history})")
    lines.append(f"Alert threshold: -{args.threshold_pct:.1f}%")
    if args.confirm_dir:
        lines.append(f"Confirm result: {args.confirm_dir}")
    lines.append("")

    lines.append("Round throughput comparison:")
    lines.append("round,current,median,delta,samples,status")
    for row in primary_results:
        row_status = "DEGRADED" if row["degraded"] else "OK"
        lines.append(
            f"{row['round_name']},{format_float(row['current'])},{format_float(row['baseline'])},"
            f"{format_pct(row['delta_pct'])},{row['samples']},{row_status}"
        )

    if confirm_results is not None:
        lines.append("")
        lines.append("Confirm run comparison:")
        lines.append("round,current,median,delta,samples,status")
        for row in confirm_results:
            row_status = "DEGRADED" if row["degraded"] else "OK"
            lines.append(
                f"{row['round_name']},{format_float(row['current'])},{format_float(row['baseline'])},"
                f"{format_pct(row['delta_pct'])},{row['samples']},{row_status}"
            )

    lines.append("")
    lines.append("History result dirs:")
    if history_dirs:
        lines.extend(history_dirs)
    else:
        lines.append("n/a")

    return status, "\n".join(lines) + "\n"


def mail_subject(status, current_dir):
    prefix = os.environ.get("FALCONFS_BASELINE_MAIL_SUBJECT_PREFIX", "[FalconFS baseline]")
    run_name = Path(current_dir).name
    if status == "ALERT":
        return f"{prefix}[ALERT] throughput degraded >10% {run_name}"
    if status == "SUSPECT":
        return f"{prefix}[SUSPECT] throughput degraded, confirmation pending {run_name}"
    if status == "TRANSIENT":
        return f"{prefix}[OK] transient throughput dip recovered {run_name}"
    return f"{prefix}[{status}] {run_name}"


def write_outputs(result_dir, status, report):
    result_path = Path(result_dir)
    (result_path / "baseline_report.txt").write_text(report, encoding="utf-8")
    subject = mail_subject(status, result_dir)
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
    parser.add_argument("--confirm-dir", default="")
    parser.add_argument("--history-window", type=int, default=int(os.environ.get("FALCONFS_BASELINE_HISTORY_WINDOW", "7")))
    parser.add_argument("--min-history", type=int, default=int(os.environ.get("FALCONFS_BASELINE_MIN_HISTORY", "3")))
    parser.add_argument("--threshold-pct", type=float, default=float(os.environ.get("FALCONFS_BASELINE_ALERT_THRESHOLD_PCT", "10")))
    args = parser.parse_args()

    args.current_dir = str(Path(args.current_dir).resolve())
    args.confirm_dir = str(Path(args.confirm_dir).resolve()) if args.confirm_dir else ""
    threshold = args.threshold_pct / 100.0

    current_summary = read_summary(args.current_dir)
    if not current_summary:
        raise SystemExit(f"missing or empty summary.csv in {args.current_dir}")

    confirm_summary = read_summary(args.confirm_dir) if args.confirm_dir else None
    history = load_history(args.result_root, [args.current_dir, args.confirm_dir], args.history_window)
    baseline = build_baseline(history, current_summary.keys(), args.min_history)
    primary_results = compare(current_summary, baseline, threshold)
    confirm_results = compare(confirm_summary, baseline, threshold) if confirm_summary else None
    status, report = build_report(args, current_summary, history, baseline, primary_results, confirm_summary, confirm_results)
    subject = write_outputs(args.current_dir, status, report)

    sent = send_mail(subject, report)
    print(f"baseline report status: {status}")
    print(f"baseline report: {Path(args.current_dir) / 'baseline_report.txt'}")
    print(f"email preview: {Path(args.current_dir) / 'email_preview.txt'}")
    if sent:
        print("email sent")
    else:
        print("email not sent (set FALCONFS_BASELINE_MAIL_ENABLED=1 and SMTP env vars to enable)")

    if status == "SUSPECT":
        raise SystemExit(ALERT_EXIT_CODE)
    if status == "ALERT":
        raise SystemExit(ALERT_EXIT_CODE)


if __name__ == "__main__":
    main()
