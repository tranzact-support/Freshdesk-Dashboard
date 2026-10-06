#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Tuple


def load_env(env_path: Path) -> Dict[str, str]:
    data: Dict[str, str] = {}
    if not env_path.exists():
        return data
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        data[key.strip()] = value.strip().strip('"').strip("'")
    return data


def env_or_file(name: str, env_values: Dict[str, str], default: str = "") -> str:
    return os.environ.get(name, env_values.get(name, default)).strip()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def parse_iso(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def load_json_dict(path: Path) -> Dict[str, object]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def save_json_dict(path: Path, payload: Dict[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def append_log(path: Path, message: str) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(message + "\n")


def post_json(url: str, payload: Dict[str, object], timeout: int = 30) -> None:
    req = urllib.request.Request(
        url=url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout):
            return
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            "Slack notification failed with HTTP {0}: {1}".format(exc.code, detail[:400])
        ) from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Slack notification failed: {exc}") from exc


def send_slack_notification(
    *,
    webhook_url: str,
    relay_url: str,
    relay_channel: str,
    subject: str,
    detail: str,
    status_file: str,
) -> str:
    text = (
        f"{subject}\n"
        f"Detail: {detail}\n"
        f"Status file: {status_file}\n"
        f"Time: {utc_now_iso()}"
    )
    if webhook_url:
        post_json(webhook_url, {"text": text})
        return "webhook"
    if relay_url and relay_channel:
        post_json(
            relay_url,
            {
                "channel_name": relay_channel,
                "message_title": subject,
                "message_params": [
                    {"key": "Detail", "value": detail},
                    {"key": "Status file", "value": status_file},
                    {"key": "Time", "value": utc_now_iso()},
                ],
            },
        )
        return "relay"
    raise RuntimeError("Slack is not configured")


def evaluate_health(
    status: Dict[str, object],
    *,
    max_delay_minutes: int,
    stuck_minutes: int,
) -> Tuple[bool, str, str]:
    now = utc_now()
    started_at = parse_iso(status.get("last_run_started_at"))
    finished_at = parse_iso(status.get("last_run_finished_at"))
    success_at = parse_iso(status.get("last_success_at"))
    running = bool(status.get("running"))
    last_exit_code = int(status.get("last_exit_code") or 0)
    last_error = str(status.get("last_error") or "")

    if not status:
        return False, "missing_status", "watcher status file is missing"
    if running and started_at and (now - started_at).total_seconds() > stuck_minutes * 60:
        return False, "stuck_running", f"watcher has been marked running for more than {stuck_minutes} minutes"
    if last_exit_code != 0 and finished_at:
        return False, "last_run_failed", last_error or "watcher last run failed"
    if success_at is None:
        return False, "missing_success", "watcher has not recorded a successful run yet"
    if (now - success_at).total_seconds() > max_delay_minutes * 60:
        return False, "stale_success", f"last successful run is older than {max_delay_minutes} minutes"
    return True, "healthy", "watcher is healthy"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Check whether the ticket note watcher is still running properly."
    )
    parser.add_argument(
        "--env-file",
        default=str(Path(__file__).with_name(".env")),
        help="Path to env file with watcher settings.",
    )
    parser.add_argument(
        "--status-file",
        help="Optional override for watcher status file path.",
    )
    parser.add_argument(
        "--health-state-file",
        help="Optional override for healthcheck state file path.",
    )
    parser.add_argument(
        "--alert-log",
        help="Optional override for health alert log path.",
    )
    parser.add_argument(
        "--max-delay-minutes",
        type=int,
        help="Maximum allowed age of the last successful watcher run.",
    )
    parser.add_argument(
        "--stuck-minutes",
        type=int,
        help="How long the watcher may stay marked as running before alerting.",
    )
    parser.add_argument(
        "--slack-webhook-url",
        help="Optional direct Slack webhook URL.",
    )
    parser.add_argument(
        "--slack-relay-url",
        help="Optional Slack relay endpoint URL.",
    )
    parser.add_argument(
        "--slack-channel-name",
        help="Optional channel name for Slack relay notifications.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    env = load_env(Path(args.env_file))

    status_file = Path(
        args.status_file
        or env_or_file(
            "WATCH_STATUS_FILE",
            env,
            str(Path(__file__).with_name(".ticket_note_watcher_status.json")),
        )
    )
    health_state_file = Path(
        args.health_state_file
        or env_or_file(
            "WATCH_HEALTH_STATE_FILE",
            env,
            str(Path(__file__).with_name(".ticket_note_health_state.json")),
        )
    )
    alert_log = Path(
        args.alert_log
        or env_or_file(
            "WATCH_ALERT_LOG",
            env,
            str(Path(__file__).with_name("ticket_note_healthcheck.log")),
        )
    )
    max_delay_minutes = args.max_delay_minutes or int(
        env_or_file("WATCH_HEALTH_MAX_DELAY_MINUTES", env, "10")
    )
    stuck_minutes = args.stuck_minutes or int(
        env_or_file("WATCH_HEALTH_STUCK_MINUTES", env, "15")
    )
    slack_webhook_url = args.slack_webhook_url or env_or_file("SLACK_WEBHOOK_URL", env)
    slack_relay_url = args.slack_relay_url or env_or_file("SLACK_RELAY_URL", env)
    slack_channel_name = args.slack_channel_name or env_or_file("SLACK_CHANNEL_NAME", env)

    status = load_json_dict(status_file)
    prior_health = load_json_dict(health_state_file)
    healthy, health_key, detail = evaluate_health(
        status,
        max_delay_minutes=max_delay_minutes,
        stuck_minutes=stuck_minutes,
    )

    payload = {
        "checked_at": utc_now_iso(),
        "healthy": healthy,
        "health_key": health_key,
        "detail": detail,
        "status_file": str(status_file),
    }
    save_json_dict(health_state_file, payload)

    prior_key = str(prior_health.get("health_key") or "")
    prior_healthy = bool(prior_health.get("healthy"))
    if health_key != prior_key or healthy != prior_healthy:
        prefix = "RECOVERY" if healthy else "ALERT"
        append_log(alert_log, f"{payload['checked_at']} {prefix} {health_key} {detail}")
        if slack_webhook_url or (slack_relay_url and slack_channel_name):
            subject = (
                "Freshdesk watcher recovered"
                if healthy
                else f"Freshdesk watcher alert: {health_key}"
            )
            try:
                mode = send_slack_notification(
                    webhook_url=slack_webhook_url,
                    relay_url=slack_relay_url,
                    relay_channel=slack_channel_name,
                    subject=subject,
                    detail=detail,
                    status_file=str(status_file),
                )
                append_log(
                    alert_log,
                    f"{payload['checked_at']} SLACK_SENT mode={mode} health_key={health_key}",
                )
            except RuntimeError as exc:
                append_log(
                    alert_log,
                    f"{payload['checked_at']} SLACK_FAIL health_key={health_key} error={exc}",
                )

    if healthy:
        print(f"HEALTHY {detail}")
        return 0

    print(f"ALERT {health_key} {detail}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
