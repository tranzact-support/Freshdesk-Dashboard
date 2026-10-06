#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, Tuple


DEFAULT_STATUS_FILE = ".freshdesk_hubspot_sync_status.json"
DEFAULT_HEALTH_STATE_FILE = ".freshdesk_hubspot_sync_health_state.json"
DEFAULT_ALERT_LOG = "freshdesk_hubspot_sync_healthcheck.log"
DEFAULT_MAX_DELAY_MINUTES = 360
DEFAULT_STUCK_MINUTES = 240


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
    return utc_now().replace(microsecond=0).isoformat().replace("+00:00", "Z")


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


def normalize_phone_candidates(phone: str) -> Iterable[str]:
    digits = "".join(ch for ch in phone if ch.isdigit())
    candidates = []
    if digits:
        candidates.append(digits)
        if digits.startswith("91") and len(digits) > 10:
            candidates.append("+" + digits)
            candidates.append(digits[-10:])
        elif len(digits) == 10:
            candidates.append("+91" + digits)
        else:
            candidates.append("+" + digits)
    seen = set()
    for candidate in candidates:
        if candidate and candidate not in seen:
            seen.add(candidate)
            yield candidate


def send_messages_alert(phone: str, text: str) -> str:
    handles = list(normalize_phone_candidates(phone))
    if not handles:
        raise RuntimeError("No usable phone handle could be built")

    applescript = """
on run argv
  set targetHandles to paragraphs of item 1 of argv
  set messageText to item 2 of argv
  tell application "Messages"
    repeat with svc in services
      set svcType to ""
      try
        set svcType to (service type of svc as text)
      end try
      if svcType is in {"SMS", "RCS", "iMessage"} then
        repeat with targetHandle in targetHandles
          try
            set targetBuddy to buddy (targetHandle as text) of svc
            send messageText to targetBuddy
            return svcType & ":" & (targetHandle as text)
          end try
          try
            set targetParticipant to participant (targetHandle as text) of svc
            send messageText to targetParticipant
            return svcType & ":" & (targetHandle as text)
          end try
        end repeat
      end if
    end repeat
  end tell
  error "Unable to send with any Messages service"
end run
"""
    proc = subprocess.run(
        ["osascript", "-", "\n".join(handles), text],
        input=applescript,
        text=True,
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "").strip()
        raise RuntimeError(detail or "Messages send failed")
    return (proc.stdout or "").strip() or "messages"


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
    last_summary = status.get("last_summary") or {}
    failure_count = 0
    if isinstance(last_summary, dict):
        failure_count = int(last_summary.get("failures") or 0)

    if not status:
        return False, "missing_status", "sync status file is missing"
    if running and started_at and (now - started_at).total_seconds() > stuck_minutes * 60:
        return False, "stuck_running", f"sync has been marked running for more than {stuck_minutes} minutes"
    if finished_at and (last_exit_code != 0 or failure_count > 0):
        detail = last_error or f"sync last run recorded failures={failure_count}"
        return False, "last_run_failed", detail
    if success_at is None:
        return False, "missing_success", "sync has not recorded a successful run yet"
    if (now - success_at).total_seconds() > max_delay_minutes * 60:
        return False, "stale_success", f"last successful sync is older than {max_delay_minutes} minutes"
    return True, "healthy", "sync is healthy"


def build_fingerprint(status: Dict[str, object], health_key: str, detail: str) -> str:
    return json.dumps(
        {
            "health_key": health_key,
            "detail": detail,
            "last_run_started_at": status.get("last_run_started_at"),
            "last_run_finished_at": status.get("last_run_finished_at"),
            "last_success_at": status.get("last_success_at"),
            "last_exit_code": status.get("last_exit_code"),
            "last_error": status.get("last_error"),
            "last_summary": status.get("last_summary"),
        },
        sort_keys=True,
        ensure_ascii=True,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Check whether the Freshdesk-HubSpot sync is running properly."
    )
    parser.add_argument(
        "--env-file",
        default=str(Path(__file__).with_name(".env")),
        help="Path to env file with sync settings.",
    )
    parser.add_argument(
        "--status-file",
        default=str(Path(__file__).with_name(DEFAULT_STATUS_FILE)),
        help="Path to the sync status JSON file.",
    )
    parser.add_argument(
        "--health-state-file",
        default=str(Path(__file__).with_name(DEFAULT_HEALTH_STATE_FILE)),
        help="Path to health state JSON file.",
    )
    parser.add_argument(
        "--alert-log",
        default=str(Path(__file__).with_name(DEFAULT_ALERT_LOG)),
        help="Path to health alert log.",
    )
    parser.add_argument(
        "--max-delay-minutes",
        type=int,
        default=DEFAULT_MAX_DELAY_MINUTES,
        help="Maximum allowed age of the last successful sync.",
    )
    parser.add_argument(
        "--stuck-minutes",
        type=int,
        default=DEFAULT_STUCK_MINUTES,
        help="How long the sync may stay marked running before alerting.",
    )
    parser.add_argument(
        "--phone",
        default="",
        help="Phone number to notify through Messages when health changes to unhealthy.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    env = load_env(Path(args.env_file))

    status_file = Path(env_or_file("HUBSPOT_SYNC_STATUS_FILE", env, args.status_file))
    health_state_file = Path(args.health_state_file)
    alert_log = Path(args.alert_log)
    max_delay_minutes = int(
        env_or_file(
            "HUBSPOT_SYNC_HEALTH_MAX_DELAY_MINUTES",
            env,
            str(args.max_delay_minutes),
        )
    )
    stuck_minutes = int(
        env_or_file(
            "HUBSPOT_SYNC_HEALTH_STUCK_MINUTES",
            env,
            str(args.stuck_minutes),
        )
    )
    phone = args.phone or env_or_file("HUBSPOT_SYNC_ALERT_PHONE", env)

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
        "fingerprint": build_fingerprint(status, health_key, detail),
    }
    save_json_dict(health_state_file, payload)

    prior_key = str(prior_health.get("health_key") or "")
    prior_healthy = bool(prior_health.get("healthy"))
    prior_fingerprint = str(prior_health.get("fingerprint") or "")
    if (
        health_key != prior_key
        or healthy != prior_healthy
        or payload["fingerprint"] != prior_fingerprint
    ):
        prefix = "RECOVERY" if healthy else "ALERT"
        append_log(alert_log, f"{payload['checked_at']} {prefix} {health_key} {detail}")
        if not healthy and phone:
            message = (
                f"Freshdesk-HubSpot sync alert: {health_key}. "
                f"{detail}. "
                f"Checked at {payload['checked_at']}."
            )
            try:
                mode = send_messages_alert(phone, message)
                append_log(
                    alert_log,
                    f"{payload['checked_at']} MESSAGE_SENT mode={mode} phone={phone}",
                )
            except RuntimeError as exc:
                append_log(
                    alert_log,
                    f"{payload['checked_at']} MESSAGE_FAIL phone={phone} error={exc}",
                )

    if healthy:
        print(f"HEALTHY {detail}")
        return 0

    print(f"ALERT {health_key} {detail}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
