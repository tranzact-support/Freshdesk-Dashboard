#!/usr/bin/env python3
import argparse
import base64
import http.client
import json
import os
import re
import socket
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


STATUS_MAP = {
    "resolved": 4,
    "closed": 5,
    "open": 2,
    "pending": 3,
}

ALLOWED_RAISED_BY = {"Shishir", "Pranit", "Shabib", "-"}
ALLOWED_TYPES = {
    "Task - Experience Team",
    "Task - Backend",
    "Bug",
    "Feature Idea",
    "Service Request",
    "Usability FI",
}
HUBSPOT_SYNC_STATE_FILE = Path(__file__).with_name(".freshdesk_hubspot_sync_state.json")
HUBSPOT_CLOSE_HELPER = Path(__file__).with_name("hubspot_close_linked_tickets.py").name
DEFAULT_FRESHDESK_MIN_INTERVAL_SECONDS = 1.5
DEFAULT_FRESHDESK_MAX_RETRIES = 6
DEFAULT_FRESHDESK_RETRY_BASE_SECONDS = 5.0


def load_env(env_path: Path) -> Dict[str, str]:
    data: Dict[str, str] = {}
    if not env_path.exists():
        return data
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        data[key.strip()] = value.strip()
    return data


def load_hubspot_sync_ticket_map(state_path: Path) -> Dict[str, Dict]:
    if not state_path.exists():
        return {}
    try:
        data = json.loads(state_path.read_text())
    except (OSError, json.JSONDecodeError):
        return {}
    tickets = data.get("tickets")
    if not isinstance(tickets, dict):
        return {}
    return {str(key): value for key, value in tickets.items() if isinstance(value, dict)}


class FreshdeskClient:
    def __init__(self, domain: str, api_key: str):
        self.base = f"https://{domain}/api/v2"
        token = base64.b64encode(f"{api_key}:X".encode("utf-8")).decode("ascii")
        self.auth_header = f"Basic {token}"
        self.min_interval_seconds = max(
            0.0,
            float(
                os.environ.get(
                    "FRESHDESK_MIN_INTERVAL_SECONDS",
                    str(DEFAULT_FRESHDESK_MIN_INTERVAL_SECONDS),
                )
            ),
        )
        self.max_retries = max(
            0,
            int(
                os.environ.get(
                    "FRESHDESK_MAX_RETRIES",
                    str(DEFAULT_FRESHDESK_MAX_RETRIES),
                )
            ),
        )
        self.retry_base_seconds = max(
            0.1,
            float(
                os.environ.get(
                    "FRESHDESK_RETRY_BASE_SECONDS",
                    str(DEFAULT_FRESHDESK_RETRY_BASE_SECONDS),
                )
            ),
        )
        self._last_request_started_at = 0.0

    def _respect_rate_limit(self) -> None:
        if self.min_interval_seconds <= 0:
            return
        now = time.monotonic()
        elapsed = now - self._last_request_started_at
        if elapsed < self.min_interval_seconds:
            time.sleep(self.min_interval_seconds - elapsed)
        self._last_request_started_at = time.monotonic()

    def _retry_delay(self, headers: Optional[object], attempt: int) -> float:
        retry_after = None
        if headers is not None:
            retry_after = headers.get("Retry-After")
        if retry_after:
            try:
                return max(float(retry_after), self.retry_base_seconds)
            except ValueError:
                pass
        return self.retry_base_seconds * (2 ** max(0, attempt - 1))

    def _is_transient_exception(self, exc: BaseException) -> bool:
        if isinstance(exc, (TimeoutError, socket.timeout, ConnectionResetError, ssl.SSLError)):
            return True
        if isinstance(exc, http.client.HTTPException):
            return True
        if isinstance(exc, urllib.error.URLError):
            reason = getattr(exc, "reason", None)
            return isinstance(
                reason,
                (
                    TimeoutError,
                    socket.timeout,
                    ConnectionResetError,
                    ssl.SSLError,
                    OSError,
                ),
            ) or reason is None
        if isinstance(exc, OSError):
            return True
        return False

    def request(
        self, method: str, path: str, payload: Optional[Dict] = None
    ) -> Tuple[int, Optional[Dict], str]:
        url = urllib.parse.urljoin(f"{self.base}/", path.lstrip("/"))
        data = None
        headers = {"Authorization": self.auth_header}
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        for attempt in range(1, self.max_retries + 2):
            self._respect_rate_limit()
            req = urllib.request.Request(url, data=data, headers=headers, method=method)
            try:
                with urllib.request.urlopen(req, timeout=120) as resp:
                    raw = resp.read().decode("utf-8", errors="replace")
                    parsed = json.loads(raw) if raw else None
                    return resp.status, parsed, raw
            except urllib.error.HTTPError as exc:
                raw = exc.read().decode("utf-8", errors="replace")
                parsed = None
                if raw:
                    try:
                        parsed = json.loads(raw)
                    except json.JSONDecodeError:
                        parsed = None
                if exc.code == 429 and attempt <= self.max_retries:
                    time.sleep(self._retry_delay(exc.headers, attempt))
                    continue
                return exc.code, parsed, raw
            except Exception as exc:
                if attempt <= self.max_retries and self._is_transient_exception(exc):
                    time.sleep(self._retry_delay(None, attempt))
                    continue
                raise
        raise RuntimeError("Freshdesk request retry loop exited unexpectedly")

    def get_ticket(self, ticket_id: int) -> Tuple[int, Optional[Dict], str]:
        return self.request("GET", f"tickets/{ticket_id}")

    def restore_ticket(self, ticket_id: int) -> Tuple[int, Optional[Dict], str]:
        return self.request("PUT", f"tickets/{ticket_id}/restore")

    def update_ticket(
        self, ticket_id: int, payload: Dict
    ) -> Tuple[int, Optional[Dict], str]:
        return self.request("PUT", f"tickets/{ticket_id}", payload)

    def add_note(self, ticket_id: int, text: str, private: bool = True):
        payload = {"body": text, "private": private}
        return self.request("POST", f"tickets/{ticket_id}/notes", payload)

    def search_tickets(self, query: str, page: int = 1) -> Tuple[int, Optional[Dict], str]:
        normalized_query = query if query.startswith("\"") and query.endswith("\"") else f"\"{query}\""
        encoded = urllib.parse.quote(normalized_query, safe="")
        return self.request("GET", f"search/tickets?query={encoded}&page={page}")


def parse_ids(args: argparse.Namespace) -> List[int]:
    ids: List[int] = []
    if args.ticket:
        ids.append(int(args.ticket))
    if args.tickets:
        ids.extend(int(part.strip()) for part in args.tickets.split(",") if part.strip())
    if args.file:
        text = Path(args.file).read_text()
        ids.extend(int(match) for match in re.findall(r"\d+", text))
    deduped: List[int] = []
    seen = set()
    for ticket_id in ids:
        if ticket_id not in seen:
            seen.add(ticket_id)
            deduped.append(ticket_id)
    return deduped


def ensure_ticket_available(
    client: FreshdeskClient, ticket_id: int, restore_missing: bool
) -> Tuple[Optional[Dict], bool, str]:
    code, data, _ = client.get_ticket(ticket_id)
    if code == 200 and data:
        return data, False, "ok"
    if code == 404 and restore_missing:
        restore_code, _, _ = client.restore_ticket(ticket_id)
        if restore_code in {200, 201, 202}:
            code, data, _ = client.get_ticket(ticket_id)
            if code == 200 and data:
                return data, True, "restored"
        return None, False, "not_found"
    if code == 404:
        return None, False, "not_found"
    return None, False, f"http_{code}"


def build_status_payload(ticket: Dict, status_value: int) -> Tuple[Dict, bool, bool]:
    payload: Dict = {"status": status_value}
    changed_raised_by = False
    changed_type = False

    custom_fields = ticket.get("custom_fields") or {}
    raised_by = custom_fields.get("cf_raised_by")
    if raised_by not in ALLOWED_RAISED_BY:
        payload["custom_fields"] = {"cf_raised_by": "-"}
        changed_raised_by = True

    legacy_company_id = custom_fields.get("cf_company_id880506")
    if legacy_company_id in {None, ""}:
        fallback_company_id = custom_fields.get("cf_company_id")
        normalized_company_id = "0" if fallback_company_id in {None, ""} else str(fallback_company_id)
        payload.setdefault("custom_fields", {})["cf_company_id880506"] = normalized_company_id
    elif not isinstance(legacy_company_id, str):
        payload.setdefault("custom_fields", {})["cf_company_id880506"] = str(legacy_company_id)

    ticket_type = ticket.get("type")
    if ticket_type not in ALLOWED_TYPES:
        payload["type"] = "Feature Idea"
        changed_type = True

    return payload, changed_raised_by, changed_type


def run_status(client: FreshdeskClient, args: argparse.Namespace) -> int:
    ids = parse_ids(args)
    target = STATUS_MAP[args.value]
    ok = restored = not_found = failed = changed_raised_by = changed_type = 0
    hubspot_ticket_map = (
        load_hubspot_sync_ticket_map(Path(args.hubspot_sync_state_file))
        if args.value == "closed"
        else {}
    )

    for ticket_id in ids:
        ticket, was_restored, state = ensure_ticket_available(
            client, ticket_id, args.restore_missing
        )
        if state == "not_found":
            print(f"MISS {ticket_id} not_found")
            not_found += 1
            continue
        if ticket is None:
            print(f"FAIL {ticket_id} {state}")
            failed += 1
            continue
        if was_restored:
            restored += 1
        if (
            args.value == "closed"
            and not args.allow_direct_close_task_experience
            and str(ticket.get("type") or "") == "Task - Experience Team"
        ):
            entry = hubspot_ticket_map.get(str(ticket_id)) or {}
            hubspot_ticket_id = str(entry.get("hubspot_ticket_id") or "").strip()
            if hubspot_ticket_id:
                print(
                    "BLOCK "
                    f"{ticket_id} linked_hubspot_ticket={hubspot_ticket_id} "
                    f"use python3 {HUBSPOT_CLOSE_HELPER} --ticket {ticket_id} "
                    "so cron closes Freshdesk"
                )
                failed += 1
                continue

        payload, changed_rb, changed_tp = build_status_payload(ticket, target)
        code, data, raw = client.update_ticket(ticket_id, payload)
        if code == 200 and data and data.get("status") == target:
            print(f"OK {ticket_id}")
            ok += 1
            changed_raised_by += int(changed_rb)
            changed_type += int(changed_tp)
        elif code == 404 and args.restore_missing:
            restore_code, _, _ = client.restore_ticket(ticket_id)
            if restore_code in {200, 201, 202}:
                code, data, raw = client.update_ticket(ticket_id, payload)
                if code == 200 and data and data.get("status") == target:
                    print(f"OK {ticket_id}")
                    ok += 1
                    restored += 1
                    changed_raised_by += int(changed_rb)
                    changed_type += int(changed_tp)
                    continue
            print(f"MISS {ticket_id} not_found")
            not_found += 1
        else:
            msg = "unexpected_response"
            if data:
                if "message" in data:
                    msg = str(data["message"])
                elif "description" in data:
                    msg = str(data["description"])
                elif "errors" in data and data["errors"]:
                    msg = str(data["errors"][0].get("message", msg))
            elif raw:
                msg = raw[:200]
            print(f"FAIL {ticket_id} {msg}")
            failed += 1

    print(
        "SUMMARY "
        f"ok={ok} restored={restored} changed_cf_raised_by={changed_raised_by} "
        f"changed_type={changed_type} failed={failed} not_found={not_found} "
        f"unique_total={len(ids)}"
    )
    return 0 if failed == 0 else 1


def run_note(client: FreshdeskClient, args: argparse.Namespace) -> int:
    ids = parse_ids(args)
    ok = not_found = failed = restored = 0

    for ticket_id in ids:
        if args.restore_missing:
            _, was_restored, state = ensure_ticket_available(client, ticket_id, True)
            if state == "not_found":
                print(f"MISS {ticket_id} not_found")
                not_found += 1
                continue
            if state != "ok" and state != "restored":
                print(f"FAIL {ticket_id} {state}")
                failed += 1
                continue
            restored += int(was_restored)

        code, data, raw = client.add_note(ticket_id, args.text, private=not args.public)
        if code == 201:
            print(f"OK {ticket_id}")
            ok += 1
        elif code == 404:
            print(f"MISS {ticket_id} not_found")
            not_found += 1
        else:
            msg = "unexpected_response"
            if data:
                if "message" in data:
                    msg = str(data["message"])
                elif "description" in data:
                    msg = str(data["description"])
            elif raw:
                msg = raw[:200]
            print(f"FAIL {ticket_id} {msg}")
            failed += 1

    print(
        "SUMMARY "
        f"ok={ok} restored={restored} failed={failed} not_found={not_found} "
        f"unique_total={len(ids)}"
    )
    return 0 if failed == 0 else 1


def run_search(client: FreshdeskClient, args: argparse.Namespace) -> int:
    query = args.query
    if args.status:
        query = f'status:{STATUS_MAP[args.status]}'

    page = 1
    total = 0
    while True:
        code, data, raw = client.search_tickets(query, page=page)
        if code != 200 or not data:
            msg = "unexpected_response"
            if data:
                if "message" in data:
                    msg = str(data["message"])
                elif "description" in data:
                    msg = str(data["description"])
            elif raw:
                msg = raw[:200]
            print(f"FAIL search {msg}")
            return 1

        results = data.get("results") or []
        if page == 1:
            total = int(data.get("total", 0))

        for ticket in results:
            ticket_id = ticket.get("id")
            status = ticket.get("status")
            ticket_type = ticket.get("type") or "-"
            subject = (ticket.get("subject") or "").replace("\n", " ").strip()
            print(f"{ticket_id}\tstatus={status}\ttype={ticket_type}\tsubject={subject}")

        if not results or len(results) < 30:
            break
        page += 1

    print(f"SUMMARY total={total} query={query}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Freshdesk daily operations CLI")
    parser.add_argument(
        "--env-file",
        default=str(Path(__file__).with_name(".env")),
        help="Path to env file with FRESHDESK_DOMAIN and FRESHDESK_API_KEY",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_id_args(cmd: argparse.ArgumentParser) -> None:
        cmd.add_argument("--ticket", help="Single ticket ID")
        cmd.add_argument("--tickets", help="Comma separated ticket IDs")
        cmd.add_argument("--file", help="Path to file containing ticket IDs")
        cmd.add_argument(
            "--restore-missing",
            action="store_true",
            help="Try Freshdesk restore on 404 before giving up",
        )

    status_cmd = subparsers.add_parser("status", help="Update ticket status")
    add_id_args(status_cmd)
    status_cmd.add_argument(
        "--value",
        required=True,
        choices=sorted(STATUS_MAP.keys()),
        help="Target status",
    )
    status_cmd.add_argument(
        "--hubspot-sync-state-file",
        default=str(HUBSPOT_SYNC_STATE_FILE),
        help="State file used to detect HubSpot-linked Task - Experience Team tickets.",
    )
    status_cmd.add_argument(
        "--allow-direct-close-task-experience",
        action="store_true",
        help="Allow direct Freshdesk close even when a Task - Experience Team ticket is linked to HubSpot.",
    )

    note_cmd = subparsers.add_parser("note", help="Add note to tickets")
    add_id_args(note_cmd)
    note_cmd.add_argument("--text", required=True, help="Note text")
    note_cmd.add_argument(
        "--public",
        action="store_true",
        help="Add as public note instead of private note",
    )

    search_cmd = subparsers.add_parser("search", help="Search tickets")
    search_cmd.add_argument("--query", help="Freshdesk search query")
    search_cmd.add_argument(
        "--status",
        choices=sorted(STATUS_MAP.keys()),
        help="Search by mapped status name",
    )

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    env = load_env(Path(args.env_file))
    domain = os.environ.get("FRESHDESK_DOMAIN", env.get("FRESHDESK_DOMAIN"))
    api_key = os.environ.get("FRESHDESK_API_KEY", env.get("FRESHDESK_API_KEY"))

    if not domain or not api_key:
        parser.error("Missing FRESHDESK_DOMAIN or FRESHDESK_API_KEY")

    client = FreshdeskClient(domain=domain, api_key=api_key)

    if args.command == "status":
        return run_status(client, args)
    if args.command == "note":
        return run_note(client, args)
    if args.command == "search":
        if not args.query and not args.status:
            parser.error("search requires --query or --status")
        return run_search(client, args)
    parser.error("Unknown command")
    return 2


if __name__ == "__main__":
    sys.exit(main())
