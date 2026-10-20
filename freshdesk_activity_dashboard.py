#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import csv
import html
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
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import freshdesk_cli
import freshdesk_hubspot_sync


IST = timezone(timedelta(hours=5, minutes=30))


STATUS_LABELS = {
    2: "Open",
    3: "Pending",
    4: "Resolved",
    5: "Closed",
}
PRIORITY_LABELS = {
    1: "Low",
    2: "Medium",
    3: "High",
    4: "Urgent",
}
CLOSED_STATUSES = {4, 5}
DEFAULT_OUTPUT_DIR = "dashboard"
DEFAULT_REVERT_TAG = "customer_reverted"
DEFAULT_ALL_TICKETS_SINCE = "1970-01-01"
DEFAULT_RETRY_COUNT = 8
DEFAULT_RETRY_DELAY_SECONDS = 2.0
DEFAULT_CONVERSATION_WORKERS = 8
DEFAULT_HUBSPOT_ENV_FILE = "/Users/shishirraj/.config/hubspot.env"
DEFAULT_HUBSPOT_UI_DOMAIN = "app.hubspot.com"
DEFAULT_HUBSPOT_DEAL_URL_FIELD = "cf_hubspot_deal_url"
DEFAULT_HTTP_TIMEOUT_SECONDS = 120

TICKET_TYPE_ALIASES = {
    "Feature Request": "Feature Idea",
}

SLA_RULES = {
    "Bug": {
        "Urgent": {"ack_hours": 0.25, "resolution_hours": 1, "label": "Bug"},
        "High": {"ack_hours": 4, "resolution_hours": 24, "label": "Bug"},
        "Medium": {"ack_hours": 24, "resolution_hours": 96, "label": "Bug"},
        "Low": {"ack_hours": 72, "resolution_hours": 240, "label": "Bug"},
    },
    "Task - Backend": {
        "Urgent": {"ack_hours": 4, "resolution_hours": 24, "max_hours": 24, "label": "Backend Task"},
        "High": {"ack_hours": 24, "resolution_hours": 24, "max_hours": 48, "label": "Backend Task"},
        "Medium": {"ack_hours": 24, "resolution_hours": 48, "max_hours": 120, "label": "Backend Task"},
        "Low": {"ack_hours": 72, "resolution_hours": 168, "max_hours": 240, "label": "Backend Task"},
    },
    "Task - Experience Team": {
        "High": {"ack_hours": 6, "resolution_hours": 24, "max_hours": 48, "label": "Experience Task"},
        "Medium": {"ack_hours": 24, "resolution_hours": 120, "max_hours": 168, "label": "Experience Task"},
    },
    "Feature Idea": {
        "Medium": {"ack_hours": 24, "resolution_hours": 168, "label": "Feature Idea"},
        "Low": {"ack_hours": 24, "resolution_hours": 720, "label": "Feature Idea"},
    },
    "Feature Request": {
        "Medium": {"ack_hours": 24, "resolution_hours": 168, "label": "Feature Idea"},
        "Low": {"ack_hours": 24, "resolution_hours": 720, "label": "Feature Idea"},
    },
}


class HttpError(RuntimeError):
    pass


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


def env_as_bool(name: str, env_values: Dict[str, str], default: bool = False) -> bool:
    raw = env_or_file(name, env_values, "1" if default else "0").lower()
    return raw in {"1", "true", "yes", "y", "on"}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def parse_iso(value: object) -> Optional[datetime]:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def iso_or_blank(value: Optional[datetime]) -> str:
    return value.isoformat() if value else ""


def format_datetime_ist(value: object) -> str:
    parsed = parse_iso(value)
    if parsed is None:
        return ""
    return parsed.astimezone(IST).strftime("%d %b %Y, %I:%M %p IST")


def as_int(value: object) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def normalize_text(value: object) -> str:
    if not isinstance(value, str):
        return ""
    text = re.sub(r"<br\\s*/?>", "\n", value, flags=re.IGNORECASE)
    text = re.sub(r"</p>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = text.replace("\xa0", " ")
    text = re.sub(r"[ \t\r\f\v]+", " ", text)
    text = re.sub(r"\n+", "\n", text)
    return text.strip()


def preview_text(value: object, limit: int = 160) -> str:
    text = normalize_text(value)
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "…"


def csv_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, (list, tuple)):
        return ", ".join(str(item) for item in value)
    return str(value)


class FreshdeskClient:
    def __init__(
        self,
        domain: str,
        api_key: str,
        *,
        max_retries: int = DEFAULT_RETRY_COUNT,
        retry_delay_seconds: float = DEFAULT_RETRY_DELAY_SECONDS,
    ):
        self.base = f"https://{domain}/api/v2"
        token = base64.b64encode(f"{api_key}:X".encode("utf-8")).decode("ascii")
        self.auth_header = f"Basic {token}"
        self.max_retries = max_retries
        self.retry_delay_seconds = retry_delay_seconds

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
        self, method: str, path: str, payload: Optional[Dict[str, object]] = None
    ) -> Tuple[int, Optional[object], str]:
        url = urllib.parse.urljoin(f"{self.base}/", path.lstrip("/"))
        body = None
        headers = {
            "Accept": "application/json",
            "Authorization": self.auth_header,
        }
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(url, data=body, headers=headers, method=method)
        for attempt in range(self.max_retries + 1):
            try:
                with urllib.request.urlopen(req, timeout=DEFAULT_HTTP_TIMEOUT_SECONDS) as resp:
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
                should_retry = exc.code == 429 or 500 <= exc.code < 600
                if should_retry and attempt < self.max_retries:
                    retry_after = exc.headers.get("Retry-After") if exc.headers else ""
                    try:
                        delay = float(retry_after)
                    except (TypeError, ValueError):
                        delay = self.retry_delay_seconds * (attempt + 1)
                    time.sleep(max(delay, self.retry_delay_seconds))
                    continue
                return exc.code, parsed, raw
            except Exception as exc:
                if attempt < self.max_retries and self._is_transient_exception(exc):
                    time.sleep(self.retry_delay_seconds * (attempt + 1))
                    continue
                raise HttpError(f"Freshdesk request failed: {exc}") from exc

    def list_tickets(
        self,
        *,
        updated_since: str,
        page: int,
        per_page: int = 100,
    ) -> List[Dict[str, object]]:
        query = urllib.parse.urlencode(
            {
                "updated_since": updated_since,
                "page": page,
                "per_page": per_page,
                "include": "stats",
            }
        )
        code, data, raw = self.request("GET", f"tickets?{query}")
        if code != 200 or not isinstance(data, list):
            raise HttpError(
                "Freshdesk ticket list failed with HTTP {0}: {1}".format(code, raw[:400])
            )
        return [item for item in data if isinstance(item, dict)]

    def list_conversations(self, ticket_id: int) -> List[Dict[str, object]]:
        code, data, raw = self.request("GET", f"tickets/{ticket_id}/conversations")
        if code != 200 or not isinstance(data, list):
            raise HttpError(
                "Freshdesk conversations failed for ticket {0} with HTTP {1}: {2}".format(
                    ticket_id, code, raw[:400]
                )
            )
        return [item for item in data if isinstance(item, dict)]

    def get_status_choices(self) -> Dict[int, str]:
        code, data, raw = self.request("GET", "ticket_fields")
        if code != 200 or not isinstance(data, list):
            raise HttpError(
                "Freshdesk ticket_fields failed with HTTP {0}: {1}".format(code, raw[:400])
            )
        for item in data:
            if not isinstance(item, dict):
                continue
            if str(item.get("name") or "") != "status":
                continue
            choices = item.get("choices")
            if not isinstance(choices, dict):
                break
            mapped: Dict[int, str] = {}
            for key, value in choices.items():
                try:
                    status_value = int(key)
                except (TypeError, ValueError):
                    continue
                label = ""
                if isinstance(value, list) and value:
                    label = str(value[0] or "").strip()
                elif isinstance(value, str):
                    label = value.strip()
                if label:
                    mapped[status_value] = label
            if mapped:
                return mapped
            break
        return dict(STATUS_LABELS)

    def get_agent_name(self, agent_id: int) -> str:
        code, data, raw = self.request("GET", f"agents/{agent_id}")
        if code != 200 or not isinstance(data, dict):
            raise HttpError(
                "Freshdesk agent lookup failed for agent {0} with HTTP {1}: {2}".format(
                    agent_id, code, raw[:400]
                )
            )
        contact = data.get("contact") if isinstance(data.get("contact"), dict) else {}
        for key_source in (contact, data):
            value = str(key_source.get("name") or "").strip() if isinstance(key_source, dict) else ""
            if value:
                return value
        return f"Agent {agent_id}"

    def get_contact_company_id(self, contact_id: int) -> str:
        code, data, raw = self.request("GET", f"contacts/{contact_id}")
        if code != 200 or not isinstance(data, dict):
            raise HttpError(
                "Freshdesk contact lookup failed for contact {0} with HTTP {1}: {2}".format(
                    contact_id, code, raw[:400]
                )
            )
        company_id = data.get("company_id")
        if company_id in {None, "", 0}:
            return ""
        return str(company_id)

    def get_company_name(self, company_id: str) -> str:
        code, data, raw = self.request("GET", f"companies/{company_id}")
        if code != 200 or not isinstance(data, dict):
            raise HttpError(
                "Freshdesk company lookup failed for company {0} with HTTP {1}: {2}".format(
                    company_id, code, raw[:400]
                )
            )
        name = str(data.get("name") or "").strip()
        return name or f"Company {company_id}"

    def update_ticket(self, ticket_id: int, payload: Dict[str, object]) -> None:
        code, data, raw = self.request("PUT", f"tickets/{ticket_id}", payload)
        if code != 200:
            details = raw[:400]
            if isinstance(data, dict):
                details = json.dumps(data)
            raise HttpError(
                "Freshdesk ticket update failed for ticket {0} with HTTP {1}: {2}".format(
                    ticket_id, code, details
                )
            )


def status_label(status_value: object, status_choices: Dict[int, str]) -> str:
    numeric = as_int(status_value)
    if numeric is None:
        return "Unknown"
    return status_choices.get(numeric, STATUS_LABELS.get(numeric, f"Status {numeric}"))


def priority_label(priority_value: object) -> str:
    numeric = as_int(priority_value)
    if numeric is None:
        return ""
    return PRIORITY_LABELS.get(numeric, str(numeric))


def normalize_ticket_type(value: object) -> str:
    ticket_type = str(value or "").strip()
    return TICKET_TYPE_ALIASES.get(ticket_type, ticket_type)


def is_agent_id_label(value: object) -> bool:
    text = str(value or "").strip()
    return bool(re.fullmatch(r"Agent ID\s+\d+", text))


def is_highlightable_status(status_value: object, status_label_value: object = "") -> bool:
    numeric = as_int(status_value)
    if numeric in CLOSED_STATUSES:
        return False
    label = str(status_label_value or "").strip().lower()
    if "reject" in label:
        return False
    return True


def format_duration_hours(hours: Optional[float]) -> str:
    if hours is None:
        return "NA"
    if hours < 1:
        return f"{int(round(hours * 60))} min"
    if hours % 24 == 0:
        days = int(hours // 24)
        return f"{days} day" + ("s" if days != 1 else "")
    if float(hours).is_integer():
        return f"{int(hours)} hr"
    return f"{hours:.1f} hr"


def classify_sla_rule(ticket_type: str, priority_name: str) -> Dict[str, object]:
    ticket_type = normalize_ticket_type(ticket_type)
    type_rules = SLA_RULES.get(ticket_type, {})
    rule = type_rules.get(priority_name)
    if rule:
        return dict(rule)
    if ticket_type == "Task - Experience Team" and priority_name == "Urgent":
        return dict(type_rules.get("High", {}))
    if ticket_type == "Feature Idea" and priority_name in {"High", "Urgent"}:
        return {"ack_hours": 24, "label": "Feature Idea", "internal_review": True}
    if ticket_type == "Usability FI":
        return {"ack_hours": 24, "resolution_hours": 168, "label": "Feature Idea"}
    if ticket_type == "Service Request":
        return {"ack_hours": 24, "resolution_hours": 168, "label": "Task"}
    return {"ack_hours": 24, "label": ticket_type or "Other"}


def evaluate_deadline(
    *,
    start_at: Optional[datetime],
    end_at: Optional[datetime],
    target_hours: Optional[float],
) -> Tuple[str, str, Optional[float], str]:
    if start_at is None or target_hours is None:
        return "na", "NA", None, "NA"
    deadline = start_at + timedelta(hours=target_hours)
    reference = end_at or utc_now()
    delta_hours = round((reference - deadline).total_seconds() / 3600, 1)
    if end_at is not None:
        if end_at <= deadline:
            return "met", "Met", max(delta_hours, 0), deadline.isoformat()
        return "breached", "Breached", delta_hours, deadline.isoformat()
    if utc_now() > deadline:
        return "breached", "Breached", delta_hours, deadline.isoformat()
    remaining = round((deadline - utc_now()).total_seconds() / 3600, 1)
    if target_hours > 0 and remaining <= max(target_hours * 0.2, 1):
        return "at_risk", "At Risk", remaining, deadline.isoformat()
    return "open", "Open", remaining, deadline.isoformat()


def build_sla_metrics(ticket: Dict[str, object], row: Dict[str, object]) -> Dict[str, object]:
    priority_name = row.get("priority_label") or ""
    ticket_type = row.get("type") or ""
    status_value = ticket.get("status")
    status_label_value = str(row.get("status_label") or "")
    highlight_enabled = is_highlightable_status(status_value, status_label_value)
    rule = classify_sla_rule(ticket_type, priority_name)
    stats = ticket.get("stats") if isinstance(ticket.get("stats"), dict) else {}
    created_at = parse_iso(ticket.get("created_at"))
    first_responded_at = parse_iso(stats.get("first_responded_at")) if stats else None
    resolved_at = parse_iso(stats.get("resolved_at")) if stats else None
    closed_at = parse_iso(stats.get("closed_at")) if stats else None
    completion_at = closed_at or resolved_at

    ack_state, ack_label, ack_delta, ack_deadline = evaluate_deadline(
        start_at=created_at,
        end_at=first_responded_at,
        target_hours=rule.get("ack_hours"),
    )
    resolution_state, resolution_label, resolution_delta, resolution_deadline = evaluate_deadline(
        start_at=created_at,
        end_at=completion_at,
        target_hours=rule.get("resolution_hours"),
    )
    max_state, max_label, max_delta, max_deadline = evaluate_deadline(
        start_at=created_at,
        end_at=completion_at,
        target_hours=rule.get("max_hours"),
    )

    if not highlight_enabled:
        ack_state, ack_label, ack_delta, ack_deadline = "na", "NA", None, "NA"
        resolution_state, resolution_label, resolution_delta, resolution_deadline = "na", "NA", None, "NA"
        max_state, max_label, max_delta, max_deadline = "na", "NA", None, "NA"

    red_flag = highlight_enabled and (
        ack_state == "breached" or resolution_state == "breached" or max_state == "breached"
    )
    amber_flag = highlight_enabled and (not red_flag) and (
        ack_state == "at_risk" or resolution_state == "at_risk" or max_state == "at_risk"
    )

    owner_team = "Technical Team"
    if ticket_type == "Task - Experience Team":
        owner_team = "Experience Team"

    if status_label_value == "Pending":
        if ticket_type == "Task - Experience Team":
            owner_team = "Shabib"
        elif ticket_type in {"Bug", "Task - Backend"}:
            owner_team = "Customer Delight"

    sop_action = "Closed" if not highlight_enabled else "Monitor"
    if highlight_enabled and max_state == "breached":
        sop_action = "Escalate internally"
    elif highlight_enabled and (resolution_state == "breached" or ack_state == "breached"):
        sop_action = "Red flag and communicate delay"
    elif highlight_enabled and "pending with client" in status_label_value.lower():
        sop_action = "Send follow-up email"
    elif highlight_enabled and rule.get("internal_review"):
        sop_action = "Internal feasibility review"

    return {
        "sla_category": rule.get("label") or ticket_type or "Other",
        "owner_team": owner_team,
        "ack_sla": format_duration_hours(rule.get("ack_hours")),
        "resolution_sla": format_duration_hours(rule.get("resolution_hours")),
        "max_timeline_sla": format_duration_hours(rule.get("max_hours")),
        "ack_state": ack_state,
        "ack_label": ack_label,
        "ack_deadline_at": ack_deadline,
        "ack_delta_hours": ack_delta,
        "resolution_state": resolution_state,
        "resolution_label": resolution_label,
        "resolution_deadline_at": resolution_deadline,
        "resolution_delta_hours": resolution_delta,
        "max_timeline_state": max_state,
        "max_timeline_label": max_label,
        "max_timeline_deadline_at": max_deadline,
        "max_timeline_delta_hours": max_delta,
        "first_responded_at": iso_or_blank(first_responded_at),
        "resolved_at": iso_or_blank(resolved_at),
        "closed_at": iso_or_blank(closed_at),
        "first_responded_at_ist": format_datetime_ist(first_responded_at),
        "resolved_at_ist": format_datetime_ist(resolved_at),
        "closed_at_ist": format_datetime_ist(closed_at),
        "sla_red_flag": red_flag,
        "sla_amber_flag": amber_flag,
        "sop_action": sop_action,
    }


def actor_label(conversation: Dict[str, object]) -> str:
    if bool(conversation.get("private")):
        return "internal"
    if bool(conversation.get("incoming")):
        return "customer"
    return "agent"


def actor_name(conversation: Dict[str, object]) -> str:
    for key in ("user_name", "from_name", "name"):
        value = str(conversation.get(key) or "").strip()
        if value:
            return value
    for key in ("from_email", "support_email"):
        value = str(conversation.get(key) or "").strip()
        if value:
            return value
    actor = actor_label(conversation)
    if actor == "customer":
        return "Customer"
    if actor == "internal":
        return "Internal Note"
    return "Agent"


def classify_last_action(row: Dict[str, object], previous_row: Optional[Dict[str, object]] = None) -> Tuple[str, str]:
    last_type = str(row.get("last_activity_type") or "").strip().lower()
    preview = normalize_text(row.get("last_activity_preview") or "")
    preview_lower = preview.lower()

    if previous_row:
        previous_status = str(previous_row.get("status_label") or "").strip()
        current_status = str(row.get("status_label") or "").strip()
        if previous_status and current_status and previous_status != current_status:
            return f"Stage changed: {previous_status} → {current_status}", "Workflow stage updated"

        previous_priority = str(previous_row.get("priority_label") or "").strip()
        current_priority = str(row.get("priority_label") or "").strip()
        if previous_priority and current_priority and previous_priority != current_priority:
            return f"Priority changed: {previous_priority} → {current_priority}", "Ticket priority updated"

    if last_type == "internal":
        return "Note added", preview or "Internal note added"
    if last_type == "customer":
        return "Customer replied", preview or "Customer replied on ticket"
    if last_type == "agent":
        delay_terms = ["delay", "apolog", "follow-up", "follow up", "sincerely apologize", "technical team is working"]
        if any(term in preview_lower for term in delay_terms):
            return "Delay email sent to customer", preview or "Delay update shared with customer"
        return "Email sent to customer", preview or "Agent reply sent to customer"
    if last_type == "ticket_update":
        return "Ticket updated", preview or "Ticket properties updated"
    return "Activity updated", preview or "Latest activity captured"


def build_activity_metrics(
    ticket: Dict[str, object], conversations: Sequence[Dict[str, object]], reply_sla_hours: int
) -> Dict[str, object]:
    sorted_conversations = sorted(
        conversations,
        key=lambda item: parse_iso(item.get("created_at")) or datetime.min.replace(tzinfo=timezone.utc),
    )
    timeline: List[Dict[str, str]] = []
    last_customer_reply_at: Optional[datetime] = None
    last_agent_reply_at: Optional[datetime] = None
    last_public_reply_at: Optional[datetime] = None
    last_public_reply_actor = ""
    last_activity_user_name = ""
    public_reply_count = 0
    internal_note_count = 0
    customer_reply_count = 0
    agent_reply_count = 0
    customer_reverted_ever = False
    replied_after_customer_revert = False
    last_customer_revert_at: Optional[datetime] = None
    conversation_checked = len(conversations) > 0
    saw_agent_public_reply = False

    for item in sorted_conversations:
        created_at = parse_iso(item.get("created_at"))
        actor = actor_label(item)
        item_actor_name = actor_name(item)
        private = bool(item.get("private"))
        preview = preview_text(item.get("body_text") or item.get("body"))
        timeline.append(
            {
                "created_at": item.get("created_at") or "",
                "actor": actor,
                "actor_name": item_actor_name,
                "private": "yes" if private else "no",
                "preview": preview,
            }
        )
        if private:
            internal_note_count += 1
            continue
        public_reply_count += 1
        if created_at:
            last_public_reply_at = created_at
            last_public_reply_actor = actor
        if actor == "customer":
            customer_reply_count += 1
            if saw_agent_public_reply and created_at:
                customer_reverted_ever = True
                last_customer_revert_at = created_at
                replied_after_customer_revert = False
            if created_at:
                last_customer_reply_at = created_at
        elif actor == "agent":
            agent_reply_count += 1
            saw_agent_public_reply = True
            if created_at:
                last_agent_reply_at = created_at
                if last_customer_revert_at and created_at > last_customer_revert_at:
                    replied_after_customer_revert = True

    ticket_updated_at = parse_iso(ticket.get("updated_at"))
    timeline_last_at = parse_iso(timeline[-1]["created_at"]) if timeline else None
    last_activity_at = ticket_updated_at or timeline_last_at
    last_activity_type = "ticket_update"
    last_activity_preview = "Ticket updated"
    last_activity_user_name = str(ticket.get("_responder_name") or "").strip() or f"Agent ID {as_int(ticket.get('responder_id')) or '-'}"
    if timeline_last_at and ((ticket_updated_at is None) or timeline_last_at >= ticket_updated_at):
        last_activity_at = timeline_last_at
        last_activity_type = timeline[-1]["actor"]
        last_activity_user_name = timeline[-1].get("actor_name") or last_activity_user_name
        last_activity_preview = timeline[-1]["preview"] or "Conversation updated"

    unresolved = is_highlightable_status(ticket.get("status"))
    customer_reverted = bool(
        unresolved
        and last_customer_reply_at
        and (last_agent_reply_at is None or last_customer_reply_at > last_agent_reply_at)
    )
    reply_age_hours: Optional[float] = None
    if customer_reverted and last_customer_reply_at:
        reply_age_hours = round((utc_now() - last_customer_reply_at).total_seconds() / 3600, 1)
    reply_highlight = "ok"
    if customer_reverted:
        reply_highlight = "overdue" if (reply_age_hours or 0) >= reply_sla_hours else "waiting"

    return {
        "public_reply_count": public_reply_count,
        "internal_note_count": internal_note_count,
        "customer_reply_count": customer_reply_count,
        "agent_reply_count": agent_reply_count,
        "conversation_checked": conversation_checked,
        "customer_reverted_ever": customer_reverted_ever,
        "replied_after_customer_revert": replied_after_customer_revert,
        "last_customer_revert_at": iso_or_blank(last_customer_revert_at),
        "last_customer_reply_at": iso_or_blank(last_customer_reply_at),
        "last_agent_reply_at": iso_or_blank(last_agent_reply_at),
        "last_public_reply_at": iso_or_blank(last_public_reply_at),
        "last_public_reply_actor": last_public_reply_actor,
        "customer_reverted": customer_reverted,
        "reply_age_hours": reply_age_hours,
        "reply_highlight": reply_highlight,
        "last_activity_at": iso_or_blank(last_activity_at),
        "last_activity_at_ist": format_datetime_ist(last_activity_at),
        "last_activity_type": last_activity_type,
        "last_activity_user_name": last_activity_user_name,
        "last_activity_preview": last_activity_preview,
        "timeline": timeline[-5:],
        "timeline_excerpt": " | ".join(
            f"{entry['actor_name']} ({entry['actor']}) @ {entry['created_at']}: {entry['preview']}"
            for entry in timeline[-3:]
            if entry["created_at"]
        ),
    }


def load_activity_cache(path: Path) -> Dict[str, Dict[str, object]]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(payload, dict):
        return {}
    tickets = payload.get("tickets")
    return tickets if isinstance(tickets, dict) else {}


def load_agent_cache(path: Path) -> Dict[str, str]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    agents = payload.get("agents") if isinstance(payload, dict) else None
    if not isinstance(agents, dict):
        return {}
    return {str(key): str(value) for key, value in agents.items() if str(value).strip()}


def load_string_cache(path: Path, key: str) -> Dict[str, str]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    values = payload.get(key) if isinstance(payload, dict) else None
    if not isinstance(values, dict):
        return {}
    return {str(cache_key): str(cache_value) for cache_key, cache_value in values.items()}


def build_hubspot_deal_url(ui_domain: str, portal_id: str, deal_id: str) -> str:
    portal = portal_id.strip()
    identifier = deal_id.strip()
    domain = ui_domain.strip() or DEFAULT_HUBSPOT_UI_DOMAIN
    if not portal or not identifier:
        return ""
    return f"https://{domain}/contacts/{portal}/record/0-3/{identifier}"


def fetch_hubspot_deal_stages(
    deal_urls_by_ticket: Dict[int, str],
    hubspot_client: Optional[freshdesk_hubspot_sync.HubSpotClient],
    fetch_errors: List[str],
) -> Dict[int, Dict[str, str]]:
    """Read the current stage of each ticket's displayed primary deal once per build."""
    deal_ids_by_ticket: Dict[int, str] = {}
    for ticket_id, url in deal_urls_by_ticket.items():
        path = urllib.parse.urlparse(url).path
        match = re.search(r"/(?:record/0-3|deal)/(\d+)(?:/|$)", path)
        if match:
            deal_ids_by_ticket[ticket_id] = match.group(1)
    if hubspot_client is None or not deal_ids_by_ticket:
        return {}

    stage_labels: Dict[Tuple[str, str], str] = {}
    try:
        code, data, _ = hubspot_client.request("GET", "/crm/v3/pipelines/deals")
        if code == 200 and isinstance(data, dict):
            for pipeline in data.get("results") or []:
                pipeline_id = str(pipeline.get("id") or "")
                pipeline_label = str(pipeline.get("label") or pipeline_id)
                for stage in pipeline.get("stages") or []:
                    stage_id = str(stage.get("id") or "")
                    label = str(stage.get("label") or stage_id)
                    stage_labels[(pipeline_id, stage_id)] = f"{pipeline_label} · {label}"
        else:
            fetch_errors.append(f"hubspot deal stage labels: HTTP {code}")
    except (OSError, ValueError, freshdesk_hubspot_sync.HttpError) as exc:
        fetch_errors.append(f"hubspot deal stage labels: {exc}")

    stages_by_deal: Dict[str, Dict[str, str]] = {}
    deal_ids = sorted(set(deal_ids_by_ticket.values()))
    for offset in range(0, len(deal_ids), 100):
        batch = deal_ids[offset:offset + 100]
        try:
            code, data, _ = hubspot_client.request(
                "POST", "/crm/v3/objects/deals/batch/read",
                {"properties": ["dealstage", "pipeline"], "inputs": [{"id": deal_id} for deal_id in batch]},
            )
            if code not in (200, 207) or not isinstance(data, dict):
                fetch_errors.append(f"hubspot deal stages batch {offset // 100 + 1}: HTTP {code}")
                continue
            for deal in data.get("results") or []:
                properties = deal.get("properties") or {}
                stage_id = str(properties.get("dealstage") or "").strip()
                pipeline_id = str(properties.get("pipeline") or "").strip()
                if stage_id:
                    stages_by_deal[str(deal.get("id"))] = {
                        "hubspot_deal_stage": f"{pipeline_id}:{stage_id}",
                        "hubspot_deal_stage_label": stage_labels.get((pipeline_id, stage_id), f"Stage {stage_id}"),
                    }
            if data.get("errors"):
                fetch_errors.append(f"hubspot deal stages batch {offset // 100 + 1}: some deals could not be read")
        except (OSError, ValueError, freshdesk_hubspot_sync.HttpError) as exc:
            fetch_errors.append(f"hubspot deal stages batch {offset // 100 + 1}: {exc}")
    return {
        ticket_id: stages_by_deal[deal_id]
        for ticket_id, deal_id in deal_ids_by_ticket.items()
        if deal_id in stages_by_deal
    }


def get_ticket_custom_field_text(ticket: Dict[str, object], field_name: str) -> str:
    custom_fields = ticket.get("custom_fields") if isinstance(ticket.get("custom_fields"), dict) else {}
    return str((custom_fields or {}).get(field_name) or "").strip()


def freshdesk_company_id_for_ticket(ticket: Dict[str, object], requester_company_ids: Dict[int, str]) -> str:
    company_id = ticket.get("company_id")
    if company_id not in (None, "", 0):
        return str(company_id)
    return requester_company_ids.get(as_int(ticket.get("requester_id")) or 0, "")


def ticket_external_company_id(ticket: Dict[str, object]) -> str:
    for field in ("cf_company_id880506", "cf_company_id"):
        value = get_ticket_custom_field_text(ticket, field)
        if re.fullmatch(r"[0-9]+|[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}", value):
            return value.lower()
    return ""


def resolve_hubspot_companies_by_external_id(
    tickets: Sequence[Dict[str, object]],
    hubspot_client: Optional[freshdesk_hubspot_sync.HubSpotClient],
    fetch_errors: List[str],
) -> Dict[str, Dict[str, object]]:
    """Resolve ticket company IDs through exact matches on deals and their companies."""
    if hubspot_client is None:
        return {}
    external_ids = sorted({ticket_external_company_id(ticket) for ticket in tickets} - {""})
    deals: Dict[str, Dict[str, object]] = {}

    def read_batch(path: str, identifiers: Sequence[str], properties: Optional[List[str]] = None) -> List[Dict]:
        results: List[Dict] = []
        for offset in range(0, len(identifiers), 100):
            payload: Dict[str, object] = {"inputs": [{"id": value} for value in identifiers[offset:offset + 100]]}
            if properties is not None:
                payload["properties"] = properties
            try:
                code, data, _ = hubspot_client.request("POST", path, payload)
                if code not in (200, 207) or not isinstance(data, dict):
                    fetch_errors.append(f"hubspot company ID resolution {path}: HTTP {code}")
                    continue
                results.extend(data.get("results") or [])
                if data.get("errors"):
                    fetch_errors.append(f"hubspot company ID resolution {path}: partial response")
            except (OSError, ValueError, freshdesk_hubspot_sync.HttpError) as exc:
                fetch_errors.append(f"hubspot company ID resolution {path}: {exc}")
        return results

    for numeric, property_name in ((False, "deal_companyid_v2"), (True, "deal_companyid")):
        identifiers = [value for value in external_ids if value.isdigit() == numeric]
        for offset in range(0, len(identifiers), 50):
            values = identifiers[offset:offset + 50]
            payload = {
                "filterGroups": [{"filters": [{"propertyName": property_name, "operator": "IN", "values": values}]}],
                "properties": [property_name, "hs_lastmodifieddate"], "limit": 100,
            }
            seen_pages = set()
            while True:
                try:
                    code, data, _ = hubspot_client.request("POST", "/crm/v3/objects/deals/search", payload)
                    if code != 200 or not isinstance(data, dict):
                        fetch_errors.append(f"hubspot exact company ID search: HTTP {code}")
                        break
                    for deal in data.get("results") or []:
                        external_id = str((deal.get("properties") or {}).get(property_name) or "").lower()
                        if external_id in values:
                            deals[str(deal["id"])] = {"external_id": external_id, "modified_at": str((deal.get("properties") or {}).get("hs_lastmodifieddate") or "")}
                    after = ((data.get("paging") or {}).get("next") or {}).get("after")
                    if after is None or str(after) in seen_pages:
                        break
                    seen_pages.add(str(after))
                    payload["after"] = after
                except (OSError, ValueError, freshdesk_hubspot_sync.HttpError) as exc:
                    fetch_errors.append(f"hubspot exact company ID search: {exc}")
                    break

    associations = read_batch("/crm/v3/associations/deals/companies/batch/read", sorted(deals))
    companies_by_deal: Dict[str, set] = {}
    for association in associations:
        deal_id = str((association.get("from") or {}).get("id") or "")
        companies_by_deal.setdefault(deal_id, set()).update(str(item["id"]) for item in association.get("to") or [] if item.get("id"))
    company_ids = sorted({company_id for values in companies_by_deal.values() for company_id in values})
    company_names = {
        str(company["id"]): str((company.get("properties") or {}).get("name") or "").strip()
        for company in read_batch("/crm/v3/objects/companies/batch/read", company_ids, ["name"])
    }
    contexts: Dict[str, Dict[str, object]] = {}
    for external_id in external_ids:
        matched_deals = [deal_id for deal_id, deal in deals.items() if deal["external_id"] == external_id]
        associated_ids = {company_id for deal_id in matched_deals for company_id in companies_by_deal.get(deal_id, set())}
        # Avoid guessing when a company ID points at multiple associated companies.
        if len(associated_ids) != 1:
            if len(associated_ids) > 1:
                fetch_errors.append(f"hubspot company ID {external_id}: ambiguous company associations")
            continue
        company_name = company_names.get(next(iter(associated_ids)), "")
        if not company_name:
            continue
        matched_deals.sort(key=lambda deal_id: (deals[deal_id]["modified_at"], deal_id), reverse=True)
        contexts[external_id] = {"name": company_name, "deal_ids": matched_deals}
    return contexts


def sync_hubspot_deal_url_field(
    client: FreshdeskClient,
    ticket_id: int,
    field_name: str,
    desired_url: str,
    current_url: str,
) -> bool:
    desired = str(desired_url or "").strip()
    current = str(current_url or "").strip()
    if not desired or desired == current:
        return False
    client.update_ticket(ticket_id, {"custom_fields": {field_name: desired}})
    return True


def normalize_company_lookup_name(value: object) -> str:
    return " ".join(str(value or "").split()).strip().lower()


def choose_best_hubspot_company_match(
    freshdesk_company_name: str,
    matches: Sequence[Dict[str, object]],
) -> Optional[Dict[str, object]]:
    normalized_target = normalize_company_lookup_name(freshdesk_company_name)
    exact_matches: List[Dict[str, object]] = []
    loose_matches: List[Dict[str, object]] = []
    for item in matches:
        properties = item.get("properties") if isinstance(item.get("properties"), dict) else {}
        candidate_name = str((properties or {}).get("name") or "").strip()
        normalized_candidate = normalize_company_lookup_name(candidate_name)
        if not normalized_candidate:
            continue
        if normalized_candidate == normalized_target:
            exact_matches.append(item)
        elif normalized_target and (
            normalized_target in normalized_candidate or normalized_candidate in normalized_target
        ):
            loose_matches.append(item)
    if len(exact_matches) == 1:
        return exact_matches[0]
    if exact_matches:
        return exact_matches[0]
    if len(loose_matches) == 1:
        return loose_matches[0]
    if len(matches) == 1:
        return matches[0]
    return loose_matches[0] if loose_matches else None


def enrich_hubspot_context(
    *,
    client: FreshdeskClient,
    tickets: Sequence[Dict[str, object]],
    ticket_map: Dict[str, Dict],
    output_dir: Path,
    hubspot_client: Optional[freshdesk_hubspot_sync.HubSpotClient],
    hubspot_ui_domain: str,
    hubspot_portal_id: str,
    hubspot_deal_url_field: str,
    fetch_errors: List[str],
) -> Tuple[Dict[int, str], Dict[int, str], Dict[int, int], Dict[str, str]]:
    contact_company_cache_path = output_dir / ".hubspot_contact_company_cache.json"
    contact_company_cache = load_string_cache(contact_company_cache_path, "contact_companies")
    direct_company_cache_path = output_dir / ".hubspot_company_deal_cache.json"
    direct_company_cache = load_string_cache(direct_company_cache_path, "company_deals")

    company_names_by_ticket: Dict[int, str] = {}
    primary_deal_url_by_ticket: Dict[int, str] = {}
    deal_count_by_ticket: Dict[int, int] = {}

    contact_ids = sorted(
        {
            str((ticket_map.get(str(as_int(ticket.get("id")) or 0)) or {}).get("hubspot_contact_id") or "").strip()
            for ticket in tickets
            if as_int(ticket.get("id"))
        }
        - {""}
    )
    hubspot_contact_companies: Dict[str, str] = {}
    for contact_id in contact_ids:
        cached_company_name = contact_company_cache.get(contact_id, "").strip()
        if cached_company_name:
            hubspot_contact_companies[contact_id] = cached_company_name
            continue
        if hubspot_client is None:
            continue
        try:
            contact = hubspot_client.get_contact(contact_id, properties=["company"])
            properties = contact.get("properties") if isinstance(contact.get("properties"), dict) else {}
            company_name = str((properties or {}).get("company") or "").strip()
            if company_name:
                hubspot_contact_companies[contact_id] = company_name
                contact_company_cache[contact_id] = company_name
        except freshdesk_hubspot_sync.HttpError as exc:
            fetch_errors.append(f"hubspot contact {contact_id}: {exc}")

    for ticket in tickets:
        ticket_id = as_int(ticket.get("id")) or 0
        if ticket_id <= 0:
            continue
        field_url = get_ticket_custom_field_text(ticket, hubspot_deal_url_field)
        if field_url:
            primary_deal_url_by_ticket[ticket_id] = field_url
            deal_count_by_ticket[ticket_id] = max(1, deal_count_by_ticket.get(ticket_id, 0))
        entry = ticket_map.get(str(ticket_id)) or {}
        contact_id = str(entry.get("hubspot_contact_id") or "").strip()
        hubspot_company_name = hubspot_contact_companies.get(contact_id, "").strip()
        if hubspot_company_name:
            company_names_by_ticket[ticket_id] = hubspot_company_name

        deal_ids = [
            str(value).strip()
            for value in (entry.get("hubspot_deal_ids") or [])
            if str(value).strip()
        ]
        if not deal_ids:
            continue
        deal_count_by_ticket[ticket_id] = len(deal_ids)
        primary_url = build_hubspot_deal_url(hubspot_ui_domain, hubspot_portal_id, deal_ids[0])
        if primary_url:
            primary_deal_url_by_ticket[ticket_id] = primary_url

    tickets_missing_company = [
        ticket for ticket in tickets
        if not company_names_by_ticket.get(as_int(ticket.get("id")) or 0)
        and str(ticket.get("_freshdesk_company_name") or "").strip().lower() in {"", "unknown"}
    ]
    external_company_contexts = resolve_hubspot_companies_by_external_id(
        tickets_missing_company, hubspot_client, fetch_errors
    )
    for ticket in tickets_missing_company:
        ticket_id = as_int(ticket.get("id")) or 0
        context = external_company_contexts.get(ticket_external_company_id(ticket))
        if ticket_id <= 0 or context is None:
            continue
        company_names_by_ticket[ticket_id] = str(context["name"])
        deal_ids = context["deal_ids"]
        if deal_ids and not primary_deal_url_by_ticket.get(ticket_id):
            url = build_hubspot_deal_url(hubspot_ui_domain, hubspot_portal_id, str(deal_ids[0]))
            if url:
                primary_deal_url_by_ticket[ticket_id] = url
                deal_count_by_ticket[ticket_id] = len(deal_ids)

    company_names_missing_deals = sorted(
        {
            str((ticket.get("_freshdesk_company_name") or ticket.get("_company_name") or "")).strip()
            for ticket in tickets
            if as_int(ticket.get("id"))
            and not primary_deal_url_by_ticket.get(as_int(ticket.get("id")) or 0)
            and str((ticket.get("_freshdesk_company_name") or ticket.get("_company_name") or "")).strip()
            and str((ticket.get("_freshdesk_company_name") or ticket.get("_company_name") or "")).strip().lower() != "unknown"
        }
    )
    direct_lookup_results: Dict[str, Tuple[str, str, int]] = {}
    for index, freshdesk_company_name in enumerate(company_names_missing_deals, start=1):
        if index == 1 or index % 25 == 0 or index == len(company_names_missing_deals):
            print(
                f"HUBSPOT company lookup {index}/{len(company_names_missing_deals)} name={freshdesk_company_name}",
                file=sys.stderr,
            )
        cache_key = normalize_company_lookup_name(freshdesk_company_name)
        cached_value = direct_company_cache.get(cache_key, "")
        cached_parts = [part.strip() for part in cached_value.split("|")]
        if len(cached_parts) == 3:
            cached_name, cached_url, cached_count = cached_parts
            try:
                parsed_count = int(cached_count or "0")
                if cached_name or cached_url or parsed_count > 0:
                    direct_lookup_results[cache_key] = (cached_name, cached_url, parsed_count)
                    continue
            except ValueError:
                pass
        if hubspot_client is None:
            continue
        try:
            matches = hubspot_client.search_companies(freshdesk_company_name, limit=10)
            matched_company = choose_best_hubspot_company_match(freshdesk_company_name, matches)
            if matched_company is None:
                direct_lookup_results[cache_key] = ("", "", 0)
                continue
            hubspot_company_id = str(matched_company.get("id") or "").strip()
            properties = matched_company.get("properties") if isinstance(matched_company.get("properties"), dict) else {}
            resolved_company_name = str((properties or {}).get("name") or "").strip()
            associated_deal_ids = (
                hubspot_client.list_associated_object_ids("companies", hubspot_company_id, "deals")
                if hubspot_company_id
                else []
            )
            best_deal_id = str(associated_deal_ids[0] if associated_deal_ids else "").strip()
            best_deal_url = build_hubspot_deal_url(hubspot_ui_domain, hubspot_portal_id, best_deal_id)
            direct_lookup_results[cache_key] = (
                resolved_company_name,
                best_deal_url,
                len(associated_deal_ids),
            )
            if resolved_company_name or best_deal_url or associated_deal_ids:
                direct_company_cache[cache_key] = (
                    f"{resolved_company_name}|{best_deal_url}|{len(associated_deal_ids)}"
                )
        except freshdesk_hubspot_sync.HttpError as exc:
            fetch_errors.append(f"hubspot company {freshdesk_company_name}: {exc}")

    synced_ticket_count = 0
    for ticket in tickets:
        ticket_id = as_int(ticket.get("id")) or 0
        if ticket_id <= 0:
            continue
        freshdesk_company_name = str((ticket.get("_freshdesk_company_name") or ticket.get("_company_name") or "")).strip()
        if not freshdesk_company_name or freshdesk_company_name.lower() == "unknown":
            continue
        cache_key = normalize_company_lookup_name(freshdesk_company_name)
        resolved_company_name, best_deal_url, deal_count = direct_lookup_results.get(cache_key, ("", "", 0))
        if resolved_company_name and not company_names_by_ticket.get(ticket_id):
            company_names_by_ticket[ticket_id] = resolved_company_name
        if best_deal_url and not primary_deal_url_by_ticket.get(ticket_id):
            primary_deal_url_by_ticket[ticket_id] = best_deal_url
        if deal_count and not deal_count_by_ticket.get(ticket_id):
            deal_count_by_ticket[ticket_id] = deal_count
        current_field_url = get_ticket_custom_field_text(ticket, hubspot_deal_url_field)
        if best_deal_url:
            try:
                if sync_hubspot_deal_url_field(
                    client,
                    ticket_id,
                    hubspot_deal_url_field,
                    best_deal_url,
                    current_field_url,
                ):
                    synced_ticket_count += 1
                    if synced_ticket_count == 1 or synced_ticket_count % 50 == 0:
                        print(
                            f"SYNC hubspot_deal_url ticket={ticket_id} updated={synced_ticket_count}",
                            file=sys.stderr,
                        )
                    custom_fields = ticket.get("custom_fields") if isinstance(ticket.get("custom_fields"), dict) else {}
                    ticket["custom_fields"] = dict(custom_fields or {})
                    ticket["custom_fields"][hubspot_deal_url_field] = best_deal_url
            except HttpError as exc:
                fetch_errors.append(f"ticket {ticket_id} hubspot_deal_url sync failed: {exc}")

    write_json(
        direct_company_cache_path,
        {"generated_at": utc_now_iso(), "company_deals": direct_company_cache},
    )

    return company_names_by_ticket, primary_deal_url_by_ticket, deal_count_by_ticket, contact_company_cache


def cached_activity_metrics(
    cache: Dict[str, Dict[str, object]], ticket_id: int, ticket_updated_at: str
) -> Optional[Dict[str, object]]:
    cached = cache.get(str(ticket_id))
    if not cached:
        return None
    if str(cached.get("ticket_updated_at") or "") != ticket_updated_at:
        return None
    metrics = cached.get("metrics")
    return metrics if isinstance(metrics, dict) else None


def save_activity_metrics(
    cache: Dict[str, Dict[str, object]],
    ticket_id: int,
    ticket_updated_at: str,
    metrics: Dict[str, object],
) -> None:
    cache[str(ticket_id)] = {
        "ticket_updated_at": ticket_updated_at,
        "metrics": metrics,
    }


def build_row(
    ticket: Dict[str, object],
    conversations: Sequence[Dict[str, object]],
    *,
    reply_sla_hours: int,
    revert_tag: str,
    status_choices: Dict[int, str],
    activity_metrics: Optional[Dict[str, object]] = None,
    previous_row: Optional[Dict[str, object]] = None,
) -> Dict[str, object]:
    metrics = activity_metrics or build_activity_metrics(ticket, conversations, reply_sla_hours)
    tags = [str(item).strip() for item in (ticket.get("tags") or []) if str(item).strip()]
    normalized_tags = {item.lower() for item in tags}
    has_revert_tag = revert_tag.lower() in normalized_tags
    row = {
        "ticket_id": as_int(ticket.get("id")) or 0,
        "subject": str(ticket.get("subject") or "").strip(),
        "status": as_int(ticket.get("status")) or 0,
        "status_label": status_label(ticket.get("status"), status_choices),
        "priority": as_int(ticket.get("priority")) or 0,
        "priority_label": priority_label(ticket.get("priority")),
        "type": normalize_ticket_type(ticket.get("type")),
        "freshdesk_company_name": str(ticket.get("_freshdesk_company_name") or "").strip() or "Unknown",
        "company_external_id": ticket_external_company_id(ticket),
        "company_name": str(ticket.get("_company_name") or "").strip() or "Unknown",
        "hubspot_deal_url": str(ticket.get("_hubspot_deal_url") or "").strip(),
        "hubspot_deal_count": as_int(ticket.get("_hubspot_deal_count")) or 0,
        "hubspot_deal_stage": str(ticket.get("_hubspot_deal_stage") or ""),
        "hubspot_deal_stage_label": str(ticket.get("_hubspot_deal_stage_label") or ""),
        "source": as_int(ticket.get("source")) or 0,
        "created_at": str(ticket.get("created_at") or ""),
        "created_at_ist": format_datetime_ist(ticket.get("created_at")),
        "updated_at": str(ticket.get("updated_at") or ""),
        "updated_at_ist": format_datetime_ist(ticket.get("updated_at")),
        "due_by": str(ticket.get("due_by") or ""),
        "fr_due_by": str(ticket.get("fr_due_by") or ""),
        "responder_id": as_int(ticket.get("responder_id")) or 0,
        "responder_name": str(ticket.get("_responder_name") or "").strip(),
        "group_id": as_int(ticket.get("group_id")) or 0,
        "requester_id": as_int(ticket.get("requester_id")) or 0,
        "first_responded_at": "",
        "resolved_at": "",
        "closed_at": "",
        "first_responded_at_ist": "",
        "resolved_at_ist": "",
        "closed_at_ist": "",
        "conversation_checked": False,
        "customer_reverted_ever": False,
        "replied_after_customer_revert": False,
        "last_customer_revert_at": "",
        "last_activity_at_ist": "",
        "last_activity_user_name": "",
        "tags": tags,
        "tags_display": ", ".join(tags),
        "has_customer_revert_tag": has_revert_tag,
        "should_add_revert_tag": bool(metrics["customer_reverted"] and not has_revert_tag),
        "should_remove_revert_tag": bool((not metrics["customer_reverted"]) and has_revert_tag),
    }
    row.update(metrics)
    row.update(build_sla_metrics(ticket, row))
    previous_responder_name = str((previous_row or {}).get("responder_name") or "").strip()
    if not row["responder_name"] and previous_responder_name:
        row["responder_name"] = previous_responder_name
    previous_last_actor = str((previous_row or {}).get("last_activity_user_name") or "").strip()
    if not row["responder_name"] and previous_last_actor and not is_agent_id_label(previous_last_actor):
        row["responder_name"] = previous_last_actor
    if is_agent_id_label(row.get("last_activity_user_name")) and row["responder_name"]:
        row["last_activity_user_name"] = row["responder_name"]
    if not is_highlightable_status(row.get("status"), row.get("status_label")):
        row["customer_reverted"] = False
        row["reply_age_hours"] = None
        row["reply_highlight"] = "ok"
        row["should_add_revert_tag"] = False
    last_action_label, last_action_detail = classify_last_action(row, previous_row)
    row["last_action_label"] = last_action_label
    row["last_action_detail"] = last_action_detail
    return row


def should_fetch_conversations(
    ticket: Dict[str, object],
    *,
    all_tickets: bool,
    recent_activity_days: int,
) -> bool:
    if not all_tickets:
        return True
    updated_at = parse_iso(ticket.get("updated_at"))
    if updated_at is None:
        return False
    return updated_at >= utc_now() - timedelta(days=recent_activity_days)


def sync_revert_tag(
    client: FreshdeskClient,
    row: Dict[str, object],
    *,
    revert_tag: str,
) -> str:
    tags = [str(item).strip() for item in (row.get("tags") or []) if str(item).strip()]
    lowered = {item.lower(): item for item in tags}
    desired = list(tags)
    tag_key = revert_tag.lower()
    has_tag = tag_key in lowered
    should_have_tag = bool(row.get("customer_reverted"))

    if should_have_tag and not has_tag:
        desired.append(revert_tag)
    elif (not should_have_tag) and has_tag:
        desired = [item for item in desired if item.lower() != tag_key]
    else:
        return "unchanged"

    client.update_ticket(int(row["ticket_id"]), {"tags": sorted(set(desired), key=str.lower)})
    row["tags"] = sorted(set(desired), key=str.lower)
    row["tags_display"] = ", ".join(row["tags"])
    row["has_customer_revert_tag"] = should_have_tag
    row["should_add_revert_tag"] = False
    row["should_remove_revert_tag"] = False
    return "updated"


def build_summary(rows: Sequence[Dict[str, object]], meta: Dict[str, object]) -> Dict[str, object]:
    status_counts = Counter(row["status_label"] for row in rows)
    type_counts = Counter((row.get("type") or "Unspecified") for row in rows)
    highlight_counts = Counter(row["reply_highlight"] for row in rows)
    sla_category_counts = Counter((row.get("sla_category") or "Other") for row in rows)
    summary = {
        "generated_at": meta["generated_at"],
        "updated_since": meta["updated_since"],
        "reply_sla_hours": meta["reply_sla_hours"],
        "customer_revert_tag": meta["customer_revert_tag"],
        "domain": meta["domain"],
        "all_tickets": meta.get("all_tickets"),
        "no_conversations": meta.get("no_conversations"),
        "total_tickets": len(rows),
        "customer_reverted": sum(1 for row in rows if row["customer_reverted"]),
        "overdue_replies": sum(1 for row in rows if row["reply_highlight"] == "overdue"),
        "waiting_replies": sum(1 for row in rows if row["reply_highlight"] == "waiting"),
        "tagged_customer_reverted": sum(1 for row in rows if row["has_customer_revert_tag"]),
        "status_counts": dict(sorted(status_counts.items())),
        "type_counts": dict(sorted(type_counts.items())),
        "highlight_counts": dict(sorted(highlight_counts.items())),
        "sla_category_counts": dict(sorted(sla_category_counts.items())),
        "sla_red_flags": sum(1 for row in rows if row.get("sla_red_flag")),
        "sla_amber_flags": sum(1 for row in rows if row.get("sla_amber_flag")),
        "ack_breaches": sum(1 for row in rows if row.get("ack_state") == "breached"),
        "resolution_breaches": sum(1 for row in rows if row.get("resolution_state") == "breached"),
        "max_timeline_breaches": sum(1 for row in rows if row.get("max_timeline_state") == "breached"),
        "tag_sync": meta["tag_sync"],
        "fetch_errors": meta["fetch_errors"],
    }
    return summary


def write_json(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def write_csv(path: Path, rows: Sequence[Dict[str, object]]) -> None:
    fieldnames = [
        "ticket_id",
        "subject",
        "status_label",
        "priority_label",
        "type",
        "company_name",
        "freshdesk_company_name",
        "company_external_id",
        "hubspot_deal_url",
        "hubspot_deal_count",
        "hubspot_deal_stage",
        "hubspot_deal_stage_label",
        "created_at",
        "updated_at",
        "last_activity_at",
        "last_activity_type",
        "last_activity_preview",
        "last_customer_reply_at",
        "last_agent_reply_at",
        "reply_age_hours",
        "reply_highlight",
        "customer_reverted",
        "has_customer_revert_tag",
        "tags_display",
        "public_reply_count",
        "internal_note_count",
        "customer_reply_count",
        "agent_reply_count",
        "group_id",
        "responder_id",
        "requester_id",
        "timeline_excerpt",
        "sla_category",
        "owner_team",
        "ack_sla",
        "ack_label",
        "ack_deadline_at",
        "resolution_sla",
        "resolution_label",
        "resolution_deadline_at",
        "max_timeline_sla",
        "max_timeline_label",
        "max_timeline_deadline_at",
        "sla_red_flag",
        "sla_amber_flag",
        "sop_action",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: csv_text(row.get(key)) for key in fieldnames})


def build_dashboard_html(
    payload: Dict[str, object],
    *,
    refresh_enabled: bool = False,
    last_refresh_message: str = "",
    csv_url: str = "",
    json_url: str = "",
) -> str:
    summary = payload.get("summary") or {}
    rows = payload.get("rows") or []
    template = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Freshdesk Dashboard</title>
  <style>
    :root { color-scheme: light dark; }
    body { font-family: -apple-system, BlinkMacSystemFont, sans-serif; margin: 0; background: #0f172a; color: #e2e8f0; }
    .wrap { max-width: 1480px; margin: 0 auto; padding: 24px; }
    .top { display:flex; justify-content:space-between; gap:12px; align-items:flex-start; flex-wrap:wrap; }
    h1 { margin: 0 0 8px; font-size: 28px; }
    .meta, .notice { color: #94a3b8; }
    .actions, .tabs, .cards, .controls { display:flex; gap:12px; flex-wrap:wrap; }
    .tabs { margin: 18px 0 14px; }
    .tab-btn, button, a.btn { background:#1e293b; color:#e2e8f0; border:1px solid #475569; border-radius:10px; padding:10px 14px; cursor:pointer; text-decoration:none; }
    .tab-btn.active, button.primary, a.btn.primary { background:#2563eb; border-color:#2563eb; color:white; }
    .cards { display:grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); margin: 18px 0; }
    .card { background:#111827; border:1px solid #334155; border-radius:12px; padding:0; min-width:180px; overflow:hidden; }
    .card-button { width:100%; background:transparent; color:inherit; border:0; padding:14px; text-align:left; cursor:pointer; }
    .card-button:hover { background:#172033; }
    .card-button.active { background:#1d4ed8; }
    .card-button.active .label, .card-button.active .value { color:#ffffff; }
    .card .label { color:#94a3b8; font-size:12px; text-transform:uppercase; letter-spacing:.06em; }
    .card .value { font-size:28px; font-weight:700; margin-top:8px; }
    .controls { align-items:flex-start; margin-bottom:14px; }
    input { border:1px solid #475569; border-radius:8px; background:#0b1220; color:#e2e8f0; padding:10px 12px; }
    .date-input { min-width: 150px; }
    .filter-box { border:1px solid #475569; border-radius:8px; background:#0b1220; min-width:190px; }
    .filter-box summary { cursor:pointer; list-style:none; padding:10px 12px; font-size:14px; }
    .filter-box summary::-webkit-details-marker { display:none; }
    .filter-list { max-height:220px; overflow:auto; padding:0 12px 12px; }
    .filter-item { display:flex; gap:8px; align-items:center; margin:6px 0; font-size:13px; }
    .filter-search { width: calc(100% - 24px); margin: 0 12px 8px; }
    .filter-item.hidden-item { display:none; }
    .table-wrap { overflow:auto; border:1px solid #334155; border-radius:12px; background:#111827; }
    table { width:100%; border-collapse:collapse; background:#111827; border-radius:12px; overflow:hidden; min-width:1100px; }
    th, td { border-bottom:1px solid #1e293b; text-align:left; padding:10px 12px; vertical-align:top; font-size:13px; }
    th { position:sticky; top:0; background:#0b1220; z-index:1; }
    .sort-button { background:transparent; color:inherit; border:0; border-radius:4px; padding:0; font:inherit; font-weight:inherit; text-align:left; white-space:nowrap; }
    .sort-button:hover { color:#93c5fd; }
    .sort-button:focus-visible { outline:2px solid #93c5fd; outline-offset:4px; }
    tr.overdue, tr.sla-red { background: rgba(220,38,38,.18); }
    tr.waiting, tr.sla-amber { background: rgba(234,179,8,.12); }
    .pill { display:inline-block; border-radius:999px; padding:2px 8px; font-size:11px; font-weight:700; }
    .pill.overdue, .pill.breached { background:#7f1d1d; color:#fecaca; }
    .pill.waiting, .pill.at_risk { background:#78350f; color:#fde68a; }
    .pill.ok, .pill.met, .pill.open { background:#14532d; color:#bbf7d0; }
    .pill.na { background:#1e293b; color:#cbd5e1; }
    .subject { min-width:260px; }
    .small { color:#94a3b8; font-size:12px; }
    .id a { color:#93c5fd; text-decoration:none; }
    .timeline { max-width:420px; white-space:pre-wrap; }
    .hidden { display:none; }
    .view-section { display:grid; gap:14px; }
    .section-head { display:flex; justify-content:space-between; align-items:flex-start; gap:12px; flex-wrap:wrap; }
    .section-copy { color:#94a3b8; font-size:12px; margin-top:4px; max-width:780px; }
    .column-box { min-width:220px; }
    .table-empty { text-align:center; color:#94a3b8; padding:20px; }
    .deal-stage-control { display:flex; align-items:center; gap:12px; flex-wrap:wrap; margin:14px 0; }
    .deal-stage-control label { font-size:14px; color:#cbd5e1; }
    .deal-stage-control select { background:#0b1220; color:#e2e8f0; border:1px solid #475569; border-radius:10px; padding:10px 12px; max-width:100%; }
    .deal-stage-control select:focus-visible { outline:2px solid #93c5fd; outline-offset:2px; }
    .overview-grid { display:grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap:16px; margin-top:16px; }
    .overview-card, .chart-card { background:#111827; border:1px solid #334155; border-radius:12px; padding:16px; min-width:0; overflow:hidden; }
    .overview-card .label, .chart-card .label, .attention-item .label { color:#94a3b8; font-size:12px; text-transform:uppercase; letter-spacing:.06em; }
    .overview-card .value { font-size:32px; font-weight:700; margin-top:8px; }
    .overview-card .subvalue, .chart-card .subvalue { color:#94a3b8; font-size:12px; margin-top:8px; }
    .overview-card-button { width:100%; border:0; background:transparent; color:inherit; text-align:left; padding:0; cursor:pointer; }
    .overview-card.active { border-color:#2563eb; box-shadow: inset 0 0 0 1px #2563eb; }
    .progress-grid { grid-template-columns:repeat(4, minmax(0, 1fr)); }
    .attention-grid { grid-template-columns:repeat(2, minmax(0, 1fr)); }
    .overview-card { padding:0; }
    .overview-card-button { padding:20px; border-radius:12px; }
    .overview-card-button:hover { background:#172033; }
    .overview-card-button:focus-visible { outline:2px solid #93c5fd; outline-offset:-3px; }
    .overview-card .label { text-transform:none; letter-spacing:0; color:#cbd5e1; font-size:14px; }
    .metric-line { display:flex; align-items:baseline; gap:12px; flex-wrap:wrap; margin-top:10px; }
    .metric-line .value { margin-top:0; font-size:36px; line-height:1.2; }
    .metric-comparison { color:#94a3b8; font-size:12px; }
    .metric-comparison strong { color:#cbd5e1; font-weight:500; }
    .metric-created .value { color:#93c5fd; }
    .metric-closed .value { color:#86efac; }
    .metric-waiting { border-left:3px solid #fbbf24; }
    .metric-waiting .value { color:#fcd34d; }
    .metric-breached { border-left:3px solid #f87171; }
    .metric-breached .value { color:#fca5a5; }
    .date-field { display:grid; gap:5px; color:#94a3b8; font-size:12px; }
    .overview-controls button[aria-pressed="true"] { background:#1d4ed8; border-color:#3b82f6; color:white; }
    .overview-controls .date-shortcuts { display:flex; gap:8px; flex-wrap:wrap; align-self:end; }
    .overview-controls .filter-box { align-self:end; }
    .overview-results { scroll-margin-top:20px; }
    .overview-results .section-copy { margin-bottom:12px; }
    .overview-details > summary { cursor:pointer; font-size:16px; color:#cbd5e1; padding:16px 0; }
    .breakdown-panel { margin-top:16px; }
    .breakdown-panel h3 { margin:0 0 8px; font-size:17px; }
    .breakdown-panel > summary { cursor:pointer; font-size:16px; }
    .ticket-chart { display:grid; gap:16px; margin-top:14px; }
    .chart-axis { display:grid; grid-template-columns:minmax(180px, 240px) minmax(0, 1fr) 60px; gap:14px; color:#94a3b8; font-size:12px; }
    .chart-ticks { display:flex; justify-content:space-between; }
    .chart-row { display:grid; grid-template-columns:minmax(180px, 240px) minmax(0, 1fr) 60px; gap:14px; align-items:center; padding-bottom:12px; border-bottom:1px solid #1e293b; }
    .chart-row-name { font-size:13px; font-weight:600; overflow-wrap:anywhere; }
    .chart-row-value { text-align:right; font-size:18px; font-weight:700; }
    .chart-row-track { min-width:0; }
    .ticket-bar-track { height:26px; background:repeating-linear-gradient(to right, #334155 0, #334155 1px, transparent 1px, transparent 50%); border-bottom:1px solid #334155; display:flex; align-items:center; }
    .ticket-bar-stack { height:20px; display:flex; border-radius:4px; overflow:hidden; }
    .ticket-bar-segment { display:block; height:100%; border:0; border-radius:0; padding:0; min-width:0; cursor:pointer; }
    .ticket-bar-segment:hover { filter:brightness(1.3); }
    .ticket-bar-segment:focus-visible { outline:2px solid white; outline-offset:-2px; }
    .chart-type-counts { display:flex; gap:10px; flex-wrap:wrap; margin-top:6px; }
    .chart-type-counts .breakdown-count { font-size:12px; padding:2px 0; font-weight:400; text-decoration:none; }
    .chart-type-dot { width:9px; height:9px; border-radius:2px; display:inline-block; margin-right:5px; }
    .chart-legend { display:flex; gap:14px; flex-wrap:wrap; color:#cbd5e1; font-size:12px; margin:12px 0; }
    .paired-bar { display:grid; grid-template-columns:54px minmax(0, 1fr) 42px; gap:8px; align-items:center; font-size:12px; color:#94a3b8; }
    .paired-bar .ticket-bar-track { height:24px; }
    .paired-bar .ticket-bar-stack { height:15px; }
    .paired-bar .breakdown-count { font-size:13px; padding:2px; }
    .paired-chart-axis { grid-template-columns:minmax(180px, 240px) minmax(0, 1fr) 60px; }
    .paired-chart-axis .chart-ticks { padding-left:62px; padding-right:50px; }
    .chart-share { color:#94a3b8; font-size:12px; }
    @media (max-width: 700px) {
      .chart-row, .chart-axis { grid-template-columns:minmax(100px, 140px) minmax(0, 1fr) 40px; gap:8px; }
      .paired-bar { grid-template-columns:44px minmax(0, 1fr) 28px; gap:4px; }
      .paired-chart-axis .chart-ticks { padding-left:48px; padding-right:32px; }
      .chart-row-value { font-size:15px; }
    }
    .breakdown-count { color:#93c5fd; background:transparent; border:0; border-radius:4px; padding:4px 6px; font:inherit; font-weight:600; text-decoration:underline; text-underline-offset:3px; }
    .breakdown-count:hover { background:#1e3a5f; }
    .breakdown-count:focus-visible { outline:2px solid #93c5fd; }
    .breakdown-note { color:#94a3b8; font-size:13px; margin:10px 0 0; line-height:1.6; }
    .overview-details > summary:hover { color:#93c5fd; }
    @media (max-width: 1000px) { .progress-grid { grid-template-columns:repeat(2, minmax(0, 1fr)); } }
    @media (max-width: 540px) { .progress-grid, .attention-grid { grid-template-columns:1fr; } }
    .overview-section { margin-top:18px; }
    .section-title { margin:0 0 12px; font-size:18px; }
    .overview-controls { display:flex; gap:12px; flex-wrap:wrap; align-items:center; margin:12px 0 6px; }
    .overview-helper { color:#94a3b8; font-size:12px; margin-top:4px; }
    .bar-chart { display:flex; align-items:flex-end; gap:10px; min-height:220px; padding-top:12px; }
    .bar-group { flex:1; display:flex; flex-direction:column; align-items:center; gap:8px; }
    .bar-stack { width:100%; max-width:56px; min-height:180px; display:flex; align-items:flex-end; }
    .bar { width:100%; border-radius:10px 10px 0 0; background:#2563eb; min-height:6px; }
    .bar.secondary { background:#7c3aed; }
    .bar-label { color:#94a3b8; font-size:12px; text-align:center; }
    .bar-value { font-size:12px; color:#e2e8f0; }
    .mini-bars { display:grid; gap:12px; margin-top:12px; }
    .mini-row { display:grid; grid-template-columns: 150px 1fr auto; gap:12px; align-items:center; }
    .mini-track { background:#0b1220; border:1px solid #334155; border-radius:999px; overflow:hidden; height:12px; }
    .mini-fill { background:#2563eb; height:100%; }
    .mini-fill.pending { background:#f59e0b; }
    .mini-fill.created { background:#2563eb; }
    .mini-fill.closed { background:#22c55e; }
    .mini-fill.bug { background:#ef4444; }
    .mini-fill.feature { background:#8b5cf6; }
    .mini-fill.task { background:#0ea5e9; }
    .mini-fill.other { background:#64748b; }
    .category-progress { display:grid; gap:12px; margin-top:12px; }
    .category-progress-head, .category-progress-row { display:grid; grid-template-columns: minmax(0, 1.2fr) repeat(3, minmax(0, 1fr)); gap:12px; align-items:center; }
    .category-progress-head { color:#94a3b8; font-size:11px; text-transform:uppercase; letter-spacing:.06em; }
    .category-progress-label { font-size:13px; font-weight:600; min-width:0; overflow-wrap:anywhere; word-break:break-word; }
    .category-progress-cell { display:grid; gap:6px; min-width:0; }
    .category-progress-value { color:#cbd5e1; font-size:12px; min-width:0; overflow-wrap:anywhere; word-break:break-word; }
    .company-progress { display:grid; gap:12px; margin-top:12px; }
    .company-progress-head, .company-progress-row { display:grid; grid-template-columns: minmax(0, 1.15fr) minmax(0, 1.7fr) minmax(44px, .35fr); gap:12px; align-items:center; }
    .company-progress-head { color:#94a3b8; font-size:11px; text-transform:uppercase; letter-spacing:.06em; }
    .company-progress-head > div, .category-progress-head > div { min-width:0; overflow-wrap:anywhere; word-break:break-word; }
    .company-progress-cell { min-width:0; }
    .company-progress-cell .small { overflow-wrap:anywhere; word-break:break-word; }
    .stacked-track { background:#0b1220; border:1px solid #334155; border-radius:999px; overflow:hidden; height:12px; display:flex; }
    .stack-segment { height:100%; }
    .stack-segment.bug { background:#ef4444; }
    .stack-segment.feature { background:#8b5cf6; }
    .stack-segment.task { background:#0ea5e9; }
    .stack-segment.other { background:#64748b; }
    .line-chart { margin-top:14px; }
    .line-chart svg { width:100%; height:220px; display:block; }
    .line-labels { display:flex; justify-content:space-between; gap:8px; color:#94a3b8; font-size:12px; margin-top:10px; }
    .line-legend { display:flex; gap:12px; flex-wrap:wrap; margin-top:10px; color:#cbd5e1; font-size:12px; }
    .legend-dot { width:10px; height:10px; border-radius:999px; display:inline-block; margin-right:6px; }
    .attention-list { display:grid; gap:12px; }
    .attention-item { background:#111827; border:1px solid #334155; border-radius:12px; padding:14px; }
    .attention-item a { color:#93c5fd; text-decoration:none; }
    .attention-title { font-weight:700; }
    .attention-meta { color:#94a3b8; font-size:12px; margin-top:4px; }
    .attention-reason { margin-top:8px; font-size:13px; }
    .snapshot-flag { display:inline-flex; align-items:center; gap:6px; margin-top:6px; flex-wrap:wrap; }
    .next-step { font-weight:600; color:#bfdbfe; }
    @media (max-width: 1200px) {
      .overview-grid.graphs-grid { grid-template-columns: 1fr; }
    }
    @media (max-width: 900px) {
      .category-progress-head, .company-progress-head { display:none; }
      .category-progress-row, .company-progress-row { grid-template-columns: 1fr; gap:8px; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="top">
      <div>
        <h1>Freshdesk Dashboard</h1>
        <div class="meta" id="meta"></div>
        <div class="notice" id="notice"></div>
      </div>
      <div class="actions">
        __REFRESH__
        __CSV__
        __JSON__
      </div>
    </div>
    <div class="tabs">
      <button class="tab-btn active" id="tab-overview">Ticket Progress</button>
      <button class="tab-btn" id="tab-sla">Ticket Pipeline</button>
      <button class="tab-btn" id="tab-activity">Email Alert</button>
    </div>
    <div class="deal-stage-control">
      <label for="deal-stage-filter">Deal stage</label>
      <select id="deal-stage-filter" aria-describedby="deal-stage-helper"><option value="all">All deal stages</option></select>
      <span class="small" id="deal-stage-helper">Applies to all tabs · Stage of the linked primary deal at the latest refresh</span>
    </div>
    <div class="cards" id="cards"></div>
    <div id="activity-view">
      <div class="view-section">
        <div class="section-head">
          <div>
            <div class="section-title">Email Alert</div>
            <div class="section-copy">Keep this page action-focused: scan customer waiting tickets, stale updates, and the latest movement. Use show/hide columns to simplify the table for daily follow-up.</div>
          </div>
        </div>
        <div class="controls">
          <input id="activity-search" type="search" placeholder="Search subject, tags, ticket id">
          <input class="date-input" id="activity-date-from" type="date" title="Created from">
          <input class="date-input" id="activity-date-to" type="date" title="Created to">
          <details class="filter-box"><summary>Reply State</summary><div class="filter-list" id="activity-filter-highlight"></div></details>
          <details class="filter-box"><summary>Status</summary><div class="filter-list" id="activity-filter-status"></div></details>
          <details class="filter-box"><summary>Ticket Category</summary><div class="filter-list" id="activity-filter-type"></div></details>
          <details class="filter-box"><summary>Company Name</summary><div class="filter-list" id="activity-filter-company"></div></details>
          <details class="filter-box column-box"><summary>Show / Hide Columns</summary><div class="filter-list" id="activity-columns"></div></details>
        </div>
        <div class="table-wrap"><table>
          <thead id="activity-table-head"></thead>
          <tbody id="activity-table-body"></tbody>
        </table></div>
      </div>
    </div>
    <div id="sla-view" class="hidden">
      <div class="view-section">
        <div class="section-head">
          <div>
            <div class="section-title">Ticket Pipeline</div>
            <div class="section-copy">Use this screen for breach control, ownership checks, and next-step planning. Hide extra columns when you only want queue stage, owner, and SLA state.</div>
          </div>
        </div>
        <div class="controls">
          <input id="sla-search" type="search" placeholder="Search subject, category, ticket id">
          <input class="date-input" id="sla-date-from" type="date" title="Created from">
          <input class="date-input" id="sla-date-to" type="date" title="Created to">
          <details class="filter-box"><summary>Status</summary><div class="filter-list" id="sla-filter-status"></div></details>
          <details class="filter-box"><summary>Ticket Category</summary><div class="filter-list" id="sla-filter-type"></div></details>
          <details class="filter-box"><summary>Company Name</summary><div class="filter-list" id="sla-filter-company"></div></details>
          <details class="filter-box"><summary>SLA Flags</summary><div class="filter-list" id="sla-filter-sla"></div></details>
          <details class="filter-box column-box"><summary>Show / Hide Columns</summary><div class="filter-list" id="sla-columns"></div></details>
        </div>
        <div class="table-wrap"><table>
          <thead id="sla-table-head"></thead>
          <tbody id="sla-table-body"></tbody>
        </table></div>
        <div class="overview-section">
          <div class="section-head">
            <div>
              <h2 class="section-title">Ticket Snapshot</h2>
              <div class="section-copy">Keep the snapshot in the Ticket Pipeline view so the ticket list, SLA filters, owner view, and next agenda stay together.</div>
            </div>
            <details class="filter-box column-box"><summary>Show / Hide Columns</summary><div class="filter-list" id="overview-columns"></div></details>
          </div>
          <div class="table-wrap"><table>
            <thead id="overview-table-head"></thead>
            <tbody id="overview-table-body"></tbody>
          </table></div>
        </div>
      </div>
    </div>
    <div id="overview-view" class="hidden">
      <div class="overview-section">
        <h2 class="section-title">Ticket Progress</h2>
        <div class="overview-controls">
          <label class="date-field">From<input class="date-input" id="overview-date-from" type="date"></label>
          <label class="date-field">To<input class="date-input" id="overview-date-to" type="date"></label>
          <div class="date-shortcuts">
          <button type="button" id="overview-current-week">This Week</button>
          <button type="button" id="overview-last-week">Last Week</button>
          <button type="button" id="overview-current-month">This Month</button>
          <button type="button" id="overview-last-month">Last Month</button>
          </div>
          <details class="filter-box"><summary>Ticket Category</summary><div class="filter-list" id="overview-filter-type"></div></details>
        </div>
        <div class="overview-helper" id="overview-helper"></div>
        <div class="overview-grid progress-grid" id="overview-metrics"></div>
      </div>
      <div class="overview-section">
        <h2 class="section-title">Needs attention</h2>
        <div class="section-copy">Currently open tickets created in the selected dates. Click a number to review the tickets. A ticket can appear in both groups.</div>
        <div class="overview-grid attention-grid" id="overview-attention-metrics"></div>
      </div>
      <div class="overview-section overview-results hidden" id="overview-results">
        <div class="section-head">
          <h2 class="section-title" id="overview-results-title" tabindex="-1">Matching tickets</h2>
          <button type="button" id="overview-clear-selection">Clear selection</button>
        </div>
        <div class="section-copy" id="overview-results-copy"></div>
        <div class="table-wrap"><table>
          <thead id="overview-results-head"></thead>
          <tbody id="overview-results-body"></tbody>
        </table></div>
      </div>
      <details class="overview-section overview-details" open>
        <summary>Ticket charts</summary>
        <div class="chart-card breakdown-panel">
          <h3>Top 10 companies raising tickets</h3>
          <p class="section-copy">New tickets · Bar length = ticket count · Click a bar or number</p>
          <div id="overview-company-bars"></div>
        </div>
        <div class="chart-card breakdown-panel">
          <h3>New tickets by type</h3>
          <p class="section-copy">Current period vs previous period · Same scale for every type</p>
          <div id="overview-category-bars"></div>
        </div>
        <details class="chart-card breakdown-panel">
          <summary>Daily new tickets and closures</summary>
          <p class="section-copy">New tickets and closed tickets · Closures may include older tickets</p>
          <div id="overview-daily-activity"></div>
        </details>
      </details>

    </div>
  </div>
  <script>
    const data = __DATA__;
    const summary = data.summary || {};
    const rows = data.rows || [];
    let currentView = 'activity';
    let breakdownSelection = null;
    const activeCardFilters = { activity: 'tickets', sla: 'tickets', overview: 'tickets' };
    const columnConfigs = {
      activity: [
        { id: 'ticket', sortValue: (row) => row.ticket_id, label: 'ID', default: true, className: 'id', render: (row) => `<a href="https://${summary.domain}/a/tickets/${row.ticket_id}" target="_blank" rel="noreferrer">${row.ticket_id}</a>` },
        { id: 'subject', label: 'Subject', default: true, className: 'subject', render: (row) => `<strong>${escapeHtml(row.subject)}</strong><div class="small">Priority: ${escapeHtml(row.priority_label || '-')} • Owner: ${escapeHtml(displayAgentName(row))}</div>` },
        { id: 'status', label: 'Status', default: true, render: (row) => escapeHtml(row.status_label || '') },
        { id: 'type', label: 'Category', default: true, render: (row) => escapeHtml(row.type || '') },
        { id: 'company', label: 'Company', default: true, render: (row) => escapeHtml(row.company_name || 'Unknown') },
        { id: 'deal_stage', label: 'HubSpot deal stage', default: true, render: (row) => escapeHtml(dealStageLabel(row)) },
        { id: 'hubspot', label: 'HubSpot Deal', default: true, render: (row) => hubspotDealCell(row) },
        { id: 'updated', sortValue: (row) => parseDate(row.updated_at)?.getTime(), label: 'Updated', default: true, render: (row) => `<div>${escapeHtml(row.updated_at_ist || '')}</div><div class="small">${escapeHtml(row.last_action_label || 'Last action')}</div>` },
        { id: 'reply_check', label: 'Reply Check', default: true, render: (row) => `${replyPill(row)}<div class="small">Customer waiting: ${row.customer_reverted ? 'Yes' : 'No'}</div>` },
        { id: 'customer_reverted', label: 'Customer Reverted', default: false, render: (row) => `${yesNoUnknown(row.customer_reverted_ever, row.conversation_checked)}<div class="small">At least one customer revert</div>` },
        { id: 'replied_same_ticket', label: 'Replied Same Ticket', default: false, render: (row) => `${yesNoUnknown(row.replied_after_customer_revert, row.conversation_checked)}<div class="small">Reply after revert</div>` },
        { id: 'tags', label: 'Tags', default: false, render: (row) => escapeHtml(row.tags_display || '') },
        { id: 'last_activity', sortValue: (row) => parseDate(row.last_activity_at)?.getTime(), label: 'Latest Activity', default: true, render: (row) => `<div><strong>${escapeHtml(row.last_activity_type || '')}</strong></div><div class="small">${escapeHtml(row.last_activity_at_ist || row.last_activity_at || '')}</div><div>${escapeHtml(row.last_activity_preview || '')}</div>` },
        { id: 'timeline', label: 'Recent Timeline', default: false, className: 'timeline', render: (row) => escapeHtml(row.timeline_excerpt || '') },
        { id: 'next_agenda', label: 'Next Agenda', default: true, render: (row) => `<span class="next-step">${escapeHtml(suggestNextAgenda(row))}</span><div class="small">${escapeHtml(row.sop_action || '')}</div>` },
      ],
      sla: [
        { id: 'ticket', sortValue: (row) => row.ticket_id, label: 'ID', default: true, className: 'id', render: (row) => `<a href="https://${summary.domain}/a/tickets/${row.ticket_id}" target="_blank" rel="noreferrer">${row.ticket_id}</a>` },
        { id: 'subject', label: 'Subject', default: true, className: 'subject', render: (row) => `<strong>${escapeHtml(row.subject)}</strong><div class="small">Status: ${escapeHtml(row.status_label)} • Type: ${escapeHtml(row.type || '')}</div>` },
        { id: 'sla_category', label: 'Category', default: true, render: (row) => escapeHtml(row.sla_category || '') },
        { id: 'priority', label: 'Priority', default: true, render: (row) => escapeHtml(row.priority_label || '') },
        { id: 'company', label: 'Company', default: true, render: (row) => escapeHtml(row.company_name || 'Unknown') },
        { id: 'deal_stage', label: 'HubSpot deal stage', default: true, render: (row) => escapeHtml(dealStageLabel(row)) },
        { id: 'hubspot', label: 'HubSpot Deal', default: true, render: (row) => hubspotDealCell(row) },
        { id: 'created', sortValue: (row) => parseDate(row.created_at)?.getTime(), label: 'Created IST', default: true, render: (row) => escapeHtml(row.created_at_ist || '') },
        { id: 'closed', sortValue: (row) => parseDate(row.closed_at || row.resolved_at)?.getTime(), label: 'Closed IST', default: true, render: (row) => escapeHtml(row.closed_at_ist || row.resolved_at_ist || '') || '<span class="small">Open</span>' },
        { id: 'last_activity', sortValue: (row) => parseDate(row.last_activity_at)?.getTime(), label: 'Last Activity', default: true, render: (row) => `<div>${escapeHtml(row.last_activity_at_ist || '')}</div><div class="small">${escapeHtml(row.last_activity_type || '')}</div>` },
        { id: 'done_by', label: 'Done By', default: true, render: (row) => escapeHtml(displayAgentName(row)) },
        { id: 'ack', label: 'Ack SLA', default: true, render: (row) => `${slaPill(row.ack_state, row.ack_label)}<div class="small">SLA: ${escapeHtml(row.ack_sla || 'NA')}</div>` },
        { id: 'resolution', label: 'Resolution SLA', default: true, render: (row) => `${slaPill(row.resolution_state, row.resolution_label)}<div class="small">SLA: ${escapeHtml(row.resolution_sla || 'NA')}</div>` },
        { id: 'timeline_sla', label: 'Max Timeline', default: false, render: (row) => `${slaPill(row.max_timeline_state, row.max_timeline_label)}<div class="small">Limit: ${escapeHtml(row.max_timeline_sla || 'NA')}</div>` },
        { id: 'owner_team', label: 'Owner Team', default: true, render: (row) => escapeHtml(row.owner_team || '') },
        { id: 'next_agenda', label: 'Next Agenda', default: true, render: (row) => `<span class="next-step">${escapeHtml(suggestNextAgenda(row))}</span><div class="small">${escapeHtml(row.sop_action || '')}</div>` },
      ],
      overview: [
        { id: 'ticket', sortValue: (row) => row.ticket_id, label: 'ID', default: true, className: 'id', render: (row) => `<a href="https://${summary.domain}/a/tickets/${row.ticket_id}" target="_blank" rel="noreferrer">${row.ticket_id}</a>` },
        { id: 'subject', label: 'Ticket Snapshot', default: true, className: 'subject', render: (row) => `<strong>${escapeHtml(row.subject)}</strong><div class="small">${escapeHtml(row._overview_reason || row.last_action_label || 'Quick snapshot')}</div>` },
        { id: 'company', label: 'Company', default: true, render: (row) => escapeHtml(row.company_name || 'Unknown') },
        { id: 'deal_stage', label: 'HubSpot deal stage', default: true, render: (row) => escapeHtml(dealStageLabel(row)) },
        { id: 'type', label: 'Category', default: true, render: (row) => escapeHtml(row.type || '') },
        { id: 'status', label: 'Stage', default: true, render: (row) => `<div>${escapeHtml(row.status_label || '')}</div><div class="small">${escapeHtml(row.last_action_label || '')}</div>` },
        { id: 'owner', label: 'Owner', default: true, render: (row) => `<div>${escapeHtml(displayAgentName(row))}</div><div class="small">${escapeHtml(row.owner_team || '')}</div>` },
        { id: 'stuck', label: 'Where Stuck', default: true, render: (row) => `<div>${escapeHtml(overviewBottleneckText(row))}</div><div class="small">${escapeHtml(row.last_activity_type || '')}</div>` },
        { id: 'ack', label: 'Ack SLA', default: true, render: (row) => `${slaPill(row.ack_state, row.ack_label)}<div class="small">${escapeHtml(row.ack_sla || 'NA')}</div>` },
        { id: 'resolution', label: 'Resolution SLA', default: true, render: (row) => `${slaPill(row.resolution_state, row.resolution_label)}<div class="small">${escapeHtml(row.resolution_sla || 'NA')}</div>` },
        { id: 'latest', sortValue: (row) => parseDate(row.last_activity_at || row.updated_at)?.getTime(), label: 'Latest Update', default: true, render: (row) => `<div>${escapeHtml(row.last_activity_at_ist || row.updated_at_ist || '')}</div><div class="small">${escapeHtml(row.last_activity_preview || '')}</div>` },
        { id: 'next_agenda', label: 'Next Agenda', default: true, render: (row) => `<span class="next-step">${escapeHtml(row._next_agenda || suggestNextAgenda(row))}</span><div class="small">${escapeHtml(row.sop_action || '')}</div>` },
        { id: 'hubspot', label: 'HubSpot Deal', default: false, render: (row) => hubspotDealCell(row) },
      ],
    };
    const sortState = { activity: null, sla: null, overview: null };
    const columnPickerIds = { activity: 'activity-columns', sla: 'sla-columns', overview: 'overview-columns' };
    const defaultColumnState = Object.fromEntries(Object.entries(columnConfigs).map(([view, columns]) => [view, columns.filter((column) => column.default !== false).map((column) => column.id)]));
    const storedColumnState = (() => {
      try {
        const saved = JSON.parse(localStorage.getItem('freshdeskDashboardColumnState') || '{}') || {};
        // Add the new column once for existing users while keeping their other choices.
        if (localStorage.getItem('freshdeskDashboardDealStageColumnV1') !== '1') {
          ['activity', 'sla', 'overview'].forEach((view) => {
            if (Array.isArray(saved[view]) && saved[view].length && !saved[view].includes('deal_stage')) {
              saved[view].push('deal_stage');
            }
          });
          localStorage.setItem('freshdeskDashboardColumnState', JSON.stringify(saved));
          localStorage.setItem('freshdeskDashboardDealStageColumnV1', '1');
        }
        return saved;
      }
      catch (error) { return {}; }
    })();
    const columnState = Object.fromEntries(Object.entries(defaultColumnState).map(([view, defaults]) => {
      const allowed = new Set((columnConfigs[view] || []).map((column) => column.id));
      const saved = Array.isArray(storedColumnState[view]) ? storedColumnState[view].filter((value) => allowed.has(value)) : [];
      return [view, saved.length ? saved : defaults];
    }));
    const agentNamesByResponderId = rows.reduce((acc, row) => {
      const responderId = Number(row.responder_id || 0);
      const responderName = String(row.responder_name || '').trim();
      const lastActor = String(row.last_activity_user_name || '').trim();
      if (responderId && responderName) acc[responderId] = responderName;
      else if (responderId && lastActor && !/^Agent ID\s+\d+$/.test(lastActor)) acc[responderId] = lastActor;
      return acc;
    }, {});
    function escapeHtml(text) { return String(text || '').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;'); }
    function displayAgentName(row) {
      const currentName = String(row.last_activity_user_name || '').trim();
      if (currentName && !/^Agent ID\s+\d+$/.test(currentName)) return currentName;
      const responderName = String(row.responder_name || '').trim();
      if (responderName) return responderName;
      const responderId = Number(row.responder_id || 0);
      if (responderId && agentNamesByResponderId[responderId]) return agentNamesByResponderId[responderId];
      return currentName || 'Unassigned';
    }
    function parseDate(value) {
      const parsed = new Date(value || '');
      return Number.isNaN(parsed.getTime()) ? null : parsed;
    }
    function formatDateInputValue(date) {
      const year = date.getFullYear();
      const month = String(date.getMonth() + 1).padStart(2, '0');
      const day = String(date.getDate()).padStart(2, '0');
      return `${year}-${month}-${day}`;
    }
    function startOfWeek(date) {
      const current = new Date(date);
      current.setHours(0, 0, 0, 0);
      const day = current.getDay();
      const diff = day === 0 ? -6 : 1 - day;
      current.setDate(current.getDate() + diff);
      return current;
    }
    function addDays(date, days) {
      const next = new Date(date);
      next.setDate(next.getDate() + days);
      return next;
    }
    function startOfMonth(date) {
      return new Date(date.getFullYear(), date.getMonth(), 1);
    }
    function endOfMonth(date) {
      return new Date(date.getFullYear(), date.getMonth() + 1, 0, 23, 59, 59, 999);
    }
    function endOfWeek(date) {
      const end = addDays(startOfWeek(date), 6);
      end.setHours(23, 59, 59, 999);
      return end;
    }
    function sameDateValue(left, right) {
      return formatDateInputValue(left) === formatDateInputValue(right);
    }
    function saveColumnState() {
      try { localStorage.setItem('freshdeskDashboardColumnState', JSON.stringify(columnState)); }
      catch (error) { /* ignore browser storage failures */ }
    }
    function getVisibleColumns(view) {
      const selected = columnState[view] || [];
      const available = columnConfigs[view] || [];
      const visible = available.filter((column) => selected.includes(column.id));
      return visible.length ? visible : available.filter((column) => column.default !== false);
    }
    function renderColumnPicker(view) {
      const container = document.getElementById(columnPickerIds[view]);
      if (!container) return;
      const selected = new Set(columnState[view] || []);
      container.innerHTML = (columnConfigs[view] || []).map((column) => `
        <label class="filter-item">
          <input type="checkbox" data-column-view="${view}" value="${column.id}" ${selected.has(column.id) ? 'checked' : ''}>
          ${escapeHtml(column.label)}
        </label>`).join('');
    }
    function renderAllColumnPickers() {
      Object.keys(columnConfigs).forEach(renderColumnPicker);
    }
    function rowClassForView(view, row) {
      if (view === 'activity') return row.reply_highlight || '';
      if (view === 'overview') {
        if (row.sla_red_flag) return 'sla-red';
        if (row.sla_amber_flag) return 'sla-amber';
        return row.reply_highlight || '';
      }
      return row.sla_red_flag ? 'sla-red' : row.sla_amber_flag ? 'sla-amber' : '';
    }
    function sortedTableRows(view, sourceRows) {
      const activeSort = sortState[view];
      const column = (columnConfigs[view] || []).find((item) => item.id === activeSort?.columnId);
      if (!column?.sortValue || !getVisibleColumns(view).some((item) => item.id === column.id)) return sourceRows;
      const numericValue = (row) => {
        const value = column.sortValue(row);
        if (value === null || value === undefined || String(value).trim() === '') return null;
        const number = Number(value);
        return Number.isFinite(number) ? number : null;
      };
      return [...sourceRows].sort((left, right) => {
        const a = numericValue(left);
        const b = numericValue(right);
        // Missing dates and numbers stay last in either direction.
        if (a === null) return b === null ? 0 : 1;
        if (b === null) return -1;
        return activeSort.direction === 'ascending' ? a - b : b - a;
      });
    }
    function renderDynamicTable(view, headId, bodyId, sourceRows) {
      const head = document.getElementById(headId);
      const body = document.getElementById(bodyId);
      if (!head || !body) return;
      const columns = getVisibleColumns(view);
      const activeSort = sortState[view];
      head.innerHTML = `<tr>${columns.map((column) => {
        if (!column.sortValue) return `<th scope="col">${escapeHtml(column.label)}</th>`;
        const direction = activeSort?.columnId === column.id ? activeSort.direction : null;
        const nextDirection = direction === 'ascending' ? 'descending' : 'ascending';
        return `<th scope="col" aria-sort="${direction || 'none'}"><button type="button" class="sort-button" data-sort-column="${column.id}" title="Sort ${escapeHtml(column.label)} ${nextDirection}">${escapeHtml(column.label)} <span aria-hidden="true">${direction === 'ascending' ? '↑' : direction === 'descending' ? '↓' : '↕'}</span></button></th>`;
      }).join('')}</tr>`;
      head.querySelectorAll('[data-sort-column]').forEach((button) => {
        button.addEventListener('click', () => {
          const columnId = button.dataset.sortColumn;
          sortState[view] = { columnId, direction: activeSort?.columnId === columnId && activeSort.direction === 'ascending' ? 'descending' : 'ascending' };
          render();
          head.querySelector(`[data-sort-column="${columnId}"]`)?.focus();
        });
      });
      const displayedRows = sortedTableRows(view, sourceRows);
      if (!sourceRows.length) {
        body.innerHTML = `<tr><td class="table-empty" colspan="${Math.max(1, columns.length)}">No tickets match the current filters.</td></tr>`;
        return;
      }
      body.innerHTML = displayedRows.map((row) => `<tr class="${rowClassForView(view, row)}">${columns.map((column) => `<td class="${escapeHtml(column.className || '')}">${column.render(row)}</td>`).join('')}</tr>`).join('');
    }
    function isRejectedStatus(label) {
      return String(label || '').toLowerCase().includes('reject');
    }
    function isClosedLikeStatus(label) {
      const normalized = String(label || '').trim().toLowerCase();
      return normalized === 'closed' || normalized === 'resolved' || isRejectedStatus(normalized);
    }
    function closedAtForRow(row) {
      return parseDate(row.closed_at || row.resolved_at);
    }
    function isPendingAtStart(row, fromDate) {
      const createdAt = parseDate(row.created_at);
      if (!(createdAt && createdAt < fromDate)) return false;
      const closedAt = closedAtForRow(row);
      if (closedAt) return closedAt >= fromDate;
      return !isClosedLikeStatus(row.status_label);
    }
    function isPendingAtEnd(row, toDate) {
      const createdAt = parseDate(row.created_at);
      if (!(createdAt && createdAt <= toDate)) return false;
      const closedAt = closedAtForRow(row);
      if (closedAt) return closedAt > toDate;
      return !isClosedLikeStatus(row.status_label);
    }
    function getOverviewDateRange() {
      const fromInput = document.getElementById('overview-date-from');
      const toInput = document.getElementById('overview-date-to');
      let fromDate = parseDate(fromInput.value);
      let toDate = parseDate(toInput.value);
      if (!fromDate || !toDate) {
        const now = new Date();
        fromDate = startOfWeek(now);
        toDate = endOfWeek(now);
      }
      if (fromDate > toDate) {
        const swap = fromDate;
        fromDate = toDate;
        toDate = swap;
      }
      fromDate.setHours(0, 0, 0, 0);
      toDate.setHours(23, 59, 59, 999);
      return { fromDate, toDate };
    }
    function buildDailyBuckets(fromDate, toDate) {
      const buckets = [];
      const cursor = new Date(fromDate);
      cursor.setHours(0, 0, 0, 0);
      while (cursor <= toDate) {
        buckets.push({
          key: formatDateInputValue(cursor),
          label: cursor.toLocaleDateString(undefined, { weekday: 'short' }),
          fullLabel: cursor.toLocaleDateString(undefined, { day: 'numeric', month: 'short' }),
          opened: 0,
          closed: 0,
        });
        cursor.setDate(cursor.getDate() + 1);
      }
      return buckets;
    }
    function buildLineChartSvg(pointsA, pointsB, maxValue) {
      const width = 640;
      const height = 220;
      const padding = 20;
      const chartHeight = height - (padding * 2);
      const chartWidth = width - (padding * 2);
      const pointGap = pointsA.length > 1 ? chartWidth / (pointsA.length - 1) : 0;
      const toY = (value) => height - padding - ((value / Math.max(1, maxValue)) * chartHeight);
      const toX = (index) => padding + (pointGap * index);
      const pathFor = (points) => points.map((value, index) => `${index === 0 ? 'M' : 'L'} ${toX(index).toFixed(1)} ${toY(value).toFixed(1)}`).join(' ');
      const circlesFor = (points, color) => points.map((value, index) => `<circle cx="${toX(index).toFixed(1)}" cy="${toY(value).toFixed(1)}" r="4" fill="${color}"></circle>`).join('');
      const grid = Array.from({ length: 4 }, (_, index) => {
        const y = padding + ((chartHeight / 3) * index);
        return `<line x1="${padding}" y1="${y.toFixed(1)}" x2="${(width - padding).toFixed(1)}" y2="${y.toFixed(1)}" stroke="#334155" stroke-width="1"></line>`;
      }).join('');
      return `<svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none">${grid}<path d="${pathFor(pointsA)}" fill="none" stroke="#2563eb" stroke-width="3" stroke-linecap="round"></path><path d="${pathFor(pointsB)}" fill="none" stroke="#22c55e" stroke-width="3" stroke-linecap="round"></path>${circlesFor(pointsA, '#2563eb')}${circlesFor(pointsB, '#22c55e')}</svg>`;
    }
    function isSopPendingRow(row) {
      return String(row.status_label || '').trim() === 'Pending';
    }
    function scoreSnapshotRows(rowsToScore) {
      return rowsToScore.map((row) => {
        const staleHours = hoursSinceLastUpdate(row) || 0;
        let reason = row.sop_action || 'Review next step';
        let severity = 0;
        if (row.sla_red_flag || row.ack_state === 'breached' || row.resolution_state === 'breached' || row.max_timeline_state === 'breached') { reason = 'SLA breach needs immediate action'; severity = 4; }
        else if (row.reply_highlight === 'overdue') { reason = 'Customer is waiting for a reply'; severity = 3; }
        else if (['Pending At Tech', 'To be Picked in Next TS'].includes(String(row.status_label || '').trim())) { reason = 'Pending technical acceptance'; severity = 2; }
        else if (staleHours >= 48) { reason = `No update for ${Math.round(staleHours)}h`; severity = 2; }
        else if (row.sla_amber_flag) { reason = 'At risk of delay'; severity = 1; }
        return {
          row: { ...row, _overview_reason: reason, _next_agenda: suggestNextAgenda(row), _stale_hours: staleHours },
          staleHours,
          reason,
          severity,
        };
      }).sort((left, right) => right.severity - left.severity || right.staleHours - left.staleHours || Number(right.row.ticket_id || 0) - Number(left.row.ticket_id || 0));
    }
    function overviewMetricFilterRows(rowsToFilter, metricKey, fromDate, toDate) {
      if (!metricKey || metricKey === 'tickets') return rowsToFilter;
      if (metricKey === 'tickets_this_week' || metricKey === 'created_in_range') {
        return rowsToFilter.filter((row) => {
          const createdAt = parseDate(row.created_at);
          return createdAt && createdAt >= fromDate && createdAt <= toDate;
        });
      }
      if (metricKey === 'pending_from_previous') return rowsToFilter.filter((row) => isPendingAtStart(row, fromDate));
      if (metricKey === 'closed_in_range') {
        return rowsToFilter.filter((row) => {
          const closedAt = closedAtForRow(row);
          return closedAt && closedAt >= fromDate && closedAt <= toDate;
        });
      }
      if (metricKey === 'pending_at_end') return rowsToFilter.filter((row) => isPendingAtEnd(row, toDate));
      const openCreatedRows = rowsToFilter.filter((row) => {
        const createdAt = parseDate(row.created_at);
        return createdAt && createdAt >= fromDate && createdAt <= toDate && !isClosedLikeStatus(row.status_label);
      });
      if (metricKey === 'customer_waiting') return openCreatedRows.filter((row) => row.reply_highlight === 'overdue' || row.reply_highlight === 'waiting');
      if (metricKey === 'red_flags') return openCreatedRows.filter((row) => row.sla_red_flag);
      return rowsToFilter;
    }
    function computeRangeStats(sourceRows, fromDate, toDate) {
      const createdRows = sourceRows.filter((row) => {
        const createdAt = parseDate(row.created_at);
        return Boolean(createdAt && createdAt >= fromDate && createdAt <= toDate);
      });
      const pendingFromPreviousRows = sourceRows.filter((row) => isPendingAtStart(row, fromDate));
      const closedRows = sourceRows.filter((row) => {
        const closedAt = closedAtForRow(row);
        return Boolean(closedAt && closedAt >= fromDate && closedAt <= toDate);
      });
      const pendingAtEndRows = sourceRows.filter((row) => isPendingAtEnd(row, toDate));
      return { createdRows, pendingFromPreviousRows, closedRows, pendingAtEndRows };
    }
    function categoryBucket(type) {
      const normalized = String(type || '').toLowerCase();
      if (normalized.includes('bug')) return 'bug';
      if (normalized.includes('feature')) return 'feature';
      if (normalized.includes('task')) return 'task';
      return 'other';
    }
    function overviewComparisonRange(fromDate, toDate) {
      const asOf = parseDate(summary.generated_at) || new Date();
      const effectiveEnd = new Date(Math.min(toDate.getTime(), asOf.getTime()));
      const fullMonth = sameDateValue(fromDate, startOfMonth(fromDate)) && sameDateValue(toDate, endOfMonth(fromDate));
      let previousStart, previousEnd;
      if (fullMonth) {
        previousStart = new Date(fromDate.getFullYear(), fromDate.getMonth() - 1, 1);
        previousEnd = endOfMonth(previousStart);
      } else {
        previousEnd = addDays(fromDate, -1);
        previousEnd.setHours(23, 59, 59, 999);
        const days = Math.round((Date.UTC(toDate.getFullYear(), toDate.getMonth(), toDate.getDate()) - Date.UTC(fromDate.getFullYear(), fromDate.getMonth(), fromDate.getDate())) / 86400000) + 1;
        previousStart = addDays(fromDate, -days);
      }
      const partial = effectiveEnd < toDate;
      if (partial) previousEnd = new Date(Math.min(previousEnd.getTime(), previousStart.getTime() + Math.max(0, effectiveEnd - fromDate)));
      return { fromDate: previousStart, toDate: previousEnd, effectiveEnd, partial, label:fullMonth ? 'vs previous month' : Math.round((toDate - fromDate) / 86400000) === 7 ? 'vs previous week' : 'vs previous period' };
    }
    function formatOverviewDate(date) {
      return date.toLocaleDateString('en-GB', { day:'numeric', month:'short', year:'numeric' });
    }
    function buildTicketBreakdown(newRows, previousRows) {
      const typeLabel = (row) => String(row.type || '').trim() || 'Unspecified';
      const counts = new Map();
      newRows.forEach((row) => counts.set(typeLabel(row), (counts.get(typeLabel(row)) || 0) + 1));
      previousRows.forEach((row) => { if (!counts.has(typeLabel(row))) counts.set(typeLabel(row), 0); });
      const types = [...counts.entries()].map(([label, count]) => ({ label, count,
        previous:previousRows.filter((row) => typeLabel(row) === label).length,
        share:newRows.length ? Math.round(count / newRows.length * 100) : 0,
      })).sort((a,b) => b.count - a.count || a.label.localeCompare(b.label));
      const companies = new Map();
      const unknownRows = [];
      newRows.forEach((row) => {
        const label = String(row.company_name || '').trim();
        if (!label || label.toLowerCase() === 'unknown') { unknownRows.push(row); return; }
        if (!companies.has(label)) companies.set(label, {label, total:0, byType:new Map()});
        const company = companies.get(label);
        company.total += 1;
        company.byType.set(typeLabel(row), (company.byType.get(typeLabel(row)) || 0) + 1);
      });
      const rankedCompanies = [...companies.values()].sort((a,b) => b.total - a.total || a.label.localeCompare(b.label));
      return { types, companies:rankedCompanies, unknownRows, total:newRows.length };
    }
    function breakdownTicketRows(selection, newRows, closedRows) {
      if (!selection) return [];
      if (selection.kind === 'company') return newRows.filter((row) => String(row.company_name || '').trim() === selection.value);
      if (selection.kind === 'company_type') return newRows.filter((row) => String(row.company_name || '').trim() === selection.value && (String(row.type || '').trim() || 'Unspecified') === selection.type);
      if (selection.kind === 'type') return newRows.filter((row) => (String(row.type || '').trim() || 'Unspecified') === selection.value);
      if (selection.kind === 'unknown_company') return newRows.filter((row) => !String(row.company_name || '').trim() || String(row.company_name).trim().toLowerCase() === 'unknown');
      if (selection.kind === 'day_new') return newRows.filter((row) => formatDateInputValue(parseDate(row.created_at)) === selection.value);
      if (selection.kind === 'day_closed') return closedRows.filter((row) => formatDateInputValue(closedAtForRow(row)) === selection.value);
      return [];
    }
    function buildOverviewStats() {
      const { fromDate, toDate } = getOverviewDateRange();
      const weekDays = buildDailyBuckets(fromDate, toDate);
      const selectedTypes = selectedValues('overview-filter-type');
      const filteredRows = rows.filter((row) => matchesMulti(row.type, selectedTypes) && matchesDealStage(row));
      const comparison = overviewComparisonRange(fromDate, toDate);
      const rangeStats = computeRangeStats(filteredRows, fromDate, comparison.effectiveEnd);
      const previousStats = computeRangeStats(filteredRows, comparison.fromDate, comparison.toDate);
      const inRangeRows = rangeStats.createdRows;
      const pendingFromPreviousRows = rangeStats.pendingFromPreviousRows;
      const closedInRangeRows = rangeStats.closedRows;
      const activeRows = rangeStats.pendingAtEndRows;
      inRangeRows.forEach((row) => {
        const createdAt = parseDate(row.created_at);
        if (!createdAt) return;
        const bucket = weekDays.find((item) => item.key === formatDateInputValue(createdAt));
        if (bucket) bucket.opened += 1;
      });
      closedInRangeRows.forEach((row) => {
        const closedAt = closedAtForRow(row);
        if (!closedAt) return;
        const closedBucket = weekDays.find((item) => item.key === formatDateInputValue(closedAt));
        if (closedBucket) closedBucket.closed += 1;
      });
      const metrics = [
        { key:'pending_from_previous', label:'Open at start', value:pendingFromPreviousRows.length, previous:previousStats.pendingFromPreviousRows.length, detail:'Tickets already open before this period', className:'' },
        { key:'created_in_range', label:'New tickets', value:inRangeRows.length, previous:previousStats.createdRows.length, detail:'Tickets created during this period', className:'metric-created' },
        { key:'closed_in_range', label:'Closed tickets', value:closedInRangeRows.length, previous:previousStats.closedRows.length, detail:'Resolved or closed during this period', className:'metric-closed' },
        { key:'pending_at_end', label:'Open at end', value:activeRows.length, previous:previousStats.pendingAtEndRows.length, detail:comparison.partial ? 'Still open as of the latest refresh' : 'Tickets still open when this period ended', className:'' },
      ];
      const attentionMetrics = [
        { key:'customer_waiting', label:'Awaiting reply', detail:'Reply to the customer on the same ticket', className:'metric-waiting' },
        { key:'red_flags', label:'SLA breached', detail:'Review the delay and decide the next step', className:'metric-breached' },
      ].map((metric) => ({ ...metric, value:overviewMetricFilterRows(filteredRows, metric.key, fromDate, comparison.effectiveEnd).length }));
      const selectedMetric = breakdownSelection
        ? {key:'breakdown', label:breakdownSelection.label, detail:'Tickets matching the selected breakdown and current filters'}
        : [...metrics, ...attentionMetrics].find((metric) => metric.key === activeCardFilters.overview);
      const selectedRows = breakdownSelection ? breakdownTicketRows(breakdownSelection, inRangeRows, closedInRangeRows)
        : selectedMetric ? overviewMetricFilterRows(filteredRows, selectedMetric.key, fromDate, comparison.effectiveEnd) : [];
      const breakdown = buildTicketBreakdown(inRangeRows, previousStats.createdRows);
      const topCompanies = breakdown.companies.slice(0, 10);
      return { weekDays, metrics, attentionMetrics, selectedMetric, selectedRows, fromDate, toDate, comparison, topCompanies, breakdown };
    }

    function renderOverview() {
      const overview = buildOverviewStats();
      const number = (value) => Number(value).toLocaleString('en-IN');
      const comparisonLabel = overview.comparison.label;
      document.getElementById('overview-helper').textContent = `${formatOverviewDate(overview.fromDate)} – ${formatOverviewDate(overview.toDate)}${overview.comparison.partial ? ' · Progress so far' : ''}. Comparison: ${formatOverviewDate(overview.comparison.fromDate)} – ${formatOverviewDate(overview.comparison.toDate)}${overview.comparison.partial ? ' (same elapsed time)' : ''}. Click a card to see its tickets.`;
      const now = new Date();
      const lastWeek = addDays(startOfWeek(now), -1);
      const lastMonth = new Date(now.getFullYear(), now.getMonth() - 1, 1);
      const presets = [
        ['overview-current-week', startOfWeek(now), endOfWeek(now)],
        ['overview-last-week', startOfWeek(lastWeek), endOfWeek(lastWeek)],
        ['overview-current-month', startOfMonth(now), endOfMonth(now)],
        ['overview-last-month', startOfMonth(lastMonth), endOfMonth(lastMonth)],
      ];
      presets.forEach(([id, start, end]) => document.getElementById(id).setAttribute('aria-pressed', String(sameDateValue(start, overview.fromDate) && sameDateValue(end, overview.toDate))));
      const cardHtml = (metric, compare) => {
        const delta = metric.value - metric.previous;
        const deltaText = delta === 0 ? 'No change' : `${delta > 0 ? '+' : '−'}${number(Math.abs(delta))}`;
        return `<div class="overview-card ${metric.className} ${activeCardFilters.overview === metric.key ? 'active' : ''}">
          <button class="overview-card-button" type="button" data-overview-card="${metric.key}" aria-pressed="${activeCardFilters.overview === metric.key}">
            <div class="label">${escapeHtml(metric.label)}</div>
            <div class="metric-line"><span class="value">${number(metric.value)}</span>${compare ? `<span class="metric-comparison"><strong>${deltaText}</strong><br>${comparisonLabel}</span>` : ''}</div>
            <div class="subvalue">${escapeHtml(metric.detail)}</div>
          </button>
        </div>`;
      };
      document.getElementById('overview-metrics').innerHTML = overview.metrics.map((metric) => cardHtml(metric, true)).join('');
      document.getElementById('overview-attention-metrics').innerHTML = overview.attentionMetrics.map((metric) => cardHtml(metric, false)).join('');
      document.querySelectorAll('[data-overview-card]').forEach((button) => {
        button.addEventListener('click', () => {
          breakdownSelection = null;
          const metricKey = button.getAttribute('data-overview-card');
          activeCardFilters.overview = activeCardFilters.overview === metricKey ? 'tickets' : metricKey;
          render();
          document.querySelector(`[data-overview-card="${metricKey}"]`)?.focus({ preventScroll:true });
          if (activeCardFilters.overview !== 'tickets') document.getElementById('overview-results').scrollIntoView({ behavior:'smooth', block:'start' });
        });
      });
      document.getElementById('overview-results').classList.toggle('hidden', !overview.selectedMetric);
      if (overview.selectedMetric) {
        document.getElementById('overview-results-title').textContent = `${overview.selectedMetric.label} · ${number(overview.selectedRows.length)} tickets`;
        document.getElementById('overview-results-copy').textContent = ['customer_waiting', 'red_flags'].includes(overview.selectedMetric.key)
          ? 'Currently open tickets created in the selected dates. Open a ticket to follow up; use Next Agenda to plan your action.'
          : `${overview.selectedMetric.detail}. Open a ticket to review its progress and plan the next step.`;
        renderDynamicTable('overview', 'overview-results-head', 'overview-results-body', overview.selectedRows);
      }
      renderTicketBreakdown(overview);
    }
    function renderTicketBreakdown(overview) {
      const breakdown = overview.breakdown;
      const number = (value) => Number(value).toLocaleString('en-IN');
      const countButton = (count, kind, value, label, type = '') => count
        ? `<button type="button" class="breakdown-count" data-breakdown-kind="${kind}" data-breakdown-value="${escapeHtml(value)}" data-breakdown-type="${escapeHtml(type)}" data-breakdown-label="${escapeHtml(label)}" aria-label="${escapeHtml(label)}: ${number(count)} tickets">${number(count)}</button>` : '0';
      const colors = ['#38bdf8', '#fbbf24', '#fb7185', '#a78bfa', '#34d399', '#f472b6'];
      const colorForType = (label) => ({'Bug':'#fb7185','Feature Idea':'#a78bfa','Task - Backend':'#38bdf8','Task - Experience Team':'#fbbf24','Unspecified':'#94a3b8'}[label] || colors[[...label].reduce((value, char) => value + char.charCodeAt(0), 0) % colors.length]);
      const attrs = (kind, value, label, type = '') => `data-breakdown-kind="${kind}" data-breakdown-value="${escapeHtml(value)}" data-breakdown-type="${escapeHtml(type)}" data-breakdown-label="${escapeHtml(label)}"`;
      const segment = (count, total, color, kind, value, label, type = '') => count
        ? `<button type="button" class="ticket-bar-segment" style="width:${count / total * 100}%;background:${color}" ${attrs(kind, value, label, type)} title="${escapeHtml(label)}: ${number(count)}" aria-label="${escapeHtml(label)}: ${number(count)} tickets"></button>` : '';
      const axis = (label, max, paired = false) => `<div class="chart-axis ${paired ? 'paired-chart-axis' : ''}"><span>${escapeHtml(label)}</span><div class="chart-ticks"><span>0</span><span>${number(max / 2)}</span><span>${number(max)}</span></div><span style="text-align:right">Tickets</span></div>`;
      const positiveTypes = breakdown.types.filter((type) => type.count > 0);
      const chartMax = (values) => Math.max(2, Math.ceil(Math.max(0, ...values) / 2) * 2);
      const companyMax = chartMax(overview.topCompanies.map((company) => company.total));
      const companyRows = overview.topCompanies.map((company, index) => {
        const label = company.label + ' · New tickets';
        const segments = positiveTypes.map((type) => segment(company.byType.get(type.label) || 0, company.total, colorForType(type.label), 'company_type', company.label, company.label + ' · ' + type.label, type.label)).join('');
        const typeCounts = positiveTypes.filter((type) => company.byType.get(type.label)).map((type) => `<span><span class="chart-type-dot" style="background:${colorForType(type.label)}"></span><button type="button" class="breakdown-count" ${attrs('company_type', company.label, company.label + ' · ' + type.label, type.label)}>${escapeHtml(type.label)} ${number(company.byType.get(type.label))}</button></span>`).join('');
        return `<div class="chart-row"><div class="chart-row-name">${index + 1}. ${escapeHtml(company.label)}</div><div class="chart-row-track"><div class="ticket-bar-track"><div class="ticket-bar-stack" style="width:${company.total / companyMax * 100}%">${segments}</div></div><div class="chart-type-counts">${typeCounts}</div></div><div class="chart-row-value">${countButton(company.total, 'company', company.label, label)}</div></div>`;
      }).join('');
      const legend = `<div class="chart-legend">${positiveTypes.map((type) => `<span><span class="chart-type-dot" style="background:${colorForType(type.label)}"></span>${escapeHtml(type.label)}</span>`).join('')}</div>`;
      const unknownNote = breakdown.unknownRows.length ? `<p class="breakdown-note">Company not identified: ${countButton(breakdown.unknownRows.length, 'unknown_company', '', 'Company not identified · New tickets')}</p>` : '';
      document.getElementById('overview-company-bars').innerHTML = (companyRows ? legend + `<div class="ticket-chart">${axis('Company', companyMax)}${companyRows}</div>` : '<p class="breakdown-note">No company data for these filters.</p>') + unknownNote;
      const pairedBar = (count, max, color, series, kind, value, label) => `<div class="paired-bar"><span>${series}</span><div class="ticket-bar-track"><div class="ticket-bar-stack" style="width:${count / max * 100}%">${kind ? segment(count, count || 1, color, kind, value, label) : `<span style="width:100%;background:${color}" title="${escapeHtml(label)}: ${number(count)}"></span>`}</div></div><span style="text-align:right">${kind ? countButton(count, kind, value, label) : number(count)}</span></div>`;
      const typeMax = chartMax(breakdown.types.flatMap((type) => [type.count, type.previous]));
      const typeRows = breakdown.types.map((type) => `<div class="chart-row"><div class="chart-row-name"><span class="chart-type-dot" style="background:${colorForType(type.label)}"></span>${escapeHtml(type.label)}</div><div class="chart-row-track">${pairedBar(type.count, typeMax, colorForType(type.label), 'Current', 'type', type.label, type.label + ' · New tickets')}${pairedBar(type.previous, typeMax, '#64748b', 'Previous', '', '', type.label + ' · Previous period')}</div><div class="chart-row-value"><span class="chart-share">${type.share}%<br>of new</span></div></div>`).join('');
      document.getElementById('overview-category-bars').innerHTML = typeRows ? `<div class="ticket-chart">${axis('Ticket type', typeMax, true)}${typeRows}</div>` : '<p class="breakdown-note">No tickets for these filters.</p>';
      const days = overview.weekDays.filter((day) => day.key <= formatDateInputValue(overview.comparison.effectiveEnd));
      const dailyMax = chartMax(days.flatMap((day) => [day.opened, day.closed]));
      const dailyRows = days.map((day) => `<div class="chart-row"><div class="chart-row-name">${escapeHtml(day.label)} ${escapeHtml(day.fullLabel)}</div><div class="chart-row-track">${pairedBar(day.opened, dailyMax, '#60a5fa', 'New', 'day_new', day.key, day.fullLabel + ' · New tickets')}${pairedBar(day.closed, dailyMax, '#34d399', 'Closed', 'day_closed', day.key, day.fullLabel + ' · Closed tickets')}</div><div></div></div>`).join('');
      document.getElementById('overview-daily-activity').innerHTML = dailyRows ? `<div class="ticket-chart">${axis('Date', dailyMax, true)}${dailyRows}</div>` : '<p class="breakdown-note">No elapsed dates in this range yet.</p>';
      document.querySelectorAll('[data-breakdown-kind]').forEach((button) => {
        button.addEventListener('click', () => {
          breakdownSelection = {kind:button.getAttribute('data-breakdown-kind'), value:button.getAttribute('data-breakdown-value'), type:button.getAttribute('data-breakdown-type'), label:button.getAttribute('data-breakdown-label')};
          activeCardFilters.overview = 'tickets';
          render();
          document.getElementById('overview-results-title').focus({preventScroll:true});
          document.getElementById('overview-results').scrollIntoView({behavior:'smooth', block:'start'});
        });
      });
    }

    function renderCheckboxGroup(containerId, options, allLabel, searchable = false) {
      const container = document.getElementById(containerId);
      const searchHtml = searchable ? `<input class="filter-search" type="search" placeholder="Search..." data-filter-search="${containerId}">` : '';
      container.innerHTML = [searchHtml, `<label class="filter-item"><input type="checkbox" value="all" checked> ${allLabel}</label>`, ...options.map((value) => `<label class="filter-item"><input type="checkbox" value="${escapeHtml(value)}"> ${escapeHtml(value)}</label>`)].join('');
    }
    function selectedValues(elementId) { return Array.from(document.querySelectorAll(`#${elementId} input:checked`)).map((input) => input.value); }
    function matchesMulti(value, selections) { return selections.length === 0 || selections.includes('all') || selections.includes(value || ''); }
    function isoDateOnly(value) {
      return String(value || '').slice(0, 10);
    }
    function matchesDate(value, fromValue, toValue) {
      const dateOnly = isoDateOnly(value);
      if (!dateOnly) return !(fromValue || toValue);
      if (fromValue && dateOnly < fromValue) return false;
      if (toValue && dateOnly > toValue) return false;
      return true;
    }
    function replyPill(row) {
      const label = row.reply_highlight === 'overdue' ? `Overdue${row.reply_age_hours ? ` (${row.reply_age_hours}h)` : ''}` : row.reply_highlight === 'waiting' ? `Waiting${row.reply_age_hours ? ` (${row.reply_age_hours}h)` : ''}` : 'OK';
      return `<span class="pill ${row.reply_highlight}">${label}</span>`;
    }
    function dealStageKey(row) {
      return row.hubspot_deal_stage || (row.hubspot_deal_url ? 'stage_unavailable' : 'no_linked_deal');
    }
    function dealStageLabel(row) {
      return row.hubspot_deal_stage_label || (row.hubspot_deal_url ? 'Stage unavailable' : 'No linked deal');
    }
    function matchesDealStage(row) {
      const selected = document.getElementById('deal-stage-filter').value;
      return !selected || selected === 'all' || dealStageKey(row) === selected;
    }
    function renderDealStageFilter() {
      const stages = new Map(rows.map((row) => [dealStageKey(row), dealStageLabel(row)]));
      const options = [...stages.entries()].sort((left, right) => left[1].localeCompare(right[1]));
      document.getElementById('deal-stage-filter').innerHTML = '<option value="all">All deal stages</option>' + options.map(([value, label]) => `<option value="${escapeHtml(value)}">${escapeHtml(label)}</option>`).join('');
    }
    function hubspotDealCell(row) {
      if (!row.hubspot_deal_url) return '<span class="small">—</span>';
      const suffix = row.hubspot_deal_count > 1 ? ` (+${row.hubspot_deal_count - 1})` : '';
      return `<a href="${escapeHtml(row.hubspot_deal_url)}" target="_blank" rel="noreferrer">Open Deal</a><div class="small">HubSpot${suffix}</div>`;
    }
    function yesNoUnknown(value, checked) {
      if (!checked) return '<span class="small">Not checked</span>';
      return value ? 'Yes' : 'No';
    }
    function hoursSinceLastUpdate(row) {
      const updatedAt = parseDate(row.updated_at || row.last_activity_at);
      if (!updatedAt) return null;
      return Math.round(((Date.now() - updatedAt.getTime()) / 3600000) * 10) / 10;
    }
    function overviewBottleneckText(row) {
      if (row.sla_red_flag || row.ack_state === 'breached' || row.resolution_state === 'breached' || row.max_timeline_state === 'breached') return 'SLA breach / red flag';
      if (row.sla_amber_flag) return 'At risk of breach';
      if (row.reply_highlight === 'overdue') return 'Customer waiting for reply';
      if (row.reply_highlight === 'waiting') return 'Reply follow-up pending';
      if (['Pending At Tech', 'To be Picked in Next TS'].includes(String(row.status_label || '').trim())) return 'Pending technical acceptance';
      const staleHours = hoursSinceLastUpdate(row);
      if (staleHours !== null && staleHours >= 48) return `No update for ${staleHours}h`;
      return row.last_action_label || row.last_activity_type || 'In progress';
    }
    function suggestNextAgenda(row) {
      if (row.sla_red_flag || row.ack_state === 'breached' || row.resolution_state === 'breached') return 'Escalate and share ETA';
      if (row.sla_amber_flag) return 'Review risk and re-align owner';
      if (row.reply_highlight === 'overdue' || row.reply_highlight === 'waiting') return 'Reply on the same ticket';
      if (['Pending At Tech', 'To be Picked in Next TS'].includes(String(row.status_label || '').trim())) return 'Push tech acceptance';
      const staleHours = hoursSinceLastUpdate(row);
      if (staleHours !== null && staleHours >= 48) return 'Follow up with owner';
      return row.sop_action || 'Monitor progress';
    }
    function slaPill(state, label) { return `<span class="pill ${state || 'na'}">${label || 'NA'}</span>`; }
    function matchesSla(row, selections) {
      if (selections.length === 0 || selections.includes('all')) return true;
      const states = [];
      if (row.sla_red_flag) states.push('red_flag');
      if (row.sla_amber_flag) states.push('at_risk');
      if (row.ack_state === 'breached') states.push('ack_breached');
      if (row.resolution_state === 'breached') states.push('resolution_breached');
      if (row.max_timeline_state === 'breached') states.push('max_timeline_breached');
      return selections.some((item) => states.includes(item));
    }
    function matchesCardFilter(row, view, cardFilter) {
      if (!cardFilter || cardFilter === 'tickets') return true;
      if (view === 'activity') {
        if (cardFilter === 'customer_reverted') return Boolean(row.customer_reverted);
        if (cardFilter === 'overdue_replies') return row.reply_highlight === 'overdue';
        if (cardFilter === 'waiting_replies') return row.reply_highlight === 'waiting';
        if (cardFilter === 'tagged') return Boolean(row.has_customer_revert_tag);
        return true;
      }
      if (cardFilter === 'red_flags') return Boolean(row.sla_red_flag);
      if (cardFilter === 'at_risk') return Boolean(row.sla_amber_flag);
      if (cardFilter === 'ack_breaches') return row.ack_state === 'breached';
      if (cardFilter === 'resolution_breaches') return row.resolution_state === 'breached';
      return true;
    }
    function filteredRows(view, includeCardFilter = true) {
      const query = document.getElementById(view === 'activity' ? 'activity-search' : 'sla-search').value.trim().toLowerCase();
      const highlights = view === 'activity' ? selectedValues('activity-filter-highlight') : [];
      const statuses = selectedValues(view === 'activity' ? 'activity-filter-status' : 'sla-filter-status');
      const types = selectedValues(view === 'activity' ? 'activity-filter-type' : 'sla-filter-type');
      const companies = selectedValues(view === 'activity' ? 'activity-filter-company' : 'sla-filter-company');
      const slaFlags = view === 'sla' ? selectedValues('sla-filter-sla') : [];
      const fromDate = document.getElementById(view === 'activity' ? 'activity-date-from' : 'sla-date-from').value;
      const toDate = document.getElementById(view === 'activity' ? 'activity-date-to' : 'sla-date-to').value;
      return rows.filter((row) => {
        if (!matchesDealStage(row)) return false;
        const haystack = [row.ticket_id, row.subject, row.tags_display, row.timeline_excerpt, row.sla_category, row.sop_action, row.last_activity_user_name, row.created_at_ist, row.closed_at_ist, row.last_activity_at_ist, row.company_name].join(' ').toLowerCase();
        if (query && !haystack.includes(query)) return false;
        if (!matchesMulti(row.status_label, statuses)) return false;
        if (!matchesMulti(row.type, types)) return false;
        if (!matchesMulti(row.company_name || 'Unknown', companies)) return false;
        if (!matchesDate(row.created_at, fromDate, toDate)) return false;
        if (view === 'activity' && highlights.length && !highlights.includes('all')) {
          const highlightValue = row.customer_reverted ? 'customer_reverted' : row.reply_highlight;
          if (!highlights.includes(row.reply_highlight) && !highlights.includes(highlightValue)) return false;
        }
        if (view === 'sla' && !matchesSla(row, slaFlags)) return false;
        if (includeCardFilter && !matchesCardFilter(row, view, activeCardFilters[view])) return false;
        return true;
      });
    }
    function renderCards(activityRows, slaRows) {
      if (currentView === 'overview') {
        document.getElementById('cards').innerHTML = '';
        return;
      }
      const cards = currentView === 'activity'
        ? [
            ['tickets', 'Tickets', activityRows.length],
            ['customer_reverted', 'Customer Reverted', activityRows.filter((row) => row.customer_reverted).length],
            ['overdue_replies', 'Overdue Replies', activityRows.filter((row) => row.reply_highlight === 'overdue').length],
            ['waiting_replies', 'Waiting Replies', activityRows.filter((row) => row.reply_highlight === 'waiting').length],
            ['tagged', 'Tagged', activityRows.filter((row) => row.has_customer_revert_tag).length],
          ]
        : [
            ['tickets', 'Tickets', slaRows.length],
            ['red_flags', 'Red Flags', slaRows.filter((row) => row.sla_red_flag).length],
            ['at_risk', 'At Risk', slaRows.filter((row) => row.sla_amber_flag).length],
            ['ack_breaches', 'Ack Breaches', slaRows.filter((row) => row.ack_state === 'breached').length],
            ['resolution_breaches', 'Resolution Breaches', slaRows.filter((row) => row.resolution_state === 'breached').length],
          ];
      document.getElementById('cards').innerHTML = cards.map(([key, label, value]) => `<div class="card"><button class="card-button ${activeCardFilters[currentView] === key ? 'active' : ''}" type="button" data-card-filter="${key}"><div class="label">${label}</div><div class="value">${value ?? 0}</div></button></div>`).join('');
      document.querySelectorAll('#cards [data-card-filter]').forEach((button) => {
        button.addEventListener('click', () => {
          const nextFilter = button.getAttribute('data-card-filter') || 'tickets';
          activeCardFilters[currentView] = activeCardFilters[currentView] === nextFilter ? 'tickets' : nextFilter;
          render();
        });
      });
    }
    function render() {
      const baseActivityRows = filteredRows('activity', false);
      const baseSlaRows = filteredRows('sla', false);
      const activityRows = filteredRows('activity');
      const slaRows = filteredRows('sla');
      const slaSnapshotRows = scoreSnapshotRows(slaRows).slice(0, 30).map((item) => item.row);
      renderCards(baseActivityRows, baseSlaRows);
      renderOverview();
      renderDynamicTable('activity', 'activity-table-head', 'activity-table-body', activityRows);
      renderDynamicTable('sla', 'sla-table-head', 'sla-table-body', slaRows);
      renderDynamicTable('overview', 'overview-table-head', 'overview-table-body', slaSnapshotRows);
    }
    function wireGroup(containerId) {
      document.getElementById(containerId).addEventListener('change', (event) => {
        const target = event.target; if (!(target instanceof HTMLInputElement)) return;
        const allInput = document.querySelector(`#${containerId} input[value="all"]`);
        const others = Array.from(document.querySelectorAll(`#${containerId} input:not([value="all"])`));
        if (target.value === 'all' && target.checked) others.forEach((input) => { input.checked = false; });
        else if (target.value !== 'all' && target.checked && allInput) allInput.checked = false;
        if (allInput && others.every((input) => !input.checked)) allInput.checked = true;
        render();
      });
    }
    function wireColumnPicker(view) {
      const container = document.getElementById(columnPickerIds[view]);
      if (!container) return;
      container.addEventListener('change', (event) => {
        const target = event.target;
        if (!(target instanceof HTMLInputElement)) return;
        const selected = new Set(columnState[view] || []);
        if (target.checked) selected.add(target.value);
        else selected.delete(target.value);
        const available = (columnConfigs[view] || []).map((column) => column.id);
        if (!selected.size && available.length) selected.add(available[0]);
        columnState[view] = available.filter((columnId) => selected.has(columnId));
        saveColumnState();
        renderColumnPicker(view);
        render();
      });
    }
    function wireFilterSearch(containerId) {
      const searchInput = document.querySelector(`#${containerId} [data-filter-search="${containerId}"]`);
      if (!searchInput) return;
      searchInput.addEventListener('input', () => {
        const query = searchInput.value.trim().toLowerCase();
        document.querySelectorAll(`#${containerId} .filter-item`).forEach((item) => {
          const input = item.querySelector('input');
          if (!input || input.value === 'all') return;
          const matches = item.textContent.toLowerCase().includes(query);
          item.classList.toggle('hidden-item', !matches);
        });
      });
    }
    function wireDatePicker(inputId) {
      const input = document.getElementById(inputId);
      if (!(input instanceof HTMLInputElement)) return;
      const openPicker = () => {
        if (typeof input.showPicker === 'function') input.showPicker();
      };
      input.addEventListener('focus', openPicker);
      input.addEventListener('click', openPicker);
    }
    function setView(view) {
      currentView = view;
      document.getElementById('activity-view').classList.toggle('hidden', view !== 'activity');
      document.getElementById('sla-view').classList.toggle('hidden', view !== 'sla');
      document.getElementById('overview-view').classList.toggle('hidden', view !== 'overview');
      document.getElementById('tab-activity').classList.toggle('active', view === 'activity');
      document.getElementById('tab-sla').classList.toggle('active', view === 'sla');
      document.getElementById('tab-overview').classList.toggle('active', view === 'overview');
      render();
    }
    renderDealStageFilter();
    document.getElementById('deal-stage-filter').addEventListener('change', render);
    document.getElementById('meta').textContent = `Updated since ${summary.updated_since} • Generated at ${summary.generated_at} • Total tickets ${summary.total_tickets}`;
    document.getElementById('notice').textContent = __NOTICE__;
    renderCheckboxGroup('activity-filter-highlight', ['overdue', 'waiting', 'customer_reverted'], 'All reply states');
    renderCheckboxGroup('activity-filter-status', [...new Set(rows.map((row) => row.status_label).filter(Boolean))].sort(), 'All statuses');
    renderCheckboxGroup('activity-filter-type', [...new Set(rows.map((row) => row.type).filter(Boolean))].sort(), 'All ticket categories');
    renderCheckboxGroup('activity-filter-company', [...new Set(rows.map((row) => row.company_name || 'Unknown').filter(Boolean))].sort(), 'All companies', true);
    renderCheckboxGroup('sla-filter-status', [...new Set(rows.map((row) => row.status_label).filter(Boolean))].sort(), 'All statuses');
    renderCheckboxGroup('sla-filter-type', [...new Set(rows.map((row) => row.type).filter(Boolean))].sort(), 'All ticket categories');
    renderCheckboxGroup('sla-filter-company', [...new Set(rows.map((row) => row.company_name || 'Unknown').filter(Boolean))].sort(), 'All companies', true);
    renderCheckboxGroup('sla-filter-sla', ['red_flag', 'ack_breached', 'resolution_breached', 'max_timeline_breached', 'at_risk'], 'All SLA flags');
    renderCheckboxGroup('overview-filter-type', [...new Set(rows.map((row) => row.type).filter(Boolean))].sort(), 'All ticket categories');
    renderAllColumnPickers();
    ['activity-filter-highlight','activity-filter-status','activity-filter-type','activity-filter-company','sla-filter-status','sla-filter-type','sla-filter-company','sla-filter-sla','overview-filter-type'].forEach(wireGroup);
    ['activity','sla','overview'].forEach(wireColumnPicker);
    ['activity-filter-company','sla-filter-company'].forEach(wireFilterSearch);
    ['activity-date-from','activity-date-to','sla-date-from','sla-date-to','overview-date-from','overview-date-to'].forEach(wireDatePicker);
    document.getElementById('activity-search').addEventListener('input', render);
    document.getElementById('sla-search').addEventListener('input', render);
    document.getElementById('activity-date-from').addEventListener('input', render);
    document.getElementById('activity-date-to').addEventListener('input', render);
    document.getElementById('sla-date-from').addEventListener('input', render);
    document.getElementById('sla-date-to').addEventListener('input', render);
    document.getElementById('overview-date-from').addEventListener('input', render);
    document.getElementById('overview-date-to').addEventListener('input', render);
    document.getElementById('overview-clear-selection').addEventListener('click', () => { activeCardFilters.overview = 'tickets'; breakdownSelection = null; render(); });
    document.getElementById('overview-current-week').addEventListener('click', () => {
      const now = new Date();
      document.getElementById('overview-date-from').value = formatDateInputValue(startOfWeek(now));
      document.getElementById('overview-date-to').value = formatDateInputValue(endOfWeek(now));
      render();
    });
    document.getElementById('overview-last-week').addEventListener('click', () => {
      const now = new Date();
      const lastWeekEnd = addDays(startOfWeek(now), -1);
      const lastWeekStart = startOfWeek(lastWeekEnd);
      document.getElementById('overview-date-from').value = formatDateInputValue(lastWeekStart);
      document.getElementById('overview-date-to').value = formatDateInputValue(endOfWeek(lastWeekStart));
      render();
    });
    document.getElementById('overview-current-month').addEventListener('click', () => {
      const now = new Date();
      const start = startOfMonth(now);
      const end = endOfMonth(now);
      document.getElementById('overview-date-from').value = formatDateInputValue(start);
      document.getElementById('overview-date-to').value = formatDateInputValue(end);
      render();
    });
    document.getElementById('overview-last-month').addEventListener('click', () => {
      const now = new Date();
      const previousMonth = new Date(now.getFullYear(), now.getMonth() - 1, 1);
      const start = startOfMonth(previousMonth);
      const end = endOfMonth(previousMonth);
      document.getElementById('overview-date-from').value = formatDateInputValue(start);
      document.getElementById('overview-date-to').value = formatDateInputValue(end);
      render();
    });
    document.getElementById('tab-activity').addEventListener('click', () => setView('activity'));
    document.getElementById('tab-sla').addEventListener('click', () => setView('sla'));
    document.getElementById('tab-overview').addEventListener('click', () => setView('overview'));
    __REFRESH_JS__
    document.getElementById('overview-date-from').value = formatDateInputValue(startOfWeek(new Date()));
    document.getElementById('overview-date-to').value = formatDateInputValue(endOfWeek(new Date()));
    setView('overview');
  </script>
</body>
</html>
"""
    refresh_button = '<button class="primary" id="refresh-btn">Refresh Now</button>' if refresh_enabled else ''
    csv_button = f'<a class="btn" href="{csv_url}">Download CSV</a>' if csv_url else ''
    json_button = f'<a class="btn" href="{json_url}">Open JSON</a>' if json_url else ''
    refresh_js = """
    document.getElementById('refresh-btn').addEventListener('click', async () => {
      const button = document.getElementById('refresh-btn');
      button.disabled = true; button.textContent = 'Refreshing...';
      try {
        const response = await fetch('/refresh', { method: 'POST' });
        const payload = await response.json();
        document.getElementById('notice').textContent = payload.message || payload.detail || 'Refresh finished.';
        window.location.reload();
      } catch (error) {
        document.getElementById('notice').textContent = `Refresh failed: ${error}`;
      } finally {
        button.disabled = false; button.textContent = 'Refresh Now';
      }
    });
    """ if refresh_enabled else ''
    return (
        template.replace('__DATA__', json.dumps({'summary': summary, 'rows': rows}))
        .replace('__NOTICE__', json.dumps(last_refresh_message))
        .replace('__REFRESH__', refresh_button)
        .replace('__CSV__', csv_button)
        .replace('__JSON__', json_button)
        .replace('__REFRESH_JS__', refresh_js)
    )


def build_html(summary: Dict[str, object], rows: Sequence[Dict[str, object]]) -> str:
    return build_dashboard_html({'summary': summary, 'rows': list(rows)})


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Export a Freshdesk activity dashboard with customer revert detection."
    )
    parser.add_argument(
        "--env-file",
        default=str(Path(__file__).with_name(".env")),
        help="Path to env file with FRESHDESK_DOMAIN and FRESHDESK_API_KEY.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(Path(__file__).with_name(DEFAULT_OUTPUT_DIR)),
        help="Directory where HTML, JSON, CSV, and status files will be written.",
    )
    parser.add_argument(
        "--hubspot-env-file",
        default=DEFAULT_HUBSPOT_ENV_FILE,
        help="Path to env file with HUBSPOT_ACCESS_TOKEN, HUBSPOT_PORTAL_ID, and HUBSPOT_UI_DOMAIN.",
    )
    parser.add_argument(
        "--hubspot-state-file",
        default=str(Path(__file__).with_name(".freshdesk_hubspot_sync_state.json")),
        help="Path to the HubSpot sync state file used to map Freshdesk tickets to HubSpot deals.",
    )
    parser.add_argument(
        "--hubspot-deal-url-field",
        default=DEFAULT_HUBSPOT_DEAL_URL_FIELD,
        help="Freshdesk custom field name used to store the HubSpot deal URL.",
    )
    parser.add_argument(
        "--updated-since",
        help="Fetch tickets updated on or after this UTC date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--all-tickets",
        action="store_true",
        help="Fetch the full ticket history instead of a lookback window.",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        help="How many days back to fetch if --updated-since is not provided.",
    )
    parser.add_argument(
        "--reply-sla-hours",
        type=int,
        help="Mark customer replies as overdue after this many hours.",
    )
    parser.add_argument(
        "--max-tickets",
        type=int,
        help="Optional safety limit on the number of tickets to process.",
    )
    parser.add_argument(
        "--customer-revert-tag",
        default=DEFAULT_REVERT_TAG,
        help="Freshdesk tag name used for customer revert detection.",
    )
    parser.add_argument(
        "--sync-customer-revert-tag",
        action="store_true",
        help="Add or remove the customer revert tag in Freshdesk to match dashboard logic.",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        help="How many times to retry Freshdesk API requests on rate limits or transient failures.",
    )
    parser.add_argument(
        "--retry-delay-seconds",
        type=float,
        help="Base delay between Freshdesk API retries.",
    )
    parser.add_argument(
        "--recent-activity-days",
        type=int,
        help="Legacy option retained for compatibility; full conversation checks now use cached per-ticket results.",
    )
    parser.add_argument(
        "--no-conversations",
        action="store_true",
        help="Build the dashboard from the ticket list only, without loading ticket conversations.",
    )
    parser.add_argument(
        "--conversation-workers",
        type=int,
        help="How many tickets to process in parallel when fetching conversation history.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    env = load_env(Path(args.env_file))

    domain = env_or_file("FRESHDESK_DOMAIN", env)
    api_key = env_or_file("FRESHDESK_API_KEY", env)
    if not domain or not api_key:
        parser.error("Missing FRESHDESK_DOMAIN or FRESHDESK_API_KEY.")

    lookback_days = args.lookback_days or int(env_or_file("ACTIVITY_DASHBOARD_LOOKBACK_DAYS", env, "7"))
    reply_sla_hours = args.reply_sla_hours or int(env_or_file("ACTIVITY_DASHBOARD_REPLY_SLA_HOURS", env, "24"))
    all_tickets = args.all_tickets or env_as_bool("ACTIVITY_DASHBOARD_ALL_TICKETS", env, False)
    recent_activity_days = args.recent_activity_days or int(
        env_or_file("ACTIVITY_DASHBOARD_RECENT_ACTIVITY_DAYS", env, "30")
    )
    no_conversations = args.no_conversations or env_as_bool(
        "ACTIVITY_DASHBOARD_NO_CONVERSATIONS",
        env,
        False,
    )
    conversation_workers = args.conversation_workers or int(
        env_or_file("ACTIVITY_DASHBOARD_CONVERSATION_WORKERS", env, str(DEFAULT_CONVERSATION_WORKERS))
    )
    updated_since = args.updated_since or (
        DEFAULT_ALL_TICKETS_SINCE
        if all_tickets
        else (utc_now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    )
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    existing_json_path = output_dir / "freshdesk_activity_dashboard.json"
    previous_rows_by_id: Dict[int, Dict[str, object]] = {}
    if existing_json_path.exists():
        try:
            existing_payload = json.loads(existing_json_path.read_text(encoding="utf-8"))
            for existing_row in existing_payload.get("rows") or []:
                ticket_id = as_int(existing_row.get("ticket_id"))
                if ticket_id:
                    previous_rows_by_id[ticket_id] = existing_row
        except (OSError, json.JSONDecodeError, AttributeError):
            previous_rows_by_id = {}

    client = FreshdeskClient(
        domain=domain,
        api_key=api_key,
        max_retries=args.max_retries or int(env_or_file("ACTIVITY_DASHBOARD_MAX_RETRIES", env, str(DEFAULT_RETRY_COUNT))),
        retry_delay_seconds=args.retry_delay_seconds or float(env_or_file("ACTIVITY_DASHBOARD_RETRY_DELAY_SECONDS", env, str(DEFAULT_RETRY_DELAY_SECONDS))),
    )
    status_choices = client.get_status_choices()
    tickets: List[Dict[str, object]] = []
    page = 1
    fetch_errors: List[str] = []
    per_page = 100

    while True:
        batch = client.list_tickets(updated_since=updated_since, page=page, per_page=per_page)
        if not batch:
            break
        tickets.extend(batch)
        print(f"FETCH tickets page={page} count={len(batch)}", file=sys.stderr)
        if args.max_tickets and len(tickets) >= args.max_tickets:
            tickets = tickets[: args.max_tickets]
            break
        if len(batch) < per_page:
            break
        page += 1

    activity_cache_path = output_dir / ".freshdesk_activity_cache.json"
    agent_cache_path = output_dir / ".freshdesk_agent_cache.json"
    requester_company_cache_path = output_dir / ".freshdesk_requester_company_cache.json"
    company_name_cache_path = output_dir / ".freshdesk_company_name_cache.json"
    activity_cache = load_activity_cache(activity_cache_path)
    agent_cache = load_agent_cache(agent_cache_path)
    requester_company_cache = load_string_cache(requester_company_cache_path, "requester_companies")
    company_name_cache = load_string_cache(company_name_cache_path, "companies")
    hubspot_env = freshdesk_hubspot_sync.load_env(Path(args.hubspot_env_file))
    hubspot_access_token = freshdesk_hubspot_sync.env_or_file("HUBSPOT_ACCESS_TOKEN", hubspot_env)
    hubspot_portal_id = freshdesk_hubspot_sync.env_or_file("HUBSPOT_PORTAL_ID", hubspot_env)
    hubspot_ui_domain = freshdesk_hubspot_sync.env_or_file(
        "HUBSPOT_UI_DOMAIN", hubspot_env, DEFAULT_HUBSPOT_UI_DOMAIN
    )
    hubspot_client = (
        freshdesk_hubspot_sync.HubSpotClient(hubspot_access_token)
        if hubspot_access_token
        else None
    )
    hubspot_ticket_map = freshdesk_cli.load_hubspot_sync_ticket_map(Path(args.hubspot_state_file))

    responder_ids = sorted({as_int(ticket.get("responder_id")) or 0 for ticket in tickets if as_int(ticket.get("responder_id"))})
    agent_names: Dict[int, str] = {}
    for responder_id in responder_ids:
        cached_name = agent_cache.get(str(responder_id), "").strip()
        if cached_name:
            agent_names[responder_id] = cached_name
            continue
        try:
            resolved_name = client.get_agent_name(responder_id)
            agent_names[responder_id] = resolved_name
            agent_cache[str(responder_id)] = resolved_name
        except HttpError as exc:
            fetch_errors.append(f"agent {responder_id}: {exc}")

    requester_ids = sorted({as_int(ticket.get("requester_id")) or 0 for ticket in tickets if as_int(ticket.get("requester_id"))})
    requester_company_ids: Dict[int, str] = {}
    for requester_id in requester_ids:
        cached_company_id = requester_company_cache.get(str(requester_id), "")
        if cached_company_id:
            requester_company_ids[requester_id] = cached_company_id
            continue
        try:
            company_id = client.get_contact_company_id(requester_id)
            requester_company_cache[str(requester_id)] = company_id
            if company_id:
                requester_company_ids[requester_id] = company_id
        except HttpError as exc:
            fetch_errors.append(f"requester {requester_id}: {exc}")

    company_ids = sorted({
        company_id for company_id in (
            freshdesk_company_id_for_ticket(ticket, requester_company_ids) for ticket in tickets
        ) if company_id
    })
    company_names: Dict[str, str] = {}
    for company_id in company_ids:
        cached_company_name = company_name_cache.get(company_id, "").strip()
        if cached_company_name:
            company_names[company_id] = cached_company_name
            continue
        try:
            resolved_company_name = client.get_company_name(company_id)
            company_names[company_id] = resolved_company_name
            company_name_cache[company_id] = resolved_company_name
        except HttpError as exc:
            fetch_errors.append(f"company {company_id}: {exc}")

    for ticket in tickets:
        requester_id = as_int(ticket.get("requester_id")) or 0
        company_id = freshdesk_company_id_for_ticket(ticket, requester_company_ids)
        ticket["_freshdesk_company_name"] = company_names.get(company_id, "")

    (
        hubspot_company_names_by_ticket,
        hubspot_deal_urls_by_ticket,
        hubspot_deal_counts_by_ticket,
        hubspot_contact_company_cache,
    ) = enrich_hubspot_context(
        client=client,
        tickets=tickets,
        ticket_map=hubspot_ticket_map,
        output_dir=output_dir,
        hubspot_client=hubspot_client,
        hubspot_ui_domain=hubspot_ui_domain,
        hubspot_portal_id=hubspot_portal_id,
        hubspot_deal_url_field=args.hubspot_deal_url_field,
        fetch_errors=fetch_errors,
    )

    hubspot_deal_stages_by_ticket = fetch_hubspot_deal_stages(
        hubspot_deal_urls_by_ticket, hubspot_client, fetch_errors
    )

    rows: List[Dict[str, object]] = []
    tag_sync = {"updated": 0, "unchanged": 0, "failed": 0}
    conversation_fetches = 0
    conversation_skips = 0
    conversation_cache_hits = 0
    resolved_metrics: Dict[int, Dict[str, object]] = {}
    failed_conversation_fetches: Dict[int, bool] = {}

    def stale_cached_metrics(ticket_id: int) -> Optional[Dict[str, object]]:
        cached = activity_cache.get(str(ticket_id))
        if not isinstance(cached, dict):
            return None
        metrics = cached.get("metrics")
        return metrics if isinstance(metrics, dict) else None

    tickets_to_fetch: List[Dict[str, object]] = []
    if no_conversations:
        conversation_skips = len(tickets)
    else:
        for ticket in tickets:
            ticket_id = as_int(ticket.get("id")) or 0
            ticket_updated_at = str(ticket.get("updated_at") or "")
            metrics_override = cached_activity_metrics(activity_cache, ticket_id, ticket_updated_at)
            if metrics_override is not None:
                resolved_metrics[ticket_id] = metrics_override
                conversation_cache_hits += 1
                conversation_skips += 1
            else:
                tickets_to_fetch.append(ticket)

        if tickets_to_fetch:
            max_workers = max(1, min(conversation_workers, len(tickets_to_fetch)))

            def fetch_ticket_metrics(ticket: Dict[str, object]) -> Tuple[int, Dict[str, object]]:
                ticket_id = as_int(ticket.get("id")) or 0
                conversations = client.list_conversations(ticket_id)
                metrics = build_activity_metrics(ticket, conversations, reply_sla_hours)
                return ticket_id, metrics

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_map = {executor.submit(fetch_ticket_metrics, ticket): ticket for ticket in tickets_to_fetch}
                for future in as_completed(future_map):
                    ticket = future_map[future]
                    ticket_id = as_int(ticket.get("id")) or 0
                    ticket_updated_at = str(ticket.get("updated_at") or "")
                    try:
                        _, metrics_override = future.result()
                        resolved_metrics[ticket_id] = metrics_override
                        save_activity_metrics(activity_cache, ticket_id, ticket_updated_at, metrics_override)
                        conversation_fetches += 1
                    except HttpError as exc:
                        fetch_errors.append(f"ticket {ticket_id}: {exc}")
                        failed_conversation_fetches[ticket_id] = True
                        stale_metrics = stale_cached_metrics(ticket_id)
                        if stale_metrics is not None:
                            resolved_metrics[ticket_id] = stale_metrics

    for ticket in tickets:
        ticket_id = as_int(ticket.get("id")) or 0
        ticket_updated_at = str(ticket.get("updated_at") or "")
        responder_id = as_int(ticket.get("responder_id")) or 0
        requester_id = as_int(ticket.get("requester_id")) or 0
        company_id = freshdesk_company_id_for_ticket(ticket, requester_company_ids)
        ticket_payload = dict(ticket)
        ticket_payload["_responder_name"] = agent_names.get(responder_id, "")
        freshdesk_company_name = company_names.get(company_id, "")
        ticket_payload["_freshdesk_company_name"] = freshdesk_company_name
        ticket_payload["_company_name"] = hubspot_company_names_by_ticket.get(ticket_id, "") or freshdesk_company_name
        ticket_payload["_hubspot_deal_url"] = hubspot_deal_urls_by_ticket.get(ticket_id, "")
        ticket_payload["_hubspot_deal_count"] = hubspot_deal_counts_by_ticket.get(ticket_id, 0)
        for key, value in hubspot_deal_stages_by_ticket.get(ticket_id, {}).items():
            ticket_payload[f"_{key}"] = value
        try:
            conversations = []
            metrics_override = None if no_conversations else resolved_metrics.get(ticket_id)
            row = build_row(
                ticket_payload,
                conversations,
                reply_sla_hours=reply_sla_hours,
                revert_tag=args.customer_revert_tag,
                status_choices=status_choices,
                activity_metrics=metrics_override,
                previous_row=previous_rows_by_id.get(ticket_id),
            )
            if failed_conversation_fetches.get(ticket_id):
                row["last_activity_type"] = "ticket_update"
                row["last_activity_preview"] = (
                    "Using cached conversation checks after API fetch failed"
                    if metrics_override
                    else "Conversation fetch skipped after API limit"
                )
            if args.sync_customer_revert_tag:
                try:
                    result = sync_revert_tag(
                        client,
                        row,
                        revert_tag=args.customer_revert_tag,
                    )
                    tag_sync[result] += 1
                except HttpError as exc:
                    tag_sync["failed"] += 1
                    fetch_errors.append(f"ticket {ticket_id} tag sync failed: {exc}")
            rows.append(row)
            print(
                "OK ticket={0} highlight={1} reverted={2}".format(
                    ticket_id,
                    row["reply_highlight"],
                    row["customer_reverted"],
                ),
                file=sys.stderr,
            )
        except HttpError as exc:
            fetch_errors.append(f"ticket {ticket_id}: {exc}")
            cached_metrics = None if no_conversations else stale_cached_metrics(ticket_id)
            fallback_row = build_row(
                ticket_payload,
                [],
                reply_sla_hours=reply_sla_hours,
                revert_tag=args.customer_revert_tag,
                status_choices=status_choices,
                activity_metrics=cached_metrics,
                previous_row=previous_rows_by_id.get(ticket_id),
            )
            fallback_row["last_activity_type"] = "ticket_update"
            fallback_row["last_activity_preview"] = (
                "Using cached conversation checks after API fetch failed"
                if cached_metrics
                else "Conversation fetch skipped after API limit"
            )
            rows.append(fallback_row)
            print(f"FALLBACK ticket={ticket_id} {exc}", file=sys.stderr)

    rows.sort(
        key=lambda row: (
            0 if row["reply_highlight"] == "overdue" else 1 if row["reply_highlight"] == "waiting" else 2,
            -(
                (parse_iso(row.get("last_activity_at")) or datetime(1970, 1, 1, tzinfo=timezone.utc)).timestamp()
            ),
        )
    )

    meta = {
        "generated_at": utc_now_iso(),
        "updated_since": updated_since,
        "reply_sla_hours": reply_sla_hours,
        "customer_revert_tag": args.customer_revert_tag,
        "domain": domain,
        "tag_sync": tag_sync,
        "fetch_errors": fetch_errors,
        "all_tickets": all_tickets,
        "conversation_fetches": conversation_fetches,
        "conversation_skips": conversation_skips,
        "conversation_cache_hits": conversation_cache_hits,
        "recent_activity_days": recent_activity_days,
        "no_conversations": no_conversations,
    }
    summary = build_summary(rows, meta)
    payload = {"summary": summary, "rows": rows}

    json_path = output_dir / "freshdesk_activity_dashboard.json"
    csv_path = output_dir / "freshdesk_activity_dashboard.csv"
    html_path = output_dir / "freshdesk_activity_dashboard.html"
    status_path = output_dir / ".freshdesk_activity_dashboard_status.json"

    write_json(agent_cache_path, {"generated_at": utc_now_iso(), "agents": agent_cache})
    write_json(
        requester_company_cache_path,
        {"generated_at": utc_now_iso(), "requester_companies": requester_company_cache},
    )
    write_json(
        company_name_cache_path,
        {"generated_at": utc_now_iso(), "companies": company_name_cache},
    )
    write_json(
        output_dir / ".hubspot_contact_company_cache.json",
        {"generated_at": utc_now_iso(), "contact_companies": hubspot_contact_company_cache},
    )
    write_json(activity_cache_path, {"generated_at": utc_now_iso(), "tickets": activity_cache})
    write_json(json_path, payload)
    write_csv(csv_path, rows)
    html_path.write_text(build_html(summary, rows), encoding="utf-8")
    write_json(status_path, summary)

    print(
        "SUMMARY total_tickets={0} customer_reverted={1} overdue_replies={2} waiting_replies={3} tag_updates={4} output_dir={5}".format(
            summary["total_tickets"],
            summary["customer_reverted"],
            summary["overdue_replies"],
            summary["waiting_replies"],
            tag_sync["updated"],
            output_dir,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
