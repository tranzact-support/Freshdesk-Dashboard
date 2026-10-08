#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import csv
import html
import json
import os
import re
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
                with urllib.request.urlopen(req) as resp:
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
            except urllib.error.URLError as exc:
                if attempt < self.max_retries:
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
        "company_name": str(ticket.get("_company_name") or "").strip() or "Unknown",
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
    table { width:100%; border-collapse:collapse; background:#111827; border-radius:12px; overflow:hidden; }
    th, td { border-bottom:1px solid #1e293b; text-align:left; padding:10px 12px; vertical-align:top; font-size:13px; }
    th { position:sticky; top:0; background:#0b1220; z-index:1; }
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
    .overview-grid { display:grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap:16px; margin-top:16px; }
    .overview-card, .chart-card, .action-card { background:#111827; border:1px solid #334155; border-radius:12px; padding:16px; }
    .overview-card .label, .chart-card .label, .action-card .label { color:#94a3b8; font-size:12px; text-transform:uppercase; letter-spacing:.06em; }
    .overview-card .value { font-size:32px; font-weight:700; margin-top:8px; }
    .overview-card .subvalue, .chart-card .subvalue, .action-card .subvalue { color:#94a3b8; font-size:12px; margin-top:8px; }
    .overview-section { margin-top:18px; }
    .section-title { margin:0 0 12px; font-size:18px; }
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
    .next-actions { display:grid; gap:12px; }
    .next-action-title { font-weight:700; }
    .next-action-meta { color:#94a3b8; font-size:12px; margin-top:4px; }
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
      <button class="tab-btn active" id="tab-activity">Activity</button>
      <button class="tab-btn" id="tab-sla">SOP / SLA</button>
      <button class="tab-btn" id="tab-overview">Overview</button>
    </div>
    <div class="cards" id="cards"></div>
    <div id="activity-view">
      <div class="controls">
        <input id="activity-search" type="search" placeholder="Search subject, tags, ticket id">
        <input class="date-input" id="activity-date-from" type="date" title="Created from">
        <input class="date-input" id="activity-date-to" type="date" title="Created to">
        <details class="filter-box"><summary>Reply State</summary><div class="filter-list" id="activity-filter-highlight"></div></details>
        <details class="filter-box"><summary>Status</summary><div class="filter-list" id="activity-filter-status"></div></details>
        <details class="filter-box"><summary>Ticket Category</summary><div class="filter-list" id="activity-filter-type"></div></details>
        <details class="filter-box"><summary>Company Name</summary><div class="filter-list" id="activity-filter-company"></div></details>
      </div>
      <table>
        <thead><tr><th>ID</th><th>Subject</th><th>Status</th><th>Type</th><th>Updated</th><th>Reply Check</th><th>Customer Reverted</th><th>Replied Same Ticket</th><th>Tags</th><th>Latest Activity</th><th>Recent Timeline</th></tr></thead>
        <tbody id="activity-table-body"></tbody>
      </table>
    </div>
    <div id="sla-view" class="hidden">
      <div class="controls">
        <input id="sla-search" type="search" placeholder="Search subject, category, ticket id">
        <input class="date-input" id="sla-date-from" type="date" title="Created from">
        <input class="date-input" id="sla-date-to" type="date" title="Created to">
        <details class="filter-box"><summary>Status</summary><div class="filter-list" id="sla-filter-status"></div></details>
        <details class="filter-box"><summary>Ticket Category</summary><div class="filter-list" id="sla-filter-type"></div></details>
        <details class="filter-box"><summary>Company Name</summary><div class="filter-list" id="sla-filter-company"></div></details>
        <details class="filter-box"><summary>SLA Flags</summary><div class="filter-list" id="sla-filter-sla"></div></details>
      </div>
      <table>
        <thead><tr><th>ID</th><th>Subject</th><th>Category</th><th>Priority</th><th>Created IST</th><th>Closed IST</th><th>Last Activity IST</th><th>Done By</th><th>Ack SLA</th><th>Resolution SLA</th><th>Max Timeline</th><th>Owner</th></tr></thead>
        <tbody id="sla-table-body"></tbody>
      </table>
    </div>
    <div id="overview-view" class="hidden">
      <div class="overview-section">
        <h2 class="section-title">Quick Look</h2>
        <div class="overview-grid" id="overview-metrics"></div>
      </div>
      <div class="overview-section">
        <h2 class="section-title">Graphs</h2>
        <div class="overview-grid">
          <div class="chart-card">
            <div class="label">Tickets This Week</div>
            <div class="subvalue">Created in the current week</div>
            <div class="bar-chart" id="weekly-created-chart"></div>
          </div>
          <div class="chart-card">
            <div class="label">Operational Snapshot</div>
            <div class="subvalue">Current workload by action area</div>
            <div class="mini-bars" id="overview-category-bars"></div>
          </div>
        </div>
      </div>
      <div class="overview-section">
        <h2 class="section-title">Next Actions</h2>
        <div class="next-actions" id="overview-actions"></div>
      </div>
    </div>
  </div>
  <script>
    const data = __DATA__;
    const summary = data.summary || {};
    const rows = data.rows || [];
    let currentView = 'activity';
    const activeCardFilters = { activity: 'tickets', sla: 'tickets', overview: 'tickets' };
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
    function isRejectedStatus(label) {
      return String(label || '').toLowerCase().includes('reject');
    }
    function isClosedLikeStatus(label) {
      const normalized = String(label || '').trim().toLowerCase();
      return normalized === 'closed' || normalized === 'resolved' || isRejectedStatus(normalized);
    }
    function buildOverviewStats() {
      const now = new Date();
      const weekStart = startOfWeek(now);
      const weekDays = Array.from({ length: 7 }, (_, index) => {
        const date = addDays(weekStart, index);
        return {
          key: date.toISOString().slice(0, 10),
          label: date.toLocaleDateString(undefined, { weekday: 'short' }),
          total: 0,
        };
      });
      const activeRows = rows.filter((row) => !isClosedLikeStatus(row.status_label));
      rows.forEach((row) => {
        const createdAt = parseDate(row.created_at);
        if (!createdAt) return;
        if (createdAt >= weekStart && createdAt < addDays(weekStart, 7)) {
          const key = createdAt.toISOString().slice(0, 10);
          const bucket = weekDays.find((item) => item.key === key);
          if (bucket) bucket.total += 1;
        }
      });
      const pendingSop = activeRows.filter((row) => row.sla_red_flag || row.sla_amber_flag || row.ack_state === 'breached' || row.resolution_state === 'breached' || row.max_timeline_state === 'breached');
      const notAcceptedByTech = activeRows.filter((row) => ['Pending At Tech', 'To be Picked in Next TS'].includes(String(row.status_label || '').trim()));
      const customerWaiting = activeRows.filter((row) => row.reply_highlight === 'overdue' || row.reply_highlight === 'waiting');
      const unresolvedThisWeek = rows.filter((row) => {
        const createdAt = parseDate(row.created_at);
        return createdAt && createdAt >= weekStart && createdAt < addDays(weekStart, 7) && !isClosedLikeStatus(row.status_label);
      });
      const metrics = [
        { key: 'tickets_this_week', label: 'Tickets This Week', value: weekDays.reduce((sum, item) => sum + item.total, 0), detail: `${unresolvedThisWeek.length} still active`, action: 'Review new tickets created this week and assign owners.' },
        { key: 'pending_sop', label: 'SOP Pending', value: pendingSop.length, detail: `${activeRows.length} active tickets in queue`, action: 'Start with red flags, then clear at-risk tickets.' },
        { key: 'not_accepted', label: 'Not Accepted By Tech', value: notAcceptedByTech.length, detail: 'Pending At Tech or To be Picked in Next TS', action: 'Push tech acceptance or move to the correct owner/team.' },
        { key: 'customer_waiting', label: 'Customer Waiting', value: customerWaiting.length, detail: 'Reply needed on the same ticket', action: 'Reply on the ticket or share a delay update with the customer.' },
        { key: 'red_flags', label: 'Red Flags', value: activeRows.filter((row) => row.sla_red_flag).length, detail: 'Open SLA breaches right now', action: 'Escalate breaches first and communicate ETAs.' },
      ];
      return { weekDays, metrics, activeRows };
    }
    function renderOverview() {
      const overview = buildOverviewStats();
      document.getElementById('overview-metrics').innerHTML = overview.metrics.map((metric) => `
        <div class="overview-card">
          <div class="label">${escapeHtml(metric.label)}</div>
          <div class="value">${metric.value}</div>
          <div class="subvalue">${escapeHtml(metric.detail)}</div>
        </div>`).join('');
      const maxWeeklyValue = Math.max(1, ...overview.weekDays.map((item) => item.total));
      document.getElementById('weekly-created-chart').innerHTML = overview.weekDays.map((item) => {
        const height = Math.max(6, Math.round((item.total / maxWeeklyValue) * 180));
        return `<div class="bar-group"><div class="bar-value">${item.total}</div><div class="bar-stack"><div class="bar" style="height:${height}px"></div></div><div class="bar-label">${escapeHtml(item.label)}</div></div>`;
      }).join('');
      const maxMetricValue = Math.max(1, ...overview.metrics.map((metric) => metric.value));
      document.getElementById('overview-category-bars').innerHTML = overview.metrics.map((metric) => {
        const width = Math.max(2, Math.round((metric.value / maxMetricValue) * 100));
        return `<div class="mini-row"><div>${escapeHtml(metric.label)}</div><div class="mini-track"><div class="mini-fill" style="width:${width}%"></div></div><div>${metric.value}</div></div>`;
      }).join('');
      document.getElementById('overview-actions').innerHTML = overview.metrics.filter((metric) => metric.value > 0).map((metric) => `
        <div class="action-card"><div class="label">${escapeHtml(metric.label)}</div><div class="next-action-title">${metric.value} ticket${metric.value === 1 ? '' : 's'}</div><div class="next-action-meta">${escapeHtml(metric.action)}</div></div>`).join('') || '<div class="action-card"><div class="label">Next Actions</div><div class="next-action-title">No immediate actions</div><div class="next-action-meta">Everything looks clear right now.</div></div>';
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
    function yesNoUnknown(value, checked) {
      if (!checked) return '<span class="small">Not checked</span>';
      return value ? 'Yes' : 'No';
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
      renderCards(baseActivityRows, baseSlaRows);
      renderOverview();
      document.getElementById('activity-table-body').innerHTML = activityRows.map((row) => `
        <tr class="${row.reply_highlight}"><td class="id"><a href="https://${summary.domain}/a/tickets/${row.ticket_id}" target="_blank" rel="noreferrer">${row.ticket_id}</a></td><td class="subject"><strong>${escapeHtml(row.subject)}</strong><div class="small">Priority: ${escapeHtml(row.priority_label)} • Group: ${row.group_id || '-'} • Agent: ${escapeHtml(row.responder_name || String(row.responder_id || '-'))}</div></td><td>${escapeHtml(row.status_label)}</td><td>${escapeHtml(row.type || '')}</td><td>${escapeHtml(row.updated_at || '')}</td><td>${replyPill(row)}<div class="small">Current waiting: ${row.customer_reverted ? 'Yes' : 'No'}</div></td><td>${yesNoUnknown(row.customer_reverted_ever, row.conversation_checked)}<div class="small">At least one customer revert</div></td><td>${yesNoUnknown(row.replied_after_customer_revert, row.conversation_checked)}<div class="small">Reply after revert</div></td><td>${escapeHtml(row.tags_display || '')}</td><td><div><strong>${escapeHtml(row.last_activity_type || '')}</strong></div><div class="small">${escapeHtml(row.last_activity_at || '')}</div><div>${escapeHtml(row.last_activity_preview || '')}</div></td><td class="timeline">${escapeHtml(row.timeline_excerpt || '')}</td></tr>`).join('');
      document.getElementById('sla-table-body').innerHTML = slaRows.map((row) => `
        <tr class="${row.sla_red_flag ? 'sla-red' : row.sla_amber_flag ? 'sla-amber' : ''}"><td class="id"><a href="https://${summary.domain}/a/tickets/${row.ticket_id}" target="_blank" rel="noreferrer">${row.ticket_id}</a></td><td class="subject"><strong>${escapeHtml(row.subject)}</strong><div class="small">Status: ${escapeHtml(row.status_label)} • Type: ${escapeHtml(row.type || '')}</div></td><td>${escapeHtml(row.sla_category || '')}</td><td>${escapeHtml(row.priority_label || '')}</td><td>${escapeHtml(row.created_at_ist || '')}</td><td>${escapeHtml(row.closed_at_ist || row.resolved_at_ist || '') || '<span class="small">Open</span>'}</td><td><div>${escapeHtml(row.last_activity_at_ist || '')}</div><div class="small">${escapeHtml(row.last_activity_type || '')}</div></td><td>${escapeHtml(displayAgentName(row))}</td><td>${slaPill(row.ack_state, row.ack_label)}<div class="small">SLA: ${escapeHtml(row.ack_sla || 'NA')}</div></td><td>${slaPill(row.resolution_state, row.resolution_label)}<div class="small">SLA: ${escapeHtml(row.resolution_sla || 'NA')}</div></td><td>${slaPill(row.max_timeline_state, row.max_timeline_label)}<div class="small">Limit: ${escapeHtml(row.max_timeline_sla || 'NA')}</div></td><td>${escapeHtml(row.owner_team || '')}</td></tr>`).join('');
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
    ['activity-filter-highlight','activity-filter-status','activity-filter-type','activity-filter-company','sla-filter-status','sla-filter-type','sla-filter-company','sla-filter-sla'].forEach(wireGroup);
    ['activity-filter-company','sla-filter-company'].forEach(wireFilterSearch);
    ['activity-date-from','activity-date-to','sla-date-from','sla-date-to'].forEach(wireDatePicker);
    document.getElementById('activity-search').addEventListener('input', render);
    document.getElementById('sla-search').addEventListener('input', render);
    document.getElementById('activity-date-from').addEventListener('input', render);
    document.getElementById('activity-date-to').addEventListener('input', render);
    document.getElementById('sla-date-from').addEventListener('input', render);
    document.getElementById('sla-date-to').addEventListener('input', render);
    document.getElementById('tab-activity').addEventListener('click', () => setView('activity'));
    document.getElementById('tab-sla').addEventListener('click', () => setView('sla'));
    document.getElementById('tab-overview').addEventListener('click', () => setView('overview'));
    __REFRESH_JS__
    setView('activity');
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

    company_ids = sorted({company_id for company_id in requester_company_ids.values() if company_id})
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
        company_id = requester_company_ids.get(requester_id, "")
        ticket_payload = dict(ticket)
        ticket_payload["_responder_name"] = agent_names.get(responder_id, "")
        ticket_payload["_company_name"] = company_names.get(company_id, "")
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
