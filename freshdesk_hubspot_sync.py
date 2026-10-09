#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import http.client
import hashlib
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
from dataclasses import dataclass
from datetime import datetime, timezone
from html import escape, unescape
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import freshdesk_cli


FRESHDESK_TASK_TYPE = "Task - Experience Team"
FRESHDESK_CLOSED_STATUSES = {4, 5}
DEFAULT_CREATED_AFTER = "2026-07-25"
DEFAULT_PAGE_LIMIT = 10
DEFAULT_PAGE_SIZE = 30
DEFAULT_HUBSPOT_OWNER_EMAIL = "shagun@letstranzact.com"
DEFAULT_FRESHDESK_MIN_INTERVAL_SECONDS = 1.5
DEFAULT_FRESHDESK_MAX_RETRIES = 6
DEFAULT_FRESHDESK_RETRY_BASE_SECONDS = 5.0
DEFAULT_RETRY_FAILED_TICKETS_SECONDS = 30.0
DEFAULT_STATUS_FILE_NAME = ".freshdesk_hubspot_sync_status.json"
STATE_VERSION = 1
HUBSPOT_NOTE_MARKER_PREFIX = "hubspot-sync:hubspot-note:"
HUBSPOT_TICKET_UPDATE_MARKER_PREFIX = "hubspot-sync:hubspot-ticket-update:"
HUBSPOT_LINK_MARKER = "hubspot-sync:hubspot-ticket-linked"
HUBSPOT_CONTENT_ID_PREFIX = "Freshdesk Ticket ID:"
FRESHDESK_UPDATED_MARKER = "Freshdesk updated at:"
TRACKED_HUBSPOT_FIELDS = (
    "subject",
    "content",
    "hs_pipeline",
    "hs_pipeline_stage",
    "hs_ticket_priority",
    "poc_contact_details",
    "source_type",
    "hubspot_owner_id",
)
SUMMARY_NOISE = {
    "hi",
    "hello",
    "hey",
    "yes",
    "okay",
    "ok",
    "thanks",
    "thank you",
    "not yet",
    "no one is",
}
CONVERSATION_BLOCK_RE = re.compile(
    r'<div class="data-conversation-(user|agent)".*?><div>(.*?)</div></div>',
    re.IGNORECASE | re.DOTALL,
)
TAG_RE = re.compile(r"<[^>]+>")


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


def iso_now() -> str:
    return utc_now().replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_datetime(raw_value: str) -> Optional[datetime]:
    if not raw_value:
        return None
    value = raw_value.strip()
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def collapse_space(text: str) -> str:
    return " ".join(text.split())


def html_paragraph(text: str) -> str:
    normalized = escape(text).replace("\n", "<br>")
    return f"<p>{normalized}</p>"


def stable_hash(value: object) -> str:
    payload = json.dumps(value, sort_keys=True, ensure_ascii=True)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


class HttpError(RuntimeError):
    pass


class FreshdeskClient:
    def __init__(self, domain: str, api_key: str):
        self.domain = domain
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
        for attempt in range(1, self.max_retries + 2):
            self._respect_rate_limit()
            request = urllib.request.Request(url, data=body, headers=headers, method=method)
            try:
                with urllib.request.urlopen(request, timeout=120) as response:
                    raw = response.read().decode("utf-8", errors="replace")
                    parsed = json.loads(raw) if raw else None
                    return response.status, parsed, raw
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
                raise exc
        raise RuntimeError("Freshdesk request retry loop exited unexpectedly")

    def search_task_experience_tickets(
        self, created_after: str, page: int
    ) -> Dict[str, object]:
        query = (
            f"type:'{FRESHDESK_TASK_TYPE}'"
            f" AND created_at:>'{created_after}'"
        )
        encoded_query = urllib.parse.quote(query, safe="")
        code, data, raw = self.request(
            "GET", f"search/tickets?query=\"{encoded_query}\"&page={page}"
        )
        if code != 200 or not isinstance(data, dict):
            raise HttpError(
                "Freshdesk search failed with HTTP {0}: {1}".format(code, raw[:400])
            )
        return data

    def get_ticket(self, ticket_id: int) -> Dict[str, object]:
        code, data, raw = self.request(
            "GET", f"tickets/{ticket_id}?include=requester,company"
        )
        if code != 200 or not isinstance(data, dict):
            raise HttpError(
                "Freshdesk ticket fetch failed for ticket {0} with HTTP {1}: {2}".format(
                    ticket_id, code, raw[:400]
                )
            )
        return data

    def add_note(self, ticket_id: int, body: str, private: bool = True) -> None:
        payload = {"body": body, "private": private}
        code, data, raw = self.request("POST", f"tickets/{ticket_id}/notes", payload)
        if code != 201:
            details = raw[:400]
            if isinstance(data, dict):
                details = json.dumps(data)
            raise HttpError(
                "Freshdesk add note failed for ticket {0} with HTTP {1}: {2}".format(
                    ticket_id, code, details
                )
            )

    def update_ticket(self, ticket_id: int, payload: Dict[str, object]) -> Dict[str, object]:
        code, data, raw = self.request("PUT", f"tickets/{ticket_id}", payload)
        if code != 200 or not isinstance(data, dict):
            details = raw[:400]
            if isinstance(data, dict):
                details = json.dumps(data)
            raise HttpError(
                "Freshdesk update failed for ticket {0} with HTTP {1}: {2}".format(
                    ticket_id, code, details
                )
            )
        return data

    def ticket_url(self, ticket_id: int) -> str:
        return f"https://{self.domain}/a/tickets/{ticket_id}"


class HubSpotClient:
    def __init__(self, access_token: str):
        self.base = "https://api.hubapi.com"
        self.access_token = access_token

    def request(
        self,
        method: str,
        path: str,
        payload: Optional[Dict[str, object]] = None,
        query: Optional[Dict[str, str]] = None,
    ) -> Tuple[int, Optional[object], str]:
        url = urllib.parse.urljoin(self.base, path)
        if query:
            url = f"{url}?{urllib.parse.urlencode(query)}"
        body = None
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.access_token}",
        }
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(url, data=body, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=120) as response:
                raw = response.read().decode("utf-8", errors="replace")
                parsed = json.loads(raw) if raw else None
                return response.status, parsed, raw
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            parsed = None
            if raw:
                try:
                    parsed = json.loads(raw)
                except json.JSONDecodeError:
                    parsed = None
            return exc.code, parsed, raw

    def list_ticket_pipelines(self) -> List[Dict[str, object]]:
        code, data, raw = self.request("GET", "/crm/v3/pipelines/tickets")
        if code != 200:
            raise HttpError(
                "HubSpot pipelines request failed with HTTP {0}: {1}".format(
                    code, raw[:400]
                )
            )
        if isinstance(data, dict):
            results = data.get("results")
            if isinstance(results, list):
                return [item for item in results if isinstance(item, dict)]
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
        raise HttpError("HubSpot pipelines response shape was not recognized.")

    def list_owners(self) -> List[Dict[str, object]]:
        code, data, raw = self.request("GET", "/crm/v3/owners/", query={"limit": "500"})
        if code != 200 or not isinstance(data, dict):
            raise HttpError(
                "HubSpot owners request failed with HTTP {0}: {1}".format(
                    code, raw[:400]
                )
            )
        return [item for item in (data.get("results") or []) if isinstance(item, dict)]

    def search_ticket_by_freshdesk_id(
        self, freshdesk_ticket_id: int
    ) -> Optional[Dict[str, object]]:
        payload = {
            "limit": 1,
            "query": f"{HUBSPOT_CONTENT_ID_PREFIX} {freshdesk_ticket_id}",
            "properties": list(TRACKED_HUBSPOT_FIELDS),
        }
        code, data, raw = self.request(
            "POST", "/crm/v3/objects/tickets/search", payload
        )
        if code != 200 or not isinstance(data, dict):
            raise HttpError(
                "HubSpot ticket search failed with HTTP {0}: {1}".format(
                    code, raw[:400]
                )
            )
        results = data.get("results") or []
        for item in results:
            if isinstance(item, dict):
                return item
        return None

    def get_ticket(self, ticket_id: str) -> Dict[str, object]:
        code, data, raw = self.request(
            "GET",
            f"/crm/v3/objects/tickets/{ticket_id}",
            query={"properties": ",".join(TRACKED_HUBSPOT_FIELDS)},
        )
        if code != 200 or not isinstance(data, dict):
            raise HttpError(
                "HubSpot ticket fetch failed for ticket {0} with HTTP {1}: {2}".format(
                    ticket_id, code, raw[:400]
                )
            )
        return data

    def search_contacts(
        self, filter_groups: List[Dict[str, object]], limit: int = 10
    ) -> List[Dict[str, object]]:
        payload = {
            "filterGroups": filter_groups,
            "properties": [
                "firstname",
                "lastname",
                "email",
                "phone",
                "mobilephone",
                "company",
            ],
            "limit": limit,
        }
        code, data, raw = self.request(
            "POST", "/crm/v3/objects/contacts/search", payload
        )
        if code != 200 or not isinstance(data, dict):
            raise HttpError(
                "HubSpot contact search failed with HTTP {0}: {1}".format(
                    code, raw[:400]
                )
            )
        return [
            item
            for item in (data.get("results") or [])
            if isinstance(item, dict) and item.get("id") is not None
        ]

    def get_contact(
        self, contact_id: str, properties: Optional[Iterable[str]] = None
    ) -> Dict[str, object]:
        property_names = [str(item).strip() for item in (properties or ["company"]) if str(item).strip()]
        code, data, raw = self.request(
            "GET",
            f"/crm/v3/objects/contacts/{contact_id}",
            query={"properties": ",".join(property_names)},
        )
        if code != 200 or not isinstance(data, dict):
            raise HttpError(
                "HubSpot contact fetch failed for contact {0} with HTTP {1}: {2}".format(
                    contact_id, code, raw[:400]
                )
            )
        return data

    def associate_ticket_to_contact(self, ticket_id: str, contact_id: str) -> None:
        code, data, raw = self.request(
            "PUT",
            f"/crm/v4/objects/tickets/{ticket_id}/associations/default/contacts/{contact_id}",
        )
        if code not in {200, 201, 204}:
            details = raw[:400]
            if isinstance(data, dict):
                details = json.dumps(data)
            raise HttpError(
                "HubSpot contact association failed for ticket {0} -> contact {1} with HTTP {2}: {3}".format(
                    ticket_id, contact_id, code, details
                )
            )

    def list_associated_object_ids(
        self, from_object_type: str, from_object_id: str, to_object_type: str
    ) -> List[str]:
        ids: List[str] = []
        after: Optional[str] = None
        while True:
            query = {"limit": "500"}
            if after:
                query["after"] = after
            code, data, raw = self.request(
                "GET",
                f"/crm/v4/objects/{from_object_type}/{from_object_id}/associations/{to_object_type}",
                query=query,
            )
            if code != 200 or not isinstance(data, dict):
                raise HttpError(
                    "HubSpot association list failed for {0} {1} -> {2} with HTTP {3}: {4}".format(
                        from_object_type, from_object_id, to_object_type, code, raw[:400]
                    )
                )
            results = data.get("results") or []
            for item in results:
                if not isinstance(item, dict):
                    continue
                to_id = item.get("toObjectId")
                if to_id is None:
                    continue
                ids.append(str(to_id))
            paging = data.get("paging") or {}
            next_page = paging.get("next") or {}
            next_after = next_page.get("after")
            if not next_after:
                break
            after = str(next_after)
        return ids

    def associate_ticket_to_deal(self, ticket_id: str, deal_id: str) -> None:
        code, data, raw = self.request(
            "PUT",
            f"/crm/v4/objects/tickets/{ticket_id}/associations/default/deals/{deal_id}",
        )
        if code not in {200, 201, 204}:
            details = raw[:400]
            if isinstance(data, dict):
                details = json.dumps(data)
            raise HttpError(
                "HubSpot deal association failed for ticket {0} -> deal {1} with HTTP {2}: {3}".format(
                    ticket_id, deal_id, code, details
                )
            )

    def create_ticket(self, properties: Dict[str, str]) -> Dict[str, object]:
        payload = {"properties": properties}
        code, data, raw = self.request("POST", "/crm/v3/objects/tickets", payload)
        if code not in {200, 201} or not isinstance(data, dict):
            raise HttpError(
                "HubSpot ticket create failed with HTTP {0}: {1}".format(
                    code, raw[:400]
                )
            )
        return data

    def update_ticket(
        self, ticket_id: str, properties: Dict[str, str]
    ) -> Dict[str, object]:
        payload = {"properties": properties}
        code, data, raw = self.request(
            "PATCH", f"/crm/v3/objects/tickets/{ticket_id}", payload
        )
        if code != 200 or not isinstance(data, dict):
            raise HttpError(
                "HubSpot ticket update failed for ticket {0} with HTTP {1}: {2}".format(
                    ticket_id, code, raw[:400]
                )
            )
        return data

    def list_associated_note_ids(self, ticket_id: str) -> List[str]:
        note_ids: List[str] = []
        after: Optional[str] = None
        while True:
            query = {"limit": "500"}
            if after:
                query["after"] = after
            code, data, raw = self.request(
                "GET",
                f"/crm/v4/objects/tickets/{ticket_id}/associations/notes",
                query=query,
            )
            if code != 200 or not isinstance(data, dict):
                raise HttpError(
                    "HubSpot note association fetch failed for ticket {0} with HTTP {1}: {2}".format(
                        ticket_id, code, raw[:400]
                    )
                )
            results = data.get("results") or []
            for item in results:
                if not isinstance(item, dict):
                    continue
                note_id = item.get("toObjectId")
                if note_id is None:
                    continue
                note_ids.append(str(note_id))
            paging = data.get("paging") or {}
            next_page = paging.get("next") or {}
            next_after = next_page.get("after")
            if not next_after:
                break
            after = str(next_after)
        return note_ids

    def get_note(self, note_id: str) -> Dict[str, object]:
        code, data, raw = self.request(
            "GET",
            f"/crm/v3/objects/notes/{note_id}",
            query={
                "properties": "hs_note_body,hs_timestamp,hs_lastmodifieddate,hubspot_owner_id"
            },
        )
        if code != 200 or not isinstance(data, dict):
            raise HttpError(
                "HubSpot note fetch failed for note {0} with HTTP {1}: {2}".format(
                    note_id, code, raw[:400]
                )
            )
        return data


@dataclass(frozen=True)
class PipelineSelection:
    pipeline_id: str
    open_stage_id: str
    closed_stage_id: str
    stage_labels: Dict[str, str]


@dataclass(frozen=True)
class OwnerSelection:
    owner_id: str
    owner_email: str
    owner_name: str


def choose_pipeline_selection(
    pipelines: List[Dict[str, object]], env_values: Dict[str, str]
) -> PipelineSelection:
    configured_pipeline_id = env_or_file("HUBSPOT_TICKET_PIPELINE_ID", env_values)
    configured_open_stage_id = env_or_file("HUBSPOT_TICKET_OPEN_STAGE_ID", env_values)
    configured_closed_stage_id = env_or_file(
        "HUBSPOT_TICKET_CLOSED_STAGE_ID", env_values
    )

    selected_pipeline: Optional[Dict[str, object]] = None
    if configured_pipeline_id:
        for pipeline in pipelines:
            if str(pipeline.get("id")) == configured_pipeline_id:
                selected_pipeline = pipeline
                break
        if selected_pipeline is None:
            raise HttpError(
                f"Configured HUBSPOT_TICKET_PIPELINE_ID={configured_pipeline_id} was not found."
            )
    else:
        sorted_pipelines = sorted(
            pipelines,
            key=lambda item: (
                int(bool(item.get("archived"))),
                int(item.get("displayOrder") or 0),
                str(item.get("label") or ""),
            ),
        )
        for pipeline in sorted_pipelines:
            if not pipeline.get("archived"):
                selected_pipeline = pipeline
                break
        if selected_pipeline is None and sorted_pipelines:
            selected_pipeline = sorted_pipelines[0]
    if selected_pipeline is None:
        raise HttpError("No HubSpot ticket pipeline was available.")

    stage_labels: Dict[str, str] = {}
    open_stage_id = configured_open_stage_id
    closed_stage_id = configured_closed_stage_id

    stages = selected_pipeline.get("stages") or []
    for stage in stages:
        if not isinstance(stage, dict):
            continue
        stage_id = str(stage.get("id") or "")
        if not stage_id:
            continue
        stage_labels[stage_id] = str(stage.get("label") or stage_id)
        metadata = stage.get("metadata") or {}
        ticket_state = str(metadata.get("ticketState") or "").upper()
        if not open_stage_id and ticket_state == "OPEN":
            open_stage_id = stage_id
        if not closed_stage_id and ticket_state == "CLOSED":
            closed_stage_id = stage_id

    if not open_stage_id and stages:
        first_stage = next((stage for stage in stages if isinstance(stage, dict)), None)
        if first_stage and first_stage.get("id") is not None:
            open_stage_id = str(first_stage.get("id"))
    if not closed_stage_id and stages:
        last_stage = next(
            (stage for stage in reversed(stages) if isinstance(stage, dict)), None
        )
        if last_stage and last_stage.get("id") is not None:
            closed_stage_id = str(last_stage.get("id"))

    if not open_stage_id or not closed_stage_id:
        raise HttpError(
            "Could not determine open/closed HubSpot ticket stages. "
            "Set HUBSPOT_TICKET_OPEN_STAGE_ID and HUBSPOT_TICKET_CLOSED_STAGE_ID."
        )

    return PipelineSelection(
        pipeline_id=str(selected_pipeline.get("id")),
        open_stage_id=str(open_stage_id),
        closed_stage_id=str(closed_stage_id),
        stage_labels=stage_labels,
    )


def choose_owner_selection(
    owners: List[Dict[str, object]], env_values: Dict[str, str]
) -> OwnerSelection:
    configured_owner_id = env_or_file("HUBSPOT_TICKET_OWNER_ID", env_values)
    configured_owner_email = (
        env_or_file("HUBSPOT_TICKET_OWNER_EMAIL", env_values)
        or DEFAULT_HUBSPOT_OWNER_EMAIL
    ).lower()

    selected: Optional[Dict[str, object]] = None
    if configured_owner_id:
        for owner in owners:
            if str(owner.get("id")) == configured_owner_id:
                selected = owner
                break
        if selected is None:
            raise HttpError(
                f"Configured HUBSPOT_TICKET_OWNER_ID={configured_owner_id} was not found."
            )
    else:
        for owner in owners:
            if str(owner.get("email") or "").lower() == configured_owner_email:
                selected = owner
                break
        if selected is None:
            raise HttpError(
                f"Configured/default HUBSPOT_TICKET_OWNER_EMAIL={configured_owner_email} was not found."
            )

    return OwnerSelection(
        owner_id=str(selected.get("id") or ""),
        owner_email=str(selected.get("email") or ""),
        owner_name=collapse_space(
            " ".join(
                part
                for part in [
                    str(selected.get("firstName") or ""),
                    str(selected.get("lastName") or ""),
                ]
                if part
            )
        )
        or str(selected.get("email") or ""),
    )


def load_state(path: Path) -> Dict[str, object]:
    if not path.exists():
        return {"version": STATE_VERSION, "tickets": {}}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"version": STATE_VERSION, "tickets": {}}
    if not isinstance(data, dict):
        return {"version": STATE_VERSION, "tickets": {}}
    tickets = data.get("tickets")
    if not isinstance(tickets, dict):
        data["tickets"] = {}
    data.setdefault("version", STATE_VERSION)
    return data


def save_state(path: Path, state: Dict[str, object]) -> None:
    path.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")


def save_status(path: Path, payload: Dict[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def load_status(path: Path) -> Dict[str, object]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def freshdesk_ticket_is_closed(ticket: Dict[str, object]) -> bool:
    return int(ticket.get("status") or 0) in FRESHDESK_CLOSED_STATUSES


def map_priority(priority_value: object) -> str:
    mapping = {
        1: "LOW",
        2: "MEDIUM",
        3: "HIGH",
        4: "HIGH",
    }
    try:
        return mapping.get(int(priority_value or 0), "MEDIUM")
    except (TypeError, ValueError):
        return "MEDIUM"


def first_non_empty(values: Iterable[object]) -> str:
    for value in values:
        text = collapse_space(str(value or ""))
        if text:
            return text
    return ""


def build_freshdesk_description(ticket: Dict[str, object]) -> str:
    description = first_non_empty(
        [
            ticket.get("description_text"),
            ticket.get("description"),
        ]
    )
    if not description:
        return "-"
    return description


def strip_tags(raw_html: str) -> str:
    return collapse_space(unescape(TAG_RE.sub(" ", raw_html)))


def extract_conversation_messages(ticket: Dict[str, object]) -> List[Tuple[str, str]]:
    description_html = str(ticket.get("description") or "")
    messages: List[Tuple[str, str]] = []
    for role, raw_message in CONVERSATION_BLOCK_RE.findall(description_html):
        message = strip_tags(raw_message)
        if not message:
            continue
        messages.append((role.lower(), message))
    return messages


def is_meaningful_summary_line(text: str) -> bool:
    normalized = collapse_space(text).strip(" .").lower()
    if not normalized:
        return False
    if normalized in SUMMARY_NOISE:
        return False
    if len(normalized) < 8:
        return False
    return True


def summarize_messages(messages: List[Tuple[str, str]], role: str, limit: int) -> List[str]:
    seen: set[str] = set()
    summary_lines: List[str] = []
    for message_role, text in messages:
        if message_role != role:
            continue
        normalized = collapse_space(text)
        if not is_meaningful_summary_line(normalized):
            continue
        lowered = normalized.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        summary_lines.append(normalized)
        if len(summary_lines) >= limit:
            break
    return summary_lines


def latest_meaningful_message(messages: List[Tuple[str, str]], role: str) -> str:
    for message_role, text in reversed(messages):
        if message_role != role:
            continue
        normalized = collapse_space(text)
        if is_meaningful_summary_line(normalized):
            return normalized
    return ""


def build_freshdesk_summary(ticket: Dict[str, object]) -> str:
    messages = extract_conversation_messages(ticket)
    customer_points = summarize_messages(messages, "user", limit=3)
    latest_agent_action = latest_meaningful_message(messages, "agent")
    additional_note = collapse_space(
        str((ticket.get("custom_fields") or {}).get("cf_additional_description") or "")
    )

    lines = [
        f"Subject: {collapse_space(str(ticket.get('subject') or '-'))}",
    ]
    if customer_points:
        lines.append(f"Customer request: {customer_points[0]}")
        if len(customer_points) > 1:
            lines.append(f"Customer context: {' | '.join(customer_points[1:])}")
    elif additional_note:
        lines.append(f"Customer request: {additional_note}")

    if latest_agent_action:
        lines.append(f"Support action: {latest_agent_action}")
    if additional_note and additional_note.lower() not in {
        line.split(': ', 1)[1].lower() for line in lines if ': ' in line
    }:
        lines.append(f"Internal note: {additional_note}")

    return "\n".join(lines)


def build_requester_summary(ticket: Dict[str, object]) -> str:
    requester = ticket.get("requester") or {}
    if not isinstance(requester, dict):
        requester = {}
    name = first_non_empty([requester.get("name")])
    email = first_non_empty([requester.get("email")])
    phone = first_non_empty([requester.get("phone"), requester.get("mobile")])
    parts = [part for part in (name, email, phone) if part]
    return " | ".join(parts) if parts else "-"


def build_hubspot_content(ticket: Dict[str, object], client: FreshdeskClient) -> str:
    custom_fields = ticket.get("custom_fields") or {}
    requester = build_requester_summary(ticket)
    lines = [
        "Synced from Freshdesk.",
        f"{HUBSPOT_CONTENT_ID_PREFIX} {ticket.get('id')}",
        f"Freshdesk URL: {client.ticket_url(int(ticket['id']))}",
        f"Type: {ticket.get('type') or '-'}",
        f"Status: {ticket.get('status') or '-'}",
        f"Priority: {ticket.get('priority') or '-'}",
        f"Created At: {ticket.get('created_at') or '-'}",
        f"Updated At: {ticket.get('updated_at') or '-'}",
        f"Requester: {requester or '-'}",
        f"Company ID: {custom_fields.get('cf_company_id') or '-'}",
        f"User ID: {custom_fields.get('cf_user_id892854') or '-'}",
        f"{FRESHDESK_UPDATED_MARKER} {ticket.get('updated_at') or '-'}",
        "",
        "Freshdesk summary:",
        build_freshdesk_summary(ticket),
    ]
    return "\n".join(lines).strip()


def build_hubspot_properties(
    ticket: Dict[str, object],
    freshdesk_client: FreshdeskClient,
    pipeline: PipelineSelection,
    owner: OwnerSelection,
) -> Dict[str, str]:
    is_closed = freshdesk_ticket_is_closed(ticket)
    properties = {
        "subject": collapse_space(str(ticket.get("subject") or f"Freshdesk {ticket['id']}")),
        "content": build_hubspot_content(ticket, freshdesk_client),
        "hs_pipeline": pipeline.pipeline_id,
        "hs_pipeline_stage": (
            pipeline.closed_stage_id if is_closed else pipeline.open_stage_id
        ),
        "hs_ticket_priority": map_priority(ticket.get("priority")),
        "poc_contact_details": build_requester_summary(ticket),
        "source_type": "CHAT",
        "hubspot_owner_id": owner.owner_id,
    }
    return properties


def hubspot_snapshot(ticket: Dict[str, object]) -> Dict[str, str]:
    properties = ticket.get("properties") or {}
    snapshot = {
        "updatedAt": str(ticket.get("updatedAt") or ""),
    }
    for field in TRACKED_HUBSPOT_FIELDS:
        snapshot[field] = collapse_space(str(properties.get(field) or ""))
    return snapshot


def build_link_note(ticket_id: str) -> str:
    return (
        "<p><strong>HubSpot ticket linked.</strong></p>"
        f"<p>HubSpot ticket ID: {escape(ticket_id)}</p>"
        f"<p><em>{HUBSPOT_LINK_MARKER}</em></p>"
    )


def build_hubspot_note_mirror(note: Dict[str, object]) -> Optional[str]:
    properties = note.get("properties") or {}
    note_id = str(note.get("id") or "")
    body = collapse_space(str(properties.get("hs_note_body") or ""))
    if not note_id or not body:
        return None
    timestamp = str(properties.get("hs_timestamp") or note.get("createdAt") or "")
    sections = [
        "<p><strong>HubSpot note mirrored to Freshdesk.</strong></p>",
        html_paragraph(body),
    ]
    if timestamp:
        sections.append(f"<p>HubSpot note timestamp: {escape(timestamp)}</p>")
    sections.append(f"<p><em>{HUBSPOT_NOTE_MARKER_PREFIX}{escape(note_id)}</em></p>")
    return "".join(sections)


def build_hubspot_update_note(
    previous_snapshot: Dict[str, str],
    current_snapshot: Dict[str, str],
    stage_labels: Dict[str, str],
) -> Optional[str]:
    if not previous_snapshot or not previous_snapshot.get("updatedAt"):
        return None
    marker = current_snapshot.get("updatedAt") or ""
    if not marker or previous_snapshot.get("updatedAt") == marker:
        return None

    changes: List[str] = []
    for field in TRACKED_HUBSPOT_FIELDS:
        if field in {"content", "hs_pipeline"}:
            continue
        previous_value = previous_snapshot.get(field, "")
        current_value = current_snapshot.get(field, "")
        if previous_value == current_value:
            continue
        label = field
        if field == "hs_pipeline_stage":
            label = "stage"
            previous_value = stage_labels.get(previous_value, previous_value or "-")
            current_value = stage_labels.get(current_value, current_value or "-")
            changes.append(
                f"<li><strong>{escape(label.title())}:</strong> "
                f"{escape(previous_value or '-')} -> {escape(current_value or '-')}</li>"
            )
            continue
        if field == "hs_ticket_priority":
            label = "priority"
        changes.append(
            f"<li><strong>{escape(label.title())}:</strong> "
            f"{escape(previous_value or '-')} -> {escape(current_value or '-')}</li>"
        )

    if not changes:
        return None

    return (
        "<p><strong>HubSpot ticket updated.</strong></p>"
        "<ul>"
        + "".join(changes)
        + "</ul>"
        + f"<p><em>{HUBSPOT_TICKET_UPDATE_MARKER_PREFIX}{escape(marker)}</em></p>"
    )


def close_freshdesk_from_hubspot(
    *,
    freshdesk_client: FreshdeskClient,
    freshdesk_ticket: Dict[str, object],
    hubspot_ticket: Dict[str, object],
    pipeline: PipelineSelection,
    entry: Dict[str, object],
    dry_run: bool,
) -> bool:
    if freshdesk_ticket_is_closed(freshdesk_ticket):
        return False

    current_stage = collapse_space(
        str((hubspot_ticket.get("properties") or {}).get("hs_pipeline_stage") or "")
    )
    if current_stage != pipeline.closed_stage_id:
        return False

    if dry_run:
        entry["freshdesk_status"] = freshdesk_cli.STATUS_MAP["closed"]
        return True

    payload, _, _ = freshdesk_cli.build_status_payload(
        freshdesk_ticket, freshdesk_cli.STATUS_MAP["closed"]
    )
    updated_ticket = freshdesk_client.update_ticket(int(freshdesk_ticket["id"]), payload)
    entry["freshdesk_status"] = int(updated_ticket.get("status") or 0)
    entry["freshdesk_updated_at"] = str(updated_ticket.get("updated_at") or "")
    return True


def trim_state_entry(entry: Dict[str, object]) -> None:
    note_ids = entry.get("mirrored_hubspot_note_ids")
    if isinstance(note_ids, list) and len(note_ids) > 500:
        entry["mirrored_hubspot_note_ids"] = note_ids[-500:]


def normalize_phone(value: str) -> str:
    digits = "".join(ch for ch in value if ch.isdigit())
    if len(digits) > 10 and digits.startswith("91"):
        return digits[-10:]
    return digits


def choose_hubspot_contact(
    hubspot_client: HubSpotClient, ticket: Dict[str, object]
) -> Optional[Dict[str, object]]:
    requester = ticket.get("requester") or {}
    if not isinstance(requester, dict):
        requester = {}

    requester_email = collapse_space(str(requester.get("email") or "")).lower()
    requester_phone = normalize_phone(str(requester.get("phone") or requester.get("mobile") or ""))
    requester_company = collapse_space(str((ticket.get("company") or {}).get("name") or "")).lower()

    if requester_email:
        matches = hubspot_client.search_contacts(
            [
                {
                    "filters": [
                        {
                            "propertyName": "email",
                            "operator": "EQ",
                            "value": requester_email,
                        }
                    ]
                }
            ],
            limit=5,
        )
        if len(matches) == 1:
            return matches[0]

    if requester_phone:
        matches = hubspot_client.search_contacts(
            [
                {
                    "filters": [
                        {
                            "propertyName": "phone",
                            "operator": "EQ",
                            "value": requester_phone,
                        }
                    ]
                },
                {
                    "filters": [
                        {
                            "propertyName": "mobilephone",
                            "operator": "EQ",
                            "value": requester_phone,
                        }
                    ]
                },
            ],
            limit=10,
        )
        exact_phone_matches: List[Dict[str, object]] = []
        for item in matches:
            properties = item.get("properties") or {}
            phone_values = {
                normalize_phone(str(properties.get("phone") or "")),
                normalize_phone(str(properties.get("mobilephone") or "")),
            }
            if requester_phone not in phone_values:
                continue
            exact_phone_matches.append(item)
        if requester_company:
            company_filtered = [
                item
                for item in exact_phone_matches
                if collapse_space(str((item.get("properties") or {}).get("company") or "")).lower()
                == requester_company
            ]
            if len(company_filtered) == 1:
                return company_filtered[0]
        if len(exact_phone_matches) == 1:
            return exact_phone_matches[0]

    return None


def ensure_contact_association(
    *,
    hubspot_client: HubSpotClient,
    ticket: Dict[str, object],
    hubspot_ticket: Dict[str, object],
    entry: Dict[str, object],
    dry_run: bool,
) -> bool:
    if not hubspot_ticket.get("id"):
        return False
    matched_contact = choose_hubspot_contact(hubspot_client, ticket)
    if matched_contact is None:
        return False
    contact_id = str(matched_contact.get("id") or "")
    if not contact_id:
        return False
    if entry.get("hubspot_contact_id") == contact_id and entry.get("hubspot_contact_associated"):
        return False
    if not dry_run:
        hubspot_client.associate_ticket_to_contact(str(hubspot_ticket["id"]), contact_id)
    entry["hubspot_contact_id"] = contact_id
    entry["hubspot_contact_associated"] = True
    return True


def ensure_deal_associations(
    *,
    hubspot_client: HubSpotClient,
    hubspot_ticket: Dict[str, object],
    entry: Dict[str, object],
    dry_run: bool,
) -> int:
    ticket_id = str(hubspot_ticket.get("id") or "")
    contact_id = str(entry.get("hubspot_contact_id") or "")
    if not ticket_id or not contact_id:
        return 0

    contact_deal_ids = hubspot_client.list_associated_object_ids("contacts", contact_id, "deals")
    if not contact_deal_ids:
        entry["hubspot_deal_ids"] = []
        return 0

    existing_ticket_deal_ids = set(
        hubspot_client.list_associated_object_ids("tickets", ticket_id, "deals")
        if not dry_run
        else [str(value) for value in entry.get("hubspot_deal_ids", [])]
    )

    linked = 0
    for deal_id in contact_deal_ids:
        if deal_id in existing_ticket_deal_ids:
            continue
        if not dry_run:
            hubspot_client.associate_ticket_to_deal(ticket_id, deal_id)
        existing_ticket_deal_ids.add(deal_id)
        linked += 1

    entry["hubspot_deal_ids"] = sorted(existing_ticket_deal_ids, key=lambda value: (len(value), value))
    return linked


def ensure_hubspot_ticket(
    *,
    hubspot_client: HubSpotClient,
    freshdesk_client: FreshdeskClient,
    pipeline: PipelineSelection,
    owner: OwnerSelection,
    ticket: Dict[str, object],
    entry: Dict[str, object],
    dry_run: bool,
) -> Tuple[Optional[Dict[str, object]], bool, bool]:
    desired_properties = build_hubspot_properties(
        ticket, freshdesk_client, pipeline, owner
    )
    desired_hash = stable_hash(desired_properties)
    ticket_id = entry.get("hubspot_ticket_id")

    if ticket_id:
        if dry_run:
            stub = {
                "id": str(ticket_id),
                "updatedAt": str(entry.get("hubspot_updated_at") or ""),
                "properties": dict(entry.get("hubspot_snapshot") or {}),
            }
            entry["freshdesk_properties_hash"] = desired_hash
            return stub, False, False
        try:
            record = hubspot_client.get_ticket(str(ticket_id))
            entry["hubspot_ticket_id"] = str(record.get("id") or ticket_id)
            entry["freshdesk_properties_hash"] = desired_hash
            return record, False, False
        except HttpError as exc:
            if "HTTP 404" not in str(exc):
                raise
            entry.pop("hubspot_ticket_id", None)

    found = hubspot_client.search_ticket_by_freshdesk_id(int(ticket["id"]))
    if found is not None:
        ticket_id = str(found.get("id") or "")
        if not ticket_id:
            raise HttpError(
                f"HubSpot search returned a ticket without an id for Freshdesk {ticket['id']}."
            )
        entry["hubspot_ticket_id"] = ticket_id
        if dry_run:
            entry["freshdesk_properties_hash"] = desired_hash
            return found, False, False
        record = hubspot_client.get_ticket(ticket_id)
        entry["freshdesk_properties_hash"] = desired_hash
        return record, False, False

    if freshdesk_ticket_is_closed(ticket):
        return None, False, False

    if dry_run:
        stub = {
            "id": "dry-run-new",
            "updatedAt": "",
            "properties": desired_properties,
        }
        entry["freshdesk_properties_hash"] = desired_hash
        return stub, True, True

    record = hubspot_client.create_ticket(desired_properties)
    entry["hubspot_ticket_id"] = str(record.get("id") or "")
    entry["freshdesk_properties_hash"] = desired_hash
    return record, True, True


def sync_hubspot_back_to_freshdesk(
    *,
    hubspot_client: HubSpotClient,
    freshdesk_client: FreshdeskClient,
    pipeline: PipelineSelection,
    freshdesk_ticket: Dict[str, object],
    hubspot_ticket: Dict[str, object],
    entry: Dict[str, object],
    dry_run: bool,
) -> Dict[str, int]:
    counts = {
        "freshdesk_notes_added": 0,
        "freshdesk_closed_from_hubspot": 0,
        "hubspot_notes_mirrored": 0,
        "hubspot_updates_noted": 0,
    }

    if close_freshdesk_from_hubspot(
        freshdesk_client=freshdesk_client,
        freshdesk_ticket=freshdesk_ticket,
        hubspot_ticket=hubspot_ticket,
        pipeline=pipeline,
        entry=entry,
        dry_run=dry_run,
    ):
        counts["freshdesk_closed_from_hubspot"] += 1
        if not dry_run:
            freshdesk_ticket = freshdesk_client.get_ticket(int(freshdesk_ticket["id"]))

    current_snapshot = hubspot_snapshot(hubspot_ticket)
    previous_snapshot = entry.get("hubspot_snapshot") or {}
    if not isinstance(previous_snapshot, dict):
        previous_snapshot = {}

    update_note = build_hubspot_update_note(
        previous_snapshot, current_snapshot, pipeline.stage_labels
    )
    if update_note:
        counts["hubspot_updates_noted"] += 1
        counts["freshdesk_notes_added"] += 1
        if not dry_run:
            freshdesk_client.add_note(int(freshdesk_ticket["id"]), update_note, private=True)

    mirrored_note_ids = entry.setdefault("mirrored_hubspot_note_ids", [])
    mirrored_set = {str(value) for value in mirrored_note_ids if str(value).strip()}

    if dry_run:
        note_ids = []
    else:
        note_ids = hubspot_client.list_associated_note_ids(str(hubspot_ticket["id"]))
    for note_id in note_ids:
        if note_id in mirrored_set:
            continue
        note = hubspot_client.get_note(note_id)
        mirror_body = build_hubspot_note_mirror(note)
        if not mirror_body:
            mirrored_set.add(note_id)
            continue
        counts["hubspot_notes_mirrored"] += 1
        counts["freshdesk_notes_added"] += 1
        if not dry_run:
            freshdesk_client.add_note(int(freshdesk_ticket["id"]), mirror_body, private=True)
        mirrored_set.add(note_id)

    entry["mirrored_hubspot_note_ids"] = sorted(
        mirrored_set, key=lambda value: (len(value), value)
    )
    entry["hubspot_snapshot"] = current_snapshot
    entry["hubspot_updated_at"] = current_snapshot.get("updatedAt", "")
    trim_state_entry(entry)
    return counts


def collect_freshdesk_ticket_ids(
    client: FreshdeskClient,
    created_after: str,
    page_limit: int,
) -> List[int]:
    ticket_ids: List[int] = []
    seen: set[int] = set()
    for page in range(1, page_limit + 1):
        response = client.search_task_experience_tickets(created_after=created_after, page=page)
        results = response.get("results") or []
        if not isinstance(results, list):
            break
        if not results:
            break
        for item in results:
            if not isinstance(item, dict):
                continue
            ticket_id = item.get("id")
            if ticket_id is None:
                continue
            try:
                normalized_id = int(ticket_id)
            except (TypeError, ValueError):
                continue
            if normalized_id in seen:
                continue
            seen.add(normalized_id)
            ticket_ids.append(normalized_id)
        if len(results) < DEFAULT_PAGE_SIZE:
            break
    return ticket_ids


def process_ticket(
    *,
    freshdesk_ticket_id: int,
    tickets_state: Dict[str, object],
    freshdesk_client: FreshdeskClient,
    hubspot_client: HubSpotClient,
    pipeline: PipelineSelection,
    owner: OwnerSelection,
    summary: Dict[str, int],
    dry_run: bool,
) -> None:
    entry = tickets_state.setdefault(str(freshdesk_ticket_id), {})
    if not isinstance(entry, dict):
        entry = {}
        tickets_state[str(freshdesk_ticket_id)] = entry

    ticket = freshdesk_client.get_ticket(freshdesk_ticket_id)
    entry["freshdesk_ticket_id"] = freshdesk_ticket_id
    entry["freshdesk_updated_at"] = str(ticket.get("updated_at") or "")
    entry["freshdesk_status"] = int(ticket.get("status") or 0)
    entry["freshdesk_type"] = str(ticket.get("type") or "")

    hubspot_ticket, created, patched = ensure_hubspot_ticket(
        hubspot_client=hubspot_client,
        freshdesk_client=freshdesk_client,
        pipeline=pipeline,
        owner=owner,
        ticket=ticket,
        entry=entry,
        dry_run=dry_run,
    )
    if hubspot_ticket is None:
        summary["skipped_closed_without_mapping"] += 1
        return

    if created:
        summary["hubspot_created"] += 1
        if not dry_run:
            freshdesk_client.add_note(
                freshdesk_ticket_id,
                build_link_note(str(hubspot_ticket.get("id") or "")),
                private=True,
            )
            summary["freshdesk_notes_added"] += 1
    elif patched:
        summary["hubspot_patched"] += 1

    if created:
        if ensure_contact_association(
            hubspot_client=hubspot_client,
            ticket=ticket,
            hubspot_ticket=hubspot_ticket,
            entry=entry,
            dry_run=dry_run,
        ):
            summary["hubspot_contact_linked"] += 1

        summary["hubspot_deal_linked"] += ensure_deal_associations(
            hubspot_client=hubspot_client,
            hubspot_ticket=hubspot_ticket,
            entry=entry,
            dry_run=dry_run,
        )

    backfill_counts = sync_hubspot_back_to_freshdesk(
        hubspot_client=hubspot_client,
        freshdesk_client=freshdesk_client,
        pipeline=pipeline,
        freshdesk_ticket=ticket,
        hubspot_ticket=hubspot_ticket,
        entry=entry,
        dry_run=dry_run,
    )
    for key, value in backfill_counts.items():
        summary[key] += value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Push Freshdesk Task - Experience Team tickets into HubSpot and mirror "
            "HubSpot notes/updates back into Freshdesk."
        )
    )
    parser.add_argument(
        "--freshdesk-env-file",
        default=str(Path(__file__).with_name(".env")),
        help="Path to env file with FRESHDESK_DOMAIN and FRESHDESK_API_KEY",
    )
    parser.add_argument(
        "--hubspot-env-file",
        default="/Users/shishirraj/.config/hubspot.env",
        help="Path to env file with HUBSPOT_ACCESS_TOKEN",
    )
    parser.add_argument(
        "--state-file",
        default=str(Path(__file__).with_name(".freshdesk_hubspot_sync_state.json")),
        help="Path to sync state JSON file",
    )
    parser.add_argument(
        "--created-after",
        default=DEFAULT_CREATED_AFTER,
        help="Sync Task - Experience Team tickets created strictly after this date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--page-limit",
        type=int,
        default=DEFAULT_PAGE_LIMIT,
        help="Maximum Freshdesk search pages to scan on each run",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute actions without writing to Freshdesk or HubSpot",
    )
    parser.add_argument(
        "--status-file",
        default=str(Path(__file__).with_name(DEFAULT_STATUS_FILE_NAME)),
        help="Path to the sync status JSON file",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    status_file = Path(args.status_file)
    previous_status = load_status(status_file)
    status_payload: Dict[str, object] = {
        "last_run_started_at": iso_now(),
        "last_run_finished_at": str(previous_status.get("last_run_finished_at") or ""),
        "last_success_at": str(previous_status.get("last_success_at") or ""),
        "last_exit_code": int(previous_status.get("last_exit_code") or 1),
        "last_error": "",
        "last_summary": dict(previous_status.get("last_summary") or {}),
        "running": True,
        "dry_run": bool(args.dry_run),
        "status_file": str(status_file),
    }
    save_status(status_file, status_payload)

    freshdesk_env = load_env(Path(args.freshdesk_env_file))
    hubspot_env = load_env(Path(args.hubspot_env_file))

    freshdesk_domain = env_or_file("FRESHDESK_DOMAIN", freshdesk_env)
    freshdesk_api_key = env_or_file("FRESHDESK_API_KEY", freshdesk_env)
    hubspot_access_token = env_or_file("HUBSPOT_ACCESS_TOKEN", hubspot_env)

    if not freshdesk_domain or not freshdesk_api_key:
        parser.error("Missing FRESHDESK_DOMAIN or FRESHDESK_API_KEY")
    if not hubspot_access_token:
        parser.error("Missing HUBSPOT_ACCESS_TOKEN")

    state_path = Path(args.state_file)
    state = load_state(state_path)
    tickets_state = state.setdefault("tickets", {})
    if not isinstance(tickets_state, dict):
        tickets_state = {}
        state["tickets"] = tickets_state

    freshdesk_client = FreshdeskClient(freshdesk_domain, freshdesk_api_key)
    hubspot_client = HubSpotClient(hubspot_access_token)
    owner = choose_owner_selection(hubspot_client.list_owners(), hubspot_env)
    pipeline = choose_pipeline_selection(
        hubspot_client.list_ticket_pipelines(), hubspot_env
    )

    summary = {
        "scanned": 0,
        "hubspot_created": 0,
        "hubspot_patched": 0,
        "hubspot_contact_linked": 0,
        "hubspot_deal_linked": 0,
        "skipped_closed_without_mapping": 0,
        "freshdesk_notes_added": 0,
        "freshdesk_closed_from_hubspot": 0,
        "hubspot_notes_mirrored": 0,
        "hubspot_updates_noted": 0,
        "failures": 0,
    }
    exit_code = 1
    try:
        try:
            ticket_ids = collect_freshdesk_ticket_ids(
                freshdesk_client, args.created_after, args.page_limit
            )
        except HttpError as exc:
            print(f"FAIL discovery {exc}")
            status_payload["last_error"] = str(exc)
            return 1

        pending_retry_ids: List[int] = []

        for freshdesk_ticket_id in ticket_ids:
            summary["scanned"] += 1
            try:
                process_ticket(
                    freshdesk_ticket_id=freshdesk_ticket_id,
                    tickets_state=tickets_state,
                    freshdesk_client=freshdesk_client,
                    hubspot_client=hubspot_client,
                    pipeline=pipeline,
                    owner=owner,
                    summary=summary,
                    dry_run=args.dry_run,
                )
            except HttpError as exc:
                pending_retry_ids.append(freshdesk_ticket_id)
                print(f"RETRY ticket={freshdesk_ticket_id} {exc}")
            except Exception as exc:  # pragma: no cover - defensive catch for cron resiliency
                pending_retry_ids.append(freshdesk_ticket_id)
                print(f"RETRY ticket={freshdesk_ticket_id} unexpected={exc}")

        if pending_retry_ids:
            print(
                "INFO retrying_failed_tickets "
                f"count={len(pending_retry_ids)} "
                f"after_seconds={int(DEFAULT_RETRY_FAILED_TICKETS_SECONDS)}"
            )
            time.sleep(DEFAULT_RETRY_FAILED_TICKETS_SECONDS)

        for freshdesk_ticket_id in pending_retry_ids:
            try:
                process_ticket(
                    freshdesk_ticket_id=freshdesk_ticket_id,
                    tickets_state=tickets_state,
                    freshdesk_client=freshdesk_client,
                    hubspot_client=hubspot_client,
                    pipeline=pipeline,
                    owner=owner,
                    summary=summary,
                    dry_run=args.dry_run,
                )
                print(f"RECOVERED ticket={freshdesk_ticket_id}")
            except HttpError as exc:
                summary["failures"] += 1
                print(f"FAIL ticket={freshdesk_ticket_id} {exc}")
            except Exception as exc:  # pragma: no cover - defensive catch for cron resiliency
                summary["failures"] += 1
                print(f"FAIL ticket={freshdesk_ticket_id} unexpected={exc}")

        state["version"] = STATE_VERSION
        if not args.dry_run:
            save_state(state_path, state)

        print(
            "SUMMARY "
            f"scanned={summary['scanned']} "
            f"hubspot_created={summary['hubspot_created']} "
            f"hubspot_patched={summary['hubspot_patched']} "
            f"hubspot_contact_linked={summary['hubspot_contact_linked']} "
            f"hubspot_deal_linked={summary['hubspot_deal_linked']} "
            f"freshdesk_notes_added={summary['freshdesk_notes_added']} "
            f"freshdesk_closed_from_hubspot={summary['freshdesk_closed_from_hubspot']} "
            f"hubspot_notes_mirrored={summary['hubspot_notes_mirrored']} "
            f"hubspot_updates_noted={summary['hubspot_updates_noted']} "
            f"skipped_closed_without_mapping={summary['skipped_closed_without_mapping']} "
            f"failures={summary['failures']} "
            f"dry_run={str(bool(args.dry_run)).lower()}"
        )
        exit_code = 0 if summary["failures"] == 0 else 1
        if exit_code != 0 and not status_payload["last_error"]:
            status_payload["last_error"] = f"run completed with {summary['failures']} failures"
        return exit_code
    except Exception as exc:  # pragma: no cover - defensive catch for status tracking
        status_payload["last_error"] = str(exc)
        print(f"FAIL fatal {exc}")
        return 1
    finally:
        status_payload["running"] = False
        status_payload["last_run_finished_at"] = iso_now()
        status_payload["last_exit_code"] = exit_code
        status_payload["last_summary"] = summary
        if exit_code == 0:
            status_payload["last_success_at"] = str(status_payload["last_run_finished_at"])
            status_payload["last_error"] = ""
        save_status(status_file, status_payload)


if __name__ == "__main__":
    sys.exit(main())
