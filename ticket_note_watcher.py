#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import json
import os
import re
import socket
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from html import escape
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple


ELIGIBLE_TYPES = {
    "Bug",
    "Feature Idea",
    "Task - Backend",
    "Task - Experience Team",
}
CHAT_SOURCE = 7
NOTE_MARKER = "auto-ticket-summary:v2"
LEGACY_NOTE_MARKERS = {"auto-ticket-summary:v1"}
SKIP_STATUSES = {4, 5}
SEARCH_PAGE_SIZE = 30
SEARCH_MAX_PAGES = 10
CHAT_TIMESTAMP_RE = re.compile(
    r"\b\d{1,2}:\d{2}\s(?:AM|PM),\s\d{2}(?:st|nd|rd|th)\s[A-Za-z]{3}\b"
)
CHAT_NOISE_EXACT = {
    "take to team inbox",
    "hi",
    "hello",
    "hii",
    "ok",
    "okay",
    "yes",
    "sure",
    "welcome",
    "connecting",
    "send link",
    "ok thankyou",
    "to team inbox",
    "please guide the same",
    "attached image",
    "please batao",
}
CHAT_NOISE_PREFIXES = (
    "and welcome! my name is",
    "hello and welcome!",
    "i can help you in english",
    "greetings from tranzact",
    "how may i help you today",
    "good morning",
    "good afternoon",
    "good evening",
    "apologies as we were away",
    "our support team remains active",
    "we are really sorry for same",
    "quick setup:",
    "please open this link",
    "screenshare pe connect kar sakte hain",
    "thank you for waiting",
    "i have raised a support ticket",
    "is there anything else i can help you with",
    "i'm closing the chat now",
    "please wait check kar raha hoon",
    "sir please wait check kar raha hoon",
    "uska ticket raise kar diya hai",
    "mouse choriye",
    "be connected",
)


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


def _env_or_file(name: str, env_values: Dict[str, str], default: str = "") -> str:
    return os.environ.get(name, env_values.get(name, default)).strip()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


class HttpError(RuntimeError):
    pass


class FreshdeskClient:
    def __init__(self, domain: str, api_key: str):
        self.base = f"https://{domain}/api/v2"
        token = base64.b64encode(f"{api_key}:X".encode("utf-8")).decode("ascii")
        self.auth_header = f"Basic {token}"

    def request(
        self, method: str, path: str, payload: Optional[Dict] = None
    ) -> Tuple[int, Optional[Dict], str]:
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
            return exc.code, parsed, raw

    def search_recent_tickets(
        self, created_after: str, page: int, created_before: Optional[str] = None
    ) -> Dict[str, object]:
        type_query = " OR ".join(f"type:'{value}'" for value in sorted(ELIGIBLE_TYPES))
        query_parts = [f"({type_query})", f"created_at:>'{created_after}'"]
        if created_before:
            query_parts.append(f"created_at:<'{created_before}'")
        query = " AND ".join(query_parts)
        encoded = urllib.parse.quote(query, safe="")
        code, data, raw = self.request(
            "GET", f"search/tickets?query=\"{encoded}\"&page={page}"
        )
        if code != 200 or not isinstance(data, dict):
            raise HttpError(
                "Freshdesk search failed with HTTP {0}: {1}".format(code, raw[:400])
            )
        return data

    def list_conversations(self, ticket_id: int) -> List[Dict[str, object]]:
        code, data, raw = self.request("GET", f"tickets/{ticket_id}/conversations")
        if code != 200 or not isinstance(data, list):
            raise HttpError(
                "Freshdesk conversations failed for ticket {0} with HTTP {1}: {2}".format(
                    ticket_id, code, raw[:400]
                )
            )
        return data

    def get_ticket(self, ticket_id: int) -> Dict[str, object]:
        code, data, raw = self.request("GET", f"tickets/{ticket_id}")
        if code != 200 or not isinstance(data, dict):
            raise HttpError(
                "Freshdesk ticket fetch failed for ticket {0} with HTTP {1}: {2}".format(
                    ticket_id, code, raw[:400]
                )
            )
        return data

    def add_note(self, ticket_id: int, body: str, private: bool = True) -> None:
        code, data, raw = self.request(
            "POST", f"tickets/{ticket_id}/notes", {"body": body, "private": private}
        )
        if code != 201:
            details = raw[:400]
            if isinstance(data, dict):
                details = json.dumps(data)
            raise HttpError(
                "Freshdesk add note failed for ticket {0} with HTTP {1}: {2}".format(
                    ticket_id, code, details
                )
            )


class OpenAIClient:
    def __init__(self, base_url: str, api_key: str, model: str):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.model = model

    def summarize(self, ticket: Dict[str, object]) -> "SummaryResult":
        ticket_description = extract_ticket_description(ticket)
        chat_transcript = extract_chat_transcript(ticket)
        payload = {
            "model": self.model,
            "instructions": (
                "You write short internal support ticket notes. "
                "Return only valid JSON and do not use markdown code fences."
            ),
            "input": build_openai_prompt(ticket, ticket_description, chat_transcript),
            "max_output_tokens": 450,
            "text": {
                "format": {
                    "type": "json_schema",
                    "name": "ticket_note_summary",
                    "strict": True,
                    "schema": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "quick_summary": {"type": "string"},
                            "issue": {"type": "string"},
                            "chat_transcript_summary": {"type": "string"},
                        },
                        "required": [
                            "quick_summary",
                            "issue",
                            "chat_transcript_summary",
                        ],
                    },
                }
            },
        }
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        req = urllib.request.Request(
            url=f"{self.base_url}/responses",
            data=json.dumps(payload).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            details = exc.read().decode("utf-8", errors="replace")
            raise HttpError(
                "OpenAI request failed with HTTP {0}: {1}".format(exc.code, details[:400])
            ) from exc
        except (urllib.error.URLError, TimeoutError, socket.timeout) as exc:
            raise HttpError(f"OpenAI request failed: {exc}") from exc
        data = json.loads(raw)
        parsed = extract_openai_json(data)
        return SummaryResult(
            quick_summary=collapse_space(str(parsed["quick_summary"])),
            issue=collapse_space(str(parsed["issue"])),
            chat_transcript_summary=collapse_space(
                str(parsed["chat_transcript_summary"])
            ),
            source="openai",
        )


@dataclass(frozen=True)
class SummaryResult:
    quick_summary: str
    issue: str
    chat_transcript_summary: str
    source: str


def extract_openai_json(data: Dict[str, object]) -> Dict[str, object]:
    output_text = data.get("output_text")
    if isinstance(output_text, str) and output_text.strip():
        return json.loads(output_text)

    for item in data.get("output", []):
        if not isinstance(item, dict) or item.get("type") != "message":
            continue
        for content in item.get("content", []):
            if not isinstance(content, dict):
                continue
            text = content.get("text")
            if isinstance(text, str) and text.strip():
                return json.loads(text)

    raise HttpError("Could not extract JSON from OpenAI response.")


def build_openai_prompt(
    ticket: Dict[str, object], ticket_description: str, chat_transcript: str
) -> str:
    custom_fields = ticket.get("custom_fields") or {}
    lines = [
        "Write three concise internal note fields for this support ticket.",
        "The audience is the technical team.",
        "Use both the ticket metadata and the chat transcript when the chat transcript is available.",
        "quick_summary: 2 to 3 sentences. Combine the ticket context and the chat transcript into a clear operational summary.",
        "issue: 1 sentence. State the core customer-facing problem or blocker in direct terms.",
        "chat_transcript_summary: 1 to 3 sentences summarizing the relevant customer and agent exchange. Return an empty string if chat transcript is unavailable.",
        "Avoid greetings, assumptions, and promises.",
        "",
        "Ticket:",
        f"id: {ticket.get('id')}",
        f"subject: {ticket.get('subject') or ''}",
        f"type: {ticket.get('type') or ''}",
        f"priority: {ticket.get('priority') or ''}",
        f"source: {ticket.get('source') or ''}",
        f"module: {custom_fields.get('cf_module') or ''}",
        f"company_id: {custom_fields.get('cf_company_id') or ''}",
        f"user_id: {custom_fields.get('cf_user_id892854') or ''}",
        "",
        "Ticket description:",
        ticket_description or "",
        "",
        "Chat transcript:",
        chat_transcript or "",
    ]
    return "\n".join(lines)


def heuristic_summary(ticket: Dict[str, object]) -> SummaryResult:
    subject = collapse_space(str(ticket.get("subject") or "No subject"))
    custom_fields = ticket.get("custom_fields") or {}
    module = custom_fields.get("cf_module") or "Unknown module"
    ticket_type = ticket.get("type") or "Unknown"
    ticket_description = extract_ticket_description(ticket)
    chat_transcript = extract_chat_transcript(ticket)
    issue = build_heuristic_issue(
        subject=subject,
        module=module,
        chat_transcript=chat_transcript,
        ticket_description=ticket_description,
    )
    quick_summary = f"{subject}. Ticket type is {ticket_type} in {module}."
    if chat_transcript:
        quick_summary = (
            f"{quick_summary} Chat transcript indicates: "
            f"{first_meaningful_line(chat_transcript) or subject}"
        )
    elif ticket_description:
        quick_summary = (
            f"{quick_summary} Ticket description indicates: "
            f"{first_meaningful_line(ticket_description) or subject}"
        )
    return SummaryResult(
        quick_summary=collapse_space(quick_summary),
        issue=collapse_space(issue),
        chat_transcript_summary=collapse_space(first_meaningful_line(chat_transcript)),
        source="heuristic",
    )


def build_heuristic_issue(
    subject: str, module: str, chat_transcript: str, ticket_description: str
) -> str:
    context = build_issue_context(subject, module)
    impact_source = first_meaningful_line(chat_transcript) or first_meaningful_line(
        ticket_description
    )
    impact = normalize_issue_impact(impact_source)
    if context and impact:
        return f"{context}, {impact}."
    if context:
        return f"{context}."
    if impact:
        return capitalize_sentence(impact)
    return "Customer reported an issue requiring technical review."


def build_issue_context(subject: str, module: str) -> str:
    normalized_subject = collapse_space(subject).rstrip(".!? ")
    lowered_subject = normalized_subject.lower()
    if normalized_subject and normalized_subject != "No subject":
        if module and module != "Unknown module" and module.lower() not in lowered_subject:
            return f"{normalized_subject} in {module}"
        return normalized_subject
    if module and module != "Unknown module":
        return f"Issue reported in {module}"
    return ""


def normalize_issue_impact(text: str) -> str:
    cleaned = collapse_space(text)
    if not cleaned:
        return ""
    lowered = cleaned.lower().rstrip(".")
    if "unable to start production process" in lowered:
        return "blocking the customer from starting the production process"
    if "cannot start production process" in lowered:
        return "blocking the customer from starting the production process"
    replacements = {
        "we are unable to start production process": "blocking the customer from starting the production process",
        "unable to start production process": "blocking the customer from starting the production process",
        "cannot start production process": "blocking the customer from starting the production process",
        "customer unable to start production process": "blocking the customer from starting the production process",
    }
    if lowered in replacements:
        return replacements[lowered]
    if lowered.startswith("unable to "):
        return f"blocking the customer from {lowered[10:]}"
    if lowered.startswith("we are unable to "):
        return f"blocking the customer from {lowered[17:]}"
    if lowered.startswith("customer is unable to "):
        return f"blocking the customer from {lowered[22:]}"
    if lowered.startswith("customer unable to "):
        return f"blocking the customer from {lowered[19:]}"
    return ""


def capitalize_sentence(text: str) -> str:
    if not text:
        return text
    return text[0].upper() + text[1:]


def normalize_text_block(raw: str, limit: int = 12) -> str:
    lines = []
    for line in raw.splitlines():
        cleaned = collapse_space(line)
        if not cleaned:
            continue
        if is_noise_line(cleaned):
            continue
        lines.append(cleaned)
    return "\n".join(lines[:limit])


def is_chat_ticket(ticket: Dict[str, object]) -> bool:
    return int(ticket.get("source") or 0) == CHAT_SOURCE


def extract_ticket_description(ticket: Dict[str, object]) -> str:
    if is_chat_ticket(ticket):
        return ""
    return normalize_text_block(str(ticket.get("description_text") or ""))


def extract_chat_transcript(ticket: Dict[str, object]) -> str:
    if not is_chat_ticket(ticket):
        return ""
    raw = str(ticket.get("description_text") or "")
    parsed_lines = parse_flat_chat_transcript(raw)
    if parsed_lines:
        return "\n".join(parsed_lines[:20])
    return normalize_text_block(raw, limit=20)


def first_meaningful_line(text: str) -> str:
    for line in text.splitlines():
        cleaned = collapse_space(line)
        if len(cleaned) >= 8 and not is_noise_line(cleaned):
            return cleaned
    return ""


def collapse_space(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def is_noise_line(text: str) -> bool:
    lowered = collapse_space(text).lower()
    if not lowered:
        return True
    if lowered in CHAT_NOISE_EXACT:
        return True
    return any(lowered.startswith(prefix) for prefix in CHAT_NOISE_PREFIXES)


def parse_flat_chat_transcript(raw: str) -> List[str]:
    matches = list(CHAT_TIMESTAMP_RE.finditer(raw))
    if not matches:
        return []

    lines: List[str] = []
    for index, match in enumerate(matches):
        chunk_start = match.end()
        chunk_end = matches[index + 1].start() if index + 1 < len(matches) else len(raw)
        message = extract_chat_message(raw[chunk_start:chunk_end])
        if not message or is_noise_line(message):
            continue
        lines.append(message)
    return lines


def extract_chat_message(chunk: str) -> str:
    parts = [
        collapse_space(part)
        for part in re.split(r"\s{2,}", chunk)
        if collapse_space(part)
    ]
    if not parts:
        return ""
    if len(parts) == 1:
        return "" if re.fullmatch(r"[A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2}", parts[0]) else parts[0]
    return parts[-1]


def strip_speaker_prefix(text: str) -> str:
    tokens = collapse_space(text).split()
    start = 0
    while start < len(tokens) and (tokens[start].startswith("<") or tokens[start].startswith("[")):
        start += 1

    trimmed = tokens[start:]
    for speaker_len in (3, 2, 1):
        if len(trimmed) <= speaker_len:
            continue
        speaker_tokens = trimmed[:speaker_len]
        if not all(re.fullmatch(r"[A-Z][a-z]+", token) for token in speaker_tokens):
            continue

        candidate_tokens = trimmed[speaker_len:]
        candidate = collapse_space(" ".join(candidate_tokens))
        if not candidate:
            continue
        if is_noise_line(candidate):
            trimmed = candidate_tokens
            break
        if re.fullmatch(r"[a-z].*", candidate_tokens[0]):
            trimmed = candidate_tokens
            break
        if (
            len(candidate_tokens) >= 2
            and re.fullmatch(r"[A-Z][a-z]+", candidate_tokens[0])
            and re.fullmatch(r"[a-z].*", candidate_tokens[1])
        ):
            trimmed = candidate_tokens
            break

    cleaned = " ".join(trimmed)
    return collapse_space(cleaned)


def format_note(summary: SummaryResult) -> str:
    return (
        f"<p><strong>Quick summary:</strong> {escape(summary.quick_summary)}</p>"
        f"<p><strong>Issue:</strong> {escape(summary.issue)}</p>"
        f"<p><strong>Chat transcript summary:</strong> "
        f"{escape(summary.chat_transcript_summary)}</p>"
        f"<p><em>{NOTE_MARKER} | source={escape(summary.source)}</em></p>"
    )


def load_state(path: Path) -> Set[int]:
    if not path.exists():
        return set()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return set()
    ids = data.get("processed_ticket_ids", [])
    return {int(value) for value in ids if str(value).isdigit()}


def load_ticket_ids(path: Path) -> List[int]:
    text = path.read_text(encoding="utf-8")
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        data = None
    if isinstance(data, dict):
        values = data.get("processed_ticket_ids", [])
        return [int(value) for value in values if str(value).isdigit()]
    seen: Set[int] = set()
    ticket_ids: List[int] = []
    for match in re.findall(r"\d+", text):
        ticket_id = int(match)
        if ticket_id not in seen:
            seen.add(ticket_id)
            ticket_ids.append(ticket_id)
    return ticket_ids


def save_state(path: Path, ticket_ids: Iterable[int]) -> None:
    payload = {"processed_ticket_ids": sorted(set(int(ticket_id) for ticket_id in ticket_ids))}
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def load_status(path: Path) -> Dict[str, object]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def save_status(path: Path, payload: Dict[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def record_status_start(path: Path) -> Dict[str, object]:
    status = load_status(path)
    status["last_run_started_at"] = utc_now_iso()
    status["running"] = True
    save_status(path, status)
    return status


def record_status_finish(
    path: Path,
    prior_status: Dict[str, object],
    *,
    success: bool,
    error: str = "",
    summary: Optional[Dict[str, object]] = None,
) -> None:
    status = dict(prior_status)
    finished_at = utc_now_iso()
    status["last_run_finished_at"] = finished_at
    status["running"] = False
    status["last_exit_code"] = 0 if success else 1
    status["last_error"] = error
    if summary is not None:
        status["last_summary"] = summary
    failures = int(status.get("consecutive_failures") or 0)
    if success:
        status["last_success_at"] = finished_at
        status["consecutive_failures"] = 0
    else:
        status["consecutive_failures"] = failures + 1
    save_status(path, status)


def ticket_note_status(client: FreshdeskClient, ticket_id: int) -> Tuple[bool, bool]:
    try:
        conversations = client.list_conversations(ticket_id)
    except HttpError:
        return False, False
    has_current = False
    has_legacy = False
    for item in conversations:
        body = str(item.get("body") or "")
        if NOTE_MARKER in body:
            has_current = True
        if any(marker in body for marker in LEGACY_NOTE_MARKERS):
            has_legacy = True
    return has_current, has_legacy


def fetch_candidates_by_ids(
    client: FreshdeskClient, ticket_ids: Iterable[int], limit: int
) -> List[Dict[str, object]]:
    results: List[Dict[str, object]] = []
    for ticket_id in ticket_ids:
        if len(results) >= limit:
            break
        try:
            ticket = client.get_ticket(int(ticket_id))
        except HttpError:
            continue
        if ticket.get("type") not in ELIGIBLE_TYPES:
            continue
        if int(ticket.get("status") or 0) in SKIP_STATUSES:
            continue
        results.append(ticket)
    results.sort(key=lambda item: str(item.get("created_at") or ""))
    return results


def fetch_recent_candidates(
    client: FreshdeskClient, created_after: str, limit: int
) -> List[Dict[str, object]]:
    start_date = datetime.strptime(created_after, "%Y-%m-%d").date()
    end_date = (utc_now() + timedelta(days=1)).date()
    results: List[Dict[str, object]] = []
    seen_ids: Set[int] = set()

    def append_page_results(page_results: object) -> None:
        if not isinstance(page_results, list):
            return
        for item in page_results:
            if not isinstance(item, dict):
                continue
            ticket_id = int(item.get("id") or 0)
            if not ticket_id or ticket_id in seen_ids:
                continue
            if item.get("type") in ELIGIBLE_TYPES and int(item.get("status") or 0) not in SKIP_STATUSES:
                seen_ids.add(ticket_id)
                results.append(item)
                if len(results) >= limit:
                    return

    def fetch_window(window_start, window_end) -> None:
        if len(results) >= limit or window_start >= window_end:
            return

        first_payload = client.search_recent_tickets(
            created_after=window_start.isoformat(),
            created_before=window_end.isoformat(),
            page=1,
        )
        first_page_results = first_payload.get("results", [])
        total = int(first_payload.get("total", 0) or 0)
        span_days = (window_end - window_start).days
        if total > SEARCH_PAGE_SIZE * SEARCH_MAX_PAGES and span_days > 1:
            midpoint = window_start + timedelta(days=max(1, span_days // 2))
            fetch_window(window_start, midpoint)
            fetch_window(midpoint, window_end)
            return

        if total == 0:
            return

        append_page_results(first_page_results)
        if len(results) >= limit:
            return

        max_pages = min(
            SEARCH_MAX_PAGES,
            ((total - 1) // SEARCH_PAGE_SIZE) + 1,
        )
        for page in range(2, max_pages + 1):
            payload = client.search_recent_tickets(
                created_after=window_start.isoformat(),
                created_before=window_end.isoformat(),
                page=page,
            )
            page_results = payload.get("results", [])
            if not isinstance(page_results, list) or not page_results:
                break
            append_page_results(page_results)
            if len(results) >= limit or len(page_results) < SEARCH_PAGE_SIZE:
                break

    fetch_window(start_date, end_date)
    results.sort(key=lambda item: str(item.get("created_at") or ""))
    return results


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Watch new Freshdesk Bug/Task/Feature Idea tickets and add summary notes."
    )
    parser.add_argument(
        "--env-file",
        default=str(Path(__file__).with_name(".env")),
        help="Path to env file with Freshdesk and OpenAI settings.",
    )
    parser.add_argument(
        "--state-file",
        help="Optional override for state file path.",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        help="How many past days to include in the search window.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Maximum candidate tickets to inspect per run.",
    )
    parser.add_argument(
        "--created-after",
        help="Date filter in YYYY-MM-DD format. Overrides --lookback-days.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the generated note instead of posting it.",
    )
    parser.add_argument(
        "--status-file",
        help="Optional override for watcher status file path.",
    )
    parser.add_argument(
        "--ticket-ids-file",
        help="Optional path to a file containing specific ticket IDs or a watcher state JSON file.",
    )
    parser.add_argument(
        "--refresh-existing",
        action="store_true",
        help="Add a new summary note for tickets that only have a legacy summary marker.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    env = load_env(Path(args.env_file))

    domain = _env_or_file("FRESHDESK_DOMAIN", env)
    api_key = _env_or_file("FRESHDESK_API_KEY", env)
    if not domain or not api_key:
        parser.error("Missing FRESHDESK_DOMAIN or FRESHDESK_API_KEY.")

    openai_base_url = _env_or_file("OPENAI_BASE_URL", env, "https://api.openai.com/v1")
    openai_api_key = _env_or_file("OPENAI_API_KEY", env)
    openai_model = _env_or_file("OPENAI_MODEL", env, "gpt-4o-mini")
    lookback_days = args.lookback_days or int(_env_or_file("WATCH_LOOKBACK_DAYS", env, "2"))
    limit = args.limit or int(_env_or_file("WATCH_LIMIT", env, "100"))
    state_file = Path(
        args.state_file
        or _env_or_file(
            "WATCH_STATE_FILE",
            env,
            str(Path(__file__).with_name(".ticket_note_watcher_state.json")),
        )
    )
    status_file = Path(
        args.status_file
        or _env_or_file(
            "WATCH_STATUS_FILE",
            env,
            str(Path(__file__).with_name(".ticket_note_watcher_status.json")),
        )
    )

    client = FreshdeskClient(domain=domain, api_key=api_key)
    openai_client = (
        OpenAIClient(openai_base_url, openai_api_key, openai_model)
        if openai_api_key
        else None
    )
    processed_ids = load_state(state_file)
    created_after = args.created_after or (
        utc_now() - timedelta(days=lookback_days)
    ).strftime("%Y-%m-%d")
    status = record_status_start(status_file)

    try:
        if args.ticket_ids_file:
            ticket_ids = load_ticket_ids(Path(args.ticket_ids_file))
            candidates = fetch_candidates_by_ids(client, ticket_ids=ticket_ids, limit=limit)
        else:
            candidates = fetch_recent_candidates(client, created_after=created_after, limit=limit)
    except HttpError as exc:
        print(f"FAIL search {exc}", file=sys.stderr)
        record_status_finish(
            status_file,
            status,
            success=False,
            error=f"search: {exc}",
        )
        return 1

    added = skipped = 0
    failures = 0
    last_error = ""
    for ticket in candidates:
        ticket_id = int(ticket["id"])
        if not args.refresh_existing and ticket_id in processed_ids:
            skipped += 1
            continue
        has_current_note, has_legacy_note = ticket_note_status(client, ticket_id)
        if args.refresh_existing:
            if has_current_note or not has_legacy_note:
                processed_ids.add(ticket_id)
                skipped += 1
                continue
        else:
            if has_current_note or has_legacy_note:
                processed_ids.add(ticket_id)
                skipped += 1
                continue

        try:
            summary = openai_client.summarize(ticket) if openai_client else heuristic_summary(ticket)
            note_body = format_note(summary)
            if args.dry_run:
                print(f"DRY_RUN {ticket_id} {summary.source}")
                print(note_body)
            else:
                client.add_note(ticket_id, note_body, private=True)
                print(f"OK {ticket_id} {summary.source}")
                processed_ids.add(ticket_id)
            added += 1
        except HttpError as exc:
            print(f"FAIL {ticket_id} {exc}", file=sys.stderr)
            failures += 1
            last_error = f"ticket {ticket_id}: {exc}"

    save_state(state_file, processed_ids)
    summary = {
        "added": added,
        "skipped": skipped,
        "candidates": len(candidates),
        "failures": failures,
        "state_file": str(state_file),
    }
    print("SUMMARY " + " ".join(f"{key}={value}" for key, value in summary.items()))
    record_status_finish(
        status_file,
        status,
        success=failures == 0,
        error=last_error,
        summary=summary,
    )
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
