#!/usr/bin/env python3
import argparse
import csv
import re
import sys
import time
import urllib.parse
from html import unescape
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple

import freshdesk_cli
import freshdesk_hubspot_sync


MODULE_ALIASES = {
    "home": "Home",
    "sales & purchase": "Sales & Purchase",
    "sales and purchase": "Sales & Purchase",
    "lead management": "Lead Management",
    "task dashboard": "Task Dashboard",
    "payments": "Payments",
    "inventory": "Inventory",
    "production": "Production",
    "resource planning": "Resource Planning",
    "quality control": "Quality Control",
    "buyers and suppliers": "Buyers and Suppliers",
    "approvals": "Approvals",
    "reports and intelligence": "Reports and Intelligence",
    "report and intelligence": "Reports and Intelligence",
    "purchase reports": "Reports and Intelligence",
    "business intelligence": "Reports and Intelligence",
    "buisiness intelligence": "Reports and Intelligence",
    "accounting integration": "Accounting Integration",
    "tally integration": "Accounting Integration",
    "lead management quotation": "Lead Management",
    "intelligence quotation": "Lead Management",
    "settings": "Settings",
    "eway bill": "Sales & Purchase",
    "sub contract": "Quality Control",
}

STATUS_LABELS = {
    2: "Open",
    3: "Pending",
    6: "Pending At Tech",
    7: "Tech Accepted",
    8: "Rejected / Not to be done",
    9: "Picked by Tech",
    15: "Done By Tech",
    16: "To be Picked in Next TS",
    17: "Previous Tickets",
    18: "Sent To Product",
    19: "Sent To Mobile Dev",
    4: "Resolved",
    5: "Closed",
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Export Freshdesk tickets whose Module field is blank."
    )
    parser.add_argument(
        "--env-file",
        default=str(Path(__file__).with_name(".env")),
        help="Path to env file with FRESHDESK_DOMAIN and FRESHDESK_API_KEY",
    )
    parser.add_argument(
        "--updated-since",
        default="2020-01-01T00:00:00Z",
        help="Only scan tickets updated on or after this ISO-8601 timestamp.",
    )
    parser.add_argument(
        "--per-page",
        type=int,
        default=100,
        help="Number of tickets to request per page while scanning.",
    )
    parser.add_argument(
        "--retry-seconds",
        type=float,
        default=15.0,
        help="Seconds to wait before retrying after HTTP 429.",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=8,
        help="Maximum retries for rate-limited list requests.",
    )
    parser.add_argument(
        "--output",
        default=str(Path(__file__).with_name("blank_module_tickets.csv")),
        help="CSV file to write.",
    )
    return parser


def request_with_retries(
    client: freshdesk_cli.FreshdeskClient,
    path: str,
    *,
    retry_seconds: float,
    max_retries: int,
) -> Tuple[int, Optional[List[Dict]], str]:
    attempt = 0
    while True:
        code, data, raw = client.request("GET", path)
        if code != 429:
            return code, data, raw
        if attempt >= max_retries:
            return code, data, raw
        attempt += 1
        time.sleep(retry_seconds * attempt)


def list_tickets(
    client: freshdesk_cli.FreshdeskClient,
    updated_since: str,
    per_page: int,
    retry_seconds: float,
    max_retries: int,
) -> Iterator[Dict]:
    page = 1
    while True:
        query = urllib.parse.urlencode(
            {
                "page": page,
                "per_page": per_page,
                "updated_since": updated_since,
                "include": "description",
            }
        )
        code, data, raw = request_with_retries(
            client,
            f"tickets?{query}",
            retry_seconds=retry_seconds,
            max_retries=max_retries,
        )
        if code != 200 or not isinstance(data, list):
            message = raw[:400] if raw else "unexpected_response"
            raise RuntimeError(f"Ticket listing failed on page {page} with HTTP {code}: {message}")

        if not data:
            break

        for ticket in data:
            if isinstance(ticket, dict):
                yield ticket

        if len(data) < per_page:
            break
        page += 1


def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", unescape(text or "")).strip()


def strip_html(text: str) -> str:
    no_tags = re.sub(r"<[^>]+>", " ", text or "")
    return normalize_whitespace(no_tags)


def detect_modules(*texts: str) -> str:
    haystack = " ".join(normalize_whitespace(text).casefold() for text in texts if text)
    matches: List[str] = []
    seen = set()
    for alias, canonical in MODULE_ALIASES.items():
        if alias in haystack and canonical not in seen:
            seen.add(canonical)
            matches.append(canonical)
    return ", ".join(matches)


def export_rows(rows: List[Dict[str, str]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "ticket_id",
                "created_at",
                "updated_at",
                "status_code",
                "status",
                "priority",
                "type",
                "subject",
                "mentioned_module_guess",
                "additional_description",
                "creation_text",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    env = freshdesk_cli.load_env(Path(args.env_file))
    domain = freshdesk_hubspot_sync.env_or_file("FRESHDESK_DOMAIN", env)
    api_key = freshdesk_hubspot_sync.env_or_file("FRESHDESK_API_KEY", env)
    if not domain or not api_key:
        parser.error("Missing FRESHDESK_DOMAIN or FRESHDESK_API_KEY")

    client = freshdesk_cli.FreshdeskClient(domain=domain, api_key=api_key)

    rows: List[Dict[str, str]] = []
    scanned = 0
    try:
        for ticket in list_tickets(
            client,
            args.updated_since,
            args.per_page,
            args.retry_seconds,
            args.max_retries,
        ):
            scanned += 1
            custom_fields = ticket.get("custom_fields") or {}
            module = str(custom_fields.get("cf_module") or "").strip()
            if module:
                continue

            subject = normalize_whitespace(str(ticket.get("subject") or ""))
            additional_description = normalize_whitespace(
                str(custom_fields.get("cf_additional_description") or "")
            )
            creation_text = normalize_whitespace(
                str(ticket.get("description_text") or "") or strip_html(str(ticket.get("description") or ""))
            )
            mentioned_module_guess = detect_modules(subject, additional_description, creation_text)

            rows.append(
                {
                    "ticket_id": str(ticket.get("id") or ""),
                    "created_at": str(ticket.get("created_at") or ""),
                    "updated_at": str(ticket.get("updated_at") or ""),
                    "status_code": str(ticket.get("status") or ""),
                    "status": STATUS_LABELS.get(int(ticket.get("status") or 0), str(ticket.get("status") or "")),
                    "priority": str(ticket.get("priority") or ""),
                    "type": str(ticket.get("type") or ""),
                    "subject": subject,
                    "mentioned_module_guess": mentioned_module_guess,
                    "additional_description": additional_description,
                    "creation_text": creation_text,
                }
            )
    except RuntimeError as exc:
        print(f"FAIL {exc}")
        return 1

    rows.sort(key=lambda row: int(row["ticket_id"]), reverse=True)
    output_path = Path(args.output)
    export_rows(rows, output_path)
    print(
        "SUMMARY "
        f"scanned={scanned} blank_module={len(rows)} output={output_path}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
