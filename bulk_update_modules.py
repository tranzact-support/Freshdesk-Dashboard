#!/usr/bin/env python3
import argparse
import sys
import time
import urllib.parse
from pathlib import Path
from typing import Dict, Iterator, Optional, Tuple

import freshdesk_cli
import freshdesk_hubspot_sync


MODULE_RENAMES = {
    "transaction": "Sales & Purchase",
    "purchase reports": "Reports and Intelligence",
    "intelligence quotation": "Lead Management",
    "tally integration": "Accounting Integration",
    "business intelligence": "Reports and Intelligence",
    "buisiness intelligence": "Reports and Intelligence",
    "eway bill": "Sales & Purchase",
    "sub contract": "Quality Control",
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Bulk rename legacy Freshdesk module dropdown values on tickets."
    )
    parser.add_argument(
        "--env-file",
        default=str(Path(__file__).with_name(".env")),
        help="Path to env file with FRESHDESK_DOMAIN and FRESHDESK_API_KEY",
    )
    parser.add_argument(
        "--updated-since",
        default="2024-01-01T00:00:00Z",
        help="Only scan tickets updated on or after this ISO-8601 timestamp.",
    )
    parser.add_argument(
        "--per-page",
        type=int,
        default=100,
        help="Number of tickets to request per page while scanning.",
    )
    parser.add_argument(
        "--pause-seconds",
        type=float,
        default=0.0,
        help="Optional pause between update calls.",
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
        help="Maximum retries for rate-limited list or update requests.",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress per-ticket output and print only counts plus the final summary.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report matches without updating tickets.",
    )
    return parser


def request_with_retries(
    client: freshdesk_cli.FreshdeskClient,
    method: str,
    path: str,
    *,
    payload: Optional[Dict] = None,
    retry_seconds: float,
    max_retries: int,
) -> Tuple[int, Dict, str]:
    attempt = 0
    while True:
        code, data, raw = client.request(method, path, payload)
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
            "GET",
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


def normalize_module(value: object) -> str:
    return str(value or "").strip().casefold()


def build_update_payload(ticket: Dict, target_module: str) -> Tuple[Dict, bool, bool]:
    payload: Dict = {"custom_fields": {"cf_module": target_module}}
    changed_raised_by = False
    changed_type = False

    custom_fields = ticket.get("custom_fields") or {}
    raised_by = custom_fields.get("cf_raised_by")
    if raised_by not in freshdesk_cli.ALLOWED_RAISED_BY:
        payload["custom_fields"]["cf_raised_by"] = "-"
        changed_raised_by = True

    legacy_company_id = custom_fields.get("cf_company_id880506")
    if legacy_company_id in {None, ""}:
        fallback_company_id = custom_fields.get("cf_company_id")
        normalized_company_id = "0" if fallback_company_id in {None, ""} else str(fallback_company_id)
        payload["custom_fields"]["cf_company_id880506"] = normalized_company_id
    elif not isinstance(legacy_company_id, str):
        payload["custom_fields"]["cf_company_id880506"] = str(legacy_company_id)

    ticket_type = ticket.get("type")
    if ticket_type not in freshdesk_cli.ALLOWED_TYPES:
        payload["type"] = "Feature Idea"
        changed_type = True

    return payload, changed_raised_by, changed_type


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    env = freshdesk_cli.load_env(Path(args.env_file))
    domain = freshdesk_hubspot_sync.env_or_file("FRESHDESK_DOMAIN", env)
    api_key = freshdesk_hubspot_sync.env_or_file("FRESHDESK_API_KEY", env)
    if not domain or not api_key:
        parser.error("Missing FRESHDESK_DOMAIN or FRESHDESK_API_KEY")

    client = freshdesk_cli.FreshdeskClient(domain=domain, api_key=api_key)

    scanned = 0
    matched = 0
    updated = 0
    failed = 0
    changed_raised_by = 0
    changed_type = 0
    per_source: Dict[str, int] = {}

    try:
        for ticket in list_tickets(
            client,
            args.updated_since,
            args.per_page,
            args.retry_seconds,
            args.max_retries,
        ):
            scanned += 1
            ticket_id = int(ticket.get("id") or 0)
            custom_fields = ticket.get("custom_fields") or {}
            current_module = str(custom_fields.get("cf_module") or "").strip()
            normalized_current = normalize_module(current_module)
            target_module = MODULE_RENAMES.get(normalized_current)
            if not target_module:
                continue

            matched += 1
            per_source[current_module or "-"] = per_source.get(current_module or "-", 0) + 1

            if args.dry_run:
                if not args.quiet:
                    print(f"DRY {ticket_id} from={current_module!r} to={target_module!r}")
                continue

            payload, changed_rb, changed_tp = build_update_payload(ticket, target_module)
            code, data, raw = request_with_retries(
                client,
                "PUT",
                f"tickets/{ticket_id}",
                payload=payload,
                retry_seconds=args.retry_seconds,
                max_retries=args.max_retries,
            )
            if code == 200 and data:
                updated += 1
                changed_raised_by += int(changed_rb)
                changed_type += int(changed_tp)
                if not args.quiet:
                    print(f"OK {ticket_id} from={current_module!r} to={target_module!r}")
            else:
                message = raw[:300] if raw else "unexpected_response"
                if data:
                    if "message" in data:
                        message = str(data["message"])
                    elif "description" in data:
                        message = str(data["description"])
                    elif "errors" in data and data["errors"]:
                        message = str(data["errors"][0].get("message", message))
                failed += 1
                if not args.quiet:
                    print(
                        f"FAIL {ticket_id} from={current_module!r} to={target_module!r} {message}"
                    )

            if args.pause_seconds > 0:
                time.sleep(args.pause_seconds)
    except RuntimeError as exc:
        print(f"FAIL scan {exc}")
        return 1

    for source_value in sorted(per_source):
        print(f"COUNT from={source_value!r} matched={per_source[source_value]}")

    print(
        "SUMMARY "
        f"scanned={scanned} matched={matched} updated={updated} "
        f"changed_cf_raised_by={changed_raised_by} changed_type={changed_type} "
        f"failed={failed} dry_run={str(bool(args.dry_run)).lower()} "
        f"updated_since={args.updated_since}"
    )
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
