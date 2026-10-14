#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import freshdesk_activity_dashboard as dashboard
import freshdesk_cli
import freshdesk_hubspot_sync


DEFAULT_DASHBOARD_JSON = Path(__file__).with_name("dashboard") / "freshdesk_activity_dashboard.json"
DEFAULT_COMPANY_CACHE = Path(__file__).with_name("dashboard") / ".hubspot_company_deal_cache.json"
DEFAULT_REPORT_DIR = Path(__file__).with_name("reports")
DEFAULT_HUBSPOT_ENV_FILE = Path("/Users/shishirraj/.config/hubspot.env")
MANDATORY_TEXT_FIELDS = {
    "cf_company_id880506",
    "cf_additional_description",
    "cf_user_id892854",
}
SAFE_DASH_VALUE = "-"
SAFE_FRESHDESK_TYPE = "Feature Idea"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill HubSpot deal URLs onto Freshdesk tickets and repair legacy mandatory fields "
            "that block updates."
        )
    )
    parser.add_argument(
        "--env-file",
        default=str(Path(__file__).with_name(".env")),
        help="Path to env file with FRESHDESK_DOMAIN and FRESHDESK_API_KEY",
    )
    parser.add_argument(
        "--hubspot-env-file",
        default=str(DEFAULT_HUBSPOT_ENV_FILE),
        help="Path to env file with HUBSPOT_ACCESS_TOKEN and HUBSPOT_PORTAL_ID",
    )
    parser.add_argument(
        "--dashboard-json",
        default=str(DEFAULT_DASHBOARD_JSON),
        help="Dashboard JSON used as the ticket source for missing HubSpot URLs",
    )
    parser.add_argument(
        "--company-cache",
        default=str(DEFAULT_COMPANY_CACHE),
        help="Cache file storing company->deal lookup results",
    )
    parser.add_argument(
        "--report-dir",
        default=str(DEFAULT_REPORT_DIR),
        help="Directory where CSV and JSON reports will be written",
    )
    parser.add_argument(
        "--pause-seconds",
        type=float,
        default=0.25,
        help="Optional pause between successful update calls",
    )
    parser.add_argument(
        "--max-tickets",
        type=int,
        help="Optional safety limit on how many tickets to attempt in this run",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Resolve URLs and compute changes without updating Freshdesk",
    )
    return parser


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def normalize_company_name(value: object) -> str:
    return " ".join(str(value or "").split()).strip().lower()


def load_json(path: Path, default: object) -> object:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def get_company_cache(path: Path) -> Dict[str, str]:
    payload = load_json(path, {})
    if isinstance(payload, dict):
        deals = payload.get("company_deals")
        if isinstance(deals, dict):
            return {str(key): str(value) for key, value in deals.items()}
    return {}


def save_company_cache(path: Path, company_deals: Dict[str, str]) -> None:
    write_json(path, {"generated_at": utc_now_iso(), "company_deals": company_deals})


def choose_deal_url_for_company(
    *,
    company_name: str,
    company_cache: Dict[str, str],
    hubspot_client: freshdesk_hubspot_sync.HubSpotClient,
    hubspot_ui_domain: str,
    hubspot_portal_id: str,
) -> Tuple[str, str, int, str]:
    cache_key = normalize_company_name(company_name)
    cached_value = str(company_cache.get(cache_key) or "")
    cached_parts = [part.strip() for part in cached_value.split("|")]
    if len(cached_parts) == 3:
        cached_name, cached_url, cached_count = cached_parts
        try:
            parsed_count = int(cached_count or "0")
        except ValueError:
            parsed_count = 0
        if cached_url:
            return cached_name or company_name, cached_url, parsed_count, "cache"

    matches = hubspot_client.search_companies(company_name, limit=10)
    matched_company = dashboard.choose_best_hubspot_company_match(company_name, matches)
    resolved_name = company_name
    deal_ids: List[str] = []
    source = "none"
    if matched_company is not None:
        properties = matched_company.get("properties") if isinstance(matched_company.get("properties"), dict) else {}
        resolved_name = str((properties or {}).get("name") or company_name).strip() or company_name
        company_id = str(matched_company.get("id") or "").strip()
        if company_id:
            deal_ids = hubspot_client.list_associated_object_ids("companies", company_id, "deals")
            if deal_ids:
                source = "company"

    if not deal_ids:
        contacts = hubspot_client.search_contacts(
            [{"filters": [{"propertyName": "company", "operator": "EQ", "value": company_name}]}],
            limit=10,
        )
        for contact in contacts:
            contact_id = str(contact.get("id") or "").strip()
            if not contact_id:
                continue
            deal_ids = hubspot_client.list_associated_object_ids("contacts", contact_id, "deals")
            if deal_ids:
                source = "contact"
                break

    best_deal_id = str(deal_ids[0] if deal_ids else "").strip()
    best_url = dashboard.build_hubspot_deal_url(hubspot_ui_domain, hubspot_portal_id, best_deal_id)
    company_cache[cache_key] = f"{resolved_name}|{best_url}|{len(deal_ids)}"
    return resolved_name, best_url, len(deal_ids), source


def build_update_payload(ticket: Dict[str, object], deal_url: str) -> Tuple[Dict[str, object], List[str]]:
    payload: Dict[str, object] = {
        "custom_fields": {dashboard.DEFAULT_HUBSPOT_DEAL_URL_FIELD: deal_url},
    }
    changes: List[str] = [f"{dashboard.DEFAULT_HUBSPOT_DEAL_URL_FIELD}={deal_url}"]

    custom_fields = ticket.get("custom_fields") if isinstance(ticket.get("custom_fields"), dict) else {}
    custom_fields = dict(custom_fields or {})

    raised_by = custom_fields.get("cf_raised_by")
    if raised_by not in freshdesk_cli.ALLOWED_RAISED_BY:
      payload["custom_fields"]["cf_raised_by"] = SAFE_DASH_VALUE
      changes.append("cf_raised_by=-")

    for field_name in sorted(MANDATORY_TEXT_FIELDS):
        value = custom_fields.get(field_name)
        if value in {None, ""}:
            payload["custom_fields"][field_name] = SAFE_DASH_VALUE
            changes.append(f"{field_name}=-")
        elif not isinstance(value, str):
            payload["custom_fields"][field_name] = str(value)
            changes.append(f"{field_name}=cast")

    ticket_type = dashboard.normalize_ticket_type(ticket.get("type"))
    if ticket_type not in freshdesk_cli.ALLOWED_TYPES:
        payload["type"] = SAFE_FRESHDESK_TYPE
        changes.append(f"type={SAFE_FRESHDESK_TYPE}")

    return payload, changes


def expand_payload_for_errors(
    payload: Dict[str, object],
    errors: Sequence[Dict[str, object]],
) -> Tuple[Dict[str, object], List[str]]:
    changes: List[str] = []
    custom_fields = payload.setdefault("custom_fields", {})
    if not isinstance(custom_fields, dict):
        custom_fields = {}
        payload["custom_fields"] = custom_fields

    for error in errors:
        field_name = str(error.get("field") or "").strip()
        if field_name == "type":
            if payload.get("type") != SAFE_FRESHDESK_TYPE:
                payload["type"] = SAFE_FRESHDESK_TYPE
                changes.append(f"type={SAFE_FRESHDESK_TYPE}")
            continue
        if field_name == "custom_fields.cf_raised_by":
            if custom_fields.get("cf_raised_by") != SAFE_DASH_VALUE:
                custom_fields["cf_raised_by"] = SAFE_DASH_VALUE
                changes.append("cf_raised_by=-")
            continue
        if field_name.startswith("custom_fields."):
            custom_field_name = field_name.split(".", 1)[1]
            if custom_fields.get(custom_field_name) != SAFE_DASH_VALUE:
                custom_fields[custom_field_name] = SAFE_DASH_VALUE
                changes.append(f"{custom_field_name}=-")
    return payload, changes


def update_ticket_with_retry(
    client: freshdesk_cli.FreshdeskClient,
    ticket_id: int,
    payload: Dict[str, object],
) -> Tuple[bool, Dict[str, object], str, List[str]]:
    code, data, raw = client.update_ticket(ticket_id, payload)
    retry_changes: List[str] = []
    if code == 200:
        return True, data or {}, raw, retry_changes

    errors = []
    if isinstance(data, dict) and isinstance(data.get("errors"), list):
        errors = [item for item in data.get("errors") if isinstance(item, dict)]
    if not errors:
        return False, data or {}, raw, retry_changes

    payload, retry_changes = expand_payload_for_errors(payload, errors)
    if not retry_changes:
        return False, data or {}, raw, retry_changes

    code, data, raw = client.update_ticket(ticket_id, payload)
    return code == 200, data or {}, raw, retry_changes


def report_row(
    *,
    ticket_id: int,
    subject: str,
    company_name: str,
    outcome: str,
    deal_source: str,
    deal_url: str,
    fields_changed: Sequence[str],
    note: str,
) -> Dict[str, object]:
    return {
        "ticket_id": ticket_id,
        "subject": subject,
        "company_name": company_name,
        "outcome": outcome,
        "deal_source": deal_source,
        "deal_url": deal_url,
        "fields_changed": list(fields_changed),
        "note": note,
    }


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    freshdesk_env = freshdesk_hubspot_sync.load_env(Path(args.env_file))
    hubspot_env = freshdesk_hubspot_sync.load_env(Path(args.hubspot_env_file))
    freshdesk_domain = freshdesk_hubspot_sync.env_or_file("FRESHDESK_DOMAIN", freshdesk_env)
    freshdesk_api_key = freshdesk_hubspot_sync.env_or_file("FRESHDESK_API_KEY", freshdesk_env)
    hubspot_access_token = freshdesk_hubspot_sync.env_or_file("HUBSPOT_ACCESS_TOKEN", hubspot_env)
    hubspot_portal_id = freshdesk_hubspot_sync.env_or_file("HUBSPOT_PORTAL_ID", hubspot_env)
    hubspot_ui_domain = freshdesk_hubspot_sync.env_or_file("HUBSPOT_UI_DOMAIN", hubspot_env, dashboard.DEFAULT_HUBSPOT_UI_DOMAIN)

    if not freshdesk_domain or not freshdesk_api_key:
        parser.error("Missing FRESHDESK_DOMAIN or FRESHDESK_API_KEY")
    if not hubspot_access_token or not hubspot_portal_id:
        parser.error("Missing HUBSPOT_ACCESS_TOKEN or HUBSPOT_PORTAL_ID")

    dashboard_payload = load_json(Path(args.dashboard_json), {})
    rows = dashboard_payload.get("rows") if isinstance(dashboard_payload, dict) else None
    if not isinstance(rows, list):
        parser.error("Dashboard JSON does not contain a valid rows list")

    client = freshdesk_cli.FreshdeskClient(freshdesk_domain, freshdesk_api_key)
    hubspot_client = freshdesk_hubspot_sync.HubSpotClient(hubspot_access_token)
    company_cache_path = Path(args.company_cache)
    company_cache = get_company_cache(company_cache_path)

    candidates = [
        row for row in rows
        if isinstance(row, dict)
        and int(row.get("ticket_id") or 0) > 0
        and not str(row.get("hubspot_deal_url") or "").strip()
        and str(row.get("company_name") or row.get("freshdesk_company_name") or "").strip()
        and str(row.get("company_name") or row.get("freshdesk_company_name") or "").strip().lower() != "unknown"
    ]
    if args.max_tickets:
        candidates = candidates[: max(0, args.max_tickets)]

    report: List[Dict[str, object]] = []
    updated = 0
    skipped = 0
    unresolved = 0
    failed = 0

    for index, row in enumerate(candidates, start=1):
        ticket_id = int(row.get("ticket_id") or 0)
        subject = str(row.get("subject") or "").strip()
        company_name = str(row.get("company_name") or row.get("freshdesk_company_name") or "").strip()
        if index == 1 or index % 25 == 0 or index == len(candidates):
            print(f"PROCESS {index}/{len(candidates)} ticket={ticket_id} company={company_name}", flush=True)

        try:
            resolved_company, deal_url, deal_count, deal_source = choose_deal_url_for_company(
                company_name=company_name,
                company_cache=company_cache,
                hubspot_client=hubspot_client,
                hubspot_ui_domain=hubspot_ui_domain,
                hubspot_portal_id=hubspot_portal_id,
            )
        except Exception as exc:
            failed += 1
            report.append(report_row(
                ticket_id=ticket_id,
                subject=subject,
                company_name=company_name,
                outcome="lookup_failed",
                deal_source="error",
                deal_url="",
                fields_changed=[],
                note=str(exc),
            ))
            continue

        if not deal_url:
            unresolved += 1
            report.append(report_row(
                ticket_id=ticket_id,
                subject=subject,
                company_name=resolved_company or company_name,
                outcome="no_deal_found",
                deal_source=deal_source,
                deal_url="",
                fields_changed=[],
                note=f"deal_count={deal_count}",
            ))
            continue

        code, ticket_data, raw = client.get_ticket(ticket_id)
        if code != 200 or not isinstance(ticket_data, dict):
            failed += 1
            report.append(report_row(
                ticket_id=ticket_id,
                subject=subject,
                company_name=resolved_company or company_name,
                outcome="ticket_fetch_failed",
                deal_source=deal_source,
                deal_url=deal_url,
                fields_changed=[],
                note=(raw[:300] if raw else f"http_{code}"),
            ))
            continue

        current_fields = ticket_data.get("custom_fields") if isinstance(ticket_data.get("custom_fields"), dict) else {}
        current_url = str((current_fields or {}).get(dashboard.DEFAULT_HUBSPOT_DEAL_URL_FIELD) or "").strip()
        if current_url == deal_url:
            skipped += 1
            report.append(report_row(
                ticket_id=ticket_id,
                subject=subject,
                company_name=resolved_company or company_name,
                outcome="already_set",
                deal_source=deal_source,
                deal_url=deal_url,
                fields_changed=[],
                note="HubSpot URL already present on ticket",
            ))
            continue

        payload, fields_changed = build_update_payload(ticket_data, deal_url)
        if args.dry_run:
            updated += 1
            report.append(report_row(
                ticket_id=ticket_id,
                subject=subject,
                company_name=resolved_company or company_name,
                outcome="dry_run",
                deal_source=deal_source,
                deal_url=deal_url,
                fields_changed=fields_changed,
                note="No Freshdesk update executed",
            ))
            continue

        success, data, raw, retry_changes = update_ticket_with_retry(client, ticket_id, payload)
        all_changes = list(dict.fromkeys([*fields_changed, *retry_changes]))
        if success:
            updated += 1
            report.append(report_row(
                ticket_id=ticket_id,
                subject=subject,
                company_name=resolved_company or company_name,
                outcome="updated",
                deal_source=deal_source,
                deal_url=deal_url,
                fields_changed=all_changes,
                note="Updated Freshdesk ticket",
            ))
            if args.pause_seconds > 0:
                time.sleep(args.pause_seconds)
            continue

        failed += 1
        note = raw[:300] if raw else json.dumps(data)[:300]
        report.append(report_row(
            ticket_id=ticket_id,
            subject=subject,
            company_name=resolved_company or company_name,
            outcome="update_failed",
            deal_source=deal_source,
            deal_url=deal_url,
            fields_changed=all_changes,
            note=note,
        ))

    save_company_cache(company_cache_path, company_cache)

    timestamp = utc_now().strftime("%Y%m%d_%H%M%S")
    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    json_path = report_dir / f"hubspot_deal_url_backfill_{timestamp}.json"
    csv_path = report_dir / f"hubspot_deal_url_backfill_{timestamp}.csv"
    write_json(json_path, {
        "generated_at": utc_now_iso(),
        "dry_run": bool(args.dry_run),
        "summary": {
            "candidates": len(candidates),
            "updated": updated,
            "skipped": skipped,
            "unresolved": unresolved,
            "failed": failed,
        },
        "rows": report,
    })
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["ticket_id", "subject", "company_name", "outcome", "deal_source", "deal_url", "fields_changed", "note"])
        writer.writeheader()
        for item in report:
            writer.writerow({**item, "fields_changed": "; ".join(item.get("fields_changed") or [])})

    print(
        "SUMMARY "
        f"candidates={len(candidates)} updated={updated} skipped={skipped} unresolved={unresolved} failed={failed} "
        f"json_report={json_path} csv_report={csv_path}",
        flush=True,
    )
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
