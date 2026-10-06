#!/usr/bin/env python3
import argparse
import json
import sys
from pathlib import Path
from typing import Dict, Optional, Tuple

import freshdesk_cli
import freshdesk_hubspot_sync


def resolve_hubspot_ticket(
    hubspot_client: freshdesk_hubspot_sync.HubSpotClient,
    tickets_state: Dict[str, object],
    freshdesk_ticket_id: int,
) -> Tuple[Optional[str], Dict[str, object]]:
    entry = tickets_state.setdefault(str(freshdesk_ticket_id), {})
    if not isinstance(entry, dict):
        entry = {}
        tickets_state[str(freshdesk_ticket_id)] = entry

    hubspot_ticket_id = str(entry.get("hubspot_ticket_id") or "").strip()
    if hubspot_ticket_id:
        return hubspot_ticket_id, entry

    found = hubspot_client.search_ticket_by_freshdesk_id(freshdesk_ticket_id)
    if found is None:
        return None, entry

    hubspot_ticket_id = str(found.get("id") or "").strip()
    if hubspot_ticket_id:
        entry["hubspot_ticket_id"] = hubspot_ticket_id
    return hubspot_ticket_id or None, entry


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Move linked HubSpot tickets to the closed stage so the hourly "
            "Freshdesk -> HubSpot cron closes the Freshdesk ticket."
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
        help="Path to the HubSpot sync state file",
    )
    parser.add_argument("--ticket", help="Single Freshdesk ticket ID")
    parser.add_argument("--tickets", help="Comma separated Freshdesk ticket IDs")
    parser.add_argument("--file", help="Path to file containing Freshdesk ticket IDs")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be routed through HubSpot without changing HubSpot stages.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    ticket_ids = freshdesk_cli.parse_ids(args)
    if not ticket_ids:
        parser.error("Provide at least one ticket ID via --ticket, --tickets, or --file")

    freshdesk_env = freshdesk_hubspot_sync.load_env(Path(args.freshdesk_env_file))
    hubspot_env = freshdesk_hubspot_sync.load_env(Path(args.hubspot_env_file))
    freshdesk_domain = freshdesk_hubspot_sync.env_or_file(
        "FRESHDESK_DOMAIN", freshdesk_env
    )
    freshdesk_api_key = freshdesk_hubspot_sync.env_or_file(
        "FRESHDESK_API_KEY", freshdesk_env
    )
    hubspot_access_token = freshdesk_hubspot_sync.env_or_file(
        "HUBSPOT_ACCESS_TOKEN", hubspot_env
    )

    if not freshdesk_domain or not freshdesk_api_key:
        parser.error("Missing FRESHDESK_DOMAIN or FRESHDESK_API_KEY")
    if not hubspot_access_token:
        parser.error("Missing HUBSPOT_ACCESS_TOKEN")

    freshdesk_client = freshdesk_hubspot_sync.FreshdeskClient(
        freshdesk_domain, freshdesk_api_key
    )
    hubspot_client = freshdesk_hubspot_sync.HubSpotClient(hubspot_access_token)
    pipeline = freshdesk_hubspot_sync.choose_pipeline_selection(
        hubspot_client.list_ticket_pipelines(), hubspot_env
    )

    state_path = Path(args.state_file)
    state = freshdesk_hubspot_sync.load_state(state_path)
    tickets_state = state.setdefault("tickets", {})
    if not isinstance(tickets_state, dict):
        tickets_state = {}
        state["tickets"] = tickets_state

    ok = already_closed = failed = missing_link = wrong_type = 0
    for ticket_id in ticket_ids:
        try:
            freshdesk_ticket = freshdesk_client.get_ticket(ticket_id)
        except freshdesk_hubspot_sync.HttpError as exc:
            print(f"FAIL {ticket_id} {exc}")
            failed += 1
            continue

        if str(freshdesk_ticket.get("type") or "") != freshdesk_hubspot_sync.FRESHDESK_TASK_TYPE:
            print(f"SKIP {ticket_id} type={freshdesk_ticket.get('type') or '-'}")
            wrong_type += 1
            continue

        hubspot_ticket_id, entry = resolve_hubspot_ticket(
            hubspot_client, tickets_state, ticket_id
        )
        if not hubspot_ticket_id:
            print(f"MISS {ticket_id} no_linked_hubspot_ticket")
            missing_link += 1
            continue

        hubspot_ticket = hubspot_client.get_ticket(hubspot_ticket_id)
        current_stage = freshdesk_hubspot_sync.collapse_space(
            str((hubspot_ticket.get("properties") or {}).get("hs_pipeline_stage") or "")
        )
        if current_stage == pipeline.closed_stage_id:
            print(f"OK {ticket_id} hubspot_ticket={hubspot_ticket_id} already_closed_stage")
            already_closed += 1
            continue

        if not args.dry_run:
            hubspot_ticket = hubspot_client.update_ticket(
                hubspot_ticket_id, {"hs_pipeline_stage": pipeline.closed_stage_id}
            )
            entry["hubspot_snapshot"] = freshdesk_hubspot_sync.hubspot_snapshot(
                hubspot_ticket
            )
            entry["hubspot_updated_at"] = str(hubspot_ticket.get("updatedAt") or "")
            state["last_manual_hubspot_close_request_at"] = freshdesk_hubspot_sync.iso_now()
        print(
            f"OK {ticket_id} hubspot_ticket={hubspot_ticket_id} "
            f"stage={current_stage or '-'}->{pipeline.closed_stage_id}"
        )
        ok += 1

    if not args.dry_run:
        freshdesk_hubspot_sync.save_state(state_path, state)

    print(
        "SUMMARY "
        f"ok={ok} already_closed={already_closed} wrong_type={wrong_type} "
        f"missing_link={missing_link} failed={failed} unique_total={len(ticket_ids)} "
        f"cron_next_run=hourly"
    )
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
