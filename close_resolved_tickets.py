#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import freshdesk_cli
import freshdesk_hubspot_sync
import hubspot_close_linked_tickets


DEFAULT_HUBSPOT_ENV_FILE = "/Users/shishirraj/.config/hubspot.env"


def search_resolved_ticket_ids(client: freshdesk_cli.FreshdeskClient) -> List[int]:
    query = f"status:{freshdesk_cli.STATUS_MAP['resolved']}"
    page = 1
    seen = set()
    ticket_ids: List[int] = []
    while True:
        code, data, raw = client.search_tickets(query, page=page)
        if code != 200 or not data:
            message = raw[:400] if raw else "unexpected_response"
            if data and isinstance(data, dict):
                message = str(data.get("message") or data.get("description") or message)
            raise RuntimeError(f"Freshdesk search failed with HTTP {code}: {message}")

        results = data.get("results") or []
        for ticket in results:
            try:
                ticket_id = int(ticket.get("id") or 0)
            except (TypeError, ValueError):
                continue
            if ticket_id <= 0 or ticket_id in seen:
                continue
            seen.add(ticket_id)
            ticket_ids.append(ticket_id)

        if len(results) < 30:
            break
        page += 1
    return ticket_ids


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Close every Freshdesk ticket currently in resolved status. "
            "HubSpot-linked Task - Experience Team tickets are routed through "
            "HubSpot first, then closed in Freshdesk."
        )
    )
    parser.add_argument(
        "--freshdesk-env-file",
        default=str(Path(__file__).with_name(".env")),
        help="Path to env file with FRESHDESK_DOMAIN and FRESHDESK_API_KEY",
    )
    parser.add_argument(
        "--hubspot-env-file",
        default=DEFAULT_HUBSPOT_ENV_FILE,
        help="Path to env file with HUBSPOT_ACCESS_TOKEN",
    )
    parser.add_argument(
        "--state-file",
        default=str(Path(__file__).with_name(".freshdesk_hubspot_sync_state.json")),
        help="Path to the HubSpot sync state file",
    )
    parser.add_argument(
        "--restore-missing",
        action="store_true",
        help="Try Freshdesk restore on 404 before giving up",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be closed without changing Freshdesk or HubSpot",
    )
    return parser


def close_direct_ticket(
    client: freshdesk_cli.FreshdeskClient,
    ticket: Dict,
) -> Tuple[bool, bool, bool, str]:
    payload, changed_rb, changed_tp = freshdesk_cli.build_status_payload(
        ticket, freshdesk_cli.STATUS_MAP["closed"]
    )
    code, data, raw = client.update_ticket(int(ticket["id"]), payload)
    if code == 200 and data and data.get("status") == freshdesk_cli.STATUS_MAP["closed"]:
        return True, changed_rb, changed_tp, ""

    message = "unexpected_response"
    if data:
        if "message" in data:
            message = str(data["message"])
        elif "description" in data:
            message = str(data["description"])
        elif "errors" in data and data["errors"]:
            message = str(data["errors"][0].get("message", message))
    elif raw:
        message = raw[:200]
    return False, changed_rb, changed_tp, message


def load_hubspot_components(
    args: argparse.Namespace,
) -> Tuple[
    freshdesk_hubspot_sync.HubSpotClient,
    freshdesk_hubspot_sync.PipelineSelection,
    Dict[str, object],
    Dict[str, object],
]:
    hubspot_env = freshdesk_hubspot_sync.load_env(Path(args.hubspot_env_file))
    hubspot_access_token = freshdesk_hubspot_sync.env_or_file(
        "HUBSPOT_ACCESS_TOKEN", hubspot_env
    )
    if not hubspot_access_token:
        raise RuntimeError("Missing HUBSPOT_ACCESS_TOKEN")

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
    return hubspot_client, pipeline, state, tickets_state


def route_blocked_ticket(
    *,
    client: freshdesk_cli.FreshdeskClient,
    ticket: Dict,
    hubspot_client: freshdesk_hubspot_sync.HubSpotClient,
    pipeline: freshdesk_hubspot_sync.PipelineSelection,
    tickets_state: Dict[str, object],
) -> Tuple[bool, bool, bool, bool, str]:
    ticket_id = int(ticket["id"])
    hubspot_ticket_id, entry = hubspot_close_linked_tickets.resolve_hubspot_ticket(
        hubspot_client, tickets_state, ticket_id
    )
    if not hubspot_ticket_id:
        return False, False, False, False, "no_linked_hubspot_ticket"

    hubspot_ticket = hubspot_client.get_ticket(hubspot_ticket_id)
    current_stage = freshdesk_hubspot_sync.collapse_space(
        str((hubspot_ticket.get("properties") or {}).get("hs_pipeline_stage") or "")
    )
    stage_updated = False
    if current_stage != pipeline.closed_stage_id:
        hubspot_ticket = hubspot_client.update_ticket(
            hubspot_ticket_id, {"hs_pipeline_stage": pipeline.closed_stage_id}
        )
        stage_updated = True

    ok, changed_rb, changed_tp, message = close_direct_ticket(client, ticket)
    if not ok:
        return False, stage_updated, changed_rb, changed_tp, message

    entry["hubspot_ticket_id"] = str(hubspot_ticket.get("id") or hubspot_ticket_id)
    entry["hubspot_snapshot"] = freshdesk_hubspot_sync.hubspot_snapshot(hubspot_ticket)
    entry["hubspot_updated_at"] = str(hubspot_ticket.get("updatedAt") or "")
    entry["freshdesk_ticket_id"] = ticket_id
    entry["freshdesk_status"] = freshdesk_cli.STATUS_MAP["closed"]
    return True, stage_updated, changed_rb, changed_tp, hubspot_ticket_id


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    freshdesk_env = freshdesk_cli.load_env(Path(args.freshdesk_env_file))
    freshdesk_domain = freshdesk_hubspot_sync.env_or_file(
        "FRESHDESK_DOMAIN", freshdesk_env
    )
    freshdesk_api_key = freshdesk_hubspot_sync.env_or_file(
        "FRESHDESK_API_KEY", freshdesk_env
    )
    if not freshdesk_domain or not freshdesk_api_key:
        parser.error("Missing FRESHDESK_DOMAIN or FRESHDESK_API_KEY")

    client = freshdesk_cli.FreshdeskClient(freshdesk_domain, freshdesk_api_key)

    try:
        ticket_ids = search_resolved_ticket_ids(client)
    except RuntimeError as exc:
        print(f"FAIL discovery {exc}")
        return 1

    hubspot_ticket_map = freshdesk_cli.load_hubspot_sync_ticket_map(Path(args.state_file))
    blocked_tickets: List[Dict] = []

    summary = {
        "discovered": len(ticket_ids),
        "direct_closed": 0,
        "hubspot_closed": 0,
        "hubspot_stage_updated": 0,
        "restored": 0,
        "changed_cf_raised_by": 0,
        "changed_type": 0,
        "stale_search_skips": 0,
        "failed": 0,
        "not_found": 0,
    }

    for ticket_id in ticket_ids:
        ticket, was_restored, state = freshdesk_cli.ensure_ticket_available(
            client, ticket_id, args.restore_missing
        )
        if state == "not_found":
            print(f"MISS {ticket_id} not_found")
            summary["not_found"] += 1
            continue
        if ticket is None:
            print(f"FAIL {ticket_id} {state}")
            summary["failed"] += 1
            continue
        if was_restored:
            summary["restored"] += 1

        if int(ticket.get("status") or 0) != freshdesk_cli.STATUS_MAP["resolved"]:
            print(
                f"SKIP {ticket_id} status={ticket.get('status') or '-'} "
                "reason=stale_search_result"
            )
            summary["stale_search_skips"] += 1
            continue

        hubspot_entry = hubspot_ticket_map.get(str(ticket_id)) or {}
        hubspot_ticket_id = str(hubspot_entry.get("hubspot_ticket_id") or "").strip()
        is_hubspot_linked_task = (
            str(ticket.get("type") or "") == freshdesk_hubspot_sync.FRESHDESK_TASK_TYPE
            and bool(hubspot_ticket_id)
        )
        if is_hubspot_linked_task:
            print(f"ROUTE {ticket_id} linked_hubspot_ticket={hubspot_ticket_id}")
            blocked_tickets.append(ticket)
            continue

        if args.dry_run:
            print(f"DRY {ticket_id} via=freshdesk")
            continue

        ok, changed_rb, changed_tp, message = close_direct_ticket(client, ticket)
        if ok:
            print(f"OK {ticket_id} via=freshdesk")
            summary["direct_closed"] += 1
            summary["changed_cf_raised_by"] += int(changed_rb)
            summary["changed_type"] += int(changed_tp)
            continue

        print(f"FAIL {ticket_id} {message}")
        summary["failed"] += 1

    if blocked_tickets and args.dry_run:
        for ticket in blocked_tickets:
            print(f"DRY {int(ticket['id'])} via=hubspot_then_freshdesk")
    elif blocked_tickets:
        try:
            hubspot_client, pipeline, state, tickets_state = load_hubspot_components(args)
        except (RuntimeError, freshdesk_hubspot_sync.HttpError) as exc:
            for ticket in blocked_tickets:
                print(f"FAIL {int(ticket['id'])} hubspot_setup {exc}")
                summary["failed"] += 1
            blocked_tickets = []
        else:
            for ticket in blocked_tickets:
                ticket_id = int(ticket["id"])
                try:
                    ok, stage_updated, changed_rb, changed_tp, detail = route_blocked_ticket(
                        client=client,
                        ticket=ticket,
                        hubspot_client=hubspot_client,
                        pipeline=pipeline,
                        tickets_state=tickets_state,
                    )
                except freshdesk_hubspot_sync.HttpError as exc:
                    print(f"FAIL {ticket_id} {exc}")
                    summary["failed"] += 1
                    continue

                if not ok:
                    print(f"FAIL {ticket_id} {detail}")
                    summary["failed"] += 1
                    continue

                state["last_manual_hubspot_close_request_at"] = (
                    freshdesk_hubspot_sync.iso_now()
                )
                if stage_updated:
                    summary["hubspot_stage_updated"] += 1
                summary["hubspot_closed"] += 1
                summary["changed_cf_raised_by"] += int(changed_rb)
                summary["changed_type"] += int(changed_tp)
                print(
                    f"OK {ticket_id} via=hubspot_then_freshdesk "
                    f"hubspot_ticket={detail}"
                )

            state["version"] = freshdesk_hubspot_sync.STATE_VERSION
            freshdesk_hubspot_sync.save_state(Path(args.state_file), state)

    print(
        "SUMMARY "
        f"discovered={summary['discovered']} "
        f"direct_closed={summary['direct_closed']} "
        f"hubspot_closed={summary['hubspot_closed']} "
        f"hubspot_stage_updated={summary['hubspot_stage_updated']} "
        f"restored={summary['restored']} "
        f"changed_cf_raised_by={summary['changed_cf_raised_by']} "
        f"changed_type={summary['changed_type']} "
        f"stale_search_skips={summary['stale_search_skips']} "
        f"failed={summary['failed']} "
        f"not_found={summary['not_found']} "
        f"dry_run={str(bool(args.dry_run)).lower()}"
    )
    return 0 if summary["failed"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
