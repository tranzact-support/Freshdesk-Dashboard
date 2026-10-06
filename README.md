# Freshdesk Tool

Local CLI for repeated Freshdesk ticket operations.

## Setup

The tool reads credentials from `.env` in this folder:

```env
FRESHDESK_DOMAIN=tranzact.freshdesk.com
FRESHDESK_API_KEY=your_api_key
```

Optional rate-limit tuning for bulk actions:

```env
FRESHDESK_MIN_INTERVAL_SECONDS=1.5
FRESHDESK_MAX_RETRIES=6
FRESHDESK_RETRY_BASE_SECONDS=5
```

## Usage

Show help:

```bash
python3 freshdesk_cli.py --help
```

Resolve one ticket:

```bash
python3 freshdesk_cli.py status --ticket 12533 --value resolved
```

Close many tickets:

```bash
python3 freshdesk_cli.py status --tickets "12533,12534,12535" --value closed
```

Close every ticket currently in `resolved`:

```bash
python3 close_resolved_tickets.py
```

Dry run the same workflow:

```bash
python3 close_resolved_tickets.py --dry-run
```

Close HubSpot-linked `Task - Experience Team` tickets via HubSpot so the hourly cron closes Freshdesk:

```bash
python3 hubspot_close_linked_tickets.py --tickets "12533,12534"
```

Add a private note:

```bash
python3 freshdesk_cli.py note --ticket 12533 --text "shared by sharad to close as this is released or not to be done"
```

Bulk note from a file:

```bash
python3 freshdesk_cli.py note --file ticket_ids.txt --text "shared by sharad to close as this is released or not to be done"
```

Try restoring archived tickets before the action:

```bash
python3 freshdesk_cli.py status --file ticket_ids.txt --value resolved --restore-missing
```

## Notes

- `resolved` maps to Freshdesk status `4`.
- `closed` maps to Freshdesk status `5`.
- Freshdesk requests now automatically pace themselves and retry `429` or transient network failures.
- When Freshdesk rejects old tickets because of invalid fields, the tool normalizes:
  - `custom_fields.cf_raised_by` to `-`
  - `custom_fields.cf_company_id880506` to a non-empty string, falling back to `cf_company_id`
  - `type` to `Feature Idea`
- Notes are added as private notes by default.

## Auto note watcher

`ticket_note_watcher.py` polls recently created tickets and automatically adds a private note for:

- `Bug`
- `Feature Idea`
- `Task - Backend`
- `Task - Experience Team`

Each note contains:

- `Quick summary`
- `Issue`
- `Chat transcript summary`

The summary is generated from both ticket context and chat transcript when the ticket source is chat. If chat transcript is unavailable, `Chat transcript summary` is left blank. It prefers OpenAI if `OPENAI_API_KEY` is configured, and falls back to a heuristic summary if not. The watcher uses a local state file plus a note marker to avoid duplicate notes.

### Config

Add these to `.env` if you want AI-generated notes:

```env
OPENAI_BASE_URL=https://api.openai.com/v1
OPENAI_API_KEY=your_openai_api_key
OPENAI_MODEL=gpt-4o-mini
WATCH_LOOKBACK_DAYS=2
WATCH_LIMIT=100
WATCH_STATE_FILE=.ticket_note_watcher_state.json
WATCH_STATUS_FILE=.ticket_note_watcher_status.json
WATCH_HEALTH_MAX_DELAY_MINUTES=10
WATCH_HEALTH_STUCK_MINUTES=15
WATCH_HEALTH_STATE_FILE=.ticket_note_health_state.json
WATCH_ALERT_LOG=ticket_note_healthcheck.log
SLACK_WEBHOOK_URL=
SLACK_RELAY_URL=
SLACK_CHANNEL_NAME=
```

### Run once

```bash
cd /Users/shishirraj/freshdesk-tool
python3 ticket_note_watcher.py
```

Dry run:

```bash
cd /Users/shishirraj/freshdesk-tool
python3 ticket_note_watcher.py --dry-run
```

### Cron example

Run every minute:

```cron
* * * * * cd /Users/shishirraj/freshdesk-tool && /usr/bin/python3 ticket_note_watcher.py >> /Users/shishirraj/freshdesk-tool/ticket_note_watcher.log 2>&1
*/5 * * * * cd /Users/shishirraj/freshdesk-tool && /usr/bin/python3 ticket_note_healthcheck.py >> /Users/shishirraj/freshdesk-tool/ticket_note_healthcheck.log 2>&1
```

The watcher now writes `.ticket_note_watcher_status.json` on each run. The healthcheck inspects that file and writes alert or recovery entries to `ticket_note_healthcheck.log` if runs become stale, fail, or get stuck.

To send those alerts to Slack, configure either:

- `SLACK_WEBHOOK_URL` for a direct Slack incoming webhook
- or `SLACK_RELAY_URL` plus `SLACK_CHANNEL_NAME` for an internal relay endpoint

## Freshdesk -> HubSpot hourly sync

`freshdesk_hubspot_sync.py` syncs Freshdesk `Task - Experience Team` tickets into HubSpot and mirrors HubSpot notes/updates back into Freshdesk.

Current behavior:

- scans Freshdesk tickets of type `Task - Experience Team`
- includes tickets created strictly after `2026-07-25`
- creates a HubSpot ticket only while the Freshdesk ticket is still not `resolved` or `closed`
- keeps already-synced tickets updated in HubSpot, including later closure
- mirrors new HubSpot notes onto the Freshdesk ticket as private notes
- adds a private Freshdesk note when the linked HubSpot ticket changes key fields such as stage, subject, content, or priority
- stores sync state in `.freshdesk_hubspot_sync_state.json` to avoid duplicate work

Direct close safeguard:

- `freshdesk_cli.py status --value closed` now blocks direct closure for HubSpot-linked `Task - Experience Team` tickets by default
- use `python3 hubspot_close_linked_tickets.py ...` to move the linked HubSpot ticket to the closed stage
- the hourly `freshdesk_hubspot_sync.py` cron then closes the Freshdesk ticket on its next run

### HubSpot config

The script reads HubSpot credentials from `~/.config/hubspot.env` by default:

```env
HUBSPOT_ACCESS_TOKEN=your_private_app_token
HUBSPOT_TICKET_PIPELINE_ID=
HUBSPOT_TICKET_OPEN_STAGE_ID=
HUBSPOT_TICKET_CLOSED_STAGE_ID=
```

Only `HUBSPOT_ACCESS_TOKEN` is required.

If the pipeline and stage IDs are left blank, the script auto-selects:

- the first non-archived HubSpot ticket pipeline
- the first stage in that pipeline marked `OPEN`
- the first stage in that pipeline marked `CLOSED`

If your HubSpot account uses a different ticket pipeline, set the IDs explicitly.

### Run once

```bash
cd /Users/shishirraj/freshdesk-tool
python3 freshdesk_hubspot_sync.py --dry-run
python3 freshdesk_hubspot_sync.py
```

### Cron

The cron file is stored at:

`/Users/shishirraj/freshdesk-tool/freshdesk_hubspot_sync.cron`

It runs the sync every hour and writes to:

- `freshdesk_hubspot_sync.log`

Example cron entry:

```cron
0 * * * * cd /Users/shishirraj/freshdesk-tool && /usr/bin/python3 freshdesk_hubspot_sync.py >> /Users/shishirraj/freshdesk-tool/freshdesk_hubspot_sync.log 2>&1
```

## Freshdesk activity dashboard

Freshdesk native analytics can show ticket trends, but the specific combination you asked for:

- ticket activity log view
- customer revert tagging
- reply highlight / reply-check queue

is more reliable as a separate API-connected dashboard.

`freshdesk_activity_dashboard.py` exports a self-contained dashboard from the Freshdesk API and writes:

- `dashboard/freshdesk_activity_dashboard.html`
- `dashboard/freshdesk_activity_dashboard.json`
- `dashboard/freshdesk_activity_dashboard.csv`
- `dashboard/.freshdesk_activity_dashboard_status.json`

What it shows:

- latest ticket activity
- recent conversation timeline
- customer revert detection
- overdue vs waiting reply highlights
- existing tags and optional Freshdesk tag sync

### Run once

```bash
cd /Users/shishirraj/freshdesk-tool
python3 freshdesk_activity_dashboard.py
```

Optional tag sync back into Freshdesk:

```bash
cd /Users/shishirraj/freshdesk-tool
python3 freshdesk_activity_dashboard.py --sync-customer-revert-tag
```

Optional tighter test run:

```bash
cd /Users/shishirraj/freshdesk-tool
python3 freshdesk_activity_dashboard.py --lookback-days 2 --max-tickets 25
```

Full-history export:

```bash
cd /Users/shishirraj/freshdesk-tool
python3 freshdesk_activity_dashboard.py --all-tickets
```

### Config

Add these optional settings to `.env`:

```env
ACTIVITY_DASHBOARD_LOOKBACK_DAYS=7
ACTIVITY_DASHBOARD_REPLY_SLA_HOURS=24
ACTIVITY_DASHBOARD_ALL_TICKETS=0
ACTIVITY_DASHBOARD_MAX_RETRIES=8
ACTIVITY_DASHBOARD_RETRY_DELAY_SECONDS=2
ACTIVITY_DASHBOARD_RECENT_ACTIVITY_DAYS=30
```

### Daily cron

The cron template is stored at:

`/Users/shishirraj/freshdesk-tool/freshdesk_activity_dashboard.cron`

Default daily run:

```cron
0 8 * * * cd /Users/shishirraj/freshdesk-tool && /usr/bin/python3 freshdesk_activity_dashboard.py >> /Users/shishirraj/freshdesk-tool/freshdesk_activity_dashboard.log 2>&1
```

If you want the script to also add/remove the `customer_reverted` tag in Freshdesk, switch the cron entry to the `--sync-customer-revert-tag` version in that file after validating the dashboard once.

### Web version

You can also serve the dashboard as a local web app:

```bash
cd /Users/shishirraj/freshdesk-tool
python3 freshdesk_activity_dashboard_web.py
```

Open it in your browser:

```text
http://127.0.0.1:8787
```

To share it with teammates on the same network, start it on all interfaces and share your computer IP plus port:

```bash
cd /Users/shishirraj/freshdesk-tool
python3 freshdesk_activity_dashboard_web.py --host 0.0.0.0 --port 8787
```

Useful routes:

- `/` web dashboard
- `/api/dashboard` raw JSON
- `/download.csv` CSV export
- `/refresh` refresh trigger used by the dashboard button

### Permanent hosted deployment

For a stable URL that keeps working when your laptop is off, deploy the web app to an always-on host.

This repo is now prepared for hosted deployment with:

- `render.yaml` for Render
- `Dockerfile` for generic container hosting
- background auto-refresh in `freshdesk_activity_dashboard_web.py`

Recommended hosted setup:

1. Push this folder to a Git repository.
2. Create a new web service on Render.
3. Point Render to this repo root.
4. Set these required environment variables:

```env
FRESHDESK_DOMAIN=tranzact.freshdesk.com
FRESHDESK_API_KEY=your_freshdesk_api_key
```

Optional hosted settings:

```env
DASHBOARD_OUTPUT_DIR=dashboard
DASHBOARD_AUTO_REFRESH_SECONDS=86400
DASHBOARD_REFRESH_ON_START=1
ACTIVITY_DASHBOARD_ALL_TICKETS=1
ACTIVITY_DASHBOARD_REPLY_SLA_HOURS=24
ACTIVITY_DASHBOARD_MAX_RETRIES=8
ACTIVITY_DASHBOARD_RETRY_DELAY_SECONDS=2
```

What this gives you:

- stable hosted URL
- background refresh every 24 hours by default
- manual refresh from the dashboard button
- dashboard access even when your laptop is off

Render notes:

- the included `render.yaml` starts `freshdesk_activity_dashboard_web.py`
- `healthCheckPath` is `/healthz`
- `autoDeploy` is disabled by default so you can control production updates

Generic Docker hosting:

```bash
docker build -t freshdesk-dashboard .
docker run -p 8787:8787 \
  -e FRESHDESK_DOMAIN=tranzact.freshdesk.com \
  -e FRESHDESK_API_KEY=your_freshdesk_api_key \
  -e DASHBOARD_AUTO_REFRESH_SECONDS=86400 \
  freshdesk-dashboard
```
