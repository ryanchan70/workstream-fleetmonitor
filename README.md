# workstream-fleetmonitor

updated 8/21

https://workstream-fleetmonitor.vercel.app

Workstreamlined. For doers who get more done™.

Talks directly to the fleet.shiftiq.us JSON API as opposed to scraping.

Disclaimer: Numbers and information is not guaranteed accurate. This tool is not officially affiliated with Workstream.

## Repository layout

| Path | What it is |
| --- | --- |
| `api/` | The serverless functions. Only `api/*.py` is deployed; `api/_lib/` holds the shared logic. |
| `public/` | Static dashboard — `index.html`, service worker, PWA assets. |
| `fleet_monitor.py` | Local long-running agent. Polls and backfills without relying on serverless. |
| `query_tasks/` | CLI: dumps sessions to `query_report.csv`. Config in `query_config.txt`. |
| `categorize_tasks_0728/` | CLI: categorised task report. Config in `categorize_config.txt`. |
| `legacy/`, `local/` | Superseded implementation and local-only runtime state. Neither is deployed. |
| `changelog.md`, `new_changes.md` | Change history, and the in-app "what's new" popup. |

## Features

Note: This is a general overview. For all features, see changelog.md.

- live rig status
  - polls every 30s by default (dropdown for more times)
    - night mode slows polling after normal work hours
  - stat boxes display status, name, operator, task name, storage, recording time, cpu temperature, fan speed, notes
  - color-coded (red = recording, gray = offline, etc)
  - total number recording, up, idle, offline, total
  - 3 methods of sorting (name, storage, status) and a reverse button
  - push notifications
  - red text and glowing red box for issues

- aggregate fleet stats (counts ongoing recordings)
  - **option to select duration estimate vs. API**
  - backfilled on startup, periodically, and can be manually triggered
  - graph for hours recorded per day, weekdays only (selectable days, tooltips, overlay)
  - graph for hours by pi/operator (horizontally scrollable, ranked, tooltips)
  - total hours, number recording right now, total rigs
  - frame health not working

- live text terminal dump
  - provides color-coded updates
  - text based notifications with timestamps (for when pis stop, start, offline, online, etc)
  - shows warnings

- ranked pi and operator timings
  - displays total active duration for the selected days, ranked 
  - contains 
  - color codes if estimates and API vary

- completed task history
  - organized and sorted by pi
  - contains timestamp, name, task name, recording time

- changelog
  - includes changes (obviously)
  - feedback form
  - also has queued features

- notification center
  - resolvable alerts
  - records recording stops and pi health warnings

- developer console

- sign-in system
  - email + password
  - OTP button