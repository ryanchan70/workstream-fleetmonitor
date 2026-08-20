# workstream-fleetmonitor

<small>updated 8/19</small>

Workstreamlined. For doers who get more done™.

Talks directly to the fleet.shiftiq.us JSON API as opposed to scraping.

Disclaimer: Numbers and information is not guaranteed accurate. This tool is not officially affiliated with Workstream.

## Features

<small>Note: This is a general overview and may not include all features.</small>

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
  - graph for hours recorded per day, weekdays only (selectable days, tooltips)
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