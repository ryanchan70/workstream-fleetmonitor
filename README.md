# workstream-scraper

Scalable rig monitor and timer for the fleet.shiftiq.us robot capture fleet.

Talks directly to the fleet.shiftiq.us JSON API (`/api/fleet/status` and the
per-device `statusboard-api/mcap-sync/sessions` endpoint) — no HTML scraping.

## Setup

```bash
pip install -r requirements.txt
```

Create `secrets.json` (git-ignored) with the credentials the background
monitor uses to authenticate itself with fleet.shiftiq.us:

```json
{ "email": "you@example.com", "password": "your-password" }
```

If the password login fails, the monitor falls back to emailing you a
6-digit verification code and prompting for it on the console.

## Running

```bash
python3 fleet_monitor.py
```

This polls fleet status every 5 seconds, writes `daily_recording_log_*.txt`
and `operator_sessions_*.txt`, and serves a local dashboard API on
`http://127.0.0.1:8080`.

Open `index.html` in a browser to view the dashboard. It's gated behind a
real email verification code: enter an email, fleet.shiftiq.us emails a
6-digit code to it (only if that email is a valid, authorized account —
there's no separate local allowlist), and entering the correct code signs
you in for 12 hours.

Press `` ` `` or `~` in the terminal running `fleet_monitor.py` at any time
to force a snapshot of daily totals to disk.
