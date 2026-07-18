# workstream-fleetmonitor

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

## Hosting on Vercel

The `api/` directory contains a serverless port of the dashboard backend
(same `index.html` frontend, auto-detected). Auth is fully stateless: the
OTP-verified fleet.shiftiq.us cookies ride inside HMAC-signed tokens, so no
database is required to sign in.

Setup:

1. Import the repo in Vercel (or `vercel deploy` from this directory).
2. Set the `AUTH_SECRET` env var to a long random string
   (`openssl rand -hex 32`). Required — tokens are signed with it.
3. Optional but recommended: attach an Upstash Redis (Vercel KV) database.
   The functions pick up `KV_REST_API_URL`/`KV_REST_API_TOKEN` automatically
   and use it for the leaderboard carry-forward cache, observed-session
   state, and the alert log. Without it, that state lives in warm function
   memory only (best effort).

Serverless limitations vs. running `fleet_monitor.py` locally: no daily
`.txt` log files, no start/stop loop controls (hidden in the UI), and
observed-session tracking only ticks while someone has the dashboard open —
there is no always-on poller.
