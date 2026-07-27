# Deploying to Vercel

## 1. Create the Redis database

Vercel dashboard → **Storage** → **Upstash Redis** → create → connect it to this
project. That injects `KV_REST_API_URL` / `KV_REST_API_TOKEN` automatically.

Creating it directly on upstash.com works too — that gives
`UPSTASH_REDIS_REST_URL` / `UPSTASH_REDIS_REST_TOKEN`. **Either naming works**;
the code reads both.

## 2. Add the fleet credentials

Project → Settings → Environment Variables:

| Name | Value |
|---|---|
| `FLEET_EMAIL` | your fleet.shiftiq.us account |
| `FLEET_PASSWORD` | its password |

These are what the background poll authenticates with. Dashboard sign-in is
separate — each user signs in with their own fleet.shiftiq.us credentials.

## 3. Deploy

```bash
vercel --prod
```

## 4. Check it

```
GET /api/health   ->  {"ok": true, "redis": true}
```

`"redis": false` means step 1 did not take.

---

## Layout

```
public/index.html       dashboard (static). Vercel treats this repo as a
                        `framework: python` backend project, so static assets
                        are served ONLY out of public/ — a file at the repo
                        root falls through to the Python catch-all instead.
public/favicon.ico      same reason

vercel.json             routes every /api/* to one function
.vercelignore           keeps local-only files out of the bundle
requirements.txt        requests (needed by api_client.py)

api/
  index.py              THE serverless function — all routes
  _lib/
    core.py             pure logic: thresholds, parsing, rig evaluation
    logic.py            poll() and backfill()
    redis_state.py      Upstash REST client + all persisted state
    fleet.py            fleet API wrapper with cached auth

api_client.py           yours, unchanged
auth.py                 yours, used by the local build
fleet_monitor.py        local long-running build (excluded from deploy)
legacy/                 superseded Vercel-KV/WSGI build, kept for reference
```

`_lib` starts with an underscore so Vercel does not expose those modules as
routes. `vercel.json` rewrites `/api/*` to `api/index.py`, which routes
internally — mirroring the original single-handler design rather than
scattering a dozen tiny functions.

---

## What changed to make this deployable

**`main.py` was a second, competing entrypoint.** It was a WSGI app on Vercel
KV with HMAC tokens, importing an older 287-line `api/_lib/core.py`. It
predates the Upstash work and lacks task history, locations, weekend
exclusion, backfill gating and the alert-transition fix. Both it and its core
now sit in `legacy/`. Nothing imports them.

**There was no `api/index.py` at all.** The serverless modules were sitting at
the repo root, where Vercel never looks, so the deploy had no function to run.
They are now under `api/`.

**`pyproject.toml` pointed at `main:app`** via a `[tool.vercel]` key Vercel
does not read. Removed; routing lives in `vercel.json`.

---

## Polling

The dashboard drives everything. One request every 20 seconds to
`/api/summary`, which runs a poll cycle and returns devices + rankings + stats
+ logs + task history in a single response. Backfill runs inside that same
request on first boot and then hourly, gated by a Redis timestamp — no cron,
so it works on Hobby.

A Redis lock wraps both, so several open tabs cannot double-count.

**Nothing is recorded while every tab is closed.** Completed sessions come
from the fleet API's own session list, so the hourly backfill recovers history
for any period the dashboard was shut. What cannot be recovered is a "Pi
stopped recording" *notification* for a stop that happened with no tab open.

### Invocation budget

One tab at the default 20s ≈ 4.3k invocations/day, against the 100k/month
Hobby includes — a permanently open tab fits with room to spare. `POLL_MS` in
`public/index.html` controls it: 5s is ≈17k/day and will blow the budget, 30s
is ≈2.9k/day. Upstash free tier is 10k commands/day and each poll is roughly
10–15 commands, so that tier is the tighter constraint of the two.

---

## Frame health counts

`core.extract_frame_counts()` looks for `frames_captured` / `frames_dropped` /
`frames_expected` and a nested `frame_summary`, deriving any missing one from
the other two. Confirmed working end-to-end when the fields are present.

I could not confirm your fleet actually returns them: `api_client.py` passes
the payload through untouched (`get_fleet_status` returns `r.json()["devices"]`
verbatim), so the field names depend on the rig firmware, not the client.
Check with:

```bash
curl -s https://<your-app>/api/devices -H "Authorization: Bearer <token>" | jq '.rigs[0]'
```

If the names differ, add them to the tuples in `extract_frame_counts` —
everything downstream already works.

---

## Verified before shipping

Ran against a Redis-compatible REST server and the **real `api_client.py`**
with only the network intercepted, driving the actual Vercel `handler` class —
first in the repo, then again from an isolated copy containing only the files
git would commit minus everything `.vercelignore` excludes:

- `/api/health` → `{"ok": true, "redis": true}`
- unauthenticated `/api/summary` → 401; wrong password → 401; valid login → token
- `/api/summary` → poll ran, backfill ran on cold start, rigs, alerts,
  rankings, stats, task history and logs all populated
- `/api/stats_range` → 200 through the rewrite; unknown route → 404
- Vercel-KV env var naming (`KV_REST_API_*`) resolves correctly
- `.gitignore` verified: secrets, `.env*`, all six caches, logs, `node_modules`,
  `__pycache__`, `.vercel` ignored; every source file tracked
