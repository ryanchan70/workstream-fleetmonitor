#!/usr/bin/env python3
"""
Fleet poller that runs on a machine you control, instead of on Vercel.

WHY THIS EXISTS
---------------
Every call to fleet.shiftiq.us used to come from a serverless invocation, which
is the expensive way to make it: the fleet sweep and the per-rig session fetches
are the only part of a request that does real work, and on Fluid compute that
work is billed as active CPU every time a tab ticks the loop over.

Running the same loop here moves those calls onto hardware you are already
paying for. The serverless function notices the heartbeat this leaves in Redis
and stops polling altogether — it drops to reading back what this wrote, which
is one Redis GET. Stop the agent and it takes the loop back within three
missed cycles, so this is an optimisation, not a dependency.

It is NOT possible to push the fleet calls all the way into the browser:
fleet.shiftiq.us sends no CORS headers and its session cookie is HttpOnly, so a
page on another origin cannot read a response from it at all. The nearest
"your device" can get is a process on your device — this one.

RUNNING
-------
    python3 agent.py

It needs the fleet login and the Upstash REST credentials. The fleet login is
already in local/secrets.json. The Upstash pair has to be copied by hand into
local/agent.env:

    KV_REST_API_URL=https://....upstash.io
    KV_REST_API_TOKEN=...

`vercel env pull` will NOT get them — Vercel marks those variables sensitive
and writes back the literal string [SENSITIVE] rather than the value. They are
on the Upstash console, or in the Vercel dashboard under Storage.

    python3 agent.py --interval 30            # seconds between sweeps (day)
    python3 agent.py --serve 8080             # also serve the dashboard here
    python3 agent.py --once                   # single sweep, for cron/testing

Outside 9am-7pm Pacific it drops to hourly on its own, matching the throttle
the rest of the system uses. Credentials come from the environment, falling
back to local/agent.env and local/secrets.json.
"""

import argparse
import os
import socket
import sys
import time

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "api"))


def load_env(path):
    """Minimal .env reader. Real environment variables win."""
    if not os.path.exists(path):
        return
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k, v = k.strip(), v.strip().strip('"').strip("'")
            # What `vercel env pull` writes for a variable marked sensitive.
            # Left unset rather than exported, so the check below names it
            # instead of the code failing on a nonsense URL later.
            if k and v != "[SENSITIVE]" and k not in os.environ:
                os.environ[k] = v


def load_secrets(path):
    """local/secrets.json is what the old standalone build authenticated with."""
    if os.environ.get("FLEET_EMAIL") or not os.path.exists(path):
        return
    try:
        import json
        with open(path, encoding="utf-8") as f:
            s = json.load(f)
        if s.get("email"):
            os.environ.setdefault("FLEET_EMAIL", s["email"])
        if s.get("password"):
            os.environ.setdefault("FLEET_PASSWORD", s["password"])
    except Exception:
        pass


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def cycle(agent_id, interval):
    """One sweep. Returns a short line describing what happened."""
    from _lib import logic

    t0 = time.time()
    # Deliberately NOT forced. Forcing would stand the shared cadence gate down
    # and retake it — an extra command every cycle — to no purpose: this sleeps
    # for longer than that gate holds, so it wins it on merit. The floor is
    # logic.POLL_MIN_INTERVAL_SEC; asking for less than that just means some
    # cycles answer "locked", and a cycle that does not poll does not stamp the
    # heartbeat either, which is the honest signal.
    res, _ = logic.poll(agent=(agent_id, interval))
    out = f"poll {time.time() - t0:.1f}s {res}"

    if logic.backfill_due():
        t1 = time.time()
        out += f" | backfill {time.time() - t1:.1f}s {logic.backfill()}"
    return out


def sync_feedback(path):
    """Append new submissions to feedback.txt, leaving existing blocks alone.

    The deployed function cannot write this file — a serverless filesystem is
    read-only — so it only ever reads statuses out of it. This is the other
    half: it brings the submissions down from Redis so there is something to
    put a status against. Ids already present are skipped, so an entry whose
    status has been edited is never overwritten.
    """
    from _lib import redis_state as R

    existing = set()
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line.startswith("["):
                    existing.add(line[1:].partition("]")[0].strip())
    except OSError:
        pass

    from _lib import core as C

    entries = []
    for e in (R.feedback_read(200) or []):
        if not isinstance(e, dict):
            continue
        fid = C.feedback_id(e)
        if fid and fid not in existing:
            entries.append(dict(e, id=fid))
    if not entries:
        log(f"no new feedback ({len(existing)} already in {os.path.basename(path)})")
        return

    # Oldest first, so the file reads chronologically as it grows.
    entries.sort(key=lambda e: float(e.get("at") or 0))
    with open(path, "a", encoding="utf-8") as f:
        for e in entries:
            f.write(f"\n[{e['id']}] {e.get('when', '')}  "
                    f"{e.get('email', '')}  status: submitted\n")
            f.write(str(e.get("text", "")).strip() + "\n")
    log(f"added {len(entries)} submission(s) to {os.path.basename(path)}")


def serve(port):
    """Serve the dashboard from this process, on top of the poll loop.

    Browsing here touches neither Vercel nor, in the common case, Redis: the
    handler reads the state blob through the in-process memo, and this process
    is the one writing it. The remote Vercel dashboard keeps working either
    way — both are views onto the same Redis.
    """
    import http.server
    from _lib import redis_state as R
    import index

    # The memo is normally a few seconds because a serverless container has no
    # idea whether another one has polled since. Here it is not a guess: this
    # process wrote the copy, and re-reading it would be pure waste.
    R.STATE_MEMO_TTL = 3600.0
    R.HISTORY_MEMO_TTL = 3600.0

    public = os.path.join(ROOT, "public")

    class Handler(index.handler):
        def _static(self):
            path = self.path.split("?", 1)[0].strip("/") or "index.html"
            full = os.path.normpath(os.path.join(public, path))
            # Normalised first, so a ../ in the URL cannot escape public/.
            if not full.startswith(public) or not os.path.isfile(full):
                self.send_error(404)
                return
            ctype = {".html": "text/html", ".js": "text/javascript",
                     ".css": "text/css", ".ico": "image/x-icon",
                     ".json": "application/json"}.get(os.path.splitext(full)[1],
                                                      "application/octet-stream")
            with open(full, "rb") as f:
                body = f.read()
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            if self.path.startswith("/api/"):
                return super().do_GET()
            return self._static()

    srv = http.server.ThreadingHTTPServer(("0.0.0.0", port), Handler)
    import threading
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    log(f"dashboard on http://localhost:{port}")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--interval", type=float, default=30.0,
                    help="seconds between sweeps during shift hours (default 30)")
    ap.add_argument("--serve", type=int, metavar="PORT",
                    help="also serve the dashboard on this port")
    ap.add_argument("--once", action="store_true", help="one sweep, then exit")
    ap.add_argument("--feedback", action="store_true",
                    help="append new dashboard suggestions to feedback.txt, then exit")
    ap.add_argument("--env", default=os.path.join(ROOT, "local", "agent.env"))
    ap.add_argument("--id", default=socket.gethostname(),
                    help="name shown on the dashboard as the poller (default: hostname)")
    args = ap.parse_args()

    load_env(args.env)
    load_secrets(os.path.join(ROOT, "local", "secrets.json"))

    from _lib import logic, redis_state as R

    missing = [k for k in ("FLEET_EMAIL", "FLEET_PASSWORD") if not os.environ.get(k)]
    if not R.configured():
        missing.append("KV_REST_API_URL / KV_REST_API_TOKEN")
    if missing:
        log("missing credentials: " + ", ".join(missing))
        log(f"  put them in {os.path.relpath(args.env, ROOT)} as KEY=value, or export them.")
        log("  the Upstash pair is on the Upstash console / Vercel > Storage;")
        log("  `vercel env pull` returns [SENSITIVE] for them, not the value.")
        return 1

    if args.feedback:
        sync_feedback(os.path.join(ROOT, "feedback.txt"))
        return 0

    if args.once:
        log(cycle(args.id, args.interval))
        return 0

    if args.serve:
        serve(args.serve)

    log(f"polling as '{args.id}' every {args.interval:g}s (hourly outside 9am-7pm Pacific)")
    while True:
        interval = logic.NIGHT_MIN_INTERVAL_SEC if logic.is_night() else args.interval
        try:
            log(cycle(args.id, interval))
        except KeyboardInterrupt:
            raise
        except Exception as e:
            # Never exit on a bad sweep: the fleet API and the network both
            # come and go, and a dead agent hands the loop back to Vercel.
            log(f"{type(e).__name__}: {e}")
        try:
            time.sleep(interval)
        except KeyboardInterrupt:
            break
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        log("stopped")
