"""
Vercel serverless entry point.

vercel.json rewrites /api/* here, so this one function routes everything —
mirroring the single-handler design of the original embedded server instead of
scattering logic across a dozen tiny files.
"""

import json
import os
import secrets
import sys
import time
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from _lib import core as C           # noqa: E402
from _lib import logic               # noqa: E402
from _lib import redis_state as R    # noqa: E402
from _lib import fleet               # noqa: E402


def _norm(path: str) -> str:
    """/api/foo and /foo both resolve to 'foo'."""
    p = urlparse(path).path.strip("/")
    if p.startswith("api/"):
        p = p[4:]
    elif p == "api":
        p = ""
    return p


class handler(BaseHTTPRequestHandler):
    # ── plumbing ─────────────────────────────────────────────────────────
    def log_message(self, *a):
        return

    def _json(self, status, payload):
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _token(self):
        h = self.headers.get("Authorization", "")
        return h[7:].strip() if h.startswith("Bearer ") else None

    def _auth(self):
        """Returns the signed-in email, or writes a 401 and returns None."""
        try:
            email = R.session_email(self._token())
        except R.RedisUnavailable as e:
            self._json(503, {"error": str(e)})
            return None
        if not email:
            self._json(401, {"error": "authentication required"})
            return None
        return email

    def _body(self):
        try:
            n = int(self.headers.get("Content-Length", 0))
        except ValueError:
            n = 0
        if n <= 0:
            return {}
        try:
            return json.loads(self.rfile.read(n) or b"{}")
        except ValueError:
            return {}

    def do_OPTIONS(self):
        self.send_response(204)
        self.end_headers()

    # ── GET ──────────────────────────────────────────────────────────────
    def do_GET(self):
        route = _norm(self.path)
        qs = parse_qs(urlparse(self.path).query)

        if route == "health":
            return self._json(200, {"ok": True, "redis": R.configured()})

        if route == "auth/status":
            try:
                email = R.session_email(self._token())
            except R.RedisUnavailable as e:
                return self._json(503, {"error": str(e)})
            return self._json(200, {"authenticated": bool(email), "email": email})

        if self._auth() is None:
            return

        try:
            if route == "summary":
                return self._json(200, self._summary(qs))
            if route == "devices":
                return self._json(200, self._devices())
            if route == "rankings":
                lb = R.jget("leaderboard", {}) or {}
                return self._json(200, {
                    "pi": lb.get("pi", []), "operator": lb.get("operator", []),
                    "active": True, "source": lb.get("source", "api")})
            if route == "stats":
                return self._json(200, self._stats())
            if route == "stats_range":
                return self._json(200, self._stats_range(qs))
            if route == "task_history":
                return self._json(200, self._task_history())
            if route == "logs":
                return self._json(200, R.log_read())
            if route == "locations":
                return self._json(200, R.hgetall_json("locations"))
            if route == "poll":
                return self._json(200, logic.poll())
            if route == "backfill":
                force = "force" in qs
                return self._json(200, logic.backfill(force=force))
            if route == "test_notification":
                return self._json(200, {"alerts": [
                    {"hostname": "test-pi", "rig": "Test Pi (Critical)",
                     "kind": "test_critical",
                     "message": "🔔 TEST ALERT — critical notification with sound"},
                    {"hostname": "test-pi-2", "rig": "Test Pi (Resolved)",
                     "kind": "test_resolved",
                     "message": "✓ TEST RESOLVED — resolved notification"},
                ]})
            # Kept for UI compatibility; there is no loop to start/stop now
            # that polling is driven by the browser.
            if route in ("start", "stop"):
                return self._json(200, {"ok": True, "note": "browser-driven polling"})
            if route == "snapshot":
                return self._json(200, logic.backfill(force=True))
        except R.RedisUnavailable as e:
            return self._json(503, {"error": str(e)})
        except Exception as e:
            return self._json(500, {"error": f"{type(e).__name__}: {e}"})

        self._json(404, {"error": "not found", "route": route})

    # ── POST ─────────────────────────────────────────────────────────────
    def do_POST(self):
        route = _norm(self.path)
        body = self._body()

        try:
            if route == "auth/login":
                return self._login(body)
            if route == "auth/request-code":
                return self._request_code(body)
            if route == "auth/verify-code":
                return self._verify_code(body)
            if route == "auth/logout":
                R.session_destroy(self._token())
                return self._json(200, {"ok": True})

            if self._auth() is None:
                return

            if route == "set_location":
                host, loc = body.get("hostname"), body.get("location")
                if not host or not loc:
                    return self._json(400, {"ok": False, "error": "missing hostname/location"})
                R.cmd("HSET", R.P + "locations", host, json.dumps(loc))
                return self._json(200, {"ok": True})
        except R.RedisUnavailable as e:
            return self._json(503, {"error": str(e)})
        except Exception as e:
            return self._json(500, {"error": f"{type(e).__name__}: {e}"})

        self._json(404, {"error": "not found", "route": route})

    # ── auth ─────────────────────────────────────────────────────────────
    def _issue(self, email):
        token = secrets.token_urlsafe(32)
        R.session_create(token, email)
        return self._json(200, {"ok": True, "token": token, "email": email})

    def _login(self, body):
        """Email + password, verified against the fleet API itself."""
        email = (body.get("email") or "").strip()
        password = body.get("password") or ""
        if not email or not password:
            return self._json(400, {"ok": False, "error": "email and password required"})
        try:
            client = fleet.FleetAPIClient()
            ok, err = client.login_password(email, password)
        except Exception as e:
            return self._json(503, {"ok": False, "error": f"fleet API unreachable: {e}"})
        if not ok:
            return self._json(401, {"ok": False, "error": err or "invalid credentials"})
        return self._issue(email)

    def _request_code(self, body):
        email = (body.get("email") or "").strip()
        if not email:
            return self._json(400, {"ok": False, "error": "email required"})
        try:
            client = fleet.FleetAPIClient()
            ok, err = client.send_otp(email)
        except Exception as e:
            return self._json(503, {"ok": False, "error": f"fleet API unreachable: {e}"})
        if not ok:
            return self._json(400, {"ok": False, "error": err or "could not send code"})
        return self._json(200, {"ok": True})

    def _verify_code(self, body):
        email = (body.get("email") or "").strip()
        code = (body.get("code") or "").strip()
        if not email or not code:
            return self._json(400, {"ok": False, "error": "email and code required"})
        try:
            client = fleet.FleetAPIClient()
            ok, err = client.verify_otp(email, code)
        except Exception as e:
            return self._json(503, {"ok": False, "error": f"fleet API unreachable: {e}"})
        if not ok:
            return self._json(400, {"ok": False, "error": err or "invalid code"})
        return self._issue(email)

    # ── data assembly ────────────────────────────────────────────────────
    def _devices(self):
        d = R.jget("rigs", {}) or {}
        return {"rigs": d.get("rigs", []), "alerts": d.get("alerts", []), "active": True}

    def _stats(self):
        today = C.get_date_str()
        lb = R.jget("leaderboard", {}) or {}
        by_pi = lb.get("by_pi", {}) or {}
        total_s = sum(v for k, v in by_pi.items() if not C.is_unnamed_pi(k))

        days = {d: float(v) for d, v in (R.hgetall_json("daily_hours") or {}).items()
                if not C.is_weekend(d)}
        days[today] = total_s
        hours_by_day = [{"date": d, "hours": round(s / 3600, 3)}
                        for d, s in sorted(days.items())]

        rigs = (R.jget("rigs", {}) or {}).get("rigs", [])
        live = [r["frame_health_pct"] for r in rigs
                if r.get("frame_health_pct") is not None and r.get("online")]
        history = R.health_read(360)
        if live:
            avg_fh, min_fh = round(sum(live) / len(live), 2), round(min(live), 2)
        elif history:
            avg_fh, min_fh = history[-1]["avg"], history[-1]["min"]
        else:
            avg_fh = min_fh = None

        captured = sum(r.get("frames_captured") or 0 for r in rigs)
        dropped = sum(r.get("frames_dropped") or 0 for r in rigs)

        return {
            "total_hours_today": round(total_s / 3600, 3),
            "avg_frame_health_pct": avg_fh,
            "min_frame_health_pct": min_fh,
            "frames_captured_total": captured or None,
            "frames_dropped_total": dropped or None,
            "active_recording_rigs": sum(1 for r in rigs if r.get("status") == "recording"),
            "total_rigs": len(rigs),
            "hours_by_day": hours_by_day,
            "frame_health_history": history,
            "last_updated_ms": int(time.time() * 1000),
        }

    def _stats_range(self, qs):
        raw = (qs.get("days") or [""])[0]
        days = sorted({d.strip() for d in raw.split(",")
                       if d.strip() and not C.is_weekend(d.strip())})
        today = C.get_date_str()
        if not days:
            days = [today]

        by_pi, by_op = {}, {}

        if today in days:
            lb = R.jget("leaderboard", {}) or {}
            for k, v in (lb.get("by_pi") or {}).items():
                by_pi[k] = by_pi.get(k, 0.0) + float(v)
            for k, v in (lb.get("by_op") or {}).items():
                by_op[k] = by_op.get(k, 0.0) + float(v)

        past = [d for d in days if d != today]
        if past:
            hosts = R.cmd("KEYS", R.P + "sessions:*") or []
            for key in hosts:
                host = str(key).split("sessions:", 1)[-1]
                for sess in R.hgetall_json("sessions:" + host).values():
                    if not isinstance(sess, dict) or sess.get("date") not in past:
                        continue
                    dur = float(sess.get("duration_s") or 0)
                    if dur <= 0:
                        continue
                    label = sess.get("label") or host
                    if C.is_unnamed_pi(label):
                        continue
                    by_pi[label] = by_pi.get(label, 0.0) + dur
                    by_op[sess.get("operator") or "Unknown"] = \
                        by_op.get(sess.get("operator") or "Unknown", 0.0) + dur

        return {
            "days": days,
            "total_hours": round(sum(by_pi.values()) / 3600, 3),
            "hours_by_pi": C.ranked(by_pi, hide_unnamed=True),
            "hours_by_operator": C.ranked(by_op),
        }

    def _task_history(self):
        out = {}
        for key in (R.cmd("KEYS", R.P + "tasks:*") or []):
            host = str(key).split("tasks:", 1)[-1]
            items = [v for v in R.hgetall_json("tasks:" + host).values()
                     if isinstance(v, dict)]
            if items:
                items.sort(key=lambda t: t.get("start_time") or 0, reverse=True)
                out[host] = items[:50]
        return out

    def _summary(self, qs):
        """Everything the dashboard needs, in ONE invocation.

        The old UI hit five endpoints every 2 seconds. On serverless that is
        five billable invocations per tick per open tab, so it is consolidated
        here and the client polls this alone.
        """
        result = {}
        if "nopoll" not in qs:
            try:
                result["poll"] = logic.poll()
            except Exception as e:
                result["poll"] = {"error": f"{type(e).__name__}: {e}"}
            try:
                if logic.backfill_due():
                    result["backfill"] = logic.backfill()
            except Exception as e:
                result["backfill"] = {"error": f"{type(e).__name__}: {e}"}

        lb = R.jget("leaderboard", {}) or {}
        result.update({
            "devices": self._devices(),
            "rankings": {"pi": lb.get("pi", []), "operator": lb.get("operator", []),
                         "active": True, "source": lb.get("source", "api")},
            "stats": self._stats(),
            "task_history": self._task_history(),
            "logs": R.log_read(300),
        })
        return result