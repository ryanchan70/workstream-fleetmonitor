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


# Per-container counters. There is no way to see the real per-request Redis
# cost from outside — Upstash bills commands but does not attribute them — so
# the container reports its own running total and /api/health hands it back.
# Both reset when the container recycles, which is why the instance id is
# reported alongside: two samples are only comparable within one instance.
_INSTANCE = secrets.token_hex(4)
_BOOTED = time.time()
_REQUESTS = 0


# vercel.json rewrites every /api/* request to this one function, and carries
# the route it was actually for in this parameter. See _route_of().
_ROUTE_PARAM = "__route"


def _norm(path: str) -> str:
    """/api/foo and /foo both resolve to 'foo'."""
    p = urlparse(path).path.strip("/")
    if p.startswith("api/"):
        p = p[4:]
    elif p == "api":
        p = ""
    return p


def _route_of(path: str) -> str:
    """Which route this request is for.

    Everything under /api/* is rewritten to the single api/index.py function.
    WHICH path the function then sees depends on the Python builder Vercel
    picks: the one that produces a function named `index` passes the request's
    original path, and the one that produces `python` passes the rewrite's
    destination — under which every route arrives as "index", matches nothing,
    falls through to the auth gate, and the entire API answers 401. That is
    not a config error to be found by reading vercel.json; the same commit
    works or does not depending on which builder built it.

    So the destination states the route explicitly, in a query parameter both
    builders preserve. The path is still honoured when it carries a real route,
    so calling the function directly — agent.py --serve, or /api/health typed
    into a browser — keeps working with no rewrite in front of it.
    """
    q = parse_qs(urlparse(path).query).get(_ROUTE_PARAM)
    if q and q[0].strip("/"):
        return _norm("/" + q[0].lstrip("/"))
    return _norm(path)


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
        global _REQUESTS
        _REQUESTS += 1
        route = _route_of(self.path)
        qs = parse_qs(urlparse(self.path).query)
        qs.pop(_ROUTE_PARAM, None)

        if route == "agent":
            # Who is driving the fleet loop, and how long ago. Unauthenticated
            # like /health, and it deliberately reports no credentials — just
            # the name the agent was started with.
            try:
                a = logic.agent_alive(logic.load_state())
            except Exception:
                a = None
            return self._json(200, {
                "agent": bool(a),
                "id": (a or {}).get("id"),
                "every_s": (a or {}).get("every"),
                "age_s": round(time.time() - float((a or {}).get("at") or 0), 1) if a else None,
                "polling": "agent" if a else "serverless",
            })

        if route == "health":
            # Timezone is reported because a container that silently lacks a
            # tz database is the difference between "today" meaning the shift
            # in California and meaning UTC — and that failure is invisible
            # from the dashboard until totals reset mid-afternoon.
            return self._json(200, {
                "ok": True,
                "redis": R.configured(),
                "tz": "America/Los_Angeles" if C.PACIFIC is not None else "fallback-dst-rule",
                "today": C.get_date_str(),
                "now": C.ts(),
                "night": logic.is_night(),
                # Sample twice against the same instance id and the difference
                # is exactly what the requests in between cost.
                "instance": _INSTANCE,
                "uptime_s": round(time.time() - _BOOTED, 1),
                "requests": _REQUESTS,
                "commands": R.CMD_COUNT,
                # Across every container, accumulated by whichever one is
                # polling. A floor rather than an exact figure: a container
                # that only ever serves reads never writes, so its handful of
                # commands is not in here.
                "fleet_commands": self._fleet_commands(),
            })

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
            # The slow-moving half of the dashboard: task history, past-day
            # totals and the frame-health seed. Together they are ten times the
            # size of a live tick and change hourly, so they are fetched only
            # when the version stamp in /summary moves rather than riding along
            # on every poll.
            if route == "history":
                return self._json(200, self._history())
            if route == "devices":
                return self._json(200, self._view().get("devices", {}))
            if route == "rankings":
                return self._json(200, self._view().get(
                    "rankings", {"pi": [], "operator": [], "active": True}))
            if route == "stats":
                return self._json(200, self._stats())
            if route == "stats_range":
                return self._json(200, self._stats_range(qs))
            if route == "day_tasks":
                return self._json(200, self._day_tasks(qs))
            if route == "task_history":
                return self._json(200, R.history_load().get("tasks") or {})
            if route == "logs":
                return self._json(200, self._view().get("logs", []))
            if route == "changelog":
                return self._json(200, {"markdown": self._changelog()})
            if route == "new_changes":
                return self._json(200, {"markdown": self._new_changes()})
            # Submissions are write-only from the page; this is how they get
            # read back out. Authenticated, like everything else here.
            if route == "feedback":
                return self._json(200, {"feedback": self._feedback()})
            if route == "locations":
                return self._json(200, R.hgetall_json("locations"))
            if route == "poll":
                return self._json(200, logic.poll(force="force" in qs)[0])
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
        route = _route_of(self.path)
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

            if route == "feedback":
                text = (body.get("text") or "").strip()
                if not text:
                    return self._json(400, {"ok": False, "error": "Write something first."})
                if len(text) > 2000:
                    return self._json(400, {"ok": False,
                                            "error": "Keep it under 2000 characters."})
                # The signed-in email is taken from the session, never from the
                # request body — otherwise anyone could file feedback as
                # someone else.
                R.feedback_push({
                    # Short and stable: it is what feedback.txt keys a status
                    # off, so it has to survive being typed next to by hand.
                    "id": secrets.token_hex(3),
                    "text": text,
                    "email": R.session_email(self._token()) or "",
                    "at": time.time(),
                    "when": C.get_date_str() + " " + C.ts(),
                })
                return self._json(200, {"ok": True})

            if route == "set_location":
                host, loc = body.get("hostname"), body.get("location")
                if not host or not loc:
                    return self._json(400, {"ok": False, "error": "missing hostname/location"})
                R.cmd("HSET", R.P + "locations", host, json.dumps(loc))
                # The poll reads locations from its own state now and refreshes
                # them from this hash only every few minutes, so patch the copy
                # it will use. The hash above stays the durable record: if this
                # patch loses a race with a concurrent poll, the next refresh
                # picks it up anyway.
                try:
                    st = logic.load_state()
                    st.setdefault("poll", {}).setdefault("locations", {})[host] = loc
                    R.state_save(st)
                except Exception:
                    pass
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
    # There is deliberately very little of it left. Both halves of the answer
    # are assembled when the data changes — the live view by logic.poll(), the
    # history by logic.backfill() — so reading is a lookup, not a rebuild.
    @staticmethod
    def _view():
        return logic.load_state().get("view") or {}

    @staticmethod
    def _history():
        h = R.history_load()
        return {
            "v": h.get("v") or 0,
            "task_history": h.get("tasks") or {},
            "hours_by_day": h.get("days") or [],
            "frame_health_history": h.get("health") or [],
        }

    @staticmethod
    def _fleet_commands():
        """Running Redis command total and the rate it implies."""
        try:
            c = (logic.load_state().get("poll") or {}).get("cmds") or {}
        except Exception:
            return None
        total, since = float(c.get("n") or 0), float(c.get("since") or 0)
        if not since:
            return None
        minutes = max((time.time() - since) / 60.0, 1 / 60.0)
        return {"total": int(total), "since": since,
                "per_min": round(total / minutes, 2),
                "minutes": round(minutes, 1)}

    # Statuses recognised in feedback.txt, in the order they progress.
    FEEDBACK_STATUSES = ("submitted", "seen", "in progress", "implemented")

    @staticmethod
    def _feedback_statuses():
        """Status per submission id, read from feedback.txt.

        The submissions themselves live in Redis — a serverless filesystem is
        read-only, so the page cannot append to a file. The file is the other
        direction: it is edited by hand and deployed, and this is what it says
        about each id. Anything it does not mention is still "submitted".

        Format, one block per submission:

            [a3f2c1] 2026-07-28 14:22  someone@example.com  status: seen
            The text of the suggestion, over as many lines as it takes.
        """
        path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "feedback.txt")
        out = {}
        try:
            with open(path, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line.startswith("["):
                        continue
                    fid, _, rest = line[1:].partition("]")
                    fid = fid.strip()
                    if not fid or "status:" not in rest:
                        continue
                    status = rest.split("status:", 1)[1].strip().lower()
                    # An unrecognised status would colour as nothing at all;
                    # falling back keeps the badge meaningful.
                    out[fid] = status if status in handler.FEEDBACK_STATUSES else "submitted"
        except OSError:
            pass
        return out

    def _feedback(self):
        statuses = self._feedback_statuses()
        out = []
        for f in (R.feedback_read(100) or []):
            if not isinstance(f, dict):
                continue
            e = dict(f)
            e["id"] = C.feedback_id(e)
            # Only enough of the address to tell two submitters apart. The
            # page is behind the auth gate, but it is read by everyone who
            # gets through it, not only by whoever wrote the entry.
            addr = str(e.pop("email", "") or "")
            e["who"] = (addr.split("@", 1)[0][:3] + "…") if addr else "anonymous"
            e["status"] = statuses.get(e.get("id"), "submitted")
            out.append(e)
        return out

    def _changelog(self):
        """changelog.md, served to /changelog.html.

        The file stays in the repo root as the single source of truth and is
        pulled into the bundle by `includeFiles` in vercel.json, rather than
        being duplicated into public/. Behind the auth gate with everything
        else — release notes are not public.
        """
        path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "changelog.md")
        try:
            with open(path, encoding="utf-8") as f:
                return f.read()
        except OSError:
            return "# changelog\n\nNo changelog.md found in the deployment."

    def _new_changes(self):
        """new_changes.md, served to the dashboard's startup popup.

        Same repo-root-plus-includeFiles setup as changelog.md, but this is
        the short "what's new" blurb rather than the full history — the
        client shows it once per distinct version of this text, not once per
        deploy, so an empty/unchanged file means no popup rather than a blank
        one.
        """
        path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "new_changes.md")
        try:
            with open(path, encoding="utf-8") as f:
                return f.read()
        except OSError:
            return ""

    def _stats(self):
        """Live counters plus the history arrays, for callers that want both.

        The dashboard no longer uses this — /summary carries the counters and
        /history the arrays, separately, because one changes every poll and the
        other once an hour.
        """
        stats = dict(self._view().get("stats") or {})
        hist = self._history()
        days = list(hist["hours_by_day"])
        today = C.get_date_str()
        if stats.get("total_hours_today") is not None:
            days = [d for d in days if d.get("date") != today]
            days.append({"date": today, "hours": stats["total_hours_today"]})
            days.sort(key=lambda d: d["date"])
        stats["hours_by_day"] = days
        stats["frame_health_history"] = hist["frame_health_history"]
        return stats

    def _stats_range(self, qs):
        """Totals for a set of days.

        Past days come from the per-day split the backfill precomputes, today
        from the running totals in the poll state. This used to walk every
        session hash in the database on every call — a KEYS plus one HGETALL
        per rig, fired again after every poll tick because the dashboard
        refreshed the range alongside the live view.
        """
        raw = (qs.get("days") or [""])[0]
        days = sorted({d.strip() for d in raw.split(",")
                       if d.strip() and not C.is_weekend(d.strip())})
        today = C.get_date_str()
        if not days:
            days = [today]

        by_pi, by_op = {}, {}

        def _add(target, src):
            for k, v in (src or {}).items():
                target[k] = target.get(k, 0.0) + float(v)

        if today in days:
            p = logic.load_state().get("poll") or {}
            _add(by_pi, p.get("by_pi"))
            _add(by_op, p.get("by_op"))

        past = [d for d in days if d != today]
        if past:
            breakdown = R.history_load().get("breakdown") or {}
            for d in past:
                day = breakdown.get(d) or {}
                _add(by_pi, day.get("pi"))
                _add(by_op, day.get("op"))

        return {
            "days": days,
            "total_hours": round(sum(by_pi.values()) / 3600, 3),
            "hours_by_pi": C.ranked(by_pi, hide_unnamed=True),
            "hours_by_operator": C.ranked(by_op),
        }

    def _day_tasks(self, qs):
        """Every completed task on a given day, for the ranking drill-downs.

        Today is not served from here — the dashboard already holds today's
        tasks in the history it fetches, and they change as the shift runs.
        Past days are assembled from the per-rig session hashes, which is the
        one remaining place that fans out across the fleet, so the result is
        cached under its date. A past day cannot change, so that is a single
        build ever, and every later request for it is one read.
        """
        date = (qs.get("date") or [""])[0].strip()
        if not date or C.is_weekend(date):
            return {"date": date, "tasks": []}

        key = "daytasks:" + date
        cached = R.jget(key)
        if isinstance(cached, list):
            return {"date": date, "tasks": cached, "cached": True}

        hosts = [r.get("hostname")
                 for r in ((self._view().get("devices") or {}).get("rigs") or [])
                 if r.get("hostname")]
        # The head-camera model the backfill learned, so a session whose bytes contradict
        # its wall clock is listed at the duration it actually recorded. One
        # read, and only on the build — the cache below holds the result.
        rates = R.jget("duration_model") or {}
        out = []
        # One round trip for the fan-out. It is still one command per rig on
        # the bill, which is why the answer is kept.
        for host, stored in zip(hosts, R.hgetall_many_json("sessions:" + h for h in hosts)):
            for sess in stored.values():
                if not isinstance(sess, dict) or sess.get("date") != date:
                    continue
                dur, api_dur = C.session_durations(host, sess, rates)
                if dur <= 0:
                    continue
                task = {
                    "label": sess.get("label") or "",
                    "operator": sess.get("operator") or "Unknown",
                    "task": sess.get("task") or "Untitled task",
                    "duration_s": dur,
                    "start_time": sess.get("start_unix"),
                }
                # Only present when the two disagree; the dashboard shows it in
                # red next to the duration above.
                if api_dur is not None:
                    task["duration_api_s"] = api_dur
                out.append(task)

        out.sort(key=lambda t: t.get("start_time") or 0, reverse=True)
        if date != C.get_date_str():
            R.jset(key, out)
        return {"date": date, "tasks": out}

    def _summary(self, qs):
        """Everything a live tick needs, in ONE invocation and ONE Redis read.

        The old UI hit five endpoints every 2 seconds. On serverless that is
        five billable invocations per tick per open tab, so it was consolidated
        into this one — but consolidating the requests did nothing about the
        work behind them: each call still rebuilt the answer from a dozen keys
        and a per-rig fan-out, whether or not the fleet had been re-polled
        since the last one. Now the answer is assembled once per poll and this
        reads it, so a tick costs a single GET (often none, off the in-process
        memo) instead of roughly sixty commands.

        The heavy, slow-moving half is not here at all: the client fetches
        /history when the `history_v` stamp below changes, which is hourly.
        """
        state = logic.load_state()
        result = {}
        # An agent on the user's own hardware owns the loop while it is alive,
        # and this drops to serving what it wrote. Manual refresh still forces
        # a sweep here — that is the one case where somebody is waiting on it.
        agent = logic.agent_alive(state)
        if agent and "force" not in qs:
            result["poll"] = {"skipped": "agent", "by": agent.get("id"),
                              "age_s": round(time.time() - float(agent.get("at") or 0), 1)}
            result.update(state.get("view") or {})
            return result

        if "nopoll" not in qs:
            try:
                # ?force=1 is the manual refresh button: one invocation that
                # both forces the sweep and returns the fresh payload, rather
                # than a separate /poll call followed by a /summary.
                # The tab's own refresh rate, so the picker on the dashboard
                # means what it says without setting the floor for everyone.
                result["poll"], state = logic.poll(
                    force="force" in qs, state=state,
                    requested=(qs.get("r") or [None])[0])
            except Exception as e:
                result["poll"] = {"error": f"{type(e).__name__}: {e}"}
            try:
                if logic.backfill_due(state):
                    result["backfill"] = logic.backfill()
                    # A sweep appends its own log line to the view.
                    state = logic.load_state()
            except Exception as e:
                result["backfill"] = {"error": f"{type(e).__name__}: {e}"}

        result.update(state.get("view") or {})
        return result