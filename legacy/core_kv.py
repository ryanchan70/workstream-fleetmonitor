"""
Shared logic for the Vercel serverless deployment.

Serverless has no always-on process, so this port differs from
fleet_monitor.py in three ways:
- Auth is stateless: the fleet.shiftiq.us cookies live inside an
  HMAC-signed token held by the browser (AUTH_SECRET env var required).
- Leaderboard/rig state persists in Vercel KV (Upstash Redis) when
  attached, else best-effort warm-instance memory.
- Observed-session tracking ticks whenever the dashboard polls, instead
  of on a background loop.
"""

import base64
import hashlib
import hmac
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, wait as futures_wait

# Rig timestamps are US Pacific; serverless hosts run UTC by default.
os.environ.setdefault("TZ", os.environ.get("FLEET_TZ", "America/Los_Angeles"))
if hasattr(time, "tzset"):
    time.tzset()

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import requests
import requests.utils

from api_client import FleetAPIClient, FleetAPIError
import fleet_monitor as fm

SESSION_TTL_S = 12 * 60 * 60
PENDING_TTL_S = 5 * 60
SNAPSHOT_TTL_S = 20
LOG_RING_MAX = 200

AUTH_SECRET = os.environ.get("AUTH_SECRET", "")


def now_str() -> str:
    return time.strftime("%H:%M:%S")


# ── Stateless signed tokens ──────────────────────────────────────────────────
def _b64e(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).decode().rstrip("=")


def _b64d(s: str) -> bytes:
    return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))


def _sign(raw: str) -> str:
    return _b64e(hmac.new(AUTH_SECRET.encode(), raw.encode(), hashlib.sha256).digest())


def make_token(kind: str, email: str, client: FleetAPIClient, ttl: float) -> str:
    payload = {
        "k": kind,
        "e": email,
        "c": requests.utils.dict_from_cookiejar(client.session.cookies),
        "x": int(time.time() + ttl),
    }
    raw = _b64e(json.dumps(payload, separators=(",", ":")).encode())
    return raw + "." + _sign(raw)


def read_token(token: str | None, kind: str) -> dict | None:
    """Returns the payload for a valid, unexpired token of the given kind."""
    if not token or not AUTH_SECRET or "." not in token:
        return None
    raw, sig = token.rsplit(".", 1)
    if not hmac.compare_digest(_sign(raw), sig):
        return None
    try:
        payload = json.loads(_b64d(raw))
    except Exception:
        return None
    if payload.get("k") != kind or payload.get("x", 0) < time.time():
        return None
    return payload


def client_from_payload(payload: dict) -> FleetAPIClient:
    client = FleetAPIClient()
    client.session.cookies = requests.utils.cookiejar_from_dict(payload.get("c", {}))
    return client


# ── State store: Vercel KV (Upstash REST) with in-memory fallback ────────────
class Store:
    def __init__(self):
        self.url = (os.environ.get("KV_REST_API_URL")
                    or os.environ.get("UPSTASH_REDIS_REST_URL") or "").rstrip("/")
        self.token = (os.environ.get("KV_REST_API_TOKEN")
                      or os.environ.get("UPSTASH_REDIS_REST_TOKEN") or "")
        self._mem: dict[str, str] = {}
        self._lock = threading.Lock()

    @property
    def durable(self) -> bool:
        return bool(self.url and self.token)

    def get_json(self, key: str):
        if self.durable:
            try:
                r = requests.get(f"{self.url}/get/{key}",
                                 headers={"Authorization": f"Bearer {self.token}"}, timeout=5)
                raw = r.json().get("result")
                return json.loads(raw) if raw else None
            except Exception:
                return None
        with self._lock:
            raw = self._mem.get(key)
        return json.loads(raw) if raw else None

    def set_json(self, key: str, value, ttl_s: int | None = None):
        raw = json.dumps(value, separators=(",", ":"))
        if self.durable:
            try:
                url = f"{self.url}/set/{key}" + (f"?EX={ttl_s}" if ttl_s else "")
                requests.post(url, data=raw,
                              headers={"Authorization": f"Bearer {self.token}"}, timeout=5)
            except Exception:
                pass
            return
        with self._lock:
            self._mem[key] = raw


store = Store()


# ── Log ring (feeds the dashboard terminal panel) ────────────────────────────
def append_log(lines: list[str]):
    if not lines:
        return
    ring = store.get_json("fleet_log") or []
    ring.extend(lines)
    store.set_json("fleet_log", ring[-LOG_RING_MAX:])


def get_log() -> list[str]:
    return store.get_json("fleet_log") or []


# ── Snapshot: rig status + leaderboard, built on demand ──────────────────────
def _fresh_state(today: str) -> dict:
    return {"date": today, "device_sessions": {}, "observed": {}, "tracker": {}, "alerted": {}}


def _update_tracker(state: dict, devices: list[dict], today: str):
    tracker, observed = state["tracker"], state["observed"]
    for d in devices:
        h = d.get("hostname") or ""
        if not h:
            continue
        status = fm.get_status(d)
        trk = tracker.get(h)
        if status == "recording":
            op = fm.clean_str(d.get("operator"))
            dur = float(d.get("recording_duration_s") or 0)
            if trk and (op != trk["op"] or dur < trk["dur"] - 30):
                observed.setdefault(h, []).append(
                    {"operator": trk["op"], "duration_s": trk["dur"], "date": trk["date"]})
                trk = None
            tracker[h] = {"op": op, "dur": max(dur, trk["dur"]) if trk else dur, "date": today}
        elif status == "offline":
            pass  # rig may reconnect mid-session; keep the tracker
        elif trk:
            observed.setdefault(h, []).append(
                {"operator": trk["op"], "duration_s": trk["dur"], "date": trk["date"]})
            tracker.pop(h, None)


def build_snapshot(client: FleetAPIClient) -> dict | None:
    devices = client.get_fleet_status()
    if devices is None:
        return None

    today = fm.get_date_str()
    state = store.get_json("fleet_state") or _fresh_state(today)
    if state.get("date") != today:
        state = _fresh_state(today)

    _update_tracker(state, devices, today)
    device_sessions = state["device_sessions"]
    observed = state["observed"]

    online = [d for d in devices if fm.get_status(d) != "offline" and d.get("hostname")]

    def fetch(d):
        h = d["hostname"]
        try:
            groups = client.get_device_sessions(h, timeout=8, retries=1)
            return h, fm._trim_groups(groups, today)
        except FleetAPIError:
            return h, None

    with ThreadPoolExecutor(max_workers=16) as ex:
        futs = [ex.submit(fetch, d) for d in online]
        done, _ = futures_wait(futs, timeout=25)
    for f in done:
        h, groups = f.result()
        if groups is not None:
            device_sessions[h] = groups
            observed.pop(h, None)  # device list is authoritative again

    by_pi: dict[str, float] = {}
    by_op: dict[str, float] = {}
    seen_ids: set[str] = set()
    for d in devices:
        h = d.get("hostname") or ""
        label = fm.device_label(d)
        groups = device_sessions.get(h)
        if groups:
            for rec in groups:
                if not fm.session_is_today(rec, today):
                    continue
                sid = rec.get("id") or rec.get("session_uuid") or f"{label}|{rec.get('name')}"
                if sid in seen_ids:
                    continue
                seen_ids.add(sid)
                dur = float(rec.get("duration_s") or 0)
                op = fm.clean_str(rec.get("operator"))
                by_pi[label] = by_pi.get(label, 0.0) + dur
                by_op[op] = by_op.get(op, 0.0) + dur
        else:
            for obs in observed.get(h, []):
                if obs.get("date") != today:
                    continue
                dur = float(obs.get("duration_s") or 0)
                op = fm.clean_str(obs.get("operator"))
                by_pi[label] = by_pi.get(label, 0.0) + dur
                by_op[op] = by_op.get(op, 0.0) + dur

    for d in devices:
        if fm.get_status(d) == "recording":
            dur = float(d.get("recording_duration_s") or 0)
            label = fm.device_label(d)
            op = fm.clean_str(d.get("operator"))
            by_pi[label] = by_pi.get(label, 0.0) + dur
            by_op[op] = by_op.get(op, 0.0) + dur

    rigs, alerts = fm.evaluate_rigs(devices)

    # Alert transitions -> red/green lines in the dashboard terminal panel.
    alerted = state.get("alerted", {})
    current = {f"{a['hostname']}|{a['kind']}": a for a in alerts}
    lines = []
    for k, a in current.items():
        if k not in alerted:
            lines.append(f"{fm.ANSI_URGENT_FMT}[{now_str()}] CRITICAL {a['rig']}: {a['message']}{fm.ANSI_RESET}")
    for k, msg in alerted.items():
        if k not in current:
            rig = k.split("|")[0]
            lines.append(f"[{now_str()}] {fm.ANSI_GREEN}RESOLVED {rig}: cleared — {msg}{fm.ANSI_RESET}")
    append_log(lines)
    state["alerted"] = {k: a["message"] for k, a in current.items()}

    def ranked(source):
        return [{"name": k, "duration": fm.format_time(v)}
                for k, v in sorted(source.items(), key=lambda x: x[1], reverse=True)]

    snapshot = {
        "ts": time.time(),
        "rankings": {"pi": ranked(by_pi), "operator": ranked(by_op), "source": "api"},
        "rigs": rigs,
        "alerts": alerts,
    }
    store.set_json("fleet_state", state)
    store.set_json("fleet_snapshot", snapshot)
    return snapshot


def get_snapshot(client: FleetAPIClient) -> dict | None:
    snap = store.get_json("fleet_snapshot")
    if snap and time.time() - snap.get("ts", 0) < SNAPSHOT_TTL_S:
        return snap
    return build_snapshot(client) or snap
