#!/usr/bin/env python3
"""
fleet_monitor.py
Queries the fleet.shiftiq.us JSON API for device/session status and serves a
local-only dashboard with live status updates, rig timings, locations, task history,
and push notifications for critical alerts.
"""

import json
import sys
import time
import datetime
import threading
import os
import re
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs

import collections
import statistics
import secrets as _secrets
from api_client import FleetAPIClient, FleetAPIError
from auth import DashboardAuth

# ── Config ────────────────────────────────────────────────────────────────────
POLL_INTERVAL = 15
WEB_PORT = 8080

# Critical alert thresholds
FRAME_HEALTH_MIN_PCT = 95.0   # frame health below this while recording => critical
STORAGE_MIN_FREE_PCT = 10.0   # free disk % below this => critical
STORAGE_MIN_FREE_GB  = 50.0   # free disk GB below this => critical
TEMP_OVERHEAT_C = 70.0        # CPU/SSD overheating threshold

# Alert deduplication: don't spam same alert within 10 minutes
ALERT_DEBOUNCE_SEC = 600

# ANSI Color Codes
ANSI_BRIGHT_RED   = "\033[91m"
ANSI_YELLOW       = "\033[93m"
ANSI_GREEN        = "\033[92m"
ANSI_BLUE         = "\033[94m"
ANSI_LIGHT_BLUE   = "\033[96m"
ANSI_LIGHT_PURPLE = "\033[95m"
ANSI_RESET        = "\033[0m"

# Formatting
ANSI_BOLD         = "\033[1m"
ANSI_UNDERLINE    = "\033[4m"
ANSI_REC          = f"\033[91m\033[1m\033[4m"
ANSI_WARN_FMT     = f"\033[43m\033[30m\033[1m\033[4m\033[5m"
ANSI_URGENT_FMT   = f"\033[41m\033[97m\033[1m\033[4m\033[5m"
# ─────────────────────────────────────────────────────────────────────────────

# Global state for logging
daily_totals: dict[str, dict] = {}
recording_cache: dict[str, dict] = {}
device_cache: dict[str, dict] = {}
logged_session_ids: set[str] = set()
log_lock = threading.Lock()
api_client: FleetAPIClient | None = None

# ── Device locations & task history cache ─────────────────────────────────
_device_locations: dict[str, str] = {}  # hostname -> "location_name"
_completed_tasks: dict[str, list] = {}   # hostname -> [{"name": "", "op": "", "dur": s, "ts": unix}]
_task_history_lock = threading.Lock()
# Runtime caches, generated logs and secrets live under local/ so the repo
# root holds only what the Vercel build and git actually need. Resolved
# against this file rather than the cwd so the script runs from anywhere.
_LOCAL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "local")
_CACHE_DIR = os.path.join(_LOCAL_DIR, "caches")
os.makedirs(_CACHE_DIR, exist_ok=True)

_LOCATIONS_FILE = os.path.join(_CACHE_DIR, ".device_locations.json")
_TASK_HISTORY_FILE = os.path.join(_CACHE_DIR, ".task_history.json")

# ── Alert history (prevent spam) ──────────────────────────────────────────
_last_alert_ts: dict[str, float] = {}  # "hostname|kind" -> unix timestamp
_alert_lock = threading.Lock()

def _should_alert(hostname: str, kind: str) -> bool:
    """Returns True if this alert hasn't fired in the last ALERT_DEBOUNCE_SEC seconds."""
    key = f"{hostname}|{kind}"
    now = time.time()
    with _alert_lock:
        last = _last_alert_ts.get(key, 0.0)
        if now - last >= ALERT_DEBOUNCE_SEC:
            _last_alert_ts[key] = now
            return True
    return False

def is_weekend(date_str: str) -> bool:
    """Returns True if date_str (YYYY-MM-DD) is Saturday or Sunday."""
    try:
        d = datetime.datetime.strptime(date_str, "%Y-%m-%d")
        return d.weekday() >= 5  # 5=Sat, 6=Sun
    except Exception:
        return False

def _load_locations():
    global _device_locations
    try:
        with open(_LOCATIONS_FILE) as f:
            _device_locations = json.load(f)
        web_print(f"[{ts()}] DEBUG  Loaded {len(_device_locations)} device locations.")
    except Exception:
        pass

def _save_locations():
    try:
        tmp = _LOCATIONS_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(_device_locations, f, indent=2)
        os.replace(tmp, _LOCATIONS_FILE)
    except Exception as e:
        web_print(f"[{ts()}] ERROR  Failed to save locations: {e}")

def _load_task_history():
    """Loads the cached task history, dropping legacy entries written before
    sessions carried a real start time / task label. Those older records
    rendered as 'Invalid Date — op / 20260619_132103'; discarding them lets
    the next backfill rebuild them correctly from the session cache."""
    global _completed_tasks
    try:
        with open(_TASK_HISTORY_FILE) as f:
            data = json.load(f)
        dropped = 0
        cleaned: dict[str, list] = {}
        for k, v in data.items():
            if not isinstance(v, list):
                continue
            keep = []
            for t in v:
                if not isinstance(t, dict):
                    continue
                # Legacy record: no start_time, or a task that is really just
                # the raw session folder name (YYYYMMDD_HHMMSS).
                task = str(t.get("task") or "")
                if not t.get("start_time") or re.match(r"^\d{8}_\d{6}", task):
                    dropped += 1
                    continue
                keep.append(t)
            if keep:
                cleaned[k] = keep
        with _task_history_lock:
            _completed_tasks = cleaned
        n = sum(len(v) for v in _completed_tasks.values())
        msg = f"[{ts()}] DEBUG  Loaded {len(_completed_tasks)} Pi(s), {n} task(s) total."
        if dropped:
            msg += f" Dropped {dropped} legacy record(s); backfill will rebuild them."
        web_print(msg)
    except Exception:
        pass

def _save_task_history():
    with _task_history_lock:
        snap = dict(_completed_tasks)
    try:
        tmp = _TASK_HISTORY_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(snap, f, indent=2)
        os.replace(tmp, _TASK_HISTORY_FILE)
    except Exception:
        pass

def _archive_completed_task(hostname: str, label: str, operator: str, task: str, duration_s: float, start_time_unix: float = None):
    """Cache a completed task to the persistent history."""
    if start_time_unix is None:
        start_time_unix = time.time() - duration_s  # estimate if not provided

    with _task_history_lock:
        if hostname not in _completed_tasks:
            _completed_tasks[hostname] = []

        # Truncate task name to 20 chars
        task_short = task[:20] if task else "Unknown"

        _completed_tasks[hostname].append({
            "label": label,
            "operator": operator,
            "task": task_short,
            "duration_s": duration_s,
            "start_time": int(start_time_unix),
            "_session_id": f"{label}|{operator}|{task}|{duration_s}|{int(start_time_unix)}"
        })
    _save_task_history()

dashboard_auth = DashboardAuth()

# ── Single-sign-on: auto-token generated when fleet.shiftiq.us login succeeds ─
# The HTML page calls /auth/auto-token on load and gets in without a second OTP.
_AUTO_TOKEN: str | None = None
_AUTO_EMAIL: str | None = None

def _setup_auto_token(email: str):
    global _AUTO_TOKEN, _AUTO_EMAIL
    _AUTO_TOKEN = _secrets.token_urlsafe(32)
    _AUTO_EMAIL = email

def _check_auto_token(token: str | None) -> str | None:
    """Returns email if the token matches the machine auto-token, else None."""
    if token and _AUTO_TOKEN and token == _AUTO_TOKEN:
        return _AUTO_EMAIL
    return None

# ── Frame health history (rolling 2-hour window at 5-s poll rate) ─────────────
_HEALTH_HISTORY: collections.deque = collections.deque(maxlen=1440)
_health_lock = threading.Lock()
_HEALTH_HISTORY_FILE = os.path.join(_CACHE_DIR, ".frame_health_cache.json")
_health_save_counter = 0   # save every 60 entries (~5 min at 5-s poll)

def _load_health_history():
    try:
        with open(_HEALTH_HISTORY_FILE) as f:
            entries = json.load(f)
        with _health_lock:
            for e in entries[-1440:]:
                _HEALTH_HISTORY.append(e)
        print(f"[{ts()}] DEBUG  Loaded {len(entries)} frame-health history entries.")
    except Exception:
        pass

def _save_health_history():
    with _health_lock:
        snap = list(_HEALTH_HISTORY)
    try:
        tmp = _HEALTH_HISTORY_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(snap[-1440:], f)
        os.replace(tmp, _HEALTH_HISTORY_FILE)
    except Exception:
        pass

# ── Persistent daily hours cache ──────────────────────────────────────────
# Survives restarts; keyed by YYYY-MM-DD, value in seconds (named pis only).
_DAILY_HOURS_FILE = os.path.join(_CACHE_DIR, ".daily_hours_cache.json")
_daily_hours_cache: dict[str, float] = {}
_daily_hours_lock = threading.Lock()

def _load_daily_hours_cache():
    global _daily_hours_cache
    try:
        with open(_DAILY_HOURS_FILE) as f:
            data = json.load(f)
        with _daily_hours_lock:
            _daily_hours_cache = {k: float(v) for k, v in data.items() if isinstance(v, (int, float))}
        print(f"[{ts()}] DEBUG  Loaded daily hours cache: {len(_daily_hours_cache)} days")
    except Exception:
        pass

def _save_daily_hours_cache():
    with _daily_hours_lock:
        snap = dict(_daily_hours_cache)
    try:
        tmp = _DAILY_HOURS_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(snap, f, indent=2)
        os.replace(tmp, _DAILY_HOURS_FILE)
    except Exception as e:
        pass

def _flush_past_days_to_cache():
    """Promotes completed days from daily_totals into _daily_hours_cache and saves.
    Called during snapshot and on exit so hours aren't lost on restart."""
    today = get_date_str()
    updated = False
    with log_lock:
        for day, day_data in daily_totals.items():
            if day == today:
                continue  # today isn't finished — don't bake it in
            src_pi = day_data.get("ui_live_pi") or day_data.get("by_pi", {})
            day_s  = sum(v for k, v in src_pi.items() if not is_unnamed_pi(k)) if src_pi else 0
            if day_s > 0:
                with _daily_hours_lock:
                    if _daily_hours_cache.get(day, 0) < day_s:
                        _daily_hours_cache[day] = day_s
                        updated = True
    if updated:
        _save_daily_hours_cache()

# ── Persistent per-Pi session cache (ALL days, not trimmed to today) ─────────
# Unlike _device_sessions_cache (today only, in-memory-refreshed each cycle),
# this holds every session ever fetched for every named Pi, merged by session
# id, and survives restarts. It is the durable source used to (a) speed up
# backfill on subsequent runs and (b) keep counting a Pi's time in daily
# totals even after it goes offline and can no longer be reached directly.
_PI_SESSION_CACHE_FILE = os.path.join(_CACHE_DIR, ".pi_session_cache.json")
_pi_session_cache: dict[str, dict] = {}   # hostname -> {"label":.., "sessions": {sid: {...}}}
_pi_session_cache_lock = threading.Lock()

def _load_pi_session_cache():
    global _pi_session_cache
    try:
        with open(_PI_SESSION_CACHE_FILE) as f:
            data = json.load(f)
        with _pi_session_cache_lock:
            _pi_session_cache = data
        n = sum(len(v.get("sessions", {})) for v in data.values())
        print(f"[{ts()}] DEBUG  Loaded per-Pi session cache: {len(data)} pi(s), {n} session(s).")
    except Exception:
        pass

def _save_pi_session_cache():
    with _pi_session_cache_lock:
        snap = _pi_session_cache
    try:
        tmp = _PI_SESSION_CACHE_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(snap, f)
        os.replace(tmp, _PI_SESSION_CACHE_FILE)
    except Exception:
        pass

def _merge_pi_sessions(hostname: str, label: str, sessions: list[dict]) -> bool:
    """Merges freshly-fetched sessions into the persistent per-Pi cache, keyed
    by session id so re-fetches never duplicate. Never deletes old sessions —
    this is exactly what lets an offline Pi's history keep counting.
    Returns True if anything new was added."""
    changed = False
    with _pi_session_cache_lock:
        entry = _pi_session_cache.setdefault(hostname, {"label": label, "sessions": {}})
        entry["label"] = label
        for rec in sessions:
            sid = rec.get("id") or rec.get("session_uuid") or f"{label}|{rec.get('name')}"
            dur = float(rec.get("duration_s") or 0)
            if dur <= 0:
                continue
            existing = entry["sessions"].get(sid)
            # What the recording actually weighs — the input to the
            # byte-implied duration that keeps a hung session's wall clock out
            # of the rankings (see session_durations()). A session still
            # uploading reports no size at all, so a stored None is refreshed
            # the moment the API starts reporting one, not left forever.
            size_bytes = _extract_size_bytes(rec)
            if (existing is None or existing.get("duration_s", 0) != dur
                    or "start_unix" not in existing
                    or (size_bytes is not None and existing.get("size_bytes") != size_bytes)):
                entry["sessions"][sid] = {
                    "date": _session_date(rec),
                    "duration_s": dur,
                    "operator": clean_str(rec.get("operator")),
                    "name": rec.get("name"),
                    # Task label + real start timestamp. Without these the task
                    # history can only fall back to the raw folder name
                    # (20260619_132103) and has no date to render.
                    "task": clean_str(rec.get("task")),
                    "start_unix": _session_start_unix(rec),
                    "size_bytes": size_bytes,
                }
                changed = True
    return changed

def _pi_cached_day_total(hostname: str, date_str: str) -> float:
    """Sums this Pi's cached session durations for a specific date, counting
    the byte-implied duration wherever it disagrees with the API's span."""
    with _pi_session_cache_lock:
        entry = _pi_session_cache.get(hostname)
        if not entry:
            return 0.0
        sessions = list(entry["sessions"].values())
    return sum(session_durations(hostname, s)[0]
               for s in sessions if s.get("date") == date_str)

def _pi_cached_label(hostname: str) -> str | None:
    with _pi_session_cache_lock:
        entry = _pi_session_cache.get(hostname)
        return entry.get("label") if entry else None


def _session_start_unix(rec: dict) -> int | None:
    """Derives the session's START time as a unix timestamp.

    Prefers the API's own start_time_unix/mtime. Falls back to parsing the
    session folder name, which is formatted YYYYMMDD_HHMMSS (e.g.
    20260619_132103) — that name is often the ONLY time information a
    backfilled session carries."""
    start_unix = rec.get("start_time_unix") or rec.get("mtime") or 0
    if start_unix:
        try:
            return int(float(start_unix))
        except Exception:
            pass
    name = str(rec.get("name", ""))
    m = re.match(r"^(\d{8})_(\d{6})", name)
    if m:
        try:
            dt = datetime.datetime.strptime(m.group(1) + m.group(2), "%Y%m%d%H%M%S")
            return int(dt.timestamp())
        except Exception:
            pass
    return None


def _session_date(rec: dict) -> str | None:
    """Derives YYYY-MM-DD from a session record via folder name or unix timestamp."""
    name = str(rec.get("name", ""))
    if len(name) >= 8 and name[:8].isdigit():
        raw = name[:8]
        return f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"
    start_unix = rec.get("start_time_unix") or rec.get("mtime") or 0
    if start_unix:
        try:
            return datetime.datetime.fromtimestamp(float(start_unix)).strftime("%Y-%m-%d")
        except Exception:
            pass
    return None


# ── Byte-implied session duration ─────────────────────────────────────────
# A session's duration_s is wall clock: the span between the recording folder
# opening and closing. Usually that IS the recording, but a rig that hangs, or
# finalizes/uploads long after capture really stopped, leaves the span far
# longer than what actually got recorded — while the bytes on disk still
# reflect the real thing. Bytes are much harder to fake, so each rig's own
# median bytes/second gives an independent estimate of how long it recorded.
#
# These are categorize_tasks.py's constants and its rule, deliberately: the
# correction only ever runs ONE WAY. A byte estimate above the wall clock means
# the rig recorded denser than its median — more cameras, a higher bitrate —
# not that it ran longer than the folder was open, so an estimate that comes in
# high is ignored. Only an estimate that falls to DURATION_MISMATCH_RATIO of
# the wall clock or below is treated as evidence the span is inflated, and only
# then does the estimate become the counted duration, with the API's number
# carried alongside so the dashboard can show what it replaced.
MIN_BASELINE_DURATION_S = 120.0  # sessions shorter than this make noisy rates
MIN_BASELINE_SESSIONS = 5        # per-rig samples needed to trust its own rate
DURATION_MISMATCH_RATIO = 0.75   # how far below wall clock the estimate must
                                 # fall before the wall clock is distrusted

# Session-level total, whatever the rig's firmware calls it.
_SIZE_KEYS = ("size_bytes", "total_bytes", "bytes_total", "total_size_bytes",
              "session_bytes", "bytes")
_SIZE_LIST_KEYS = ("mcap_files", "session_files", "files", "recordings")

_byte_rate_host: dict[str, float] = {}   # hostname -> median bytes/sec
_byte_rate_fleet: float | None = None    # fleet-wide fallback median
_byte_rate_lock = threading.Lock()


def _extract_size_bytes(rec: dict) -> int | None:
    """Total bytes for one session record, or None if the API didn't say.

    Falls back to summing the per-file lists, because the light=1 payload the
    session fetch uses does not always carry a session-level total."""
    for k in _SIZE_KEYS:
        v = rec.get(k)
        if v is None or isinstance(v, bool):
            continue
        try:
            n = float(v)
        except (TypeError, ValueError):
            continue
        if n > 0:
            return int(n)

    total = 0.0
    for sub in _SIZE_LIST_KEYS:
        items = rec.get(sub)
        if not isinstance(items, list):
            continue
        for it in items:
            if not isinstance(it, dict):
                continue
            try:
                total += float(it.get("size_bytes") or 0)
            except (TypeError, ValueError):
                pass
    return int(total) if total > 0 else None


def _rebuild_byte_rate_baselines():
    """Recomputes every rig's median bytes/second from its own cached sessions,
    plus a fleet-wide median for rigs with too few samples of their own.

    Medians, not means, so the very hangs this is meant to catch can't drag a
    rig's baseline along with them. Runs once per backfill, off the persistent
    cache, so the baselines keep sharpening as history accumulates."""
    global _byte_rate_host, _byte_rate_fleet
    with _pi_session_cache_lock:
        snapshot = {h: dict(v) for h, v in _pi_session_cache.items()}

    host_rates: dict[str, float] = {}
    all_rates: list[float] = []
    for hostname, entry in snapshot.items():
        rates = []
        for sess in entry.get("sessions", {}).values():
            dur = float(sess.get("duration_s") or 0)
            try:
                nbytes = float(sess.get("size_bytes") or 0)
            except (TypeError, ValueError):
                continue
            if dur < MIN_BASELINE_DURATION_S or nbytes <= 0:
                continue
            rates.append(nbytes / dur)
        all_rates.extend(rates)
        if len(rates) >= MIN_BASELINE_SESSIONS:
            host_rates[hostname] = statistics.median(rates)

    with _byte_rate_lock:
        _byte_rate_host = host_rates
        _byte_rate_fleet = statistics.median(all_rates) if all_rates else None


def session_durations(hostname: str, sess: dict) -> tuple[float, float | None]:
    """(duration to count, API duration when the bytes contradict it).

    Returns the byte-implied estimate only when it falls to
    DURATION_MISMATCH_RATIO of the API's span or below — the signature of an
    inflated wall clock. The second element is None every other time: an
    estimate that agrees, an estimate that comes in high, or no size and no
    baseline to judge with. In all of those the API duration stands."""
    api_dur = float(sess.get("duration_s") or 0)
    try:
        nbytes = float(sess.get("size_bytes") or 0)
    except (TypeError, ValueError):
        nbytes = 0.0
    if api_dur <= 0 or nbytes <= 0:
        return api_dur, None

    with _byte_rate_lock:
        rate = _byte_rate_host.get(hostname) or _byte_rate_fleet
    if not rate:
        return api_dur, None

    implied = nbytes / rate
    if implied < api_dur * DURATION_MISMATCH_RATIO:
        return implied, api_dur
    return api_dur, None


def backfill_daily_hours(client: "FleetAPIClient"):
    """Fetches the FULL session history for every named device, merges it
    into the persistent per-Pi session cache (_pi_session_cache), and
    recomputes _daily_hours_cache for all past days from that cache — not
    just from what was fetched this cycle. This means a Pi that has since
    gone offline still contributes its previously-cached sessions to the
    daily totals. Runs on startup and every 5 minutes thereafter."""
    global _last_backfill_message
    devices = client.get_fleet_status()
    if not devices:
        return

    results: dict[str, tuple[str, list]] = {}
    failed_502: list[str] = []
    failed_other: list[tuple[str, str]] = []
    results_lock = threading.Lock()

    def _fetch(hostname: str, label: str):
        try:
            groups = client.get_device_sessions(hostname)
            with results_lock:
                results[hostname] = (label, groups)
        except Exception as e:
            msg = str(e)
            with results_lock:
                if "502" in msg:
                    failed_502.append(label)
                else:
                    failed_other.append((label, msg))

    threads = []
    named_hosts: list[tuple[str, str]] = []
    for d in devices:
        hostname = d.get("hostname")
        if not hostname:
            continue
        label = device_label(d)
        if is_unnamed_pi(label):
            continue   # never count anonymous pis
        named_hosts.append((hostname, label))
        t = threading.Thread(target=_fetch, args=(hostname, label), daemon=True)
        threads.append(t); t.start()
    for t in threads:
        t.join(timeout=45)

    # ── Merge freshly-fetched sessions into the persistent cache ───────────────
    any_new = False
    for hostname, (label, groups) in results.items():
        if _merge_pi_sessions(hostname, label, groups or []):
            any_new = True
    if any_new:
        _save_pi_session_cache()

    # Rates first: everything below counts byte-implied durations, and the
    # baselines have to see this cycle's freshly merged sessions to produce
    # them.
    _rebuild_byte_rate_baselines()

    # ── Recompute all_by_day from the FULL persistent cache (covers Pis that ──
    # are currently offline and weren't in `results` this cycle at all).
    today = get_date_str()
    all_by_day: dict[str, float] = {}
    n_estimated = 0
    with _pi_session_cache_lock:
        cache_snapshot = {h: dict(v) for h, v in _pi_session_cache.items()}
    for hostname, entry in cache_snapshot.items():
        label = entry.get("label", hostname)
        if is_unnamed_pi(label):
            continue
        for sess in entry.get("sessions", {}).values():
            date_str = sess.get("date")
            if not date_str or date_str == today:
                continue   # today is handled live by the leaderboard
            if is_weekend(date_str):
                continue   # skip weekends
            dur, api_dur = session_durations(hostname, sess)
            if api_dur is not None:
                n_estimated += 1
            if dur > 0:
                all_by_day[date_str] = all_by_day.get(date_str, 0.0) + dur

    updated = False
    with _daily_hours_lock:
        for day, total_s in all_by_day.items():
            prev = _daily_hours_cache.get(day, 0)
            # This total is recomputed from the FULL persistent session cache,
            # including Pis that have since gone offline, so it is authoritative
            # for any day that cache covers — it replaces the stored figure
            # rather than only raising it. Without that, a day left inflated by
            # an earlier run could never settle back down. Live-log totals that
            # the cache never saw are still folded in by
            # _flush_past_days_to_cache(), which does only raise.
            if abs(total_s - prev) > 1.0:
                _daily_hours_cache[day] = total_s
                updated = True

    if updated:
        _save_daily_hours_cache()

    # ── Backfill task history from session cache ─────────────────────────────
    # Add all cached sessions to task history for later review
    with _pi_session_cache_lock:
        cache_snapshot = {h: dict(v) for h, v in _pi_session_cache.items()}
    with _task_history_lock:
        for hostname, entry in cache_snapshot.items():
            label = entry.get("label", hostname)
            if hostname not in _completed_tasks:
                _completed_tasks[hostname] = []
            existing = {t.get("_session_id"): t for t in _completed_tasks[hostname]}
            for sid, sess in entry.get("sessions", {}).items():
                # What this session actually recorded, and the API's own
                # figure when the bytes say it was wrong. dur is what the
                # history counts; api_dur is only carried so the dashboard can
                # show, in red, the number it replaced.
                dur, api_dur = session_durations(hostname, sess)

                prior = existing.get(sid)
                if prior is not None:
                    # Already archived — but the baselines sharpen with every
                    # sweep, so re-apply the current verdict rather than
                    # leaving the first run's duration frozen in place.
                    prior["duration_s"] = dur
                    if api_dur is None:
                        prior.pop("duration_api_s", None)
                    else:
                        prior["duration_api_s"] = api_dur
                    continue

                # Real task label ("First aid"), NOT the session folder
                # name. Only fall back to the folder name when the API
                # genuinely gave us no task for this session.
                task_name = sess.get("task")
                if not task_name or task_name == "Unknown":
                    task_name = sess.get("name") or "Unknown"
                task_short = str(task_name)[:20]

                # Real start time, parsed from the API field or the
                # YYYYMMDD_HHMMSS folder name — never "now", which is
                # what produced Invalid Date / wrong timestamps.
                start_unix = sess.get("start_unix")
                if not start_unix:
                    start_unix = _session_start_unix({"name": sess.get("name")})

                record = {
                    "label": label,
                    "operator": sess.get("operator", "Unknown"),
                    "task": task_short,
                    "duration_s": dur,
                    "start_time": start_unix,   # may be None -> UI shows "—"
                    "_session_id": sid
                }
                if api_dur is not None:
                    record["duration_api_s"] = api_dur
                _completed_tasks[hostname].append(record)
    _save_task_history()

    # ── Exactly ONE line per run, summarizing everything ────────────────────
    n = len(all_by_day)
    n_offline_cached = sum(1 for h, _ in named_hosts if h not in results and h in cache_snapshot)
    fail_bits = []
    if failed_502:
        fail_bits.append(f"{len(failed_502)} 502'd ({', '.join(failed_502)})")
    if failed_other:
        fail_bits.append(f"{len(failed_other)} failed ({', '.join(l for l, _ in failed_other)})")
    fail_summary = f", {'; '.join(fail_bits)}" if fail_bits else ""
    est_summary = (f", {ANSI_BRIGHT_RED}{n_estimated} session(s) counted by "
                   f"byte estimate{ANSI_RESET}") if n_estimated else ""
    message = (f"[{ts()}] INFO   Backfill — {n} past day(s) totaled, "
               f"{n_offline_cached} Pi(s) from cache while unreachable, "
               f"cache {'updated' if updated else 'unchanged'}"
               f"{est_summary}{fail_summary}.")
    if message != _last_backfill_message:
        web_print(message)
        _last_backfill_message = message


def _record_health_snapshot(rigs: list[dict]):
    """Called after each fleet poll. Appends avg/min frame health across
    all ONLINE rigs that report a frame_health_pct, to the rolling history."""
    global _health_save_counter
    readings = [r["frame_health_pct"] for r in rigs
                if r.get("frame_health_pct") is not None and r.get("online")]
    if not readings:
        return
    avg_h = sum(readings) / len(readings)
    min_h = min(readings)
    with _health_lock:
        _HEALTH_HISTORY.append({
            "t": int(time.time() * 1000),
            "avg": round(avg_h, 2),
            "min": round(min_h, 2),
        })
    _health_save_counter += 1
    if _health_save_counter % 60 == 0:
        _save_health_history()

# Web UI Tracking State
loop_active = True
terminal_buffer = []
buffer_lock = threading.Lock()

def ts():
    return datetime.datetime.now().strftime("%H:%M:%S")

def get_date_str():
    return datetime.datetime.now().strftime("%Y-%m-%d")

def web_print(text):
    print(text)
    with buffer_lock:
        terminal_buffer.append(text)
        if len(terminal_buffer) > 1000:
            terminal_buffer.pop(0)

def format_time(seconds: float) -> str:
    sign = "-" if seconds < 0 else ""
    sec_val = abs(float(seconds))
    h = int(sec_val // 3600)
    m = int((sec_val % 3600) // 60)
    s = int(sec_val % 60)
    return f"{sign}{h:02d}:{m:02d}:{s:02d}"

def clean_str(val, default="Unknown"):
    if not val or str(val).strip() in ("", "None", "null", "—"):
        return default
    return str(val).strip()

def parse_duration_from_log(duration_str: str) -> float:
    duration_str = duration_str.strip()
    match_paren = re.search(r"\((\-?[\d\.]+)s\)", duration_str)
    if match_paren: return float(match_paren.group(1))
    match_multi = re.search(r"^(\-?[\d\.]+)s\s*\|", duration_str)
    if match_multi: return float(match_multi.group(1))
    if duration_str.endswith("s") and ":" not in duration_str:
        try: return float(duration_str.replace("s", ""))
        except ValueError: pass
    match_hms = re.search(r"(\-?\d{1,2}):(\d{2}):([\d\.]+)", duration_str)
    if match_hms:
        h, m, s = match_hms.groups()
        sign = -1 if h.startswith("-") else 1
        return sign * (abs(float(h)) * 3600 + float(m) * 60 + float(s))
    return 0.0

# ── Log file layout ───────────────────────────────────────────────────────
# Each kind of log gets its own subfolder so the repo root stays clean.
OPERATOR_LOG_DIR  = os.environ.get("FLEET_OPERATOR_LOG_DIR",  os.path.join(_LOCAL_DIR, "logs", "operator_sessions"))
RECORDING_LOG_DIR = os.environ.get("FLEET_RECORDING_LOG_DIR", os.path.join(_LOCAL_DIR, "logs", "recording_logs"))


def _log_path(directory: str, filename: str) -> str:
    try:
        os.makedirs(directory, exist_ok=True)
    except Exception:
        pass
    return os.path.join(directory, filename)


def operator_log_path(date_str: str) -> str:
    return _log_path(OPERATOR_LOG_DIR, f"operator_sessions_{date_str}.txt")


def recording_log_path(date_str: str) -> str:
    return _log_path(RECORDING_LOG_DIR, f"daily_recording_log_{date_str}.txt")


def _field(value: str) -> str:
    """Sanitises a value for a pipe-delimited line.

    Real locations already contain pipes ("Kung Fu Tea | Woodside Road"), which
    silently corrupts the column layout and breaks anything parsing these logs.
    """
    return str(value).replace("|", "/").strip()


def operator_session_line(start_unix, operator, task, duration_s,
                          location="", label="", suffix="") -> str:
    """One operator-session row.

        09:03:24 | Konstantin Kostin | Chair building | 00:48:21 (2901.6s) | Workstream Menlo Office

    The leading time is when the RECORDING STARTED, not when this line was
    written. Previously every session flushed in one polling batch shared that
    batch's timestamp, so a whole afternoon of work appeared to happen at
    13:03:01.
    """
    if start_unix:
        try:
            started = datetime.datetime.fromtimestamp(float(start_unix)).strftime("%H:%M:%S")
        except Exception:
            started = "--:--:--"
    else:
        started = "--:--:--"

    parts = [
        started,
        _field(operator),
        _field(task),
        f"{format_time(duration_s)} ({float(duration_s):.1f}s)",
    ]
    if location and location != "Unknown":
        parts.append(_field(location))
    if label:
        parts.append(_field(label))
    return " | ".join(parts) + suffix


def load_daily_totals():
    with log_lock:
        today_str = get_date_str()
        log_filename = recording_log_path(today_str)

        if today_str not in daily_totals:
            daily_totals[today_str] = {"total": 0, "by_pi": {}, "by_operator": {}}

        day_stats = daily_totals[today_str]
        if not os.path.exists(log_filename): return

        web_print(f"[{ts()}] DEBUG  Scanning {log_filename} to restore math history...")
        pattern = re.compile(r"Session Ended \| Pi:\s*(.*?)\s*\| Operator:\s*(.*?)\s*\|.*?Session Duration:\s*(.*)")
        try:
            with open(log_filename, "r") as f:
                for line in f:
                    match = pattern.search(line)
                    if match:
                        label, op, dur_str = match.group(1).strip(), match.group(2).strip(), match.group(3).strip()
                        dur = parse_duration_from_log(dur_str)
                        day_stats["total"] += dur
                        day_stats["by_pi"][label] = day_stats["by_pi"].get(label, 0) + dur
                        day_stats["by_operator"][op] = day_stats["by_operator"].get(op, 0) + dur
            web_print(f"[{ts()}] DEBUG  Successfully recovered history: {ANSI_LIGHT_PURPLE}{format_time(day_stats['total'])}{ANSI_RESET} fleet overall.")
        except Exception as e:
            web_print(f"[{ts()}] ERROR  Could not scan log file for history: {e}")

def fetch_and_log_tasks(hostname, label, fallback_op=None, fallback_task=None, fallback_dur=None):
    """Pulls this device's sessions from the mcap-sync API and logs any new ones from today."""
    today_str = get_date_str()
    op_filename = operator_log_path(today_str)
    success = False
    groups = []

    try:
        groups = api_client.get_device_sessions(hostname)
        success = True
    except FleetAPIError as e:
        web_print(f"[{ts()}] {ANSI_YELLOW}WARN   Could not retrieve sessions for {label}: {e}{ANSI_RESET}")

    for rec in groups:
        if not session_is_today(rec, today_str): continue

        op   = clean_str(rec.get("operator"))
        task = clean_str(rec.get("task"))
        loc  = clean_str(rec.get("location") or rec.get("environment"), default="")
        dur  = float(rec.get("duration_s") or 0)

        session_id = rec.get("id") or rec.get("session_uuid") or f"{label}|{rec.get('name')}|{op}|{task}|{dur:.0f}"
        if session_id in logged_session_ids: continue
        logged_session_ids.add(session_id)

        with log_lock:
            with open(op_filename, "a") as f:
                f.write(operator_session_line(
                    _session_start_unix(rec), op, task, dur, loc, label) + "\n")

    if not success and fallback_dur is not None:
        op   = clean_str(fallback_op)
        task = clean_str(fallback_task)
        session_id = f"{label}|Fallback|{op}|{task}|{fallback_dur:.0f}"
        if session_id not in logged_session_ids:
            logged_session_ids.add(session_id)
            with log_lock:
                with open(op_filename, "a") as f:
                    # No API record, so no true start time — derive it from the
                    # duration we observed rather than stamping "now".
                    f.write(operator_session_line(
                        time.time() - fallback_dur, op, task, fallback_dur,
                        "", label, suffix=" [fallback]") + "\n")

def poll_all_device_tasks():
    with log_lock: snapshot = dict(device_cache)
    if not snapshot: return
    web_print(f"[{ts()}] INFO   Refreshing completed tasks for {len(snapshot)} device(s)...")
    threads = []
    for hostname, d in snapshot.items():
        label = device_label(d)
        t = threading.Thread(target=fetch_and_log_tasks, args=(hostname, label), daemon=True)
        threads.append(t)
        t.start()
    for t in threads: t.join(timeout=10)

def sync_all_tasks(devices):
    web_print(f"[{ts()}] INFO   Syncing operator sessions from completed tasks...")
    threads = []
    for d in devices:
        hostname = d.get("hostname")
        label    = device_label(d)
        t = threading.Thread(target=fetch_and_log_tasks, args=(hostname, label), daemon=True)
        threads.append(t)
        t.start()
    for t in threads: t.join(timeout=15)

def log_session_end(label: str, op: str, task: str, dur: float):
    op, task = clean_str(op), clean_str(task)
    with log_lock:
        today_str = get_date_str()
        if today_str not in daily_totals:
            daily_totals[today_str] = {"total": 0, "by_pi": {}, "by_operator": {}}

        day_stats = daily_totals[today_str]
        day_stats["total"] += dur
        day_stats["by_pi"][label] = day_stats["by_pi"].get(label, 0) + dur
        day_stats["by_operator"][op] = day_stats["by_operator"].get(op, 0) + dur

        log_filename = recording_log_path(today_str)
        try:
            with open(log_filename, "a") as f:
                f.write(f"[{ts()}] Session Ended | Pi: {label:<15} | Operator: {op:<15} | Task: {task:<15} | Session Duration: {format_time(dur)} ({dur:.2f}s)\n")
                f.write(f"[{ts()}] --- Daily Running Totals ---\n")
                f.write(f"[{ts()}] Overall Fleet: {format_time(day_stats['total'])}\n")
                f.write(f"[{ts()}] Pi ({label}): {format_time(day_stats['by_pi'][label])}\n")
                f.write(f"[{ts()}] Operator ({op}): {format_time(day_stats['by_operator'][op])}\n\n")
        except Exception as e:
            web_print(f"[{ts()}] ERROR  Could not write to {log_filename}: {e}")

def log_current_totals(reason: str):
    poll_all_device_tasks()

    with log_lock:
        today_str = get_date_str()
        if today_str not in daily_totals:
            daily_totals[today_str] = {"total": 0, "by_pi": {}, "by_operator": {}}

        day_stats = daily_totals[today_str]
        snap_total = day_stats["total"]
        snap_by_pi = dict(day_stats["by_pi"])
        snap_by_op = dict(day_stats["by_operator"])

        for host, info in recording_cache.items():
            dur = info.get("duration", 0)
            op = info.get("operator", "Unknown")
            label = info.get("label", host)
            snap_total += dur
            snap_by_pi[label] = snap_by_pi.get(label, 0) + dur
            snap_by_op[op] = snap_by_op.get(op, 0) + dur

        day_stats["ui_live_pi"] = snap_by_pi
        day_stats["ui_live_operator"] = snap_by_op

        log_filename = recording_log_path(today_str)
        try:
            with open(log_filename, "a") as f:
                f.write(f"[{ts()}] === Snapshot Triggered By: {reason} ===\n")
                f.write(f"[{ts()}] Overall Fleet: {format_time(snap_total)}\n")
                if snap_by_pi:
                    f.write(f"[{ts()}] --- By Pi ---\n")
                    for p, d in sorted(snap_by_pi.items()): f.write(f"[{ts()}]   {p:<15}: {format_time(d)}\n")
                if snap_by_op:
                    f.write(f"[{ts()}] --- By Operator ---\n")
                    for o, d in sorted(snap_by_op.items()): f.write(f"[{ts()}]   {o:<15}: {format_time(d)}\n")
                f.write("\n")
            web_print(f"[{ts()}] INFO   Wrote snapshot to {log_filename} ({reason})")
        except Exception as e:
            web_print(f"[{ts()}] ERROR  Could not write to {log_filename}: {e}")

def session_is_today(rec: dict, today_str: str) -> bool:
    start_unix = rec.get("start_time_unix") or rec.get("mtime") or 0
    try:
        return start_unix > 0 and datetime.datetime.fromtimestamp(float(start_unix)).strftime("%Y-%m-%d") == today_str
    except Exception:
        return False

# ── Live rig status & critical alerts ────────────────────────────────────
RIG_CACHE_TTL = 5
_rig_cache: dict = {"ts": 0.0, "data": None}
_rig_lock = threading.Lock()
_active_alerts: dict[str, dict] = {}   # "hostname|kind" -> alert dict

def _pick_number(d: dict, keys) -> float | None:
    for k in keys:
        v = d.get(k)
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            return float(v)
    return None

def extract_frame_health(device: dict) -> float | None:
    """Frame health as a percentage, from whichever field the fleet status
    payload provides it under (device schemas vary between rig builds)."""
    v = _pick_number(device, ("frame_health", "frame_health_percent", "frame_health_pct",
                              "health_percent", "capture_health_percent", "frames_percent"))
    if v is not None:
        return v
    fs = device.get("frame_summary")
    if isinstance(fs, dict):
        return _pick_number(fs, ("completion_percent", "worst_camera_percent"))
    return None

def extract_storage(device: dict) -> tuple[float | None, float | None]:
    """Returns (free_gb, free_pct), None where the payload doesn't say."""
    GB = 1024 ** 3
    free_b  = _pick_number(device, ("disk_free_bytes", "storage_free_bytes", "free_bytes",
                                    "disk_available_bytes", "available_bytes"))
    total_b = _pick_number(device, ("disk_total_bytes", "storage_total_bytes", "total_bytes",
                                    "disk_size_bytes"))
    free_gb = _pick_number(device, ("disk_free_gb", "storage_free_gb", "free_gb"))
    if free_gb is None and free_b is not None:
        free_gb = free_b / GB
    free_pct = _pick_number(device, ("disk_free_percent", "storage_free_percent"))
    if free_pct is None:
        used_pct = _pick_number(device, ("disk_used_percent", "disk_usage_percent",
                                         "disk_percent", "storage_used_percent"))
        if used_pct is not None:
            free_pct = 100.0 - used_pct
    if free_pct is None and free_b is not None and total_b:
        free_pct = free_b / total_b * 100.0
    return free_gb, free_pct

CPU_TEMP_WARN_C  = 75.0   # warn above this
CPU_TEMP_CRIT_C  = 85.0   # critical above this
SSD_TEMP_WARN_C  = 60.0
SSD_TEMP_CRIT_C  = 70.0

def extract_thermals(device: dict) -> dict:
    """Pulls CPU temp (°C), fan speed (RPM or %), and SSD/NVMe temp (°C)
    from whichever field names the rig firmware uses. All values optional."""
    cpu_c = _pick_number(device, (
        "cpu_temp_c", "cpu_temperature_c", "cpu_temperature", "cpu_temp",
        "soc_temp_c", "soc_temperature", "temperature_cpu", "core_temp_c",
    ))
    # Some payloads nest thermals under a sub-dict
    thermals = device.get("thermals") or device.get("thermal") or {}
    if isinstance(thermals, dict):
        if cpu_c is None:
            cpu_c = _pick_number(thermals, ("cpu", "cpu_c", "soc", "core"))
    # milli-celsius (common in Linux hwmon)
    if cpu_c is not None and cpu_c > 1000:
        cpu_c = cpu_c / 1000.0

    fan_rpm = _pick_number(device, (
        "fan_speed_rpm", "fan_rpm", "fan_speed", "fan1_rpm",
        "cooling_fan_rpm", "fan_tach",
    ))
    if fan_rpm is None and isinstance(thermals, dict):
        fan_rpm = _pick_number(thermals, ("fan_rpm", "fan", "fan1"))
    fan_pct = None
    if fan_rpm is None:
        fan_pct = _pick_number(device, ("fan_percent", "fan_speed_percent", "fan_duty_pct"))

    ssd_c = _pick_number(device, (
        "ssd_temp_c", "nvme_temp_c", "disk_temp_c", "storage_temp_c",
        "nvme_temperature", "ssd_temperature", "m2_temp_c",
    ))
    if ssd_c is None and isinstance(thermals, dict):
        ssd_c = _pick_number(thermals, ("ssd", "nvme", "disk", "storage"))
    if ssd_c is not None and ssd_c > 1000:
        ssd_c = ssd_c / 1000.0

    return {
        "cpu_temp_c":  round(cpu_c, 1)  if cpu_c  is not None else None,
        "fan_rpm":     round(fan_rpm)   if fan_rpm is not None else None,
        "fan_pct":     round(fan_pct,1) if fan_pct is not None else None,
        "ssd_temp_c":  round(ssd_c, 1) if ssd_c   is not None else None,
    }

def extract_upload_speed(device: dict) -> float | None:
    """Upload speed in bytes/sec from whichever field the rig firmware uses.
    Returns None if not available."""
    bps = _pick_number(device, (
        "upload_speed_bps", "upload_rate_bps", "upload_bytes_per_sec",
        "network_upload_bps", "transfer_rate_bps", "mcap_upload_bps",
        "sync_rate_bps", "uplink_bps",
    ))
    if bps is not None:
        return bps
    # MB/s field — convert
    mbps = _pick_number(device, (
        "upload_speed_mbps", "upload_rate_mbps", "transfer_rate_mbps",
        "sync_rate_mbps",
    ))
    if mbps is not None:
        return mbps * 1_000_000
    # nested network/sync sub-dict
    for sub_key in ("network", "sync", "transfer"):
        sub = device.get(sub_key)
        if isinstance(sub, dict):
            v = _pick_number(sub, ("upload_bps", "upload_rate", "rate_bps", "bps"))
            if v is not None:
                return v
    return None

def format_speed(bps: float | None) -> str | None:
    if bps is None:
        return None
    if bps >= 1_000_000:
        return f"{bps/1_000_000:.1f} MB/s"
    if bps >= 1_000:
        return f"{bps/1_000:.0f} KB/s"
    return f"{bps:.0f} B/s"

def evaluate_rigs(devices: list[dict]) -> tuple[list[dict], list[dict]]:
    """Normalizes fleet status devices into rig cards + critical alerts."""
    rigs, alerts = [], []
    for d in devices:
        hostname = d.get("hostname", "")
        label = device_label(d)
        status = get_status(d)
        online = status != "offline"
        # Offline rigs report zeroed/stale metrics — don't read health or
        # storage from them, and never alert on them.
        health = extract_frame_health(d) if online else None
        free_gb, free_pct = extract_storage(d) if online else (None, None)
        thermals = extract_thermals(d) if online else {"cpu_temp_c": None, "fan_rpm": None, "fan_pct": None, "ssd_temp_c": None}
        upload_bps = extract_upload_speed(d) if online else None
        dur = float(d.get("recording_duration_s") or 0)
        location = _device_locations.get(hostname, "")
        critical = []

        # ── Generate alerts ──────────────────────────────────────────────────
        if status == "recording" and health is not None and health < FRAME_HEALTH_MIN_PCT:
            if _should_alert(hostname, "frame_health"):
                critical.append("frame_health")
                alerts.append({"hostname": hostname, "rig": label, "kind": "frame_health",
                               "message": f"Frame health {health:.1f}% (below {FRAME_HEALTH_MIN_PCT:.0f}%)"})

        low_gb  = free_gb is not None and free_gb < STORAGE_MIN_FREE_GB
        low_pct = free_pct is not None and free_pct < STORAGE_MIN_FREE_PCT
        if low_gb or low_pct:
            if _should_alert(hostname, "storage"):
                if free_gb is not None and free_pct is not None:
                    detail = f"{free_gb:.0f} GB ({free_pct:.0f}%) free"
                elif free_gb is not None:
                    detail = f"{free_gb:.0f} GB free"
                else:
                    detail = f"{free_pct:.0f}% free"
                critical.append("storage")
                alerts.append({"hostname": hostname, "rig": label, "kind": "storage",
                               "message": "Storage running low: " + detail})

        cpu_c = thermals["cpu_temp_c"]
        ssd_c = thermals["ssd_temp_c"]
        if cpu_c is not None and cpu_c >= CPU_TEMP_CRIT_C:
            if _should_alert(hostname, "cpu_temp"):
                critical.append("cpu_temp")
                alerts.append({"hostname": hostname, "rig": label, "kind": "cpu_temp",
                               "message": f"CPU temp critical: {cpu_c:.1f}°C"})
        elif cpu_c is not None and cpu_c >= CPU_TEMP_WARN_C:
            if _should_alert(hostname, "cpu_temp_warn"):
                critical.append("cpu_temp_warn")
                alerts.append({"hostname": hostname, "rig": label, "kind": "cpu_temp_warn",
                               "message": f"CPU temp high: {cpu_c:.1f}°C"})
        if ssd_c is not None and ssd_c >= SSD_TEMP_CRIT_C:
            if _should_alert(hostname, "ssd_temp"):
                critical.append("ssd_temp")
                alerts.append({"hostname": hostname, "rig": label, "kind": "ssd_temp",
                               "message": f"SSD temp critical: {ssd_c:.1f}°C"})

        # Recording stopped alert
        if status in ("idle", "uploading") and hostname in recording_cache:
            if _should_alert(hostname, "recording_stopped"):
                alerts.append({"hostname": hostname, "rig": label, "kind": "recording_stopped",
                               "message": f"Pi stopped recording"})

        rigs.append({
            "hostname": hostname,
            "label": label,
            "status": status,
            "online": online,
            "operator": clean_str(d.get("operator")),
            "task": clean_str(d.get("task")),
            "location": location,
            "recording_duration_s": dur,
            "duration_label": format_time(dur),
            "frame_health_pct": health,
            "storage_free_gb": free_gb,
            "storage_free_pct": free_pct,
            "cpu_temp_c": thermals["cpu_temp_c"],
            "fan_rpm": thermals["fan_rpm"],
            "fan_pct": thermals["fan_pct"],
            "ssd_temp_c": thermals["ssd_temp_c"],
            "upload_bps": upload_bps,
            "upload_speed_label": format_speed(upload_bps),
            "critical": critical,
        })
    return rigs, alerts

def process_alert_transitions(alerts: list[dict]):
    """Logs newly critical conditions in bright red (once per incident) and
    logs recovery in green when they clear."""
    current = {f"{a['hostname']}|{a['kind']}": a for a in alerts}
    with _rig_lock:
        new_keys = [k for k in current if k not in _active_alerts]
        cleared = [(k, _active_alerts[k]) for k in list(_active_alerts) if k not in current]
        for k in list(_active_alerts):
            if k not in current:
                del _active_alerts[k]
        for k in new_keys:
            _active_alerts[k] = current[k]
    for k in new_keys:
        a = current[k]
        web_print(f"{ANSI_URGENT_FMT}[{ts()}] CRITICAL {a['rig']}: {a['message']}{ANSI_RESET}")
    for k, a in cleared:
        web_print(f"[{ts()}] {ANSI_GREEN}RESOLVED {a['rig']}: cleared — {a['message']}{ANSI_RESET}")

def get_rig_status(client: FleetAPIClient, include_raw: bool = False) -> dict | None:
    with _rig_lock:
        cached = _rig_cache["data"]
        if not include_raw and cached is not None and time.time() - _rig_cache["ts"] < RIG_CACHE_TTL:
            return cached

    devices = client.get_fleet_status()
    if devices is None:
        with _rig_lock:
            return _rig_cache["data"]

    rigs, alerts = evaluate_rigs(devices)
    process_alert_transitions(alerts)
    data = {"rigs": rigs, "alerts": alerts}
    with _rig_lock:
        _rig_cache["ts"] = time.time()
        _rig_cache["data"] = data
    if include_raw:
        return {**data, "raw": devices}
    return data

# ── API-backed leaderboard ───────────────────────────────────────────────────
LEADERBOARD_CACHE_TTL = 20
_leaderboard_cache: dict = {"ts": 0.0, "data": None}
_leaderboard_lock = threading.Lock()

# Last good session_groups per device (trimmed to today's, essential fields
# only). Proxy fetches to individual Pis time out or 502 regularly
# (especially offline rigs), and dropping a device's completed sessions from
# one build makes operators' totals collapse — so failures fall back to the
# last data that device did return. Persisted to disk so restarts keep it.
_device_sessions_cache: dict[str, list[dict]] = {}

# Self-tracked recording time per device, as an idempotent running total —
# NOT a growing list of "finalized session" events. Re-observing the same
# ongoing recording (e.g. after a Pi flickers offline and reconnects) can
# only raise `cur_dur` via max(), it can never add a second entry for time
# already counted. A segment is only "banked" (permanently added) when the
# operator changes or the duration counter resets low, which is the actual
# signal that a *new* recording began. This is what prevents the previous
# design's bug where repeated offline/online flicker during one continuous
# recording caused the observed total to climb forever and never settle.
#   { hostname: {"date": "YYYY-MM-DD", "banked_s": float, "cur_dur": float,
#                "cur_op": str, "label": str} }
_observed_totals: dict[str, dict] = {}

# Prevents momentary drops when a Pi stops recording and the API hasn't
# committed the session yet (~20-second window). Cleared each new day.
_pi_total_floor: dict = {"day": "", "pi": {}, "op": {}}
_pi_floor_lock = threading.Lock()

# Only print a Leaderboard fetch WARN when a device's fetch state actually
# CHANGES (first failure, or recovery) — otherwise a chronically-502ing rig
# spams an identical warning every single leaderboard cycle (every few
# seconds), forever, drowning out everything else in the terminal.
_leaderboard_fetch_state: dict[str, str] = {}   # hostname -> "ok" | "failed_cached" | "failed_nocache"
_leaderboard_fetch_state_lock = threading.Lock()

# Backfill runs every 5 minutes; if the summary is identical to last run
# (same day count, same cache state, same failing devices) we don't want to
# reprint that every single run — only when the message actually changes.
_last_backfill_message: str = ""
_device_sessions_lock = threading.Lock()
LEADERBOARD_STATE_FILE = os.path.join(_CACHE_DIR, ".leaderboard_cache.json")

def _trim_groups(groups: list[dict], today_str: str) -> list[dict]:
    keep = ("id", "session_uuid", "name", "operator", "task", "duration_s",
            "start_time_unix", "mtime", "location", "environment")
    return [{k: g.get(k) for k in keep} for g in groups if session_is_today(g, today_str)]

def _load_leaderboard_state():
    try:
        with open(LEADERBOARD_STATE_FILE, "r") as f:
            st = json.load(f)
    except Exception:
        return
    if st.get("date") != get_date_str():
        return
    with _device_sessions_lock:
        _device_sessions_cache.update(st.get("device_sessions", {}))
        _observed_totals.update(st.get("observed_totals", {}))

def _save_leaderboard_state():
    with _device_sessions_lock:
        st = {"date": get_date_str(),
              "device_sessions": _device_sessions_cache,
              "observed_totals": _observed_totals}
    try:
        tmp = LEADERBOARD_STATE_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(st, f)
        os.replace(tmp, LEADERBOARD_STATE_FILE)
    except Exception:
        pass

def _observed_today_total(hostname: str, today_str: str) -> tuple[float, str]:
    """Returns (seconds, last_operator) this monitor has itself observed for
    the device today: banked (finalized) segments plus whatever the current
    segment has reached so far. Safe to call repeatedly — never inflates."""
    st = _observed_totals.get(hostname)
    if not st or st.get("date") != today_str:
        return 0.0, "Unknown"
    return st.get("banked_s", 0.0) + st.get("cur_dur", 0.0), st.get("cur_op") or "Unknown"

def _update_live_tracker(devices: list[dict], today_str: str):
    """Feeds every fleet-status poll into the idempotent per-device running
    total. Recording samples raise `cur_dur` via max() — repeated sampling of
    the SAME ongoing recording (including across brief offline blips) can
    never add extra time. A segment is only banked (permanently folded into
    the day's total) when the operator changes or the duration counter drops
    (a real new-session signal), or when the rig cleanly stops recording."""
    with _device_sessions_lock:
        for d in devices:
            hostname = d.get("hostname") or ""
            if not hostname: continue
            status = get_status(d)
            label = device_label(d)

            st = _observed_totals.get(hostname)
            if st is None or st.get("date") != today_str:
                st = {"date": today_str, "banked_s": 0.0, "cur_dur": 0.0, "cur_op": "", "label": label}
                _observed_totals[hostname] = st
            st["label"] = label

            if status == "recording":
                op  = clean_str(d.get("operator"))
                dur = float(d.get("recording_duration_s") or 0)
                if st["cur_op"] and (op != st["cur_op"] or dur < st["cur_dur"] - 30):
                    # Operator changed or counter reset -> a genuinely new
                    # session started; bank the previous segment's time.
                    st["banked_s"] += st["cur_dur"]
                    st["cur_dur"] = dur
                    st["cur_op"] = op
                else:
                    st["cur_op"] = op
                    st["cur_dur"] = max(st["cur_dur"], dur)   # idempotent — never double-adds
            elif status == "offline":
                # Freeze the current segment. No append, no growth — if the
                # rig reconnects still recording the same session, the
                # "recording" branch above will simply raise cur_dur again.
                pass
            else:
                # idle/uploading -> the current segment (if any) ended cleanly
                if st["cur_dur"] > 0:
                    st["banked_s"] += st["cur_dur"]
                    st["cur_dur"] = 0.0
                    st["cur_op"] = ""


def build_api_leaderboard(client: FleetAPIClient) -> dict | None:
    """Aggregates today's completed sessions straight from the mcap-sync API
    (plus live in-progress recording time from fleet status) into per-Pi and
    per-operator duration rankings. Returns None if fleet status is
    unavailable (e.g. the client's upstream session expired)."""
    devices = client.get_fleet_status()
    if devices is None:
        return None

    today_str = get_date_str()
    by_pi: dict[str, float] = {}
    by_op: dict[str, float] = {}
    results: dict[str, tuple[str, list[dict]]] = {}

    # ── Tracks which Pis had a genuinely successful, fresh API fetch this ──────
    # cycle. Only THESE labels are allowed to reset the anti-drop floor below;
    # everyone else (offline / cache-fallback / observed-only) can only raise
    # it. Without this distinction, a single inflated estimate would get
    # baked into the floor forever and never be allowed to correct itself —
    # which is exactly what caused times to stay permanently double-counted.
    confirmed_labels: set[str] = set()
    confirmed_lock = threading.Lock()

    # ── Snapshot status + label ONCE per device for this whole cycle ───────────
    # get_status()/device_label() are called from several places below; if they
    # were re-evaluated at different points they could return a different
    # answer for the same device as time passes during the ~35s thread-join
    # (e.g. a staleness threshold ticking over), which let a single hostname
    # get classified two different ways in one cycle and be added twice. Every
    # later step reads from these two dicts instead of re-querying the device.
    device_status: dict[str, str] = {}
    device_lbl: dict[str, str] = {}
    for d in devices:
        hostname = d.get("hostname") or ""
        if not hostname:
            continue
        device_status[hostname] = get_status(d)
        device_lbl[hostname] = device_label(d)

    def fetch(hostname: str, label: str):
        try:
            groups = _trim_groups(client.get_device_sessions(hostname), today_str)
            with _device_sessions_lock:
                _device_sessions_cache[hostname] = groups
                # The device's own session list is authoritative again for
                # completed sessions — clear the banked (finalized) portion
                # of our self-tracked total so it doesn't linger and inflate
                # future merges. Keep cur_dur: a session still in progress
                # right now isn't in the API's completed list yet.
                st = _observed_totals.get(hostname)
                if st and st.get("date") == today_str:
                    st["banked_s"] = 0.0
            results[hostname] = (label, groups)
            with confirmed_lock:
                confirmed_labels.add(label)
            with _leaderboard_fetch_state_lock:
                prev_state = _leaderboard_fetch_state.get(hostname)
                if prev_state and prev_state != "ok":
                    web_print(f"[{ts()}] {ANSI_GREEN}INFO   Leaderboard: {label} recovered — session fetch succeeded.{ANSI_RESET}")
                _leaderboard_fetch_state[hostname] = "ok"
        except FleetAPIError as e:
            with _device_sessions_lock:
                cached = _device_sessions_cache.get(hostname)
            new_state = "failed_cached" if cached is not None else "failed_nocache"
            with _leaderboard_fetch_state_lock:
                prev_state = _leaderboard_fetch_state.get(hostname)
                should_print = prev_state != new_state
                _leaderboard_fetch_state[hostname] = new_state
            if cached is not None:
                results[hostname] = (label, cached)
                if should_print:
                    web_print(f"[{ts()}] {ANSI_YELLOW}WARN   Leaderboard: {label} fetch failed, using last known sessions ({e}){ANSI_RESET}")
            else:
                if should_print:
                    web_print(f"[{ts()}] {ANSI_YELLOW}WARN   Leaderboard: no session data for {label} yet ({e}){ANSI_RESET}")

    threads = []
    for hostname, label in device_lbl.items():
        if device_status[hostname] == "offline":
            # The proxy to an offline rig always fails — skip the network
            # round-trip and serve whatever it last reported.
            with _device_sessions_lock:
                cached = _device_sessions_cache.get(hostname)
            if cached is not None:
                results[hostname] = (label, cached)
            continue
        t = threading.Thread(target=fetch, args=(hostname, label), daemon=True)
        threads.append(t)
        t.start()
    for t in threads: t.join(timeout=35)

    _update_live_tracker(devices, today_str)

    seen_ids: set[str] = set()
    confirmed_ops: set[str] = set()
    for label, groups in results.values():
        for rec in groups:
            if not session_is_today(rec, today_str): continue
            session_id = rec.get("id") or rec.get("session_uuid") or f"{label}|{rec.get('name')}"
            if session_id in seen_ids: continue
            seen_ids.add(session_id)
            dur = float(rec.get("duration_s") or 0)
            op = clean_str(rec.get("operator"))
            by_pi[label] = by_pi.get(label, 0.0) + dur
            by_op[op] = by_op.get(op, 0.0) + dur
            if label in confirmed_labels:
                confirmed_ops.add(op)

    # ── Single merge point for observed/self-tracked session time ──────────────
    # Covers BOTH: (a) devices with no reachable session list at all this
    # cycle, and (b) devices whose API list is fetchable but hasn't ingested
    # the in-progress/just-finished session yet. Exactly one pass, one hostname
    # touched once, using max() rather than an additive "top-up" — so a
    # hostname can never be counted from both its API total AND its observed
    # total; whichever is larger wins outright.
    with _device_sessions_lock:
        observed_hosts = list(_observed_totals.keys())
    for hostname in observed_hosts:
        label = device_lbl.get(hostname, hostname)
        st = _observed_totals.get(hostname)
        if not st or st.get("date") != today_str:
            continue
        # If this device is CURRENTLY recording, the "Live recordings" loop
        # below will separately add its current segment's duration fresh
        # from fleet status — so only fold in the BANKED (already-finalized)
        # portion here. Folding in cur_dur too would add the live segment
        # twice: once here, once in the live loop. For devices that aren't
        # currently recording (offline/idle), nothing else will touch them,
        # so the full banked_s + cur_dur estimate is used.
        is_live_now = device_status.get(hostname) == "recording"
        obs_dur = st.get("banked_s", 0.0) + (0.0 if is_live_now else st.get("cur_dur", 0.0))
        obs_op = st.get("cur_op") or "Unknown"
        if obs_dur <= 0:
            continue
        api_dur = by_pi.get(label, 0.0)
        if obs_dur > api_dur:
            # Move the whole Pi total up to the observed value rather than
            # adding a "gap" — arithmetically identical when done once, but
            # immune to double-adding if this hostname is ever visited twice.
            by_pi[label] = obs_dur
            by_op[obs_op] = by_op.get(obs_op, 0.0) + (obs_dur - api_dur)
            # This label's total no longer purely reflects a clean API fetch —
            # don't let it reset the floor as if it were fully confirmed.
            confirmed_labels.discard(label)
            confirmed_ops.discard(obs_op)

    _save_leaderboard_state()

    # Live recordings aren't in mcap-sync yet — add their running time.
    # Uses the SAME status snapshot taken at the top of this function, so a
    # device that was classified "offline" for fetch purposes can't also be
    # treated as "recording" here just because time passed during the fetch.
    for hostname, status in device_status.items():
        if status != "recording":
            continue
        # Guard: a hostname whose observed/committed session already covers
        # today (e.g. it's mid-TTL waiting to reconnect) is handled above —
        # only add live time for devices genuinely reporting live right now.
        d = next((dd for dd in devices if dd.get("hostname") == hostname), None)
        if d is None:
            continue
        dur = float(d.get("recording_duration_s") or 0)
        label = device_lbl[hostname]
        op = clean_str(d.get("operator"))
        by_pi[label] = by_pi.get(label, 0.0) + dur
        by_op[op] = by_op.get(op, 0.0) + dur

    # ── Floor: never let an ESTIMATED total drop compared to last cycle ────────
    # Clears at midnight so stale data from yesterday doesn't carry forward.
    # Confirmed labels/operators (backed by a clean, successful API fetch this
    # cycle) are trusted completely and RESET the floor to match, even if
    # that's lower than before — this is what lets a previously-inflated
    # estimate self-correct once the real API total comes in, instead of
    # being locked in permanently.
    with _pi_floor_lock:
        if _pi_total_floor["day"] != today_str:
            _pi_total_floor["day"] = today_str
            _pi_total_floor["pi"] = {}
            _pi_total_floor["op"] = {}
        for label, total in by_pi.items():
            if label in confirmed_labels:
                _pi_total_floor["pi"][label] = total   # trust fully, reset floor
                continue
            floor = _pi_total_floor["pi"].get(label, 0.0)
            if total < floor:
                by_pi[label] = floor          # hold the floor
            else:
                _pi_total_floor["pi"][label] = total   # raise the floor
        for op, total in by_op.items():
            if op in confirmed_ops:
                _pi_total_floor["op"][op] = total       # trust fully, reset floor
                continue
            floor = _pi_total_floor["op"].get(op, 0.0)
            if total < floor:
                by_op[op] = floor
            else:
                _pi_total_floor["op"][op] = total

    def ranked(source: dict[str, float], hide_unnamed: bool = False) -> list[dict]:
        return [
            {"name": k, "duration": format_time(v)}
            for k, v in sorted(source.items(), key=lambda x: x[1], reverse=True)
            if not (hide_unnamed and is_unnamed_pi(k))
        ]

    return {"pi": ranked(by_pi, hide_unnamed=True), "operator": ranked(by_op), "source": "api"}

def _leaderboard_refresher():
    """Rebuilds the leaderboard in the background. Also runs a full session
    backfill once per day (or on first boot) to populate historical hours."""
    _last_backfill: list[float] = [0.0]  # mutable cell: timestamp of last backfill

    while True:
        client = api_client or dashboard_auth.get_any_client()
        if client is None:
            time.sleep(3)
            continue
        try:
            data = build_api_leaderboard(client)
        except Exception as e:
            web_print(f"[{ts()}] ERROR  Leaderboard refresh failed: {e}")
            data = None
        if data is not None:
            with _leaderboard_lock:
                _leaderboard_cache["ts"] = time.time()
                _leaderboard_cache["data"] = data
        _flush_past_days_to_cache()

        # Backfill: run on startup, then every 5 minutes
        now = time.time()
        if now - _last_backfill[0] >= 300:
            try:
                threading.Thread(
                    target=backfill_daily_hours,
                    args=(client,),
                    daemon=True,
                    name="backfill"
                ).start()
            except Exception as e:
                web_print(f"[{ts()}] WARN   Backfill launch failed: {e}")
            _last_backfill[0] = now

        time.sleep(LEADERBOARD_CACHE_TTL)

def get_api_leaderboard() -> dict | None:
    with _leaderboard_lock:
        return _leaderboard_cache["data"]

def get_status(device: dict) -> str:
    if not device.get("online"): return "offline"
    cs = device.get("capture_state", "unknown")
    if cs == "recording": return "recording"
    if (device.get("upload_queue") or 0) > 0: return "uploading"
    return cs or "idle"

_UNNAMED_PI_RE = re.compile(r'^rpi\d*-[0-9a-f]{4}-[0-9a-f]{4}$', re.IGNORECASE)

def is_unnamed_pi(label: str) -> bool:
    """Returns True for default hostnames like rpi5-867a-7c29 that have no display name."""
    return bool(_UNNAMED_PI_RE.match(label))

def device_label(device: dict) -> str:
    return device.get("display_name") or device.get("hostname", "?")

def verify_on_close():
    web_print(f"[{ts()}] INFO   Starting verification of daily totals against the API...")
    today_str = get_date_str()
    log_filename = recording_log_path(today_str)

    local_sessions = {}
    if os.path.exists(log_filename):
        pattern = re.compile(r"Session Ended \| Pi:\s*(.*?)\s*\| Operator:\s*(.*?)\s*\|.*?Session Duration:\s*(.*)")
        with open(log_filename, "r") as f:
            for line in f:
                match = pattern.search(line)
                if match:
                    label = match.group(1).strip()
                    dur = parse_duration_from_log(match.group(3).strip())
                    if label not in local_sessions: local_sessions[label] = []
                    local_sessions[label].append(dur)

    correction_lines = []
    for hostname, d in list(device_cache.items()):
        label = device_label(d)
        local_sum = sum(local_sessions.get(label, []))
        api_day_sum = 0.0
        operator_contributions = {}

        try:
            groups = api_client.get_device_sessions(hostname)
        except FleetAPIError:
            continue

        for rec in groups:
            if session_is_today(rec, today_str):
                duration = float(rec.get("duration_s") or 0)
                api_day_sum += duration
                op_name = clean_str(rec.get("operator"))
                operator_contributions[op_name] = operator_contributions.get(op_name, 0.0) + duration

        diff = api_day_sum - local_sum
        if abs(diff) >= 5 and api_day_sum > 0:
            with log_lock:
                if today_str not in daily_totals: daily_totals[today_str] = {"total": 0, "by_pi": {}, "by_operator": {}}
                day_stats = daily_totals[today_str]
                old_pi_total = day_stats["by_pi"].get(label, 0)
                day_stats["by_pi"][label] = api_day_sum
                day_stats["total"] = day_stats["total"] - old_pi_total + api_day_sum
                for op_k, op_v in operator_contributions.items(): day_stats["by_operator"][op_k] = day_stats["by_operator"].get(op_k, 0) + op_v

            correction_lines.append(f"[{ts()}] CORRECTION | Pi: {label:<15} | Local Running Sum: {format_time(local_sum)} → API True Total: {format_time(api_day_sum)} (diff={format_time(diff)})\n")
            web_print(f"[{ts()}] OVERWRITE {label:<15} | True API Total: {format_time(api_day_sum)} | Local: {format_time(local_sum)} | Corrected")
        else:
            web_print(f"[{ts()}] VERIFY    {ANSI_GREEN}{label:<15}{ANSI_RESET} | True API Total: {format_time(api_day_sum)} | Local: {format_time(local_sum)} | Matches")

    if correction_lines:
        try:
            with open(log_filename, "a") as f:
                f.write(f"[{ts()}] === Consolidated Dashboard Validation Summary ===\n")
                f.writelines(correction_lines)
                f.write(f"[{ts()}] ===================================================\n\n")
        except Exception: pass

# ── Embedded Local Dashboard Server ──────────────────────────────────────────
def _send_json(handler: "EmbeddedUIServer", status: int, payload: dict):
    body = json.dumps(payload).encode()
    handler.send_response(status)
    handler.send_header('Content-Type', 'application/json')
    handler.send_header('Content-Length', str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)

class EmbeddedUIServer(BaseHTTPRequestHandler):
    def log_message(self, format, *args): return

    def end_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'X-Requested-With, Content-Type, Authorization, ngrok-skip-browser-warning')
        super().end_headers()

    def do_OPTIONS(self):
        self.send_response(200)
        self.end_headers()

    def _bearer_token(self):
        auth_header = self.headers.get('Authorization', '')
        if auth_header.startswith('Bearer '):
            return auth_header[len('Bearer '):].strip()
        return None

    def _require_auth(self):
        token = self._bearer_token()
        email = _check_auto_token(token) or dashboard_auth.check_session(token)
        if not email:
            _send_json(self, 401, {"error": "authentication required"})
            return None
        return email

    def _read_json_body(self) -> dict:
        try:
            length = int(self.headers.get('Content-Length', 0))
        except ValueError:
            length = 0
        if length <= 0:
            return {}
        try:
            return json.loads(self.rfile.read(length) or b'{}')
        except ValueError:
            return {}

    def do_GET(self):
        global loop_active
        path = urlparse(self.path).path

        if path == '/auth/status':
            email = dashboard_auth.check_session(self._bearer_token())
            return _send_json(self, 200, {"authenticated": bool(email), "email": email})

        if path == '/auth/auto-token':
            if _AUTO_TOKEN:
                return _send_json(self, 200, {"token": _AUTO_TOKEN, "email": _AUTO_EMAIL})
            return _send_json(self, 503, {"error": "python not yet authenticated"})

        if path == '/locations':
            if self._require_auth() is None:
                return
            return _send_json(self, 200, _device_locations)

        if path == '/task_history':
            if self._require_auth() is None:
                return
            with _task_history_lock:
                snap = dict(_completed_tasks)
            return _send_json(self, 200, snap)

        if path == '/weekend_filter':
            qs = parse_qs(urlparse(self.path).query)
            date_str = (qs.get('date') or [''])[0]
            return _send_json(self, 200, {"is_weekend": is_weekend(date_str)})

        if path == '/test_notification':
            if self._require_auth() is None:
                return
            # Return test alert for frontend to display
            return _send_json(self, 200, {
                "alerts": [
                    {"hostname": "test-pi", "rig": "Test Pi (Critical)", "kind": "test_critical",
                     "message": "🔔 TEST ALERT — This is a test critical notification with sound"},
                    {"hostname": "test-pi-2", "rig": "Test Pi (Resolved)", "kind": "test_resolved",
                     "message": "✓ TEST RESOLVED — This is a test resolved notification"}
                ]
            })

        if path not in ('/logs', '/rankings', '/devices', '/start', '/stop', '/snapshot', '/stats', '/stats_range'):
            self.send_response(404); self.end_headers()
            return
        if self._require_auth() is None:
            return

        if path == '/logs':
            with buffer_lock: _send_json(self, 200, terminal_buffer)
        elif path == '/devices':
            client = dashboard_auth.get_client(self._bearer_token()) or api_client
            include_raw = 'raw' in parse_qs(urlparse(self.path).query)
            payload = get_rig_status(client, include_raw=include_raw) if client else None
            if payload is None:
                return _send_json(self, 503, {"error": "fleet status unavailable"})
            payload = dict(payload)
            payload["active"] = loop_active
            _send_json(self, 200, payload)
        elif path == '/rankings':
            leaderboard = get_api_leaderboard()
            if leaderboard is not None:
                leaderboard = dict(leaderboard)
                leaderboard["active"] = loop_active
                return _send_json(self, 200, leaderboard)

            today_str = get_date_str()
            with log_lock:
                stats = daily_totals.get(today_str, {"by_pi": {}, "by_operator": {}})
                pi_source = stats.get("ui_live_pi") if "ui_live_pi" in stats else stats.get("by_pi", {})
                op_source = stats.get("ui_live_operator") if "ui_live_operator" in stats else stats.get("by_operator", {})
                pi_rank = [{"name": k, "duration": format_time(v)} for k, v in sorted(pi_source.items(), key=lambda x: x[1], reverse=True)]
                op_rank = [{"name": k, "duration": format_time(v)} for k, v in sorted(op_source.items(), key=lambda x: x[1], reverse=True)]
            _send_json(self, 200, {"pi": pi_rank, "operator": op_rank, "active": loop_active, "source": "local-logs"})
        elif path == '/start':
            loop_active = True
            web_print(f"[{ts()}] SYSTEM  Loop tracking manually STARTED via UI.")
            _send_json(self, 200, {"ok": True})
        elif path == '/stop':
            loop_active = False
            web_print(f"[{ts()}] SYSTEM  {ANSI_BRIGHT_RED}Loop tracking manually STOPPED via UI.{ANSI_RESET}")
            _send_json(self, 200, {"ok": True})
        elif path == '/snapshot':
            log_current_totals("Web UI Trigger")
            _flush_past_days_to_cache()
            _send_json(self, 200, {"ok": True})
        elif path == '/stats':
            today_str = get_date_str()
            with log_lock:
                today_stats = daily_totals.get(today_str, {"total": 0, "by_pi": {}, "by_operator": {}})
                by_pi = {k: v for k, v in (today_stats.get("ui_live_pi") or today_stats.get("by_pi", {})).items()
                         if not is_unnamed_pi(k)}
                total_s = sum(by_pi.values())
                # Filter out weekends from daily totals
                merged_days: dict[str, float] = dict(_daily_hours_cache)
                for day in sorted(daily_totals.keys()):
                    if day == today_str or is_weekend(day):
                        continue
                    day_data = daily_totals[day]
                    src_pi = day_data.get("ui_live_pi") or day_data.get("by_pi", {})
                    day_s  = sum(v for k, v in src_pi.items() if not is_unnamed_pi(k)) if src_pi else 0
                    if day_s > merged_days.get(day, 0):
                        merged_days[day] = day_s
                merged_days[today_str] = total_s
                hours_by_day = [{"date": d, "hours": round(s / 3600, 3)}
                                 for d, s in sorted(merged_days.items())]
            with _rig_lock:
                dev_data = _rig_cache.get("data") or {}
            rigs_now = dev_data.get("rigs", [])
            fh_live  = [r["frame_health_pct"] for r in rigs_now
                        if r.get("frame_health_pct") is not None and r.get("online")]
            with _health_lock:
                history = list(_HEALTH_HISTORY)
            if fh_live:
                avg_fh = round(sum(fh_live) / len(fh_live), 2)
                min_fh = round(min(fh_live), 2)
            elif history:
                avg_fh = history[-1]["avg"]
                min_fh = history[-1]["min"]
            else:
                avg_fh = None
                min_fh = None
            _send_json(self, 200, {
                "total_hours_today":    round(total_s / 3600, 3),
                "avg_frame_health_pct": avg_fh,
                "min_frame_health_pct": min_fh,
                "active_recording_rigs": sum(1 for r in rigs_now if r.get("status") == "recording"),
                "total_rigs": len(rigs_now),
                "hours_by_day": hours_by_day,
                "frame_health_history": history[-360:],
                "last_updated_ms": int(time.time() * 1000),
            })
        elif path == '/stats_range':
            qs = parse_qs(urlparse(self.path).query)
            days_param = (qs.get('days') or [''])[0]
            days = sorted({d.strip() for d in days_param.split(',') if d.strip() and not is_weekend(d.strip())})
            if not days:
                days = [get_date_str()]

            today_str = get_date_str()
            by_pi: dict[str, float] = {}
            by_op: dict[str, float] = {}

            if today_str in days:
                board = get_api_leaderboard()
                if board:
                    for p in board.get("pi", []):
                        by_pi[p["name"]] = by_pi.get(p["name"], 0.0) + parse_duration_from_log(p["duration"])
                    for o in board.get("operator", []):
                        by_op[o["name"]] = by_op.get(o["name"], 0.0) + parse_duration_from_log(o["duration"])
                else:
                    with log_lock:
                        stats = daily_totals.get(today_str, {"by_pi": {}, "by_operator": {}})
                        for k, v in (stats.get("ui_live_pi") or stats.get("by_pi", {})).items():
                            if not is_unnamed_pi(k):
                                by_pi[k] = by_pi.get(k, 0.0) + v
                        for k, v in (stats.get("ui_live_operator") or stats.get("by_operator", {})).items():
                            by_op[k] = by_op.get(k, 0.0) + v

            past_days = [d for d in days if d != today_str]
            if past_days:
                with _pi_session_cache_lock:
                    cache_snapshot = {h: dict(v) for h, v in _pi_session_cache.items()}
                for hostname, entry in cache_snapshot.items():
                    label = entry.get("label", hostname)
                    if is_unnamed_pi(label):
                        continue
                    for sess in entry.get("sessions", {}).values():
                        if sess.get("date") not in past_days:
                            continue
                        # Byte-implied duration wherever it disagrees with the
                        # API's wall clock, so a hung session can't rank a rig
                        # or an operator above one that actually recorded.
                        dur, _api_dur = session_durations(hostname, sess)
                        if dur <= 0:
                            continue
                        op = sess.get("operator") or "Unknown"
                        by_pi[label] = by_pi.get(label, 0.0) + dur
                        by_op[op] = by_op.get(op, 0.0) + dur

            total_s = sum(by_pi.values())

            def ranked(source: dict[str, float]) -> list[dict]:
                return [{"name": k, "duration": format_time(v), "hours": round(v / 3600, 3)}
                        for k, v in sorted(source.items(), key=lambda x: x[1], reverse=True)]

            _send_json(self, 200, {
                "days": days,
                "total_hours": round(total_s / 3600, 3),
                "hours_by_pi": ranked(by_pi),
                "hours_by_operator": ranked(by_op),
            })

    def do_POST(self):
        path = urlparse(self.path).path
        body = self._read_json_body()

        if path == '/auth/request-code':
            ok, err = dashboard_auth.request_code(body.get('email', ''))
            return _send_json(self, 200 if ok else 400, {"ok": ok, "error": err})

        if path == '/auth/verify-code':
            token, err = dashboard_auth.verify_code(body.get('email', ''), body.get('code', ''))
            if token:
                return _send_json(self, 200, {"ok": True, "token": token})
            return _send_json(self, 400, {"ok": False, "error": err})

        if path == '/auth/logout':
            dashboard_auth.logout(self._bearer_token())
            return _send_json(self, 200, {"ok": True})

        if path == '/set_location':
            if self._require_auth() is None:
                return
            hostname = body.get('hostname', '')
            location = body.get('location', '')
            if hostname and location:
                _device_locations[hostname] = location
                _save_locations()
                return _send_json(self, 200, {"ok": True})
            return _send_json(self, 400, {"ok": False, "error": "missing hostname or location"})

        self.send_response(404); self.end_headers()

def start_web_server():
    _load_leaderboard_state()
    _load_locations()
    _load_task_history()
    server = HTTPServer(('127.0.0.1', WEB_PORT), EmbeddedUIServer)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    threading.Thread(target=_leaderboard_refresher, daemon=True).start()

def execute_otp_flow(email: str) -> bool:
    print(f"[{ts()}] INFO   Password login failed, falling back to email verification for: {email}...")
    ok, err = api_client.send_otp(email)
    if not ok:
        print(f"[{ts()}] FATAL  Failed to send verification code: {err}")
        return False

    print("\n" + "="*70)
    print(f" {ANSI_YELLOW}ATTENTION: Check your inbox for an email sent to {email}.{ANSI_RESET} ")
    otp_code = input(" Enter the 6-digit verification code: ").strip()
    print("="*70 + "\n")

    if not otp_code or len(otp_code) != 6:
        print(f"[{ts()}] FATAL  Verification code must be 6 digits.")
        return False

    ok, err = api_client.verify_otp(email, otp_code)
    if ok:
        _setup_auto_token(email)
        print(f"[{ts()}] SUCCESS Authenticated with fleet.shiftiq.us.")
        return True
    print(f"[{ts()}] FATAL  Verification rejected: {err}")
    return False

def login() -> bool:
    try:
        with open(os.path.join(_LOCAL_DIR, "secrets.json"), "r") as sf:
            secrets_data = json.load(sf)
            email = secrets_data.get("email")
            password = secrets_data.get("password")
    except Exception:
        print(f"[{ts()}] FATAL  Could not read local/secrets.json.")
        return False

    if not email:
        print(f"[{ts()}] FATAL  \"email\" is missing from local/secrets.json.")
        return False

    print(f"[{ts()}] DEBUG  Authenticating with {api_client.base_url}...")
    ok, err = api_client.login_password(email, password or "")
    if ok:
        _setup_auto_token(email)
        print(f"[{ts()}] SUCCESS Authenticated with fleet.shiftiq.us.")
        return True

    # A failed password no longer fires off a verification email on its own.
    # A typo used to mean an unwanted code landing in the inbox every attempt,
    # so the fallback is now opt-in.
    print(f"[{ts()}] {ANSI_YELLOW}WARN   Password login failed: {err}{ANSI_RESET}")

    if not sys.stdin.isatty():
        print(f"[{ts()}] FATAL  Check the password. To sign in with an emailed "
              f"code instead, run interactively or use the dashboard.")
        return False

    try:
        answer = input(f" Email a one-time code to {email} instead? [y/N]: ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        return False
    if answer not in ("y", "yes"):
        print(f"[{ts()}] FATAL  Password login failed and no code was requested.")
        return False
    return execute_otp_flow(email)

def poll() -> list[dict] | None:
    return api_client.get_fleet_status()

def start_key_listener():
    def _listener():
        if sys.platform == 'win32':
            import msvcrt
            while True:
                try:
                    key = msvcrt.getwch()
                    if key in ('~', '`'): log_current_totals("Tilde Key Pressed")
                    elif key == '\x03': import _thread; _thread.interrupt_main(); break
                except Exception: pass
        else:
            import tty, termios, _thread
            try:
                fd = sys.stdin.fileno()
                old_settings = termios.tcgetattr(fd)
            except Exception: return
            try:
                tty.setcbreak(fd)
                while True:
                    ch = sys.stdin.read(1)
                    if ch in ('~', '`'): log_current_totals("Tilde Key Pressed")
                    elif ch == '\x03': _thread.interrupt_main(); break
            except Exception: pass
            finally:
                try: termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
                except Exception: pass

    t = threading.Thread(target=_listener, daemon=True)
    t.start()

def run():
    global api_client
    api_client = FleetAPIClient()

    if not login():
        print(f"[{ts()}] FATAL  Could not authenticate with fleet.shiftiq.us. Exiting.")
        sys.exit(1)

    _load_daily_hours_cache()
    _load_pi_session_cache()
    # Byte rates off the restored cache, so requests served before the first
    # backfill finishes already count corrected durations.
    _rebuild_byte_rate_baselines()
    load_daily_totals()
    start_web_server()
    start_key_listener()
    web_print(f"[{ts()}] SYSTEM  Localhost-only dashboard API listening on port {WEB_PORT}.")

    prev_status: dict[str, str] = {}

    while True:
        time.sleep(POLL_INTERVAL)
        if not loop_active: continue

        devices = poll()
        if devices is None:
            if not login(): time.sleep(30)
            continue

        rigs, alerts = evaluate_rigs(devices)
        process_alert_transitions(alerts)
        _record_health_snapshot(rigs)

        for d in devices:
            hostname = d.get("hostname", "")
            device_cache[hostname] = d
            new_s = get_status(d)
            old_s = prev_status.get(hostname)
            label = device_label(d)
            was_recording = hostname in recording_cache

            if new_s == "recording":
                live_dur = float(d.get("recording_duration_s") or 0)
                prev_dur = recording_cache.get(hostname, {}).get("duration", 0)
                recording_cache[hostname] = {
                    "duration":       max(live_dur, 0),
                    "_prev_duration": prev_dur if prev_dur > live_dur else live_dur,
                    "operator":       clean_str(d.get("operator")),
                    "task":           clean_str(d.get("task")),
                    "label":          label,
                }

            if old_s is None:
                if was_recording and new_s not in ("recording", "offline"):
                    info = recording_cache.pop(hostname)
                    dur_to_commit = info["duration"] if info["duration"] >= 5 else info.get("_prev_duration", info["duration"])
                    log_session_end(label, info["operator"], info["task"], dur_to_commit)
                    _archive_completed_task(hostname, label, info["operator"], info["task"], dur_to_commit, time.time() - dur_to_commit)
                elif new_s == "recording":
                    web_print(f"{ANSI_REC}[{ts()}] CHANGE {label:<30}  Started | Op: {d.get('operator')} | Task: {d.get('task')}{ANSI_RESET}")
            elif new_s != old_s:
                if was_recording and new_s not in ("recording", "offline"):
                    info = recording_cache.pop(hostname)
                    dur_to_commit = info["duration"] if info["duration"] >= 5 else info.get("_prev_duration", info["duration"])
                    web_print(f"{ANSI_REC}[{ts()}] CHANGE {label:<30}  {old_s} → STOPPED | Op: {info['operator']} | Task: {info['task']} | {format_time(dur_to_commit)}{ANSI_RESET}")
                    log_session_end(label, info["operator"], info["task"], dur_to_commit)
                    _archive_completed_task(hostname, label, info["operator"], info["task"], dur_to_commit, time.time() - dur_to_commit)
                    threading.Thread(target=fetch_and_log_tasks, args=(hostname, label, info["operator"], info["task"], dur_to_commit)).start()
                elif new_s == "recording" and not was_recording:
                    web_print(f"{ANSI_REC}[{ts()}] CHANGE {label:<30}  {old_s} → RECORDING | Op: {d.get('operator')} | Task: {d.get('task')}{ANSI_RESET}")
            prev_status[hostname] = new_s

if __name__ == "__main__":
    try: run()
    except KeyboardInterrupt: pass
    finally:
        _flush_past_days_to_cache()
        _save_health_history()