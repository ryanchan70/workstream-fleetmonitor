#!/usr/bin/env python3
"""
fleet_monitor.py
Polls the fleet.shiftiq.us JSON API for device/session status and serves a
local-only dashboard (gated behind real email verification codes) that
streams terminal metrics and per-Pi/operator timing rankings.
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

from api_client import FleetAPIClient, FleetAPIError
from auth import DashboardAuth

# ── Config ────────────────────────────────────────────────────────────────────
POLL_INTERVAL = 5
WEB_PORT = 8080

# Critical alert thresholds
FRAME_HEALTH_MIN_PCT = 95.0   # frame health below this while recording => critical
STORAGE_MIN_FREE_PCT = 10.0   # free disk % below this => critical
STORAGE_MIN_FREE_GB  = 50.0   # free disk GB below this => critical

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

dashboard_auth = DashboardAuth()

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

def load_daily_totals():
    with log_lock:
        today_str = get_date_str()
        log_filename = f"daily_recording_log_{today_str}.txt"

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
    op_filename = f"operator_sessions_{today_str}.txt"
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

        loc_str = f" | Location: {loc:<20}" if loc and loc != "Unknown" else ""
        with log_lock:
            with open(op_filename, "a") as f:
                f.write(f"[{ts()}] Operator: {op:<20} | Pi: {label:<18} | Task: {task:<25}{loc_str} | Session Duration: {format_time(dur)} ({dur:.2f}s)\n")

    if not success and fallback_dur is not None:
        op   = clean_str(fallback_op)
        task = clean_str(fallback_task)
        session_id = f"{label}|Fallback|{op}|{task}|{fallback_dur:.0f}"
        if session_id not in logged_session_ids:
            logged_session_ids.add(session_id)
            with log_lock:
                with open(op_filename, "a") as f:
                    f.write(f"[{ts()}] Operator: {op:<20} | Pi: {label:<18} | Task: {task:<25} | Session Duration: {format_time(fallback_dur)} ({fallback_dur:.2f}s) [fallback]\n")

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

        log_filename = f"daily_recording_log_{today_str}.txt"
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

        log_filename = f"daily_recording_log_{today_str}.txt"
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

# ── Live rig status & critical alerts ────────────────────────────────────────
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
        dur = float(d.get("recording_duration_s") or 0)
        critical = []

        if status == "recording" and health is not None and health < FRAME_HEALTH_MIN_PCT:
            critical.append("frame_health")
            alerts.append({"hostname": hostname, "rig": label, "kind": "frame_health",
                           "message": f"Frame health {health:.1f}% (below {FRAME_HEALTH_MIN_PCT:.0f}%)"})

        low_gb  = free_gb is not None and free_gb < STORAGE_MIN_FREE_GB
        low_pct = free_pct is not None and free_pct < STORAGE_MIN_FREE_PCT
        if low_gb or low_pct:
            if free_gb is not None and free_pct is not None:
                detail = f"{free_gb:.0f} GB ({free_pct:.0f}%) free"
            elif free_gb is not None:
                detail = f"{free_gb:.0f} GB free"
            else:
                detail = f"{free_pct:.0f}% free"
            critical.append("storage")
            alerts.append({"hostname": hostname, "rig": label, "kind": "storage",
                           "message": "Storage running low: " + detail})

        rigs.append({
            "hostname": hostname,
            "label": label,
            "status": status,
            "online": online,
            "operator": clean_str(d.get("operator")),
            "task": clean_str(d.get("task")),
            "recording_duration_s": dur,
            "duration_label": format_time(dur),
            "frame_health_pct": health,
            "storage_free_gb": free_gb,
            "storage_free_pct": free_pct,
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
# Sessions this monitor observed itself via fleet-status transitions
# (recording -> stopped) for devices whose proxy is unreachable. Superseded
# and discarded once the device's own session list is fetchable again.
_observed_sessions: dict[str, list[dict]] = {}
_live_tracker: dict[str, dict] = {}   # hostname -> {op, dur, date, label}
_device_sessions_lock = threading.Lock()
LEADERBOARD_STATE_FILE = ".leaderboard_cache.json"   # git-ignored

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
        _observed_sessions.update(st.get("observed", {}))

def _save_leaderboard_state():
    with _device_sessions_lock:
        st = {"date": get_date_str(),
              "device_sessions": _device_sessions_cache,
              "observed": _observed_sessions}
    try:
        tmp = LEADERBOARD_STATE_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(st, f)
        os.replace(tmp, LEADERBOARD_STATE_FILE)
    except Exception:
        pass

def _update_live_tracker(devices: list[dict], today_str: str):
    """Watches fleet-status transitions so recording time isn't lost for
    devices whose session list can't be fetched. A session is 'finalized'
    into _observed_sessions when its rig stops recording (or its duration
    resets, meaning a new session started)."""
    with _device_sessions_lock:
        for d in devices:
            hostname = d.get("hostname") or ""
            if not hostname: continue
            status = get_status(d)
            trk = _live_tracker.get(hostname)
            if status == "recording":
                op = clean_str(d.get("operator"))
                dur = float(d.get("recording_duration_s") or 0)
                if trk and (op != trk["op"] or dur < trk["dur"] - 30):
                    _observed_sessions.setdefault(hostname, []).append(
                        {"operator": trk["op"], "duration_s": trk["dur"], "date": trk["date"]})
                    trk = None
                _live_tracker[hostname] = {"op": op, "dur": max(dur, trk["dur"]) if trk else dur,
                                           "date": today_str, "label": device_label(d)}
            elif status == "offline":
                pass  # keep the tracker — the rig may reconnect mid-session
            elif trk:
                _observed_sessions.setdefault(hostname, []).append(
                    {"operator": trk["op"], "duration_s": trk["dur"], "date": trk["date"]})
                del _live_tracker[hostname]

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

    def fetch(hostname: str, label: str):
        try:
            groups = _trim_groups(client.get_device_sessions(hostname), today_str)
            with _device_sessions_lock:
                _device_sessions_cache[hostname] = groups
                # The device's own session list is authoritative again —
                # drop the transitions we tracked ourselves in the interim.
                _observed_sessions.pop(hostname, None)
            results[hostname] = (label, groups)
        except FleetAPIError as e:
            with _device_sessions_lock:
                cached = _device_sessions_cache.get(hostname)
            if cached is not None:
                results[hostname] = (label, cached)
                web_print(f"[{ts()}] {ANSI_YELLOW}WARN   Leaderboard: {label} fetch failed, using last known sessions ({e}){ANSI_RESET}")
            else:
                web_print(f"[{ts()}] {ANSI_YELLOW}WARN   Leaderboard: no session data for {label} yet ({e}){ANSI_RESET}")

    threads = []
    for d in devices:
        hostname = d.get("hostname")
        if not hostname: continue
        label = device_label(d)
        if get_status(d) == "offline":
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

    # Devices with no reachable session list at all: fall back to the
    # sessions this monitor observed itself via status transitions.
    with _device_sessions_lock:
        observed_snapshot = {h: list(v) for h, v in _observed_sessions.items()}
    for d in devices:
        hostname = d.get("hostname") or ""
        if not hostname or hostname in results: continue
        label = device_label(d)
        for obs in observed_snapshot.get(hostname, []):
            if obs.get("date") != today_str: continue
            dur = float(obs.get("duration_s") or 0)
            op = clean_str(obs.get("operator"))
            by_pi[label] = by_pi.get(label, 0.0) + dur
            by_op[op] = by_op.get(op, 0.0) + dur

    _save_leaderboard_state()

    # Live recordings aren't in mcap-sync yet — add their running time.
    for d in devices:
        if get_status(d) == "recording":
            dur = float(d.get("recording_duration_s") or 0)
            label = device_label(d)
            op = clean_str(d.get("operator"))
            by_pi[label] = by_pi.get(label, 0.0) + dur
            by_op[op] = by_op.get(op, 0.0) + dur

    def ranked(source: dict[str, float]) -> list[dict]:
        return [{"name": k, "duration": format_time(v)} for k, v in sorted(source.items(), key=lambda x: x[1], reverse=True)]

    return {"pi": ranked(by_pi), "operator": ranked(by_op), "source": "api"}

def _leaderboard_refresher():
    """Rebuilds the leaderboard in the background. The per-device fan-out can
    take tens of seconds when rigs are slow, and the embedded HTTP server is
    single-threaded — building inside a request handler would freeze the
    whole dashboard, so /rankings only ever serves the latest snapshot."""
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

def device_label(device: dict) -> str:
    return device.get("display_name") or device.get("hostname", "?")

def verify_on_close():
    web_print(f"[{ts()}] INFO   Starting verification of daily totals against the API...")
    today_str = get_date_str()
    log_filename = f"daily_recording_log_{today_str}.txt"

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
        email = dashboard_auth.check_session(self._bearer_token())
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

        if path not in ('/logs', '/rankings', '/devices', '/start', '/stop', '/snapshot'):
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
            # Serves the background refresher's latest snapshot; falls back
            # to the locally accumulated log totals if none exists yet.
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
            _send_json(self, 200, {"ok": True})

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

        self.send_response(404); self.end_headers()

def start_web_server():
    _load_leaderboard_state()
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
        print(f"[{ts()}] SUCCESS Authenticated with fleet.shiftiq.us.")
        return True
    print(f"[{ts()}] FATAL  Verification rejected: {err}")
    return False

def login() -> bool:
    try:
        with open("secrets.json", "r") as sf:
            secrets_data = json.load(sf)
            email = secrets_data.get("email")
            password = secrets_data.get("password")
    except Exception:
        print(f"[{ts()}] FATAL  Could not read secrets.json.")
        return False

    if not email:
        print(f"[{ts()}] FATAL  \"email\" is missing from secrets.json.")
        return False

    print(f"[{ts()}] DEBUG  Authenticating with {api_client.base_url}...")
    ok, err = api_client.login_password(email, password or "")
    if ok:
        print(f"[{ts()}] SUCCESS Authenticated with fleet.shiftiq.us.")
        return True

    print(f"[{ts()}] {ANSI_YELLOW}WARN   Password login failed: {err}{ANSI_RESET}")
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

        for d in devices:
            hostname = d.get("hostname", "")
            device_cache[hostname] = d
            new_s = get_status(d)
            old_s = prev_status.get(hostname)
            label = device_label(d)
            was_recording = hostname in recording_cache

            if new_s == "recording":
                recording_cache[hostname] = {"duration": d.get("recording_duration_s", 0), "operator": clean_str(d.get("operator")), "task": clean_str(d.get("task")), "label": label}

            if old_s is None:
                if was_recording and new_s not in ("recording", "offline"):
                    info = recording_cache.pop(hostname)
                    log_session_end(label, info["operator"], info["task"], info["duration"])
                elif new_s == "recording":
                    web_print(f"{ANSI_REC}[{ts()}] CHANGE {label:<30}  Started | Op: {d.get('operator')} | Task: {d.get('task')}{ANSI_RESET}")
            elif new_s != old_s:
                if was_recording and new_s not in ("recording", "offline"):
                    info = recording_cache.pop(hostname)
                    web_print(f"{ANSI_REC}[{ts()}] CHANGE {label:<30}  {old_s} → STOPPED | Op: {info['operator']}{ANSI_RESET}")
                    log_session_end(label, info["operator"], info["task"], info["duration"])
                    threading.Thread(target=fetch_and_log_tasks, args=(hostname, label, info["operator"], info["task"], info["duration"])).start()
                elif new_s == "recording" and not was_recording:
                    web_print(f"{ANSI_REC}[{ts()}] CHANGE {label:<30}  {old_s} → RECORDING | Op: {d.get('operator')}{ANSI_RESET}")
            prev_status[hostname] = new_s

if __name__ == "__main__":
    try: run()
    except KeyboardInterrupt: pass
