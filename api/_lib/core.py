"""
Pure logic ported unchanged from fleet_monitor.py.

Nothing in here touches Redis, the network, or the clock beyond datetime, so
it stays trivially testable — which matters more now that the rest of the
system is spread across serverless invocations.
"""

import datetime
import os
import re
import statistics
import time

# ── Thresholds ────────────────────────────────────────────────────────────
FRAME_HEALTH_MIN_PCT = 95.0
STORAGE_MIN_FREE_PCT = 10.0
STORAGE_MIN_FREE_GB = 50.0
CPU_TEMP_WARN_C = 75.0
CPU_TEMP_CRIT_C = 85.0
SSD_TEMP_WARN_C = 60.0
SSD_TEMP_CRIT_C = 70.0

ALERT_DEBOUNCE_SEC = 900          # 15 minutes
BACKFILL_INTERVAL_SEC = 3600      # hourly, per the brief

# How long a rig keeps its servicing state after the fleet API stops
# reporting service_mode. Long enough to ride out the API's intermittent
# dropouts, short enough that a rig genuinely put back into service shows up
# as available within a couple of poll cycles.
SERVICE_STICKY_SEC = 180

_UNNAMED_PI_RE = re.compile(r"^rpi\d*-[0-9a-f]{4}-[0-9a-f]{4}$", re.IGNORECASE)
_SESSION_NAME_RE = re.compile(r"^(\d{8})_(\d{6})")


# ── Time zone ─────────────────────────────────────────────────────────────
# Every "today", "this weekend" and log timestamp in this system is a claim
# about the shift floor in California, not about the server.
#
# Serverless containers run in UTC, so datetime.now() rolled the date over at
# 17:00 PDT: from 5pm the whole fleet's totals reset to zero mid-shift, and
# is_weekend() started reporting Saturday, which silently dropped every
# Friday-evening hour from the daily chart.
#
# America/Los_Angeles rather than a fixed -8: "Pacific" is PDT for two thirds
# of the year, and pinning UTC-8 would put every summer timestamp an hour out.
try:
    from zoneinfo import ZoneInfo
    PACIFIC = ZoneInfo("America/Los_Angeles")
except Exception:              # tzdata missing from the runtime image
    PACIFIC = None


def _dst_bounds(year: int):
    """US DST window: 02:00 on the 2nd Sunday of March -> 1st Sunday of Nov."""
    mar = datetime.datetime(year, 3, 8)                   # 2nd Sunday is Mar 8-14
    start = mar + datetime.timedelta(days=(6 - mar.weekday()) % 7)
    nov = datetime.datetime(year, 11, 1)                  # 1st Sunday is Nov 1-7
    end = nov + datetime.timedelta(days=(6 - nov.weekday()) % 7)
    return start.replace(hour=2), end.replace(hour=2)


def _pacific_offset(utc_dt: datetime.datetime) -> datetime.timedelta:
    """Fallback for when zoneinfo has no tz database to read."""
    approx = utc_dt - datetime.timedelta(hours=8)
    start, end = _dst_bounds(approx.year)
    return datetime.timedelta(hours=-7 if start <= approx < end else -8)


def pacific_now() -> datetime.datetime:
    if PACIFIC is not None:
        return datetime.datetime.now(PACIFIC)
    utc = datetime.datetime.utcnow()
    return utc + _pacific_offset(utc)


def pacific_from_unix(epoch) -> datetime.datetime:
    if PACIFIC is not None:
        return datetime.datetime.fromtimestamp(float(epoch), PACIFIC)
    utc = datetime.datetime.utcfromtimestamp(float(epoch))
    return utc + _pacific_offset(utc)


# ── Small helpers ─────────────────────────────────────────────────────────
def ts():
    return pacific_now().strftime("%H:%M:%S")


def get_date_str():
    return pacific_now().strftime("%Y-%m-%d")


def format_time(seconds: float) -> str:
    sign = "-" if seconds < 0 else ""
    v = abs(float(seconds))
    return f"{sign}{int(v // 3600):02d}:{int((v % 3600) // 60):02d}:{int(v % 60):02d}"


def clean_str(val, default="Unknown"):
    if not val or str(val).strip() in ("", "None", "null", "—"):
        return default
    return str(val).strip()


def is_weekend(date_str: str) -> bool:
    try:
        return datetime.datetime.strptime(date_str, "%Y-%m-%d").weekday() >= 5
    except Exception:
        return False


def is_unnamed_pi(label: str) -> bool:
    return bool(_UNNAMED_PI_RE.match(label or ""))


# Hostnames arrive from the page as a path segment, and are pasted straight
# into an upstream URL. Anything outside this set is rejected before the call
# rather than escaped after it, so there is no argument to be had about
# whether "rpi5-..%2f..%2fadmin" survives one round of decoding.
_HOSTNAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")


def valid_hostname(host) -> bool:
    return bool(_HOSTNAME_RE.match(str(host or "")))


def device_label(device: dict) -> str:
    return device.get("display_name") or device.get("hostname", "?")


def service_info(device: dict):
    """The fleet API's own service-mode record, or None.

    `service_mode` carries who flagged it, the issue, free-text detail and a
    `flagged_at` unix timestamp. It is set on exactly the same devices the
    API reports as card_state == "servicing", so either field identifies the
    state; this one is used because it also carries the metadata.
    """
    m = device.get("service_mode")
    return m if isinstance(m, dict) and m else None


def get_status(device: dict) -> str:
    # Servicing outranks everything, including offline: a rig pulled for
    # repair is frequently offline, and reporting it as merely "offline"
    # loses the reason it is down. The API agrees — it reports card_state
    # "servicing" for those devices rather than "offline".
    if service_info(device) or device.get("card_state") == "servicing":
        return "servicing"
    if not device.get("online"):
        return "offline"
    cs = device.get("capture_state", "unknown")
    if cs == "recording":
        return "recording"
    if (device.get("upload_queue") or 0) > 0:
        return "uploading"
    # The API's "preview" and "idle" are the same thing to an operator: the
    # rig is up and not recording. They are reported as one state, "idle",
    # rather than two that need explaining.
    if cs in ("preview", "idle", "", None):
        return "idle"
    return cs


def parse_duration_from_log(duration_str: str) -> float:
    s = (duration_str or "").strip()
    m = re.search(r"\((\-?[\d\.]+)s\)", s)
    if m:
        return float(m.group(1))
    m = re.search(r"^(\-?[\d\.]+)s\s*\|", s)
    if m:
        return float(m.group(1))
    if s.endswith("s") and ":" not in s:
        try:
            return float(s[:-1])
        except ValueError:
            pass
    m = re.search(r"(\-?\d{1,2}):(\d{2}):([\d\.]+)", s)
    if m:
        h, mi, se = m.groups()
        sign = -1 if h.startswith("-") else 1
        return sign * (abs(float(h)) * 3600 + float(mi) * 60 + float(se))
    return 0.0


# ── Session time parsing ──────────────────────────────────────────────────
def session_start_unix(rec: dict):
    """Session start as a unix timestamp.

    Prefers the API's own field, then falls back to the session folder name
    (YYYYMMDD_HHMMSS) — often the only time info a backfilled session carries.
    """
    raw = rec.get("start_time_unix") or rec.get("mtime") or 0
    if raw:
        try:
            return int(float(raw))
        except Exception:
            pass
    m = _SESSION_NAME_RE.match(str(rec.get("name", "")))
    if m:
        try:
            # The Pi names its folders in local wall time. .timestamp() on a
            # naive datetime reads it in the *server's* zone, which on Vercel
            # is UTC — putting every name-derived session 7-8 hours out.
            naive = datetime.datetime.strptime(
                m.group(1) + m.group(2), "%Y%m%d%H%M%S")
            if PACIFIC is not None:
                return int(naive.replace(tzinfo=PACIFIC).timestamp())
            utc = naive - _pacific_offset(naive)
            return int(utc.replace(tzinfo=datetime.timezone.utc).timestamp())
        except Exception:
            pass
    return None


def session_date(rec: dict):
    name = str(rec.get("name", ""))
    if len(name) >= 8 and name[:8].isdigit():
        r = name[:8]
        return f"{r[:4]}-{r[4:6]}-{r[6:8]}"
    su = session_start_unix(rec)
    if su:
        try:
            return pacific_from_unix(su).strftime("%Y-%m-%d")
        except Exception:
            pass
    return None


def session_is_today(rec: dict, today_str: str) -> bool:
    su = session_start_unix(rec)
    if not su:
        return False
    try:
        return pacific_from_unix(su).strftime("%Y-%m-%d") == today_str
    except Exception:
        return False


def task_label(rec: dict) -> str:
    """The human task name, never the raw session folder name."""
    t = clean_str(rec.get("task"), default="")
    if t and not _SESSION_NAME_RE.match(t):
        return t[:20]
    return "Untitled task"


def is_test_task(task) -> bool:
    """True when a task name mentions a test, case-insensitively.

    Test sessions are real recordings — they burn disk and show up in the
    history — but they are not work, so the ranked totals leave them out while
    still listing them under the rig and the operator.

    A plain substring, so it also catches names with no word boundary to find:
    ls4test, IM4TEST, SMOKE_TEST_PR68_AUTOMATED. The cost of that breadth is
    that a genuine task whose name happens to contain the letters is excluded
    too — "LS4-1285 Filling Test Tubes" is the live example, 11 sessions and
    10.3 hours of real work. Name matching is the only signal available: the
    fleet API carries its own `test` boolean, and it is False on all 2215
    sessions in the sample, so it cannot be used for this.
    """
    return "test" in str(task or "").lower()


def feedback_id(entry: dict) -> str:
    """Stable id for a feedback submission.

    Submissions made before ids existed have none, and without one they can
    never be given a status — feedback.txt keys on it. Deriving one from the
    submission time is stable across reads and cannot collide with the random
    hex ids, which never start with a letter t.
    """
    fid = str((entry or {}).get("id") or "").strip()
    if fid:
        return fid
    try:
        return "t" + str(int(float(entry.get("at") or 0)))[-6:]
    except (TypeError, ValueError):
        return ""


# ── Completed-task deduplication ──────────────────────────────────────────
# A completed recording reaches us by two routes that describe the same real
# session:
#   poll()     records it the moment a rig stops, with the duration its
#              counter had reached and a start time inferred from that
#   backfill() records the fleet API's own session once it appears
# Neither key can be derived from the other, so they are matched on the
# interval they occupy instead.
TASK_HISTORY_MAX = 20             # per rig; the UI lists ten


def same_session(a: dict, b: dict) -> bool:
    """True if two task records describe one recording on one rig."""
    sa, da = float(a.get("start_time") or 0), float(a.get("duration_s") or 0)
    sb, db = float(b.get("start_time") or 0), float(b.get("duration_s") or 0)
    if not sa or not sb:
        return False
    shorter = min(da, db)
    if shorter <= 0:
        # No duration to compare: fall back to starts within two minutes.
        return abs(sa - sb) <= 120
    overlap = min(sa + da, sb + db) - max(sa, sb)
    return overlap > shorter * 0.5


def dedupe_tasks(entries):
    """Collapse records that describe the same recording.

    A rig cannot record two sessions at once, so any two records on one host
    that overlap in time are the same session seen twice. That covers the
    live/backfill pair above and also the occasional duplicate the fleet API
    itself returns (a session that was split or re-uploaded appears as both a
    long record and a shorter one nested inside it).

    Ordering decides which copy survives: API records before live ones — their
    duration is authoritative, where the live one is whatever the counter read
    at the moment we noticed the stop — then longest first, so a fragment can
    never displace the full session.
    """
    ordered = sorted(entries, key=lambda e: (e.get("src") == "live",
                                             -float(e.get("duration_s") or 0)))
    kept = []
    for e in ordered:
        if not any(same_session(e, k) for k in kept):
            kept.append(e)
    kept.sort(key=lambda e: e.get("start_time") or 0, reverse=True)
    return kept[:TASK_HISTORY_MAX]


# ── Device metric extraction ──────────────────────────────────────────────
def _pick_number(d: dict, keys):
    for k in keys:
        v = d.get(k)
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            return float(v)
    return None


def _dig(d, *path):
    cur = d
    for key in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur


def _first_frame_stats(device: dict, paths):
    """The first of `paths` that resolves to a dict carrying frame totals."""
    for path in paths:
        blob = _dig(device, *path)
        if isinstance(blob, dict) and blob.get("actual_total_frames") is not None:
            return blob
    return None


def extract_frame_health(device: dict):
    v = _pick_number(device, (
        "frame_health", "frame_health_percent", "frame_health_pct",
        "health_percent", "capture_health_percent", "frames_percent"))
    if v is not None:
        return v
    fs = device.get("frame_summary")
    if isinstance(fs, dict):
        v = _pick_number(fs, ("completion_percent", "worst_camera_percent"))
        if v is not None:
            return v
    # No rig reports a direct percentage field — fall back to the raw
    # actual/expected totals under recording_info / recording_frame_stats
    # (see extract_frame_counts) and derive one.
    counts = extract_frame_counts(device)
    captured, expected = counts["captured"], counts["expected"]
    if captured is not None and expected:
        return max(0.0, min(100.0, captured / expected * 100.0))
    return None


def extract_frame_counts(device: dict):
    """Frame health COUNTS (captured vs expected/dropped), when the payload
    reports them rather than only a percentage.

    Returns {"captured": int|None, "expected": int|None, "dropped": int|None}
    with any missing member derived from the other two where possible.
    """
    captured = _pick_number(device, (
        "frames_captured", "frame_count", "frames_written", "captured_frames",
        "frames_recorded", "frames_ok"))
    expected = _pick_number(device, (
        "frames_expected", "expected_frames", "frames_total", "total_frames"))
    dropped = _pick_number(device, (
        "frames_dropped", "dropped_frames", "frame_drops", "frames_missed",
        "missed_frames", "frames_lost"))

    fs = device.get("frame_summary")
    if isinstance(fs, dict):
        captured = captured if captured is not None else _pick_number(
            fs, ("captured", "frames_captured", "count"))
        expected = expected if expected is not None else _pick_number(
            fs, ("expected", "frames_expected", "total"))
        dropped = dropped if dropped is not None else _pick_number(
            fs, ("dropped", "frames_dropped", "missed"))

    # The fleet API's actual shape (confirmed from a live /api/status dump):
    # the whole-session totals sit at cameras.recording_info.recording_frame_
    # stats.{actual,expected}_total_frames, duplicated one level deeper under
    # ...recording_info.runner.recording_frame_stats. Per-camera role stats
    # (cameras.roles.<role>.recording_frame_stats) use "frames"/"expected_
    # frames" instead and are a different, narrower number — only the
    # whole-session totals belong here. Every plausible nesting is tried,
    # oldest/shallowest first, since which one a given rig build populates
    # has moved around.
    fstats = _first_frame_stats(device, (
        ("recording_frame_stats",),
        ("recording_info", "recording_frame_stats"),
        ("cameras", "recording_frame_stats"),
        ("cameras", "recording_info", "recording_frame_stats"),
        ("cameras", "recording_info", "runner", "recording_frame_stats"),
    ))
    if fstats:
        captured = captured if captured is not None else _pick_number(
            fstats, ("actual_total_frames",))
        expected = expected if expected is not None else _pick_number(
            fstats, ("expected_total_frames",))

    if expected is None and captured is not None and dropped is not None:
        expected = captured + dropped
    if dropped is None and captured is not None and expected is not None:
        dropped = max(0.0, expected - captured)

    return {
        "captured": int(captured) if captured is not None else None,
        "expected": int(expected) if expected is not None else None,
        "dropped": int(dropped) if dropped is not None else None,
    }


def extract_storage(device: dict):
    GB = 1024 ** 3
    free_b = _pick_number(device, (
        "disk_free_bytes", "storage_free_bytes", "free_bytes",
        "disk_available_bytes", "available_bytes"))
    total_b = _pick_number(device, (
        "disk_total_bytes", "storage_total_bytes", "total_bytes", "disk_size_bytes"))
    free_gb = _pick_number(device, ("disk_free_gb", "storage_free_gb", "free_gb"))
    if free_gb is None and free_b is not None:
        free_gb = free_b / GB
    free_pct = _pick_number(device, ("disk_free_percent", "storage_free_percent"))
    if free_pct is None:
        used = _pick_number(device, (
            "disk_used_percent", "disk_usage_percent", "disk_percent",
            "storage_used_percent"))
        if used is not None:
            free_pct = 100.0 - used
    if free_pct is None and free_b is not None and total_b:
        free_pct = free_b / total_b * 100.0
    return free_gb, free_pct


def extract_thermals(device: dict) -> dict:
    cpu_c = _pick_number(device, (
        "cpu_temp_c", "cpu_temperature_c", "cpu_temperature", "cpu_temp",
        "soc_temp_c", "soc_temperature", "temperature_cpu", "core_temp_c"))
    th = device.get("thermals") or device.get("thermal") or {}
    if isinstance(th, dict) and cpu_c is None:
        cpu_c = _pick_number(th, ("cpu", "cpu_c", "soc", "core"))
    if cpu_c is not None and cpu_c > 1000:
        cpu_c /= 1000.0

    fan_rpm = _pick_number(device, (
        "fan_speed_rpm", "fan_rpm", "fan_speed", "fan1_rpm",
        "cooling_fan_rpm", "fan_tach"))
    if fan_rpm is None and isinstance(th, dict):
        fan_rpm = _pick_number(th, ("fan_rpm", "fan", "fan1"))
    fan_pct = None
    if fan_rpm is None:
        fan_pct = _pick_number(device, ("fan_percent", "fan_speed_percent", "fan_duty_pct"))

    ssd_c = _pick_number(device, (
        "ssd_temp_c", "nvme_temp_c", "disk_temp_c", "storage_temp_c",
        "nvme_temperature", "ssd_temperature", "m2_temp_c"))
    if ssd_c is None and isinstance(th, dict):
        ssd_c = _pick_number(th, ("ssd", "nvme", "disk", "storage"))
    if ssd_c is not None and ssd_c > 1000:
        ssd_c /= 1000.0

    return {
        "cpu_temp_c": round(cpu_c, 1) if cpu_c is not None else None,
        "fan_rpm": round(fan_rpm) if fan_rpm is not None else None,
        "fan_pct": round(fan_pct, 1) if fan_pct is not None else None,
        "ssd_temp_c": round(ssd_c, 1) if ssd_c is not None else None,
    }


def extract_upload_speed(device: dict):
    bps = _pick_number(device, (
        "upload_speed_bps", "upload_rate_bps", "upload_bytes_per_sec",
        "network_upload_bps", "transfer_rate_bps", "mcap_upload_bps",
        "sync_rate_bps", "uplink_bps"))
    if bps is not None:
        return bps
    mbps = _pick_number(device, (
        "upload_speed_mbps", "upload_rate_mbps", "transfer_rate_mbps", "sync_rate_mbps"))
    if mbps is not None:
        return mbps * 1_000_000
    for k in ("network", "sync", "transfer"):
        sub = device.get(k)
        if isinstance(sub, dict):
            v = _pick_number(sub, ("upload_bps", "upload_rate", "rate_bps", "bps"))
            if v is not None:
                return v
    return None


def format_speed(bps):
    if bps is None:
        return None
    if bps >= 1_000_000:
        return f"{bps/1_000_000:.1f} MB/s"
    if bps >= 1_000:
        return f"{bps/1_000:.0f} KB/s"
    return f"{bps:.0f} B/s"


# ── Rig evaluation ────────────────────────────────────────────────────────
def evaluate_rigs(devices, locations=None, should_alert=None, prev_status=None,
                  service_memory=None, now=None):
    """Normalises fleet-status devices into rig cards plus critical alerts.

    `should_alert(hostname, kind) -> bool` is injected so the debounce can be
    backed by Redis instead of a process-local dict. When omitted every
    condition reports, which is what the tests want.

    Rigs the API has flagged into service mode report status "servicing" and
    raise no alerts — they are known-bad and already being worked on, so
    paging about the fault they were pulled for is pure noise.

    `service_memory` is {hostname: {"ts": epoch, "svc": {...}}} of the last
    time the API actually reported service mode. The fleet API intermittently
    drops `service_mode` for a poll or two and the rig briefly reports as
    preview/idle instead, which made a dozen rigs vanish out of Servicing and
    back every few seconds. Within SERVICE_STICKY_SEC of the last real report
    the previous service state is kept, and the rig is marked service_sticky
    so the caller knows not to refresh the timestamp from its own output —
    otherwise a rig genuinely returned to duty would stay servicing forever.
    """
    locations = locations or {}
    prev_status = prev_status or {}
    service_memory = service_memory or {}
    now = time.time() if now is None else now
    if should_alert is None:
        def should_alert(_h, _k):
            return True

    rigs, alerts = [], []
    for d in devices:
        hostname = d.get("hostname", "")
        label = device_label(d)
        status = get_status(d)
        svc = service_info(d)

        service_sticky = False
        if not svc:
            mem = service_memory.get(hostname)
            if isinstance(mem, dict) and (now - float(mem.get("ts") or 0)) <= SERVICE_STICKY_SEC:
                svc = mem.get("svc") or {}
                status = "servicing"
                service_sticky = True

        is_servicing = status == "servicing"
        # Read from the raw field, not from the status: a servicing rig is
        # very often offline, and treating it as online would surface stale
        # zeroed metrics as if they were live.
        online = bool(d.get("online"))

        # Offline rigs report stale/zeroed metrics — never read or alert on them.
        health = extract_frame_health(d) if online else None
        counts = extract_frame_counts(d) if online else {
            "captured": None, "expected": None, "dropped": None}
        free_gb, free_pct = extract_storage(d) if online else (None, None)
        thermals = extract_thermals(d) if online else {
            "cpu_temp_c": None, "fan_rpm": None, "fan_pct": None, "ssd_temp_c": None}
        upload_bps = extract_upload_speed(d) if online else None
        dur = float(d.get("recording_duration_s") or 0)
        critical = []

        def fire(kind, message):
            if is_servicing:
                return      # under maintenance: known-bad, must not page
            critical.append(kind)
            alerts.append({"hostname": hostname, "rig": label,
                           "kind": kind, "message": message})

        if status == "recording" and health is not None and health < FRAME_HEALTH_MIN_PCT:
            if should_alert(hostname, "frame_health"):
                fire("frame_health",
                     f"Frame health {health:.1f}% (below {FRAME_HEALTH_MIN_PCT:.0f}%)")

        low_gb = free_gb is not None and free_gb < STORAGE_MIN_FREE_GB
        low_pct = free_pct is not None and free_pct < STORAGE_MIN_FREE_PCT
        if low_gb or low_pct:
            if should_alert(hostname, "storage"):
                if free_gb is not None and free_pct is not None:
                    detail = f"{free_gb:.0f} GB ({free_pct:.0f}%) free"
                elif free_gb is not None:
                    detail = f"{free_gb:.0f} GB free"
                else:
                    detail = f"{free_pct:.0f}% free"
                fire("storage", "Storage running low: " + detail)

        cpu_c, ssd_c = thermals["cpu_temp_c"], thermals["ssd_temp_c"]
        if cpu_c is not None and cpu_c >= CPU_TEMP_CRIT_C:
            if should_alert(hostname, "cpu_temp"):
                fire("cpu_temp", f"CPU temp critical: {cpu_c:.1f}°C")
        elif cpu_c is not None and cpu_c >= CPU_TEMP_WARN_C:
            if should_alert(hostname, "cpu_temp_warn"):
                fire("cpu_temp_warn", f"CPU temp high: {cpu_c:.1f}°C")
        if ssd_c is not None and ssd_c >= SSD_TEMP_CRIT_C:
            if should_alert(hostname, "ssd_temp"):
                fire("ssd_temp", f"SSD temp critical: {ssd_c:.1f}°C")

        # Recording stopped: previous poll saw it recording, this one does not.
        # Taking a rig out of service stops its recording by definition, so
        # this transition must not fire either — hence is_servicing here as
        # well as inside fire().
        if (not is_servicing
                and prev_status.get(hostname) == "recording"
                and status in ("idle", "uploading")
                and should_alert(hostname, "recording_stopped")):
            alerts.append({"hostname": hostname, "rig": label,
                           "kind": "recording_stopped",
                           "message": "Pi stopped recording"})

        rigs.append({
            "hostname": hostname,
            "label": label,
            "status": status,
            "online": online,
            "operator": clean_str(d.get("operator")),
            "task": clean_str(d.get("task")),
            "location": locations.get(hostname, ""),
            # Why the rig is out of service, and since when, so the tile can
            # say "left wrist missing, flagged Jul 16" instead of just a badge.
            # True when this poll is holding the rig in servicing through a
            # gap in the API's reporting rather than being told to.
            "service_sticky": service_sticky,
            "service_issue": (svc or {}).get("issue") or "",
            "service_detail": (svc or {}).get("detail") or "",
            "service_by": (svc or {}).get("by") or "",
            "service_since": (svc or {}).get("flagged_at") or None,
            "recording_duration_s": dur,
            "duration_label": format_time(dur),
            "frame_health_pct": health,
            "frames_captured": counts["captured"],
            "frames_expected": counts["expected"],
            "frames_dropped": counts["dropped"],
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


# ── Byte-implied session duration ─────────────────────────────────────────
# A session's duration_s is wall clock: the span between the recording folder
# opening and closing. Usually that IS the recording, but a rig that hangs, or
# finalizes/uploads long after capture really stopped, leaves the span far
# longer than what actually got recorded — while the bytes on disk still
# reflect the real thing.
#
# The estimate reads the HEAD CAMERA ONLY, never the session total. The head
# unit (OAK-D-W) writes one fixed bundle into head_oakd.mcap — rgb 1080p30,
# depth, both mono streams and the IMU — so its output does not move when a
# wrist or chest camera is unplugged. Measured over 1961 sessions it runs at
# 4.28 MB/s with a p10-p90 spread of 0.7%, against 22.4% for the session
# total, whose swing is almost entirely camera count: a two-camera session
# writes 0.673 of a four-camera one, which a total-bytes baseline reads as a
# hung recording and "corrects" away.
#
# The head-only input and the least-squares fit still mirror
# categorize_tasks.py exactly, and changing either belongs in both. What no
# longer matches is which duration gets COUNTED: the dashboard now counts the
# estimate whenever it has one, while the report still counts the wall clock
# unless the estimate falls below DURATION_MISMATCH_RATIO of it. So the two
# agree on every corrected session and can differ by the model's residual —
# under a percent on the head unit — on ordinary ones.
#
# The estimate is the COUNTED duration whenever the head camera gives one,
# in both directions. It used to apply one way only — a high estimate was
# ignored as "the head wrote faster than its neighbours" — but that rule is
# what let a rig whose clock was 15 days slow report a two-hour recording as
# 374 hours: nothing else in the pipeline questions a wall clock, so a span
# the bytes cannot possibly account for was banked in full. The wall clock is
# now carried alongside rather than trusted, and the dashboard shows both.
#
# DURATION_MISMATCH_RATIO no longer gates what is counted here. It still gates
# categorize_tasks.py, whose report keeps the one-way rule — see the note on
# mirroring below.
#
# The model lives in Redis under "duration_model", rebuilt by each backfill,
# so a request can judge a session without re-reading the fleet's history.
MIN_TRAIN_DURATION_S = 120.0     # shorter sessions make noisy training points
DURATION_MISMATCH_RATIO = 0.75   # how far below wall clock the estimate must
                                 # fall before the wall clock is distrusted
# Least-squares fit of duration against head bytes, fleet-wide rather than per
# rig: the head unit is the same hardware writing the same streams on every rig
# and the measured spread across all of them is under a percent, so splitting
# by rig would only shrink the sample. The whole model is ONE number — a rate,
# with no offset — so it costs almost nothing to keep in Redis and read on a
# request.
TRIM_SIGMA = 3.0        # robust sigmas past which a residual is an outlier
TRIM_PASSES = 5         # refits before the trim is taken as settled
# Two points is what a straight line mathematically needs, and that is now the
# only bar. It used to be 30, which meant a fleet whose stored sessions had not
# yet accumulated 30 head-byte readings got an empty model — and an empty model
# is indistinguishable from "no estimate", so every duration silently fell back
# to the API's wall clock with nothing to show it had.
MIN_TRAIN_POINTS = 2

# Estimation is ON. It was opt-in while the model was being trusted, which is
# how two 370-hour sessions reached the ranked totals unchallenged; the whole
# point of the model is to catch exactly that, so it no longer waits to be
# asked. FLEET_DISABLE_ESTIMATION=y (or 1/true/yes) in the environment is the
# kill switch, and turning it off also removes the API figure the dashboard
# shows in parentheses — with nothing estimated there is nothing to compare.
DISABLE_ESTIMATION = os.environ.get(
    "FLEET_DISABLE_ESTIMATION", "n").strip().lower() in ("y", "yes", "true", "1")

# The head camera's files, by the role the API labels them with. Segmented
# recordings appear as head_oakd.seg1.mcap and friends, so names are matched on
# a prefix rather than equality.
#
# Deliberately the OAK unit ONLY. A handful of rigs carry a different head
# build that writes head_video/head_audio/head_stereo instead, and it runs at
# 1.36 MB/s against the OAK's 4.28 — measuring one against the other's rate
# would report a third of the real recording. Those sessions match nothing
# here, so they get no estimate at all, which is the right answer rather than
# a confident wrong one.
_HEAD_ROLE_PREFIXES = ("head_oakd",)
_HEAD_FILE_LISTS = ("mcap_files", "session_files", "files", "recordings")


def _positive_number(value):
    """The value as a positive float, or None. Booleans are not numbers here."""
    if value is None or isinstance(value, bool):
        return None
    try:
        n = float(value)
    except (TypeError, ValueError):
        return None
    return n if n > 0 else None


def extract_upload_state(rec: dict) -> str:
    """The session's upload state, lowercased, or "" if the API said nothing.

    The fleet API reports "uploaded" once every recording has landed,
    "uploading" while bytes are moving, and "waiting" when a recording is
    queued but the uploader has not started on it. Anything else it grows
    later is passed through as-is rather than forced into one of the three.

    Older fleet builds carry no upload_state at all. "" is what tells the
    dashboard to say nothing, rather than to claim a finished session is
    still going up — which is why the caller must not read "not uploaded"
    as "still uploading".
    """
    v = rec.get("upload_state") or rec.get("upload_label")
    return str(v or "").strip().lower()


UPLOAD_DONE = "uploaded"


def upload_pending(state: str) -> bool:
    """True while a session still owes bytes to the cloud.

    Deliberately not `state != UPLOAD_DONE`: "" means the API reported no
    state at all, and an unknown session must not be treated as one that is
    still going up — sessions swept in before upload_state was stored would
    otherwise all read as pending forever.
    """
    return bool(state) and state != UPLOAD_DONE


def extract_head_bytes(rec: dict):
    """Bytes written by the head camera for one session, or None.

    Per-file sizes out of mcap_files, never the session-level total — the
    total moves with camera count and that is exactly what this avoids. None
    means no head file was reported, and those sessions get no estimate rather
    than a guess from whatever else happened to be recording.
    """
    # First list that yields anything wins, rather than summing across all of
    # them: a rig reports the same head_oakd.mcap under both mcap_files and
    # session_files, and adding both counted every head recording twice.
    for sub in _HEAD_FILE_LISTS:
        items = rec.get(sub)
        if not isinstance(items, list):
            continue
        total = 0.0
        seen = set()
        for it in items:
            if not isinstance(it, dict):
                continue
            role = str(it.get("role") or "").lower()
            name = str(it.get("name") or "").lower()
            # No topic fallback: /head-camera/... is also what the other head
            # build publishes, and matching it would pull that rig's much
            # smaller files in under the OAK's rate.
            if not (role.startswith(_HEAD_ROLE_PREFIXES)
                    or name.startswith(_HEAD_ROLE_PREFIXES)):
                continue
            key = str(it.get("path") or it.get("name") or it.get("topic") or "")
            if key and key in seen:
                continue
            seen.add(key)
            total += _positive_number(it.get("size_bytes")) or 0.0
        if total > 0:
            return int(total)
    return None


def head_bytes_of(sess: dict):
    """The session's head-camera bytes, stored or derived, or None.

    The stored field is only ever written by the backfill. Everything else —
    today's live leaderboard, anything reading a session straight off the fleet
    API — was handing raw records to session_durations(), where `head_bytes`
    simply was not a key, so predict_duration() got None and the estimate
    silently never applied. Falling back to the per-file sizes on the record
    itself means a caller no longer has to know which of the two it is holding.
    """
    stored = _positive_number(sess.get("head_bytes"))
    if stored is not None:
        return stored
    return _positive_number(extract_head_bytes(sess))


def build_duration_model(sessions_by_host: dict) -> dict:
    """Fits the duration model over every stored session with head bytes."""
    pts = []
    for stored in (sessions_by_host or {}).values():
        for sess in (stored or {}).values():
            if not isinstance(sess, dict):
                continue
            dur = _positive_number(sess.get("duration_s"))
            head = head_bytes_of(sess)
            if dur is None or head is None or dur < MIN_TRAIN_DURATION_S:
                continue
            pts.append((head, dur))
    return _fit_trimmed(pts) or {}


def _median(xs):
    """Middle value of a non-empty list."""
    s = sorted(xs)
    n = len(s)
    return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2.0


def _head_rate(pts):
    """Seconds of recording per head byte: least squares through the origin.

    Through the origin deliberately. A free intercept is the one parameter a
    hung session can move without moving the slope, so that is where the
    contamination collected — and a bias parked in the intercept is added to
    every estimate in the fleet at full strength, however long the session.
    It is also unphysical: no bytes written is no recording. Fitted on clean
    data the intercept comes out at 0.6s, well inside the noise, so dropping
    the term costs nothing and removes the failure mode.
    """
    sxx = sum(b * b for b, _ in pts)
    if sxx <= 0:
        return None
    slope = sum(b * d for b, d in pts) / sxx
    return slope if slope > 0 else None


def _fit_trimmed(pts):
    """The head rate, refit until the outliers stop moving it.

    The outliers this model exists to FIND are high-leverage points in its own
    training data: a hung session pairs an ordinary byte count with an enormous
    wall clock. Trimming those on mean and standard deviation does not work,
    because the monsters ARE most of the standard deviation — on the live fleet
    the residual sd is 46,000s, so a 3-sigma cut lands at 39 HOURS and drops 2
    points out of 1653. Every bad session stays in and drags the fit.

    Median and MAD have no such feedback: both ignore the tail outright, so the
    cut is set by the sessions that behave rather than by the ones being looked
    for. Iterating tightens it as the worst points leave. On the live fleet it
    settles after four passes at 4.280 MB/s — the OAK head\'s known rate — with
    437 sessions set aside.
    """
    if len(pts) < MIN_TRAIN_POINTS:
        return None
    kept = list(pts)
    slope = _head_rate(kept)
    if slope is None:
        return None

    for _ in range(TRIM_PASSES):
        res = [d - slope * b for b, d in kept]
        mid = _median(res)
        mad = _median([abs(r - mid) for r in res])
        if mad <= 0:                       # the kept set already agrees exactly
            break
        cut = TRIM_SIGMA * 1.4826 * mad    # MAD -> sigma, for a normal core
        survivors = [p for p, r in zip(kept, res) if abs(r - mid) <= cut]
        if len(survivors) == len(kept) or len(survivors) < MIN_TRAIN_POINTS:
            break
        refit = _head_rate(survivors)
        if refit is None:
            break
        kept, slope = survivors, refit

    return {"slope": slope, "n": len(kept), "dropped": len(pts) - len(kept)}


def predict_duration(head_bytes, model: dict):
    """How long the head camera was recording, from its byte count."""
    if not head_bytes or not model:
        return None
    slope = model.get("slope")
    if not slope or slope <= 0:
        return None
    # No intercept term, deliberately — see _head_rate. An intercept left on a
    # model still sitting in Redis from before this changed is ignored rather
    # than applied: that stale +92s is the whole bug, and reading past it means
    # the fix lands on the next request instead of the next backfill.
    est = slope * float(head_bytes)
    return est if est > 0 else None


def session_durations(host: str, sess: dict, model: dict):
    """(duration to count, the API's own wall clock alongside it).

    The head-camera estimate is what counts whenever there is one, however far
    it lands from the span — see the note above on why the old one-way rule was
    dropped. The second element is the API's figure, returned whenever the API
    reported one, so callers can show both numbers and aggregate either.

    When the two are the same object the caller is looking at an unestimated
    session: no head file to measure, or estimation switched off. Those return
    the API duration as the counted value and None alongside it, so nothing
    downstream renders a number in parentheses against itself.

    `host` is unused by the fleet-wide model and kept in the signature so the
    call sites do not have to change if a per-rig fit is ever reintroduced.
    """
    api_dur = _positive_number(sess.get("duration_s"))
    if api_dur is None:
        return 0.0, None
    if DISABLE_ESTIMATION:
        return api_dur, None

    implied = predict_duration(head_bytes_of(sess), model)
    if implied is None:
        return api_dur, None
    return implied, api_dur


def ranked(source: dict, hide_unnamed: bool = False):
    return [
        {"name": k, "duration": format_time(v), "hours": round(v / 3600, 3)}
        for k, v in sorted(source.items(), key=lambda x: x[1], reverse=True)
        if not (hide_unnamed and is_unnamed_pi(k))
    ]
