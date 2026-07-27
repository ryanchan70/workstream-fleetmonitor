"""
Pure logic ported unchanged from fleet_monitor.py.

Nothing in here touches Redis, the network, or the clock beyond datetime, so
it stays trivially testable — which matters more now that the rest of the
system is spread across serverless invocations.
"""

import datetime
import re

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

_UNNAMED_PI_RE = re.compile(r"^rpi\d*-[0-9a-f]{4}-[0-9a-f]{4}$", re.IGNORECASE)
_SESSION_NAME_RE = re.compile(r"^(\d{8})_(\d{6})")


# ── Small helpers ─────────────────────────────────────────────────────────
def ts():
    return datetime.datetime.now().strftime("%H:%M:%S")


def get_date_str():
    return datetime.datetime.now().strftime("%Y-%m-%d")


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


def device_label(device: dict) -> str:
    return device.get("display_name") or device.get("hostname", "?")


def get_status(device: dict) -> str:
    if not device.get("online"):
        return "offline"
    cs = device.get("capture_state", "unknown")
    if cs == "recording":
        return "recording"
    if (device.get("upload_queue") or 0) > 0:
        return "uploading"
    return cs or "idle"


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
            return int(datetime.datetime.strptime(
                m.group(1) + m.group(2), "%Y%m%d%H%M%S").timestamp())
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
            return datetime.datetime.fromtimestamp(su).strftime("%Y-%m-%d")
        except Exception:
            pass
    return None


def session_is_today(rec: dict, today_str: str) -> bool:
    su = session_start_unix(rec)
    if not su:
        return False
    try:
        return datetime.datetime.fromtimestamp(su).strftime("%Y-%m-%d") == today_str
    except Exception:
        return False


def task_label(rec: dict) -> str:
    """The human task name, never the raw session folder name."""
    t = clean_str(rec.get("task"), default="")
    if t and not _SESSION_NAME_RE.match(t):
        return t[:20]
    return "Untitled task"


# ── Device metric extraction ──────────────────────────────────────────────
def _pick_number(d: dict, keys):
    for k in keys:
        v = d.get(k)
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            return float(v)
    return None


def extract_frame_health(device: dict):
    v = _pick_number(device, (
        "frame_health", "frame_health_percent", "frame_health_pct",
        "health_percent", "capture_health_percent", "frames_percent"))
    if v is not None:
        return v
    fs = device.get("frame_summary")
    if isinstance(fs, dict):
        return _pick_number(fs, ("completion_percent", "worst_camera_percent"))
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
def evaluate_rigs(devices, locations=None, should_alert=None, prev_status=None):
    """Normalises fleet-status devices into rig cards plus critical alerts.

    `should_alert(hostname, kind) -> bool` is injected so the debounce can be
    backed by Redis instead of a process-local dict. When omitted every
    condition reports, which is what the tests want.
    """
    locations = locations or {}
    prev_status = prev_status or {}
    if should_alert is None:
        def should_alert(_h, _k):
            return True

    rigs, alerts = [], []
    for d in devices:
        hostname = d.get("hostname", "")
        label = device_label(d)
        status = get_status(d)
        online = status != "offline"

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
        if prev_status.get(hostname) == "recording" and status in ("idle", "uploading"):
            if should_alert(hostname, "recording_stopped"):
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


def ranked(source: dict, hide_unnamed: bool = False):
    return [
        {"name": k, "duration": format_time(v), "hours": round(v / 3600, 3)}
        for k, v in sorted(source.items(), key=lambda x: x[1], reverse=True)
        if not (hide_unnamed and is_unnamed_pi(k))
    ]
