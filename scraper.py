#!/usr/bin/env python3
"""
fleet_monitor.py
Logs into fleet.shiftiq.us and polls /api/fleet/status.
Includes an isolated local-only server to stream terminal metrics securely.
Features a re-activated CLI keyboard input listener and UI snapshot bindings.
"""

import json
import sys
import time
import datetime
import requests
import threading
import os
import re
from bs4 import BeautifulSoup
from http.server import BaseHTTPRequestHandler, HTTPServer

# ── Config ────────────────────────────────────────────────────────────────────
BASE_URL      = "https://fleet.shiftiq.us"
PASSWORD      = "workstream"
POLL_INTERVAL = 5          
WEB_PORT      = 8080       

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
scraped_sessions = set()  
log_lock = threading.Lock()
global_session = None

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
                        day_stats["by_operator"][op] = day_stats["by_operator"][op].get(op, 0) + dur
            web_print(f"[{ts()}] DEBUG  Successfully recovered history: {ANSI_LIGHT_PURPLE}{format_time(day_stats['total'])}{ANSI_RESET} fleet overall.")
        except Exception as e:
            web_print(f"[{ts()}] ERROR  Could not scan log file for history: {e}")

def fetch_and_log_tasks(http_session, hostname, label, fallback_op=None, fallback_task=None, fallback_dur=None):
    """Fetches individual task metrics from a single Pi device."""
    today_str = get_date_str()
    op_filename = f"operator_sessions_{today_str}.txt"
    today_prefix = today_str.replace("-", "")
    success = False

    recordings_url = f"{BASE_URL}/proxy/{hostname}/recordings?embed=1"
    try: 
        http_session.get(recordings_url, timeout=5)
    except requests.RequestException: 
        web_print(f"[{ts()}] {ANSI_YELLOW}WARN   Snapshot Timeout: {label} recordings page was unreachable.{ANSI_RESET}")
        if fallback_dur is None: return

    api_url = f"{BASE_URL}/proxy/{hostname}/statusboard-api/mcap-sync/sessions?light=1&limit=100"
    try:
        r = http_session.get(api_url, timeout=5)
        if r.ok:
            data = r.json()
            groups = data if isinstance(data, list) else (data.get("session_groups") or data.get("sessions") or [])

            for rec in groups:
                name = str(rec.get("name", ""))
                start_unix = rec.get("start_time_unix") or rec.get("mtime") or 0
                try:
                    from_unix_today = (start_unix > 0 and datetime.datetime.fromtimestamp(float(start_unix)).strftime("%Y-%m-%d") == today_str)
                except Exception: from_unix_today = False

                if not (name.startswith(today_prefix) or from_unix_today): continue

                op   = clean_str(rec.get("operator"))
                task = clean_str(rec.get("task"))
                loc  = clean_str(rec.get("location") or rec.get("environment"), default="")
                dur  = float(rec.get("duration_s") or 0)

                sig = f"{label}|{name}|{op}|{task}|{dur:.0f}"
                if sig not in scraped_sessions:
                    scraped_sessions.add(sig)
                    loc_str = f" | Location: {loc:<20}" if loc and loc != "Unknown" else ""
                    with log_lock:
                        with open(op_filename, "a") as f:
                            f.write(f"[{ts()}] Operator: {op:<20} | Pi: {label:<18} | Task: {task:<25}{loc_str} | Session Duration: {format_time(dur)} ({dur:.2f}s)\n")
            success = True
        else:
            web_print(f"[{ts()}] {ANSI_YELLOW}WARN   Snapshot Failed: {label} database API returned status {r.status_code}.{ANSI_RESET}")
    except requests.RequestException as e:
        web_print(f"[{ts()}] {ANSI_YELLOW}WARN   Snapshot Timeout: Could not retrieve target API records from {label} ({e}).{ANSI_RESET}")

    if not success and fallback_dur is not None:
        op   = clean_str(fallback_op)
        task = clean_str(fallback_task)
        sig  = f"{label}|Fallback|{op}|{task}|{fallback_dur:.0f}"
        if sig not in scraped_sessions:
            scraped_sessions.add(sig)
            with log_lock:
                with open(op_filename, "a") as f:
                    f.write(f"[{ts()}] Operator: {op:<20} | Pi: {label:<18} | Task: {task:<25} | Session Duration: {format_time(fallback_dur)} ({fallback_dur:.2f}s) [fallback]\n")

def scrape_all_device_tasks(http_session):
    with log_lock: snapshot = dict(device_cache)
    if not snapshot: return
    web_print(f"[{ts()}] INFO   Scraping Completed Tasks for {len(snapshot)} device(s)...")
    threads = []
    for hostname, d in snapshot.items():
        label = device_label(d)
        t = threading.Thread(target=fetch_and_log_tasks, args=(http_session, hostname, label), daemon=True)
        threads.append(t)
        t.start()
    for t in threads: t.join(timeout=10) # 10s combined parallel execution timeout window

def sync_all_tasks(http_session, devices):
    web_print(f"[{ts()}] INFO   Syncing Operator Sessions from Completed Tasks tabs...")
    threads = []
    for d in devices:
        hostname = d.get("hostname")
        label    = device_label(d)
        t = threading.Thread(target=fetch_and_log_tasks, args=(http_session, hostname, label), daemon=True)
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
    if global_session:
        scrape_all_device_tasks(global_session)

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

def get_status(device: dict) -> str:
    if not device.get("online"): return "offline"
    cs = device.get("capture_state", "unknown")
    if cs == "recording": return "recording"
    if (device.get("upload_queue") or 0) > 0: return "uploading"
    return cs or "idle"

def device_label(device: dict) -> str:
    return device.get("display_name") or device.get("hostname", "?")

def verify_on_close(session: requests.Session):
    web_print(f"[{ts()}] INFO   Starting granular verification of individual rows vs local history...", "sys")
    today_str = get_date_str()
    today_prefix = today_str.replace("-", "")
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
        scraped_day_sum = 0.0
        operator_contributions = {}
        
        api_url = f"{BASE_URL}/proxy/{hostname}/statusboard-api/mcap-sync/sessions?light=1&limit=100"
        try:
            r = session.get(api_url, timeout=5)
            if r.ok:
                data = r.json()
                groups = data if isinstance(data, list) else (data.get("session_groups") or data.get("sessions") or [])
                for rec in groups:
                    name = str(rec.get("name", ""))
                    start_unix = rec.get("start_time_unix") or rec.get("mtime") or 0
                    try:
                        from_unix_today = (start_unix > 0 and datetime.datetime.fromtimestamp(float(start_unix)).strftime("%Y-%m-%d") == today_str)
                    except Exception: from_unix_today = False

                    if name.startswith(today_prefix) or from_unix_today:
                        duration = float(rec.get("duration_s") or 0)
                        scraped_day_sum += duration
                        op_name = clean_str(rec.get("operator"))
                        operator_contributions[op_name] = operator_contributions.get(op_name, 0.0) + duration
        except Exception: continue

        diff = scraped_day_sum - local_sum
        if abs(diff) >= 5 and scraped_day_sum > 0:
            with log_lock:
                if today_str not in daily_totals: daily_totals[today_str] = {"total": 0, "by_pi": {}, "by_operator": {}}
                day_stats = daily_totals[today_str]
                old_pi_total = day_stats["by_pi"].get(label, 0)
                day_stats["by_pi"][label] = scraped_day_sum
                day_stats["total"] = day_stats["total"] - old_pi_total + scraped_day_sum
                for op_k, op_v in operator_contributions.items(): day_stats["by_operator"][op_k] = day_stats["by_operator"].get(op_k, 0) + op_v
                
            correction_lines.append(f"[{ts()}] CORRECTION | Pi: {label:<15} | Local Running Sum: {format_time(local_sum)} → Scraped True Total: {format_time(scraped_day_sum)} (diff={format_time(diff)})\n")
            web_print(f"[{ts()}] OVERWRITE {label:<15} | True Scraped Total: {format_time(scraped_day_sum)} | Local: {format_time(local_sum)} | Corrected")
        else:
            web_print(f"[{ts()}] VERIFY    {ANSI_GREEN}{label:<15}{ANSI_RESET} | True Scraped Total: {format_time(scraped_day_sum)} | Local: {format_time(local_sum)} | Matches")

    if correction_lines:
        try:
            with open(log_filename, "a") as f:
                f.write(f"[{ts()}] === Consolidated Dashboard Validation Summary ===\n")
                f.writelines(correction_lines)
                f.write(f"[{ts()}] ===================================================\n\n")
        except Exception: pass

# ── Embedded Local API Server ────────────────────────────────────────────────
class EmbeddedUIServer(BaseHTTPRequestHandler):
    def log_message(self, format, *args): return 
    
    def end_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'X-Requested-With, Content-Type, ngrok-skip-browser-warning')
        super().end_headers()

    def do_OPTIONS(self):
        self.send_response(200)
        self.end_headers()

    def do_GET(self):
        global loop_active
        if self.path == '/logs':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            with buffer_lock: self.wfile.write(json.dumps(terminal_buffer).encode())
        elif self.path == '/rankings':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            today_str = get_date_str()
            with log_lock:
                stats = daily_totals.get(today_str, {"by_pi": {}, "by_operator": {}})
                pi_source = stats.get("ui_live_pi") if "ui_live_pi" in stats else stats.get("by_pi", {})
                op_source = stats.get("ui_live_operator") if "ui_live_operator" in stats else stats.get("by_operator", {})
                pi_rank = [{"name": k, "duration": format_time(v)} for k, v in sorted(pi_source.items(), key=lambda x: x[1], reverse=True)]
                op_rank = [{"name": k, "duration": format_time(v)} for k, v in sorted(op_source.items(), key=lambda x: x[1], reverse=True)]
            self.wfile.write(json.dumps({"pi": pi_rank, "operator": op_rank, "active": loop_active}).encode())
        elif self.path == '/start':
            loop_active = True
            web_print(f"[{ts()}] SYSTEM  Loop tracking manually STARTED via UI.")
            self.send_response(200); self.end_headers()
        elif self.path == '/stop':
            loop_active = False
            web_print(f"[{ts()}] SYSTEM  {ANSI_BRIGHT_RED}Loop tracking manually STOPPED via UI.{ANSI_RESET}")
            self.send_response(200); self.end_headers()
        elif self.path == '/snapshot':
            log_current_totals("Web UI Trigger")
            self.send_response(200); self.end_headers()
        else:
            self.send_response(404); self.end_headers()

def start_web_server():
    server = HTTPServer(('127.0.0.1', WEB_PORT), EmbeddedUIServer)
    threading.Thread(target=server.serve_forever, daemon=True).start()

def execute_otp_flow(session: requests.Session, email: str) -> bool:
    send_url = f"{BASE_URL}/api/auth/otp/send"
    print(f"[{ts()}] INFO   Defaulting to OTP execution fallback. Requesting token code for: {email}...")
    try:
        r = session.post(send_url, json={"email": email}, timeout=10)
        if not r.ok or not r.json().get("ok"):
            print(f"[{ts()}] FATAL  Failed to send code sequence: {r.json().get('error', 'Unknown error')}")
            return False
    except Exception as e:
        print(f"[{ts()}] FATAL  Network failure while dispatching code configuration rules: {e}")
        return False

    print("\n" + "="*70)
    print(f" {ANSI_YELLOW}ATTENTION: Check your inbox for an email sent to {email}.{ANSI_RESET} ")
    otp_code = input(" Enter the 6-digit verification fallback login code: ").strip()
    print("="*70 + "\n")

    if not otp_code or len(otp_code) != 6:
        print(f"[{ts()}] FATAL  Invalid formatting token length pattern matches.")
        return False

    verify_url = f"{BASE_URL}/api/auth/otp/verify"
    try:
        r = session.post(verify_url, json={"email": email, "token": otp_code}, timeout=10)
        if r.ok and r.json().get("ok"):
            print(f"[{ts()}] SUCCESS Fully authorized with capture fleet infrastructure.")
            return True
        else:
            print(f"[{ts()}] FATAL  Verification rejected: {r.json().get('error', 'Invalid token context')}")
    except Exception as e:
        print(f"[{ts()}] FATAL  Network error while verifying fallback values: {e}")
    return False

def login(session: requests.Session) -> bool:
    try:
        with open("secrets.json", "r") as sf:
            secrets = json.load(sf)
            email = secrets.get("email")
            password = secrets.get("password")
    except Exception:
        print(f"[{ts()}] FATAL  Could not read secrets.json file architecture parameters.")
        return False

    if not email:
        print(f"[{ts()}] FATAL  Email credential parameter is missing inside secrets.json configuration parameters.")
        return False

    print(f"[{ts()}] DEBUG  Initializing credential handshakes with {BASE_URL}...")
    try:
        session.get(BASE_URL + "/", timeout=10)
    except requests.RequestException:
        print(f"[{ts()}] FATAL  Unable to establish safe connections with target network layer.")
        return False

    pw_auth_url = f"{BASE_URL}/api/auth/password"
    print(f"[{ts()}] INFO   Submitting authentication password parameters to database...")
    try:
        r = session.post(pw_auth_url, json={"email": email, "password": password or ""}, timeout=10)
        res_data = r.json()
        
        if r.ok and res_data.get("ok"):
            print(f"[{ts()}] SUCCESS Fully authorized with password infrastructure profile configuration rules.")
            return True
        else:
            err_msg = res_data.get("error", "Invalid configuration token.")
            print(f"[{ts()}] {ANSI_YELLOW}WARN   Password rejection matching detected: {err_msg}{ANSI_RESET}")
            return execute_otp_flow(session, email)
            
    except Exception as e:
        print(f"[{ts()}] WARN   Connection issue with password endpoints: {e}. Attempting OTP fallback flow rules...")
        return execute_otp_flow(session, email)

def poll(session: requests.Session) -> list[dict] | None:
    try: r = session.get(BASE_URL + "/api/fleet/status", timeout=10)
    except requests.RequestException: return None
    if r.status_code in (401, 403) or not r.ok: return None
    try: return r.json().get("devices", [])
    except ValueError: return None

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
    global global_session
    global_session = requests.Session()
    global_session.headers.update({"User-Agent": "fleet-monitor/1.0"})
    session = global_session

    if not login(session):
        print(f"[{ts()}] FATAL  Cannot authenticate sequence framework. Exiting.")
        sys.exit(1)

    load_daily_totals()
    start_web_server()
    start_key_listener() 
    web_print(f"[{ts()}] SYSTEM  Localhost-only API core listening on port {WEB_PORT}.")
    
    prev_status: dict[str, str] = {}
    
    while True:
        time.sleep(POLL_INTERVAL)
        if not loop_active: continue  

        devices = poll(session)
        if devices is None:
            if not login(session): time.sleep(30)
            continue

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
                    threading.Thread(target=fetch_and_log_tasks, args=(session, hostname, label, info["operator"], info["task"], info["duration"])).start()
                elif new_s == "recording" and not was_recording:
                    web_print(f"{ANSI_REC}[{ts()}] CHANGE {label:<30}  {old_s} → RECORDING | Op: {d.get('operator')}{ANSI_RESET}")
            prev_status[hostname] = new_s

if __name__ == "__main__":
    try: run()
    except KeyboardInterrupt: pass