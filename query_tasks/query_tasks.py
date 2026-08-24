#!/usr/bin/env python3
"""
query_tasks.py — pulls completed recording sessions and exports each one's
    session-json manifest as CSV. No categorization.

Scans for a query_config.txt file to avoid repeated prompts.

Requires Python 3.9+ and the `requests` package.

NOTE: if zsh cannot find pip and/or an error about externally-managed
    environment appears, run the following to make a virtual environment.

    python3 -m venv .venv
    source .venv/bin/activate
    python -m pip install --upgrade pip


    python3 query_tasks.py
    python3 query_tasks.py --out reports/sessions.csv
    python3 query_tasks.py --columns core,program
    python3 query_tasks.py --list-fields
    python3 query_tasks.py --from-json query_report.raw.json

WHAT THIS IS
------------
categorize_tasks.py answers "what kind of work was recorded, and for how
long". This answers "how was this session actually recorded" — it reads the
per-session manifest the rig writes, which carries the capture format, the
camera hardware and stream settings, the trigger, the software revision and
how the run stopped. None of that is in the sessions listing.

    /proxy/<hostname>/api/mcap-sync/<SESSION_ID>/session-json

Note that is `api/mcap-sync`, not the `statusboard-api/mcap-sync` the sessions
listing uses. Both are overridable — see --sessions-path / --session-json-path.

Two calls per rig-plus-session, then: the sessions listing enumerates session
IDs (and is the only source of file sizes and upload status), and session-json
is fetched once per session for everything else.

OUTPUT
------
    query_report.csv       ONE ROW PER SESSION. Which columns appear is
                           decided by `columns` in the config (or --columns);
                           the default is every group. The groups are:

                           core     row_type, date, weekday, pi_id, pi_name,
                                    operator, operator_user_id, task_name,
                                    instruction, folder, session_id, link,
                                    start_time, end_time, start_iso,
                                    duration_s, duration_hhmmss, duration_hours
                           program  environment, location, mission_id,
                                    is_test
                           sizes    total_bytes, size_label, mcap_count,
                                    mcap_files, upload_status
                                    — from the sessions listing, NOT from
                                    session-json, which carries no byte counts
                           capture  capture_format, role_sources,
                                    camera_roles, stream_count, then one fixed
                                    block per position named in `positions`
                                    (config; the four the fleet currently
                                    mounts — head, chest, wrist_left,
                                    wrist_right):
                                    <pos>_device_kind, <pos>_streams, and
                                    <pos>_video_codec/_fps/_resolution/
                                    _topic/_mcap
                           outcome  status, stop_reason, restart_reason,
                                    failure_reason, failed_roles,
                                    start_waived_roles, trigger_source,
                                    trigger_client, software_version,
                                    capture_script_revision/_branch/_dirty,
                                    depthai_version, platform, python_version,
                                    stereo_complete_pairs, stop_elapsed_s,
                                    process_start_unix

                           A TOTAL row leads the file: session count and
                           summed duration/bytes. Drop it with
                           --no-aggregate-row.

    query_report.raw.json  the untouched API responses — the device list, each
                           rig's sessions listing, and every session-json —
                           saved before any filtering. Nothing is lost by the
                           column set above. Disable with --no-json.

POSITIONS
---------
The camera detail under session_json.positions is keyed by mount position
(head, left, chest…) and differs between rigs, so emitting a column per
position found would make the column set depend on which rigs happened to
answer — and append mode would then be splicing rows of different widths into
one file. Instead `positions` (config) fixes the blocks up front, so the
header is decided by config and is identical run to run. The default names the
four positions the fleet currently mounts; a rig that carries only a head
camera leaves the other three blocks blank rather than shifting its row. Any
position NOT named there still shows up in camera_roles and stream_count, so
a rig with an unexpected mount is visible rather than silently dropped; add it
to `positions` to get its own columns.

Run --list-fields to print every key the fleet actually returned, flattened,
with how many sessions carried each. That is the way to find a field this
column set does not cover.

FILTERS
-------
days (config) limits the run to given MM/DD dates. min_duration drops sessions
shorter than an hh:mm:ss — shorter forms are accepted (5:00 is five minutes,
90 is ninety seconds). Both are measured against the API's duration_s: unlike
categorize_tasks.py, nothing here re-estimates a duration from byte counts,
because this report is about what the manifest says, not about correcting it.

include_test=n drops sessions whose manifest sets test=true. They are kept by
default, flagged in the is_test column, and are always left out of the TOTAL
row either way — a test recording is real footage on disk but is not work.

APPEND vs FULL REWRITE
----------------------
append=y (config) only adds sessions the file doesn't already have; existing
rows are never touched. append=n rewrites the file from this run's fresh pull,
matched against the existing file by session_id:
- A session returned again this run is rewritten fresh, except for any
  keep_columns (config), which survive untouched instead of being recomputed.
- A session NOT returned this run (its rig was unreachable, or it fell outside
  this run's filters/limit) is left exactly as it was rather than dropped.

Either way the TOTAL row is recomputed at the end over every session row the
file holds afterwards, not just the ones this run fetched.

SHEETS OUTPUT
-------------
sheets_output=y (config) also writes <out>_sheets.csv (query_report_sheets.csv
by default): the same report, but with every column that is merely derived
from another column replaced by a live formula pointing at its source cell on
that row — duration_hhmmss and duration_hours off duration_s, size_label off
total_bytes, link off session_id. Google Sheets evaluates a cell as a formula
the moment the CSV is imported, so once it is in Sheets, correcting a source
cell recalculates the derived ones in that row automatically.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import getpass
import json
import os
import re
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import requests
except Exception as _requests_error:            # noqa: N816
    sys.stderr.write(
        "\nCould not import `requests`.\n\n"
        f"  reason       : {type(_requests_error).__name__}: {_requests_error}\n"
        f"  interpreter  : {sys.executable}\n"
        f"  python       : {sys.version.split()[0]}\n\n"
        "If requests IS installed, it belongs to a different interpreter than the\n"
        "one above. Install it into this one:\n\n"
        f"    {sys.executable} -m pip install requests\n\n"
        "Run with --check-env for a full diagnostic.\n"
    )
    sys.exit(1)

BASE_URL = os.environ.get("FLEET_BASE_URL", "https://fleet.shiftiq.us")

# The two proxied rig endpoints. They do NOT share a prefix — the listing is
# served under statusboard-api and the manifest under api — so each is its own
# template rather than one base plus a suffix.
SESSIONS_PATH = "/proxy/{host}/statusboard-api/mcap-sync/sessions"
SESSION_JSON_PATH = "/proxy/{host}/api/mcap-sync/{session}/session-json"


# ─────────────────────────────────────────────────────────────────────────
# Fleet API client
#
# standalone copy of the fleet API client, so it can run on any machine
# ─────────────────────────────────────────────────────────────────────────
class FleetAPIError(Exception):
    pass


class _EmbeddedClient:
    def __init__(self, base_url: str = BASE_URL, timeout: float = 10.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "query-tasks/1.0"})

    def login_password(self, email: str, password: str):
        try:
            self.session.get(self.base_url + "/", timeout=self.timeout)
        except requests.RequestException as e:
            return False, f"could not reach {self.base_url}: {e}"
        try:
            r = self.session.post(f"{self.base_url}/api/auth/password",
                                  json={"email": email, "password": password or ""},
                                  timeout=self.timeout)
            data = r.json()
        except (requests.RequestException, ValueError) as e:
            return False, str(e)
        if r.ok and data.get("ok"):
            return True, None
        return False, data.get("error", "invalid credentials")

    def send_otp(self, email: str):
        try:
            r = self.session.post(f"{self.base_url}/api/auth/otp/send",
                                  json={"email": email}, timeout=self.timeout)
            data = r.json()
        except (requests.RequestException, ValueError) as e:
            return False, str(e)
        if r.ok and data.get("ok"):
            return True, None
        return False, data.get("error", "failed to send verification code")

    def verify_otp(self, email: str, token: str):
        try:
            r = self.session.post(f"{self.base_url}/api/auth/otp/verify",
                                  json={"email": email, "token": token},
                                  timeout=self.timeout)
            data = r.json()
        except (requests.RequestException, ValueError) as e:
            return False, str(e)
        if r.ok and data.get("ok"):
            return True, None
        return False, data.get("error", "invalid verification code")

    def get_fleet_status(self):
        try:
            r = self.session.get(f"{self.base_url}/api/fleet/status", timeout=self.timeout)
        except requests.RequestException:
            return None
        if r.status_code in (401, 403) or not r.ok:
            return None
        try:
            return r.json().get("devices", [])
        except ValueError:
            return None

    def get_device_sessions(self, hostname: str, light: bool = True, limit: int = 100,
                            timeout: float | None = None, retries: int = 1):
        url = f"{self.base_url}{SESSIONS_PATH.format(host=hostname)}"
        params = {"limit": limit}
        if light:
            params["light"] = 1
        timeout = timeout if timeout is not None else max(self.timeout, 15.0)
        r, last = None, None
        for _ in range(retries + 1):
            try:
                r = self.session.get(url, params=params, timeout=timeout)
                break
            except requests.RequestException as e:
                last = e
        if r is None:
            raise FleetAPIError(f"{hostname} unreachable: {last}")
        if not r.ok:
            raise FleetAPIError(f"{hostname} returned status {r.status_code}")
        try:
            return r.json().get("session_groups") or []
        except ValueError as e:
            raise FleetAPIError(f"{hostname} returned invalid JSON: {e}")


# Set when --sessions-path overrides the default: api_client.py has that URL
# baked in and would silently ignore the override, so the embedded client —
# which reads SESSIONS_PATH — is used instead. See main().
FORCE_EMBEDDED = False


def make_client(base_url: str):
    """Prefers the repo's api_client.py; falls back to the embedded copy.

    The repo root is one level up, since this script lives in its own folder.
    """
    if FORCE_EMBEDDED:
        return _EmbeddedClient(base_url), "embedded client (--sessions-path)"
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    try:
        from api_client import FleetAPIClient        # type: ignore
        return FleetAPIClient(base_url=base_url), "api_client.py"
    except Exception:
        return _EmbeddedClient(base_url), "embedded client"


def fetch_session_json(client, host: str, session_id: str, path_tmpl: str,
                       timeout: float = 30.0, retries: int = 1):
    """The per-session manifest. Returns the `session_json` object, or None.

    Uses the client's own authenticated requests.Session, so this works
    against api_client.py and the embedded client alike — neither has a method
    for this endpoint, and reaching through to the cookie jar is what keeps
    them interchangeable.
    """
    sess = getattr(client, "session", None)
    if sess is None:
        raise FleetAPIError("client exposes no requests session")
    base = getattr(client, "base_url", BASE_URL).rstrip("/")
    url = base + path_tmpl.format(host=host, session=session_id)
    r, last = None, None
    for _ in range(retries + 1):
        try:
            r = sess.get(url, timeout=timeout)
            break
        except requests.RequestException as e:
            last = e
    if r is None:
        raise FleetAPIError(f"{host}/{session_id[:8]}: {last}")
    if not r.ok:
        raise FleetAPIError(f"{host}/{session_id[:8]}: status {r.status_code}")
    try:
        body = r.json()
    except ValueError as e:
        raise FleetAPIError(f"{host}/{session_id[:8]}: invalid JSON: {e}")
    # The payload wraps the manifest: {"name":…, "session":…, "session_json":{…}}.
    # A rig that answers with the bare manifest is accepted too rather than
    # being read as an empty session.
    if isinstance(body, dict) and "session_json" in body:
        return body.get("session_json") or {}
    return body if isinstance(body, dict) else {}


# ── Configuration ────────────────────────────────────────────────────────
# Lives in this script's own folder, anchored to the file rather than the
# working directory so a run started from anywhere still finds it. It holds a
# fleet password — .gitignore and .vercelignore both match it by bare name at
# any depth, so it stays out of git and out of the deploy bundle.
DEFAULT_CONFIG = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "query_config.txt")

CONFIG_DEFAULTS = {
    "email": "",
    "password": "",
    "days": "all",
    "min_duration": "",
    "append": "n",
    "keep_columns": "",
    "sheets_output": "n",
    "columns": "all",
    "positions": "head,chest,wrist_left,wrist_right",
    "include_test": "y",
}


def load_config(config_file: str = None) -> dict:
    """Load config from file, or prompt the user if the file doesn't exist.

    Config file format:
        email=user@example.com
        password=mypassword
        days=08/11,08/10
        min_duration=00:05:00
        append=n
        keep_columns=task_name
        sheets_output=y
        columns=all
        positions=head
        include_test=y
    """
    config_file = config_file or DEFAULT_CONFIG
    config = dict(CONFIG_DEFAULTS)

    if os.path.exists(config_file):
        try:
            with open(config_file, "r") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    if "=" in line:
                        key, value = line.split("=", 1)
                        key = key.strip().lower()
                        value = value.strip()
                        if key in config:
                            config[key] = value
            print(f"Loaded config from {config_file}", file=sys.stderr)
            return config
        except Exception as e:
            print(f"Error reading {config_file}: {e}", file=sys.stderr)

    print(f"\nNo {config_file} found. Enter configuration:\n", file=sys.stderr)
    config["email"] = input("Email: ").strip()
    config["password"] = getpass.getpass("Password: ")

    days_input = input("Days to include (comma-separated, e.g. 08/11,08/10 "
                       "or 'all' for all): ").strip()
    config["days"] = days_input if days_input else "all"

    while True:
        min_input = input("Minimum session length as hh:mm:ss "
                          "(blank for no minimum): ").strip()
        try:
            parse_hhmmss(min_input)
        except ValueError as e:
            print(f"  {e}", file=sys.stderr)
            continue
        config["min_duration"] = min_input
        break

    append_input = input("Append mode? (y/n, default=n): ").strip().lower()
    config["append"] = "y" if append_input == "y" else "n"

    save = input(f"Save to {config_file}? (y/n, default=n): ").strip().lower()
    if save == "y":
        try:
            with open(config_file, "w") as f:
                for k in CONFIG_DEFAULTS:
                    f.write(f"{k}={config[k]}\n")
            print(f"Config saved to {config_file}", file=sys.stderr)
        except Exception as e:
            print(f"Error saving {config_file}: {e}", file=sys.stderr)

    return config


def parse_days_filter(days_str: str) -> set[str] | None:
    """Parse days string into a set of MM/DD dates, or None for all."""
    if days_str.lower() == "all" or not days_str.strip():
        return None
    return set(d.strip() for d in days_str.split(",") if d.strip())


def parse_list(value: str) -> list[str]:
    return [v.strip() for v in (value or "").split(",") if v.strip()]


# ── Terminal login ────────────────────────────────────────────────────────
def _ask(prompt: str, default: str = "n") -> str:
    try:
        return (input(prompt).strip().lower() or default)
    except (EOFError, KeyboardInterrupt):
        return default


def interactive_login(base_url: str, email: str, attempts: int = 3):
    """Authenticates against the fleet API using credentials.

    FLEET_EMAIL / FLEET_PASSWORD environment vars still honoured if set.
    """
    client, which = make_client(base_url)
    print(f"Signing in to {base_url}  (via {which})", file=sys.stderr)

    env_password = os.environ.get("FLEET_PASSWORD")

    for attempt in range(1, attempts + 1):
        if not email:
            print("  An email address is required.", file=sys.stderr)
            break

        if env_password and attempt == 1:
            password = env_password
        else:
            try:
                password = getpass.getpass(f"Password for {email}: ")
            except (EOFError, KeyboardInterrupt):
                sys.exit("\nCancelled.")

        ok, err = client.login_password(email, password)
        del password                     # drop the plaintext promptly
        if ok:
            print(f"  Signed in as {email}", file=sys.stderr)
            return client, email

        print(f"  Sign-in failed: {err}", file=sys.stderr)
        env_password = None        # a bad env password must not be retried
        if attempt < attempts:
            if _ask("  Try the password again? [Y/n]: ", default="y") not in ("y", "yes"):
                break

    # A failed password never triggers an email on its own — asking first.
    if _ask(f"  Email a one-time code to {email} instead? [y/N]: ") in ("y", "yes"):
        ok, err = client.send_otp(email)
        if not ok:
            sys.exit(f"Could not send a verification code: {err}")
        try:
            code = input("  6-digit code: ").strip()
        except (EOFError, KeyboardInterrupt):
            sys.exit("\nCancelled.")
        ok, err = client.verify_otp(email, code)
        if ok:
            print(f"  Signed in as {email}", file=sys.stderr)
            return client, email
        sys.exit(f"Verification failed: {err}")

    sys.exit("Could not authenticate.")


def check_env() -> int:
    """Prints exactly which interpreter is running and where requests came
    from. Run this first on a new machine."""
    print("interpreter :", sys.executable)
    print("python      :", sys.version.split()[0])
    print("platform    :", sys.platform)
    print("script      :", os.path.abspath(__file__))
    print("cwd         :", os.getcwd())
    print(f"requests    : {requests.__version__}  ({os.path.dirname(requests.__file__)})")

    shadow = []
    for d in ("", os.path.dirname(os.path.abspath(__file__)), os.getcwd()):
        for candidate in ("requests.py", "requests"):
            p = os.path.join(d, candidate) if d else candidate
            if os.path.exists(p) and os.path.abspath(p) != os.path.dirname(requests.__file__):
                shadow.append(os.path.abspath(p))
    problems = []
    if shadow:
        problems.append("a local file is shadowing the real requests package: "
                        + ", ".join(sorted(set(shadow))))
    try:
        requests.Session()
    except Exception as e:
        problems.append(f"requests.Session() failed: {type(e).__name__}: {e}")
    try:
        print("client      :", make_client(BASE_URL)[1])
    except Exception as e:
        problems.append(f"could not build a fleet client: {type(e).__name__}: {e}")

    if problems:
        print("\n!! this environment will NOT work:", file=sys.stderr)
        for p in problems:
            print("   * " + p, file=sys.stderr)
        return 1
    print("\nEnvironment looks usable.")
    return 0


# ── Helpers ───────────────────────────────────────────────────────────────
def hhmmss(seconds: float) -> str:
    s = int(round(float(seconds or 0)))
    return f"{s // 3600:02d}:{(s % 3600) // 60:02d}:{s % 60:02d}"


def parse_hhmmss(text: str) -> float:
    """Seconds from hh:mm:ss, mm:ss or ss. Blank means no minimum (0)."""
    text = (text or "").strip()
    if not text:
        return 0.0
    parts = text.split(":")
    if len(parts) > 3:
        raise ValueError(f"'{text}' is not a valid hh:mm:ss duration")
    try:
        nums = [float(p) for p in parts]
    except ValueError:
        raise ValueError(f"'{text}' is not a valid hh:mm:ss duration")
    if any(n < 0 for n in nums):
        raise ValueError(f"'{text}' is not a valid hh:mm:ss duration")
    total = 0.0
    for n in nums:
        total = total * 60 + n
    return total


def size_label(num_bytes) -> str:
    if num_bytes is None or num_bytes == "":
        return ""
    try:
        n = float(num_bytes)
    except (TypeError, ValueError):
        return ""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{int(n)} B"
        n /= 1024
    return ""


def _joined(value) -> str:
    """Lists and dicts flattened to something a CSV cell can hold."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, list):
        return ", ".join(_joined(v) for v in value)
    if isinstance(value, dict):
        return ", ".join(f"{k}={_joined(v)}" for k, v in sorted(value.items()))
    return str(value)


def _dig(d, *path, default=""):
    """Nested lookup that treats a missing branch as a blank cell rather
    than raising — rig builds differ in which sub-objects they send."""
    cur = d
    for key in path:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(key)
        if cur is None:
            return default
    return cur


def excel_safe(value):
    """Neutralises CSV formula injection because Excel is annoying."""
    if isinstance(value, SheetsFormula):
        return value
    if isinstance(value, str) and value[:1] in ("=", "+", "-", "@"):
        return "'" + value
    return value


class SheetsFormula(str):
    """A cell that IS a formula. excel_safe leaves these alone — everything
    else starting with '=' gets quoted."""


def col_letter(n: int) -> str:
    """1 -> A, 27 -> AA."""
    out = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        out = chr(65 + rem) + out
    return out


# ── Byte counts and upload status ────────────────────────────────────────
# session-json carries no byte counts at all, so these read the sessions
# listing record. Field names differ across rig builds, hence the key lists.
def _num(d: dict, keys):
    for k in keys:
        if k in d and d[k] not in (None, ""):
            try:
                return float(d[k])
            except (TypeError, ValueError):
                continue
    return None


def extract_total_bytes(rec: dict):
    """Total bytes for the session, summed across its mcap files where the
    rig reports them per file rather than as one total."""
    direct = _num(rec, ("total_bytes", "size_bytes", "bytes", "total_size_bytes"))
    if direct is not None:
        return direct
    files = rec.get("mcap_files") or rec.get("files") or []
    if isinstance(files, list) and files:
        total = 0.0
        seen = False
        for f in files:
            if not isinstance(f, dict):
                continue
            n = _num(f, ("size_bytes", "bytes", "size"))
            if n is not None:
                total += n
                seen = True
        if seen:
            return total
    return None


def extract_upload_status(rec: dict) -> str:
    for key in ("upload_status", "sync_status", "status"):
        v = rec.get(key)
        if isinstance(v, str) and v:
            return v
    synced = _num(rec, ("synced_files", "uploaded_files", "files_synced"))
    total = _num(rec, ("total_files", "file_count", "mcap_count"))
    if synced is not None and total is not None:
        pending = max(total - synced, 0)
        return "uploaded" if pending == 0 else f"pending {int(pending)}"
    return ""


def _start_unix(manifest: dict, rec: dict):
    """Session start as unix time: the manifest's field, else the listing's,
    else the YYYYMMDD_HHMMSS folder name, which is sometimes all there is."""
    raw = (manifest.get("start_time_unix") or rec.get("start_time_unix")
           or rec.get("mtime") or 0)
    if raw:
        try:
            return float(raw)
        except (TypeError, ValueError):
            pass
    name = manifest.get("session_id") or rec.get("name") or ""
    m = re.match(r"^(\d{8})_(\d{6})$", str(name))
    if m:
        try:
            return dt.datetime.strptime(m.group(1) + m.group(2),
                                        "%Y%m%d%H%M%S").timestamp()
        except ValueError:
            pass
    return None


# ── Columns ───────────────────────────────────────────────────────────────
# Grouped so `columns` (config) can select whole sections by name. Order here
# is the order in the CSV, and every group name is spelled out in --columns
# help — a typo names a group that does not exist and is rejected up front
# rather than silently writing a narrower report.
COLUMN_GROUPS: dict[str, list[str]] = {
    "core": [
        "row_type", "date", "weekday", "pi_id", "pi_name",
        "operator", "operator_user_id", "task_name", "instruction",
        "folder", "session_id", "link",
        "start_time", "end_time", "start_iso",
        "duration_s", "duration_hhmmss", "duration_hours",
    ],
    "program": [
        "environment", "location", "mission_id", "is_test",
    ],
    "sizes": [
        "total_bytes", "size_label", "mcap_count", "mcap_files", "upload_status",
    ],
    # The per-position blocks are appended to this list at runtime, once
    # `positions` is known — see build_fields.
    "capture": [
        "capture_format", "role_sources",
        "camera_roles", "stream_count",
    ],
    "outcome": [
        "status", "stop_reason", "restart_reason", "failure_reason",
        "failed_roles", "start_waived_roles",
        "trigger_source", "trigger_client",
        "software_version", "capture_script_revision", "capture_script_branch",
        "capture_script_dirty", "depthai_version", "platform", "python_version",
        "stereo_complete_pairs", "stop_elapsed_s", "process_start_unix",
    ],
}

GROUP_ORDER = ["core", "program", "sizes", "capture", "outcome"]

# One fixed block per position, so the header is decided by config rather than
# by which rigs answered this run. See POSITIONS in the module docstring.
POSITION_COLUMNS = [
    "device_kind", "streams",
    "video_codec", "video_fps", "video_resolution", "video_topic", "video_mcap",
]


def resolve_groups(spec: str) -> list[str]:
    """`all`, or a comma-separated subset, in the canonical order above."""
    wanted = parse_list(spec)
    if not wanted or "all" in [w.lower() for w in wanted]:
        return list(GROUP_ORDER)
    unknown = [w for w in wanted if w.lower() not in COLUMN_GROUPS]
    if unknown:
        sys.exit(f"unknown column group(s): {', '.join(unknown)}\n"
                 f"  known groups: {', '.join(GROUP_ORDER)}")
    lowered = {w.lower() for w in wanted}
    return [g for g in GROUP_ORDER if g in lowered]


# Structural, not informational: row_type tells a TOTAL row from a session
# row and session_id is what append mode dedupes on and what keep_columns
# matches against. A `columns` setting that leaves out the core group would
# otherwise produce a file this script cannot append to or re-total, so they
# are put back regardless of which groups were asked for.
REQUIRED_FIELDS = ["row_type", "session_id"]


def build_fields(groups: list[str], positions: list[str]) -> list[str]:
    fields: list[str] = []
    for g in groups:
        fields.extend(COLUMN_GROUPS[g])
        if g == "capture":
            for pos in positions:
                fields.extend(f"{pos}_{c}" for c in POSITION_COLUMNS)
    for i, name in enumerate(REQUIRED_FIELDS):
        if name not in fields:
            fields.insert(i, name)
    return fields


def _position_cells(manifest: dict, positions: list[str]) -> dict:
    """The fixed per-position block. A position the rig didn't report leaves
    its columns blank rather than shifting the row."""
    out = {}
    all_positions = _dig(manifest, "positions", default={}) or {}
    for pos in positions:
        block = all_positions.get(pos) if isinstance(all_positions, dict) else None
        devices = (block or {}).get("devices") if isinstance(block, dict) else None
        dev = devices[0] if isinstance(devices, list) and devices else {}
        if not isinstance(dev, dict):
            dev = {}
        streams = dev.get("streams") if isinstance(dev.get("streams"), dict) else {}

        # The video stream is whichever of these the build names first; rgb is
        # the common case, but a mono or depth-only rig has neither.
        video = {}
        for key in ("rgb", "video", "color", "left", "mono"):
            if isinstance(streams.get(key), dict):
                video = streams[key]
                break
        res = video.get("resolution")
        res_txt = ("x".join(str(v) for v in res)
                   if isinstance(res, list) and res else _joined(res))

        out.update({
            f"{pos}_device_kind": _joined(dev.get("kind")),
            f"{pos}_streams": ", ".join(sorted(streams)) if streams else "",
            f"{pos}_video_codec": _joined(video.get("codec")),
            f"{pos}_video_fps": _joined(video.get("fps")),
            f"{pos}_video_resolution": res_txt,
            f"{pos}_video_topic": _joined(video.get("topic")),
            f"{pos}_video_mcap": _joined(video.get("mcap")),
        })
    return out


def enrich(entry: dict, positions: list[str]) -> dict:
    """One CSV row from one session. `entry` is {manifest, listing, host,
    label} as assembled by fetch_sessions / load_sessions_from_json."""
    manifest = entry.get("manifest") or {}
    rec = entry.get("listing") or {}

    # duration_s is taken as given. Nothing here re-derives it from byte
    # counts the way categorize_tasks.py does — see FILTERS in the docstring.
    dur = 0.0
    for src in (manifest, rec):
        try:
            v = float(src.get("duration_s") or 0)
        except (TypeError, ValueError):
            v = 0.0
        if v > 0:
            dur = v
            break

    date = weekday = start_time = end_time = start_iso = ""
    start_unix = _start_unix(manifest, rec)
    if start_unix:
        try:
            start = dt.datetime.fromtimestamp(start_unix)
            end = start + dt.timedelta(seconds=dur)
            date = start.strftime("%Y-%m-%d")
            weekday = start.strftime("%A")
            start_time = start.strftime("%H:%M:%S")
            end_time = end.strftime("%H:%M:%S")
            start_iso = start.isoformat(timespec="seconds")
        except (OverflowError, OSError, ValueError):
            pass

    session_id = (manifest.get("session_uuid") or rec.get("id")
                  or entry.get("session_id") or "")
    task = (manifest.get("task") or rec.get("task") or "").strip()
    nbytes = extract_total_bytes(rec)
    all_positions = _dig(manifest, "positions", default={}) or {}
    stream_count = 0
    if isinstance(all_positions, dict):
        for block in all_positions.values():
            for dev in (block or {}).get("devices") or []:
                if isinstance(dev, dict) and isinstance(dev.get("streams"), dict):
                    stream_count += len(dev["streams"])

    files = rec.get("mcap_files") or []
    file_names = [f.get("name") for f in files
                  if isinstance(f, dict) and f.get("name")]

    row = {
        "row_type": "SESSION",
        "date": date,
        "weekday": weekday,
        "pi_id": entry.get("host") or "",
        "pi_name": entry.get("label") or "",
        "operator": (manifest.get("operator") or rec.get("operator")
                     or "Unknown").strip(),
        "operator_user_id": _joined(manifest.get("operator_user_id")),
        "task_name": task or "(no task)",
        "instruction": _joined(manifest.get("instruction")),
        # The rig's own YYYYMMDD_HHMMSS directory name. session-json calls it
        # session_id, which is NOT the UUID the rest of the fleet means by
        # that word — hence the rename here.
        "folder": _joined(manifest.get("session_id") or rec.get("name")),
        "session_id": session_id,
        "link": f"https://ops.usephysical.ai/sessions/{session_id}" if session_id else "",
        "start_time": start_time,
        "end_time": end_time,
        "start_iso": start_iso,
        "duration_s": round(dur, 2),
        "duration_hhmmss": hhmmss(dur),
        "duration_hours": round(dur / 3600, 4),

        "environment": _joined(manifest.get("environment") or rec.get("environment")),
        "location": _joined(manifest.get("location") or rec.get("location")),
        "mission_id": _joined(manifest.get("mission_id")),
        "is_test": "TRUE" if manifest.get("test") else "FALSE",

        "total_bytes": "" if nbytes is None else int(nbytes),
        "size_label": size_label(nbytes),
        "mcap_count": _joined(rec.get("mcap_count") or (len(files) or "")),
        "mcap_files": ", ".join(file_names),
        "upload_status": extract_upload_status(rec),

        "capture_format": _joined(manifest.get("capture_format")),
        "role_sources": _joined(manifest.get("role_sources")),
        "camera_roles": ", ".join(sorted(all_positions))
                        if isinstance(all_positions, dict) else "",
        "stream_count": stream_count or "",

        "status": _joined(manifest.get("status")),
        "stop_reason": _joined(manifest.get("stop_reason")),
        "restart_reason": _joined(manifest.get("restart_reason")),
        "failure_reason": _joined(manifest.get("failure_reason")
                                  or rec.get("failure_reason")),
        "failed_roles": _joined(manifest.get("failed_roles")),
        "start_waived_roles": _joined(manifest.get("start_waived_roles")),
        "trigger_source": _joined(_dig(manifest, "trigger", "source")),
        "trigger_client": _joined(_dig(manifest, "trigger", "client")),
        "software_version": _joined(_dig(manifest, "software", "version")),
        "capture_script_revision": _joined(
            _dig(manifest, "software", "capture_script_revision")),
        "capture_script_branch": _joined(
            _dig(manifest, "software", "capture_script_branch")),
        "capture_script_dirty": _joined(
            _dig(manifest, "software", "capture_script_dirty")),
        "depthai_version": _joined(_dig(manifest, "software", "depthai_version")),
        "platform": _joined(_dig(manifest, "software", "platform")),
        "python_version": _joined(_dig(manifest, "software", "python_version")),
        "stereo_complete_pairs": _joined(
            sum(v.get("complete_pairs") or 0
                for v in (manifest.get("stereo_recovery") or {}).values()
                if isinstance(v, dict))
            or ""),
        "stop_elapsed_s": _joined(_dig(manifest, "stop_timing", "elapsed_s")),
        "process_start_unix": _joined(manifest.get("process_start_unix")),

        # Not columns — used for filtering, sorting and the TOTAL row.
        "_start_unix": start_unix,
        "_test": bool(manifest.get("test")),
        "_bytes": nbytes,
        "_duration": dur,
    }
    row.update(_position_cells(manifest, positions))
    return row


# ── Fetch ─────────────────────────────────────────────────────────────────
def _entries_for_host(host: str, label: str, groups: list,
                      manifests: dict) -> list[dict]:
    """One entry per session with a positive duration, pairing that session's
    listing record with its fetched manifest. Shared by the live fetch and
    --from-json (see load_sessions_from_json), so a session read back from a
    saved payload gets identical treatment to one just pulled from the API."""
    out = []
    for rec in groups or []:
        sid = rec.get("id") or rec.get("session_uuid")
        if not sid:
            continue
        manifest = manifests.get(sid) or {}
        try:
            dur = float(manifest.get("duration_s") or rec.get("duration_s") or 0)
        except (TypeError, ValueError):
            dur = 0.0
        if dur <= 0:
            continue
        out.append({"session_id": sid, "host": host, "label": label,
                    "listing": rec, "manifest": manifest})
    return out


def _dedupe(entries: list[dict]) -> list[dict]:
    """One device can report the same session twice across cameras."""
    unique, seen = [], set()
    for e in entries:
        if e["session_id"] in seen:
            continue
        seen.add(e["session_id"])
        unique.append(e)
    return unique


def fetch_sessions(client, limit: int, workers: int, session_json_path: str):
    """Device list, then each device's sessions listing, then one session-json
    per session. Returns (entries, stats, payload).

    Offline rigs fail their proxy hop and a session whose manifest 404s still
    yields a row from the listing alone; both are counted and reported rather
    than aborting the run.
    """
    devices = client.get_fleet_status()
    if devices is None:
        sys.exit("Could not read the fleet status — the session may have expired.")
    if not devices:
        sys.exit("The fleet API returned no devices.")

    targets = [(d.get("hostname"), d.get("display_name") or d.get("hostname"))
               for d in devices if d.get("hostname")]
    print(f"Fetching sessions from {len(targets)} device(s)…", file=sys.stderr)

    raw_by_host: dict[str, list] = {}
    failures: list[tuple[str, str]] = []
    unreachable_hosts: set[str] = set()
    lock = threading.Lock()
    done = 0

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(client.get_device_sessions, h, True, limit): (h, l)
                for h, l in targets}
        for fut in as_completed(futs):
            host, label = futs[fut]
            try:
                groups = fut.result()
            except Exception as e:
                with lock:
                    unreachable_hosts.add(host)
                    failures.append((label, str(e)))
                groups = []
            with lock:
                done += 1
                raw_by_host[host] = groups or []
                sys.stderr.write(f"\r  {done}/{len(targets)} devices")
                sys.stderr.flush()
    sys.stderr.write("\n")

    # Every session id that came back, paired with the rig that owns it. The
    # manifest is per session, so this is the second and much larger fan-out.
    wanted: list[tuple[str, str]] = []
    for host, groups in raw_by_host.items():
        for rec in groups or []:
            sid = rec.get("id") or rec.get("session_uuid")
            if sid:
                wanted.append((host, sid))

    print(f"Fetching {len(wanted)} session manifest(s)…", file=sys.stderr)
    manifests: dict[str, dict] = {}
    manifest_failures: list[str] = []
    done = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(fetch_session_json, client, h, s, session_json_path): (h, s)
                for h, s in wanted}
        for fut in as_completed(futs):
            host, sid = futs[fut]
            try:
                manifest = fut.result()
            except Exception as e:
                manifest = None
                with lock:
                    manifest_failures.append(str(e))
            with lock:
                done += 1
                if manifest is not None:
                    manifests[sid] = manifest
                if done % 10 == 0 or done == len(wanted):
                    sys.stderr.write(f"\r  {done}/{len(wanted)} manifests, "
                                     f"{len(manifests)} ok")
                    sys.stderr.flush()
    sys.stderr.write("\n")

    for label, err in failures:
        print(f"  ! {label}: {err}", file=sys.stderr)
    if manifest_failures:
        print(f"  ! {len(manifest_failures)} session manifest(s) could not be "
              f"read; those rows fall back to the sessions listing alone",
              file=sys.stderr)
        for err in manifest_failures[:5]:
            print(f"      {err}", file=sys.stderr)

    labels = {h: l for h, l in targets}
    entries: list[dict] = []
    for host, groups in raw_by_host.items():
        entries.extend(_entries_for_host(host, labels.get(host, host),
                                         groups, manifests))
    unique = _dedupe(entries)

    payload = {
        "fetched_at": dt.datetime.now().isoformat(timespec="seconds"),
        "base_url": getattr(client, "base_url", ""),
        "devices": devices,
        "sessions_by_device": raw_by_host,
        "session_json": manifests,
        "counts": {
            "devices": len(targets),
            "unreachable": len(failures),
            "sessions_returned": sum(len(v) for v in raw_by_host.values()),
            "manifests_ok": len(manifests),
            "manifests_failed": len(manifest_failures),
            "sessions_kept": len(unique),
        },
        "unreachable": [{"device": l, "error": e} for l, e in failures],
    }
    stats = {
        "devices": len(targets),
        "failed": len(failures),
        "manifests_ok": len(manifests),
        "manifests_failed": len(manifest_failures),
        "dupes": len(entries) - len(unique),
        "unreachable_hosts": unreachable_hosts,
    }
    return unique, stats, payload


def load_sessions_from_json(path: str):
    """Rebuilds (entries, stats, payload) from a raw JSON file saved earlier
    by --json/--json-out, instead of hitting the fleet API — see --from-json.
    `payload` is returned as loaded, so re-running with --json still saves an
    exact copy if the caller wants one at a different path."""
    with open(path, encoding="utf-8") as f:
        payload = json.load(f)
    devices = payload.get("devices") or []
    labels = {d.get("hostname"): d.get("display_name") or d.get("hostname")
              for d in devices if d.get("hostname")}
    raw_by_host = payload.get("sessions_by_device") or {}
    manifests = payload.get("session_json") or {}

    entries = []
    for host, groups in raw_by_host.items():
        entries.extend(_entries_for_host(host, labels.get(host, host),
                                         groups, manifests))
    unique = _dedupe(entries)
    unreachable = payload.get("unreachable") or []
    stats = {
        "devices": len(devices),
        "failed": len(unreachable),
        "manifests_ok": len(manifests),
        "manifests_failed": 0,
        "dupes": len(entries) - len(unique),
        "unreachable_hosts": {u.get("device") for u in unreachable
                              if u.get("device")},
    }
    return unique, stats, payload


def list_fields(entries: list[dict]) -> None:
    """Every key the manifests actually carried, flattened, with how many
    sessions had one. The way to find a field the column set misses."""
    counts: dict[str, int] = {}
    samples: dict[str, str] = {}

    def walk(obj, prefix=""):
        if isinstance(obj, dict):
            for k, v in obj.items():
                walk(v, f"{prefix}.{k}" if prefix else k)
        elif isinstance(obj, list):
            counts[prefix] = counts.get(prefix, 0) + 1
            samples.setdefault(prefix, f"[list of {len(obj)}]")
            if obj and isinstance(obj[0], (dict, list)):
                walk(obj[0], f"{prefix}[]")
        else:
            counts[prefix] = counts.get(prefix, 0) + 1
            samples.setdefault(prefix, repr(obj)[:60])

    for e in entries:
        walk(e.get("manifest") or {})
    total = len(entries)
    print(f"\n{len(counts)} distinct field(s) across {total} session(s):\n")
    for key in sorted(counts):
        print(f"  {counts[key]:>5}/{total}  {key:<52} {samples.get(key, '')}")


# ── Aggregate row ─────────────────────────────────────────────────────────
def totals_row(rows: list[dict], email: str) -> dict:
    """One TOTAL row. Test sessions are excluded from the count and the sums —
    a test recording is real footage but is not work — and are tallied
    separately in the is_test cell so the exclusion is visible rather than
    just making the total look low."""
    real = [r for r in rows if r.get("row_type") == "SESSION"
            and str(r.get("is_test", "")).upper() != "TRUE"]
    tests = [r for r in rows if r.get("row_type") == "SESSION"
             and str(r.get("is_test", "")).upper() == "TRUE"]

    def _f(r, key):
        try:
            return float(r.get(key) or 0)
        except (TypeError, ValueError):
            return 0.0

    secs = sum(_f(r, "duration_s") for r in real)
    nbytes = sum(_f(r, "total_bytes") for r in real)
    return {
        "row_type": "TOTAL",
        "date": dt.datetime.now().strftime("%Y-%m-%d %H:%M"),
        "operator": email or "",
        "task_name": f"{len(real)} sessions",
        "is_test": f"{len(tests)} excluded" if tests else "0 excluded",
        "duration_s": round(secs, 2),
        "duration_hhmmss": hhmmss(secs),
        "duration_hours": round(secs / 3600, 4),
        "total_bytes": int(nbytes) if nbytes else "",
        "size_label": size_label(nbytes) if nbytes else "",
    }


# ── CSV I/O ───────────────────────────────────────────────────────────────
def read_existing_rows(path: str) -> dict[str, dict[str, str]]:
    """Existing SESSION rows keyed by session_id, exactly as they sit in the
    file. Returns {} if the file is missing or unreadable."""
    if not os.path.exists(path):
        return {}
    try:
        with open(path, newline="", encoding="utf-8-sig") as f:
            out = {}
            for row in csv.DictReader(f):
                if row.get("row_type") == "SESSION" and row.get("session_id"):
                    out[row["session_id"]] = row
            return out
    except (OSError, csv.Error):
        return {}


def ends_with_newline(path: str) -> bool:
    try:
        with open(path, "rb") as f:
            if f.seek(0, os.SEEK_END) == 0:
                return True
            f.seek(-1, os.SEEK_END)
            return f.read(1) in (b"\n", b"\r")
    except OSError:
        return True


def merge_full_rewrite(rows: list[dict], path: str,
                       keep_columns: list[str]) -> tuple[list[dict], int, int]:
    """append=n. Fresh rows win, except for keep_columns, which are carried
    over from the existing file. A session the file has but this run did not
    return is kept as-is rather than dropped — see APPEND vs FULL REWRITE."""
    existing = read_existing_rows(path)
    if not existing:
        return rows, 0, 0

    fresh_ids = set()
    for r in rows:
        sid = r.get("session_id")
        if not sid:
            continue
        fresh_ids.add(sid)
        old = existing.get(sid)
        if not old:
            continue
        for col in keep_columns:
            if col in old and old[col] not in (None, ""):
                r[col] = old[col]

    carried = [old for sid, old in existing.items() if sid not in fresh_ids]
    return rows + carried, len(fresh_ids & set(existing)), len(carried)


def write_csv(path: str, fields: list[str], rows: list[dict],
              append_mode: bool = False):
    d = os.path.dirname(os.path.abspath(path))
    if d:
        os.makedirs(d, exist_ok=True)

    if append_mode:
        existing = set(read_existing_rows(path))
        rows_to_write = [r for r in rows
                         if r.get("row_type") != "SESSION"
                         or r.get("session_id") not in existing]
    else:
        rows_to_write = rows

    mode = "a" if append_mode and os.path.exists(path) else "w"
    # A CSV round-tripped through Sheets/Excel often has no final newline.
    # Appending straight onto that splices the first new row onto the tail of
    # the last existing one. Close the line first. Plain utf-8 so the BOM
    # logic stays out of it; "\r\n" matches csv.writer's line terminator.
    if mode == "a" and not ends_with_newline(path):
        with open(path, "a", newline="", encoding="utf-8") as f:
            f.write("\r\n")
    # utf-8-sig: Excel needs the BOM or accented operator names arrive mangled.
    with open(path, mode, newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        if mode == "w":
            w.writeheader()
        for r in rows_to_write:
            w.writerow({k: excel_safe(r.get(k, "")) for k in fields})
    return len(rows_to_write)


def count_csv_rows(path: str) -> int:
    """Physical rows already in the file, header included — the offset the
    next appended row's formulas have to point at."""
    if not os.path.exists(path):
        return 0
    try:
        with open(path, newline="", encoding="utf-8-sig") as f:
            return sum(1 for _ in csv.reader(f))
    except (OSError, csv.Error):
        return 0


def build_sheets_rows(rows: list[dict], fields: list[str],
                      first_row: int) -> list[dict]:
    """A copy of `rows` for <out>_sheets.csv: on every SESSION row, each
    column that is merely derived from another column is replaced by a live
    formula pointing at its source cell on that same row. Correcting a source
    cell in Sheets then recalculates the derived ones automatically.

    A derived column whose source is not in this run's column set is left as
    the literal value — a formula pointing at a column that isn't there would
    evaluate against whatever else landed in that position.
    """
    idx = {name: i + 1 for i, name in enumerate(fields)}

    def cell(name, row_no):
        return f"{col_letter(idx[name])}{row_no}" if name in idx else None

    out = []
    for offset, r in enumerate(rows):
        row_no = first_row + offset
        new = dict(r)
        if r.get("row_type") == "SESSION":
            dur = cell("duration_s", row_no)
            if dur and "duration_hhmmss" in idx:
                new["duration_hhmmss"] = SheetsFormula(
                    f'=IF({dur}="","",TEXT({dur}/86400,"[h]:mm:ss"))')
            if dur and "duration_hours" in idx:
                new["duration_hours"] = SheetsFormula(f'=IF({dur}="","",{dur}/3600)')

            tb = cell("total_bytes", row_no)
            if tb and "size_label" in idx:
                # LOG(0) is an error, so the zero/blank case is short-circuited
                # before the unit maths ever runs.
                new["size_label"] = SheetsFormula(
                    f'=IF(N({tb})=0,"",TEXT({tb}/POWER(1024,INT(LOG({tb},1024))),"0.0")'
                    f'&" "&CHOOSE(INT(LOG({tb},1024))+1,"B","KB","MB","GB","TB"))')

            sid = cell("session_id", row_no)
            if sid and "link" in idx:
                new["link"] = SheetsFormula(
                    f'=IF({sid}="","",HYPERLINK("https://ops.usephysical.ai/sessions/"'
                    f'&{sid},{sid}))')
        out.append(new)
    return out


def refresh_totals(path: str, email: str, allow_insert: bool = True) -> bool:
    """Recompute the TOTAL row over every SESSION row the file holds.

    Row ORDER is preserved exactly: an existing TOTAL row is replaced where it
    sits and nothing else moves. That matters for the _sheets copy, whose
    formulas are anchored to physical row numbers — shifting a row down by one
    silently points every formula at its neighbour. For the same reason
    allow_insert=False refuses to prepend a TOTAL to a file that has none,
    rather than pushing the whole body down.

    Reads the header back off the file rather than using this run's field
    list: a file written by an earlier run with a wider `columns` setting
    would otherwise have those columns silently dropped here.
    """
    if not os.path.exists(path):
        return False
    try:
        with open(path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            header = reader.fieldnames or []
            rows = list(reader)
    except (OSError, csv.Error) as e:
        print(f"  ! could not refresh the TOTAL row: {e}", file=sys.stderr)
        return False
    if not header:
        return False

    total = totals_row([r for r in rows if r.get("row_type") == "SESSION"], email)
    at = next((i for i, r in enumerate(rows) if r.get("row_type") == "TOTAL"), None)
    if at is None:
        if not allow_insert:
            return False
        rows.insert(0, total)
    else:
        rows[at] = total

    try:
        with open(path, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=header, extrasaction="ignore")
            w.writeheader()
            for r in rows:
                w.writerow({k: excel_safe(r.get(k, "")) for k in header})
    except OSError as e:
        print(f"  ! could not refresh the TOTAL row: {e}", file=sys.stderr)
        return False
    return True


# ── CLI ───────────────────────────────────────────────────────────────────
# The report, its _sheets copy and the .raw.json all derive from --out, so
# this one default decides where all three land. Anchored to this file rather
# than the working directory so a run started from anywhere writes one report.
DEFAULT_OUT = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "query_report.csv")


def main(argv=None):
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--email", help="skip the email prompt (password is still asked for)")
    ap.add_argument("--base-url", default=BASE_URL)
    ap.add_argument("--config", help=f"config file (default: {DEFAULT_CONFIG})")
    ap.add_argument("--out", default=DEFAULT_OUT)
    ap.add_argument("--limit", type=int, default=500,
                    help="max sessions requested per device (default 500)")
    ap.add_argument("--workers", type=int, default=8,
                    help="parallel requests (default 8)")
    ap.add_argument("--columns", default=None,
                    help="comma-separated groups, or 'all' (default: config). "
                         "Groups: " + ", ".join(GROUP_ORDER))
    ap.add_argument("--positions", default=None,
                    help="camera positions to give their own columns "
                         "(default: config; head,chest,wrist_left,wrist_right)")
    ap.add_argument("--days", default=None,
                    help="MM/DD dates to keep, comma-separated (default: config)")
    ap.add_argument("--min-seconds", type=float, default=None,
                    help="minimum session length in seconds; overrides "
                         "min_duration for one run (0 for no minimum)")
    ap.add_argument("--include-test", dest="include_test", action="store_true",
                    default=None, help="keep sessions flagged test=true")
    ap.add_argument("--no-include-test", dest="include_test", action="store_false",
                    help="drop sessions flagged test=true")
    ap.add_argument("--append", dest="append", action="store_true", default=None,
                    help="add only sessions the file lacks (default: config)")
    ap.add_argument("--no-append", dest="append", action="store_false",
                    help="rewrite the file (default: config)")
    ap.add_argument("--no-aggregate-row", action="store_true",
                    help="skip the leading TOTAL row")
    ap.add_argument("--sheets", dest="sheets", action="store_true", default=None,
                    help="also write <out>_sheets.csv (default: config)")
    ap.add_argument("--no-sheets", dest="sheets", action="store_false",
                    help="skip <out>_sheets.csv (default: config)")
    ap.add_argument("--no-json", action="store_true",
                    help="skip saving the raw API payload")
    ap.add_argument("--json-out", help="where to save the raw API JSON "
                                       "(default: <out>.raw.json)")
    ap.add_argument("--from-json", help="read a saved raw payload instead of "
                                        "calling the fleet API")
    ap.add_argument("--sessions-path", default=SESSIONS_PATH,
                    help="sessions listing template (default: %(default)s). "
                         "Overriding it forces the embedded client, since "
                         "api_client.py has its own URL baked in")
    ap.add_argument("--session-json-path", default=SESSION_JSON_PATH,
                    help="manifest template (default: %(default)s)")
    ap.add_argument("--list-fields", action="store_true",
                    help="print every manifest field the fleet returned, then exit")
    ap.add_argument("--dry-run", action="store_true",
                    help="fetch and report, but write nothing")
    ap.add_argument("--check-env", action="store_true",
                    help="diagnose the interpreter and requests install, then exit")
    a = ap.parse_args(argv)

    if a.check_env:
        return check_env()

    if a.sessions_path != SESSIONS_PATH:
        globals()["SESSIONS_PATH"] = a.sessions_path
        globals()["FORCE_EMBEDDED"] = True

    cfg = load_config(a.config)

    groups = resolve_groups(a.columns if a.columns is not None else cfg["columns"])
    positions = parse_list(a.positions if a.positions is not None
                           else cfg["positions"]) or parse_list(
                               CONFIG_DEFAULTS["positions"])
    fields = build_fields(groups, positions)

    days = parse_days_filter(a.days if a.days is not None else cfg["days"])
    if a.min_seconds is not None:
        min_seconds = max(a.min_seconds, 0.0)
    else:
        try:
            min_seconds = parse_hhmmss(cfg["min_duration"])
        except ValueError as e:
            sys.exit(f"min_duration in query_config.txt: {e}")
    include_test = (a.include_test if a.include_test is not None
                    else cfg["include_test"].lower() == "y")
    append_mode = (a.append if a.append is not None
                   else cfg["append"].lower() == "y")
    sheets_output = (a.sheets if a.sheets is not None
                     else cfg["sheets_output"].lower() == "y")
    keep_columns = parse_list(cfg["keep_columns"])

    # ── Gather ────────────────────────────────────────────────────────────
    if a.from_json:
        entries, stats, payload = load_sessions_from_json(a.from_json)
        email = cfg["email"]
        print(f"Read {len(entries)} session(s) from {a.from_json}", file=sys.stderr)
    else:
        email = a.email or cfg["email"] or os.environ.get("FLEET_EMAIL", "")
        password = cfg["password"] or os.environ.get("FLEET_PASSWORD", "")
        if email and password:
            client, which = make_client(a.base_url)
            ok, err = client.login_password(email, password)
            if not ok:
                print(f"  Sign-in with the stored credentials failed: {err}",
                      file=sys.stderr)
                client, email = interactive_login(a.base_url, email)
            else:
                print(f"  Signed in as {email}  (via {which})", file=sys.stderr)
        else:
            client, email = interactive_login(a.base_url, email)
        entries, stats, payload = fetch_sessions(
            client, a.limit, a.workers, a.session_json_path)

    if not entries:
        sys.exit("No sessions returned.")

    if a.list_fields:
        list_fields(entries)
        return 0

    # ── Filter ────────────────────────────────────────────────────────────
    rows = [enrich(e, positions) for e in entries]
    rows.sort(key=lambda r: r.get("_start_unix") or 0)

    kept, dropped_day, dropped_short, dropped_test = [], 0, 0, 0
    for r in rows:
        if days is not None:
            # MM/DD against the row's own local date, so the filter reads the
            # way it is written in the config.
            mmdd = ""
            if r.get("date"):
                y, m, d = r["date"].split("-")
                mmdd = f"{m}/{d}"
            if mmdd not in days:
                dropped_day += 1
                continue
        if min_seconds and r["_duration"] < min_seconds:
            dropped_short += 1
            continue
        if not include_test and r["_test"]:
            dropped_test += 1
            continue
        kept.append(r)

    print(f"\n  {len(kept)} session(s) kept", file=sys.stderr)
    if dropped_day:
        print(f"  {dropped_day} outside the day filter", file=sys.stderr)
    if dropped_short:
        print(f"  {dropped_short} under {hhmmss(min_seconds)}", file=sys.stderr)
    if dropped_test:
        print(f"  {dropped_test} flagged as test sessions", file=sys.stderr)
    if stats.get("manifests_failed"):
        print(f"  {stats['manifests_failed']} manifest(s) unavailable",
              file=sys.stderr)
    if not kept:
        sys.exit("Nothing left to write after filtering.")

    if a.dry_run:
        print("\n--dry-run: nothing written.", file=sys.stderr)
        return 0

    # ── Write ─────────────────────────────────────────────────────────────
    base, ext = os.path.splitext(a.out)
    if not a.no_json:
        json_path = a.json_out or f"{base}.raw.json"
        try:
            os.makedirs(os.path.dirname(os.path.abspath(json_path)), exist_ok=True)
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2)
            print(f"  raw payload  -> {json_path}", file=sys.stderr)
        except OSError as e:
            print(f"  ! could not save the raw payload: {e}", file=sys.stderr)

    out_rows = list(kept)
    if not append_mode:
        out_rows, rewritten, carried = merge_full_rewrite(out_rows, a.out, keep_columns)
        if rewritten or carried:
            print(f"  rewrote {rewritten} existing row(s), kept {carried} "
                  f"this run did not return", file=sys.stderr)
        out_rows.sort(key=lambda r: str(r.get("start_iso") or ""))

    # The TOTAL row is written by refresh_totals below, over the whole file
    # rather than over this run's slice — appending its own TOTAL here would
    # leave one per run stacked in the body.
    written = write_csv(a.out, fields, out_rows, append_mode=append_mode)
    print(f"  report       -> {a.out}  ({written} row(s) written)", file=sys.stderr)

    if not a.no_aggregate_row:
        if refresh_totals(a.out, email):
            print("  TOTAL row recomputed over the whole file", file=sys.stderr)

    if sheets_output:
        sheets_path = f"{base}_sheets{ext or '.csv'}"
        if append_mode:
            # The formulas have to start after whatever is already in the file.
            sheets_rows = build_sheets_rows(
                out_rows, fields, count_csv_rows(sheets_path) + 1)
        else:
            # The TOTAL row is prepended HERE rather than added afterwards the
            # way it is for the report: it occupies a physical row, so the
            # formulas below have to be numbered with it already in place.
            # Adding it after numbering shifts every session row down one and
            # points each formula at the row above it.
            leading = ([] if a.no_aggregate_row
                       else [totals_row(out_rows, email)])
            sheets_rows = build_sheets_rows(leading + out_rows, fields, 2)
        n = write_csv(sheets_path, fields, sheets_rows, append_mode=append_mode)
        print(f"  sheets copy  -> {sheets_path}  ({n} row(s) written)",
              file=sys.stderr)
        if append_mode and not a.no_aggregate_row:
            # allow_insert=False: a sheets file with no TOTAL row must not
            # gain one now, since every formula in it is row-anchored.
            refresh_totals(sheets_path, email, allow_insert=False)

    return 0


if __name__ == "__main__":
    sys.exit(main())
