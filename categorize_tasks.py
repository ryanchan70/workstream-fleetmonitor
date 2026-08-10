#!/usr/bin/env python3
"""
categorize_tasks.py — pulls completed recording sessions, categorizes, and
    exports useful info as CSV. raw JSON also saved for reference.

Requires Python 3.9+ and the `requests` package.

NOTE: if zsh cannot find pip and/or an error about externally-managed
    environment appears, run the following to make a virtual environment.

    python3 -m venv .venv
    source .venv/bin/activate
    python -m pip install --upgrade pip


    python3 categorize_tasks.py
    python3 categorize_tasks.py --out reports/tasks.csv --explain
    python3 categorize_tasks.py --rules my_rules.json
    python3 categorize_tasks.py --make-rules-template rules.json
    python3 categorize_tasks.py --from-json task_report.raw.json --out reports/tasks.csv

OUTPUT
------
    task_report.csv        ONE ROW PER SESSION, 22 columns:
                             row_type, date, pi_id, pi_name, operator,
                             task_name, task_prefix, category, link,
                             is_categorized,
                             duration_seconds, duration_hhmmss, duration_hours,
                             start_time, end_time, session_id,
                             total_bytes, size_label, upload_status
                           …then one boolean per category code:
                             is_rc4, is_xo4, is_et4, is_im4, is_bp4, is_ho4,
                             is_at4, is_ls4, is_ch4, is_by4, is_sr4, is_pd4,
                             is_af4
                           …then, at the very right: duration_implied (the
                           byte-rate estimate, whether or not it was trusted
                           over the wall clock — see DURATION below),
                           duration_s (the untouched wall-clock span), and
                           yield_pct (always blank — for a human to fill in
                           by hand against the two duration columns above).
                           task_prefix is just task_name[:8] — handy as a quick
                           sort/group key since many task names lead with a
                           category code (e.g. "CH4-3429-Cattree" -> "CH4-3429").

                           Three totals rows lead the file: TOTAL (session counts
                           per category), TOTAL_HOURS (hours per category), and
                           CATEGORY_TAGS (human-readable category names).
                           Drop both with --no-aggregate-row. --raw-columns
                           appends a raw.<field> column for every API field.
                           --bool-1-0 writes 1/0 instead of TRUE/FALSE.

                           A session carrying two codes is TRUE for both, so the
                           per-category counts can add up to more than the
                           session count. That is intended for boolean columns.

    task_report.raw.json   the untouched API response, saved before any
                           filtering or categorising. Nothing is lost by the
                           trimmed column set. Disable with --no-json.

Per-category totals are NOT written by default - that file is aggregates only
and is easy to mistake for the report. Ask for it with --category-file.

total_bytes and upload_status are read from whichever field names the rig build
uses, including nested per-file byte counts and synced/total file pairs. If
they come up empty, --list-fields prints what your fleet actually returns.

DURATION
--------
The API's duration_s is a wall-clock span: session start timestamp to end
timestamp. Usually that's also how long the rig recorded, but a hang or a
late finalize/upload can leave the span far longer than what actually got
captured. To catch that, each rig's own bytes/second rate (learned from its
other sessions) is used to check whether the file sizes could plausibly
account for the full span. When they fall far short (below
DURATION_MISMATCH_RATIO), duration_seconds/hhmmss/hours report the
byte-implied duration instead of the raw span.

min_duration (config) drops sessions shorter than a given hh:mm:ss - the usual
way to keep aborted starts and a few seconds of test footage out of the
report. It is measured against duration_seconds, i.e. after the byte-implied
correction above, so a session is judged on what it looks like it actually
recorded rather than on a span a hang inflated. --min-seconds overrides it for
a single run, and is the only way to ask for no minimum when the config sets
one (--min-seconds 0).

APPEND vs FULL REWRITE
----------------------
append=y (config) only adds sessions the file doesn't already have; existing
session rows are never touched. append=n rewrites the file from this run's
fresh API pull instead, matched against the existing file by session_id:
- A session returned again this run is always rewritten fresh - that's the
  point of a rewrite - except for any keep_columns (config), which survive
  untouched instead of being recomputed (e.g. a hand-corrected category).
- A session NOT returned this run (its rig was unreachable, or it just fell
  outside this run's filters/limit) is left exactly as it was, rather than
  being dropped. A row is always either the fresh version or the untouched
  old one, never both.

Either way, TOTAL and TOTAL_HOURS are recomputed in place at the end of every
run, over every session row the file holds afterwards - not just the ones this
run happened to fetch. Without that they would stay frozen at whatever the run
that created the file saw, and drift further from the body under them with
each append. CATEGORY_TAGS is static and is left alone.

SHEETS OUTPUT
-------------
sheets_output=y (config) also writes <out>_sheets.csv (task_report_sheets.csv
by default): the same report, but with task_prefix/category/is_categorized/
is_<code> on every SESSION row replaced by live Sheets formulas driven off
that row's task_name, instead of fixed values. Google Sheets evaluates a
cell as a formula the moment the CSV is imported, so once it's in Sheets,
task_name is the only column that ever needs hand-cleaning - correcting it
recalculates everything else in that row automatically. Needs the aggregate
rows (skipped if --no-aggregate-row is set), since the category formula's
code -> name lookup is the CATEGORY_TAGS row.

MATCHING
--------
Category codes are matched case-insensitively and flexibly:
- Allows lowercase letters (im4, iM4)
- Allows spaces and hyphens (IM4 - 1234, IM4-1234, IM41234 all match IM4)
- Detected in the TASK NAME, only as whole token.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import getpass
import json
import os
import re
import statistics
import sys
import threading
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import requests
except Exception as _requests_error:
    # requests not being installed/installed in a different environment
    # is the most common failure point
    sys.stderr.write(
        "\nCould not import `requests`.\n\n"
        f"  reason       : {type(_requests_error).__name__}: {_requests_error}\n"
        f"  interpreter  : {sys.executable}\n"
        f"  python       : {sys.version.split()[0]}\n\n"
        "If requests IS installed, it belongs to a different interpreter than the\n"
        "one above. Install it into this one:\n\n"
        f"    {sys.executable} -m pip install requests\n\n"
        "Other things that cause this:\n"
        "  * a file or folder named requests.py / requests/ beside this script,\n"
        "    which shadows the real package\n"
        "  * an active virtualenv that lacks requests (deactivate, or install it)\n"
        "  * a broken install whose dependencies fail to import — the reason line\n"
        "    above will name the offending module if so\n\n"
        "Run with --check-env for a full diagnostic.\n"
    )
    sys.exit(1)

BASE_URL = os.environ.get("FLEET_BASE_URL", "https://fleet.shiftiq.us")

# ── CATEGORY CODES ────────────────────────────────────────────────────────
# keys are case-insensitive. Override or extend with --rules pointing at
# {"codes": {"RC4": "...", ...}, "keywords": {"boba": "Hospitality", ...}}

# ──────────────────────────────────────────────────────────────────────────
#                    EDIT HERE
# ──────────────────────────────────────────────────────────────────────────

CATEGORY_CODES: dict[str, str] = {
    "RC4": "Retail and consumer goods",
    "XO4": "X",                                  # random tasks
    "ET4": "Education and training",
    "IM4": "Industrial manufacturing",
    "BP4": "Beauty and personal care",
    "HO4": "Hospitality",
    "AT4": "Automotive and transport",
    "LS4": "Laboratory / scientific",
    "CH4": "Construction and hardware",
    "BY4": "Backyard / yard",
    "SR4": "Sports and recreation",
    "PD4": "Printing and design",
    "AF4": "Agriculture and farming",
}

# ──────────────────────────────────────────────────────────────────────────
#                    EDIT HERE
# ──────────────────────────────────────────────────────────────────────────

# Optional second pass for task names carrying no code. Mapping
# "Gaskets" or "Boba" to a category is domain knowledge this script
# should not invent. Fill it in here or via --rules.
CATEGORY_KEYWORDS: dict[str, str] = {}

# put whatever you want for the uncategorized category
UNCATEGORIZED = "none"


# ─────────────────────────────────────────────────────────────────────────
# Fleet API client
#
# standalone copy of the fleet API client, so it can run on machine
# ─────────────────────────────────────────────────────────────────────────
class FleetAPIError(Exception):
    pass


class _EmbeddedClient:
    def __init__(self, base_url: str = BASE_URL, timeout: float = 10.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "categorize-tasks/1.0"})

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
        url = f"{self.base_url}/proxy/{hostname}/statusboard-api/mcap-sync/sessions"
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


def make_client(base_url: str):
    """Prefers the repo's api_client.py; falls back to the embedded copy."""
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    try:
        from api_client import FleetAPIClient        # type: ignore
        return FleetAPIClient(base_url=base_url), "api_client.py"
    except Exception:
        return _EmbeddedClient(base_url), "embedded client"


# ── Configuration ────────────────────────────────────────────────────────
def load_config(config_file: str = "categorize_config.txt") -> dict:
    """Load config from file, or prompt user if file doesn't exist.

    Config file format:
        email=user@example.com
        password=mypassword
        days=07/30,07/29,07/25
        min_duration=00:05:00
        include_uncategorized=n
        append=n
        keep_columns=category,task_name
        sheets_output=n
    """
    config = {
        "email": "",
        "password": "",
        "days": "all",
        "min_duration": "",
        "include_uncategorized": "n",
        "append": "n",
        "keep_columns": "",
        "sheets_output": "n",
    }

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

    # Prompt user for config
    print(f"\nNo {config_file} found. Enter configuration:\n", file=sys.stderr)

    config["email"] = input("Email: ").strip()
    config["password"] = getpass.getpass("Password: ")

    days_input = input("Days to include (comma-separated, e.g. 07/30,07/29 or 'all' for all): ").strip()
    config["days"] = days_input if days_input else "all"

    while True:
        min_input = input("Minimum session length as hh:mm:ss (blank for no minimum): ").strip()
        try:
            parse_hhmmss(min_input)
        except ValueError as e:
            print(f"  {e}", file=sys.stderr)
            continue
        config["min_duration"] = min_input
        break

    uncat_input = input("Include uncategorized? (y/n, default=n): ").strip().lower()
    config["include_uncategorized"] = "y" if uncat_input == "y" else "n"

    append_input = input("Append mode? (y/n, default=n): ").strip().lower()
    config["append"] = "y" if append_input == "y" else "n"

    # Optionally save config
    save = input(f"Save to {config_file}? (y/n, default=n): ").strip().lower()
    if save == "y":
        try:
            with open(config_file, "w") as f:
                f.write(f"email={config['email']}\n")
                f.write(f"password={config['password']}\n")
                f.write(f"days={config['days']}\n")
                f.write(f"min_duration={config['min_duration']}\n")
                f.write(f"include_uncategorized={config['include_uncategorized']}\n")
                f.write(f"append={config['append']}\n")
                f.write(f"keep_columns={config['keep_columns']}\n")
                f.write(f"sheets_output={config['sheets_output']}\n")
            print(f"Config saved to {config_file}", file=sys.stderr)
        except Exception as e:
            print(f"Error saving {config_file}: {e}", file=sys.stderr)

    return config


def parse_days_filter(days_str: str) -> set[str] | None:
    """Parse days string into set of dates (MM/DD format), or None for all."""
    if days_str.lower() == "all" or not days_str.strip():
        return None
    return set(d.strip() for d in days_str.split(",") if d.strip())


# ── Terminal login ────────────────────────────────────────────────────────
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
            continue

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
            # Keep the address and re-prompt for the password only.

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
    from. Run this first on a new machine — it settles the "but I installed it"
    class of problem in one command."""
    print("interpreter :", sys.executable)
    print("python      :", sys.version.split()[0])
    print("platform    :", sys.platform)
    print("script      :", os.path.abspath(__file__))
    print("cwd         :", os.getcwd())
    print(f"requests    : {requests.__version__}  ({os.path.dirname(requests.__file__)})")

    # A shadowing module is silent and baffling, so name it explicitly.
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

    # Actually exercise the library rather than trusting that the import worked.
    # A shadowing stub imports fine and only fails later, mid-run.
    try:
        requests.Session()
    except Exception as e:
        problems.append(f"requests.Session() failed: {type(e).__name__}: {e}")

    try:
        make_client(BASE_URL)
        print("client      :", make_client(BASE_URL)[1])
    except Exception as e:
        problems.append(f"could not build a fleet client: {type(e).__name__}: {e}")

    if problems:
        print("\n!! this environment will NOT work:", file=sys.stderr)
        for p in problems:
            print("   * " + p, file=sys.stderr)
        print("\n   If a file above is shadowing requests, rename or delete it,\n"
              "   or run the script from a different directory.", file=sys.stderr)
        return 1

    print("\nEnvironment looks usable.")
    return 0


def _ask(prompt: str, default: str = "n") -> str:
    try:
        return (input(prompt).strip().lower() or default)
    except (EOFError, KeyboardInterrupt):
        return default


# ── Fetch ─────────────────────────────────────────────────────────────────
def _records_for_host(host: str, label: str, groups: list) -> list[dict]:
    """One flat record per session with a positive duration, from one
    device's raw session groups. Shared by the live fetch and --from-json
    (see load_sessions_from_json), so a session read back from a saved
    payload gets identical treatment to one just pulled from the API.
    """
    out = []
    for rec in groups or []:
        dur = float(rec.get("duration_s") or 0)
        if dur <= 0:
            continue
        out.append({
            "session_id": (rec.get("id") or rec.get("session_uuid")
                           or f"{host}|{rec.get('name')}"),
            "pi_id": host,
            "pi_name": label,
            "operator": rec.get("operator"),
            "task": rec.get("task"),
            "folder": rec.get("name"),
            "location": rec.get("location") or rec.get("environment"),
            "duration_s": dur,
            "start_unix": _start_unix(rec),
            # Everything the API returned, kept verbatim so the CSV can
            # carry every field rather than a curated subset.
            "_raw": rec,
        })
    return out


def _dedupe_by_session_id(records: list[dict]) -> list[dict]:
    """One device can report the same session twice across cameras."""
    unique, seen = [], set()
    for r in records:
        if r["session_id"] in seen:
            continue
        seen.add(r["session_id"])
        unique.append(r)
    return unique


def load_sessions_from_json(path: str):
    """Rebuilds (records, stats, payload) from a raw JSON file saved earlier
    by --json/--json-out, instead of hitting the fleet API — see --from-json.
    `payload` is returned as loaded, so re-running with --json still saves an
    exact copy if the caller wants one at a different path.
    """
    with open(path, encoding="utf-8") as f:
        payload = json.load(f)
    devices = payload.get("devices") or []
    labels = {d.get("hostname"): d.get("display_name") or d.get("hostname")
              for d in devices if d.get("hostname")}
    raw_by_host = payload.get("sessions_by_device") or {}

    records = []
    for host, groups in raw_by_host.items():
        records.extend(_records_for_host(host, labels.get(host, host), groups))
    unique = _dedupe_by_session_id(records)

    unreachable = payload.get("unreachable") or []
    stats = {
        "devices": len(devices),
        "failed": len(unreachable),
        "failed_502": 0,
        "failed_timeout": 0,
        "dupes": len(records) - len(unique),
        "unreachable_hosts": {u.get("device") for u in unreachable if u.get("device")},
    }
    return unique, stats, payload


def fetch_sessions(client, limit: int, light: bool, workers: int):
    """Pulls the device list, then every device's completed sessions.

    Returns (records, stats). Offline rigs fail their proxy hop; those are
    counted and reported rather than aborting the run.
    """
    devices = client.get_fleet_status()
    if devices is None:
        sys.exit("Could not read the fleet status — the session may have expired.")
    if not devices:
        sys.exit("The fleet API returned no devices.")

    targets = [(d.get("hostname"), d.get("display_name") or d.get("hostname"))
               for d in devices if d.get("hostname")]
    print(f"Fetching sessions from {len(targets)} device(s)…", file=sys.stderr)

    records: list[dict] = []
    raw_by_host: dict[str, list] = {}      # verbatim API payloads, for the .json
    failures: list[tuple[str, str]] = []
    failures_502: list[str] = []
    failures_timeout: list[str] = []
    unreachable_hosts: set[str] = set()    # hostnames whose fetch failed entirely
    done = 0
    lock = threading.Lock()

    def work(host, label):
        return host, label, client.get_device_sessions(host, light=light, limit=limit)

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(work, h, l): (h, l) for h, l in targets}
        for fut in as_completed(futs):
            host, label = futs[fut]
            try:
                host, label, groups = fut.result()
            except Exception as e:
                with lock:
                    err_str = str(e).lower()
                    unreachable_hosts.add(host)
                    if "502" in err_str:
                        failures_502.append(label)
                    elif "timeout" in err_str or "unreachable" in err_str:
                        failures_timeout.append(label)
                    else:
                        failures.append((label, str(e)))
                groups = []
            with lock:
                done += 1
                sys.stderr.write(f"\r  {done}/{len(targets)} devices, "
                                 f"{len(records)} sessions")
                sys.stderr.flush()
                raw_by_host[host] = groups or []
                records.extend(_records_for_host(host, label, groups))
    sys.stderr.write("\n")
    if failures_502:
        print(f"  ! {len(failures_502)} device(s) returned 502 error", file=sys.stderr)
    if failures_timeout:
        print(f"  ! {len(failures_timeout)} device(s) timed out or unreachable", file=sys.stderr)
    for label, err in failures:
        print(f"  ! {label}: {err}", file=sys.stderr)

    unique = _dedupe_by_session_id(records)

    payload = {
        "fetched_at": dt.datetime.now().isoformat(timespec="seconds"),
        "base_url": getattr(client, "base_url", ""),
        "devices": devices,
        "sessions_by_device": raw_by_host,
        "counts": {
            "devices": len(targets),
            "unreachable": len(failures),
            "sessions_returned": sum(len(v) for v in raw_by_host.values()),
            "sessions_kept": len(unique),
        },
        "unreachable": [{"device": l, "error": e} for l, e in failures],
    }
    return unique, {
        "devices": len(targets),
        "failed": len(failures) + len(failures_502) + len(failures_timeout),
        "failed_502": len(failures_502),
        "failed_timeout": len(failures_timeout),
        "dupes": len(records) - len(unique),
        "unreachable_hosts": unreachable_hosts,
    }, payload


def _start_unix(rec: dict):
    """Session start as unix time: the API field, else the YYYYMMDD_HHMMSS
    folder name, which is sometimes the only time information present."""
    raw = rec.get("start_time_unix") or rec.get("mtime") or 0
    if raw:
        try:
            return int(float(raw))
        except Exception:
            pass
    m = re.match(r"^(\d{8})_(\d{6})", str(rec.get("name", "")))
    if m:
        try:
            return int(dt.datetime.strptime(m.group(1) + m.group(2),
                                            "%Y%m%d%H%M%S").timestamp())
        except Exception:
            pass
    return None


# ── Categorizing ──────────────────────────────────────────────────────────
def build_matchers(codes: dict[str, str]):
    """One whole-token, case-insensitive regex per code.

    Flexible matching: allows spaces and hyphens between characters.
    Recognizes 0 as O (e.g., X0 matches XO).
    IM4, iM4, IM4-1234, IM4 - 1234, IM41234, X04 (as XO4) all match.
    """
    out = []
    for code, name in codes.items():
        c = str(code).strip()
        if not c:
            continue
        # Build regex with optional space/hyphen between each character
        # Also allow 0 to match O and vice versa
        # IM4 becomes I[\s\-]*M[\s\-]*4
        # XO4 becomes X[\s\-]*[O0][\s\-]*4 to match both XO4 and X04
        parts = []
        for char in c:
            if char.upper() == "O":
                # Allow 0 or O
                parts.append(r"[O0]")
            elif char == "0":
                # Allow 0 or O
                parts.append(r"[O0]")
            else:
                parts.append(re.escape(char))
        flexible_code = "[\\s\\-]*".join(parts)
        rx = re.compile(rf"(?<![A-Za-z0-9]){flexible_code}(?![A-Za-z0-9])", re.IGNORECASE)
        out.append((c.upper(), name, rx))
    return out


def categorize(task: str, matchers, keywords: dict[str, str]):
    """Returns (code, category_name, matched_by, codes).

    `codes` is the list of every code that matched, which is what drives the
    per-category boolean columns. Case-insensitive throughout.
    """
    t = (task or "").strip()
    if not t:
        return "", UNCATEGORIZED, "none", []

    hits = [(code, name) for code, name, rx in matchers if rx.search(t)]
    if len(hits) == 1:
        return hits[0][0], hits[0][1], "code", [hits[0][0]]
    if len(hits) > 1:
        # Ambiguous — report every code rather than silently choosing one.
        return ("+".join(sorted(c for c, _ in hits)),
                " + ".join(sorted(n for _, n in hits)),
                "multiple-codes",
                sorted(c for c, _ in hits))

    low = t.casefold()
    for kw, name in keywords.items():
        if str(kw).casefold() in low:
            return "", name, f"keyword:{kw}", []
    return "", UNCATEGORIZED, "none", []


def bool_columns(codes: dict[str, str]) -> list[str]:
    """is_rc4, is_xo4, … one per active category code, in declaration order."""
    return ["is_" + c.lower() for c in codes]


def code_flags(row_codes, category: str, codes: dict[str, str]) -> dict[str, bool]:
    """Which category booleans are true for one session.

    A session counts as belonging to a category either because its task name
    carried the code, or because a keyword rule resolved it to that category's
    name. Without the second half, keyword-mapped sessions would show a category
    but leave every boolean false — and right now nearly all of your history is
    keyword-mapped rather than coded.
    """
    matched = {str(c).upper() for c in (row_codes or [])}
    out = {}
    for code, name in codes.items():
        out["is_" + code.lower()] = (code.upper() in matched) or (
            category != UNCATEGORIZED and category == name)
    return out


# ── Formatting ────────────────────────────────────────────────────────────
def hhmmss(seconds: float) -> str:
    s = max(0, int(round(float(seconds or 0))))
    return f"{s // 3600:02d}:{(s % 3600) // 60:02d}:{s % 60:02d}"


def parse_hhmmss(text: str) -> float:
    """hh:mm:ss -> seconds. The inverse of hhmmss(), for config/CLI input.

    Written to read back anything hhmmss() emits, plus the shorter forms a
    person actually types: "01:30:00", "1:30:00", "5:00" (five minutes), "90"
    (ninety seconds). Minutes and seconds may exceed 59 - "90:00" is the same
    ninety minutes it looks like - because clamping there would silently
    reinterpret the filter rather than apply it. Blank means no minimum.
    """
    s = (text or "").strip()
    if not s:
        return 0.0
    parts = s.split(":")
    if len(parts) > 3:
        raise ValueError(f"{s!r} is not a duration (want hh:mm:ss)")
    try:
        nums = [float(p) for p in parts]
    except ValueError:
        raise ValueError(f"{s!r} is not a duration (want hh:mm:ss)") from None
    if any(n < 0 for n in nums):
        raise ValueError(f"{s!r} is not a duration (want hh:mm:ss)")
    total = 0.0
    for n in nums:                      # left to right: h, m, s (or m, s / s)
        total = total * 60 + n
    return total


def size_label(num_bytes) -> str:
    """Human-readable size. Binary units, matching how the rigs report disk."""
    try:
        b = float(num_bytes)
    except (TypeError, ValueError):
        return ""
    if b <= 0:
        return ""
    for unit in ("B", "KB", "MB", "GB", "TB", "PB"):
        if b < 1024 or unit == "PB":
            return f"{b:.0f} {unit}" if unit == "B" else f"{b:.1f} {unit}"
        b /= 1024
    return ""


def _num(d: dict, keys):
    for k in keys:
        v = d.get(k)
        if isinstance(v, bool):
            continue
        if isinstance(v, (int, float)):
            return float(v)
        if isinstance(v, str):
            try:
                return float(v)
            except ValueError:
                pass
    return None


# field names vary between rigs
_BYTES_KEYS = (
    "total_bytes", "bytes_total", "size_bytes", "total_size_bytes", "total_size",
    "bytes", "size", "mcap_bytes", "file_bytes", "disk_bytes", "byte_count",
    "recorded_bytes", "session_bytes",
)
_UPLOAD_KEYS = (
    "upload_status", "sync_status", "upload_state", "sync_state",
    "status", "state", "upload", "sync",
)
_UPLOADED_FLAGS = ("uploaded", "synced", "upload_complete", "sync_complete", "is_synced")


def extract_total_bytes(rec: dict):
    """Session size in bytes, summing nested file lists when necessary."""
    v = _num(rec, _BYTES_KEYS)
    if v is not None:
        return int(v)
    for sub in ("files", "recordings", "cameras", "mcaps"):
        items = rec.get(sub)
        if isinstance(items, list) and items:
            total = 0.0
            found = False
            for it in items:
                if isinstance(it, dict):
                    n = _num(it, _BYTES_KEYS)
                    if n is not None:
                        total += n
                        found = True
            if found:
                return int(total)
    for sub in ("summary", "sync", "upload", "storage"):
        d = rec.get(sub)
        if isinstance(d, dict):
            n = _num(d, _BYTES_KEYS)
            if n is not None:
                return int(n)
    return None


# A session's duration_s is start_time_unix -> end_time_unix: the wall-clock
# span the folder was open for. Usually that's also how long the rig actually
# recorded — but a hang, a stuck upload, or a late finalize can leave the span
# far longer than what actually got captured, while the file sizes still
# reflect the real recording. MIN_BASELINE_DURATION_S / _SESSIONS keep short or
# sparse rigs from producing a noisy bytes/second baseline; RATIO is how far
# below the wall clock the byte-implied duration has to fall before we trust
# it over duration_s.
MIN_BASELINE_DURATION_S = 120.0
MIN_BASELINE_SESSIONS = 5
DURATION_MISMATCH_RATIO = 0.75


def build_byte_rate_baselines(raw: list[dict]) -> tuple[dict[str, float], float | None]:
    """Median bytes/second per rig, learned from that rig's own sessions.

    Rigs with too few qualifying sessions fall back to the fleet-wide median
    (second return value; None if nothing qualified at all).
    """
    by_host: dict[str, list[float]] = defaultdict(list)
    for rec in raw:
        dur = rec.get("duration_s") or 0
        if dur < MIN_BASELINE_DURATION_S:
            continue
        nbytes = extract_total_bytes(rec.get("_raw") or {})
        if not nbytes:
            continue
        by_host[rec.get("pi_id")].append(nbytes / dur)

    host_rates: dict[str, float] = {}
    all_rates: list[float] = []
    for host, rates in by_host.items():
        all_rates.extend(rates)
        if len(rates) >= MIN_BASELINE_SESSIONS:
            host_rates[host] = statistics.median(rates)
    fleet_rate = statistics.median(all_rates) if all_rates else None
    return host_rates, fleet_rate


def extract_upload_status(rec: dict) -> str:
    """Upload/sync state as a short label.

    Handles three shapes: an explicit status string, a boolean 'uploaded' flag,
    and a synced/total file count pair.
    """
    for k in _UPLOAD_KEYS:
        v = rec.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
        if isinstance(v, dict):
            for kk in _UPLOAD_KEYS:
                vv = v.get(kk)
                if isinstance(vv, str) and vv.strip():
                    return vv.strip()

    for k in _UPLOADED_FLAGS:
        v = rec.get(k)
        if isinstance(v, bool):
            return "uploaded" if v else "pending"

    synced = _num(rec, ("synced_files", "uploaded_files", "files_synced", "files_uploaded"))
    total = _num(rec, ("total_files", "file_count", "files_total", "num_files"))
    if synced is not None and total:
        if synced >= total:
            return "uploaded"
        return f"partial {int(synced)}/{int(total)}"
    if synced is not None:
        return f"{int(synced)} file(s) synced"

    pending = _num(rec, ("pending_files", "files_pending", "upload_queue", "queue"))
    if pending is not None:
        return "uploaded" if pending == 0 else f"pending {int(pending)}"
    return ""


def excel_safe(value):
    """Neutralises CSV formula injection because Excel is annoying"""
    if isinstance(value, SheetsFormula):
        return value
    if isinstance(value, str) and value[:1] in ("=", "+", "-", "@"):
        return "'" + value
    return value


UNNAMED_PI = re.compile(r"^rpi\d*-[0-9a-f]{4}-[0-9a-f]{4}$", re.IGNORECASE)


def enrich(rec: dict, matchers, keywords,
           host_rates: dict[str, float] | None = None,
           fleet_rate: float | None = None) -> dict:
    task = (rec.get("task") or "").strip()
    code, category, matched_by, hit_codes = categorize(task, matchers, keywords)
    dur = float(rec.get("duration_s") or 0)

    date = weekday = is_weekend = start_time = end_time = start_iso = ""
    if rec.get("start_unix"):
        try:
            start = dt.datetime.fromtimestamp(float(rec["start_unix"]))
            end = start + dt.timedelta(seconds=dur)
            date = start.strftime("%Y-%m-%d")
            weekday = start.strftime("%A")
            is_weekend = "yes" if start.weekday() >= 5 else "no"
            start_time = start.strftime("%H:%M:%S")
            end_time = end.strftime("%H:%M:%S")
            start_iso = start.isoformat(timespec="seconds")
        except Exception:
            pass

    raw = rec.get("_raw") or {}
    nbytes = extract_total_bytes(raw)

    # duration_s (the wall-clock span between session start and end) can be
    # much longer than what actually got recorded (a rig that hung, or
    # finalized/uploaded long after recording really stopped). File size is a
    # much harder thing to fake, so when the rig's own bytes/second rate
    # implies a much shorter recording than duration_s claims, trust the
    # bytes instead — duration_seconds/hhmmss/hours below are that corrected
    # figure.
    dur_actual = dur
    implied = None
    rate = (host_rates or {}).get(rec.get("pi_id")) or fleet_rate
    if rate and nbytes and dur > 0:
        implied = nbytes / rate
        if implied < dur * DURATION_MISMATCH_RATIO:
            dur_actual = implied

    session_id = rec.get("session_id") or ""
    link = f"https://ops.usephysical.ai/sessions/{session_id}" if session_id else ""
    return {
        "row_type": "SESSION",
        "date": date,
        "pi_id": rec.get("pi_id") or "",
        "pi_name": rec.get("pi_name") or "",
        "operator": (rec.get("operator") or "Unknown").strip(),
        "task_name": task or "(no task)",
        "task_prefix": (task or "(no task)")[:8],
        "category": category,
        "link": link,
        "is_categorized": "no" if category == UNCATEGORIZED else "yes",
        "duration_seconds": round(dur_actual, 2),
        "duration_hhmmss": hhmmss(dur_actual),
        "duration_hours": round(dur_actual / 3600, 4),
        "location": (rec.get("location") or "").strip(),
        "start_time": start_time,
        "end_time": end_time,
        "session_id": session_id,
        "total_bytes": "" if nbytes is None else nbytes,
        "size_label": size_label(nbytes),
        "upload_status": extract_upload_status(raw),

        # Appended at the very right of the CSV (see FIELDS). duration_s is
        # the untouched wall-clock span the correction above starts from;
        # duration_implied is the byte-rate estimate, present whenever it
        # could be computed even if it wasn't trusted over the wall clock.
        # yield_pct is never computed here — it's left blank for a human to
        # fill in against duration_implied/duration_s.
        "duration_implied": "" if implied is None else round(implied, 2),
        "duration_s": dur,
        "yield_pct": "",

        # not in the CSV by default but used for sorting, the aggregate row
        # and the optional raw.* passthrough.
        "_category_code": code,
        "_codes": hit_codes,
        "_matched_by": matched_by,
        "_start_iso": start_iso,
        "_weekday": weekday,
        "_is_weekend": is_weekend,
        "_raw": raw,
    }


def raw_columns(rows: list[dict]) -> list[str]:
    """Saves all fields."""
    keys: set[str] = set()
    for r in rows:
        keys.update((r.get("_raw") or {}).keys())
    return sorted(keys)


def flatten_raw(row: dict, cols: list[str]) -> dict:
    """Adds one raw.<field> column per API field. Nested values are JSON-encoded
    so a dict never collapses into an unreadable Python repr."""
    raw = row.get("_raw") or {}
    out = {}
    for k in cols:
        v = raw.get(k)
        if isinstance(v, (dict, list)):
            v = json.dumps(v, separators=(",", ":"), ensure_ascii=False)
        out["raw." + k] = "" if v is None else v
    return out


FIELDS = [
    "row_type", "date", "duration_hhmmss", "pi_name", "operator", "task_name",
    "task_prefix",
    "category", "link", "start_time", "end_time", "duration_hours",
    "duration_seconds",
    "pi_id", "is_categorized", "session_id",
    "total_bytes", "size_label", "upload_status",
]


def bool_totals(rows: list[dict], codes: dict[str, str], hours: bool = False) -> dict:
    """Column-wise total for each is_<code>: session count, or hours."""
    out = {}
    for code in codes:
        col = "is_" + code.lower()
        hits = [r for r in rows if r.get(col)]
        if hours:
            out[col] = round(sum(r["duration_seconds"] for r in hits) / 3600, 2) or ""
        else:
            out[col] = len(hits) or ""
    return out


def hours_row(rows: list[dict], codes: dict[str, str]) -> dict:
    """Second totals line: hours rather than session counts."""
    total = sum(r["duration_seconds"] for r in rows)
    row = {
        "row_type": "TOTAL_HOURS",
        "date": "hours per category ->",
        "duration_hhmmss": hhmmss(total),
        "pi_name": "", "operator": "",
        "task_name": "", "task_prefix": "", "category": "", "link": "", "is_categorized": "",
        "duration_seconds": round(total, 2),
        "duration_hours": round(total / 3600, 2),
        "start_time": "", "end_time": "", "pi_id": "", "session_id": "",
        "total_bytes": "", "size_label": "", "upload_status": "",
    }
    row.update(bool_totals(rows, codes, hours=True))
    return row


def category_tags_row(codes: dict[str, str]) -> dict:
    """Human-readable category names row."""
    row = {
        "row_type": "CATEGORY_TAGS",
        "date": "category tags ->",
        "duration_hhmmss": "",
        "pi_name": "", "operator": "",
        "task_name": "", "task_prefix": "", "category": "", "link": "", "is_categorized": "",
        "duration_seconds": "", "duration_hours": "",
        "start_time": "", "end_time": "", "pi_id": "", "session_id": "",
        "total_bytes": "", "size_label": "", "upload_status": "",
    }
    for code, name in codes.items():
        row["is_" + code.lower()] = name
    return row


def aggregate_row(rows: list[dict], email: str) -> dict:
    total = sum(r["duration_seconds"] for r in rows)
    cats = {r["category"] for r in rows if r["category"] != UNCATEGORIZED}
    uncat = sum(1 for r in rows if r["category"] == UNCATEGORIZED)
    dates = sorted({r["date"] for r in rows if r["date"]})
    pct = (len(rows) - uncat) / len(rows) * 100 if rows else 0.0
    total_bytes = sum(int(r["total_bytes"]) for r in rows
                      if str(r["total_bytes"]).strip() not in ("", "None"))
    return {
        "row_type": "TOTAL",
        "date": f"{dates[0]} .. {dates[-1]}" if dates else "",
        "duration_hhmmss": hhmmss(total),
        "pi_name": f"{len({r['pi_name'] for r in rows})} names",
        "operator": f"{len({r['operator'] for r in rows})} operators",
        "task_name": (f"{len(rows)} sessions / "
                      f"{len({r['task_name'] for r in rows})} distinct tasks"),
        "task_prefix": "",
        "category": f"{len(cats)} categories",
        "link": "",
        "is_categorized": f"{pct:.1f}% ({uncat} uncategorized)",
        "duration_seconds": round(total, 2),
        "duration_hours": round(total / 3600, 4),
        "start_time": "",
        "end_time": dt.datetime.now().isoformat(timespec="seconds"),
        "pi_id": f"{len({r['pi_id'] for r in rows})} pis",
        "session_id": f"pulled live by {email}",
        "total_bytes": total_bytes or "",
        "size_label": size_label(total_bytes),
        "upload_status": "AGGREGATE",
    }


def category_rows(rows: list[dict]) -> list[dict]:
    by = defaultdict(list)
    for r in rows:
        by[(r["_category_code"], r["category"])].append(r)
    grand = sum(r["duration_seconds"] for r in rows) or 1.0
    out = []
    for (code, name), items in by.items():
        total = sum(i["duration_seconds"] for i in items)
        out.append({
            "category_code": code,
            "category_name": name,
            "sessions": len(items),
            "distinct_tasks": len({i["task_name"] for i in items}),
            "distinct_operators": len({i["operator"] for i in items}),
            "distinct_pis": len({i["pi_id"] for i in items}),
            "duration_hhmmss": hhmmss(total),
            "duration_seconds": round(total, 2),
            "duration_hours": round(total / 3600, 4),
            "pct_of_total_time": round(total / grand * 100, 2),
        })
    out.sort(key=lambda r: (-r["duration_seconds"], r["category_name"]))
    return out


def read_existing_session_ids(path: str) -> set[str]:
    """Read session_ids from existing CSV file."""
    session_ids = set()
    if not os.path.exists(path):
        return session_ids
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("session_id"):
                    session_ids.add(row["session_id"])
    except Exception as e:
        print(f"Warning: could not read existing sessions from {path}: {e}", file=sys.stderr)
    return session_ids


def read_existing_rows(path: str) -> dict[str, dict[str, str]]:
    """Existing SESSION rows keyed by session_id, for keep_columns merging."""
    by_id: dict[str, dict[str, str]] = {}
    if not os.path.exists(path):
        return by_id
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                sid = row.get("session_id")
                if sid and row.get("row_type") == "SESSION":
                    by_id[sid] = row
    except Exception as e:
        print(f"Warning: could not read existing rows from {path}: {e}", file=sys.stderr)
    return by_id


def coerce_like(computed, preserved: str):
    """A preserved cell, put back in the type the pipeline computed.

    Everything read off a CSV is a string, but a column this script computes
    as a number is summed, sorted and averaged downstream - by the totals, by
    category_rows(), by the run summary. Handing those a "7.0" that looks
    numeric but isn't either raises TypeError or, worse, sorts and compares as
    text. Non-numeric columns pass through as the strings they already were.
    """
    if isinstance(computed, bool) or not isinstance(computed, (int, float)):
        return preserved
    try:
        return type(computed)(float(preserved))
    except (TypeError, ValueError):
        return computed          # unparseable: the fresh number beats a wrong type


def apply_keep_columns(rows: list[dict], keep_columns: list[str],
                        existing: dict[str, dict[str, str]]) -> int:
    """Overwrites `keep_columns` on each SESSION row with its prior value from
    `existing` (as returned by read_existing_rows), for rows that already
    exist there (matched by session_id).

    Lets a hand-edited column (a corrected category, a manually verified
    duration, ...) survive a full rewrite instead of being recomputed every
    run. Everything not listed in keep_columns is still refreshed as normal.
    Returns how many rows were touched.

    SESSION rows only, deliberately: keep_columns protects a *measurement*,
    never a total derived from one. Naming duration_seconds here means "keep
    the duration I corrected on this session", not "freeze the fleet total" -
    the total's whole job is to add up whatever the session rows now say, so
    it gets recomputed from them afterwards either way (see
    refresh_aggregate_rows). Letting a kept column reach TOTAL/TOTAL_HOURS
    would strand the header at a number nothing under it adds up to.
    """
    if not keep_columns or not existing:
        return 0
    touched = 0
    for r in rows:
        if r.get("row_type") != "SESSION":
            continue
        old = existing.get(r.get("session_id"))
        if not old:
            continue
        for col in keep_columns:
            if col in old:
                r[col] = coerce_like(r.get(col), old[col])
        touched += 1
    return touched


def merge_full_rewrite(rows: list[dict], path: str, keep_columns: list[str]) -> tuple[int, int]:
    """Applies a full rewrite's two carry-overs against `path`'s own existing
    content, in place on `rows`: keep_columns survive instead of being
    recomputed, and any session `path` already has that isn't in `rows` this
    run gets appended untouched rather than dropped. Returns
    (keep_columns rows touched, sessions preserved this way).
    """
    existing = read_existing_rows(path)
    touched = apply_keep_columns(rows, keep_columns, existing) if keep_columns else 0
    fresh_ids = {r.get("session_id") for r in rows if r.get("row_type") == "SESSION"}
    preserved = 0
    for sid, old in existing.items():
        if sid not in fresh_ids:
            rows.append(old)
            preserved += 1
    return touched, preserved


def col_letter(n: int) -> str:
    """1-indexed column number -> A1 letter(s): 1 -> A, 27 -> AA."""
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def sheets_code_pattern(code: str) -> str:
    """Plain case-insensitive substring match for a Sheets REGEXMATCH
    formula - simpler than build_matchers()'s flexible Python regex
    (no O/0 interchange, no optional space/hyphen, no word boundary), by
    request: keep the formulas already in task_report_sheets.csv as they are."""
    return f"(?i){code}"


class SheetsFormula(str):
    """A cell value that's an intentional formula. write_csv's excel_safe()
    neutralizes any value starting with '=' by default (CSV/formula
    injection guard) - this marks the handful of cells below where that
    leading '=' is deliberate, so it passes through untouched."""


def build_sheets_rows(out_rows: list[dict], fields: list[str], boolcols: list[str],
                       codes: dict[str, str], start_row: int = 2) -> list[dict]:
    """A copy of out_rows for task_report_sheets.csv: on every SESSION row,
    task_prefix/category/is_categorized/is_<code> become live Sheets formulas
    driven off that row's task_name, instead of the value this run happened
    to compute. Sheets evaluates a cell as a formula the moment the CSV is
    imported (any cell starting with '='), so once it's in Sheets, correcting
    task_name by hand is the only edit needed - everything else recalculates.

    The category formula reads its code -> name lookup off the CATEGORY_TAGS
    row, which read_csv/main() always place at sheet row 4 (row 1 is the
    header, rows 2-4 are TOTAL/TOTAL_HOURS/CATEGORY_TAGS) - so this only
    makes sense when the aggregate rows are present.

    start_row is where out_rows[0] actually lands in the file: 2 for a fresh
    write, but higher when appending below rows that already exist there -
    otherwise the formulas embedded in an appended row would reference
    whatever cells happen to sit at row 2 instead of the row they're really
    on.
    """
    col = {name: col_letter(i + 1) for i, name in enumerate(fields)}
    tn = col["task_name"]
    cat = col["category"]
    names_range = f"${col[boolcols[0]]}$4:${col[boolcols[-1]]}$4"

    sheets_rows = []
    for i, r in enumerate(out_rows):
        row_num = start_row + i
        new = dict(r)
        if r.get("row_type") == "SESSION":
            f_cell = f"{tn}{row_num}"
            bool_range = f"{col[boolcols[0]]}{row_num}:{col[boolcols[-1]]}{row_num}"
            for code, colname in zip(codes, boolcols):
                pattern = sheets_code_pattern(code)
                new[colname] = SheetsFormula(f'=REGEXMATCH({f_cell},"{pattern}")')
            new["task_prefix"] = SheetsFormula(f"=LEFT({f_cell},8)")
            new["category"] = SheetsFormula(
                f'=XLOOKUP(TRUE,{bool_range},{names_range},"none")'
            )
            new["is_categorized"] = SheetsFormula(f'=IF({cat}{row_num}="none","no","yes")')
        sheets_rows.append(new)
    return sheets_rows


def count_csv_rows(path: str) -> int:
    """How many CSV records `path` already holds, header included.

    Counts real records rather than physical lines: a quoted field can carry
    an embedded newline and span several lines (task names do get pasted in
    with one), and counting lines there would overstate the row count and
    push every appended formula below the row it is meant to sit on.
    """
    try:
        with open(path, newline="", encoding="utf-8-sig") as f:
            return sum(1 for _ in csv.reader(f))
    except OSError as e:
        print(f"Warning: could not measure {path}: {e}", file=sys.stderr)
        return 0


def ends_with_newline(path: str) -> bool:
    """Whether `path` already ends in a line break. An empty (or unreadable)
    file counts as fine — there is nothing for a new row to collide with."""
    try:
        with open(path, "rb") as f:
            if f.seek(0, os.SEEK_END) == 0:
                return True
            f.seek(-1, os.SEEK_END)
            return f.read(1) in (b"\n", b"\r")
    except OSError:
        return True


def flag_from_cell(value: str, code: str, task_name: str) -> bool:
    """Whether an is_<code> cell read back off disk is set.

    Two shapes reach here. task_report.csv holds the literal this script wrote
    (TRUE/FALSE, or 1/0 under --bool-1-0). task_report_sheets.csv holds the
    REGEXMATCH formula instead, and a formula's *value* only exists once
    Sheets evaluates it - it is never in the file - so re-apply the rule the
    formula encodes (sheets_code_pattern: a plain case-insensitive search for
    the code in the task name) rather than reading a result that isn't there.
    """
    v = (value or "").strip()
    if v.startswith("="):
        return bool(re.search(code, task_name or "", re.IGNORECASE))
    return v.upper() in ("TRUE", "1")


def rows_for_totals(path: str, codes: dict[str, str]) -> list[dict]:
    """Every SESSION row `path` currently holds, coerced back into the shape
    aggregate_row/hours_row/bool_totals want: numeric duration and bytes, real
    booleans for is_<code>, and a category that is a name rather than a
    formula. Everything else is passed through as the strings on disk, which
    is all those three read it as anyway.
    """
    out: list[dict] = []
    try:
        with open(path, newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                if row.get("row_type") != "SESSION":
                    continue
                task = row.get("task_name") or ""
                r = dict(row)
                try:
                    r["duration_seconds"] = float(str(row.get("duration_seconds") or 0).strip())
                except ValueError:
                    r["duration_seconds"] = 0.0
                try:
                    r["total_bytes"] = int(float(str(row.get("total_bytes")).strip()))
                except (TypeError, ValueError):
                    r["total_bytes"] = ""
                flags = {"is_" + c.lower(): flag_from_cell(row.get("is_" + c.lower()), c, task)
                         for c in codes}
                r.update(flags)
                # Same story as the booleans: the sheets file's category is an
                # XLOOKUP over those flags, so resolve it the way XLOOKUP will
                # - first flag that's set wins, "none" if none are.
                if (row.get("category") or "").startswith("="):
                    r["category"] = next((name for c, name in codes.items()
                                          if flags["is_" + c.lower()]), UNCATEGORIZED)
                out.append(r)
    except OSError as e:
        print(f"Warning: could not re-read {path} for totals: {e}", file=sys.stderr)
    return out


def refresh_aggregate_rows(path: str, codes: dict[str, str], email: str) -> bool:
    """Recompute TOTAL and TOTAL_HOURS in place from what `path` now holds.

    Those two rows are only *written* when the file is created, off that run's
    own sessions - so on every run after the first they describe a body that
    has since moved on. Append adds rows underneath them without touching
    them; even a full rewrite carries over sessions this run didn't return,
    which they never counted. Recomputing from the finished file is what keeps
    the header honest about the rows actually under it, whichever route those
    rows took to get there.

    Two rows are swapped for two rows, so the row count is unchanged and every
    formula in task_report_sheets.csv still points at the row it sits on.
    """
    try:
        with open(path, newline="", encoding="utf-8-sig") as f:
            rows = list(csv.reader(f))
    except OSError as e:
        print(f"Warning: could not re-read {path} for totals: {e}", file=sys.stderr)
        return False
    if not rows:
        return False

    header = rows[0]
    at: dict[str, int] = {}
    for i, r in enumerate(rows):
        if r and r[0] in ("TOTAL", "TOTAL_HOURS") and r[0] not in at:
            at[r[0]] = i
    if not at:
        return False                      # --no-aggregate-row: nothing to keep in step

    sessions = rows_for_totals(path, codes)
    if not sessions:
        # The rows are there but unreadable — say so rather than leaving a
        # header quietly describing a body it no longer matches.
        print(f"Warning: {path} has TOTAL rows but no readable SESSION rows - "
              f"its totals are stale", file=sys.stderr)
        return False

    fresh: dict[int, dict] = {}
    if "TOTAL" in at:
        total_row = aggregate_row(sessions, email)
        total_row.update(bool_totals(sessions, codes))
        fresh[at["TOTAL"]] = total_row
    if "TOTAL_HOURS" in at:
        fresh[at["TOTAL_HOURS"]] = hours_row(sessions, codes)

    for i, r in fresh.items():
        vals = ["" if r.get(name) is None else str(r.get(name, "")) for name in header]
        # A file that arrived from Sheets can carry trailing spacer columns the
        # header never named; keep its width rather than trimming the row.
        vals += [""] * (len(rows[i]) - len(vals))
        rows[i] = vals

    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        csv.writer(f).writerows(rows)
    return True


def write_csv(path: str, fields: list[str], rows: list[dict], append_mode: bool = False):
    d = os.path.dirname(os.path.abspath(path))
    if d:
        os.makedirs(d, exist_ok=True)

    # In append mode, filter out existing sessions and write only new ones
    if append_mode:
        existing_ids = read_existing_session_ids(path)
        rows_to_write = [r for r in rows if r.get("row_type") != "SESSION" or r.get("session_id") not in existing_ids]
    else:
        rows_to_write = rows

    # utf-8-sig: Excel needs the BOM or accented operator names arrive mangled.
    mode = "a" if append_mode and os.path.exists(path) else "w"
    # A CSV that has been round-tripped through Sheets/Excel (or hand-edited)
    # often has no final newline. Appending straight onto that splices the
    # first new row onto the tail of the last existing one - one corrupt
    # double-width row - and shifts every row after it up by one, so the
    # formulas built for those rows end up pointing a row off. Close the line
    # first. Written as plain utf-8 so the BOM logic stays out of it; "\r\n"
    # matches csv.writer's own line terminator.
    if mode == "a" and not ends_with_newline(path):
        with open(path, "a", newline="", encoding="utf-8") as f:
            f.write("\r\n")
    with open(path, mode, newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        # Only write header if not appending to existing file
        if mode == "w":
            w.writeheader()
        for r in rows_to_write:
            w.writerow({k: excel_safe(r.get(k, "")) for k in fields})


# ── CLI ───────────────────────────────────────────────────────────────────
def main(argv=None):
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--email", help="skip the email prompt (password is still asked for)")
    ap.add_argument("--base-url", default=BASE_URL)
    ap.add_argument("--out", default="task_report.csv")
    ap.add_argument("--rules", help='JSON: {"codes": {...}, "keywords": {...}}')
    ap.add_argument("--limit", type=int, default=500,
                    help="max sessions requested per device (default 500)")
    ap.add_argument("--full", action="store_true",
                    help="request full session payloads instead of the light ones")
    ap.add_argument("--workers", type=int, default=8, help="parallel device fetches")
    # default=None, not 0.0, so "was it actually passed?" is answerable below -
    # an explicit --min-seconds 0 has to be able to override a config minimum.
    ap.add_argument("--min-seconds", type=float, default=None,
                    help="minimum session length in seconds; overrides the "
                         "config's min_duration for this run")
    ap.add_argument("--no-weekends", action="store_true")
    ap.add_argument("--uncategorized-only", action="store_true")
    # The per-category file is aggregates only and is easy to mistake for the
    # main output, so it is opt-in rather than written every run.
    ap.add_argument("--category-file", action="store_true",
                    help="also write a <name>_by_category.csv of per-category totals")
    ap.add_argument("--json", action="store_true",
                    help="save the raw API JSON (disabled by default)")
    ap.add_argument("--json-out", metavar="PATH",
                    help="where to save the raw API JSON (default: <out>.raw.json)")
    ap.add_argument("--no-aggregate-row", action="store_true",
                    help="omit the leading TOTAL row, for a pure per-session dump")
    ap.add_argument("--explain", action="store_true",
                    help="print the category breakdown and unmatched task names")
    ap.add_argument("--make-rules-template", metavar="PATH",
                    help="write a --rules skeleton listing uncategorized task names")
    ap.add_argument("--check-env", action="store_true",
                    help="print interpreter / requests diagnostics and exit")
    ap.add_argument("--list-fields", action="store_true",
                    help="print every field the API returns, with fill rate and a "
                         "sample value, then exit — use this to confirm the real "
                         "names for total_bytes / upload_status")
    ap.add_argument("--raw-columns", action="store_true",
                    help="append a raw.<field> column for every API field")
    ap.add_argument("--bool-1-0", action="store_true",
                    help="write the is_<code> columns as 1/0 instead of TRUE/FALSE")
    ap.add_argument("--from-json", metavar="PATH",
                    help="skip login and the live fetch; categorize from a raw "
                         "JSON file saved earlier with --json/--json-out instead")
    a = ap.parse_args(argv)

    if a.check_env:
        return check_env()

    # Load config
    config = load_config()
    email = config["email"]
    password = config["password"]
    days_filter = parse_days_filter(config["days"])
    include_uncategorized = config["include_uncategorized"].lower() == "y"
    append_mode = config["append"].lower() == "y"
    # --min-seconds is the one-off override; min_duration (hh:mm:ss) is the
    # standing setting. A bad value is worth stopping for: silently falling
    # back to "no minimum" would quietly widen the report instead of narrowing
    # it, which is the opposite of what was asked for.
    try:
        config_min_seconds = parse_hhmmss(config["min_duration"])
    except ValueError as e:
        sys.exit(f"min_duration in categorize_config.txt: {e}")
    min_seconds = a.min_seconds if a.min_seconds is not None else config_min_seconds

    codes, keywords = dict(CATEGORY_CODES), dict(CATEGORY_KEYWORDS)
    if a.rules:
        with open(a.rules, encoding="utf-8") as f:
            r = json.load(f)
        codes.update(r.get("codes") or {})
        keywords.update(r.get("keywords") or {})
    codes = {str(k).strip().upper(): v for k, v in codes.items() if str(k).strip()}

    if a.from_json:
        raw, stats, payload = load_sessions_from_json(a.from_json)
    else:
        # Use config password if available
        if password:
            os.environ["FLEET_PASSWORD"] = password
        client, email = interactive_login(a.base_url, email)
        raw, stats, payload = fetch_sessions(client, a.limit, not a.full, a.workers)
    unreachable_hosts = stats.get("unreachable_hosts") or set()
    if not raw:
        sys.exit(f"No completed sessions found in {a.from_json}." if a.from_json
                 else "No completed sessions were returned by any device.")

    # Save the untouched API response before any filtering or categorising, so
    # the JSON is always a faithful record of what the fleet actually returned.
    # Pointless when the rows just came from one of these files already.
    json_path = None
    if a.json and not a.from_json:
        base, _ = os.path.splitext(a.out)
        json_path = a.json_out or f"{base}.raw.json"
        d = os.path.dirname(os.path.abspath(json_path))
        if d:
            os.makedirs(d, exist_ok=True)
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)

    if a.list_fields:
        # total_bytes / upload_status field names vary by rig build, so rather
        # than guess forever, show what this fleet actually returns.
        counts: Counter = Counter()
        samples: dict[str, object] = {}
        for r in raw:
            for k, v in (r.get("_raw") or {}).items():
                if v is None or v == "":
                    continue
                counts[k] += 1
                samples.setdefault(k, v)
        n = len(raw)
        print(f"\n{len(counts)} field(s) across {n} session(s):\n")
        print(f"  {'field':<26} {'filled':>12}   sample")
        print("  " + "-" * 72)
        for k, c in counts.most_common():
            s = samples.get(k)
            if isinstance(s, (dict, list)):
                s = json.dumps(s, separators=(",", ":"))[:40]
            print(f"  {k:<26} {c:>6} ({c/n*100:>3.0f}%)   {str(s)[:40]}")
        print("\n  Columns total_bytes / upload_status look for, in order:")
        print("    bytes  :", ", ".join(_BYTES_KEYS))
        print("    upload :", ", ".join(_UPLOAD_KEYS + _UPLOADED_FLAGS))
        print("\n  If a field above is the right one but is not in those lists,\n"
              "  add it to _BYTES_KEYS / _UPLOAD_KEYS near the top of this file.")
        if not a.full:
            print("\n  NOTE: this ran with light=1 (the default). Size and upload\n"
                  "  fields are the most likely to be stripped from light payloads —\n"
                  "  re-run with --full to compare.")
        return 0

    matchers = build_matchers(codes)
    host_rates, fleet_rate = build_byte_rate_baselines(raw)
    rows = [enrich(r, matchers, keywords, host_rates, fleet_rate) for r in raw]

    if a.make_rules_template:
        by_name: dict[str, float] = defaultdict(float)
        for r in rows:
            if r["category"] == UNCATEGORIZED and r["task_name"] != "(no task)":
                by_name[r["task_name"]] += r["duration_seconds"]
        ordered = sorted(by_name.items(), key=lambda kv: -kv[1])
        with open(a.make_rules_template, "w", encoding="utf-8") as f:
            json.dump({
                "_help": [
                    "Fill each value with one of the category names below, then run:",
                    f"  python3 {os.path.basename(__file__)} --rules <this file>",
                    "Keys match as case-insensitive substrings of the task name.",
                    "Delete any line you do not want to map.",
                ],
                "_available_categories": sorted(set(codes.values())),
                "codes": {},
                "keywords": {name: "" for name, _ in ordered},
            }, f, indent=2, ensure_ascii=False)
        print(f"Wrote a rules template with {len(ordered)} task name(s) "
              f"-> {a.make_rules_template}")
        return 0

    eligible = [r for r in rows
                if not (a.no_weekends and r["_is_weekend"] == "yes")
                and not (a.uncategorized_only and r["category"] != UNCATEGORIZED)
                and (days_filter is None or r["date"].split("-")[1] + "/" + r["date"].split("-")[2] in days_filter)
                and (include_uncategorized or r["category"] != UNCATEGORIZED)]
    # The minimum is applied last and counted against what the other filters
    # already kept, so the number reported is what this setting actually cost -
    # not sessions some other filter had excluded anyway.
    kept = [r for r in eligible if r["duration_seconds"] >= min_seconds]
    too_short = len(eligible) - len(kept)
    if not kept:
        sys.exit("Every session was filtered out; loosen the filters."
                 + (f" ({too_short} were shorter than {hhmmss(min_seconds)})"
                    if too_short else ""))

    # Alphabetical by task name, then chronological within a task.
    kept.sort(key=lambda r: (r["task_name"].casefold(), r["_start_iso"]))

    # Per-category booleans on the right, one per active code. Attached to each
    # row before the totals are computed so the totals can just count them.
    boolcols = bool_columns(codes)
    for r in kept:
        r.update(code_flags(r.get("_codes"), r["category"], codes))

    # One row per session, restricted to the requested column set. The raw API
    # fields all remain in the .raw.json; --raw-columns puts them back here.
    fields = FIELDS + boolcols
    out_rows = list(kept)
    if a.raw_columns:
        rawcols = raw_columns(kept)
        fields += ["raw." + c for c in rawcols]
        out_rows = [dict(r, **flatten_raw(r, rawcols)) for r in kept]

    # Always last, after is_<code> and any --raw-columns passthrough — see
    # enrich() for what duration_implied/duration_s carry. yield_pct is
    # deliberately never written to (DictWriter's restval leaves it blank);
    # it exists so it can be filled in by hand in the spreadsheet.
    fields += ["duration_implied", "duration_s", "yield_pct"]

    # The three aggregate/tags rows: computed whenever they're wanted at all.
    # Whether they actually get PREPENDED is decided per file, further down -
    # each file only gets them if not in append mode or the file doesn't
    # exist yet (appending re-adds every fetched session each run, so
    # re-adding these three every time would pile up duplicates of them).
    # These two totals are the file's FIRST ones only; a file that already had
    # them keeps the rows it has, and refresh_aggregate_rows() recomputes them
    # in place once the writing is done - see there for why off `kept` alone
    # is not good enough.
    aggregate_rows = []
    if not a.no_aggregate_row:
        total_row = aggregate_row(kept, email)
        total_row.update(bool_totals(kept, codes))
        tags_row = category_tags_row(codes)
        hours_total_row = hours_row(kept, codes)
        aggregate_rows = [total_row, hours_total_row, tags_row]

    # Independent copy of this run's session rows, captured before either
    # file's own aggregate-row/merge handling below, so task_report_sheets.csv
    # can apply its own rules against its own existing content instead of
    # inheriting task_report.csv's.
    fresh_session_rows = [dict(r) for r in out_rows]

    out_rows_have_aggregates = bool(aggregate_rows) and (
        not append_mode or not os.path.exists(a.out))
    if out_rows_have_aggregates:
        out_rows = aggregate_rows + out_rows

    # Sheets/Excel writes booleans as TRUE/FALSE; 1/0 sums directly with =SUM().
    for rows_ in (out_rows, fresh_session_rows):
        for r in rows_:
            if r.get("row_type") == "SESSION":
                for c in boolcols:
                    r[c] = ("1" if r.get(c) else "0") if a.bool_1_0 else \
                           ("TRUE" if r.get(c) else "FALSE")

    # A full rewrite normally recomputes every column from this run's fresh
    # API pull. keep_columns lets specific columns (a hand-corrected category,
    # a manually verified duration, ...) survive instead of being clobbered.
    # A session that comes back fresh is always rewritten fresh (that's the
    # whole point of a rewrite) - but a session that DOESN'T come back this
    # run (its rig was unreachable, or it just fell outside this run's
    # filters/limit) is left exactly as it was, rather than being dropped.
    # Matched by session_id alone, so nothing is ever written twice: a row is
    # either the fresh version or the untouched old one, never both.
    # Append mode never touches existing rows in the first place, so both are
    # no-ops there.
    keep_columns = [c.strip() for c in config["keep_columns"].split(",") if c.strip()]
    kept_touched = preserved_missing = 0
    if not append_mode:
        kept_touched, preserved_missing = merge_full_rewrite(out_rows, a.out, keep_columns)

    write_csv(a.out, fields, out_rows, append_mode=append_mode)
    # After the rows are down, not before: the totals describe the finished
    # file, including whatever was already in it and whatever was preserved.
    totals_refreshed = refresh_aggregate_rows(a.out, codes, email)

    # task_report_sheets.csv follows the exact same overwrite/append rules as
    # task_report.csv above, applied independently against its own existing
    # content: append=y only adds sessions it doesn't already have (and
    # never re-adds the aggregate rows once they're already there), and a
    # full rewrite keeps whatever session it can't find this run. The only
    # difference is keep_columns never touches the formula columns here -
    # those are supposed to always be live formulas, never a frozen old
    # value, so they're re-marked as formulas after the merge even for a row
    # carried over untouched from the existing file.
    sheets_path = None
    sheets_kept_touched = sheets_preserved_missing = 0
    if config["sheets_output"].lower() == "y":
        if not aggregate_rows:
            print("Warning: sheets_output needs the aggregate rows (CATEGORY_TAGS "
                  "is the formulas' lookup table) - skipping, --no-aggregate-row "
                  "is set", file=sys.stderr)
        else:
            base, ext = os.path.splitext(a.out)
            sheets_path = f"{base}_sheets{ext or '.csv'}"
            sheets_have_aggregates = not append_mode or not os.path.exists(sheets_path)
            sheets_source = (aggregate_rows + fresh_session_rows if sheets_have_aggregates
                              else fresh_session_rows)
            # In append mode, rows land below whatever's already in the file
            # (not at row 2), and only genuinely-new sessions get written at
            # all - both need accounting for before the formulas are built,
            # since a formula bakes in the exact row it's meant to sit on.
            start_row = 2
            if append_mode and os.path.exists(sheets_path):
                existing_ids = read_existing_session_ids(sheets_path)
                sheets_source = [r for r in sheets_source if r.get("row_type") != "SESSION"
                                  or r.get("session_id") not in existing_ids]
                start_row = count_csv_rows(sheets_path) + 1
            # Merge BEFORE building formulas, not after: a preserved session
            # can land on a different physical row than it had last time
            # (fresh rows from this run are ordered first), and a formula
            # bakes in the row it's on. Building formulas once on the final,
            # fully-merged row order is what keeps every reference correct -
            # regenerating a preserved row's formulas from scratch is exactly
            # right here, since they'd have come out identical anyway had it
            # been fetched fresh this run.
            formula_cols = {"task_prefix", "category", "is_categorized", *boolcols}
            sheets_keep_columns = [c for c in keep_columns if c not in formula_cols]
            if not append_mode:
                sheets_kept_touched, sheets_preserved_missing = merge_full_rewrite(
                    sheets_source, sheets_path, sheets_keep_columns)
            sheets_rows = build_sheets_rows(sheets_source, fields, boolcols, codes,
                                             start_row=start_row)
            write_csv(sheets_path, fields, sheets_rows, append_mode=append_mode)
            totals_refreshed |= refresh_aggregate_rows(sheets_path, codes, email)

    cat_path = None
    if a.category_file:
        base, ext = os.path.splitext(a.out)
        cat_path = f"{base}_by_category{ext or '.csv'}"
        crows = category_rows(kept)
        write_csv(cat_path, list(crows[0].keys()), crows)

    uncat = [r for r in kept if r["category"] == UNCATEGORIZED]
    total = sum(r["duration_seconds"] for r in kept)
    failed_msg = f"{stats['failed']} failed"
    if stats.get("failed_502"):
        failed_msg += f" ({stats['failed_502']} 502"
        if stats.get("failed_timeout"):
            failed_msg += f", {stats['failed_timeout']} timeout"
        failed_msg += ")"
    elif stats.get("failed_timeout"):
        failed_msg += f" ({stats['failed_timeout']} timeout)"
    print(f"\nDevices  {stats['devices']} queried, {failed_msg}")
    print(f"Sessions {len(raw)} fetched"
          + (f", {stats['dupes']} duplicate(s) dropped" if stats["dupes"] else ""))
    if min_seconds > 0:
        print(f"Minimum  {hhmmss(min_seconds)} per session"
              + (f", {too_short} shorter one(s) dropped" if too_short
                 else ", none were shorter"))
    print(f"Wrote    {len(kept)} session row(s)"
          + ("" if a.no_aggregate_row else " + 3 aggregate/tags rows")
          + f", {len(fields)} columns -> {a.out}")
    if json_path:
        print(f"         raw API JSON               -> {json_path}")
    if cat_path:
        print(f"         per-category totals        -> {cat_path}")
    if sheets_path:
        print(f"         Sheets version (live formulas) -> {sheets_path}")
    if totals_refreshed:
        print("Totals   TOTAL / TOTAL_HOURS recomputed over every row now in "
              "the file")
    if keep_columns:
        print(f"Kept     {', '.join(keep_columns)} preserved from the existing "
              f"file for {kept_touched} row(s)")
    if preserved_missing:
        print(f"Kept     {preserved_missing} existing session row(s) not returned "
              f"this run left untouched"
              + (f" ({len(unreachable_hosts)} device(s) were unreachable)"
                 if unreachable_hosts else ""))
    if sheets_kept_touched:
        print(f"Kept     {', '.join(sheets_keep_columns)} preserved in {sheets_path} "
              f"for {sheets_kept_touched} row(s)")
    if sheets_preserved_missing:
        print(f"Kept     {sheets_preserved_missing} existing session row(s) in "
              f"{sheets_path} not returned this run left untouched")
    print(f"Total    {hhmmss(total)} ({total/3600:.2f} h) recorded")
    print(f"Tagged   {len(kept)-len(uncat)}/{len(kept)} "
          f"({(len(kept)-len(uncat))/len(kept)*100:.1f}%)")

    # total_bytes / upload_status depend on field names that vary by rig build,
    # so say plainly when they came back empty instead of shipping blank columns.
    have_bytes = sum(1 for r in kept if str(r["total_bytes"]).strip() not in ("", "None"))
    have_upload = sum(1 for r in kept if str(r["upload_status"]).strip())
    print(f"Sizes    {have_bytes}/{len(kept)} rows have total_bytes")
    print(f"Upload   {have_upload}/{len(kept)} rows have upload_status")
    if not have_bytes or not have_upload:
        missing = " and ".join(x for x in (
            "total_bytes" if not have_bytes else "",
            "upload_status" if not have_upload else "") if x)
        print(f"\n!! {missing} came back empty for every session.", file=sys.stderr)
        if not a.full:
            print("   The default request uses light=1, which is the most likely\n"
                  "   thing stripping them. Try:  --full", file=sys.stderr)
        print("   Then run --list-fields to see the exact field names this fleet\n"
              "   returns, and add them to _BYTES_KEYS / _UPLOAD_KEYS.",
              file=sys.stderr)

    if uncat:
        print(f"\n!! {len(uncat)} session(s) matched no category code.", file=sys.stderr)
        if len(uncat) == len(kept):
            print("   NO task name contains any category code (RC4, XO4, ET4, …).\n"
                  "   Either the codes are not in use in this data yet, or they live\n"
                  "   in a field this script cannot see. Use --make-rules-template\n"
                  "   to map the existing names by keyword instead.", file=sys.stderr)
        if a.explain:
            print("\n   most common unmatched task names:", file=sys.stderr)
            for name, n in Counter(r["task_name"] for r in uncat).most_common(20):
                print(f"     {n:5}  {name!r}", file=sys.stderr)

    if a.explain:
        print("\n   category breakdown:", file=sys.stderr)
        for r in category_rows(kept):
            print(f"     {r['category_name']:<34} {r['sessions']:>5} sessions  "
                  f"{r['duration_hhmmss']:>11}  {r['pct_of_total_time']:>6.2f}%",
                  file=sys.stderr)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit("\nInterrupted.")