"""
Upstash Redis state layer.

Every piece of state that used to live in a module-level dict or a .json file
on disk now lives here. Serverless invocations share nothing, so anything that
must outlive a single request has to round-trip through Redis.

Uses the Upstash REST API over urllib (stdlib) so there is no extra dependency
and no connection pooling to worry about in a serverless environment.
"""

import json
import os
import time
import urllib.request
import urllib.error

# Accept both naming conventions. Creating the database on Upstash directly
# gives UPSTASH_REDIS_REST_*, while attaching it through the Vercel Marketplace
# / KV integration injects KV_REST_API_* for the same endpoint. Reading both
# means the deploy works whichever route was taken.
_URL = (os.environ.get("UPSTASH_REDIS_REST_URL")
        or os.environ.get("KV_REST_API_URL")
        or "").rstrip("/")
_TOKEN = (os.environ.get("UPSTASH_REDIS_REST_TOKEN")
          or os.environ.get("KV_REST_API_TOKEN")
          or "")

P = "fm:"          # key prefix, so the DB can be shared if you ever need to


class RedisUnavailable(Exception):
    pass


def configured() -> bool:
    return bool(_URL and _TOKEN)


def _post(path: str, payload, timeout: float = 8.0):
    if not configured():
        raise RedisUnavailable(
            "Redis is not configured. Set UPSTASH_REDIS_REST_URL and "
            "UPSTASH_REDIS_REST_TOKEN (or KV_REST_API_URL / KV_REST_API_TOKEN) "
            "in the Vercel project's environment variables.")
    req = urllib.request.Request(
        _URL + path,
        data=json.dumps(payload).encode(),
        headers={
            "Authorization": "Bearer " + _TOKEN,
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        raise RedisUnavailable(f"Upstash HTTP {e.code}: {e.read()[:200]!r}")
    except Exception as e:
        raise RedisUnavailable(f"Upstash request failed: {e}")


# Billable commands issued by this container since it started. Upstash charges
# per command, not per round trip, so this counts what the invoice counts —
# a pipeline of five is five. /api/health reports it, which is the only way to
# see the real per-request cost from outside.
CMD_COUNT = 0


def cmd(*args, timeout: float = 8.0):
    """Runs a single Redis command and returns its result."""
    global CMD_COUNT
    CMD_COUNT += 1
    res = _post("", [str(a) for a in args], timeout=timeout)
    if isinstance(res, dict):
        if "error" in res:
            raise RedisUnavailable(res["error"])
        return res.get("result")
    return res


def pipeline(commands, timeout: float = 10.0):
    """Runs many commands in ONE HTTP round trip.

    Serverless latency is dominated by network hops, so batching matters a lot
    more here than it did in the long-running process.
    """
    global CMD_COUNT
    if not commands:
        return []
    CMD_COUNT += len(commands)
    res = _post("/pipeline", [[str(a) for a in c] for c in commands], timeout=timeout)
    out = []
    for item in res:
        if isinstance(item, dict) and "error" in item:
            out.append(None)
        else:
            out.append(item.get("result") if isinstance(item, dict) else item)
    return out


# ── In-process cache ──────────────────────────────────────────────────────
# A warm container serves many requests, and under Fluid compute it serves
# several at once. Anything on the hot read path is memoised here for a few
# seconds, so a burst of tabs landing together costs ONE Upstash command
# between them rather than one each. Purely an optimisation: every entry has
# a short TTL and losing the whole cache (a cold container) is correct, just
# slower.
_memo: dict = {}


def memo_get(key, ttl):
    hit = _memo.get(key)
    if hit is not None and (time.time() - hit[0]) < ttl:
        return hit[1]
    return None


def memo_set(key, value):
    _memo[key] = (time.time(), value)
    return value


def memo_drop(key):
    _memo.pop(key, None)


# ── JSON helpers ──────────────────────────────────────────────────────────
def jget(key, default=None):
    raw = cmd("GET", P + key)
    if raw is None:
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default


def jset(key, value, ttl: int | None = None):
    payload = json.dumps(value, separators=(",", ":"))
    if ttl:
        return cmd("SET", P + key, payload, "EX", int(ttl))
    return cmd("SET", P + key, payload)


def hgetall_json(key) -> dict:
    """HGETALL, decoding each value as JSON. Upstash returns a flat array."""
    raw = cmd("HGETALL", P + key)
    out = {}
    if not raw:
        return out
    if isinstance(raw, dict):
        items = raw.items()
    else:
        items = zip(raw[0::2], raw[1::2])
    for k, v in items:
        try:
            out[k] = json.loads(v)
        except Exception:
            out[k] = v
    return out


def hset_json(key, field, value):
    return cmd("HSET", P + key, field, json.dumps(value, separators=(",", ":")))


def hset_many_json(key, mapping: dict):
    if not mapping:
        return
    args = ["HSET", P + key]
    for f, v in mapping.items():
        args.append(f)
        args.append(json.dumps(v, separators=(",", ":")))
    return cmd(*args)


# ── Consolidated state ────────────────────────────────────────────────────
# poll:last, prev_status, service_memory, last_online, active_alerts,
# observed, locations and one SET-NX per live alert used to be a key each:
# fifteen-odd billable commands per cycle for a few kilobytes that are always
# read together and always written together. They are one blob now.
#
# The same blob carries the assembled dashboard payload under "view", which is
# the larger saving. Reassembling that answer per request cost around sixty
# commands — most of them one HGETALL per rig for the task history — and every
# open tab paid it on every tick even when the fleet had not been re-polled.
# Now a tick that finds the poll gated is a single GET, often served from the
# memo above without touching Upstash at all.
STATE_KEY = "state"
STATE_MEMO_TTL = 3.0

# The cold half: task history, past-day totals and the frame-health seed.
# It is an order of magnitude larger than the view and changes hourly, so it
# lives in its own key and the client fetches it only when its version moves.
HISTORY_KEY = "history"
HISTORY_MEMO_TTL = 20.0


# Read at call time, not bound as a default argument: agent.py raises both of
# these when it is the process doing the polling, because then the memo is not
# a guess about staleness — it is the copy it just wrote.
def state_load(memo_ttl: float | None = None) -> dict:
    hit = memo_get(STATE_KEY, STATE_MEMO_TTL if memo_ttl is None else memo_ttl)
    if hit is not None:
        return hit
    return memo_set(STATE_KEY, jget(STATE_KEY, {}) or {})


def state_save(state: dict):
    memo_set(STATE_KEY, state)
    jset(STATE_KEY, state)


def history_load(memo_ttl: float | None = None) -> dict:
    hit = memo_get(HISTORY_KEY, HISTORY_MEMO_TTL if memo_ttl is None else memo_ttl)
    if hit is not None:
        return hit
    return memo_set(HISTORY_KEY, jget(HISTORY_KEY, {}) or {})


def history_save(history: dict):
    memo_set(HISTORY_KEY, history)
    jset(HISTORY_KEY, history)


# ── Distributed lock ──────────────────────────────────────────────────────
# Several browser tabs poll at once. Without this they would each run the
# transition detection against the same fleet and double-count the result.
#
# The poll's lock is not released: its TTL is set to the poll interval, so the
# key expiring IS the next cycle falling due. One SET NX EX per cycle does the
# job three commands used to (read the last-poll time, take the lock, release
# it), and it is atomic where the read-then-lock pair was not.
def acquire_lock(name: str, ttl: int = 30) -> bool:
    token = str(time.time())
    return cmd("SET", P + "lock:" + name, token, "NX", "EX", int(ttl)) == "OK"


def release_lock(name: str):
    try:
        cmd("DEL", P + "lock:" + name)
    except Exception:
        pass


# The alert debounce used to be a SET NX EX per live alert per cycle — a
# dozen commands every poll to answer "has it been fifteen minutes yet". It is
# a dict of expiry timestamps inside the state blob now (see logic.poll), which
# costs nothing: that blob is already being read and written.


# ── Terminal log buffer ───────────────────────────────────────────────────
def log_push(lines):
    if not lines:
        return
    args = ["LPUSH", P + "logs"] + [str(l) for l in lines]
    pipeline([args, ["LTRIM", P + "logs", 0, 499]])


def log_read(limit: int = 500):
    raw = cmd("LRANGE", P + "logs", 0, int(limit) - 1) or []
    return list(reversed(raw))


# ── Feedback ──────────────────────────────────────────────────────────────
# Capped like the log: this is a suggestion box, not a ticket system, and it
# must not be able to grow without bound from the public-facing page.
FEEDBACK_MAX = 200


def feedback_push(entry: dict):
    pipeline([
        ["LPUSH", P + "feedback", json.dumps(entry, separators=(",", ":"))],
        ["LTRIM", P + "feedback", 0, FEEDBACK_MAX - 1],
    ])


def feedback_read(limit: int = 50):
    raw = cmd("LRANGE", P + "feedback", 0, int(limit) - 1) or []
    out = []
    for r in raw:
        try:
            out.append(json.loads(r))
        except Exception:
            pass
    return out


# ── Frame health history ──────────────────────────────────────────────────
def health_push(entry: dict, maxlen: int = 1440):
    pipeline([
        ["LPUSH", P + "health", json.dumps(entry, separators=(",", ":"))],
        ["LTRIM", P + "health", 0, int(maxlen) - 1],
    ])


def health_read(limit: int = 360):
    raw = cmd("LRANGE", P + "health", 0, int(limit) - 1) or []
    out = []
    for r in reversed(raw):
        try:
            out.append(json.loads(r))
        except Exception:
            pass
    return out


# ── Dashboard sessions (replaces auth.py's in-memory session dict) ────────
SESSION_TTL = 60 * 60 * 12

# Every authenticated request validates its token, so this was a guaranteed
# Redis command per request no matter what else got cached. Memoised for half
# a minute instead. The cost of that window is a token staying usable on an
# already-warm container for up to 30s after a logout elsewhere; the session
# itself is 12 hours, so the exposure is negligible either way.
SESSION_MEMO_TTL = 30.0
_SESS = "sess:"


def session_create(token: str, email: str):
    cmd("SET", f"{P}{_SESS}{token}", email, "EX", SESSION_TTL)
    memo_set(_SESS + token, email)


def session_email(token: str | None) -> str | None:
    if not token:
        return None
    hit = memo_get(_SESS + token, SESSION_MEMO_TTL)
    if hit is not None:
        return hit
    email = cmd("GET", f"{P}{_SESS}{token}")
    if email:
        memo_set(_SESS + token, email)
    return email


def session_destroy(token: str | None):
    if token:
        memo_drop(_SESS + token)
        cmd("DEL", f"{P}{_SESS}{token}")


# ── OTP codes (also previously in-memory) ─────────────────────────────────
def otp_store(email: str, code: str, ttl: int = 600):
    cmd("SET", f"{P}otp:{email.lower()}", code, "EX", int(ttl))


def otp_check(email: str, code: str) -> bool:
    key = f"{P}otp:{email.lower()}"
    stored = cmd("GET", key)
    if stored and str(stored) == str(code):
        cmd("DEL", key)
        return True
    return False
