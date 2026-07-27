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


def cmd(*args, timeout: float = 8.0):
    """Runs a single Redis command and returns its result."""
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
    if not commands:
        return []
    res = _post("/pipeline", [[str(a) for a in c] for c in commands], timeout=timeout)
    out = []
    for item in res:
        if isinstance(item, dict) and "error" in item:
            out.append(None)
        else:
            out.append(item.get("result") if isinstance(item, dict) else item)
    return out


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


# ── Distributed lock ──────────────────────────────────────────────────────
# Several browser tabs poll at once. Without this they would each run the
# transition detection against the same fleet and double-count the result.
def acquire_lock(name: str, ttl: int = 30) -> bool:
    token = str(time.time())
    return cmd("SET", P + "lock:" + name, token, "NX", "EX", int(ttl)) == "OK"


def release_lock(name: str):
    try:
        cmd("DEL", P + "lock:" + name)
    except Exception:
        pass


# ── Alert debounce ────────────────────────────────────────────────────────
# The old code kept a dict of last-fired timestamps. A Redis key with a TTL
# does the same job atomically: if SET NX succeeds the alert had expired, so
# it is allowed to fire again.
def should_alert(hostname: str, kind: str, window: int) -> bool:
    key = f"{P}alert:{hostname}:{kind}"
    return cmd("SET", key, "1", "NX", "EX", int(window)) == "OK"


# ── Terminal log buffer ───────────────────────────────────────────────────
def log_push(lines):
    if not lines:
        return
    args = ["LPUSH", P + "logs"] + [str(l) for l in lines]
    pipeline([args, ["LTRIM", P + "logs", 0, 499]])


def log_read(limit: int = 500):
    raw = cmd("LRANGE", P + "logs", 0, int(limit) - 1) or []
    return list(reversed(raw))


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


def session_create(token: str, email: str):
    cmd("SET", f"{P}sess:{token}", email, "EX", SESSION_TTL)


def session_email(token: str | None) -> str | None:
    if not token:
        return None
    return cmd("GET", f"{P}sess:{token}")


def session_destroy(token: str | None):
    if token:
        cmd("DEL", f"{P}sess:{token}")


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
