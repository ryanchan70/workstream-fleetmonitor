"""
Fleet API access for serverless.

The long-running build logged in once at boot and reused the session forever.
Each serverless invocation starts cold, so logging in every time would add a
round trip to fleet.shiftiq.us on every request. Instead the authenticated
cookie jar is cached in Redis and restored on the next invocation.

INTEGRATION NOTE
----------------
This reaches into FleetAPIClient's underlying requests.Session to save and
restore cookies. That is the one place coupled to your api_client.py. If your
client stores auth as a bearer token rather than cookies, extend
_snapshot_auth / _restore_auth below — everything else keeps working, it will
just re-login on each cold start.
"""

import os
import time

from . import redis_state as R

try:
    from api_client import FleetAPIClient, FleetAPIError
except Exception:  # pragma: no cover - surfaced at request time instead
    FleetAPIClient = None

    class FleetAPIError(Exception):
        pass


_AUTH_KEY = "fleetauth"
_AUTH_TTL = 60 * 30

# The cached cookie was re-read from Redis for every single fleet call, and a
# poll makes thirteen of them — one fleet sweep plus a session fetch per rig in
# the shard. That was thirteen billable commands a cycle to answer "am I still
# logged in", dwarfing the three the poll spends on its own state.
#
# Held here for five minutes instead, well inside the half-hour the snapshot is
# stored for. Only the snapshot is memoised, not the client: a poll fans its
# session fetches across twelve threads, and requests.Session does not promise
# to be safe shared between them.
_AUTH_MEMO_TTL = 300.0
_auth_memo = None
_auth_memo_at = 0.0


def _cached_auth():
    global _auth_memo, _auth_memo_at
    if _auth_memo is not None and (time.time() - _auth_memo_at) < _AUTH_MEMO_TTL:
        return _auth_memo
    try:
        data = R.jget(_AUTH_KEY)
    except R.RedisUnavailable:
        return None
    _auth_memo, _auth_memo_at = data, time.time()
    return data


def _remember_auth(data):
    global _auth_memo, _auth_memo_at
    _auth_memo, _auth_memo_at = data, time.time()


def _snapshot_auth(client):
    """Extracts whatever represents 'logged in' from the client."""
    data = {}
    sess = getattr(client, "session", None)
    if sess is not None and hasattr(sess, "cookies"):
        try:
            data["cookies"] = {c.name: c.value for c in sess.cookies}
        except Exception:
            pass
    for attr in ("token", "access_token", "auth_token", "bearer"):
        val = getattr(client, attr, None)
        if isinstance(val, str) and val:
            data[attr] = val
    return data or None


def _restore_auth(client, data) -> bool:
    if not data:
        return False
    ok = False
    sess = getattr(client, "session", None)
    if sess is not None and data.get("cookies"):
        try:
            for k, v in data["cookies"].items():
                sess.cookies.set(k, v)
            ok = True
        except Exception:
            pass
    for attr in ("token", "access_token", "auth_token", "bearer"):
        if data.get(attr):
            try:
                setattr(client, attr, data[attr])
                ok = True
            except Exception:
                pass
    return ok


def get_client(force_login: bool = False):
    """Returns an authenticated FleetAPIClient, reusing cached auth if possible."""
    if FleetAPIClient is None:
        raise FleetAPIError(
            "api_client.py is not importable. Copy it (and auth.py) into the "
            "project root so the serverless bundle includes it.")

    client = FleetAPIClient()

    if not force_login:
        if _restore_auth(client, _cached_auth()):
            return client
    else:
        # The cached cookie is what just failed; forget it here as well or
        # every thread in the sweep retries with the same dead one.
        _remember_auth(None)

    email = os.environ.get("FLEET_EMAIL")
    password = os.environ.get("FLEET_PASSWORD")
    if not email or not password:
        raise FleetAPIError("FLEET_EMAIL / FLEET_PASSWORD are not configured.")

    ok, err = client.login_password(email, password)
    if not ok:
        raise FleetAPIError(f"Fleet login failed: {err}")

    snap = _snapshot_auth(client)
    if snap:
        _remember_auth(snap)
        try:
            R.jset(_AUTH_KEY, snap, ttl=_AUTH_TTL)
        except R.RedisUnavailable:
            pass
    return client


def with_retry(fn):
    """Runs fn(client); on failure retries once with a fresh login.

    Cached credentials expire server-side without warning, and a cold
    invocation cannot tell the difference between 'stale cookie' and 'API
    down' until it tries.
    """
    try:
        return fn(get_client())
    except Exception:
        return fn(get_client(force_login=True))


def fleet_status():
    return with_retry(lambda c: c.get_fleet_status()) or []


def device_sessions(hostname: str):
    return with_retry(lambda c: c.get_device_sessions(hostname)) or []
