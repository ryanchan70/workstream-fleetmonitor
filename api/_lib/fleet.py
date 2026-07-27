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
        try:
            if _restore_auth(client, R.jget(_AUTH_KEY)):
                return client
        except R.RedisUnavailable:
            pass

    email = os.environ.get("FLEET_EMAIL")
    password = os.environ.get("FLEET_PASSWORD")
    if not email or not password:
        raise FleetAPIError("FLEET_EMAIL / FLEET_PASSWORD are not configured.")

    ok, err = client.login_password(email, password)
    if not ok:
        raise FleetAPIError(f"Fleet login failed: {err}")

    try:
        snap = _snapshot_auth(client)
        if snap:
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
