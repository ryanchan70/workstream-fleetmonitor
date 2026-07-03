"""
Vercel entrypoint: a single WSGI app serving the dashboard (index.html) and
the /api/* endpoints. Auth is stateless (HMAC-signed tokens carrying the
fleet.shiftiq.us OTP cookies — AUTH_SECRET env var required); leaderboard/rig
state lives in Vercel KV when attached. See api/_lib/core.py.
"""

import json
import os
from http.client import responses as http_reasons

from api._lib import core
from api_client import FleetAPIClient

_HERE = os.path.dirname(os.path.abspath(__file__))


def _read_body(environ) -> dict:
    try:
        length = int(environ.get("CONTENT_LENGTH") or 0)
    except ValueError:
        length = 0
    if length <= 0:
        return {}
    try:
        return json.loads(environ["wsgi.input"].read(length) or b"{}")
    except Exception:
        return {}


def _bearer(environ) -> str | None:
    auth = environ.get("HTTP_AUTHORIZATION", "")
    if auth.startswith("Bearer "):
        return auth[len("Bearer "):].strip()
    return None


def _session(environ) -> dict | None:
    return core.read_token(_bearer(environ), "sess")


# ── route handlers: return (status, payload) ─────────────────────────────────
def auth_request_code(environ):
    email = (_read_body(environ).get("email") or "").strip().lower()
    if not email:
        return 400, {"ok": False, "error": "email is required"}
    client = FleetAPIClient()
    ok, err = client.send_otp(email)
    if not ok:
        return 400, {"ok": False, "error": err or "could not send verification code"}
    # The send-time cookies ride along in a signed token so verify can resume
    # the same upstream session — no server-side state.
    return 200, {"ok": True, "pending": core.make_token("pend", email, client, core.PENDING_TTL_S)}


def auth_verify_code(environ):
    body = _read_body(environ)
    email = (body.get("email") or "").strip().lower()
    code = (body.get("code") or "").strip()
    payload = core.read_token(body.get("pending"), "pend")
    if payload is None or payload.get("e") != email:
        return 400, {"ok": False, "error": "request a new code first"}
    client = core.client_from_payload(payload)
    ok, err = client.verify_otp(email, code)
    if not ok:
        return 400, {"ok": False, "error": err or "invalid verification code"}
    return 200, {"ok": True, "token": core.make_token("sess", email, client, core.SESSION_TTL_S)}


def auth_status(environ):
    payload = _session(environ)
    return 200, {"authenticated": payload is not None,
                 "email": payload.get("e") if payload else None}


def auth_logout(environ):
    # Tokens are stateless — the browser dropping its copy is the logout.
    return 200, {"ok": True}


def rankings(environ):
    payload = _session(environ)
    if payload is None:
        return 401, {"error": "authentication required"}
    snap = core.get_snapshot(core.client_from_payload(payload))
    if snap is None:
        return 503, {"error": "fleet status unavailable"}
    return 200, {**snap["rankings"], "active": True}


def devices(environ):
    payload = _session(environ)
    if payload is None:
        return 401, {"error": "authentication required"}
    snap = core.get_snapshot(core.client_from_payload(payload))
    if snap is None:
        return 503, {"error": "fleet status unavailable"}
    return 200, {"rigs": snap["rigs"], "alerts": snap["alerts"], "active": True}


def logs(environ):
    if _session(environ) is None:
        return 401, {"error": "authentication required"}
    return 200, core.get_log()


ROUTES = {
    ("POST", "/api/auth/request-code"): auth_request_code,
    ("POST", "/api/auth/verify-code"): auth_verify_code,
    ("GET", "/api/auth/status"): auth_status,
    ("POST", "/api/auth/logout"): auth_logout,
    ("GET", "/api/rankings"): rankings,
    ("GET", "/api/devices"): devices,
    ("GET", "/api/logs"): logs,
}


def app(environ, start_response):
    method = environ.get("REQUEST_METHOD", "GET")
    path = (environ.get("PATH_INFO") or "/").rstrip("/") or "/"

    if method == "GET" and path in ("/", "/index.html"):
        try:
            with open(os.path.join(_HERE, "index.html"), "rb") as f:
                body = f.read()
        except OSError:
            body = b"index.html missing from deployment"
        start_response("200 OK", [("Content-Type", "text/html; charset=utf-8"),
                                  ("Content-Length", str(len(body)))])
        return [body]

    route = ROUTES.get((method, path))
    if route is None:
        status, payload = 404, {"error": "not found"}
    elif path != "/api/auth/status" and not core.AUTH_SECRET:
        status, payload = 500, {"ok": False, "error": "AUTH_SECRET env var is not set on the deployment"}
    else:
        status, payload = route(environ)

    body = json.dumps(payload).encode()
    start_response(f"{status} {http_reasons.get(status, 'OK')}",
                   [("Content-Type", "application/json"),
                    ("Content-Length", str(len(body)))])
    return [body]
