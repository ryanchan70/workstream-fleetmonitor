#!/usr/bin/env python3
"""
auth.py
Gates the local dashboard behind a real email verification code. Codes are
issued and checked by fleet.shiftiq.us's own OTP endpoints, so a visitor is
only ever let in if that backend accepts both their email and the code it
just emailed them — there's no separate password or allowlist to maintain
here, the upstream account system is the source of truth for who's
authorized.
"""

import json
import os
import secrets
import threading
import time

import requests.utils

from api_client import BASE_URL, FleetAPIClient

PENDING_TTL_S = 5 * 60          # window to enter a code after requesting one
SESSION_TTL_S = 12 * 60 * 60    # dashboard session lifetime
# Kept under local/ with the other runtime state; git-ignored either way.
SESSIONS_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "local", "caches",
    ".dashboard_sessions.json")


class DashboardAuth:
    def __init__(self, base_url: str = BASE_URL):
        self.base_url = base_url
        self._lock = threading.Lock()
        self._pending: dict[str, dict] = {}    # email -> {client, expires}
        self._sessions: dict[str, dict] = {}   # token -> {email, expires, client}
        self._load()

    def _load(self):
        """Restores dashboard sessions (and their fleet.shiftiq.us cookies)
        from disk so a server restart doesn't sign everyone out."""
        try:
            with open(SESSIONS_FILE, "r") as f:
                stored = json.load(f)
        except Exception:
            return
        now = time.time()
        for token, s in stored.items():
            if s.get("expires", 0) < now:
                continue
            client = FleetAPIClient(self.base_url)
            client.session.cookies = requests.utils.cookiejar_from_dict(s.get("cookies", {}))
            self._sessions[token] = {"email": s["email"], "expires": s["expires"], "client": client}

    def _save_locked(self):
        """Persist sessions to disk. Caller must hold self._lock."""
        stored = {
            token: {
                "email": s["email"],
                "expires": s["expires"],
                "cookies": requests.utils.dict_from_cookiejar(s["client"].session.cookies),
            }
            for token, s in self._sessions.items()
        }
        try:
            tmp = SESSIONS_FILE + ".tmp"
            with open(tmp, "w") as f:
                json.dump(stored, f)
            os.replace(tmp, SESSIONS_FILE)
        except Exception:
            pass

    def _purge_expired(self):
        now = time.time()
        with self._lock:
            for email in [e for e, v in self._pending.items() if v["expires"] < now]:
                del self._pending[email]
            expired = [t for t, v in self._sessions.items() if v["expires"] < now]
            for token in expired:
                del self._sessions[token]
            if expired:
                self._save_locked()

    def request_code(self, email: str) -> tuple[bool, str | None]:
        email = (email or "").strip().lower()
        if not email:
            return False, "email is required"
        self._purge_expired()

        client = FleetAPIClient(self.base_url)
        ok, err = client.send_otp(email)
        if not ok:
            return False, err or "could not send verification code"

        with self._lock:
            self._pending[email] = {"client": client, "expires": time.time() + PENDING_TTL_S}
        return True, None

    def verify_code(self, email: str, code: str) -> tuple[str | None, str | None]:
        email = (email or "").strip().lower()
        code = (code or "").strip()
        self._purge_expired()

        with self._lock:
            pending = self._pending.get(email)
        if not pending:
            return None, "request a new code first"

        ok, err = pending["client"].verify_otp(email, code)
        if not ok:
            return None, err or "invalid verification code"

        with self._lock:
            self._pending.pop(email, None)
            token = secrets.token_urlsafe(32)
            # Keep the OTP-verified client: its cookies are a live
            # fleet.shiftiq.us session we can reuse for API queries on
            # behalf of this dashboard user.
            self._sessions[token] = {
                "email": email,
                "expires": time.time() + SESSION_TTL_S,
                "client": pending["client"],
            }
            self._save_locked()
        return token, None

    def check_session(self, token: str | None) -> str | None:
        """Returns the authenticated email for a valid session token, else None."""
        if not token:
            return None
        self._purge_expired()
        with self._lock:
            session = self._sessions.get(token)
        return session["email"] if session else None

    def get_client(self, token: str | None) -> FleetAPIClient | None:
        """Returns the fleet.shiftiq.us-authenticated client for a valid
        dashboard session token, else None."""
        if not token:
            return None
        self._purge_expired()
        with self._lock:
            session = self._sessions.get(token)
        return session["client"] if session else None

    def get_any_client(self) -> FleetAPIClient | None:
        """Returns any signed-in user's fleet client — used by background
        refreshers when the monitor has no credentials of its own."""
        self._purge_expired()
        with self._lock:
            for s in self._sessions.values():
                return s["client"]
        return None

    def logout(self, token: str | None):
        if not token:
            return
        with self._lock:
            if self._sessions.pop(token, None) is not None:
                self._save_locked()
