#!/usr/bin/env python3
"""
api_client.py
Thin client for the fleet.shiftiq.us JSON API: password/OTP authentication
and the mcap-sync session + fleet status endpoints. No HTML parsing —
everything here talks directly to the same JSON endpoints the dashboard
frontend uses.
"""

import requests
from urllib.parse import quote

BASE_URL = "https://fleet.shiftiq.us"


class FleetAPIError(Exception):
    """Raised when a fleet.shiftiq.us API call fails or returns bad data."""


def _as_float(v) -> float:
    """A sort key that survives a null or a string timestamp."""
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


class FleetAPIClient:
    def __init__(self, base_url: str = BASE_URL, timeout: float = 10.0):
        self.base_url = base_url
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "fleet-monitor/2.0"})

    # ── auth ────────────────────────────────────────────────────────────
    def login_password(self, email: str, password: str) -> tuple[bool, str | None]:
        try:
            self.session.get(self.base_url + "/", timeout=self.timeout)
        except requests.RequestException as e:
            return False, f"could not reach {self.base_url}: {e}"

        try:
            r = self.session.post(
                f"{self.base_url}/api/auth/password",
                json={"email": email, "password": password or ""},
                timeout=self.timeout,
            )
            data = r.json()
        except (requests.RequestException, ValueError) as e:
            return False, str(e)

        if r.ok and data.get("ok"):
            return True, None
        return False, data.get("error", "invalid credentials")

    def send_otp(self, email: str) -> tuple[bool, str | None]:
        try:
            r = self.session.post(
                f"{self.base_url}/api/auth/otp/send",
                json={"email": email},
                timeout=self.timeout,
            )
            data = r.json()
        except (requests.RequestException, ValueError) as e:
            return False, str(e)

        if r.ok and data.get("ok"):
            return True, None
        return False, data.get("error", "failed to send verification code")

    def verify_otp(self, email: str, token: str) -> tuple[bool, str | None]:
        try:
            r = self.session.post(
                f"{self.base_url}/api/auth/otp/verify",
                json={"email": email, "token": token},
                timeout=self.timeout,
            )
            data = r.json()
        except (requests.RequestException, ValueError) as e:
            return False, str(e)

        if r.ok and data.get("ok"):
            return True, None
        return False, data.get("error", "invalid verification code")

    # ── data ────────────────────────────────────────────────────────────
    def get_fleet_status(self) -> list[dict] | None:
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

    def get_device_notes(self, hostname: str) -> list[dict] | None:
        """Returns the fleet's maintenance notes for a device, newest first.

        Each note is {"id", "text", "by", "created_at"}. Unlike the session
        endpoints this one is answered by fleet.shiftiq.us itself rather than
        proxied through to the Pi, so it stays fast even for an offline rig —
        which is exactly when the note ("hotspot spotty in field") is worth
        reading.

        Returns None if the fleet does not know the hostname, as opposed to []
        for a known device carrying no notes. Callers surface that difference
        as a 404 rather than an empty list, so a typo in a hostname does not
        look like a rig nobody has written about.
        """
        url = f"{self.base_url}/api/notes/{quote(hostname, safe='')}"
        try:
            r = self.session.get(url, timeout=self.timeout)
        except requests.RequestException as e:
            raise FleetAPIError(f"notes for {hostname} unreachable: {e}") from e
        if r.status_code == 404:
            return None
        if not r.ok:
            raise FleetAPIError(f"notes for {hostname} returned status {r.status_code}")
        try:
            data = r.json()
        except ValueError as e:
            raise FleetAPIError(f"notes for {hostname} returned invalid JSON: {e}") from e

        notes = data.get("notes") or []
        # The API happens to hand these back newest-first, but nothing
        # documents that. Sorting here means both backends and the page agree
        # on the order without each repeating the rule.
        notes.sort(key=lambda n: _as_float(n.get("created_at")), reverse=True)
        return notes

    def get_device_sessions(self, hostname: str, light: bool = True, limit: int = 100,
                            timeout: float | None = None, retries: int = 1) -> list[dict]:
        """Returns the deduplicated session_groups for a device from the
        mcap-sync statusboard API (each entry already merges the per-camera
        recordings that belong to one operator session). The proxy hop to the
        Pi can be slow, so this uses a longer timeout and one retry by
        default."""
        url = f"{self.base_url}/proxy/{hostname}/statusboard-api/mcap-sync/sessions"
        params = {"limit": limit}
        if light:
            params["light"] = 1
        timeout = timeout if timeout is not None else max(self.timeout, 30.0)

        r = None
        last_exc = None
        for _ in range(retries + 1):
            try:
                r = self.session.get(url, params=params, timeout=timeout)
                break
            except requests.RequestException as e:
                last_exc = e
        if r is None:
            raise FleetAPIError(f"{hostname} unreachable: {last_exc}") from last_exc
        if not r.ok:
            raise FleetAPIError(f"{hostname} returned status {r.status_code}")
        try:
            data = r.json()
        except ValueError as e:
            raise FleetAPIError(f"{hostname} returned invalid JSON: {e}") from e

        return data.get("session_groups") or []
