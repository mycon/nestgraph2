"""
Unofficial Nest thermostat API client.

Mirrors the behaviour of the PHP library at nest-api/nest.class.php (the
gboudreau/nest-api submodule) using the same undocumented REST endpoints at
home.nest.com. Authentication is token/session based:

**Google account (issue_token + cookies)**
    For Google-migrated Nest accounts.  Extract `issue_token` (the URL from
    the 2nd XHR request visible in browser DevTools on nest.com) and
    `cookies` (the full Cookie header from the same request), then store
    them in `config/tokens.json` (`issue_token` and `google_cookie_header`).
    The client performs the 3-step OAuth exchange automatically and caches
    the resulting session.

**Browser session (pre-extracted tokens)**
  If you already extracted tokens manually (cztoken, transport_url, etc.),
  supply `access_token`, `transport_url`, `user_id`, and `user` directly.

Session data is cached to /tmp/nestcache/ (or NESTCACHE_DIR env var) so that
container restarts don't require a fresh login every time.
"""

from __future__ import annotations

import calendar
import base64
import hashlib
import json
import logging
import os
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote
from requests.cookies import RequestsCookieJar

import requests

from src import token_store

log = logging.getLogger(__name__)

USER_AGENT = "Nest/5.0.0.23 (iOScom.nestlabs.jasper.release) os=11.0"
PROTOCOL_VERSION = 1
GOOGLE_JWT_URL = "https://nestauthproxyservice-pa.googleapis.com/v1/issue_jwt"
GOOGLE_API_KEY = "AIzaSyAdkSIMNc51XGNEAYWasX9UOWkS5P6sZE4"  # Nest website public key  # legacy; no longer used for pickle
# Firefox on Windows UA — matches HAR capture (Firefox cadence, ~55-min issueToken refresh)
GOOGLE_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:140.0) "
    "Gecko/20100101 Firefox/140.0"
)
# Margin before Google bearer expiry at which to trigger a proactive issueToken refresh.
# HAR shows ~3300 s (55 min) inter-call delta; 250 s margin keeps cadence tight.
BEARER_REFRESH_MARGIN_SEC: float = 250.0
REFRESH_RETRY_BACKOFF_SEC: float = 300.0
GOOGLE_ACCEPT_LANGUAGE = "en-US,en;q=0.9"

# The exact 20 cookies carried on every issueToken request per Firefox HAR analysis.
# Stable cookies (16): values stay constant across all 7 observed issueToken calls.
# Rotating cookies (4): rotated by issueToken responses; see ISSUETOKEN_ROTATING_COOKIES.
ISSUETOKEN_ALL_COOKIES: frozenset[str] = frozenset({
    "ACCOUNT_CHOOSER",
    "APISID",
    "HSID",
    "LSID",
    "NID",
    "OTZ",
    "SAPISID",
    "SID",
    "SIDCC",
    "SMSV",
    "SSID",
    "__Host-1PLSID",
    "__Host-3PLSID",
    "__Host-GAPS",
    "__Secure-1PAPISID",
    "__Secure-1PSID",
    "__Secure-1PSIDCC",
    "__Secure-3PAPISID",
    "__Secure-3PSID",
    "__Secure-3PSIDCC",
})

# Subset of ISSUETOKEN_ALL_COOKIES that are refreshed by every issueToken response.
# Per HAR: SIDCC/__Secure-1PSIDCC/__Secure-3PSIDCC rotate every ~55-min cycle;
# NID is updated occasionally.  Only these are read from issueToken Set-Cookie
# responses so the stable 16 cookies are never overwritten by partial responses.
ISSUETOKEN_ROTATING_COOKIES: frozenset[str] = frozenset({
    "SIDCC",
    "__Secure-1PSIDCC",
    "__Secure-3PSIDCC",
    "NID",
})

# Device type constants — mirrors PHP DEVICE_TYPE_* defines
DEVICE_TYPE_THERMOSTAT = "thermostat"
DEVICE_TYPE_PROTECT = "protect"
DEVICE_TYPE_SENSOR = "sensor"

# Bucket types to request in app_launch — mirrors PHP getStatusUserBuckets().
# If the server returns 400 with a list of disallowed types, _get_status_via_app_launch
# will strip them and retry automatically.
APP_LAUNCH_BUCKETS = [
    "buckets", "delayed_topaz", "demand_response", "device", "device_alert_dialog",
    "geofence_info", "kryptonite", "link", "message", "message_center", "metadata",
    "occupancy", "quartz", "safety", "rcs_settings", "safety_summary", "schedule",
    "shared", "structure", "structure_history", "structure_metadata", "topaz",
    "topaz_resource", "track", "trip", "tuneups", "user", "user_alert_dialog",
    "user_settings", "where", "widget_track",
]

# Cycle type bitmask values from energy_latest
CYCLE_HEAT_STAGE1 = 1
CYCLE_HEAT_STAGE2 = 2
CYCLE_COOL_STAGE1 = 4
CYCLE_COOL_STAGE2 = 8
CYCLE_FAN = 16
CYCLE_COOL_HEATPUMP = 32  # heat-pump compressor cooling
CYCLE_HUMIDIFIER = 64
CYCLE_DEHUMIDIFIER = 128


def _cycle_type_name(type_int: int) -> str:
    """Return a human-readable name for a cycle type integer."""
    names = {
        CYCLE_HEAT_STAGE1: "heat_stage1",
        CYCLE_HEAT_STAGE2: "heat_stage2",
        CYCLE_COOL_STAGE1: "cool_stage1",
        CYCLE_COOL_STAGE2: "cool_stage2",
        CYCLE_FAN: "fan",
        CYCLE_COOL_HEATPUMP: "cool_heatpump",
        CYCLE_HUMIDIFIER: "humidifier",
        CYCLE_DEHUMIDIFIER: "dehumidifier",
    }
    return names.get(type_int, f"type_{type_int}")


class NestAuthError(RuntimeError):
    pass


class NestClient:
    def __init__(
        self,
        access_token: str = "",
        transport_url: str = "",
        user_id: str = "",
        user: str = "",
        issue_token: str = "",
        cookies: str = "",
        google_bearer_token: str = "",
        token_file: str | None = None,
        refresh_margin_sec: float = 250.0,
    ) -> None:
        self._issue_token = str(issue_token or "").strip()
        self._cookies = str(cookies or "").strip()
        self._session = requests.Session()
        self._google_session = requests.Session()
        self._session.headers.update({"User-Agent": USER_AGENT})
        self._google_session.headers.update({
            "User-Agent": GOOGLE_USER_AGENT,
            "Accept": "*/*",
            "Accept-Language": GOOGLE_ACCEPT_LANGUAGE,
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Connection": "keep-alive",
        })
        self._token_file = token_file  # path for flat-file JSON token store
        self._refresh_margin_sec = refresh_margin_sec

        self._transport_url: str = transport_url
        self._access_token: str = access_token
        self._google_bearer_token: str = google_bearer_token
        self._google_bearer_expiration: float = 0.0
        self._user: str = user
        self._userid: str = user_id
        self._cache_expiration: float = 0.0
        self._last_status: dict[str, Any] | None = None
        self._last_refresh_path: str = "unknown"
        self._refresh_retry_after: float = 0.0

        # If caller supplied pre-extracted browser-session tokens, use them directly.
        if self._access_token and self._transport_url:
            log.info("Using pre-supplied session tokens (skipping login).")
            self._load_cache()
            if self._cookies:
                self._bootstrap_google_session_cookies()
            self._update_auth_headers()
            self._cache_expiration = time.time() + 365 * 86400
        else:
            self._load_cache()
            if self._cookies:
                self._bootstrap_google_session_cookies()
            if not self._is_cache_valid():
                self._login()

    @staticmethod
    def _parse_cookie_header(cookie_header: str) -> dict[str, str]:
        """Parse an HTTP Cookie header string into a name->value mapping."""
        out: dict[str, str] = {}
        attr_names = {
            "expires",
            "max-age",
            "domain",
            "path",
            "secure",
            "httponly",
            "samesite",
            "priority",
            "partitioned",
        }
        for raw_part in (cookie_header or "").split(";"):
            part = raw_part.strip()
            if not part or "=" not in part:
                continue
            key, value = part.split("=", 1)
            key = key.strip()
            value = value.strip()
            if key and key.lower() not in attr_names:
                out[key] = value
        return out

    def _cookie_header_from_google_session(self) -> str:
        """Flatten the google-session cookie jar to a Cookie header string."""
        pairs: list[str] = []
        seen: set[str] = set()
        for cookie in self._google_session.cookies:
            name = cookie.name
            if not name or name in seen:
                continue
            seen.add(name)
            pairs.append(f"{name}={cookie.value}")
        return "; ".join(pairs)

    def _cookie_header_for_request(self) -> str:
        """Build an HTTP-safe Cookie header from tracked cookies.

        urllib3/http.client requires header values encodable to latin-1. Some
        copied browser cookie values can contain Unicode characters (for example
        ACCOUNT_CHOOSER metadata), which would otherwise raise UnicodeEncodeError
        during request send. Percent-encoding keeps semantics while producing an
        ASCII-only header string.
        """
        cookie_map = self._parse_cookie_header(self._cookies)
        parts: list[str] = []
        for name, value in cookie_map.items():
            safe_name = quote(name, safe="!#$%&'*+-.^_`|~")
            safe_value = quote(value, safe="!#$%&'()*+-./:<=>?@[]^_`{|}~")
            parts.append(f"{safe_name}={safe_value}")
        return "; ".join(parts)

    def _home_cookie_value(self, name: str) -> str:
        for cookie in self._session.cookies:
            if cookie.name == name and cookie.value:
                return str(cookie.value)
        return ""

    def _set_home_auth_cookies(self, token: str) -> None:
        token = str(token or "").strip()
        if not token:
            return
        for cookie_name in ("cztoken", "user_token"):
            self._session.cookies.set(cookie_name, token, domain="home.nest.com", path="/")
            self._session.cookies.set(cookie_name, token, domain=".home.nest.com", path="/")

    @staticmethod
    def _token_fingerprint(token: str) -> str:
        token = str(token or "").strip()
        if not token:
            return "none"
        digest = hashlib.sha256(token.encode("utf-8")).hexdigest()[:12]
        return f"len={len(token)} sha12={digest}"

    def auth_debug_snapshot(self) -> dict[str, Any]:
        access_token = str(self._access_token or "")
        cztoken = self._home_cookie_value("cztoken")
        transport_token = cztoken or access_token
        source = "cztoken" if cztoken else "access_token"
        return {
            "transport_source": source,
            "transport_fp": self._token_fingerprint(transport_token),
            "access_fp": self._token_fingerprint(access_token),
            "cztoken_fp": self._token_fingerprint(cztoken),
            "access_equals_cztoken": bool(access_token and cztoken and access_token == cztoken),
        }

    def _bootstrap_google_session_cookies(self) -> None:
        """Seed the Google cookie jar from configured/header cookie text."""
        cookie_map = self._parse_cookie_header(self._cookies)
        if not cookie_map:
            return

        jar: RequestsCookieJar = self._google_session.cookies
        for name, value in cookie_map.items():
            # Seed for common Google auth domains used in issue_token flow.
            jar.set(name, value, domain=".google.com", path="/")
            jar.set(name, value, domain="accounts.google.com", path="/")

        log.info(
            "Seeded Google OAuth session with %d cookies: %s",
            len(cookie_map),
            ", ".join(sorted(cookie_map.keys()))
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def access_token(self) -> str:
        return self._access_token

    @property
    def transport_auth_token(self) -> str:
        cookie_token = self._home_cookie_value("cztoken")
        if cookie_token:
            return cookie_token
        return self._access_token

    @property
    def token_expiry(self) -> float:
        return self._cache_expiration

    def refresh_if_needed(self) -> bool:
        """
        Re-authenticate if the Nest JWT is within *refresh_margin_sec* of expiry.

        This intentionally mirrors nest-api behaviour: it does not proactively
        refresh based on Google bearer lifetime, and instead refreshes only from
        Nest JWT cache expiry (or explicit forced refresh on auth failure).

        Returns True if a refresh was performed, False if auth is still fresh.
        Raises NestAuthError if the refresh fails and no valid token remains.
        """
        now = time.time()

        nest_jwt_valid = token_store.is_valid(
            {"access_token": self._access_token,
             "transport_url": self._transport_url,
             "cache_expiration": self._cache_expiration},
            margin_sec=self._refresh_margin_sec,
        )
        # Match Firefox cadence: proactively refresh when Google bearer is near
        # expiry (~55-min cycle per HAR). bearer_expiration==0.0 means no bearer
        # is in use (for example pre-extracted token path), so skip the check.
        google_bearer_valid = (
            self._google_bearer_expiration == 0.0
            or self._google_bearer_expiration > now + BEARER_REFRESH_MARGIN_SEC
        )
        if nest_jwt_valid and google_bearer_valid:
            return False
        if self._refresh_retry_after > now and self._cache_expiration > now:
            log.debug(
                "Nest JWT refresh suppressed until %.0f (in %.0fs) after prior failed refresh",
                self._refresh_retry_after,
                self._refresh_retry_after - now,
            )
            return False
        log.info("Nest JWT near expiry — refreshing …")
        try:
            self._login()
            self._refresh_retry_after = 0.0
            return True
        except NestAuthError as exc:
            # Keep running on the current Nest JWT when refresh fails but the
            # token has not actually expired yet. This avoids reconnect loops
            # caused by transient issue_token failures (for example USER_LOGGED_OUT).
            still_valid = bool(
                self._access_token
                and self._transport_url
                and self._cache_expiration > time.time()
            )
            if still_valid:
                retry_after = min(
                    now + REFRESH_RETRY_BACKOFF_SEC,
                    max(now + 60.0, self._cache_expiration - 30.0),
                )
                self._refresh_retry_after = retry_after
                log.warning(
                    "Refresh failed but current Nest JWT is still valid; "
                    "continuing with existing session until next retry at %s: %s",
                    time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(retry_after)),
                    exc,
                )
                return False
            raise

    def refresh_now(self) -> None:
        """Force a fresh login regardless of cached token expiry."""
        log.info("Forcing immediate token refresh …")
        self._login()

    def auth_status(self) -> dict[str, Any]:
        """Return safe auth metadata for diagnostics (no token values)."""
        return {
            "nest_jwt_expires_unix": self._cache_expiration or None,
            "google_bearer_present": bool(self._google_bearer_token),
            "google_bearer_expires_unix": self._google_bearer_expiration or None,
            "last_refresh_path": self._last_refresh_path,
            "refresh_retry_after_unix": self._refresh_retry_after or None,
        }

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_devices(self, device_type: str = DEVICE_TYPE_THERMOSTAT) -> list[str]:
        """Return device serial numbers for the given device type.

        Mirrors PHP Nest::getDevices().  ``device_type`` is one of
        ``DEVICE_TYPE_THERMOSTAT`` (default), ``DEVICE_TYPE_PROTECT``, or
        ``DEVICE_TYPE_SENSOR``.

        For thermostats, falls back to ``rcs_settings`` keys when
        ``structure.devices`` is empty — rcs_settings are keyed by thermostat
        serial, so they expose the serial even when structure.devices is not
        populated by the API.
        """
        status = self._get_status()

        if device_type == DEVICE_TYPE_PROTECT:
            return list(status.get("topaz", {}).keys())

        if device_type == DEVICE_TYPE_SENSOR:
            return list(status.get("kryptonite", {}).keys())

        # DEVICE_TYPE_THERMOSTAT: from structure.devices (mirrors PHP)
        serials: list[str] = []
        for structure in status.get("structure", {}).values():
            for ref in structure.get("devices", []):
                serial = ref.split(".")[-1]
                if serial not in serials:
                    serials.append(serial)

        # Fallback: rcs_settings are keyed by thermostat serial
        if not serials and status.get("rcs_settings"):
            serials = list(status["rcs_settings"].keys())
            log.debug("Thermostat serials from rcs_settings fallback: %s", serials)

        return serials

    def get_sensors(self) -> dict[str, Any]:
        """Return a dict of sensor serial → sensor info for Nest Temperature Sensors.

        Convenience wrapper — delegates to get_devices(DEVICE_TYPE_SENSOR) +
        get_device_info(), mirroring the PHP getDevices(DEVICE_TYPE_SENSOR) +
        getDeviceInfo() pattern.
        """
        return {
            serial: self.get_device_info(serial)
            for serial in self.get_devices(DEVICE_TYPE_SENSOR)
        }

    def get_user_locations(self) -> list[dict[str, Any]]:
        """Return the full device tree organized by structure (location).

        Mirrors PHP Nest::getUserLocations().  Each entry contains:
          structure_id, name, address, city, postal_code, country,
          away, away_last_changed, thermostats, protects, sensors
        where thermostats / protects / sensors are lists of serial strings.
        """
        status = self._get_status()
        locations: list[dict[str, Any]] = []

        topaz = status.get("topaz", {})
        kryptonite = status.get("kryptonite", {})

        for struct_id, structure in status.get("structure", {}).items():
            # Nest Protects at this location
            protects = [
                serial for serial, p in topaz.items()
                if p.get("structure_id") == struct_id
            ]
            # Temperature sensors at this location
            sensors = [
                serial for serial, s in kryptonite.items()
                if s.get("structure_id") == struct_id
            ]
            # Thermostats from structure.devices (mirrors PHP cleanDevices())
            thermostats = [
                ref.split(".")[-1]
                for ref in structure.get("devices", [])
            ]

            # Mirrors PHP: skip completely empty locations
            if not protects and not sensors and not thermostats:
                continue

            locations.append({
                "structure_id": struct_id,
                "name": structure.get("name", ""),
                "address": structure.get("street_address") or None,
                "city": structure.get("location", ""),
                "postal_code": structure.get("postal_code", ""),
                "country": structure.get("country_code", ""),
                "away": bool(structure.get("away", False)),
                "away_last_changed": structure.get("away_timestamp"),
                "thermostats": thermostats,
                "protects": protects,
                "sensors": sensors,
            })

        return locations

    def _get_where_by_id(self, structure_id: str, where_id: str) -> str:
        """Resolve a where_id to a human-readable location name."""
        if not where_id:
            return ""
        status = self._get_status()
        # where bucket keyed by structure_id; value has a 'wheres' list
        where_data = status.get("where", {}).get(structure_id, {})
        for w in where_data.get("wheres", []):
            if w.get("where_id") == where_id:
                return w.get("name", where_id)
        return where_id

    def get_device_info(self, serial: str) -> dict[str, Any]:
        """
        Return current device state for a thermostat, sensor, or protect.

        Mirrors PHP Nest::getDeviceInfo() — checks kryptonite (sensors) and
        topaz (protects) before falling back to device/shared for thermostats.
        """
        status = self._get_status()

        # ── Kryptonite: Nest Temperature Sensor ─────────────────────────
        kryptonite = status.get("kryptonite", {})
        if serial in kryptonite:
            sensor = kryptonite[serial]
            structure_id = sensor.get("structure_id", "")
            where_id = sensor.get("where_id", "")
            return {
                "serial": serial,
                "device_type": DEVICE_TYPE_SENSOR,
                "temperature": self._to_f(sensor.get("current_temperature", 0)),
                "battery_level": sensor.get("battery_level"),
                "last_updated": sensor.get("last_updated_at", 0),
                "where_id": where_id,
                "location": self._get_where_by_id(structure_id, where_id),
                "structure_id": structure_id,
                "model": sensor.get("model", ""),
            }

        # ── Topaz: Nest Protect ──────────────────────────────────────────
        topaz = status.get("topaz", {})
        if serial in topaz:
            protect = topaz[serial]
            return {
                "serial": serial,
                "device_type": DEVICE_TYPE_PROTECT,
                "co_status": protect.get("co_status", 0),
                "smoke_status": protect.get("smoke_status", 0),
                "battery_level": protect.get("battery_level"),
                "battery_health_state": protect.get("battery_health_state", 0),
                "model": protect.get("model", ""),
                "software_version": protect.get("software_version", ""),
                "online": protect.get("component_wifi_test_passed"),
                "name": protect.get("description", ""),
                "where": self._get_where_by_id(
                    protect.get("structure_id", ""),
                    protect.get("spoken_where_id", ""),
                ),
                "replace_by_date_utc_secs": protect.get("replace_by_date_utc_secs"),
            }

        # ── Thermostat: device / shared / link / track ───────────────────
        shared = status.get("shared", {}).get(serial, {})
        device = status.get("device", {}).get(serial, {})
        track = status.get("track", {}).get(serial, {})

        # Resolve structure for away state
        link = status.get("link", {}).get(serial, {})
        structure_ref = link.get("structure", "")
        structure_id = structure_ref.split(".")[-1] if structure_ref else ""
        structure = status.get("structure", {}).get(structure_id, {})
        structure_away = bool(structure.get("away", False))

        target_mode = shared.get("target_temperature_type", "heat")
        eco = device.get("eco", {})
        eco_mode = eco.get("mode", "schedule")  # manual-eco, auto-eco, schedule

        # Determine current mode
        if target_mode == "off":
            mode = "off"
        elif eco_mode != "schedule":
            # In eco — check which eco temps are enabled
            low_enabled = device.get("away_temperature_low_enabled", False)
            high_enabled = device.get("away_temperature_high_enabled", False)
            if low_enabled and high_enabled:
                mode = "range"
            elif low_enabled:
                mode = "heat"
            elif high_enabled:
                mode = "cool"
            else:
                mode = "off"
        else:
            mode = target_mode  # heat / cool / range

        # Target temperatures (Celsius from API; converted later)
        if target_mode == "range":
            target_temp = self._to_f(shared.get("target_temperature_low", 0))
            target_temp_high = self._to_f(shared.get("target_temperature_high", 0))
        else:
            target_temp = self._to_f(shared.get("target_temperature", 0))
            target_temp_high = None

        current_modes = [mode]
        if eco_mode != "schedule":
            current_modes.append(eco_mode)
        if structure_away:
            current_modes.append("away")

        # Last connection timestamp (milliseconds → epoch seconds)
        last_conn_ms = track.get("last_connection", 0)
        last_connection = last_conn_ms / 1000 if last_conn_ms else time.time()

        return {
            "serial": serial,
            "device_type": DEVICE_TYPE_THERMOSTAT,
            "name": shared.get("name", ""),
            "timestamp": last_connection,
            "mode": ",".join(current_modes),
            "target_mode": target_mode,
            "current_temp": self._to_f(shared.get("current_temperature", 0)),
            "target_temp": target_temp,
            "target_temp_high": target_temp_high,
            "humidity": device.get("current_humidity", 0),
            # Simple on/off flags (match old PHP collect.php)
            "heating": int(bool(shared.get("hvac_heater_state", False))),
            "cooling": int(bool(shared.get("hvac_ac_state", False))),
            "fan": int(bool(shared.get("hvac_fan_state", False))),
            "leaf": int(bool(device.get("leaf", False))),
            "away": int(structure_away),
            "auto_away": shared.get("auto_away", 0),
            # --- Stage 1 / Stage 2 / Alt detail ---
            "heat_stage1": int(bool(shared.get("hvac_heater_state", False))),
            "heat_stage2": int(bool(shared.get("hvac_heat_x2_state", False))),
            "heat_stage3": int(bool(shared.get("hvac_heat_x3_state", False))),
            "heat_alt": int(bool(shared.get("hvac_alt_heat_state", False))),
            "heat_alt_stage2": int(bool(shared.get("hvac_alt_heat_x2_state", False))),
            "heat_aux": int(bool(shared.get("hvac_aux_heater_state", False))),
            "heat_emergency": int(bool(shared.get("hvac_emer_heat_state", False))),
            "cool_stage1": int(bool(shared.get("hvac_ac_state", False))),
            "cool_stage2": int(bool(shared.get("hvac_cool_x2_state", False))),
            "cool_stage3": int(bool(shared.get("hvac_cool_x3_state", False))),
            # Temperatures
            "backplate_temp": self._to_f(device.get("backplate_temperature", 0)) if device.get("backplate_temperature") else None,
            # Humidifier / hot water (if present)
            "humidifier": int(bool(device.get("humidifier_state", False))),
            "has_humidifier": bool(device.get("has_humidifier", False)),
            "hot_water": bool(device.get("hot_water_active")) if device.get("has_hot_water_control") else None,
            # Fan mode detail (only present on devices that have a fan)
            "fan_mode": device.get("fan_mode") if device.get("has_fan") else None,
        }

    def get_energy_latest(self, serial: str) -> dict[str, Any]:
        """
        Return the last ~10 days of energy/cycle history.

        Mirrors PHP getEnergyLatest().  The `objects` list contains dicts with
        a `value.days[]` list, where each day has `cycles[]` and `events[]`.
        """
        payload = {
            "objects": [{"object_key": f"energy_latest.{serial}"}]
        }
        result = self._post("/v5/subscribe", payload)
        return result

    # ------------------------------------------------------------------
    # Auth / session helpers
    # ------------------------------------------------------------------

    def _login(self) -> None:
        if self._issue_token and self._cookies:
            self._login_google()
        else:
            raise NestAuthError(
                "Missing issue_token/cookies for token refresh. "
                "Provide connect_nest.token_file with issue_token and google_cookie_header, "
                "or supply access_token and transport_url directly."
            )
        self._update_auth_headers()
        self._save_cache()

    def _login_google(self) -> None:
        """
        Google account login via issue_token + cookies.

        Mirrors the PHP library's Google auth flow:
          1. GET issue_token URL with Google cookies → Bearer token
          2. POST to nestauthproxyservice → JWT (Nest access_token) + userid
          3. POST app_launch with ["user"] bucket → transport_url + user key

        IMPORTANT: Google's OAuth session (represented by cookies) has an implicit
        server-side timeout of ~45-50 minutes. The cookies file is meant for
        ONE-TIME bootstrap only. After ~45 minutes of operation, issueToken will
        begin returning USER_LOGGED_OUT regardless of cookie freshness because
        the server-side session has expired. At that point, the only solution is
        to provide fresh cookies from an active browser session.
        """
        log.info("Logging in to Nest API (Google OAuth / issue_token) …")

        # Try to get a fresh bearer token from the issue_token + cookies flow.
        # This works as long as the cookies represent a valid server-side session
        # (typically for ~45-50 minutes after initial capture from the browser).
        try:
            bearer_token = self._exchange_issue_token_for_bearer()
            self._google_bearer_token = bearer_token
            self._issue_nest_jwt_from_bearer(bearer_token)
            self._complete_google_login()
            self._last_refresh_path = "issue_token_exchange"
            log.info("Google Nest login successful for userid=%s", self._userid)
            return
        except NestAuthError as exc:
            log.warning("issue_token exchange failed (cookies may have server-side session timeout): %s", exc)

        # Browser parity fallback: once home.nest.com/session has been bootstrapped,
        # it sets user_token/cztoken cookies with multi-day lifetime. Use that
        # channel before giving up when issueToken starts returning USER_LOGGED_OUT.
        try:
            if self._refresh_from_home_session_cookie():
                self._last_refresh_path = "home_session_cookie_refresh"
                log.info("Google Nest login successful for userid=%s (home session cookie refresh)", self._userid)
                return
        except NestAuthError as exc:
            log.warning("home session cookie refresh failed: %s", exc)

        # Attempt to use cached bearer token as a fallback.
        # This may work for a short time but should not be relied upon for
        # sustained operation beyond the original token's lifetime.
        if self._google_bearer_token:
            try:
                self._issue_nest_jwt_from_bearer(self._google_bearer_token)
                self._complete_google_login()
                self._last_refresh_path = "cached_google_bearer_fallback"
                log.info("Google Nest login successful for userid=%s (using cached bearer token)", self._userid)
                log.warning(
                    "Cached bearer token fallback is temporary. "
                    "For sustained operation, provide fresh cookies from an active browser session."
                )
                return
            except NestAuthError as exc:
                log.warning("Cached bearer token fallback also failed: %s", exc)

        raise NestAuthError(
            "Google login failed: issue_token exchange failed and cached bearer token unavailable. "
            "Cookies may have reached server-side session timeout (~45-50 min). "
            "Provide fresh cookies from an active browser session (Cookie header from issueToken request in DevTools)."
        )

    @staticmethod
    def _jwt_expiration_unix(jwt_token: str) -> float:
        """Best-effort decode of JWT exp claim to Unix timestamp."""
        try:
            parts = jwt_token.split(".")
            if len(parts) != 3:
                return 0.0
            payload = parts[1]
            payload += "=" * (-len(payload) % 4)
            decoded = base64.urlsafe_b64decode(payload.encode("ascii"))
            data = json.loads(decoded.decode("utf-8"))
            return float(data.get("exp", 0.0) or 0.0)
        except Exception:
            return 0.0

    def _refresh_from_home_session_cookie(self) -> bool:
        """Try refreshing auth state through home.nest.com/session cookies.
        
        This uses the existing _access_token in the Authorization header
        (if available) to tell the server which JWT to sync cookies to.
        Browser behavior shows /session with both Authorization and cookies.
        """
        headers = {}
        if self._access_token:
            headers["Authorization"] = f"Basic {self._access_token}"
        
        session_resp = self._session.get(
            f"https://home.nest.com/session?_={int(time.time() * 1000)}",
            headers={
                **headers,
                "Accept": "*/*",
                "Accept-Language": GOOGLE_ACCEPT_LANGUAGE,
                "Referer": "https://home.nest.com/",
                "X-Requested-With": "XMLHttpRequest",
                "Alt-Used": "home.nest.com",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
                "HostName": "home.nest.com",
                "TE": "trailers",
            },
            timeout=30,
        )
        if not session_resp.ok:
            raise NestAuthError(
                f"home session cookie refresh failed: HTTP {session_resp.status_code} — {session_resp.text[:300]}"
            )
        try:
            session_data = session_resp.json()
        except Exception as exc:
            raise NestAuthError(f"home session cookie refresh returned non-JSON response: {exc}") from exc

        refreshed_token = str(session_data.get("access_token") or "").strip()
        if not refreshed_token:
            raise NestAuthError("home session cookie refresh missing access_token")

        self._access_token = refreshed_token
        self._set_home_auth_cookies(refreshed_token)
        log.debug("home session cookie refresh token snapshot: %s", self.auth_debug_snapshot())
        session_userid = str(session_data.get("userid") or "").strip()
        if session_userid:
            self._userid = session_userid
        session_user = str(session_data.get("user") or "").strip()
        if session_user:
            self._user = session_user

        # Prefer JWT exp claim as authoritative expiry for the returned token.
        # Some /session expires_in values can outlive the JWT's real validity.
        jwt_exp = self._jwt_expiration_unix(refreshed_token)
        expires_in = str(session_data.get("expires_in") or "").strip()
        parsed_expires_in = 0.0
        if expires_in:
            try:
                parsed_expires_in = float(calendar.timegm(
                    time.strptime(expires_in, "%Y-%m-%dT%H:%M:%SZ")
                ))
            except Exception:
                parsed_expires_in = 0.0

        self._cache_expiration = jwt_exp or parsed_expires_in

        if self._cache_expiration <= 0:
            # Safety net for unknown expiry: assume ~1 hour JWT lifetime.
            self._cache_expiration = time.time() + 3500

        # Bearer lifetime is not used for proactive refresh policy.
        self._google_bearer_expiration = 0.0

        # app_launch refreshes transport_url and canonical user object key.
        self._complete_google_login()
        return True

    def _exchange_issue_token_for_bearer(self) -> str:
        """Exchange issue_token + browser cookies for a Google OAuth bearer token.

        The Cookie header is built explicitly from self._cookies so that all
        20 issueToken cookies (per Firefox HAR) are always sent, even if the
        requests jar failed to store some of them (e.g. __Host-* prefixed names).

        After each response, ONLY the rotating cookies (SIDCC / __Secure-1PSIDCC /
        __Secure-3PSIDCC / NID) are updated in self._cookies.  The 16 stable
        cookies are never overwritten by a partial Set-Cookie response, preventing
        the cookie-set from degrading over multiple refresh cycles.
        """
        # Headers match Firefox on Windows making a cors XHR to the OAuth iframe.
        bearer_resp = self._google_session.get(
            self._issue_token,
            headers={
                "Accept": "*/*",
                "Accept-Language": GOOGLE_ACCEPT_LANGUAGE,
                "Accept-Encoding": "gzip, deflate, br, zstd",
                "X-Requested-With": "XmlHttpRequest",
                "Alt-Used": "accounts.google.com",
                "Referer": "https://accounts.google.com/o/oauth2/iframe",
                # Explicit Cookie header ensures all config-seeded cookies are sent
                # regardless of what the requests jar holds for each domain/prefix.
                "Cookie": self._cookie_header_for_request(),
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
                "Connection": "keep-alive",
                "Pragma": "no-cache",
                "Cache-Control": "no-cache",
                "TE": "trailers",
            },
            timeout=30,
        )

        # Extract Set-Cookie values from the response and apply ONLY the rotating
        # ones to self._cookies.  This preserves the full 20-cookie set intact.
        response_cookies = {c.name: c.value for c in bearer_resp.cookies if c.name}
        rotating_updates = {
            k: v for k, v in response_cookies.items()
            if k in ISSUETOKEN_ROTATING_COOKIES
        }
        if rotating_updates:
            current_map = self._parse_cookie_header(self._cookies)
            current_map.update(rotating_updates)
            self._cookies = "; ".join([f"{k}={v}" for k, v in current_map.items()])
            # Keep jar in sync so the session sends fresh values on any jar-based calls.
            for name, value in rotating_updates.items():
                self._google_session.cookies.set(name, value, domain=".google.com", path="/")
                self._google_session.cookies.set(name, value, domain="accounts.google.com", path="/")
            log.debug(
                "issueToken: updated %d rotating cookie(s): %s (tracked total=%d)",
                len(rotating_updates),
                ", ".join(sorted(rotating_updates.keys())),
                len(self._parse_cookie_header(self._cookies)),
            )
        elif response_cookies:
            log.debug(
                "issueToken: response carried %d non-rotating Set-Cookie(s) (not applied): %s",
                len(response_cookies),
                ", ".join(sorted(response_cookies.keys())),
            )
        else:
            log.debug("issueToken: response carried no Set-Cookie headers")
        
        bearer_data: dict[str, Any] = {}
        try:
            bearer_data = bearer_resp.json()
        except Exception:
            bearer_data = {}

        bearer_token = bearer_data.get("access_token", "")
        if not bearer_token:
            error_code = str(bearer_data.get("error") or "").strip()
            detail = str(bearer_data.get("detail") or "").strip()
            cookie_names = sorted(self._parse_cookie_header(self._cookies).keys())
            first_party_markers = (
                "SID", "HSID", "SSID", "SAPISID", "APISID",
                "__Secure-1PSID", "__Secure-1PAPISID", "__Secure-1PSIDTS",
            )
            has_first_party = any(name in cookie_names for name in first_party_markers)
            cookie_summary = ",".join(cookie_names[:8]) + ("..." if len(cookie_names) > 8 else "")
            
            # Detailed logging for issue_token failure
            log.debug(
                "issue_token exchange failed: URL=%s status=%s error=%s detail=%s "
                "cookie_count=%d has_first_party=%s response=%s",
                self._issue_token[:100], bearer_resp.status_code, error_code, detail,
                len(cookie_names), has_first_party, bearer_resp.text[:300]
            )
            
            if error_code == "USER_LOGGED_OUT":
                raise NestAuthError(
                    f"Google issue_token returned USER_LOGGED_OUT (session expired). "
                    f"Cookies expired or bound to a different session. "
                    f"Recapture fresh Cookie header from iframerpc request in active browser. "
                    f"cookie_names={cookie_summary or 'none'}"
                )
            raise NestAuthError(
                f"Google issue_token response missing access_token (error={error_code}): {bearer_resp.text[:400]}"
            )
        expires_in = bearer_data.get("expires_in")
        try:
            if expires_in is not None:
                self._google_bearer_expiration = time.time() + float(expires_in)
            else:
                self._google_bearer_expiration = time.time() + 3500
        except Exception:
            self._google_bearer_expiration = time.time() + 3500
        return bearer_token

    def _issue_nest_jwt_from_bearer(self, bearer_token: str) -> None:
        """Exchange a Google bearer token for a Nest JWT and userid."""
        jwt_resp = requests.post(
            GOOGLE_JWT_URL,
            json={
                "embed_google_oauth_access_token": True,
                "expire_after": "3600s",
                "google_oauth_access_token": bearer_token,
                "policy_id": "authproxy-oauth-policy",
            },
            headers={
                "Authorization": f"Bearer {bearer_token}",
                "User-Agent": GOOGLE_USER_AGENT,
                "Accept": "*/*",
                "Accept-Language": "en-US,en;q=0.5",
                "X-Goog-API-Key": GOOGLE_API_KEY,
                "Referer": "https://home.nest.com",
                "Origin": "https://home.nest.com",
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "cross-site",
            },
            timeout=30,
        )
        try:
            jwt_data = jwt_resp.json()
        except Exception:
            jwt_data = {}

        if not jwt_resp.ok:
            msg = ""
            if isinstance(jwt_data, dict):
                msg = (
                    jwt_data.get("error", {}).get("message")
                    or jwt_data.get("error_description", "")
                    or jwt_data.get("error", "")
                )
            detail = f" ({msg})" if msg else ""
            raise NestAuthError(
                f"Nest JWT exchange failed: HTTP {jwt_resp.status_code}{detail} — {jwt_resp.text[:400]}"
            )

        nest_id = (jwt_data.get("claims", {}).get("subject", {})
                          .get("nestId", {}).get("id", ""))
        if not nest_id:
            raise NestAuthError(
                f"Google JWT response missing nestId: {jwt_resp.text[:400]}"
            )
        jwt_token = jwt_data.get("jwt", "")
        if not jwt_token:
            raise NestAuthError(
                f"Google JWT response missing jwt token: {jwt_resp.text[:400]}"
            )

        self._userid = nest_id
        self._access_token = jwt_token
        expiry_str = jwt_data["claims"]["expirationTime"]
        if "." in expiry_str:  # strip fractional seconds if present
            expiry_str = expiry_str.split(".")[0] + "Z"
        self._cache_expiration = float(calendar.timegm(
            time.strptime(expiry_str, "%Y-%m-%dT%H:%M:%SZ")
        ))

    def _complete_google_login(self) -> None:
        """Finalize Google login by bootstrapping session and resolving transport_url."""

        # Browser parity: after issue_jwt, the web app calls /session before
        # app_launch. This call sets cztoken/user_token cookies and returns
        # session metadata used by downstream transport APIs.
        self._bootstrap_home_session()

        # Step 3: app_launch to get transport_url and user key
        launch_resp = self._session.post(
            f"https://home.nest.com/api/0.1/user/{self._userid}/app_launch",
            data=json.dumps({"known_bucket_types": ["user"], "known_bucket_versions": []}),
            headers={
                "Content-Type": "text/json",
                "Authorization": f"Basic {self._access_token}",
            },
            timeout=30,
        )
        launch_data = launch_resp.json()
        transport_url = (launch_data.get("service_urls", {})
                                    .get("urls", {})
                                    .get("transport_url", ""))
        if not transport_url:
            raise NestAuthError(
                f"app_launch response missing transport_url: {launch_resp.text[:400]}"
            )
        self._transport_url = transport_url
        self._user = f"user.{self._userid}"  # derived; matches PHP fallback
        for bucket in launch_data.get("updated_buckets", []):
            if bucket.get("object_key", "").startswith("user."):
                self._user = bucket["object_key"]
                break

    def _bootstrap_home_session(self) -> None:
        """Call home.nest.com/session to align state with browser login flow."""
        session_resp = self._session.get(
            f"https://home.nest.com/session?_={int(time.time() * 1000)}",
            headers={"Authorization": f"Basic {self._access_token}"},
            timeout=30,
        )
        if not session_resp.ok:
            raise NestAuthError(
                f"home session bootstrap failed: HTTP {session_resp.status_code} — "
                f"{session_resp.text[:300]}"
            )

        try:
            session_data = session_resp.json()
        except Exception as exc:
            raise NestAuthError(f"home session bootstrap returned non-JSON response: {exc}") from exc

        # Keep server-issued canonical identity fields when available.
        session_userid = str(session_data.get("userid") or "").strip()
        if session_userid:
            self._userid = session_userid
        session_user = str(session_data.get("user") or "").strip()
        if session_user:
            self._user = session_user

        # If /session returns a fresh access token, prefer it.
        session_access_token = str(session_data.get("access_token") or "").strip()
        if session_access_token and session_access_token != self._access_token:
            self._access_token = session_access_token
            log.debug("home session bootstrap returned refreshed access token")

        if session_access_token:
            self._set_home_auth_cookies(session_access_token)
        elif self._access_token:
            self._set_home_auth_cookies(self._access_token)

        log.debug("home session bootstrap token snapshot: %s", self.auth_debug_snapshot())

        # Optional expiry in legacy format; ignore if missing/unparseable.
        expires_in = str(session_data.get("expires_in") or "").strip()
        if expires_in:
            try:
                self._cache_expiration = float(calendar.timegm(
                    time.strptime(expires_in, "%Y-%m-%dT%H:%M:%SZ")
                ))
            except Exception:
                pass

        # Keep expiry aligned to the token itself when /session rotates JWT.
        jwt_exp = self._jwt_expiration_unix(self._access_token)
        if jwt_exp > 0:
            self._cache_expiration = jwt_exp

    def _update_auth_headers(self) -> None:
        """Set Authorization and user-id headers on the shared session."""
        self._session.headers.update({
            "Authorization": f"Basic {self._access_token}",
            "X-nl-user-id": self._userid,
            "X-nl-protocol-version": str(PROTOCOL_VERSION),
        })

    def _is_cache_valid(self) -> bool:
        return bool(
            self._transport_url
            and self._access_token
            and self._cache_expiration > time.time()
        )

    def _load_cache(self) -> None:
        data = token_store.load(self._token_file)
        if not data:
            return
        cached_issue_token = str(data.get("issue_token", "") or "").strip()
        if cached_issue_token:
            self._issue_token = cached_issue_token
        self._transport_url = data.get("transport_url", "")
        self._access_token = data.get("access_token", "")
        if not self._google_bearer_token:
            self._google_bearer_token = data.get("google_bearer_token", "")
        cached_cookie_header = str(data.get("google_cookie_header", "") or "")
        if cached_cookie_header:
            cached_map = self._parse_cookie_header(cached_cookie_header)
            if self._cookies:
                # Backward compatibility: when a config cookie header is still present,
                # prefer it for stable values and merge in rotating cookies from cache.
                rotating_from_cache = {
                    k: v for k, v in cached_map.items()
                    if k in ISSUETOKEN_ROTATING_COOKIES
                }
                if rotating_from_cache:
                    original_map = self._parse_cookie_header(self._cookies)
                    merged = {**original_map, **rotating_from_cache}
                    self._cookies = "; ".join([f"{k}={v}" for k, v in merged.items()])
                    log.debug(
                        "Loaded %d cached rotating cookie(s) into config header (total=%d): %s",
                        len(rotating_from_cache),
                        len(merged),
                        ", ".join(sorted(rotating_from_cache.keys())),
                    )
            else:
                # Primary mode: cookies are fully sourced from token store.
                self._cookies = "; ".join([f"{k}={v}" for k, v in cached_map.items()])
                log.debug("Loaded %d cookie(s) from token store", len(cached_map))
        self._google_bearer_expiration = float(data.get("google_bearer_expiration", 0.0) or 0.0)
        self._user = data.get("user", "")
        self._userid = data.get("userid", "")
        self._cache_expiration = float(data.get("cache_expiration", 0.0))
        if self._access_token:
            self._update_auth_headers()

    def _save_cache(self) -> None:
        token_store.save(
            {
                "transport_url": self._transport_url,
                "access_token": self._access_token,
                "issue_token": self._issue_token,
                "google_bearer_token": self._google_bearer_token,
                # Save self._cookies: the authoritative tracked set (all tracked Google cookies
                # with rotating ones updated by issueToken responses).  Do NOT use the
                # jar — the jar may be missing __Host-* cookies that the requests library
                # fails to store under a domain attribute.
                "google_cookie_header": self._cookies,
                "google_bearer_expiration": self._google_bearer_expiration,
                "user": self._user,
                "userid": self._userid,
                "cache_expiration": self._cache_expiration,
            },
            self._token_file,
        )

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    def _get_status(self, retry: bool = True) -> dict[str, Any]:
        if self._last_status is not None:
            return self._last_status

        # Try the modern app_launch bucket fetch first (required for Google accounts).
        # Mirrors PHP getStatusUserBuckets().
        status = self._get_status_via_app_launch()
        if not status and retry:
            # Fall back to the legacy /v3/mobile/ endpoint for older accounts.
            log.debug("app_launch returned empty status; falling back to /v3/mobile/")
            status = self._get_status_via_mobile(retry=False)

        self._last_status = status
        return status

    def _get_status_via_app_launch(self) -> dict[str, Any]:
        """Fetch full status via app_launch with all bucket types (PHP getStatusUserBuckets).

        If the server returns 400 with a "Non-allowed known bucket types" error,
        the disallowed types are removed and the request is retried once.
        """
        allowed = list(APP_LAUNCH_BUCKETS)
        for attempt in range(2):
            try:
                resp = self._session.post(
                    f"https://home.nest.com/api/0.1/user/{self._userid}/app_launch",
                    data=json.dumps({
                        "known_bucket_types": allowed,
                        "known_bucket_versions": [],
                    }),
                    headers={"Content-Type": "text/json"},
                    timeout=30,
                )
                if resp.status_code == 400 and "Non-allowed known bucket types" in resp.text:
                    # Parse disallowed list from error: "Set(a, b, c)"
                    import re
                    m = re.search(r"Set\(([^)]+)\)", resp.text)
                    if m and attempt == 0:
                        disallowed = {s.strip() for s in m.group(1).split(",")}
                        log.debug("app_launch: removing disallowed bucket types: %s", disallowed)
                        allowed = [b for b in allowed if b not in disallowed]
                        continue  # retry
                    log.warning("app_launch failed (400): %s", resp.text[:300])
                    return {}
                if not resp.ok:
                    log.warning("app_launch status fetch failed: %s %s — %s",
                                resp.status_code, resp.reason, resp.text[:300])
                    return {}
                result = resp.json()
                break
            except Exception as exc:
                log.warning("app_launch status fetch failed: %s", exc)
                return {}
        else:
            return {}
        status: dict[str, Any] = {}
        for bucket in result.get("updated_buckets", []):
            object_key = bucket.get("object_key", "")
            if "." not in object_key:
                continue
            bucket_type, bucket_id = object_key.split(".", 1)
            if bucket_type not in status:
                status[bucket_type] = {}
            status[bucket_type][bucket_id] = bucket.get("value", {})
        return status

    def _get_status_via_mobile(self, retry: bool = True) -> dict[str, Any]:
        """Legacy /v3/mobile/ status fetch."""
        url = f"/v3/mobile/{self._user}"
        status = self._get(url)
        if not isinstance(status, dict):
            raise RuntimeError(f"Unexpected status response type: {type(status)}")
        if status.get("cmd") == "REINIT_STATE":
            if retry:
                log.warning("Nest API returned REINIT_STATE; clearing cache and re-login.")
                self._cache_path.unlink(missing_ok=True)
                self._last_status = None
                self._login()
                return self._get_status_via_mobile(retry=False)
            raise RuntimeError("Nest API REINIT_STATE loop — aborting.")
        return status

    def clear_status_cache(self) -> None:
        """Call between polls to force a fresh status fetch next time."""
        self._last_status = None

    def _get(self, path: str) -> Any:
        url = self._transport_url + path if not path.startswith("http") else path
        resp = self._session.get(url, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def _post(self, path: str, data: dict) -> Any:
        url = self._transport_url + path if not path.startswith("http") else path
        resp = self._session.post(
            url,
            data=json.dumps(data),
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    def _to_f(self, celsius: float) -> float:
        """Convert Celsius (Nest internal scale) to Fahrenheit."""
        return round(celsius * 1.8 + 32, 2)
