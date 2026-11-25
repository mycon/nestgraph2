"""
Application configuration loaded from config/config.json.

The config file path can be overridden via the CONFIG_PATH environment variable.
Default path: config/config.json  (relative to cwd, or /app/config/config.json in Docker)
"""

import json
import os
from pathlib import Path

_CONFIG_PATH = Path(os.getenv("CONFIG_PATH", "config/config.json"))


def _load() -> dict:
    if not _CONFIG_PATH.exists():
        raise FileNotFoundError(
            f"Config file not found: {_CONFIG_PATH}\n"
            "Copy config/config.json.example to config/config.json and fill in your values."
        )
    with _CONFIG_PATH.open() as f:
        # Strip _comment keys before parsing
        raw = json.load(f)
    return raw


_cfg = _load()


def _section(name: str) -> dict:
    return _cfg.get(name, {})


def _require(section: str, key: str) -> str:
    val = _section(section).get(key)
    if not val:
        raise ValueError(
            f"Required config [{section}][{key}] is missing or empty in {_CONFIG_PATH}"
        )
    return val


# ── Nest ──────────────────────────────────────────────────────────────────────
_nest = _section("nest")
# Pre-extracted browser-session tokens
NEST_ACCESS_TOKEN: str       = _nest.get("access_token", "")
NEST_TRANSPORT_URL: str      = _nest.get("transport_url", "")
NEST_USER_ID: str            = _nest.get("user_id", "")
NEST_USER: str               = _nest.get("user", "")
# Direct Google OAuth Bearer token for gRPC-Web calls (ya29.xxx); refreshed
# automatically when issue_token+cookies are valid; can also be pasted here.
NEST_GOOGLE_BEARER_TOKEN: str = _nest.get("google_bearer_token", "")

# ── InfluxDB ──────────────────────────────────────────────────────────────────
_influx = _section("influxdb")
INFLUXDB_URL: str    = _require("influxdb", "url")
INFLUXDB_TOKEN: str  = _require("influxdb", "token")
INFLUXDB_ORG: str    = _influx.get("org", "myorg")
INFLUXDB_BUCKET_ROOT: str = _influx.get("bucket", "nestgraph")
INFLUXDB_BUCKET_V2_RAW: str = f"{INFLUXDB_BUCKET_ROOT}_v2_raw"
INFLUXDB_BUCKET_V2_MODEL: str = f"{INFLUXDB_BUCKET_ROOT}_v2_model"
INFLUXDB_BUCKET_V2_ROLLUP: str = f"{INFLUXDB_BUCKET_ROOT}_v2_rollup"
INFLUXDB_BUCKET_V2_EVENT: str = f"{INFLUXDB_BUCKET_ROOT}_v2_event"
INFLUXDB_BUCKET_V2_DEVMAP: str = f"{INFLUXDB_BUCKET_ROOT}_v2_devmap"

# ── Weather ───────────────────────────────────────────────────────────────────
_weather = _section("weather")
WEATHER_LAT: float | None = _weather.get("lat")
WEATHER_LON: float | None = _weather.get("lon")

# ── Settings ──────────────────────────────────────────────────────────────────
_settings = _section("settings")
TZ: str                   = _settings.get("timezone", "America/New_York")
TEMP_UNITS: str           = _settings.get("temp_units", "F").upper()
ANALOG_MIN_INTERVAL_SECONDS: int   = int(_settings.get("analog_min_interval_seconds", 60))
WEATHER_POLL_INTERVAL_SECONDS: int = int(_settings.get("weather_poll_interval_seconds", 900))
MAPPING_SYNC_INTERVAL_SECONDS: int = int(_settings.get("mapping_sync_interval_seconds", 86400))
DEVICE_LABEL_SHORT_ID_LEN: int = int(_settings.get("device_label_short_id_len", 6))
LOG_LEVEL: str            = _settings.get("log_level", "INFO").upper()

# ── connect_nest stream configuration ─────────────────────────────────────────
_daemon = _section("connect_nest")
CONNECT_NEST_OBSERVE_TIMEOUT_SEC: float = float(_daemon.get("observe_timeout_sec", 120))
CONNECT_NEST_TOKEN_REFRESH_MARGIN_SEC: float = float(_daemon.get("token_refresh_margin_sec", 300))
# Data source: "foyer" (default, Google Home Foyer protobufs only; Observe disabled),
#              "auto" (Observe stream with automatic Foyer fallback), or "observe"
CONNECT_NEST_DATA_SOURCE: str = str(_daemon.get("data_source", "foyer")).strip().lower()
CONNECT_NEST_FOYER_POLL_INTERVAL_SEC: float = float(_daemon.get("foyer_poll_interval_sec", 30.0))
CONNECT_NEST_COOKIE_ROTATE_INTERVAL_SEC: float = float(_daemon.get("cookie_rotate_interval_sec", 540.0))



# ── Test ──────────────────────────────────────────────────────────────────────
_test = _section("test")
TEST_THERMOSTAT_DEVICE_RESOURCE_ID: str = _test.get("thermostat_device_resource_id", "")
TEST_THERMOSTAT_SERIAL: str             = _test.get("thermostat_serial", "")
TEST_STRUCTURE_ID: str                   = _test.get("structure_id", "")
TEST_NEST_USER_ID_KNOWN: str            = _test.get("nest_user_id_known", "")

# Dynamically derive redundant variations if not explicitly defined
TEST_THERMOSTAT_SERIAL_SHORT: str       = _test.get("thermostat_serial_short") or TEST_THERMOSTAT_SERIAL.lower()
TEST_THERMOSTAT_DEVICE_SHORT: str       = _test.get("thermostat_device_short") or TEST_THERMOSTAT_DEVICE_RESOURCE_ID.removeprefix("DEVICE_").lower()
TEST_THERMOSTAT_DEVICE_FULL: str        = _test.get("thermostat_device_full") or TEST_THERMOSTAT_DEVICE_RESOURCE_ID.lower()
