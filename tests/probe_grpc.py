"""
Probe the Nest protobuf gateway to resolve thermostat IDs and fetch energy data.

Auth hierarchy (first that works):
  1. nest.access_token in config.json  (pre-extracted Nest JWT)
  2. home.nest.com/session endpoint    (with nest.cookies)
  3. NestClient full login flow        (issue_token + cookies)

Usage:
    python tests/probe_grpc.py
"""
import sys
import logging
import requests
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
logging.basicConfig(level=logging.ERROR)

import src.config as cfg
from src.nest_client import NestClient, NestAuthError
import src.token_store as token_store
from src.grpc_web import (
    get_resource_id_mapping, energy_history_request,
    decode_proto_fields, observe_snapshot, OBSERVE_TRAITS_THERMOSTAT,
    build_observe_request, parse_observe_response,
)

THERMOSTAT_DEVICE_RESOURCE_ID = cfg.TEST_THERMOSTAT_DEVICE_RESOURCE_ID
THERMOSTAT_SERIAL   = cfg.TEST_THERMOSTAT_SERIAL
STRUCTURE_ID        = cfg.TEST_STRUCTURE_ID
NEST_USER_ID_KNOWN  = cfg.TEST_NEST_USER_ID_KNOWN
if not NEST_USER_ID_KNOWN:
    try:
        NEST_USER_ID_KNOWN = token_store.load().get("userid", "")
    except Exception:
        pass

# ── Get a valid Nest JWT ───────────────────────────────────────────────────────

def _try_session_endpoint(cookies: str) -> str:
    """Hit home.nest.com/session to exchange browser cookies for a Nest JWT."""
    if not cookies:
        return ""
    try:
        resp = requests.get(
            "https://home.nest.com/session",
            headers={
                "Cookie": cookies,
                "X-Requested-With": "XMLHttpRequest",
                "Referer": "https://home.nest.com/",
            },
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json()
            return data.get("access_token", "")
        else:
            print(f"  /session returned HTTP {resp.status_code}")
    except Exception as e:
        print(f"  /session attempt failed: {e}")
    return ""


def _jwt_is_valid(jwt: str, margin_sec: int = 60) -> bool:
    """Return True if the Nest JWT exists and has more than *margin_sec* left."""
    if not jwt:
        return False
    try:
        import base64 as _b64, json as _json, time as _time
        parts = jwt.split(".")
        if len(parts) < 4:
            return False
        pad = 4 - len(parts[3]) % 4
        claims = _json.loads(_b64.urlsafe_b64decode(parts[3] + ("=" * pad if pad != 4 else "")))
        return claims.get("exp", 0) - _time.time() > margin_sec
    except Exception:
        return False


nest_jwt = cfg.NEST_ACCESS_TOKEN if _jwt_is_valid(cfg.NEST_ACCESS_TOKEN) else ""

if not nest_jwt:
    print("Stored JWT missing/expired — trying home.nest.com/session with cookies...")
    tokens_data = token_store.load()
    google_cookies = tokens_data.get("google_cookie_header")
    nest_jwt = _try_session_endpoint(google_cookies)
    if nest_jwt:
        print(f"  Got JWT from /session: {nest_jwt[:60]}...")

if not nest_jwt:
    print("Trying full NestClient login flow...")
    try:
        nest = NestClient()
        nest_jwt = nest._access_token
        if nest_jwt:
            print(f"  Got JWT from NestClient: {nest_jwt[:60]}...")
    except NestAuthError as e:
        print(f"  NestClient login failed: {e}")

if not nest_jwt:
    print()
    print("No Nest JWT available.  To proceed:")
    print()
    print("  OPTION A - Easy, paste JWT:")
    print("  1. Open Chrome -> https://home.nest.com (logged in)")
    print("  2. DevTools -> Network -> find the GET 'session' request")
    print("  3. Click 'Preview' on the response")
    print("  4. Copy the 'access_token' value (starts with g.0.eyJ...)")
    print("  5. Paste into config/config.json -> nest.access_token")
    print()
    print("  OPTION B - Update cookies for home.nest.com session:")
    print("  1. DevTools -> Application -> Cookies -> home.nest.com")
    print("  2. Find cookie named 'n'  (the Nest session cookie)")
    print("  3. Set config/config.json -> nest.cookies  to  'n=<value>'")
    sys.exit(1)

print(f"\nNest JWT : {nest_jwt[:80]}...")
print(f"UserID   : {NEST_USER_ID_KNOWN}")
print()

# ── GetResourceIdMappingByLegacyIds ───────────────────────────────────────────
print("=" * 60)
print("  GetResourceIdMappingByLegacyIds")
print("=" * 60)

legacy_ids = [
    f"user.{NEST_USER_ID_KNOWN}",
    THERMOSTAT_DEVICE_RESOURCE_ID,
    f"structure.{STRUCTURE_ID}",
]

result = get_resource_id_mapping(legacy_ids, nest_jwt)
print(f"  HTTP status : {result['http_status']}")
if result.get("raw_bytes"):
    print(f"  Response: {len(result['raw_bytes'])} bytes")
    print(f"  Hex: {result['raw_bytes'].hex()[:300]}")

if result["id_map"]:
    print("  ID mappings:")
    for k, v in result["id_map"].items():
        print(f"    {k!r} -> {v!r}")

if result["proto_fields"]:
    print("  Proto fields:")
    for fnum, wtype, val in result["proto_fields"]:
        if isinstance(val, bytes):
            s = val.decode("utf-8", "replace")
            print(f"    field={fnum} wire={wtype}  {s[:100]!r}")
        else:
            print(f"    field={fnum} wire={wtype}  {val}")
elif result["http_status"] == 200 and not result.get("raw_bytes"):
    print("  (empty response body)")

print()

# ── energy_history SendCommand ────────────────────────────────────────────────
print("=" * 60)
print("  SendCommand: LegacyEnergyHistoryRequest")
print("=" * 60)

status, body = energy_history_request(THERMOSTAT_DEVICE_RESOURCE_ID, nest_jwt)
print(f"  HTTP status : {status}")
if body:
    print(f"  Response: {len(body)} bytes")
    print(f"  Hex (400 chars): {body.hex()[:400]}")
    print("  Proto fields:")
    fields = decode_proto_fields(body)
    for fnum, wtype, val in fields:
        if isinstance(val, bytes):
            s = val.decode("utf-8", "replace")
            print(f"    field={fnum} wire={wtype}  {s[:120]!r}")
        else:
            print(f"    field={fnum} wire={wtype}  {val}")
else:
    print("  (empty response body)")

# ── Observe (streaming snapshot) ─────────────────────────────────────────────
print()
print("=" * 60)
print("  Observe: thermostat traits snapshot (15s timeout)")
print("=" * 60)
print(f"  Subscribing to {len(OBSERVE_TRAITS_THERMOSTAT)} traits: {OBSERVE_TRAITS_THERMOSTAT}")
print()

messages = observe_snapshot(OBSERVE_TRAITS_THERMOSTAT, nest_jwt, timeout_sec=15.0)
print(f"  Received {len(messages)} message(s)")

for i, msg in enumerate(messages):
    if not msg:
        continue
    print(f"\n  --- Message {i+1}: {len(msg)} bytes ---")

    devices = parse_observe_response(msg)
    import json
    print(json.dumps(devices, indent=4))

