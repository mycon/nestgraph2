"""
Google Home Foyer (home.google.com) RPC client and cookie rotation helper.

Implements the authentication and data flows captured from ``home.google.com``
and decoded using compiled Foyer protobuf definitions under
``nest-observe/protobuf/pb/foyer/`` (generated into ``proto/generated/foyer/``):

1. Cookie keep-alive via ``accounts.google.com/RotateCookiesPage`` +
   ``POST accounts.google.com/RotateCookies`` (~600 s browser cadence), keeping
   ``__Secure-1PSIDTS``, ``__Secure-3PSIDTS``, ``SIDCC``, ``__Secure-1PSIDCC``,
   and ``__Secure-3PSIDCC`` fresh.
2. ``SAPISIDHASH`` / ``SAPISID1PHASH`` / ``SAPISID3PHASH`` request signing for
   ``https://googlehomefoyer-pa.clients6.google.com/$rpc/...`` supporting both
   JSON+protobuf (JSPB) and binary ``application/x-protobuf`` wire formats.
3. Typed Foyer RPCs decoded into compiled ``*_pb2.py`` protobuf messages:
   - ``UsersService/GetNestAccountLinkState``
   - ``StructuresService/GetHomeGraph``
   - ``EnergyService/GetThermostatStatus`` (precise temps + HVAC stage breakdown)
   - ``EnergyService/GetWiringConfiguration`` (terminal wiring)
   - ``HomeControlService/GetTraits`` (fan speed, remote temperature sensors)
   - ``HomeOccupancyService/GetStructureMode`` (HOME / AWAY)
   - ``ClientOliveTokenService/GetOliveToken``
4. Conversion of Foyer snapshots into the canonical Observe snapshot structure
   consumed by ``EventThermostatCollector`` so InfluxDB schemas stay unchanged.
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import re
import sys
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable, TypeVar
from urllib.parse import quote

import requests
from google.protobuf import struct_pb2
from google.protobuf.descriptor import FieldDescriptor
from google.protobuf.message import Message

log = logging.getLogger(__name__)

_ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_PROTO_GENERATED_DIR = os.path.abspath(os.path.join(_ROOT_DIR, "proto", "generated"))
if _PROTO_GENERATED_DIR not in sys.path:
    sys.path.insert(0, _PROTO_GENERATED_DIR)

from foyer import (  # noqa: E402
    common_pb2,
    energy_pb2,
    home_control_pb2,
    home_occupancy_pb2,
    structures_pb2,
    users_pb2,
)

TMessage = TypeVar("TMessage", bound=Message)

FOYER_BASE_URL = "https://googlehomefoyer-pa.clients6.google.com"
HOME_ORIGIN = "https://home.google.com"
DEFAULT_FOYER_API_KEY = "AIzaSyCMqap8NH88PrhvoBwY1W8ChRUJRjIOJXM"
DEFAULT_ROTATE_REFERER = (
    "https://accounts.google.com/RotateCookiesPage"
    "?og_pid=621&rot=3&origin=https%3A%2F%2Fhome.google.com&exp_id=0"
)
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:140.0) "
    "Gecko/20100101 Firefox/140.0"
)

FOYER_ROTATING_COOKIES: frozenset[str] = frozenset({
    "SIDCC",
    "__Secure-1PSIDCC",
    "__Secure-3PSIDCC",
    "__Secure-1PSIDTS",
    "__Secure-3PSIDTS",
    "NID",
})

_HOMEGRAPH_REQUEST_BODY: list[Any] = [
    None,
    None,
    None,
    None,
    None,
    None,
    [
        ["user.invite.unrestricted"],
        [
            "structure.update.subscription_nestaware",
            "routine.create",
            "structure.update.membership",
            "routine.read",
        ],
        [
            "device.execute.camera_onoff",
            "device.read.camera_history",
            "device.read.camera_notifications",
        ],
    ],
    1,
    None,
    [1, 2],
    None,
    None,
    None,
    None,
    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14, 15, 16, 17, 18, 19, 20],
]

_THERMOSTAT_MODE_TO_SETPOINT_TYPE: dict[int, str] = {
    energy_pb2.THERMOSTAT_MODE_HEAT: "heat",
    energy_pb2.THERMOSTAT_MODE_COOL: "cool",
    energy_pb2.THERMOSTAT_MODE_RANGE: "range",
    energy_pb2.THERMOSTAT_MODE_OFF: "off",
    energy_pb2.THERMOSTAT_MODE_ECO: "eco",
}

_INT_FIELD_TYPES = frozenset({
    FieldDescriptor.TYPE_INT32,
    FieldDescriptor.TYPE_INT64,
    FieldDescriptor.TYPE_UINT32,
    FieldDescriptor.TYPE_UINT64,
    FieldDescriptor.TYPE_SINT32,
    FieldDescriptor.TYPE_SINT64,
    FieldDescriptor.TYPE_FIXED32,
    FieldDescriptor.TYPE_FIXED64,
    FieldDescriptor.TYPE_SFIXED32,
    FieldDescriptor.TYPE_SFIXED64,
    FieldDescriptor.TYPE_ENUM,
})
_FLOAT_FIELD_TYPES = frozenset({
    FieldDescriptor.TYPE_FLOAT,
    FieldDescriptor.TYPE_DOUBLE,
})


class FoyerAuthError(RuntimeError):
    """Foyer rejected authentication credentials (HTTP 401/403)."""


class FoyerError(RuntimeError):
    """Non-auth Foyer RPC error."""


# ── Generic Protobuf <-> JSPB wire codec ─────────────────────────────────────


def _is_map_field(field_desc: FieldDescriptor) -> bool:
    return (
        field_desc.type == FieldDescriptor.TYPE_MESSAGE
        and field_desc.message_type is not None
        and field_desc.message_type.GetOptions().map_entry
    )


def _coerce_jspb_scalar(value: Any, field_desc: FieldDescriptor) -> Any:
    ftype = field_desc.type
    if ftype == FieldDescriptor.TYPE_BOOL:
        if isinstance(value, str):
            return value.strip().lower() not in ("", "0", "false")
        return bool(value)
    if ftype in _INT_FIELD_TYPES:
        return int(value)
    if ftype in _FLOAT_FIELD_TYPES:
        return float(value)
    if ftype == FieldDescriptor.TYPE_STRING:
        return str(value)
    if ftype == FieldDescriptor.TYPE_BYTES:
        if isinstance(value, (bytes, bytearray)):
            return bytes(value)
        if isinstance(value, str):
            return base64.b64decode(value)
        return bytes(value)
    return value


def _populate_field_from_jspb(msg: Message, field_desc: FieldDescriptor, raw_val: Any) -> None:
    if raw_val is None:
        return

    if _is_map_field(field_desc):
        if not isinstance(raw_val, list):
            return
        entry_desc = field_desc.message_type
        key_fd = entry_desc.fields_by_name["key"]
        val_fd = entry_desc.fields_by_name["value"]
        map_container = getattr(msg, field_desc.name)
        for pair in raw_val:
            if not isinstance(pair, list) or len(pair) < 2 or pair[0] is None:
                continue
            try:
                k = _coerce_jspb_scalar(pair[0], key_fd)
            except (TypeError, ValueError):
                continue
            v_raw = pair[1]
            if v_raw is None:
                continue
            if val_fd.type == FieldDescriptor.TYPE_MESSAGE:
                if isinstance(v_raw, list):
                    _populate_proto_from_jspb(v_raw, map_container[k])
            else:
                try:
                    map_container[k] = _coerce_jspb_scalar(v_raw, val_fd)
                except (TypeError, ValueError):
                    continue
        return

    if field_desc.label == FieldDescriptor.LABEL_REPEATED:
        if not isinstance(raw_val, list):
            return
        container = getattr(msg, field_desc.name)
        if field_desc.type == FieldDescriptor.TYPE_MESSAGE:
            for elem in raw_val:
                if elem is None:
                    continue
                if isinstance(elem, list):
                    sub = container.add()
                    _populate_proto_from_jspb(elem, sub)
        else:
            for elem in raw_val:
                if elem is None:
                    continue
                try:
                    container.append(_coerce_jspb_scalar(elem, field_desc))
                except (TypeError, ValueError):
                    continue
        return

    if field_desc.type == FieldDescriptor.TYPE_MESSAGE:
        if isinstance(raw_val, list) and len(raw_val) > 0:
            sub = getattr(msg, field_desc.name)
            _populate_proto_from_jspb(raw_val, sub)
        return

    try:
        setattr(msg, field_desc.name, _coerce_jspb_scalar(raw_val, field_desc))
    except (TypeError, ValueError) as exc:
        log.debug(
            "Skipping incompatible JSPB value for %s.%s: %r (%s)",
            msg.DESCRIPTOR.name,
            field_desc.name,
            raw_val,
            exc,
        )


def _populate_proto_from_jspb(data: list[Any], msg: Message) -> None:
    fields_by_num = msg.DESCRIPTOR.fields_by_number
    last_idx = len(data) - 1
    for idx, item in enumerate(data):
        if item is None:
            continue
        # JSPB places high/extension field numbers in a trailing dict {"<num>": val}
        if idx == last_idx and isinstance(item, dict):
            for k, v in item.items():
                try:
                    fnum = int(k)
                except (TypeError, ValueError):
                    continue
                fd = fields_by_num.get(fnum)
                if fd is not None:
                    _populate_field_from_jspb(msg, fd, v)
            continue
        fd = fields_by_num.get(idx + 1)
        if fd is not None:
            _populate_field_from_jspb(msg, fd, item)


def jspb_to_proto(data: Any, msg_cls: type[TMessage]) -> TMessage:
    """Decode a JSPB array, binary protobuf payload, or existing Message into ``msg_cls``.

    Maps JSPB positional array index ``i`` to protobuf field number ``i + 1``
    using ``msg_cls.DESCRIPTOR.fields_by_number``. Unknown fields are ignored
    for forward compatibility.
    """
    if isinstance(data, msg_cls):
        return data
    msg = msg_cls()
    if isinstance(data, (bytes, bytearray)):
        msg.ParseFromString(bytes(data))
        return msg
    if isinstance(data, list):
        _populate_proto_from_jspb(data, msg)
    return msg


def _scalar_to_jspb(value: Any, field_desc: FieldDescriptor) -> Any:
    if field_desc.type == FieldDescriptor.TYPE_BYTES:
        return base64.b64encode(bytes(value)).decode("ascii")
    if field_desc.type == FieldDescriptor.TYPE_BOOL:
        return 1 if value else 0
    return value


def proto_to_jspb(msg: Message) -> list[Any]:
    """Serialize a compiled protobuf Message into Google's positional JSPB array."""
    populated: dict[int, Any] = {}
    for field_desc, value in msg.ListFields():
        fnum = field_desc.number
        if _is_map_field(field_desc):
            entry_desc = field_desc.message_type
            val_fd = entry_desc.fields_by_name["value"]
            pairs: list[list[Any]] = []
            for k, v in value.items():
                if val_fd.type == FieldDescriptor.TYPE_MESSAGE:
                    pairs.append([k, proto_to_jspb(v)])
                else:
                    pairs.append([k, _scalar_to_jspb(v, val_fd)])
            populated[fnum] = pairs
        elif field_desc.label == FieldDescriptor.LABEL_REPEATED:
            if field_desc.type == FieldDescriptor.TYPE_MESSAGE:
                populated[fnum] = [proto_to_jspb(elem) for elem in value]
            else:
                populated[fnum] = [_scalar_to_jspb(elem, field_desc) for elem in value]
        elif field_desc.type == FieldDescriptor.TYPE_MESSAGE:
            populated[fnum] = proto_to_jspb(value)
        else:
            populated[fnum] = _scalar_to_jspb(value, field_desc)

    if not populated:
        return []
    max_num = max(populated)
    out: list[Any] = [None] * max_num
    for fnum, val in populated.items():
        out[fnum - 1] = val
    return out


def proto_value_to_python(val: struct_pb2.Value) -> Any:
    """Convert a ``google.protobuf.Value`` message to a native Python value."""
    kind = val.WhichOneof("kind")
    if kind == "number_value":
        num = val.number_value
        return int(num) if num.is_integer() else num
    if kind == "string_value":
        return val.string_value
    if kind == "bool_value":
        return val.bool_value
    if kind == "struct_value":
        return proto_struct_to_dict(val.struct_value)
    if kind == "list_value":
        return [proto_value_to_python(v) for v in val.list_value.values]
    return None


def proto_struct_to_dict(struct: struct_pb2.Struct) -> dict[str, Any]:
    """Convert a ``google.protobuf.Struct`` message to a plain Python dict."""
    return {k: proto_value_to_python(v) for k, v in struct.fields.items()}


# ── Cookie & Auth helpers ────────────────────────────────────────────────────


def parse_cookie_header(cookie_header: str) -> dict[str, str]:
    """Parse an HTTP Cookie header string into a name -> value dict."""
    out: dict[str, str] = {}
    attr_names = {
        "expires", "max-age", "domain", "path", "secure",
        "httponly", "samesite", "priority", "partitioned", "sameparty",
    }
    for raw_part in (cookie_header or "").split(";"):
        part = raw_part.strip()
        if not part or "=" not in part:
            continue
        k, v = part.split("=", 1)
        k = k.strip()
        v = v.strip()
        if k and k.lower() not in attr_names:
            out[k] = v
    return out


def format_cookie_header(cookie_map: dict[str, str], *, percent_encode: bool = False) -> str:
    """Format a name -> value cookie dict as an HTTP Cookie header."""
    parts: list[str] = []
    for name, value in cookie_map.items():
        if not name:
            continue
        if percent_encode:
            safe_name = quote(name, safe="!#$%&'*+-.^_`|~")
            safe_val = quote(value, safe="!#$%&'()*+-./:<=>?@[]^_`{|}~")
            parts.append(f"{safe_name}={safe_val}")
        else:
            parts.append(f"{name}={value}")
    return "; ".join(parts)


def build_sapisidhash_header(
    cookies: dict[str, str],
    origin: str = HOME_ORIGIN,
    timestamp: int | None = None,
) -> str:
    """Compute Google's SAPISIDHASH / SAPISID1PHASH / SAPISID3PHASH Authorization value."""
    ts = int(time.time()) if timestamp is None else int(timestamp)
    parts: list[str] = []
    for scheme, cookie_name in (
        ("SAPISIDHASH", "SAPISID"),
        ("SAPISID1PHASH", "__Secure-1PAPISID"),
        ("SAPISID3PHASH", "__Secure-3PAPISID"),
    ):
        val = cookies.get(cookie_name, "")
        if not val and scheme == "SAPISIDHASH":
            val = cookies.get("__Secure-3PAPISID", "")
        if val:
            digest = hashlib.sha1(f"{ts} {val} {origin}".encode("utf-8")).hexdigest()
            parts.append(f"{scheme} {ts}_{digest}")
    return " ".join(parts)


def _parse_hvac_stage_flags(
    stage_label: str,
    badges: list[tuple[str, int | None]],
    active_mode_str: str = "",
) -> dict[str, bool]:
    """Map Foyer EnergyService/GetThermostatStatus captions + badges to HvacControlTrait flags."""
    flags: dict[str, bool] = {
        "cool_stage1_active": False,
        "cool_stage2_active": False,
        "cool_stage3_active": False,
        "heat_stage1_active": False,
        "heat_stage2_active": False,
        "heat_stage3_active": False,
        "alt_heat_stage1_active": False,
        "alt_heat_stage2_active": False,
        "auxiliary_heat_active": False,
        "emergency_heat_active": False,
        "humidifier_active": False,
        "dehumidifier_active": False,
        "ventilator_active": False,
    }

    lbl = (stage_label or "").strip().lower()
    active_mode = (active_mode_str or "").strip().lower()
    badge_names = {b[0].strip().lower() for b in badges if b and b[0]}
    badge_codes = {b[1] for b in badges if b and b[1] is not None}

    if "dehumidif" in lbl or any("dehumid" in n for n in badge_names):
        flags["dehumidifier_active"] = True
    elif "humidif" in lbl or any("humid" in n for n in badge_names):
        flags["humidifier_active"] = True
    if "ventilat" in lbl or any("ventilat" in n for n in badge_names):
        flags["ventilator_active"] = True

    # When HomeControlService/GetTraits explicitly reports activeThermostatMode == "none"/"off",
    # heating and cooling are idle (e.g. stage_label is "Heat set to" or "Cool set to").
    if active_mode in ("none", "off", "idle"):
        return flags

    # Passive mode/setpoint headers (e.g. "Heat set to", "Cool set to", "Eco", "Off")
    # indicate the configured mode while idle, not an active HVAC call.
    is_passive_label = (
        not lbl
        or "set to" in lbl
        or lbl in ("off", "eco", "heat", "cool", "range", "heatcool", "heat • cool", "idle")
    )

    if not is_passive_label:
        if "alt" in lbl and "heat" in lbl:
            if "stage 2" in lbl or "2" in lbl:
                flags["alt_heat_stage2_active"] = True
            else:
                flags["alt_heat_stage1_active"] = True
        elif "emer" in lbl and "heat" in lbl:
            flags["emergency_heat_active"] = True
        elif "aux" in lbl and "heat" in lbl:
            flags["auxiliary_heat_active"] = True
        elif "heating" in lbl or ("heat" in lbl and "stage" in lbl):
            if "stage 3" in lbl or "3" in lbl:
                flags["heat_stage3_active"] = True
            elif "stage 2" in lbl or "2" in lbl:
                flags["heat_stage2_active"] = True
            else:
                flags["heat_stage1_active"] = True
        elif "cooling" in lbl or ("cool" in lbl and "stage" in lbl):
            if "stage 3" in lbl or "3" in lbl:
                flags["cool_stage3_active"] = True
            elif "stage 2" in lbl or "2" in lbl:
                flags["cool_stage2_active"] = True
            else:
                flags["cool_stage1_active"] = True

    # Fallback to badges / activeThermostatMode when stage_label wasn't an active stage string
    any_heat = any(
        flags[k]
        for k in (
            "heat_stage1_active", "heat_stage2_active", "heat_stage3_active",
            "alt_heat_stage1_active", "alt_heat_stage2_active",
            "auxiliary_heat_active", "emergency_heat_active",
        )
    )
    any_cool = any(
        flags[k] for k in ("cool_stage1_active", "cool_stage2_active", "cool_stage3_active")
    )
    if not any_heat:
        if any("alt" in n for n in badge_names) or 4 in badge_codes:
            flags["alt_heat_stage1_active"] = True
        elif any("emerg" in n for n in badge_names):
            flags["emergency_heat_active"] = True
        elif any("aux" in n for n in badge_names):
            flags["auxiliary_heat_active"] = True
        elif any("heating" in n for n in badge_names) or active_mode == "heat":
            flags["heat_stage1_active"] = True
    if not any_cool:
        if any("cooling" in n for n in badge_names) or active_mode == "cool":
            flags["cool_stage1_active"] = True

    return flags


class FoyerClient:
    """Authenticated client for Google Home Foyer (``home.google.com``)."""

    def __init__(
        self,
        foyer_cookie_header: str = "",
        accounts_cookie_header: str = "",
        api_key: str = DEFAULT_FOYER_API_KEY,
        authuser: str = "0",
        client_environment: str = "",
        user_agent: str = DEFAULT_USER_AGENT,
        rotate_cookies_body: str = "",
        rotate_cookies_referer: str = DEFAULT_ROTATE_REFERER,
        on_cookies_updated: Callable[[str, str, str, str], None] | None = None,
        wire_format: str = "jspb",
    ) -> None:
        self._foyer_cookies = parse_cookie_header(foyer_cookie_header)
        self._accounts_cookies = parse_cookie_header(accounts_cookie_header or foyer_cookie_header)
        if not self._foyer_cookies and self._accounts_cookies:
            self._foyer_cookies = dict(self._accounts_cookies)

        self._api_key = (api_key or DEFAULT_FOYER_API_KEY).strip()
        self._authuser = str(authuser or "0").strip()
        self._client_environment = str(client_environment or "").strip()
        self._user_agent = (user_agent or DEFAULT_USER_AGENT).strip()
        self._rotate_body = str(rotate_cookies_body or "").strip()
        self._rotate_referer = (rotate_cookies_referer or DEFAULT_ROTATE_REFERER).strip()
        self._on_cookies_updated = on_cookies_updated
        self._wire_format = (wire_format or "jspb").strip().lower()

        self._session = requests.Session()
        self._cookie_lock = threading.RLock()
        self._homegraph_cache: dict[str, Any] | None = None
        self._homegraph_fetched_ts: float = 0.0
        self._homegraph_ttl_sec: float = 1800.0
        self._last_rotate_ts: float = 0.0

    @property
    def foyer_cookie_header(self) -> str:
        return format_cookie_header(self._foyer_cookies)

    @property
    def accounts_cookie_header(self) -> str:
        return format_cookie_header(self._accounts_cookies)

    @property
    def rotate_cookies_body(self) -> str:
        return self._rotate_body

    @property
    def rotate_cookies_referer(self) -> str:
        return self._rotate_referer

    def has_foyer_credentials(self) -> bool:
        has_sid = bool(
            self._foyer_cookies.get("SAPISID")
            or self._foyer_cookies.get("__Secure-1PAPISID")
            or self._foyer_cookies.get("__Secure-3PAPISID")
        )
        return has_sid and bool(self._foyer_cookies.get("SID") or self._foyer_cookies.get("__Secure-1PSID"))

    def update_cookies(self, *, foyer_cookie_header: str = "", accounts_cookie_header: str = "") -> None:
        with self._cookie_lock:
            if foyer_cookie_header:
                self._foyer_cookies = parse_cookie_header(foyer_cookie_header)
            if accounts_cookie_header:
                self._accounts_cookies = parse_cookie_header(accounts_cookie_header)

    def _apply_rotating_response_cookies(self, resp: requests.Response) -> list[str]:
        with self._cookie_lock:
            updated: list[str] = []
            for cookie in resp.cookies:
                if cookie.name in FOYER_ROTATING_COOKIES and cookie.value:
                    self._foyer_cookies[cookie.name] = cookie.value
                    self._accounts_cookies[cookie.name] = cookie.value
                    updated.append(cookie.name)
            if updated and self._on_cookies_updated is not None:
                try:
                    self._on_cookies_updated(
                        self.foyer_cookie_header,
                        self.accounts_cookie_header,
                        self._rotate_body,
                        self._rotate_referer,
                    )
                except Exception as exc:
                    log.debug("on_cookies_updated callback error: %s", exc)
            return sorted(set(updated))

    def _refresh_rotate_nonce(self) -> bool:
        """Fetch a fresh RotateCookies nonce from accounts.google.com/RotateCookiesPage."""
        if not self._accounts_cookies:
            return False
        resp = self._session.get(
            self._rotate_referer,
            headers={
                "User-Agent": self._user_agent,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": f"{HOME_ORIGIN}/",
                "Cookie": format_cookie_header(self._accounts_cookies, percent_encode=True),
            },
            timeout=20,
        )
        if not resp.ok:
            log.warning("RotateCookiesPage returned HTTP %d", resp.status_code)
            return False
        self._apply_rotating_response_cookies(resp)
        m = re.search(r"init\(\s*'([^']+)'\s*,\s*([0-9\.]+)", resp.text)
        if not m:
            log.warning("RotateCookiesPage response missing init() nonce")
            return False
        nonce = m.group(1)
        pid = int(float(m.group(2)))
        self._rotate_body = json.dumps([pid, nonce], separators=(",", ":"))
        return True

    def rotate_cookies(self, *, force_refresh_page: bool = False) -> bool:
        """Call accounts.google.com/RotateCookies to refresh __Secure-*PSIDTS and *SIDCC."""
        with self._cookie_lock:
            if not self._accounts_cookies:
                return False

            if force_refresh_page or not self._rotate_body:
                if not self._refresh_rotate_nonce():
                    return False

            for attempt in range(2):
                resp = self._session.post(
                    "https://accounts.google.com/RotateCookies",
                    data=self._rotate_body,
                    headers={
                        "User-Agent": self._user_agent,
                        "Accept": "*/*",
                        "Accept-Language": "en-US,en;q=0.9",
                        "Content-Type": "application/json",
                        "Origin": "https://accounts.google.com",
                        "Referer": self._rotate_referer,
                        "Cookie": format_cookie_header(self._accounts_cookies, percent_encode=True),
                    },
                    timeout=20,
                )
                if resp.ok:
                    updated = self._apply_rotating_response_cookies(resp)
                    self._last_rotate_ts = time.time()
                    log.debug("RotateCookies succeeded; updated %d cookie(s): %s", len(updated), ", ".join(updated))
                    return True

                if resp.status_code == 429:
                    # Google enforces a ~600s minimum interval between rotations (identity.hfcr).
                    # HTTP 429 indicates the current session cookies were rotated recently and are fresh.
                    self._last_rotate_ts = time.time()
                    log.debug("RotateCookies rate-limited (HTTP 429); session cookies already fresh")
                    return True

                if attempt == 0 and resp.status_code in (400, 401, 403):
                    log.debug("RotateCookies HTTP %d; fetching fresh RotateCookiesPage nonce", resp.status_code)
                    if self._refresh_rotate_nonce():
                        continue
                log.warning("RotateCookies failed: HTTP %d — %s", resp.status_code, resp.text[:200])
                return False
            return False

    def _post_rpc_raw(
        self,
        method_path: str,
        body: bytes | str,
        *,
        content_type: str,
        base_url: str = FOYER_BASE_URL,
        timeout: float = 20.0,
        retry_on_401: bool = True,
    ) -> requests.Response:
        if not self.has_foyer_credentials():
            raise FoyerAuthError("Missing Foyer SAPISID/SID cookies")

        url = f"{base_url.rstrip('/')}/$rpc/{method_path.lstrip('/')}"
        for attempt in range(2):
            auth_header = build_sapisidhash_header(self._foyer_cookies, HOME_ORIGIN)
            if not auth_header:
                raise FoyerAuthError("Could not build SAPISIDHASH Authorization header")

            headers = {
                "User-Agent": self._user_agent,
                "Accept": "*/*",
                "Accept-Language": "en-US,en;q=0.9",
                "Content-Type": content_type,
                "X-User-Agent": "grpc-web-javascript/0.1",
                "X-Goog-Api-Key": self._api_key,
                "X-Goog-AuthUser": self._authuser,
                "Authorization": auth_header,
                "Origin": HOME_ORIGIN,
                "Referer": f"{HOME_ORIGIN}/",
                "Cookie": format_cookie_header(self._foyer_cookies, percent_encode=True),
            }
            if self._client_environment:
                headers["x-foyer-client-environment"] = self._client_environment

            resp = self._session.post(url, data=body, headers=headers, timeout=timeout)

            if resp.status_code in (401, 403):
                if retry_on_401 and attempt == 0 and self.rotate_cookies(force_refresh_page=True):
                    log.info("Foyer RPC %s got HTTP %d; retried after RotateCookies", method_path, resp.status_code)
                    continue
                raise FoyerAuthError(
                    f"Foyer RPC {method_path} auth failed: HTTP {resp.status_code} — {resp.text[:300]}"
                )

            if not resp.ok:
                raise FoyerError(
                    f"Foyer RPC {method_path} failed: HTTP {resp.status_code} — {resp.text[:300]}"
                )

            self._apply_rotating_response_cookies(resp)
            return resp

        raise FoyerAuthError(f"Foyer RPC {method_path} failed after retry")

    def rpc(
        self,
        method_path: str,
        payload: Any,
        *,
        base_url: str = FOYER_BASE_URL,
        timeout: float = 20.0,
        retry_on_401: bool = True,
    ) -> Any:
        """Execute a Foyer JSON+protobuf RPC and return the raw decoded JSON payload."""
        if isinstance(payload, Message):
            payload = proto_to_jspb(payload)
        resp = self._post_rpc_raw(
            method_path,
            json.dumps(payload),
            content_type="application/json+protobuf",
            base_url=base_url,
            timeout=timeout,
            retry_on_401=retry_on_401,
        )
        try:
            return resp.json()
        except Exception as exc:
            raise FoyerError(f"Foyer RPC {method_path} returned non-JSON: {exc}") from exc

    def rpc_proto(
        self,
        method_path: str,
        request: Message | list[Any],
        response_cls: type[TMessage],
        *,
        wire_format: str | None = None,
        base_url: str = FOYER_BASE_URL,
        timeout: float = 20.0,
        retry_on_401: bool = True,
    ) -> TMessage:
        """Execute a Foyer RPC and decode the response into a compiled ``*_pb2`` Message.

        Supports both ``jspb`` (``application/json+protobuf``, default on web) and
        ``binary`` (``application/x-protobuf``). Automatically decodes binary responses
        if the server replies with ``application/x-protobuf``.
        """
        fmt = (wire_format or self._wire_format or "jspb").strip().lower()
        if fmt == "binary" and isinstance(request, Message):
            body: bytes | str = request.SerializeToString()
            content_type = "application/x-protobuf"
        else:
            jspb_req = proto_to_jspb(request) if isinstance(request, Message) else request
            body = json.dumps(jspb_req)
            content_type = "application/json+protobuf"

        resp = self._post_rpc_raw(
            method_path,
            body,
            content_type=content_type,
            base_url=base_url,
            timeout=timeout,
            retry_on_401=retry_on_401,
        )
        resp_ct = (resp.headers.get("Content-Type") or "").lower()
        if "application/x-protobuf" in resp_ct and "json" not in resp_ct:
            return jspb_to_proto(resp.content, response_cls)
        try:
            data = resp.json()
        except Exception as exc:
            raise FoyerError(f"Foyer RPC {method_path} returned non-JSON: {exc}") from exc
        return jspb_to_proto(data, response_cls)

    # ── High-level Foyer RPCs ────────────────────────────────────────────────

    def get_nest_account_link_state(self) -> dict[str, str]:
        resp = self.rpc_proto(
            "google.internal.home.foyer.v1.UsersService/GetNestAccountLinkState",
            users_pb2.GetNestAccountLinkStateRequest(),
            users_pb2.GetNestAccountLinkStateResponse,
        )
        return self.parse_nest_account_link_state(resp)

    @staticmethod
    def parse_nest_account_link_state(
        raw: users_pb2.GetNestAccountLinkStateResponse | list[Any] | bytes,
    ) -> dict[str, str]:
        msg = jspb_to_proto(raw, users_pb2.GetNestAccountLinkStateResponse)
        out: dict[str, str] = {}
        if msg.HasField("nest_ids"):
            if msg.nest_ids.legacy_user_key:
                out["user"] = msg.nest_ids.legacy_user_key
                if "." in msg.nest_ids.legacy_user_key:
                    out["userid"] = msg.nest_ids.legacy_user_key.split(".", 1)[-1]
            if msg.nest_ids.phoenix_user_id:
                out["nest_user_id"] = msg.nest_ids.phoenix_user_id
        return out

    def get_olive_token(self) -> tuple[str, float]:
        """Request an Olive JWT from ClientOliveTokenService/GetOliveToken.

        Returns ``(token, expires_unix)``. Note that web-issued Olive tokens
        carry ``home-camera-client-policy`` (camera API scope).
        """
        resp = self.rpc_proto(
            "google.internal.home.foyer.v1.ClientOliveTokenService/GetOliveToken",
            users_pb2.GetOliveTokenRequest(),
            users_pb2.GetOliveTokenResponse,
        )
        return self.parse_olive_token(resp)

    @staticmethod
    def parse_olive_token(
        raw: users_pb2.GetOliveTokenResponse | list[Any] | bytes,
        *,
        now: float | None = None,
    ) -> tuple[str, float]:
        msg = jspb_to_proto(raw, users_pb2.GetOliveTokenResponse)
        if not msg.HasField("olive_token") or not msg.olive_token.token:
            return "", 0.0
        now_ts = time.time() if now is None else float(now)
        ttl_sec = 0.0
        if msg.olive_token.HasField("ttl"):
            ttl_sec = float(msg.olive_token.ttl.seconds) + (float(msg.olive_token.ttl.nanos) / 1e9)
        expires_unix = (now_ts + ttl_sec) if ttl_sec > 0 else (now_ts + 300.0)
        return msg.olive_token.token, expires_unix

    def get_home_graph(self, *, force_refresh: bool = False) -> dict[str, Any]:
        """Fetch and parse structures, rooms, thermostats, and Nest temperature sensors."""
        now = time.time()
        if (
            not force_refresh
            and self._homegraph_cache is not None
            and (now - self._homegraph_fetched_ts) < self._homegraph_ttl_sec
        ):
            return self._homegraph_cache

        resp = self.rpc_proto(
            "google.internal.home.foyer.v1.StructuresService/GetHomeGraph",
            _HOMEGRAPH_REQUEST_BODY,
            structures_pb2.GetHomeGraphResponse,
        )
        parsed = self._parse_home_graph(resp)
        self._homegraph_cache = parsed
        self._homegraph_fetched_ts = now
        return parsed

    @staticmethod
    def _parse_home_graph(
        raw: structures_pb2.GetHomeGraphResponse | list[Any] | bytes,
    ) -> dict[str, Any]:
        msg = jspb_to_proto(raw, structures_pb2.GetHomeGraphResponse)
        structures: list[dict[str, Any]] = []
        devices: dict[str, dict[str, Any]] = {}
        rooms: dict[str, str] = {}

        for struct in msg.structures:
            struct_id = struct.id
            structures.append({"structure_id": struct_id, "name": struct.name})

            device_room_map: dict[str, tuple[str, str]] = {}
            for space in struct.spaces:
                if space.id and space.name:
                    rooms[space.id] = space.name
                for ref in space.devices:
                    if ref.HasField("device_id") and ref.device_id.uuid:
                        device_room_map[ref.device_id.uuid] = (space.id, space.name)

            for dev in struct.devices:
                dev_uuid = dev.id.uuid if dev.HasField("id") else ""
                if not dev_uuid:
                    continue
                agent_id = dev.id.agent_info.agent_id if dev.id.HasField("agent_info") else ""
                external_id = dev.id.agent_info.device_id if dev.id.HasField("agent_info") else ""

                phoenix_id = external_id if external_id.startswith("DEVICE_") else ""
                if not phoenix_id and dev.HasField("external_ids"):
                    for ext in dev.external_ids.ids:
                        if ext.agent_id == "PHX" and ext.device_id:
                            phoenix_id = ext.device_id
                            break

                room_id, room_name = device_room_map.get(dev_uuid, ("", dev.name))
                devices[dev_uuid] = {
                    "uuid": dev_uuid,
                    "structure_id": struct_id,
                    "name": dev.name or room_name,
                    "room_id": room_id,
                    "room_name": room_name or dev.name,
                    "device_type": dev.device_type,
                    "agent_id": agent_id,
                    "external_id": external_id,
                    "phoenix_id": phoenix_id,
                    "traits": list(dev.traits),
                }

        return {"structures": structures, "devices": devices, "rooms": rooms}

    def get_structure_mode(self, structure_id: str) -> str:
        """Query HomeOccupancyService/GetStructureMode -> 'STRUCTURE_MODE_HOME' or 'STRUCTURE_MODE_AWAY'."""
        resp = self.rpc_proto(
            "google.internal.home.foyer.v1.HomeOccupancyService/GetStructureMode",
            home_occupancy_pb2.GetStructureModeRequest(structure_id=structure_id),
            home_occupancy_pb2.GetStructureModeResponse,
        )
        return self.parse_structure_mode(resp)

    @staticmethod
    def parse_structure_mode(
        raw: home_occupancy_pb2.GetStructureModeResponse | list[Any] | bytes,
    ) -> str:
        msg = jspb_to_proto(raw, home_occupancy_pb2.GetStructureModeResponse)
        log.debug(
            "Foyer GetStructureMode: presence_state=%d mode=%s (%d)",
            msg.presence_state,
            home_occupancy_pb2.FoyerStructureMode.Name(msg.mode)
            if msg.mode in home_occupancy_pb2.FoyerStructureMode.values()
            else str(msg.mode),
            msg.mode,
        )
        if msg.mode == home_occupancy_pb2.FOYER_STRUCTURE_MODE_HOME:
            return "STRUCTURE_MODE_HOME"
        if msg.mode in (
            home_occupancy_pb2.FOYER_STRUCTURE_MODE_AWAY,
            home_occupancy_pb2.FOYER_STRUCTURE_MODE_VACATION,
        ):
            return "STRUCTURE_MODE_AWAY"
        return "STRUCTURE_MODE_HOME"

    def get_thermostat_status(self, device_uuid: str) -> dict[str, Any]:
        """Call EnergyService/GetThermostatStatus and decode into structured fields."""
        resp = self.rpc_proto(
            "google.internal.home.foyer.v1.EnergyService/GetThermostatStatus",
            energy_pb2.GetThermostatStatusRequest(device_id=device_uuid),
            energy_pb2.GetThermostatStatusResponse,
        )
        return self.parse_thermostat_status(resp)

    @staticmethod
    def parse_thermostat_status(
        raw: energy_pb2.GetThermostatStatusResponse | list[Any] | bytes,
    ) -> dict[str, Any]:
        """Parse EnergyService/GetThermostatStatus into normalized thermostat telemetry."""
        msg = jspb_to_proto(raw, energy_pb2.GetThermostatStatusResponse)
        out: dict[str, Any] = {}
        if not msg.HasField("thermostat_status"):
            return out

        ts = msg.thermostat_status
        heat_target_c = float(ts.heating_target.value) if ts.HasField("heating_target") else None
        current_temp_c = float(ts.current_temperature.value) if ts.HasField("current_temperature") else None
        cool_target_c = float(ts.cooling_target.value) if ts.HasField("cooling_target") else None
        humidity = float(ts.current_humidity.value) if ts.HasField("current_humidity") else None
        backplate_temp_c = float(ts.backplate_temperature.value) if ts.HasField("backplate_temperature") else None

        setpoint_type = _THERMOSTAT_MODE_TO_SETPOINT_TYPE.get(ts.mode, "heat")

        stage_label = ""
        mode_icon = 0
        badges: list[tuple[str, int | None]] = []
        if ts.HasField("status_display"):
            sd = ts.status_display
            stage_label = sd.stage_label or ""
            mode_icon = sd.mode_icon
            for b in sd.badges:
                badges.append((b.title, b.badge_type if b.badge_type else None))

        eco_heat_c: float | None = None
        eco_cool_c: float | None = None
        if ts.HasField("eco_temperature_range"):
            if ts.eco_temperature_range.HasField("low") and ts.eco_temperature_range.low.value > -900:
                eco_heat_c = float(ts.eco_temperature_range.low.value)
            if ts.eco_temperature_range.HasField("high") and ts.eco_temperature_range.high.value > -900:
                eco_cool_c = float(ts.eco_temperature_range.high.value)

        log.debug(
            "Foyer GetThermostatStatus: device=%s mode=%d status_code=%d unit=%d "
            "stage_label=%r mode_icon=%d badges=%r",
            ts.device_id,
            ts.mode,
            ts.thermostat_status_code,
            ts.preferred_temperature_unit,
            stage_label,
            mode_icon,
            badges,
        )

        hvac_flags = _parse_hvac_stage_flags(stage_label, badges)

        out.update({
            "current_temp_c": round(current_temp_c, 3) if current_temp_c is not None else None,
            "backplate_temp_c": round(backplate_temp_c, 3) if backplate_temp_c is not None else None,
            "humidity_pct": round(humidity, 1) if humidity is not None else None,
            "heating_target_c": round(heat_target_c, 2) if heat_target_c is not None else None,
            "cooling_target_c": round(cool_target_c, 2) if cool_target_c is not None else None,
            "setpoint_type": setpoint_type,
            "preferred_temperature_unit": (
                energy_pb2.PreferredTemperatureUnit.Name(ts.preferred_temperature_unit)
                if ts.preferred_temperature_unit in energy_pb2.PreferredTemperatureUnit.values()
                else str(ts.preferred_temperature_unit)
            ),
            "eco_heat_c": round(eco_heat_c, 2) if eco_heat_c is not None else None,
            "eco_cool_c": round(eco_cool_c, 2) if eco_cool_c is not None else None,
            "stage_label": stage_label,
            "mode_icon": mode_icon,
            "badges": badges,
            "hvac_control": hvac_flags,
        })
        return out

    def get_wiring_configuration(self, device_uuid: str) -> list[dict[str, Any]]:
        """Call EnergyService/GetWiringConfiguration and return connected terminal metadata."""
        resp = self.rpc_proto(
            "google.internal.home.foyer.v1.EnergyService/GetWiringConfiguration",
            energy_pb2.GetWiringConfigurationRequest(device_id=device_uuid),
            energy_pb2.GetWiringConfigurationResponse,
        )
        return self.parse_wiring_configuration(resp)

    @staticmethod
    def parse_wiring_configuration(
        raw: energy_pb2.GetWiringConfigurationResponse | list[Any] | bytes,
    ) -> list[dict[str, Any]]:
        msg = jspb_to_proto(raw, energy_pb2.GetWiringConfigurationResponse)
        return [
            {
                "label": t.label,
                "function_name": t.function_name,
                "connected": t.connected,
                "terminal_type": t.terminal_type,
                "wire_function": t.wire_function,
            }
            for t in msg.terminals
        ]

    def get_traits(self, device_uuids: list[str]) -> dict[str, dict[str, dict[str, Any]]]:
        """Call HomeControlService/GetTraits for a list of device UUIDs."""
        if not device_uuids:
            return {}
        req = home_control_pb2.GetTraitsRequest(
            device_ids=[common_pb2.DeviceId(uuid=u) for u in device_uuids]
        )
        resp = self.rpc_proto(
            "google.internal.home.foyer.v1.HomeControlService/GetTraits",
            req,
            home_control_pb2.GetTraitsResponse,
        )
        return self.parse_get_traits(resp)

    @staticmethod
    def parse_get_traits(
        raw: home_control_pb2.GetTraitsResponse | list[Any] | bytes,
    ) -> dict[str, dict[str, dict[str, Any]]]:
        """Parse HomeControlService/GetTraits response into {uuid: {trait_name: params_dict}}."""
        msg = jspb_to_proto(raw, home_control_pb2.GetTraitsResponse)
        out: dict[str, dict[str, dict[str, Any]]] = {}
        for dev_entry in msg.device_traits:
            dev_uuid = dev_entry.device_id.uuid if dev_entry.HasField("device_id") else ""
            if not dev_uuid:
                continue
            trait_dict: dict[str, dict[str, Any]] = {}
            for trait_state in dev_entry.traits:
                if not trait_state.name:
                    continue
                params = {
                    p.key: proto_value_to_python(p.value)
                    for p in trait_state.parameters
                    if p.key
                }
                trait_dict[trait_state.name] = params
            out[dev_uuid] = trait_dict
        return out

    def poll_observe_compatible_snapshot(
        self,
        device_id_overrides: dict[str, str] | None = None,
    ) -> dict[str, dict[str, Any]]:
        """Poll Foyer and return a snapshot in the exact format of ``parse_observe_response()``.

        ``device_id_overrides`` may map either a Foyer UUID or a lowercase room/device name
        to a canonical ``DEVICE_...`` ID (e.g. ``DEVICE_0123456789ABCDEF``) so InfluxDB
        ``dev_idx`` assignments remain continuous with the Observe stream.
        """
        overrides = device_id_overrides or {}
        graph = self.get_home_graph()
        devices = graph.get("devices", {})
        structures = graph.get("structures", [])
        now_iso = datetime.now(timezone.utc).isoformat(timespec="milliseconds")

        # Determine structure away/home state
        structure_modes: dict[str, str] = {}
        for st in structures:
            sid = st.get("structure_id", "")
            if not sid:
                continue
            try:
                structure_modes[sid] = self.get_structure_mode(sid)
            except Exception as exc:
                log.debug("Foyer GetStructureMode failed for %s: %s", sid, exc)
                structure_modes[sid] = "STRUCTURE_MODE_HOME"

        nest_uuids = [
            u for u, d in devices.items()
            if d.get("device_type") in ("action.devices.types.THERMOSTAT", "action.devices.types.SENSOR")
            and "nest" in (d.get("agent_id") or "").lower()
        ]
        traits_by_uuid = self.get_traits(nest_uuids) if nest_uuids else {}

        snapshot: dict[str, dict[str, Any]] = {}

        # Emit LocatedAnnotationsTrait so EventThermostatCollector resolves room names
        annotations: list[dict[str, Any]] = []
        for room_id, room_name in graph.get("rooms", {}).items():
            annotations.append({
                "info": {
                    "id": {"value": room_id},
                    "name": {"value": room_name},
                }
            })
        if structures and annotations:
            first_sid = structures[0]["structure_id"]
            snapshot[f"STRUCTURE_FOYER_{first_sid}"] = {
                "resource_type": "nest.resource.StructureResource",
                "traits": {
                    "located_annotations": {
                        "trait_type": "type.nestlabs.com/nest.trait.located.LocatedAnnotationsTrait",
                        "fields": {"annotations": annotations},
                        "state_meta": {"decoded_at": now_iso},
                    },
                    "structure_mode": {
                        "trait_type": "type.nestlabs.com/nest.trait.occupancy.StructureModeTrait",
                        "fields": {"structureMode": structure_modes.get(first_sid, "STRUCTURE_MODE_HOME")},
                        "state_meta": {"decoded_at": now_iso},
                    },
                },
            }

        for dev_uuid in nest_uuids:
            meta = devices[dev_uuid]
            dev_type = meta["device_type"]
            room_name = meta.get("room_name") or meta.get("name") or ""
            room_id = meta.get("room_id") or ""
            struct_id = meta.get("structure_id") or ""

            # Resolve canonical DEVICE_... ID
            canonical_id = (
                overrides.get(dev_uuid)
                or (meta.get("phoenix_id") if str(meta.get("phoenix_id", "")).startswith("DEVICE_") else "")
                or overrides.get(room_name.strip().lower())
                or overrides.get(dev_type)
                or f"DEVICE_FOYER_{dev_uuid.replace('-', '').upper()[:16]}"
            )

            foyer_traits = traits_by_uuid.get(dev_uuid, {})
            ts_params = foyer_traits.get("temperatureSetting", {})

            if dev_type == "action.devices.types.SENSOR":
                temp_c = ts_params.get("ambientAirTemperatureC")
                dev_traits: dict[str, Any] = {}
                if temp_c is not None:
                    dev_traits["current_temperature"] = {
                        "trait_type": "type.nestlabs.com/nest.trait.sensor.TemperatureTrait",
                        "fields": {"value_c": round(float(temp_c), 3)},
                        "state_meta": {"decoded_at": now_iso},
                    }
                if room_id:
                    dev_traits["device_located_settings"] = {
                        "trait_type": "type.nestlabs.com/nest.trait.located.DeviceLocatedSettingsTrait",
                        "fields": {"where_id": {"value": room_id}},
                        "state_meta": {"decoded_at": now_iso},
                    }
                snapshot[canonical_id] = {
                    "resource_type": "nest.resource.NestKryptoniteResource",
                    "traits": dev_traits,
                }
                continue

            # Thermostat: combine EnergyService/GetThermostatStatus + HomeControlService/GetTraits
            status = {}
            try:
                status = self.get_thermostat_status(dev_uuid)
            except Exception as exc:
                log.warning("Foyer GetThermostatStatus failed for %s: %s", dev_uuid, exc)

            current_temp_c = status.get("current_temp_c")
            if current_temp_c is None and ts_params.get("ambientAirTemperatureC") is not None:
                current_temp_c = round(float(ts_params["ambientAirTemperatureC"]), 3)

            backplate_temp_c = status.get("backplate_temp_c")
            if backplate_temp_c is None and ts_params.get("backplateAmbientAirTemperatureC") is not None:
                backplate_temp_c = round(float(ts_params["backplateAmbientAirTemperatureC"]), 3)

            humidity_pct = status.get("humidity_pct")
            if humidity_pct is None and ts_params.get("ambientAirHumidity") is not None:
                hum_raw = float(ts_params["ambientAirHumidity"])
                humidity_pct = round(hum_raw * 100.0 if hum_raw <= 1.0 else hum_raw, 1)

            hvac_fields = _parse_hvac_stage_flags(
                str(status.get("stage_label") or ""),
                list(status.get("badges") or []),
                active_mode_str=str(ts_params.get("activeThermostatMode") or ""),
            )

            # Fan speed from HomeControlService/GetTraits fanSpeed
            fan_params = foyer_traits.get("fanSpeed", {})
            raw_fan = str(fan_params.get("currentFanSpeedSetting") or "OFF").upper()
            fan_map = {"OFF": "off", "S1": "stage1", "S2": "stage2", "S3": "stage3", "AUTO": "auto"}
            fan_speed = fan_map.get(raw_fan, "off" if raw_fan in ("", "OFF", "NONE") else "stage1")

            # Setpoints
            setpoint_type = str(status.get("setpoint_type") or ts_params.get("mode") or "heat").lower()
            if setpoint_type == "heatcool":
                setpoint_type = "range"
            heat_target = status.get("heating_target_c")
            cool_target = status.get("cooling_target_c")
            if heat_target is None and ts_params.get("thermostatTemperatureSetpointC") is not None:
                if setpoint_type == "cool":
                    cool_target = round(float(ts_params["thermostatTemperatureSetpointC"]), 2)
                else:
                    heat_target = round(float(ts_params["thermostatTemperatureSetpointC"]), 2)
            if heat_target is None and ts_params.get("thermostatTemperatureSetpointLowC") is not None:
                heat_target = round(float(ts_params["thermostatTemperatureSetpointLowC"]), 2)
            if cool_target is None and ts_params.get("thermostatTemperatureSetpointHighC") is not None:
                cool_target = round(float(ts_params["thermostatTemperatureSetpointHighC"]), 2)

            tts_fields: dict[str, Any] = {"setpoint_type": setpoint_type}
            if heat_target is not None:
                tts_fields["heating_target_c"] = heat_target
            if cool_target is not None:
                tts_fields["cooling_target_c"] = cool_target

            dev_traits = {
                "hvac_control": {
                    "trait_type": "type.nestlabs.com/nest.trait.hvac.HvacControlTrait",
                    "fields": hvac_fields,
                    "state_meta": {"decoded_at": now_iso},
                },
                "fan_control": {
                    "trait_type": "type.nestlabs.com/nest.trait.hvac.FanControlTrait",
                    "fields": {"current_speed": fan_speed},
                    "state_meta": {"decoded_at": now_iso},
                },
                "target_temperature_settings": {
                    "trait_type": "type.nestlabs.com/nest.trait.hvac.TargetTemperatureSettingsTrait",
                    "fields": tts_fields,
                    "state_meta": {"decoded_at": now_iso},
                },
                "structure_mode": {
                    "trait_type": "type.nestlabs.com/nest.trait.occupancy.StructureModeTrait",
                    "fields": {"structureMode": structure_modes.get(struct_id, "STRUCTURE_MODE_HOME")},
                    "state_meta": {"decoded_at": now_iso},
                },
            }
            if current_temp_c is not None:
                dev_traits["current_temperature"] = {
                    "trait_type": "type.nestlabs.com/nest.trait.sensor.TemperatureTrait",
                    "fields": {"value_c": current_temp_c},
                    "state_meta": {"decoded_at": now_iso},
                }
            if backplate_temp_c is not None:
                dev_traits["backplate_temperature"] = {
                    "trait_type": "type.nestlabs.com/nest.trait.sensor.TemperatureTrait",
                    "fields": {"value_c": backplate_temp_c},
                    "state_meta": {"decoded_at": now_iso},
                }
            if humidity_pct is not None:
                dev_traits["current_humidity"] = {
                    "trait_type": "type.nestlabs.com/nest.trait.sensor.HumidityTrait",
                    "fields": {"value_pct": humidity_pct},
                    "state_meta": {"decoded_at": now_iso},
                }
            if room_id:
                dev_traits["device_located_settings"] = {
                    "trait_type": "type.nestlabs.com/nest.trait.located.DeviceLocatedSettingsTrait",
                    "fields": {"where_id": {"value": room_id}},
                    "state_meta": {"decoded_at": now_iso},
                }

            snapshot[canonical_id] = {
                "resource_type": "nest.resource.NestLearningThermostat3Resource",
                "traits": dev_traits,
            }

        return snapshot
