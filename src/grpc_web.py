"""
Minimal gRPC-Web client for Nest's internal gRPC-Web gateway.

Wire format (application/x-protobuf.google.rpc.streambody):
  Streaming responses are wrapped in StreamBody { repeated bytes message = 1 }
  where each bytes element is a serialized ObserveResponse.

Proto specs from https://github.com/Humpheh/nest-observe (submodule).
"""

import os
import sys
import importlib
import struct
import requests
import logging
from datetime import datetime, timezone
from typing import Tuple, List, Union, Dict, Optional, Generator, Any

try:
    from google.protobuf.json_format import MessageToDict
    from google.protobuf.symbol_database import Default as _ProtoSymbolDbDefault
    from google.protobuf.descriptor import FieldDescriptor as _ProtoFieldDescriptor
    _PROTO_SYMBOL_DB = _ProtoSymbolDbDefault()
except Exception:
    MessageToDict = None
    _PROTO_SYMBOL_DB = None
    _ProtoFieldDescriptor = None

log = logging.getLogger(__name__)

# Shared module state for tracking active stream connectivity (heartbeat)
last_stream_activity_ts: float = 0.0


class ObserveAuthError(RuntimeError):
    """Observe stream rejected the current auth token."""

_ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_PROTO_GENERATED_DIR = os.path.abspath(os.path.join(_ROOT_DIR, "proto", "generated"))
if _PROTO_GENERATED_DIR not in sys.path:
    sys.path.insert(0, _PROTO_GENERATED_DIR)

try:
    from nest import gateway_pb2 as nest_gateway_pb2
    import nest_hvac_extra_pb2
    from nest import messages_pb2
    from nest.trait import (
        detector_pb2,
        hvac_pb2,
        located_pb2,
        occupancy_pb2,
        security_pb2,
        sensor_pb2,
        structure_pb2,
        user_pb2,
    )
except ImportError as e:
    log.warning("Could not import generated protobuf modules: %s", e)
    nest_gateway_pb2 = None
    nest_hvac_extra_pb2 = None
    messages_pb2 = None
    detector_pb2 = None
    hvac_pb2 = None
    located_pb2 = None
    occupancy_pb2 = None
    security_pb2 = None
    sensor_pb2 = None
    structure_pb2 = None
    user_pb2 = None


def _import_all_generated_pb2_modules() -> None:
    """Import all generated *_pb2 modules so descriptors are registered."""
    if not os.path.isdir(_PROTO_GENERATED_DIR):
        return

    imported = 0
    for root, _, files in os.walk(_PROTO_GENERATED_DIR):
        for name in files:
            if not name.endswith("_pb2.py"):
                continue
            full_path = os.path.join(root, name)
            rel = os.path.relpath(full_path, _PROTO_GENERATED_DIR)
            mod_name = rel[:-3].replace(os.sep, ".")
            if mod_name == "__init__":
                continue
            try:
                importlib.import_module(mod_name)
                imported += 1
            except Exception:
                continue

    log.debug("Imported %d generated protobuf modules", imported)


_import_all_generated_pb2_modules()

_GRPC_WEB_BASE = "https://grpc-web.production.nest.com"
_WEBAPP_VERSION = "NlAppSDKVersion/9.266.0 NlSchemaVersion/2.1.45-844-g8f5584498"

# ── Minimal protobuf helpers ────────────────────────────────────────────────

def _encode_varint(value: int) -> bytes:
    """Encode a non-negative integer as a protobuf varint."""
    out = []
    while True:
        bits = value & 0x7F
        value >>= 7
        if value:
            out.append(bits | 0x80)
        else:
            out.append(bits)
            break
    return bytes(out)


def _decode_varint(data: bytes, pos: int) -> Tuple[int, int]:
    """Decode a varint from *data* starting at *pos*. Returns (value, new_pos)."""
    result = 0
    shift = 0
    while pos < len(data):
        b = data[pos]
        pos += 1
        result |= (b & 0x7F) << shift
        shift += 7
        if not (b & 0x80):
            break
    return result, pos


def _encode_timestamp(seconds: int, nanos: int = 0) -> bytes:
    """Encode a google.protobuf.Timestamp message."""
    msg = _encode_varint_field(1, seconds)
    if nanos:
        msg += _encode_varint_field(2, nanos)
    return msg


def _encode_duration(seconds: int, nanos: int = 0) -> bytes:
    """Encode a google.protobuf.Duration message."""
    msg = _encode_varint_field(1, seconds)
    if nanos:
        msg += _encode_varint_field(2, nanos)
    return msg


def encode_string_field(field_num: int, value: str) -> bytes:
    """Encode a single protobuf string field (wire type 2)."""
    tag = (field_num << 3) | 2
    encoded = value.encode("utf-8")
    return _encode_varint(tag) + _encode_varint(len(encoded)) + encoded


def encode_repeated_strings(field_num: int, values: List[str]) -> bytes:
    """Encode a repeated string protobuf field."""
    return b"".join(encode_string_field(field_num, v) for v in values)


def decode_proto_fields(
    data: bytes,
    warn_unknown_wire_type: bool = True,
) -> List[Tuple[int, int, Union[bytes, int]]]:
    """
    Decode a protobuf message into a list of (field_num, wire_type, raw_value).
    wire_type 0 → int value; wire_type 2 → bytes value; others → raw bytes.
    """
    fields = []
    pos = 0
    while pos < len(data):
        tag_wire, pos = _decode_varint(data, pos)
        field_num = tag_wire >> 3
        wire_type = tag_wire & 0x07
        if wire_type == 0:
            value, pos = _decode_varint(data, pos)
            fields.append((field_num, wire_type, value))
        elif wire_type == 2:
            length, pos = _decode_varint(data, pos)
            value = data[pos: pos + length]
            pos += length
            fields.append((field_num, wire_type, value))
        elif wire_type == 5:
            fields.append((field_num, wire_type, data[pos: pos + 4]))
            pos += 4
        elif wire_type == 1:
            fields.append((field_num, wire_type, data[pos: pos + 8]))
            pos += 8
        else:
            if warn_unknown_wire_type:
                log.warning("Unknown wire type %d at pos %d in %d-byte message (first 20 bytes: %s) — stopping decode",
                           wire_type, pos, len(data), data[:20].hex())
            break
    return fields


def _looks_mostly_text(data: bytes) -> bool:
    if not data:
        return False
    printable = sum(1 for b in data if b in (9, 10, 13) or 0x20 <= b < 0x7F)
    return (printable / max(1, len(data))) >= 0.85


def _summarize_bytes(data: bytes, max_len: int = 80) -> dict:
    if len(data) <= max_len:
        return {"len": len(data), "hex": data.hex()}
    return {
        "len": len(data),
        "hex_prefix": data[:max_len].hex(),
        "truncated": True,
    }


def _decode_generic_value(
    wire_type: int,
    value: Union[bytes, int],
    depth: int,
    max_depth: int,
    field_descriptor: Any = None,
) -> Any:
    if wire_type == 0:
        if field_descriptor is not None:
            try:
                enum_type = getattr(field_descriptor, "enum_type", None)
                if enum_type is not None and hasattr(enum_type, "values_by_number"):
                    enum_val = enum_type.values_by_number.get(int(value))
                    if enum_val is not None:
                        return enum_val.name
            except Exception:
                pass
        return int(value)

    if wire_type == 5 and isinstance(value, bytes):
        out: dict[str, Any] = {"bytes": _summarize_bytes(value)}
        if len(value) == 4:
            try:
                out["float"] = round(struct.unpack("<f", value)[0], 6)
            except Exception:
                pass
        return out

    if wire_type == 1 and isinstance(value, bytes):
        out = {"bytes": _summarize_bytes(value)}
        if len(value) == 8:
            try:
                out["double"] = round(struct.unpack("<d", value)[0], 6)
            except Exception:
                pass
        return out

    if wire_type == 2 and isinstance(value, bytes):
        if field_descriptor is not None:
            try:
                ftype = getattr(field_descriptor, "type", None)
                if ftype == getattr(_ProtoFieldDescriptor, "TYPE_STRING", 9):
                    return value.decode("utf-8", errors="replace")
                if ftype == getattr(_ProtoFieldDescriptor, "TYPE_MESSAGE", 11):
                    nested_descriptor = getattr(field_descriptor, "message_type", None)
                    if nested_descriptor is not None and depth < max_depth:
                        nested = _decode_generic_proto(
                            value,
                            depth=depth + 1,
                            max_depth=max_depth,
                            descriptor=nested_descriptor,
                        )
                        if nested:
                            return {"message": nested}
            except Exception:
                pass

        if depth < max_depth:
            try:
                nested = _decode_generic_proto(value, depth=depth + 1, max_depth=max_depth)
                if nested:
                    return {"message": nested}
            except Exception:
                pass
        if _looks_mostly_text(value):
            return {"text": value.decode("utf-8", errors="replace"), "len": len(value)}
        return {"bytes": _summarize_bytes(value)}

    return {"raw": value}


def _decode_generic_proto(
    data: bytes,
    depth: int = 0,
    max_depth: int = 2,
    descriptor: Any = None,
) -> dict[str, Any]:
    """Best-effort generic protobuf decoder for unknown trait payloads.
    
    When descriptor is provided, tries to use actual field names instead of field_N.
    """
    fields = decode_proto_fields(data, warn_unknown_wire_type=False)
    out: dict[str, Any] = {}
    for fnum, wire, val in fields:
        # Try descriptor first, fall back to field_N
        field_descriptor = None
        if descriptor:
            field_name = _get_field_name_from_descriptor(descriptor, fnum)
            field_descriptor = _get_field_descriptor_from_descriptor(descriptor, fnum)
            key = field_name or f"field_{fnum}"
        else:
            key = f"field_{fnum}"
        
        decoded = _decode_generic_value(
            wire,
            val,
            depth=depth,
            max_depth=max_depth,
            field_descriptor=field_descriptor,
        )
        if key in out:
            if not isinstance(out[key], list):
                out[key] = [out[key]]
            out[key].append(decoded)
        else:
            out[key] = decoded
    return out

def _get_field_name_from_descriptor(descriptor, field_num: int) -> Optional[str]:
    """Try to get the field name from a protobuf descriptor."""
    try:
        if hasattr(descriptor, 'fields_by_number'):
            field = descriptor.fields_by_number.get(field_num)
            if field:
                return field.name
        elif hasattr(descriptor, 'fields'):
            for field in descriptor.fields:
                if field.number == field_num:
                    return field.name
    except Exception:
        pass
    return None


def _get_field_descriptor_from_descriptor(descriptor, field_num: int) -> Any:
    """Return a protobuf field descriptor for field_num when available."""
    if descriptor is None:
        return None
    try:
        if hasattr(descriptor, "fields_by_number"):
            return descriptor.fields_by_number.get(field_num)
        if hasattr(descriptor, "fields"):
            for field in descriptor.fields:
                if field.number == field_num:
                    return field
    except Exception:
        pass
    return None


def _get_message_descriptor_from_type_url(type_url: str):
    """Resolve a protobuf message descriptor from a type_url when possible."""
    if _PROTO_SYMBOL_DB is None or not type_url:
        return None

    full_name = type_url
    if "/" in full_name:
        full_name = full_name.split("/", 1)[1]

    try:
        msg_cls = _PROTO_SYMBOL_DB.GetSymbol(full_name)
        return getattr(msg_cls, "DESCRIPTOR", None)
    except Exception:
        return None

def _decode_with_generated_proto(type_url: str, data: bytes) -> Optional[dict[str, Any]]:
    """Decode trait bytes with generated protobuf descriptors when available."""
    if MessageToDict is None or _PROTO_SYMBOL_DB is None or not type_url:
        return None

    full_name = type_url
    if "/" in full_name:
        full_name = full_name.split("/", 1)[1]

    try:
        msg_cls = _PROTO_SYMBOL_DB.GetSymbol(full_name)
    except Exception as e:
        log.debug("_decode_with_generated_proto: symbol lookup failed for %s: %s", full_name, e)
        return None

    try:
        msg = msg_cls()
        msg.ParseFromString(data)
        return MessageToDict(
            msg,
            preserving_proto_field_name=True,
        )
    except Exception as e:
        log.debug(
            "_decode_with_generated_proto: parse failed for %s (%d bytes): %s",
            full_name,
            len(data),
            e,
        )
        return None


def _decode_timestamp_fields(ts_bytes: bytes) -> dict[str, Any]:
    """Decode google.protobuf.Timestamp-ish bytes into unix/nanos/iso."""
    seconds = 0
    nanos = 0
    for tf, tw, tv in decode_proto_fields(ts_bytes):
        if tf == 1 and tw == 0:
            seconds = int(tv)
        elif tf == 2 and tw == 0:
            nanos = int(tv)

    result: dict[str, Any] = {
        "seconds": seconds,
        "nanos": nanos,
    }
    try:
        result["iso_utc"] = datetime.fromtimestamp(
            seconds + (nanos / 1_000_000_000.0),
            tz=timezone.utc,
        ).isoformat(timespec="milliseconds")
    except Exception:
        pass
    return result


# ── gRPC-Web framing ─────────────────────────────────────────────────────────

def grpc_web_encode(proto_bytes: bytes) -> bytes:
    """Wrap protobuf bytes in a gRPC-Web data frame."""
    return b"\x00" + struct.pack(">I", len(proto_bytes)) + proto_bytes


def grpc_web_decode(body: bytes) -> List[Tuple[int, bytes]]:
    """
    Decode a gRPC-Web response body into a list of (flag, payload) frames.
    flag=0x00 → data frame; flag=0x80 → trailer frame (ASCII headers).
    """
    frames = []
    pos = 0
    while pos + 5 <= len(body):
        flag = body[pos]
        length = struct.unpack(">I", body[pos + 1: pos + 5])[0]
        pos += 5
        payload = body[pos: pos + length]
        pos += length
        frames.append((flag, payload))
    return frames


# ── High-level calls ─────────────────────────────────────────────────────────

def call(method_path: str, proto_bytes: bytes, nest_jwt: str,
         extra_headers: Optional[Dict[str, str]] = None) -> Tuple[int, bytes]:
    """
    Make a Nest protobuf-over-HTTP POST request.

    The server uses Content-Type: application/x-protobuf with raw (unframed)
    request bodies, and responds with Content-Transfer-Encoding: base64 bodies
    that must be base64-decoded to get the raw protobuf response bytes.

    Returns (http_status, proto_response_bytes).
    """
    headers = {
        "Content-Type": "application/x-protobuf",
        "Accept": "application/x-protobuf, application/json",
        "Authorization": f"Basic {nest_jwt}",
        "Origin": "https://home.nest.com",
        "Referer": "https://home.nest.com/",
        "X-Accept-Response-Streaming": "true",
        "X-Accept-Content-Transfer-Encoding": "base64",
        "X-Nl-Webapp-Version": _WEBAPP_VERSION,
    }
    if extra_headers:
        headers.update(extra_headers)

    url = _GRPC_WEB_BASE + method_path
    log.debug("Nest protobuf POST %s  (%d bytes)", url, len(proto_bytes))
    resp = requests.post(url, data=proto_bytes, headers=headers, timeout=30)
    log.debug("Response %d  content-type=%s  (%d bytes)",
              resp.status_code, resp.headers.get("content-type", "?"), len(resp.content))

    body = resp.content
    ct = resp.headers.get("content-type", "")
    cte = resp.headers.get("content-transfer-encoding", "")
    if "base64" in cte and body:
        import base64 as _b64
        try:
            body = _b64.b64decode(body)
        except Exception:
            pass
    return resp.status_code, body


# ── Specific service calls ────────────────────────────────────────────────────

_ID_MAP_PATH = (
    "/nest.services.apigateway.ResourceRelationshipService"
    "/GetResourceIdMappingByLegacyIds"
)


def get_resource_id_mapping(legacy_ids: List[str], nest_jwt: str) -> Dict[str, Any]:
    """
    Call GetResourceIdMappingByLegacyIds and return a dict of results.

    Returns a dict with keys:
      'http_status'   – int HTTP status code
      'raw_bytes'     – raw decoded protobuf bytes
      'proto_fields'  – decoded fields from the response
      'id_map'        – best-effort {legacy_id: resource_id} extraction
      'resources'     – best-effort list of ResourceMetadata dicts
    """
    # Encode request: repeated string legacy_ids = field 1
    req_proto = encode_repeated_strings(1, legacy_ids)

    status, body = call(_ID_MAP_PATH, req_proto, nest_jwt)

    result: dict = {
        "http_status": status,
        "raw_bytes": body,
        "proto_fields": [],
        "id_map": {},
        "resources": [],
    }

    if not body:
        return result

    fields = decode_proto_fields(body)
    result["proto_fields"] = fields

    for fnum, wtype, val in fields:
        if fnum == 1 and wtype == 2:
            # map entry: sub-message with key=1 (string), value=2 (string)
            sub = decode_proto_fields(val)
            entry: Dict[str, str] = {}
            for sf, sw, sv in sub:
                if sw == 2:
                    if sf == 1:
                        entry["key"] = sv.decode("utf-8", errors="replace")
                    elif sf == 2:
                        entry["value"] = sv.decode("utf-8", errors="replace")
            if "key" in entry:
                result["id_map"][entry["key"]] = entry.get("value", "")
        elif fnum == 2 and wtype == 2:
            # ResourceMetadata sub-message
            sub = decode_proto_fields(val)
            meta: Dict[str, str] = {}
            for sf, sw, sv in sub:
                if sw == 2:
                    text = sv.decode("utf-8", errors="replace")
                    if sf == 1:
                        meta["legacy_id"] = text
                    elif sf == 2:
                        meta["resource_id"] = text
                    elif sf == 3:
                        meta["type"] = text
            result["resources"].append(meta)

    return result


# ── SendCommand ───────────────────────────────────────────────────────────────

_SEND_COMMAND_PATH = "/nestlabs.gateway.v1.ResourceApi/SendCommand"


def _encode_bytes_field(field_num: int, value: bytes) -> bytes:
    """Encode a protobuf bytes/sub-message field (wire type 2)."""
    tag = _encode_varint((field_num << 3) | 2)
    return tag + _encode_varint(len(value)) + value


def _encode_varint_field(field_num: int, value: int) -> bytes:
    """Encode a protobuf varint field (wire type 0)."""
    tag = _encode_varint(field_num << 3)
    return tag + _encode_varint(value)


def energy_history_request(device_resource_id: str, nest_jwt: str,
                            request_id: Optional[str] = None,
                            days_back: int = 10) -> Tuple[int, bytes]:
    """
    Request LegacyEnergyHistory for a thermostat via SendCommand.

    Proto structure (reverse-engineered from HAR):
      SendCommandRequest {
        field 1: CommandTarget {
          field 1: device_resource_id (string)
          field 2: request_uuid (string)
        }
        field 2: TraitCommand {
          field 1: "energy_history" (string)
          field 2: google.protobuf.Any {
            field 1: type_url (string)
            field 2: LegacyEnergyHistoryRequest bytes {
              field 1: start_time sub-msg { field 1: unix_seconds }
              field 2: end_time  sub-msg { field 1: unix_seconds }
            }
          }
          field 3: request_timestamp {
            field 1: unix_seconds
            field 2: nanos
          }
        }
      }

    device_resource_id: "DEVICE_12345678901234"
    Returns (http_status, raw_proto_response_bytes).
    """
    import uuid
    import time

    if request_id is None:
        request_id = str(uuid.uuid4())

    now_float = time.time()
    now_sec = int(now_float)
    now_nanos = int((now_float % 1) * 1_000_000_000)

    # Round end to 6-hour boundary (Nest billing day seems to start at 06:00 UTC)
    end_sec = now_sec - (now_sec % 3600)
    start_sec = end_sec - (days_back * 86400)

    type_url = (
        "type.nestlabs.com/nest.trait.history"
        ".EnergyHistoryTrait.LegacyEnergyHistoryRequest"
    )

    # Encode LegacyEnergyHistoryRequest bytes:
    #   field 1 = {field1: start_sec}   (start time sub-message)
    #   field 2 = {field1: end_sec}     (end time sub-message)
    energy_req_bytes = (
        _encode_bytes_field(1, _encode_varint_field(1, start_sec))
        + _encode_bytes_field(2, _encode_varint_field(1, end_sec))
    )

    # Encode google.protobuf.Any:
    #   field 1 = type_url string
    #   field 2 = serialized LegacyEnergyHistoryRequest
    any_msg = (
        encode_string_field(1, type_url)
        + _encode_bytes_field(2, energy_req_bytes)
    )

    # Encode request timestamp (wall-clock time of the request):
    #   field 1 = seconds,  field 2 = nanos
    req_ts = (
        _encode_varint_field(1, now_sec)
        + _encode_varint_field(2, now_nanos)
    )

    # Encode TraitCommand (field 2 of SendCommandRequest):
    #   field 1 = trait name "energy_history"
    #   field 2 = Any message
    #   field 3 = request timestamp
    trait_cmd = (
        encode_string_field(1, "energy_history")
        + _encode_bytes_field(2, any_msg)
        + _encode_bytes_field(3, req_ts)
    )

    # Encode CommandTarget (field 1 of SendCommandRequest):
    #   field 1 = device_resource_id,  field 2 = request_uuid
    cmd_target = (
        encode_string_field(1, device_resource_id)
        + encode_string_field(2, request_id)
    )

    req = (
        _encode_bytes_field(1, cmd_target)
        + _encode_bytes_field(2, trait_cmd)
    )

    return call(_SEND_COMMAND_PATH, req, nest_jwt,
                extra_headers={"Request-Id": request_id})


# ── Observe (streaming) ───────────────────────────────────────────────────────

_OBSERVE_PATH = "/nestlabs.gateway.v2.GatewayService/Observe"

# All thermostat-relevant traits found in sdk-421f7ba4a82b319d562a.js.
# The Observe API silently ignores traits the device doesn't support,
# so it is safe to request all of them; the response contains only the
# traits present on the target resource.
#
# Run `scripts/compile_protos.sh` after capturing a new JS bundle to
# regenerate Python bindings.  See PROTO_CAPTURE.md for instructions.
OBSERVE_TRAITS_THERMOSTAT = [
    # Core HVAC state & control
    "nest.trait.hvac.HvacControlTrait",
    "nest.trait.hvac.EcoModeStateTrait",
    "nest.trait.hvac.EcoModeTrait",
    "nest.trait.hvac.EcoModeSettingsTrait",
    "nest.trait.hvac.FanControlTrait",
    "nest.trait.hvac.FanControlSettingsTrait",
    "nest.trait.hvac.FanControlCapabilitiesTrait",
    "nest.trait.hvac.TargetTemperatureSettingsTrait",
    "nest.trait.hvac.SetPointScheduleSettingsTrait",
    "nest.trait.hvac.IntervalScheduleSettingsTrait",
    "nest.trait.hvac.ScheduleHoldTrait",
    "nest.trait.hvac.ScheduleHoldSettingsTrait",
    "nest.trait.hvac.ScheduleLearningSettingsTrait",
    "nest.trait.hvac.ScheduleOffsetTrait",
    "nest.trait.hvac.ScheduleOffsetSettingsTrait",
    # Equipment & wiring
    "nest.trait.hvac.EquipmentSettingsTrait",
    "nest.trait.hvac.HvacEquipmentCapabilitiesTrait",
    "nest.trait.hvac.WiringTrait",
    "nest.trait.hvac.InstallationSettingsTrait",
    "nest.trait.hvac.BackplateInfoTrait",
    "nest.trait.hvac.HvacDisplayTrait",
    "nest.trait.hvac.DisplaySettingsTrait",
    # Heat pump / dual fuel / radiant
    "nest.trait.hvac.HeatPumpControlTrait",
    "nest.trait.hvac.HeatPumpControlSettingsTrait",
    "nest.trait.hvac.EmergencyHeatSettingsTrait",
    "nest.trait.hvac.RadiantControlSettingsTrait",
    # Humidity control
    "nest.trait.hvac.HumidityControlSettingsTrait",
    # Airwave (evaporative cooling assist)
    "nest.trait.hvac.AirwaveTrait",
    "nest.trait.hvac.AirwaveSettingsTrait",
    # Cool-to-dry
    "nest.trait.hvac.CoolToDryTrait",
    "nest.trait.hvac.CoolToDrySettingsTrait",
    # Safety & shutoff
    "nest.trait.hvac.SafetyTemperatureTrait",
    "nest.trait.hvac.SafetyTemperatureSettingsTrait",
    "nest.trait.hvac.SafetyShutoffTrait",
    "nest.trait.hvac.SafetyShutoffSettingsTrait",
    "nest.trait.hvac.SafetyShutoffCapabilitiesTrait",
    # Leaf / energy saving
    "nest.trait.hvac.LeafTrait",
    # Preconditioning
    "nest.trait.hvac.PreconditioningTrait",
    "nest.trait.hvac.PreconditioningSettingsTrait",
    # Remote comfort sensing (sensors)
    "nest.trait.hvac.RemoteComfortSensingStateTrait",
    "nest.trait.hvac.RemoteComfortSensingSettingsTrait",
    # Sunblock (window heating detection)
    "nest.trait.hvac.SunblockTrait",
    "nest.trait.hvac.SunblockSettingsTrait",
    # Filter reminder
    "nest.trait.hvac.FilterReminderTrait",
    "nest.trait.hvac.FilterReminderSettingsTrait",
    # Time-to-temperature
    "nest.trait.hvac.TimeToTemperatureTrait",
    # Temperature lock
    "nest.trait.hvac.TemperatureLockSettingsTrait",
    # Demand response
    "nest.trait.hvac.DemandResponseTrait",
    "nest.trait.hvac.DemandResponseConfigurationTrait",
    # Seasonal savings
    "nest.trait.hvac.SeasonalSavingsTrait",
    "nest.trait.hvac.SeasonalSavingsSettingsTrait",
    # Vendor / diagnostics
    "nest.trait.hvac.HvacVendorPartnerInfoTrait",
    "nest.trait.hvac.HvacDiagnosticsAlertsSettingsTrait",
    # Sensors
    "nest.trait.sensor.TemperatureTrait",
    "nest.trait.sensor.HumidityTrait",
    "nest.trait.sensor.PassiveInfraredTrait",
    "nest.trait.sensor.AmbientLightTrait",
    "nest.trait.sensor.BatteryVoltageTrait",
    "nest.trait.sensor.CarbonMonoxideTrait",
    "nest.trait.sensor.SmokeTrait",
    # History
    "nest.trait.history.EnergyHistoryTrait",
    "nest.trait.history.OccupancyHistoryTrait",
    # Structure / occupancy mode
    "nest.trait.occupancy.StructureModeTrait",
    # Device & location info
    "nest.trait.service.DeviceInfoTrait",
    "nest.trait.located.DeviceLocatedSettingsTrait",
    "nest.trait.located.LocatedAnnotationsTrait",
    "nest.trait.located.LocatedTrait",
    # Network / connectivity
    "nest.trait.network.WifiInterfaceTrait",
    "nest.trait.network.TelemetryNetworkTrait",
    "nest.trait.network.RadioSignalStrengthTrait",
    # Firmware
    "nest.trait.firmware.FirmwareTrait",
    # Diagnostics / faults
    "nest.trait.system.FaultTrait",
]


def build_observe_request(traits: List[str]) -> bytes:
    """
    Build an Observe request proto.

    If the generated protobuf bindings are available, use the compiled
    ObserveRequest type. Otherwise, fall back to the legacy manual encoding.
    """
    # Keep manual encoding as the source of truth; this matches captured HAR
    # behavior and reliably returns trait state updates.
    # field 1 = mode indicator (opaque 2 bytes from HAR capture)
    req = _encode_bytes_field(1, b"\x02\x01")
    for trait in traits:
        trait_spec = encode_string_field(1, trait)
        req += _encode_bytes_field(3, trait_spec)
    return req


def observe_snapshot(traits: List[str], nest_jwt: str,
                     timeout_sec: float = 15.0) -> List[bytes]:
    """
    Open a streaming Observe connection, collect all response frames until
    the connection closes or *timeout_sec* is reached, then return a list of
    decoded proto message bytes.

    The server sends content-transfer-encoding: base64 with
    content-type: application/x-protobuf.google.rpc.streambody.
    Each HTTP chunk may be a full or partial base64-encoded proto message;
    we accumulate and strip whitespace before decoding.
    """
    import base64 as _b64

    req = build_observe_request(traits)
    headers = {
        "Content-Type": "application/x-protobuf",
        "Accept": "application/x-protobuf.google.rpc.streambody",
        "Authorization": f"Basic {nest_jwt}",
        "Origin": "https://home.nest.com",
        "Referer": "https://home.nest.com/",
        "X-Accept-Response-Streaming": "true",
        "X-Accept-Content-Transfer-Encoding": "base64",
        "X-Nl-Webapp-Version": _WEBAPP_VERSION,
    }

    url = _GRPC_WEB_BASE + _OBSERVE_PATH
    log.debug("Observe POST %s (%d bytes req, %d traits)", url, len(req), len(traits))

    messages: List[bytes] = []
    raw_chunks: List[bytes] = []

    try:
        resp = requests.post(
            url, data=req, headers=headers,
            timeout=(10, timeout_sec),
            stream=True,
        )
        log.debug("Observe response %d  content-type=%s",
                  resp.status_code, resp.headers.get("content-type", "?"))

        if resp.status_code != 200:
            log.warning("Observe returned HTTP %d", resp.status_code)
            # Try to read the error body
            try:
                err_body = resp.content
                if err_body:
                    messages.append(_b64.b64decode(err_body))
            except Exception:
                pass
            return messages

        for chunk in resp.iter_content(chunk_size=None):
            if chunk:
                raw_chunks.append(chunk)
                log.debug("Observe chunk: %d bytes", len(chunk))

    except requests.exceptions.ReadTimeout:
        log.debug("Observe read timeout after %.1fs — got %d chunks",
                  timeout_sec, len(raw_chunks))
    except Exception as e:
        log.warning("Observe error: %s", e)

    if not raw_chunks:
        return messages

    # Diagnostic: log first raw chunk to understand what we're dealing with
    if raw_chunks:
        first_chunk = raw_chunks[0]
        preview = first_chunk[:50]
        log.debug(
            "Observe: first chunk (%d bytes): hex=%s  ascii=%s",
            len(first_chunk),
            preview.hex(),
            preview.decode('ascii', errors='replace')
        )

    # Strategy 1: entire accumulated buffer is one base64 blob
    all_bytes = b"".join(raw_chunks)
    b64_clean = all_bytes.replace(b"\r", b"").replace(b"\n", b"")

    # Try to decode as one big message first
    try:
        proto = _b64.b64decode(b64_clean)
        log.debug("Observe: successfully decoded single blob from base64 (%d → %d bytes)", 
                  len(b64_clean), len(proto))
        # Log first few bytes of decoded proto to check for gRPC-Web framing
        log.debug("Observe: decoded blob preview: %s", proto[:20].hex())
        messages.append(proto)
        return messages
    except Exception as e:
        log.debug("Observe: single blob base64 decode failed: %s", e)

    # Strategy 2: Try as raw gRPC-Web framed data (not base64)
    # gRPC-Web frame: [1 byte flag][4 bytes length][N bytes payload]
    all_bytes = b"".join(raw_chunks)
    pos = 0
    decoded_count = 0
    while pos < len(all_bytes):
        if pos + 5 > len(all_bytes):
            break
        flag = all_bytes[pos]
        length = int.from_bytes(all_bytes[pos+1:pos+5], 'big')
        pos += 5
        if pos + length > len(all_bytes):
            log.warning("Observe: gRPC frame truncated at pos %d (expected %d more bytes)", pos, length)
            break
        payload = all_bytes[pos:pos+length]
        pos += length
        messages.append(payload)
        decoded_count += 1
        log.debug("Observe: extracted gRPC-Web frame %d (flag=0x%02x, %d bytes)", 
                  decoded_count, flag, length)

    if decoded_count > 0:
        log.debug("Observe: extracted %d gRPC-Web frames (raw binary, not base64)", decoded_count)
        return messages

    # Strategy 3: each chunk is an independent base64 message
    for i, chunk in enumerate(raw_chunks):
        chunk_clean = chunk.strip()
        if not chunk_clean:
            continue
        try:
            proto = _b64.b64decode(chunk_clean)
            messages.append(proto)
            log.debug("Observe: decoded chunk %d from base64 (%d → %d bytes)", 
                      i, len(chunk_clean), len(proto))
        except Exception as e:
            log.debug("Observe: chunk %d base64 decode failed: %s", i, e)
            pass

    if messages:
        log.debug("Observe: decoded %d proto messages from chunks", len(messages))

    return messages


# ── Parse Observe response ────────────────────────────────────────────────────

_ECO_MODE = {0: "unspecified", 1: "inactive", 2: "manual_eco", 3: "auto_eco"}


def _read_float_value(data: bytes) -> Optional[float]:
    """Decode google.protobuf.FloatValue: { float value = 1 } (wire type 5)."""
    for f, w, v in decode_proto_fields(data):
        if f == 1 and w == 5 and isinstance(v, bytes) and len(v) == 4:
            import struct as _struct
            return _struct.unpack("<f", v)[0]
    return None


def _read_temperature_trait(data: bytes) -> Optional[float]:
    """
    TemperatureTrait { TemperatureSample temperature_value = 1;
                       TemperatureFaultInformation temperature_fault = 2 }
    TemperatureSample { FloatValue temperature = 1 }
    Returns temperature in Celsius, or None.
    
    Source: Verified against SDK field definitions via extract_sdk.py.
    """
    if sensor_pb2 is not None:
        try:
            msg = sensor_pb2.TemperatureTrait()
            msg.ParseFromString(data)
            if msg.HasField("temperature_value") and msg.temperature_value.HasField("value"):
                return float(msg.temperature_value.value.value)
        except Exception:
            pass

    for f, w, v in decode_proto_fields(data):
        if f == 1 and isinstance(v, bytes):          # TemperatureSample
            for sf, sw, sv in decode_proto_fields(v):
                if sf == 1 and isinstance(sv, bytes):  # FloatValue
                    return _read_float_value(sv)
    return None


def _read_humidity_trait(data: bytes) -> Optional[float]:
    """
    HumidityTrait { HumiditySample humidity_value = 1;
                    HumidityFaultInformation humidity_fault = 2 }
    HumiditySample { FloatValue humidity = 1 }
    Returns relative humidity 0-100, or None.
    
    Source: Verified against SDK field definitions via extract_sdk.py.
    """
    if sensor_pb2 is not None:
        try:
            msg = sensor_pb2.HumidityTrait()
            msg.ParseFromString(data)
            if msg.HasField("humidity_value") and msg.humidity_value.HasField("value"):
                return float(msg.humidity_value.value.value)
        except Exception:
            pass

    for f, w, v in decode_proto_fields(data):
        if f == 1 and isinstance(v, bytes):
            for sf, sw, sv in decode_proto_fields(v):
                if sf == 1 and isinstance(sv, bytes):
                    return _read_float_value(sv)
    return None


def _read_carbon_monoxide_trait(data: bytes) -> Optional[float]:
    """
    CarbonMonoxideTrait { CoSample co_value = 1;
                          CoFaultInformation co_fault = 2 }
    CoSample { uint32 value = 1 }   ← raw integer ppm, NOT a FloatValue wrapper

    Source: Verified against SDK sdk-421f7ba4a82b319d562a.js.
    Returns CO concentration in ppm as float, or None.
    """
    for f, w, v in decode_proto_fields(data):
        if f == 1 and isinstance(v, bytes):  # CoSample sub-message
            for sf, sw, sv in decode_proto_fields(v):
                if sf == 1 and sw == 0:  # uint32 varint (wire type 0)
                    return float(sv)
    return None


def _read_smoke_trait(data: bytes) -> Optional[float]:
    """
    SmokeTrait { SmokeSample infrared_led_value = 3;
                 SmokeSample heat_sensor_value = 4;
                 SmokeFaultInformation infrared_led_fault = 5;
                 SmokeFaultInformation heat_sensor_fault = 6 }
    SmokeSample { FloatValue value = 3 }   ← field 3, NOT field 1

    Note: SmokeTrait uses legacy field-numbering starting at 3, and inside
    SmokeSample the FloatValue is also at field 3 (not field 1).

    Source: Verified against SDK sdk-421f7ba4a82b319d562a.js.
    Returns smoke level, or None.
    """
    for f, w, v in decode_proto_fields(data):
        if f in (3, 4) and isinstance(v, bytes):  # SmokeSample
            for sf, sw, sv in decode_proto_fields(v):
                if sf == 3 and isinstance(sv, bytes):  # FloatValue at field 3
                    result = _read_float_value(sv)
                    if result is not None:
                        return result
    return None


def _read_battery_voltage_trait(data: bytes) -> Optional[float]:
    """
    BatteryVoltageTrait { BatteryVoltageSample batteryValue = 1;
                          BatteryVoltageFaultInformation battery_fault = 2 }
    BatteryVoltageSample { FloatValue batteryVoltage = 1 }
    Returns battery voltage in volts, or None.
    
    Source: Verified against SDK field definitions via extract_sdk.py.
    """
    for f, w, v in decode_proto_fields(data):
        if f == 1 and isinstance(v, bytes):  # BatteryVoltageSample
            for sf, sw, sv in decode_proto_fields(v):
                if sf == 1 and isinstance(sv, bytes):  # FloatValue
                    return _read_float_value(sv)
    return None


def _read_hvac_control_trait(data: bytes) -> dict:
    """
    HvacControlTrait { HvacState hvac_state = 1; ... }
    HvacState { bool cool_stage1_active=1; bool heat_stage1_active=4; ... }

    Source: reverse-engineered from sdk-421f7ba4a82b319d562a.js
    proto/nest_hvac_extra.proto documents the full field map.

    Returns dict with decoded HvacControlTrait fields.
    """
    _HVAC_STATE_FIELDS = {
        1: "cool_stage1_active",     2: "cool_stage2_active",
        3: "cool_stage3_active",     4: "heat_stage1_active",
        5: "heat_stage2_active",     6: "heat_stage3_active",
        7: "alt_heat_stage1_active", 8: "alt_heat_stage2_active",
        9: "auxiliary_heat_active",  10: "emergency_heat_active",
        11: "humidifier_active",     12: "dehumidifier_active",
        13: "ventilator_active",
    }
    # Default all stage fields to False so they always appear in output
    result: dict = {name: False for name in _HVAC_STATE_FIELDS.values()}

    if hvac_pb2 is not None:
        try:
            msg = hvac_pb2.HvacControlTrait()
            msg.ParseFromString(data)
            if msg.HasField("settings"):
                for name in _HVAC_STATE_FIELDS.values():
                    result[name] = bool(getattr(msg.settings, name, False))
            if getattr(msg, "is_delayed", None) is not None:
                result["is_delayed"] = bool(msg.is_delayed)
            if getattr(msg, "is_cooling", None) is not None:
                result["is_cooling"] = bool(msg.is_cooling)
            if getattr(msg, "is_heating", None) is not None:
                result["is_heating"] = bool(msg.is_heating)
        except Exception:
            pass

    for f, w, v in decode_proto_fields(data):
        if f == 1 and isinstance(v, bytes):  # HvacState sub-message
            for sf, sw, sv in decode_proto_fields(v):
                if sf in _HVAC_STATE_FIELDS and sw == 0:
                    result[_HVAC_STATE_FIELDS[sf]] = bool(sv)
        elif f == 2 and w == 0:
            result["compressor_lockout_enabled"] = bool(v)
        elif f == 4 and w == 0:
            result["min_cycle_lockout_enabled"] = bool(v)

    return result


_FAN_SPEED_SETTING = {
    0: "unspecified", 1: "stage1", 2: "stage2", 3: "stage3",
    4: "off", 5: "auto",
}
_FAN_MODE = {
    0: "unspecified", 1: "auto", 2: "continuous_on", 3: "duty_cycle",
}
_FAN_SPEED_STATE = {
    0: "unspecified", 1: "off", 2: "timer", 3: "schedule",
    4: "equipment_activation", 5: "ventilation_activation", 6: "system_test",
}
_VENTILATION_STATE = {
    0: "unspecified", 1: "off", 2: "timer", 3: "schedule",
    4: "indoor_air_quality", 5: "heating", 6: "cooling", 7: "system_test",
}


def _read_fan_control_trait(data: bytes) -> dict:
    """
    FanControlTrait { FanSpeedSetting current_speed=1; bool user_requested_fan_running=2;
                       VentilationState ventilation_state=3; VentilationAlert ventilation_alert=4;
                       FanSpeedState fan_speed_state=5 }

    Source: reverse-engineered from sdk-421f7ba4a82b319d562a.js
    Observed raw (idle): 0804 → f1=4=FAN_SPEED_SETTING_OFF
    """
    result: dict = {}

    if hvac_pb2 is not None:
        try:
            msg = hvac_pb2.FanControlTrait()
            msg.ParseFromString(data)
            speed = _FAN_SPEED_SETTING.get(msg.currentSpeed, str(msg.currentSpeed))
            result["current_speed"] = speed
            result["user_requested_fan_running"] = bool(msg.userRequestedFanRunning)
        except Exception:
            pass

    for f, w, v in decode_proto_fields(data):
        if f == 1 and w == 0:
            speed = _FAN_SPEED_SETTING.get(v, f"unknown({v})")
            result["current_speed"] = speed
        elif f == 2 and w == 0:
            result["user_requested_fan_running"] = bool(v)
        elif f == 3 and w == 0:
            result["ventilation_state"] = _VENTILATION_STATE.get(v, f"unknown({v})")
        elif f == 5 and w == 0:
            result["current_speed_state"] = _FAN_SPEED_STATE.get(v, f"unknown({v})")
    return result


def _read_fan_control_settings_trait(data: bytes) -> dict:
    """Decode FanControlSettingsTrait with all known fields."""
    result: dict = {}

    def _read_int32_indirect(raw: bytes) -> Optional[int]:
        # Int32_Indirect { int32 value = 1 }
        for sf, sw, sv in decode_proto_fields(raw):
            if sf == 1 and sw == 0:
                try:
                    return int(sv)
                except Exception:
                    return None
        return None

    if hvac_pb2 is not None:
        try:
            msg = hvac_pb2.FanControlSettingsTrait()
            msg.ParseFromString(data)

            result["mode"] = _FAN_MODE.get(int(msg.mode), str(int(msg.mode)))
            result["hvac_override_speed"] = _FAN_SPEED_SETTING.get(
                int(msg.hvacOverrideSpeed), str(int(msg.hvacOverrideSpeed))
            )
            result["schedule_speed"] = _FAN_SPEED_SETTING.get(
                int(msg.scheduleSpeed), str(int(msg.scheduleSpeed))
            )
            result["schedule_duty_cycle"] = int(msg.scheduleDutyCycle)
            result["schedule_start_time"] = int(msg.scheduleStartTime)
            result["schedule_end_time"] = int(msg.scheduleEndTime)
            result["timer_speed"] = _FAN_SPEED_SETTING.get(int(msg.timerSpeed), str(int(msg.timerSpeed)))

            if hasattr(msg, "fanTimerTimeout") and msg.fanTimerTimeout is not None:
                timeout_val = getattr(msg.fanTimerTimeout, "value", None)
                if timeout_val is not None:
                    result["fan_timer_timeout"] = int(timeout_val)
            if hasattr(msg, "timerDuration") and msg.timerDuration is not None:
                duration_val = getattr(msg.timerDuration, "value", None)
                if duration_val is not None:
                    result["timer_duration"] = int(duration_val)
        except Exception:
            pass

    for f, w, v in decode_proto_fields(data):
        if f == 1 and w == 0:
            result["mode"] = _FAN_MODE.get(v, f"unknown({v})")
        elif f == 2 and w == 0:
            result["hvac_override_speed"] = _FAN_SPEED_SETTING.get(v, f"unknown({v})")
        elif f == 3 and w == 0:
            result["schedule_speed"] = _FAN_SPEED_SETTING.get(v, f"unknown({v})")
        elif f == 4 and w == 0:
            result["schedule_duty_cycle"] = int(v)
        elif f == 5 and w == 0:
            result["schedule_start_time"] = int(v)
        elif f == 6 and w == 0:
            result["schedule_end_time"] = int(v)
        elif f == 7 and w == 0:
            result["timer_speed"] = _FAN_SPEED_SETTING.get(v, f"unknown({v})")
        elif f == 8 and isinstance(v, bytes):
            timeout = _read_int32_indirect(v)
            if timeout is not None:
                result["fan_timer_timeout"] = timeout
        elif f == 9 and isinstance(v, bytes):
            duration = _read_int32_indirect(v)
            if duration is not None:
                result["timer_duration"] = duration

    return result


def _read_target_temperature_trait(data: bytes) -> dict:
    """
    TargetTemperatureSettingsTrait { TemperatureSetPoint target_temperature = 1 }
    TemperatureSetPoint { setpoint_type=1; seconds_in_day=3;
                          HvacControl.Temperature heating_target=4;
                          HvacControl.Temperature cooling_target=5 }
    HvacControl.Temperature { float value = 1 }

    Source: reverse-engineered from sdk-421f7ba4a82b319d562a.js
    """
    _SETPOINT_TYPE = {0: "unspecified", 1: "heat", 2: "cool", 3: "range"}
    result: dict = {}
    for f, w, v in decode_proto_fields(data):
        if f == 1 and isinstance(v, bytes):  # TemperatureSetPoint
            for sf, sw, sv in decode_proto_fields(v):
                if sf == 1 and sw == 0:
                    result["setpoint_type"] = _SETPOINT_TYPE.get(sv, f"unknown({sv})")
                elif sf == 4 and isinstance(sv, bytes):  # heating_target Temperature
                    fv = _read_float_value(sv)
                    if fv is not None:
                        result["heating_target_c"] = round(fv, 2)
                elif sf == 5 and isinstance(sv, bytes):  # cooling_target Temperature
                    fv = _read_float_value(sv)
                    if fv is not None:
                        result["cooling_target_c"] = round(fv, 2)
    return result


def parse_observe_response(
    stream_body_bytes: bytes,
    include_empty_traits: bool = False,
    warn_unknown_wire_type: bool = True,
) -> Dict[str, Dict[str, Any]]:
    """
        Parse a StreamBody-wrapped ObserveResponse and return a trait-structured dict:

      {
        "DEVICE_XXXX": {
                    "resource_type": str,
                    "traits": {
                        "current_temperature": {
                            "trait_type": "nest.trait.sensor.TemperatureTrait",
                            "fields": {"value_c": 21.9}
                        },
                        "fan_control": {
                            "trait_type": "nest.trait.hvac.FanControlTrait",
                            "fields": {"current_speed": "stage1", ...}
                        }
                    }
        },
        ...
      }

    Only keys with successfully decoded values are included.

    Proto layout (from nest-observe submodule protos):
      StreamBody { repeated bytes message = 1 }  (each = ObserveResponse)
      ObserveResponse { repeated ResourceMeta resource_metas = 1;
                        repeated TraitState trait_states = 3 }
      TraitState { TraitId trait_id = 1; Patch patch = 3 }
      TraitId { string resource_id = 1; string trait_label = 2 }
      Patch { google.protobuf.Any values = 1 }
      Any { string type_url = 1; bytes value = 2 }
    """
    devices: Dict[str, Dict[str, Any]] = {}

    def _dpf(data: bytes) -> List[Tuple[int, int, Union[bytes, int]]]:
        return decode_proto_fields(data, warn_unknown_wire_type=warn_unknown_wire_type)

    try:
        # Unwrap StreamBody → repeated ObserveResponse bytes
        for f, w, v in _dpf(stream_body_bytes):
            if f != 1 or not isinstance(v, bytes):
                continue  # also f=2 Status, f=15 noop

            # Each v is one serialized ObserveResponse
            for of, ow, ov in _dpf(v):
                if of == 1 and isinstance(ov, bytes):
                    # ResourceMeta: extract resource_id and type
                    dev_info: dict = {}
                    trait_metas: list[dict[str, str]] = []
                    for rf, rw, rv in _dpf(ov):
                        if rf == 1 and isinstance(rv, bytes):
                            dev_info["resource_id"] = rv.decode("utf-8", "replace")
                        elif rf == 2 and isinstance(rv, bytes):
                            dev_info["resource_type"] = rv.decode("utf-8", "replace")
                        elif rf == 4 and isinstance(rv, bytes):
                            trait_meta: dict[str, str] = {}
                            for tmf, tmw, tmv in _dpf(rv):
                                if tmf == 1 and isinstance(tmv, bytes):
                                    trait_meta["trait_label"] = tmv.decode("utf-8", "replace")
                                elif tmf == 2 and isinstance(tmv, bytes):
                                    trait_meta["trait_type"] = tmv.decode("utf-8", "replace")
                            if trait_meta:
                                trait_metas.append(trait_meta)
                    if "resource_id" in dev_info:
                        d = devices.setdefault(dev_info["resource_id"], {})
                        if "resource_type" in dev_info:
                            d["resource_type"] = dev_info["resource_type"]
                        trait_map = d.setdefault("traits", {})
                        for trait_meta in trait_metas:
                            trait_label = trait_meta.get("trait_label", "")
                            if not trait_label:
                                continue
                            entry = trait_map.setdefault(trait_label, {"trait_type": "", "fields": {}})
                            if trait_meta.get("trait_type"):
                                entry["trait_type"] = trait_meta["trait_type"]
                            entry.setdefault("fields", {})

                elif of == 2 and ow == 0:
                    meta = devices.setdefault("__observe_meta__", {})
                    meta["initial_resource_metas_continue"] = bool(ov)

                elif of == 4 and isinstance(ov, bytes):
                    # TraitOperationList { trait_id=1; trait_operations=2[] }
                    op_resource_id = ""
                    op_trait_label = ""
                    operations: list[dict[str, Any]] = []
                    for opf, opw, opv in _dpf(ov):
                        if opf == 1 and isinstance(opv, bytes):
                            for idf, idw, idv in _dpf(opv):
                                if idf == 1 and isinstance(idv, bytes):
                                    op_resource_id = idv.decode("utf-8", "replace")
                                elif idf == 2 and isinstance(idv, bytes):
                                    op_trait_label = idv.decode("utf-8", "replace")
                        elif opf == 2 and isinstance(opv, bytes):
                            operations.append(_decode_generic_proto(opv))

                    if op_resource_id and op_trait_label:
                        d = devices.setdefault(op_resource_id, {})
                        trait_map = d.setdefault("traits", {})
                        entry = trait_map.setdefault(op_trait_label, {"trait_type": "", "fields": {}})
                        entry["operations"] = operations

                elif of == 5 and isinstance(ov, bytes):
                    meta = devices.setdefault("__observe_meta__", {})
                    meta["current_time"] = _decode_timestamp_fields(ov)

                elif of == 3 and isinstance(ov, bytes):
                    # TraitState
                    resource_id = ""
                    trait_label = ""
                    type_url = ""
                    trait_bytes = b""
                    monotonic_version: Optional[int] = None
                    publisher_version: Optional[int] = None

                    # Prefer generated protobuf decoding for TraitState/Patch/Any.
                    if nest_gateway_pb2 is not None:
                        try:
                            trait_state = nest_gateway_pb2.TraitState()
                            trait_state.ParseFromString(ov)
                            resource_id = trait_state.trait_id.resource_id
                            trait_label = trait_state.trait_id.trait_label
                            if trait_state.patch.values:
                                any_value = trait_state.patch.values[0]
                                type_url = any_value.type_url or ""
                                trait_bytes = any_value.value or b""
                            monotonic_version = int(trait_state.monotonic_version)
                            publisher_version = int(trait_state.publisher_version)
                        except Exception:
                            pass

                    # Fallback to manual protobuf walking if generated decode failed.
                    if not resource_id or not trait_bytes:
                        for tf, tw, tv in _dpf(ov):
                            if tf == 1 and isinstance(tv, bytes):
                                # TraitId
                                for idf, idw, idv in _dpf(tv):
                                    if idf == 1 and isinstance(idv, bytes):
                                        resource_id = idv.decode("utf-8", "replace")
                                    elif idf == 2 and isinstance(idv, bytes):
                                        trait_label = idv.decode("utf-8", "replace")
                            elif tf == 3 and isinstance(tv, bytes):
                                # Patch { Any values = 1 }
                                for pf, pw, pv in _dpf(tv):
                                    if pf == 1 and isinstance(pv, bytes):
                                        # Any { type_url = 1, value = 2 }
                                        for af, aw, av in _dpf(pv):
                                            if af == 1 and isinstance(av, bytes):
                                                type_url = av.decode("utf-8", "replace")
                                            elif af == 2 and isinstance(av, bytes):
                                                trait_bytes = av
                            elif tf == 4 and tw == 0:
                                monotonic_version = int(tv)
                            elif tf == 5 and tw == 0:
                                publisher_version = int(tv)

                    if not resource_id or not trait_bytes:
                        continue

                    d = devices.setdefault(resource_id, {})
                    trait_map = d.setdefault("traits", {})

                    # Some Observe TraitState updates omit Any.type_url and rely on
                    # ResourceMeta trait type. Fall back to the known trait metadata
                    # so generated-descriptor decoding still works.
                    meta_type_url = ""
                    if trait_label:
                        existing_entry = trait_map.get(trait_label)
                        if isinstance(existing_entry, dict):
                            meta_type_url = str(existing_entry.get("trait_type", "") or "")
                    effective_type_url = type_url or meta_type_url

                    generated_decoded = _decode_with_generated_proto(effective_type_url, trait_bytes)
                    generated_decode_ok = generated_decoded is not None
                    decoded_trait: Dict[str, Any] = generated_decoded or {}
                    decode_source = "generated" if generated_decode_ok else "none"
                    trait_descriptor = _get_message_descriptor_from_type_url(effective_type_url)

                    if "TemperatureTrait" in effective_type_url:
                        c = _read_temperature_trait(trait_bytes)
                        if c is not None:
                            decoded_trait = {"value_c": round(c, 3)}
                            if not generated_decode_ok:
                                decode_source = "fallback:temperature_trait"

                    elif "HumidityTrait" in effective_type_url:
                        h = _read_humidity_trait(trait_bytes)
                        if h is not None:
                            decoded_trait = {"value_pct": round(h, 1)}
                            if not generated_decode_ok:
                                decode_source = "fallback:humidity_trait"

                    elif "CarbonMonoxideTrait" in effective_type_url:
                        co = _read_carbon_monoxide_trait(trait_bytes)
                        if co is not None:
                            decoded_trait = {"value_ppm": round(co, 1)}
                            if not generated_decode_ok:
                                decode_source = "fallback:carbon_monoxide_trait"

                    elif "SmokeTrait" in effective_type_url:
                        smoke = _read_smoke_trait(trait_bytes)
                        if smoke is not None:
                            decoded_trait = {"value": round(smoke, 1)}
                            if not generated_decode_ok:
                                decode_source = "fallback:smoke_trait"

                    elif "BatteryVoltageTrait" in effective_type_url:
                        bv = _read_battery_voltage_trait(trait_bytes)
                        if bv is not None:
                            decoded_trait = {"battery_voltage_v": round(bv, 2)}
                            if not generated_decode_ok:
                                decode_source = "fallback:battery_voltage_trait"

                    elif "EcoModeStateTrait" in effective_type_url:
                        for ef, ew, ev in _dpf(trait_bytes):
                            if ef == 1 and ew == 0:
                                decoded_trait["mode"] = _ECO_MODE.get(ev, f"unknown({ev})")
                        if not generated_decode_ok and decoded_trait:
                            decode_source = "fallback:eco_mode_state_trait"

                    elif "EcoModeTrait" in effective_type_url:
                        # EcoModeTrait { Temperature current_eco_temperature_heat = 1;
                        #                Temperature current_eco_temperature_cool = 2 }
                        # Temperature { float value = 1 } (wire type 5)
                        for ef, ew, ev in _dpf(trait_bytes):
                            if isinstance(ev, bytes):
                                fv = _read_float_value(ev)
                                if fv is not None:
                                    if ef == 1:
                                        decoded_trait["current_eco_temperature_heat_c"] = round(fv, 2)
                                    elif ef == 2:
                                        decoded_trait["current_eco_temperature_cool_c"] = round(fv, 2)
                        if not generated_decode_ok and decoded_trait:
                            decode_source = "fallback:eco_mode_trait"

                    elif "HvacControlTrait" in effective_type_url:
                        decoded_trait = _read_hvac_control_trait(trait_bytes)
                        if not generated_decode_ok and decoded_trait:
                            decode_source = "fallback:hvac_control_trait"

                    elif "FanControlTrait" in effective_type_url:
                        decoded_trait = _read_fan_control_trait(trait_bytes)
                        if not generated_decode_ok and decoded_trait:
                            decode_source = "fallback:fan_control_trait"

                    elif "FanControlSettingsTrait" in effective_type_url:
                        decoded_trait = _read_fan_control_settings_trait(trait_bytes)
                        if not generated_decode_ok and decoded_trait:
                            decode_source = "fallback:fan_control_settings_trait"

                    elif "TargetTemperatureSettingsTrait" in effective_type_url:
                        decoded_trait = _read_target_temperature_trait(trait_bytes)
                        if not generated_decode_ok and decoded_trait:
                            decode_source = "fallback:target_temperature_settings_trait"

                    else:
                        if not decoded_trait:
                            decoded_trait = _decode_generic_proto(
                                trait_bytes,
                                descriptor=trait_descriptor,
                            )
                            if not generated_decode_ok and decoded_trait:
                                decode_source = "fallback:generic_proto"

                    if decoded_trait and not generated_decode_ok:
                        log.error(
                            "Fallback decoder used trait_label=%s trait_type=%s resource_id=%s source=%s raw_bytes_hex=%s",
                            trait_label,
                            effective_type_url or "",
                            resource_id,
                            decode_source,
                            trait_bytes.hex(),
                        )

                    if decoded_trait:
                        entry = trait_map.setdefault(trait_label, {"trait_type": "", "fields": {}})
                        if effective_type_url:
                            entry["trait_type"] = effective_type_url
                        entry["fields"] = decoded_trait
                        if trait_label == "structure_mode":
                            occ = decoded_trait.get("occupancy") if isinstance(decoded_trait, dict) else None
                            occ_presence = occ.get("presence") if isinstance(occ, dict) else None
                            occ_activity = occ.get("activity") if isinstance(occ, dict) else None
                            log.debug(
                                "observe structure_mode decoded resource=%s mode=%s reason=%s presence=%s activity=%s source=%s",
                                resource_id,
                                decoded_trait.get("structureMode") if isinstance(decoded_trait, dict) else None,
                                decoded_trait.get("structureModeReason") if isinstance(decoded_trait, dict) else None,
                                occ_presence,
                                occ_activity,
                                decode_source,
                            )
                    elif include_empty_traits:
                        entry = trait_map.setdefault(trait_label, {"trait_type": "", "fields": {}})
                        if effective_type_url:
                            entry["trait_type"] = effective_type_url
                        entry.setdefault("fields", {})

                    if trait_label in trait_map:
                        state_meta = trait_map[trait_label].setdefault("state_meta", {})
                        state_meta["decoded_at"] = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
                        if monotonic_version is not None:
                            state_meta["monotonic_version"] = monotonic_version
                        if publisher_version is not None:
                            state_meta["publisher_version"] = publisher_version

    except Exception as e:
        log.warning("parse_observe_response: failed to parse %d bytes: %s", len(stream_body_bytes), e)
        return {}

    return devices


# ── Long-lived streaming Observe ─────────────────────────────────────────────

def observe_stream(
    traits: List[str],
    nest_jwt: str,
    connection_timeout_sec: float = 120.0,
) -> "Generator[Dict[str, Dict[str, Any]], None, None]":
    """
    Generator that opens a persistent Observe connection and yields parsed
    state snapshots as they arrive.

    Each yielded value is the same format as ``parse_observe_response()`` —
    a ``{device_id: {field: value, ...}}`` dict containing only the traits
    present in that frame.

    The caller is responsible for reconnect logic.  Typical usage in a daemon::

        while running:
            try:
                for snapshot in observe_stream(traits, jwt, timeout_sec=120):
                    with lock:
                        deep_merge(state, snapshot)
            except Exception as exc:
                log.warning("Observe disconnected: %s, reconnecting …", exc)
                time.sleep(5)

    The function raises ``requests.exceptions.ReadTimeout`` if no data arrives
    within *connection_timeout_sec*, allowing the caller to reconnect cleanly.
    Pass ``0`` (or a negative value) to disable read timeout for quiet periods.
    """
    import base64 as _b64
    from types import GeneratorType

    _B64_ALPHABET = set(b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=")

    def _looks_like_base64_text(data: bytes) -> bool:
        """Return True when bytes look like base64 text, not raw binary."""
        return bool(data) and all(b in _B64_ALPHABET for b in data)

    def _filtered_base64(data: bytes) -> bytes:
        """Keep only base64 alphabet bytes from mixed stream buffers."""
        if not data:
            return b""
        return bytes(b for b in data if b in _B64_ALPHABET)

    def _deep_merge_snapshot(target: Dict[str, Dict[str, Any]], source: Dict[str, Dict[str, Any]]) -> None:
        for rid, src in source.items():
            dst = target.setdefault(rid, {})
            for k, v in src.items():
                if isinstance(v, dict) and isinstance(dst.get(k), dict):
                    _deep_merge_snapshot(dst[k], v)  # type: ignore[arg-type]
                else:
                    dst[k] = v

    def _snapshot_has_trait_fields(snapshot: Dict[str, Dict[str, Any]]) -> bool:
        for rid, dev in snapshot.items():
            if rid == "__observe_meta__":
                continue
            traits = dev.get("traits", {}) if isinstance(dev, dict) else {}
            if not isinstance(traits, dict):
                continue
            for trait in traits.values():
                if isinstance(trait, dict) and isinstance(trait.get("fields"), dict) and trait.get("fields"):
                    return True
        return False

    def _is_plausible_streambody_start(data: bytes, offset: int = 0) -> bool:
        """Fast guard to avoid parsing arbitrary bytes as StreamBody candidates."""
        if offset >= len(data):
            return False
        tag = data[offset]
        if tag not in (0x0A, 0x12, 0x7A):
            return False
        try:
            field_len, field_len_end = _decode_varint(data, offset + 1)
        except Exception:
            return False
        if field_len < 0:
            return False
        payload_start = field_len_end
        payload_end = payload_start + field_len
        if payload_end > len(data):
            return False
        if payload_start >= payload_end:
            return False
        return True

    def _parse_observe_payload(payload: bytes) -> Dict[str, Dict[str, Any]]:
        """
        Parse Observe payload bytes.
        Handles either raw StreamBody protobuf bytes or gRPC-Web framed bytes,
        including multiple concatenated frames in one payload.
        """
        if not payload:
            return {}

        if len(payload) >= 5 and payload[0] in (0x00, 0x80):
            pos = 0
            merged: Dict[str, Dict[str, Any]] = {}
            saw_frame = False
            while pos + 5 <= len(payload):
                flag = payload[pos]
                frame_len = int.from_bytes(payload[pos + 1:pos + 5], "big")
                if frame_len > 500_000 or pos + 5 + frame_len > len(payload):
                    break
                frame_payload = payload[pos + 5:pos + 5 + frame_len]
                pos += 5 + frame_len
                saw_frame = True
                if flag == 0x00:
                    snap = parse_observe_response(
                        frame_payload,
                        include_empty_traits=True,
                        warn_unknown_wire_type=False,
                    )
                    if snap:
                        _deep_merge_snapshot(merged, snap)
            if saw_frame:
                return merged

        # Raw StreamBody protobuf should usually begin with field tag 1 (0x0a)
        # and occasionally status/noop tags (0x12 / 0x7a).
        if payload[0] in (0x0A, 0x12, 0x7A):
                if not _is_plausible_streambody_start(payload, 0):
                    return {}
                snap = parse_observe_response(
                    payload,
                    include_empty_traits=True,
                    warn_unknown_wire_type=False,
                )
                if snap and _snapshot_has_trait_fields(snap):
                    return snap

                # Metadata-only or empty result: the server may concatenate multiple
                # StreamBody messages back-to-back, OR a chunk boundary may fall inside
                # a NestMessage field making the first parse incomplete.
                # Strategy: skip past the first length-delimited field(s) in the outer
                # StreamBody and try parsing the remainder as another StreamBody.
                if len(payload) > 100 and payload[0] == 0x0A:
                    pos = 0
                    # Skip through each outer field, trying to parse the tail as a new StreamBody
                    while pos < len(payload) - 10:
                        if payload[pos] not in (0x0A, 0x12, 0x7A):
                            # Skip to next plausible StreamBody start byte
                            next_start = -1
                            for i in range(pos, min(pos + 8192, len(payload))):
                                if payload[i] in (0x0A, 0x12, 0x7A):
                                    next_start = i
                                    break
                            if next_start < 0:
                                break
                            pos = next_start
                        tag_byte = payload[pos]
                        if tag_byte not in (0x0A, 0x12, 0x7A):
                            break
                        try:
                            field_len, field_len_end = _decode_varint(payload, pos + 1)
                        except Exception:
                            break
                        if field_len > len(payload):
                            # Truncated field — the data continues in future chunks.
                            # Try parsing the tail anyway; it may yield partial trait data.
                            snap2 = parse_observe_response(
                                payload[pos:],
                                include_empty_traits=True,
                                warn_unknown_wire_type=False,
                            )
                            if snap2 and _snapshot_has_trait_fields(snap2):
                                merged: Dict[str, Any] = {}
                                if snap:
                                    _deep_merge_snapshot(merged, snap)
                                _deep_merge_snapshot(merged, snap2)
                                log.debug("observe_stream: found traits in truncated tail at +%d bytes", pos)
                                return merged
                            break
                        next_pos = field_len_end + field_len
                        # Try parsing from right after this field
                        if (
                            next_pos < len(payload)
                            and payload[next_pos] in (0x0A, 0x12, 0x7A)
                            and _is_plausible_streambody_start(payload, next_pos)
                        ):
                            snap2 = parse_observe_response(
                                payload[next_pos:],
                                include_empty_traits=True,
                                warn_unknown_wire_type=False,
                            )
                            if snap2 and _snapshot_has_trait_fields(snap2):
                                merged = {}
                                if snap:
                                    _deep_merge_snapshot(merged, snap)
                                _deep_merge_snapshot(merged, snap2)
                                log.debug("observe_stream: found traits in StreamBody tail at +%d bytes", next_pos)
                                return merged
                        pos = next_pos

                    # Fallback: full payload scan for additional StreamBody starts.
                    # Some responses include long metadata/noop spans before the
                    # first trait-bearing segment; local stepping can miss those.
                    for offset in range(1, len(payload) - 4):
                        if payload[offset] not in (0x0A, 0x12, 0x7A):
                            continue
                        if not _is_plausible_streambody_start(payload, offset):
                            continue
                        snap2 = parse_observe_response(
                            payload[offset:],
                            include_empty_traits=True,
                            warn_unknown_wire_type=False,
                        )
                        if snap2 and _snapshot_has_trait_fields(snap2):
                            merged = {}
                            if snap:
                                _deep_merge_snapshot(merged, snap)
                            _deep_merge_snapshot(merged, snap2)
                            log.debug("observe_stream: recovered traits via full resync scan at +%d bytes", offset)
                            return merged
                return snap or {}

        # Resync heuristic: base64 chunk boundaries can misalign by 1-3 bytes,
        # producing decoded blobs that start mid-varint (example: starts with
        # 0x82 0x01 0x0a ... where the 0x0a tag was shifted). Scan a few bytes
        # forward for a plausible StreamBody start and decode from there.
        for offset in range(1, min(4, len(payload))):
            if payload[offset] in (0x0A, 0x12, 0x7A):
                if not _is_plausible_streambody_start(payload, offset):
                    continue
                snap = parse_observe_response(
                    payload[offset:],
                    include_empty_traits=True,
                    warn_unknown_wire_type=False,
                )
                if snap:
                    log.debug("observe_stream: resynced payload at +%d bytes", offset)
                    return snap

        return {}

    req = build_observe_request(traits)
    headers = {
        "Content-Type": "application/x-protobuf",
        "Accept": "application/x-protobuf.google.rpc.streambody",
        "Authorization": f"Basic {nest_jwt}",
        "Origin": "https://home.nest.com",
        "Referer": "https://home.nest.com/",
        "X-Accept-Response-Streaming": "true",
        "X-Accept-Content-Transfer-Encoding": "base64",
        "X-Nl-Webapp-Version": _WEBAPP_VERSION,
    }

    url = _GRPC_WEB_BASE + _OBSERVE_PATH
    read_timeout: Optional[float] = float(connection_timeout_sec)
    if read_timeout <= 0:
        read_timeout = None

    timeout_desc = "none" if read_timeout is None else f"{read_timeout:.0f}s"
    log.debug("observe_stream: connecting (%d traits, read-timeout=%s)",
              len(traits), timeout_desc)

    resp = requests.post(
        url, data=req, headers=headers,
        timeout=(10, read_timeout),
        stream=True,
    )
    if resp.status_code != 200:
        log.warning("observe_stream: HTTP %d", resp.status_code)
        return

    buf = b""
    decoded_buf = b""  # accumulates decoded bytes across base64 chunks
    pending_b64 = b""  # carry incomplete base64 quartets across chunks
    for chunk in resp.iter_content(chunk_size=None):
        if not chunk:
            continue
        
        # Update last stream activity timestamp
        import time as _time
        global last_stream_activity_ts
        last_stream_activity_ts = _time.time()

        buf += chunk

        # Strategy A: try decoding the newest chunk as base64 (most common case).
        # Also maintains a decoded_buf accumulator across chunks, because the server
        # may split a single large StreamBody proto across multiple base64 TCP chunks.
        # This handles the case where chunk N's decoded bytes start mid-varint from
        # the end of chunk N-1, making individual chunk decodes produce partial protos.
        chunk_clean = chunk.strip()
        filtered_chunk = _filtered_base64(chunk_clean)
        if filtered_chunk:
            try:
                # SDK parity: process incremental response-text deltas continuously.
                # Python chunk boundaries can split base64 quartets, so carry the
                # tail and only decode complete groups of 4 chars each iteration.
                pending_b64 += filtered_chunk
                dec_len = len(pending_b64) - (len(pending_b64) % 4)
                if dec_len <= 0:
                    continue
                b64_ready = pending_b64[:dec_len]
                pending_b64 = pending_b64[dec_len:]

                proto = _b64.b64decode(b64_ready, validate=False)
                if proto:
                    _text = "".join(chr(b) for b in proto if 0x20 <= b < 0x7F)
                    log.debug("observe_stream StratA: decoded %d bytes from %d-byte chunk; hex_prefix=%s text_prefix=%r",
                              len(proto), len(chunk), proto[:64].hex(), _text[:160])
                    if "authentication failed" in _text.lower():
                        raise ObserveAuthError("Observe rejected token: authentication failed")

                    snapshot = _parse_observe_payload(proto)
                    if snapshot and _snapshot_has_trait_fields(snapshot):
                        yield snapshot
                        buf = b""
                        decoded_buf = b""
                    else:
                        decoded_buf += proto
                        if len(decoded_buf) > len(proto):
                            combined_snapshot = _parse_observe_payload(decoded_buf)
                            if combined_snapshot and _snapshot_has_trait_fields(combined_snapshot):
                                log.debug("observe_stream StratA-combined: yielding snapshot from %d accumulated decoded bytes", len(decoded_buf))
                                yield combined_snapshot
                                buf = b""
                                decoded_buf = b""
            except ObserveAuthError:
                raise
            except Exception as _e:
                # SDK ignores incremental parse failures and waits for more bytes.
                msg = str(_e)
                if "Invalid byte length" not in msg:
                    log.debug("observe_stream StratA: b64decode failed: %s", _e)
        else:
            decoded_buf = b""  # non-base64 chunk — reset decoded accumulator
        # Strategy B: try to extract gRPC-Web frames from accumulated buffer
        # (raw binary frames, not base64-encoded).
        # gRPC-Web frame format: [1 byte flag][4 bytes length][N bytes payload]
        pos = 0
        frame_found = False
        binary_looks_like_text = False
        while pos + 5 <= len(buf):
            flag = buf[pos]
            try:
                length = int.from_bytes(buf[pos+1:pos+5], 'big')
            except (ValueError, struct.error):
                break  # malformed frame header
            if length > 100_000:  # sanity check — Observe responses shouldn't be huge
                # If the "length" bytes are printable ASCII, this is almost certainly
                # a base64/text response being misread as binary — don't warn repeatedly.
                header_bytes = buf[pos+1:pos+5]
                if all(0x20 <= b < 0x7F for b in header_bytes):
                    binary_looks_like_text = True
                else:
                    log.warning("observe_stream: suspiciously large frame (%d bytes) at pos %d, resyncing",
                               length, pos)
                break
            if pos + 5 + length > len(buf):
                break  # incomplete frame — wait for more data

            # Valid frame found
            payload = buf[pos+5:pos+5+length]
            pos += 5 + length
            frame_found = True

            try:
                snapshot = _parse_observe_payload(payload)
                if snapshot:
                    yield snapshot
            except Exception as e:
                log.warning("observe_stream: failed to parse gRPC-Web frame payload: %s", e)

        if frame_found:
            # Consumed some frames — keep remainder for next iteration
            buf = buf[pos:]
            continue

        # Strategy C: decode the full accumulated buffer as base64
        # (handles line-wrapped base64 messages, including when Strategy B detected
        # that the buffer looks like text/base64 rather than raw binary frames).
        buf_clean = buf.replace(b"\r", b"").replace(b"\n", b"")
        filtered_b64 = _filtered_base64(buf_clean)
        if filtered_b64:
            try:
                proto = _b64.b64decode(filtered_b64, validate=False)
                snapshot = _parse_observe_payload(proto)
                if snapshot:
                    if _snapshot_has_trait_fields(snapshot):
                        yield snapshot
                        buf = b""
                    else:
                        # Keep accumulating: this is often a metadata-only prefix
                        # and following chunks contain the actual trait state payloads.
                        log.debug("observe_stream StratC: metadata-only snapshot, buffering for continuation")
                elif binary_looks_like_text:
                    # Decoded OK but no data yet — clear so we don't re-parse old data
                    buf = b""
            except Exception:
                if binary_looks_like_text:
                    # Buffer is growing with text we can't parse as base64 yet — reset
                    # so we don't accumulate stale data that blocks future frames.
                    buf = b""
