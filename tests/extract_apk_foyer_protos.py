#!/usr/bin/env python3
"""Extract Google Home Foyer Protobuf-Lite schemas & enum tables from a Google Home APK/APKM.

Mirrors tests/sdk_extract_all.py for the new Google Home Android APK
(``com.google.android.apps.chromecast.app``). Locates gRPC MethodDescriptors for
``google.internal.home.foyer.v1.*`` services inside ``base.apk`` DEX files,
decodes Google Protobuf-Lite ``RawMessageInfo`` descriptors (field numbers, wire
types, nested message graphs), and dumps matching ``java.lang.Enum`` tables.

Usage:
  python tests/extract_apk_foyer_protos.py path/to/chromecast.apkm
  python tests/extract_apk_foyer_protos.py path/to/base.apk --rpc GetThermostatStatus GetStructureMode
  python tests/extract_apk_foyer_protos.py path/to/chromecast.apkm --enums
"""

from __future__ import annotations

import argparse
import io
import struct
import zipfile
from pathlib import Path
from typing import Any

# Dalvik opcode widths in 16-bit code units (DEX 035-039)
_OP_LEN = [1] * 256
for _op in (
    0x02, 0x05, 0x08, 0x13, 0x15, 0x16, 0x19, 0x1A, 0x1C, 0x1F, 0x20, 0x22, 0x23,
    0x29, 0x2D, 0x2E, 0x2F, 0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37,
    0x38, 0x39, 0x3A, 0x3B, 0x3C, 0x3D,
    *range(0x44, 0x6E),
    *range(0x90, 0xB0),
    *range(0xD0, 0xE3),
):
    _OP_LEN[_op] = 2
for _op in (
    0x03, 0x06, 0x09, 0x14, 0x17, 0x1B, 0x24, 0x25, 0x26, 0x2A, 0x2B, 0x2C,
    0x6E, 0x6F, 0x70, 0x71, 0x72, 0x74, 0x75, 0x76, 0x77, 0x78,
    0xFA, 0xFB, 0xFC, 0xFD,
):
    _OP_LEN[_op] = 3
_OP_LEN[0x18] = 5

_FIELD_TYPES = {
    0: "double", 1: "float", 2: "int64", 3: "uint64", 4: "int32", 5: "fixed64",
    6: "fixed32", 7: "bool", 8: "string", 9: "message", 10: "bytes", 11: "uint32",
    12: "enum", 13: "sfixed32", 14: "sfixed64", 15: "sint32", 16: "sint64", 17: "group",
    18: "repeated double", 19: "repeated float", 20: "repeated int64", 21: "repeated uint64",
    22: "repeated int32", 23: "repeated fixed64", 24: "repeated fixed32", 25: "repeated bool",
    26: "repeated string", 27: "repeated message", 28: "repeated bytes", 29: "repeated uint32",
    30: "repeated enum", 31: "repeated sfixed32", 32: "repeated sfixed64", 33: "repeated sint32",
    34: "repeated sint64", 35: "packed double", 36: "packed float", 37: "packed int64",
    38: "packed uint64", 39: "packed int32", 40: "packed fixed64", 41: "packed fixed32",
    42: "packed bool", 43: "packed uint32", 44: "packed enum", 45: "packed sfixed32",
    46: "packed sfixed64", 47: "packed sint32", 48: "packed sint64", 49: "repeated group",
    50: "map",
}

DEFAULT_RPCS = (
    "GetThermostatStatus",
    "GetWiringConfiguration",
    "GetStructureMode",
    "GetTraits",
    "GetHomeGraph",
    "GetNestAccountLinkState",
    "GetOliveToken",
)


def _read_uleb128(data: bytes, pos: int) -> tuple[int, int]:
    val, shift = 0, 0
    while True:
        b = data[pos]
        pos += 1
        val |= (b & 0x7F) << shift
        if not (b & 0x80):
            return val, pos
        shift += 7


def _decode_mutf8_to_u16(raw: bytes) -> list[int]:
    """Decode DEX Modified UTF-8 bytes into exact Java UTF-16 code units."""
    out: list[int] = []
    i, n = 0, len(raw)
    while i < n:
        b = raw[i]
        if b < 0x80:
            out.append(b)
            i += 1
        elif (b & 0xE0) == 0xC0 and i + 1 < n:
            out.append(((b & 0x1F) << 6) | (raw[i + 1] & 0x3F))
            i += 2
        elif (b & 0xF0) == 0xE0 and i + 2 < n:
            out.append(((b & 0x0F) << 12) | ((raw[i + 1] & 0x3F) << 6) | (raw[i + 2] & 0x3F))
            i += 3
        else:
            out.append(b)
            i += 1
    return out


def decode_raw_message_info(
    u16: list[int],
    objects: list[str],
    field_types: dict[str, str],
) -> tuple[str, list[tuple[int, str, str, str, list[str], int | None]]]:
    """Decode com.google.protobuf.RawMessageInfo UTF-16 char stream."""
    pos = 0

    def next_varint() -> int:
        nonlocal pos
        v = u16[pos]
        pos += 1
        if v < 0xD800:
            return v
        result = v & 0x1FFF
        shift = 13
        while True:
            c = u16[pos]
            pos += 1
            if c >= 0xD800:
                result |= (c & 0x1FFF) << shift
                shift += 13
            else:
                return result | (c << shift)

    flags = next_varint()
    is_proto2 = (flags & 1) != 0
    is_editions = (flags & 4) == 4
    syntax = "editions" if is_editions else ("proto2" if is_proto2 else "proto3")
    if pos >= len(u16):
        return syntax, []
    field_count = next_varint()
    if field_count == 0:
        return syntax, []

    oneof_count = next_varint()
    has_bits_count = next_varint()
    _min_fnum = next_varint()
    _max_fnum = next_varint()
    num_entries = next_varint()
    _map_cnt = next_varint()
    _rep_cnt = next_varint()
    _chk_init = next_varint()

    obj_idx = 2 * oneof_count + has_bits_count
    fields: list[tuple[int, str, str, str, list[str], int | None]] = []
    for _ in range(num_entries):
        fnum = next_varint()
        type_with_flags = next_varint()
        ftype_id = type_with_flags & 0xFF
        is_oneof = ftype_id >= 51
        base_id = (ftype_id - 51) if is_oneof else ftype_id
        ftype_str = _FIELD_TYPES.get(base_id, f"type_{base_id}")
        if is_oneof:
            ftype_str = f"oneof {ftype_str}"

        oneof_idx: int | None = None
        java_field = ""
        extra: list[str] = []
        if is_oneof:
            oneof_idx = next_varint()
            if base_id in (9, 17):
                if obj_idx < len(objects):
                    extra.append(objects[obj_idx])
                    obj_idx += 1
            elif base_id == 12 and (is_proto2 or (type_with_flags & 0x800)):
                if obj_idx < len(objects):
                    extra.append(objects[obj_idx])
                    obj_idx += 1
        else:
            if obj_idx < len(objects):
                java_field = objects[obj_idx]
                obj_idx += 1
            if base_id in (50, 27, 49):
                if obj_idx < len(objects):
                    extra.append(objects[obj_idx])
                    obj_idx += 1
            elif base_id in (12, 30, 44) and (is_proto2 or bool(type_with_flags & 0x800)):
                if obj_idx < len(objects):
                    extra.append(objects[obj_idx])
                    obj_idx += 1
            if base_id <= 17 and (is_proto2 or bool(type_with_flags & 0x1000)):
                _ = next_varint()

        jtype = field_types.get(java_field, "")
        fields.append((fnum, ftype_str, java_field, jtype, extra, oneof_idx))
    return syntax, fields


class DexIndex:
    """Minimal DEX table + bytecode reader for Protobuf-Lite & gRPC MethodDescriptor extraction."""

    def __init__(self, name: str, data: bytes) -> None:
        self.name = name
        self.data = data
        str_cnt, str_off = struct.unpack_from("<II", data, 56)
        typ_cnt, typ_off = struct.unpack_from("<II", data, 64)
        fld_cnt, fld_off = struct.unpack_from("<II", data, 80)
        mtd_cnt, mtd_off = struct.unpack_from("<II", data, 88)
        cls_cnt, cls_off = struct.unpack_from("<II", data, 96)

        self.strings: list[str] = []
        self.string_raw: list[bytes] = []
        for i in range(str_cnt):
            soff = struct.unpack_from("<I", data, str_off + i * 4)[0]
            _, p = _read_uleb128(data, soff)
            end = data.index(b"\x00", p)
            raw = data[p:end]
            self.string_raw.append(raw)
            self.strings.append(raw.decode("utf-8", errors="replace"))

        self.types = [
            self.strings[struct.unpack_from("<I", data, typ_off + i * 4)[0]]
            for i in range(typ_cnt)
        ]
        self.fields = [
            (self.types[cid], self.types[tid], self.strings[nid])
            for cid, tid, nid in (
                struct.unpack_from("<HHI", data, fld_off + i * 8) for i in range(fld_cnt)
            )
        ]
        self.methods = [
            (self.types[cid], pid, self.strings[nid])
            for cid, pid, nid in (
                struct.unpack_from("<HHI", data, mtd_off + i * 8) for i in range(mtd_cnt)
            )
        ]
        self.class_defs: dict[str, tuple[str, int]] = {}
        for i in range(cls_cnt):
            off = cls_off + i * 32
            cid, _acc, sup_id, _io, _so, _ao, cdata_off, _sv = struct.unpack_from("<IIIIIIII", data, off)
            sup = self.types[sup_id] if sup_id != 0xFFFFFFFF else ""
            self.class_defs[self.types[cid]] = (sup, cdata_off)

    def _iter_methods(self, cname: str):
        if cname not in self.class_defs:
            return
        _, cdata_off = self.class_defs[cname]
        if not cdata_off:
            return
        p = cdata_off
        sf, p = _read_uleb128(self.data, p)
        inf, p = _read_uleb128(self.data, p)
        dm, p = _read_uleb128(self.data, p)
        vm, p = _read_uleb128(self.data, p)
        for _ in range(sf + inf):
            _, p = _read_uleb128(self.data, p)
            _, p = _read_uleb128(self.data, p)
        for cnt in (dm, vm):
            mid = 0
            for _ in range(cnt):
                diff, p = _read_uleb128(self.data, p)
                mid += diff
                _, p = _read_uleb128(self.data, p)
                co, p = _read_uleb128(self.data, p)
                if co:
                    yield self.methods[mid][2], co

    def get_instance_fields(self, cname: str) -> dict[str, str]:
        out: dict[str, str] = {}
        if cname not in self.class_defs:
            return out
        _, cdata_off = self.class_defs[cname]
        if not cdata_off:
            return out
        p = cdata_off
        sf, p = _read_uleb128(self.data, p)
        inf, p = _read_uleb128(self.data, p)
        _dm, p = _read_uleb128(self.data, p)
        _vm, p = _read_uleb128(self.data, p)
        for _ in range(sf):
            _, p = _read_uleb128(self.data, p)
            _, p = _read_uleb128(self.data, p)
        fid = 0
        for _ in range(inf):
            diff, p = _read_uleb128(self.data, p)
            fid += diff
            _, p = _read_uleb128(self.data, p)
            out[self.fields[fid][2]] = self.fields[fid][1]
        return out

    def extract_message_info(self, cname: str) -> tuple[bytes, list[str], dict[str, str]] | None:
        for mname, code_off in self._iter_methods(cname):
            if mname != "dynamicMethod":
                continue
            sz = struct.unpack_from("<I", self.data, code_off + 12)[0]
            insns = struct.unpack_from(f"<{sz}H", self.data, code_off + 16)
            regs: dict[int, Any] = {}
            arr_items: dict[int, str] = {}
            info_raw: bytes | None = None
            i = 0
            while i < len(insns):
                w = insns[i]
                if w == 0x0100:
                    i += 4 + insns[i + 1] * 2
                    continue
                if w == 0x0200:
                    i += 2 + insns[i + 1] * 4
                    continue
                if w == 0x0300:
                    ew = insns[i + 1]
                    asz = insns[i + 2] | (insns[i + 3] << 16)
                    i += 4 + (asz * ew + 1) // 2
                    continue
                op = w & 0xFF
                if op == 0x12:
                    vA = (w >> 8) & 0xF
                    imm = (w >> 12) & 0xF
                    regs[vA] = imm - 16 if imm >= 8 else imm
                elif op == 0x13 and i + 1 < len(insns):
                    vA = (w >> 8) & 0xFF
                    regs[vA] = struct.unpack("<h", struct.pack("<H", insns[i + 1]))[0]
                elif op == 0x14 and i + 2 < len(insns):
                    vA = (w >> 8) & 0xFF
                    regs[vA] = insns[i + 1] | (insns[i + 2] << 16)
                elif op == 0x1A and i + 1 < len(insns):
                    vA = (w >> 8) & 0xFF
                    sid = insns[i + 1]
                    raw = self.string_raw[sid]
                    if (raw.startswith(b"\xc0\x80") or (raw and raw[0] in (0, 1, 2, 3, 4, 5))) and len(raw) >= 3:
                        info_raw = raw
                    regs[vA] = self.strings[sid]
                elif op == 0x1B and i + 2 < len(insns):
                    vA = (w >> 8) & 0xFF
                    sid = insns[i + 1] | (insns[i + 2] << 16)
                    raw = self.string_raw[sid]
                    if (raw.startswith(b"\xc0\x80") or (raw and raw[0] in (0, 1, 2, 3, 4, 5))) and len(raw) >= 3:
                        info_raw = raw
                    regs[vA] = self.strings[sid]
                elif op == 0x1C and i + 1 < len(insns):
                    vA = (w >> 8) & 0xFF
                    regs[vA] = f"cls:{self.types[insns[i + 1]]}"
                elif op == 0x62 and i + 1 < len(insns):
                    vA = (w >> 8) & 0xFF
                    f = self.fields[insns[i + 1]]
                    regs[vA] = f"sget:{f[0]}.{f[2]}:{f[1]}"
                elif op == 0x4D and i + 1 < len(insns):
                    vVal = (w >> 8) & 0xFF
                    vIdx = (insns[i + 1] >> 8) & 0xFF
                    idx_val = regs.get(vIdx)
                    val = regs.get(vVal)
                    if isinstance(idx_val, int) and isinstance(val, str):
                        arr_items[idx_val] = val
                i += _OP_LEN[op]
            if info_raw is not None:
                max_k = max(arr_items.keys(), default=-1)
                objs = [arr_items.get(k, "") for k in range(max_k + 1)]
                return info_raw, objs, self.get_instance_fields(cname)
        return None

    def find_foyer_rpcs(self, rpc_names: set[str]) -> dict[str, tuple[str, str, str]]:
        """Return {rpc_name: (service_name, request_cls, response_cls)}."""
        found: dict[str, tuple[str, str, str]] = {}
        present = {s for s in rpc_names if s in self.strings}
        if not present:
            return found
        target_sids = {i for i, s in enumerate(self.strings) if s in present}
        for cname in self.class_defs:
            for _mname, co in self._iter_methods(cname):
                sz = struct.unpack_from("<I", self.data, co + 12)[0]
                insns = struct.unpack_from(f"<{sz}H", self.data, co + 16)
                strs: list[str] = []
                sgets: list[str] = []
                hit: str | None = None
                i = 0
                while i < len(insns):
                    w = insns[i]
                    if w == 0x0100:
                        i += 4 + insns[i + 1] * 2
                        continue
                    if w == 0x0200:
                        i += 2 + insns[i + 1] * 4
                        continue
                    if w == 0x0300:
                        ew = insns[i + 1]
                        asz = insns[i + 2] | (insns[i + 3] << 16)
                        i += 4 + (asz * ew + 1) // 2
                        continue
                    op = w & 0xFF
                    if op == 0x1A and i + 1 < len(insns):
                        sid = insns[i + 1]
                        if sid in target_sids:
                            hit = self.strings[sid]
                        strs.append(self.strings[sid])
                    elif op == 0x62 and i + 1 < len(insns):
                        f = self.fields[insns[i + 1]]
                        if f[0] == f[1] and f[0].startswith(("Lay", "Laz")):
                            sgets.append(f[0])
                    i += _OP_LEN[op]
                if hit:
                    svcs = [s for s in strs if s.startswith("google.internal.home.foyer.")]
                    if svcs and len(sgets) >= 2:
                        found[hit] = (svcs[0], sgets[0], sgets[1])
        return found

    def extract_enum_entries(self, cname: str) -> list[tuple[str, int]]:
        if cname not in self.class_defs or self.class_defs[cname][0] != "Ljava/lang/Enum;":
            return []
        for mname, co in self._iter_methods(cname):
            if mname != "<clinit>":
                continue
            sz = struct.unpack_from("<I", self.data, co + 12)[0]
            insns = struct.unpack_from(f"<{sz}H", self.data, co + 16)
            regs: dict[int, Any] = {}
            entries: list[tuple[str, int]] = []
            i = 0
            while i < len(insns):
                w = insns[i]
                if w == 0x0100:
                    i += 4 + insns[i + 1] * 2
                    continue
                if w == 0x0200:
                    i += 2 + insns[i + 1] * 4
                    continue
                if w == 0x0300:
                    ew = insns[i + 1]
                    asz = insns[i + 2] | (insns[i + 3] << 16)
                    i += 4 + (asz * ew + 1) // 2
                    continue
                op = w & 0xFF
                if op == 0x12:
                    vA = (w >> 8) & 0xF
                    imm = (w >> 12) & 0xF
                    regs[vA] = imm - 16 if imm >= 8 else imm
                elif op == 0x13 and i + 1 < len(insns):
                    vA = (w >> 8) & 0xFF
                    regs[vA] = struct.unpack("<h", struct.pack("<H", insns[i + 1]))[0]
                elif op == 0x1A and i + 1 < len(insns):
                    vA = (w >> 8) & 0xFF
                    regs[vA] = self.strings[insns[i + 1]]
                elif op == 0x70 and i + 2 < len(insns):
                    m = self.methods[insns[i + 1]]
                    if m[0] == cname and m[2] == "<init>":
                        cnt_args = (w >> 12) & 0xF
                        rword = insns[i + 2]
                        arg_regs = [(rword >> (4 * k)) & 0xF for k in range(min(4, cnt_args))]
                        if cnt_args == 5:
                            arg_regs.append((w >> 8) & 0xF)
                        vals = [regs.get(r) for r in arg_regs[1:]]
                        if len(vals) >= 3 and isinstance(vals[0], str) and isinstance(vals[2], int):
                            entries.append((vals[0], vals[2]))
                i += _OP_LEN[op]
            return entries
        return []


def load_apk_dexes(path: Path) -> tuple[list[DexIndex], dict[str, DexIndex]]:
    """Load all classes*.dex from an .apkm bundle or base.apk file."""
    with zipfile.ZipFile(path, "r") as zf:
        if "base.apk" in zf.namelist():
            apk_bytes = zf.read("base.apk")
            apk_zf = zipfile.ZipFile(io.BytesIO(apk_bytes), "r")
        else:
            apk_zf = zf
        dexes: list[DexIndex] = []
        class_to_dex: dict[str, DexIndex] = {}
        with apk_zf:
            for name in sorted(n for n in apk_zf.namelist() if n.endswith(".dex")):
                dex = DexIndex(name, apk_zf.read(name))
                dexes.append(dex)
                for cname in dex.class_defs:
                    class_to_dex[cname] = dex
    return dexes, class_to_dex


def dump_proto(
    class_to_dex: dict[str, DexIndex],
    cname: str,
    label: str,
    *,
    depth: int = 0,
    max_depth: int = 3,
    seen: set[str] | None = None,
) -> None:
    if seen is None:
        seen = set()
    indent = "  " * depth
    if cname in seen:
        print(f"{indent}{label} ({cname}) [already dumped]")
        return
    dex = class_to_dex.get(cname)
    if not dex:
        print(f"{indent}{label} ({cname}) [not found]")
        return
    res = dex.extract_message_info(cname)
    if not res:
        print(f"{indent}{label} ({cname}) [no RawMessageInfo]")
        return
    seen.add(cname)
    info_raw, objs, jfields = res
    u16 = _decode_mutf8_to_u16(info_raw)
    syntax, fields = decode_raw_message_info(u16, objs, jfields)
    print(f"{indent}message {label} ({cname}) [{syntax}, {len(fields)} fields]:")
    nested: list[tuple[str, str]] = []
    for fnum, ftype, jf, jt, extra, oneof_idx in fields:
        oneof_s = f" [oneof#{oneof_idx}]" if oneof_idx is not None else ""
        extra_s = f" extra={extra}" if extra else ""
        print(f"{indent}  field {fnum:2d}: {ftype:18s} {jf:4s} ({jt}){oneof_s}{extra_s}")
        sub_cls = None
        if "message" in ftype:
            if extra and extra[0].startswith("cls:"):
                sub_cls = extra[0][4:]
            elif jt.startswith("L") and not jt.startswith(("Ljava/", "Lcom/google/protobuf/")):
                sub_cls = jt
        if sub_cls and depth < max_depth:
            nested.append((sub_cls, f"f{fnum}_{sub_cls.strip('L;')}"))
    for sub_cls, sub_label in nested:
        dump_proto(class_to_dex, sub_cls, sub_label, depth=depth + 1, max_depth=max_depth, seen=seen)


def main() -> int:
    parser = argparse.ArgumentParser(description="Extract Foyer Protobuf-Lite schemas from Google Home APK/APKM")
    parser.add_argument("apk_path", type=Path, help="Path to Google Home .apkm or base.apk")
    parser.add_argument("--rpc", nargs="*", default=list(DEFAULT_RPCS), help="Foyer RPC method names to dump")
    parser.add_argument("--depth", type=int, default=3, help="Max nested message recursion depth")
    parser.add_argument("--enums", action="store_true", help="Also dump thermostat/HVAC/fan/unit Foyer enum tables")
    args = parser.parse_args()

    dexes, class_to_dex = load_apk_dexes(args.apk_path)
    print(f"Loaded {len(dexes)} DEX files ({len(class_to_dex)} classes) from {args.apk_path.name}")

    rpc_map: dict[str, tuple[str, str, str]] = {}
    for dex in dexes:
        rpc_map.update(dex.find_foyer_rpcs(set(args.rpc)))

    for rpc in args.rpc:
        if rpc not in rpc_map:
            print(f"\n=== RPC {rpc}: NOT FOUND ===")
            continue
        svc, req_cls, resp_cls = rpc_map[rpc]
        print(f"\n{'=' * 78}")
        print(f"RPC {svc}/{rpc}  (req={req_cls}, resp={resp_cls})")
        dump_proto(class_to_dex, req_cls, f"{rpc}Request", max_depth=min(2, args.depth))
        dump_proto(class_to_dex, resp_cls, f"{rpc}Response", max_depth=args.depth)

    if args.enums:
        print(f"\n{'=' * 78}")
        print("FOYER THERMOSTAT / HVAC / FAN / TEMPERATURE ENUMS:")
        keywords = ("THERMOSTAT_", "HVAC_", "FAN_", "PREFERRED_TEMPERATURE_", "TEMPERATURE_TRAIT_")
        for cname, dex in sorted(class_to_dex.items()):
            if not cname.startswith(("Lay", "Laz")):
                continue
            entries = dex.extract_enum_entries(cname)
            if entries and any(any(k in name for k in keywords) for name, _ in entries):
                print(f"\nenum {cname}:")
                for name, num in entries:
                    print(f"  {name} = {num};")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
