"""Offline unit tests for home.google.com HAR extraction, compiled Foyer protobufs,
JSPB <-> Protobuf wire codec, SAPISIDHASH signing, token_store validation, and
collector compatibility."""

from __future__ import annotations

import base64
import hashlib
import json
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src import token_store
from src.collector import ThermostatCollector
from src.foyer_client import (
    FoyerClient,
    _parse_hvac_stage_flags,
    build_sapisidhash_header,
    common_pb2,
    energy_pb2,
    home_control_pb2,
    home_occupancy_pb2,
    jspb_to_proto,
    proto_struct_to_dict,
    proto_to_jspb,
    structures_pb2,
    users_pb2,
)
from utilities.ff_har_tokens import extract_tokens


def test_sapisidhash_matches_spec() -> None:
    cookies = {
        "SAPISID": "test_sapisid_val",
        "__Secure-1PAPISID": "test_1p_val",
        "__Secure-3PAPISID": "test_3p_val",
    }
    ts = 1700000000
    origin = "https://home.google.com"
    hdr = build_sapisidhash_header(cookies, origin=origin, timestamp=ts)
    expected = " ".join([
        f"SAPISIDHASH {ts}_{hashlib.sha1(f'{ts} test_sapisid_val {origin}'.encode()).hexdigest()}",
        f"SAPISID1PHASH {ts}_{hashlib.sha1(f'{ts} test_1p_val {origin}'.encode()).hexdigest()}",
        f"SAPISID3PHASH {ts}_{hashlib.sha1(f'{ts} test_3p_val {origin}'.encode()).hexdigest()}",
    ])
    assert hdr == expected, f"Mismatch: {hdr} != {expected}"


def test_parse_thermostat_status_and_stages() -> None:
    # Modeled directly on live EnergyService/GetThermostatStatus response
    sample = [
        None,
        [1, ["1791838361", 470000000]],
        [],
        [1, 1, 4, ["900"]],
        None,
        None,
        [
            [20],
            [19.983902],
            None,
            1,
            [1, 2, 3, 5, 4],
            1,
            None,
            2,
            None,
            [
                None,
                None,
                "Alt. heat stage 1",
                None,
                None,
                None,
                20,
                None,
                [
                    ["Heating", None, ["Heating to maintain 68°"], 1],
                    ["Alt. Heat", None, ["Using alternate heating"], 4],
                ],
            ],
            [43],
            "00000000-0000-4000-8000-000000000001",
            None,
            None,
            [[18.873901], [28.87999]],
            [],
            None,
            None,
            1,
            None,
            [[-9999], [-9999]],
            None,
            [["00000000-0000-4000-8000-000000000002"]],
            [],
            [20.829987],
        ],
    ]

    # 1. Verify direct compiled protobuf decoding via jspb_to_proto
    pb_msg = jspb_to_proto(sample, energy_pb2.GetThermostatStatusResponse)
    assert pb_msg.connectivity.state == 1
    assert pb_msg.connectivity.last_updated.seconds == 1791838361
    assert pb_msg.connectivity.last_updated.nanos == 470000000
    assert pb_msg.fan_settings.timer_duration.seconds == 900
    ts = pb_msg.thermostat_status
    assert ts.mode == energy_pb2.THERMOSTAT_MODE_HEAT
    assert ts.preferred_temperature_unit == energy_pb2.PREFERRED_TEMPERATURE_UNIT_FAHRENHEIT
    assert ts.status_display.stage_label == "Alt. heat stage 1"
    assert ts.status_display.mode_icon == 20
    assert len(ts.status_display.badges) == 2
    assert ts.status_display.badges[1].title == "Alt. Heat"
    assert ts.status_display.badges[1].badge_type == 4
    assert ts.active_sensors[0].sensor_id == "00000000-0000-4000-8000-000000000002"
    assert not ts.HasField("cooling_target")
    assert not ts.HasField("fan_control_settings")

    # 2. Verify binary protobuf round-trip produces identical parsed output
    wire_bytes = pb_msg.SerializeToString()
    parsed_jspb = FoyerClient.parse_thermostat_status(sample)
    parsed_bin = FoyerClient.parse_thermostat_status(wire_bytes)
    assert parsed_jspb == parsed_bin

    assert parsed_jspb["current_temp_c"] == 20.0
    assert parsed_jspb["backplate_temp_c"] == 20.83
    assert parsed_jspb["humidity_pct"] == 43.0
    assert parsed_jspb["heating_target_c"] == 19.98
    assert parsed_jspb["cooling_target_c"] is None
    assert parsed_jspb["setpoint_type"] == "heat"
    assert parsed_jspb["preferred_temperature_unit"] == "PREFERRED_TEMPERATURE_UNIT_FAHRENHEIT"
    assert parsed_jspb["eco_heat_c"] == 18.87
    assert parsed_jspb["eco_cool_c"] == 28.88
    hvc = parsed_jspb["hvac_control"]
    assert hvc["alt_heat_stage1_active"] is True
    assert hvc["heat_stage1_active"] is False
    assert hvc["cool_stage1_active"] is False

    # Verify other stage strings and idle passive labels ("Heat set to" / "Cool set to")
    s2 = _parse_hvac_stage_flags("Heat stage 2", [])
    assert s2["heat_stage2_active"] is True and s2["heat_stage1_active"] is False
    c2 = _parse_hvac_stage_flags("Cool stage 2", [])
    assert c2["cool_stage2_active"] is True and c2["cool_stage1_active"] is False
    idle = _parse_hvac_stage_flags("", [], active_mode_str="none")
    assert not any(idle.values())
    idle_heat_set_to = _parse_hvac_stage_flags("Heat set to", [("", 2)], active_mode_str="none")
    assert not any(idle_heat_set_to.values())
    idle_heat_set_to_no_traits = _parse_hvac_stage_flags("Heat set to", [("", 2)])
    assert not any(idle_heat_set_to_no_traits.values())
    idle_cool_set_to = _parse_hvac_stage_flags("Cool set to", [("", 2)])
    assert not any(idle_cool_set_to.values())


def test_parse_get_traits_and_collector_info_parity() -> None:
    sample_traits = [
        [
            [
                ["00000000-0000-4000-8000-000000000001"],
                [
                    [
                        "temperatureSetting",
                        [
                            ["ambientAirTemperatureC", [None, 19.5]],
                            ["backplateAmbientAirTemperatureC", [None, 21]],
                            ["ambientAirHumidity", [None, 0.45]],
                            ["mode", [None, None, "heat"]],
                            ["activeThermostatMode", [None, None, "heat"]],
                            ["thermostatTemperatureSetpointC", [None, 20]],
                            ["isOnline", [None, None, None, 1]],
                            ["nested", [None, None, None, None, [[["k", [None, 42]]]]]],
                            ["items", [None, None, None, None, None, [[[None, None, "a"], [None, 2]]]]],
                        ],
                        [[["queryOnlyTemperatureSetting", [None, None, None, 0]]]],
                    ],
                    [
                        "fanSpeed",
                        [["currentFanSpeedSetting", [None, None, "S1"]]],
                    ],
                ],
            ]
        ]
    ]
    pb_traits = jspb_to_proto(sample_traits, home_control_pb2.GetTraitsResponse)
    attrs = proto_struct_to_dict(pb_traits.device_traits[0].traits[0].attributes)
    assert attrs == {"queryOnlyTemperatureSetting": False}

    # Verify both JSPB and binary round-trip decode identically
    parsed = FoyerClient.parse_get_traits(sample_traits)
    parsed_bin = FoyerClient.parse_get_traits(pb_traits.SerializeToString())
    assert parsed == parsed_bin

    t = parsed["00000000-0000-4000-8000-000000000001"]
    assert t["temperatureSetting"]["ambientAirTemperatureC"] == 19.5
    assert t["temperatureSetting"]["isOnline"] is True
    assert t["temperatureSetting"]["nested"] == {"k": 42}
    assert t["temperatureSetting"]["items"] == ["a", 2]
    assert t["fanSpeed"]["currentFanSpeedSetting"] == "S1"

    # Verify ThermostatCollector._info_from_device consumes Foyer snapshot cleanly
    device = {
        "resource_type": "nest.resource.NestLearningThermostat3Resource",
        "traits": {
            "hvac_control": {"fields": _parse_hvac_stage_flags("Alt. heat stage 1", [])},
            "fan_control": {"fields": {"current_speed": "stage1"}},
            "target_temperature_settings": {"fields": {"setpoint_type": "heat", "heating_target_c": 20.0}},
            "structure_mode": {"fields": {"structureMode": "STRUCTURE_MODE_AWAY"}},
            "current_temperature": {"fields": {"value_c": 19.984}},
            "backplate_temperature": {"fields": {"value_c": 20.83}},
            "current_humidity": {"fields": {"value_pct": 43.0}},
        },
    }
    info = ThermostatCollector._info_from_device("DEVICE_TEST", device, "Entryway", 1700000000.0)
    assert info["heating"] == 1
    assert info["heat_alt"] == 1
    assert info["heat_stage1"] == 0
    assert info["fan"] == 1
    assert info["away"] == 1
    assert info["current_temp_c"] == 19.984
    assert info["backplate_temp_c"] == 20.83
    assert info["target_temp_c"] == 20.0
    assert info["humidity"] == 43.0


def test_foyer_other_rpcs_and_proto_to_jspb() -> None:
    # Request serialization (proto -> JSPB)
    assert proto_to_jspb(energy_pb2.GetThermostatStatusRequest(device_id="uuid-1")) == ["uuid-1"]
    assert proto_to_jspb(home_occupancy_pb2.GetStructureModeRequest(structure_id="s-1")) == ["s-1"]
    assert proto_to_jspb(
        home_control_pb2.GetTraitsRequest(device_ids=[common_pb2.DeviceId(uuid="u1"), common_pb2.DeviceId(uuid="u2")])
    ) == [[["u1"], ["u2"]]]
    assert proto_to_jspb(users_pb2.GetNestAccountLinkStateRequest()) == []

    # Structure mode decoding (JSPB + binary)
    assert FoyerClient.parse_structure_mode([None, None, 2, []]) == "STRUCTURE_MODE_HOME"
    assert FoyerClient.parse_structure_mode([None, None, 1, []]) == "STRUCTURE_MODE_AWAY"
    away_pb = home_occupancy_pb2.GetStructureModeResponse(mode=home_occupancy_pb2.FOYER_STRUCTURE_MODE_VACATION)
    assert FoyerClient.parse_structure_mode(away_pb.SerializeToString()) == "STRUCTURE_MODE_AWAY"

    # Link state + Olive token decoding
    link = FoyerClient.parse_nest_account_link_state([2, ["user.123456", "USER_0123456789ABCDEF"]])
    assert link == {"user": "user.123456", "userid": "123456", "nest_user_id": "USER_0123456789ABCDEF"}

    tok, exp = FoyerClient.parse_olive_token([["g.0.tok", ["299", 500000000], None, "nest-phoenix-prod"]], now=1000.0)
    assert tok == "g.0.tok"
    assert abs(exp - 1299.5) < 1e-6

    # Wiring configuration decoding
    wiring = FoyerClient.parse_wiring_configuration([[["W", "heat", 1, 1], ["Y", "cool", 1, 2], ["G", "fan", 1, 6]]])
    assert len(wiring) == 3
    assert wiring[0]["label"] == "W" and wiring[0]["connected"] is True and wiring[0]["terminal_type"] == 1

    # HomeGraph decoding (including ExternalDeviceIds PHX field 39)
    dev = [None] * 39
    dev[0] = ["thermo-uuid-1", ["nest-home-assistant-prod", "z.2.2.ext"]]
    dev[3] = "Entryway"
    dev[5] = "action.devices.types.THERMOSTAT"
    dev[6] = ["action.devices.traits.TemperatureSetting"]
    dev[38] = [[["PHX", "DEVICE_0123456789ABCDEF"]]]

    struct = [None] * 7
    struct[0] = "struct-1"
    struct[1] = "Home"
    struct[5] = [["room-1", None, "Entryway", ["OTHER"], [[["thermo-uuid-1"]]]]]
    struct[6] = [dev]

    hg_raw = ["1700000000000", [struct], None, None, "HGID"]
    hg = FoyerClient._parse_home_graph(hg_raw)
    assert hg["structures"] == [{"structure_id": "struct-1", "name": "Home"}]
    assert hg["rooms"] == {"room-1": "Entryway"}
    d = hg["devices"]["thermo-uuid-1"]
    assert d["phoenix_id"] == "DEVICE_0123456789ABCDEF"
    assert d["room_name"] == "Entryway"

    # Binary round-trip of GetHomeGraphResponse
    hg_pb = jspb_to_proto(hg_raw, structures_pb2.GetHomeGraphResponse)
    assert FoyerClient._parse_home_graph(hg_pb.SerializeToString()) == hg


def test_real_har_foyer_proto_decoding_if_present() -> None:
    har_candidates = sorted(ROOT.glob("home.google.com*.har"))
    if not har_candidates:
        return
    har_path = har_candidates[-1]
    har = json.loads(har_path.read_text(encoding="utf-8"))
    checked = set()
    for entry in har.get("log", {}).get("entries", []):
        url = entry.get("request", {}).get("url", "")
        if entry.get("response", {}).get("status") != 200:
            continue
        content = entry.get("response", {}).get("content", {})
        txt = content.get("text", "")
        if not txt:
            continue
        if content.get("encoding") == "base64":
            txt = base64.b64decode(txt).decode("utf-8", errors="replace")
        try:
            payload = json.loads(txt)
        except Exception:
            continue
        if "StructuresService/GetHomeGraph" in url and "GetHomeGraph" not in checked:
            hg = FoyerClient._parse_home_graph(payload)
            assert hg["structures"], "Expected at least one structure in captured GetHomeGraph"
            thermos = [d for d in hg["devices"].values() if d["device_type"] == "action.devices.types.THERMOSTAT"]
            sensors = [d for d in hg["devices"].values() if d["device_type"] == "action.devices.types.SENSOR"]
            assert thermos and thermos[0]["phoenix_id"].startswith("DEVICE_")
            assert len(sensors) >= 1 and all(s["phoenix_id"].startswith("DEVICE_") for s in sensors)
            checked.add("GetHomeGraph")
        elif "HomeControlService/GetTraits" in url and "GetTraits" not in checked:
            tr = FoyerClient.parse_get_traits(payload)
            assert len(tr) >= 1
            checked.add("GetTraits")
        elif "UsersService/GetNestAccountLinkState" in url and "GetNestAccountLinkState" not in checked:
            link = FoyerClient.parse_nest_account_link_state(payload)
            assert link.get("user", "").startswith("user.")
            assert link.get("nest_user_id", "").startswith("USER_")
            checked.add("GetNestAccountLinkState")
    assert checked == {"GetHomeGraph", "GetTraits", "GetNestAccountLinkState"}


def test_token_store_valid_without_transport_url() -> None:
    toks = {
        "access_token": "jwt_abc",
        "transport_url": "",
        "cache_expiration": time.time() + 1800,
    }
    assert token_store.is_valid(toks, margin_sec=300) is True
    assert token_store.is_valid(toks, margin_sec=300, require_transport_url=True) is False


def test_har_extractor_home_google_mode() -> None:
    link_b64 = base64.b64encode(b'[2,["user.99999","USER_AABBCC"]]').decode("ascii")
    har = {
        "log": {
            "entries": [
                {
                    "startedDateTime": "2026-01-01T12:01:00Z",
                    "request": {
                        "method": "POST",
                        "url": "https://accounts.google.com/RotateCookies",
                        "headers": [
                            {"name": "Cookie", "value": "SID=s1; LSID=l1; SAPISID=ap1; SIDCC=old_cc"},
                            {"name": "Referer", "value": "https://accounts.google.com/RotateCookiesPage?og_pid=621"},
                        ],
                        "postData": {"text": '[621,"-12345"]'},
                    },
                    "response": {
                        "status": 200,
                        "headers": [
                            {"name": "Set-Cookie", "value": "__Secure-1PSIDTS=ts1; Path=/; Domain=.google.com"},
                            {"name": "Set-Cookie", "value": "SIDCC=new_cc; Path=/; Domain=.google.com"},
                        ],
                        "content": {"text": ')]}\'\n\n[["identity.hfcr",600]]'},
                    },
                },
                {
                    "startedDateTime": "2026-01-01T12:02:00Z",
                    "request": {
                        "method": "POST",
                        "url": "https://googlehomefoyer-pa.clients6.google.com/$rpc/google.internal.home.foyer.v1.UsersService/GetNestAccountLinkState",
                        "headers": [
                            {"name": "Cookie", "value": "SID=s1; SAPISID=ap1; SIDCC=new_cc; __Secure-1PSIDTS=ts1"},
                            {"name": "X-Goog-Api-Key", "value": "AIzaTESTKEY"},
                            {"name": "X-Goog-AuthUser", "value": "0"},
                            {"name": "x-foyer-client-environment", "value": "ENV_VAL"},
                            {"name": "User-Agent", "value": "Firefox/161.0"},
                        ],
                    },
                    "response": {
                        "status": 200,
                        "headers": [
                            {"name": "Set-Cookie", "value": "SIDCC=foyer_cc; Path=/; Domain=.google.com"},
                        ],
                        "content": {"encoding": "base64", "text": link_b64},
                    },
                },
            ]
        }
    }
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "test.har"
        p.write_text(json.dumps(har), encoding="utf-8")
        out = extract_tokens(p)

    assert out["auth_mode"] == "home_google"
    assert out["userid"] == "99999"
    assert out["user"] == "user.99999"
    assert out["nest_user_id"] == "USER_AABBCC"
    assert out["foyer_api_key"] == "AIzaTESTKEY"
    assert out["rotate_cookies_body"] == '[621,"-12345"]'
    assert "SIDCC=foyer_cc" in out["foyer_cookie_header"]
    assert "LSID=l1" in out["google_cookie_header"]
    assert "SIDCC=foyer_cc" in out["google_cookie_header"]


def test_foyer_only_default_skips_observe_login() -> None:
    import src.config as cfg
    from src.nest_client import NestClient

    assert cfg.CONNECT_NEST_DATA_SOURCE == "foyer"
    with tempfile.TemporaryDirectory() as td:
        tok_path = Path(td) / "tokens.json"
        tok_path.write_text(
            json.dumps({
                "auth_mode": "home_google",
                "foyer_cookie_header": "SID=s1; SAPISID=ap1; __Secure-1PSID=p1",
                "google_cookie_header": "SID=s1; SAPISID=ap1; __Secure-1PSID=p1",
                "foyer_api_key": "AIzaTEST",
            }),
            encoding="utf-8",
        )
        # Must NOT attempt any network call / Observe JWT login when foyer_only=True
        client = NestClient(token_file=str(tok_path), foyer_only=True)
        assert client.foyer_client is not None
        assert client.access_token == ""


if __name__ == "__main__":
    test_sapisidhash_matches_spec()
    test_parse_thermostat_status_and_stages()
    test_parse_get_traits_and_collector_info_parity()
    test_foyer_other_rpcs_and_proto_to_jspb()
    test_real_har_foyer_proto_decoding_if_present()
    test_token_store_valid_without_transport_url()
    test_har_extractor_home_google_mode()
    test_foyer_only_default_skips_observe_login()
    print("All Foyer, compiled proto, and HAR tests PASSED.")
