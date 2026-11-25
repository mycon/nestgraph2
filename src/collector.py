"""
Thermostat data collector with change-only, cadenced write policy.

Poll cadence (every FAST_POLL_INTERVAL_SECONDS, default 15 s):
  GET /state_min from the connect_nest daemon on every poll tick.
  The daemon manages the gRPC-Web observe stream and weather fetching;
  this collector only handles InfluxDB write decisions.

Write policy
------------
  State fields  (boolean HVAC bits, away flag):
      Written whenever any value changes.  No time gate.

  Analog fields (temperatures in °C, humidity):
      Written when any value changes AND at least ANALOG_MIN_INTERVAL_SECONDS
      (default 60 s) have elapsed since the last analog-triggered write.

  Weather (outdoor):
      Comes from the daemon's /state_min weather block on every poll.
      A standalone 'weather' measurement point is written only when
      outdoor_temp_c or outdoor_humidity differs from the last written values.
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


# ── Write-gate field sets ─────────────────────────────────────────────────────

# Written on any change — no time gate.
_STATE_FIELDS: frozenset[str] = frozenset({
    "heating", "cooling", "fan", "away",
    "heat_stage1", "heat_stage2", "alt_heat_stage1", "alt_heat_stage2",
    "cool_stage1", "cool_stage2", "humidifier",
})

# State keys that should emit duration events into the _event bucket.
_CYCLE_EVENT_KEYS: tuple[str, ...] = (
    "heating",
    "cooling",
    "fan",
    "humidifier",
    "heat_stage1",
    "heat_stage2",
    "alt_heat_stage1",
    "alt_heat_stage2",
    "cool_stage1",
    "cool_stage2",
    "away",
)

# Written on change, rate-limited to one write per ANALOG_MIN_INTERVAL_SECONDS.
# All temperature keys use the Celsius (_c) names as received from /state_min.
_ANALOG_KEYS: frozenset[str] = frozenset({
    "current_temp_c", "target_temp_c", "target_temp_high_c", "humidity",
})

# Weather keys compared when deciding whether to write a weather point.
_WEATHER_COMPARE_KEYS: tuple[str, ...] = ("outdoor_temp_c", "outdoor_humidity")


# ── Collector ─────────────────────────────────────────────────────────────────

class ThermostatCollector:
    def __init__(
        self,
        influx,
        analog_min_interval: int = 60,
        mapping_sync_interval: int = 300,
        label_short_id_len: int = 6,
        device_names: dict[str, str] | None = None,
    ) -> None:
        self._influx = influx
        self._analog_min_interval = analog_min_interval
        self._mapping_sync_interval = max(30, int(mapping_sync_interval))
        self._label_short_id_len = max(4, int(label_short_id_len))
        self._device_names = dict(device_names or {})

        # Per-device change-tracking state
        self._last_info: dict[str, dict] = {}             # serial → last info snapshot
        self._last_analog_write_ts: dict[str, float] = {} # serial → epoch of last analog write
        self._last_indoor: dict[str, dict] = {}           # serial → last indoor snapshot
        self._last_indoor_write_ts: dict[str, float] = {} # serial → epoch of last indoor write
        self._active_state_runs: dict[str, dict[str, float]] = {}  # serial -> state_key -> start epoch

        # Weather cache
        self._weather_cache: dict[str, Any] = {}
        self._last_weather_state: dict[str, Any] = {}     # last *written* weather values

        # Device->where mapping sync cache
        self._last_mapping_sync_ts: float = 0.0
        self._last_mapping_written: dict[str, tuple[str, str]] = {}

        # Device index registry — maps device_id → stable integer (dev_idx)
        # Persisted to config/device_index.json so indices survive restarts.
        self._idx_registry_path: Path = (
            Path(os.getenv("CONFIG_PATH", "config/config.json")).parent / "device_index.json"
        )
        json_registry = self._load_dev_idx_registry()
        db_registry = self._load_dev_idx_registry_from_db()
        self._db_dev_idx_registry: dict[str, int] = dict(db_registry)
        self._dev_idx_registry: dict[str, int] = self._merge_dev_idx_registries(db_registry, json_registry)
        if self._dev_idx_registry != json_registry:
            self._save_dev_idx_registry()
        self._next_dev_idx: int = max(self._dev_idx_registry.values(), default=-1) + 1
        self._log_dev_idx_startup_dump()



    @staticmethod
    def _device_label(device_id: str) -> str:
        return device_id

    @staticmethod
    def _device_label_short(device_name: str, device_id: str, short_len: int) -> str:
        if not device_id:
            return device_name or ""
        suffix_id = device_id[-short_len:]
        base = device_name or device_id
        suffix = f"({suffix_id})"
        if base.endswith(suffix):
            return base
        return f"{base}{suffix}"

    def _ensure_dev_idx_cached(
        self,
        device_id: str,
        device_name: str,
        device_label: str,
        device_label_short: str,
        resource_type: str,
        where_id: str = "",
    ) -> int:
        """Ensure device_id has a stable dev_idx in cache/DB and return it."""
        info = {
            "name": device_name or device_id,
            "device_label": device_label,
            "device_label_short": device_label_short,
            "resource_type": resource_type,
            "where_id": where_id,
        }
        return self._get_or_assign_dev_idx(device_id, info)

    # ── Device index registry ──────────────────────────────────────────────────

    def _load_dev_idx_registry(self) -> dict[str, int]:
        """Load the persistent device_id → dev_idx mapping from disk."""
        try:
            if self._idx_registry_path.exists():
                with self._idx_registry_path.open(encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    return {k: int(v) for k, v in data.items()}
        except Exception as exc:
            log.warning("Could not load device_index registry from %s: %s", self._idx_registry_path, exc)
        return {}

    def _save_dev_idx_registry(self) -> None:
        """Persist the device_id → dev_idx mapping to disk."""
        try:
            self._idx_registry_path.parent.mkdir(parents=True, exist_ok=True)
            with self._idx_registry_path.open("w", encoding="utf-8") as f:
                json.dump(self._dev_idx_registry, f, indent=2)
        except Exception as exc:
            log.warning("Could not save device_index registry to %s: %s", self._idx_registry_path, exc)

    def _log_dev_idx_startup_dump(self, max_rows: int = 50) -> None:
        """Emit a one-shot startup dump of cached device_id → dev_idx mappings."""
        if not self._dev_idx_registry:
            log.info("dev_idx startup cache: empty")
            return

        items = sorted(self._dev_idx_registry.items(), key=lambda kv: kv[1])
        shown = items[:max_rows]
        log.info(
            "dev_idx startup cache loaded: %d mappings (showing %d)",
            len(items),
            len(shown),
        )
        for device_id, dev_idx in shown:
            log.info("dev_idx map: %s -> %d", device_id, dev_idx)
        if len(items) > max_rows:
            log.info("dev_idx startup cache: %d additional mappings omitted", len(items) - max_rows)

    def _load_dev_idx_registry_from_db(self) -> dict[str, int]:
        """Load device_id → dev_idx from InfluxDB device_index measurement."""
        try:
            mapping = self._influx.read_device_index_map()
            if mapping:
                log.info("Loaded %d device_index mappings from InfluxDB", len(mapping))
            return mapping
        except Exception as exc:
            log.warning("Could not load device_index mapping from InfluxDB: %s", exc)
            return {}

    @staticmethod
    def _merge_dev_idx_registries(
        db_registry: dict[str, int],
        json_registry: dict[str, int],
    ) -> dict[str, int]:
        """
        Merge registries with DB as ground truth:
          1) DB mappings win when both contain a device_id
          2) JSON fills only device_ids not present in DB
        """
        merged: dict[str, int] = dict(db_registry)
        for device_id, dev_idx in json_registry.items():
            if device_id not in merged:
                merged[device_id] = dev_idx
        return merged

    def _get_or_assign_dev_idx(self, device_id: str, info: dict[str, Any]) -> int:
        """
        Return the stable integer index for device_id, assigning a new one if
        this is the first time we've seen the device.  Persists to disk and
        writes a device_index point to InfluxDB on first assignment.
        """
        if device_id in self._dev_idx_registry:
            idx = self._dev_idx_registry[device_id]
            # If registry came from JSON fallback only, upsert it into DB when seen.
            if device_id not in self._db_dev_idx_registry:
                self._influx.write_device_index(
                    device_id=device_id,
                    dev_idx=idx,
                    device_name=info.get("name", device_id),
                    device_label=info.get("device_label", device_id),
                    device_label_short=info.get("device_label_short", ""),
                    resource_type=info.get("resource_type", ""),
                    where_id=info.get("where_id", ""),
                )
                self._db_dev_idx_registry[device_id] = idx
            return idx

        idx = self._next_dev_idx
        self._dev_idx_registry[device_id] = idx
        self._next_dev_idx += 1
        self._save_dev_idx_registry()

        # Write the mapping to InfluxDB so it can be joined against indoor_sensor.
        self._influx.write_device_index(
            device_id=device_id,
            dev_idx=idx,
            device_name=info.get("name", device_id),
            device_label=info.get("device_label", device_id),
            device_label_short=info.get("device_label_short", ""),
            resource_type=info.get("resource_type", ""),
            where_id=info.get("where_id", ""),
        )
        self._db_dev_idx_registry[device_id] = idx
        log.info(
            "Assigned dev_idx=%d to %s (%s)",
            idx, device_id, info.get("name", device_id),
        )
        return idx



    # ── /state_min → canonical info dict ──────────────────────────────────────

    @staticmethod
    def _info_from_device(device_id: str, device: dict, device_name: str, ts: float) -> dict:
        """
        Map a /state_min device entry to the canonical info dict used by
        _process_device() and InfluxWriter.write_thermostat().

        All temperatures are in Celsius (received that way from the daemon).
        """
        def f(trait: str) -> dict:
            return device.get("traits", {}).get(trait, {}).get("fields", {})

        hvc  = f("hvac_control")
        fan  = f("fan_control")
        fan_settings = f("fan_control_settings")
        sm   = f("structure_mode")
        tts  = f("target_temperature_settings")

        heat_s1   = int(bool(hvc.get("heat_stage1_active",    False)))
        heat_s2   = int(bool(hvc.get("heat_stage2_active",    False)))
        alt_s1    = int(bool(hvc.get("alt_heat_stage1_active", False)))
        alt_s2    = int(bool(hvc.get("alt_heat_stage2_active", False)))
        cool_s1   = int(bool(hvc.get("cool_stage1_active",    False)))
        cool_s2   = int(bool(hvc.get("cool_stage2_active",    False)))
        heating   = int(bool(heat_s1 or heat_s2 or alt_s1 or alt_s2))
        cooling   = int(bool(cool_s1 or cool_s2))

        fan_speed = str(fan.get("current_speed") or "").lower()
        if fan_speed:
            fan_on = int(fan_speed != "off")
        elif fan.get("user_requested_fan_running") is not None:
            fan_on = int(bool(fan.get("user_requested_fan_running")))
        else:
            # Some Observe updates only carry fan_control_settings (mode toggles).
            # Use that as a fallback so fan duration events still open/close.
            mode = str(fan_settings.get("mode") or "").lower()
            timer_speed = str(fan_settings.get("timer_speed") or "").lower()
            fan_on = int(mode == "continuous_on" or timer_speed in ("stage1", "stage2", "stage3"))

        structure_mode = str(sm.get("structureMode", "")).upper()
        away      = int(structure_mode == "AWAY" or structure_mode.endswith("_AWAY"))
        humidifier = int(bool(hvc.get("humidifier_active", False)))

        setpoint_type = tts.get("setpoint_type", "").upper()
        heat_target_c = tts.get("heating_target_c")
        cool_target_c = tts.get("cooling_target_c")
        if setpoint_type == "COOL":
            target_temp_c      = cool_target_c
            target_temp_high_c = None
        elif setpoint_type in ("HEAT_COOL", "RANGE"):
            target_temp_c      = heat_target_c
            target_temp_high_c = cool_target_c
        else:  # HEAT or unknown
            target_temp_c      = heat_target_c
            target_temp_high_c = None

        raw_humidity = f("current_humidity").get("value_pct")
        humidity_val = float(raw_humidity) if raw_humidity is not None else None
        if humidity_val is not None and not (0.0 < humidity_val <= 100.0):
            humidity_val = None

        return {
            "serial":            device_id,
            "name":              device_name,
            "timestamp":         ts,
            "current_temp_c":    f("current_temperature").get("value_c"),
            "backplate_temp_c":  f("backplate_temperature").get("value_c"),
            "target_temp_c":     target_temp_c,
            "target_temp_high_c": target_temp_high_c,
            "humidity":          humidity_val,
            "heating":           heating,
            "cooling":           cooling,
            "fan":               fan_on,
            "away":              away,
            "heat_stage1":       heat_s1,
            "heat_stage2":       heat_s2,
            "heat_alt":          alt_s1,
            "heat_alt_stage2":   alt_s2,
            "cool_stage1":       cool_s1,
            "cool_stage2":       cool_s2,
            "humidifier":        humidifier,
            "has_humidifier":    "humidifier_active" in hvc,
        }

    @staticmethod
    def _where_id_from_device(device: dict[str, Any]) -> str | None:
        trait = device.get("traits", {}).get("device_located_settings", {})
        fields = trait.get("fields", {}) if isinstance(trait, dict) else {}
        where_obj = fields.get("where_id")
        if isinstance(where_obj, dict):
            val = where_obj.get("value")
            return str(val) if val else None
        if where_obj:
            return str(where_obj)
        return None

    @staticmethod
    def _hvac_context_from_device(device: dict[str, Any]) -> dict[str, int]:
        hvc = (
            device.get("traits", {})
            .get("hvac_control", {})
            .get("fields", {})
        )
        if not isinstance(hvc, dict) or not hvc:
            return {}

        heat_stage1 = int(bool(hvc.get("heat_stage1_active", False)))
        heat_stage2 = int(bool(hvc.get("heat_stage2_active", False)))
        alt_heat_stage1 = int(bool(hvc.get("alt_heat_stage1_active", False)))
        alt_heat_stage2 = int(bool(hvc.get("alt_heat_stage2_active", False)))
        cool_stage1 = int(bool(hvc.get("cool_stage1_active", False)))
        cool_stage2 = int(bool(hvc.get("cool_stage2_active", False)))

        return {
            "heating": int(bool(heat_stage1 or heat_stage2 or alt_heat_stage1 or alt_heat_stage2)),
            "cooling": int(bool(cool_stage1 or cool_stage2)),
            "heat_stage1": heat_stage1,
            "heat_stage2": heat_stage2,
            "alt_heat_stage1": alt_heat_stage1,
            "alt_heat_stage2": alt_heat_stage2,
            "cool_stage1": cool_stage1,
            "cool_stage2": cool_stage2,
        }

    @classmethod
    def _default_hvac_context(cls, devices: dict[str, Any]) -> dict[str, int]:
        for device in devices.values():
            if not isinstance(device, dict):
                continue
            if "Thermostat" in str(device.get("resource_type", "")):
                context = cls._hvac_context_from_device(device)
                if context:
                    return context
        return {
            "heating": 0,
            "cooling": 0,
            "heat_stage1": 0,
            "heat_stage2": 0,
            "alt_heat_stage1": 0,
            "alt_heat_stage2": 0,
            "cool_stage1": 0,
            "cool_stage2": 0,
        }

    @staticmethod
    def _parse_state_meta_ts(value: Any) -> datetime | None:
        if not value:
            return None
        if isinstance(value, datetime):
            return value.astimezone(timezone.utc)
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
            except ValueError:
                return None
        return None

    @classmethod
    def _indoor_info_from_device(
        cls,
        device_id: str,
        device: dict,
        device_name: str,
        epoch: float,
    ) -> dict[str, Any] | None:
        traits = device.get("traits", {})
        temp_trait = traits.get("current_temperature", {})
        humidity_trait = traits.get("current_humidity", {})
        temp_fields = temp_trait.get("fields", {}) if isinstance(temp_trait, dict) else {}
        humidity_fields = humidity_trait.get("fields", {}) if isinstance(humidity_trait, dict) else {}

        current_temp_c = temp_fields.get("value_c")
        if current_temp_c is None:
            return None

        ts = cls._parse_state_meta_ts((temp_trait.get("state_meta") or {}).get("decoded_at"))
        if ts is None:
            ts = cls._parse_state_meta_ts((humidity_trait.get("state_meta") or {}).get("decoded_at"))
        if ts is None:
            ts = datetime.fromtimestamp(epoch, tz=timezone.utc)

        humidity = humidity_fields.get("value_pct")
        return {
            "serial": device_id,
            "name": device_name,
            "resource_type": device.get("resource_type", ""),
            "where_id": cls._where_id_from_device(device),
            "timestamp": ts,
            "current_temp_c": float(current_temp_c),
            "humidity": None if humidity is None else float(humidity),
        }

    def _maybe_write_weather(self) -> None:
        """Write a weather point when outdoor_temp_c or humidity changed."""
        w = self._weather_cache
        if not w:
            return
        prev = self._last_weather_state
        if any(w.get(k) != prev.get(k) for k in _WEATHER_COMPARE_KEYS):
            try:
                self._influx.write_weather(w)
                self._last_weather_state = {k: w.get(k) for k in _WEATHER_COMPARE_KEYS}
                log.debug(
                    "Wrote weather point outdoor_temp_c=%.1f",
                    w.get("outdoor_temp_c") or 0,
                )
            except Exception as exc:
                log.error("Weather write failed: %s", exc)

    # ── Per-device change-gated collection ────────────────────────────────────

    def _process_device(self, serial: str, info: dict[str, Any], epoch: float) -> None:
        """
        Write a thermostat point only if the change-gate allows it.

        State fields  (booleans): write on any change, no time gate.
        Analog fields (temps/humidity): write on change + ≥ analog_min_interval.
        """
        temp_missing = info.get("current_temp_c") is None

        last = self._last_info.get(serial, {})

        state_changed = any(info.get(f) != last.get(f) for f in _STATE_FIELDS)

        analog_changed = any(info.get(k) != last.get(k) for k in _ANALOG_KEYS)
        last_analog_ts = self._last_analog_write_ts.get(serial, 0.0)
        analog_ready = (epoch - last_analog_ts) >= self._analog_min_interval

        if state_changed or (analog_changed and analog_ready):
            info["dev_idx"] = self._get_or_assign_dev_idx(serial, info)
            self._influx.write_thermostat(info, self._weather_cache)
            if analog_changed and analog_ready:
                self._last_analog_write_ts[serial] = epoch
            log.debug(
                "Wrote thermostat %s  state=%s analog=%s",
                serial,
                state_changed,
                analog_changed and analog_ready,
            )
            if temp_missing:
                log.debug("Wrote thermostat state-only point for %s (current_temp_c missing)", serial)

        self._update_state_duration_events(serial, info, epoch)

        # Always update snapshot so next comparison is against fresh values.
        self._last_info[serial] = dict(info)

        # Carry prev flags for R-value mode detection on next tick.
        self._last_info[serial]["_prev_heating"] = bool(info["heating"])
        self._last_info[serial]["_prev_cooling"] = bool(info["cooling"])

    def _update_state_duration_events(self, serial: str, info: dict[str, Any], epoch: float) -> None:
        """Emit derived duration events when tracked cycle states turn off."""
        last = self._last_info.get(serial, {})
        active_runs = self._active_state_runs.setdefault(serial, {})

        for state_key in _CYCLE_EVENT_KEYS:
            prev_on = bool(last.get(state_key, False))
            curr_on = bool(info.get(state_key, False))

            if state_key == "fan" and prev_on != curr_on:
                log.debug(
                    "Duration transition fan serial=%s prev=%s curr=%s epoch=%.3f",
                    serial,
                    prev_on,
                    curr_on,
                    epoch,
                )

            if not prev_on and curr_on:
                active_runs[state_key] = epoch
                if state_key == "fan":
                    log.debug(
                        "Duration start fan serial=%s start_epoch=%.3f",
                        serial,
                        epoch,
                    )
                continue

            if prev_on and not curr_on:
                start_epoch = active_runs.pop(state_key, epoch)
                duration_seconds = max(0.0, float(epoch - start_epoch))
                if duration_seconds <= 0:
                    if state_key == "fan":
                        log.debug(
                            "Duration skip fan serial=%s non-positive duration start=%.3f end=%.3f",
                            serial,
                            start_epoch,
                            epoch,
                        )
                    continue

                dev_idx = info.get("dev_idx")
                if dev_idx is None:
                    dev_idx = self._get_or_assign_dev_idx(serial, info)
                    info["dev_idx"] = dev_idx

                self._influx.write_state_duration_event(
                    dev_idx=int(dev_idx),
                    state_key=state_key,
                    start_ts=datetime.fromtimestamp(start_epoch, tz=timezone.utc),
                    end_ts=datetime.fromtimestamp(epoch, tz=timezone.utc),
                    duration_seconds=duration_seconds,
                )
                if state_key == "fan":
                    log.debug(
                        "Duration emit fan serial=%s dev_idx=%s duration=%.1fs start=%.3f end=%.3f",
                        serial,
                        dev_idx,
                        duration_seconds,
                        start_epoch,
                        epoch,
                    )

    def _process_indoor_device(
        self,
        serial: str,
        info: dict[str, Any],
        epoch: float,
        hvac_context: dict[str, int],
    ) -> None:
        """Write indoor temperature/humidity snapshots for one device."""
        last = self._last_indoor.get(serial, {})
        analog_changed = any(info.get(k) != last.get(k) for k in ("current_temp_c", "humidity"))
        last_write_ts = self._last_indoor_write_ts.get(serial, 0.0)
        analog_ready = (epoch - last_write_ts) >= self._analog_min_interval

        if analog_changed and analog_ready:
            info["dev_idx"] = self._get_or_assign_dev_idx(serial, info)
            self._influx.write_indoor_reading(info)
            self._last_indoor_write_ts[serial] = epoch
            log.debug(
                "Wrote indoor device %s (%s)",
                serial,
                info.get("name", serial),
            )

        self._last_indoor[serial] = dict(info)
