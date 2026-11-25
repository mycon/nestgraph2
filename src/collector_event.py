"""
Event-driven thermostat collector.

This collector keeps the same write-gate behavior as src.collector.ThermostatCollector,
but it consumes decoded Observe protobuf frames directly instead of polling /state_min.

Pipeline:
  Observe stream frame -> merge into in-memory device state -> process with the same
  per-device change gates/event emission logic used by ThermostatCollector.

Run directly:
  python -m src.collector_event
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import src.config as cfg
import src.grpc_web as grpc_web
from src.collector import ThermostatCollector
from src.foyer_client import FoyerAuthError, FoyerError
from src.grpc_web import OBSERVE_TRAITS_THERMOSTAT, ObserveAuthError, observe_stream
from src.influx_writer import InfluxWriter
from src.nest_client import NestAuthError, NestClient
from src.weather_client import make_weather_client

log = logging.getLogger("collector_event")


def _deep_merge(target: dict[str, Any], source: dict[str, Any]) -> None:
    for key, val in source.items():
        if key in target and isinstance(target[key], dict) and isinstance(val, dict):
            _deep_merge(target[key], val)
        else:
            target[key] = val


def _prune_empty_traits(state: dict[str, dict]) -> dict[str, dict]:
    pruned: dict[str, dict] = {}
    for resource_id, resource in state.items():
        if not isinstance(resource, dict):
            continue
        resource_copy = dict(resource)
        traits = resource.get("traits", {})
        if isinstance(traits, dict):
            filtered_traits = {
                trait_label: trait_info
                for trait_label, trait_info in traits.items()
                if isinstance(trait_info, dict) and trait_info.get("fields")
            }
            if filtered_traits:
                resource_copy["traits"] = filtered_traits
            else:
                resource_copy.pop("traits", None)
        pruned[resource_id] = resource_copy
    return pruned


class EventThermostatCollector(ThermostatCollector):
    """Event-driven variant of ThermostatCollector using observe_stream()."""

    def __init__(self, influx: InfluxWriter, verbose_observe_decoded: bool = False) -> None:
        super().__init__(
            influx=influx,
            analog_min_interval=cfg.ANALOG_MIN_INTERVAL_SECONDS,
            mapping_sync_interval=cfg.MAPPING_SYNC_INTERVAL_SECONDS,
            label_short_id_len=cfg.DEVICE_LABEL_SHORT_ID_LEN,
            device_names=None,
        )
        self._running = True
        self._stop_event = threading.Event()
        self._verbose_observe_decoded = bool(verbose_observe_decoded)
        self._state_devices: dict[str, dict[str, Any]] = {}
        self._weather_poll_interval = max(30, int(cfg.WEATHER_POLL_INTERVAL_SECONDS))
        self._next_weather_refresh_ts = 0.0
        self._where_name_cache: dict[str, str] = {}
        self._last_device_index_written: dict[str, tuple[int, str, str]] = {}  # device_id -> (dev_idx, where_name, where_id)
        self._weather_client = make_weather_client(self._load_weather_cfg(), cfg.NEST_ACCESS_TOKEN)

        # Pre-populate the device_index write-guard and names cache from DB so we don't
        # re-write unchanged entries or fall back to dev_<idx> for existing devices.
        try:
            db_full = influx.read_device_index_full()
            for device_id, row in db_full.items():
                dev_idx = row.get("dev_idx")
                device_name = str(row.get("device_name") or "")
                where_id = str(row.get("where_id") or "")
                if dev_idx is not None and device_name:
                    self._last_device_index_written[device_id] = (int(dev_idx), device_name, where_id)
                    # Load the name into memory cache if it's a real name (not fallback dev_<idx>)
                    if not device_name.startswith("dev_"):
                        self._device_names[device_id] = device_name
                        if where_id:
                            self._device_names[where_id] = device_name
            if self._last_device_index_written:
                log.info(
                    "Pre-populated device_index cache and write-guard for %d device(s) from DB",
                    len(self._last_device_index_written),
                )
        except Exception as exc:
            log.warning("Could not pre-populate device_index cache: %s", exc)

        token_file = self._resolve_token_file()
        foyer_only = cfg.CONNECT_NEST_DATA_SOURCE == "foyer"
        self._nest_client = NestClient(
            access_token=cfg.NEST_ACCESS_TOKEN,
            transport_url=cfg.NEST_TRANSPORT_URL,
            user_id=cfg.NEST_USER_ID,
            user=cfg.NEST_USER,
            google_bearer_token=cfg.NEST_GOOGLE_BEARER_TOKEN,
            token_file=token_file,
            refresh_margin_sec=cfg.CONNECT_NEST_TOKEN_REFRESH_MARGIN_SEC,
            foyer_only=foyer_only,
        )
        self._token_refresh_thread = threading.Thread(
            target=self._token_refresh_loop,
            name="event_token_refresh",
            daemon=True,
        )
        self._token_refresh_thread.start()

    @staticmethod
    def _resolve_token_file() -> str:
        config_path = Path(os.getenv("CONFIG_PATH", "config/config.json"))
        with config_path.open(encoding="utf-8") as fh:
            data = json.load(fh)
        token_file = data.get("connect_nest", {}).get("token_file")
        if token_file:
            return str(token_file)
        return str(config_path.parent / "tokens.json")

    @staticmethod
    def _load_weather_cfg() -> dict[str, Any]:
        config_path = Path(os.getenv("CONFIG_PATH", "config/config.json"))
        with config_path.open(encoding="utf-8") as fh:
            data = json.load(fh)

        weather_cfg = data.get("weather", {}) if isinstance(data, dict) else {}
        nest_cfg = data.get("nest", {}) if isinstance(data, dict) else {}
        return {
            **weather_cfg,
            "postal_code": nest_cfg.get("postal_code", ""),
            "country_code": nest_cfg.get("country_code", "US"),
        }

    def stop(self) -> None:
        self._running = False
        self._stop_event.set()

    def _token_refresh_loop(self) -> None:
        log.info("Event token refresh loop started")
        data_source = cfg.CONNECT_NEST_DATA_SOURCE
        rotate_interval = max(120.0, float(cfg.CONNECT_NEST_COOKIE_ROTATE_INTERVAL_SEC))
        while not self._stop_event.wait(60):
            # Keep home.google.com cookies alive via RotateCookies (~540s cadence)
            try:
                rotated = self._nest_client.rotate_google_cookies(min_interval_sec=rotate_interval)
                if rotated:
                    log.info("Event token refresh loop: rotated Google session cookies via RotateCookies")
            except Exception as exc:
                log.warning("Event token refresh loop: RotateCookies error: %s", exc)

            if data_source == "foyer":
                continue

            try:
                refreshed = self._nest_client.refresh_if_needed()
                if refreshed:
                    log.info(
                        "Event token refresh loop: token refreshed, expires %s",
                        datetime.fromtimestamp(
                            self._nest_client.token_expiry,
                            tz=timezone.utc,
                        ).isoformat(),
                    )
            except NestAuthError as exc:
                if data_source == "auto" and self._nest_client.foyer_client is not None:
                    log.warning(
                        "Event token refresh loop: Observe JWT refresh failed (%s); "
                        "Foyer fallback remains available.",
                        exc,
                    )
                    continue
                log.error("Event token refresh loop: fatal auth error: %s", exc)
                self.stop()
                return
            except Exception as exc:
                log.warning("Event token refresh loop: transient error: %s", exc)
        log.info("Event token refresh loop stopped")

    @staticmethod
    def _extract_value(obj: Any) -> str | None:
        if isinstance(obj, dict):
            val = obj.get("value")
            if val is None:
                return None
            return str(val)
        if obj is None:
            return None
        return str(obj)

    def _update_where_name_cache_from_snapshot(self, snapshot: dict[str, Any]) -> None:
        for resource in snapshot.values():
            if not isinstance(resource, dict):
                continue

            traits = resource.get("traits", {})
            if not isinstance(traits, dict):
                continue

            for trait in traits.values():
                if not isinstance(trait, dict):
                    continue

                trait_type = str(trait.get("trait_type", "") or "")
                if "LocatedAnnotationsTrait" not in trait_type:
                    continue

                fields = trait.get("fields", {})
                if not isinstance(fields, dict):
                    continue

                for key in ("annotations", "custom_annotations"):
                    entries = fields.get(key)
                    if not isinstance(entries, list):
                        continue
                    for entry in entries:
                        if not isinstance(entry, dict):
                            continue
                        info = entry.get("info", {})
                        if not isinstance(info, dict):
                            continue
                        where_id = self._extract_value(info.get("id"))
                        where_name = self._extract_value(info.get("name"))
                        if where_id and where_name:
                            self._where_name_cache[where_id] = where_name

    def _refresh_weather(self, epoch: float) -> None:
        if epoch < self._next_weather_refresh_ts:
            return
        self._next_weather_refresh_ts = epoch + self._weather_poll_interval

        try:
            if hasattr(self._weather_client, "update_token"):
                self._weather_client.update_token(self._nest_client.access_token)

            payload = self._weather_client.get_current()
            if isinstance(payload, dict):
                self._weather_cache = payload
                self._maybe_write_weather()
        except Exception as exc:
            log.debug("Weather refresh failed: %s", exc)

    def _resolve_where_from_observe(self, device: dict[str, Any]) -> tuple[str | None, str | None]:
        """Resolve (where_id, where_name) using observe traits and annotation cache only."""
        where_id = self._where_id_from_device(device)

        # Thermostat observe snapshots may omit device_located_settings.where_id;
        # fallback to the canonical default annotation id when available.
        if not where_id and "Thermostat" in str(device.get("resource_type", "")):
            default_id = "ANNOTATION_0000000000000000"
            if default_id in self._where_name_cache or default_id in self._device_names:
                where_id = default_id
            else:
                for ann_id, ann_name in self._where_name_cache.items():
                    if str(ann_name).strip().lower() == "entryway":
                        where_id = ann_id
                        break

        if not where_id:
            return None, None

        where_name = self._where_name_cache.get(where_id) or self._device_names.get(where_id)
        return where_id, (str(where_name) if where_name else None)

    def _get_device_name(self, device_id: str, device: dict[str, Any]) -> str:
        """Get resolved room name, or fallback to dev_<dev_idx>."""
        if not device_id.startswith("DEVICE_"):
            return device_id

        where_id, where_name = self._resolve_where_from_observe(device)
        name = (
            self._device_names.get(device_id)
            or (self._device_names.get(where_id) if where_id else None)
        )
        if name:
            return str(name)

        # Fallback to dev_<dev_idx>
        info_temp = {
            "resource_type": device.get("resource_type", ""),
            "where_id": str(where_id or ""),
        }
        dev_idx = self._get_or_assign_dev_idx(device_id, info_temp)
        return f"dev_{dev_idx}"

    def _sync_device_mappings_from_observe(self, devices: dict[str, Any], epoch: float) -> None:
        for device_id, device in devices.items():
            if not isinstance(device, dict) or not device_id.startswith("DEVICE_"):
                continue

            where_id, where_name = self._resolve_where_from_observe(device)
            if not where_id:
                continue

            # If we don't have a dynamic where_name yet, default to f"dev_{dev_idx}" temporarily
            resolved_name = where_name
            if not resolved_name:
                info_temp = {
                    "resource_type": device.get("resource_type", ""),
                    "where_id": where_id,
                }
                dev_idx = self._get_or_assign_dev_idx(device_id, info_temp)
                resolved_name = f"dev_{dev_idx}"

            self._device_names[where_id] = resolved_name
            self._device_names[device_id] = resolved_name

            # where_id now captured in write_device_index
            self._last_mapping_written[device_id] = (where_id, resolved_name)

            info = {
                "name": resolved_name,
                "device_label": self._device_label(device_id),
                "device_label_short": self._device_label_short(resolved_name, device_id, self._label_short_id_len),
                "resource_type": device.get("resource_type", ""),
                "where_id": where_id,
            }
            dev_idx = self._get_or_assign_dev_idx(device_id, info)
            last_idx_written = self._last_device_index_written.get(device_id)
            if last_idx_written != (dev_idx, resolved_name, where_id):
                self._influx.write_device_index(
                    device_id=device_id,
                    dev_idx=dev_idx,
                    device_name=resolved_name,
                    device_label=info["device_label"],
                    device_label_short=info["device_label_short"],
                    resource_type=info["resource_type"],
                    where_id=where_id,
                )
                self._last_device_index_written[device_id] = (dev_idx, resolved_name, where_id)

    def _process_merged_state(self, epoch: float) -> None:
        devices = self._state_devices
        for device_id, device in devices.items():
            if not isinstance(device, dict) or not device_id.startswith("DEVICE_"):
                continue
            where_id, _ = self._resolve_where_from_observe(device)
            device_name = self._get_device_name(device_id, device)
            self._ensure_dev_idx_cached(
                device_id=device_id,
                device_name=device_name,
                device_label=self._device_label(device_id),
                device_label_short=self._device_label_short(device_name, device_id, self._label_short_id_len),
                resource_type=device.get("resource_type", ""),
                where_id=str(where_id or ""),
            )

        self._sync_device_mappings_from_observe(devices, epoch)
        default_hvac = self._default_hvac_context(devices)

        for device_id, device in devices.items():
            if not isinstance(device, dict) or not device_id.startswith("DEVICE_"):
                continue

            rt = device.get("resource_type", "")
            device_name = self._get_device_name(device_id, device)
            device_label = self._device_label(device_id)
            device_label_short = self._device_label_short(device_name, device_id, self._label_short_id_len)

            try:
                indoor = self._indoor_info_from_device(device_id, device, device_name, epoch)
                if indoor is not None:
                    indoor["device_label"] = device_label
                    indoor["device_label_short"] = device_label_short
                    hvac_context = self._hvac_context_from_device(device) or dict(default_hvac)
                    self._process_indoor_device(device_id, indoor, epoch, hvac_context)

                if "Thermostat" not in rt:
                    continue

                info = self._info_from_device(device_id, device, device_name, epoch)
                info["device_label"] = device_label
                info["device_label_short"] = device_label_short

                if info.get("current_temp_c") is None:
                    rcs_temps = [
                        d.get("traits", {}).get("current_temperature", {})
                        .get("fields", {}).get("value_c")
                        for d in devices.values()
                        if isinstance(d, dict) and "Kryptonite" in d.get("resource_type", "")
                    ]
                    rcs_temps = [t for t in rcs_temps if t is not None]
                    if rcs_temps:
                        info["current_temp_c"] = sum(rcs_temps) / len(rcs_temps)

                self._process_device(device_id, info, epoch)
            except Exception as exc:
                log.error("Error processing device %s: %s", device_id, exc)

    def _build_foyer_device_id_overrides(self) -> dict[str, str]:
        """Map known room names / thermostat type to canonical DEVICE_... IDs from DB/state."""
        overrides: dict[str, str] = {}
        thermostat_ids: list[str] = []

        for device_id, (_dev_idx, device_name, _where_id) in self._last_device_index_written.items():
            if not device_id.startswith("DEVICE_"):
                continue
            if device_name and not device_name.startswith("dev_"):
                overrides[device_name.strip().lower()] = device_id
            # Heuristic: single known non-Kryptonite thermostat id from DB or state
            rt = str(self._state_devices.get(device_id, {}).get("resource_type", ""))
            if "Thermostat" in rt and device_id not in thermostat_ids:
                thermostat_ids.append(device_id)

        if cfg.TEST_THERMOSTAT_DEVICE_RESOURCE_ID.startswith("DEVICE_"):
            if cfg.TEST_THERMOSTAT_DEVICE_RESOURCE_ID not in thermostat_ids:
                thermostat_ids.append(cfg.TEST_THERMOSTAT_DEVICE_RESOURCE_ID)

        if len(thermostat_ids) == 1:
            overrides["action.devices.types.THERMOSTAT"] = thermostat_ids[0]
        return overrides

    def _ingest_snapshot(self, snapshot: dict[str, Any]) -> None:
        if self._verbose_observe_decoded:
            log.info("observe decoded snapshot: %s", json.dumps(snapshot, sort_keys=True, default=str))
        self._update_where_name_cache_from_snapshot(snapshot)
        decoded_snapshot = _prune_empty_traits(snapshot)
        _deep_merge(self._state_devices, decoded_snapshot)
        epoch = time.time()
        self._refresh_weather(epoch)
        self._process_merged_state(epoch)

    def _run_foyer_poll_once(self) -> bool:
        foyer = self._nest_client.foyer_client
        if foyer is None:
            return False
        overrides = self._build_foyer_device_id_overrides()
        snapshot = foyer.poll_observe_compatible_snapshot(device_id_overrides=overrides)
        if snapshot:
            grpc_web.last_stream_activity_ts = time.time()
            self._ingest_snapshot(snapshot)
            return True
        return False

    def run_forever(self) -> None:
        fatal_auth_error: NestAuthError | None = None
        data_source = cfg.CONNECT_NEST_DATA_SOURCE
        foyer_poll_interval = max(5.0, float(cfg.CONNECT_NEST_FOYER_POLL_INTERVAL_SEC))
        log.info(
            "Collector loop starting (data_source=%s, foyer_poll_interval=%.0fs)",
            data_source,
            foyer_poll_interval,
        )
        consecutive_observe_failures = 0
        foyer_fallback_until = 0.0

        while self._running:
            use_foyer = (
                data_source == "foyer"
                or (
                    data_source == "auto"
                    and self._nest_client.foyer_client is not None
                    and (
                        not self._nest_client.transport_auth_token
                        or time.time() < foyer_fallback_until
                    )
                )
            )

            if use_foyer:
                try:
                    self._run_foyer_poll_once()
                    if self._stop_event.wait(foyer_poll_interval):
                        break
                except FoyerAuthError as exc:
                    log.error("Foyer auth error: %s", exc)
                    fatal_auth_error = NestAuthError(str(exc))
                    self.stop()
                    break
                except (FoyerError, Exception) as exc:
                    log.warning("Foyer poll error: %s", exc)
                    if self._stop_event.wait(min(30.0, foyer_poll_interval)):
                        break
                continue

            try:
                self._nest_client.refresh_if_needed()
                jwt = self._nest_client.transport_auth_token
                if not jwt:
                    if data_source == "auto" and self._nest_client.foyer_client is not None:
                        log.warning("No Observe JWT available; switching to Google Home Foyer polling fallback for 30m")
                        foyer_fallback_until = time.time() + 1800.0
                        continue
                    raise NestAuthError("No Observe JWT available and Foyer fallback is not configured")

                auth_snapshot = self._nest_client.auth_debug_snapshot()
                log.debug("Observe connect auth snapshot: %s", auth_snapshot)

                saw_frame = False
                for snapshot in observe_stream(
                    OBSERVE_TRAITS_THERMOSTAT,
                    jwt,
                    connection_timeout_sec=cfg.CONNECT_NEST_OBSERVE_TIMEOUT_SEC,
                ):
                    if not self._running:
                        break
                    saw_frame = True
                    consecutive_observe_failures = 0
                    self._ingest_snapshot(snapshot)

                if not saw_frame:
                    consecutive_observe_failures += 1
                    if (
                        data_source == "auto"
                        and self._nest_client.foyer_client is not None
                        and consecutive_observe_failures >= 3
                    ):
                        log.warning(
                            "Observe stream yielded no frames after %d attempts; "
                            "switching to Google Home Foyer polling fallback for 30m",
                            consecutive_observe_failures,
                        )
                        foyer_fallback_until = time.time() + 1800.0
                        consecutive_observe_failures = 0
                        continue

                if self._running:
                    time.sleep(2)

            except ObserveAuthError as exc:
                log.warning(
                    "Observe auth error: %s; forcing token refresh; auth_snapshot=%s",
                    exc,
                    self._nest_client.auth_debug_snapshot(),
                )
                try:
                    self._nest_client.refresh_now()
                    if not self._nest_client.transport_auth_token and data_source == "auto" and self._nest_client.foyer_client is not None:
                        log.warning("Observe JWT unavailable after refresh; switching to Foyer fallback for 30m")
                        foyer_fallback_until = time.time() + 1800.0
                except NestAuthError as refresh_exc:
                    if data_source == "auto" and self._nest_client.foyer_client is not None:
                        log.warning("Observe JWT refresh failed (%s); switching to Foyer fallback for 30m", refresh_exc)
                        foyer_fallback_until = time.time() + 1800.0
                    else:
                        log.error("Forced token refresh failed with fatal auth error: %s", refresh_exc)
                        fatal_auth_error = refresh_exc
                        self.stop()
                        break
                except Exception as refresh_exc:
                    log.error("Forced token refresh failed: %s", refresh_exc)
                    time.sleep(10)
            except NestAuthError as exc:
                if data_source == "auto" and self._nest_client.foyer_client is not None:
                    log.warning("Observe NestAuthError (%s); switching to Foyer fallback for 30m", exc)
                    foyer_fallback_until = time.time() + 1800.0
                    continue
                log.error("Nest auth error: %s", exc)
                fatal_auth_error = exc
                self.stop()
                break
            except Exception as exc:
                consecutive_observe_failures += 1
                log.warning("Observe loop error: %s", exc)
                if (
                    data_source == "auto"
                    and self._nest_client.foyer_client is not None
                    and consecutive_observe_failures >= 3
                ):
                    log.warning("Observe failed %d times; switching to Foyer fallback for 30m", consecutive_observe_failures)
                    foyer_fallback_until = time.time() + 1800.0
                    consecutive_observe_failures = 0
                else:
                    time.sleep(5)

        log.info("Event collector stopped")
        if fatal_auth_error is not None:
            raise SystemExit(1)


def main() -> None:
    parser = argparse.ArgumentParser(description="Event-driven Nest collector")
    parser.add_argument(
        "--verbose-observe-decoded",
        action="store_true",
        help="Log the full decoded Observe snapshot JSON for each frame",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, cfg.LOG_LEVEL, logging.INFO),
        format="%(asctime)s %(levelname)-8s %(name)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )

    influx = InfluxWriter(
        url=cfg.INFLUXDB_URL,
        token=cfg.INFLUXDB_TOKEN,
        org=cfg.INFLUXDB_ORG,
        bucket=cfg.INFLUXDB_BUCKET_ROOT,
    )
    collector = EventThermostatCollector(
        influx=influx,
        verbose_observe_decoded=bool(args.verbose_observe_decoded),
    )

    def _shutdown(_sig: int, _frame: Any) -> None:
        collector.stop()
        influx.close()
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    try:
        collector.run_forever()
    finally:
        influx.close()


if __name__ == "__main__":
    main()
