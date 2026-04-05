"""
InfluxDB v2 write helper.

Wraps influxdb-client-python with convenience methods for each measurement
type used in nestgraph2.

Measurements
------------
thermostat          – Real-time thermostat state (every 5 min)
indoor_sensor        – Per-device indoor temperature/humidity snapshots
device_index        – Stable device_id → dev_idx + metadata mapping (including where_id)
hvac_cycle          – Individual HVAC cycle from energy history (daily)
energy_daily        – Daily energy totals from energy history (daily)
computed_metrics    – Aggregated metrics at 4h/8h/12h/24h cadence
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

log = logging.getLogger(__name__)


class InfluxWriter:
    def __init__(self, url: str, token: str, org: str, bucket: str) -> None:
        self._bucket_root = bucket
        self._bucket_raw = f"{bucket}_v2_raw"
        self._bucket_model = f"{bucket}_v2_model"
        self._bucket_rollup = f"{bucket}_v2_rollup"
        self._bucket_event = f"{bucket}_v2_event"
        self._bucket_devmap = f"{bucket}_v2_devmap"
        self._org = org
        self._client = InfluxDBClient(url=url, token=token, org=org)
        self._write_api = self._client.write_api(write_options=SYNCHRONOUS)
        self._ensure_bucket(self._bucket_devmap)
        log.info(
            "InfluxDB writer connected to %s org=%s root=%s raw=%s model=%s rollup=%s event=%s devmap=%s",
            url,
            org,
            self._bucket_root,
            self._bucket_raw,
            self._bucket_model,
            self._bucket_rollup,
            self._bucket_event,
            self._bucket_devmap,
        )

    def close(self) -> None:
        self._client.close()

    # ------------------------------------------------------------------
    # thermostat  (every 5 min)
    # ------------------------------------------------------------------

    @staticmethod
    def _f_to_c(temp_f: float) -> float:
        return (temp_f - 32.0) * 5.0 / 9.0

    @staticmethod
    def _outdoor_temp_c(weather: dict[str, Any]) -> float | None:
        """
        Return outdoor temperature in Celsius for raw thermostat writes.

        Preferred keys:
          - outdoor_temp_c (canonical)
          - outdoor_temp (legacy Fahrenheit)
          - outdoor_temp_f
        """
        if weather.get("outdoor_temp_c") is not None:
            return float(weather["outdoor_temp_c"])
        if weather.get("outdoor_temp") is not None:
            return InfluxWriter._f_to_c(float(weather["outdoor_temp"]))
        if weather.get("outdoor_temp_f") is not None:
            return InfluxWriter._f_to_c(float(weather["outdoor_temp_f"]))
        return None

    @staticmethod
    def _outdoor_humidity_pct(weather: dict[str, Any]) -> float | None:
        """Return outdoor humidity percentage from either canonical weather key."""
        if weather.get("outdoor_humidity_pct") is not None:
            return float(weather["outdoor_humidity_pct"])
        if weather.get("outdoor_humidity") is not None:
            return float(weather["outdoor_humidity"])
        return None

    def write_thermostat(self, info: dict[str, Any], weather: dict[str, Any]) -> None:
        """Write a real-time thermostat data point."""
        ts = datetime.fromtimestamp(info["timestamp"], tz=timezone.utc)
        outdoor_temp_c = self._outdoor_temp_c(weather)
        outdoor_humidity_pct = self._outdoor_humidity_pct(weather)
        p = (
            Point("thermostat")
            .tag("dev_idx", str(info["dev_idx"]))
            .time(ts, WritePrecision.S)
            # HVAC states (simple)
            .field("heating", int(info["heating"]))
            .field("cooling", int(info["cooling"]))
            .field("fan", int(info["fan"]))
            .field("away", int(info["away"]))
            # Stage detail
            .field("heat_stage1", int(info["heat_stage1"]))
            .field("heat_stage2", int(info["heat_stage2"]))
            .field("alt_heat_stage1", int(info["heat_alt"]))
            .field("alt_heat_stage2", int(info["heat_alt_stage2"]))

            .field("cool_stage1", int(info["cool_stage1"]))
            .field("cool_stage2", int(info["cool_stage2"]))
        )

        # Analog fields can be absent when Nest omits a trait snapshot.
        if info.get("current_temp_c") is not None:
            p = p.field("current_temp_c", float(info["current_temp_c"]))
        if info.get("backplate_temp_c") is not None:
            p = p.field("backplate_temp_c", float(info["backplate_temp_c"]))
        if info.get("target_temp_c") is not None:
            p = p.field("target_temp_c", float(info["target_temp_c"]))
        if info.get("humidity") is not None:
            p = p.field("humidity", float(info["humidity"]))

        if info.get("target_temp_high_c") is not None:
            p = p.field("target_temp_high_c", float(info["target_temp_high_c"]))
        if info.get("has_humidifier"):
            p = p.field("humidifier", int(info["humidifier"]))

        if outdoor_temp_c is not None:
            p = p.field("outdoor_temp_c", outdoor_temp_c)
        if outdoor_humidity_pct is not None:
            p = p.field("outdoor_humidity", outdoor_humidity_pct)

        self._write(p, bucket=self._bucket_raw)
        log.debug("Wrote thermostat point for dev_idx=%s at %s", info["dev_idx"], ts.isoformat())

    # ------------------------------------------------------------------
    # hvac_cycle  (from energy history)
    # ------------------------------------------------------------------

    def write_hvac_cycle(
        self,
        device_id: str,
        device_name: str,
        day_str: str,       # "2026-01-15"
        start_seconds: int, # seconds after midnight
        duration_seconds: int,
        cycle_type: str,    # e.g. "heat_stage1"
    ) -> None:
        """Write a single HVAC cycle record."""
        from datetime import timedelta

        day_dt = datetime.strptime(day_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        ts = day_dt + timedelta(seconds=start_seconds)

        p = (
            Point("hvac_cycle")
            .tag("device_id", device_id)
            .tag("device_name", device_name)
            .tag("cycle_type", cycle_type)
            .time(ts, WritePrecision.S)
            .field("duration_seconds", int(duration_seconds))
        )
        self._write(p, bucket=self._bucket_raw)

    # ------------------------------------------------------------------
    # energy_daily  (from energy history)
    # ------------------------------------------------------------------

    def write_energy_daily(
        self,
        device_id: str,
        device_name: str,
        day_str: str,
        totals: dict[str, int],
    ) -> None:
        """Write daily energy totals for a device."""
        ts = datetime.strptime(day_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        p = (
            Point("energy_daily")
            .tag("device_id", device_id)
            .tag("device_name", device_name)
            .time(ts, WritePrecision.S)
            .field("total_heating_time", int(totals.get("total_heating_time", 0)))
            .field("total_cooling_time", int(totals.get("total_cooling_time", 0)))
            .field("total_fan_cooling_time", int(totals.get("total_fan_cooling_time", 0)))
            .field("total_humidifier_time", int(totals.get("total_humidifier_time", 0)))
            .field("total_dehumidifier_time", int(totals.get("total_dehumidifier_time", 0)))
        )
        self._write(p, bucket=self._bucket_raw)
        log.debug("Wrote energy_daily for %s on %s", device_id, day_str)

    # ------------------------------------------------------------------
    # weather  (standalone, on-change at 15-min cadence)
    # ------------------------------------------------------------------

    def write_weather(self, weather: dict[str, Any]) -> None:
        """Write a standalone weather point to the raw bucket."""
        ts = datetime.now(tz=timezone.utc)
        p = Point("weather").time(ts, WritePrecision.S)
        for key in (
            "outdoor_temp_c",
            "outdoor_humidity",
            "wind_speed_mph",
            "wind_gusts_mph",
            "solar_irradiance_wm2",
            "cloud_cover_pct",
        ):
            val = weather.get(key)
            if val is not None:
                p = p.field(key, float(val))
        self._write(p, bucket=self._bucket_raw)
        log.debug("Wrote weather point outdoor_temp_c=%.1f", weather.get("outdoor_temp_c") or 0)

    # ------------------------------------------------------------------
    # indoor_sensor  (every 5 min, thermostat + remote sensors)
    # ------------------------------------------------------------------

    def write_indoor_reading(self, reading: dict[str, Any]) -> None:
        """Write one indoor temperature/humidity point keyed by dev_idx."""
        ts = reading.get("timestamp")
        if not isinstance(ts, datetime):
            ts = datetime.now(tz=timezone.utc)
        p = (
            Point("indoor_sensor")
            .tag("dev_idx", str(reading["dev_idx"]))
            .time(ts, WritePrecision.S)
            .field("current_temp_c", float(reading["current_temp_c"]))
        )
        if reading.get("humidity") is not None:
            p = p.field("humidity", float(reading["humidity"]))
        self._write(p, bucket=self._bucket_raw)
        log.debug(
            "Wrote indoor_sensor dev_idx=%s (%s)",
            reading["dev_idx"],
            reading.get("name", reading.get("serial", "")),
        )

    # ------------------------------------------------------------------
    # indoor_dynamics  (post-processed per-cycle dT/dt)
    # ------------------------------------------------------------------

    def write_indoor_dynamics(self, dynamics: dict[str, Any]) -> None:
        """Write per-device temperature dynamics and stage-specific dT/dt."""
        ts = dynamics.get("timestamp")
        if not isinstance(ts, datetime):
            ts = datetime.now(tz=timezone.utc)

        p = (
            Point("indoor_dynamics")
            .tag("dev_idx", str(dynamics["dev_idx"]))
            .tag("resource_type", dynamics.get("resource_type", ""))
            .time(ts, WritePrecision.S)
            .field("delta_deg_c_per_hr", float(dynamics["delta_deg_c_per_hr"]))
        )

        for field_name in (
            "outdoor_temp_c",
            "indoor_temp_c",
            "thermal_recovery_ratio",
            "delta_deg_c_per_hr_heat",
            "delta_deg_c_per_hr_cool",
            "delta_deg_c_per_hr_alt_heat",
            "delta_deg_c_per_hr_heat_stage1",
            "delta_deg_c_per_hr_heat_stage2",
            "delta_deg_c_per_hr_alt_heat_stage1",
            "delta_deg_c_per_hr_alt_heat_stage2",
            "delta_deg_c_per_hr_cool_stage1",
            "delta_deg_c_per_hr_cool_stage2",
        ):
            val = dynamics.get(field_name)
            if val is not None:
                p = p.field(field_name, float(val))

        for field_name in (
            "heating",
            "cooling",
            "heat_stage1",
            "heat_stage2",
            "alt_heat_stage1",
            "alt_heat_stage2",
            "cool_stage1",
            "cool_stage2",
        ):
            if field_name in dynamics:
                p = p.field(field_name, int(bool(dynamics[field_name])))

        self._write(p, bucket=self._bucket_raw)
        log.debug(
            "Wrote indoor_dynamics dT/dt=%.3f C/h",
            float(dynamics["delta_deg_c_per_hr"]),
        )

    # ------------------------------------------------------------------
    # thermal_resistance  (per-cycle R-value estimate)
    # ------------------------------------------------------------------

    def write_thermal_resistance(
        self,
        dev_idx: int,
        timestamp: datetime,
        hvac_mode: str,             # "heating" or "cooling"
        r_factor: float,
        indoor_start_c: float,
        indoor_end_c: float,
        outdoor_avg_c: float,
        off_duration_hours: float,
    ) -> None:
        """Write one R-factor estimate (per HVAC off→on transition)."""
        p = (
            Point("thermal_resistance")
            .tag("dev_idx", str(dev_idx))
            .tag("hvac_mode", hvac_mode)
            .time(timestamp, WritePrecision.S)
            .field("r_factor", float(r_factor))
            .field("indoor_start_c", float(indoor_start_c))
            .field("indoor_end_c", float(indoor_end_c))
            .field("outdoor_avg_c", float(outdoor_avg_c))
            .field("off_duration_hours", float(off_duration_hours))
        )
        self._write(p, bucket=self._bucket_raw)
        log.info(
            "R-factor dev_idx=%s (%s): %.2f h  off=%.1f h  ΔT_in=%.1f°  T_out=%.1f°",
            dev_idx,
            hvac_mode,
            r_factor,
            off_duration_hours,
            abs(indoor_start_c - indoor_end_c),
            outdoor_avg_c,
        )

    # ------------------------------------------------------------------
    # computed_metrics  (aggregated model at 4h/8h/12h/24h cadence)
    # ------------------------------------------------------------------

    def write_computed_metrics(
        self,
        dev_idx: int,
        cadence: str,
        period_end: "datetime",
        metrics: dict,
    ) -> None:
        """Write one computed_metrics summary point into the model bucket."""
        p = (
            Point("computed_metrics")
            .tag("dev_idx", str(dev_idx))
            .tag("cadence", cadence)
            .time(period_end, WritePrecision.S)
        )
        for field, value in metrics.items():
            if value is None:
                continue
            if isinstance(value, bool):
                p = p.field(field, int(value))
            elif isinstance(value, int):
                p = p.field(field, value)
            else:
                p = p.field(field, float(value))
        self._write(p, bucket=self._bucket_model)
        log.debug(
            "Wrote computed_metrics dev_idx=%s cadence=%s ts=%s fields=%d",
            dev_idx,
            cadence,
            period_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            len(metrics),
        )

    # ------------------------------------------------------------------
    # device_index  (stable device_id → dev_idx mapping)
    # ------------------------------------------------------------------

    def write_device_index(
        self,
        device_id: str,
        dev_idx: int,
        device_name: str,
        device_label: str,
        device_label_short: str,
        resource_type: str,
        where_id: str = "",
    ) -> None:
        """Write a device_index entry mapping device_id to its stable integer index.
        
        Uses fixed epoch timestamp (0) so updates upsert rather than append,
        keeping the devmap table flat at exactly (num_devices * num_fields) points.
        Includes where_id for location tracking (replaces old device_mapping).
        """
        p = (
            Point("device_index")
            .tag("device_id", device_id)
            .time(0, WritePrecision.S)
            .field("dev_idx", dev_idx)
            .field("device_name", device_name)
            .field("device_label", device_label)
            .field("device_label_short", device_label_short)
            .field("resource_type", resource_type)
            .field("where_id", where_id)
        )
        self._write(p, bucket=self._bucket_devmap)
        log.info(
            "Wrote device_index: %s → dev_idx=%d (%s)",
            device_id, dev_idx, device_name,
        )

    def read_device_index_map(self) -> dict[str, int]:
        """
        Read latest device_id → dev_idx mapping from InfluxDB.

        Returns a dict keyed by device_id. If a device has multiple historical
        mappings, the latest point wins.
        """
        query = f'''
from(bucket: "{self._bucket_devmap}")
  |> range(start: 0)
  |> filter(fn: (r) => r._measurement == "device_index" and r._field == "dev_idx")
  |> group(columns: ["device_id"])
  |> last()
'''

        result = self._client.query_api().query(org=self._org, query=query)
        mapping: dict[str, int] = {}
        for table in result:
            for record in table.records:
                device_id = str(record.values.get("device_id") or "")
                if not device_id:
                    continue
                value = record.get_value()
                if value is None:
                    continue
                try:
                    mapping[device_id] = int(value)
                except (TypeError, ValueError):
                    log.debug("Skipping invalid dev_idx for %s: %r", device_id, value)

        return mapping

    # ------------------------------------------------------------------
    # state_duration_event  (derived runtime events)
    # ------------------------------------------------------------------

    def read_device_index_full(self) -> dict[str, dict]:
        """
        Read latest device_index entries from InfluxDB, returning all fields.

        Returns dict keyed by device_id:
          {device_id: {"dev_idx": int, "device_name": str, ...}}
        """
        query = f'''
from(bucket: "{self._bucket_devmap}")
  |> range(start: 0)
  |> filter(fn: (r) => r._measurement == "device_index")
  |> filter(fn: (r) =>
      r._field == "dev_idx" or
      r._field == "device_name" or
      r._field == "device_label" or
      r._field == "device_label_short" or
      r._field == "resource_type" or
      r._field == "where_id"
  )
  |> group(columns: ["device_id", "_field"])
  |> last()
'''
        result = self._client.query_api().query(org=self._org, query=query)
        rows: dict[str, dict] = {}
        for table in result:
            for record in table.records:
                device_id = str(record.values.get("device_id") or "")
                field_name = str(record.get_field() or "")
                if not device_id or not field_name:
                    continue
                row = rows.setdefault(device_id, {"device_id": device_id})
                value = record.get_value()
                if field_name == "dev_idx":
                    try:
                        row["dev_idx"] = int(value)
                    except (TypeError, ValueError):
                        row["dev_idx"] = value
                else:
                    row[field_name] = value
        return rows

    def write_state_duration_event(
        self,
        dev_idx: int,
        state_key: str,
        start_ts: datetime,
        end_ts: datetime,
        duration_seconds: float,
    ) -> None:
        """Write one derived state duration event to the event bucket."""
        p = (
            Point("state_duration_event")
            .tag("dev_idx", str(dev_idx))
            .tag("state_key", state_key)
            .time(end_ts, WritePrecision.S)
            .field("start_epoch_s", int(start_ts.timestamp()))
            .field("end_epoch_s", int(end_ts.timestamp()))
            .field("duration_seconds", float(duration_seconds))
        )
        self._write(p, bucket=self._bucket_event)
        log.debug(
            "Wrote state_duration_event dev_idx=%s state=%s duration=%.1fs",
            dev_idx,
            state_key,
            duration_seconds,
        )

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _ensure_bucket(self, bucket_name: str) -> None:
        """Create a bucket if it does not already exist."""
        try:
            buckets_api = self._client.buckets_api()
            existing = buckets_api.find_bucket_by_name(bucket_name)
            if existing is not None:
                return

            orgs = self._client.organizations_api().find_organizations(org=self._org)
            if not orgs:
                log.warning("Could not resolve org id for bucket creation: %s", self._org)
                return

            buckets_api.create_bucket(bucket_name=bucket_name, org_id=orgs[0].id)
            log.info("Created missing Influx bucket: %s", bucket_name)
        except Exception as exc:
            log.warning("Could not ensure bucket %s exists: %s", bucket_name, exc)

    def _write(self, point: Point, bucket: str | None = None) -> None:
        try:
            self._write_api.write(bucket=bucket or self._bucket_raw, org=self._org, record=point)
        except Exception as exc:
            log.error("InfluxDB write error: %s", exc)
