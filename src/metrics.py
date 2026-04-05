"""
Computed metrics — aggregation at 4h / 8h / 12h / 24h cadences.

These metrics are computed by querying InfluxDB for recent thermostat,
weather, thermostat, and state_duration_event data,
then writing daily summary points plus per-cycle indoor_dynamics and
thermal_resistance rows back into InfluxDB.

Metrics computed per cadence window:
    avg_outdoor_temp_c        – Mean outdoor temperature (°C)
    min_outdoor_temp_c        – Minimum outdoor temperature (°C)
    max_outdoor_temp_c        – Maximum outdoor temperature (°C)
    avg_indoor_temp_c         – Mean indoor temperature (°C)
    avg_delta_deg_c_per_hr    – Mean indoor temperature slope (°C/hr)
    avg_r_factor              – Mean thermal time constant from recent thermal
                                                            resistance measurements (hours)
    heating_stage1_time       – Total seconds heating stage 1 was active
    heating_stage2_time       – Total seconds heating stage 2 was active
    total_heat_time           – stage1 + stage2 (seconds)
    alt_heat_stage1_time      – Total seconds alt-heat stage 1 was active
    alt_heat_stage2_time      – Total seconds alt-heat stage 2 was active
    total_alt_heat_time       – alt_stage1 + alt_stage2 (seconds)
    cooling_stage1_time       – Total seconds cooling stage 1 was active
    cooling_stage2_time       – Total seconds cooling stage 2 was active
    total_cooling_time        – stage1 + stage2 (seconds)
    fan_time                  – Total seconds fan was running (seconds)
    total_humidifier_time     – Total seconds humidifier was active (seconds)
    away_time                 – Total seconds structure was in AWAY mode
    home_time                 – Total seconds structure was in HOME mode
    away_fraction             – away_time divided by cadence window length
    *_cycle_count             – Completed cycle counts from state_duration_event
    thermal_recovery_ratio    – avg_delta_deg_c_per_hr / (avg_indoor_temp_c - avg_outdoor_temp_c)

Stage durations use Flux integral() on the change-only `thermostat`
measurement, giving exact elapsed-time totals rather than poll-count estimates.

Future expansion ideas
----------------------
  away_time              – Total seconds structure was in AWAY mode
  setpoint_delta_avg_c   – Mean difference between current and target temp (comfort gap)
  thermal_recovery_ratio – dT/dt divided by (indoor - outdoor) delta; normalises heating
                           rate against outdoor conditions for cross-day comparisons
  per_stage_r_factor     – Separate R-value averages for heat vs cool transitions
  outdoor_temp_histogram – Binned distribution of outdoor temps during HVAC runtime
                           (useful for correlating runtime with temperature bands)
  multi_cadence          – Re-enable 4h/8h/12h sub-day cadences in the scheduler for
                           intra-day dashboards (MetricsComputer already supports them)
  per_sensor_indoor_avg  – Separate avg_indoor_temp_c per Kryptonite remote sensor,
                           not just the thermostat device
"""

from __future__ import annotations

import argparse
import logging
import math
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from influxdb_client import InfluxDBClient

import src.config as cfg
from src.influx_writer import InfluxWriter

log = logging.getLogger(__name__)

THERMOSTAT_RESOURCE_TYPE = "nest.resource.NestLearningThermostat3Resource"

GLOBAL_METRIC_FIELDS: frozenset[str] = frozenset({
    "avg_outdoor_temp_c",
    "min_outdoor_temp_c",
    "max_outdoor_temp_c",
})

PER_DEVICE_METRIC_FIELDS: frozenset[str] = frozenset({
    "avg_indoor_temp_c",
    "avg_delta_deg_c_per_hr",
    "avg_delta_deg_c_per_hr_heat",
    "avg_delta_deg_c_per_hr_cool",
    "avg_delta_deg_c_per_hr_alt_heat",
    "avg_delta_deg_c_per_hr_heat_stage1",
    "avg_delta_deg_c_per_hr_heat_stage2",
    "avg_delta_deg_c_per_hr_alt_heat_stage1",
    "avg_delta_deg_c_per_hr_alt_heat_stage2",
    "avg_delta_deg_c_per_hr_cool_stage1",
    "avg_delta_deg_c_per_hr_cool_stage2",
    "thermal_recovery_ratio",
})

THERMOSTAT_ONLY_METRIC_FIELDS: frozenset[str] = frozenset({
    "heating_stage1_time",
    "heating_stage2_time",
    "total_heat_time",
    "alt_heat_stage1_time",
    "alt_heat_stage2_time",
    "total_alt_heat_time",
    "cooling_stage1_time",
    "cooling_stage2_time",
    "total_cooling_time",
    "fan_time",
    "total_humidifier_time",
    "away_time",
    "home_time",
    "away_fraction",
    "avg_r_factor",
    "heating_cycle_count",
    "cooling_cycle_count",
    "fan_cycle_count",
    "humidifier_cycle_count",
    "heating_stage1_cycle_count",
    "heating_stage2_cycle_count",
    "alt_heat_stage1_cycle_count",
    "alt_heat_stage2_cycle_count",
    "cooling_stage1_cycle_count",
    "cooling_stage2_cycle_count",
})

HVAC_STAGE_FIELDS: tuple[str, ...] = (
    "heat_stage1",
    "heat_stage2",
    "alt_heat_stage1",
    "alt_heat_stage2",
    "cool_stage1",
    "cool_stage2",
)

HVAC_STAGE_TIME_KEYS: dict[str, str] = {
    "heat_stage1": "heating_stage1_time",
    "heat_stage2": "heating_stage2_time",
    "alt_heat_stage1": "alt_heat_stage1_time",
    "alt_heat_stage2": "alt_heat_stage2_time",
    "cool_stage1": "cooling_stage1_time",
    "cool_stage2": "cooling_stage2_time",
}

HVAC_STAGE_CYCLE_KEYS: dict[str, str] = {
    "heat_stage1": "heating_stage1_cycle_count",
    "heat_stage2": "heating_stage2_cycle_count",
    "alt_heat_stage1": "alt_heat_stage1_cycle_count",
    "alt_heat_stage2": "alt_heat_stage2_cycle_count",
    "cool_stage1": "cooling_stage1_cycle_count",
    "cool_stage2": "cooling_stage2_cycle_count",
}


def _resolve_tzinfo() -> ZoneInfo:
    try:
        return ZoneInfo(cfg.TZ)
    except Exception as exc:
        raise ValueError(f"Invalid timezone in config: {cfg.TZ}") from exc


def _align_period_end(period_end: datetime, cadence_hours: int) -> datetime:
    """
    Align daily windows to the last completed local midnight.

    Sub-day cadences continue to use the explicit timestamp passed in.
    """
    period_end_utc = period_end.astimezone(timezone.utc)
    if cadence_hours != 24:
        return period_end_utc

    local_tz = _resolve_tzinfo()
    local_end = period_end_utc.astimezone(local_tz)
    local_midnight = local_end.replace(hour=0, minute=0, second=0, microsecond=0)

    if local_end < local_midnight:
        local_midnight -= timedelta(days=1)

    return local_midnight.astimezone(timezone.utc)


class MetricsComputer:
    def __init__(
        self,
        influx_url: str,
        influx_token: str,
        influx_org: str,
        influx_bucket: str,
        influx_writer,               # InfluxWriter instance
        enable_duration_fallback: bool = False,
    ) -> None:
        self._org = influx_org
        self._bucket = influx_bucket
        self._writer = influx_writer
        self._event_bucket = cfg.INFLUXDB_BUCKET_V2_EVENT
        self._devmap_bucket = cfg.INFLUXDB_BUCKET_V2_DEVMAP
        self._enable_duration_fallback = enable_duration_fallback
        self._device_index_map: dict[str, int] = {}
        self._query_client = InfluxDBClient(
            url=influx_url, token=influx_token, org=influx_org
        ).query_api()

    def discover_devices(self) -> list[dict[str, Any]]:
        """
        Return list of device metadata rows from the latest device_index entries.
        """
        try:
            rows = self._writer.read_device_index_full()
            devices: list[dict[str, Any]] = []
            for device_id, row in sorted(rows.items()):
                try:
                    dev_idx = int(row.get("dev_idx"))
                except (TypeError, ValueError):
                    continue
                devices.append(
                    {
                        "serial": device_id,
                        "device_name": str(row.get("device_name") or device_id),
                        "dev_idx": dev_idx,
                        "resource_type": str(row.get("resource_type") or ""),
                    }
                )
            log.info("discover_devices: found %d device(s) in devmap bucket", len(devices))
            return devices
        except Exception as exc:
            log.warning("discover_devices: query failed: %s", exc)
            return []

    def run_for_cadence(self, cadence_hours: int, devices: list[dict]) -> None:
        """
        Compute and store metrics for one cadence window ending at now.

        `devices` is a list of {'serial': str, 'dev_idx': int, 'resource_type': str} dicts.
        """
        cadence_label = f"{cadence_hours}h" if cadence_hours < 24 else "1d"
        period_end = _align_period_end(datetime.now(tz=timezone.utc), cadence_hours)
        period_start = period_end - timedelta(hours=cadence_hours)
        self._refresh_device_index_map()

        for device in devices:
            try:
                self._compute_and_write(
                    device["serial"],
                    str(device.get("device_name") or device["serial"]),
                    int(device["dev_idx"]),
                    str(device.get("resource_type") or ""),
                    cadence_label,
                    period_start,
                    period_end,
                    dry_run=False,
                )
            except Exception as exc:
                log.error(
                    "Metrics compute error for %s cadence=%s: %s",
                    device["serial"],
                    cadence_label,
                    exc,
                )

    def run_window(
        self,
        cadence_hours: int,
        devices: list[dict],
        period_end: datetime,
        dry_run: bool = False,
    ) -> tuple[int, int]:
        """
        Compute metrics for one explicit window.

        Returns tuple: (attempted_devices, written_points).
        """
        cadence_label = f"{cadence_hours}h" if cadence_hours < 24 else "1d"
        period_end_utc = _align_period_end(period_end, cadence_hours)
        period_start = period_end_utc - timedelta(hours=cadence_hours)
        attempted = 0
        written = 0
        self._refresh_device_index_map()

        for device in devices:
            attempted += 1
            try:
                did_write = self._compute_and_write(
                    device["serial"],
                    str(device.get("device_name") or device["serial"]),
                    int(device["dev_idx"]),
                    str(device.get("resource_type") or ""),
                    cadence_label,
                    period_start,
                    period_end_utc,
                    dry_run=dry_run,
                )
                if did_write:
                    written += 1
            except Exception as exc:
                log.error(
                    "Metrics compute error for %s cadence=%s: %s",
                    device["serial"],
                    cadence_label,
                    exc,
                )

        return attempted, written

    # ------------------------------------------------------------------
    # Core computation
    # ------------------------------------------------------------------

    def _compute_and_write(
        self,
        serial: str,
        device_name: str,
        dev_idx_hint: int,
        resource_type: str,
        cadence: str,
        period_start: datetime,
        period_end: datetime,
        dry_run: bool = False,
    ) -> bool:
        start_rfc = period_start.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_rfc = period_end.strftime("%Y-%m-%dT%H:%M:%SZ")
        dev_idx = self._device_index_map.get(serial, dev_idx_hint)
        window_seconds = max(0.0, (period_end - period_start).total_seconds())

        # Averages: outdoor from the standalone weather measurement (change-only, 15-min);
        # indoor from the per-device indoor_sensor measurement (tagged by dev_idx).
        avg_outdoor = self._query_avg_field("weather", "outdoor_temp_c", None, start_rfc, end_rfc)
        min_outdoor = self._query_field_aggregate("weather", "outdoor_temp_c", None, start_rfc, end_rfc, "min")
        max_outdoor = self._query_field_aggregate("weather", "outdoor_temp_c", None, start_rfc, end_rfc, "max")
        avg_indoor = self._query_avg_field_by_tag(
            "indoor_sensor",
            "current_temp_c",
            "dev_idx",
            dev_idx,
            start_rfc,
            end_rfc,
        )

        if avg_indoor is None:
            log.debug("No indoor sensor data for %s in window %s–%s", serial, start_rfc, end_rfc)
            return False

        stage_metrics = self._derive_stage_runtime_and_counts(dev_idx, period_start, period_end)
        heat_s1 = stage_metrics["heating_stage1_time"]
        heat_s2 = stage_metrics["heating_stage2_time"]
        alt_heat_s1 = stage_metrics["alt_heat_stage1_time"]
        alt_heat_s2 = stage_metrics["alt_heat_stage2_time"]
        cool_s1 = stage_metrics["cooling_stage1_time"]
        cool_s2 = stage_metrics["cooling_stage2_time"]
        fan      = self._query_state_duration(serial, "fan",            start_rfc, end_rfc)
        hum      = self._query_state_duration(serial, "humidifier",     start_rfc, end_rfc)
        away_time = self._query_state_duration(serial, "away", start_rfc, end_rfc, force_raw=True)
        home_time = max(0.0, window_seconds - away_time)
        away_fraction = (away_time / window_seconds) if window_seconds > 0 else None

        dynamics, r_data = self._derive_postprocessed_metrics(
            serial=serial,
            device_name=device_name,
            dev_idx=dev_idx,
            resource_type=resource_type,
            period_start=period_start,
            period_end=period_end,
            emit_points=not dry_run,
        )
        cycle_counts = self._query_cycle_counts(dev_idx, start_rfc, end_rfc)
        cycle_counts.update({
            "heating_stage1_cycle_count": stage_metrics["heating_stage1_cycle_count"],
            "heating_stage2_cycle_count": stage_metrics["heating_stage2_cycle_count"],
            "alt_heat_stage1_cycle_count": stage_metrics["alt_heat_stage1_cycle_count"],
            "alt_heat_stage2_cycle_count": stage_metrics["alt_heat_stage2_cycle_count"],
            "cooling_stage1_cycle_count": stage_metrics["cooling_stage1_cycle_count"],
            "cooling_stage2_cycle_count": stage_metrics["cooling_stage2_cycle_count"],
            "heating_cycle_count": (
                stage_metrics["heating_stage1_cycle_count"]
                + stage_metrics["heating_stage2_cycle_count"]
                + stage_metrics["alt_heat_stage1_cycle_count"]
                + stage_metrics["alt_heat_stage2_cycle_count"]
            ),
            "cooling_cycle_count": (
                stage_metrics["cooling_stage1_cycle_count"]
                + stage_metrics["cooling_stage2_cycle_count"]
            ),
        })

        avg_delta = dynamics.get("avg_delta_deg_c_per_hr")
        thermal_recovery_ratio = None
        if avg_outdoor is not None and avg_delta is not None:
            indoor_outdoor_delta = avg_indoor - avg_outdoor
            if abs(indoor_outdoor_delta) >= 0.1:
                thermal_recovery_ratio = avg_delta / indoor_outdoor_delta

        metrics: dict[str, Any] = {
            "avg_outdoor_temp_c": avg_outdoor,
            "min_outdoor_temp_c": min_outdoor,
            "max_outdoor_temp_c": max_outdoor,
            "avg_indoor_temp_c": avg_indoor,
            "heating_stage1_time": heat_s1,
            "heating_stage2_time": heat_s2,
            "total_heat_time": heat_s1 + heat_s2,
            "alt_heat_stage1_time": alt_heat_s1,
            "alt_heat_stage2_time": alt_heat_s2,
            "total_alt_heat_time": alt_heat_s1 + alt_heat_s2,
            "cooling_stage1_time": cool_s1,
            "cooling_stage2_time": cool_s2,
            "total_cooling_time": cool_s1 + cool_s2,
            "fan_time": fan,
            "total_humidifier_time": hum,
            "away_time": away_time,
            "home_time": home_time,
            "away_fraction": away_fraction,
            "thermal_recovery_ratio": thermal_recovery_ratio,
            **cycle_counts,
            **dynamics,
        }

        if r_data:
            metrics["avg_r_factor"] = sum(r_data) / len(r_data)

        metrics = self._select_metrics_for_device(metrics, resource_type)
        if not metrics:
            log.debug("No applicable computed metrics for dev_idx=%s resource_type=%s", dev_idx, resource_type)
            return False

        if dry_run:
            write_ts = period_start if cadence == "1d" else period_end
            log.info(
                "[DRY RUN] computed_metrics dev_idx=%s cadence=%s ts=%s fields=%d resource_type=%s",
                dev_idx,
                cadence,
                write_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                len([k for k, v in metrics.items() if v is not None]),
                resource_type,
            )
        else:
            write_ts = period_start if cadence == "1d" else period_end
            self._writer.write_computed_metrics(
                dev_idx=dev_idx,
                cadence=cadence,
                period_end=write_ts,
                metrics=metrics,
            )

        return True

    @staticmethod
    def _select_metrics_for_device(metrics: dict[str, Any], resource_type: str) -> dict[str, Any]:
        allowed_fields = set(PER_DEVICE_METRIC_FIELDS)
        if resource_type == THERMOSTAT_RESOURCE_TYPE:
            allowed_fields.update(GLOBAL_METRIC_FIELDS)
            allowed_fields.update(THERMOSTAT_ONLY_METRIC_FIELDS)
        return {
            key: value
            for key, value in metrics.items()
            if key in allowed_fields and value is not None
        }

    # ------------------------------------------------------------------
    # InfluxDB queries
    # ------------------------------------------------------------------

    def _query_avg_field(
        self, measurement: str, field: str, serial: str | None, start: str, stop: str
    ) -> float | None:
        """Mean value of a field over the window. serial=None skips device_id filter."""
        return self._query_field_aggregate(measurement, field, serial, start, stop, "mean")

    def _query_avg_field_by_tag(
        self,
        measurement: str,
        field: str,
        tag_name: str,
        tag_value: str | int | None,
        start: str,
        stop: str,
    ) -> float | None:
        """Mean value of a field over the window filtered by an arbitrary tag."""
        return self._query_field_aggregate_by_tag(
            measurement,
            field,
            tag_name,
            tag_value,
            start,
            stop,
            "mean",
        )

    def _query_field_aggregate(
        self,
        measurement: str,
        field: str,
        serial: str | None,
        start: str,
        stop: str,
        aggregate: str,
    ) -> float | None:
        """Aggregate one field over the window using mean/min/max."""
        return self._query_field_aggregate_by_tag(
            measurement,
            field,
            "device_id",
            serial,
            start,
            stop,
            aggregate,
        )

    def _query_field_aggregate_by_tag(
        self,
        measurement: str,
        field: str,
        tag_name: str,
        tag_value: str | int | None,
        start: str,
        stop: str,
        aggregate: str,
    ) -> float | None:
        """Aggregate one field over the window filtered by a specific tag."""
        if aggregate not in {"mean", "min", "max"}:
            raise ValueError(f"Unsupported aggregate: {aggregate}")

        tag_filter = f'|> filter(fn: (r) => r.{tag_name} == "{tag_value}")' if tag_value is not None else ""
        flux = f"""
from(bucket: "{self._bucket}")
  |> range(start: {start}, stop: {stop})
  |> filter(fn: (r) => r._measurement == "{measurement}")
  {tag_filter}
  |> filter(fn: (r) => r._field == "{field}")
  |> {aggregate}()
"""
        try:
            tables = self._query_client.query(flux)
            for table in tables:
                for record in table.records:
                    val = record.get_value()
                    if val is not None:
                        return float(val)
        except Exception as exc:
            log.warning("field aggregate query (%s.%s %s) failed: %s", measurement, field, aggregate, exc)
        return None

    def _query_state_duration(
        self, serial: str, stage_field: str, start: str, stop: str, force_raw: bool = False
    ) -> float:
        """Return total seconds active for one thermostat state over a window.

        Primary source is `state_duration_event.duration_seconds`, which already
        represents exact completed on-intervals. We only fall back to raw-field
        integration if event data is unavailable.
        """
        dev_idx = self._device_index_map.get(serial)
        if dev_idx is None:
            log.debug("No dev_idx mapping for %s; %s duration defaults to 0", serial, stage_field)
            return 0.0

        if not force_raw:
            flux_event = f"""
from(bucket: "{self._event_bucket}")
  |> range(start: {start}, stop: {stop})
  |> filter(fn: (r) => r._measurement == "state_duration_event")
  |> filter(fn: (r) => r.dev_idx == "{dev_idx}")
  |> filter(fn: (r) => r.state_key == "{stage_field}")
  |> filter(fn: (r) => r._field == "duration_seconds")
  |> sum()
"""
            try:
                event_total = 0.0
                saw_value = False
                tables = self._query_client.query(flux_event)
                for table in tables:
                    for record in table.records:
                        val = record.get_value()
                        if val is not None:
                            saw_value = True
                            event_total += float(val)
                if saw_value:
                    return event_total
            except Exception as exc:
                log.warning("event duration query (%s) failed: %s", stage_field, exc)

            if not self._enable_duration_fallback:
                return 0.0

        # Fallback: edge-aware step integration from raw change-only 0/1 samples.
        # This avoids Flux integral() linear interpolation error on sparse transitions.
        # Guard against stale carry-in values after long telemetry gaps.
        start_dt = datetime.fromisoformat(start.replace("Z", "+00:00"))
        stop_dt = datetime.fromisoformat(stop.replace("Z", "+00:00"))

        flux_before = f"""
from(bucket: "{self._bucket}")
  |> range(start: 0, stop: {start})
  |> filter(fn: (r) => r._measurement == "thermostat")
  |> filter(fn: (r) => r.dev_idx == "{dev_idx}")
  |> filter(fn: (r) => r._field == "{stage_field}")
  |> last()
"""
        flux_window = f"""
from(bucket: "{self._bucket}")
  |> range(start: {start}, stop: {stop})
  |> filter(fn: (r) => r._measurement == "thermostat")
  |> filter(fn: (r) => r.dev_idx == "{dev_idx}")
  |> filter(fn: (r) => r._field == "{stage_field}")
  |> sort(columns: ["_time"])
"""
        try:
            current_state = False
            before_time: datetime | None = None
            before_tables = self._query_client.query(flux_before)
            for table in before_tables:
                for record in table.records:
                    val = record.get_value()
                    if val is not None:
                        current_state = bool(float(val))
                        before_time = record.get_time()

            # Ignore stale carry state when the most recent sample is too old.
            # This prevents multi-day false runtimes when telemetry resumes with a trailing 0.
            max_carry_age = timedelta(hours=24)
            if current_state and before_time is not None and (start_dt - before_time) > max_carry_age:
                log.debug(
                    "Ignoring stale carry state for %s (age=%s > %s)",
                    stage_field,
                    start_dt - before_time,
                    max_carry_age,
                )
                current_state = False

            points: list[tuple[datetime, bool]] = []
            window_tables = self._query_client.query(flux_window)
            for table in window_tables:
                for record in table.records:
                    t = record.get_time()
                    val = record.get_value()
                    if t is not None and val is not None:
                        points.append((t, bool(float(val))))
            points.sort(key=lambda item: item[0])

            cursor = start_dt
            total = 0.0
            for point_time, point_state in points:
                if point_time < cursor:
                    continue
                if current_state:
                    total += max(0.0, (point_time - cursor).total_seconds())
                cursor = point_time
                current_state = point_state

            if current_state and cursor < stop_dt:
                total += (stop_dt - cursor).total_seconds()

            return max(0.0, total)
        except Exception as exc:
            log.warning("state_duration fallback query (%s) failed: %s", stage_field, exc)
            return 0.0

    @staticmethod
    def _coerce_float(value: Any) -> float | None:
        try:
            return float(value) if value is not None else None
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _active_stage_from_row(row: dict[str, Any]) -> str | None:
        if bool(row.get("heat_stage2")):
            return "heat_stage2"
        if bool(row.get("heat_stage1")):
            return "heat_stage1"
        if bool(row.get("alt_heat_stage2")):
            return "alt_heat_stage2"
        if bool(row.get("alt_heat_stage1")):
            return "alt_heat_stage1"
        if bool(row.get("cool_stage2")):
            return "cool_stage2"
        if bool(row.get("cool_stage1")):
            return "cool_stage1"
        return None

    def _query_thermostat_rows(
        self,
        dev_idx: int,
        start: datetime,
        stop: datetime,
    ) -> list[dict[str, Any]]:
        flux = f"""
from(bucket: "{self._bucket}")
  |> range(start: {start.strftime("%Y-%m-%dT%H:%M:%SZ")}, stop: {stop.strftime("%Y-%m-%dT%H:%M:%SZ")})
  |> filter(fn: (r) => r._measurement == "thermostat")
  |> filter(fn: (r) => r.dev_idx == "{dev_idx}")
  |> filter(fn: (r) =>
      r._field == "current_temp_c" or
      r._field == "outdoor_temp_c" or
      r._field == "heat_stage1" or
      r._field == "heat_stage2" or
      r._field == "alt_heat_stage1" or
      r._field == "alt_heat_stage2" or
      r._field == "cool_stage1" or
      r._field == "cool_stage2")
  |> pivot(rowKey:["_time"], columnKey:["_field"], valueColumn:"_value")
  |> sort(columns: ["_time"])
"""
        rows: list[dict[str, Any]] = []
        try:
            tables = self._query_client.query(flux)
            for table in tables:
                for record in table.records:
                    row = dict(record.values)
                    row["_time"] = record.get_time()
                    rows.append(row)
        except Exception as exc:
            log.warning("thermostat row query failed for dev_idx=%s: %s", dev_idx, exc)
        rows.sort(key=lambda row: row.get("_time") or datetime.min.replace(tzinfo=timezone.utc))
        return rows

    @staticmethod
    def _coerce_bool(value: Any) -> bool | None:
        if value is None:
            return None
        try:
            return bool(float(value))
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _effective_stage_states(raw_states: dict[str, bool]) -> dict[str, bool]:
        return {
            "heat_stage1": raw_states["heat_stage1"] and not raw_states["heat_stage2"],
            "heat_stage2": raw_states["heat_stage2"],
            "alt_heat_stage1": raw_states["alt_heat_stage1"] and not raw_states["alt_heat_stage2"],
            "alt_heat_stage2": raw_states["alt_heat_stage2"],
            "cool_stage1": raw_states["cool_stage1"] and not raw_states["cool_stage2"],
            "cool_stage2": raw_states["cool_stage2"],
        }

    def _query_stage_state_before(self, dev_idx: int, start: datetime) -> dict[str, bool]:
        state = {field: False for field in HVAC_STAGE_FIELDS}
        stage_filter = " or\n      ".join(f'r._field == "{field}"' for field in HVAC_STAGE_FIELDS)
        flux = f"""
from(bucket: "{self._bucket}")
  |> range(start: 0, stop: {start.strftime("%Y-%m-%dT%H:%M:%SZ")})
  |> filter(fn: (r) => r._measurement == "thermostat")
  |> filter(fn: (r) => r.dev_idx == "{dev_idx}")
  |> filter(fn: (r) =>
      {stage_filter})
  |> group(columns: ["_field"])
  |> last()
"""
        try:
            tables = self._query_client.query(flux)
            for table in tables:
                for record in table.records:
                    field = str(record.get_field() or "")
                    if field not in state:
                        continue
                    bool_val = self._coerce_bool(record.get_value())
                    if bool_val is not None:
                        state[field] = bool_val
        except Exception as exc:
            log.warning("stage carry-in query failed for dev_idx=%s: %s", dev_idx, exc)
        return state

    def _query_stage_rows(self, dev_idx: int, start: datetime, stop: datetime) -> list[dict[str, Any]]:
        stage_filter = " or\n      ".join(f'r._field == "{field}"' for field in HVAC_STAGE_FIELDS)
        flux = f"""
from(bucket: "{self._bucket}")
  |> range(start: {start.strftime("%Y-%m-%dT%H:%M:%SZ")}, stop: {stop.strftime("%Y-%m-%dT%H:%M:%SZ")})
  |> filter(fn: (r) => r._measurement == "thermostat")
  |> filter(fn: (r) => r.dev_idx == "{dev_idx}")
  |> filter(fn: (r) =>
      {stage_filter})
  |> pivot(rowKey:["_time"], columnKey:["_field"], valueColumn:"_value")
  |> sort(columns: ["_time"])
"""
        rows: list[dict[str, Any]] = []
        try:
            tables = self._query_client.query(flux)
            for table in tables:
                for record in table.records:
                    row = dict(record.values)
                    row["_time"] = record.get_time()
                    rows.append(row)
        except Exception as exc:
            log.warning("stage row query failed for dev_idx=%s: %s", dev_idx, exc)
        rows.sort(key=lambda row: row.get("_time") or datetime.min.replace(tzinfo=timezone.utc))
        return rows

    def _derive_stage_runtime_and_counts(
        self,
        dev_idx: int,
        period_start: datetime,
        period_end: datetime,
    ) -> dict[str, float]:
        metrics: dict[str, float] = {
            "heating_stage1_time": 0.0,
            "heating_stage2_time": 0.0,
            "alt_heat_stage1_time": 0.0,
            "alt_heat_stage2_time": 0.0,
            "cooling_stage1_time": 0.0,
            "cooling_stage2_time": 0.0,
            "heating_stage1_cycle_count": 0.0,
            "heating_stage2_cycle_count": 0.0,
            "alt_heat_stage1_cycle_count": 0.0,
            "alt_heat_stage2_cycle_count": 0.0,
            "cooling_stage1_cycle_count": 0.0,
            "cooling_stage2_cycle_count": 0.0,
        }

        raw_states = self._query_stage_state_before(dev_idx, period_start)
        stage_rows = self._query_stage_rows(dev_idx, period_start, period_end)
        cursor = period_start

        for row in stage_rows:
            row_time = row.get("_time")
            if not isinstance(row_time, datetime):
                continue
            if row_time < period_start:
                continue
            if row_time > period_end:
                break

            elapsed = max(0.0, (row_time - cursor).total_seconds())
            if elapsed > 0:
                effective_states = self._effective_stage_states(raw_states)
                for field, is_on in effective_states.items():
                    if is_on:
                        metrics[HVAC_STAGE_TIME_KEYS[field]] += elapsed
                cursor = row_time

            for field in HVAC_STAGE_FIELDS:
                if field not in row:
                    continue
                new_state = self._coerce_bool(row.get(field))
                if new_state is None:
                    continue
                if (not raw_states[field]) and new_state:
                    metrics[HVAC_STAGE_CYCLE_KEYS[field]] += 1.0
                raw_states[field] = new_state

        tail_elapsed = max(0.0, (period_end - cursor).total_seconds())
        if tail_elapsed > 0:
            effective_states = self._effective_stage_states(raw_states)
            for field, is_on in effective_states.items():
                if is_on:
                    metrics[HVAC_STAGE_TIME_KEYS[field]] += tail_elapsed

        return metrics

    @staticmethod
    def _append_stage_rate(
        results: dict[str, list[float]],
        stage: str,
        rate: float,
    ) -> None:
        results["avg_delta_deg_c_per_hr"].append(rate)
        stage_key = f"avg_delta_deg_c_per_hr_{stage}"
        if stage_key in results:
            results[stage_key].append(rate)
        if stage in ("heat_stage1", "heat_stage2"):
            results["avg_delta_deg_c_per_hr_heat"].append(rate)
        elif stage in ("alt_heat_stage1", "alt_heat_stage2"):
            results["avg_delta_deg_c_per_hr_alt_heat"].append(rate)
        elif stage in ("cool_stage1", "cool_stage2"):
            results["avg_delta_deg_c_per_hr_cool"].append(rate)

    def _derive_postprocessed_metrics(
        self,
        serial: str,
        device_name: str,
        dev_idx: int,
        resource_type: str,
        period_start: datetime,
        period_end: datetime,
        emit_points: bool = False,
    ) -> tuple[dict[str, float | None], list[float]]:
        lookback_start = period_start - timedelta(days=2)
        rows = self._query_thermostat_rows(dev_idx, lookback_start, period_end)

        rate_lists: dict[str, list[float]] = {
            "avg_delta_deg_c_per_hr": [],
            "avg_delta_deg_c_per_hr_heat": [],
            "avg_delta_deg_c_per_hr_cool": [],
            "avg_delta_deg_c_per_hr_alt_heat": [],
            "avg_delta_deg_c_per_hr_heat_stage1": [],
            "avg_delta_deg_c_per_hr_heat_stage2": [],
            "avg_delta_deg_c_per_hr_alt_heat_stage1": [],
            "avg_delta_deg_c_per_hr_alt_heat_stage2": [],
            "avg_delta_deg_c_per_hr_cool_stage1": [],
            "avg_delta_deg_c_per_hr_cool_stage2": [],
        }
        r_factors: list[float] = []
        if not rows:
            return {key: None for key in rate_lists}, r_factors

        active_run: dict[str, Any] | None = None
        off_state: dict[str, Any] | None = None
        last_temp_c: float | None = None
        last_outdoor_temp_c: float | None = None

        for row in rows:
            row_time = row.get("_time")
            if not isinstance(row_time, datetime):
                continue

            temp_c = self._coerce_float(row.get("current_temp_c"))
            if temp_c is not None:
                last_temp_c = temp_c
            else:
                temp_c = last_temp_c

            outdoor_temp_c = self._coerce_float(row.get("outdoor_temp_c"))
            if outdoor_temp_c is not None:
                last_outdoor_temp_c = outdoor_temp_c
            else:
                outdoor_temp_c = last_outdoor_temp_c

            stage = self._active_stage_from_row(row)
            currently_heating = stage in {"heat_stage1", "heat_stage2", "alt_heat_stage1", "alt_heat_stage2"}
            currently_cooling = stage in {"cool_stage1", "cool_stage2"}

            if stage is not None:
                if active_run is None:
                    if temp_c is not None:
                        active_run = {
                            "stage": stage,
                            "start_time": row_time,
                            "start_temp_c": temp_c,
                            "outdoor_temps": [outdoor_temp_c] if outdoor_temp_c is not None else [],
                        }
                elif stage != active_run["stage"]:
                    end_temp_c = temp_c
                    if end_temp_c is not None:
                        dt_hours = (row_time - active_run["start_time"]).total_seconds() / 3600.0
                        if period_start <= row_time <= period_end and dt_hours > 0:
                            rate = (end_temp_c - float(active_run["start_temp_c"])) / dt_hours
                            self._append_stage_rate(rate_lists, str(active_run["stage"]), rate)
                            
                            cycle_outdoor = None
                            thermal_recovery_ratio = None
                            indoor_avg = (end_temp_c + float(active_run["start_temp_c"])) / 2.0
                            if active_run["outdoor_temps"]:
                                cycle_outdoor = sum(active_run["outdoor_temps"]) / len(active_run["outdoor_temps"])
                                indoor_outdoor_delta = indoor_avg - cycle_outdoor
                                if abs(indoor_outdoor_delta) >= 0.1:
                                    thermal_recovery_ratio = rate / indoor_outdoor_delta

                            if emit_points:
                                self._writer.write_indoor_dynamics(
                                    {
                                        "dev_idx": dev_idx,
                                        "serial": serial,
                                        "name": device_name,
                                        "resource_type": resource_type,
                                        "timestamp": row_time,
                                        "delta_deg_c_per_hr": rate,
                                        f"delta_deg_c_per_hr_{active_run['stage']}": rate,
                                        "outdoor_temp_c": cycle_outdoor if cycle_outdoor is not None else last_outdoor_temp_c,
                                        "indoor_temp_c": indoor_avg,
                                        "thermal_recovery_ratio": thermal_recovery_ratio,
                                        "heating": active_run["stage"] in {"heat_stage1", "heat_stage2", "alt_heat_stage1", "alt_heat_stage2"},
                                        "cooling": active_run["stage"] in {"cool_stage1", "cool_stage2"},
                                        "heat_stage1": active_run["stage"] == "heat_stage1",
                                        "heat_stage2": active_run["stage"] == "heat_stage2",
                                        "alt_heat_stage1": active_run["stage"] == "alt_heat_stage1",
                                        "alt_heat_stage2": active_run["stage"] == "alt_heat_stage2",
                                        "cool_stage1": active_run["stage"] == "cool_stage1",
                                        "cool_stage2": active_run["stage"] == "cool_stage2",
                                    }
                                )
                    active_run = None
                    if temp_c is not None:
                        active_run = {
                            "stage": stage,
                            "start_time": row_time,
                            "start_temp_c": temp_c,
                            "outdoor_temps": [outdoor_temp_c] if outdoor_temp_c is not None else [],
                        }
                elif outdoor_temp_c is not None:
                    active_run["outdoor_temps"].append(outdoor_temp_c)
            else:
                if active_run is not None and temp_c is not None:
                    dt_hours = (row_time - active_run["start_time"]).total_seconds() / 3600.0
                    if period_start <= row_time <= period_end and dt_hours > 0:
                        rate = (temp_c - float(active_run["start_temp_c"])) / dt_hours
                        self._append_stage_rate(rate_lists, str(active_run["stage"]), rate)
                        
                        cycle_outdoor = None
                        thermal_recovery_ratio = None
                        indoor_avg = (temp_c + float(active_run["start_temp_c"])) / 2.0
                        if active_run["outdoor_temps"]:
                            cycle_outdoor = sum(active_run["outdoor_temps"]) / len(active_run["outdoor_temps"])
                            indoor_outdoor_delta = indoor_avg - cycle_outdoor
                            if abs(indoor_outdoor_delta) >= 0.1:
                                thermal_recovery_ratio = rate / indoor_outdoor_delta

                        if emit_points:
                            self._writer.write_indoor_dynamics(
                                {
                                    "dev_idx": dev_idx,
                                    "serial": serial,
                                    "name": device_name,
                                    "resource_type": resource_type,
                                    "timestamp": row_time,
                                    "delta_deg_c_per_hr": rate,
                                    f"delta_deg_c_per_hr_{active_run['stage']}": rate,
                                    "outdoor_temp_c": cycle_outdoor if cycle_outdoor is not None else last_outdoor_temp_c,
                                    "indoor_temp_c": indoor_avg,
                                    "thermal_recovery_ratio": thermal_recovery_ratio,
                                    "heating": active_run["stage"] in {"heat_stage1", "heat_stage2", "alt_heat_stage1", "alt_heat_stage2"},
                                    "cooling": active_run["stage"] in {"cool_stage1", "cool_stage2"},
                                    "heat_stage1": active_run["stage"] == "heat_stage1",
                                    "heat_stage2": active_run["stage"] == "heat_stage2",
                                    "alt_heat_stage1": active_run["stage"] == "alt_heat_stage1",
                                    "alt_heat_stage2": active_run["stage"] == "alt_heat_stage2",
                                    "cool_stage1": active_run["stage"] == "cool_stage1",
                                    "cool_stage2": active_run["stage"] == "cool_stage2",
                                }
                            )
                active_run = None

            if not currently_heating and not currently_cooling:
                if temp_c is not None and outdoor_temp_c is not None:
                    if off_state is None:
                        off_state = {
                            "off_time": row_time,
                            "indoor_temp": temp_c,
                            "outdoor_temps": [outdoor_temp_c],
                        }
                    else:
                        off_state["outdoor_temps"].append(outdoor_temp_c)
            else:
                if (
                    off_state is not None
                    and temp_c is not None
                    and off_state.get("outdoor_temps")
                    and period_start <= row_time <= period_end
                ):
                    off_duration_hours = (row_time - off_state["off_time"]).total_seconds() / 3600.0
                    t_start = float(off_state["indoor_temp"])
                    t_end = temp_c
                    t_out = sum(off_state["outdoor_temps"]) / len(off_state["outdoor_temps"])
                    try:
                        ratio = (t_end - t_out) / (t_start - t_out)
                        if ratio > 0 and ratio != 1.0 and off_duration_hours > 0:
                            r_factor = -off_duration_hours / math.log(ratio)
                            r_factors.append(r_factor)
                            if emit_points:
                                self._writer.write_thermal_resistance(
                                    dev_idx=dev_idx,
                                    timestamp=row_time,
                                    hvac_mode="heating" if currently_heating else "cooling",
                                    r_factor=r_factor,
                                    indoor_start_c=t_start,
                                    indoor_end_c=t_end,
                                    outdoor_avg_c=t_out,
                                    off_duration_hours=off_duration_hours,
                                )
                    except (ZeroDivisionError, ValueError):
                        pass
                off_state = None

        dynamics = {
            key: (sum(values) / len(values) if values else None)
            for key, values in rate_lists.items()
        }
        return dynamics, r_factors

    def _refresh_device_index_map(self) -> None:
        """Refresh device_id -> dev_idx mapping used by thermostat/event queries."""
        try:
            self._device_index_map = self._writer.read_device_index_map()
        except Exception as exc:
            log.warning("device_index refresh failed: %s", exc)
            self._device_index_map = {}

    def _query_cycle_count(self, dev_idx: int | None, state_key: str, start: str, stop: str) -> float:
        """Return the number of completed state duration events for one state key."""
        if dev_idx is None:
            return 0.0

        flux = f"""
from(bucket: "{self._event_bucket}")
  |> range(start: {start}, stop: {stop})
  |> filter(fn: (r) => r._measurement == "state_duration_event")
  |> filter(fn: (r) => r.dev_idx == "{dev_idx}")
  |> filter(fn: (r) => r.state_key == "{state_key}")
  |> filter(fn: (r) => r._field == "duration_seconds")
  |> count()
"""
        try:
            tables = self._query_client.query(flux)
            for table in tables:
                for record in table.records:
                    val = record.get_value()
                    if val is not None:
                        return float(val)
        except Exception as exc:
            log.warning("cycle_count query (%s) failed: %s", state_key, exc)
        return 0.0

    def _query_cycle_counts(self, dev_idx: int | None, start: str, stop: str) -> dict[str, float]:
        """Return completed cycle counts for aggregate and per-stage HVAC states."""
        field_map = {
            "heating_cycle_count": "heating",
            "cooling_cycle_count": "cooling",
            "fan_cycle_count": "fan",
            "humidifier_cycle_count": "humidifier",
            "heating_stage1_cycle_count": "heat_stage1",
            "heating_stage2_cycle_count": "heat_stage2",
            "alt_heat_stage1_cycle_count": "alt_heat_stage1",
            "alt_heat_stage2_cycle_count": "alt_heat_stage2",
            "cooling_stage1_cycle_count": "cool_stage1",
            "cooling_stage2_cycle_count": "cool_stage2",
        }
        return {
            out_key: self._query_cycle_count(dev_idx, state_key, start, stop)
            for out_key, state_key in field_map.items()
        }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute and backfill computed_metrics windows")
    parser.add_argument(
        "--cadence-hours",
        type=int,
        default=24,
        help="Metrics window length in hours (default: 24)",
    )
    parser.add_argument(
        "--backfill-days",
        type=int,
        default=1,
        help="Number of days to backfill ending at now (default: 1)",
    )
    parser.add_argument(
        "--loop-interval-hours",
        type=int,
        default=24,
        help="Hours to step backwards between windows (default: 24)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute all windows but do not write computed_metrics points",
    )
    parser.add_argument(
        "--enable-duration-fallback",
        action="store_true",
        help="Allow raw thermostat fallback when state_duration_event rows are missing",
    )
    parser.add_argument(
        "--log-level",
        default=cfg.LOG_LEVEL,
        help="Logging level (default from config)",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    if args.cadence_hours <= 0:
        raise SystemExit("--cadence-hours must be > 0")
    if args.backfill_days <= 0:
        raise SystemExit("--backfill-days must be > 0")
    if args.loop_interval_hours <= 0:
        raise SystemExit("--loop-interval-hours must be > 0")

    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)-8s %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    influx = InfluxWriter(
        url=cfg.INFLUXDB_URL,
        token=cfg.INFLUXDB_TOKEN,
        org=cfg.INFLUXDB_ORG,
        bucket=cfg.INFLUXDB_BUCKET_ROOT,
    )
    computer = MetricsComputer(
        influx_url=cfg.INFLUXDB_URL,
        influx_token=cfg.INFLUXDB_TOKEN,
        influx_org=cfg.INFLUXDB_ORG,
        influx_bucket=cfg.INFLUXDB_BUCKET_V2_RAW,
        influx_writer=influx,
        enable_duration_fallback=args.enable_duration_fallback,
    )

    try:
        devices = computer.discover_devices()
        if not devices:
            log.warning("No devices found in devmap bucket; nothing to compute")
            return

        now_utc = datetime.now(tz=timezone.utc)
        total_hours = args.backfill_days * 24
        windows = max(1, (total_hours + args.loop_interval_hours - 1) // args.loop_interval_hours)
        anchor_end = _align_period_end(now_utc, args.cadence_hours)
        log.info(
            "Starting metrics backfill: cadence=%dh windows=%d loop_interval=%dh dry_run=%s duration_fallback=%s",
            args.cadence_hours,
            windows,
            args.loop_interval_hours,
            args.dry_run,
            args.enable_duration_fallback,
        )

        total_attempted = 0
        total_written = 0
        for i in range(windows):
            period_end = anchor_end - timedelta(hours=i * args.loop_interval_hours)
            attempted, written = computer.run_window(
                cadence_hours=args.cadence_hours,
                devices=devices,
                period_end=period_end,
                dry_run=args.dry_run,
            )
            total_attempted += attempted
            total_written += written
            log.info(
                "Window %d/%d end=%s attempted=%d written=%d",
                i + 1,
                windows,
                period_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
                attempted,
                written,
            )

        log.info(
            "Backfill complete: attempted=%d written=%d dry_run=%s",
            total_attempted,
            total_written,
            args.dry_run,
        )
    finally:
        influx.close()


if __name__ == "__main__":
    main()
