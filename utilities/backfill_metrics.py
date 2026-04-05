"""
Backfill computed metrics from existing InfluxDB thermostat data.

Use this to:
  - Recompute metrics after importing historical data (e.g. from nestgraph TSV)
  - Regenerate metrics if the computation logic changes
  - Fill gaps caused by downtime

Queries the `thermostat` measurement in daily windows between --start and --end,
then writes `computed_metrics` points.

Usage:
  python utilities/backfill_metrics.py --start 2025-01-01 --end 2025-12-31
  python utilities/backfill_metrics.py --start 2025-01-01 --end 2025-12-31 --dry-run
  python utilities/backfill_metrics.py --start 2025-01-01 --end 2025-12-31 --device <serial>
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

# Allow running from repo root
sys.path.insert(0, str(Path(__file__).parent.parent))

import src.config as cfg
from src.influx_writer import InfluxWriter
from src.metrics import MetricsComputer


class _DryRunWriter(InfluxWriter):
    """InfluxWriter subclass that prints instead of writing."""

    def __init__(self) -> None:  # type: ignore[override]
        super().__init__(
            url=cfg.INFLUXDB_URL,
            token=cfg.INFLUXDB_TOKEN,
            org=cfg.INFLUXDB_ORG,
            bucket=cfg.INFLUXDB_BUCKET_ROOT,
        )

    def write_computed_metrics(self, device_id, device_name, cadence, period_end, metrics):
        print(
            f"  [DRY RUN] computed_metrics  device={device_id}  cadence={cadence}"
            f"  period_end={period_end.date()}"
        )
        for k, v in metrics.items():
            if v is not None:
                print(f"            {k} = {v:.2f}" if isinstance(v, float) else f"            {k} = {v}")


    def _write(self, point, bucket=None):
        pass

    def close(self):
        pass


def _iter_days(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def _resolve_devices(influx_url, influx_token, influx_org, influx_bucket, device_filter=None):
    """Query influx for distinct device_id tags in the thermostat measurement."""
    from influxdb_client import InfluxDBClient

    client = InfluxDBClient(url=influx_url, token=influx_token, org=influx_org)
    query_api = client.query_api()
    flux = f"""
import "influxdata/influxdb/schema"
schema.tagValues(
  bucket: "{influx_bucket}",
  tag: "device_id",
  predicate: (r) => r._measurement == "thermostat"
)
"""
    try:
        tables = query_api.query(flux)
        serials = [r.get_value() for t in tables for r in t.records]
    except Exception as exc:
        print(f"Warning: could not auto-discover devices: {exc}")
        serials = []
    finally:
        client.close()

    if device_filter:
        serials = [s for s in serials if s == device_filter]

    # Try to get names from tag
    flux2 = f"""
import "influxdata/influxdb/schema"
schema.tagValues(
  bucket: "{influx_bucket}",
  tag: "device_name",
  predicate: (r) => r._measurement == "thermostat"
)
"""
    names: dict[str, str] = {}
    try:
        client2 = InfluxDBClient(url=influx_url, token=influx_token, org=influx_org)
        tables2 = client2.query_api().query(flux2)
        # Not directly paired, but we'll do a best-effort query per device
        client2.close()
    except Exception:
        pass

    # Resolve name per serial
    devices = []
    for serial in serials:
        devices.append({"serial": serial, "name": serial})  # name fallback = serial
    return devices


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill nestgraph2 computed metrics")
    parser.add_argument("--start",  required=True, help="Start date YYYY-MM-DD (inclusive)")
    parser.add_argument("--end",    required=True, help="End date YYYY-MM-DD (inclusive)")
    parser.add_argument("--device", default=None,  help="Limit to one device serial number")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would be written without writing to InfluxDB")
    args = parser.parse_args()

    start = date.fromisoformat(args.start)
    end   = date.fromisoformat(args.end)
    if end < start:
        print("Error: --end must be >= --start")
        sys.exit(1)

    print(f"Backfill range : {start} → {end}  ({(end - start).days + 1} day(s))")
    print(f"Dry run        : {args.dry_run}")
    print(f"Config         : {cfg._CONFIG_PATH.resolve()}")

    if args.dry_run:
        writer: InfluxWriter = _DryRunWriter()  # type: ignore[assignment]
    else:
        writer = InfluxWriter(
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
        influx_writer=writer,
    )
    computer._refresh_device_index_map()

    # Discover devices from the devmap bucket so backfills use the same mapping
    print("\nDiscovering devices in thermostat measurement …")
    devices = computer.discover_devices()
    if args.device:
        devices = [device for device in devices if device["serial"] == args.device]
    if not devices:
        print("No devices found. Check --device or that thermostat data exists.")
        sys.exit(1)
    print(f"Devices: {[d['serial'] for d in devices]}")

    # Iterate days
    total_days = (end - start).days + 1
    print(f"\nProcessing {total_days} day window(s) …\n")

    for i, day in enumerate(_iter_days(start, end), 1):
        period_end = datetime(day.year, day.month, day.day, 23, 59, 59, tzinfo=timezone.utc)
        period_start = period_end - timedelta(hours=23, minutes=59, seconds=59)

        print(f"  [{i:4d}/{total_days}] {day}", end="  ", flush=True)

        # Use internal method directly to control the time window
        for device in devices:
            dev_idx = computer._device_index_map.get(device["serial"], int(device.get("dev_idx", 0)))
            thermostat_data = computer._query_thermostat_rows(dev_idx, period_start, period_end)
            if thermostat_data:
                computer._compute_and_write(
                    device["serial"],
                    str(device.get("device_name") or device.get("name") or device["serial"]),
                    dev_idx,
                    str(device.get("resource_type") or ""),
                    "1d",
                    period_start,
                    period_end,
                    dry_run=args.dry_run,
                )
                print(f"{device['serial']}: {len(thermostat_data)} pts", end="  ")
            else:
                print(f"{device['serial']}: no data", end="  ")
        print()

    if not args.dry_run:
        writer.close()

    print("\nBackfill complete.")


if __name__ == "__main__":
    main()
