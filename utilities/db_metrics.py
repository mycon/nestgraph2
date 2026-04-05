"""
Quick InfluxDB metrics snapshot for nestgraph2 buckets.

Prints:
- Per-bucket total point counts over a time window
- Per-bucket point counts for key measurements (thermostat, indoor_sensor, etc.)

Usage:
    python utilities/db_metrics.py
    python utilities/db_metrics.py --hours 168
    python utilities/db_metrics.py --json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent))

from influxdb_client import InfluxDBClient

import src.config as cfg


def _flux_duration(hours: int) -> str:
    if hours <= 0:
        return "0s"
    return f"-{hours}h"


def _range_start_for_bucket(bucket: str, hours: int) -> str:
    # device_index uses fixed epoch timestamp (0) for upsert semantics.
    # A sliding window like -24h will always miss those points.
    if bucket == cfg.INFLUXDB_BUCKET_V2_DEVMAP:
        return "0"
    return _flux_duration(hours)


def _query_total_points(client: InfluxDBClient, bucket: str, hours: int) -> int:
    query = f'''
from(bucket: "{bucket}")
    |> range(start: {_range_start_for_bucket(bucket, hours)})
  |> count(column: "_value")
'''
    total = 0
    tables = client.query_api().query(org=cfg.INFLUXDB_ORG, query=query)
    for table in tables:
        for record in table.records:
            val = record.get_value()
            if isinstance(val, (int, float)):
                total += int(val)
    return total


def _query_measurement_counts(
    client: InfluxDBClient,
    bucket: str,
    measurements: list[str],
    hours: int,
) -> dict[str, int]:
    counts: dict[str, int] = {m: 0 for m in measurements}

    if not measurements:
        return counts

    filters = " or ".join([f'r._measurement == "{m}"' for m in measurements])
    query = f'''
from(bucket: "{bucket}")
    |> range(start: {_range_start_for_bucket(bucket, hours)})
  |> filter(fn: (r) => {filters})
  |> group(columns: ["_measurement"])
  |> count(column: "_value")
'''

    tables = client.query_api().query(org=cfg.INFLUXDB_ORG, query=query)
    for table in tables:
        for record in table.records:
            measurement = str(record.values.get("_measurement") or "")
            val = record.get_value()
            if measurement in counts and isinstance(val, (int, float)):
                counts[measurement] += int(val)

    return counts


def collect_metrics(hours: int) -> dict[str, Any]:
    buckets = [
        cfg.INFLUXDB_BUCKET_V2_RAW,
        cfg.INFLUXDB_BUCKET_V2_EVENT,
        cfg.INFLUXDB_BUCKET_V2_MODEL,
        cfg.INFLUXDB_BUCKET_V2_DEVMAP,
        cfg.INFLUXDB_BUCKET_V2_ROLLUP,
    ]

    key_measurements_by_bucket: dict[str, list[str]] = {
        cfg.INFLUXDB_BUCKET_V2_RAW: [
            "thermostat",
            "indoor_sensor",
            "indoor_dynamics",
            "weather",
            "hvac_cycle",
            "energy_daily",
            "thermal_resistance",
        ],
        cfg.INFLUXDB_BUCKET_V2_EVENT: ["state_duration_event"],
        cfg.INFLUXDB_BUCKET_V2_MODEL: ["computed_metrics"],
        cfg.INFLUXDB_BUCKET_V2_DEVMAP: ["device_index"],
        cfg.INFLUXDB_BUCKET_V2_ROLLUP: [],
    }

    out: dict[str, Any] = {
        "window_hours": hours,
        "influx": {
            "url": cfg.INFLUXDB_URL,
            "org": cfg.INFLUXDB_ORG,
            "bucket_root": cfg.INFLUXDB_BUCKET_ROOT,
        },
        "buckets": {},
    }

    client = InfluxDBClient(
        url=cfg.INFLUXDB_URL,
        token=cfg.INFLUXDB_TOKEN,
        org=cfg.INFLUXDB_ORG,
    )

    try:
        for bucket in buckets:
            bucket_row: dict[str, Any] = {
                "total_points": 0,
                "measurement_points": {},
                "error": None,
            }
            try:
                bucket_row["total_points"] = _query_total_points(client, bucket, hours)
                bucket_row["measurement_points"] = _query_measurement_counts(
                    client,
                    bucket,
                    key_measurements_by_bucket.get(bucket, []),
                    hours,
                )
            except Exception as exc:
                bucket_row["error"] = str(exc)

            out["buckets"][bucket] = bucket_row
    finally:
        client.close()

    return out


def print_human(metrics: dict[str, Any]) -> None:
    print(f"Influx DB Metrics (last {metrics['window_hours']}h)")
    print(f"URL: {metrics['influx']['url']}")
    print(f"Org: {metrics['influx']['org']}")
    print(f"Root bucket: {metrics['influx']['bucket_root']}")
    print()

    for bucket, row in metrics["buckets"].items():
        print(f"[{bucket}]")
        if row.get("error"):
            print(f"  error: {row['error']}")
            print()
            continue

        print(f"  total_points: {row['total_points']}")
        m = row.get("measurement_points", {})
        if m:
            for name in sorted(m.keys()):
                print(f"  {name}: {m[name]}")
        else:
            print("  measurement_points: (none configured)")
        print()


def main() -> None:
    parser = argparse.ArgumentParser(description="Print basic Influx bucket/measurement metrics")
    parser.add_argument("--hours", type=int, default=24, help="Lookback window in hours (default: 24)")
    parser.add_argument("--json", action="store_true", help="Print JSON output")
    args = parser.parse_args()

    metrics = collect_metrics(args.hours)

    if args.json:
        print(json.dumps(metrics, indent=2, sort_keys=False))
        return

    print_human(metrics)


if __name__ == "__main__":
    main()
