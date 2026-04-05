"""
Dump latest device index metadata as a table.

Shows data from:
    - bucket <root>_v2_devmap, measurement device_index (includes where_id)

Usage:
  python utilities/dump_device_mappings.py
  python utilities/dump_device_mappings.py --json
  python utilities/dump_device_mappings.py --sort device_id
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Allow running from repo root
sys.path.insert(0, str(Path(__file__).parent.parent))

from influxdb_client import InfluxDBClient

import src.config as cfg


def _iso(ts: datetime | None) -> str:
    if ts is None:
        return ""
    return ts.astimezone(timezone.utc).isoformat(timespec="seconds")


def _age_min(ts: datetime | None) -> str:
    if ts is None:
        return ""
    delta = datetime.now(timezone.utc) - ts.astimezone(timezone.utc)
    return f"{delta.total_seconds() / 60.0:.1f}"


def _query_device_index(client: InfluxDBClient) -> dict[str, dict[str, Any]]:
    """Return latest device_index fields keyed by device_id, including where_id."""
    query = f'''
from(bucket: "{cfg.INFLUXDB_BUCKET_V2_DEVMAP}")
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

    rows: dict[str, dict[str, Any]] = {}
    result = client.query_api().query(org=cfg.INFLUXDB_ORG, query=query)

    for table in result:
        for record in table.records:
            device_id = str(record.values.get("device_id") or "")
            field_name = str(record.get_field() or "")
            if not device_id or not field_name:
                continue

            row = rows.setdefault(device_id, {"device_id": device_id})
            value = record.get_value()
            ts = record.get_time()

            if field_name == "dev_idx":
                try:
                    row["dev_idx"] = int(value)
                except (TypeError, ValueError):
                    row["dev_idx"] = value
            else:
                row[field_name] = value

            if isinstance(ts, datetime):
                prev = row.get("idx_ts")
                if not isinstance(prev, datetime) or ts > prev:
                    row["idx_ts"] = ts

    return rows



def _merge_rows(index_rows: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    """Convert index rows to output format (device_mapping merged into device_index)."""
    device_ids = sorted(index_rows.keys())
    merged: list[dict[str, Any]] = []

    for device_id in device_ids:
        idx = index_rows.get(device_id, {})
        row: dict[str, Any] = {
            "dev_idx": idx.get("dev_idx", ""),
            "device_id": device_id,
            "device_name": idx.get("device_name", ""),
            "where_name": idx.get("device_name", ""),
            "device_label": idx.get("device_label", ""),
            "device_label_short": idx.get("device_label_short", ""),
            "resource_type": idx.get("resource_type", ""),
            "where_id": idx.get("where_id", ""),
            "idx_ts": _iso(idx.get("idx_ts")),
        }
        merged.append(row)

    return merged


def _sort_rows(rows: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    if key == "device_id":
        return sorted(rows, key=lambda r: str(r.get("device_id", "")))
    if key == "device_name":
        return sorted(rows, key=lambda r: str(r.get("device_name", "")))

    def _idx_sort_value(row: dict[str, Any]) -> tuple[int, str]:
        val = row.get("dev_idx")
        if isinstance(val, int):
            return (val, str(row.get("device_id", "")))
        return (10**9, str(row.get("device_id", "")))

    return sorted(rows, key=_idx_sort_value)


def _print_table(rows: list[dict[str, Any]]) -> None:
    columns = [
        "dev_idx",
        "device_id",
        "device_name",
        "device_label_short",
        "where_name",
        "where_id",
        "resource_type",
        "idx_ts",
    ]

    widths: dict[str, int] = {}
    for col in columns:
        max_data_len = max((len(str(r.get(col, ""))) for r in rows), default=0)
        widths[col] = max(len(col), max_data_len)

    def _render_line(values: dict[str, Any]) -> str:
        return " | ".join(str(values.get(col, "")).ljust(widths[col]) for col in columns)

    header = {col: col for col in columns}
    print(_render_line(header))
    print("-+-".join("-" * widths[col] for col in columns))
    for row in rows:
        print(_render_line(row))


def main() -> None:
    parser = argparse.ArgumentParser(description="Dump device mapping/index table from InfluxDB")
    parser.add_argument(
        "--sort",
        choices=("dev_idx", "device_id", "device_name"),
        default="dev_idx",
        help="Sort output rows",
    )
    parser.add_argument("--json", action="store_true", help="Print JSON instead of table")
    args = parser.parse_args()

    client = InfluxDBClient(url=cfg.INFLUXDB_URL, token=cfg.INFLUXDB_TOKEN, org=cfg.INFLUXDB_ORG)
    try:
        index_rows = _query_device_index(client)
    finally:
        client.close()

    rows = _merge_rows(index_rows)
    rows = _sort_rows(rows, args.sort)

    if args.json:
        print(json.dumps(rows, indent=2, sort_keys=False))
        return

    print(
        f"Rows: {len(rows)}  "
        f"(device_index: {len(index_rows)})"
    )
    _print_table(rows)


if __name__ == "__main__":
    main()
