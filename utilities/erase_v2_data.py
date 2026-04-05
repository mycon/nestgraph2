"""
erase_v2_data — delete ALL data from all nestgraph_v2_* buckets.

Usage:
  python utilities/erase_v2_data.py [--config CONFIG]

Requires double confirmation before proceeding.

Options:
  --config PATH   path to config.json (default: config/config.json)
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone

from influxdb_client import InfluxDBClient


def _load_config(path: str) -> dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _delete_bucket_data(delete_api, org: str, bucket: str) -> None:
    """Delete all data points in the bucket. The bucket itself is preserved."""
    start = "1970-01-01T00:00:00Z"
    stop = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    delete_api.delete(start, stop, predicate='', bucket=bucket, org=org)
    print(f"  ✓ {bucket}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Erase all nestgraph_v2_* InfluxDB data.")
    parser.add_argument("--config", default="config/config.json")
    args = parser.parse_args()

    cfg = _load_config(args.config)
    influx = cfg["influxdb"]
    url   = influx["url"]
    token = influx["token"]
    org   = influx["org"]
    root  = influx["bucket"]

    buckets = [
        f"{root}_v2_raw",
        f"{root}_v2_model",
        f"{root}_v2_rollup",
        f"{root}_v2_event",
        #f"{root}_v2_devmap",
    ]

    print()
    print("=" * 60)
    print("  WARNING: DESTRUCTIVE OPERATION")
    print("=" * 60)
    print(f"  InfluxDB : {url}  org={org}")
    print(f"  Buckets  : {', '.join(buckets)}")
    print()
    print("  This will permanently delete ALL data in the above buckets.")
    print()

    answer1 = input("  Type 'erase' to confirm you want to delete all data: ").strip()
    if answer1 != "erase":
        print("Aborted.")
        sys.exit(0)

    answer2 = input("  Type the bucket root name to confirm (e.g. 'nestgraph'): ").strip()
    if answer2 != root:
        print(f"Expected '{root}'. Aborted.")
        sys.exit(0)

    print()
    print("Erasing data (buckets are preserved)...")
    with InfluxDBClient(url=url, token=token, org=org) as client:
        delete_api = client.delete_api()
        for bucket in buckets:
            _delete_bucket_data(delete_api, org, bucket)

    print()
    print("Done. All data has been deleted from the nestgraph_v2_* buckets.")
    print("The buckets themselves remain intact.")


if __name__ == "__main__":
    main()
