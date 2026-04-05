"""Delete thermal_resistance, indoor_dynamics, and computed_metrics before backfill."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import src.config as cfg
from influxdb_client import InfluxDBClient

client = InfluxDBClient(url=cfg.INFLUXDB_URL, token=cfg.INFLUXDB_TOKEN, org=cfg.INFLUXDB_ORG)
delete_api = client.delete_api()

START = "2000-01-01T00:00:00Z"
STOP  = "2030-01-01T00:00:00Z"
ORG   = cfg.INFLUXDB_ORG
ROOT  = cfg.INFLUXDB_BUCKET_ROOT

raw_bucket    = f"{ROOT}_v2_raw"
rollup_bucket = f"{ROOT}_v2_rollup"
model_bucket  = f"{ROOT}_v2_model"

for measurement, bucket in [
    ("thermal_resistance", raw_bucket),
    ("indoor_dynamics",    raw_bucket),
    ("computed_metrics",   rollup_bucket),
    ("computed_metrics",   model_bucket),
]:
    predicate = f'_measurement="{measurement}"'
    print(f"Deleting {measurement!r} from {bucket!r} ...", end=" ", flush=True)
    delete_api.delete(START, STOP, predicate, bucket=bucket, org=ORG)
    print("done.")

client.close()
print("All deletions complete.")
