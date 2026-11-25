"""
nestgraph2 — entry point and scheduler.

Runs the event-driven collector (Observe stream) and schedules daily metrics computation.

Run directly:
  python -m src.main
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

import src.config as cfg
from src.collector_event import EventThermostatCollector
from src.influx_writer import InfluxWriter
from src.metrics import MetricsComputer

logging.basicConfig(
    level=getattr(logging, cfg.LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("nestgraph2")
_LIFECHECK_FILE = Path(os.getenv("LIFECHECK_FILE", "/tmp/nestgraph2.lifecheck"))
_LIFECHECK_INTERVAL_SECONDS = max(15, int(os.getenv("LIFECHECK_INTERVAL_SECONDS", "60")))


def _touch_lifecheck() -> None:
    _LIFECHECK_FILE.parent.mkdir(parents=True, exist_ok=True)
    _LIFECHECK_FILE.write_text(datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ\n"), encoding="utf-8")


def _lifecheck_loop(stop_event: threading.Event) -> None:
    import src.grpc_web as grpc_web

    # Initialize timestamp to current time at startup
    grpc_web.last_stream_activity_ts = time.time()

    # Dynamic stale threshold based on configured observe stream read timeout.
    # Scaled to 2.5x the timeout (min 5 mins) to prevent false positives during
    # low-frequency periods (like AWAY mode), or 3 hours if timeout is disabled.
    data_source = getattr(cfg, "CONNECT_NEST_DATA_SOURCE", "foyer")
    if data_source == "foyer":
        poll_interval = max(5.0, float(getattr(cfg, "CONNECT_NEST_FOYER_POLL_INTERVAL_SEC", 30.0)))
        stale_threshold = max(300, int(poll_interval * 10))
    else:
        observe_timeout = getattr(cfg, "CONNECT_NEST_OBSERVE_TIMEOUT_SEC", 120.0)
        if observe_timeout > 0:
            stale_threshold = max(300, int(observe_timeout * 2.5))
        else:
            stale_threshold = 10800  # 3 hours fallback

    _touch_lifecheck()
    log.debug("lifecheck: alive (heartbeat file=%s)", _LIFECHECK_FILE)
    while not stop_event.wait(_LIFECHECK_INTERVAL_SECONDS):
        elapsed = time.time() - grpc_web.last_stream_activity_ts
        if elapsed <= stale_threshold:
            _touch_lifecheck()
            log.debug("lifecheck: alive (heartbeat file=%s)", _LIFECHECK_FILE)
        else:
            log.warning(
                "lifecheck: stream has been inactive for %d seconds (limit %ds)! Skipping heartbeat touch.",
                int(elapsed),
                stale_threshold,
            )


def build_services(
    verbose_observe_decoded: bool = False,
) -> tuple[InfluxWriter, EventThermostatCollector, MetricsComputer]:
    log.info("Initialising services")

    influx = InfluxWriter(
        url=cfg.INFLUXDB_URL,
        token=cfg.INFLUXDB_TOKEN,
        org=cfg.INFLUXDB_ORG,
        bucket=cfg.INFLUXDB_BUCKET_ROOT,
    )

    collector = EventThermostatCollector(
        influx=influx,
        verbose_observe_decoded=verbose_observe_decoded,
    )

    metrics_computer = MetricsComputer(
        influx_url=cfg.INFLUXDB_URL,
        influx_token=cfg.INFLUXDB_TOKEN,
        influx_org=cfg.INFLUXDB_ORG,
        influx_bucket=cfg.INFLUXDB_BUCKET_V2_RAW,
        influx_writer=influx,
    )
    return influx, collector, metrics_computer


def job_compute_metrics(computer: MetricsComputer) -> None:
    log.info("→ compute daily metrics (24h)")
    try:
        devices = computer.discover_devices()
        if not devices:
            log.warning("Metrics compute: no devices found in devmap bucket — skipping")
            return
        computer.run_for_cadence(24, devices)
    except Exception as exc:
        log.error("Metrics compute failed: %s", exc, exc_info=True)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="nestgraph2 collector runner")
    parser.add_argument(
        "--verbose-observe-decoded",
        action="store_true",
        help="Log the full decoded Observe snapshot JSON for each frame",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    influx, collector, metrics_computer = build_services(
        verbose_observe_decoded=bool(args.verbose_observe_decoded),
    )

    tz = pytz.timezone(cfg.TZ)
    scheduler = BackgroundScheduler(timezone=tz)

    collector_thread = threading.Thread(
        target=collector.run_forever,
        name="event_collector",
        daemon=True,
    )
    collector_thread.start()

    scheduler.add_job(
        job_compute_metrics,
        trigger=CronTrigger(hour=2, minute=0, timezone=tz),
        args=[metrics_computer],
        id="metrics_daily",
        name="Daily computed metrics (24h)",
        max_instances=1,
        coalesce=True,
    )

    shutdown_event = threading.Event()

    def _shutdown(_sig: int, _frame: Any) -> None:
        shutdown_event.set()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    log.info("Scheduler started in event mode — event collector active, metrics daily at 02:00")
    scheduler.start()

    lifecheck_thread = threading.Thread(
        target=_lifecheck_loop,
        args=(shutdown_event,),
        name="lifecheck",
        daemon=True,
    )
    lifecheck_thread.start()

    try:
        while not shutdown_event.is_set():
            time.sleep(0.5)
    except (KeyboardInterrupt, SystemExit):
        pass

    log.info("Shutting down …")
    collector.stop()
    scheduler.shutdown(wait=False)
    shutdown_event.set()
    lifecheck_thread.join(timeout=5)
    influx.close()
    sys.exit(0)


if __name__ == "__main__":
    main()
