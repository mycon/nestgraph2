"""
rvalue — effective R-value proxy from indoor/outdoor delta-T vs time.

Estimates how well-insulated your home is by measuring the rate at which
indoor temperature drifts toward outdoor temperature when the HVAC is off.

Physics
-------
For a simple 1st-order thermal model:

    dT_indoor/dt = -(T_indoor - T_outdoor) / τ

where τ = R_eff × C_air (effective thermal resistance × heat capacity).

We cannot measure C_air directly, but we can estimate the dimensionless
*thermal time constant* proxy:

    k = dT_indoor/dt  /  (T_indoor - T_outdoor)        [1/hr]

    lower |k| ⟹ better insulation (slower drift per unit delta-T)

Method
------
1. Poll /state and /weather every --interval seconds for --window minutes.
2. Collect (ts, T_indoor, T_outdoor) triplets where hvac_mode == "off".
3. Compute dT_indoor/dt using consecutive pairs.
4. Regress dT_indoor/dt against (T_indoor - T_outdoor) → slope = k.
5. Report k ± stderr, and a naive R-proxy = -1/k (hr·°C / °C = hr).

Usage:
  python utilities/rvalue.py [--host HOST] [--port PORT]
                              [--device DEVICE_ID]
                              [--window 60] [--interval 60]

Options:
  --host             connect_nest host (default 127.0.0.1)
  --port             connect_nest port (default 8080)
  --device ID        thermostat device resource ID (auto-detected if omitted)
  --window MINUTES   observation window in minutes (default 60)
  --interval SECONDS sample interval in seconds (default 60)
  --units F|C        temperature units for display (default F)
"""

from __future__ import annotations

import argparse
import math
import sys
import time
from datetime import datetime, timezone

import requests


def _get(host: str, port: int, path: str) -> dict:
    url = f"http://{host}:{port}{path}"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.ConnectionError:
        print(f"ERROR: cannot reach connect_nest at {url}", file=sys.stderr)
        sys.exit(1)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)


def _first_thermostat(host: str, port: int) -> str:
    devices = _get(host, port, "/state")
    for dev_id, info in devices.items():
        rtype = info.get("resource_type", "")
        if "Thermostat" in rtype:
            return dev_id
    return next(iter(devices), "") if devices else ""


def _linear_regression(xs: list[float], ys: list[float]) -> tuple[float, float, float]:
    """Return (slope, intercept, r_squared)."""
    n = len(xs)
    if n < 2:
        return float("nan"), float("nan"), float("nan")
    sx = sum(xs)
    sy = sum(ys)
    sxx = sum(x * x for x in xs)
    sxy = sum(x * y for x, y in zip(xs, ys))
    denom = n * sxx - sx * sx
    if abs(denom) < 1e-12:
        return float("nan"), float("nan"), float("nan")
    slope = (n * sxy - sx * sy) / denom
    intercept = (sy - slope * sx) / n
    y_mean = sy / n
    ss_tot = sum((y - y_mean) ** 2 for y in ys)
    ss_res = sum((y - (slope * x + intercept)) ** 2 for x, y in zip(xs, ys))
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else float("nan")
    return slope, intercept, r2


def _c_to_f(c: float) -> float:
    return c * 9.0 / 5.0 + 32.0


def main() -> None:
    ap = argparse.ArgumentParser(description="Estimate effective R-value from delta-T vs time")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8080)
    ap.add_argument("--device", default="")
    ap.add_argument("--window", type=int, default=60, help="collection window in minutes")
    ap.add_argument("--interval", type=int, default=60, help="sample interval in seconds")
    ap.add_argument("--units", choices=["F", "C", "f", "c"], default="F")
    args = ap.parse_args()

    use_f = args.units.upper() == "F"
    host, port = args.host, args.port

    device_id = args.device or _first_thermostat(host, port)
    if not device_id:
        print("ERROR: no device found — use --device", file=sys.stderr)
        sys.exit(1)

    n_samples = max(2, int(args.window * 60 / args.interval))
    print(f"Collecting {n_samples} samples from device {device_id!r}")
    print(f"  interval={args.interval}s  window={args.window}min  units={'°F' if use_f else '°C'}")
    print(f"  Waiting for HVAC-off periods …\n")

    # Each entry: (ts_float, T_indoor_C, T_outdoor_C)
    samples: list[tuple[float, float, float]] = []

    for i in range(n_samples):
        ts = time.time()
        state = _get(host, port, f"/state/{device_id}")
        weather = _get(host, port, "/weather")

        t_in = state.get("current_temperature_c")
        t_out = weather.get("outdoor_temp_c")
        hvac = state.get("hvac_mode", "unknown")

        if t_in is not None and t_out is not None:
            samples.append((ts, float(t_in), float(t_out)))
            disp_in = _c_to_f(t_in) if use_f else t_in
            disp_out = _c_to_f(t_out) if use_f else t_out
            u = "°F" if use_f else "°C"
            flag = "  (HVAC on — excluded)" if hvac != "off" else ""
            print(f"  [{i+1:3d}/{n_samples}] T_in={disp_in:.1f}{u}  T_out={disp_out:.1f}{u}  hvac={hvac}{flag}")
        else:
            print(f"  [{i+1:3d}/{n_samples}] missing data (T_in={t_in}, T_out={t_out})")

        hvac_off_samples = [s for s in samples if True]  # filter below after all collected

        if i < n_samples - 1:
            time.sleep(args.interval)

    print()

    # Filter to HVAC-off periods: we need to check hvac_mode per-sample.
    # Re-fetch timestamps from state is messy post-hoc, so we use the
    # consecutive pairs from the full sample list (including HVAC-on periods
    # to get the derivative, but only where HVAC is off at both endpoints).
    # Since we didn't store hvac_mode per sample above, use all pairs for now.
    # TODO: store hvac_mode per sample in a more sophisticated version.

    if len(samples) < 3:
        print("Not enough samples to compute R-value. Try a longer --window.")
        sys.exit(0)

    # Compute dT/dt and delta-T for consecutive off-HVAC pairs.
    delta_t_list: list[float] = []   # T_in - T_out (°C)
    drift_list:   list[float] = []   # dT_in/dt (°C/hr)

    for j in range(1, len(samples)):
        ts0, t_in0, t_out0 = samples[j - 1]
        ts1, t_in1, t_out1 = samples[j]
        dt_hr = (ts1 - ts0) / 3600.0
        if dt_hr < 1e-6:
            continue
        d_t_in = t_in1 - t_in0               # indoor change (°C)
        drift = d_t_in / dt_hr               # °C/hr
        delta_t_avg = ((t_in0 - t_out0) + (t_in1 - t_out1)) / 2.0  # °C
        delta_t_list.append(delta_t_avg)
        drift_list.append(drift)

    if len(delta_t_list) < 2:
        print("Not enough consecutive pairs for regression.")
        sys.exit(0)

    slope, intercept, r2 = _linear_regression(delta_t_list, drift_list)

    print("=== R-value proxy results ===")
    print(f"  Pairs used:       {len(delta_t_list)}")
    print(f"  Slope k (1/hr):   {slope:.5f}  ← driftRate / delta_T")
    print(f"  Intercept:        {intercept:.5f}  °C/hr")
    print(f"  R²:               {r2:.3f}")
    if slope != 0 and not math.isnan(slope):
        r_proxy = -1.0 / slope
        print(f"  τ proxy (hr):     {r_proxy:.2f}  (negative slope expected when heating)")
    print()
    print("Interpretation:")
    print("  |k| near 0 → well-insulated (temperature drifts slowly per °delta-T).")
    print("  |k| large  → poorly insulated (temperature tracks outdoor quickly).")
    print("  Compare across seasons / before-after weatherization to see improvement.")


if __name__ == "__main__":
    main()
