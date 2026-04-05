"""
Stop running nestgraph2 Python collector processes.

Default target is processes launched via `-m src.main`.
Optionally include `-m src.connect_nest` as well.

Usage:
  python utilities/stop_main.py
  python utilities/stop_main.py --dry-run
  python utilities/stop_main.py --include-connect-nest
"""

from __future__ import annotations

import argparse
import json
import platform
import subprocess
import sys
from typing import Any


def _windows_find(include_connect_nest: bool) -> list[dict[str, Any]]:
    filters = ["$_.CommandLine -match 'src\\.main'"]
    if include_connect_nest:
        filters.append("$_.CommandLine -match 'src\\.connect_nest'")
    where_expr = " -or ".join(filters)

    ps = (
        "Get-CimInstance Win32_Process | "
        "Where-Object { $_.Name -match '^python(\\.exe)?$' -and "
        f"({where_expr}) }} | "
        "Select-Object ProcessId, Name, CommandLine | "
        "ConvertTo-Json -Compress"
    )

    proc = subprocess.run(
        ["powershell", "-NoProfile", "-Command", ps],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or "PowerShell query failed")

    out = proc.stdout.strip()
    if not out:
        return []

    data = json.loads(out)
    if isinstance(data, dict):
        return [data]
    if isinstance(data, list):
        return data
    return []


def _posix_find(include_connect_nest: bool) -> list[dict[str, Any]]:
    proc = subprocess.run(
        ["ps", "-eo", "pid=,comm=,args="],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or "ps query failed")

    targets: list[dict[str, Any]] = []
    for raw in proc.stdout.splitlines():
        line = raw.strip()
        if not line:
            continue
        parts = line.split(maxsplit=2)
        if len(parts) < 3:
            continue
        pid_s, name, cmd = parts
        name_l = name.lower()
        if "python" not in name_l:
            continue
        if "src.main" in cmd or (include_connect_nest and "src.connect_nest" in cmd):
            try:
                pid = int(pid_s)
            except ValueError:
                continue
            targets.append({"ProcessId": pid, "Name": name, "CommandLine": cmd})
    return targets


def _find_targets(include_connect_nest: bool) -> list[dict[str, Any]]:
    if platform.system().lower().startswith("win"):
        return _windows_find(include_connect_nest)
    return _posix_find(include_connect_nest)


def _kill_target(pid: int) -> tuple[bool, str]:
    if platform.system().lower().startswith("win"):
        proc = subprocess.run(
            ["taskkill", "/PID", str(pid), "/T", "/F"],
            capture_output=True,
            text=True,
            check=False,
        )
        ok = proc.returncode == 0
        msg = (proc.stdout or proc.stderr).strip()
        return ok, msg

    proc = subprocess.run(["kill", "-TERM", str(pid)], capture_output=True, text=True, check=False)
    ok = proc.returncode == 0
    msg = (proc.stdout or proc.stderr).strip()
    return ok, msg


def main() -> int:
    parser = argparse.ArgumentParser(description="Stop running src.main collector processes")
    parser.add_argument(
        "--include-connect-nest",
        action="store_true",
        help="Also stop src.connect_nest processes",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print matching processes without terminating",
    )
    args = parser.parse_args()

    try:
        targets = _find_targets(args.include_connect_nest)
    except Exception as exc:
        print(f"error: could not enumerate processes: {exc}")
        return 1

    if not targets:
        print("No matching processes found.")
        return 0

    print(f"Found {len(targets)} matching process(es):")
    for t in targets:
        pid = t.get("ProcessId")
        cmd = str(t.get("CommandLine") or "")
        print(f"  PID {pid}: {cmd}")

    if args.dry_run:
        return 0

    failed = 0
    for t in targets:
        pid = int(t.get("ProcessId"))
        ok, msg = _kill_target(pid)
        if ok:
            print(f"Stopped PID {pid}")
        else:
            failed += 1
            print(f"Failed to stop PID {pid}: {msg}")

    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
