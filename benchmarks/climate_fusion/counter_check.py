"""Check software counter agreement in a dedicated process without a power backend."""

from __future__ import annotations

import json
import resource
import statistics
import subprocess
import sys
import time

from pisces.energy.operational_metrics import start_operational_measurement, stop_operational_measurement


def _cpu_seconds(who: int) -> float:
    usage = resource.getrusage(who)
    return usage.ru_utime + usage.ru_stime


def _busy() -> None:
    start = time.monotonic()
    while time.monotonic() - start < 0.15:
        sum(i * i for i in range(5_000))


def check_counters() -> dict:
    platform = stop_operational_measurement(start_operational_measurement(), scope="setup")["platform"]
    observations = {}
    for name, operation in (
        ("sleep", lambda: time.sleep(0.15)),
        ("cpu", _busy),
        ("child_cpu", lambda: subprocess.run(
            [sys.executable, "-m", "benchmarks.climate_fusion.counter_check", "--busy"], check=True, timeout=15,
        )),
    ):
        wall_before = time.monotonic_ns()
        cpu_before = _cpu_seconds(resource.RUSAGE_SELF)
        child_before = _cpu_seconds(resource.RUSAGE_CHILDREN)
        started = start_operational_measurement()
        operation()
        measured = stop_operational_measurement(started, scope="counter_check")
        wall_after = time.monotonic_ns()
        cpu_after = _cpu_seconds(resource.RUSAGE_SELF)
        observations[name] = {
            "measured": measured,
            "reference_wall_s": (wall_after - wall_before) / 1e9,
            "reference_self_cpu_s": cpu_after - cpu_before,
            "reference_child_cpu_s": _cpu_seconds(resource.RUSAGE_CHILDREN) - child_before,
        }
    overhead = []
    for _ in range(30):
        started = time.monotonic_ns()
        stop_operational_measurement(start_operational_measurement(), scope="empty_counter_check")
        overhead.append((time.monotonic_ns() - started) / 1e9)
    return {
        "platform": platform,
        "reference": "time.monotonic_ns and getrusage around the same in-process window",
        "observations": observations,
        "empty_monitor_median_wall_s": statistics.median(overhead),
        "empty_monitor_max_wall_s": max(overhead),
    }


if __name__ == "__main__":
    if sys.argv[1:] == ["--busy"]:
        _busy()
    else:
        print(json.dumps(check_counters(), indent=2, allow_nan=False))
