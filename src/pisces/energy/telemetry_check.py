"""Check the OCI wall and CPU collector without running a workflow or reading data."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import statistics
import subprocess
import sys
import time
from typing import Any

from .cgroup_metrics import measure_cgroup


CASES = ("empty", "short_sleep", "sleep", "self_cpu", "child_cpu")
_SLEEPS = {"short_sleep": 0.05, "sleep": 0.25}
_BUSY_WALL_S = 0.25
_MIN_CONTROL_CPU_S = 0.05


def _busy() -> None:
    deadline = time.monotonic() + _BUSY_WALL_S
    while time.monotonic() < deadline:
        sum(i * i for i in range(5_000))


def observe_case(case: str) -> dict[str, Any]:
    """Worker-side reference instruments only the controlled operation."""
    import resource

    if case not in CASES:
        raise ValueError("Unknown telemetry fixture")

    def usage_s(who: int) -> float:
        usage = resource.getrusage(who)
        return usage.ru_utime + usage.ru_stime

    def reference() -> dict[str, float]:
        self_before = usage_s(resource.RUSAGE_SELF)
        child_before = usage_s(resource.RUSAGE_CHILDREN)
        start = time.clock_gettime_ns(time.CLOCK_MONOTONIC)
        if case in _SLEEPS:
            time.sleep(_SLEEPS[case])
        elif case == "self_cpu":
            _busy()
        elif case == "child_cpu":
            subprocess.run([sys.executable, "-I", "-c", "from pisces.energy.telemetry_check import _busy; _busy()"],
                           check=True, timeout=5)
        stop = time.clock_gettime_ns(time.CLOCK_MONOTONIC)
        return {
            "wall_s": (stop - start) / 1e9,
            "self_cpu_s": usage_s(resource.RUSAGE_SELF) - self_before,
            "reaped_child_cpu_s": usage_s(resource.RUSAGE_CHILDREN) - child_before,
        }

    started_ns = time.perf_counter_ns()
    reference_values, measured = measure_cgroup(reference)
    elapsed_s = (time.perf_counter_ns() - started_ns) / 1e9
    return {"case": case, "reference": reference_values, "container_telemetry": measured,
            "collector_call_wall_s": elapsed_s}


def _reading(value: Any) -> bool:
    return type(value) in (int, float) and 0 <= value < math.inf


def _check_window(telemetry: dict[str, Any]) -> list[str]:
    window = telemetry.get("counter_window") or {}
    before, after = (window.get(label) or {} for label in ("cpu_before", "cpu_after"))
    stamps = (before.get("read_started_ns"), before.get("read_finished_ns"), window.get("wall_started_ns"),
              window.get("wall_finished_ns"), after.get("read_started_ns"), after.get("read_finished_ns"))
    issues = []
    if not all(type(stamp) is int for stamp in stamps) or list(stamps) != sorted(stamps):
        issues.append("invalid_counter_window")
    elif telemetry["wall_clock_s"] != (stamps[3] - stamps[2]) / 1e9:
        issues.append("wall_reading_does_not_match_raw_window")
    initial, final = before.get("usage_usec"), after.get("usage_usec")
    if type(initial) is not int or type(final) is not int or not 0 <= initial <= final:
        issues.append("invalid_cpu_counter_endpoints")
    elif telemetry["cpu_s"] != (final - initial) / 1e6:
        issues.append("cpu_reading_does_not_match_raw_endpoints")
    return issues


def assess(observation: dict[str, Any], case: str, tolerances: dict[str, float] | None) -> dict[str, Any]:
    if observation.get("case") != case:
        return {"issues": [observation.get("error") or "fixture_mismatch_or_missing"]}
    ref = observation.get("reference") or {}
    telemetry = observation.get("container_telemetry") or {}
    wall, cpu = telemetry.get("wall_clock_s"), telemetry.get("cpu_s")
    ref_wall, self_cpu, child_cpu = (ref.get(key) for key in ("wall_s", "self_cpu_s", "reaped_child_cpu_s"))
    elapsed = observation.get("collector_call_wall_s")
    if not all(map(_reading, (wall, cpu, ref_wall, self_cpu, child_cpu, elapsed))):
        return {"issues": ["missing_or_invalid_wall_or_cpu_reading"]}
    expected_cpu = self_cpu + child_cpu
    errors = {"wall_s": wall - ref_wall, "cpu_s": cpu - expected_cpu}
    issues = _check_window(telemetry)
    if case in _SLEEPS and ref_wall < _SLEEPS[case]:
        issues.append("sleep_control_not_exercised")
    if case == "self_cpu" and self_cpu < _MIN_CONTROL_CPU_S:
        issues.append("self_cpu_control_not_exercised")
    if case == "child_cpu" and child_cpu < _MIN_CONTROL_CPU_S:
        issues.append("child_cpu_control_not_exercised")
    if tolerances is not None:
        for key, error in errors.items():
            if abs(error) > tolerances[key]:
                issues.append(f"{key}_outside_tolerance")
        # A loose absolute tolerance must not let a collector that lost the child pass.
        if case == "child_cpu" and cpu <= self_cpu + child_cpu / 2:
            issues.append("child_cpu_not_accounted_for")
    return {"issues": issues, "signed_error_s": errors, "reference_total_cpu_s": expected_cpu,
            "collector_outside_wall_s": elapsed - wall}


def _spread(values: list[float]) -> dict[str, float] | None:
    if not values:
        return None
    center = statistics.median(values)
    return {"median": center, "max": max(values), "min": min(values),
            "median_absolute_deviation": statistics.median(abs(value - center) for value in values)}


def run_telemetry_check(image: str, *, repetitions: int, tolerances: dict[str, float] | None) -> dict[str, Any]:
    from .oci_runtime import OCIError, _execute, _exit_outcome, _inspect_engine, _local_engine

    report: dict[str, Any] = {
        "schema_version": "1.0", "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "mode": "calibration" if tolerances is None else "confirmation",
        "tolerances_absolute_s": tolerances, "repetitions_per_case": repetitions,
        "reference": {"wall": "clock_gettime_ns(CLOCK_MONOTONIC) inside the fixture",
                      "cpu": "getrusage: user+system for self and waited-for children",
                      "scope": "controlled fixture only; collector surrounds reference instrumentation",
                      "limitations": "Separate software accounting interfaces, not independent physical sensors"},
        "fixture_parameters": {"sleeps_s": _SLEEPS, "busy_target_wall_s": _BUSY_WALL_S,
                               "minimum_reference_busy_cpu_s": _MIN_CONTROL_CPU_S},
        "not_validated_by_this_check": ["memory", "block_io", "energy", "carbon", "workflow_repeatability"],
    }
    observations: dict[str, list[dict[str, Any]]] = {case: [] for case in CASES}
    report["observations"] = observations
    try:
        with _local_engine() as cli:
            environment = _inspect_engine(cli, image)
            report["environment"] = environment
            for repetition in range(1, repetitions + 1):
                for case in CASES:
                    try:
                        result = _execute(cli, environment["image_id"], None, 15, telemetry_case=case)
                        payload = result.pop("payload", None) or {}
                        if not isinstance(payload, dict):
                            payload = {}
                        outcome = _exit_outcome({**result, "payload": payload}) or result.get("cleanup_error")
                    except (OCIError, OSError, ValueError, subprocess.TimeoutExpired) as exc:
                        payload, result = {}, {}
                        outcome = str(exc) if isinstance(exc, OCIError) else "oci_execution_unavailable"
                    checks = assess(payload, case, tolerances) if outcome is None else {"issues": [outcome]}
                    observations[case].append({"repetition": repetition, "lifecycle": result,
                                               "reading": payload, **checks})
    except (OCIError, OSError, ValueError, subprocess.TimeoutExpired) as exc:
        report.update(status="unavailable", reason=str(exc) if isinstance(exc, OCIError) else "oci_probe_failed")
        return report

    report["summary"] = {
        case: {
            "attempted": len(attempts),
            "with_readings": sum("signed_error_s" in attempt for attempt in attempts),
            "without_issues": sum(not attempt["issues"] for attempt in attempts),
            "absolute_error_s": {
                key: _spread([abs(attempt["signed_error_s"][key]) for attempt in attempts if "signed_error_s" in attempt])
                for key in ("wall_s", "cpu_s")
            },
            "collector_outside_wall_s": _spread([attempt["collector_outside_wall_s"] for attempt in attempts
                                                 if "collector_outside_wall_s" in attempt]),
        }
        for case, attempts in observations.items()
    }
    issues = any(attempt["issues"] for attempts in observations.values() for attempt in attempts)
    report["status"] = "failed" if issues else ("calibration_only" if tolerances is None else "passed")
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--oci-image", required=True, help="Locally available immutable image ID or digest")
    parser.add_argument("--repetitions", type=int, default=5, help="Fresh containers per fixture (1-50)")
    parser.add_argument("--wall-tolerance-s", type=float, help="Predeclared maximum absolute wall error in seconds")
    parser.add_argument("--cpu-tolerance-s", type=float, help="Predeclared maximum absolute CPU error in seconds")
    parser.add_argument("--out", type=Path, help="Write the JSON report to a file instead of standard output")
    args = parser.parse_args()
    if not 1 <= args.repetitions <= 50:
        parser.error("--repetitions must be between 1 and 50")
    limits = args.wall_tolerance_s, args.cpu_tolerance_s
    tolerances = None
    if limits != (None, None):
        if not all(_reading(limit) and limit > 0 for limit in limits):
            parser.error("confirmation requires both tolerances as finite positive seconds")
        tolerances = dict(zip(("wall_s", "cpu_s"), limits))
    report = run_telemetry_check(args.oci_image, repetitions=args.repetitions, tolerances=tolerances)
    output = json.dumps(report, indent=2, allow_nan=False)
    if args.out:
        args.out.write_text(output + "\n", encoding="utf-8")
    else:
        print(output)
    return 0 if report["status"] in {"passed", "calibration_only"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
