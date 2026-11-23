"""Measure the same Climate Fusion OCI pipeline across declared total row counts."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import math
from pathlib import Path
import platform
import random
import statistics

from pisces.energy.oci_runtime import run_oci
from pisces.energy.task_timing import SOURCE

from .benchmark import SOURCES, build_bundle
from .repeatability import ENVIRONMENTS, _assess, _confirmed_profile, _load_baseline, _write


LOADS = (10_000, 100_000, 500_000, 1_000_000)
METRICS = ("task_wall_s", "task_thread_cpu_s", "execution_wall_s", "execution_process_cpu_s", "verification_wall_s", "worker_wall_s", "worker_cpu_s",
           "cgroup_lifetime_peak_bytes")
SCHEMA_VERSION = "1.1"


def validate_loads(loads) -> tuple[int, ...]:
    values = tuple(loads)
    if (not 3 <= len(values) <= 8 or any(type(value) is not int or not len(SOURCES) <= value <= 1_000_000
            or value % len(SOURCES) for value in values) or len(set(values)) != len(values)):
        raise ValueError("Choose 3-8 distinct total row counts, multiples of 5 between 5 and 1,000,000")
    return tuple(sorted(values))


def _task_reading(task: dict) -> dict:
    if (task.get("status"), task.get("source"), task.get("scope"), task.get("task_id")) != (
        "available", SOURCE, "task_callable_body", "fuse",
    ):
        return {}
    bounds = task.get("execution_wall_bounds_ns")
    if not isinstance(bounds, list) or len(bounds) != 2:
        return {}
    start, end, cpu_start, cpu_end = (task.get(key) for key in (
        "wall_start_ns", "wall_end_ns", "thread_cpu_start_ns", "thread_cpu_end_ns",
    ))
    if not all(type(value) is int and 0 <= value < 2**63 for value in (*bounds, start, end, cpu_start, cpu_end)):
        return {}
    if not bounds[0] <= start < end <= bounds[1] or not 0 < cpu_end - cpu_start <= end - start:
        return {}
    wall, cpu = (end - start) / 1e9, (cpu_end - cpu_start) / 1e9
    if task.get("wall_clock_s") != wall or task.get("thread_cpu_s") != cpu:
        return {}
    return {"wall_clock_s": wall, "thread_cpu_s": cpu}


def assess_attempt(payload: dict, profile: dict, provenance: dict, rows: int) -> dict:
    assessment = _assess(payload, profile, provenance)
    runtime = payload.get("runtime_measurement") or {}
    counts = [result.get("row_count") for result in (runtime.get("output_assertions") or {}).get("results", [])]
    if counts != [rows]:
        assessment["issues"].append("unexpected_output_row_count")
        assessment["output_verified"] = False
    readings = {}
    if not assessment["issues"]:
        execution = runtime.get("operational_metrics") or {}
        if execution.get("measurement_scope") != "bounded_execution_block":
            execution = {}
        telemetry = runtime.get("container_telemetry") or {}
        task = _task_reading(runtime.get("task_timing") or {})
        core = assessment["eligible_readings_s"]
        values = dict(zip(METRICS, (task.get("wall_clock_s"), task.get("thread_cpu_s"),
                                   execution.get("wall_clock_s"), execution.get("process_cpu_s"),
                                   runtime.get("output_verification_wall_s"), core.get("wall_clock_s"), core.get("cpu_s"),
                                   telemetry.get("memory_peak_bytes"))))
        readings = {metric: value for metric, value in values.items()
                    if type(value) in (int, float) and 0 < value < math.inf}
    return {**assessment, "readings": readings}


def _linear_fit(points: list[tuple[int, float]]) -> dict:
    xs, ys = zip(*points)
    x_center, y_center = statistics.mean(xs), statistics.mean(ys)
    slope = sum((x - x_center) * (y - y_center) for x, y in points) / sum((x - x_center)**2 for x in xs)
    intercept = y_center - slope * x_center
    total = sum((y - y_center)**2 for y in ys)
    residuals = [y - (intercept + slope * x) for x, y in points]
    return {"intercept_s": intercept, "slope_s_per_100k_rows": slope * 100_000,
            "r_squared": 1 - sum(value**2 for value in residuals) / total if total else None,
            "residuals_s": dict(zip(xs, residuals))}


def _summarize(attempts: list[dict], repetitions: int, *, loads: tuple[int, ...] = LOADS) -> dict:
    planned_loads = validate_loads(loads)
    loads = {}
    for rows in planned_loads:
        group = [attempt for attempt in attempts if attempt["rows"] == rows]
        verified = [attempt for attempt in group if attempt["output_verified"]]
        stable = len({attempt["fingerprints"].get("fusion") for attempt in verified}) == 1
        readings = {}
        for metric in METRICS:
            values = [attempt["readings"][metric] for attempt in group if metric in attempt["readings"]]
            median = statistics.median(values) if values and stable else None
            readings[metric] = {"coverage": len(values) / repetitions, "median": median,
                                "mad": statistics.median(abs(v - median) for v in values) if median is not None else None}
        loads[rows] = {"attempted": len(group), "verified": len(verified), "fingerprints_match": stable, "metrics": readings}
    fits = {}
    for metric in METRICS[:-1]:
        if all(load["verified"] == repetitions and load["fingerprints_match"]
               and load["metrics"][metric]["coverage"] == 1 for load in loads.values()):
            points = [(rows, load["metrics"][metric]["median"]) for rows, load in loads.items()]
            fits[metric] = {**_linear_fit(points), "monotone_medians": all(a[1] <= b[1] for a, b in zip(points, points[1:]))}
    return {"status": "descriptive_only" if len(fits) == len(METRICS) - 1 else "incomplete",
            "loads": loads, "time_fits_descriptive_only": fits,
            "memory_note": "Container lifetime high-water; collected here, evaluated separately by memory_check"}


def run_campaign(result_dir: Path, *, image: str, confirmation: Path, environment: str,
                 repetitions: int, timeout_s: int, seed: int, loads: tuple[int, ...] = LOADS) -> dict:
    loads = validate_loads(loads)
    profile, conformance = _confirmed_profile(image, confirmation, environment)
    result_dir.mkdir(parents=True, exist_ok=False)
    workloads = {}
    for rows in loads:
        bundle = result_dir / f"bundle-{rows}"
        build_bundle(bundle, rows_per_source=rows // len(SOURCES), scaling=True, task_timing=True)
        workloads[rows] = _load_baseline(bundle)
    rng = random.Random(seed)
    schedule = [rng.sample(loads, len(loads)) for _ in range(repetitions)]
    _write(result_dir / "manifest.json", {
        "schema_version": SCHEMA_VERSION, "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "oci_profile": profile, "conformance_report_sha256": conformance,
        "controller_platform": platform.platform(), "repetitions": repetitions,
        "metrics": METRICS,
        "loads_are_total_rows": True, "sources": SOURCES, "seed": seed, "round_order": schedule, "timeout_s": timeout_s,
        "cache_policy": "fresh_containers_and_database_os_cache_uncontrolled",
        "task_timing_source": SOURCE,
        "workloads": {rows: workload.provenance() for rows, workload in workloads.items()},
    })
    attempts = []
    for index, order in enumerate(schedule, 1):
        for rows in order:
            started = datetime.now(timezone.utc).isoformat()
            workload = workloads[rows]
            payload = run_oci(workload, image=profile["image_id"], timeout_s=timeout_s, task_timing_id="fuse")
            runtime = payload.get("runtime_measurement") or {}
            attempt = {"round": index, "rows": rows, "started_at_utc": started,
                       **assess_attempt(payload, profile, workload.provenance(), rows), "payload": payload}
            _write(result_dir / f"round-{index:02d}-rows-{rows}.json", attempt)
            attempts.append(attempt)
            if (runtime.get("container_lifecycle") or {}).get("cleanup_error"):
                summary = _summarize(attempts, repetitions, loads=loads)
                _write(result_dir / "summary.json", summary)
                return summary
    summary = _summarize(attempts, repetitions, loads=loads)
    _write(result_dir / "summary.json", summary)
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("result_dir", type=Path)
    parser.add_argument("--oci-image", required=True)
    parser.add_argument("--confirmation", required=True, type=Path)
    parser.add_argument("--environment", required=True, choices=ENVIRONMENTS)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--timeout-s", type=int, default=90)
    parser.add_argument("--seed", type=int, default=1729)
    parser.add_argument("--rows", type=int, nargs="+", default=list(LOADS), help="3-8 total row counts across all five sources")
    args = parser.parse_args()
    if not 2 <= args.repetitions <= 20 or not 1 <= args.timeout_s <= 300:
        parser.error("Specify 2-20 repetitions and a timeout of 1-300 seconds")
    try:
        loads = validate_loads(args.rows)
        summary = run_campaign(args.result_dir, image=args.oci_image, confirmation=args.confirmation,
                               environment=args.environment, repetitions=args.repetitions, timeout_s=args.timeout_s, seed=args.seed, loads=loads)
    except (OSError, ValueError) as exc:
        parser.error(str(exc))
    print(args.result_dir / "summary.json")
    return int(summary["status"] == "incomplete")


if __name__ == "__main__":
    raise SystemExit(main())
