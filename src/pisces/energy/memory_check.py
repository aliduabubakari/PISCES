"""Controlled OCI page-charge and bounded OOM checks, separate from workflow benchmarks."""

from __future__ import annotations

import argparse
import json
import mmap
from pathlib import Path
import subprocess
import sys
from datetime import datetime, timezone

from .cgroup_metrics import measure_cgroup


MIB = 1024**2
OOM_LIMIT = 128 * MIB
ALLOCATIONS = (32 * MIB, 96 * MIB, 256 * MIB)
HEADROOM = 16 * MIB
ROOT = Path("/sys/fs/cgroup")


def _counter(name: str) -> int:
    return int((ROOT / name).read_text(encoding="ascii"))


def _touch(region: mmap.mmap, start: int, stop: int) -> None:
    for offset in range(start, stop, mmap.PAGESIZE):
        region[offset] = 1


def _allocate_for_oom() -> None:
    # Anonymous mapping, faulted in 8 MiB steps, with a fixed 160 MiB ceiling.
    with mmap.mmap(-1, 160 * MIB, flags=mmap.MAP_PRIVATE | mmap.MAP_ANONYMOUS) as region:
        for offset in range(0, 160 * MIB, 8 * MIB):
            _touch(region, offset, offset + 8 * MIB)


def observe_memory(case: str) -> dict:
    if case == "oom":
        child, telemetry = measure_cgroup(lambda: subprocess.run(
            [sys.executable, "-I", "-c", "from pisces.energy.memory_check import _allocate_for_oom; _allocate_for_oom()"],
            check=False, timeout=10, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        ))
        return {"case": case, "allocation_ceiling_bytes": 160 * MIB, "allocation_child_returncode": child.returncode,
                "container_telemetry": telemetry}

    size = int(case)
    if size not in ALLOCATIONS:
        raise ValueError("Unknown memory fixture")

    def allocate() -> dict:
        baseline = _counter("memory.current")
        peak_before = _counter("memory.peak")
        with mmap.mmap(-1, size, flags=mmap.MAP_PRIVATE | mmap.MAP_ANONYMOUS) as region:
            _touch(region, 0, size)
            held = _counter("memory.current")
        return {"allocation_bytes": size, "baseline_bytes": baseline, "held_bytes": held,
                "released_bytes": _counter("memory.current"), "peak_before_bytes": peak_before}

    reference, telemetry = measure_cgroup(allocate)
    return {"case": case, "reference": reference, "container_telemetry": telemetry}


def assess_memory(case: str, payload: dict, outcome: str | None) -> list[str]:
    if payload.get("case") != case:
        return [outcome or payload.get("error") or "fixture_not_received"]
    telemetry = payload.get("container_telemetry") or {}
    if case == "oom":
        return [] if (outcome == "container_oom_killed" and (telemetry.get("oom_kill_delta") or 0) >= 1
                      and payload.get("allocation_child_returncode") == -9) else ["oom_not_confirmed_by_cgroup_and_child"]
    if outcome:
        return [outcome]
    reading = payload["reference"]
    size = int(case)
    baseline, held, released = (reading[key] for key in ("baseline_bytes", "held_bytes", "released_bytes"))
    peak = telemetry.get("memory_peak_bytes")
    issues = []
    if abs(held - baseline - size) > HEADROOM or abs(released - baseline) > HEADROOM:
        issues.append("charged_pages_or_release_outside_bound")
    if peak is None or not max(held, reading["peak_before_bytes"]) <= peak <= max(held, reading["peak_before_bytes"]) + HEADROOM:
        issues.append("lifetime_peak_outside_bound")
    return issues


def run_check(image: str, repetitions: int) -> dict:
    from .oci_runtime import OCIError, _execute, _exit_outcome, _inspect_engine, _local_engine

    cases = [*map(str, ALLOCATIONS), "oom"]
    report = {"started_at_utc": datetime.now(timezone.utc).isoformat(), "status": "unavailable", "attempts": [],
              "repetitions_per_case": repetitions, "allocation_levels_bytes": ALLOCATIONS,
              "absolute_charge_headroom_bytes": HEADROOM, "oom_fixture_memory_limit_bytes": OOM_LIMIT,
              "reference": "Known page-touched anonymous allocations and cgroup memory.current; software accounting only"}
    try:
        with _local_engine() as cli:
            environment = _inspect_engine(cli, image)
            report["environment"] = environment
            for index in range(1, repetitions + 1):
                for case in cases:
                    result = _execute(cli, environment["image_id"], None, 15, memory_case=case)
                    outcome = _exit_outcome(result)
                    payload = result.pop("payload", None) or {}
                    issues = assess_memory(case, payload, outcome)
                    if result.get("cleanup_error"):
                        issues.append(result["cleanup_error"])
                    report["attempts"].append({"case": case, "repetition": index, "issues": issues,
                                               "outcome": outcome, "lifecycle": result, "reading": payload})
                    if result.get("cleanup_error"):
                        report["status"] = "failed"
                        return report
    except (OCIError, OSError, ValueError, subprocess.TimeoutExpired) as exc:
        report["reason"] = str(exc) if isinstance(exc, OCIError) else "memory_fixture_unavailable"
        return report
    report["status"] = "failed" if any(item["issues"] for item in report["attempts"]) else "passed"
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--oci-image", required=True)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    if not 1 <= args.repetitions <= 10:
        parser.error("Specify 1-10 repetitions")
    report = run_check(args.oci_image, args.repetitions)
    with args.out.open("x", encoding="utf-8") as stream:
        json.dump(report, stream, indent=2, allow_nan=False)
        stream.write("\n")
    return int(report["status"] != "passed")


if __name__ == "__main__":
    raise SystemExit(main())
