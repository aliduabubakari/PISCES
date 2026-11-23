"""Compare independent Climate Fusion scaling sessions without pooling their runs."""

from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
import statistics

from pisces.energy.task_timing import SOURCE

from .repeatability import _read, _write
from .scaling import METRICS, SCHEMA_VERSION, _summarize, assess_attempt, validate_loads


_IDENTITY = ("oci_profile", "controller_platform", "conformance_report_sha256", "workloads", "sources",
             "timeout_s", "cache_policy", "task_timing_source", "repetitions", "metrics")


def _timestamp(value: str) -> datetime:
    stamp = datetime.fromisoformat(value)
    if stamp.utcoffset() is None:
        raise ValueError("Expected a timezone-aware session timestamp")
    return stamp


def _load_session(directory: Path) -> dict:
    manifest = _read(directory / "manifest.json")
    if (manifest.get("schema_version") != SCHEMA_VERSION or manifest.get("task_timing_source") != SOURCE
            or manifest.get("metrics") != list(METRICS) or manifest.get("loads_are_total_rows") is not True
            or any(key not in manifest for key in _IDENTITY)):
        raise ValueError(f"Unsupported instrumented session manifest: {directory}")
    start = _timestamp(manifest["started_at_utc"])
    schedule = manifest["round_order"]
    loads = validate_loads(int(rows) for rows in manifest["workloads"])
    if (not 2 <= len(schedule) == manifest["repetitions"] <= 20
            or any(sorted(order) != list(loads) for order in schedule)
            or set(manifest["workloads"]) != set(map(str, loads))):
        raise ValueError(f"Invalid scaling schedule or workloads: {directory}")
    files = {path.name for path in directory.glob("round-*-rows-*.json")}
    attempts = []
    last_start = start
    missing = []
    fingerprints = {row: set() for row in loads}
    for index, order in enumerate(schedule, 1):
        for rows in order:
            name = f"round-{index:02d}-rows-{rows}.json"
            if name not in files:
                missing.append(name)
                continue
            files.remove(name)
            record = _read(directory / name)
            timestamp = _timestamp(record["started_at_utc"])
            if record.get("round") != index or record.get("rows") != rows or timestamp <= last_start:
                raise ValueError(f"Attempt does not match declared schedule: {directory / name}")
            last_start = timestamp
            scored = assess_attempt(record["payload"], manifest["oci_profile"], manifest["workloads"][str(rows)], rows)
            if scored["output_verified"]:
                fingerprints[rows].add(scored["fingerprints"]["fusion"])
            attempts.append({"rows": rows, **scored})
    if files:
        raise ValueError(f"Unexpected attempt files in {directory}: {', '.join(sorted(files))}")
    return {"directory": str(directory), "manifest": manifest, "missing_attempt_files": missing,
            "last_attempt_started_at_utc": last_start.isoformat(),
            "fingerprints_by_load": {rows: sorted(values) for rows, values in fingerprints.items()},
            "summary": _summarize(attempts, len(schedule), loads=loads)}


def _spread(values: list[float], *, relative: bool = True) -> dict:
    low, high, center = min(values), max(values), statistics.median(values)
    return {"session_values": values, "span": high - low,
            "span_fraction_of_median": (high - low) / abs(center) if relative and center else None}


def compare_sessions(directories: list[Path]) -> dict:
    if len(directories) < 2 or len({path.resolve() for path in directories}) != len(directories):
        raise ValueError("At least two distinct scaling session directories are required")
    sessions = sorted((_load_session(path) for path in directories),
                      key=lambda session: _timestamp(session["manifest"]["started_at_utc"]))
    manifest = sessions[0]["manifest"]
    for session in sessions[1:]:
        changed = [key for key in _IDENTITY if session["manifest"][key] != manifest[key]]
        if changed:
            raise ValueError(f"Scaling session profile mismatch: {', '.join(changed)}")
    for previous, current in zip(sessions, sessions[1:]):
        if _timestamp(previous["last_attempt_started_at_utc"]) >= _timestamp(current["manifest"]["started_at_utc"]):
            raise ValueError("Session start must follow the preceding session's last attempt start")

    summaries = [s["summary"] for s in sessions]
    loads = validate_loads(int(rows) for rows in manifest["workloads"])
    reasons = []
    if any(s["status"] != "descriptive_only" for s in summaries):
        reasons.append("incomplete_session_coverage")
    for rows in loads:
        if len({tuple(s["fingerprints_by_load"][rows]) for s in sessions}) != 1:
            reasons.append(f"output_fingerprints_differ_at_{rows}_rows")
    slopes, medians = {}, {}
    if not reasons:
        for key in METRICS[:-1]:
            slopes[key] = _spread([s["time_fits_descriptive_only"][key]["slope_s_per_100k_rows"] for s in summaries],
                                  relative=False)
        for rows in loads:
            medians[rows] = {key: _spread([s["loads"][rows]["metrics"][key]["median"] for s in summaries])
                             for key in METRICS if all(s["loads"][rows]["metrics"][key]["coverage"] == 1
                                                      for s in summaries)}
    return {"schema_version": "1.0", "status": "incomplete" if reasons else "descriptive_only", "reasons": reasons,
            "oci_profile": manifest["oci_profile"], "sessions": sessions, "slope_spread_s_per_100k_rows": slopes,
            "load_median_spread": medians,
            "limitations": "No pooled fit or accuracy claim. Host identity and session separation beyond recorded timestamps are operator-controlled."}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sessions", type=Path, nargs="+")
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    try:
        report = compare_sessions(args.sessions)
        _write(args.out, report)
    except (OSError, ValueError, TypeError, KeyError) as exc:
        parser.error(str(exc))
    print(args.out)
    return int(report["status"] != "descriptive_only")


if __name__ == "__main__":
    raise SystemExit(main())
