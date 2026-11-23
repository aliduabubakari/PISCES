"""Run and summarize same-baseline Climate Fusion OCI telemetry sessions."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from hashlib import sha256
import json
import math
from pathlib import Path
import platform
import re
import statistics
from typing import Any

from pisces.energy.oci_runtime import check_oci, run_oci
from pisces.energy.output_equivalence import ORACLE_VERSION
from pisces.energy.telemetry_check import CASES, _check_window
from pisces.energy.workload_contract import LoadedWorkloadContract, load_workload_contract


METRICS = ("wall_clock_s", "cpu_s")
ENVIRONMENTS = ("docker_desktop_vm", "local_linux_engine")
SCOPE = "container_worker_window_including_descendants_setup_and_validation"


def _write(path: Path, value: Any) -> None:
    with path.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2, allow_nan=False)
        stream.write("\n")


def _read(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _profile(environment: dict) -> dict:
    return {key: value for key, value in environment.items() if key != "image_reference"}


def _confirmed_profile(image: str, confirmation: Path, expected_environment: str) -> tuple[dict, str]:
    if expected_environment == "local_linux_engine" and platform.system() != "Linux":
        raise ValueError("The native Linux profile requires a Linux controller on the same worker")
    probe = check_oci(image)
    environment = probe.get("environment") or {}
    if not probe.get("ready") or environment.get("environment_class") != expected_environment:
        raise ValueError(f"Expected ready {expected_environment} OCI profile; got {probe.get('reason') or environment}")
    profile = _profile(environment)
    evidence = confirmation.read_bytes()
    if not _passing_confirmation(json.loads(evidence), profile):
        raise ValueError("A passing conformance confirmation from the same OCI profile is required")
    return profile, sha256(evidence).hexdigest()


def _load_baseline(bundle: Path) -> LoadedWorkloadContract:
    workload = load_workload_contract(contract_path=bundle / "contract.json",
                                      binding_path=bundle / "baseline.binding.json",
                                      artifact_path=bundle / "baseline.py")
    if (workload.contract["schema_version"] != "2.0" or workload.contract.get("pipeline_id") != "climate-fusion"
            or workload.binding.get("binding_id") != "baseline" or workload.binding.get("target") != "airflow"
            or any(column.get("absolute_tolerance", 0) for output in workload.contract["outputs"]
                   for column in output["columns"])):
        raise ValueError("A Climate Fusion baseline with an exact output contract is required")
    return workload


def _passing_confirmation(report: dict, profile: dict) -> bool:
    count = report.get("repetitions_per_case")
    summary = report.get("summary") or {}
    limits = report.get("tolerances_absolute_s") or {}
    return (
        report.get("schema_version") == "1.0" and report.get("status") == "passed"
        and report.get("mode") == "confirmation" and _profile(report.get("environment") or {}) == profile
        and type(count) is int and count >= 1 and set(summary) == set(CASES)
        and all(type(limits.get(key)) in (int, float) and 0 < limits[key] < math.inf for key in ("wall_s", "cpu_s"))
        and all(item.get("attempted") == item.get("with_readings") == item.get("without_issues") == count
                and all(_within_limit((item.get("absolute_error_s", {}).get(key) or {}).get("max"), limits[key])
                        for key in ("wall_s", "cpu_s"))
                for item in summary.values())
    )


def _within_limit(value: Any, limit: float) -> bool:
    return type(value) in (int, float) and 0 <= value <= limit


def _assess(payload: dict, profile: dict, provenance: dict) -> dict:
    runtime = payload.get("runtime_measurement") or {}
    proof = runtime.get("output_assertions") or {}
    references = provenance["reference_sha256"]
    results = proof.get("results") or []
    fingerprints = {result.get("logical_name"): result.get("semantic_sha256") for result in results}
    verified = (
        runtime.get("execution_outcome") == "completed" and runtime.get("execution_adapter_used") == "oci"
        and runtime.get("workload_contract") == provenance and proof.get("passed") is True
        and proof.get("oracle_version") == ORACLE_VERSION
        and all(proof.get(key) == provenance[key] for key in ("contract_sha256", "artifact_sha256", "binding_sha256"))
        and all((runtime.get(key) or {}).get("passed") is True for key in ("task_coverage", "input_consumption"))
        and len(results) == len(references) and fingerprints.keys() == references.keys()
        and all(result.get("passed") is True and result.get("reference_sha256") == references[result["logical_name"]]
                and re.fullmatch(r"[a-f0-9]{64}", result.get("semantic_sha256") or "") for result in results)
    )
    same_profile = _profile(runtime.get("oci_environment") or {}) == profile
    bounds, warmup = runtime.get("bounds") or {}, runtime.get("warmup") or {}
    single_run = (
        bounds.get("repetitions_completed") == bounds.get("maximum_repetitions") == 1
        and warmup.get("completed_repetitions") == warmup.get("requested_repetitions") == 0
        and warmup.get("included_in_measurement_window") is False
        and runtime.get("modeled_energy") is None
    )
    telemetry = runtime.get("container_telemetry") or {}
    readings = {key: telemetry.get(key) for key in METRICS}
    issues = []
    if not same_profile:
        issues.append("oci_profile_missing_or_changed")
    if not verified:
        issues.append("output_not_verified")
    if not single_run:
        issues.append("not_a_single_unmodeled_run")
    if telemetry.get("scope") != SCOPE:
        issues.append("container_worker_scope_required")
    if (runtime.get("container_lifecycle") or {}).get("cleanup_error"):
        issues.append("container_cleanup_failed")
    # Validate each metric independently; a missing CPU reading need not hide valid wall time.
    metric_issues = {key: [] for key in METRICS}
    window_issues = _check_window({**telemetry, **readings})
    for key, value in readings.items():
        if type(value) not in (int, float) or not 0 < value < math.inf:
            metric_issues[key].append("missing_or_invalid_reading")
    for issue in window_issues:
        affected = METRICS if issue == "invalid_counter_window" else (
            ("wall_clock_s",) if issue.startswith("wall") else ("cpu_s",)
        )
        for key in affected:
            metric_issues[key].append(issue)
    eligible = {key: value for key, value in readings.items() if not issues and not metric_issues[key]}
    return {"output_verified": verified, "fingerprints": fingerprints if verified else {},
            "issues": issues, "metric_issues": metric_issues, "eligible_readings_s": eligible}


def _session_summary(attempts: list[dict], planned: int) -> dict:
    verified = [attempt for attempt in attempts if attempt["output_verified"]]
    fingerprints = {tuple(sorted(attempt["fingerprints"].items())) for attempt in verified}
    metrics = {}
    for key in METRICS:
        values = [attempt["eligible_readings_s"][key] for attempt in attempts if key in attempt["eligible_readings_s"]]
        spread = None
        if values and len(fingerprints) == 1:
            center = statistics.median(values)
            mad = statistics.median(abs(value - center) for value in values)
            spread = {"median": center, "mad": mad, "min": min(values), "max": max(values),
                      "mad_fraction_of_median": mad / center}
        metrics[key] = {"eligible": len(values), "coverage": len(values) / planned, "spread_s": spread}
    complete = len(verified) == planned and len(fingerprints) == 1 and all(
        metric["eligible"] == planned for metric in metrics.values()
    )
    return {"status": "complete" if complete else "incomplete", "planned": planned, "attempted": len(attempts),
            "verified": len(verified), "fingerprints": dict(next(iter(fingerprints))) if len(fingerprints) == 1 else None,
            "metrics": metrics}


def run_session(bundle: Path, result_dir: Path, *, image: str, confirmation: Path,
                expected_environment: str, repetitions: int, timeout_s: int) -> dict:
    workload = _load_baseline(bundle)
    profile, confirmation_sha256 = _confirmed_profile(image, confirmation, expected_environment)
    provenance = workload.provenance()
    result_dir.mkdir(parents=True, exist_ok=False)
    _write(result_dir / "manifest.json", {
        "schema_version": "1.0", "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "oci_profile": profile, "workload": provenance, "repetitions": repetitions, "timeout_s": timeout_s,
        "controller_platform": platform.platform(), "cache_policy": "fresh_containers_and_database_os_cache_uncontrolled",
        "conformance_report_sha256": confirmation_sha256, "metric_scope": "container_worker", "metrics": METRICS,
    })
    attempts = []
    for index in range(1, repetitions + 1):
        started = datetime.now(timezone.utc).isoformat()
        payload = run_oci(workload, image=profile["image_id"], timeout_s=timeout_s)
        attempt = {"index": index, "started_at_utc": started, **_assess(payload, profile, provenance), "payload": payload}
        _write(result_dir / f"attempt-{index:03d}.json", attempt)
        attempts.append(attempt)
    result = _session_summary(attempts, repetitions)
    _write(result_dir / "summary.json", result)
    return result


def compare_sessions(directories: list[Path]) -> dict:
    if len(directories) < 2 or len({directory.resolve() for directory in directories}) != len(directories):
        raise ValueError("At least two distinct sessions are required")
    manifests = [_read(path / "manifest.json") for path in directories]
    summaries = [_read(path / "summary.json") for path in directories]
    identity = ("oci_profile", "workload", "cache_policy", "metric_scope", "metrics", "controller_platform", "timeout_s",
                "conformance_report_sha256")
    if any(any(manifest.get(key) != manifests[0].get(key) for key in identity) for manifest in manifests[1:]):
        raise ValueError("Cannot compare sessions with different workload or execution profiles")
    complete = all(summary["status"] == "complete" for summary in summaries) and all(
        summary["fingerprints"] == summaries[0]["fingerprints"] for summary in summaries[1:]
    )
    between = {}
    if complete:
        for key in METRICS:
            medians = [session["metrics"][key]["spread_s"]["median"] for session in summaries]
            center = statistics.median(medians)
            between[key] = {"session_medians_s": medians, "min_s": min(medians), "max_s": max(medians),
                            "span_fraction_of_median": (max(medians) - min(medians)) / center}
    return {"status": "descriptive_only" if complete else "incomplete", "oci_profile": manifests[0]["oci_profile"],
            "workload": manifests[0]["workload"], "between_session_dispersion": between,
            "sessions": [{"directory": str(path), "manifest": manifest, **summary}
                         for path, manifest, summary in zip(directories, manifests, summaries)]}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run")
    run.add_argument("bundle", type=Path)
    run.add_argument("result_dir", type=Path)
    run.add_argument("--oci-image", required=True)
    run.add_argument("--confirmation", type=Path, required=True)
    run.add_argument("--environment", choices=ENVIRONMENTS, required=True)
    run.add_argument("--repetitions", type=int, default=5)
    run.add_argument("--timeout-s", type=int, default=60)
    summarize = commands.add_parser("summarize")
    summarize.add_argument("sessions", type=Path, nargs="+")
    summarize.add_argument("--out", type=Path)
    args = parser.parse_args()
    try:
        if args.command == "run":
            if not (2 <= args.repetitions <= 50 and 1 <= args.timeout_s <= 300):
                raise ValueError("Specify 2-50 repetitions and a timeout of 1-300 seconds")
            result = run_session(args.bundle, args.result_dir, image=args.oci_image, confirmation=args.confirmation,
                                 expected_environment=args.environment, repetitions=args.repetitions, timeout_s=args.timeout_s)
        else:
            result = compare_sessions(args.sessions)
            if args.out:
                _write(args.out, result)
    except (OSError, ValueError) as exc:
        parser.error(str(exc))
    print(json.dumps(result, indent=2, allow_nan=False))
    return int(result["status"] == "incomplete")


if __name__ == "__main__":
    raise SystemExit(main())
