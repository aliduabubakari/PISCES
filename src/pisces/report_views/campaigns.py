"""Presentation of saved campaign claims, without executing or pooling workloads."""

from __future__ import annotations

from functools import lru_cache
import hashlib
from importlib.resources import files
import json
from math import isfinite
from pathlib import Path
import statistics

from jsonschema import Draft202012Validator

from .metrics import EXECUTION, WORKER, MetricDefinition


SCALING_METRICS = {
    "task_wall_s": (
        MetricDefinition(
            "task.wall",
            "Task body wall time",
            "wall_clock_s",
            "s",
            "s",
            "Opt-in trusted task-callable wall interval. Not independent instrumentation of arbitrary code.",
            "trusted_benchmark_task_instrumentation_v1",
            "Fixture reads, transform and output close; excludes dispatch and verification.",
        ),
        "task_callable_body",
    ),
    "task_thread_cpu_s": (
        MetricDefinition(
            "task.cpu",
            "Task thread CPU",
            "thread_cpu_s",
            "CPU-s",
            "s",
            "Current-thread CPU time in the instrumented task; excludes child processes and background threads.",
            "time.thread_time_ns",
            "Trusted task wrapper only.",
        ),
        "task_callable_body",
    ),
    "execution_wall_s": (EXECUTION[0], "bounded_execution_block"),
    "execution_process_cpu_s": (EXECUTION[1], "bounded_execution_block"),
    "verification_wall_s": (
        MetricDefinition(
            "verification.wall",
            "Output verification wall time",
            "output_verification_wall_s",
            "s",
            "s",
            "Elapsed time for output verification, not pipeline-only runtime.",
            "time.perf_counter_ns",
            "Output verification interval.",
        ),
        "output_verification",
    ),
    "worker_wall_s": (WORKER[0], "container_worker_window"),
    "worker_cpu_s": (WORKER[1], "container_worker_window"),
    "cgroup_lifetime_peak_bytes": (
        WORKER[2],
        "container_cgroup_lifetime_including_page_cache",
    ),
}


@lru_cache(maxsize=1)
def campaign_schema() -> dict:
    return json.loads(
        files("pisces")
        .joinpath("schemas", "campaign-view-1.0.0.schema.json")
        .read_text(encoding="utf-8")
    )


def is_campaign(report: dict) -> bool:
    return (
        report.get("tool") == "pisces-load-campaign"
        or "time_fits_descriptive_only" in report
        or "slope_spread_s_per_100k_rows" in report
        or ("pipelines" in report and "telemetry_role" in report)
    )


def _number(value, *, nonnegative=True):
    if value is None:
        return None
    if (
        type(value) not in (int, float)
        or not isfinite(value)
        or abs(value) > 2**53 - 1
        or (nonnegative and value < 0)
    ):
        raise ValueError("invalid_campaign_number")
    return value


def _count(value, *, minimum=0):
    if value is None:
        return None
    if type(value) is not int or not minimum <= value <= 2**53 - 1:
        raise ValueError("invalid_campaign_count")
    return value


def _pointer(base, key):
    return base + "/" + str(key).replace("~", "~0").replace("/", "~1")


def _rows(keys):
    if not keys or len(keys) > 64:
        raise ValueError("campaign_requires_1_to_64_loads")
    result = []
    for key in keys:
        try:
            row = int(key)
        except (ValueError, TypeError) as exc:
            raise ValueError("invalid_load_rows") from exc
        if str(row) != str(key) or not 1 <= row <= 2**53 - 1:
            raise ValueError("invalid_load_rows")
        result.append(row)
    return sorted(result)


def _session(manifest, summary, *, base, pipeline, index):
    if (
        not isinstance(manifest, dict)
        or not isinstance(summary, dict)
        or not isinstance(summary.get("loads"), dict)
    ):
        raise ValueError("invalid_load_summary")
    if manifest and (
        manifest.get("schema_version") != "1.1"
        or manifest.get("loads_are_total_rows") is not True
    ):
        raise ValueError("unsupported_load_manifest")
    plan = manifest.get("workloads") if manifest else summary["loads"]
    if not isinstance(plan, dict):
        raise ValueError("missing_campaign_workload_plan")
    rows = _rows(plan)
    if set(map(str, summary["loads"])) - set(map(str, plan)):
        raise ValueError("unplanned_summary_load")
    repetitions = _count(manifest.get("repetitions"), minimum=1)
    profile = manifest.get("oci_profile", {})
    if not isinstance(profile, dict):
        raise ValueError("invalid_campaign_profile")
    workloads = manifest.get("workloads") or {}
    if not isinstance(workloads, dict) or any(
        not isinstance(value, dict)
        or not isinstance(value.get("target", "unknown"), str)
        for value in workloads.values()
    ):
        raise ValueError("invalid_campaign_workloads")
    targets = {value.get("target", "unknown") for value in workloads.values()}
    engine = next(iter(targets)) if len(targets) == 1 else "unknown"
    if not isinstance(engine, str):
        raise ValueError("invalid_campaign_engine")
    metrics = []
    fits = summary.get("time_fits_descriptive_only", {})
    if not isinstance(fits, dict):
        raise ValueError("invalid_campaign_fits")
    for key, (definition, scope) in SCALING_METRICS.items():
        points = []
        for row in rows:
            load = summary["loads"].get(str(row), {})
            if not isinstance(load, dict) or not isinstance(
                load.get("metrics", {}), dict
            ):
                raise ValueError("invalid_load_metrics")
            reading = load.get("metrics", {}).get(key, {})
            if not isinstance(reading, dict):
                raise ValueError("invalid_metric_summary")
            median, mad, coverage = (
                _number(reading.get(field)) for field in ("median", "mad", "coverage")
            )
            if coverage is not None and coverage > 1:
                raise ValueError("invalid_metric_coverage")
            attempted, verified = (
                _count(load.get(field)) for field in ("attempted", "verified")
            )
            if attempted is not None and (
                (verified is not None and verified > attempted)
                or (repetitions is not None and attempted > repetitions)
            ):
                raise ValueError("inconsistent_campaign_counts")
            if coverage is not None and repetitions is not None:
                available = coverage * repetitions
                if abs(available - round(available)) > 1e-7 or (
                    verified is not None and available > verified + 1e-7
                ):
                    raise ValueError("coverage_exceeds_verified_attempts")
            fingerprints = load.get("fingerprints_match")
            if fingerprints is not None and type(fingerprints) is not bool:
                raise ValueError("invalid_fingerprint_status")
            reason = (
                "load_not_reported"
                if not load
                else "metric_not_reported"
                if not reading
                else "reading_not_available"
                if median is None
                else "output_fingerprints_not_verified"
                if fingerprints is not True
                else "metric_coverage_missing"
                if coverage is None
                else "no_metric_coverage"
                if coverage == 0
                else None
            )
            path = _pointer(_pointer(base + "/loads", row) + "/metrics", key)
            points.append(
                {
                    "rows": row,
                    "median": median / definition.divisor if reason is None else None,
                    "raw_median": median,
                    "mad": mad / definition.divisor
                    if mad is not None and reason is None
                    else None,
                    "coverage": coverage,
                    "attempted": attempted,
                    "verified": verified,
                    "planned": repetitions,
                    "fingerprints_match": fingerprints,
                    "missing_reason": reason,
                    "pointer": path + "/median" if "median" in reading else None,
                    "fitted": None,
                }
            )
        fit = fits.get(key)
        complete = (
            len(rows) >= 3
            and repetitions is not None
            and all(
                point["coverage"] == 1
                and point["verified"] == repetitions
                and point["attempted"] == repetitions
                and point["fingerprints_match"] is True
                and point["median"] is not None
                for point in points
            )
        )
        fit_view = {
            "status": "not_available",
            "intercept": None,
            "slope_per_100k_rows": None,
            "r_squared": None,
            "reason": "memory_high_water_not_fitted"
            if key == "cgroup_lifetime_peak_bytes"
            else "manifest_required_for_fit"
            if not manifest
            else "incomplete_signal_coverage"
            if not complete
            else "fit_not_reported",
            "pointer": None,
        }
        if complete and key != "cgroup_lifetime_peak_bytes" and fit is not None:
            if not isinstance(fit, dict):
                raise ValueError("invalid_campaign_fit")
            intercept, slope, r_squared = (
                _number(fit.get(field), nonnegative=False)
                for field in ("intercept_s", "slope_s_per_100k_rows", "r_squared")
            )
            if intercept is not None and slope is not None:
                # A supplied fit is shown only when it describes these admitted medians.
                residual = sum(
                    (point["median"] - intercept - slope * point["rows"] / 100_000) ** 2
                    for point in points
                )
                constant = all(
                    point["median"] == points[0]["median"] for point in points
                )
                if constant and (slope != 0 or intercept != points[0]["median"]):
                    raise ValueError("fit_does_not_describe_summary_points")
                center = statistics.mean(point["median"] for point in points)
                total = sum((point["median"] - center) ** 2 for point in points)
                actual_r2 = 1 - residual / total if total else None
                if (r_squared is None) != (actual_r2 is None) or (
                    actual_r2 is not None and abs(actual_r2 - r_squared) > 1e-7
                ):
                    raise ValueError("fit_does_not_describe_summary_points")
                fit_view.update(
                    status="reported_descriptive",
                    intercept=intercept,
                    slope_per_100k_rows=slope,
                    r_squared=r_squared,
                    reason=None,
                    pointer=_pointer(base + "/time_fits_descriptive_only", key),
                )
                for point in points:
                    point["fitted"] = intercept + slope * point["rows"] / 100_000
        metrics.append(
            {
                "id": key,
                "label": {
                    "execution_wall_s": "Execution block wall time",
                    "execution_process_cpu_s": "Execution process CPU",
                    "worker_wall_s": "Worker wall time",
                    "worker_cpu_s": "Worker cgroup CPU",
                }.get(key, definition.label),
                "unit": definition.unit,
                "raw_unit": definition.raw_unit,
                "scope": scope,
                "definition": definition.definition,
                "source": definition.source,
                "points": points,
                "fit": fit_view,
            }
        )
    profile_id = (
        hashlib.sha256(
            json.dumps(
                {
                    key: manifest.get(key)
                    for key in (
                        "oci_profile",
                        "controller_platform",
                        "cache_policy",
                        "task_timing_source",
                    )
                },
                sort_keys=True,
            ).encode()
        ).hexdigest()
        if manifest
        else None
    )
    return {
        "id": f"session-{index}",
        "label": f"Session {index}",
        "pipeline": pipeline,
        "engine": engine,
        "environment": str(profile.get("environment_class", "not_reported")),
        "profile_id": profile_id,
        "started_at": manifest.get("started_at_utc"),
        "planned_repetitions": repetitions,
        "metrics": metrics,
    }


def build_campaign_view(report: dict) -> dict:
    from . import load_report

    encoded = json.dumps(
        report,
        sort_keys=True,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    ).encode()
    report = load_report(encoded)
    view = {
        "view_type": "campaign",
        "view_schema_version": "1.0.0",
        "report_id": hashlib.sha256(encoded).hexdigest(),
        "report_kind": "load_sweep",
        "identity": {
            "name": "Load campaign",
            "engine": "unknown",
            "case": None,
            "evidence_kind": "reported_campaign_summary",
        },
        "admission": {"status": "summary_inventory", "reason": None},
        "status": str(report.get("status", "not_reported")),
        "sessions": [],
        "inventory": [],
        "warnings": [
            "Saved summary claims; attempts are not re-executed or requalified by this viewer.",
            "Descriptive trends do not establish a complexity law, measurement accuracy or resource-comparison eligibility.",
            "Sessions and scopes are never pooled. MAD is dispersion, not a confidence interval.",
        ],
    }
    if report.get("tool") == "pisces-load-campaign":
        if report.get("schema_version") != "1.0":
            raise ValueError("unsupported_campaign_envelope")
        pipeline = report.get("pipeline", "Load campaign")
        view["sessions"] = [
            _session(
                report["manifest"],
                report["summary"],
                base="/summary",
                pipeline=pipeline,
                index=1,
            )
        ]
        view["status"] = str(report["summary"].get("status", "not_reported"))
        view["admission"]["status"] = "campaign_inventory"
    elif "slope_spread_s_per_100k_rows" in report:
        if (
            report.get("schema_version") != "1.0"
            or not isinstance(report.get("sessions"), list)
            or not 1 <= len(report["sessions"]) <= 32
        ):
            raise ValueError("invalid_scaling_sessions")
        view["report_kind"] = "scaling_sessions"
        view["identity"]["name"] = "Scaling sessions"
        for index, session in enumerate(report["sessions"]):
            if not isinstance(session, dict):
                raise ValueError("invalid_campaign_session")
            view["sessions"].append(
                _session(
                    session["manifest"],
                    session["summary"],
                    base=f"/sessions/{index}/summary",
                    pipeline=report.get("pipeline", "Climate Fusion"),
                    index=index + 1,
                )
            )
        view["admission"]["status"] = "campaign_inventory"
    elif "time_fits_descriptive_only" in report:
        view["sessions"] = [
            _session({}, report, base="", pipeline="Load campaign", index=1)
        ]
        view["warnings"].append(
            "Manifest missing: planned repetitions and execution profile are unknown; source fits are not displayed."
        )
    elif isinstance(report.get("pipelines"), dict) and "telemetry_role" in report:
        view["report_kind"] = "suite_summary"
        view["identity"]["name"] = "Benchmark suite"
        for pipeline, record in report["pipelines"].items():
            if not isinstance(record, dict) or not isinstance(
                record.get("cases"), dict
            ):
                raise ValueError("invalid_suite_summary")
            for case, counts in record["cases"].items():
                if not isinstance(counts, dict):
                    raise ValueError("invalid_suite_case_counts")
                row = {
                    field: _count(
                        counts.get(field), minimum=1 if field == "planned" else 0
                    )
                    for field in (
                        "planned",
                        "attempted",
                        "expected_behavior_matched",
                        "verified_outputs",
                    )
                }
                if (
                    any(value is None for value in row.values())
                    or row["attempted"] > row["planned"]
                    or max(row["expected_behavior_matched"], row["verified_outputs"])
                    > row["attempted"]
                ):
                    raise ValueError("inconsistent_suite_summary_counts")
                view["inventory"].append(
                    {
                        "pipeline": pipeline,
                        "case": case,
                        **row,
                        "pointer": _pointer(
                            _pointer("/pipelines", pipeline) + "/cases", case
                        ),
                    }
                )
        view["warnings"].append(
            "Expected negative-case rejection is not valid useful work. Attempt telemetry is not ranked."
        )
    else:
        raise ValueError("unsupported_campaign_format")
    if view["sessions"]:
        engines = {session["engine"] for session in view["sessions"]}
        view["identity"]["engine"] = (
            next(iter(engines)) if len(engines) == 1 else "mixed"
        )
        if view["report_kind"] != "scaling_sessions":
            view["identity"]["name"] = view["sessions"][0]["pipeline"]
    if not Draft202012Validator(campaign_schema()).is_valid(view):
        raise ValueError("invalid_campaign_view")
    return view


def load_campaign_directory(directory: Path, *, pipeline: str) -> dict:
    """Read only explicitly selected manifest/summary files, never paths inside them."""
    from . import MAX_REPORT_BYTES, load_report

    documents = {}
    for name in ("manifest", "summary"):
        path = directory / f"{name}.json"
        if path.is_symlink():
            raise ValueError("campaign_file_symlinks_not_supported")
        with path.open("rb") as stream:
            documents[name] = load_report(stream.read(MAX_REPORT_BYTES + 1))
    report = {
        "tool": "pisces-load-campaign",
        "schema_version": "1.0",
        "pipeline": pipeline,
        **documents,
    }
    build_campaign_view(report)
    return report
