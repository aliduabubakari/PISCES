"""Plan and analyze cold, fresh-process Airflow v2 contract experiments."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import platform
import random
import statistics
import time
from typing import Any

from .output_equivalence import ORACLE_VERSION
from .runtime_evaluation import EnergyEvaluationRequest, _run_runtime_measurement, _runtime_observation
from .workload_contract import LoadedWorkloadContract, sha256_file
from ..knowledge_pack import detect_orchestrator_version


ROLES = ("baseline", "candidate")
ADDITIVE_METRICS = ("wall_clock_s", "process_cpu_s")
PROFILE = {
    "airflow_version": "3.4.0",
    "execution_interface": "DAG.test",
    "database": "temporary_sqlite",
    "executor": "not_used",
    "measurement_scope": "bounded_execution_block",
    "cache_policy": "fresh_runner_and_database_os_cache_uncontrolled",
    "modeled_energy": "disabled",
}


@dataclass(frozen=True)
class PairedPlan:
    pairs: int
    seed: int = 1729
    metric: str = "wall_clock_s"
    minimum_effect: float = 0.05
    precision_goal: float = 0.10
    minimum_pair_fraction: float = 0.8
    timeout_s: int = 120

    def __post_init__(self) -> None:
        if self.metric not in ADDITIVE_METRICS or type(self.pairs) is not int or not 2 <= self.pairs <= 200:
            raise ValueError("Select an additive metric and 2 to 200 pairs")
        if not 0 < self.minimum_effect < 1 or not 0 < self.precision_goal < 1:
            raise ValueError("Effect and precision thresholds must be between 0 and 1")
        if not 0 < self.minimum_pair_fraction <= 1 or self.timeout_s <= 0:
            raise ValueError("Invalid coverage or timeout")

    def schedule(self) -> list[tuple[str, str]]:
        reverse = tuple(reversed(ROLES))
        orders = [ROLES if index % 2 == 0 else reverse for index in range(self.pairs)]
        random.Random(self.seed).shuffle(orders)
        return orders


def _number(value: Any) -> bool:
    return (
        (type(value) is int and 0 <= value <= 10**12)
        or (type(value) is float and math.isfinite(value) and 0 <= value <= 10**12)
    )


def _screen(measurement: dict[str, Any], workload: LoadedWorkloadContract) -> tuple[str | None, dict[str, str]]:
    if measurement.get("status") != "ok" or measurement.get("execution_outcome") != "completed":
        return "execution_or_contract_failed", {}
    if measurement.get("execution_adapter_used") != "native":
        return "native_execution_required", {}
    if measurement.get("workload_contract") != workload.provenance():
        return "workload_provenance_mismatch", {}
    bounds = measurement.get("bounds") or {}
    warmup = measurement.get("warmup") or {}
    if (
        bounds.get("repetitions_completed") != 1 or bounds.get("maximum_repetitions") != 1
        or warmup.get("completed_repetitions") != 0 or warmup.get("requested_repetitions") != 0
        or warmup.get("included_in_measurement_window") is not False
    ):
        return "single_fresh_run_required", {}
    if (measurement.get("contract_validation") or {}).get("scope") != "single_execution_checks":
        return "single_execution_checks_required", {}
    if not all((measurement.get(key) or {}).get("passed") is True for key in ("task_coverage", "input_consumption")):
        return "task_or_input_checks_failed", {}
    if (measurement.get("execution_binding") or {}).get("bound_workflow_sha256") != workload.artifact_sha256:
        return "staged_artifact_mismatch", {}
    output = measurement.get("output_assertions") or {}
    if output.get("passed") is not True or output.get("oracle_version") != ORACLE_VERSION:
        return "reference_verification_required", {}
    for key in ("contract_sha256", "binding_sha256", "artifact_sha256"):
        if output.get(key) != getattr(workload, key):
            return "oracle_provenance_mismatch", {}
    fingerprints = {}
    references = workload.provenance()["reference_sha256"]
    for result in output.get("results", []):
        name = result.get("logical_name")
        fingerprint = result.get("semantic_sha256")
        if (
            name in fingerprints or name not in references or result.get("passed") is not True
            or result.get("reference_sha256") != references[name] or not isinstance(fingerprint, str)
            or len(fingerprint) != 64 or any(char not in "0123456789abcdef" for char in fingerprint)
        ):
            return "exact_output_fingerprints_required", {}
        fingerprints[name] = fingerprint
    if fingerprints.keys() != references.keys():
        return "exact_output_fingerprints_required", {}
    return None, fingerprints


def _profile_matches(runtime: dict[str, Any], metric: str) -> bool:
    native = runtime.get("native_environment") or {}
    runner = runtime.get("runner_environment") or {}
    metrics = runtime.get("operational_metrics") or {}
    return (
        all(native.get(key) == PROFILE[key] for key in ("airflow_version", "execution_interface", "database", "executor"))
        and native.get("initialization_in_measurement_window") is False
        and isinstance(runner.get("distributions_sha256"), str)
        and len(runner["distributions_sha256"]) == 64
        and metrics.get("measurement_scope") == PROFILE["measurement_scope"]
        and metrics.get("clock_sources", {}).get("wall_clock") == "time.perf_counter"
        and metrics.get("clock_sources", {}).get("process_cpu") == "time.process_time"
        and runtime.get("modeled_energy") is None
        and _number(metrics.get(metric)) and metrics[metric] > 0
    )


def _pair_record(attempts: dict[str, Any], metric: str) -> dict[str, Any]:
    left, right = (attempts[role] for role in ROLES)
    if not left["credited_units"] or not right["credited_units"]:
        return {"status": "ineligible", "reason": "both_attempts_must_verify", "equivalence": "not_evaluated"}
    if left["fingerprints"] != right["fingerprints"]:
        return {"status": "ineligible", "reason": "output_fingerprints_differ", "equivalence": "failed"}
    a, b = left["runtime"], right["runtime"]
    if (
        not all(_profile_matches(run, metric) for run in (a, b))
        or any(a.get(key) != b.get(key) for key in ("runner_environment", "native_environment"))
    ):
        return {"status": "ineligible", "reason": "measurement_profile_mismatch", "equivalence": "passed"}
    before, after = a["operational_metrics"][metric], b["operational_metrics"][metric]
    reduction = 1 - after / before
    if not math.isfinite(reduction):
        return {"status": "ineligible", "reason": "invalid_relative_reduction", "equivalence": "passed"}
    return {"status": "eligible", "equivalence": "passed", "relative_reduction": reduction}


def _analyze(blocks: list[dict[str, Any]], plan: PairedPlan) -> dict[str, Any]:
    eligible = [block for block in blocks if block["comparison"]["status"] == "eligible"]
    reductions = [block["comparison"]["relative_reduction"] for block in eligible]
    summary: dict[str, Any] = {
        "status": "inconclusive",
        "assigned_pairs": plan.pairs,
        "eligible_pairs": len(eligible),
        "metric": plan.metric,
        "functional_unit": "valid_batch",
        "verified_attempts": {
            role: sum(block["attempts"][role]["credited_units"] for block in blocks) for role in ROLES
        },
        "paired_resource_totals": {},
    }
    if eligible:
        for role in ROLES:
            metrics = [block["attempts"][role]["runtime"]["operational_metrics"] for block in eligible]
            totals = {
                key: math.fsum(item[key] for item in metrics)
                for key in ADDITIVE_METRICS if all(_number(item.get(key)) for item in metrics)
            }
            summary["paired_resource_totals"][role] = {
                "verified_units": len(metrics), "additive_totals": totals,
                "per_valid_batch": {key: value / len(metrics) for key, value in totals.items()},
            }
        summary["median_relative_reduction"] = statistics.median(reductions)
    if len(eligible) / plan.pairs < plan.minimum_pair_fraction:
        return {**summary, "reason": "insufficient_pair_coverage"}
    if len(eligible) < 5:
        return {**summary, "reason": "insufficient_repetitions_for_interval"}
    rng = random.Random(plan.seed)
    samples = sorted(statistics.median(rng.choices(reductions, k=len(reductions))) for _ in range(2000))
    low, high = samples[49], samples[1949]
    summary["technical_bootstrap_interval_95"] = {"low": low, "high": high}
    if (high - low) / 2 > plan.precision_goal:
        return {**summary, "reason": "insufficient_precision"}
    if low >= plan.minimum_effect:
        return {**summary, "status": "candidate_lower", "reason": None}
    if high <= -plan.minimum_effect:
        return {**summary, "status": "candidate_higher", "reason": None}
    return {**summary, "reason": "meaningful_effect_not_established"}


def _write(path: Path, value: Any) -> None:
    with path.open("x", encoding="utf-8") as handle:
        json.dump(value, handle, indent=2, allow_nan=False)
        handle.write("\n")


def run_paired_experiment(
    baseline: LoadedWorkloadContract, candidate: LoadedWorkloadContract, *,
    runtime_python: Path, result_dir: Path, plan: PairedPlan,
) -> dict[str, Any]:
    """Run and archive one verified repetition per subprocess and arm."""
    if (
        baseline.contract_sha256 != candidate.contract_sha256
        or any(w.contract.get("schema_version") != "2.0" or w.binding.get("target") != "airflow"
               for w in (baseline, candidate))
    ):
        raise ValueError("A shared Airflow workload contract v2 is required")
    if any(column.get("absolute_tolerance", 0) for output in baseline.contract["outputs"] for column in output["columns"]):
        raise ValueError("Paired execution currently requires exact output contracts")
    if detect_orchestrator_version("airflow", python_exe=runtime_python) != PROFILE["airflow_version"]:
        raise ValueError("Paired execution requires the pinned Airflow 3.4.0 interpreter")
    result_dir.mkdir(parents=True, exist_ok=False)
    workloads = dict(zip(ROLES, (baseline, candidate)))
    schedule = plan.schedule()
    _write(result_dir / "manifest.json", {
        "schema_version": "1.0", "plan": plan.__dict__, "profile": PROFILE,
        "workloads": {role: work.provenance() for role, work in workloads.items()},
        "order": schedule, "missing_pair_policy": "exclude_pair_keep_both_attempts",
        "oracle_version": ORACLE_VERSION, "experimental_unit": "one_artifact_pair",
        "controller_platform": platform.platform(),
    })
    blocks = []
    reference_environment = None
    for index, order in enumerate(schedule, 1):
        attempts = {}
        for role in order:
            workload = workloads[role]
            started_at = datetime.now(timezone.utc).isoformat()
            started = time.perf_counter()
            request = EnergyEvaluationRequest(
                file_path=workload.artifact_path, code_text="", orchestrator="airflow",
                repo_root=workload.artifact_path.parent, runtime_python=runtime_python,
                execution_adapter="native", mode_selected="zero_fill", experiment_mode=True,
                measurement_maximum_repetitions=1, timeout_s=plan.timeout_s,
                workload_contract=workload.contract_path, artifact_binding=workload.binding_path,
            )
            try:
                payload = _run_runtime_measurement(
                    request=request, mode_name="zero_fill", data_source="contract_fixtures",
                    rows=[], collect_modeled_energy=False,
                )
            except OSError:
                payload = {"ok": False, "error": {"error_type": "RunnerLaunchFailed"}}
            elapsed = time.perf_counter() - started
            runtime = _runtime_observation(payload, request)
            reason, fingerprints = _screen(runtime, workload)
            if payload.get("ok") is not True:
                reason = reason or "runner_failed"
            attempt = {
                "pair": index, "role": role, "started_at": started_at,
                "controller_elapsed_s": elapsed, "credited_units": int(reason is None),
                "reason": reason, "fingerprints": fingerprints, "runtime": runtime,
            }
            _write(result_dir / f"pair-{index:03d}-{role}.json", attempt)
            attempts[role] = attempt
        comparison = _pair_record(attempts, plan.metric)
        if comparison["status"] == "eligible":
            runtime = attempts["baseline"]["runtime"]
            environment = (runtime["runner_environment"], runtime["native_environment"])
            if reference_environment is None:
                reference_environment = environment
            elif environment != reference_environment:
                comparison = {"status": "ineligible", "reason": "environment_changed_between_pairs", "equivalence": "passed"}
        comparison["attempt_sha256"] = {
            role: sha256_file(result_dir / f"pair-{index:03d}-{role}.json") for role in ROLES
        }
        _write(result_dir / f"pair-{index:03d}.json", {"order": order, **comparison})
        blocks.append({"attempts": attempts, "comparison": comparison})
    summary = _analyze(blocks, plan)
    _write(result_dir / "analysis.json", summary)
    return summary
