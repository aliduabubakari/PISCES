#!/usr/bin/env python3
from __future__ import annotations

import argparse
import atexit
import hashlib
import json
import os
import statistics
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from .adapters import (
    NativeAdapterError,
    NativeSafetyPolicy,
    execute_native_workload,
    prepare_airflow_test_environment,
)
from .carbon_tracker import add_scaled_estimate, try_start_tracker, try_stop_tracker
from .execution_sandbox import ExecutionSandbox, prepared_execution_sandbox
from .fixture_registry import FixtureRegistry
from .operational_metrics import (
    start_operational_measurement,
    stop_operational_measurement,
)
from .output_assertions import evaluate_output_assertions
from .workload_contract import WorkloadContractError, load_workload_contract


def _emit(payload: Dict[str, Any], out: Path) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")


def _numeric(v: Any) -> float:
    if isinstance(v, bool):
        return 1.0 if v else 0.0
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        try:
            return float(v)
        except Exception:
            return float(len(v))
    return 0.0


def _operator_profile_units(operator_tiers: Dict[str, Any]) -> int:
    heavy = int(operator_tiers.get("heavy_compute_count") or 0)
    polling = int(operator_tiers.get("polling_heavy_count") or 0)
    distribution = operator_tiers.get("distribution") if isinstance(operator_tiers.get("distribution"), dict) else {}
    io_bound = int(distribution.get("io_bound") or 0)
    work_units = 1 + (heavy * 2) + polling + io_bound
    return max(1, min(12, work_units))


def _logical_date(value: Any) -> datetime:
    text = str(value or "2024-01-01T00:00:00Z").strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _task_coverage(
    workload,
    run_stats: Dict[str, Any],
) -> Dict[str, Any]:
    expected = {
        str(value)
        for value in workload.binding.get("expected_task_ids") or []
    }
    allowed_additional = {
        str(value)
        for value in workload.binding.get("allowed_additional_task_ids") or []
    }
    allowed_skipped = {
        str(value)
        for value in workload.binding.get("allowed_skipped_task_ids") or []
    }
    alternative_groups = [
        {str(value) for value in group}
        for group in workload.binding.get("alternative_task_groups") or []
    ]
    task_results = run_stats.get("native_task_results")
    observed = {
        str(item.get("task_id"))
        for item in task_results or []
        if isinstance(item, dict) and item.get("status") == "executed"
    }
    statuses = {
        str(item.get("task_id")): str(item.get("status"))
        for item in task_results or []
        if isinstance(item, dict) and item.get("task_id")
    }
    missing = sorted(expected - observed)
    unexpected = sorted(
        observed - expected - allowed_additional - set().union(*alternative_groups)
        if alternative_groups
        else observed - expected - allowed_additional
    )
    unsatisfied_alternatives = [
        sorted(group)
        for group in alternative_groups
        if not (group & observed)
    ]
    disallowed_skips = sorted(
        task_id
        for task_id, status in statuses.items()
        if status == "skipped" and task_id not in allowed_skipped
    )
    failed = sorted(
        task_id
        for task_id, status in statuses.items()
        if status not in {"executed", "skipped"}
    )
    passed = (
        bool(expected or alternative_groups)
        and not missing
        and not unexpected
        and not unsatisfied_alternatives
        and not disallowed_skips
        and not failed
    )
    return {
        "status": "passed" if passed else "failed",
        "passed": passed,
        "expected_task_count": len(expected),
        "executed_task_count": len(observed),
        "expected_task_ids": sorted(expected),
        "executed_task_ids": sorted(observed),
        "missing_task_ids": missing,
        "unexpected_task_ids": unexpected,
        "allowed_additional_task_ids": sorted(allowed_additional),
        "allowed_skipped_task_ids": sorted(allowed_skipped),
        "alternative_task_groups": [
            sorted(group) for group in alternative_groups
        ],
        "unsatisfied_alternative_task_groups": unsatisfied_alternatives,
        "disallowed_skipped_task_ids": disallowed_skips,
        "failed_task_ids": failed,
    }


def _input_consumption(
    workload,
    sandbox: ExecutionSandbox,
    run_stats: Dict[str, Any],
) -> Dict[str, Any]:
    required = {
        str(value)
        for value in (
            workload.contract.get("required_input_ids")
            or workload.fixtures.keys()
        )
    }
    external_log = (
        (run_stats.get("native_metadata") or {}).get("fixture_access_log") or []
    )
    served = {
        str(item.get("integration_id"))
        for item in external_log
        if isinstance(item, dict)
        and item.get("status") == "served"
        and item.get("integration_id")
    }
    safety_log = (
        (run_stats.get("native_safety") or {}).get("accessed_operations") or []
    )
    read_paths = set()
    for item in safety_log:
        if not isinstance(item, dict):
            continue
        mode = str(item.get("mode") or "r")
        if any(flag in mode for flag in ("w", "a", "x")) and "+" not in mode:
            continue
        raw_path = item.get("path")
        if not raw_path:
            continue
        candidate = Path(str(raw_path))
        if not candidate.is_absolute():
            candidate = sandbox.working / candidate
        try:
            read_paths.add(str(candidate.resolve()))
        except (OSError, ValueError):
            continue
    file_bindings = workload.binding.get("file_bindings") or {}
    consumed_files: Dict[str, str] = {}
    for integration_id, raw_destination in file_bindings.items():
        candidate = (sandbox.working / str(raw_destination)).resolve()
        if str(candidate) in read_paths:
            served.add(str(integration_id))
            consumed_files[str(integration_id)] = str(candidate)
    missing = sorted(required - served)
    passed = bool(required) and not missing
    return {
        "status": "passed" if passed else "failed",
        "passed": passed,
        "required_input_ids": sorted(required),
        "consumed_input_ids": sorted(served),
        "missing_input_ids": missing,
        "consumed_file_bindings": consumed_files,
        "observed_read_path_count": len(read_paths),
    }


def _run_workload(
    rows: List[Dict[str, Any]],
    *,
    max_tasks: int,
    seed: int,
    workload_profile: str = "standard",
    operator_tiers: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    t0 = time.perf_counter()
    row_count = len(rows)
    if row_count == 0:
        return {
            "rows_processed": 0,
            "tasks_exercised": 0,
            "numeric_accumulator": 0.0,
            "hash_accumulator": 0,
            "duration_s": 0.0,
        }

    task_budget = max(1, int(max_tasks))

    numeric_acc = 0.0
    hash_acc = 0
    tiers = operator_tiers if isinstance(operator_tiers, dict) else {}
    work_units = _operator_profile_units(tiers) if workload_profile == "operator_profile" else 1

    for t in range(task_budget):
        for unit in range(work_units):
            for r in rows:
                for k, v in r.items():
                    numeric_acc += _numeric(v) * (1.0 + (t * 0.01) + (unit * 0.005))
                    blob = f"{seed}:{t}:{unit}:{k}:{v}".encode("utf-8", errors="ignore")
                    h = hashlib.sha256(blob).hexdigest()
                    hash_acc ^= int(h[:8], 16)

    duration_s = max(0.0, time.perf_counter() - t0)
    return {
        "rows_processed": row_count,
        "tasks_exercised": task_budget,
        "numeric_accumulator": round(numeric_acc, 6),
        "hash_accumulator": int(hash_acc),
        "duration_s": round(duration_s, 6),
        "workload_profile": workload_profile,
        "work_units": work_units,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Runtime representative energy workload runner")
    parser.add_argument("--in", dest="in_path", required=True)
    parser.add_argument("--out", dest="out_path", required=True)
    args = parser.parse_args()

    in_path = Path(args.in_path)
    out_path = Path(args.out_path)

    try:
        req = json.loads(in_path.read_text(encoding="utf-8"))
        if not isinstance(req, dict):
            raise ValueError("input payload must be JSON object")
    except Exception as e:
        payload = {
            "evaluation_type": "runtime_energy",
            "timestamp": datetime.now().isoformat(),
            "ok": False,
            "error": {"error_type": type(e).__name__, "message": str(e)},
        }
        _emit(payload, out_path)
        return

    orchestrator = str(req.get("orchestrator") or "unknown")
    mode = str(req.get("mode") or "unknown")
    data_source = str(req.get("data_source") or "none")
    workflow_file = Path(str(req.get("workflow_file") or ""))
    execution_adapter = str(req.get("execution_adapter") or "surrogate_benchmark").strip().lower()
    if execution_adapter == "representative":
        execution_adapter = "surrogate_benchmark"
    max_rows = int(req.get("max_rows") or 500)
    max_tasks = int(req.get("max_tasks") or 25)
    seed = int(req.get("seed") or 1729)
    workload_profile = str(req.get("workload_profile") or "standard")
    operator_tiers = req.get("operator_tiers") if isinstance(req.get("operator_tiers"), dict) else {}
    native_safety_raw = req.get("native_safety") if isinstance(req.get("native_safety"), dict) else {}
    native_safety = NativeSafetyPolicy(
        enabled=bool(native_safety_raw.get("enabled", True)),
        block_network=bool(native_safety_raw.get("block_network", True)),
        block_subprocess=bool(native_safety_raw.get("block_subprocess", True)),
        block_file_writes=bool(native_safety_raw.get("block_file_writes", True)),
        allowed_write_roots=[
            str(value)
            for value in native_safety_raw.get("allowed_write_roots") or []
        ],
    )
    country_iso = req.get("carbon_country_iso")
    measure_power_secs = int(req.get("carbon_measure_power_secs") or 1)
    scale_factor = float(req.get("carbon_scale_factor") or 1.0)
    minimum_duration_s = max(0.0, float(req.get("minimum_duration_s") or 0.0))
    minimum_repetitions = max(1, int(req.get("minimum_repetitions") or 1))
    maximum_repetitions = max(
        minimum_repetitions,
        int(req.get("maximum_repetitions") or max(1000, minimum_repetitions)),
    )
    warmup_repetitions = max(
        0,
        int(req.get("warmup_repetitions") or 0),
    )
    experiment_mode = bool(req.get("experiment_mode", False))
    workload_contract_path = req.get("workload_contract")
    artifact_binding_path = req.get("artifact_binding")

    workload = None
    if bool(workload_contract_path) != bool(artifact_binding_path):
        payload = {
            "evaluation_type": "runtime_energy",
            "timestamp": datetime.now().isoformat(),
            "ok": False,
            "mode": mode,
            "data_source": data_source,
            "orchestrator": orchestrator,
            "error": {
                "error_type": "WorkloadContractError",
                "message": "workload_contract_and_artifact_binding_are_both_required",
            },
            "runtime_measurement": {
                "status": "failed",
                "execution_outcome": "contract_invalid",
                "operational_metrics": None,
                "modeled_energy": None,
                "eligible_for_resource_comparison": False,
                "eligible_for_paired_equivalence_comparison": False,
                "ineligibility_reasons": ["contract_configuration_incomplete"],
            },
        }
        _emit(payload, out_path)
        return
    if workload_contract_path and artifact_binding_path:
        try:
            workload = load_workload_contract(
                contract_path=Path(str(workload_contract_path)),
                binding_path=Path(str(artifact_binding_path)),
                artifact_path=workflow_file,
            )
        except WorkloadContractError as exc:
            payload = {
                "evaluation_type": "runtime_energy",
                "timestamp": datetime.now().isoformat(),
                "ok": False,
                "mode": mode,
                "data_source": data_source,
                "orchestrator": orchestrator,
                "error": {
                    "error_type": "WorkloadContractError",
                    "message": str(exc),
                },
                "runtime_measurement": {
                    "status": "failed",
                    "execution_outcome": "contract_invalid",
                    "operational_metrics": None,
                    "modeled_energy": None,
                    "eligible_for_resource_comparison": False,
                    "eligible_for_paired_equivalence_comparison": False,
                    "ineligibility_reasons": ["contract_validation_failed"],
                },
            }
            _emit(payload, out_path)
            return
        if workload.binding.get("status") == "known_ineligible":
            payload = {
                "evaluation_type": "runtime_energy",
                "timestamp": datetime.now().isoformat(),
                "ok": False,
                "mode": mode,
                "data_source": data_source,
                "orchestrator": orchestrator,
                "error": {
                    "error_type": "KnownIneligibleArtifact",
                    "message": "artifact binding is marked known_ineligible",
                },
                "runtime_measurement": {
                    "status": "failed",
                    "execution_outcome": "known_ineligible",
                    "operational_metrics": None,
                    "modeled_energy": None,
                    "eligible_for_resource_comparison": False,
                    "eligible_for_paired_equivalence_comparison": False,
                    "ineligibility_reasons": ["binding_marked_known_ineligible"],
                    "workload_contract": workload.provenance(),
                },
            }
            _emit(payload, out_path)
            return

    rows_raw = req.get("rows")
    rows: List[Dict[str, Any]] = []
    if isinstance(rows_raw, list):
        for r in rows_raw:
            if isinstance(r, dict):
                rows.append(r)

    adapter_attempts: List[Dict[str, Any]] = []
    selected_adapter = (
        execution_adapter
        if execution_adapter in {"native", "surrogate_benchmark", "auto"}
        else "surrogate_benchmark"
    )
    if experiment_mode and selected_adapter == "auto":
        selected_adapter = "native"

    sandbox: ExecutionSandbox | None = None
    execution_workflow_file = workflow_file
    fixture_registry: FixtureRegistry | None = None
    native_environment: Dict[str, Any] | None = None
    logical_date = _logical_date(
        workload.contract.get("fixed_logical_date") if workload is not None else None
    )
    if selected_adapter in {"native", "auto"}:
        try:
            if workload is not None:
                sandbox_context = prepared_execution_sandbox(workload)
                sandbox = sandbox_context.__enter__()
                atexit.register(sandbox_context.__exit__, None, None, None)
                fixture_registry = FixtureRegistry(
                    workload,
                    fixture_paths=sandbox.fixtures,
                )
                execution_workflow_file = sandbox.workflow_file
                native_safety.allowed_write_roots.extend(
                    [str(sandbox.root), str(sandbox.working), str(sandbox.outputs)]
                )
            if orchestrator == "airflow":
                airflow_context = tempfile.TemporaryDirectory(
                    prefix="pisces_airflow_test_"
                )
                airflow_home = Path(airflow_context.__enter__()).resolve()
                atexit.register(airflow_context.__exit__, None, None, None)
                os.environ.update(
                    {
                        "AIRFLOW_HOME": str(airflow_home),
                        "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
                        "AIRFLOW__CORE__EXECUTOR": "SequentialExecutor",
                        "AIRFLOW__CORE__DAGS_FOLDER": str(
                            execution_workflow_file.parent
                        ),
                        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": (
                            f"sqlite:///{airflow_home / 'airflow.db'}"
                        ),
                        "AIRFLOW__LOGGING__BASE_LOG_FOLDER": str(
                            airflow_home / "logs"
                        ),
                    }
                )
                native_safety.allowed_write_roots.append(str(airflow_home))
                try:
                    native_environment = prepare_airflow_test_environment()
                except NativeAdapterError as exc:
                    # Contract-bound experiments require a real Airflow
                    # runtime and must fail closed.  Without a workload
                    # contract, however, the native adapter also supports
                    # small DAG-like fixtures used to validate its execution
                    # semantics independently of an Airflow installation.
                    if workload is not None or str(exc) != "airflow_runtime_not_available":
                        raise
                    native_environment = {
                        "status": "not_initialized",
                        "reason": "airflow_runtime_not_available",
                        "execution_interface": "dag_like_test_fixture",
                    }
                native_environment.update(
                    {
                        "python_executable": os.sys.executable,
                        "airflow_home_scope": "temporary",
                        "database": "temporary_sqlite",
                        "executor": "SequentialExecutor",
                        "initialization_in_measurement_window": False,
                    }
                )
        except Exception as exc:
            payload = {
                "evaluation_type": "runtime_energy",
                "timestamp": datetime.now().isoformat(),
                "ok": False,
                "mode": mode,
                "data_source": data_source,
                "orchestrator": orchestrator,
                "error": {
                    "error_type": type(exc).__name__,
                    "message": str(exc),
                },
                "runtime_measurement": {
                    "status": "failed",
                    "execution_outcome": "environment_setup_failed",
                    "operational_metrics": None,
                    "modeled_energy": None,
                    "eligible_for_resource_comparison": False,
                    "eligible_for_paired_equivalence_comparison": False,
                    "ineligibility_reasons": [
                        "native_environment_setup_failed"
                    ],
                },
            }
            _emit(payload, out_path)
            return

    run_stats: Dict[str, Any]
    adapter_used = "surrogate_benchmark"
    repetitions_completed = 0
    execution_index = 0
    repeated_stats: List[Dict[str, Any]] = []
    repetition_metrics: List[Dict[str, Any]] = []
    warmup_stats: List[Dict[str, Any]] = []

    def run_once() -> Dict[str, Any]:
        nonlocal adapter_used, execution_index
        current_execution_index = execution_index
        execution_index += 1
        if selected_adapter == "surrogate_benchmark":
            adapter_used = "surrogate_benchmark"
            return _run_workload(
                rows,
                max_tasks=max_tasks,
                seed=seed,
                workload_profile=workload_profile,
                operator_tiers=operator_tiers,
            )
        if selected_adapter == "native":
            adapter_used = "native"
            return execute_native_workload(
                execution_workflow_file,
                orchestrator=orchestrator,
                max_tasks=max_tasks,
                safety_policy=native_safety,
                logical_date=logical_date,
                run_index=current_execution_index,
                fixture_registry=fixture_registry,
            )
        try:
            candidate = execute_native_workload(
                execution_workflow_file,
                orchestrator=orchestrator,
                max_tasks=max_tasks,
                safety_policy=native_safety,
                logical_date=logical_date,
                run_index=current_execution_index,
                fixture_registry=fixture_registry,
            )
            if int(candidate.get("native_tasks_executed") or 0) <= 0:
                raise NativeAdapterError("native_adapter_no_tasks_executed")
            adapter_used = "native"
            adapter_attempts.append({"adapter": "native", "status": "success"})
            return candidate
        except Exception as exc:
            adapter_attempts.append(
                {
                    "adapter": "native",
                    "status": "failed",
                    "reason": f"{type(exc).__name__}:{exc}",
                }
            )
            adapter_used = "surrogate_benchmark"
            adapter_attempts.append(
                {"adapter": "surrogate_benchmark", "status": "success"}
            )
            return _run_workload(
                rows,
                max_tasks=max_tasks,
                seed=seed,
                workload_profile=workload_profile,
                operator_tiers=operator_tiers,
            )

    try:
        for _ in range(warmup_repetitions):
            warmup = run_once()
            if (
                adapter_used == "native"
                and int(warmup.get("native_tasks_executed") or 0) <= 0
            ):
                raise NativeAdapterError("warmup_no_native_tasks_executed")
            if (
                adapter_used == "native"
                and int(warmup.get("native_tasks_skipped") or 0) > 0
            ):
                raise NativeAdapterError("warmup_native_tasks_skipped")
            warmup_stats.append(warmup)
    except Exception as exc:
        payload = {
            "evaluation_type": "runtime_energy",
            "timestamp": datetime.now().isoformat(),
            "ok": False,
            "mode": mode,
            "data_source": data_source,
            "orchestrator": orchestrator,
            "error": {
                "error_type": type(exc).__name__,
                "message": str(exc),
            },
            "runtime_measurement": {
                "status": "failed",
                "execution_outcome": "warmup_failed",
                "operational_metrics": None,
                "modeled_energy": None,
                "eligible_for_resource_comparison": False,
                "eligible_for_paired_equivalence_comparison": False,
                "ineligibility_reasons": ["warmup_failed"],
                "warmup": {
                    "requested_repetitions": warmup_repetitions,
                    "completed_repetitions": len(warmup_stats),
                    "included_in_measurement_window": False,
                },
            },
        }
        _emit(payload, out_path)
        return

    tracker = try_start_tracker(
        country_iso_code=str(country_iso) if country_iso else None,
        measure_power_secs=measure_power_secs,
        project_name=f"pisces_runtime_{orchestrator}_{mode}",
        extra={
            "measurement_scope": "bounded_execution_block",
            "disclaimer": (
                "CodeCarbon is secondary modeled evidence. Primary operational evidence is reported "
                "as wall-clock time, process CPU time, and peak resident memory."
            ),
        },
    )
    operational_started = start_operational_measurement()

    try:
        while repetitions_completed < maximum_repetitions:
            repetition_wall_started = time.perf_counter()
            repetition_cpu_started = time.process_time()
            run_stats = run_once()
            repeated_stats.append(run_stats)
            repetition_wall_s = (
                time.perf_counter() - repetition_wall_started
            )
            repetition_cpu_s = time.process_time() - repetition_cpu_started
            repetition_metrics.append(
                {
                    "repetition": repetitions_completed + 1,
                    "wall_clock_s": round(repetition_wall_s, 9),
                    "process_cpu_s": round(repetition_cpu_s, 9),
                    "adapter_duration_s": run_stats.get("duration_s"),
                    "native_tasks_executed": run_stats.get(
                        "native_tasks_executed"
                    ),
                    "native_tasks_skipped": run_stats.get(
                        "native_tasks_skipped"
                    ),
                }
            )
            repetitions_completed += 1
            elapsed = time.perf_counter() - operational_started.wall_started
            if repetitions_completed >= minimum_repetitions and elapsed >= minimum_duration_s:
                break
        if selected_adapter != "auto":
            adapter_attempts.append(
                {"adapter": adapter_used, "status": "success"}
            )
    except Exception as e:
        attempt_metrics = stop_operational_measurement(
            operational_started,
            scope="failed_bounded_execution_attempt",
        )
        modeled_energy = add_scaled_estimate(try_stop_tracker(tracker), scale_factor)
        payload = {
            "evaluation_type": "runtime_energy",
            "timestamp": datetime.now().isoformat(),
            "ok": False,
            "mode": mode,
            "data_source": data_source,
            "orchestrator": orchestrator,
            "error": {"error_type": type(e).__name__, "message": str(e)},
            "runtime_measurement": {
                "status": "failed",
                "execution_outcome": "failed",
                "operational_metrics": None,
                "modeled_energy": None,
                "attempt_diagnostics": {
                    "operational_metrics": attempt_metrics,
                    "modeled_energy": modeled_energy,
                },
                "eligible_for_resource_comparison": False,
                "ineligibility_reasons": ["execution_failed"],
                "execution_adapter_requested": selected_adapter,
                "execution_adapter_used": adapter_used,
                "adapter_attempts": adapter_attempts,
            },
        }
        _emit(payload, out_path)
        return
    operational_metrics = stop_operational_measurement(
        operational_started,
        scope="bounded_execution_block",
    )
    operational_metrics["completed_repetitions"] = repetitions_completed
    operational_metrics["wall_clock_s_per_repetition"] = round(
        operational_metrics["wall_clock_s"] / repetitions_completed,
        9,
    )
    operational_metrics["process_cpu_s_per_repetition"] = round(
        operational_metrics["process_cpu_s"] / repetitions_completed,
        9,
    )
    wall_values = [
        float(item["wall_clock_s"]) for item in repetition_metrics
    ]
    cpu_values = [
        float(item["process_cpu_s"]) for item in repetition_metrics
    ]

    def stability(values: List[float]) -> Dict[str, Any]:
        mean = statistics.fmean(values)
        sample_sd = statistics.stdev(values) if len(values) > 1 else None
        return {
            "n": len(values),
            "mean": round(mean, 9),
            "sample_sd": (
                round(sample_sd, 9) if sample_sd is not None else None
            ),
            "coefficient_of_variation": (
                round(sample_sd / mean, 6)
                if sample_sd is not None and mean > 0
                else None
            ),
            "relative_standard_error": (
                round((sample_sd / (len(values) ** 0.5)) / mean, 6)
                if sample_sd is not None and mean > 0
                else None
            ),
        }

    repetition_summary = {
        "wall_clock_s": stability(wall_values),
        "process_cpu_s": stability(cpu_values),
    }
    energy = add_scaled_estimate(try_stop_tracker(tracker), scale_factor)
    if adapter_used == "native":
        energy["measurement_scope"] = "native_local_runtime"
        energy["disclaimer"] = (
            "This measurement covers bounded local native execution under PISCES safety guards. "
            "External effects are blocked by default, so results may skip effectful tasks."
        )
    if tracker is None:
        if adapter_used == "native":
            energy["measurement_scope"] = "native_local_runtime"
            energy["disclaimer"] = (
                "This measurement covers bounded local native execution under PISCES safety guards. "
                "External effects are blocked by default, so results may skip effectful tasks."
            )
        else:
            energy["measurement_scope"] = "surrogate_benchmark"
            energy["disclaimer"] = (
                "This modeled-energy estimate covers a PISCES surrogate benchmark. It is not "
                "workflow execution and is ineligible for workflow resource comparison."
            )

    measured_kwh = energy.get("energy_consumed_kwh")
    measured_kg = energy.get("emissions_kgco2eq")
    output_assertions = None
    task_coverage = None
    input_consumption = None
    if workload is not None and sandbox is not None:
        output_assertions = evaluate_output_assertions(
            workload,
            workspace_root=sandbox.working,
            native_metadata=run_stats.get("native_metadata"),
        )
        task_coverage = _task_coverage(workload, run_stats)
        input_consumption = _input_consumption(
            workload,
            sandbox,
            run_stats,
        )
    rows_processed = int(run_stats.get("rows_processed") or 0)
    if output_assertions is not None:
        asserted_row_counts = [
            int(item["row_count"])
            for item in output_assertions.get("results") or []
            if isinstance(item, dict)
            and isinstance(item.get("row_count"), int)
        ]
        if asserted_row_counts:
            rows_processed = max(asserted_row_counts)
    native_executed = int(run_stats.get("native_tasks_executed") or 0)
    native_skipped = int(run_stats.get("native_tasks_skipped") or 0)
    if adapter_used == "surrogate_benchmark":
        execution_outcome = "surrogate"
        eligible = False
        ineligibility_reasons = ["surrogate_is_not_workflow_execution"]
    elif native_executed <= 0:
        execution_outcome = "zero_work"
        eligible = False
        ineligibility_reasons = ["no_native_tasks_executed"]
    elif workload is None and native_skipped > 0:
        execution_outcome = "partial"
        eligible = False
        ineligibility_reasons = ["native_tasks_skipped"]
    else:
        execution_outcome = "completed"
        eligible = True
        ineligibility_reasons = []
    minimum_duration_reached = (
        operational_metrics["wall_clock_s"] >= minimum_duration_s
    )
    if not minimum_duration_reached:
        eligible = False
        ineligibility_reasons.append("minimum_measurement_duration_not_reached")
    if output_assertions is not None and not output_assertions.get("passed"):
        eligible = False
        ineligibility_reasons.append("output_assertions_failed")
    if task_coverage is not None and not task_coverage.get("passed"):
        eligible = False
        ineligibility_reasons.append("task_coverage_failed")
    if input_consumption is not None and not input_consumption.get("passed"):
        eligible = False
        ineligibility_reasons.append("input_consumption_failed")
    paired_eligible = bool(
        workload is not None
        and eligible
        and output_assertions
        and output_assertions.get("passed")
        and task_coverage
        and task_coverage.get("passed")
        and input_consumption
        and input_consumption.get("passed")
    )

    strict_execution_failed = experiment_mode and not eligible
    primary_operational_metrics = (
        None if strict_execution_failed else operational_metrics
    )
    primary_modeled_energy = None if strict_execution_failed else energy
    attempt_diagnostics = (
        {
            "operational_metrics": operational_metrics,
            "modeled_energy": energy,
        }
        if strict_execution_failed
        else None
    )

    normalized = {
        "energy_kwh_per_1k_rows": None,
        "emissions_kgco2eq_per_1k_rows": None,
    }
    if isinstance(measured_kwh, (int, float)) and rows_processed > 0:
        normalized["energy_kwh_per_1k_rows"] = round((float(measured_kwh) / rows_processed) * 1000.0, 10)
    if isinstance(measured_kg, (int, float)) and rows_processed > 0:
        normalized["emissions_kgco2eq_per_1k_rows"] = round((float(measured_kg) / rows_processed) * 1000.0, 10)

    payload = {
        "evaluation_type": "runtime_energy",
        "timestamp": datetime.now().isoformat(),
        "ok": not strict_execution_failed,
        "mode": mode,
        "data_source": data_source,
        "orchestrator": orchestrator,
        "runtime_measurement": {
            "status": "failed" if strict_execution_failed else "ok",
            "execution_outcome": execution_outcome,
            "operational_metrics": primary_operational_metrics,
            "repetition_metrics": repetition_metrics,
            "repetition_summary": repetition_summary,
            "modeled_energy": primary_modeled_energy,
            "attempt_diagnostics": attempt_diagnostics,
            "workload_contract": (
                workload.provenance() if workload is not None else None
            ),
            "execution_binding": (
                {
                    "bound_workflow_path": str(sandbox.workflow_file),
                    "bound_workflow_sha256": sandbox.workflow_sha256,
                    "literal_rewrites": sandbox.literal_rewrites,
                }
                if sandbox is not None
                else None
            ),
            "warmup": {
                "requested_repetitions": warmup_repetitions,
                "completed_repetitions": len(warmup_stats),
                "included_in_measurement_window": False,
            },
            "output_assertions": output_assertions,
            "task_coverage": task_coverage,
            "input_consumption": input_consumption,
            "eligible_for_resource_comparison": eligible,
            "eligible_for_paired_equivalence_comparison": paired_eligible,
            "paired_equivalence_status": (
                "eligible_pending_pairwise_comparison"
                if paired_eligible
                else "ineligible"
            ),
            "ineligibility_reasons": ineligibility_reasons,
            "execution_adapter_requested": selected_adapter,
            "execution_adapter_used": adapter_used,
            "adapter_attempts": adapter_attempts,
            "rows_processed": rows_processed,
            "tasks_exercised": int(run_stats.get("tasks_exercised") or 0),
            "duration_s": float(run_stats.get("duration_s") or 0.0),
            "numeric_accumulator": run_stats.get("numeric_accumulator"),
            "hash_accumulator": run_stats.get("hash_accumulator"),
            "workload_profile": run_stats.get("workload_profile"),
            "work_units": run_stats.get("work_units"),
            "native_tasks_executed": run_stats.get("native_tasks_executed"),
            "native_tasks_skipped": run_stats.get("native_tasks_skipped"),
            "native_adapter": run_stats.get("native_adapter"),
            "native_task_results": run_stats.get("native_task_results"),
            "native_safety": run_stats.get("native_safety"),
            "native_metadata": run_stats.get("native_metadata"),
            "native_environment": native_environment,
            "energy_profile": primary_modeled_energy,
            "normalized": (
                normalized
                if not strict_execution_failed
                else {
                    "energy_kwh_per_1k_rows": None,
                    "emissions_kgco2eq_per_1k_rows": None,
                }
            ),
            "bounds": {
                "max_rows": max_rows,
                "max_tasks": max_tasks,
                "seed": seed,
                "minimum_duration_s": minimum_duration_s,
                "minimum_repetitions": minimum_repetitions,
                "maximum_repetitions": maximum_repetitions,
                "warmup_repetitions": warmup_repetitions,
                "repetitions_completed": repetitions_completed,
                "minimum_duration_reached": minimum_duration_reached,
            },
        },
    }
    _emit(payload, out_path)


if __name__ == "__main__":
    main()
