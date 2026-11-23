from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from .airflow import AirflowNativeAdapter, prepare_airflow_test_environment
from .base import NativeAdapterError, NativeExecutionResult, NativeSafetyPolicy
from .dagster import DagsterNativeAdapter
from .prefect import PrefectNativeAdapter


def get_native_adapter(orchestrator: str):
    key = (orchestrator or "").strip().lower()
    if key == "airflow":
        return AirflowNativeAdapter()
    if key == "prefect":
        return PrefectNativeAdapter()
    if key == "dagster":
        return DagsterNativeAdapter()
    raise NativeAdapterError(f"native_adapter_unsupported_orchestrator:{orchestrator}")


def execute_native_workload(
    workflow_file: Path,
    *,
    orchestrator: str,
    max_tasks: int,
    safety_policy: NativeSafetyPolicy | None = None,
    logical_date=None,
    run_index: int = 0,
    fixture_registry=None,
    qualified_oci_target=None,
) -> Dict[str, Any]:
    if qualified_oci_target == orchestrator and orchestrator in {"prefect", "dagster"}:
        from .qualified import execute_qualified

        return execute_qualified(target=orchestrator, workflow_file=workflow_file, max_tasks=max_tasks,
                                 safety_policy=safety_policy or NativeSafetyPolicy())
    adapter = get_native_adapter(orchestrator)
    kwargs = {
        "workflow_file": workflow_file,
        "max_tasks": max_tasks,
        "safety_policy": safety_policy or NativeSafetyPolicy(),
    }
    if adapter.name == "airflow":
        kwargs["logical_date"] = logical_date
        kwargs["run_index"] = run_index
        kwargs["fixture_registry"] = fixture_registry
    result = adapter.execute(**kwargs)
    return result.to_dict()


__all__ = [
    "AirflowNativeAdapter",
    "DagsterNativeAdapter",
    "NativeAdapterError",
    "NativeExecutionResult",
    "NativeSafetyPolicy",
    "PrefectNativeAdapter",
    "execute_native_workload",
    "get_native_adapter",
    "prepare_airflow_test_environment",
]
