from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from .airflow import AirflowNativeAdapter
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
) -> Dict[str, Any]:
    adapter = get_native_adapter(orchestrator)
    result = adapter.execute(
        workflow_file=workflow_file,
        max_tasks=max_tasks,
        safety_policy=safety_policy or NativeSafetyPolicy(),
    )
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
]
