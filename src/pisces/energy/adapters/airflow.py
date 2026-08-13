from __future__ import annotations

import time
import contextlib
import hashlib
import inspect
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from ..fixture_registry import FixtureRegistry, patched_external_services
from .base import (
    NativeAdapterError,
    NativeExecutionResult,
    NativeSafetyPolicy,
    NativeTaskResult,
    combine_safety,
    finish_result,
    import_safety_policy,
    load_module_from_file,
    native_safety_guard,
)


def _is_dag_like(value: Any) -> bool:
    if not hasattr(value, "tasks") or not callable(getattr(value, "test", None)):
        return False
    tasks = getattr(value, "tasks", None)
    return isinstance(tasks, (list, tuple))


def prepare_airflow_test_environment() -> Dict[str, Any]:
    """Initialize the Airflow metadata database before measurement begins."""
    try:
        import airflow
        from airflow.utils import db
    except ImportError as exc:
        raise NativeAdapterError("airflow_runtime_not_available") from exc
    init_parameters = inspect.signature(db.initdb).parameters
    if "load_connections" in init_parameters:
        db.initdb(load_connections=False)
    else:
        db.initdb()
    return {
        "airflow_version": str(airflow.__version__),
        "execution_interface": "DAG.test",
    }


def _task_output_summary(task_instance: Any) -> Dict[str, Any]:
    try:
        value = task_instance.xcom_pull(task_ids=task_instance.task_id)
    except Exception as exc:
        return {
            "present": False,
            "reason": "xcom_read_failed",
            "message": str(exc),
        }
    if value is None:
        return {"present": False, "python_type": "NoneType"}
    try:
        canonical = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
    except Exception:
        canonical = repr(value)
    summary: Dict[str, Any] = {
        "present": True,
        "python_type": type(value).__name__,
        "sha256": hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
    }
    if isinstance(value, dict):
        summary["json_type"] = "object"
        summary["keys"] = sorted(str(key) for key in value)
        summary["length"] = len(value)
    elif isinstance(value, (list, tuple)):
        summary["json_type"] = "array"
        summary["length"] = len(value)
    elif isinstance(value, str):
        summary["json_type"] = "string"
        summary["length"] = len(value)
    elif isinstance(value, bool):
        summary["json_type"] = "boolean"
    elif isinstance(value, (int, float)):
        summary["json_type"] = "number"
    else:
        summary["json_type"] = type(value).__name__
    return summary


class AirflowNativeAdapter:
    name = "airflow"

    def execute(
        self,
        *,
        workflow_file: Path,
        max_tasks: int,
        safety_policy: NativeSafetyPolicy,
        logical_date: datetime | None = None,
        run_index: int = 0,
        fixture_registry: FixtureRegistry | None = None,
    ) -> NativeExecutionResult:
        started = time.time()
        fixture_context = (
            patched_external_services(fixture_registry)
            if fixture_registry is not None
            else contextlib.nullcontext()
        )
        with native_safety_guard(import_safety_policy(safety_policy)) as import_safety:
            with fixture_context:
                module = load_module_from_file(
                    workflow_file,
                    module_name="pisces_native_airflow_workflow",
                )
        dags = [v for v in vars(module).values() if _is_dag_like(v)]
        if not dags:
            raise NativeAdapterError("no_dag_object_found")

        dag = dags[0]
        tasks = list(getattr(dag, "tasks", []) or [])
        if not tasks:
            raise NativeAdapterError("airflow_dag_has_no_tasks")
        if len(tasks) > max(1, int(max_tasks)):
            raise NativeAdapterError(
                f"airflow_dag_exceeds_task_limit:{len(tasks)}:{int(max_tasks)}"
            )
        retry_configuration = {
            str(getattr(task, "task_id", "unknown")): int(
                getattr(task, "retries", 0) or 0
            )
            for task in tasks
        }
        for task in tasks:
            task.retries = 0
        execution_date = logical_date or datetime(2024, 1, 1, tzinfo=timezone.utc)
        if execution_date.tzinfo is None:
            execution_date = execution_date.replace(tzinfo=timezone.utc)
        if run_index:
            execution_date = execution_date.replace(
                microsecond=(execution_date.microsecond + int(run_index)) % 1_000_000
            )

        fixture_context = (
            patched_external_services(fixture_registry)
            if fixture_registry is not None
            else contextlib.nullcontext()
        )
        with native_safety_guard(safety_policy) as execution_safety:
            with fixture_context:
                dag_run = dag.test(execution_date=execution_date)

        task_instances = list(dag_run.get_task_instances())
        results: List[NativeTaskResult] = []
        task_outputs: Dict[str, Dict[str, Any]] = {}
        for task_instance in task_instances:
            state = str(getattr(task_instance, "state", "unknown") or "unknown")
            task_id = str(getattr(task_instance, "task_id", "unknown"))
            results.append(
                NativeTaskResult(
                    task_id=task_id,
                    status="executed" if state == "success" else state,
                    message=None if state == "success" else f"airflow_task_state:{state}",
                )
            )
            if state == "success":
                task_outputs[task_id] = _task_output_summary(task_instance)

        try:
            import airflow

            airflow_version = str(airflow.__version__)
        except ImportError:
            airflow_version = "unknown"
        return finish_result(
            adapter=self.name,
            started_at=started,
            task_results=results,
            safety=combine_safety(
                policy=safety_policy,
                import_safety=import_safety,
                execution_safety=execution_safety,
            ),
            metadata={
                "dag_count": len(dags),
                "dag_id": str(getattr(dag, "dag_id", "unknown")),
                "dag_state": str(getattr(dag_run, "state", "unknown")),
                "airflow_version": airflow_version,
                "execution_interface": "DAG.test",
                "logical_date": execution_date.isoformat(),
                "declared_task_count": len(tasks),
                "observed_task_instance_count": len(task_instances),
                "test_retry_policy": "single_attempt",
                "declared_retries": retry_configuration,
                "task_outputs": task_outputs,
                "fixture_access_log": (
                    list(fixture_registry.access_log)
                    if fixture_registry is not None
                    else []
                ),
            },
        )
