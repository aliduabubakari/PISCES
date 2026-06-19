from __future__ import annotations

import inspect
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

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


class _MockTaskInstance:
    def __init__(self, values: Dict[str, Any]):
        self._values = values

    def xcom_pull(self, task_ids=None, *args: Any, **kwargs: Any) -> Any:
        if isinstance(task_ids, (list, tuple)):
            return [self._values.get(str(t)) for t in task_ids]
        if task_ids is None:
            return None
        return self._values.get(str(task_ids))


def _is_dag_like(value: Any) -> bool:
    if not hasattr(value, "tasks"):
        return False
    tasks = getattr(value, "tasks", None)
    return isinstance(tasks, (list, tuple))


def _call_python_callable(fn, *, task_id: str, xcom_values: Dict[str, Any]) -> Any:
    op_args = []
    op_kwargs = {}
    signature = inspect.signature(fn)
    context = {
        "ti": _MockTaskInstance(xcom_values),
        "task_instance": _MockTaskInstance(xcom_values),
        "execution_date": datetime(2024, 1, 1, tzinfo=timezone.utc),
        "ds": "2024-01-01",
        "task_id": task_id,
    }

    accepts_kwargs = any(p.kind == p.VAR_KEYWORD for p in signature.parameters.values())
    for name, param in signature.parameters.items():
        if name == "ti":
            op_kwargs["ti"] = context["ti"]
        elif name == "task_instance":
            op_kwargs["task_instance"] = context["task_instance"]
        elif name in context and accepts_kwargs is False:
            op_kwargs[name] = context[name]
        elif (
            param.default is inspect.Parameter.empty
            and param.kind in {param.POSITIONAL_ONLY, param.POSITIONAL_OR_KEYWORD, param.KEYWORD_ONLY}
            and name not in op_kwargs
        ):
            raise NativeAdapterError(f"required_argument_not_mocked:{name}")

    if accepts_kwargs:
        op_kwargs.update(context)
    return fn(*op_args, **op_kwargs)


class AirflowNativeAdapter:
    name = "airflow"

    def execute(
        self,
        *,
        workflow_file: Path,
        max_tasks: int,
        safety_policy: NativeSafetyPolicy,
    ) -> NativeExecutionResult:
        started = time.time()
        with native_safety_guard(import_safety_policy(safety_policy)) as import_safety:
            module = load_module_from_file(workflow_file, module_name="pisces_native_airflow_workflow")
        dags = [v for v in vars(module).values() if _is_dag_like(v)]
        if not dags:
            raise NativeAdapterError("no_dag_object_found")

        dag = dags[0]
        tasks = list(getattr(dag, "tasks", []) or [])
        task_budget = max(1, min(int(max_tasks), len(tasks) or 1))
        xcom_values: Dict[str, Any] = {}
        results: List[NativeTaskResult] = []

        with native_safety_guard(safety_policy) as execution_safety:
            for task in tasks[:task_budget]:
                task_id = str(getattr(task, "task_id", f"task_{len(results)}"))
                fn = getattr(task, "python_callable", None)
                if not callable(fn):
                    results.append(NativeTaskResult(task_id=task_id, status="skipped", message="no_python_callable"))
                    continue
                try:
                    value = _call_python_callable(fn, task_id=task_id, xcom_values=xcom_values)
                    xcom_values[task_id] = value
                    results.append(NativeTaskResult(task_id=task_id, status="executed"))
                except Exception as exc:
                    results.append(
                        NativeTaskResult(
                            task_id=task_id,
                            status="skipped",
                            error_type=type(exc).__name__,
                            message=str(exc)[:300],
                        )
                    )

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
                },
            )
