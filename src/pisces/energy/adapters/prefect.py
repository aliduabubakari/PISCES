from __future__ import annotations

import time
from pathlib import Path
from typing import Any, List

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


def _is_flow_like(value: Any) -> bool:
    return callable(getattr(value, "fn", None)) or (
        callable(value) and bool(getattr(value, "__pisces_prefect_flow__", False))
    )


class PrefectNativeAdapter:
    name = "prefect"

    def execute(
        self,
        *,
        workflow_file: Path,
        max_tasks: int,
        safety_policy: NativeSafetyPolicy,
    ) -> NativeExecutionResult:
        started = time.time()
        with native_safety_guard(import_safety_policy(safety_policy)) as import_safety:
            module = load_module_from_file(workflow_file, module_name="pisces_native_prefect_workflow")
        flows = [v for v in vars(module).values() if _is_flow_like(v)]
        if not flows:
            raise NativeAdapterError("no_prefect_flow_found")

        results: List[NativeTaskResult] = []
        with native_safety_guard(safety_policy) as execution_safety:
            for idx, flow_obj in enumerate(flows[: max(1, int(max_tasks))]):
                flow_name = str(getattr(flow_obj, "name", getattr(flow_obj, "__name__", f"flow_{idx}")))
                fn = getattr(flow_obj, "fn", None)
                if not callable(fn):
                    fn = flow_obj if callable(flow_obj) else None
                if not callable(fn):
                    results.append(NativeTaskResult(task_id=flow_name, status="skipped", message="flow_not_callable"))
                    continue
                try:
                    fn()
                    results.append(NativeTaskResult(task_id=flow_name, status="executed"))
                except Exception as exc:
                    results.append(
                        NativeTaskResult(
                            task_id=flow_name,
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
                metadata={"flow_count": len(flows)},
            )
