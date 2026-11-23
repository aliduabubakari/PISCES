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


def _candidate_jobs(module: Any) -> List[Any]:
    direct = [v for v in vars(module).values() if callable(getattr(v, "execute_in_process", None))]
    if direct:
        return direct

    jobs: List[Any] = []
    for value in vars(module).values():
        get_all_job_defs = getattr(value, "get_all_job_defs", None)
        if callable(get_all_job_defs):
            try:
                jobs.extend([j for j in get_all_job_defs() if callable(getattr(j, "execute_in_process", None))])
            except Exception:
                continue
    return jobs


class DagsterNativeAdapter:
    name = "dagster"

    def execute(
        self,
        *,
        workflow_file: Path,
        max_tasks: int,
        safety_policy: NativeSafetyPolicy,
    ) -> NativeExecutionResult:
        started = time.time()
        with native_safety_guard(import_safety_policy(safety_policy)) as import_safety:
            module = load_module_from_file(workflow_file, module_name="pisces_native_dagster_workflow")
        jobs = _candidate_jobs(module)
        if not jobs:
            raise NativeAdapterError("no_dagster_job_found")

        results: List[NativeTaskResult] = []
        with native_safety_guard(safety_policy) as execution_safety:
            for idx, job in enumerate(jobs[: max(1, int(max_tasks))]):
                job_name = str(getattr(job, "name", f"job_{idx}"))
                try:
                    result = job.execute_in_process()
                    if bool(getattr(result, "success", True)):
                        results.append(NativeTaskResult(task_id=job_name, status="executed"))
                    else:
                        results.append(NativeTaskResult(task_id=job_name, status="skipped", message="job_unsuccessful"))
                except Exception as exc:
                    results.append(
                        NativeTaskResult(
                            task_id=job_name,
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
                metadata={"job_count": len(jobs), "engine_execution_verified": False,
                          "execution_interface": "legacy_job_level_probe"},
            )
