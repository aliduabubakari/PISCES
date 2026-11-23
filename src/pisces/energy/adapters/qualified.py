"""Engine-level evidence for the approved, bounded Prefect and Dagster OCI profiles."""

from __future__ import annotations

from dataclasses import replace
from importlib.metadata import version
import inspect
from pathlib import Path
import tempfile
import time

from .base import (NativeAdapterError, NativeTaskResult, combine_safety, finish_result, import_safety_policy,
                   load_module_from_file, native_safety_guard)


def _selection(module, kind):
    candidates = {id(value): value for value in vars(module).values() if isinstance(value, kind)}
    selected = getattr(module, "pisces_entrypoint", None)
    if selected is not None:
        if not isinstance(selected, kind):
            raise NativeAdapterError("invalid_pisces_entrypoint")
        return selected
    if len(candidates) != 1:
        raise NativeAdapterError("explicit_or_unambiguous_entrypoint_required")
    return next(iter(candidates.values()))


def _prefect(module, max_tasks, policy):
    from prefect.flows import Flow
    from prefect.tasks import Task
    from prefect.testing.utilities import prefect_test_harness
    from prefect.client.orchestration import get_client
    from prefect.client.schemas.filters import FlowRunFilter, FlowRunFilterId
    from prefect.events.worker import EventsWorker
    from prefect.utilities.asyncutils import run_coro_as_sync

    flow = _selection(module, Flow)
    tasks = {id(value): value for value in vars(module).values() if isinstance(value, Task)}
    if flow.retries or flow.isasync or not 1 <= len(tasks) <= max_tasks:
        raise NativeAdapterError("unsupported_prefect_flow_profile")
    declared = {}
    for task in tasks.values():
        if task.retries or task.isasync or task.task_key in declared:
            raise NativeAdapterError("unsupported_prefect_task_profile")
        declared[task.task_key] = task.name
    if len(set(declared.values())) != len(declared):
        raise NativeAdapterError("unique_prefect_task_names_required")

    # The actual API process is evaluator-owned, local to the no-network container.
    # Python guards are convenience instrumentation, not this profile's isolation boundary.
    engine_policy = replace(policy, block_network=False, block_subprocess=False,
                            allowed_write_roots=[*policy.allowed_write_roots, "/tmp", "/dev/null"])
    with prefect_test_harness(server_startup_timeout=60):
        with native_safety_guard(engine_policy) as safety:
            state = flow(return_state=True)
            run_id = state.state_details.flow_run_id
            if run_id is None:
                raise NativeAdapterError("prefect_flow_run_identity_missing")
            # Delivery and persistence of task events can lag the returned flow state.
            async def flush_events():
                delivery = EventsWorker.drain_all()
                if inspect.isawaitable(delivery):
                    await delivery

            run_coro_as_sync(flush_events())
            with get_client(sync_client=True) as client:
                deadline = time.monotonic() + 5
                while True:
                    records = client.read_task_runs(flow_run_filter=FlowRunFilter(id=FlowRunFilterId(any_=[run_id])),
                                                    limit=max_tasks + 1)
                    settled = (len(records) == len(tasks) and all(record.state and record.state.is_final()
                               and record.run_count == 1 for record in records))
                    if settled or time.monotonic() >= deadline or any(record.run_count > 1 for record in records):
                        break
                    time.sleep(0.1)
            if len(records) > max_tasks:
                raise NativeAdapterError("prefect_task_limit_exceeded")
            observed, results, raw = set(), [], []
            for record in records:
                key = record.task_key
                if key not in declared:
                    raise NativeAdapterError("prefect_undeclared_task_not_supported")
                if key in observed:
                    raise NativeAdapterError("prefect_repeated_task_not_supported")
                if record.run_count != 1:
                    raise NativeAdapterError(f"prefect_task_attempt_count_not_single:{record.run_count}")
                observed.add(key)
                native_state = record.state
                if native_state is None or not native_state.is_final():
                    raise NativeAdapterError("prefect_task_state_not_terminal")
                status = "executed" if native_state and native_state.is_completed() else "failed"
                results.append(NativeTaskResult(task_id=declared[key], status=status))
                raw.append({"task_id": declared[key], "state": native_state.name if native_state else "missing", "status": status,
                            "attempt_count": record.run_count})
    return results, safety, {
        "engine_state": "completed" if state.is_completed() else "failed",
        "engine_evidence": {"target": "prefect", "version": version("prefect"), "interface": "Flow.__call__(return_state=True)",
                            "run_state": state.name, "task_states": raw, "source": "local_api_task_run_states",
                            "control_plane": "local_test_harness_api_and_sqlite", "setup_in_execution_window": True},
    }


def _dagster(module, max_tasks, policy):
    from dagster import DagsterInstance, JobDefinition, OpDefinition

    job = _selection(module, JobDefinition)
    nodes = job.graph.node_defs
    if not 1 <= len(job.graph.node_dict) <= max_tasks or any(not isinstance(node, OpDefinition) or node.retry_policy for node in nodes) or job.op_retry_policy:
        raise NativeAdapterError("unsupported_dagster_job_profile")
    expected = set(job.graph.node_dict)
    engine_policy = replace(policy, allowed_write_roots=[*policy.allowed_write_roots, "/tmp", "/dev/null"])
    with tempfile.TemporaryDirectory(prefix="pisces-dagster-") as scratch:
        with DagsterInstance.ephemeral(tempdir=scratch) as instance:
            with native_safety_guard(engine_policy) as safety:
                execution = job.execute_in_process(instance=instance, raise_on_error=False)
            raw, states = [], {}
            for event in execution.all_events:
                if event.event_type_value in {"STEP_SUCCESS", "STEP_FAILURE", "STEP_SKIPPED"}:
                    key = event.step_key
                    if key not in expected or key in states:
                        raise NativeAdapterError("dagster_dynamic_or_duplicate_step_not_supported")
                    status = {"STEP_SUCCESS": "executed", "STEP_FAILURE": "failed", "STEP_SKIPPED": "skipped"}[event.event_type_value]
                    states[key] = status
                    raw.append({"task_id": key, "state": event.event_type_value, "status": status})
                if event.event_type_value in {"STEP_UP_FOR_RETRY", "STEP_RESTARTED"}:
                    raise NativeAdapterError("dagster_retry_not_supported")
            results = [NativeTaskResult(task_id=key, status=status) for key, status in states.items()]
    return results, safety, {
        "engine_state": "completed" if execution.success else "failed",
        "engine_evidence": {"target": "dagster", "version": version("dagster"), "interface": "JobDefinition.execute_in_process",
                            "run_state": "success" if execution.success else "failure", "task_states": raw,
                            "source": "dagster_step_events", "control_plane": "ephemeral_instance", "setup_in_execution_window": True},
    }


def execute_qualified(*, target: str, workflow_file: Path, max_tasks: int, safety_policy):
    started = time.time()
    with native_safety_guard(import_safety_policy(safety_policy)) as import_safety:
        module = load_module_from_file(workflow_file, module_name=f"pisces_qualified_{target}")
    results, safety, metadata = {"prefect": _prefect, "dagster": _dagster}[target](module, max_tasks, safety_policy)
    return finish_result(adapter=target, started_at=started, task_results=results, metadata=metadata,
                         safety=combine_safety(policy=safety_policy, import_safety=import_safety, execution_safety=safety)).to_dict()
