"""Unit-test the API readout protocol, not the Prefect engine itself."""

import asyncio
from contextlib import nullcontext
import sys
from types import ModuleType, SimpleNamespace

import pytest

from pisces.energy.adapters import qualified
from pisces.energy.adapters.base import NativeAdapterError, NativeSafetyPolicy


@pytest.mark.parametrize("scenario", ["settles", "stale", "retried", "pending"])
def test_prefect_readout_drains_waits_and_fails_closed(monkeypatch, scenario):
    delivered = []

    class Flow:
        retries = 0
        isasync = False

        def __call__(self, *, return_state):
            assert return_state
            return SimpleNamespace(state_details=SimpleNamespace(flow_run_id="root"), name="Completed", is_completed=lambda: True)

        def fn(self):
            pytest.fail("Must invoke the flow engine, not .fn")

    class Task:
        retries = 0
        isasync = False

        def __init__(self, name):
            self.task_key = name + "-hash"
            self.name = name

    names = ("normalize", "transform", "write_outputs")
    declarations = {name: Task(name) for name in names}
    pending = SimpleNamespace(name="Running", is_final=lambda: False, is_completed=lambda: False)
    terminal = SimpleNamespace(name="Completed", is_final=lambda: True, is_completed=lambda: True)
    stale = [SimpleNamespace(task_key=value.task_key, run_count=0, state=pending) for value in declarations.values()]
    complete = [SimpleNamespace(task_key=value.task_key, run_count=1, state=terminal) for value in declarations.values()]
    if scenario == "retried":
        complete[0].run_count = 2
    elif scenario == "pending":
        complete[0].state = pending

    class Client:
        reads = 0

        def read_task_runs(self, **kwargs):
            assert delivered, "Task event delivery must finish before collecting states"
            assert kwargs["limit"] == 4
            self.reads += 1
            return stale if scenario == "stale" or (scenario == "settles" and self.reads == 1) else complete

    client = Client()

    async def drain_all():
        delivered.append(True)

    def harness(*, server_startup_timeout):
        assert server_startup_timeout == 60
        return nullcontext()

    modules = {
        "prefect.flows": {"Flow": Flow}, "prefect.tasks": {"Task": Task},
        "prefect.testing.utilities": {"prefect_test_harness": harness},
        "prefect.client.orchestration": {"get_client": lambda **kwargs: nullcontext(client)},
        "prefect.client.schemas.filters": {"FlowRunFilter": lambda **kwargs: kwargs, "FlowRunFilterId": lambda **kwargs: kwargs},
        "prefect.events.worker": {"EventsWorker": SimpleNamespace(drain_all=drain_all)},
        "prefect.utilities.asyncutils": {"run_coro_as_sync": asyncio.run},
    }
    for name, members in modules.items():
        module = ModuleType(name)
        module.__dict__.update(members)
        monkeypatch.setitem(sys.modules, name, module)
    ticks = iter([0, 6] if scenario in {"stale", "pending"} else [0, 1])
    monkeypatch.setattr(qualified, "time", SimpleNamespace(monotonic=lambda: next(ticks), sleep=lambda _: None))
    monkeypatch.setattr(qualified, "version", lambda _: "3.6.18")
    monkeypatch.setattr(qualified, "native_safety_guard", lambda _: nullcontext({"policy": {}}))
    module = SimpleNamespace(pisces_entrypoint=Flow(), **declarations)
    if scenario == "settles":
        results, _, metadata = qualified._prefect(module, 3, NativeSafetyPolicy())
        assert client.reads == 2
        assert all(result.status == "executed" for result in results)
        assert all(state["attempt_count"] == 1 for state in metadata["engine_evidence"]["task_states"])
    else:
        reason = "prefect_task_state_not_terminal" if scenario == "pending" else f"prefect_task_attempt_count_not_single:{0 if scenario == 'stale' else 2}"
        with pytest.raises(NativeAdapterError, match=reason):
            qualified._prefect(module, 3, NativeSafetyPolicy())
