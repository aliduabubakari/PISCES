from __future__ import annotations

import sys
from pathlib import Path

from pisces.energy.adapters import NativeSafetyPolicy, execute_native_workload
from pisces.energy.runtime_evaluation import EnergyEvaluationRequest, evaluate_energy


def test_airflow_native_adapter_executes_dag_tasks_with_mock_context(tmp_path: Path) -> None:
    workflow = tmp_path / "fake_airflow.py"
    workflow.write_text(
        """
class Task:
    def __init__(self, task_id, python_callable):
        self.task_id = task_id
        self.python_callable = python_callable

class Dag:
    def __init__(self, tasks):
        self.dag_id = "fake"
        self.tasks = tasks

def extract(**context):
    return [{"value": 2}]

def transform(ti, **context):
    rows = ti.xcom_pull(task_ids="extract")
    return [{"value": rows[0]["value"] * 3}]

dag = Dag([
    Task("extract", extract),
    Task("transform", transform),
])
""".strip()
        + "\n",
        encoding="utf-8",
    )

    result = execute_native_workload(
        workflow,
        orchestrator="airflow",
        max_tasks=5,
        safety_policy=NativeSafetyPolicy(),
    )

    assert result["native_adapter"] == "airflow"
    assert result["tasks_exercised"] == 2
    assert result["native_tasks_executed"] == 2
    assert result["native_tasks_skipped"] == 0
    assert result["native_safety"]["enabled"] is True


def test_native_safety_blocks_external_effects(tmp_path: Path) -> None:
    workflow = tmp_path / "effectful_airflow.py"
    out = tmp_path / "should_not_exist.txt"
    workflow.write_text(
        f"""
class Task:
    def __init__(self, task_id, python_callable):
        self.task_id = task_id
        self.python_callable = python_callable

class Dag:
    def __init__(self, tasks):
        self.dag_id = "effectful"
        self.tasks = tasks

def write_file(**context):
    with open({str(out)!r}, "w") as handle:
        handle.write("unsafe")

dag = Dag([Task("write_file", write_file)])
""".strip()
        + "\n",
        encoding="utf-8",
    )

    result = execute_native_workload(
        workflow,
        orchestrator="airflow",
        max_tasks=5,
        safety_policy=NativeSafetyPolicy(),
    )

    assert result["native_tasks_executed"] == 0
    assert result["native_tasks_skipped"] == 1
    assert not out.exists()
    assert result["native_safety"]["blocked_operations"][0]["operation"] == "file_write"


def test_prefect_and_dagster_native_adapters_execute_duck_typed_objects(tmp_path: Path) -> None:
    prefect_workflow = tmp_path / "fake_prefect.py"
    prefect_workflow.write_text(
        """
class Flow:
    name = "fake_flow"
    def fn(self):
        return 1

flow_obj = Flow()
""".strip()
        + "\n",
        encoding="utf-8",
    )
    dagster_workflow = tmp_path / "fake_dagster.py"
    dagster_workflow.write_text(
        """
class Result:
    success = True

class Job:
    name = "fake_job"
    def execute_in_process(self):
        return Result()

job = Job()
""".strip()
        + "\n",
        encoding="utf-8",
    )

    prefect = execute_native_workload(prefect_workflow, orchestrator="prefect", max_tasks=5)
    dagster = execute_native_workload(dagster_workflow, orchestrator="dagster", max_tasks=5)

    assert prefect["native_adapter"] == "prefect"
    assert prefect["native_tasks_executed"] == 1
    assert dagster["native_adapter"] == "dagster"
    assert dagster["native_tasks_executed"] == 1


def test_evaluate_energy_native_airflow_report_shape(tmp_path: Path) -> None:
    workflow = tmp_path / "fake_airflow.py"
    workflow.write_text(
        """
class Task:
    def __init__(self, task_id, python_callable):
        self.task_id = task_id
        self.python_callable = python_callable

class Dag:
    def __init__(self, tasks):
        self.dag_id = "fake"
        self.tasks = tasks

def task_a(**context):
    return 1

dag = Dag([Task("task_a", task_a)])
""".strip()
        + "\n",
        encoding="utf-8",
    )

    req = EnergyEvaluationRequest(
        file_path=workflow,
        code_text=workflow.read_text(encoding="utf-8"),
        orchestrator="airflow",
        repo_root=Path.cwd(),
        runtime_python=Path(sys.executable),
        mode_selected="zero_fill",
        execution_adapter="native",
        max_rows=10,
        max_tasks=5,
        timeout_s=30,
    )

    report = evaluate_energy(req).to_dict()
    measurement = report["runtime_measurement"]
    assert report["mode_used"] == "zero_fill"
    assert report["evidence"]["measurement_kind"] == "native_runtime"
    assert measurement["execution_adapter_used"] == "native"
    assert measurement["native_tasks_executed"] == 1
    assert measurement["native_tasks_skipped"] == 0
    assert measurement["energy_profile"]["measurement_scope"] == "native_local_runtime"
