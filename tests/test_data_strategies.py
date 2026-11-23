from __future__ import annotations

from pathlib import Path

from pisces.energy.data_strategies import (
    FuzzFillStrategy,
    OperatorProfileStrategy,
    ScaleProbeStrategy,
    ZeroFillStrategy,
)
from pisces.energy.zero_fill import generate_zero_fill_dataset


def test_zero_fill_handles_invalid_python_without_internal_key_error() -> None:
    result = generate_zero_fill_dataset(
        code_text="def broken(:\n",
        max_rows=2,
        orchestrator="airflow",
    )

    provenance = result["provenance"][0]
    assert provenance["table_refs_detected"] == []
    assert provenance["task_ids_detected"] == []


def _workflow_text() -> str:
    return """
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def process(order_id, amount, is_valid):
    return amount if is_valid else 0

with DAG(dag_id='x', start_date=datetime(2024, 1, 1), schedule=None) as dag:
    t1 = PythonOperator(task_id='a', python_callable=process)
""".strip()


def test_fuzz_fill_is_deterministic_and_non_zero(tmp_path: Path) -> None:
    workflow = tmp_path / "flow.py"
    workflow.write_text(_workflow_text(), encoding="utf-8")
    strategy = FuzzFillStrategy()

    first = strategy.generate(
        code_text=workflow.read_text(encoding="utf-8"),
        max_rows=5,
        seed=123,
        file_path=workflow,
        orchestrator="airflow",
    )
    second = strategy.generate(
        code_text=workflow.read_text(encoding="utf-8"),
        max_rows=5,
        seed=123,
        file_path=workflow,
        orchestrator="airflow",
    )

    assert first.rows == second.rows
    assert first.row_count == 5
    assert any(value not in (0, 0.0, "", False) for row in first.rows for value in row.values())
    assert first.meta["mode"] == "fuzz_fill"
    assert first.meta["data_values"] == "deterministic_fuzz"


def test_zero_fill_strategy_preserves_zero_values(tmp_path: Path) -> None:
    workflow = tmp_path / "flow.py"
    workflow.write_text(_workflow_text(), encoding="utf-8")
    result = ZeroFillStrategy().generate(
        code_text=workflow.read_text(encoding="utf-8"),
        max_rows=3,
        seed=123,
        file_path=workflow,
        orchestrator="airflow",
    )

    assert result.row_count == 3
    assert result.meta["mode"] == "zero_fill"
    assert all(value in (0, 0.0, "", False) for row in result.rows for value in row.values())


def test_scale_probe_row_counts_are_bounded_and_unique() -> None:
    assert ScaleProbeStrategy.row_counts(5) == [5]
    assert ScaleProbeStrategy.row_counts(50) == [10, 50]
    assert ScaleProbeStrategy.row_counts(500) == [10, 100, 500]


def test_scale_probe_generates_multiple_deterministic_probes(tmp_path: Path) -> None:
    workflow = tmp_path / "flow.py"
    workflow.write_text(_workflow_text(), encoding="utf-8")
    strategy = ScaleProbeStrategy()

    probes = strategy.generate_probes(
        code_text=workflow.read_text(encoding="utf-8"),
        max_rows=50,
        seed=77,
        file_path=workflow,
        orchestrator="airflow",
    )
    probes_again = strategy.generate_probes(
        code_text=workflow.read_text(encoding="utf-8"),
        max_rows=50,
        seed=77,
        file_path=workflow,
        orchestrator="airflow",
    )

    assert [probe.row_count for probe in probes] == [10, 50]
    assert [probe.rows for probe in probes] == [probe.rows for probe in probes_again]
    assert all(probe.meta["mode"] == "scale_probe" for probe in probes)


def test_operator_profile_strategy_marks_operator_profile(tmp_path: Path) -> None:
    workflow = tmp_path / "flow.py"
    workflow.write_text(_workflow_text(), encoding="utf-8")

    result = OperatorProfileStrategy().generate(
        code_text=workflow.read_text(encoding="utf-8"),
        max_rows=5,
        seed=99,
        file_path=workflow,
        orchestrator="airflow",
    )

    assert result.name == "operator_profile"
    assert result.meta["mode"] == "operator_profile"
    assert result.meta["base_strategy"] == "fuzz_fill"
    assert result.rows
