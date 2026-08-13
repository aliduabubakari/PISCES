import json
from pathlib import Path

from pisces.energy.structural_energy_analyzer import StructuralEnergyAnalyzer


AIRFLOW_HEADER = """\
from airflow import DAG
from airflow.operators.python import PythonOperator

dag = DAG(dag_id="parser_regression")
a = PythonOperator(task_id="a", python_callable=lambda: None, dag=dag)
b = PythonOperator(task_id="b", python_callable=lambda: None, dag=dag)
c = PythonOperator(task_id="c", python_callable=lambda: None, dag=dag)
d = PythonOperator(task_id="d", python_callable=lambda: None, dag=dag)
"""


def _analyze(tmp_path: Path, dependency_code: str) -> dict:
    tmp_path.mkdir(parents=True, exist_ok=True)
    artifact = tmp_path / "pipeline.py"
    artifact.write_text(AIRFLOW_HEADER + "\n" + dependency_code, encoding="utf-8")
    return StructuralEnergyAnalyzer().analyze(artifact, orchestrator="airflow")


def _edges(report: dict) -> set[tuple[str, str]]:
    return {
        (edge["from"], edge["to"])
        for edge in report["edges"]["effective_edges_simple"]
    }


def test_chained_rshift_matches_separate_statements(tmp_path: Path) -> None:
    separate = _analyze(tmp_path / "separate", "a >> b\nb >> c\nc >> d\n")
    chained = _analyze(tmp_path / "chained", "a >> b >> c >> d\n")

    expected = {("a", "b"), ("b", "c"), ("c", "d")}
    assert _edges(separate) == expected
    assert _edges(chained) == expected
    assert separate["metrics"] == chained["metrics"]


def test_chained_lshift_preserves_direction(tmp_path: Path) -> None:
    report = _analyze(tmp_path, "d << c << b << a\n")
    assert _edges(report) == {("a", "b"), ("b", "c"), ("c", "d")}


def test_shift_lists_expand_at_chain_boundary(tmp_path: Path) -> None:
    report = _analyze(tmp_path, "[a, b] >> c >> d\n")
    assert _edges(report) == {("a", "c"), ("b", "c"), ("c", "d")}


def test_sidecar_discovery_can_be_disabled_for_code_only_comparison(
    tmp_path: Path,
) -> None:
    artifact = tmp_path / "pipeline.py"
    artifact.write_text(AIRFLOW_HEADER + "\na >> b\n", encoding="utf-8")
    (tmp_path / "intermediate_opos.json").write_text(
        json.dumps(
            {
                "components": [{"id": "spec_a"}, {"id": "spec_b"}],
                "flow": {"edges": [{"from": "spec_a", "to": "spec_b"}]},
            }
        ),
        encoding="utf-8",
    )

    spec_aware = StructuralEnergyAnalyzer().analyze(
        artifact,
        orchestrator="airflow",
    )
    code_only = StructuralEnergyAnalyzer().analyze(
        artifact,
        orchestrator="airflow",
        discover_sidecars=False,
    )

    assert spec_aware["tasks"]["canonical_source"] == "opos"
    assert code_only["tasks"]["canonical_source"] == "code"
    assert code_only["spec_context"]["opos"]["summary"] == {"present": False}
