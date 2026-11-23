from __future__ import annotations

import json
from pathlib import Path

import pisces.energy.runtime_evaluation as reval
from pisces.dag_comparator import compare_reports
from pisces.unified_evaluator import UnifiedEnergyConfig, run_unified_energy_evaluation


def _write_plain_workflow(tmp_path: Path) -> Path:
    workflow = tmp_path / "plain_workflow.py"
    workflow.write_text(
        """
def extract():
    return [{"id": 1, "value": 2.0}]

def transform(rows):
    return [{"id": row["id"], "value": row["value"] * 2} for row in rows]
""".strip()
        + "\n",
        encoding="utf-8",
    )
    return workflow


def _write_airflow_like_workflow(tmp_path: Path) -> Path:
    workflow = tmp_path / "airflow_like.py"
    workflow.write_text(
        """
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

with DAG(dag_id='x', start_date=datetime(2024, 1, 1), schedule=None) as dag:
    t1 = PythonOperator(task_id='a', python_callable=lambda: 1)
""".strip()
        + "\n",
        encoding="utf-8",
    )
    return workflow


def test_unified_evaluator_runs_heuristic_by_default(tmp_path: Path) -> None:
    workflow = _write_plain_workflow(tmp_path)
    report = run_unified_energy_evaluation(
        UnifiedEnergyConfig(workflow_path=workflow, stdout=True)
    )

    assert report["energy_evaluation"]["enabled"] is True
    assert report["energy_evaluation"]["mode_requested"] == "heuristic"
    assert report["energy"]["mode_used"] == "heuristic"
    assert "heuristic" in report["energy"]
    assert report["energy"]["evidence"]["measurement_kind"] == "static"
    assert report["energy"]["evidence"]["measurement_scope"] == "static_only"
    assert report["energy"]["measurements"]["runtime"] is None
    assert report["energy"]["attempts"][0]["tier"] == "static"
    assert report["workflow"]["target_validation"] == "not_evaluated"


def test_explicit_target_is_not_replaced_by_detection(tmp_path: Path) -> None:
    workflow = _write_airflow_like_workflow(tmp_path)
    report = run_unified_energy_evaluation(
        UnifiedEnergyConfig(workflow_path=workflow, orchestrator="argo", stdout=True)
    )

    assert report["workflow"]["orchestrator"] == "argo"
    assert report["workflow"]["orchestrator_requested"] == "argo"
    assert report["workflow"]["orchestrator_detected"] == "airflow"
    assert report["workflow"]["runtime_supported"] is False
    assert report["workflow"]["target_validation"] == "not_evaluated"
    assert report["warnings"][0]["code"] == "ORCHESTRATOR_DETECTION_MISMATCH"
    assert report["metadata"]["environment_scope"] == "controller"


def test_track_carbon_records_import_build_attempt(tmp_path: Path) -> None:
    workflow = _write_plain_workflow(tmp_path)
    report = run_unified_energy_evaluation(
        UnifiedEnergyConfig(
            workflow_path=workflow,
            track_carbon=True,
            carbon_country_iso="NOR",
            carbon_scale_factor=25,
            stdout=True,
        )
    )

    import_measurement = report["energy"]["import_build_measurement"]
    assert isinstance(import_measurement, dict)
    assert import_measurement["measurement_scope"] == "import_build_only"
    assert import_measurement["import_status"] == "ok"
    energy_profile = import_measurement["energy_profile"]
    assert energy_profile["scale_factor"] == 25.0
    assert "scaled_estimate" in energy_profile
    assert report["energy"]["attempted_modes"][0]["mode"] == "import_build"
    assert report["energy"]["evidence"]["measurement_kind"] == "import_build"
    assert report["energy"]["evidence"]["confidence"] == "low"
    assert report["energy"]["measurements"]["import_build"] == import_measurement


def test_zero_fill_runtime_subprocess_works_from_source_checkout(tmp_path: Path) -> None:
    workflow = _write_airflow_like_workflow(tmp_path)
    report = run_unified_energy_evaluation(
        UnifiedEnergyConfig(
            workflow_path=workflow,
            orchestrator="airflow",
            enable_energy_evaluation=True,
            energy_mode="zero_fill",
            energy_timeout_s=30,
            stdout=True,
        )
    )

    assert report["energy"]["attempted_modes"][0]["mode"] == "zero_fill"
    assert report["energy"]["mode_used"] == "zero_fill"
    assert report["energy"]["runtime_measurement"]["status"] == "ok"
    assert report["energy"]["evidence"]["measurement_kind"] == "surrogate_benchmark"
    assert report["energy"]["evidence"]["data_source"] == "zero_fill"
    assert report["energy"]["evidence"]["production_equivalent"] is False
    assert report["energy"]["measurements"]["runtime"] == report["energy"]["runtime_measurement"]


def test_fuzz_fill_runtime_mode(tmp_path: Path) -> None:
    workflow = _write_airflow_like_workflow(tmp_path)
    report = run_unified_energy_evaluation(
        UnifiedEnergyConfig(
            workflow_path=workflow,
            orchestrator="airflow",
            enable_energy_evaluation=True,
            energy_mode="fuzz_fill",
            energy_timeout_s=30,
            stdout=True,
        )
    )

    assert report["energy"]["mode_used"] == "fuzz_fill"
    assert report["energy"]["evidence"]["measurement_kind"] == "surrogate_benchmark"
    assert report["energy"]["evidence"]["data_source"] == "fuzz_fill"
    assert report["energy"]["runtime_measurement"]["status"] == "ok"


def test_scale_probe_runtime_mode(tmp_path: Path) -> None:
    workflow = _write_airflow_like_workflow(tmp_path)
    report = run_unified_energy_evaluation(
        UnifiedEnergyConfig(
            workflow_path=workflow,
            orchestrator="airflow",
            enable_energy_evaluation=True,
            energy_mode="scale_probe",
            energy_max_rows=25,
            energy_timeout_s=30,
            stdout=True,
        )
    )

    measurement = report["energy"]["runtime_measurement"]
    assert report["energy"]["mode_used"] == "scale_probe"
    assert report["energy"]["evidence"]["data_source"] == "scale_probe"
    assert measurement["status"] == "ok"
    assert measurement["scale_probe"]["row_counts"] == [10, 25]
    assert "duration_slope_per_row" in measurement["scale_probe"]
    for run in measurement["scale_probe"]["runs"]:
        assert run["normalized"]["energy_kwh_per_1k_rows"] is None
        assert run["telemetry"]["use"] == "attempt_diagnostics"


def test_operator_profile_runtime_mode(tmp_path: Path) -> None:
    workflow = _write_airflow_like_workflow(tmp_path)
    report = run_unified_energy_evaluation(
        UnifiedEnergyConfig(
            workflow_path=workflow,
            orchestrator="airflow",
            enable_energy_evaluation=True,
            energy_mode="operator_profile",
            energy_timeout_s=30,
            stdout=True,
        )
    )

    measurement = report["energy"]["runtime_measurement"]
    assert report["energy"]["mode_used"] == "operator_profile"
    assert report["energy"]["evidence"]["data_source"] == "operator_profile"
    assert measurement["status"] == "ok"
    assert measurement["workload_profile"] == "operator_profile"
    assert measurement["work_units"] >= 1


def test_surrogate_cannot_be_projected_even_with_energy(tmp_path: Path, monkeypatch) -> None:
    workflow = _write_airflow_like_workflow(tmp_path)
    monkeypatch.setattr(
        reval,
        "_run_runtime_measurement",
        lambda *args, **kwargs: {
            "ok": True,
            "runtime_measurement": {
                "status": "ok",
                "execution_adapter_used": "surrogate_benchmark",
                "energy_profile": {
                    "available": True,
                    "status": "ok",
                    "energy_consumed_kwh": 0.1,
                    "emissions_kgco2eq": 0.01,
                },
            },
        },
    )
    without_scenario = run_unified_energy_evaluation(
        UnifiedEnergyConfig(
            workflow_path=workflow,
            orchestrator="airflow",
            enable_energy_evaluation=True,
            energy_mode="zero_fill",
            energy_timeout_s=30,
            stdout=True,
        )
    )
    assert without_scenario["energy"]["measurements"]["estimate"] is None

    with_scenario = run_unified_energy_evaluation(
        UnifiedEnergyConfig(
            workflow_path=workflow,
            orchestrator="airflow",
            enable_energy_evaluation=True,
            energy_mode="zero_fill",
            energy_timeout_s=30,
            estimate_scenario_name="daily-prod",
            estimate_runs_per_year=365,
            estimate_years=2,
            estimate_repetitions_per_run=3,
            stdout=True,
        )
    )

    estimate = with_scenario["energy"]["measurements"]["estimate"]
    assert estimate["status"] == "not_available"
    assert estimate["measurement_kind"] == "scenario_projection"
    assert estimate["reason"] == "surrogate_is_not_workflow_execution"
    assert estimate["assumptions"]["scenario_name"] == "daily-prod"
    assert estimate["assumptions"]["runs_per_year"] == 365
    assert estimate["assumptions"]["years"] == 2.0
    assert estimate["assumptions"]["repetitions_per_run"] == 3.0
    assert not {"yearly", "period", "per_workflow_run", "uncertainty"} & estimate.keys()
    runtime = with_scenario["energy"]["measurements"]["runtime"]
    assert runtime["energy_profile"] is None
    assert runtime["attempt_diagnostics"]["modeled_energy"]["energy_consumed_kwh"] == 0.1


def test_public_api_exports() -> None:
    import pisces

    assert hasattr(pisces, "EnergyEvaluationRequest")
    assert hasattr(pisces, "EnergyEvaluationResult")
    assert hasattr(pisces, "evaluate_energy")
    assert hasattr(pisces, "UnifiedEnergyConfig")
    assert hasattr(pisces, "run_unified_energy_evaluation")


def test_compare_reports_ranks_by_rule_risk(tmp_path: Path) -> None:
    low = tmp_path / "low.json"
    high = tmp_path / "high.json"
    low.write_text(
        json.dumps(
            {
                "workflow": {"name": "low", "orchestrator": "airflow"},
                "energy": {
                    "mode_used": "heuristic",
                    "data_source": "none",
                    "heuristic": {"rules": {"rules": []}},
                },
            }
        ),
        encoding="utf-8",
    )
    high.write_text(
        json.dumps(
            {
                "workflow": {"name": "high", "orchestrator": "airflow"},
                "energy": {
                    "mode_used": "heuristic",
                    "data_source": "none",
                    "heuristic": {
                        "rules": {
                            "rules": [
                                {"rule_id": "X", "severity": "major"},
                                {"rule_id": "Y", "severity": "minor"},
                            ]
                        }
                    },
                },
            }
        ),
        encoding="utf-8",
    )

    result = compare_reports([low, high])
    assert result["report_count"] == 2
    assert result["ranked_by_rule_risk"][0]["workflow"]["name"] == "high"


def test_compare_reports_skips_non_evaluation_json(tmp_path: Path) -> None:
    report = tmp_path / "report.json"
    readiness = tmp_path / "readiness.json"
    report.write_text(
        json.dumps(
            {
                "tool": "pisces-unified",
                "workflow": {"name": "flow", "orchestrator": "airflow"},
                "energy": {
                    "mode_used": "heuristic",
                    "data_source": "none",
                    "heuristic": {"rules": {"rules": []}},
                    "evidence": {"measurement_kind": "static", "confidence": "low"},
                },
            }
        ),
        encoding="utf-8",
    )
    readiness.write_text(
        json.dumps({"tool": "pisces-diagnostics", "ready": True}),
        encoding="utf-8",
    )

    result = compare_reports([report, readiness])
    assert result["input_count"] == 2
    assert result["report_count"] == 1
    assert result["skipped_reports"][0]["path"].endswith("readiness.json")
    assert any("skipped" in warning for warning in result["warnings"])
