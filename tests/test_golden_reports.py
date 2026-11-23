from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

import pisces.energy.runtime_evaluation as reval
from pisces.report_validation import validate_report
from pisces.unified_evaluator import UnifiedEnergyConfig, run_unified_energy_evaluation


GOLDEN_DIR = Path(__file__).parent / "golden_reports"


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
    workflow = tmp_path / "flow.py"
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


def _fixed_runtime_measurement(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    return {
        "ok": True,
        "runtime_measurement": {
            "status": "ok",
            "execution_outcome": "surrogate",
            "execution_adapter_requested": "representative",
            "execution_adapter_used": "representative",
            "adapter_attempts": [{"adapter": "representative", "status": "success"}],
            "rows_processed": 3,
            "tasks_exercised": 1,
            "duration_s": 0.01,
            "energy_profile": {
                "tool": "codecarbon",
                "available": True,
                "status": "ok",
                "measurement_scope": "runtime_representative_workload",
                "energy_consumed_kwh": 0.001,
                "emissions_kgco2eq": 0.0005,
            },
            "normalized": {
                "energy_kwh_per_1k_rows": 0.3333333333,
                "emissions_kgco2eq_per_1k_rows": 0.1666666667,
            },
            "bounds": {"max_rows": 10, "max_tasks": 5, "seed": 1729},
        },
    }


def _static_snapshot(report: Dict[str, Any]) -> Dict[str, Any]:
    energy = report["energy"]
    return {
        "report_schema_version": report["report_schema_version"],
        "tool": report["tool"],
        "workflow": {key: value for key, value in report["workflow"].items() if key != "path"},
        "energy_evaluation": report["energy_evaluation"],
        "energy": {
            "mode_used": energy["mode_used"],
            "data_source": energy["data_source"],
            "attempts": [
                {
                    "tier": item["tier"],
                    "mode": item["mode"],
                    "status": item["status"],
                    "reason": item["reason"],
                }
                for item in energy["attempts"]
            ],
            "evidence": {
                key: energy["evidence"][key]
                for key in [
                    "measurement_kind",
                    "confidence",
                    "production_equivalent",
                    "data_source",
                    "execution_adapter",
                    "measurement_scope",
                ]
            },
            "measurements": energy["measurements"],
        },
    }


def _sample_snapshot(report: Dict[str, Any]) -> Dict[str, Any]:
    energy = report["energy"]
    summary = energy["attempted_modes"][0]["sample_summary"]
    estimate = energy["measurements"]["estimate"]
    runtime = energy["measurements"]["runtime"]
    return {
        "report_schema_version": report["report_schema_version"],
        "tool": report["tool"],
        "workflow": {
            "name": report["workflow"]["name"],
            "orchestrator": report["workflow"]["orchestrator"],
            "runtime_supported": report["workflow"]["runtime_supported"],
        },
        "energy_evaluation": report["energy_evaluation"],
        "energy": {
            "mode_used": energy["mode_used"],
            "data_source": energy["data_source"],
            "evidence": {
                key: energy["evidence"][key]
                for key in [
                    "measurement_kind",
                    "confidence",
                    "production_equivalent",
                    "data_source",
                    "execution_adapter",
                ]
            },
            "sample_summary": {
                "row_count": summary["row_count"],
                "schema_column_count": summary["schema_summary"]["column_count"],
                "mapping_enabled": summary["mapping"]["enabled"],
                "mapped_column_count": summary["mapping"]["mapped_column_count"],
            },
            "estimate": {
                "status": estimate["status"],
                "measurement_kind": estimate["measurement_kind"],
                "reason": estimate["reason"],
                "assumptions": estimate["assumptions"],
            },
            "runtime": {
                key: runtime[key]
                for key in ("execution_outcome", "contract_validation", "telemetry", "pairwise_equivalence", "normalized")
            },
        },
    }


def test_static_heuristic_golden_snapshot(tmp_path: Path) -> None:
    workflow = _write_plain_workflow(tmp_path)
    report = run_unified_energy_evaluation(UnifiedEnergyConfig(workflow_path=workflow, stdout=True))

    expected = json.loads((GOLDEN_DIR / "static_heuristic.json").read_text(encoding="utf-8"))
    assert _static_snapshot(report) == expected
    assert validate_report(report).status == "current_inventory"


def test_sample_estimate_golden_snapshot(tmp_path: Path, monkeypatch) -> None:
    workflow = _write_airflow_like_workflow(tmp_path)
    sample = Path(__file__).parent / "fixtures" / "sample_orders.csv"
    monkeypatch.setattr(reval, "_run_runtime_measurement", _fixed_runtime_measurement)

    report = run_unified_energy_evaluation(
        UnifiedEnergyConfig(
            workflow_path=workflow,
            orchestrator="airflow",
            enable_energy_evaluation=True,
            energy_mode="sample",
            energy_sample_paths=[sample],
            energy_sample_schema_map={
                "orderIdentifier": "order_id",
                "totalAmount": "amount",
                "isPriority": "priority",
            },
            estimate_scenario_name="golden-demo",
            estimate_runs_per_year=365,
            estimate_years=1,
            estimate_repetitions_per_run=1,
            stdout=True,
        )
    )

    expected = json.loads((GOLDEN_DIR / "sample_estimate.json").read_text(encoding="utf-8"))
    assert _sample_snapshot(report) == expected
    assert validate_report(report).status == "current_inventory"
