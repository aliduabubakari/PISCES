from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

from pisces.energy import runtime_evaluation as evaluator
from pisces.energy import runtime_runner_cli as runner
from pisces.energy.runtime_evidence import finalize_runtime_measurement


@pytest.mark.parametrize(
    ("outcome", "contract_requested", "output_passed", "contract_status", "screening"),
    [
        ("surrogate", False, None, "not_requested", False),
        ("failed", True, None, "not_verified", False),
        ("partial", True, None, "not_verified", False),
        ("zero_work", True, None, "not_verified", False),
        ("mixed_execution", True, True, "not_verified", False),
        ("completed", False, None, "not_requested", False),
        ("completed", True, False, "failed", False),
        ("completed", True, True, "checks_passed", True),
    ],
)
def test_observations_never_credit_unverified_work(
    outcome, contract_requested, output_passed, contract_status, screening
) -> None:
    measurement = {
        "status": "ok",
        "execution_outcome": outcome,
        "execution_adapter_used": "surrogate_benchmark" if outcome == "surrogate" else "native",
        "eligible_for_resource_comparison": True,
        "eligible_for_paired_equivalence_comparison": True,
        "normalized": {"energy_kwh_per_1k_rows": 0.3},
        "operational_metrics": {"wall_clock_s": 3, "process_cpu_s": 2, "peak_rss_mb": 4},
        "modeled_energy": {"status": "not_started", "available": False},
        "task_coverage": {"passed": True} if output_passed is not None else None,
        "input_consumption": {"passed": True} if output_passed is not None else None,
        "output_assertions": {"passed": output_passed} if output_passed is not None else None,
    }

    finalize_runtime_measurement(measurement, adapter_requested="auto", contract_requested=contract_requested)
    first_result = dict(measurement)
    finalize_runtime_measurement(measurement, adapter_requested="auto", contract_requested=contract_requested)

    assert measurement == first_result
    assert measurement["execution_outcome"] == outcome
    assert measurement["execution_adapter_requested"] == "auto"
    assert measurement["contract_validation"]["status"] == contract_status
    assert measurement["pairwise_equivalence"] == {"status": "not_evaluated", "screening_eligible": screening}
    assert measurement["eligible_for_resource_comparison"] is False
    assert measurement["normalized"]["energy_kwh_per_1k_rows"] is None
    assert measurement["normalized"]["emissions_kgco2eq_per_1k_rows"] is None
    assert measurement["telemetry"]["operational"] == "available"
    assert measurement["telemetry"]["modeled_energy"] == "unavailable"
    if screening:
        assert measurement["telemetry"]["use"] == "workload_screening"
        assert measurement["operational_metrics"]["wall_clock_s"] == 3
    else:
        assert measurement["telemetry"]["use"] == "attempt_diagnostics"
        assert measurement["operational_metrics"] is None
        assert measurement["attempt_diagnostics"]["operational_metrics"]["wall_clock_s"] == 3


def test_pending_equivalence_does_not_upgrade_confidence() -> None:
    evidence = evaluator._build_evidence(
        mode_used="sample",
        data_source="sample",
        execution_adapter="native",
        import_build_measurement=None,
        runtime_measurement={
            "execution_outcome": "completed",
            "execution_adapter_used": "native",
            "eligible_for_paired_equivalence_comparison": True,
        },
    )
    assert evidence["confidence"] == "medium"


def test_failed_runner_is_not_reported_as_resolved_adapter(tmp_path: Path) -> None:
    request = evaluator.EnergyEvaluationRequest(
        file_path=tmp_path / "workflow.py", code_text="", orchestrator="airflow",
        repo_root=tmp_path, execution_adapter="native",
    )
    observation = evaluator._runtime_observation({"ok": False, "_subprocess": {"timed_out": True}}, request)
    assert observation["execution_outcome"] == "timed_out"
    assert observation["execution_adapter_requested"] == "native"
    assert observation["execution_adapter_used"] is None
    assert observation["telemetry"]["operational"] == "not_collected"
    assert observation["attempt_diagnostics"] is None


def test_mixed_block_is_not_labeled_by_final_repetition(tmp_path: Path, monkeypatch) -> None:
    def run_native(*args, run_index, **kwargs):
        if run_index == 0:
            raise runner.NativeAdapterError("first_run_unavailable")
        return {"native_tasks_executed": 1, "duration_s": 0.01}

    request, output = tmp_path / "request.json", tmp_path / "output.json"
    request.write_text(json.dumps({
        "orchestrator": "prefect", "execution_adapter": "auto",
        "minimum_repetitions": 2, "maximum_repetitions": 2,
        "rows": [{"id": 1}],
    }), encoding="utf-8")
    monkeypatch.setattr(sys, "argv", ["runner", "--in", str(request), "--out", str(output)])
    monkeypatch.setattr(runner, "try_start_tracker", lambda **kwargs: None)
    monkeypatch.setattr(runner, "execute_native_workload", run_native)

    runner.main()

    measurement = json.loads(output.read_text(encoding="utf-8"))["runtime_measurement"]
    assert measurement["execution_outcome"] == "mixed_execution"
    assert measurement["execution_adapter_requested"] == "auto"
    assert measurement["execution_adapter_used"] == "mixed"
    assert measurement["eligible_for_resource_comparison"] is False
    assert measurement["normalized"]["energy_kwh_per_1k_rows"] is None
    diagnostics = measurement["attempt_diagnostics"]
    assert len(diagnostics["repetition_metrics"]) == 2
    assert diagnostics["modeled_energy"]["measurement_scope"] == "mixed_execution_block"
