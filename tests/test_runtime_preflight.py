from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from pisces.energy import runtime_evaluation as evaluator
from pisces.energy import runtime_runner_cli as runner
from pisces.report_validation import validate_report
from pisces.unified_evaluator import UnifiedEnergyConfig, run_unified_energy_evaluation


def _unexpected(*args, **kwargs):
    pytest.fail("Preflight must not start an import, runtime, or hosted provider")


@pytest.mark.parametrize(("config", "reason"), [
    ({"experiment_mode": True, "energy_execution_adapter": "auto"}, "experiment_requires_explicit_native_adapter"),
    ({"experiment_mode": True, "energy_execution_adapter": "surrogate_benchmark"}, "experiment_requires_explicit_native_adapter"),
    ({"experiment_mode": True, "energy_mode": "auto"}, "experiment_requires_explicit_local_input_mode"),
    ({"experiment_mode": True, "energy_mode": "synthetic"}, "experiment_requires_explicit_local_input_mode"),
    ({"experiment_mode": True, "energy_mode": "heuristic"}, "experiment_requires_explicit_local_input_mode"),
    ({"experiment_mode": True}, "experiment_requires_workload_contract_and_binding"),
    ({"experiment_mode": True, "workload_contract": Path("contract.json"), "artifact_binding": Path("binding.json")}, "experiment_requires_runtime_python"),
    ({"energy_mode": "synthetic"}, "native_hosted_synthetic_not_supported"),
    ({"energy_mode": "scale_probe"}, "native_diagnostic_mode_not_supported"),
    ({"energy_mode": "sample"}, "native_sample_requires_workload_contract_and_binding"),
    ({"orchestrator": "unknown"}, "native_target_not_supported"),
    ({"orchestrator": "argo"}, "native_target_not_supported"),
    ({"energy_runtime_python": Path("nonexistent-runtime/python")}, "runtime_python_unavailable"),
    ({}, "native_runtime_unavailable"),
])
def test_controller_preflight_rejects_before_side_effects(tmp_path, monkeypatch, config, reason) -> None:
    workflow = tmp_path / "flow.py"
    workflow.write_text("from airflow import DAG\n", encoding="utf-8")
    for name in ("_run_import_build_measurement", "_run_runtime_measurement", "build_provider"):
        monkeypatch.setattr(evaluator, name, _unexpected)
    monkeypatch.setattr(evaluator, "detect_orchestrator_version", lambda *a, **kw: None)
    args = {"energy_execution_adapter": "native", "energy_mode": "fuzz_fill", **config}

    report = run_unified_energy_evaluation(UnifiedEnergyConfig(
        workflow_path=workflow, enable_energy_evaluation=True, track_carbon=True, **args,
    ))

    energy = report["energy"]
    assert energy["errors"][0]["message"] == reason
    assert energy["runtime_measurement"]["execution_outcome"] == "preflight_rejected"
    assert energy["runtime_measurement"]["execution_adapter_used"] is None
    assert len(energy["attempts"]) == 1
    assert validate_report(report).status == "current_inventory"


@pytest.mark.parametrize(("experiment", "adapter", "mode", "reason"), [
    (True, "auto", "fuzz_fill", "experiment_requires_explicit_native_adapter"),
    (True, "surrogate_benchmark", "fuzz_fill", "experiment_requires_explicit_native_adapter"),
    (True, "native", "synthetic", "experiment_requires_explicit_local_input_mode"),
    (True, "native", "fuzz_fill", "experiment_requires_workload_contract_and_binding"),
    (False, "misspelled", "fuzz_fill", "unknown_execution_adapter"),
    (False, "native", "fuzz_fill", "airflow_runtime_not_available"),
])
def test_worker_preflight_never_executes_a_rejected_request(tmp_path, monkeypatch, experiment, adapter, mode, reason):
    request, output = tmp_path / "request.json", tmp_path / "output.json"
    request.write_text(json.dumps({
        "experiment_mode": experiment, "execution_adapter": adapter,
        "mode": mode, "orchestrator": "airflow", "rows": [{"id": 1}],
    }), encoding="utf-8")
    monkeypatch.setattr(sys, "argv", ["runner", "--in", str(request), "--out", str(output)])
    monkeypatch.setattr(runner, "find_spec", lambda name: None)
    for name in ("_run_workload", "execute_native_workload", "try_start_tracker", "prepare_airflow_test_environment"):
        monkeypatch.setattr(runner, name, _unexpected)

    runner.main()

    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["ok"] is False
    assert payload["error"]["message"] == reason
    assert payload["runtime_measurement"]["execution_adapter_used"] is None


def test_auto_data_mode_never_implies_hosted_synthetic(tmp_path, monkeypatch) -> None:
    workflow = tmp_path / "workflow.py"
    workflow.write_text("from airflow import DAG\n", encoding="utf-8")
    monkeypatch.setattr(evaluator, "build_provider", _unexpected)
    monkeypatch.setattr(evaluator, "_run_runtime_measurement", lambda **kwargs: {"ok": False})

    report = run_unified_energy_evaluation(UnifiedEnergyConfig(
        workflow_path=workflow, enable_energy_evaluation=True,
        llm_provider="openrouter", llm_model="configured-model",
    ))
    assert [attempt["mode"] for attempt in report["energy"]["attempts"]] == [
        "sample", "fuzz_fill", "zero_fill", "heuristic",
    ]
