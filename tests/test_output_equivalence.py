from __future__ import annotations

import json
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

from benchmarks.climate_fusion.benchmark import build_bundle, run_bundle
from pisces.energy import output_equivalence as oracle
from pisces.energy import runtime_runner_cli as runner
from pisces.energy.execution_sandbox import prepared_execution_sandbox
from pisces.energy.workload_contract import WorkloadContractError, _v2_validator, load_workload_contract, sha256_file


def _load(root: Path, variant: str = "baseline"):
    return load_workload_contract(
        contract_path=root / "contract.json", binding_path=root / f"{variant}.binding.json",
        artifact_path=root / f"{variant}.py",
    )


def _update(path: Path, change) -> None:
    value = json.loads(path.read_text(encoding="utf-8"))
    change(value)
    path.write_text(json.dumps(value), encoding="utf-8")


@pytest.fixture
def bundle(tmp_path: Path) -> Path:
    root = tmp_path / "bundle"
    build_bundle(root, rows_per_source=2)
    return root


def _output(bundle: Path, root: Path, transform=lambda text: text) -> Path:
    output = root / "outputs" / "fusion.csv"
    output.parent.mkdir(parents=True)
    output.write_text(transform((bundle / "expected.csv").read_text(encoding="utf-8")), encoding="utf-8")
    return root


def test_climate_fusion_discriminates_equal_shape_wrong_values(bundle: Path) -> None:
    verdict = run_bundle(bundle)
    good, wrong = verdict["verification"]["baseline"], verdict["verification"]["wrong_units"]
    assert good["status"] == "passed"
    assert wrong["status"] == "failed"
    assert wrong["results"][0]["reason"] == "output_content_mismatch"
    assert good["results"][0]["row_count"] == wrong["results"][0]["row_count"] == 10
    assert verdict["pairwise"]["single_read"]["status"] == "passed"
    assert verdict["pairwise"]["wrong_units"]["status"] == "failed"
    assert str(bundle) not in json.dumps(verdict)
    pair = verdict["pairwise"]["single_read"]
    assert pair["left"]["artifact_sha256"] != pair["right"]["artifact_sha256"]
    assert pair["left_verification"]["results"][0]["sha256"] != pair["right_verification"]["results"][0]["sha256"]
    assert pair["left_verification"]["results"][0]["semantic_sha256"] == pair["right_verification"]["results"][0]["semantic_sha256"]


@pytest.mark.parametrize(("change", "reason"), [
    (lambda lines: [*lines, lines[1]], "duplicate_output_key"),
    (lambda lines: [lines[0].replace("temperature_c", "temperature_f"), *lines[1:]], "output_schema_mismatch"),
    (lambda lines: [lines[0], "bom,0,", *lines[2:]], "output_value_type_mismatch"),
    (lambda lines: [lines[0], "bom,bad,20", *lines[2:]], "output_value_type_mismatch"),
    (lambda lines: lines[:-1], "output_key_set_mismatch"),
])
def test_typed_output_mismatches(bundle: Path, tmp_path: Path, change, reason: str) -> None:
    workspace = _output(bundle, tmp_path / "work", lambda text: "\n".join(change(text.splitlines())) + "\n")
    result = oracle.verify_output(_load(bundle), workspace_root=workspace)
    assert result["status"] == "failed"
    assert result["results"][0]["reason"] == reason


def test_pairwise_checks_tolerance_directly(bundle: Path, tmp_path: Path) -> None:
    from decimal import Decimal

    _update(bundle / "contract.json", lambda data: data["outputs"][0]["columns"][2].update(absolute_tolerance=0.1))
    workload = _load(bundle)

    def adjust(text: str, delta: str) -> str:
        lines = text.splitlines()
        prefix, value = lines[1].rsplit(",", 1)
        lines[1] = f"{prefix},{Decimal(value) + Decimal(delta)}"
        return "\n".join(lines) + "\n"

    left = _output(bundle, tmp_path / "left", lambda text: adjust(text, "0.1"))
    right = _output(bundle, tmp_path / "right", lambda text: adjust(text, "-0.1"))
    left_verdict = oracle.verify_output(workload, workspace_root=left)
    assert left_verdict["status"] == "passed"
    assert "semantic_sha256" not in left_verdict["results"][0]
    assert oracle.verify_output(workload, workspace_root=right)["status"] == "passed"
    verdict = oracle.compare_outputs(workload, workload, left_workspace=left, right_workspace=right)
    assert verdict["status"] == "failed"
    assert verdict["reason"] == "output_content_mismatch"


def test_exact_row_order_is_enforced(bundle: Path, tmp_path: Path) -> None:
    _update(bundle / "contract.json", lambda data: data["outputs"][0].update(row_order="exact"))
    workspace = _output(bundle, tmp_path / "work", lambda text: "\n".join(
        [text.splitlines()[0], *reversed(text.splitlines()[1:])]
    ) + "\n")
    assert oracle.verify_output(_load(bundle), workspace_root=workspace)["results"][0]["reason"] == "output_order_mismatch"


def test_validation_budget_is_not_a_pass(bundle: Path, tmp_path: Path, monkeypatch) -> None:
    workspace = _output(bundle, tmp_path / "work")
    monkeypatch.setattr(oracle, "MAX_ROWS", 1)
    result = oracle.verify_output(_load(bundle), workspace_root=workspace)
    assert result["status"] == "not_verified"
    assert result["passed"] is None
    assert result["results"][0]["reason"] == "output_row_budget_exceeded"


def test_sorted_stream_matches_existing_digest_beyond_legacy_limit(tmp_path: Path, monkeypatch) -> None:
    bundle = tmp_path / "scaled"
    build_bundle(bundle, rows_per_source=3, scaling=True)
    def reformat(content):
        lines = content.splitlines()
        lines[1] += "0"
        return "\n".join(lines) + "\n"

    workspace = _output(bundle, tmp_path / "work", reformat)
    workload = _load(bundle)
    spec = workload.contract["outputs"][0]
    observed = oracle._read_table(bundle / "expected.csv", spec)
    monkeypatch.setattr(oracle, "MAX_ROWS", 1)
    result = oracle.verify_output(workload, workspace_root=workspace)["results"][0]
    assert result["row_count"] == 15
    assert result["semantic_sha256"] == oracle._semantic_digest(observed, spec)
    assert result["sha256"] != observed.sha256
    assert result["passed"] is True
    assert oracle.compare_outputs(workload, workload, left_workspace=workspace, right_workspace=workspace)["status"] == "passed"
    monkeypatch.setattr(oracle, "MAX_SORTED_ROWS", 14)
    assert oracle.verify_output(workload, workspace_root=workspace)["status"] == "not_verified"


def test_sorted_stream_keeps_byte_budget_and_requires_exact_values(tmp_path: Path, monkeypatch) -> None:
    bundle = tmp_path / "scaled"
    build_bundle(bundle, rows_per_source=2, scaling=True)
    workspace = _output(bundle, tmp_path / "work")
    monkeypatch.setattr(oracle, "MAX_BYTES", 16)
    result = oracle.verify_output(_load(bundle), workspace_root=workspace)["results"][0]
    assert result["status"] == "not_verified"
    assert result["reason"] == "output_byte_budget_exceeded"
    _update(bundle / "contract.json", lambda data: data["outputs"][0]["columns"][2].update(absolute_tolerance=0.1))
    with pytest.raises(WorkloadContractError, match="key_sorted_requires_exact_values"):
        _load(bundle)


@pytest.mark.parametrize(("problem", "reason"), [
    ("duplicate", "duplicate_output_key"), ("reorder", "output_order_mismatch"),
    ("short", "output_key_set_mismatch"), ("wrong", "output_content_mismatch"),
    ("reference", "reference_hash_mismatch"),
])
def test_sorted_stream_rejects_invalid_output_and_reference(tmp_path: Path, problem: str, reason: str) -> None:
    bundle = tmp_path / "scaled"
    build_bundle(bundle, rows_per_source=2, scaling=True)
    workload = _load(bundle)

    def change(content):
        lines = content.splitlines(keepends=True)
        if problem == "reference":
            (bundle / "expected.csv").write_text(content.replace(lines[-1], "noaa,1,999\n"), encoding="utf-8")
        elif problem == "duplicate":
            lines.insert(2, lines[1])
        elif problem == "reorder":
            lines[1], lines[2] = lines[2], lines[1]
        elif problem == "short":
            lines.pop()
        elif problem == "wrong":
            lines[-1] = "noaa,1,999\n"
        return "".join(lines)

    result = oracle.verify_output(workload, workspace_root=_output(bundle, tmp_path / "work", change))
    assert result["results"][0]["reason"] == reason
    assert result["status"] == ("not_verified" if problem == "reference" else "failed")


def test_required_service_calls_are_verified_separately(bundle: Path, tmp_path: Path) -> None:
    def contract_change(data):
        data["inputs"][0]["kind"] = "http"
        data["service_effects"] = [{"integration_id": "bom", "served_count": 1}]

    def binding_change(data):
        data["file_bindings"].pop("bom")
        data["integration_bindings"] = {"https://example.test/data": "bom"}

    _update(bundle / "contract.json", contract_change)
    _update(bundle / "baseline.binding.json", binding_change)
    workload = _load(bundle)
    workspace = _output(bundle, tmp_path / "work")
    assert oracle.verify_output(workload, workspace_root=workspace)["status"] == "not_verified"
    assert oracle.verify_output(workload, workspace_root=workspace, service_log=[])["status"] == "failed"
    log = [{"integration_id": "bom", "status": "served"}]
    assert oracle.verify_output(workload, workspace_root=workspace, service_log=log)["status"] == "passed"
    assert oracle.verify_output(workload, workspace_root=workspace, service_log=log * 2)["status"] == "failed"


@pytest.mark.parametrize(("field", "value", "reason"), [
    ("artifact_sha256", "0" * 64, "binding_artifact_sha256_mismatch"),
    ("target", "unknown", "artifact-binding_schema_invalid"),
    ("file_bindings", {}, "file_bindings_do_not_match_contract"),
    ("output_bindings", {}, "output_bindings_do_not_match_contract"),
    ("output_bindings", {"fusion": "../escaped.csv"}, "path_must_be_confined_relative"),
])
def test_v2_binding_validation(bundle: Path, field: str, value, reason: str) -> None:
    _update(bundle / "baseline.binding.json", lambda data: data.update({field: value}))
    with pytest.raises(WorkloadContractError, match=reason):
        _load(bundle)


def test_relocated_bundle_and_staged_hashes(bundle: Path, tmp_path: Path) -> None:
    relocated = tmp_path / "relocated"
    shutil.copytree(bundle, relocated)
    workload = _load(relocated)
    assert workload.provenance() == _load(bundle).provenance()
    with prepared_execution_sandbox(workload) as staged:
        assert sha256_file(staged.workflow_file) == workload.artifact_sha256
    workload.fixtures["bom"].write_text("changed", encoding="utf-8")
    with pytest.raises(WorkloadContractError, match="staged_fixture_sha256_mismatch"):
        with prepared_execution_sandbox(workload):
            pytest.fail("changed fixture was staged")


def test_v2_schemas_are_valid_and_fixture_symlinks_cannot_escape(bundle: Path, tmp_path: Path) -> None:
    for name in ("workload-contract", "artifact-binding"):
        validator = _v2_validator(name)
        validator.check_schema(validator.schema)
    external = tmp_path / "external.csv"
    fixture = bundle / "fixtures" / "bom.csv"
    fixture.rename(external)
    fixture.symlink_to(external)
    with pytest.raises(WorkloadContractError, match="path_outside_declared_root"):
        _load(bundle)


def test_changed_reference_is_not_verified(bundle: Path, tmp_path: Path) -> None:
    workload = _load(bundle)
    workspace = _output(bundle, tmp_path / "work")
    (bundle / "expected.csv").write_text("changed", encoding="utf-8")
    result = oracle.verify_output(workload, workspace_root=workspace)
    assert result["status"] == "not_verified"
    assert result["results"][0]["reason"] == "reference_hash_mismatch"


@pytest.mark.parametrize(("variant", "passes"), [("baseline", True), ("wrong_units", False)])
def test_native_runner_applies_v2_oracle(bundle: Path, tmp_path: Path, monkeypatch, variant, passes) -> None:
    request, output = tmp_path / "request.json", tmp_path / "output.json"
    request.write_text(json.dumps({
        "orchestrator": "airflow", "workflow_file": str(bundle / f"{variant}.py"),
        "mode": "zero_fill", "execution_adapter": "native", "experiment_mode": True,
        "workload_contract": str(bundle / "contract.json"),
        "artifact_binding": str(bundle / f"{variant}.binding.json"),
        "maximum_repetitions": 1,
    }), encoding="utf-8")

    def execute(path, **kwargs):
        subprocess.run([sys.executable, str(path)], check=True, timeout=10)
        return {
            "native_tasks_executed": 1,
            "native_task_results": [{"task_id": "fuse", "status": "executed"}],
            "native_metadata": {"dag_state": "success", "fixture_access_log": []},
            "native_safety": {"accessed_operations": [
                {"path": str(path.parent / destination), "mode": "r"}
                for destination in _load(bundle, variant).binding["file_bindings"].values()
            ]},
        }

    # The adapter boundary is mocked; staging, task computation, and the oracle are real.
    from contextlib import ExitStack
    import os

    with ExitStack() as cleanup:
        for key in ("AIRFLOW_HOME", "AIRFLOW__CORE__LOAD_EXAMPLES", "AIRFLOW__CORE__EXECUTOR",
                    "AIRFLOW__CORE__DAGS_FOLDER", "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
                    "AIRFLOW__LOGGING__BASE_LOG_FOLDER"):
            if key in os.environ:
                cleanup.callback(os.environ.__setitem__, key, os.environ[key])
            else:
                cleanup.callback(os.environ.pop, key, None)
        monkeypatch.setattr(runner.atexit, "register", lambda callback, *args: cleanup.callback(callback, *args))
        monkeypatch.setattr(sys, "argv", ["runner", "--in", str(request), "--out", str(output)])
        monkeypatch.setattr(runner, "find_spec", lambda name: object())
        monkeypatch.setattr(runner, "prepare_airflow_test_environment", lambda: {})
        monkeypatch.setattr(runner, "try_start_tracker", lambda **kwargs: None)
        monkeypatch.setattr(runner, "execute_native_workload", execute)
        runner.main()

    payload = json.loads(output.read_text(encoding="utf-8"))
    measurement = payload["runtime_measurement"]
    assert payload["ok"] is passes
    assert measurement["execution_outcome"] == "completed"
    assert measurement["output_assertions"]["passed"] is passes
    assert measurement["contract_validation"]["scope"] == "single_execution_checks"
    assert measurement["eligible_for_resource_comparison"] is False
    assert measurement["pairwise_equivalence"]["status"] == "not_evaluated"
    assert "bound_workflow_path" not in measurement["execution_binding"]
    assert "native_safety" not in measurement
