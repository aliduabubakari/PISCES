from __future__ import annotations

import ftplib
import hashlib
import json
from pathlib import Path
import urllib.request

import pytest

from pisces.energy.execution_sandbox import prepared_execution_sandbox
from pisces.energy.fixture_registry import (
    FixtureRegistry,
    UnregisteredExternalAccess,
    patched_external_services,
)
from pisces.energy.output_assertions import evaluate_output_assertions
from pisces.energy.runtime_runner_cli import _task_coverage
from pisces.energy.workload_contract import (
    WorkloadContractError,
    load_workload_contract,
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_contract_bundle(tmp_path: Path):
    artifact = tmp_path / "workflow.py"
    artifact.write_text("VALUE = 1\n", encoding="utf-8")
    fixture = tmp_path / "input.csv"
    fixture.write_text("id,value\n1,10\n2,20\n", encoding="utf-8")
    contract = tmp_path / "contract.json"
    contract.write_text(
        json.dumps(
            {
                "schema_version": "1.0",
                "contract_id": "example_v1",
                "pipeline_id": "example",
                "inputs": [
                    {
                        "integration_id": "source",
                        "kind": "http",
                        "fixture": "input.csv",
                        "sha256": _sha256(fixture),
                    }
                ],
                "required_input_ids": ["source"],
                "expected_work": {"logical_components": ["extract", "load"]},
                "outputs": [
                    {
                        "logical_name": "result",
                        "format": "csv",
                        "exact_rows": 2,
                        "required_columns": ["id", "value"],
                    }
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    binding = tmp_path / "binding.json"
    binding.write_text(
        json.dumps(
            {
                "schema_version": "1.0",
                "binding_id": "example_direct",
                "contract_id": "example_v1",
                "target": "airflow",
                "route": "direct",
                "artifact_path": str(artifact),
                "artifact_sha256": _sha256(artifact),
                "integration_bindings": {
                    "https://example.test/input.csv": "source",
                    "ftp://example.test/input.csv": "source",
                },
                "output_bindings": {"result": "outputs/result.csv"},
                "expected_task_ids": ["extract", "load"],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return artifact, fixture, contract, binding


def test_contract_loader_validates_artifact_and_fixture_hashes(tmp_path: Path) -> None:
    artifact, fixture, contract, binding = _write_contract_bundle(tmp_path)
    loaded = load_workload_contract(
        contract_path=contract,
        binding_path=binding,
        artifact_path=artifact,
    )

    provenance = loaded.provenance()
    assert provenance["status"] == "validated"
    assert provenance["contract_id"] == "example_v1"
    assert provenance["fixture_sha256"]["source"] == _sha256(fixture)

    fixture.write_text("changed\n", encoding="utf-8")
    with pytest.raises(WorkloadContractError, match="fixture_sha256_mismatch"):
        load_workload_contract(
            contract_path=contract,
            binding_path=binding,
            artifact_path=artifact,
        )


def test_sandbox_copies_fixtures_and_restores_working_directory(
    tmp_path: Path,
) -> None:
    artifact, _, contract, binding = _write_contract_bundle(tmp_path)
    loaded = load_workload_contract(
        contract_path=contract,
        binding_path=binding,
        artifact_path=artifact,
    )
    before = Path.cwd()
    with prepared_execution_sandbox(loaded) as sandbox:
        assert Path.cwd() == sandbox.working
        assert sandbox.fixtures["source"].read_text(encoding="utf-8").startswith(
            "id,value"
        )
    assert Path.cwd() == before


def test_sandbox_applies_recorded_exact_literal_rewrites(
    tmp_path: Path,
) -> None:
    artifact, _, contract, binding = _write_contract_bundle(tmp_path)
    artifact.write_text(
        'INPUT = "/data/input.csv"\nOUTPUT = "/tmp/result.csv"\n',
        encoding="utf-8",
    )
    binding_value = json.loads(binding.read_text(encoding="utf-8"))
    binding_value["artifact_sha256"] = _sha256(artifact)
    binding_value["literal_rewrites"] = {
        "/data/input.csv": "{working}/inputs/source.csv",
        "/tmp/result.csv": "{outputs}/result.csv",
    }
    binding.write_text(json.dumps(binding_value, indent=2), encoding="utf-8")
    loaded = load_workload_contract(
        contract_path=contract,
        binding_path=binding,
        artifact_path=artifact,
    )

    with prepared_execution_sandbox(loaded) as sandbox:
        bound = sandbox.workflow_file.read_text(encoding="utf-8")
        assert str(sandbox.working / "inputs" / "source.csv") in bound
        assert str(sandbox.outputs / "result.csv") in bound
        assert artifact.read_text(encoding="utf-8").startswith(
            'INPUT = "/data/input.csv"'
        )
        assert len(sandbox.literal_rewrites) == 2
        assert sandbox.workflow_sha256 != _sha256(artifact)


def test_fixture_registry_serves_registered_http_and_ftp_only(
    tmp_path: Path,
) -> None:
    artifact, _, contract, binding = _write_contract_bundle(tmp_path)
    loaded = load_workload_contract(
        contract_path=contract,
        binding_path=binding,
        artifact_path=artifact,
    )
    registry = FixtureRegistry(loaded)

    with patched_external_services(registry):
        with urllib.request.urlopen("https://example.test/input.csv") as response:
            assert response.read().startswith(b"id,value")

        downloaded = tmp_path / "downloaded.csv"
        urllib.request.urlretrieve(
            "https://example.test/input.csv",
            downloaded,
        )
        assert downloaded.read_bytes().startswith(b"id,value")

        received = bytearray()
        with ftplib.FTP("example.test") as ftp:
            ftp.login()
            ftp.cwd("/")
            assert ftp.pwd() == "/"
            ftp.retrbinary("RETR input.csv", received.extend)
        assert bytes(received).startswith(b"id,value")

        with pytest.raises(
            UnregisteredExternalAccess,
            match="unregistered_external_access",
        ):
            urllib.request.urlopen("https://unknown.test/input.csv")

    assert [item["status"] for item in registry.access_log] == [
        "served",
        "served",
        "served",
        "blocked",
    ]


def test_output_assertions_require_scoped_valid_output(tmp_path: Path) -> None:
    artifact, _, contract, binding = _write_contract_bundle(tmp_path)
    loaded = load_workload_contract(
        contract_path=contract,
        binding_path=binding,
        artifact_path=artifact,
    )
    workspace = tmp_path / "workspace"
    output = workspace / "outputs" / "result.csv"
    output.parent.mkdir(parents=True)
    output.write_text("id,value\n1,10\n2,20\n", encoding="utf-8")

    result = evaluate_output_assertions(loaded, workspace_root=workspace)
    assert result["passed"] is True
    assert result["results"][0]["row_count"] == 2
    assert result["results"][0]["sha256"] == _sha256(output)


def test_output_assertions_support_airflow_task_returns(tmp_path: Path) -> None:
    artifact, _, contract, binding = _write_contract_bundle(tmp_path)
    contract_value = json.loads(contract.read_text(encoding="utf-8"))
    contract_value["outputs"] = [
        {
            "logical_name": "audit",
            "format": "task_return",
            "expected_type": "object",
            "required_keys": ["status"],
        }
    ]
    contract.write_text(json.dumps(contract_value, indent=2), encoding="utf-8")
    binding_value = json.loads(binding.read_text(encoding="utf-8"))
    binding_value["output_bindings"] = {
        "audit": {"kind": "task_return", "task_id": "audit_log"}
    }
    binding.write_text(json.dumps(binding_value, indent=2), encoding="utf-8")
    loaded = load_workload_contract(
        contract_path=contract,
        binding_path=binding,
        artifact_path=artifact,
    )

    result = evaluate_output_assertions(
        loaded,
        workspace_root=tmp_path,
        native_metadata={
            "task_outputs": {
                "audit_log": {
                    "present": True,
                    "json_type": "object",
                    "keys": ["status", "rows"],
                    "sha256": "example",
                }
            }
        },
    )

    assert result["passed"] is True
    assert result["results"][0]["task_id"] == "audit_log"


def test_task_coverage_accepts_declared_branch_skip() -> None:
    class Workload:
        binding = {
            "expected_task_ids": ["scan", "audit"],
            "alternative_task_groups": [["publish", "remove"]],
            "allowed_skipped_task_ids": ["publish", "remove"],
        }

    result = _task_coverage(
        Workload(),
        {
            "native_task_results": [
                {"task_id": "scan", "status": "executed"},
                {"task_id": "remove", "status": "executed"},
                {"task_id": "publish", "status": "skipped"},
                {"task_id": "audit", "status": "executed"},
            ]
        },
    )

    assert result["passed"] is True
    assert result["unsatisfied_alternative_task_groups"] == []
