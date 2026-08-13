"""Validation and provenance for experiment-owned workload contracts."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
from typing import Any, Dict


class WorkloadContractError(ValueError):
    pass


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_object(path: Path, *, label: str) -> Dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise WorkloadContractError(f"{label}_invalid_json:{exc}") from exc
    if not isinstance(value, dict):
        raise WorkloadContractError(f"{label}_must_be_object")
    return value


def _require_nonempty_string(value: Dict[str, Any], key: str, *, label: str) -> str:
    found = value.get(key)
    if not isinstance(found, str) or not found.strip():
        raise WorkloadContractError(f"{label}_missing_or_invalid:{key}")
    return found.strip()


@dataclass(frozen=True)
class LoadedWorkloadContract:
    contract_path: Path
    binding_path: Path
    artifact_path: Path
    contract: Dict[str, Any]
    binding: Dict[str, Any]
    fixtures: Dict[str, Path]
    contract_sha256: str
    binding_sha256: str
    artifact_sha256: str

    def provenance(self) -> Dict[str, Any]:
        return {
            "status": "validated",
            "contract_id": self.contract["contract_id"],
            "binding_id": self.binding["binding_id"],
            "contract_path": str(self.contract_path),
            "binding_path": str(self.binding_path),
            "artifact_path": str(self.artifact_path),
            "contract_sha256": self.contract_sha256,
            "binding_sha256": self.binding_sha256,
            "artifact_sha256": self.artifact_sha256,
            "fixture_sha256": {
                integration_id: sha256_file(path)
                for integration_id, path in sorted(self.fixtures.items())
            },
            "target": self.binding["target"],
            "route": self.binding["route"],
            "contract_status": self.contract.get("status"),
            "binding_status": self.binding.get("status"),
            "known_limitations": list(self.binding.get("known_limitations") or []),
        }


def load_workload_contract(
    *,
    contract_path: Path,
    binding_path: Path,
    artifact_path: Path,
) -> LoadedWorkloadContract:
    contract_path = contract_path.expanduser().resolve()
    binding_path = binding_path.expanduser().resolve()
    artifact_path = artifact_path.expanduser().resolve()
    for path, label in (
        (contract_path, "contract"),
        (binding_path, "binding"),
        (artifact_path, "artifact"),
    ):
        if not path.is_file():
            raise WorkloadContractError(f"{label}_not_found:{path}")

    contract = _load_object(contract_path, label="contract")
    binding = _load_object(binding_path, label="binding")
    if contract.get("schema_version") != "1.0":
        raise WorkloadContractError("unsupported_contract_schema_version")
    if binding.get("schema_version") != "1.0":
        raise WorkloadContractError("unsupported_binding_schema_version")

    contract_id = _require_nonempty_string(
        contract, "contract_id", label="contract"
    )
    _require_nonempty_string(contract, "pipeline_id", label="contract")
    binding_id = _require_nonempty_string(binding, "binding_id", label="binding")
    if binding.get("contract_id") != contract_id:
        raise WorkloadContractError(
            f"binding_contract_id_mismatch:{binding_id}:{binding.get('contract_id')}"
        )
    if binding.get("target") not in {"airflow", "prefect", "dagster"}:
        raise WorkloadContractError("binding_target_invalid")
    if binding.get("route") not in {"direct", "opos_mediated"}:
        raise WorkloadContractError("binding_route_invalid")

    declared_artifact_path = Path(
        _require_nonempty_string(binding, "artifact_path", label="binding")
    ).expanduser().resolve()
    if declared_artifact_path != artifact_path:
        raise WorkloadContractError("binding_artifact_path_mismatch")
    actual_artifact_hash = sha256_file(artifact_path)
    if binding.get("artifact_sha256") != actual_artifact_hash:
        raise WorkloadContractError("binding_artifact_sha256_mismatch")

    inputs = contract.get("inputs")
    if not isinstance(inputs, list):
        raise WorkloadContractError("contract_inputs_must_be_array")
    fixtures: Dict[str, Path] = {}
    for index, item in enumerate(inputs):
        if not isinstance(item, dict):
            raise WorkloadContractError(f"contract_input_must_be_object:{index}")
        integration_id = _require_nonempty_string(
            item, "integration_id", label=f"contract_input_{index}"
        )
        if integration_id in fixtures:
            raise WorkloadContractError(
                f"duplicate_contract_integration_id:{integration_id}"
            )
        fixture_ref = _require_nonempty_string(
            item, "fixture", label=f"contract_input_{index}"
        )
        fixture_path = (contract_path.parent / fixture_ref).resolve()
        if not fixture_path.is_file():
            raise WorkloadContractError(f"fixture_not_found:{integration_id}")
        if item.get("sha256") != sha256_file(fixture_path):
            raise WorkloadContractError(f"fixture_sha256_mismatch:{integration_id}")
        fixtures[integration_id] = fixture_path
    required_input_ids = contract.get("required_input_ids")
    if required_input_ids is None:
        required_input_ids = sorted(fixtures)
    if not isinstance(required_input_ids, list) or not required_input_ids:
        raise WorkloadContractError(
            "contract_required_input_ids_must_be_nonempty_array"
        )
    unknown_required_inputs = sorted(
        {
            str(value)
            for value in required_input_ids
            if str(value) not in fixtures
        }
    )
    if unknown_required_inputs:
        raise WorkloadContractError(
            "contract_required_input_ids_unknown:"
            + ",".join(unknown_required_inputs)
        )

    integration_bindings = binding.get("integration_bindings")
    if not isinstance(integration_bindings, dict):
        raise WorkloadContractError("binding_integration_bindings_must_be_object")
    unknown_ids = sorted(
        {
            str(integration_id)
            for integration_id in integration_bindings.values()
            if integration_id not in fixtures
        }
    )
    if unknown_ids:
        raise WorkloadContractError(
            f"binding_references_unknown_integrations:{','.join(unknown_ids)}"
        )
    if not isinstance(binding.get("output_bindings"), dict):
        raise WorkloadContractError("binding_output_bindings_must_be_object")
    if not isinstance(binding.get("expected_task_ids"), list):
        raise WorkloadContractError("binding_expected_task_ids_must_be_array")
    for optional_array in (
        "allowed_additional_task_ids",
        "allowed_skipped_task_ids",
    ):
        if optional_array in binding and not isinstance(
            binding.get(optional_array), list
        ):
            raise WorkloadContractError(
                f"binding_{optional_array}_must_be_array"
            )
    alternative_groups = binding.get("alternative_task_groups")
    if alternative_groups is not None and (
        not isinstance(alternative_groups, list)
        or any(not isinstance(group, list) or not group for group in alternative_groups)
    ):
        raise WorkloadContractError(
            "binding_alternative_task_groups_must_be_nonempty_arrays"
        )
    if "literal_rewrites" in binding and not isinstance(
        binding.get("literal_rewrites"), dict
    ):
        raise WorkloadContractError("binding_literal_rewrites_must_be_object")

    return LoadedWorkloadContract(
        contract_path=contract_path,
        binding_path=binding_path,
        artifact_path=artifact_path,
        contract=contract,
        binding=binding,
        fixtures=fixtures,
        contract_sha256=sha256_file(contract_path),
        binding_sha256=sha256_file(binding_path),
        artifact_sha256=actual_artifact_hash,
    )
