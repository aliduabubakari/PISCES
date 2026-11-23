"""Validation and provenance for experiment-owned workload contracts."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from functools import lru_cache
from importlib.resources import files
from pathlib import Path
from typing import Any, Dict

from jsonschema import Draft202012Validator

class WorkloadContractError(ValueError):
    pass


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_object(path: Path, *, label: str) -> tuple[Dict[str, Any], str]:
    try:
        payload = path.read_bytes()
        value = json.loads(payload.decode("utf-8"))
        json.dumps(value, allow_nan=False)
    except Exception as exc:
        raise WorkloadContractError(f"{label}_invalid_json:{exc}") from exc
    if not isinstance(value, dict):
        raise WorkloadContractError(f"{label}_must_be_object")
    return value, hashlib.sha256(payload).hexdigest()


def _require_nonempty_string(value: Dict[str, Any], key: str, *, label: str) -> str:
    found = value.get(key)
    if not isinstance(found, str) or not found.strip():
        raise WorkloadContractError(f"{label}_missing_or_invalid:{key}")
    return found.strip()


@lru_cache(maxsize=2)
def _v2_validator(name: str) -> Draft202012Validator:
    schema = files("pisces").joinpath("schemas", f"{name}-2.0.schema.json")
    return Draft202012Validator(json.loads(schema.read_text(encoding="utf-8")))


def _validate_v2(value: Dict[str, Any], name: str) -> None:
    error = next(_v2_validator(name).iter_errors(value), None)
    if error:
        field = "/".join(str(part) for part in error.absolute_path)
        raise WorkloadContractError(f"{name}_schema_invalid:{field}:{error.validator}")


def confined_path(root: Path, reference: str) -> Path:
    relative = Path(reference)
    if relative.is_absolute() or ".." in relative.parts:
        raise WorkloadContractError("path_must_be_confined_relative")
    root = root.resolve()
    target = (root / relative).resolve()
    if root not in target.parents:
        raise WorkloadContractError("path_outside_declared_root")
    return target


def _validate_v2_bindings(contract: Dict[str, Any], binding: Dict[str, Any], root: Path) -> None:
    inputs = {item["integration_id"]: item["kind"] for item in contract["inputs"]}
    file_ids = {key for key, kind in inputs.items() if kind == "file"}
    if set(binding["file_bindings"]) != file_ids:
        raise WorkloadContractError("file_bindings_do_not_match_contract")
    if set(binding["integration_bindings"].values()) != set(inputs) - file_ids:
        raise WorkloadContractError("service_bindings_do_not_match_contract")
    effects = [item["integration_id"] for item in contract.get("service_effects", [])]
    if len(set(effects)) != len(effects) or set(effects) != set(inputs) - file_ids:
        raise WorkloadContractError("service_effects_do_not_match_contract")
    outputs = contract["outputs"]
    names = {spec["logical_name"] for spec in outputs}
    if len(names) != len(outputs) or set(binding["output_bindings"]) != names:
        raise WorkloadContractError("output_bindings_do_not_match_contract")
    destinations = [
        confined_path(root, value)
        for field in ("file_bindings", "output_bindings")
        for value in binding[field].values()
    ]
    artifact_name = Path(binding["artifact_path"]).name
    if any(path.relative_to(root).parts[0] == artifact_name for path in destinations):
        raise WorkloadContractError("binding_destination_overlaps_workflow")
    for index, path in enumerate(destinations):
        if any(path == other or path in other.parents or other in path.parents for other in destinations[:index]):
            raise WorkloadContractError("binding_destination_collision")
    for spec in outputs:
        columns = {column["name"]: column for column in spec["columns"]}
        if len(columns) != len(spec["columns"]):
            raise WorkloadContractError("duplicate_output_column")
        for key in spec["primary_key"]:
            if key not in columns or columns[key]["nullable"] or "absolute_tolerance" in columns[key]:
                raise WorkloadContractError("invalid_output_primary_key")
        for column in columns.values():
            if "absolute_tolerance" in column and column["type"] != "decimal":
                raise WorkloadContractError("tolerance_requires_decimal_column")
            if spec["row_order"] == "key_sorted" and column.get("absolute_tolerance", 0):
                raise WorkloadContractError("key_sorted_requires_exact_values")


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
        version = self.contract["schema_version"]
        if version == "2.0":
            return {
                "status": "validated",
                "schema_version": version,
                "contract_id": self.contract["contract_id"],
                "binding_id": self.binding["binding_id"],
                "contract_sha256": self.contract_sha256,
                "binding_sha256": self.binding_sha256,
                "artifact_sha256": self.artifact_sha256,
                "fixture_sha256": {item["integration_id"]: item["sha256"] for item in self.contract["inputs"]},
                "reference_sha256": {item["logical_name"]: item["reference"]["sha256"] for item in self.contract["outputs"]},
                "functional_unit": self.contract["functional_unit"],
                "target": self.binding["target"],
                "route": self.binding.get("route"),
            }
        return {
            "status": "validated",
            "schema_version": "1.0",
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

    contract, contract_hash = _load_object(contract_path, label="contract")
    binding, binding_hash = _load_object(binding_path, label="binding")
    version = contract.get("schema_version")
    if version not in ("1.0", "2.0"):
        raise WorkloadContractError("unsupported_contract_schema_version")
    if binding.get("schema_version") != version:
        raise WorkloadContractError("unsupported_binding_schema_version")
    v2 = version == "2.0"
    if v2:
        _validate_v2(contract, "workload-contract")
        _validate_v2(binding, "artifact-binding")

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
    if not v2 and binding.get("route") not in {"direct", "opos_mediated"}:
        raise WorkloadContractError("binding_route_invalid")

    artifact_ref = _require_nonempty_string(binding, "artifact_path", label="binding")
    declared_artifact_path = (
        confined_path(binding_path.parent, artifact_ref) if v2 else Path(artifact_ref).expanduser().resolve()
    )
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
        fixture_path = confined_path(contract_path.parent, fixture_ref) if v2 else (contract_path.parent / fixture_ref).resolve()
        if not fixture_path.is_file():
            raise WorkloadContractError(f"fixture_not_found:{integration_id}")
        if item.get("sha256") != sha256_file(fixture_path):
            raise WorkloadContractError(f"fixture_sha256_mismatch:{integration_id}")
        fixtures[integration_id] = fixture_path
    if v2:
        _validate_v2_bindings(contract, binding, binding_path.parent)
        for specification in contract["outputs"]:
            reference = specification["reference"]
            path = confined_path(contract_path.parent, reference["path"])
            if not path.is_file() or sha256_file(path) != reference["sha256"]:
                raise WorkloadContractError("output_reference_sha256_mismatch")
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
        contract_sha256=contract_hash,
        binding_sha256=binding_hash,
        artifact_sha256=actual_artifact_hash,
    )
