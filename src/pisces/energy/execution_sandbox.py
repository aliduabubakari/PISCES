"""Temporary workspace preparation for a validated workload contract."""

from __future__ import annotations

import contextlib
import ast
from dataclasses import dataclass
import hashlib
import io
import os
from pathlib import Path
import shutil
import tempfile
import tokenize
from typing import Any, Dict, Iterator, List

from .workload_contract import LoadedWorkloadContract, WorkloadContractError, sha256_file


@dataclass(frozen=True)
class ExecutionSandbox:
    root: Path
    inputs: Path
    outputs: Path
    working: Path
    fixtures: Dict[str, Path]
    workflow_file: Path
    workflow_sha256: str
    literal_rewrites: List[Dict[str, str]]


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _expand_scoped_value(raw_value: Any, *, root: Path, inputs: Path, outputs: Path, working: Path) -> str:
    value = str(raw_value)
    value = value.replace("{sandbox}", str(root))
    value = value.replace("{inputs}", str(inputs))
    value = value.replace("{outputs}", str(outputs))
    value = value.replace("{working}", str(working))
    candidate = Path(value)
    if candidate.is_absolute():
        resolved = candidate.resolve()
        if resolved != root and root not in resolved.parents:
            raise ValueError(f"literal_rewrite_outside_sandbox:{resolved}")
    return value


def _rewrite_string_literals(
    source: str,
    *,
    rewrites: Dict[str, str],
) -> tuple[str, List[Dict[str, str]]]:
    """Replace exact Python string literals without modifying the archived file."""
    if not rewrites:
        return source, []
    applied: List[Dict[str, str]] = []
    tokens = []
    for token in tokenize.generate_tokens(io.StringIO(source).readline):
        replacement = token
        if token.type == tokenize.STRING:
            try:
                value = ast.literal_eval(token.string)
            except (SyntaxError, ValueError):
                value = None
            if isinstance(value, str) and value in rewrites:
                new_value = rewrites[value]
                replacement = tokenize.TokenInfo(
                    type=token.type,
                    string=repr(new_value),
                    start=token.start,
                    end=token.end,
                    line=token.line,
                )
                applied.append({"from": value, "to": new_value})
        tokens.append(replacement)
    return tokenize.untokenize(tokens), applied


@contextlib.contextmanager
def prepared_execution_sandbox(
    workload: LoadedWorkloadContract,
    *,
    read_only_fixture_mount: bool = False,
) -> Iterator[ExecutionSandbox]:
    original_cwd = Path.cwd()
    with tempfile.TemporaryDirectory(prefix="pisces_contract_") as temp:
        root = Path(temp).resolve()
        inputs = root / "inputs"
        outputs = root / "outputs"
        working = root / "working"
        inputs.mkdir()
        outputs.mkdir()
        working.mkdir()
        copied: Dict[str, Path] = {}
        v2 = workload.contract["schema_version"] == "2.0"
        expected_fixtures = {item["integration_id"]: item["sha256"] for item in workload.contract["inputs"]}
        for integration_id, source in workload.fixtures.items():
            destination = inputs / f"{integration_id}{source.suffix}"
            if read_only_fixture_mount:
                destination.symlink_to(source)
            else:
                shutil.copy2(source, destination)
            if v2 and sha256_file(destination) != expected_fixtures[integration_id]:
                raise WorkloadContractError("staged_fixture_sha256_mismatch")
            copied[integration_id] = destination

        file_bindings = workload.binding.get("file_bindings")
        if isinstance(file_bindings, dict):
            for integration_id, raw_destination in file_bindings.items():
                if integration_id not in copied:
                    raise ValueError(
                        f"file_binding_unknown_integration:{integration_id}"
                    )
                relative_destination = Path(str(raw_destination))
                if relative_destination.is_absolute():
                    raise ValueError(
                        f"file_binding_must_be_relative:{integration_id}"
                    )
                destination = (working / relative_destination).resolve()
                if destination != working and working not in destination.parents:
                    raise ValueError(
                        f"file_binding_outside_workspace:{integration_id}"
                    )
                destination.parent.mkdir(parents=True, exist_ok=True)
                if read_only_fixture_mount:
                    destination.symlink_to(copied[integration_id])
                else:
                    shutil.copy2(copied[integration_id], destination)
                if v2 and sha256_file(destination) != expected_fixtures[integration_id]:
                    raise WorkloadContractError("bound_fixture_sha256_mismatch")

        expanded_rewrites: Dict[str, str] = {}
        literal_rewrites = workload.binding.get("literal_rewrites")
        if literal_rewrites is not None and not isinstance(literal_rewrites, dict):
            raise ValueError("literal_rewrites_must_be_object")
        for original, replacement in (literal_rewrites or {}).items():
            expanded_rewrites[str(original)] = _expand_scoped_value(
                replacement,
                root=root,
                inputs=inputs,
                outputs=outputs,
                working=working,
            )
        if v2:
            source_bytes = workload.artifact_path.read_bytes()
            if _sha256_bytes(source_bytes) != workload.artifact_sha256:
                raise WorkloadContractError("staged_artifact_sha256_mismatch")
            source = source_bytes.decode("utf-8")
        else:
            source = workload.artifact_path.read_text(encoding="utf-8")
        bound_source, applied_rewrites = _rewrite_string_literals(
            source,
            rewrites=expanded_rewrites,
        )
        workflow_file = working / workload.artifact_path.name
        workflow_file.write_bytes(bound_source.encode("utf-8"))

        previous_environment: Dict[str, str | None] = {}
        environment = workload.binding.get("environment")
        if isinstance(environment, dict):
            for key, raw_value in environment.items():
                name = str(key)
                previous_environment[name] = os.environ.get(name)
                value = str(raw_value)
                value = value.replace("{sandbox}", str(root))
                value = value.replace("{inputs}", str(inputs))
                value = value.replace("{outputs}", str(outputs))
                value = value.replace("{working}", str(working))
                os.environ[name] = value

        os.chdir(working)
        try:
            yield ExecutionSandbox(
                root=root,
                inputs=inputs,
                outputs=outputs,
                working=working,
                fixtures=copied,
                workflow_file=workflow_file,
                workflow_sha256=_sha256_bytes(bound_source.encode("utf-8")),
                literal_rewrites=applied_rewrites,
            )
        finally:
            os.chdir(original_cwd)
            for name, previous in previous_environment.items():
                if previous is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = previous
