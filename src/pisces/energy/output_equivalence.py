"""Bounded CSV reference verification and pairwise equivalence for contract v2."""

from __future__ import annotations

import csv
from contextlib import contextmanager, ExitStack
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, localcontext
import hashlib
import io
from itertools import zip_longest
import json
from pathlib import Path
import re
from typing import Any, Iterator

from .workload_contract import LoadedWorkloadContract, WorkloadContractError, confined_path


ORACLE_VERSION = "csv-reference-v1"
MAX_BYTES = 32 * 1024 * 1024
MAX_ROWS = 100_000
MAX_SORTED_ROWS = 1_000_000
_INTEGER = re.compile(r"[+-]?[0-9]+")
_DECIMAL = re.compile(r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?")


class _CheckError(ValueError):
    def __init__(self, reason: str, *, status: str = "failed"):
        super().__init__(reason)
        self.status = status


@dataclass
class _Table:
    sha256: str
    rows: dict[tuple[Any, ...], tuple[Any, ...]]


def _cell(value: str, column: dict[str, Any]) -> Any:
    if value == "" and column["nullable"]:
        return None
    kind = column["type"]
    if kind == "string":
        return value
    if kind == "boolean" and value in {"true", "false"}:
        return value == "true"
    if kind == "integer" and len(value) <= 40 and _INTEGER.fullmatch(value):
        return int(value)
    if kind == "decimal" and len(value) <= 64 and _DECIMAL.fullmatch(value):
        try:
            number = Decimal(value)
        except InvalidOperation:
            pass
        else:
            if abs(number.as_tuple().exponent) <= 32 and len(number.as_tuple().digits) <= 40:
                return number
    raise _CheckError("output_value_type_mismatch")


def _read_table(path: Path, spec: dict[str, Any], *, expected_hash: str | None = None) -> _Table:
    if not path.is_file():
        raise _CheckError("output_file_missing")
    with path.open("rb") as handle:
        payload = handle.read(MAX_BYTES + 1)
    if len(payload) > MAX_BYTES:
        raise _CheckError("output_byte_budget_exceeded", status="not_verified")
    digest = hashlib.sha256(payload).hexdigest()
    if expected_hash is not None and digest != expected_hash:
        raise _CheckError("reference_hash_mismatch", status="not_verified")
    with io.TextIOWrapper(io.BytesIO(payload), encoding="utf-8", newline="") as stream:
        reader = csv.reader(stream, strict=True)
        columns = spec["columns"]
        header = next(reader, [])
        names = [column["name"] for column in columns]
        if len(header) != len(names) or set(header) != set(names):
            raise _CheckError("output_schema_mismatch")
        positions = [header.index(name) for name in names]
        keys = [names.index(name) for name in spec["primary_key"]]
        rows: dict[tuple[Any, ...], tuple[Any, ...]] = {}
        for fields in reader:
            if len(rows) >= MAX_ROWS:
                raise _CheckError("output_row_budget_exceeded", status="not_verified")
            if len(fields) != len(header):
                raise _CheckError("output_row_width_mismatch")
            row = tuple(_cell(fields[position], column) for position, column in zip(positions, columns))
            key = tuple(row[index] for index in keys)
            if any(value == "" for value in key):
                raise _CheckError("empty_output_key")
            if key in rows:
                raise _CheckError("duplicate_output_key")
            rows[key] = row
    return _Table(digest, rows)


def _compare_tables(left: _Table, right: _Table, spec: dict[str, Any]) -> None:
    if left.rows.keys() != right.rows.keys():
        raise _CheckError("output_key_set_mismatch")
    if spec["row_order"] == "exact" and list(left.rows) != list(right.rows):
        raise _CheckError("output_order_mismatch")
    tolerances = [Decimal(str(column.get("absolute_tolerance", 0))) for column in spec["columns"]]
    with localcontext() as context:
        context.prec = 128
        for key, row in left.rows.items():
            for actual, expected, tolerance in zip(row, right.rows[key], tolerances):
                if actual == expected:
                    continue
                if isinstance(actual, Decimal) and isinstance(expected, Decimal) and abs(actual - expected) <= tolerance:
                    continue
                raise _CheckError("output_content_mismatch")


def _semantic_line(row: tuple[Any, ...]) -> bytes:
    def canonical(value: Any) -> Any:
        if isinstance(value, Decimal):
            if not value:
                return "0"
            text = format(value, "f")
            return text.rstrip("0").rstrip(".") if "." in text else text
        return value

    return (json.dumps([canonical(value) for value in row], separators=(",", ":")) + "\n").encode("ascii")


def _semantic_digest(table: _Table, spec: dict[str, Any]) -> str:
    digest = hashlib.sha256()
    keys = table.rows if spec["row_order"] == "exact" else sorted(table.rows)
    for key in keys:
        digest.update(_semantic_line(table.rows[key]))
    return digest.hexdigest()


@contextmanager
def _sorted_rows(path: Path, spec: dict[str, Any]) -> Iterator[tuple[Any, Iterator[tuple[Any, ...]]]]:
    if not path.is_file():
        raise _CheckError("output_file_missing")
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        if stream.seek(0, io.SEEK_END) > MAX_BYTES:
            raise _CheckError("output_byte_budget_exceeded", status="not_verified")
        stream.seek(0)

        def lines() -> Iterator[str]:
            remaining = MAX_BYTES
            while raw := stream.readline(remaining + 1):
                remaining -= len(raw)
                if remaining < 0:
                    raise _CheckError("output_byte_budget_exceeded", status="not_verified")
                digest.update(raw)
                yield raw.decode("utf-8")

        def rows() -> Iterator[tuple[Any, ...]]:
            reader = csv.reader(lines(), strict=True)
            columns = spec["columns"]
            names = [column["name"] for column in columns]
            header = next(reader, [])
            if len(header) != len(names) or set(header) != set(names):
                raise _CheckError("output_schema_mismatch")
            positions = [header.index(name) for name in names]
            keys = [names.index(name) for name in spec["primary_key"]]
            previous = None
            for count, fields in enumerate(reader, 1):
                if count > MAX_SORTED_ROWS:
                    raise _CheckError("output_row_budget_exceeded", status="not_verified")
                if len(fields) != len(names):
                    raise _CheckError("output_row_width_mismatch")
                row = tuple(_cell(fields[position], column) for position, column in zip(positions, columns))
                key = tuple(row[index] for index in keys)
                if "" in key:
                    raise _CheckError("empty_output_key")
                if previous is not None and key <= previous:
                    reason = "duplicate_output_key" if key == previous else "output_order_mismatch"
                    raise _CheckError(reason)
                previous = key
                yield row

        yield digest, rows()


def _verify_sorted(path: Path, reference: Path, expected_hash: str, spec: dict[str, Any], result: dict) -> None:
    def reference_error(exc: Exception) -> _CheckError:
        return _CheckError(str(exc) if isinstance(exc, _CheckError) else "reference_unreadable", status="not_verified")

    with ExitStack() as stack:
        try:
            reference_digest, expected = stack.enter_context(_sorted_rows(reference, spec))
        except (OSError, _CheckError) as exc:
            raise reference_error(exc) from exc

        def checked_reference():
            try:
                yield from expected
            except (OSError, UnicodeError, csv.Error, _CheckError) as exc:
                raise reference_error(exc) from exc

        digest, observed = stack.enter_context(_sorted_rows(path, spec))
        semantic = hashlib.sha256()
        error = None
        count = expected_count = 0
        for actual, target in zip_longest(observed, checked_reference()):
            expected_count += target is not None
            if actual is not None:
                count += 1
                semantic.update(_semantic_line(actual))
            if actual is None or target is None:
                error = "output_key_set_mismatch"
            elif actual != target and error is None:
                error = "output_content_mismatch"
        if reference_digest.hexdigest() != expected_hash:
            raise _CheckError("reference_hash_mismatch", status="not_verified")
        if not expected_count:
            raise _CheckError("empty_reference", status="not_verified")
        result.update(sha256=digest.hexdigest(), row_count=count)
        if error:
            raise _CheckError(error)
        result.update(status="passed", passed=True, reason=None, semantic_sha256=semantic.hexdigest())


def _service_verdict(workload: LoadedWorkloadContract, log: list[dict[str, Any]] | None) -> dict[str, Any]:
    expected = {item["integration_id"]: item["served_count"] for item in workload.contract.get("service_effects", [])}
    if log is None:
        return {"status": "not_verified" if expected else "passed", "reason": "service_log_missing" if expected else None}
    if not isinstance(log, list) or not all(isinstance(entry, dict) for entry in log):
        return {"status": "not_verified", "reason": "service_log_invalid"}
    counts: dict[str, int] = {}
    blocked = False
    for entry in log:
        if entry.get("status") == "served":
            key = str(entry.get("integration_id"))
            counts[key] = counts.get(key, 0) + 1
        else:
            blocked = True
    passed = counts == expected and not blocked
    return {"status": "passed" if passed else "failed", "reason": None if passed else "service_effect_mismatch", "served_counts": counts}


def _verify(
    workload: LoadedWorkloadContract, workspace: Path, service_log: list[dict[str, Any]] | None,
) -> tuple[dict[str, Any], dict[str, _Table]]:
    verdict: dict[str, Any] = {
        "oracle_version": ORACLE_VERSION,
        "contract_sha256": workload.contract_sha256,
        "artifact_sha256": workload.artifact_sha256,
        "binding_sha256": workload.binding_sha256,
        "status": "not_verified",
        "passed": None,
        "results": [],
    }
    tables: dict[str, _Table] = {}
    if workload.contract["schema_version"] != "2.0":
        verdict["reason"] = "v2_contract_required"
        return verdict, tables
    for spec in workload.contract["outputs"]:
        name = spec["logical_name"]
        reference = spec["reference"]
        result: dict[str, Any] = {"logical_name": name, "reference_sha256": reference["sha256"]}
        try:
            path = confined_path(workspace, workload.binding["output_bindings"][name])
            if spec["row_order"] == "key_sorted":
                _verify_sorted(path, confined_path(workload.contract_path.parent, reference["path"]),
                               reference["sha256"], spec, result)
                verdict["results"].append(result)
                continue
            try:
                expected = _read_table(
                    confined_path(workload.contract_path.parent, reference["path"]), spec,
                    expected_hash=reference["sha256"],
                )
                if not expected.rows:
                    raise _CheckError("empty_reference")
            except (OSError, UnicodeError, csv.Error, WorkloadContractError, _CheckError) as exc:
                reason = str(exc) if isinstance(exc, _CheckError) else "reference_unreadable"
                raise _CheckError(reason, status="not_verified") from exc
            observed = _read_table(path, spec)
            result.update(sha256=observed.sha256, row_count=len(observed.rows))
            _compare_tables(observed, expected, spec)
            result.update(status="passed", passed=True, reason=None)
            if not any(column.get("absolute_tolerance", 0) for column in spec["columns"]):
                result["semantic_sha256"] = _semantic_digest(observed, spec)
            tables[name] = observed
        except _CheckError as exc:
            result.update(status=exc.status, passed=False if exc.status == "failed" else None, reason=str(exc))
        except (OSError, UnicodeError, csv.Error, WorkloadContractError):
            result.update(status="failed", passed=False, reason="output_unreadable_or_missing")
        verdict["results"].append(result)
    service = _service_verdict(workload, service_log)
    verdict["service_effects"] = service
    statuses = {item["status"] for item in verdict["results"]} | {service["status"]}
    status = "failed" if "failed" in statuses else "not_verified" if "not_verified" in statuses else "passed"
    verdict.update(
        status=status,
        passed=None if status == "not_verified" else status == "passed",
        assertion_count=len(verdict["results"]),
    )
    return verdict, tables


def verify_output(
    workload: LoadedWorkloadContract, *, workspace_root: Path,
    service_log: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Check an actual output workspace against independent contract references."""
    return _verify(workload, workspace_root, service_log)[0]


def compare_outputs(
    left: LoadedWorkloadContract, right: LoadedWorkloadContract, *,
    left_workspace: Path, right_workspace: Path,
    left_service_log: list[dict[str, Any]] | None = None,
    right_service_log: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Verify both candidates, then compare their observed typed values directly."""
    result: dict[str, Any] = {
        "oracle_version": ORACLE_VERSION, "status": "not_verified",
        "left": left.provenance(), "right": right.provenance(),
    }
    if left.contract_sha256 != right.contract_sha256:
        return {**result, "reason": "workload_contract_mismatch"}
    left_verdict, left_tables = _verify(left, left_workspace, left_service_log)
    right_verdict, right_tables = _verify(right, right_workspace, right_service_log)
    result.update(left_verification=left_verdict, right_verification=right_verdict)
    if not (left_verdict["passed"] and right_verdict["passed"]):
        status = "failed" if False in (left_verdict["passed"], right_verdict["passed"]) else "not_verified"
        return {**result, "status": status, "reason": "reference_verification_required"}
    for spec in left.contract["outputs"]:
        name = spec["logical_name"]
        if spec["row_order"] == "key_sorted":
            # Both streams matched the same authenticated reference exactly.
            continue
        try:
            _compare_tables(left_tables[name], right_tables[name], spec)
        except _CheckError as exc:
            return {**result, "status": exc.status, "reason": str(exc), "logical_name": name}
    return {**result, "status": "passed", "reason": None}
