"""Format-aware output assertions for controlled workload contracts."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, List

from .workload_contract import LoadedWorkloadContract, sha256_file


def _resolve_output_path(
    raw_path: str,
    *,
    workspace_root: Path,
) -> Path:
    candidate = Path(raw_path)
    if not candidate.is_absolute():
        candidate = workspace_root / candidate
    candidate = candidate.resolve()
    root = workspace_root.resolve()
    if candidate != root and root not in candidate.parents:
        raise ValueError(f"output_outside_workspace:{candidate}")
    return candidate


def _assert_csv(path: Path, specification: Dict[str, Any]) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        columns = list(reader.fieldnames or [])
    required = [str(value) for value in specification.get("required_columns") or []]
    missing = sorted(set(required) - set(columns))
    exact_rows = specification.get("exact_rows")
    minimum_rows = specification.get("minimum_rows")
    passed = not missing
    if isinstance(exact_rows, int):
        passed = passed and len(rows) == exact_rows
    if isinstance(minimum_rows, int):
        passed = passed and len(rows) >= minimum_rows
    return {
        "passed": passed,
        "row_count": len(rows),
        "columns": columns,
        "missing_required_columns": missing,
    }


def _assert_json(path: Path, specification: Dict[str, Any]) -> Dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    expected_type = specification.get("expected_type")
    type_ok = (
        expected_type is None
        or (expected_type == "object" and isinstance(value, dict))
        or (expected_type == "array" and isinstance(value, list))
    )
    return {"passed": bool(type_ok), "json_type": type(value).__name__}


def _assert_parquet(path: Path, specification: Dict[str, Any]) -> Dict[str, Any]:
    try:
        import pandas as pd
    except ImportError:
        return {"passed": False, "reason": "pandas_not_available"}
    try:
        frame = pd.read_parquet(path)
    except Exception as exc:
        return {
            "passed": False,
            "reason": "parquet_read_failed",
            "message": str(exc),
        }
    required = [str(value) for value in specification.get("required_columns") or []]
    columns = [str(value) for value in frame.columns]
    missing = sorted(set(required) - set(columns))
    exact_rows = specification.get("exact_rows")
    minimum_rows = specification.get("minimum_rows")
    passed = not missing
    if isinstance(exact_rows, int):
        passed = passed and len(frame) == exact_rows
    if isinstance(minimum_rows, int):
        passed = passed and len(frame) >= minimum_rows
    return {
        "passed": passed,
        "row_count": int(len(frame)),
        "columns": columns,
        "missing_required_columns": missing,
    }


def evaluate_output_assertions(
    workload: LoadedWorkloadContract,
    *,
    workspace_root: Path,
    native_metadata: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    specifications = workload.contract.get("outputs") or []
    bindings = workload.binding.get("output_bindings") or {}
    results: List[Dict[str, Any]] = []
    for specification in specifications:
        logical_name = str(specification.get("logical_name") or "")
        raw_binding = bindings.get(logical_name)
        output_format = str(specification.get("format") or "file").lower()
        if output_format == "task_return":
            if not isinstance(raw_binding, dict):
                results.append(
                    {
                        "logical_name": logical_name,
                        "passed": False,
                        "reason": "task_output_binding_missing",
                    }
                )
                continue
            task_id = str(raw_binding.get("task_id") or "")
            task_outputs = (native_metadata or {}).get("task_outputs") or {}
            summary = task_outputs.get(task_id)
            if not isinstance(summary, dict):
                results.append(
                    {
                        "logical_name": logical_name,
                        "task_id": task_id,
                        "format": output_format,
                        "passed": False,
                        "reason": "task_output_not_observed",
                    }
                )
                continue
            expected_type = specification.get("expected_type")
            required_keys = {
                str(value)
                for value in specification.get("required_keys") or []
            }
            observed_keys = {
                str(value) for value in summary.get("keys") or []
            }
            missing_keys = sorted(required_keys - observed_keys)
            passed = bool(summary.get("present"))
            if expected_type is not None:
                passed = passed and summary.get("json_type") == expected_type
            passed = passed and not missing_keys
            results.append(
                {
                    "logical_name": logical_name,
                    "task_id": task_id,
                    "format": output_format,
                    "passed": passed,
                    "expected_type": expected_type,
                    "missing_required_keys": missing_keys,
                    "observed": summary,
                }
            )
            continue
        raw_path = raw_binding
        if not isinstance(raw_path, str) or not raw_path:
            results.append(
                {
                    "logical_name": logical_name,
                    "passed": False,
                    "reason": "output_binding_missing",
                }
            )
            continue
        try:
            path = _resolve_output_path(raw_path, workspace_root=workspace_root)
        except ValueError as exc:
            results.append(
                {
                    "logical_name": logical_name,
                    "passed": False,
                    "reason": str(exc),
                }
            )
            continue
        if not path.is_file():
            results.append(
                {
                    "logical_name": logical_name,
                    "path": str(path),
                    "passed": False,
                    "reason": "output_not_found",
                }
            )
            continue
        try:
            if output_format == "csv":
                details = _assert_csv(path, specification)
            elif output_format == "json":
                details = _assert_json(path, specification)
            elif output_format == "parquet":
                details = _assert_parquet(path, specification)
            else:
                details = {"passed": path.stat().st_size > 0}
        except Exception as exc:
            details = {
                "passed": False,
                "reason": "output_assertion_error",
                "message": str(exc),
            }
        results.append(
            {
                "logical_name": logical_name,
                "path": str(path),
                "format": output_format,
                "sha256": sha256_file(path),
                **details,
            }
        )
    return {
        "status": "passed" if results and all(item["passed"] for item in results) else "failed",
        "passed": bool(results) and all(item["passed"] for item in results),
        "assertion_count": len(results),
        "results": results,
    }
