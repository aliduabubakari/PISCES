#!/usr/bin/env python3
"""
Zero-Fill Data Generator (v1.0)
===============================

Generates trivial (all-zero / empty-string) synthetic data by analysing the
pipeline code's AST to infer expected input schemas — no LLM required.

This allows CodeCarbon to measure actual energy usage during workflow
execution even when no real sample data is available. The zero-fill
approach exercises all code paths (imports, DAG construction, operator
execution) while keeping the data payload trivially cheap.

Design goals:
- No external dependencies (stdlib only)
- Deterministic output (seed-based)
- Extracts schemas from:
    - PythonOperator callable signatures
    - Decorated task kwargs (table names, column refs)
    - Operator type hints and parameters
    - XCom push/pull key patterns
"""

from __future__ import annotations

import ast
import hashlib
import random
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple


SCHEMA_VERSION = "1.0.0"


# ---------------------------------------------------------------------------
# AST extraction
# ---------------------------------------------------------------------------

class _SchemaVisitor(ast.NodeVisitor):
    """Walk the workflow AST and collect schema hints."""

    # Recognised decorator names for task/flow detection
    _TASK_DECORATORS = {"asset", "task", "op", "job", "flow"}
    _ORCHESTRATOR_FUNCTIONS = {
        "define_asset_job", "Definitions",
        "PythonOperator", "BranchPythonOperator", "ShortCircuitOperator",
        "BashOperator", "EmptyOperator", "DummyOperator",
        "PostgresOperator", "SnowflakeOperator", "BigQueryOperator",
        "S3ToGCSOperator", "GCSToS3Operator", "GCSToBigQueryOperator",
        "S3ToRedshiftOperator", "S3CreateBucketOperator",
        "EmailOperator", "SlackAPIOperator",
        "KubernetesPodOperator", "DockerOperator",
    }

    def __init__(self) -> None:
        self.columns: Dict[str, str] = {}       # column_name → inferred_type
        self.task_ids: List[str] = []
        self.operator_types: Set[str] = set()
        self.xcom_keys: Set[str] = set()
        self.table_refs: Set[str] = set()
        self.callable_params: Dict[str, List[str]] = {}  # task_id → param_names
        self.kwargs_keys: Set[str] = set()
        self._function_defs: Dict[str, ast.FunctionDef] = {}  # name → node

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        """Detect @asset (Dagster), @task/@flow (Prefect) decorated functions."""
        self._function_defs[node.name] = node

        for decorator in node.decorator_list:
            dec_name = None
            if isinstance(decorator, ast.Name):
                dec_name = decorator.id
            elif isinstance(decorator, ast.Attribute):
                dec_name = decorator.attr
            elif isinstance(decorator, ast.Call):
                if isinstance(decorator.func, ast.Name):
                    dec_name = decorator.func.id
                elif isinstance(decorator.func, ast.Attribute):
                    dec_name = decorator.func.attr

            if dec_name and dec_name in self._TASK_DECORATORS:
                # This decorated function is a task/asset/flow
                self.task_ids.append(node.name)
                self.operator_types.add(dec_name)

                # Extract parameter names as column hints
                params = []
                for arg in node.args.args:
                    params.append(arg.arg)
                if params:
                    self.callable_params[node.name] = params

                # Also check return type annotation for hints
                if node.returns:
                    pass  # Could extract from type hints in future

        # Visit function body for nested calls (Tasks inside flows)
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        # --- Detect operator/task constructors ---
        op_name = None
        if isinstance(node.func, ast.Attribute):
            op_name = node.func.attr
        elif isinstance(node.func, ast.Name):
            op_name = node.func.id

        if op_name and op_name in self._ORCHESTRATOR_FUNCTIONS:
            self.operator_types.add(op_name)

            for kw in node.keywords:
                if kw.arg == "task_id":
                    tid = self._extract_str(kw.value)
                    if tid:
                        self.task_ids.append(tid)
                elif kw.arg == "python_callable":
                    params = self._extract_callable_params(kw.value)
                    if params:
                        tid = self._last_task_id()
                        if tid:
                            self.callable_params[tid] = params
                elif kw.arg in {"op_kwargs", "op_args", "provide_context"}:
                    pass
                elif kw.arg is None:
                    pass
                else:
                    self.kwargs_keys.add(kw.arg)

        # Track XCom references
        if isinstance(node.func, ast.Attribute):
            if node.func.attr in {"xcom_push", "xcom_pull", "get", "set"}:
                for kw in node.keywords:
                    if kw.arg == "key":
                        key = self._extract_str(kw.value)
                        if key:
                            self.xcom_keys.add(key)

        # Track common data-source keywords
        if isinstance(node.func, ast.Name):
            for kw in node.keywords:
                if kw.arg in {"table", "table_name", "dataset", "columns", "schema"}:
                    val = self._extract_str(kw.value)
                    if val:
                        self.table_refs.add(val)

        self.generic_visit(node)

    def _extract_str(self, node: ast.expr) -> Optional[str]:
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        if isinstance(node, ast.Name):
            return node.id
        return None

    def _extract_callable_params(self, node: ast.expr) -> List[str]:
        """Extract parameter names from a callable reference.

        Handles:
        - lambda x, y: ...     → ['x', 'y']
        - function_name        → looks up function def if available
        - partial(func, ...)   → falls back to empty
        """
        params: List[str] = []
        if isinstance(node, ast.Lambda):
            # lambda args: body
            if isinstance(node.args, ast.arguments):
                for arg in node.args.args:
                    params.append(arg.arg)
        elif isinstance(node, ast.Name):
            # Named function reference — look up the def if we've seen it
            func_def = self._function_defs.get(node.id)
            if func_def:
                for arg in func_def.args.args:
                    params.append(arg.arg)
        return params

    def _last_task_id(self) -> Optional[str]:
        if self.task_ids:
            return self.task_ids[-1]
        return None


def _extract_schema_hints(code_text: str) -> Dict[str, Any]:
    """Parse workflow code AST and return collected schema hints."""
    try:
        tree = ast.parse(code_text)
    except SyntaxError:
        return {"columns": {}, "task_ids": [], "operator_types": [], "xcom_keys": []}

    visitor = _SchemaVisitor()
    visitor.visit(tree)

    # Infer column types from parameter names using heuristics
    for task_id, params in visitor.callable_params.items():
        for p in params:
            if p not in visitor.columns:
                visitor.columns[p] = _infer_type_from_name(p)

    # If no columns found at all, create generic ones based on operator count
    if not visitor.columns:
        num_tasks = len(visitor.task_ids)
        if num_tasks > 0:
            visitor.columns["id"] = "int"
            visitor.columns["value"] = "float"
            visitor.columns["name"] = "str"
            if num_tasks >= 3:
                visitor.columns["category"] = "str"
            if num_tasks >= 5:
                visitor.columns["timestamp"] = "str"
                visitor.columns["flag"] = "bool"

    return {
        "columns": dict(sorted(visitor.columns.items())),
        "task_ids": sorted(visitor.task_ids),
        "operator_types": sorted(visitor.operator_types),
        "xcom_keys": sorted(visitor.xcom_keys),
        "table_refs": sorted(visitor.table_refs),
    }


def _infer_type_from_name(name: str) -> str:
    """Heuristic type inference from parameter/column name."""
    n = name.lower()
    # Integer patterns
    if any(w in n for w in ("id", "idx", "count", "num", "age", "year", "month", "day", "size")):
        return "int"
    # Boolean patterns
    if any(w in n for w in ("is_", "has_", "flag", "bool", "enabled", "active", "valid")):
        return "bool"
    # Float patterns
    if any(w in n for w in ("amount", "price", "rate", "score", "value", "weight", "ratio", "avg", "mean", "std")):
        return "float"
    # Default to string
    return "str"


# ---------------------------------------------------------------------------
# Zero-fill row generation
# ---------------------------------------------------------------------------

def _generate_zero_rows(
    columns: Dict[str, str],
    *,
    row_count: int = 10,
    seed: int = 0,
) -> List[Dict[str, Any]]:
    """Generate rows of trivial data (zeros for numerics, empty for strings).

    Uses a deterministic random seed so that every call with the same
    parameters produces identical output — important for reproducible
    energy benchmarks.

    The "zero" values are deliberately chosen to be the cheapest possible
    data payload while still exercising all code paths:
    - int    → 0
    - float  → 0.0
    - str    → "" (empty string)
    - bool   → False
    """
    rng = random.Random(seed)
    rows: List[Dict[str, Any]] = []
    for _ in range(max(1, row_count)):
        row: Dict[str, Any] = {}
        for col, typ in columns.items():
            if typ == "int":
                row[col] = 0
            elif typ == "float":
                row[col] = 0.0
            elif typ == "bool":
                row[col] = False
            elif typ == "date":
                row[col] = "1970-01-01"
            else:
                row[col] = ""
        rows.append(row)
    return rows


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def _is_yaml_content(text: str) -> bool:
    """Quick heuristic to detect if text is YAML (not Python)."""
    stripped = text.strip()
    # Python files usually start with import/from/def/class/#
    # YAML files have key: value or --- patterns
    if stripped.startswith("apiVersion:") or stripped.startswith("kind:"):
        return True
    if stripped.startswith("id:") and "namespace:" in stripped[:200]:
        return True
    # Heuristic: no Python keywords at start
    python_starts = ("import ", "from ", "def ", "class ", "@", "#!", "#!/")
    for ps in python_starts:
        if stripped.startswith(ps):
            return False
    # If it has YAML-like colon-separated top-level keys
    first_lines = stripped.split("\n")[:5]
    yaml_like = sum(1 for l in first_lines if re.match(r"^\w[\w.]*:\s", l))
    python_like = sum(1 for l in first_lines if l.strip().startswith(("import ", "from ", "def ", "class ")))
    return yaml_like > python_like and yaml_like >= 1


def generate_zero_fill_dataset(
    *,
    code_text: str,
    max_rows: int = 10,
    seed: int = 0,
    file_path: Optional[Path] = None,
    orchestrator: Optional[str] = None,
) -> Dict[str, Any]:
    """Main entry point: analyse pipeline code and generate zero-fill data.

    Auto-detects Python vs YAML pipelines. For YAML (Argo/Kestra), uses
    the structural YAML analyzer to extract task metadata and generate
    appropriate zero-fill rows.

    Args:
        code_text: The workflow source code (Python or YAML).
        max_rows: Maximum number of rows to generate.
        seed: Deterministic seed for reproducibility.
        file_path: Optional file path for format detection.
        orchestrator: Optional orchestrator hint (argo, kestra, etc.).

    Returns:
        A dataset dict with rows, schema, and provenance metadata.
    """
    # --- YAML pipeline path ---
    if _is_yaml_content(code_text):
        return _generate_from_yaml(
            code_text=code_text,
            max_rows=max_rows,
            seed=seed,
            file_path=file_path,
            orchestrator=orchestrator,
        )

    # --- Python pipeline path ---
    hints = _extract_schema_hints(code_text)
    columns = hints["columns"]

    col_schema = [
        {"name": name, "type": typ, "nullable": True}
        for name, typ in columns.items()
    ]

    rows = _generate_zero_rows(columns, row_count=max_rows, seed=seed)

    return {
        "rows": rows,
        "row_count": len(rows),
        "schema": col_schema,
        "provenance": [
            {
                "source_name": "zero_fill_generator",
                "format": "synthetic",
                "generator_version": SCHEMA_VERSION,
                "task_ids_detected": hints["task_ids"],
                "operator_types_detected": hints["operator_types"],
                "xcom_keys_detected": hints["xcom_keys"],
                "table_refs_detected": hints["table_refs"],
                "inference_method": "ast_heuristic",
            }
        ],
        "errors": [],
        "meta": {
            "mode": "zero_fill",
            "seed": seed,
            "max_rows": max_rows,
            "columns_detected": len(columns),
            "tasks_detected": len(hints["task_ids"]),
        },
    }


def _generate_from_yaml(
    *,
    code_text: str,
    max_rows: int = 10,
    seed: int = 0,
    file_path: Optional[Path] = None,
    orchestrator: Optional[str] = None,
) -> Dict[str, Any]:
    """Generate zero-fill data from a YAML pipeline (Argo/Kestra)."""
    from .structural_yaml_analyzer import (
        parse_yaml_manifest,
        detect_yaml_orchestrator,
        HAS_YAML,
    )

    if not HAS_YAML:
        # Fallback: generic columns
        columns = {"id": "int", "value": "float", "name": "str"}
        rows = _generate_zero_rows(columns, row_count=max_rows, seed=seed)
        return {
            "rows": rows,
            "row_count": len(rows),
            "schema": [{"name": n, "type": t, "nullable": True} for n, t in columns.items()],
            "provenance": [{
                "source_name": "zero_fill_generator",
                "format": "synthetic",
                "generator_version": SCHEMA_VERSION,
                "inference_method": "yaml_fallback_no_pyyaml",
            }],
            "errors": [{"message": "PyYAML not installed, using generic columns"}],
            "meta": {
                "mode": "zero_fill",
                "seed": seed,
                "max_rows": max_rows,
                "columns_detected": len(columns),
                "tasks_detected": 0,
            },
        }

    # Parse YAML to get topology
    try:
        if file_path:
            topology = parse_yaml_manifest(file_path, orchestrator=orchestrator)
        else:
            # Parse from string
            import yaml
            manifest = yaml.safe_load(code_text)
            if not isinstance(manifest, dict):
                raise ValueError("YAML is not a mapping")

            detected = orchestrator or detect_yaml_orchestrator(manifest) or "unknown"
            from .structural_yaml_analyzer import ArgoParser, KestraParser
            if detected == "argo":
                topology = ArgoParser.parse(manifest)
            elif detected == "kestra":
                topology = KestraParser.parse(manifest)
            else:
                raise ValueError(f"Unknown YAML orchestrator: {detected}")
    except Exception as e:
        # Fallback on parse error
        columns = {"id": "int", "value": "float", "name": "str"}
        rows = _generate_zero_rows(columns, row_count=max_rows, seed=seed)
        return {
            "rows": rows,
            "row_count": len(rows),
            "schema": [{"name": n, "type": t, "nullable": True} for n, t in columns.items()],
            "provenance": [{
                "source_name": "zero_fill_generator",
                "format": "synthetic",
                "generator_version": SCHEMA_VERSION,
                "inference_method": "yaml_parse_error_fallback",
            }],
            "errors": [{"message": str(e)}],
            "meta": {
                "mode": "zero_fill",
                "seed": seed,
                "max_rows": max_rows,
                "columns_detected": 3,
                "tasks_detected": 0,
            },
        }

    # Extract columns from task metadata
    columns: Dict[str, str] = {}
    if topology.total_tasks > 0:
        columns["id"] = "int"
        columns["value"] = "float"
        columns["name"] = "str"
        if topology.total_tasks >= 3:
            columns["category"] = "str"
        if topology.total_tasks >= 5:
            columns["timestamp"] = "str"
            columns["flag"] = "bool"

    rows = _generate_zero_rows(columns, row_count=max_rows, seed=seed)
    col_schema = [
        {"name": name, "type": typ, "nullable": True}
        for name, typ in columns.items()
    ]

    return {
        "rows": rows,
        "row_count": len(rows),
        "schema": col_schema,
        "provenance": [{
            "source_name": "zero_fill_generator",
            "format": "synthetic",
            "generator_version": SCHEMA_VERSION,
            "task_ids_detected": [t.task_id for t in topology.tasks],
            "operator_types_detected": list(set(t.task_type for t in topology.tasks)),
            "inference_method": "yaml_structural_analysis",
        }],
        "errors": [],
        "meta": {
            "mode": "zero_fill",
            "seed": seed,
            "max_rows": max_rows,
            "columns_detected": len(columns),
            "tasks_detected": topology.total_tasks,
            "orchestrator": topology.orchestrator,
            "total_edges": topology.total_edges,
        },
    }


def extract_expected_row_count(code_text: str) -> int:
    """Quick heuristic to estimate a reasonable row count for testing.

    More complex pipelines get fewer rows to keep runtime bounded.
    """
    hints = _extract_schema_hints(code_text)
    num_tasks = len(hints["task_ids"])
    num_ops = len(hints["operator_types"])

    if num_tasks == 0:
        return 5
    if num_tasks <= 3:
        return 20
    if num_tasks <= 7:
        return 10
    if num_tasks <= 15:
        return 5
    return 3
