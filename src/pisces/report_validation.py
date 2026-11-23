"""Versioned report admission for writers and inventory consumers."""

from __future__ import annotations

import json
from dataclasses import dataclass
from functools import lru_cache
from importlib.resources import files
from typing import Any

from jsonschema import Draft202012Validator


REPORT_SCHEMA_VERSION = "1.4.0"
LEGACY_VERSIONS = {"1.0.0", "1.1.0", "1.2.0", "1.3.0"}


@dataclass(frozen=True)
class ReportAdmission:
    status: str
    reason: str | None = None
    detail: str | None = None


@lru_cache(maxsize=1)
def _validator() -> Draft202012Validator:
    resource = files("pisces").joinpath("schemas", f"unified-report-{REPORT_SCHEMA_VERSION}.schema.json")
    return Draft202012Validator(json.loads(resource.read_text(encoding="utf-8")))


def validate_report(report: Any, *, allow_legacy: bool = False) -> ReportAdmission:
    """Admit current evidence or, explicitly, legacy inventory; neither qualifies a pair."""
    if not isinstance(report, dict) or report.get("tool") not in (None, "pisces-unified"):
        return ReportAdmission("rejected", "not_pisces_unified_evaluation_report")
    try:
        json.dumps(report, allow_nan=False)
    except (TypeError, ValueError, OverflowError):
        return ReportAdmission("rejected", "invalid_report_json_values")
    version = report.get("report_schema_version")
    if version != REPORT_SCHEMA_VERSION:
        if allow_legacy and (version is None or (isinstance(version, str) and version in LEGACY_VERSIONS)):
            if isinstance(report.get("workflow"), dict) and isinstance(report.get("energy"), dict):
                return ReportAdmission("legacy_inventory", "legacy_report_inventory_only")
            return ReportAdmission("rejected", "not_pisces_unified_evaluation_report")
        return ReportAdmission("rejected", "unsupported_report_schema_version")
    error = next(_validator().iter_errors(report), None)
    if error is not None:
        path = "/" + "/".join(str(item) for item in error.absolute_path)
        return ReportAdmission("rejected", "invalid_report_schema", f"{path}: {error.validator}")
    energy = report.get("energy") or {}
    measurements = energy.get("measurements", {})
    for alias, key in (("runtime_measurement", "runtime"), ("import_build_measurement", "import_build")):
        if alias in energy and energy[alias] != measurements.get(key):
            return ReportAdmission("rejected", "inconsistent_measurement_alias", alias)
    if energy and not report["metadata"]["artifact_sha256"]:
        return ReportAdmission("rejected", "missing_artifact_sha256")
    return ReportAdmission("current_inventory")


def render_report(report: dict[str, Any]) -> str:
    admission = validate_report(report)
    if admission.status != "current_inventory":
        detail = f" ({admission.detail})" if admission.detail else ""
        raise ValueError(f"{admission.reason}{detail}")
    return json.dumps(report, indent=2, allow_nan=False)
