from __future__ import annotations

import json
from pathlib import Path

import pytest

from pisces.dag_comparator import compare_reports
from pisces.report_validation import _validator, render_report, validate_report
from pisces.unified_evaluator import UnifiedEnergyConfig, run_unified_energy_evaluation


@pytest.fixture
def report(tmp_path: Path) -> dict:
    workflow = tmp_path / "workflow.py"
    workflow.write_text("VALUE = 1\n", encoding="utf-8")
    return run_unified_energy_evaluation(UnifiedEnergyConfig(workflow_path=workflow))


def test_current_report_conforms_to_packaged_schema(report: dict) -> None:
    validator = _validator()
    validator.check_schema(validator.schema)
    assert validate_report(report).status == "current_inventory"
    assert json.loads(render_report(report))["report_schema_version"] == "1.4.0"


def test_missing_artifact_is_a_valid_error_report(tmp_path: Path) -> None:
    report = run_unified_energy_evaluation(UnifiedEnergyConfig(workflow_path=tmp_path / "missing.py"))
    assert validate_report(report).status == "current_inventory"
    assert report["errors"][0]["code"] == "WORKFLOW_NOT_FOUND"


@pytest.mark.parametrize(("field", "value", "reason"), [
    ("report_schema_version", "9.0.0", "unsupported_report_schema_version"),
    ("metadata.artifact_sha256", None, "missing_artifact_sha256"),
    ("metadata.artifact_sha256", "bad-hash", "invalid_report_schema"),
    ("metadata.duration_s", float("nan"), "invalid_report_json_values"),
    ("energy.measurements.import_build", {"modeled_energy": {"energy_consumed_kwh": True}}, "invalid_report_schema"),
    ("energy.runtime_measurement", {"status": "ok"}, "invalid_report_schema"),
])
def test_bad_report_is_rejected_by_writer_and_comparator(report, tmp_path, field, value, reason) -> None:
    target = report
    *parents, name = field.split(".")
    for key in parents:
        target = target[key]
    target[name] = value
    path = tmp_path / "bad.json"
    path.write_text(json.dumps(report), encoding="utf-8")

    assert validate_report(report).reason == reason
    with pytest.raises(ValueError, match=reason):
        render_report(report)
    comparison = compare_reports([path])
    assert comparison["report_count"] == 0
    assert comparison["skipped_reports"][0]["reason"] == reason


def test_current_measurement_alias_cannot_disagree(report) -> None:
    report["energy"]["import_build_measurement"] = {"status": "ok"}
    assert validate_report(report).reason == "inconsistent_measurement_alias"


def test_legacy_projection_is_inventory_only() -> None:
    path = Path(__file__).parent / "golden_reports" / "legacy_projection.json"
    report = json.loads(path.read_text(encoding="utf-8"))
    assert validate_report(report).status == "rejected"
    result = compare_reports([path])
    assert result["summaries"][0]["report_admission"] == "legacy_inventory"
    assert result["summaries"][0]["estimate"]["yearly_emissions_kgco2eq"] == 1.25
    assert result["comparison"]["status"] == "not_comparable"
