from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
import subprocess
import sys

from jsonschema import Draft202012Validator
import pytest

from pisces.report_views import (
    MAX_REPORT_BYTES,
    build_report_view,
    build_view_bundle,
    load_report,
    view_schema,
)
from pisces.unified_evaluator import UnifiedEnergyConfig, run_unified_energy_evaluation

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def examples():
    return json.loads((ROOT / "frontend/src/test/fixtures/examples.json").read_text())


def test_saved_view_snapshots_and_exact_byte_source_match(examples):
    for example in examples:
        before = copy.deepcopy(example["report"])
        view = build_report_view(example["report"])
        assert view == example["view"]
        assert example["report"] == before
        assert view["admission"]["status"] == "suite_inventory"
        assert (
            hashlib.sha256(example["source"]["text"].encode()).hexdigest()
            == view["identity"]["artifact_sha256"]
        )
        assert "/Users/" not in json.dumps(view)
    assert [example["view"]["outputs"]["status"] for example in examples] == [
        "passed",
        "failed",
        "failed",
    ]


def test_metric_zero_missingness_scopes_and_diagnostics(examples):
    for index, example in enumerate(examples):
        view = build_report_view(example["report"])
        metrics = {metric["id"]: metric for metric in view["metrics"]}
        assert metrics["worker.read"]["value"] is None
        assert metrics["worker.read"]["reason_codes"] == ["counter_file_empty"]
        assert metrics["modeled.carbon"]["value"] is None
        assert metrics["modeled.carbon"]["pointer"] is None
        assert metrics["execution.memory"]["scope"] == "execution_block"
        assert "lifetime" in metrics["execution.memory"]["definition"]
        if index == 0:
            assert metrics["worker.throttling"]["value"] == 0
            assert metrics["worker.throttling"]["availability"] == "available"
        else:
            assert metrics["execution.cpu"]["evidence_role"] == "diagnostic"
            assert (
                "/attempt_diagnostics/operational_metrics/"
                in metrics["execution.cpu"]["pointer"]
            )
        assert not view["comparison"]["resource_eligible"]
        assert view["comparison"]["pairwise_status"] == "not_evaluated"


def test_all_metric_pointers_resolve_to_unchanged_raw_readings(examples):
    for example in examples:
        for metric in example["view"]["metrics"]:
            if metric["pointer"]:
                value = example["report"]
                for key in metric["pointer"].strip("/").split("/"):
                    value = value[int(key)] if isinstance(value, list) else value[key]
                assert value == metric["raw_value"]


def test_current_static_report_and_error_report(tmp_path):
    path = tmp_path / "plain.py"
    path.write_text("VALUE = 1\n")
    for workflow in (path, tmp_path / "missing.py"):
        report = run_unified_energy_evaluation(
            UnifiedEnergyConfig(workflow_path=workflow)
        )
        view = build_report_view(report)
        assert view["admission"]["status"] == "current_inventory"
        assert view["outputs"]["status"] == "not_evaluated"
        assert all(metric["value"] is None for metric in view["metrics"])


def test_unsupported_legacy_and_inconsistent_alias_are_inventory_only(examples):
    for report, status in (
        ({"tool": "other"}, "unsupported"),
        ({"tool": "pisces-unified", "report_schema_version": "9.0.0"}, "rejected"),
        (
            {
                "tool": "pisces-unified",
                "report_schema_version": "1.3.0",
                "workflow": {},
                "energy": {},
            },
            "legacy_inventory",
        ),
    ):
        view = build_report_view(report)
        assert view["admission"]["status"] == status
        assert view["metrics"] == []
        assert view["comparison"]["resource_eligible"] is None
    report = copy.deepcopy(examples[0]["report"])
    report["result"]["payload"]["runtime_measurement"][
        "eligible_for_resource_comparison"
    ] = True
    assert build_report_view(report)["admission"]["status"] == "rejected"


@pytest.mark.parametrize(
    "data", ['{"a":1,"a":2}', '{"a":NaN}', "[1]", "{bad", '{"a":1e9999}']
)
def test_invalid_json_is_rejected(data):
    with pytest.raises(ValueError):
        build_report_view(load_report(data))


def test_size_depth_and_type_limits():
    with pytest.raises(ValueError, match="too_large"):
        load_report(b" " * (MAX_REPORT_BYTES + 1))
    with pytest.raises(ValueError, match="nesting"):
        load_report('{"a":' * 70 + "1" + "}" * 70)
    with pytest.raises(ValueError):
        build_report_view([])


def test_unknown_counter_reasons_unsafe_integers_and_timeout_snapshots(examples):
    report = copy.deepcopy(examples[0]["report"])
    runtime = report["result"]["payload"]["runtime_measurement"]
    runtime["execution_outcome"] = "timed_out"
    runtime["container_telemetry_missing_reason"] = "final_snapshot_lost"
    runtime["container_telemetry"] = None
    metrics = {item["id"]: item for item in build_report_view(report)["metrics"]}
    assert metrics["worker.cpu"]["value"] is None
    assert metrics["worker.cpu"]["reason_codes"] == ["final_snapshot_lost"]
    report = copy.deepcopy(examples[0]["report"])
    worker = report["result"]["payload"]["runtime_measurement"]["container_telemetry"]
    worker["memory_peak_bytes"] = 2**53 + 1
    worker["metric_quality"]["block_read_bytes"]["reasons"] = {
        "before": "new_counter_reason"
    }
    metrics = {item["id"]: item for item in build_report_view(report)["metrics"]}
    assert metrics["worker.memory"]["value"] is None
    assert metrics["worker.memory"]["reason_codes"] == [
        "reading_exceeds_json_safe_integer"
    ]
    assert metrics["worker.read"]["reason_codes"] == ["new_counter_reason"]


def test_unified_runtime_aliases_and_airflow_coverage_are_not_dependency_edges(
    examples, tmp_path
):
    workflow = tmp_path / "workflow.py"
    workflow.write_text("VALUE = 1\n")
    report = run_unified_energy_evaluation(UnifiedEnergyConfig(workflow_path=workflow))
    runtime = copy.deepcopy(
        examples[0]["report"]["result"]["payload"]["runtime_measurement"]
    )
    del runtime["engine_evidence"]
    runtime["task_coverage"] = {
        "executed_task_ids": ["extract", "transform"],
        "failed_task_ids": [],
    }
    report["energy"]["measurements"]["runtime"] = runtime
    report["energy"]["runtime_measurement"] = runtime
    view = build_report_view(report)
    assert view["admission"]["status"] == "current_inventory"
    assert [task["task_id"] for task in view["tasks"]] == ["extract", "transform"]
    assert all("task_coverage" in task["pointer"] for task in view["tasks"])
    assert "edges" not in view
    report["energy"]["runtime_measurement"] = None
    view = build_report_view(report)
    assert view["admission"]["reason"] == "inconsistent_measurement_alias"
    assert view["metrics"] == []


def test_suite_conflicts_are_rejected_and_export_preserves_large_json_integers(
    examples,
):
    report = copy.deepcopy(examples[0]["report"])
    report["result"]["output_assertions"]["passed"] = False
    assert (
        build_report_view(report)["admission"]["reason"]
        == "inconsistent_output_assertion_alias"
    )
    report = {"unrecognized_large_integer": 2**53 + 1}
    bundle = build_view_bundle(report)
    assert json.loads(bundle["report_json"]) == report


def test_packaged_schema_is_valid_and_cli_preserves_original(examples, tmp_path):
    Draft202012Validator.check_schema(view_schema())
    report = examples[0]["report"]
    path = tmp_path / "report.json"
    path.write_text(json.dumps(report))
    command = [sys.executable, "-m", "pisces.cli", "report-view", str(path)]
    result = subprocess.run(command, capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stderr
    bundle = json.loads(result.stdout)
    assert bundle == build_view_bundle(report)
    output = tmp_path / "view.json"
    assert (
        subprocess.run(
            command + ["--out", str(output)], capture_output=True, timeout=10
        ).returncode
        == 0
    )
    assert (
        subprocess.run(
            command + ["--out", str(output)], capture_output=True, timeout=10
        ).returncode
        != 0
    )
