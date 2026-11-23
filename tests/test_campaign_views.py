from __future__ import annotations

import copy
import json
from pathlib import Path
import subprocess
import sys

import pytest
from jsonschema import Draft202012Validator

from pisces.report_views import build_view_bundle, build_workspace_view
from pisces.report_views.campaigns import (
    build_campaign_view,
    campaign_schema,
    load_campaign_directory,
)

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def examples():
    return json.loads((ROOT / "frontend/src/test/fixtures/campaigns.json").read_text())


@pytest.fixture
def envelope(examples):
    session = examples[0]["report"]["sessions"][0]
    return {
        "tool": "pisces-load-campaign",
        "schema_version": "1.0",
        "pipeline": "Climate Fusion",
        "manifest": copy.deepcopy(session["manifest"]),
        "summary": copy.deepcopy(session["summary"]),
    }


def test_saved_snapshots_and_pointer_units_are_preserved(examples):
    Draft202012Validator.check_schema(campaign_schema())
    for example in examples:
        original = copy.deepcopy(example["report"])
        view = build_workspace_view(original)
        assert view == example["view"]
        assert original == example["report"]
        assert "/Users/" not in json.dumps(view)
        for session in view["sessions"]:
            for metric in session["metrics"]:
                for point in metric["points"]:
                    if point["pointer"]:
                        raw = original
                        for part in point["pointer"].strip("/").split("/"):
                            raw = (
                                raw[int(part)]
                                if isinstance(raw, list)
                                else raw[part.replace("~1", "/").replace("~0", "~")]
                            )
                        assert raw == point["raw_median"]
                        if metric["unit"] == "MiB":
                            assert point["median"] == raw / 1048576


def test_missing_planned_load_stays_visible_without_fit(envelope):
    del envelope["summary"]["loads"]["100000"]
    metric = build_campaign_view(envelope)["sessions"][0]["metrics"][0]
    assert [point["rows"] for point in metric["points"]] == [
        10000,
        100000,
        500000,
        1000000,
    ]
    assert metric["points"][1]["median"] is None
    assert metric["points"][1]["missing_reason"] == "load_not_reported"
    assert metric["fit"]["status"] == "not_available"
    assert all(point["fitted"] is None for point in metric["points"])


def test_missing_metric_partial_coverage_and_fingerprint_drift(envelope):
    load = envelope["summary"]["loads"]["10000"]
    load["metrics"]["task_wall_s"].update(median=None, mad=None, coverage=0)
    metrics = build_campaign_view(envelope)["sessions"][0]["metrics"]
    assert metrics[0]["fit"]["reason"] == "incomplete_signal_coverage"
    assert metrics[1]["fit"]["status"] == "reported_descriptive"
    load["fingerprints_match"] = False
    assert all(
        metric["fit"]["status"] == "not_available"
        for metric in build_campaign_view(envelope)["sessions"][0]["metrics"]
    )


def test_summary_without_manifest_does_not_grant_fit_eligibility(envelope):
    view = build_campaign_view(envelope["summary"])
    assert view["admission"]["status"] == "summary_inventory"
    assert view["sessions"][0]["planned_repetitions"] is None
    assert all(
        metric["fit"]["status"] == "not_available"
        for metric in view["sessions"][0]["metrics"]
    )


def test_memory_high_water_and_independent_sessions(examples):
    view = build_campaign_view(examples[0]["report"])
    assert len(view["sessions"]) == 2
    first, second = view["sessions"]
    assert first["metrics"][0]["points"] != second["metrics"][0]["points"]
    memory = first["metrics"][-1]
    assert memory["fit"]["reason"] == "memory_high_water_not_fitted"
    assert "lifetime" in memory["scope"]
    assert "page cache" in memory["definition"] or "lifetime" in memory["definition"]
    assert "pooled" in " ".join(view["warnings"])


def test_constant_three_load_series_preserves_null_r_squared_and_rejects_wrong_line(
    envelope,
):
    del envelope["manifest"]["workloads"]["1000000"]
    del envelope["summary"]["loads"]["1000000"]
    envelope["manifest"]["round_order"] = [
        [rows for rows in order if rows != 1000000]
        for order in envelope["manifest"]["round_order"]
    ]
    for load in envelope["summary"]["loads"].values():
        load["metrics"]["task_wall_s"]["median"] = 0.1
    envelope["summary"]["time_fits_descriptive_only"] = {
        "task_wall_s": {
            "intercept_s": 0.1,
            "slope_s_per_100k_rows": 0,
            "r_squared": None,
        }
    }
    metric = build_campaign_view(envelope)["sessions"][0]["metrics"][0]
    assert metric["fit"]["status"] == "reported_descriptive"
    assert metric["fit"]["r_squared"] is None
    envelope["summary"]["time_fits_descriptive_only"]["task_wall_s"]["intercept_s"] = (
        0.2
    )
    with pytest.raises(ValueError, match="fit_does_not_describe"):
        build_campaign_view(envelope)


def test_suite_denominators_and_negative_cases(examples):
    view = build_campaign_view(examples[1]["report"])
    assert len(view["inventory"]) == 20
    assert sum(row["planned"] for row in view["inventory"]) == 20
    negative = [row for row in view["inventory"] if row["case"] == "same_shape_wrong"]
    assert all(
        row["expected_behavior_matched"] == 1 and row["verified_outputs"] == 0
        for row in negative
    )
    assert view["sessions"] == []


@pytest.mark.parametrize(
    "change",
    [
        lambda report: report.update(schema_version="99"),
        lambda report: report["summary"]["loads"]["10000"]["metrics"][
            "task_wall_s"
        ].update(median=True),
        lambda report: report["summary"]["loads"]["10000"]["metrics"][
            "task_wall_s"
        ].update(coverage=2),
        lambda report: report["summary"]["loads"]["10000"].update(attempted=1),
        lambda report: report["summary"]["time_fits_descriptive_only"][
            "task_wall_s"
        ].update(slope_s_per_100k_rows=100),
        lambda report: report["summary"].update(time_fits_descriptive_only=[]),
    ],
)
def test_malformed_numbers_counts_fits_and_versions_are_rejected(envelope, change):
    change(envelope)
    with pytest.raises(ValueError):
        build_campaign_view(envelope)


def test_export_cli_bounded_files_and_no_execution(envelope, tmp_path):
    for name in ("manifest", "summary"):
        (tmp_path / f"{name}.json").write_text(json.dumps(envelope[name]))
    output = tmp_path / "view.json"
    command = [
        sys.executable,
        "-m",
        "pisces.cli",
        "campaign-view",
        str(tmp_path),
        "--pipeline",
        "Climate Fusion",
        "--out",
        str(output),
    ]
    result = subprocess.run(command, capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stderr
    assert json.loads(output.read_text()) == build_view_bundle(envelope)
    assert subprocess.run(command, capture_output=True, timeout=10).returncode != 0
    (tmp_path / "manifest.json").unlink()
    (tmp_path / "manifest.json").symlink_to(output)
    with pytest.raises(ValueError, match="symlinks"):
        load_campaign_directory(tmp_path, pipeline="test")


def test_http_campaign_contract_matches_library(examples):
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient
    from pisces.web import create_app

    client = TestClient(create_app(), base_url="http://127.0.0.1")
    for example in examples:
        response = client.post("/api/v1/reports/view", json=example["report"])
        assert response.status_code == 200
        assert response.json() == build_workspace_view(example["report"])
