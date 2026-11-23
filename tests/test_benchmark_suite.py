from __future__ import annotations

from contextlib import ExitStack
import csv
import json
import subprocess
import sys

import pytest

from pisces.benchmarks.catalog import CASES, PIPELINES, build_suite
from pisces.benchmarks.runner import _load, assess, load_suite, run_suite
from pisces.energy.execution_sandbox import prepared_execution_sandbox
from pisces.energy.output_equivalence import compare_outputs


@pytest.fixture
def suite(tmp_path):
    root = tmp_path / "suite"
    build_suite(root, rows=40, keys=4)
    return root


def test_five_families_and_all_twenty_cases_are_discriminated(suite, tmp_path):
    results = tmp_path / "standalone"
    summary = run_suite(suite, results)
    assert summary["status"] == "passed"
    assert set(summary["pipelines"]) == set(PIPELINES)
    assert len(list(results.glob("*.json"))) == 22
    for pipeline, record in summary["pipelines"].items():
        assert record["positive_exact_equivalence"] == "passed"
        for case in CASES:
            record = json.loads((results / f"{pipeline}-{case}-01.json").read_text())
            assert record["expected_behavior_matched"]
            assert record["output_verified"] == (case in ("valid", "equivalent"))
            assert record["same_shape_as_reference"] == (case != "partial")
            assert record["result"]["execution_outcome"] == "completed"
    manifest = json.loads((results / "manifest.json").read_text())
    assert manifest["execution_scope"] == "standalone_task_bodies"
    assert manifest["oci_environment"] is None


@pytest.mark.parametrize("pipeline", PIPELINES)
def test_direct_exact_equivalence_and_distinct_artifacts(suite, pipeline):
    with ExitStack() as stack:
        left, right = (_load(suite, pipeline, case) for case in ("valid", "equivalent"))
        assert left.artifact_sha256 != right.artifact_sha256
        assert left.contract_sha256 == right.contract_sha256
        assert left.binding["expected_task_ids"] == ["normalize", "transform", "write_outputs"]
        spaces = [stack.enter_context(prepared_execution_sandbox(work)) for work in (left, right)]
        for space in spaces:
            subprocess.run([sys.executable, str(space.workflow_file)], cwd=space.working, check=True, timeout=10)
        assert compare_outputs(left, right, left_workspace=spaces[0].working, right_workspace=spaces[1].working)["status"] == "passed"


def test_reference_facts_are_independently_checkable(tmp_path):
    root = tmp_path / "small"
    build_suite(root, rows=10, keys=2)

    def rows(pipeline):
        with (root / pipeline / "references/result.csv").open(newline="") as stream:
            return list(csv.reader(stream))[1:]

    assert rows("event_aggregation") == [["0", "4", "344"], ["1", "5", "430"]]
    assert rows("order_enrichment")[0] == ["1", "region_1", "196"]
    assert rows("inventory_latest") == [["0", "4", "24", "true"], ["1", "4", "27", "true"]]
    assert rows("sensor_windows") == [["0", "0", "3", "-516"], ["0", "60", "1", "-144"],
                                      ["1", "0", "4", "-688"], ["1", "60", "1", "-137"]]
    assert rows("climate_fusion")[0] == ["bom", "0", "32.7"]
    assert len(rows("climate_fusion")) == 10


def test_generation_is_relocatable_deterministic_and_cardinality_varies(tmp_path):
    a, b, c = (tmp_path / name for name in ("a", "b", "c"))
    assert build_suite(a, rows=40, keys=4) == build_suite(b, rows=40, keys=4)
    build_suite(c, rows=40, keys=8)
    assert load_suite(a) == load_suite(b)
    assert _load(a, "inventory_latest", "valid").contract_sha256 != _load(c, "inventory_latest", "valid").contract_sha256
    with pytest.raises(FileExistsError):
        build_suite(a, rows=40, keys=4)
    (a / "event_aggregation" / "valid.py").write_text("changed", encoding="utf-8")
    with pytest.raises(ValueError, match="hash mismatch"):
        load_suite(a)


def test_negative_fixture_crash_is_not_a_successful_rejection():
    proof = {"status": "failed", "passed": False, "results": [
        {"logical_name": "result", "reason": "output_content_mismatch", "row_count": 5},
        {"logical_name": "quality", "passed": True, "row_count": 2},
    ]}
    assert assess("same_shape_wrong", proof, {"result": 5, "quality": 2}, execution_completed=True)["expected_behavior_matched"]
    assert not assess("same_shape_wrong", proof, {"result": 5, "quality": 2}, execution_completed=False)["expected_behavior_matched"]


def test_suite_inventory_cannot_omit_executed_files(suite):
    path = suite / "suite.json"
    manifest = json.loads(path.read_text())
    del manifest["files"]["event_aggregation/valid.py"]
    path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ValueError, match="inventory does not cover"):
        load_suite(suite)


@pytest.mark.parametrize(("rows", "keys"), [(10, 1), (11, 11)])
def test_small_and_uneven_loads_preserve_negative_controls(tmp_path, rows, keys):
    root = tmp_path / "suite"
    build_suite(root, rows=rows, keys=keys)
    assert run_suite(root, tmp_path / "run")["status"] == "passed"


def test_cli_subsets_and_no_clobber(suite, tmp_path):
    result = tmp_path / "selected"
    command = [sys.executable, "-m", "pisces.benchmarks", "run", str(suite), str(result), "--pipeline", "event_aggregation"]
    assert subprocess.run(command, capture_output=True, text=True, timeout=15).returncode == 0
    assert set(json.loads((result / "summary.json").read_text())["pipelines"]) == {"event_aggregation"}
    assert subprocess.run(command, capture_output=True, text=True, timeout=15).returncode != 0


def test_repeated_attempts_keep_distinct_files_and_equivalence(suite, tmp_path):
    path = tmp_path / "repeated"
    summary = run_suite(suite, path, repetitions=2, pipelines=["inventory_latest"])
    assert summary["status"] == "passed"
    assert summary["pipelines"]["inventory_latest"]["positive_exact_equivalence"] == "passed"
    assert all(case["attempted"] == case["expected_behavior_matched"] == 2
               for case in summary["pipelines"]["inventory_latest"]["cases"].values())
    assert len(list(path.glob("inventory_latest-*.json"))) == 8


@pytest.mark.parametrize(("rows", "keys"), [(9, 1), (1_000_001, 1), (10, 11), (10, 0)])
def test_invalid_generation_budgets(tmp_path, rows, keys):
    with pytest.raises(ValueError):
        build_suite(tmp_path / "suite", rows=rows, keys=keys)
    assert not (tmp_path / "suite").exists()
