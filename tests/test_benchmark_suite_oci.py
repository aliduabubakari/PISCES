"""Explicitly opt in: this test launches twenty isolated Airflow containers."""

import json
import os

import pytest

from pisces.benchmarks.catalog import PIPELINES, build_suite
from pisces.benchmarks.runner import run_suite


@pytest.mark.parametrize(("target", "variable"), [
    ("airflow", "PISCES_SUITE_OCI_TEST_IMAGE"), ("prefect", "PISCES_PREFECT_TEST_IMAGE"), ("dagster", "PISCES_DAGSTER_TEST_IMAGE"),
])
def test_all_twenty_engine_cases(tmp_path, target, variable):
    image = os.environ.get(variable)
    if not image:
        pytest.skip(f"Set {variable} to run the full {target} benchmark OCI suite")
    suite, results = tmp_path / "suite", tmp_path / "oci"
    build_suite(suite, rows=100, keys=8, target=target)
    summary = run_suite(suite, results, backend="oci", image=image)
    assert summary["status"] == "passed"
    for pipeline in PIPELINES:
        for case in ("valid", "equivalent", "same_shape_wrong", "partial"):
            attempt = json.loads((results / f"{pipeline}-{case}-01.json").read_text())
            assert attempt["expected_behavior_matched"]
            runtime = attempt["result"]["payload"]["runtime_measurement"]
            assert runtime["execution_outcome"] == "completed"
            assert runtime["task_coverage"]["executed_task_count"] == 3
            assert runtime["input_consumption"]["passed"]
            assert runtime["container_telemetry"]["cpu_s"] > 0
            if target != "airflow":
                assert runtime["engine_evidence"]["target"] == target
                assert len(runtime["engine_evidence"]["task_states"]) == 3
                assert all(task["status"] == "executed" for task in runtime["engine_evidence"]["task_states"])
