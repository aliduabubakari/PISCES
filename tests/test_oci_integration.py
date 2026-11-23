from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess

import pytest

from benchmarks.climate_fusion.benchmark import build_bundle
from pisces.energy.oci_runtime import check_oci, run_oci
from pisces.energy.task_timing import SOURCE
from pisces.energy.workload_contract import load_workload_contract, sha256_file
from pisces.report_validation import validate_report
from pisces.unified_evaluator import UnifiedEnergyConfig, run_unified_energy_evaluation


@pytest.fixture(scope="module")
def image() -> str:
    value = os.environ.get("PISCES_OCI_TEST_IMAGE")
    if not value:
        pytest.skip("Set PISCES_OCI_TEST_IMAGE to an immutable local Airflow image")
    probe = check_oci(value)
    assert probe["ready"], probe
    assert probe["controllers"]["limits_verified"]
    return value


@pytest.mark.parametrize("case", ["baseline", "wrong_units", "timeout"])
def test_oci_workload(image: str, tmp_path: Path, monkeypatch, case: str) -> None:
    bundle = tmp_path / "bundle"
    build_bundle(bundle, rows_per_source=8)
    name = "baseline" if case == "timeout" else case
    artifact = bundle / f"{name}.py"
    binding = bundle / f"{name}.binding.json"
    body = """
    import errno
    import os
    import socket
    assert os.getuid() == 50000
    assert "PISCES_HOST_SECRET_CANARY" not in os.environ
    assert not Path("/var/run/docker.sock").exists()
    try:
        os.close(os.open("inputs/bom.csv", os.O_WRONLY | os.O_APPEND))
    except OSError as exc:
        assert exc.errno == errno.EROFS
    else:
        raise AssertionError("fixture was writable")
    try:
        socket.create_connection(("192.0.2.1", 80), timeout=0.2)
    except Exception:
        pass
    else:
        raise AssertionError("undeclared network was reachable")
"""
    if case == "timeout":
        body += "    import time\n    time.sleep(60)\n"
    text = artifact.read_text(encoding="utf-8").replace("def fuse():\n", "def fuse():\n" + body)
    artifact.write_text(text, encoding="utf-8")
    spec = json.loads(binding.read_text(encoding="utf-8"))
    spec["artifact_sha256"] = sha256_file(artifact)
    binding.write_text(json.dumps(spec), encoding="utf-8")
    monkeypatch.setenv("PISCES_HOST_SECRET_CANARY", "never-in-container")
    report = run_unified_energy_evaluation(UnifiedEnergyConfig(
        workflow_path=artifact, orchestrator="airflow", enable_energy_evaluation=True,
        energy_execution_adapter="oci", oci_image=image, energy_measurement_maximum_repetitions=1,
        energy_timeout_s=8 if case == "timeout" else 45,
        workload_contract=bundle / "contract.json", artifact_binding=binding,
    ))
    assert validate_report(report).status == "current_inventory", report.get("errors")
    runtime = report["energy"]["measurements"]["runtime"]
    assert not runtime["eligible_for_resource_comparison"]
    if case == "timeout":
        assert runtime["execution_outcome"] == "timed_out", runtime
        assert runtime["container_telemetry"] is None
        assert runtime["container_telemetry_missing_reason"] == "worker_snapshot_lost_to_timeout"
        assert runtime["telemetry"]["container"] == "not_collected"
        name = runtime["container_lifecycle"]["container_name"]
        remaining = subprocess.check_output(
            ["docker", "ps", "-a", "--filter", f"name={name}", "--format", "{{.Names}}"], text=True,
        )
        assert not remaining.strip()
        return
    assert runtime["execution_outcome"] == "completed", runtime
    assert runtime["execution_adapter_used"] == "oci"
    telemetry = runtime["container_telemetry"]
    assert telemetry["cpu_s"] > 0
    assert telemetry["memory_peak_bytes"] > 0
    assert runtime["telemetry"]["container"] == "available"
    assert telemetry["metric_quality"]["cpu_s"] == {"status": "available", "reasons": {}}
    for metric in ("block_read_bytes", "block_write_bytes"):
        assert (telemetry[metric] is not None) == (telemetry["metric_quality"][metric]["status"] == "available")
    window = telemetry["counter_window"]
    before, after = window["cpu_before"], window["cpu_after"]
    assert before["read_finished_ns"] <= window["wall_started_ns"] <= window["wall_finished_ns"] <= after["read_started_ns"]
    assert telemetry["wall_clock_s"] == (window["wall_finished_ns"] - window["wall_started_ns"]) / 1e9
    assert telemetry["cpu_s"] == (after["usage_usec"] - before["usage_usec"]) / 1e6
    assert runtime["modeled_energy"] is None
    assert runtime["input_consumption"]["passed"]
    assert runtime["output_assertions"]["results"][0]["row_count"] == 40
    if case == "wrong_units":
        assert runtime["output_assertions"]["results"][0]["reason"] == "output_content_mismatch"
    else:
        assert runtime["output_assertions"]["status"] == "passed"
        # Includes the separate Airflow database setup subprocess and verification.
        assert runtime["container_telemetry"]["cpu_s"] > runtime["operational_metrics"]["process_cpu_s"]


def test_oci_task_timing_is_nested_and_keeps_verified_output(image: str, tmp_path: Path) -> None:
    bundle = tmp_path / "timed"
    build_bundle(bundle, rows_per_source=100, scaling=True, task_timing=True)
    workload = load_workload_contract(contract_path=bundle / "contract.json", binding_path=bundle / "baseline.binding.json",
                                      artifact_path=bundle / "baseline.py")
    report = run_oci(workload, image=image, timeout_s=45, task_timing_id="fuse")
    assert report["ok"], report
    runtime = report["runtime_measurement"]
    assert runtime["output_assertions"]["passed"]
    assert runtime["output_assertions"]["results"][0]["row_count"] == 500
    timing = runtime["task_timing"]
    assert timing["status"] == "available" and timing["source"] == SOURCE
    assert 0 < timing["thread_cpu_s"] <= timing["wall_clock_s"] < runtime["operational_metrics"]["wall_clock_s"]
    start, end = timing["execution_wall_bounds_ns"]
    assert start <= timing["wall_start_ns"] < timing["wall_end_ns"] <= end
