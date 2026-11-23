from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import time

import pytest

from benchmarks.climate_fusion.benchmark import build_bundle
from pisces.energy.execution_sandbox import prepared_execution_sandbox
from pisces.energy.output_equivalence import verify_output
from pisces.energy.task_timing import FILENAME, read_task_timing
from pisces.energy.workload_contract import load_workload_contract


@pytest.mark.parametrize(("changes", "reason"), [
    ({"task_id": "other"}, "record_malformed"),
    ({"wall_start_ns": True}, "record_malformed"),
    ({"wall_end_ns": 501}, "outside_execution_window"),
    ({"thread_cpu_end_ns": 201}, "invalid_thread_cpu_interval"),
])
def test_rejects_invalid_intervals(tmp_path: Path, changes: dict, reason: str) -> None:
    (tmp_path / FILENAME).write_text(json.dumps({
        "task_id": "fuse", "wall_start_ns": 200, "wall_end_ns": 300,
        "thread_cpu_start_ns": 100, "thread_cpu_end_ns": 170, **changes,
    }), encoding="utf-8")
    result = read_task_timing(tmp_path, task_id="fuse", block_start_ns=100, block_end_ns=500)
    assert result["status"] == "invalid" and result["reason"] == reason
    assert "wall_clock_s" not in result


def test_missing_oversize_and_symlink_records_are_not_measurements(tmp_path: Path) -> None:
    def read():
        return read_task_timing(tmp_path, task_id="fuse", block_start_ns=100, block_end_ns=500)

    assert read()["status"] == "unavailable"
    path = tmp_path / FILENAME
    path.write_bytes(b" " * 1025)
    assert read()["reason"] == "record_malformed"
    path.unlink()
    path.symlink_to(tmp_path / "not-a-timing-file")
    assert read()["reason"] == "record_unreadable"


def test_timed_task_preserves_output_bytes_and_contract(tmp_path: Path) -> None:
    outputs = []
    contract_digests = []
    for enabled in (False, True):
        bundle = tmp_path / str(enabled)
        build_bundle(bundle, rows_per_source=20, scaling=True, task_timing=enabled)
        workload = load_workload_contract(contract_path=bundle / "contract.json", binding_path=bundle / "baseline.binding.json",
                                          artifact_path=bundle / "baseline.py")
        contract_digests.append(workload.contract_sha256)
        with prepared_execution_sandbox(workload) as sandbox:
            start = time.perf_counter_ns()
            subprocess.run([sys.executable, str(sandbox.workflow_file)], cwd=sandbox.working, check=True, timeout=10)
            end = time.perf_counter_ns()
            timing = read_task_timing(sandbox.working, task_id="fuse", block_start_ns=start, block_end_ns=end)
            output = verify_output(workload, workspace_root=sandbox.working)
            assert output["passed"] is True
            outputs.append(output["results"][0]["sha256"])
            assert timing["status"] == ("available" if enabled else "unavailable")
            if enabled:
                assert 0 < timing["thread_cpu_s"] <= timing["wall_clock_s"] < (end - start) / 1e9
    assert outputs[0] == outputs[1]
    assert contract_digests[0] == contract_digests[1]
