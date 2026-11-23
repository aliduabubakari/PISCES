"""Opt-in engine failure, retry, ambiguity and deadline checks."""

import json
import os
import subprocess

import pytest

from pisces.benchmarks.catalog import build_suite
from pisces.benchmarks.runner import _load
from pisces.energy.oci_runtime import run_oci
from pisces.energy.workload_contract import sha256_file


@pytest.mark.parametrize("target", ["prefect", "dagster"])
@pytest.mark.parametrize("case", ["failed_task", "retries", "ambiguous", "deadline"])
def test_qualified_engine_failure_states(tmp_path, target, case):
    image = os.environ.get(f"PISCES_{target.upper()}_TEST_IMAGE")
    if not image:
        pytest.skip(f"Set PISCES_{target.upper()}_TEST_IMAGE")
    bundle = tmp_path / "suite"
    build_suite(bundle, rows=30, keys=4, target=target)
    artifact = bundle / "event_aggregation/valid.py"
    source = artifact.read_text(encoding="utf-8")
    if case == "failed_task":
        source = source.replace("def transform():\n", "def transform():\n    raise RuntimeError('deliberate_step_failure')\n")
    elif case == "retries":
        if target == "prefect":
            source = source.replace("retries=0", "retries=1")
        else:
            source = source.replace("from dagster import job, op", "from dagster import job, op, RetryPolicy")
            source = source.replace("@job(name=", "@job(op_retry_policy=RetryPolicy(max_retries=1), name=")
    elif case == "ambiguous":
        source += "\ndel pisces_entrypoint\n" + (
            "other_flow = pipeline.with_options(name='other')\n" if target == "prefect" else
            "other_job = pipeline.graph.to_job(name='other')\n"
        )
    artifact.write_text(source, encoding="utf-8")
    binding = artifact.with_suffix(".binding.json")
    value = json.loads(binding.read_text())
    value["artifact_sha256"] = sha256_file(artifact)
    binding.write_text(json.dumps(value), encoding="utf-8")
    payload = run_oci(_load(bundle, "event_aggregation", "valid"), image=image, timeout_s=2 if case == "deadline" else 120)
    assert payload["ok"] is False
    runtime = payload["runtime_measurement"]
    if case == "deadline":
        assert runtime["execution_outcome"] == "timed_out"
        name = runtime["container_lifecycle"]["container_name"]
        assert not subprocess.check_output(["docker", "ps", "-a", "--filter", f"name={name}", "--format", "{{.Names}}"], text=True).strip()
    elif case == "failed_task":
        assert runtime["execution_outcome"] == "failed", payload
        assert runtime["engine_evidence"]["target"] == target
        assert any(state["task_id"] == "transform" and state["status"] == "failed"
                   for state in runtime["engine_evidence"]["task_states"])
        assert runtime["task_coverage"]["passed"] is False
    else:
        expected = "unsupported_" if case == "retries" else "explicit_or_unambiguous_entrypoint_required"
        assert expected in payload["error"]["message"], payload
