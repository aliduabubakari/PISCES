from __future__ import annotations

from contextlib import nullcontext
import json
from pathlib import Path
import subprocess

import pytest

from benchmarks.climate_fusion.benchmark import build_bundle
from pisces.energy import oci_runtime as oci
from pisces.energy.cgroup_metrics import snapshot, summarize
from pisces.energy.output_equivalence import ORACLE_VERSION
from pisces.energy.runtime_evidence import finalize_runtime_measurement
from pisces.energy.workload_contract import load_workload_contract
from pisces.unified_evaluator import UnifiedEnergyConfig, run_unified_energy_evaluation


def test_cgroup_deltas_and_lifetime_peak(tmp_path: Path) -> None:
    before = snapshot(tmp_path)
    assert summarize(before, before, wall_s=1)["cpu_s"] is None
    (tmp_path / "cpu.stat").write_text("usage_usec 100\nthrottled_usec 20\n", encoding="ascii")
    (tmp_path / "memory.peak").write_text("4096\n", encoding="ascii")
    (tmp_path / "io.stat").write_text("8:0 rbytes=200 wbytes=80\n8:1 rbytes=0 wbytes=0\n", encoding="ascii")
    before = snapshot(tmp_path)
    (tmp_path / "cpu.stat").write_text("usage_usec 500100\nthrottled_usec 21\n", encoding="ascii")
    (tmp_path / "io.stat").write_text("8:0 rbytes=350 wbytes=80\n8:1 rbytes=50 wbytes=0\n", encoding="ascii")
    result = summarize(before, snapshot(tmp_path), wall_s=1)
    assert result["cpu_s"] == 0.5
    assert result["memory_peak_bytes"] == 4096
    assert result["block_read_bytes"] == 200
    assert result["block_write_bytes"] == 0
    assert result["metric_quality"]["block_write_bytes"] == {"status": "available", "reasons": {}}
    assert result["oom_kill_delta"] is None
    (tmp_path / "io.stat").write_text("", encoding="ascii")
    assert summarize(before, snapshot(tmp_path), wall_s=1)["block_write_bytes"] is None


@pytest.mark.parametrize(("baseline", "reason", "status"), [
    (None, "counter_file_missing", "unavailable"),
    ("", "counter_file_empty", "unavailable"),
    ("bad-device rbytes=10\n", "malformed_counter", "invalid"),
    ("8:0 wbytes=80\n", "counter_field_missing", "unavailable"),
    ("8:0 rbytes=? wbytes=80\n", "malformed_counter", "invalid"),
    ("8:0 rbytes=300 wbytes=80\n", "counter_decreased", "invalid"),
    ("8:1 rbytes=200 wbytes=80\n", "device_set_changed", "unavailable"),
])
def test_missing_or_invalid_io_baseline_does_not_become_zero(tmp_path, baseline, reason, status):
    path = tmp_path / "io.stat"
    if baseline is not None:
        path.write_text(baseline, encoding="ascii")
    before = snapshot(tmp_path)
    path.write_text("8:0 rbytes=200 wbytes=80\n", encoding="ascii")
    result = summarize(before, snapshot(tmp_path), wall_s=1)
    assert result["block_read_bytes"] is None
    quality = result["metric_quality"]["block_read_bytes"]
    assert quality["status"] == status and reason in quality["reasons"].values()
    if baseline is not None and baseline.startswith("8:0"):
        assert result["block_write_bytes"] == 0


def test_unreadable_counter_and_decreasing_cpu_are_distinct(tmp_path, monkeypatch):
    read = Path.read_text

    def fail_io(path, *args, **kwargs):
        if path.name == "io.stat":
            raise PermissionError
        return read(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", fail_io)
    cpu = tmp_path / "cpu.stat"
    cpu.write_text("usage_usec 10\n", encoding="ascii")
    before = snapshot(tmp_path)
    cpu.write_text("usage_usec 5\n", encoding="ascii")
    result = summarize(before, snapshot(tmp_path), wall_s=1)
    assert result["cpu_s"] is None
    assert result["metric_quality"]["cpu_s"] == {"status": "invalid", "reasons": {"delta": "counter_decreased"}}
    assert result["metric_quality"]["block_read_bytes"]["reasons"]["before"] == "counter_file_unreadable"


def test_create_command_pins_limits_and_does_not_pass_host_env(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("OPENROUTER_API_KEY", "test-only")
    command = oci._create_command(["docker"], "container", "sha256:" + "a" * 64, tmp_path)
    for flag in ("--pull=never", "--network=none", "--read-only", "--cap-drop=ALL",
                 "--security-opt=no-new-privileges", "--user=50000:50000", "--cgroupns=private"):
        assert flag in command
    assert command[command.index("--mount") + 1].endswith(",target=/input,readonly")
    assert not any("test-only" in arg or "docker.sock" in arg or "--env-file" in arg for arg in command)


@pytest.mark.parametrize(("details", "outcome"), [
    ({"timed_out": True}, "timed_out"),
    ({"oom_killed": True}, "container_oom_killed"),
    ({"exit_code": 1}, "container_exit_failed"),
    ({"payload": {"worker_returncode": 1}}, "container_worker_failed"),
    ({"payload": {"container_telemetry": {"oom_kill_delta": 1}}}, "container_oom_killed"),
    ({"creation_error": "container_create_failed"}, "preflight_rejected"),
])
def test_exit_outcomes(details, outcome) -> None:
    assert oci._exit_outcome({"exit_code": 0, "attach_returncode": 0, "payload": {}, **details}) == outcome


def test_timeout_inspects_then_removes_container(monkeypatch) -> None:
    actions = []

    def call(command, **kwargs):
        action = command[1]
        actions.append(action)
        if action == "start":
            raise subprocess.TimeoutExpired(command, 1)
        state = json.dumps({"Running": True, "OOMKilled": False, "ExitCode": 0}) if action == "inspect" else ""
        return subprocess.CompletedProcess(command, 0, state, "")

    monkeypatch.setattr(oci, "_call", call)
    result = oci._execute(["docker"], "sha256:" + "a" * 64, None, 1)
    assert oci._exit_outcome(result) == "timed_out"
    assert actions == ["create", "start", "inspect", "rm"]


@pytest.mark.parametrize("inspect_failed", [False, True])
def test_post_start_failures_retain_collected_evidence(tmp_path, monkeypatch, inspect_failed):
    bundle = tmp_path / "bundle"
    build_bundle(bundle, rows_per_source=1)
    workload = load_workload_contract(contract_path=bundle / "contract.json", binding_path=bundle / "baseline.binding.json",
                                      artifact_path=bundle / "baseline.py")
    telemetry = {"wall_clock_s": 1, "cpu_s": None, "memory_peak_bytes": 1024}
    measured = {"status": "ok", "execution_outcome": "completed", "execution_adapter_used": "native",
                "eligible_for_paired_equivalence_comparison": True, "workload_contract": workload.provenance(),
                "task_coverage": {"passed": True}, "input_consumption": {"passed": True},
                "output_assertions": {"passed": True, "oracle_version": ORACLE_VERSION,
                                      **{key: getattr(workload, key) for key in (
                                          "contract_sha256", "artifact_sha256", "binding_sha256")}}}

    def call(command, **kwargs):
        action = command[1]
        if action == "start":
            return subprocess.CompletedProcess(command, 0, json.dumps({"ok": True, "runtime_measurement": measured,
                                                                       "container_telemetry": telemetry}), "")
        if action == "rm":
            raise subprocess.TimeoutExpired(command, 15)
        if action == "inspect":
            return subprocess.CompletedProcess(command, int(inspect_failed),
                                               json.dumps({"Running": False, "OOMKilled": False, "ExitCode": 0}), "")
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(oci, "_local_engine", lambda: nullcontext(["docker"]))
    monkeypatch.setattr(oci, "_inspect_engine", lambda cli, image: {"image_id": image})
    monkeypatch.setattr(oci, "_call", call)
    result = oci.run_oci(workload, image="sha256:" + "a" * 64, timeout_s=15)
    runtime = result["runtime_measurement"]
    assert result["ok"] is not inspect_failed
    assert runtime["execution_outcome"] == ("container_state_unverified" if inspect_failed else "completed")
    assert runtime["container_lifecycle"]["cleanup_error"] == "container_cleanup_failed"
    assert runtime["container_telemetry"] == telemetry
    assert runtime["output_assertions"]["passed"] is True
    finalize_runtime_measurement(runtime, adapter_requested="oci", contract_requested=True)
    assert runtime["telemetry"]["container"] == "partial"
    assert runtime["contract_validation"]["status"] == ("not_verified" if inspect_failed else "checks_passed")
    assert not runtime["eligible_for_paired_equivalence_comparison"]


def test_mutable_image_is_rejected_before_docker_call(monkeypatch) -> None:
    monkeypatch.setattr(oci, "_call", lambda *args, **kw: pytest.fail("unexpected Docker call"))
    with pytest.raises(oci.OCIError, match="immutable_image"):
        oci._inspect_engine(["docker"], "image:latest")


def test_stage_copies_only_verified_inputs(tmp_path: Path) -> None:
    bundle = tmp_path / "bundle"
    build_bundle(bundle, rows_per_source=1)
    workload = load_workload_contract(
        contract_path=bundle / "contract.json", binding_path=bundle / "baseline.binding.json",
        artifact_path=bundle / "baseline.py",
    )
    (bundle / ".env").write_text("CANARY=never-stage", encoding="utf-8")
    stage = tmp_path / "stage"
    stage.mkdir()
    oci._stage(workload, stage)
    assert not list(stage.rglob(".env"))
    assert not list(stage.rglob("single_read.py"))
    assert json.loads((stage / "request.json").read_text())["read_only_fixture_mount"] is True
    workload.fixtures["noaa"].write_text("changed", encoding="utf-8")
    other = tmp_path / "other"
    other.mkdir()
    with pytest.raises(oci.OCIError, match="staged_input_sha256_mismatch"):
        oci._stage(workload, other)


def test_oci_preflight_does_not_start_host_or_container(tmp_path: Path, monkeypatch) -> None:
    from pisces.energy import runtime_evaluation as evaluator

    path = tmp_path / "dag.py"
    path.write_text("from airflow.sdk import DAG\n", encoding="utf-8")

    def unexpected(*args, **kwargs):
        pytest.fail("preflight must not execute an artifact or contact a provider")

    for name in ("run_oci", "_run_runtime_measurement", "_run_import_build_measurement", "build_provider"):
        monkeypatch.setattr(evaluator, name, unexpected)
    report = run_unified_energy_evaluation(UnifiedEnergyConfig(
        workflow_path=path, enable_energy_evaluation=True, energy_execution_adapter="oci",
    ))
    assert report["energy"]["runtime_measurement"]["execution_outcome"] == "preflight_rejected"
    assert report["energy"]["errors"][0]["message"] == "oci_requires_image_contract_and_binding"
