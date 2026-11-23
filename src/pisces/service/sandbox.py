"""Disposable evaluation-container entry point, receiving only one bounded request."""

from __future__ import annotations

from contextlib import redirect_stderr, redirect_stdout
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

from pisces.energy.cgroup_metrics import measure_cgroup
from pisces.energy.runtime_evidence import finalize_runtime_measurement
from pisces.report_validation import render_report
from pisces.report_views import load_report
from pisces.unified_evaluator import (
    UnifiedEnergyConfig,
    run_unified_energy_evaluation,
    _detect_orchestrator,
)
from .models import JobSubmission, MAX_JOB_BYTES


def isolated() -> bool:
    try:
        root = Path("/sys/fs/cgroup")
        quota, period = (root / "cpu.max").read_text().split()
        status = dict(
            line.split(":", 1)
            for line in Path("/proc/self/status").read_text().splitlines()
            if ":" in line
        )
        return (
            os.geteuid() == 50000
            and Path("/proc/self/cgroup").read_text().strip() == "0::/"
            and int(quota) == 2 * int(period)
            and (root / "memory.max").read_text().strip() == str(2**31)
            and (root / "memory.swap.max").read_text().strip() == "0"
            and (root / "pids.max").read_text().strip() == "128"
            and bool(os.statvfs("/").f_flag & os.ST_RDONLY)
            and status["NoNewPrivs"].strip() == "1"
            and status["Seccomp"].strip() == "2"
            and int(status["CapEff"], 16) == 0
            and all(
                path.name == "lo" or not int((path / "flags").read_text(), 16) & 1
                for path in Path("/sys/class/net").iterdir()
                if path.is_dir()
            )
        )
    except (OSError, ValueError, KeyError):
        return False


def evaluate(request: JobSubmission, directory: Path):
    artifact = directory / "artifact" / request.filename
    artifact.parent.mkdir()
    artifact.write_bytes(request.source.encode("utf-8"))
    report = run_unified_energy_evaluation(
        UnifiedEnergyConfig(workflow_path=artifact, synthetic_cache_mode="off")
    )
    if request.profile in ("airflow", "prefect", "dagster"):
        from pisces.energy.oci_worker import _probe
        from pisces.energy.workload_contract import load_workload_contract

        if (
            _probe().get("status") != "ready"
            or os.environ.get("PISCES_RUNTIME_TARGET", "airflow") != request.profile
        ):
            raise ValueError("native_profile_probe_failed")
        contract_dir = directory / "contract"
        contract_dir.mkdir()
        for file in request.files:
            destination = contract_dir / file.path
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(file.decoded())
        contract = contract_dir / "contract.json"
        binding = artifact.parent / "binding.json"
        contract.write_bytes(request.contract.encode("utf-8"))
        binding.write_bytes(request.binding.encode("utf-8"))
        workload = load_workload_contract(
            contract_path=contract, binding_path=binding, artifact_path=artifact
        )
        if (
            workload.contract["schema_version"] != "2.0"
            or workload.binding["target"] != request.profile
        ):
            raise ValueError("native_contract_target_mismatch")
        for path in (artifact, binding, contract, *contract_dir.rglob("*")):
            if path.is_file():
                path.chmod(0o444)
        runner_input, runner_output = (
            directory / "request.json",
            directory / "runtime.json",
        )
        runner_input.write_text(
            json.dumps(
                {
                    "orchestrator": request.profile,
                    "mode": "zero_fill",
                    "data_source": "contract_fixtures",
                    "execution_adapter": "native",
                    "experiment_mode": True,
                    "collect_modeled_energy": False,
                    "minimum_repetitions": 1,
                    "maximum_repetitions": 1,
                    "warmup_repetitions": 0,
                    "fixture_delivery": "private_stdin_bundle",
                    "workflow_file": str(artifact),
                    "workload_contract": str(contract),
                    "artifact_binding": str(binding),
                }
            ),
            encoding="utf-8",
        )
        if request.profile == "prefect":
            Path("/tmp/prefect").mkdir(exist_ok=True)
            Path("/tmp/.prefect").mkdir(exist_ok=True)
        child, telemetry = measure_cgroup(
            lambda: subprocess.run(
                [
                    sys.executable,
                    "-I",
                    "-m",
                    "pisces.energy.runtime_runner_cli",
                    "--in",
                    str(runner_input),
                    "--out",
                    str(runner_output),
                ],
                cwd=directory,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                env={
                    "HOME": "/tmp",
                    "TMPDIR": "/tmp",
                    "PATH": "/usr/local/bin:/usr/bin:/bin",
                    "LANG": "C.UTF-8",
                    "PISCES_SKIP_POWERMETRICS": "1",
                    "PISCES_OCI_TARGET": request.profile,
                    "PISCES_OCI_INPUT_DELIVERY": "private_stdin_bundle",
                    "PREFECT_HOME": "/tmp/prefect",
                    "PREFECT_SERVER_ANALYTICS_ENABLED": "false",
                    "PREFECT_LOGGING_TO_API_ENABLED": "false",
                    "PREFECT_LOGGING_LEVEL": "ERROR",
                    "DAGSTER_HOME": "/tmp/dagster",
                },
                check=False,
            )
        )
        payload = load_report(runner_output.read_bytes())
        runtime = payload.get("runtime_measurement")
        if not isinstance(runtime, dict):
            raise ValueError("native_runtime_report_missing")
        if child.returncode:
            runtime.update(status="failed", execution_outcome="container_worker_failed")
        # Temporary input files are not mounted read-only: verify their hashes again.
        after = load_workload_contract(
            contract_path=contract, binding_path=binding, artifact_path=artifact
        )
        if after.provenance() != workload.provenance():
            raise ValueError("native_input_bundle_changed")
        runtime["container_telemetry"] = telemetry
        if runtime.get("execution_adapter_used") == "native":
            runtime["execution_adapter_used"] = "oci"
        finalize_runtime_measurement(
            runtime, adapter_requested="oci", contract_requested=True
        )
        attach_runtime(
            report,
            runtime,
            mode="oci",
            source="contract_fixtures",
            scope="isolated_native_runner",
        )
        report.setdefault("warnings", []).append(
            {
                "code": "PRIVATE_STDIN_INPUT_BUNDLE",
                "message": "Inputs are hash-checked private temporary files, not a read-only input mount; approved workloads only.",
            }
        )
        return report
    if request.profile == "static":
        return report
    if _detect_orchestrator(artifact) != "unknown":
        raise ValueError("orchestrator_script_requires_native_profile_and_contract")
    metrics_file = directory / "process-metrics.json"
    child, telemetry = measure_cgroup(
        lambda: subprocess.run(
            [
                sys.executable,
                "-I",
                "-m",
                "pisces.service.script_runner",
                str(artifact),
                str(metrics_file),
            ],
            cwd=directory,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env={
                "HOME": "/tmp",
                "TMPDIR": "/tmp",
                "PATH": "/usr/local/bin:/usr/bin:/bin",
                "LANG": "C.UTF-8",
            },
            check=False,
        )
    )
    operational = (
        load_report(metrics_file.read_bytes()) if metrics_file.exists() else None
    )
    runtime = {
        "status": "ok" if child.returncode == 0 else "failed",
        "execution_outcome": "completed" if child.returncode == 0 else "failed",
        "execution_adapter_used": "isolated_python",
        "container_telemetry": telemetry,
        "operational_metrics": operational,
        "script_exit_code": child.returncode,
    }
    finalize_runtime_measurement(
        runtime, adapter_requested="oci", contract_requested=False
    )
    attach_runtime(
        report,
        runtime,
        mode="isolated_python",
        source="uploaded_script",
        scope="isolated_script_process",
    )
    report.setdefault("warnings", []).append(
        {
            "code": "SCRIPT_OUTPUT_NOT_VERIFIED",
            "message": "Process completion is not useful-work verification or orchestrator execution.",
        }
    )
    return report


def attach_runtime(report, runtime, *, mode, source, scope):
    report["energy_evaluation"].update(enabled=True, mode_requested=mode)
    energy = report["energy"]
    energy.update(
        mode_used=mode,
        data_source=source,
        runtime_measurement=runtime,
    )
    energy["measurements"]["runtime"] = runtime
    energy["evidence"].update(
        measurement_kind="runtime_attempt",
        measurement_scope=scope,
        execution_adapter=runtime.get("execution_adapter_used"),
    )


def main():
    if len(sys.argv) == 3 and sys.argv[1] == "--probe":
        profile = sys.argv[2]
        ready = isolated() and profile in (
            "static",
            "python",
            "airflow",
            "prefect",
            "dagster",
        )
        if ready and profile not in ("static", "python"):
            from pisces.energy.oci_worker import _probe

            ready = (
                os.environ.get("PISCES_RUNTIME_TARGET", "airflow") == profile
                and _probe().get("status") == "ready"
            )
        print(json.dumps({"ready": ready}))
        return
    if not isolated():
        raise SystemExit("container_isolation_preflight_failed")
    raw = sys.stdin.buffer.read(MAX_JOB_BYTES + 1)
    if len(raw) > MAX_JOB_BYTES:
        raise SystemExit("request_too_large")
    request = JobSubmission.model_validate(load_report(raw))
    with (
        tempfile.TemporaryDirectory(prefix="pisces-evaluation-") as directory,
        open(os.devnull, "w") as quiet,
    ):
        with redirect_stdout(quiet), redirect_stderr(quiet):
            report = evaluate(request, Path(directory))
        print(render_report(report), flush=True)


if __name__ == "__main__":
    main()
