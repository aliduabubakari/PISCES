"""Docker-backed whole-workflow execution for approved Airflow v2 bundles."""

from __future__ import annotations

from contextlib import contextmanager
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import tempfile
import time
from typing import Any, Iterator
from uuid import uuid4

from .output_equivalence import ORACLE_VERSION
from .runtime_profiles import PROFILES
from .workload_contract import LoadedWorkloadContract, confined_path, sha256_file


_IMAGE = re.compile(r"(?:sha256:[0-9a-f]{64}|[a-zA-Z0-9][a-zA-Z0-9_.:/-]*@sha256:[0-9a-f]{64})")
_ENTRYPOINT = "/usr/local/bin/python"
_FLAGS = (
    "--pull=never", "--network=none", "--cgroupns=private", "--user=50000:50000",
    "--read-only", "--cap-drop=ALL", "--security-opt=no-new-privileges",
    "--cpus=2", "--memory=2g", "--memory-swap=2g", "--pids-limit=128",
    "--tmpfs=/tmp:rw,noexec,nosuid,nodev,size=256m,mode=1777",
    "--no-healthcheck", "--restart=no", "--log-driver=none",
    "--env=HOME=/tmp", "--env=TMPDIR=/tmp", "--env=PYTHONUNBUFFERED=1",
    "--entrypoint=" + _ENTRYPOINT,
)


class OCIError(RuntimeError):
    pass


def _call(command: list[str], *, timeout: float = 15) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, capture_output=True, text=True, timeout=timeout, check=False)


def _checked(command: list[str], reason: str) -> str:
    result = _call(command)
    if result.returncode:
        raise OCIError(reason)
    return result.stdout.strip()


@contextmanager
def _local_engine() -> Iterator[list[str]]:
    docker = shutil.which("docker")
    if not docker:
        raise OCIError("docker_cli_unavailable")
    endpoint = os.environ.get("DOCKER_HOST")
    if not endpoint or os.environ.get("DOCKER_CONTEXT"):
        endpoint = _checked([docker, "context", "inspect", "--format", "{{.Endpoints.docker.Host}}"], "docker_context_unavailable")
    if not endpoint.startswith("unix:///"):
        raise OCIError("local_unix_docker_endpoint_required")
    # An empty client config prevents Docker from injecting configured proxy credentials.
    with tempfile.TemporaryDirectory(prefix="pisces-docker-config-") as config:
        yield [
            "/usr/bin/env", "-u", "DOCKER_CONTEXT", "-u", "DOCKER_HOST",
            "-u", "DOCKER_TLS_VERIFY", "-u", "DOCKER_CERT_PATH", "-u", "DOCKER_TLS",
            docker, "--config", config, "--host", endpoint,
        ]


def _inspect_engine(cli: list[str], image: str) -> dict[str, Any]:
    if not _IMAGE.fullmatch(image):
        raise OCIError("immutable_image_id_or_digest_required")
    info = json.loads(_checked([*cli, "info", "--format", "{{json .}}"], "docker_engine_unavailable"))
    if info.get("OSType") != "linux" or str(info.get("CgroupVersion")) != "2":
        raise OCIError("linux_cgroup_v2_required")
    if not all(info.get(key) for key in ("MemoryLimit", "SwapLimit", "CpuCfsQuota", "PidsLimit")):
        raise OCIError("required_container_limits_unavailable")
    if not any(option.startswith("name=seccomp,") for option in info.get("SecurityOptions") or []):
        raise OCIError("docker_seccomp_required")
    config = json.loads(_checked([*cli, "image", "inspect", image], "pinned_image_not_local"))[0]
    image_id = config.get("Id")
    if not isinstance(image_id, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", image_id):
        raise OCIError("immutable_image_id_unavailable")
    if (config.get("Config") or {}).get("Volumes") or config.get("Os") != "linux":
        raise OCIError("unsupported_image_volumes_or_os")
    target = "airflow"
    for item in (config.get("Config") or {}).get("Env") or []:
        if item.startswith("PISCES_RUNTIME_TARGET="):
            target = item.partition("=")[2]
    if target not in PROFILES:
        raise OCIError("unsupported_image_runtime_target")
    return {
        "image_id": image_id, "image_reference": image, "image_architecture": config.get("Architecture"),
        "engine_version": info.get("ServerVersion"), "kernel_version": info.get("KernelVersion"),
        "runtime_target": target,
        "engine_platform": info.get("OperatingSystem"), "cgroup_version": "2",
        "environment_class": "docker_desktop_vm" if "Docker Desktop" in (info.get("OperatingSystem") or "") else "local_linux_engine",
        "limits": {"cpu_cores": 2, "memory_bytes": 2**31, "pids": 128, "scratch_bytes": 2**28},
        "isolation": {"network": "none", "user": "50000:50000", "root_filesystem": "read_only",
                      "input_mount": "read_only", "capabilities": "none", "no_new_privileges": True},
    }


def _create_command(
    cli: list[str], name: str, image: str, inputs: Path | None = None, *, telemetry_case: str | None = None,
    memory_case: str | None = None,
) -> list[str]:
    flags = _FLAGS
    if memory_case is not None:
        from .memory_check import ALLOCATIONS, OOM_LIMIT

        if inputs is not None or telemetry_case is not None or memory_case not in (*map(str, ALLOCATIONS), "oom"):
            raise OCIError("unsupported_memory_fixture")
        if memory_case == "oom":
            flags = tuple(f"{flag.split('=')[0]}={OOM_LIMIT}" if flag.startswith(("--memory=", "--memory-swap="))
                          else flag for flag in flags)
    command = [*cli, "create", "--name", name, *flags]
    if inputs is not None:
        if "," in str(inputs):
            raise OCIError("unsupported_input_mount_path")
        command += ["--mount", f"type=bind,source={inputs},target=/input,readonly"]
    worker_args = ["--telemetry-check", telemetry_case] if telemetry_case is not None else (
        ["--memory-check", memory_case] if memory_case is not None else ["--probe"] if inputs is None else []
    )
    return [*command, image, "-I", "-m", "pisces.energy.oci_worker", *worker_args]


def _execute(
    cli: list[str], image: str, inputs: Path | None, timeout_s: float, *, telemetry_case: str | None = None,
    memory_case: str | None = None,
) -> dict[str, Any]:
    name = f"pisces-oci-{uuid4().hex}"
    started = time.perf_counter()
    result: dict[str, Any] = {"container_name": name}
    try:
        try:
            _checked(_create_command(cli, name, image, inputs, telemetry_case=telemetry_case, memory_case=memory_case),
                     "container_create_failed")
        except (OCIError, OSError, subprocess.TimeoutExpired) as exc:
            result["creation_error"] = str(exc) if isinstance(exc, OCIError) else "container_create_failed"
            return result
        result["setup_wall_s"] = time.perf_counter() - started
        running = time.perf_counter()
        try:
            attached = _call([*cli, "start", "--attach", name], timeout=timeout_s)
            result["attach_returncode"] = attached.returncode
            try:
                result["payload"] = json.loads(attached.stdout)
            except ValueError:
                result["payload"] = None
        except subprocess.TimeoutExpired:
            result["timed_out"] = True
        except (OSError, UnicodeError):
            result["attach_error"] = "container_attach_unavailable"
        result["container_wall_s"] = time.perf_counter() - running
        try:
            state = json.loads(_checked([*cli, "inspect", "--format", "{{json .State}}", name], "container_state_unavailable"))
            if not isinstance(state, dict) or type(state.get("ExitCode")) is not int or not all(
                type(state.get(key)) is bool for key in ("OOMKilled", "Running")
            ):
                raise ValueError("Invalid container state")
            result.update(exit_code=state["ExitCode"], oom_killed=state["OOMKilled"], running_at_inspection=state["Running"])
        except (OCIError, OSError, ValueError, subprocess.TimeoutExpired):
            result["inspection_error"] = "container_state_unavailable"
    finally:
        cleanup = time.perf_counter()
        try:
            removed = _call([*cli, "rm", "--force", name])
            if removed.returncode and "No such container" not in removed.stderr:
                result["cleanup_error"] = "container_cleanup_failed"
        except (OSError, UnicodeError, subprocess.TimeoutExpired):
            result["cleanup_error"] = "container_cleanup_failed"
        result["cleanup_wall_s"] = time.perf_counter() - cleanup
    return result


def _failure(reason: str, *, environment: dict[str, Any] | None = None) -> dict[str, Any]:
    return {"ok": False, "error": {"error_type": "OCIError", "message": reason}, "runtime_measurement": {
        "status": "failed", "execution_outcome": "preflight_rejected",
        "ineligibility_reasons": [reason], "oci_environment": environment,
    }}


def check_oci(image: str) -> dict[str, Any]:
    try:
        with _local_engine() as cli:
            environment = _inspect_engine(cli, image)
            result = _execute(cli, environment["image_id"], None, 15)
            probe = result.get("payload")
            ready = (_exit_outcome(result) is None and not result.get("cleanup_error")
                     and isinstance(probe, dict) and probe.get("status") == "ready" and probe.get("cpu") is True)
            ready = ready and probe.get("runtime_target", "airflow") == environment.get("runtime_target", "airflow")
            return {"ready": ready, "environment": environment, "controllers": probe,
                    "lifecycle": {key: value for key, value in result.items() if key != "payload"},
                    "reason": None if ready else "container_capability_probe_failed"}
    except (OCIError, OSError, ValueError, subprocess.TimeoutExpired) as exc:
        return {"ready": False, "reason": str(exc) if isinstance(exc, OCIError) else "oci_probe_failed"}


def _stage(workload: LoadedWorkloadContract, root: Path, *, task_timing_id: str | None = None) -> None:
    def copy(source: Path, destination: Path, digest: str) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.exists():
            if sha256_file(destination) == digest:
                return
            raise OCIError("staged_input_collision")
        shutil.copyfile(source, destination)
        if sha256_file(destination) != digest:
            raise OCIError("staged_input_sha256_mismatch")
        destination.chmod(stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)

    contract_root, artifact_root = root / "contract", root / "artifact"
    contract = contract_root / workload.contract_path.name
    binding = artifact_root / workload.binding_path.name
    copy(workload.contract_path, contract, workload.contract_sha256)
    copy(workload.binding_path, binding, workload.binding_sha256)
    artifact = confined_path(artifact_root, workload.binding["artifact_path"])
    copy(workload.artifact_path, artifact, workload.artifact_sha256)
    for item in workload.contract["inputs"]:
        copy(workload.fixtures[item["integration_id"]], confined_path(contract_root, item["fixture"]), item["sha256"])
    for output in workload.contract["outputs"]:
        ref = output["reference"]
        copy(confined_path(workload.contract_path.parent, ref["path"]), confined_path(contract_root, ref["path"]), ref["sha256"])
    request = {
        "orchestrator": workload.binding["target"], "mode": "zero_fill", "data_source": "contract_fixtures",
        "execution_adapter": "native", "experiment_mode": True,
        "collect_modeled_energy": False, "maximum_repetitions": 1,
        "read_only_fixture_mount": True,
        "workflow_file": str(Path("/input") / artifact.relative_to(root)),
        "workload_contract": str(Path("/input") / contract.relative_to(root)),
        "artifact_binding": str(Path("/input") / binding.relative_to(root)),
    }
    if task_timing_id is not None:
        request["trusted_task_timing_id"] = task_timing_id
    (root / "request.json").write_text(json.dumps(request), encoding="utf-8")
    root.chmod(0o755)


def _exit_outcome(result: dict[str, Any]) -> str | None:
    if result.get("creation_error"):
        return "preflight_rejected"
    if result.get("timed_out"):
        return "timed_out"
    payload = result.get("payload")
    telemetry = (payload.get("container_telemetry") or {}) if isinstance(payload, dict) else {}
    if result.get("oom_killed") or (telemetry.get("oom_kill_delta") or 0) > 0:
        return "container_oom_killed"
    if result.get("inspection_error") and result.get("attach_returncode") == 0 and isinstance(payload, dict):
        return "container_state_unverified"
    if result.get("exit_code") != 0 or result.get("attach_returncode") != 0 or result.get("running_at_inspection"):
        return "container_exit_failed"
    if not isinstance(payload, dict) or payload.get("worker_returncode", 0) != 0:
        return "container_worker_failed"
    return None


def run_oci(workload: LoadedWorkloadContract, *, image: str, timeout_s: float,
            task_timing_id: str | None = None) -> dict[str, Any]:
    if workload.contract["schema_version"] != "2.0" or workload.binding["target"] not in PROFILES:
        return _failure("oci_requires_supported_contract_v2")
    if workload.binding["target"] != "airflow" and any(item["kind"] != "file" for item in workload.contract["inputs"]):
        return _failure("oci_profile_requires_file_fixtures")
    try:
        with _local_engine() as cli, tempfile.TemporaryDirectory(prefix="pisces-oci-input-") as directory:
            environment = _inspect_engine(cli, image)
            if environment.get("runtime_target", "airflow") != workload.binding["target"]:
                return _failure("oci_image_target_mismatch", environment=environment)
            inputs = Path(directory).resolve()
            stage_start = time.perf_counter()
            _stage(workload, inputs, task_timing_id=task_timing_id)
            staging_wall_s = time.perf_counter() - stage_start
            result = _execute(cli, environment["image_id"], inputs, timeout_s)
    except (OCIError, OSError, ValueError, subprocess.TimeoutExpired) as exc:
        return _failure(str(exc) if isinstance(exc, OCIError) else "oci_execution_unavailable")
    payload = result.pop("payload", None)
    result["input_staging_wall_s"] = staging_wall_s
    exit_outcome = _exit_outcome({**result, "payload": payload})
    payload = payload if isinstance(payload, dict) else {}
    measurement = payload.get("runtime_measurement")
    if not isinstance(measurement, dict):
        measurement = {}
    telemetry = payload.get("container_telemetry")
    success = not exit_outcome and payload.get("ok") is True and measurement.get("status") == "ok"
    reason = exit_outcome
    proof = measurement.get("output_assertions") or {}
    if success and not (
        measurement.get("execution_outcome") == "completed"
        and measurement.get("execution_adapter_used") == "native"
        and measurement.get("workload_contract") == workload.provenance()
        and proof.get("oracle_version") == ORACLE_VERSION
        and proof.get("passed") is True
        and all(proof.get(field) == getattr(workload, field) for field in
                ("contract_sha256", "artifact_sha256", "binding_sha256"))
        and (measurement.get("task_coverage") or {}).get("passed") is True
        and (measurement.get("input_consumption") or {}).get("passed") is True
    ):
        success = False
        reason = "container_workload_evidence_mismatch"
        measurement.update(status="failed", ineligibility_reasons=[reason])
    if exit_outcome:
        reason = result.get("creation_error") or result.get("attach_error") or exit_outcome
        measurement.update(status="failed", execution_outcome=exit_outcome, ineligibility_reasons=[reason])
    elif not measurement:
        reason = (payload.get("error") or {}).get("message") or "container_worker_failed"
        outcome = "preflight_rejected" if reason == "container_isolation_preflight_failed" else "container_worker_failed"
        measurement.update(status="failed", execution_outcome=outcome, ineligibility_reasons=[reason])
    if result.get("cleanup_error"):
        measurement["ineligibility_reasons"] = [*(measurement.get("ineligibility_reasons") or []), result["cleanup_error"]]
        measurement["eligible_for_paired_equivalence_comparison"] = False
    measurement.update(oci_environment=environment, container_lifecycle=result, container_telemetry=telemetry,
                       container_telemetry_missing_reason=None if isinstance(telemetry, dict) else (
                           "worker_snapshot_lost_to_timeout" if result.get("timed_out") else
                           "worker_snapshot_lost_to_oom" if exit_outcome == "container_oom_killed" else
                           "worker_snapshot_not_received"
                       ))
    measurement["execution_adapter_used"] = "oci" if measurement.get("execution_adapter_used") == "native" else None
    return {"ok": success, "runtime_measurement": measurement,
            "error": payload.get("error") or ({"message": reason} if reason else None)}
