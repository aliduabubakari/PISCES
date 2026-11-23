"""Docker control only: immutable images, bounded output, cancellation and cleanup."""

from __future__ import annotations

import json
import re
import selectors
import subprocess
import threading
import time

from pisces.energy.oci_runtime import _FLAGS, _call
from pisces.report_validation import validate_report
from pisces.report_views import MAX_REPORT_BYTES, load_report


class DockerExecutor:
    def __init__(self, cli, deployment: str, images: dict[str, str]):
        self.cli, self.deployment = cli, deployment
        info = json.loads(_call([*cli, "info", "--format", "{{json .}}"]).stdout)
        platform = info.get("OperatingSystem") or ""
        self.environment = {
            "environment_class": (
                "docker_desktop_vm"
                if "Docker Desktop" in platform
                else "local_linux_engine"
                if info.get("OSType") == "linux"
                else "unknown"
            ),
            "engine_version": info.get("ServerVersion"),
            "kernel_version": info.get("KernelVersion"),
            "cgroup_version": str(info.get("CgroupVersion", "unknown")),
        }
        self.images = {}
        self.profiles = []
        for profile, reference in images.items():
            try:
                result = _call([*cli, "image", "inspect", reference])
                image = json.loads(result.stdout)[0]
                identifier = image["Id"]
                config = image.get("Config") or {}
                if (
                    not re.fullmatch(r"sha256:[a-f0-9]{64}", identifier)
                    or image.get("Os") != "linux"
                    or config.get("Volumes")
                    or (config.get("Labels") or {}).get("io.pisces.sandbox") != "1"
                ):
                    raise ValueError("unapproved_image")
                probe = self.probe(identifier, profile)
                if not probe:
                    raise ValueError("isolation_probe_failed")
                self.images[profile] = identifier
                self.profiles.append(
                    {
                        "name": profile,
                        "ready": True,
                        "image_id": identifier,
                        "reason": None,
                    }
                )
            except (ValueError, KeyError, OSError, subprocess.TimeoutExpired):
                self.profiles.append(
                    {
                        "name": profile,
                        "ready": False,
                        "image_id": None,
                        "reason": "approved_local_image_or_isolation_probe_unavailable",
                    }
                )

    def command(self, name, image, *, probe_profile=None):
        return [
            *self.cli,
            "create",
            "--interactive",
            "--name",
            name,
            *_FLAGS,
            "--label=io.pisces.execution=1",
            f"--label=io.pisces.deployment={self.deployment}",
            image,
            "-I",
            "-m",
            "pisces.service.sandbox",
            *(["--probe", probe_profile] if probe_profile else []),
        ]

    def probe(self, image, profile):
        from uuid import uuid4

        name = "pisces-probe-" + uuid4().hex
        try:
            if _call(self.command(name, image, probe_profile=profile)).returncode:
                return False
            result = _call([*self.cli, "start", "--attach", name], timeout=15)
            return (
                result.returncode == 0
                and json.loads(result.stdout).get("ready") is True
            )
        finally:
            removed = _call([*self.cli, "rm", "--force", name])
            if removed.returncode and "No such container" not in removed.stderr:
                raise RuntimeError("container_cleanup_failed")

    def recover(self):
        result = _call(
            [
                *self.cli,
                "ps",
                "--all",
                "--quiet",
                "--no-trunc",
                "--filter",
                f"label=io.pisces.deployment={self.deployment}",
            ]
        )
        if result.returncode:
            raise RuntimeError("cannot_check_orphan_containers")
        for identity in result.stdout.split():
            if (
                not re.fullmatch(r"[a-f0-9]{64}", identity)
                or _call([*self.cli, "rm", "--force", identity]).returncode
            ):
                raise RuntimeError("orphan_cleanup_failed")

    def execute(self, job, store):
        request = json.loads(job["request"])
        image = self.images.get(job["profile"])
        if not image:
            store.finish(job["id"], state="failed", error="profile_not_ready")
            return
        name = "pisces-job-" + job["id"]
        state, error, report = "failed", "container_execution_failed", None
        process = None
        cleanup_failed = False
        try:
            if _call(self.command(name, image)).returncode:
                raise ValueError("container_create_failed")
            process = subprocess.Popen(
                [*self.cli, "start", "--attach", "--interactive", name],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )

            def send():
                try:
                    process.stdin.write(json.dumps(request).encode())
                    process.stdin.close()
                except (BrokenPipeError, OSError, ValueError):
                    pass

            sender = threading.Thread(target=send, daemon=True)
            sender.start()
            deadline = time.monotonic() + request["timeout_s"]
            output = bytearray()
            with selectors.DefaultSelector() as selector:
                selector.register(process.stdout, selectors.EVENT_READ)
                while selector.get_map():
                    store.heartbeat(self.profiles)
                    if (store.get(job["id"]) or {}).get("cancel_requested"):
                        state, error = "cancelled", None
                        break
                    if time.monotonic() >= deadline:
                        state, error = "timed_out", "worker_snapshot_lost_to_timeout"
                        break
                    for key, _ in selector.select(timeout=0.25):
                        chunk = key.fileobj.read1(16384)
                        if not chunk:
                            selector.unregister(key.fileobj)
                        else:
                            output.extend(chunk)
                    if len(output) > MAX_REPORT_BYTES:
                        error = "container_report_limit_exceeded"
                        break
                else:
                    process.wait(timeout=5)
                    inspected = _call(
                        [*self.cli, "inspect", "--format", "{{json .State}}", name]
                    )
                    actual = json.loads(inspected.stdout)
                    if actual.get("OOMKilled"):
                        error = "container_oom_killed"
                    elif (
                        process.returncode == 0
                        and actual.get("ExitCode") == 0
                        and actual.get("Running") is False
                    ):
                        report = load_report(bytes(output))
                        if (
                            validate_report(report).status != "current_inventory"
                            or report["metadata"]["artifact_sha256"]
                            != job["artifact_sha256"]
                        ):
                            raise ValueError("invalid_or_unbound_container_report")
                        report["metadata"]["service_execution"] = {
                            "job_id": job["id"],
                            "image_id": image,
                            "environment": "isolated_linux_container",
                            "engine": self.environment,
                            "attested": False,
                        }
                        state, error = "completed", None
        except (ValueError, OSError, KeyError, subprocess.TimeoutExpired):
            error = "container_report_or_execution_failed"
        finally:
            try:
                result = _call([*self.cli, "rm", "--force", name])
                cleanup_failed = bool(
                    result.returncode and "No such container" not in result.stderr
                )
            except (OSError, subprocess.TimeoutExpired):
                cleanup_failed = True
            if process:
                if process.poll() is None:
                    process.kill()
                process.wait(timeout=5)
                if process.stdout:
                    process.stdout.close()
                if process.stdin:
                    process.stdin.close()
            if cleanup_failed:
                state, error, report = "failed", "container_cleanup_failed", None
            store.finish(job["id"], state=state, error=error, report=report)
            if cleanup_failed:
                raise RuntimeError("Refusing more work after failed container cleanup")
