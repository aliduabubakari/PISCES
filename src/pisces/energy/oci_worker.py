"""In-container entry point. Snapshot the cgroup before the container exits."""

from __future__ import annotations

import json
from importlib.metadata import PackageNotFoundError, version
import os
from pathlib import Path
import subprocess
import sys
import tempfile

from .cgroup_metrics import measure_cgroup, snapshot
from .runtime_profiles import PROFILES


def _probe(*, memory_limit: int = 2**31) -> dict:
    root = Path("/sys/fs/cgroup")
    counters = snapshot()
    target = os.environ.get("PISCES_RUNTIME_TARGET", "airflow")
    try:
        profile = PROFILES[target]
        compatible = all(version(name) == expected for name, expected in profile["distributions"].items())
        private_cgroup = Path("/proc/self/cgroup").read_text(encoding="ascii").strip() == "0::/"
        cpu_quota, cpu_period = (root / "cpu.max").read_text(encoding="ascii").split()
        limits = (
            int(cpu_quota) == 2 * int(cpu_period)
            and (root / "memory.max").read_text(encoding="ascii").strip() == str(memory_limit)
            and (root / "memory.swap.max").read_text(encoding="ascii").strip() == "0"
            and (root / "pids.max").read_text(encoding="ascii").strip() == "128"
        )
        network_isolated = all(
            interface.name == "lo" or not int((interface / "flags").read_text(encoding="ascii"), 16) & 1
            for interface in Path("/sys/class/net").iterdir() if interface.is_dir()
        )
        root_read_only = bool(os.statvfs("/").f_flag & os.ST_RDONLY)
        status = dict(line.split(":", 1) for line in Path("/proc/self/status").read_text().splitlines() if ":" in line)
        privileges_locked = (
            status.get("NoNewPrivs", "").strip() == "1"
            and status.get("Seccomp", "").strip() == "2"
            and int(status.get("CapEff", "1"), 16) == 0
        )
    except (OSError, ValueError, KeyError, PackageNotFoundError):
        return {"status": "unavailable"}
    return {
        "status": "ready" if all((compatible, private_cgroup, limits, network_isolated,
                                  root_read_only, privileges_locked, os.geteuid() == 50000)) else "unavailable",
        "cpu": counters["cpu_usage_usec"] is not None,
        "memory_peak": counters["memory_peak_bytes"] is not None,
        "block_io_device_counters_visible": bool(counters["io_by_device"]),
        "private_cgroup": private_cgroup, "limits_verified": limits,
        "network_isolated": network_isolated, "root_read_only": root_read_only,
        "privileges_locked": privileges_locked,
        **({"airflow_profile_matches": compatible} if target == "airflow" else {}),
        "runtime_target": target, "runtime_profile": profile, "profile_matches": compatible,
    }


def main() -> None:
    if len(sys.argv) == 3 and sys.argv[1] == "--memory-check":
        from .memory_check import OOM_LIMIT, observe_memory

        case = sys.argv[2]
        probe = _probe(memory_limit=OOM_LIMIT if case == "oom" else 2**31)
        payload = observe_memory(case) if probe["status"] == "ready" else {"error": "container_isolation_preflight_failed"}
        print(json.dumps(payload, allow_nan=False))
        return
    probe = _probe()
    if sys.argv[1:] == ["--probe"]:
        print(json.dumps(probe))
        return
    if len(sys.argv) == 3 and sys.argv[1] == "--telemetry-check":
        from .telemetry_check import observe_case

        payload = observe_case(sys.argv[2]) if probe["status"] == "ready" else {
            "error": "container_isolation_preflight_failed",
        }
        print(json.dumps(payload, allow_nan=False))
        return
    if sys.argv[1:]:
        raise SystemExit("Unexpected worker arguments")
    if probe["status"] != "ready" or not (os.statvfs("/input").f_flag & os.ST_RDONLY):
        print(json.dumps({"ok": False, "error": {"message": "container_isolation_preflight_failed"}}))
        return
    request = json.loads(Path("/input/request.json").read_text(encoding="utf-8"))
    if request.get("orchestrator") != probe["runtime_target"]:
        print(json.dumps({"ok": False, "error": {"message": "oci_image_target_mismatch"}}))
        return
    if probe["runtime_target"] == "prefect":
        Path("/tmp/prefect").mkdir(exist_ok=True)
        Path("/tmp/.prefect").mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="pisces-worker-") as scratch:
        output = Path(scratch) / "report.json"
        child, telemetry = measure_cgroup(lambda: subprocess.run(
            [sys.executable, "-I", "-m", "pisces.energy.runtime_runner_cli",
             "--in", "/input/request.json", "--out", str(output)],
            cwd=scratch, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            env={"HOME": "/tmp", "TMPDIR": "/tmp", "PATH": "/usr/local/bin:/usr/bin:/bin",
                 "LANG": "C.UTF-8", "PISCES_SKIP_POWERMETRICS": "1", "PISCES_OCI_TARGET": probe["runtime_target"],
                 "PREFECT_HOME": "/tmp/prefect", "PREFECT_SERVER_ANALYTICS_ENABLED": "false",
                 "PREFECT_LOGGING_TO_API_ENABLED": "false", "PREFECT_LOGGING_LEVEL": "ERROR",
                 "DAGSTER_HOME": "/tmp/dagster"},
        ))
        try:
            payload = json.loads(output.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                raise ValueError("Invalid worker report")
        except (OSError, ValueError):
            payload = {"ok": False, "error": {"message": "container_worker_report_unavailable"}}
        if child.returncode:
            payload["ok"] = False
        payload.update(container_telemetry=telemetry, worker_returncode=child.returncode)
        print(json.dumps(payload, allow_nan=False))


if __name__ == "__main__":
    main()
