from __future__ import annotations

import builtins
import contextlib
import importlib.util
import pathlib
import socket
import subprocess
import time
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterator, List, Protocol


class NativeAdapterError(RuntimeError):
    pass


@dataclass
class NativeSafetyPolicy:
    enabled: bool = True
    block_network: bool = True
    block_subprocess: bool = True
    block_file_writes: bool = True


@dataclass
class NativeTaskResult:
    task_id: str
    status: str
    error_type: str | None = None
    message: str | None = None

    def to_dict(self) -> Dict[str, Any]:
        out = {"task_id": self.task_id, "status": self.status}
        if self.error_type:
            out["error_type"] = self.error_type
        if self.message:
            out["message"] = self.message
        return out


@dataclass
class NativeExecutionResult:
    adapter: str
    tasks_exercised: int
    native_tasks_executed: int
    native_tasks_skipped: int
    duration_s: float
    task_results: List[NativeTaskResult] = field(default_factory=list)
    safety: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "tasks_exercised": int(self.tasks_exercised),
            "native_tasks_executed": int(self.native_tasks_executed),
            "native_tasks_skipped": int(self.native_tasks_skipped),
            "native_adapter": self.adapter,
            "native_task_results": [r.to_dict() for r in self.task_results],
            "native_safety": self.safety,
            "native_metadata": self.metadata,
            "duration_s": round(float(self.duration_s), 6),
        }


class NativeAdapter(Protocol):
    name: str

    def execute(
        self,
        *,
        workflow_file: Path,
        max_tasks: int,
        safety_policy: NativeSafetyPolicy,
    ) -> NativeExecutionResult:
        ...


def load_module_from_file(workflow_file: Path, *, module_name: str) -> Any:
    spec = importlib.util.spec_from_file_location(module_name, str(workflow_file))
    if spec is None or spec.loader is None:
        raise NativeAdapterError("unable_to_create_module_spec")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _blocked_operation(name: str, blocked: List[Dict[str, Any]]):
    def _raise(*args: Any, **kwargs: Any) -> Any:
        blocked.append({"operation": name})
        raise NativeAdapterError(f"native_safety_blocked:{name}")

    return _raise


def _safe_open(original_open, blocked: List[Dict[str, Any]]):
    def _open(file, mode="r", *args: Any, **kwargs: Any) -> Any:
        mode_s = str(mode or "r")
        if any(flag in mode_s for flag in ("w", "a", "x", "+")):
            blocked.append({"operation": "file_write", "path": str(file), "mode": mode_s})
            raise NativeAdapterError("native_safety_blocked:file_write")
        return original_open(file, mode, *args, **kwargs)

    return _open


def _safe_path_open(original_path_open, blocked: List[Dict[str, Any]]):
    def _path_open(self, mode="r", *args: Any, **kwargs: Any) -> Any:
        mode_s = str(mode or "r")
        if any(flag in mode_s for flag in ("w", "a", "x", "+")):
            blocked.append({"operation": "file_write", "path": str(self), "mode": mode_s})
            raise NativeAdapterError("native_safety_blocked:file_write")
        return original_path_open(self, mode, *args, **kwargs)

    return _path_open


@contextlib.contextmanager
def native_safety_guard(policy: NativeSafetyPolicy) -> Iterator[Dict[str, Any]]:
    blocked: List[Dict[str, Any]] = []
    if not policy.enabled:
        yield {
            "enabled": False,
            "blocked_operations": blocked,
            "policy": policy.__dict__,
        }
        return

    original_open = builtins.open
    original_path_open = pathlib.Path.open
    original_create_connection = socket.create_connection
    original_connect = socket.socket.connect
    original_urlopen = urllib.request.urlopen
    original_popen = subprocess.Popen
    original_run = subprocess.run
    original_call = subprocess.call
    original_check_call = subprocess.check_call
    original_check_output = subprocess.check_output
    requests_session_request = None
    original_requests_request = None

    try:
        if policy.block_file_writes:
            builtins.open = _safe_open(original_open, blocked)  # type: ignore[assignment]
            pathlib.Path.open = _safe_path_open(original_path_open, blocked)  # type: ignore[assignment]
        if policy.block_network:
            socket.create_connection = _blocked_operation("network", blocked)  # type: ignore[assignment]
            socket.socket.connect = _blocked_operation("network", blocked)  # type: ignore[assignment]
            urllib.request.urlopen = _blocked_operation("network", blocked)  # type: ignore[assignment]
            try:
                import requests

                requests_session_request = requests.sessions.Session.request
                original_requests_request = requests.api.request
                requests.sessions.Session.request = _blocked_operation("network", blocked)  # type: ignore[assignment]
                requests.api.request = _blocked_operation("network", blocked)  # type: ignore[assignment]
            except Exception:
                pass
        if policy.block_subprocess:
            blocked_subprocess = _blocked_operation("subprocess", blocked)
            subprocess.Popen = blocked_subprocess  # type: ignore[assignment]
            subprocess.run = blocked_subprocess  # type: ignore[assignment]
            subprocess.call = blocked_subprocess  # type: ignore[assignment]
            subprocess.check_call = blocked_subprocess  # type: ignore[assignment]
            subprocess.check_output = blocked_subprocess  # type: ignore[assignment]

        yield {
            "enabled": True,
            "blocked_operations": blocked,
            "policy": policy.__dict__,
        }
    finally:
        builtins.open = original_open  # type: ignore[assignment]
        pathlib.Path.open = original_path_open  # type: ignore[assignment]
        socket.create_connection = original_create_connection  # type: ignore[assignment]
        socket.socket.connect = original_connect  # type: ignore[assignment]
        urllib.request.urlopen = original_urlopen  # type: ignore[assignment]
        subprocess.Popen = original_popen  # type: ignore[assignment]
        subprocess.run = original_run  # type: ignore[assignment]
        subprocess.call = original_call  # type: ignore[assignment]
        subprocess.check_call = original_check_call  # type: ignore[assignment]
        subprocess.check_output = original_check_output  # type: ignore[assignment]
        if requests_session_request is not None and original_requests_request is not None:
            try:
                import requests

                requests.sessions.Session.request = requests_session_request  # type: ignore[assignment]
                requests.api.request = original_requests_request  # type: ignore[assignment]
            except Exception:
                pass


def finish_result(
    *,
    adapter: str,
    started_at: float,
    task_results: List[NativeTaskResult],
    safety: Dict[str, Any],
    metadata: Dict[str, Any] | None = None,
) -> NativeExecutionResult:
    executed = sum(1 for r in task_results if r.status == "executed")
    skipped = sum(1 for r in task_results if r.status != "executed")
    return NativeExecutionResult(
        adapter=adapter,
        tasks_exercised=len(task_results),
        native_tasks_executed=executed,
        native_tasks_skipped=skipped,
        duration_s=max(0.0, time.time() - started_at),
        task_results=task_results,
        safety=safety,
        metadata=metadata or {},
    )


def import_safety_policy(policy: NativeSafetyPolicy) -> NativeSafetyPolicy:
    return NativeSafetyPolicy(
        enabled=policy.enabled,
        block_network=policy.block_network,
        block_subprocess=policy.block_subprocess,
        block_file_writes=False,
    )


def combine_safety(
    *,
    policy: NativeSafetyPolicy,
    import_safety: Dict[str, Any],
    execution_safety: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "enabled": bool(policy.enabled),
        "policy": policy.__dict__,
        "import_policy": import_safety.get("policy"),
        "execution_policy": execution_safety.get("policy"),
        "blocked_operations": (
            list(import_safety.get("blocked_operations") or [])
            + list(execution_safety.get("blocked_operations") or [])
        ),
        "phases": {
            "import": import_safety,
            "execution": execution_safety,
        },
    }
