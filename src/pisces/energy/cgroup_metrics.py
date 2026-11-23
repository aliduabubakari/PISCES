"""Software counters for an isolated container's private cgroup v2 root."""

from __future__ import annotations

import math
from pathlib import Path
import time
from typing import Any, Callable, TypeVar


_T = TypeVar("_T")
_INVALID = {"malformed_counter", "counter_decreased"}


def _text(root: Path, name: str) -> tuple[str, str | None]:
    try:
        raw = (root / name).read_text(encoding="ascii").strip()
    except FileNotFoundError:
        return "", "counter_file_missing"
    except UnicodeError:
        return "", "malformed_counter"
    except OSError:
        return "", "counter_file_unreadable"
    return raw, None if raw else "counter_file_empty"


def _count(value: str) -> int | None:
    return int(value) if len(value) <= 20 and value.isascii() and value.isdecimal() else None


def _fields(text: str) -> dict[str, int | None]:
    result = {}
    for line in text.splitlines():
        parts = line.split()
        if parts:
            key = parts[0]
            result[key] = _count(parts[1]) if len(parts) == 2 and key not in result else None
    return result


def _field_issue(fields: dict, key: str, read_issue: str | None) -> str | None:
    return read_issue or ("counter_field_missing" if key not in fields else
                          "malformed_counter" if fields[key] is None else None)


def _cpu_snapshot(root: Path) -> tuple[dict[str, int | None], dict[str, Any], str | None]:
    start_ns = time.perf_counter_ns()
    raw, issue = _text(root, "cpu.stat")
    end_ns = time.perf_counter_ns()
    cpu = _fields(raw)
    return cpu, {"read_started_ns": start_ns, "read_finished_ns": end_ns,
                 "usage_usec": cpu.get("usage_usec")}, issue


def snapshot(root: Path = Path("/sys/fs/cgroup"), *, cpu_first: bool = True) -> dict[str, Any]:
    if cpu_first:
        cpu, cpu_sample, cpu_issue = _cpu_snapshot(root)
    devices = {}
    io_raw, io_issue = _text(root, "io.stat")
    for line in io_raw.splitlines():
        fields = line.split()
        if not fields:
            continue
        device, *pairs = fields
        major, _, minor = device.partition(":")
        if _count(major) is None or _count(minor) is None or device in devices or not pairs:
            io_issue = "malformed_counter"
            break
        devices[device] = {}
        for pair in pairs:
            key, _, raw = pair.partition("=")
            devices[device][key] = None if key in devices[device] else _count(raw)
    memory_raw, memory_issue = _text(root, "memory.peak")
    memory_peak = _count(memory_raw)
    events_raw, events_issue = _text(root, "memory.events")
    events = _fields(events_raw)
    if not cpu_first:
        cpu, cpu_sample, cpu_issue = _cpu_snapshot(root)
    return {
        "cpu_usage_usec": cpu.get("usage_usec"),
        "cpu_throttled_usec": cpu.get("throttled_usec"),
        "memory_peak_bytes": memory_peak,
        "oom_kill": events.get("oom_kill"),
        "io_by_device": devices,
        "cpu_sample": cpu_sample,
        "issues": {
            "cpu_usage_usec": _field_issue(cpu, "usage_usec", cpu_issue),
            "cpu_throttled_usec": _field_issue(cpu, "throttled_usec", cpu_issue),
            "memory_peak_bytes": memory_issue or ("malformed_counter" if memory_peak is None else None),
            "oom_kill": _field_issue(events, "oom_kill", events_issue),
            "io_by_device": io_issue,
        },
    }


def _quality(reasons: dict[str, str]) -> dict[str, Any]:
    status = "invalid" if _INVALID.intersection(reasons.values()) else "unavailable" if reasons else "available"
    return {"status": status, "reasons": reasons}


def _delta(before: int | None, after: int | None, reasons: dict[str, str]) -> int | None:
    if reasons:
        return None
    if before is None or after is None:
        reasons["delta"] = "counter_field_missing"
        return None
    if after < before:
        reasons["delta"] = "counter_decreased"
        return None
    return after - before


def summarize(before: dict[str, Any], after: dict[str, Any], *, wall_s: float) -> dict[str, Any]:
    def endpoint_issues(key: str) -> dict[str, str]:
        return {label: sample["issues"][key] for label, sample in (("before", before), ("after", after))
                if sample["issues"][key] is not None}

    quality = {"wall_clock_s": _quality({} if math.isfinite(wall_s) and wall_s >= 0 else {"sample": "malformed_counter"})}
    values = {"wall_clock_s": wall_s if quality["wall_clock_s"]["status"] == "available" else None}
    for metric, key in (("cpu_s", "cpu_usage_usec"), ("cpu_throttled_usec", "cpu_throttled_usec"),
                        ("oom_kill_delta", "oom_kill")):
        reasons = endpoint_issues(key)
        value = _delta(before[key], after[key], reasons)
        values[metric] = value / 1_000_000 if metric == "cpu_s" and value is not None else value
        quality[metric] = _quality(reasons)
    memory_issue = after["issues"]["memory_peak_bytes"]
    quality["memory_peak_bytes"] = _quality({"after": memory_issue} if memory_issue else {})
    values["memory_peak_bytes"] = after["memory_peak_bytes"]
    initial_io, final_io = before["io_by_device"], after["io_by_device"]
    for metric, field in (("block_read_bytes", "rbytes"), ("block_write_bytes", "wbytes")):
        reasons = endpoint_issues("io_by_device")
        if not reasons and initial_io.keys() != final_io.keys():
            reasons["delta"] = "device_set_changed"
        total = 0
        if not reasons:
            for device, initial in initial_io.items():
                final = final_io[device]
                device_issues = {label: issue for label, data in (("before", initial), ("after", final))
                                 if (issue := _field_issue(data, field, None))}
                value = _delta(initial.get(field), final.get(field), device_issues)
                total += value if value is not None else 0
                reasons.update({f"{label}:{device}": issue for label, issue in device_issues.items()})
        values[metric] = None if reasons else total
        quality[metric] = _quality(reasons)
    return {
        "scope": "container_worker_window_including_descendants_setup_and_validation",
        "memory_peak_scope": "container_cgroup_lifetime_including_page_cache",
        **values,
        "metric_quality": quality,
    }


def measure_cgroup(operation: Callable[[], _T], root: Path = Path("/sys/fs/cgroup")) -> tuple[_T, dict[str, Any]]:
    # Read CPU closest to the operation; other counter reads are outside its bracket.
    before = snapshot(root, cpu_first=False)
    start_ns = time.perf_counter_ns()
    result = operation()
    end_ns = time.perf_counter_ns()
    after = snapshot(root)
    telemetry = summarize(before, after, wall_s=(end_ns - start_ns) / 1e9)
    telemetry["counter_window"] = {
        "clock": "perf_counter_ns",
        "wall_started_ns": start_ns,
        "wall_finished_ns": end_ns,
        "cpu_source": "cgroup_v2:cpu.stat:usage_usec",
        "cpu_before": before["cpu_sample"],
        "cpu_after": after["cpu_sample"],
    }
    return result, telemetry
