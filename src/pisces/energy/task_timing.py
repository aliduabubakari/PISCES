"""Read the opt-in timing record from an approved, instrumented workload."""

from __future__ import annotations

import json
import os
from pathlib import Path
import stat


FILENAME = ".pisces-task-timing.json"
SOURCE = "trusted_benchmark_task_instrumentation_v1"
MAX_RECORD_BYTES = 1024


def read_task_timing(workspace: Path, *, task_id: str, block_start_ns: int, block_end_ns: int) -> dict:
    result = {"source": SOURCE, "scope": "task_callable_body", "task_id": task_id,
              "wall_clock": "perf_counter_ns", "cpu_clock": "thread_time_ns"}
    try:
        fd = os.open(workspace / FILENAME, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
        with os.fdopen(fd, "rb") as stream:
            if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                return {**result, "status": "invalid", "reason": "not_a_regular_file"}
            raw = stream.read(MAX_RECORD_BYTES + 1)
    except FileNotFoundError:
        return {**result, "status": "unavailable", "reason": "record_missing"}
    except OSError:
        return {**result, "status": "invalid", "reason": "record_unreadable"}
    try:
        if len(raw) > MAX_RECORD_BYTES:
            raise ValueError
        record = json.loads(raw)
        if not isinstance(record, dict) or record.get("task_id") != task_id:
            raise ValueError
        fields = ("wall_start_ns", "wall_end_ns", "thread_cpu_start_ns", "thread_cpu_end_ns")
        stamps = {field: record[field] for field in fields}
        if not all(type(value) is int and 0 <= value < 2**63 for value in stamps.values()):
            raise ValueError
    except (ValueError, KeyError, RecursionError):
        return {**result, "status": "invalid", "reason": "record_malformed"}
    start, end, cpu_start, cpu_end = stamps.values()
    if not block_start_ns <= start < end <= block_end_ns:
        return {**result, "status": "invalid", "reason": "outside_execution_window"}
    if not 0 < cpu_end - cpu_start <= end - start:
        return {**result, "status": "invalid", "reason": "invalid_thread_cpu_interval"}
    return {**result, "status": "available", "reason": None, **stamps,
            "execution_wall_bounds_ns": [block_start_ns, block_end_ns],
            "wall_clock_s": (end - start) / 1e9, "thread_cpu_s": (cpu_end - cpu_start) / 1e9}
