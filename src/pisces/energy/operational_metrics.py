"""Process-level operational measurements for bounded PISCES evaluations.

These counters are available on macOS without privileged hardware access.  They
are the primary operational evidence in PISCES reports:

* monotonic wall-clock time;
* process CPU time;
* peak resident memory for the dedicated runner process.

Peak resident memory is a process-lifetime high-water mark.  PISCES executes
each measured phase in a dedicated subprocess so that this scope remains clear.
"""

from __future__ import annotations

from dataclasses import dataclass
import platform
import time
from typing import Any, Dict, Optional

try:
    import resource
except ImportError:  # pragma: no cover - resource is available on macOS/Linux
    resource = None  # type: ignore[assignment]


def _peak_rss_mb() -> Optional[float]:
    if resource is None:
        return None
    try:
        raw = float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    except Exception:
        return None

    # macOS reports bytes; Linux and the BSDs commonly report KiB.
    if platform.system() == "Darwin":
        return raw / (1024.0 * 1024.0)
    return raw / 1024.0


@dataclass(frozen=True)
class OperationalMeasurement:
    wall_started: float
    cpu_started: float
    peak_rss_started_mb: Optional[float]


def start_operational_measurement() -> OperationalMeasurement:
    return OperationalMeasurement(
        wall_started=time.perf_counter(),
        cpu_started=time.process_time(),
        peak_rss_started_mb=_peak_rss_mb(),
    )


def stop_operational_measurement(
    measurement: OperationalMeasurement,
    *,
    scope: str,
) -> Dict[str, Any]:
    wall_clock_s = max(0.0, time.perf_counter() - measurement.wall_started)
    process_cpu_s = max(0.0, time.process_time() - measurement.cpu_started)
    peak_rss_mb = _peak_rss_mb()
    peak_delta_mb: Optional[float] = None
    if peak_rss_mb is not None and measurement.peak_rss_started_mb is not None:
        peak_delta_mb = max(0.0, peak_rss_mb - measurement.peak_rss_started_mb)

    return {
        "status": "ok",
        "evidence_role": "primary",
        "measurement_scope": scope,
        "wall_clock_s": round(wall_clock_s, 9),
        "process_cpu_s": round(process_cpu_s, 9),
        "process_cpu_to_wall_ratio": (
            round(process_cpu_s / wall_clock_s, 6) if wall_clock_s > 0 else None
        ),
        "peak_rss_mb": round(peak_rss_mb, 6) if peak_rss_mb is not None else None,
        "peak_rss_delta_mb": (
            round(peak_delta_mb, 6) if peak_delta_mb is not None else None
        ),
        "peak_rss_scope": "dedicated_runner_process_lifetime",
        "clock_sources": {
            "wall_clock": "time.perf_counter",
            "process_cpu": "time.process_time",
            "peak_memory": "resource.getrusage(RUSAGE_SELF).ru_maxrss",
        },
        "platform": platform.platform(),
    }
