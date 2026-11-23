"""Python-owned metric definitions. Values remain linked to their original fields."""

from __future__ import annotations

from dataclasses import dataclass
from math import isfinite


METRIC_REGISTRY_VERSION = "1.0.0"


@dataclass(frozen=True)
class MetricDefinition:
    id: str
    label: str
    field: str
    unit: str
    raw_unit: str
    definition: str
    source: str
    includes: str
    divisor: float = 1


WORKER = (
    MetricDefinition(
        "worker.wall",
        "Wall time",
        "wall_clock_s",
        "s",
        "s",
        "Elapsed monotonic time across the container worker window.",
        "time.perf_counter_ns",
        "Runner startup, setup, execution, verification and descendants; not controller staging or cleanup.",
    ),
    MetricDefinition(
        "worker.cpu",
        "Cgroup CPU",
        "cpu_s",
        "CPU-s",
        "s",
        "Accumulated CPU time, not elapsed time or electricity.",
        "cgroup v2 cpu.stat:usage_usec",
        "All processes and descendants in the container worker window.",
    ),
    MetricDefinition(
        "worker.memory",
        "Peak memory",
        "memory_peak_bytes",
        "MiB",
        "bytes",
        "Container lifetime high-water mark, not additive memory consumption.",
        "cgroup v2 memory.peak",
        "Container cgroup lifetime, including descendants and charged page cache; not process RSS.",
        1048576,
    ),
    MetricDefinition(
        "worker.read",
        "Block read bytes",
        "block_read_bytes",
        "bytes",
        "bytes",
        "Block-device reads between valid counter endpoints; not logical input-file bytes.",
        "cgroup v2 io.stat",
        "Block I/O charged to this cgroup during the worker window.",
    ),
    MetricDefinition(
        "worker.write",
        "Block write bytes",
        "block_write_bytes",
        "bytes",
        "bytes",
        "Block-device writes between valid counter endpoints; not logical output-file bytes.",
        "cgroup v2 io.stat",
        "Block I/O charged to this cgroup during the worker window.",
    ),
    MetricDefinition(
        "worker.throttling",
        "CPU throttling",
        "cpu_throttled_usec",
        "ms",
        "microseconds",
        "Observed CPU throttling-time delta; zero is a valid reading.",
        "cgroup v2 cpu.stat:throttled_usec",
        "Throttling recorded by the cgroup during the worker window.",
        1000,
    ),
)
EXECUTION = (
    MetricDefinition(
        "execution.wall",
        "Wall time",
        "wall_clock_s",
        "s",
        "s",
        "Elapsed monotonic time across the bounded execution block.",
        "time.perf_counter",
        "Selected adapter execution block; engine setup may be included. Not a task-body measurement.",
    ),
    MetricDefinition(
        "execution.cpu",
        "Process CPU",
        "process_cpu_s",
        "CPU-s",
        "s",
        "Runner process CPU time, excluding descendant process CPU.",
        "time.process_time",
        "Runner process threads within the execution block, not the whole container.",
    ),
    MetricDefinition(
        "execution.memory",
        "Peak process RSS",
        "peak_rss_mb",
        "MiB",
        "MiB",
        "Process lifetime high-water mark; not a peak restricted to the execution interval.",
        "resource.getrusage(RUSAGE_SELF).ru_maxrss",
        "Dedicated runner process lifetime. The legacy _mb field stores MiB; not additive with cgroup memory.",
    ),
)
MODELED = (
    MetricDefinition(
        "modeled.energy",
        "Modeled energy",
        "energy_consumed_kwh",
        "kWh",
        "kWh",
        "Software-model energy estimate, not a direct electrical measurement.",
        "Report-provided energy model",
        "Model scope as reported; not inferred from CPU time or elapsed time.",
    ),
    MetricDefinition(
        "modeled.carbon",
        "Modeled carbon",
        "emissions_kgco2eq",
        "kg CO2e",
        "kg CO2e",
        "Model-estimated emissions, not measured electricity or a carbon saving.",
        "Report-provided carbon model",
        "Depends on the attached model and carbon-intensity assumptions.",
    ),
)


def observations(
    definitions,
    profile: dict,
    *,
    scope: str,
    base: str | None,
    role: str = "reported",
    missing_reason: str | None = None,
) -> list[dict]:
    metrics = []
    for definition in definitions:
        raw = profile.get(definition.field)
        quality = (profile.get("metric_quality") or {}).get(definition.field) or {}
        reasons = list(dict.fromkeys((quality.get("reasons") or {}).values()))
        numeric = type(raw) in (int, float) and isfinite(raw) and raw >= 0
        unsafe_integer = type(raw) is int and raw > 2**53 - 1
        if unsafe_integer:
            numeric = False
        availability = "available" if numeric else "not_collected"
        if raw is not None and not numeric:
            availability, reasons = (
                "invalid",
                [
                    "reading_exceeds_json_safe_integer"
                    if unsafe_integer
                    else "invalid_metric_value"
                ],
            )
        if quality.get("status") in ("invalid", "unavailable"):
            availability = quality["status"]
        elif (
            quality.get("status") == "available" and not numeric and not unsafe_integer
        ):
            availability, reasons = "invalid", ["available_metric_has_no_valid_reading"]
        if role == "modeled" and (
            profile.get("available") is False
            or profile.get("status")
            in ("failed", "disabled", "unavailable", "not_available")
        ):
            availability, reasons = "unavailable", ["model_unavailable"]
        if not numeric or availability != "available":
            reasons = reasons or [missing_reason or "not_collected"]
        metrics.append(
            {
                "id": definition.id,
                "label": definition.label,
                "value": raw / definition.divisor
                if numeric and availability == "available"
                else None,
                "raw_value": raw if numeric else None,
                "unit": definition.unit,
                "raw_unit": definition.raw_unit,
                "scope": scope,
                "definition": definition.definition,
                "source": definition.source,
                "includes": definition.includes,
                "availability": availability,
                "reason_codes": reasons,
                "pointer": f"{base}/{definition.field}"
                if base and definition.field in profile
                else None,
                "evidence_role": role,
            }
        )
    return metrics
