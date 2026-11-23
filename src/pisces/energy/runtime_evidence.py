"""Separate runtime observations from validation and comparison verdicts."""

from __future__ import annotations

import math
from typing import Any


def _telemetry_status(profile: dict[str, Any] | None, fields: tuple[str, ...]) -> str:
    if not isinstance(profile, dict) or not profile:
        return "not_collected"
    if profile.get("status") == "error":
        return "failed"
    if profile.get("status") == "not_started":
        return "unavailable"
    if profile.get("available") is False:
        return "unavailable"
    values = [profile.get(field) for field in fields]
    if any(
        value is not None and not (
            (type(value) is int and value >= 0)
            or (type(value) is float and math.isfinite(value) and value >= 0)
        )
        for value in values
    ):
        return "invalid"
    count = sum(value is not None for value in values)
    if count == len(fields):
        return "available"
    return "partial" if count else "unavailable"


def finalize_runtime_measurement(
    measurement: dict[str, Any], *, adapter_requested: str | None,
    contract_requested: bool,
) -> None:
    """Mark bounded observations without crediting any verified functional units."""
    outcome = measurement.get("execution_outcome")
    checks = [measurement.get(key) for key in ("task_coverage", "input_consumption", "output_assertions")]
    if outcome == "contract_invalid":
        contract_status = "invalid"
    elif not contract_requested:
        contract_status = "not_requested"
    elif any(isinstance(check, dict) and check.get("passed") is False for check in checks):
        contract_status = "failed"
    elif outcome == "completed" and all(isinstance(check, dict) and check.get("passed") is True for check in checks):
        contract_status = "checks_passed"
    else:
        contract_status = "not_verified"

    screening_eligible = (
        outcome == "completed"
        and measurement.get("status") == "ok"
        and contract_status == "checks_passed"
        and measurement.get("eligible_for_paired_equivalence_comparison") is True
    )
    measurement["contract_validation"] = {
        "status": contract_status,
        "scope": (
            "single_execution_checks"
            if (measurement.get("workload_contract") or {}).get("schema_version") == "2.0"
            else "final_repetition_checks"
        ) if any(check is not None for check in checks) else None,
    }
    measurement["pairwise_equivalence"] = {
        "status": "not_evaluated",
        "screening_eligible": screening_eligible,
    }
    measurement["eligible_for_paired_equivalence_comparison"] = screening_eligible
    measurement["paired_equivalence_status"] = (
        "eligible_pending_pairwise_comparison" if screening_eligible else "ineligible"
    )
    measurement["eligible_for_resource_comparison"] = False
    measurement["ineligibility_reasons"] = list(dict.fromkeys([
        *(measurement.get("ineligibility_reasons") or []),
        "verified_workload_units_unavailable",
    ]))
    measurement["execution_adapter_requested"] = adapter_requested
    measurement.setdefault("execution_adapter_used", None)
    measurement["normalized"] = {
        "status": "not_available",
        "reason": "verified_workload_units_unavailable",
        "energy_kwh_per_1k_rows": None,
        "emissions_kgco2eq_per_1k_rows": None,
    }

    raw_diagnostics = measurement.get("attempt_diagnostics")
    diagnostics = dict(raw_diagnostics) if isinstance(raw_diagnostics, dict) else {}
    if not screening_eligible:
        for key in ("operational_metrics", "modeled_energy", "repetition_metrics", "repetition_summary"):
            if measurement.get(key) is not None:
                diagnostics[key] = measurement.pop(key)
        if "modeled_energy" not in diagnostics and measurement.get("energy_profile") is not None:
            diagnostics["modeled_energy"] = measurement["energy_profile"]
        measurement.update(operational_metrics=None, modeled_energy=None, energy_profile=None)
        if isinstance(diagnostics.get("operational_metrics"), dict):
            diagnostics["operational_metrics"] = {
                **diagnostics["operational_metrics"], "evidence_role": "diagnostic"
            }
    measurement["attempt_diagnostics"] = diagnostics or None
    operational = measurement.get("operational_metrics") or diagnostics.get("operational_metrics")
    energy = measurement.get("modeled_energy") or diagnostics.get("modeled_energy")
    use = "none"
    if screening_eligible:
        use = "workload_screening"
    elif diagnostics:
        use = "attempt_diagnostics"
    measurement["telemetry"] = {
        "operational": _telemetry_status(operational, ("wall_clock_s", "process_cpu_s", "peak_rss_mb")),
        "modeled_energy": _telemetry_status(energy, ("energy_consumed_kwh", "emissions_kgco2eq")),
        "use": use,
    }
    if adapter_requested == "oci":
        container = measurement.get("container_telemetry")
        status = _telemetry_status(container, ("wall_clock_s", "cpu_s", "memory_peak_bytes"))
        quality = (container.get("metric_quality") or {}) if isinstance(container, dict) else {}
        if any(quality.get(key, {}).get("status") == "invalid"
               for key in ("wall_clock_s", "cpu_s", "memory_peak_bytes")):
            status = "invalid"
        measurement["telemetry"]["container"] = status
        if container is None and not measurement.get("container_telemetry_missing_reason"):
            measurement["container_telemetry_missing_reason"] = "worker_snapshot_not_received"
