#!/usr/bin/env python3
"""
Carbon/Energy Tracker Wrapper (v1)
==================================

This wraps CodeCarbon in a "best-effort" way for use inside subprocess smoke tests.

Goal:
- Measure *import/parse overhead* energy (exec_module stage) in the orchestrator venv
- Never break smoke tests if codecarbon isn't installed or fails

Usage pattern (later, inside import_smoke_test_cli.py):

  from evaluators.energy.carbon_tracker import try_start_tracker, try_stop_tracker

  tracker = try_start_tracker(country_iso_code="GBR")
  spec.loader.exec_module(module)
  energy_payload = try_stop_tracker(tracker)

  payload["energy_profile"] = energy_payload
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional
import os
import platform
import time
import sys
import traceback


@dataclass
class TrackerHandle:
    impl: Any
    started_at: float
    config: Dict[str, Any]


def add_scaled_estimate(payload: Dict[str, Any], scale_factor: float) -> Dict[str, Any]:
    """Return a copy of a CodeCarbon payload with explicit scaled estimates."""
    out = dict(payload or {})
    try:
        factor = float(scale_factor)
    except Exception:
        factor = 1.0

    out["scale_factor"] = factor
    energy_kwh = out.get("energy_consumed_kwh")
    emissions_kg = out.get("emissions_kgco2eq")
    out["scaled_estimate"] = {
        "scale_factor": factor,
        "energy_consumed_kwh": (
            float(energy_kwh) * factor
            if isinstance(energy_kwh, (int, float))
            else None
        ),
        "emissions_kgco2eq": (
            float(emissions_kg) * factor
            if isinstance(emissions_kg, (int, float))
            else None
        ),
        "disclaimer": "Scaled values are estimates derived from raw local measurement, not separate measurements.",
    }
    return out


def _disable_powermetrics_sudo() -> None:
    """
    Monkey-patch codecarbon's powermetrics module to avoid the sudo
    password prompt on macOS.  ``_has_powermetrics_sudo()`` runs
    ``sudo powermetrics ...`` which triggers the system password
    prompt even though codecarbon only uses it as a capability
    check.  Replacing the function with a hard ``False`` keeps the
    rest of codecarbon working (it falls back to TDP estimation)
    while removing the interactive prompt that breaks automation.

    Controlled by the environment variable:
        PISCES_SKIP_POWERMETRICS=1   # disable the sudo check
    Set it to "0" or leave it unset to keep the default behaviour.
    """
    import os as _os

    skip = _os.environ.get("PISCES_SKIP_POWERMETRICS", "1").strip().lower()
    if skip in ("0", "false", "no", "off"):
        return

    try:
        import codecarbon.core.powermetrics as _pm

        _pm._has_powermetrics_sudo = lambda: False
        _pm.is_powermetrics_available = lambda: False
    except Exception:
        pass


def try_start_tracker(
    *,
    country_iso_code: Optional[str] = None,
    measure_power_secs: int = 1,
    project_name: str = "workflow_smoke_import",
    extra: Optional[Dict[str, Any]] = None,
) -> Optional[TrackerHandle]:
    """
    Start a CodeCarbon tracker if available.

    Returns:
      - TrackerHandle if started
      - None if unavailable or failed
    """
    # Avoid sudo password prompt on macOS (powermetrics check).
    _disable_powermetrics_sudo()

    try:
        # Delayed import (optional dependency)
        from codecarbon import OfflineEmissionsTracker  # type: ignore
    except Exception:
        return None

    cfg = {
        "country_iso_code": country_iso_code,
        "measure_power_secs": int(measure_power_secs),
        "project_name": project_name,
    }
    if extra and isinstance(extra, dict):
        cfg.update(extra)

    try:
        # OfflineEmissionsTracker is good for CI/offline environments.
        # We avoid writing to file if possible.
        tracker = OfflineEmissionsTracker(
            country_iso_code=country_iso_code,
            measure_power_secs=int(measure_power_secs),
            project_name=project_name,
            log_level="error",
            save_to_file=False,
        )
        tracker.start()
        return TrackerHandle(impl=tracker, started_at=time.time(), config=cfg)
    except Exception:
        return None


def try_stop_tracker(handle: Optional[TrackerHandle]) -> Dict[str, Any]:
    """
    Stop tracker and return a JSON-serializable payload.

    If handle is None, returns available=False payload.
    """
    measurement_scope = "import_exec_only"
    disclaimer = (
        "This measurement covers only the energy/carbon during import/exec of the workflow module "
        "(DAG/flow/job parse and registration). It does not include task runtime energy."
    )
    if handle is not None:
        cfg = handle.config or {}
        if isinstance(cfg.get("measurement_scope"), str):
            measurement_scope = cfg["measurement_scope"]
        if isinstance(cfg.get("disclaimer"), str):
            disclaimer = cfg["disclaimer"]

    base = {
        "tool": "codecarbon",
        "available": bool(handle is not None),
        "evidence_role": "secondary",
        "interpretation": "modeled_energy",
        "power_source_kind": (
            "modeled_tdp"
            if platform.system() == "Darwin"
            and os.environ.get("PISCES_SKIP_POWERMETRICS", "1").strip().lower()
            not in ("0", "false", "no", "off")
            else "unverified"
        ),
        "hardware_power_source_verified": False,
        "python": sys.executable,
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "measurement_scope": measurement_scope,
        "disclaimer": disclaimer,
    }

    if handle is None:
        base["status"] = "not_started"
        return base

    try:
        handle.impl.stop()
        duration_s = max(0.0, time.time() - float(handle.started_at))

        # CodeCarbon API differs across versions:
        #   v2.x: tracker.emissions (float), tracker.energy_consumed (float)
        #   v3.x: tracker.final_emissions_data (EmissionsData dataclass)
        energy_kwh = None
        emissions_kg = None
        codecarbon_version = "unknown"

        # Try v3.x API first
        final_data = getattr(handle.impl, "final_emissions_data", None)
        if final_data is not None:
            codecarbon_version = getattr(final_data, "codecarbon_version", "3.x")
            energy_kwh = getattr(final_data, "energy_consumed", None)
            emissions_kg = getattr(final_data, "emissions", None)
            # Use tracked duration from the tracker if available
            tracked_duration = getattr(final_data, "duration", None)
            if tracked_duration is not None:
                duration_s = float(tracked_duration)
        else:
            # Fallback to v2.x API
            codecarbon_version = "2.x"
            energy_kwh = getattr(handle.impl, "energy_consumed", None)
            emissions_kg = getattr(handle.impl, "emissions", None)

        payload = {
            **base,
            "status": "ok",
            "codecarbon_version": codecarbon_version,
            "duration_s": duration_s,
            "energy_consumed_kwh": float(energy_kwh) if energy_kwh is not None else None,
            "emissions_kgco2eq": float(emissions_kg) if emissions_kg is not None else None,
            "config": handle.config,
        }
        return payload

    except Exception as e:
        return {
            **base,
            "status": "error",
            "error_type": type(e).__name__,
            "message": str(e),
            "traceback": traceback.format_exc(),
            "config": handle.config,
        }
