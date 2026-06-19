#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from .adapters import NativeAdapterError, NativeSafetyPolicy, execute_native_workload
from .carbon_tracker import add_scaled_estimate, try_start_tracker, try_stop_tracker


def _emit(payload: Dict[str, Any], out: Path) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")


def _numeric(v: Any) -> float:
    if isinstance(v, bool):
        return 1.0 if v else 0.0
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        try:
            return float(v)
        except Exception:
            return float(len(v))
    return 0.0


def _operator_profile_units(operator_tiers: Dict[str, Any]) -> int:
    heavy = int(operator_tiers.get("heavy_compute_count") or 0)
    polling = int(operator_tiers.get("polling_heavy_count") or 0)
    distribution = operator_tiers.get("distribution") if isinstance(operator_tiers.get("distribution"), dict) else {}
    io_bound = int(distribution.get("io_bound") or 0)
    work_units = 1 + (heavy * 2) + polling + io_bound
    return max(1, min(12, work_units))


def _run_workload(
    rows: List[Dict[str, Any]],
    *,
    orchestrator: str,
    max_tasks: int,
    seed: int,
    workload_profile: str = "standard",
    operator_tiers: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    t0 = time.time()
    row_count = len(rows)
    if row_count == 0:
        return {
            "rows_processed": 0,
            "tasks_exercised": 0,
            "numeric_accumulator": 0.0,
            "hash_accumulator": 0,
            "duration_s": 0.0,
        }

    task_factor_map = {
        "airflow": 1.3,
        "prefect": 1.0,
        "dagster": 1.15,
    }
    factor = task_factor_map.get(orchestrator, 1.0)
    task_budget = max(1, min(int(max_tasks), int(max_tasks * factor)))

    numeric_acc = 0.0
    hash_acc = 0
    tiers = operator_tiers if isinstance(operator_tiers, dict) else {}
    work_units = _operator_profile_units(tiers) if workload_profile == "operator_profile" else 1

    for t in range(task_budget):
        for unit in range(work_units):
            for r in rows:
                for k, v in r.items():
                    numeric_acc += _numeric(v) * (1.0 + (t * 0.01) + (unit * 0.005))
                    blob = f"{seed}:{t}:{unit}:{k}:{v}".encode("utf-8", errors="ignore")
                    h = hashlib.sha256(blob).hexdigest()
                    hash_acc ^= int(h[:8], 16)

    duration_s = max(0.0, time.time() - t0)
    return {
        "rows_processed": row_count,
        "tasks_exercised": task_budget,
        "numeric_accumulator": round(numeric_acc, 6),
        "hash_accumulator": int(hash_acc),
        "duration_s": round(duration_s, 6),
        "workload_profile": workload_profile,
        "work_units": work_units,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Runtime representative energy workload runner")
    parser.add_argument("--in", dest="in_path", required=True)
    parser.add_argument("--out", dest="out_path", required=True)
    args = parser.parse_args()

    in_path = Path(args.in_path)
    out_path = Path(args.out_path)

    try:
        req = json.loads(in_path.read_text(encoding="utf-8"))
        if not isinstance(req, dict):
            raise ValueError("input payload must be JSON object")
    except Exception as e:
        payload = {
            "evaluation_type": "runtime_energy",
            "timestamp": datetime.now().isoformat(),
            "ok": False,
            "error": {"error_type": type(e).__name__, "message": str(e)},
        }
        _emit(payload, out_path)
        return

    orchestrator = str(req.get("orchestrator") or "unknown")
    mode = str(req.get("mode") or "unknown")
    data_source = str(req.get("data_source") or "none")
    workflow_file = Path(str(req.get("workflow_file") or ""))
    execution_adapter = str(req.get("execution_adapter") or "representative").strip().lower()
    max_rows = int(req.get("max_rows") or 500)
    max_tasks = int(req.get("max_tasks") or 25)
    seed = int(req.get("seed") or 1729)
    workload_profile = str(req.get("workload_profile") or "standard")
    operator_tiers = req.get("operator_tiers") if isinstance(req.get("operator_tiers"), dict) else {}
    native_safety_raw = req.get("native_safety") if isinstance(req.get("native_safety"), dict) else {}
    native_safety = NativeSafetyPolicy(
        enabled=bool(native_safety_raw.get("enabled", True)),
        block_network=bool(native_safety_raw.get("block_network", True)),
        block_subprocess=bool(native_safety_raw.get("block_subprocess", True)),
        block_file_writes=bool(native_safety_raw.get("block_file_writes", True)),
    )
    country_iso = req.get("carbon_country_iso")
    measure_power_secs = int(req.get("carbon_measure_power_secs") or 1)
    scale_factor = float(req.get("carbon_scale_factor") or 1.0)

    rows_raw = req.get("rows")
    rows: List[Dict[str, Any]] = []
    if isinstance(rows_raw, list):
        for r in rows_raw:
            if isinstance(r, dict):
                rows.append(r)

    tracker = try_start_tracker(
        country_iso_code=str(country_iso) if country_iso else None,
        measure_power_secs=measure_power_secs,
        project_name=f"pisces_runtime_{orchestrator}_{mode}",
        extra={
            "measurement_scope": "runtime_representative_workload",
            "disclaimer": (
                "This measurement covers a bounded representative workload used for comparative "
                "evaluation; it is not full production workflow runtime."
            ),
        },
    )
    adapter_attempts: List[Dict[str, Any]] = []
    selected_adapter = execution_adapter if execution_adapter in {"native", "representative", "auto"} else "representative"
    run_stats: Dict[str, Any]
    adapter_used = "representative"
    try:
        if selected_adapter == "representative":
            run_stats = _run_workload(
                rows,
                orchestrator=orchestrator,
                max_tasks=max_tasks,
                seed=seed,
                workload_profile=workload_profile,
                operator_tiers=operator_tiers,
            )
            adapter_attempts.append({"adapter": "representative", "status": "success"})
        elif selected_adapter == "native":
            run_stats = execute_native_workload(
                workflow_file,
                orchestrator=orchestrator,
                max_tasks=max_tasks,
                safety_policy=native_safety,
            )
            adapter_used = "native"
            adapter_attempts.append({"adapter": "native", "status": "success"})
        else:
            try:
                run_stats = execute_native_workload(
                    workflow_file,
                    orchestrator=orchestrator,
                    max_tasks=max_tasks,
                    safety_policy=native_safety,
                )
                if int(run_stats.get("native_tasks_executed") or 0) <= 0:
                    raise NativeAdapterError("native_adapter_no_tasks_executed")
                adapter_used = "native"
                adapter_attempts.append({"adapter": "native", "status": "success"})
            except Exception as e:
                adapter_attempts.append({"adapter": "native", "status": "failed", "reason": f"{type(e).__name__}:{e}"})
                run_stats = _run_workload(
                    rows,
                    orchestrator=orchestrator,
                    max_tasks=max_tasks,
                    seed=seed,
                    workload_profile=workload_profile,
                    operator_tiers=operator_tiers,
                )
                adapter_used = "representative"
                adapter_attempts.append({"adapter": "representative", "status": "success"})
    except Exception as e:
        payload = {
            "evaluation_type": "runtime_energy",
            "timestamp": datetime.now().isoformat(),
            "ok": False,
            "mode": mode,
            "data_source": data_source,
            "orchestrator": orchestrator,
            "error": {"error_type": type(e).__name__, "message": str(e)},
            "runtime_measurement": {
                "status": "failed",
                "execution_adapter_requested": selected_adapter,
                "execution_adapter_used": adapter_used,
                "adapter_attempts": adapter_attempts,
            },
        }
        _emit(payload, out_path)
        return
    energy = add_scaled_estimate(try_stop_tracker(tracker), scale_factor)
    if adapter_used == "native":
        energy["measurement_scope"] = "native_local_runtime"
        energy["disclaimer"] = (
            "This measurement covers bounded local native execution under PISCES safety guards. "
            "External effects are blocked by default, so results may skip effectful tasks."
        )
    if tracker is None:
        if adapter_used == "native":
            energy["measurement_scope"] = "native_local_runtime"
            energy["disclaimer"] = (
                "This measurement covers bounded local native execution under PISCES safety guards. "
                "External effects are blocked by default, so results may skip effectful tasks."
            )
        else:
            energy["measurement_scope"] = "runtime_representative_workload"
            energy["disclaimer"] = (
                "This measurement covers a bounded representative workload used for comparative "
                "evaluation; it is not full production workflow runtime."
            )

    measured_kwh = energy.get("energy_consumed_kwh")
    measured_kg = energy.get("emissions_kgco2eq")
    rows_processed = int(run_stats.get("rows_processed") or 0)

    normalized = {
        "energy_kwh_per_1k_rows": None,
        "emissions_kgco2eq_per_1k_rows": None,
    }
    if isinstance(measured_kwh, (int, float)) and rows_processed > 0:
        normalized["energy_kwh_per_1k_rows"] = round((float(measured_kwh) / rows_processed) * 1000.0, 10)
    if isinstance(measured_kg, (int, float)) and rows_processed > 0:
        normalized["emissions_kgco2eq_per_1k_rows"] = round((float(measured_kg) / rows_processed) * 1000.0, 10)

    payload = {
        "evaluation_type": "runtime_energy",
        "timestamp": datetime.now().isoformat(),
        "ok": True,
        "mode": mode,
        "data_source": data_source,
        "orchestrator": orchestrator,
        "runtime_measurement": {
            "status": "ok",
            "execution_adapter_requested": selected_adapter,
            "execution_adapter_used": adapter_used,
            "adapter_attempts": adapter_attempts,
            "rows_processed": rows_processed,
            "tasks_exercised": int(run_stats.get("tasks_exercised") or 0),
            "duration_s": float(run_stats.get("duration_s") or 0.0),
            "numeric_accumulator": run_stats.get("numeric_accumulator"),
            "hash_accumulator": run_stats.get("hash_accumulator"),
            "workload_profile": run_stats.get("workload_profile"),
            "work_units": run_stats.get("work_units"),
            "native_tasks_executed": run_stats.get("native_tasks_executed"),
            "native_tasks_skipped": run_stats.get("native_tasks_skipped"),
            "native_adapter": run_stats.get("native_adapter"),
            "native_task_results": run_stats.get("native_task_results"),
            "native_safety": run_stats.get("native_safety"),
            "native_metadata": run_stats.get("native_metadata"),
            "energy_profile": energy,
            "normalized": normalized,
            "bounds": {
                "max_rows": max_rows,
                "max_tasks": max_tasks,
                "seed": seed,
            },
        },
    }
    _emit(payload, out_path)


if __name__ == "__main__":
    main()
