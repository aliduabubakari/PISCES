#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

from .carbon_tracker import add_scaled_estimate, try_start_tracker, try_stop_tracker


def _emit(payload: Dict[str, Any], out: Path) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")


def _load_module_from_file(workflow_file: Path) -> None:
    spec = importlib.util.spec_from_file_location("pisces_import_workflow", str(workflow_file))
    if spec is None or spec.loader is None:
        raise RuntimeError("unable_to_create_module_spec")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)


def main() -> None:
    parser = argparse.ArgumentParser(description="PISCES import/build carbon measurement runner")
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
        _emit(
            {
                "evaluation_type": "import_build_carbon",
                "timestamp": datetime.now().isoformat(),
                "ok": False,
                "error": {"error_type": type(e).__name__, "message": str(e)},
            },
            out_path,
        )
        return

    workflow_file = Path(str(req.get("workflow_file") or ""))
    orchestrator = str(req.get("orchestrator") or "unknown")
    country_iso = req.get("carbon_country_iso")
    measure_power_secs = int(req.get("carbon_measure_power_secs") or 1)
    scale_factor = float(req.get("carbon_scale_factor") or 1.0)

    tracker = try_start_tracker(
        country_iso_code=str(country_iso) if country_iso else None,
        measure_power_secs=measure_power_secs,
        project_name=f"pisces_import_build_{orchestrator}",
        extra={
            "measurement_scope": "import_build_only",
            "disclaimer": (
                "This measurement covers importing/building the workflow module only. "
                "It does not include task runtime energy."
            ),
        },
    )

    started = time.time()
    import_status = "ok"
    import_error = None
    try:
        _load_module_from_file(workflow_file)
    except Exception as e:
        import_status = "failed"
        import_error = {"error_type": type(e).__name__, "message": str(e)}

    energy = add_scaled_estimate(try_stop_tracker(tracker), scale_factor)
    if tracker is None:
        energy["measurement_scope"] = "import_build_only"
        energy["disclaimer"] = (
            "This measurement covers importing/building the workflow module only. "
            "It does not include task runtime energy."
        )

    payload = {
        "evaluation_type": "import_build_carbon",
        "timestamp": datetime.now().isoformat(),
        "ok": import_status == "ok",
        "orchestrator": orchestrator,
        "workflow_file": str(workflow_file),
        "measurement": {
            "status": "ok" if energy.get("status") == "ok" else "not_measured",
            "measurement_scope": "import_build_only",
            "duration_s": round(max(0.0, time.time() - started), 6),
            "import_status": import_status,
            "import_error": import_error,
            "energy_profile": energy,
        },
    }
    _emit(payload, out_path)


if __name__ == "__main__":
    main()
