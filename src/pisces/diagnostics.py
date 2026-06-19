#!/usr/bin/env python3
from __future__ import annotations

import argparse
import contextlib
import importlib.util
import io
import json
import os
import platform
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from importlib import metadata
from pathlib import Path
from typing import Any, Dict, Optional

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass

from .energy.carbon_tracker import try_start_tracker, try_stop_tracker
from .unified_evaluator import RUNTIME_SUPPORTED, _detect_orchestrator


DIAGNOSTICS_SCHEMA_VERSION = "1.0.0"


@dataclass
class DiagnosticsConfig:
    workflow_path: Optional[Path] = None
    orchestrator: str = "auto"
    llm_provider: str = "openrouter"
    llm_model: str = "openai/gpt-oss-120b"
    llm_api_key_env: Optional[str] = None
    carbon_country_iso: Optional[str] = None
    carbon_measure_power_secs: int = 1
    validate_codecarbon: bool = True


def _package_status(import_name: str, distribution_name: Optional[str] = None) -> Dict[str, Any]:
    dist = distribution_name or import_name
    available = importlib.util.find_spec(import_name) is not None
    version = None
    if available:
        try:
            version = metadata.version(dist)
        except Exception:
            version = "unknown"
    return {
        "available": available,
        "version": version,
        "distribution": dist,
    }


def _env_status(name: Optional[str]) -> Dict[str, Any]:
    if not name:
        return {"name": None, "present": False}
    value = os.environ.get(name)
    return {
        "name": name,
        "present": bool(value),
        "length": len(value) if value else 0,
    }


def _provider_key_env(provider: str, explicit_env: Optional[str]) -> str:
    if explicit_env:
        return explicit_env
    return {
        "openai": "OPENAI_API_KEY",
        "openrouter": "OPENROUTER_API_KEY",
        "deepinfra": "DEEPINFRA_API_KEY",
        "deepseek": "DEEPSEEK_API_KEY",
        "anthropic": "ANTHROPIC_API_KEY",
    }.get(provider, "OPENAI_API_KEY")


def _validate_codecarbon(config: DiagnosticsConfig) -> Dict[str, Any]:
    captured_stdout = io.StringIO()
    captured_stderr = io.StringIO()
    with contextlib.redirect_stdout(captured_stdout), contextlib.redirect_stderr(captured_stderr):
        tracker = try_start_tracker(
            country_iso_code=config.carbon_country_iso,
            measure_power_secs=config.carbon_measure_power_secs,
            project_name="pisces_diagnostics",
            extra={
                "measurement_scope": "diagnostics_smoke_test",
                "disclaimer": "This is a short readiness smoke test, not an evaluation measurement.",
            },
        )
        payload = try_stop_tracker(tracker)
    return {
        "startable": bool(tracker is not None),
        "smoke_test": payload,
        "captured_output": {
            "stdout": captured_stdout.getvalue().splitlines()[-20:],
            "stderr": captured_stderr.getvalue().splitlines()[-20:],
        },
    }


def collect_diagnostics(config: DiagnosticsConfig) -> Dict[str, Any]:
    provider = (config.llm_provider or "openrouter").strip().lower()
    key_env = _provider_key_env(provider, config.llm_api_key_env)
    codecarbon_status = _package_status("codecarbon")

    workflow: Optional[Dict[str, Any]] = None
    if config.workflow_path is not None:
        wf = config.workflow_path.expanduser().resolve()
        detected = None
        exists = wf.exists()
        if exists:
            detected = _detect_orchestrator(wf)
        orchestrator = detected if config.orchestrator == "auto" else config.orchestrator
        workflow = {
            "path": str(wf),
            "exists": exists,
            "detected_orchestrator": detected,
            "selected_orchestrator": orchestrator,
            "runtime_supported": orchestrator in RUNTIME_SUPPORTED,
        }

    carbon_validation = None
    if config.validate_codecarbon and codecarbon_status["available"]:
        carbon_validation = _validate_codecarbon(config)

    checks = {
        "codecarbon_available": bool(codecarbon_status["available"]),
        "llm_api_key_present": bool(os.environ.get(key_env)),
    }
    if workflow is not None:
        checks["workflow_exists"] = bool(workflow["exists"])
        checks["workflow_runtime_supported"] = bool(workflow["runtime_supported"])
    if carbon_validation is not None:
        checks["codecarbon_startable"] = bool(carbon_validation["startable"])

    blockers = []
    warnings = []
    if not checks["codecarbon_available"]:
        blockers.append("codecarbon_missing: install with `pip install \"pisces[energy]\"`")
    if config.validate_codecarbon and carbon_validation is not None and not checks.get("codecarbon_startable"):
        warnings.append("codecarbon_imported_but_tracker_did_not_start")
    if workflow is not None and not checks.get("workflow_exists"):
        blockers.append("workflow_not_found")
    if workflow is not None and not checks.get("workflow_runtime_supported"):
        warnings.append("workflow_runtime_not_supported; static evaluation can still run")
    if not checks["llm_api_key_present"]:
        warnings.append(f"missing_llm_api_key_env:{key_env}; synthetic provider refresh will fail")

    return {
        "schema_version": DIAGNOSTICS_SCHEMA_VERSION,
        "tool": "pisces-diagnostics",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "python": {
            "executable": sys.executable,
            "version": platform.python_version(),
            "platform": platform.platform(),
        },
        "packages": {
            "pisces": _package_status("pisces"),
            "codecarbon": codecarbon_status,
            "openai": _package_status("openai"),
            "dotenv": _package_status("dotenv", "python-dotenv"),
            "yaml": _package_status("yaml", "PyYAML"),
        },
        "carbon": {
            "country_iso": config.carbon_country_iso,
            "measure_power_secs": int(config.carbon_measure_power_secs),
            "validation": carbon_validation,
        },
        "llm": {
            "provider": provider,
            "model": config.llm_model,
            "api_key_env": _env_status(key_env),
        },
        "workflow": workflow,
        "checks": checks,
        "ready": not blockers,
        "blockers": blockers,
        "warnings": warnings,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Report PISCES environment readiness before evaluation")
    parser.add_argument("workflow", nargs="?", help="Optional workflow file to inspect")
    parser.add_argument("--orchestrator", default="auto", choices=["auto", "airflow", "prefect", "dagster", "argo", "kestra"])
    parser.add_argument("--llm-provider", default="openrouter")
    parser.add_argument("--llm-model", default="openai/gpt-oss-120b")
    parser.add_argument("--llm-api-key-env")
    parser.add_argument("--carbon-country-iso")
    parser.add_argument("--carbon-measure-power-secs", type=int, default=1)
    parser.add_argument("--skip-codecarbon-smoke", action="store_true")
    parser.add_argument("--out", type=str, help="Optional JSON output path")
    args = parser.parse_args()

    report = collect_diagnostics(
        DiagnosticsConfig(
            workflow_path=Path(args.workflow) if args.workflow else None,
            orchestrator=args.orchestrator,
            llm_provider=args.llm_provider,
            llm_model=args.llm_model,
            llm_api_key_env=args.llm_api_key_env,
            carbon_country_iso=args.carbon_country_iso,
            carbon_measure_power_secs=args.carbon_measure_power_secs,
            validate_codecarbon=not args.skip_codecarbon_smoke,
        )
    )

    rendered = json.dumps(report, indent=2, default=str)
    if args.out:
        out = Path(args.out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)


if __name__ == "__main__":
    main()
