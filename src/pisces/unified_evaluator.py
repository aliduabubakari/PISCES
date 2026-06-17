#!/usr/bin/env python3
"""
PISCES Unified Energy Evaluator
===============================

Simplified unified evaluator focused on energy/carbon assessment of
LLM-generated workflow code. Accepts a workflow file and produces a
comprehensive sustainability report.

Usage:
    pisces-unified path/to/workflow.py
    pisces-unified path/to/workflow.py --track-carbon --carbon-country-iso ITA
    pisces-unified path/to/workflow.py --enable-energy-eval --energy-mode auto
    pisces-unified path/to/workflow.py --out-dir ./reports --stdout
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from .base_evaluator import Orchestrator
from .energy.runtime_evaluation import EnergyEvaluationRequest, evaluate_energy


REPORT_SCHEMA_VERSION = "1.0.0"
RUNTIME_SUPPORTED = {"airflow", "prefect", "dagster"}


def _detect_orchestrator(file_path: Path) -> str:
    """Heuristic orchestrator detection from file content and extension."""
    suffix = file_path.suffix.lower()

    # YAML files → try to detect Argo vs Kestra
    if suffix in (".yaml", ".yml"):
        try:
            from .energy.structural_yaml_analyzer import detect_yaml_orchestrator
            try:
                import yaml
                manifest = yaml.safe_load(file_path.read_text(encoding="utf-8", errors="ignore"))
                if isinstance(manifest, dict):
                    detected = detect_yaml_orchestrator(manifest)
                    if detected:
                        return detected
            except Exception:
                pass
        except ImportError:
            pass
        return "unknown"

    try:
        text = file_path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return "unknown"

    imports_lower = text.lower()
    if "from airflow" in imports_lower or "import airflow" in imports_lower:
        return "airflow"
    if "from prefect" in imports_lower or "import prefect" in imports_lower:
        return "prefect"
    if "from dagster" in imports_lower or "import dagster" in imports_lower:
        return "dagster"
    return "unknown"


@dataclass
class UnifiedEnergyConfig:
    """Configuration for a unified energy evaluation run."""

    workflow_path: Path
    orchestrator: str = "auto"
    out_dir: Optional[Path] = None
    stdout: bool = False

    # Carbon tracking
    track_carbon: bool = False
    carbon_country_iso: Optional[str] = None
    carbon_measure_power_secs: int = 1
    carbon_scale_factor: float = 1.0

    # Runtime energy evaluation
    enable_energy_evaluation: bool = False
    energy_mode: str = "auto"
    energy_sample_paths: List[Path] = field(default_factory=list)
    energy_max_rows: int = 500
    energy_max_tasks: int = 25
    energy_timeout_s: int = 120
    energy_seed: int = 1729
    energy_execution_adapter: str = "representative"

    # LLM (for synthetic data generation)
    llm_provider: Optional[str] = None
    llm_model: Optional[str] = None
    llm_api_key_env: Optional[str] = None

    # Knowledge pack
    knowledge_pack_mode: str = "legacy"
    knowledge_pack: Optional[Path] = None

    def __post_init__(self):
        if self.out_dir is None:
            self.out_dir = self.workflow_path.parent / "pisces_reports"


def _build_report(
    *,
    config: UnifiedEnergyConfig,
    energy_result: Optional[Dict[str, Any]],
    errors: List[Dict[str, Any]],
    warnings: List[Dict[str, Any]],
    duration_s: float,
) -> Dict[str, Any]:
    """Assemble the unified PISCES energy report."""
    orchestrator = _detect_orchestrator(config.workflow_path)

    report: Dict[str, Any] = {
        "report_schema_version": REPORT_SCHEMA_VERSION,
        "tool": "pisces-unified",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "workflow": {
            "path": str(config.workflow_path),
            "name": config.workflow_path.stem,
            "orchestrator": orchestrator,
            "runtime_supported": orchestrator in RUNTIME_SUPPORTED,
        },
        "carbon_tracking": {
            "enabled": config.track_carbon,
            "country_iso": config.carbon_country_iso,
            "scale_factor": config.carbon_scale_factor,
        },
        "energy_evaluation": {
            "enabled": config.enable_energy_evaluation,
            "mode_requested": config.energy_mode,
        },
    }

    if energy_result:
        report["energy"] = energy_result

    if errors:
        report["errors"] = errors

    if warnings:
        report["warnings"] = warnings

    report["metadata"] = {
        "duration_s": round(duration_s, 4),
        "knowledge_pack_mode": config.knowledge_pack_mode,
    }

    return report


def run_unified_energy_evaluation(config: UnifiedEnergyConfig) -> Dict[str, Any]:
    """Run a unified energy/sustainability evaluation on a single workflow file."""
    import time
    t0 = time.time()
    errors: List[Dict[str, Any]] = []
    warnings: List[Dict[str, Any]] = []

    wf = config.workflow_path.expanduser().resolve()
    if not wf.exists():
        return _build_report(
            config=config,
            energy_result=None,
            errors=[{"code": "WORKFLOW_NOT_FOUND", "message": f"File not found: {wf}"}],
            warnings=[],
            duration_s=time.time() - t0,
        )

    try:
        code_text = wf.read_text(encoding="utf-8", errors="ignore")
    except Exception as e:
        return _build_report(
            config=config,
            energy_result=None,
            errors=[{"code": "FILE_READ_ERROR", "message": str(e)}],
            warnings=[],
            duration_s=time.time() - t0,
        )

    orchestrator = config.orchestrator
    if orchestrator == "auto":
        orchestrator = _detect_orchestrator(wf)

    energy_result: Optional[Dict[str, Any]] = None

    if config.enable_energy_evaluation or config.track_carbon:
        try:
            req = EnergyEvaluationRequest(
                file_path=wf,
                code_text=code_text,
                orchestrator=orchestrator,
                repo_root=wf.parent.parent if wf.parent.parent.exists() else wf.parent,
                mode_selected=config.energy_mode if config.enable_energy_evaluation else "heuristic",
                sample_paths=config.energy_sample_paths,
                max_rows=config.energy_max_rows,
                max_tasks=config.energy_max_tasks,
                timeout_s=config.energy_timeout_s,
                seed=config.energy_seed,
                execution_adapter=config.energy_execution_adapter,
                llm_provider=config.llm_provider,
                llm_model=config.llm_model,
                llm_api_key_env=config.llm_api_key_env,
                knowledge_pack_mode=config.knowledge_pack_mode,
                knowledge_pack=config.knowledge_pack,
            )
            result = evaluate_energy(req)
            energy_result = result.to_dict()
        except Exception as e:
            errors.append({
                "code": "ENERGY_EVALUATION_FAILED",
                "message": str(e),
            })

    report = _build_report(
        config=config,
        energy_result=energy_result,
        errors=errors,
        warnings=warnings,
        duration_s=time.time() - t0,
    )

    return report


def main():
    """CLI entry point for pisces-unified."""
    import argparse

    parser = argparse.ArgumentParser(
        description="PISCES Unified Energy Evaluator — evaluate sustainability of LLM-generated workflow code.",
    )
    parser.add_argument("workflow", type=str, help="Path to workflow Python file")
    parser.add_argument(
        "--orchestrator",
        default="auto",
        choices=["auto", "airflow", "prefect", "dagster", "argo", "kestra"],
        help="Orchestrator target (default: auto-detect)",
    )
    parser.add_argument("--out-dir", type=str, help="Output directory for reports")
    parser.add_argument("--stdout", action="store_true", help="Print report to stdout")

    # Carbon tracking
    parser.add_argument("--track-carbon", action="store_true", help="Enable CodeCarbon tracking during evaluation")
    parser.add_argument("--carbon-country-iso", type=str, help="Country ISO code for carbon intensity (e.g., ITA, GBR)")
    parser.add_argument("--carbon-measure-power-secs", type=int, default=1, help="Power measurement interval in seconds")
    parser.add_argument("--carbon-scale-factor", type=float, default=1.0, help="Scale factor for measured carbon values")

    # Energy evaluation
    parser.add_argument("--enable-energy-eval", action="store_true", help="Enable runtime energy evaluation")
    parser.add_argument("--energy-mode", default="auto", choices=["auto", "zero_fill", "sample", "synthetic", "heuristic"],
                        help="Energy mode: auto (cascade), zero_fill (AST-inferred trivial data, no LLM), sample (real CSV/JSON), synthetic (LLM-generated), heuristic (static rules only)")
    parser.add_argument("--energy-sample-path", type=str, action="append", help="Sample data file or folder (repeatable)")
    parser.add_argument("--energy-max-rows", type=int, default=500)
    parser.add_argument("--energy-max-tasks", type=int, default=25)
    parser.add_argument("--energy-timeout-s", type=int, default=120)
    parser.add_argument("--energy-seed", type=int, default=1729)
    parser.add_argument("--energy-execution-adapter", default="representative", choices=["representative", "native", "auto"])

    # LLM
    parser.add_argument("--llm-provider", type=str, default="openrouter",
                        help="LLM provider for synthetic data (default: openrouter)")
    parser.add_argument("--llm-model", type=str, default="openai/gpt-oss-120b",
                        help="LLM model name (default: openai/gpt-oss-120b)")
    parser.add_argument("--llm-api-key-env", type=str, help="Environment variable for LLM API key")

    # Knowledge pack
    parser.add_argument("--knowledge-pack", type=str, help="Path to knowledge pack JSON")
    parser.add_argument("--knowledge-pack-mode", default="legacy", choices=["legacy", "pack", "auto"])

    args = parser.parse_args()

    wf = Path(args.workflow).expanduser().resolve()
    sample_paths = [Path(p) for p in (args.energy_sample_path or [])]

    config = UnifiedEnergyConfig(
        workflow_path=wf,
        orchestrator=args.orchestrator,
        out_dir=Path(args.out_dir) if args.out_dir else None,
        stdout=args.stdout,
        track_carbon=args.track_carbon,
        carbon_country_iso=args.carbon_country_iso,
        carbon_measure_power_secs=args.carbon_measure_power_secs,
        carbon_scale_factor=args.carbon_scale_factor,
        enable_energy_evaluation=args.enable_energy_eval,
        energy_mode=args.energy_mode,
        energy_sample_paths=sample_paths,
        energy_max_rows=args.energy_max_rows,
        energy_max_tasks=args.energy_max_tasks,
        energy_timeout_s=args.energy_timeout_s,
        energy_seed=args.energy_seed,
        energy_execution_adapter=args.energy_execution_adapter,
        llm_provider=args.llm_provider,
        llm_model=args.llm_model,
        llm_api_key_env=args.llm_api_key_env,
        knowledge_pack_mode=args.knowledge_pack_mode,
        knowledge_pack=Path(args.knowledge_pack) if args.knowledge_pack else None,
    )

    report = run_unified_energy_evaluation(config)

    if args.stdout:
        print(json.dumps(report, indent=2, default=str))
    else:
        out_dir = config.out_dir
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"pisces_{wf.stem}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        out_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
        print(f"Report written to: {out_path}")


if __name__ == "__main__":
    main()
