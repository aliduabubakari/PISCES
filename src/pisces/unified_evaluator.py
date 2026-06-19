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
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from .energy.runtime_evaluation import EnergyEvaluationRequest, evaluate_energy


REPORT_SCHEMA_VERSION = "1.1.0"
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


def _find_repo_root(file_path: Path) -> Path:
    """Find the nearest project root for subprocess working directories."""
    start = file_path.parent if file_path.is_file() else file_path
    for candidate in [start, *start.parents]:
        if (candidate / "pyproject.toml").exists() or (candidate / ".git").exists():
            return candidate
    return start


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

    # Scenario estimates
    estimate_scenario_name: Optional[str] = None
    estimate_runs_per_year: Optional[int] = None
    estimate_years: float = 1.0
    estimate_repetitions_per_run: float = 1.0

    # Runtime energy evaluation
    enable_energy_evaluation: bool = False
    energy_mode: str = "auto"
    energy_sample_paths: List[Path] = field(default_factory=list)
    energy_sample_schema_map: Dict[str, str] = field(default_factory=dict)
    energy_max_rows: int = 500
    energy_max_tasks: int = 25
    energy_timeout_s: int = 120
    energy_seed: int = 1729
    energy_execution_adapter: str = "representative"

    # LLM (for synthetic data generation)
    llm_provider: Optional[str] = None
    llm_model: Optional[str] = None
    llm_api_key_env: Optional[str] = None
    synthetic_cache_mode: str = "use"

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
    mode_requested = config.energy_mode if config.enable_energy_evaluation else "heuristic"

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
            "enabled": bool(energy_result),
            "mode_requested": mode_requested,
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

    try:
        req = EnergyEvaluationRequest(
            file_path=wf,
            code_text=code_text,
            orchestrator=orchestrator,
            repo_root=_find_repo_root(wf),
            mode_selected=config.energy_mode if config.enable_energy_evaluation else "heuristic",
            sample_paths=config.energy_sample_paths,
            sample_schema_map=config.energy_sample_schema_map,
            max_rows=config.energy_max_rows,
            max_tasks=config.energy_max_tasks,
            timeout_s=config.energy_timeout_s,
            seed=config.energy_seed,
            execution_adapter=config.energy_execution_adapter,
            track_carbon=config.track_carbon,
            carbon_country_iso=config.carbon_country_iso,
            carbon_measure_power_secs=config.carbon_measure_power_secs,
            carbon_scale_factor=config.carbon_scale_factor,
            estimate_scenario_name=config.estimate_scenario_name,
            estimate_runs_per_year=config.estimate_runs_per_year,
            estimate_years=config.estimate_years,
            estimate_repetitions_per_run=config.estimate_repetitions_per_run,
            llm_provider=config.llm_provider,
            llm_model=config.llm_model,
            llm_api_key_env=config.llm_api_key_env,
            synthetic_cache_mode=config.synthetic_cache_mode,
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

    # Scenario estimates
    parser.add_argument("--estimate-scenario-file", type=str, help="JSON scenario file with runs_per_year, years, repetitions_per_run, and optional name")
    parser.add_argument("--estimate-scenario-name", type=str, help="Name for the explicit estimate scenario")
    parser.add_argument("--estimate-runs-per-year", type=int, help="Explicit workflow runs per year for calibrated estimates")
    parser.add_argument("--estimate-years", type=float, default=1.0, help="Scenario period in years (default: 1)")
    parser.add_argument("--estimate-repetitions-per-run", type=float, default=1.0, help="How many measured runs correspond to one workflow run")

    # Energy evaluation
    parser.add_argument("--enable-energy-eval", action="store_true", help="Enable runtime energy evaluation")
    parser.add_argument(
        "--energy-mode",
        default="auto",
        choices=["auto", "zero_fill", "fuzz_fill", "scale_probe", "operator_profile", "sample", "synthetic", "heuristic"],
        help=(
            "Energy mode: auto (sample if provided, otherwise fuzz_fill → zero_fill → synthetic → heuristic), "
            "zero_fill (all-zero generated data), fuzz_fill (deterministic schema-guided values), "
            "scale_probe (multi-size generated workloads), operator_profile (static tier-weighted representative workload), "
            "sample (real CSV/JSON), synthetic (LLM-generated), heuristic (static rules only)"
        ),
    )
    parser.add_argument("--energy-sample-path", type=str, action="append", help="Sample data file or folder (repeatable)")
    parser.add_argument(
        "--energy-sample-map",
        type=str,
        action="append",
        help="Sample schema mapping as SOURCE=TARGET (repeatable)",
    )
    parser.add_argument(
        "--energy-sample-map-file",
        type=str,
        help="JSON object mapping sample source columns to canonical target columns",
    )
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
    parser.add_argument(
        "--synthetic-cache",
        default="use",
        choices=["use", "refresh", "off"],
        help="Synthetic recipe cache policy: use existing cache, refresh from provider, or disable cache writes/reads",
    )

    # Knowledge pack
    parser.add_argument("--knowledge-pack", type=str, help="Path to knowledge pack JSON")
    parser.add_argument("--knowledge-pack-mode", default="legacy", choices=["legacy", "pack", "auto"])

    args = parser.parse_args()

    wf = Path(args.workflow).expanduser().resolve()
    sample_paths = [Path(p) for p in (args.energy_sample_path or [])]
    sample_schema_map: Dict[str, str] = {}
    if args.energy_sample_map_file:
        try:
            loaded_map = json.loads(Path(args.energy_sample_map_file).read_text(encoding="utf-8"))
            if isinstance(loaded_map, dict):
                sample_schema_map.update({str(k): str(v) for k, v in loaded_map.items()})
        except Exception as e:
            raise SystemExit(f"Failed to read --energy-sample-map-file: {e}") from e
    for item in args.energy_sample_map or []:
        if "=" not in item:
            raise SystemExit("--energy-sample-map must use SOURCE=TARGET")
        source, target = item.split("=", 1)
        sample_schema_map[source.strip()] = target.strip()
    estimate_scenario_name = args.estimate_scenario_name
    estimate_runs_per_year = args.estimate_runs_per_year
    estimate_years = args.estimate_years
    estimate_repetitions_per_run = args.estimate_repetitions_per_run
    if args.estimate_scenario_file:
        try:
            scenario = json.loads(Path(args.estimate_scenario_file).read_text(encoding="utf-8"))
            if not isinstance(scenario, dict):
                raise ValueError("scenario file must contain a JSON object")
            estimate_scenario_name = estimate_scenario_name or scenario.get("name") or scenario.get("scenario_name")
            if estimate_runs_per_year is None and scenario.get("runs_per_year") is not None:
                estimate_runs_per_year = int(scenario["runs_per_year"])
            if scenario.get("years") is not None and args.estimate_years == 1.0:
                estimate_years = float(scenario["years"])
            if scenario.get("repetitions_per_run") is not None and args.estimate_repetitions_per_run == 1.0:
                estimate_repetitions_per_run = float(scenario["repetitions_per_run"])
        except Exception as e:
            raise SystemExit(f"Failed to read --estimate-scenario-file: {e}") from e

    config = UnifiedEnergyConfig(
        workflow_path=wf,
        orchestrator=args.orchestrator,
        out_dir=Path(args.out_dir) if args.out_dir else None,
        stdout=args.stdout,
        track_carbon=args.track_carbon,
        carbon_country_iso=args.carbon_country_iso,
        carbon_measure_power_secs=args.carbon_measure_power_secs,
        carbon_scale_factor=args.carbon_scale_factor,
        estimate_scenario_name=estimate_scenario_name,
        estimate_runs_per_year=estimate_runs_per_year,
        estimate_years=estimate_years,
        estimate_repetitions_per_run=estimate_repetitions_per_run,
        enable_energy_evaluation=args.enable_energy_eval,
        energy_mode=args.energy_mode,
        energy_sample_paths=sample_paths,
        energy_sample_schema_map=sample_schema_map,
        energy_max_rows=args.energy_max_rows,
        energy_max_tasks=args.energy_max_tasks,
        energy_timeout_s=args.energy_timeout_s,
        energy_seed=args.energy_seed,
        energy_execution_adapter=args.energy_execution_adapter,
        llm_provider=args.llm_provider,
        llm_model=args.llm_model,
        llm_api_key_env=args.llm_api_key_env,
        synthetic_cache_mode=args.synthetic_cache,
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
