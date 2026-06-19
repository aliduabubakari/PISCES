#!/usr/bin/env python3
"""
PISCES Interactive CLI
======================

Interactive command-line interface for PISCES sustainability evaluation.
Supports guided mode for building evaluation commands step by step.

Usage:
    pisces evaluate path/to/workflow.py --track-carbon
    pisces diagnostics path/to/workflow.py
    pisces agent
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import List


def _print_energy_mode_help() -> None:
    print("""
Energy Evaluation Modes:
  auto       Try sample if provided, otherwise fuzz_fill → zero_fill → synthetic → heuristic
  zero_fill  AST-inferred trivial data (all zeros), no LLM needed
  fuzz_fill  AST-inferred deterministic non-zero data, no LLM needed
  scale_probe Run deterministic generated workloads at multiple row counts
  operator_profile  Static tier-weighted representative workload
  sample     Use provided sample data for runtime measurement
  synthetic  Use LLM-generated synthetic data for runtime measurement
  heuristic  Static eco-design rules only (no runtime measurement)
""")


def _confirm(prompt: str, default: bool = False) -> bool:
    """Simple confirmation prompt compatible with no-dependency CLI."""
    suffix = " [Y/n]: " if default else " [y/N]: "
    try:
        resp = input(prompt + suffix).strip().lower()
    except (EOFError, KeyboardInterrupt):
        return default
    if not resp:
        return default
    return resp.startswith("y")


def agent():
    """Guided interactive command builder for PISCES evaluation."""
    print("=" * 60)
    print("  PISCES — Pipeline Sustainability Evaluation Agent")
    print("=" * 60)
    print()

    try:
        wf_path = input("Workflow file path: ").strip()
    except (EOFError, KeyboardInterrupt):
        print("\nCancelled.")
        return

    if not wf_path:
        print("No workflow path provided. Exiting.")
        return

    parts: List[str] = ["pisces-unified", wf_path]

    track_carbon = _confirm("Enable carbon tracking?", default=False)
    if track_carbon:
        parts.append("--track-carbon")
        try:
            iso = input("  Country ISO code (e.g., ITA, GBR) [skip]: ").strip()
            if iso:
                parts.extend(["--carbon-country-iso", iso])
        except (EOFError, KeyboardInterrupt):
            pass
        try:
            scale = input("  Carbon scale factor [1.0]: ").strip()
            if scale:
                parts.extend(["--carbon-scale-factor", scale])
        except (EOFError, KeyboardInterrupt):
            pass

    enable_energy = _confirm("Enable runtime energy evaluation?", default=False)
    if enable_energy:
        parts.append("--enable-energy-eval")
        _print_energy_mode_help()
        try:
            emode = input("  Energy mode [auto]: ").strip()
            if emode:
                parts.extend(["--energy-mode", emode])
        except (EOFError, KeyboardInterrupt):
            pass
        try:
            adapter = input("  Execution adapter (representative|native|auto) [representative]: ").strip()
            if adapter:
                parts.extend(["--energy-execution-adapter", adapter])
        except (EOFError, KeyboardInterrupt):
            pass
        try:
            cache_mode = input("  Synthetic cache (use|refresh|off) [use]: ").strip()
            if cache_mode:
                parts.extend(["--synthetic-cache", cache_mode])
        except (EOFError, KeyboardInterrupt):
            pass

    try:
        stdout = _confirm("Print report to stdout? (otherwise saved to file)", default=False)
    except (EOFError, KeyboardInterrupt):
        stdout = False
    if stdout:
        parts.append("--stdout")
    else:
        try:
            out_dir = input("  Output directory [./pisces_reports]: ").strip()
            if out_dir:
                parts.extend(["--out-dir", out_dir])
        except (EOFError, KeyboardInterrupt):
            pass

    print()
    print("Generated command:")
    print("  " + " ".join(parts))
    print()

    try:
        execute = _confirm("Execute this command now?", default=True)
    except (EOFError, KeyboardInterrupt):
        execute = False

    if execute:
        import subprocess
        print("\nRunning...\n")
        subprocess.run([sys.executable, "-m", "pisces.unified_evaluator"] + parts[1:])
    else:
        print("Command ready. Copy and run it manually.")


def main():
    """Main CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description="PISCES — Pipeline Intelligent Sustainability & Carbon Evaluation Suite",
    )
    sub = parser.add_subparsers(dest="command", help="Available commands")

    # evaluate subcommand
    eval_parser = sub.add_parser("evaluate", help="Run unified energy evaluation on a workflow file")
    eval_parser.add_argument("workflow", type=str, help="Path to workflow Python file")
    eval_parser.add_argument("--track-carbon", action="store_true")
    eval_parser.add_argument("--carbon-country-iso", type=str)
    eval_parser.add_argument("--carbon-measure-power-secs", type=int, default=1)
    eval_parser.add_argument("--carbon-scale-factor", type=float, default=1.0)
    eval_parser.add_argument("--estimate-scenario-file")
    eval_parser.add_argument("--estimate-scenario-name")
    eval_parser.add_argument("--estimate-runs-per-year", type=int)
    eval_parser.add_argument("--estimate-years", type=float, default=1.0)
    eval_parser.add_argument("--estimate-repetitions-per-run", type=float, default=1.0)
    eval_parser.add_argument("--enable-energy-eval", action="store_true")
    eval_parser.add_argument(
        "--energy-mode",
        default="auto",
        choices=["auto", "zero_fill", "fuzz_fill", "scale_probe", "operator_profile", "sample", "synthetic", "heuristic"],
    )
    eval_parser.add_argument("--energy-seed", type=int, default=1729)
    eval_parser.add_argument("--energy-max-rows", type=int, default=500)
    eval_parser.add_argument("--energy-max-tasks", type=int, default=25)
    eval_parser.add_argument("--energy-timeout-s", type=int, default=120)
    eval_parser.add_argument("--energy-execution-adapter", default="representative", choices=["representative", "native", "auto"])
    eval_parser.add_argument("--energy-sample-path", action="append", default=[])
    eval_parser.add_argument("--energy-sample-map", action="append", default=[], help="Sample schema mapping as SOURCE=TARGET")
    eval_parser.add_argument("--energy-sample-map-file", help="JSON object mapping sample source columns to target columns")
    eval_parser.add_argument("--llm-provider", default="openrouter")
    eval_parser.add_argument("--llm-model", default="openai/gpt-oss-120b")
    eval_parser.add_argument("--llm-api-key-env")
    eval_parser.add_argument("--synthetic-cache", default="use", choices=["use", "refresh", "off"])
    eval_parser.add_argument("--out-dir", type=str)
    eval_parser.add_argument("--stdout", action="store_true")

    diag_parser = sub.add_parser("diagnostics", help="Report environment readiness before evaluation")
    diag_parser.add_argument("workflow", nargs="?", help="Optional workflow file to inspect")
    diag_parser.add_argument("--orchestrator", default="auto", choices=["auto", "airflow", "prefect", "dagster", "argo", "kestra"])
    diag_parser.add_argument("--llm-provider", default="openrouter")
    diag_parser.add_argument("--llm-model", default="openai/gpt-oss-120b")
    diag_parser.add_argument("--llm-api-key-env")
    diag_parser.add_argument("--carbon-country-iso")
    diag_parser.add_argument("--carbon-measure-power-secs", type=int, default=1)
    diag_parser.add_argument("--skip-codecarbon-smoke", action="store_true")
    diag_parser.add_argument("--out", type=str)

    # agent subcommand
    sub.add_parser("agent", help="Interactive guided command builder")

    args = parser.parse_args()

    if args.command == "agent":
        agent()
    elif args.command == "evaluate":
        from .unified_evaluator import UnifiedEnergyConfig, run_unified_energy_evaluation

        sample_schema_map = {}
        if args.energy_sample_map_file:
            loaded_map = json.loads(Path(args.energy_sample_map_file).read_text(encoding="utf-8"))
            if isinstance(loaded_map, dict):
                sample_schema_map.update({str(k): str(v) for k, v in loaded_map.items()})
        for item in args.energy_sample_map:
            if "=" not in item:
                raise SystemExit("--energy-sample-map must use SOURCE=TARGET")
            source, target = item.split("=", 1)
            sample_schema_map[source.strip()] = target.strip()
        estimate_scenario_name = args.estimate_scenario_name
        estimate_runs_per_year = args.estimate_runs_per_year
        estimate_years = args.estimate_years
        estimate_repetitions_per_run = args.estimate_repetitions_per_run
        if args.estimate_scenario_file:
            scenario = json.loads(Path(args.estimate_scenario_file).read_text(encoding="utf-8"))
            if not isinstance(scenario, dict):
                raise SystemExit("--estimate-scenario-file must contain a JSON object")
            estimate_scenario_name = estimate_scenario_name or scenario.get("name") or scenario.get("scenario_name")
            if estimate_runs_per_year is None and scenario.get("runs_per_year") is not None:
                estimate_runs_per_year = int(scenario["runs_per_year"])
            if scenario.get("years") is not None and args.estimate_years == 1.0:
                estimate_years = float(scenario["years"])
            if scenario.get("repetitions_per_run") is not None and args.estimate_repetitions_per_run == 1.0:
                estimate_repetitions_per_run = float(scenario["repetitions_per_run"])

        config = UnifiedEnergyConfig(
            workflow_path=Path(args.workflow),
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
            energy_seed=args.energy_seed,
            energy_max_rows=args.energy_max_rows,
            energy_max_tasks=args.energy_max_tasks,
            energy_timeout_s=args.energy_timeout_s,
            energy_execution_adapter=args.energy_execution_adapter,
            energy_sample_paths=[Path(p) for p in args.energy_sample_path],
            energy_sample_schema_map=sample_schema_map,
            llm_provider=args.llm_provider,
            llm_model=args.llm_model,
            llm_api_key_env=args.llm_api_key_env,
            synthetic_cache_mode=args.synthetic_cache,
            out_dir=Path(args.out_dir) if args.out_dir else None,
            stdout=args.stdout,
        )
        report = run_unified_energy_evaluation(config)
        if args.stdout:
            print(json.dumps(report, indent=2, default=str))
        else:
            out_dir = config.out_dir
            out_dir.mkdir(parents=True, exist_ok=True)
            from datetime import datetime
            wf = Path(args.workflow)
            out_path = out_dir / f"pisces_{wf.stem}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            out_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
            print(f"Report written to: {out_path}")
    elif args.command == "diagnostics":
        from .diagnostics import DiagnosticsConfig, collect_diagnostics

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
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
