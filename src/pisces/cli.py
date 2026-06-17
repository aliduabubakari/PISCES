#!/usr/bin/env python3
"""
PISCES Interactive CLI
======================

Interactive command-line interface for PISCES sustainability evaluation.
Supports guided mode for building evaluation commands step by step.

Usage:
    pisces evaluate path/to/workflow.py --track-carbon
    pisces agent
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import List, Optional


def _print_energy_mode_help() -> None:
    print("""
Energy Evaluation Modes:
  auto       Try zero_fill → sample → synthetic → heuristic (default)
  zero_fill  AST-inferred trivial data (all zeros), no LLM needed
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
    eval_parser.add_argument("--carbon-scale-factor", type=float, default=1.0)
    eval_parser.add_argument("--enable-energy-eval", action="store_true")
    eval_parser.add_argument("--energy-mode", default="auto")
    eval_parser.add_argument("--out-dir", type=str)
    eval_parser.add_argument("--stdout", action="store_true")

    # agent subcommand
    sub.add_parser("agent", help="Interactive guided command builder")

    args = parser.parse_args()

    if args.command == "agent":
        agent()
    elif args.command == "evaluate":
        from .unified_evaluator import UnifiedEnergyConfig, run_unified_energy_evaluation

        config = UnifiedEnergyConfig(
            workflow_path=Path(args.workflow),
            track_carbon=args.track_carbon,
            carbon_country_iso=args.carbon_country_iso,
            carbon_scale_factor=args.carbon_scale_factor,
            enable_energy_evaluation=args.enable_energy_eval,
            energy_mode=args.energy_mode,
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
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
