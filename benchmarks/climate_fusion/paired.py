"""Run a fresh-process paired experiment on a generated climate-fusion bundle."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pisces.energy.paired_experiment import ADDITIVE_METRICS, PairedPlan, run_paired_experiment
from pisces.energy.workload_contract import load_workload_contract


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("bundle", type=Path)
    parser.add_argument("result_dir", type=Path)
    parser.add_argument("--runtime-python", type=Path, required=True)
    parser.add_argument("--candidate", choices=["single_read", "wrong_units"], default="single_read")
    parser.add_argument("--pairs", type=int, default=6)
    parser.add_argument("--seed", type=int, default=1729)
    parser.add_argument("--metric", choices=ADDITIVE_METRICS, default="wall_clock_s")
    parser.add_argument("--minimum-effect", type=float, default=0.05)
    parser.add_argument("--precision-goal", type=float, default=0.10)
    parser.add_argument("--minimum-pair-fraction", type=float, default=0.8)
    args = parser.parse_args()

    def load(name: str):
        return load_workload_contract(
            contract_path=args.bundle / "contract.json", binding_path=args.bundle / f"{name}.binding.json",
            artifact_path=args.bundle / f"{name}.py",
        )

    plan = PairedPlan(
        pairs=args.pairs, seed=args.seed, metric=args.metric, minimum_effect=args.minimum_effect,
        precision_goal=args.precision_goal, minimum_pair_fraction=args.minimum_pair_fraction,
    )
    result = run_paired_experiment(
        load("baseline"), load(args.candidate), runtime_python=args.runtime_python,
        result_dir=args.result_dir, plan=plan,
    )
    print(json.dumps(result, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
