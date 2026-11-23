"""Generate and qualify the five-pipeline PISCES fixture suite."""

from __future__ import annotations

import argparse
from pathlib import Path

from .catalog import PIPELINES, build_suite
from .runner import run_suite


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("list", help="List available pipeline families")
    build = commands.add_parser("build", help="Generate a new immutable fixture suite")
    build.add_argument("directory", type=Path)
    build.add_argument("--rows", type=int, default=1000, help="Primary input records per pipeline, total across Climate Fusion sources")
    build.add_argument("--keys", type=int, default=32, help="Group, customer, sensor and item cardinality")
    build.add_argument("--target", choices=("airflow", "prefect", "dagster"), default="airflow")
    run = commands.add_parser("run", help="Execute approved generated artifacts and check all four cases")
    run.add_argument("suite", type=Path)
    run.add_argument("result_dir", type=Path)
    run.add_argument("--backend", choices=("standalone", "oci"), default="standalone")
    run.add_argument("--oci-image", help="Immutable local Airflow OCI image ID or digest")
    run.add_argument("--pipeline", action="append", choices=PIPELINES, help="Repeat to select a subset; default: all five")
    run.add_argument("--repetitions", type=int, default=1)
    run.add_argument("--timeout-s", type=int, default=90)
    run.add_argument("--seed", type=int, default=1729)
    args = parser.parse_args()
    try:
        if args.command == "list":
            for name, config in PIPELINES.items():
                print(f"{name}: {config['description']}")
        elif args.command == "build":
            build_suite(args.directory, rows=args.rows, keys=args.keys, target=args.target)
            print(args.directory / "suite.json")
        else:
            summary = run_suite(args.suite, args.result_dir, backend=args.backend, image=args.oci_image,
                                repetitions=args.repetitions, timeout_s=args.timeout_s, seed=args.seed, pipelines=args.pipeline)
            print(f"Suite {summary['status']}: {args.result_dir / 'summary.json'}")
            return int(summary["status"] != "passed")
    except (OSError, ValueError) as exc:
        parser.error(str(exc))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
