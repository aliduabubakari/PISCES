"""Build and exercise a deterministic climate-fusion contract bundle."""

from __future__ import annotations

import argparse
import csv
from decimal import Decimal
import json
from pathlib import Path
import subprocess
import sys
from typing import Any

from pisces.energy.output_equivalence import compare_outputs, verify_output
from pisces.energy.task_timing import FILENAME
from pisces.energy.workload_contract import load_workload_contract, sha256_file


SOURCES = ("bom", "ecmwf", "jma", "metoffice", "noaa")
VARIANTS = ("baseline", "single_read", "wrong_units")
HEADER = ("source", "station_id", "temperature_c")
SEED = 1729
_WORKFLOW = '''import csv
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

SOURCES = ("bom", "ecmwf", "jma", "metoffice", "noaa")
VARIANT = __VARIANT__

def fuse():
    destination = Path("outputs/fusion.csv")
    destination.parent.mkdir(exist_ok=True)
    with destination.open("w", encoding="utf-8", newline="") as output:
        writer = csv.writer(output)
        writer.writerow(("source", "station_id", "temperature_c"))
        sources = SOURCES if VARIANT == "baseline" else reversed(SOURCES)
        for source in sources:
            path = Path("inputs") / (source + ".csv")
            if VARIANT == "baseline":
                with path.open(encoding="utf-8", newline="") as stream:
                    for _ in csv.DictReader(stream):
                        pass
            with path.open(encoding="utf-8", newline="") as stream:
                for row in csv.DictReader(stream):
                    value = Decimal(row["temperature"])
                    if row["unit"] == "F" and VARIANT != "wrong_units":
                        value = (value - 32) * 5 / 9
                    writer.writerow((source, row["station_id"], format(value, ".2f")))

if __name__ == "__main__":
    fuse()
else:
    from airflow.sdk import DAG
    from airflow.providers.standard.operators.python import PythonOperator

    with DAG(dag_id="climate_fusion_" + VARIANT, schedule=None, catchup=False,
             start_date=datetime(2024, 1, 1, tzinfo=timezone.utc)) as dag:
        PythonOperator(task_id="fuse", python_callable=fuse, retries=0)
'''

_TIMED_CALLABLE = '''def timed_fuse():
    import json
    from time import perf_counter_ns, thread_time_ns

    wall_start = perf_counter_ns()
    cpu_start = thread_time_ns()
    fuse()
    cpu_end = thread_time_ns()
    wall_end = perf_counter_ns()
    Path(__TIMING_FILENAME__).write_text(json.dumps({
        "task_id": "fuse", "wall_start_ns": wall_start, "wall_end_ns": wall_end,
        "thread_cpu_start_ns": cpu_start, "thread_cpu_end_ns": cpu_end,
    }), encoding="ascii")

'''


def _json(path: Path, value: dict[str, Any]) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def build_bundle(root: Path, *, rows_per_source: int, scaling: bool = False, task_timing: bool = False) -> None:
    if task_timing and not scaling:
        raise ValueError("Task timing is only supported by the load sweep baseline")
    limit = 200_000 if scaling else 20_000
    if not 1 <= rows_per_source <= limit:
        raise ValueError(f"rows_per_source must be between 1 and {limit}")
    root.mkdir(parents=True, exist_ok=False)
    fixtures = root / "fixtures"
    fixtures.mkdir()
    inputs = []
    reference = root / "expected.csv"
    # Construct the normalized truth first, then encode the source observations.
    # No candidate implementation is imported to build the reference.
    with reference.open("w", encoding="utf-8", newline="") as expected:
        truth = csv.writer(expected, lineterminator="\n")
        truth.writerow(HEADER)
        for index, source in enumerate(SOURCES):
            path = fixtures / f"{source}.csv"
            with path.open("w", encoding="utf-8", newline="") as raw:
                observed = csv.writer(raw, lineterminator="\n")
                observed.writerow(("station_id", "temperature", "unit"))
                for station in range(rows_per_source):
                    celsius = Decimal((SEED + 17 * station + 43 * index) % 601 - 200) / 10
                    truth.writerow((source, station, celsius))
                    if source == "noaa":
                        observed.writerow((station, celsius * Decimal("1.8") + 32, "F"))
                    else:
                        observed.writerow((station, celsius, "C"))
            inputs.append({
                "integration_id": source, "kind": "file", "fixture": f"fixtures/{source}.csv",
                "sha256": sha256_file(path),
            })
    _json(root / "contract.json", {
        "schema_version": "2.0", "contract_id": f"climate-fusion-{rows_per_source}",
        "pipeline_id": "climate-fusion", "functional_unit": "valid_batch",
        "inputs": inputs,
        "outputs": [{
            "logical_name": "fusion", "format": "csv", "row_order": "key_sorted" if scaling else "ignore",
            "primary_key": ["source", "station_id"],
            "reference": {"path": "expected.csv", "sha256": sha256_file(reference)},
            "columns": [
                {"name": "source", "type": "string", "nullable": False},
                {"name": "station_id", "type": "integer", "nullable": False},
                {"name": "temperature_c", "type": "decimal", "nullable": False, "unit": "C"},
            ],
        }],
    })
    variants = ("baseline",) if scaling else VARIANTS
    for name in variants:
        artifact = root / f"{name}.py"
        source = _WORKFLOW.replace("__VARIANT__", repr(name))
        if task_timing:
            wrapper = _TIMED_CALLABLE.replace("__TIMING_FILENAME__", repr(FILENAME))
            source = source.replace('if __name__ == "__main__":', wrapper + 'if __name__ == "__main__":')
            source = source.replace("    fuse()\nelse:", "    timed_fuse()\nelse:")
            source = source.replace("python_callable=fuse,", "python_callable=timed_fuse,")
        artifact.write_text(source, encoding="utf-8")
        _json(root / f"{name}.binding.json", {
            "schema_version": "2.0", "binding_id": name, "contract_id": f"climate-fusion-{rows_per_source}",
            "target": "airflow", "artifact_path": artifact.name, "artifact_sha256": sha256_file(artifact),
            "file_bindings": {source: f"inputs/{source}.csv" for source in SOURCES},
            "integration_bindings": {}, "output_bindings": {"fusion": "outputs/fusion.csv"},
            "expected_task_ids": ["fuse"],
        })
    _json(root / "manifest.json", {
        "schema_version": "1.0", "experiment_id": "climate-fusion-semantic-check",
        "seed": SEED, "rows_per_source": rows_per_source, "run_order": list(variants),
        "execution": "standalone_task_body", "python_version": sys.version.split()[0],
        "warmups": 0, "repetitions": 1, "timeout_s": 60, "scope": "semantic_verification_only",
        "oracle_version": "csv-reference-v1",
        "contract_sha256": sha256_file(root / "contract.json"),
        "binding_sha256": {name: sha256_file(root / f"{name}.binding.json") for name in variants},
    })


def run_bundle(root: Path) -> dict[str, Any]:
    from pisces.energy.execution_sandbox import prepared_execution_sandbox
    from contextlib import ExitStack

    workloads = {
        name: load_workload_contract(
            contract_path=root / "contract.json", binding_path=root / f"{name}.binding.json",
            artifact_path=root / f"{name}.py",
        )
        for name in VARIANTS
    }
    with ExitStack() as stack:
        workspaces = {}
        for name, workload in workloads.items():
            sandbox = stack.enter_context(prepared_execution_sandbox(workload))
            subprocess.run([sys.executable, str(sandbox.workflow_file)], cwd=sandbox.working, check=True, timeout=60)
            workspaces[name] = sandbox.working
        verdict = {
            "execution": "standalone_task_body",
            "verification": {
                name: verify_output(workload, workspace_root=workspaces[name])
                for name, workload in workloads.items()
            },
            "pairwise": {
                name: compare_outputs(
                    workloads["baseline"], workloads[name],
                    left_workspace=workspaces["baseline"], right_workspace=workspaces[name],
                )
                for name in VARIANTS[1:]
            },
        }
    _json(root / "verdicts.json", verdict)
    return verdict


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("bundle", type=Path, help="New directory for the generated benchmark")
    parser.add_argument("--rows-per-source", type=int, default=8)
    args = parser.parse_args()
    root = args.bundle.resolve()
    build_bundle(root, rows_per_source=args.rows_per_source)
    verdict = run_bundle(root)
    print(root / "verdicts.json")
    if [verdict["verification"][name]["status"] for name in VARIANTS] != ["passed", "passed", "failed"]:
        raise SystemExit(1)
    if [verdict["pairwise"][name]["status"] for name in VARIANTS[1:]] != ["passed", "failed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
