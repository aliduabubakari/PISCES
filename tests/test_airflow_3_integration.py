from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from benchmarks.climate_fusion.benchmark import VARIANTS, build_bundle
from pisces.energy.paired_experiment import PairedPlan, run_paired_experiment
from pisces.energy.workload_contract import load_workload_contract, sha256_file
from pisces.report_validation import validate_report


@pytest.fixture(scope="module")
def airflow_python() -> Path:
    configured = os.environ.get("PISCES_AIRFLOW3_PYTHON")
    if not configured:
        pytest.skip("Set PISCES_AIRFLOW3_PYTHON to run the pinned Airflow integration test")
    python = Path(configured).expanduser().absolute()
    result = subprocess.run(
        [str(python), "-c", "from importlib.metadata import version; print(version('apache-airflow'))"],
        capture_output=True, text=True, timeout=15, check=True,
    )
    assert result.stdout.strip() == "3.4.0"
    return python


@pytest.mark.parametrize("variant", (*VARIANTS, "declared_retries"))
def test_airflow_3_climate_fusion(variant: str, airflow_python: Path, tmp_path: Path) -> None:
    bundle = tmp_path / "bundle"
    build_bundle(bundle, rows_per_source=8)
    (bundle / "unrelated.py").write_text(
        "from airflow.sdk import DAG\nraise RuntimeError('unexpected DAG import')\n", encoding="utf-8",
    )
    artifact_name = "baseline" if variant == "declared_retries" else variant
    artifact = bundle / f"{artifact_name}.py"
    binding = bundle / f"{artifact_name}.binding.json"
    if variant == "declared_retries":
        artifact.write_text(artifact.read_text(encoding="utf-8").replace("retries=0", "retries=1"), encoding="utf-8")
        spec = json.loads(binding.read_text(encoding="utf-8"))
        spec["artifact_sha256"] = sha256_file(artifact)
        binding.write_text(json.dumps(spec), encoding="utf-8")
    report_path = bundle / "native.json"
    proc = subprocess.run(
        [
            sys.executable, "-m", "pisces.unified_evaluator", str(artifact),
            "--orchestrator", "airflow", "--enable-energy-eval", "--energy-mode", "zero_fill",
            "--energy-execution-adapter", "native", "--energy-runtime-python", str(airflow_python),
            "--experiment-mode", "--energy-maximum-repetitions", "1",
            "--workload-contract", str(bundle / "contract.json"),
            "--artifact-binding", str(binding),
            "--out-file", str(report_path),
        ],
        env={**os.environ, "AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": "not-json"},
        capture_output=True, text=True, timeout=75,
    )
    assert report_path.is_file(), proc.stderr
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert validate_report(report).status == "current_inventory"
    runtime = report["energy"]["measurements"]["runtime"]
    assert runtime["native_environment"]["airflow_version"] == "3.4.0"
    assert runtime["native_environment"]["database_initialization"] == "airflow db migrate"
    assert runtime["native_environment"]["executor"] == "not_used"
    if variant == "declared_retries":
        assert proc.returncode == 2
        assert runtime["execution_outcome"] == "failed"
        assert runtime["error"]["message"] == "airflow_3_dag_test_requires_zero_declared_retries"
        assert runtime.get("output_assertions") is None
        return
    assert runtime["execution_outcome"] == "completed", runtime
    assert runtime["task_coverage"]["passed"] and runtime["input_consumption"]["passed"]
    assert runtime["output_assertions"]["results"][0]["row_count"] == 40
    assert runtime["pairwise_equivalence"]["status"] == "not_evaluated"
    assert runtime["eligible_for_resource_comparison"] is False
    if variant == "wrong_units":
        assert proc.returncode == 2
        assert runtime["output_assertions"]["results"][0]["reason"] == "output_content_mismatch"
    else:
        assert proc.returncode == 0
        assert runtime["output_assertions"]["status"] == "passed"


def test_airflow_3_paired_campaign(airflow_python: Path, tmp_path: Path) -> None:
    bundle = tmp_path / "bundle"
    build_bundle(bundle, rows_per_source=8)
    workloads = [
        load_workload_contract(
            contract_path=bundle / "contract.json", binding_path=bundle / f"{name}.binding.json",
            artifact_path=bundle / f"{name}.py",
        )
        for name in ("baseline", "single_read")
    ]
    result = run_paired_experiment(
        *workloads, runtime_python=airflow_python, result_dir=tmp_path / "campaign", plan=PairedPlan(pairs=2),
    )
    assert result["eligible_pairs"] == 2
    assert result["verified_attempts"] == {"baseline": 2, "candidate": 2}
    assert result["status"] == "inconclusive"
    assert result["reason"] == "insufficient_repetitions_for_interval"
