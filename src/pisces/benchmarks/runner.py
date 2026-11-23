"""Run the fixture suite through standalone bodies or the existing Airflow OCI profile."""

from __future__ import annotations

import csv
from datetime import datetime, timezone
import json
from pathlib import Path
import platform
import random
import statistics
import subprocess
import sys
import time

from .catalog import CASES, PIPELINES, write_json
from ..energy.execution_sandbox import prepared_execution_sandbox
from ..energy.oci_runtime import check_oci, run_oci
from ..energy.output_equivalence import ORACLE_VERSION, verify_output
from ..energy.workload_contract import confined_path, load_workload_contract, sha256_file


def load_suite(root: Path) -> dict:
    manifest = json.loads((root / "suite.json").read_text(encoding="utf-8"))
    if (not isinstance(manifest, dict) or manifest.get("schema_version") != "1.0" or manifest.get("suite_id") != "pisces-five-pipelines-v1"
            or manifest.get("pipelines") != list(PIPELINES) or manifest.get("cases") != list(CASES)
            or not isinstance(manifest.get("files"), dict) or not manifest["files"]
            or type(manifest.get("primary_rows_per_pipeline")) is not int
            or not 10 <= manifest["primary_rows_per_pipeline"] <= 1_000_000
            or type(manifest.get("keys")) is not int or not 1 <= manifest["keys"] <= min(manifest["primary_rows_per_pipeline"], 10_000)):
        raise ValueError("Unsupported or incomplete benchmark suite manifest")
    for name, digest in manifest["files"].items():
        if sha256_file(confined_path(root, name)) != digest:
            raise ValueError(f"Suite file hash mismatch: {name}")
    # All executable bindings, references, and fixtures must be in the hashed inventory.
    for pipeline in PIPELINES:
        for case in CASES:
            workload = _load(root, pipeline, case)
            if workload.binding["target"] != manifest.get("target", "airflow"):
                raise ValueError("Suite binding target mismatch")
            paths = [workload.contract_path, workload.binding_path, workload.artifact_path, *workload.fixtures.values()]
            paths.extend(confined_path(workload.contract_path.parent, spec["reference"]["path"])
                         for spec in workload.contract["outputs"])
            if any(str(path.resolve().relative_to(root.resolve())) not in manifest["files"] for path in paths):
                raise ValueError("Suite inventory does not cover all workload files")
    return manifest


def _load(root: Path, pipeline: str, case: str):
    bundle = root / pipeline
    return load_workload_contract(contract_path=bundle / "contract.json", binding_path=bundle / f"{case}.binding.json",
                                 artifact_path=bundle / f"{case}.py")


def _expected_counts(workload) -> dict:
    counts = {}
    for spec in workload.contract["outputs"]:
        with confined_path(workload.contract_path.parent, spec["reference"]["path"]).open(encoding="utf-8", newline="") as stream:
            reader = csv.reader(stream)
            next(reader)
            counts[spec["logical_name"]] = sum(1 for _ in reader)
    return counts


def assess(case: str, proof: dict, counts: dict, *, execution_completed: bool) -> dict:
    results = {item["logical_name"]: item for item in proof.get("results", [])}
    complete_inventory = results.keys() == counts.keys() and len(proof.get("results", [])) == len(counts)
    verified = execution_completed and complete_inventory and proof.get("passed") is True
    same_shape = complete_inventory and all(results[name].get("row_count") == count for name, count in counts.items())
    if case in ("valid", "equivalent"):
        matched = verified and same_shape
    elif case == "same_shape_wrong":
        matched = (execution_completed and same_shape and proof.get("status") == "failed"
                   and results["result"].get("reason") == "output_content_mismatch"
                   and results["quality"].get("passed") is True)
    else:
        matched = (execution_completed and complete_inventory and proof.get("status") == "failed"
                   and results["result"].get("passed") is True
                   and results["quality"].get("reason") == "output_file_missing")
    return {"expected_behavior_matched": bool(matched), "output_verified": bool(verified),
            "same_shape_as_reference": same_shape,
            "fingerprints": {name: item["semantic_sha256"] for name, item in results.items()
                             if item.get("passed") is True and item.get("semantic_sha256")}}


def _standalone(workload, timeout_s: int) -> dict:
    # Approved generated task bodies only; this is not host isolation or an engine run.
    with prepared_execution_sandbox(workload) as sandbox:
        try:
            child = subprocess.run([sys.executable, str(sandbox.workflow_file)], cwd=sandbox.working,
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=timeout_s, check=False)
            completed = child.returncode == 0
            outcome = "completed" if completed else "task_body_failed"
        except subprocess.TimeoutExpired:
            completed, outcome = False, "timed_out"
        return {"execution_outcome": outcome, "execution_completed": completed,
                "returncode": child.returncode if outcome != "timed_out" else None,
                "output_assertions": verify_output(workload, workspace_root=sandbox.working)}


def _oci(workload, image: str, timeout_s: int, environment: dict) -> dict:
    payload = run_oci(workload, image=image, timeout_s=timeout_s)
    runtime = payload.get("runtime_measurement") or {}
    proof = runtime.get("output_assertions") or {}
    completed = (runtime.get("execution_outcome") == "completed" and runtime.get("execution_adapter_used") == "oci"
                 and runtime.get("workload_contract") == workload.provenance()
                 and proof.get("oracle_version") == ORACLE_VERSION
                 and all(proof.get(key) == getattr(workload, key) for key in
                         ("artifact_sha256", "contract_sha256", "binding_sha256"))
                 and all((runtime.get(key) or {}).get("passed") is True for key in ("task_coverage", "input_consumption")))
    observed = runtime.get("oci_environment") or {}
    profile_matches = all(observed.get(key) == value for key, value in environment.items() if key != "image_reference")
    return {"execution_outcome": runtime.get("execution_outcome"), "execution_completed": bool(completed and profile_matches),
            "output_assertions": proof, "oci_profile_matches": profile_matches, "payload": payload}


def _summarize(attempts: list[dict], repetitions: int, pipelines: list[str]) -> dict:
    result = {}
    for pipeline in pipelines:
        cases = {}
        for case in CASES:
            group = [attempt for attempt in attempts if attempt["pipeline"] == pipeline and attempt["case"] == case]
            cases[case] = {"planned": repetitions, "attempted": len(group),
                           "expected_behavior_matched": sum(attempt["expected_behavior_matched"] for attempt in group),
                           "verified_outputs": sum(attempt["output_verified"] for attempt in group)}
            telemetry = [((attempt.get("result", {}).get("payload") or {}).get("runtime_measurement") or {}).get("container_telemetry") or {}
                         for attempt in group]
            metrics = {}
            for name in ("wall_clock_s", "cpu_s", "memory_peak_bytes"):
                values = [item[name] for item in telemetry if type(item.get(name)) in (int, float)
                          and 0 <= item[name] < float("inf")
                          and (item.get("metric_quality", {}).get(name) or {}).get("status") == "available"]
                median = statistics.median(values) if values else None
                metrics[name] = {"available": len(values), "coverage": len(values) / repetitions, "median": median,
                                 "mad": statistics.median(abs(value - median) for value in values) if values else None}
            cases[case]["attempt_telemetry"] = metrics
        accepted = [attempt for attempt in attempts if attempt["pipeline"] == pipeline and attempt["case"] in ("valid", "equivalent")]
        identities = {tuple(sorted(attempt["fingerprints"].items())) for attempt in accepted if attempt["output_verified"]}
        equivalent = len(accepted) == 2 * repetitions and all(attempt["output_verified"] for attempt in accepted) and len(identities) == 1
        result[pipeline] = {"cases": cases, "positive_exact_equivalence": "passed" if equivalent else "not_established"}
    passed = all(item["positive_exact_equivalence"] == "passed" and all(case["expected_behavior_matched"] == repetitions
                 for case in item["cases"].values()) for item in result.values())
    return {"status": "passed" if passed else "failed", "pipelines": result,
            "claim": "Semantic discrimination on deterministic fixtures, not general performance or carbon efficiency",
            "telemetry_role": "attempt diagnostics, including intentionally invalid and incomplete work"}


def run_suite(root: Path, result_dir: Path, *, backend: str = "standalone", image: str | None = None,
              repetitions: int = 1, timeout_s: int = 90, seed: int = 1729,
              pipelines: list[str] | None = None) -> dict:
    if backend not in ("standalone", "oci") or not 1 <= repetitions <= 20 or not 1 <= timeout_s <= 300:
        raise ValueError("Select standalone/oci, 1-20 repetitions and a timeout of 1-300 seconds")
    if (backend == "oci") != bool(image):
        raise ValueError("--oci-image is required only with --backend oci")
    selected = list(PIPELINES) if pipelines is None else pipelines
    if not selected or len(set(selected)) != len(selected) or not set(selected) <= PIPELINES.keys():
        raise ValueError("Select distinct known pipelines")
    manifest = load_suite(root)
    environment = None
    if backend == "oci":
        probe = check_oci(image)
        if not probe.get("ready"):
            raise ValueError(f"OCI readiness failed: {probe.get('reason')}")
        environment = probe["environment"]
        if environment.get("runtime_target", "airflow") != manifest.get("target", "airflow"):
            raise ValueError("OCI image and suite target must match")
        image = environment["image_id"]
    workloads = {(pipeline, case): _load(root, pipeline, case) for pipeline in selected for case in CASES}
    counts = {pipeline: _expected_counts(workloads[pipeline, "valid"]) for pipeline in selected}
    order = [(repetition, pipeline, case) for repetition in range(1, repetitions + 1) for pipeline in selected for case in CASES]
    random.Random(seed).shuffle(order)
    result_dir.mkdir(parents=True, exist_ok=False)
    write_json(result_dir / "manifest.json", {
        "schema_version": "1.0", "suite_sha256": sha256_file(root / "suite.json"),
        "suite_id": manifest["suite_id"], "primary_rows_per_pipeline": manifest["primary_rows_per_pipeline"], "keys": manifest["keys"],
        "started_at_utc": datetime.now(timezone.utc).isoformat(), "backend": backend,
        "execution_scope": "standalone_task_bodies" if backend == "standalone" else manifest.get("target", "airflow") + "_oci_whole_workflow",
        "controller_platform": platform.platform(), "oci_environment": environment, "seed": seed, "order": order,
        "timeout_s": timeout_s, "repetitions": repetitions, "oracle_version": ORACLE_VERSION,
        "cache_policy": ("fresh_workspace_and_process_os_cache_uncontrolled" if backend == "standalone"
                         else "fresh_container_and_database_os_cache_uncontrolled"),
        "workloads": {f"{pipeline}/{case}": workload.provenance() for (pipeline, case), workload in workloads.items()},
    })
    attempts = []
    for repetition, pipeline, case in order:
        workload = workloads[pipeline, case]
        started_at = datetime.now(timezone.utc).isoformat()
        start = time.perf_counter_ns()
        result = _standalone(workload, timeout_s) if backend == "standalone" else _oci(workload, image, timeout_s, environment)
        outcome = assess(case, result["output_assertions"], counts[pipeline], execution_completed=result["execution_completed"])
        attempt = {"pipeline": pipeline, "case": case, "repetition": repetition, "started_at_utc": started_at,
                   "controller_attempt_elapsed_s": (time.perf_counter_ns() - start) / 1e9,
                   "provenance": workload.provenance(), **outcome, "result": result}
        write_json(result_dir / f"{pipeline}-{case}-{repetition:02d}.json", attempt)
        attempts.append(attempt)
        label = "matched" if outcome["expected_behavior_matched"] else "UNEXPECTED"
        print(f"{pipeline} {case} [{repetition}/{repetitions}]: {label}", flush=True)
        runtime = (result.get("payload") or {}).get("runtime_measurement") or {}
        if (runtime.get("container_lifecycle") or {}).get("cleanup_error"):
            break
    summary = _summarize(attempts, repetitions, selected)
    if any((((a["result"].get("payload") or {}).get("runtime_measurement") or {}).get("container_lifecycle") or {}).get("cleanup_error")
           for a in attempts):
        summary.update(status="failed", reason="container_cleanup_failed")
    write_json(result_dir / "summary.json", summary)
    return summary
