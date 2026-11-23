from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

from benchmarks.climate_fusion.benchmark import build_bundle
from pisces.energy import paired_experiment as paired
from pisces.energy.output_equivalence import ORACLE_VERSION
from pisces.energy.workload_contract import load_workload_contract, sha256_file


@pytest.fixture
def workloads(tmp_path: Path):
    bundle = tmp_path / "bundle"
    build_bundle(bundle, rows_per_source=2)
    return tuple(
        load_workload_contract(
            contract_path=bundle / "contract.json", binding_path=bundle / f"{name}.binding.json",
            artifact_path=bundle / f"{name}.py",
        )
        for name in ("baseline", "single_read")
    )


def _payload(workload, *, candidate=False, mode="ok"):
    proof = workload.provenance()
    return {
        "ok": mode not in {"failed", "partial", "zero_work"},
        "runtime_measurement": {
            "status": "failed" if mode in {"failed", "partial", "zero_work"} else "ok",
            "execution_outcome": mode if mode in {"failed", "partial", "zero_work"} else "completed",
            "execution_adapter_used": "native",
            "workload_contract": proof,
            "execution_binding": {"bound_workflow_sha256": workload.artifact_sha256},
            "warmup": {
                "requested_repetitions": int(mode == "warm"),
                "completed_repetitions": int(mode == "warm"),
                "included_in_measurement_window": False,
            },
            "bounds": {"repetitions_completed": 1, "maximum_repetitions": 1},
            "task_coverage": {"passed": True},
            "input_consumption": {"passed": True},
            "output_assertions": {
                "passed": True,
                "oracle_version": ORACLE_VERSION,
                **{key: proof[key] for key in ("artifact_sha256", "binding_sha256", "contract_sha256")},
                "results": [
                    {
                        "logical_name": name, "reference_sha256": reference,
                        "passed": True, "semantic_sha256": ("b" if mode == "wrong_output" else "a") * 64,
                    }
                    for name, reference in proof["reference_sha256"].items()
                ],
            },
            "runner_environment": {"distributions_sha256": ("b" if mode == "environment" else "a") * 64},
            "native_environment": {
                **{key: paired.PROFILE[key] for key in ("airflow_version", "execution_interface", "database", "executor")},
                "initialization_in_measurement_window": False,
            },
            "eligible_for_paired_equivalence_comparison": True,
            "operational_metrics": {
                "wall_clock_s": 1 if candidate else 2, "process_cpu_s": 1,
                "peak_rss_mb": 100, "measurement_scope": "bounded_execution_block",
                "clock_sources": {"wall_clock": "time.perf_counter", "process_cpu": "time.process_time"},
            },
        },
    }


def _run(tmp_path, monkeypatch, workloads, modes, *, minimum_pair_fraction=0.8):
    calls = dict.fromkeys(paired.ROLES, 0)
    plan = paired.PairedPlan(pairs=len(modes), minimum_pair_fraction=minimum_pair_fraction)

    def fake_run(*, request, collect_modeled_energy, **kwargs):
        assert request.measurement_maximum_repetitions == 1
        assert request.measurement_warmup_repetitions == 0
        assert collect_modeled_energy is False
        index = int(request.file_path == workloads[1].artifact_path)
        role = paired.ROLES[index]
        mode = modes[calls[role]][index]
        calls[role] += 1
        if mode == "timeout":
            return {"ok": False, "_subprocess": {"timed_out": True}}
        return _payload(workloads[index], candidate=bool(index), mode=mode)

    monkeypatch.setattr(paired, "_run_runtime_measurement", fake_run)
    monkeypatch.setattr(paired, "detect_orchestrator_version", lambda *args, **kwargs: "3.4.0")
    root = tmp_path / "run"
    summary = paired.run_paired_experiment(
        *workloads, runtime_python=Path(sys.executable), result_dir=root, plan=plan,
    )
    assert calls == dict.fromkeys(paired.ROLES, len(modes))
    return root, summary


def test_schedule_is_seeded_and_counterbalanced():
    plan = paired.PairedPlan(pairs=6, seed=42)
    assert plan.schedule() == plan.schedule()
    assert plan.schedule().count(("baseline", "candidate")) == 3
    assert plan.schedule().count(("candidate", "baseline")) == 3


@pytest.mark.parametrize("failed_block", [0, 2])
def test_failed_attempts_never_enter_verified_totals(tmp_path, monkeypatch, workloads, failed_block):
    modes = [("ok", "ok")] * 3
    modes[failed_block] = ("ok", "failed")
    root, summary = _run(tmp_path, monkeypatch, workloads, modes, minimum_pair_fraction=0.5)
    assert summary["eligible_pairs"] == 2
    assert summary["verified_attempts"] == {"baseline": 3, "candidate": 2}
    totals = summary["paired_resource_totals"]
    assert totals["baseline"] == {
        "verified_units": 2,
        "additive_totals": {"wall_clock_s": 4, "process_cpu_s": 2},
        "per_valid_batch": {"wall_clock_s": 2, "process_cpu_s": 1},
    }
    assert summary["reason"] == "insufficient_repetitions_for_interval"
    attempt = root / f"pair-{failed_block + 1:03d}-candidate.json"
    record = json.loads(attempt.read_text(encoding="utf-8"))
    assert record["credited_units"] == 0
    assert record["runtime"]["attempt_diagnostics"]["operational_metrics"]["wall_clock_s"] == 1
    pair = json.loads((root / f"pair-{failed_block + 1:03d}.json").read_text(encoding="utf-8"))
    assert pair["attempt_sha256"]["candidate"] == sha256_file(attempt)


@pytest.mark.parametrize(("mode", "pair_reason"), [
    ("timeout", "both_attempts_must_verify"),
    ("zero_work", "both_attempts_must_verify"),
    ("partial", "both_attempts_must_verify"),
    ("warm", "both_attempts_must_verify"),
    ("wrong_output", "output_fingerprints_differ"),
    ("environment", "measurement_profile_mismatch"),
])
def test_incompatible_attempt_cannot_rank(tmp_path, monkeypatch, workloads, mode, pair_reason):
    root, result = _run(tmp_path, monkeypatch, workloads, [("ok", mode), ("ok", "ok")])
    assert result["status"] == "inconclusive"
    assert result["reason"] == "insufficient_pair_coverage"
    comparison = json.loads((root / "pair-001.json").read_text(encoding="utf-8"))
    assert comparison["reason"] == pair_reason
    assert "relative_reduction" not in comparison


def test_fixed_effect_requires_enough_verified_pairs(tmp_path, monkeypatch, workloads):
    _, result = _run(tmp_path, monkeypatch, workloads, [("ok", "ok")] * 6)
    assert result["status"] == "candidate_lower"
    assert result["technical_bootstrap_interval_95"] == {"low": 0.5, "high": 0.5}


def test_environment_drift_between_otherwise_matched_pairs(tmp_path, monkeypatch, workloads):
    root, result = _run(tmp_path, monkeypatch, workloads, [("ok", "ok"), ("environment", "environment")])
    assert result["eligible_pairs"] == 1
    pair = json.loads((root / "pair-002.json").read_text(encoding="utf-8"))
    assert pair["reason"] == "environment_changed_between_pairs"
