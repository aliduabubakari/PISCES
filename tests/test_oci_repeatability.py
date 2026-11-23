from __future__ import annotations

from copy import deepcopy

import pytest

from benchmarks.climate_fusion import repeatability as repeat
from benchmarks.climate_fusion.benchmark import build_bundle
from pisces.energy.output_equivalence import ORACLE_VERSION


@pytest.fixture
def bundle(tmp_path):
    path = tmp_path / "bundle"
    build_bundle(path, rows_per_source=1)
    return path


@pytest.fixture
def profile():
    return {"image_id": "sha256:" + "a" * 64, "environment_class": "docker_desktop_vm"}


def _payload(workload, profile):
    provenance = workload.provenance()
    return {"ok": True, "runtime_measurement": {
        "execution_outcome": "completed", "execution_adapter_used": "oci", "workload_contract": provenance,
        "oci_environment": profile, "bounds": {"repetitions_completed": 1, "maximum_repetitions": 1},
        "warmup": {"completed_repetitions": 0, "requested_repetitions": 0, "included_in_measurement_window": False},
        "task_coverage": {"passed": True}, "input_consumption": {"passed": True}, "modeled_energy": None,
        "output_assertions": {
            "passed": True, "oracle_version": ORACLE_VERSION,
            **{key: provenance[key] for key in ("contract_sha256", "artifact_sha256", "binding_sha256")},
            "results": [{"logical_name": "fusion", "passed": True, "semantic_sha256": "b" * 64,
                         "reference_sha256": provenance["reference_sha256"]["fusion"]}],
        },
        "container_telemetry": {
            "scope": repeat.SCOPE,
            "wall_clock_s": 2.0, "cpu_s": 1.0,
            "counter_window": {
                "wall_started_ns": 20, "wall_finished_ns": 2_000_000_020,
                "cpu_before": {"read_started_ns": 1, "read_finished_ns": 10, "usage_usec": 100},
                "cpu_after": {"read_started_ns": 2_000_000_030, "read_finished_ns": 2_000_000_040,
                              "usage_usec": 1_000_100},
            },
        },
    }}


def test_missing_cpu_preserves_wall_coverage_and_workload_verdict(bundle, profile):
    workload = repeat._load_baseline(bundle)
    complete = _payload(workload, profile)
    failed_cpu = deepcopy(complete)
    failed_cpu["ok"] = False
    failed_cpu["runtime_measurement"]["container_telemetry"]["cpu_s"] = None
    attempts = [repeat._assess(payload, profile, workload.provenance()) for payload in (complete, failed_cpu)]
    assert attempts[1]["output_verified"]
    assert attempts[1]["eligible_readings_s"] == {"wall_clock_s": 2.0}
    summary = repeat._session_summary(attempts, 2)
    assert summary["status"] == "incomplete"
    assert summary["verified"] == 2
    assert summary["metrics"]["cpu_s"]["coverage"] == 0.5
    assert summary["metrics"]["wall_clock_s"]["coverage"] == 1


def test_wrong_output_and_profile_drift_are_kept_but_excluded(bundle, profile):
    workload = repeat._load_baseline(bundle)
    payload = _payload(workload, {**profile, "kernel_version": "different"})
    payload["runtime_measurement"]["output_assertions"]["results"][0]["reference_sha256"] = "c" * 64
    assessment = repeat._assess(payload, profile, workload.provenance())
    assert assessment["issues"] == ["oci_profile_missing_or_changed", "output_not_verified"]
    assert not assessment["eligible_readings_s"]


def test_conflicting_output_fingerprints_suppress_statistics(bundle, profile):
    workload = repeat._load_baseline(bundle)
    payload = _payload(workload, profile)
    first = repeat._assess(payload, profile, workload.provenance())
    payload["runtime_measurement"]["output_assertions"]["results"][0]["semantic_sha256"] = "c" * 64
    second = repeat._assess(payload, profile, workload.provenance())
    summary = repeat._session_summary([first, second], 2)
    assert summary["status"] == "incomplete"
    assert summary["metrics"]["wall_clock_s"]["spread_s"] is None


def test_run_and_compare_requires_same_passing_profile(tmp_path, bundle, profile, monkeypatch):
    monkeypatch.setattr(repeat, "check_oci", lambda image: {"ready": True, "environment": profile})
    monkeypatch.setattr(repeat, "run_oci", lambda work, **kwargs: _payload(work, profile))
    image = profile["image_id"]
    evidence = tmp_path / "confirmation.json"
    repeat._write(evidence, {"status": "calibration_only", "mode": "calibration", "environment": profile})
    opts = {"image": image, "confirmation": evidence, "expected_environment": "docker_desktop_vm",
            "repetitions": 2, "timeout_s": 15}
    with pytest.raises(ValueError, match="passing conformance"):
        repeat.run_session(bundle, tmp_path / "rejected", **opts)
    evidence.unlink()
    repeat._write(evidence, {"status": "passed", "mode": "confirmation", "environment": profile})
    with pytest.raises(ValueError, match="passing conformance"):
        repeat.run_session(bundle, tmp_path / "no-readings", **opts)
    evidence.unlink()
    repeat._write(evidence, {"schema_version": "1.0", "status": "passed", "mode": "confirmation",
                             "environment": profile, "repetitions_per_case": 2,
                             "tolerances_absolute_s": {"wall_s": 0.001, "cpu_s": 0.003},
                             "summary": {case: {"attempted": 2, "with_readings": 2, "without_issues": 2,
                                                "absolute_error_s": {"wall_s": {"max": 0}, "cpu_s": {"max": 0}}}
                                         for case in repeat.CASES}})
    sessions = [tmp_path / name for name in ("a", "b")]
    for path in sessions:
        assert repeat.run_session(bundle, path, **opts)["status"] == "complete"
        assert (path / "attempt-002.json").is_file()
    summary = repeat.compare_sessions(sessions)
    assert summary["status"] == "descriptive_only"
    assert summary["between_session_dispersion"]["cpu_s"]["span_fraction_of_median"] == 0
    with pytest.raises(ValueError, match="distinct sessions"):
        repeat.compare_sessions(sessions + sessions[:1])
    manifest = sessions[0] / "manifest.json"
    changed = repeat._read(manifest)
    changed["oci_profile"]["environment_class"] = "local_linux_engine"
    manifest.unlink()
    repeat._write(manifest, changed)
    with pytest.raises(ValueError, match="different workload or execution profiles"):
        repeat.compare_sessions(sessions)
