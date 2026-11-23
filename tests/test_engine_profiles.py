from __future__ import annotations

import json
import sys
from types import SimpleNamespace

import pytest

from pisces.benchmarks.catalog import build_suite
from pisces.benchmarks.runner import _load, load_suite, run_suite
from pisces.energy.adapters.base import NativeAdapterError
from pisces.energy.adapters.qualified import _selection
from pisces.energy import oci_runtime as oci
from pisces.energy import runtime_runner_cli as runner
from pisces.energy.runtime_preflight import runtime_policy_error


@pytest.mark.parametrize("target", ["prefect", "dagster"])
def test_engine_templates_reuse_the_same_reference_contracts(tmp_path, target):
    airflow, other = tmp_path / "airflow", tmp_path / target
    build_suite(airflow, rows=30, keys=4)
    build_suite(other, rows=30, keys=4, target=target)
    assert load_suite(other)["target"] == target
    for pipeline in ("climate_fusion", "event_aggregation", "order_enrichment", "sensor_windows", "inventory_latest"):
        left, right = _load(airflow, pipeline, "valid"), _load(other, pipeline, "valid")
        assert left.contract_sha256 == right.contract_sha256
        assert left.provenance()["fixture_sha256"] == right.provenance()["fixture_sha256"]
        assert right.binding["target"] == target
        assert left.artifact_sha256 != right.artifact_sha256
    assert run_suite(other, tmp_path / "standalone")["status"] == "passed"


def test_entrypoint_requires_real_type_or_unambiguous_selection():
    class EngineType:
        pass

    first, second = EngineType(), EngineType()
    assert _selection(SimpleNamespace(alias=first, other_alias=first), EngineType) is first
    with pytest.raises(NativeAdapterError, match="unambiguous"):
        _selection(SimpleNamespace(first=first, second=second), EngineType)
    assert _selection(SimpleNamespace(first=first, second=second, pisces_entrypoint=second), EngineType) is second
    with pytest.raises(NativeAdapterError, match="invalid_pisces_entrypoint"):
        _selection(SimpleNamespace(pisces_entrypoint=lambda: None), EngineType)


@pytest.mark.parametrize("target", ["prefect", "dagster"])
def test_native_experiment_gate_is_not_opened_for_host_execution(target):
    kwargs = dict(mode="zero_fill", adapter="native", target=target, experiment=True,
                  has_contract=True, has_binding=True, scale_factor=1)
    assert runtime_policy_error(**kwargs) == "experiment_target_not_supported"
    assert runtime_policy_error(**kwargs, qualified_oci_target=target) is None


def test_host_environment_marker_cannot_enable_qualified_execution(tmp_path, monkeypatch):
    request, output = tmp_path / "request.json", tmp_path / "output.json"
    request.write_text(json.dumps({"orchestrator": "prefect", "experiment_mode": True, "execution_adapter": "native",
                                   "mode": "zero_fill", "read_only_fixture_mount": True,
                                   "workload_contract": "unused", "artifact_binding": "unused"}), encoding="utf-8")
    monkeypatch.setenv("PISCES_OCI_TARGET", "prefect")
    monkeypatch.setattr(runner.os, "geteuid", lambda: 1000)
    monkeypatch.setattr(sys, "argv", ["runner", "--in", str(request), "--out", str(output)])
    runner.main()
    report = json.loads(output.read_text())
    assert report["error"]["message"] == "qualified_engine_requires_private_oci_worker"


def test_wrong_image_target_fails_before_staging(tmp_path, monkeypatch):
    from contextlib import nullcontext

    root = tmp_path / "suite"
    build_suite(root, rows=20, keys=4, target="prefect")
    monkeypatch.setattr(oci, "_local_engine", lambda: nullcontext(["docker"]))
    monkeypatch.setattr(oci, "_inspect_engine", lambda *args: {"runtime_target": "dagster", "image_id": "image"})
    monkeypatch.setattr(oci, "_stage", lambda *args, **kwargs: pytest.fail("Must reject before staging"))
    report = oci.run_oci(_load(root, "event_aggregation", "valid"), image="image", timeout_s=10)
    assert report["error"]["message"] == "oci_image_target_mismatch"


def test_readiness_reports_target_mismatch(monkeypatch):
    from pisces.diagnostics import DiagnosticsConfig, collect_diagnostics

    monkeypatch.setattr(oci, "check_oci", lambda image: {"ready": True, "reason": None,
                                                       "environment": {"runtime_target": "dagster"}})
    report = collect_diagnostics(DiagnosticsConfig(orchestrator="prefect", oci_image="image"))
    assert not report["ready"]
    assert report["blockers"] == ["oci_image_target_mismatch"]
