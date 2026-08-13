from __future__ import annotations

import time

from pisces.energy.carbon_tracker import try_stop_tracker
from pisces.energy.operational_metrics import (
    start_operational_measurement,
    stop_operational_measurement,
)
from pisces.energy.runtime_runner_cli import _run_workload


def test_operational_metrics_are_primary_process_evidence() -> None:
    started = start_operational_measurement()
    sum(i * i for i in range(10_000))
    time.sleep(0.001)
    result = stop_operational_measurement(started, scope="test")

    assert result["evidence_role"] == "primary"
    assert result["measurement_scope"] == "test"
    assert result["wall_clock_s"] > 0
    assert result["process_cpu_s"] >= 0
    assert result["peak_rss_mb"] is not None
    assert result["peak_rss_scope"] == "dedicated_runner_process_lifetime"


def test_codecarbon_payload_is_secondary_modeled_evidence_when_not_started() -> None:
    result = try_stop_tracker(None)

    assert result["evidence_role"] == "secondary"
    assert result["interpretation"] == "modeled_energy"
    assert result["hardware_power_source_verified"] is False


def test_surrogate_workload_is_target_neutral() -> None:
    rows = [{"value": 1}, {"value": 2}]
    first = _run_workload(rows, max_tasks=4, seed=1729)
    second = _run_workload(rows, max_tasks=4, seed=1729)

    assert first["tasks_exercised"] == 4
    assert second["tasks_exercised"] == 4
    assert first["numeric_accumulator"] == second["numeric_accumulator"]
    assert first["hash_accumulator"] == second["hash_accumulator"]
