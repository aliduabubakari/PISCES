from pathlib import Path
import pytest

from benchmarks.climate_fusion import scaling
from pisces.energy import memory_check
from pisces.energy.oci_runtime import _create_command


def test_linear_fit_preserves_setup_intercept():
    result = scaling._linear_fit([(rows, 2 + rows / 100_000) for rows in scaling.LOADS])
    assert result["intercept_s"] == 2
    assert result["slope_s_per_100k_rows"] == 1
    assert result["r_squared"] == 1


def test_custom_loads_preserve_nonzero_intercept_and_numeric_order():
    loads = scaling.validate_loads([1000, 10, 100])
    assert loads == (10, 100, 1000)
    attempts = [{"rows": rows, "output_verified": True, "fingerprints": {"fusion": "same"},
                 "readings": {key: 2 + rows / 100_000 for key in scaling.METRICS}}
                for rows in loads for _ in range(2)]
    summary = scaling._summarize(attempts, 2, loads=loads)
    assert list(summary["loads"]) == list(loads)
    assert summary["time_fits_descriptive_only"]["task_wall_s"]["intercept_s"] == pytest.approx(2)


@pytest.mark.parametrize("loads", [(10, 20), (10, 10, 20), (10, 15, 21), (5, 10, 1000005), (True, 10, 15)])
def test_invalid_load_plans_are_rejected(loads):
    with pytest.raises(ValueError):
        scaling.validate_loads(loads)


def test_no_fit_for_missing_metric_or_disagreeing_outputs():
    attempts = [{"rows": rows, "output_verified": True, "fingerprints": {"fusion": "match"},
                 "readings": {key: rows for key in scaling.METRICS}}
                for rows in scaling.LOADS for _ in range(2)]
    assert scaling._summarize(attempts, 2)["status"] == "descriptive_only"
    attempts[0]["readings"].pop("worker_cpu_s")
    assert "worker_cpu_s" not in scaling._summarize(attempts, 2)["time_fits_descriptive_only"]
    attempts[0]["fingerprints"]["fusion"] = "different"
    assert scaling._summarize(attempts, 2)["time_fits_descriptive_only"] == {}


def test_task_reading_revalidates_raw_clock_deltas():
    task = {"status": "available", "source": scaling.SOURCE, "scope": "task_callable_body", "task_id": "fuse",
            "execution_wall_bounds_ns": [100, 500], "wall_start_ns": 200, "wall_end_ns": 300,
            "thread_cpu_start_ns": 100, "thread_cpu_end_ns": 170,
            "wall_clock_s": 100 / 1e9, "thread_cpu_s": 70 / 1e9}
    assert scaling._task_reading(task) == {"wall_clock_s": 100 / 1e9, "thread_cpu_s": 70 / 1e9}
    assert scaling._task_reading({**task, "wall_clock_s": 2}) == {}
    assert scaling._task_reading({**task, "execution_wall_bounds_ns": [300, 500]}) == {}


def test_memory_control_checks_peak_and_real_oom_evidence():
    mib = memory_check.MIB
    case = str(96 * mib)
    payload = {"case": case, "reference": {"baseline_bytes": 20*mib, "held_bytes": 116*mib,
                                          "released_bytes": 20*mib, "peak_before_bytes": 21*mib},
               "container_telemetry": {"memory_peak_bytes": 118*mib}}
    assert memory_check.assess_memory(case, payload, None) == []
    payload["container_telemetry"]["memory_peak_bytes"] = 21*mib
    assert memory_check.assess_memory(case, payload, None) == ["lifetime_peak_outside_bound"]
    oom = {"case": "oom", "allocation_child_returncode": -9, "container_telemetry": {"oom_kill_delta": 1}}
    assert memory_check.assess_memory("oom", oom, "container_oom_killed") == []
    oom["container_telemetry"]["oom_kill_delta"] = 0
    assert memory_check.assess_memory("oom", oom, "container_oom_killed")


def test_oom_fixture_container_is_bounded_and_has_no_mount():
    image_id = "sha256:" + "a" * 64
    command = _create_command(["docker"], "test", image_id, memory_case="oom")
    assert "--memory=134217728" in command and "--memory-swap=134217728" in command
    assert "--mount" not in command and "--network=none" in command
    regular = _create_command(["docker"], "test", image_id, Path("/fixtures"))
    assert "--memory=2g" in regular and "--memory-swap=2g" in regular
