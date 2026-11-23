from __future__ import annotations

from contextlib import nullcontext
from copy import deepcopy

from pisces.energy import cgroup_metrics, oci_runtime, telemetry_check


TOLERANCES = {"wall_s": 0.002, "cpu_s": 0.005}


def _observation() -> dict:
    return {
        "case": "child_cpu", "collector_call_wall_s": 0.312,
        "reference": {"wall_s": 0.309, "self_cpu_s": 0.001, "reaped_child_cpu_s": 0.2},
        "container_telemetry": {
            "wall_clock_s": 0.31, "cpu_s": 0.202,
            "counter_window": {
                "wall_started_ns": 1_000_000, "wall_finished_ns": 311_000_000,
                "cpu_before": {"read_started_ns": 1, "read_finished_ns": 15, "usage_usec": 150_000},
                "cpu_after": {"read_started_ns": 311_000_001, "read_finished_ns": 311_000_020,
                              "usage_usec": 352_000},
            },
        },
    }


def test_confirmation_rejects_conversion_even_with_loose_tolerances() -> None:
    reading = _observation()
    assert telemetry_check.assess(reading, "child_cpu", TOLERANCES)["issues"] == []
    reading["container_telemetry"]["cpu_s"] *= 1000
    result = telemetry_check.assess(reading, "child_cpu", {"wall_s": 1, "cpu_s": 1000})
    assert result["issues"] == ["cpu_reading_does_not_match_raw_endpoints"]


def test_confirmation_rejects_parent_only_cpu_even_if_raw_endpoints_agree() -> None:
    reading = _observation()
    reading["container_telemetry"]["cpu_s"] = 0.001
    reading["container_telemetry"]["counter_window"]["cpu_after"]["usage_usec"] = 151_000
    result = telemetry_check.assess(reading, "child_cpu", TOLERANCES)
    assert result["issues"] == ["cpu_s_outside_tolerance", "child_cpu_not_accounted_for"]


def test_confirms_wall_error_and_missing_measurement() -> None:
    reading = _observation()
    reading["reference"]["wall_s"] = 0.304
    assert telemetry_check.assess(reading, "child_cpu", TOLERANCES)["issues"] == ["wall_s_outside_tolerance"]
    reading["container_telemetry"]["cpu_s"] = None
    assert telemetry_check.assess(reading, "child_cpu", None)["issues"] == ["missing_or_invalid_wall_or_cpu_reading"]


def test_measure_cgroup_wall_excludes_snapshot_reads(tmp_path, monkeypatch) -> None:
    ticks = iter((100, 110, 120, 220, 230, 240))
    monkeypatch.setattr(cgroup_metrics.time, "perf_counter_ns", lambda: next(ticks))
    cpu = tmp_path / "cpu.stat"
    cpu.write_text("usage_usec 10\n", encoding="ascii")

    def work():
        cpu.write_text("usage_usec 15\n", encoding="ascii")
        return "result"

    result, measured = cgroup_metrics.measure_cgroup(work, tmp_path)
    assert result == "result"
    assert measured["wall_clock_s"] == 100 / 1e9
    assert measured["cpu_s"] == 5 / 1e6
    assert telemetry_check._check_window(measured) == []


def test_calibration_does_not_claim_confirmation(monkeypatch) -> None:
    monkeypatch.setattr(telemetry_check, "CASES", ("child_cpu",))
    monkeypatch.setattr(oci_runtime, "_local_engine", lambda: nullcontext(["docker"]))
    monkeypatch.setattr(oci_runtime, "_inspect_engine", lambda cli, image: {"image_id": image})
    reading = _observation()

    def execute(cli, image, inputs, timeout, *, telemetry_case):
        assert inputs is None
        assert telemetry_case == "child_cpu"
        return {"payload": deepcopy(reading), "exit_code": 0, "attach_returncode": 0}

    monkeypatch.setattr(oci_runtime, "_execute", execute)
    calibration = telemetry_check.run_telemetry_check("image", repetitions=2, tolerances=None)
    assert calibration["status"] == "calibration_only"
    assert calibration["summary"]["child_cpu"]["attempted"] == 2
    assert telemetry_check.run_telemetry_check("image", repetitions=1, tolerances=TOLERANCES)["status"] == "passed"


def test_telemetry_fixture_uses_no_input_mount() -> None:
    command = oci_runtime._create_command(["docker"], "name", "sha256:" + "a" * 64, telemetry_case="empty")
    assert command[-2:] == ["--telemetry-check", "empty"]
    assert "--mount" not in command
