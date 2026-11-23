from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from benchmarks.climate_fusion import scaling_sessions as sessions
from benchmarks.climate_fusion.repeatability import _read, _write
from benchmarks.climate_fusion.scaling import LOADS, METRICS, SCHEMA_VERSION
from pisces.energy.task_timing import SOURCE


@pytest.fixture
def pair(tmp_path, monkeypatch):
    def assess(payload, profile, provenance, rows):
        assert payload["rows"] == rows
        return {"output_verified": True, "fingerprints": {"fusion": payload["fingerprint"]},
                "readings": {key: payload["duration"] for key in METRICS}}

    monkeypatch.setattr(sessions, "assess_attempt", assess)
    paths = []
    for day in (1, 2):
        path = tmp_path / f"session-{day}"
        path.mkdir()
        start = datetime(2026, 1, day, tzinfo=timezone.utc)
        _write(path / "manifest.json", {
            "schema_version": SCHEMA_VERSION, "task_timing_source": SOURCE, "metrics": METRICS,
            "loads_are_total_rows": True, "started_at_utc": start.isoformat(), "round_order": [LOADS, LOADS],
            "repetitions": 2, "workloads": {rows: {} for rows in LOADS}, "sources": ["fixture"],
            "oci_profile": {"environment_class": "docker_desktop_vm"}, "controller_platform": "test",
            "conformance_report_sha256": "a" * 64, "timeout_s": 90, "cache_policy": "uncontrolled",
        })
        for index in (1, 2):
            for offset, rows in enumerate(LOADS):
                _write(path / f"round-{index:02d}-rows-{rows}.json", {
                    "round": index, "rows": rows,
                    "started_at_utc": (start + timedelta(seconds=(index * len(LOADS)) + offset)).isoformat(),
                    "payload": {"rows": rows, "fingerprint": "same", "duration": day * rows / 100_000},
                    "readings": {"task_wall_s": -1},  # The saved derived fields are not used.
                })
        paths.append(path)
    return paths


def _change(path, modify):
    value = _read(path)
    modify(value)
    path.unlink()
    _write(path, value)


def test_compares_independent_slopes_and_recomputes_from_payload(pair):
    report = sessions.compare_sessions(list(reversed(pair)))
    assert report["status"] == "descriptive_only"
    assert report["slope_spread_s_per_100k_rows"]["task_wall_s"]["session_values"] == pytest.approx([1, 2])
    assert report["load_median_spread"][1_000_000]["task_wall_s"]["session_values"] == [10, 20]


def test_missing_attempts_and_fingerprint_drift_do_not_produce_spreads(pair):
    (pair[0] / "round-01-rows-10000.json").unlink()
    for index in (1, 2):
        _change(pair[1] / f"round-{index:02d}-rows-100000.json",
                lambda value: value["payload"].update(fingerprint="different"))
    report = sessions.compare_sessions(pair)
    assert report["status"] == "incomplete"
    assert report["reasons"] == ["incomplete_session_coverage", "output_fingerprints_differ_at_100000_rows"]
    assert not report["slope_spread_s_per_100k_rows"]
    assert report["sessions"][0]["summary"]["loads"][10000]["metrics"]["task_wall_s"]["coverage"] == 0.5


def test_refuses_mixed_environments_and_repeated_sessions(pair):
    with pytest.raises(ValueError, match="distinct"):
        sessions.compare_sessions([pair[0], pair[0]])
    _change(pair[1] / "manifest.json", lambda value: value["oci_profile"].update(environment_class="local_linux_engine"))
    with pytest.raises(ValueError, match="profile mismatch: oci_profile"):
        sessions.compare_sessions(pair)


def test_rejects_unplanned_attempt(pair):
    _write(pair[0] / "round-99-rows-10000.json", {})
    with pytest.raises(ValueError, match="Unexpected attempt"):
        sessions.compare_sessions(pair)


def test_three_custom_loads_are_revalidated_without_default_load_assumptions(pair):
    mapping = dict(zip(LOADS[:3], (10, 100, 1000)))
    for day, directory in enumerate(pair, 1):
        manifest_path = directory / "manifest.json"
        manifest = _read(manifest_path)
        manifest["workloads"] = {str(new): manifest["workloads"][str(old)] for old, new in mapping.items()}
        manifest["round_order"] = [[mapping[old] for old in order if old in mapping] for order in manifest["round_order"]]
        manifest_path.unlink()
        _write(manifest_path, manifest)
        for path in directory.glob("round-*-rows-*.json"):
            record = _read(path)
            path.unlink()
            if record["rows"] not in mapping:
                continue
            record["rows"] = mapping[record["rows"]]
            record["payload"].update(rows=record["rows"], duration=day * record["rows"] / 100000)
            _write(directory / f"round-{record['round']:02d}-rows-{record['rows']}.json", record)
    report = sessions.compare_sessions(pair)
    assert report["status"] == "descriptive_only"
    assert set(report["sessions"][0]["summary"]["loads"]) == {10, 100, 1000}
    assert report["slope_spread_s_per_100k_rows"]["task_wall_s"]["session_values"] == pytest.approx([1, 2])
