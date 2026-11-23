from __future__ import annotations

import json
from pathlib import Path

import pytest

from pisces.dag_comparator import compare_reports


def _write_report(
    path: Path, *, kind: str, scenario: str, energy_kwh: float, confidence: str
) -> Path:
    path.write_text(
        json.dumps(
            {
                "tool": "pisces-unified",
                "report_schema_version": "1.2.0",
                "workflow": {"name": path.stem, "orchestrator": "airflow"},
                "energy": {
                    "evidence": {"measurement_kind": kind, "confidence": confidence},
                    "runtime_measurement": {
                        "paired_equivalence_status": "eligible_pending_pairwise_comparison",
                        "energy_profile": {
                            "available": True,
                            "status": "ok",
                            "energy_consumed_kwh": energy_kwh,
                        },
                    },
                    "measurements": {
                        "estimate": {
                            "status": "ok",
                            "scenario_hash": scenario,
                            "yearly": {"emissions_kgco2eq": 365 * energy_kwh},
                        }
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    return path


@pytest.mark.parametrize(
    ("kind", "scenario", "confidence", "extra_reason"),
    [
        ("surrogate_benchmark", "same", "low", "mixed_evidence_kinds"),
        ("native_runtime", "different", "medium", "mixed_scenario_assumptions"),
        ("native_runtime", "same", "medium", None),
    ],
)
def test_numeric_reports_are_inventory_only(
    tmp_path: Path, kind: str, scenario: str, confidence: str, extra_reason: str | None
) -> None:
    baseline = _write_report(
        tmp_path / "baseline.json",
        kind="native_runtime",
        scenario="same",
        energy_kwh=0.2,
        confidence="medium",
    )
    candidate = _write_report(
        tmp_path / "candidate.json",
        kind=kind,
        scenario=scenario,
        energy_kwh=0.1,
        confidence=confidence,
    )

    result = compare_reports([baseline, candidate])

    assert result["schema_version"] == "2.0.0"
    assert "ranked_by_runtime_energy" not in result
    assert "ranked_by_estimated_yearly_emissions" not in result
    assert result["comparison"]["status"] == "not_comparable"
    reasons = result["comparison"]["reason_codes"]
    assert "verified_paired_comparison_unavailable" in reasons
    if extra_reason:
        assert extra_reason in reasons
    assert [item["workflow"]["name"] for item in result["summaries"]] == [
        "baseline", "candidate"
    ]
    assert result["summaries"][1]["runtime_energy"]["energy_consumed_kwh"] == 0.1
    assert result["summaries"][1]["resource_values_use"] == "inventory_only"
    assert result["summaries"][1]["report_schema_version"] == "1.2.0"
    assert result["summaries"][1]["estimate"]["yearly_emissions_kgco2eq"] == 36.5


@pytest.mark.parametrize("invalid", [True, -1.0, "0.1"])
def test_invalid_numbers_are_not_reported_as_inventory_metrics(tmp_path: Path, invalid) -> None:
    path = _write_report(
        tmp_path / "report.json",
        kind="native_runtime",
        scenario="same",
        energy_kwh=0.1,
        confidence="medium",
    )
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["energy"]["runtime_measurement"]["energy_profile"]["energy_consumed_kwh"] = invalid
    payload["energy"]["measurements"]["estimate"]["yearly"]["emissions_kgco2eq"] = invalid
    path.write_text(json.dumps(payload), encoding="utf-8")

    result = compare_reports([path])

    assert result["summaries"][0]["runtime_energy"]["energy_consumed_kwh"] is None
    assert result["summaries"][0]["estimate"]["yearly_emissions_kgco2eq"] is None
    json.dumps(result, allow_nan=False)


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_nonfinite_reports_are_skipped_even_in_legacy_inventory(tmp_path: Path, value: float) -> None:
    path = _write_report(
        tmp_path / "nonfinite.json", kind="native_runtime", scenario="same", energy_kwh=value,
        confidence="medium",
    )
    result = compare_reports([path])
    assert result["summaries"] == []
    assert result["skipped_reports"][0]["reason"] == "invalid_report_json_values"
