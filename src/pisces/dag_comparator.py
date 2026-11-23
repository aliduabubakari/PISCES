#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any, Dict, List

from .report_validation import ReportAdmission, validate_report


SEVERITY_WEIGHTS = {
    "critical": 100,
    "major": 30,
    "minor": 10,
    "info": 1,
}


def _load_report(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _rules(report: Dict[str, Any]) -> List[Dict[str, Any]]:
    energy = report.get("energy")
    heuristic = energy.get("heuristic") if isinstance(energy, dict) else None
    group = heuristic.get("rules") if isinstance(heuristic, dict) else None
    rules = group.get("rules") if isinstance(group, dict) else None
    return [rule for rule in rules if isinstance(rule, dict)] if isinstance(rules, list) else []


def _runtime_energy(report: Dict[str, Any]) -> Dict[str, Any]:
    energy = report.get("energy") or {}
    measurements = energy.get("measurements")
    runtime = measurements.get("runtime") if isinstance(measurements, dict) else None
    if runtime is None:
        runtime = energy.get("runtime_measurement")
    profile = runtime.get("energy_profile") if isinstance(runtime, dict) else None
    return profile if isinstance(profile, dict) else {}


def _estimate(report: Dict[str, Any]) -> Dict[str, Any]:
    energy = report.get("energy") if isinstance(report.get("energy"), dict) else {}
    measurements = energy.get("measurements") if isinstance(energy.get("measurements"), dict) else {}
    estimate = measurements.get("estimate") if isinstance(measurements.get("estimate"), dict) else None
    if estimate is None:
        estimate = energy.get("estimate") if isinstance(energy.get("estimate"), dict) else None
    return estimate if isinstance(estimate, dict) else {}


def _evidence(report: Dict[str, Any]) -> Dict[str, Any]:
    energy = report.get("energy") if isinstance(report.get("energy"), dict) else {}
    evidence = energy.get("evidence") if isinstance(energy.get("evidence"), dict) else {}
    return evidence


def _score_rules(rules: List[Dict[str, Any]]) -> Dict[str, Any]:
    counts: Dict[str, int] = {"critical": 0, "major": 0, "minor": 0, "info": 0}
    score = 0
    for rule in rules:
        severity = str(rule.get("severity") or "info").lower()
        if severity not in counts:
            severity = "info"
        counts[severity] += 1
        score += SEVERITY_WEIGHTS[severity]
    return {"risk_score": score, "severity_counts": counts, "rule_count": len(rules)}


def _inventory_metric(value: Any) -> Any:
    if type(value) is int and value >= 0:
        return value
    if type(value) is float and math.isfinite(value) and value >= 0:
        return value
    return None


def summarize_report(path: Path) -> Dict[str, Any]:
    report = _load_report(path)
    admission = validate_report(report, allow_legacy=True)
    if admission.status == "rejected":
        raise ValueError(f"Cannot summarize {path}: {admission.reason}")
    return _summarize_report(path, report, admission)


def _summarize_report(path: Path, report: Dict[str, Any], admission: ReportAdmission) -> Dict[str, Any]:
    workflow = report.get("workflow") if isinstance(report.get("workflow"), dict) else {}
    energy = report.get("energy") if isinstance(report.get("energy"), dict) else {}
    rules = _rules(report)
    rule_summary = _score_rules(rules)
    energy_profile = _runtime_energy(report)
    evidence = _evidence(report)
    estimate = _estimate(report)
    yearly = estimate.get("yearly") if isinstance(estimate.get("yearly"), dict) else {}
    heuristic = energy.get("heuristic")
    rule_group = heuristic.get("rules") if isinstance(heuristic, dict) else None
    static_findings_available = isinstance(rule_group, dict) and rule_group.get("status") != "not_available"

    return {
        "path": str(path),
        "report_schema_version": report.get("report_schema_version"),
        "resource_values_use": "inventory_only",
        "report_admission": admission.status,
        "static_findings_available": static_findings_available,
        "workflow": {
            "name": workflow.get("name"),
            "orchestrator": workflow.get("orchestrator"),
            "runtime_supported": workflow.get("runtime_supported"),
        },
        "mode_used": energy.get("mode_used"),
        "data_source": energy.get("data_source"),
        "evidence": {
            "measurement_kind": evidence.get("measurement_kind"),
            "confidence": evidence.get("confidence"),
            "production_equivalent": evidence.get("production_equivalent"),
            "measurement_scope": evidence.get("measurement_scope"),
        },
        **rule_summary,
        "runtime_energy": {
            "available": bool(energy_profile.get("available")),
            "status": energy_profile.get("status"),
            "energy_consumed_kwh": _inventory_metric(energy_profile.get("energy_consumed_kwh")),
            "emissions_kgco2eq": _inventory_metric(energy_profile.get("emissions_kgco2eq")),
            "measurement_scope": energy_profile.get("measurement_scope"),
        },
        "estimate": {
            "available": estimate.get("status") == "ok",
            "status": estimate.get("status"),
            "reason": estimate.get("reason"),
            "scenario_hash": estimate.get("scenario_hash"),
            "scenario_name": ((estimate.get("assumptions") or {}).get("scenario_name") if isinstance(estimate.get("assumptions"), dict) else None),
            "runs_per_year": ((estimate.get("assumptions") or {}).get("runs_per_year") if isinstance(estimate.get("assumptions"), dict) else None),
            "yearly_energy_consumed_kwh": _inventory_metric(yearly.get("energy_consumed_kwh")),
            "yearly_emissions_kgco2eq": _inventory_metric(yearly.get("emissions_kgco2eq")),
            "baseline_measurement_kind": ((estimate.get("baseline") or {}).get("measurement_kind") if isinstance(estimate.get("baseline"), dict) else None),
            "confidence": estimate.get("confidence"),
        },
    }


def compare_reports(paths: List[Path]) -> Dict[str, Any]:
    summaries: List[Dict[str, Any]] = []
    skipped_reports: List[Dict[str, Any]] = []
    for path in paths:
        try:
            report = _load_report(path)
        except (OSError, ValueError):
            skipped_reports.append({"path": str(path), "reason": "unreadable_report_json"})
            continue
        admission = validate_report(report, allow_legacy=True)
        if admission.status == "rejected":
            skipped_reports.append(
                {
                    "path": str(path),
                    "reason": admission.reason,
                    "tool": report.get("tool") if isinstance(report, dict) and isinstance(report.get("tool"), str) else None,
                    **({"detail": admission.detail} if admission.detail else {}),
                }
            )
            continue
        summaries.append(_summarize_report(path, report, admission))
    ranked_by_rule_risk = sorted(
        (item for item in summaries if item["static_findings_available"]),
        key=lambda item: (int(item.get("risk_score") or 0), int(item.get("rule_count") or 0)),
        reverse=True,
    )
    scenario_hashes = sorted(
        {
            item["estimate"]["scenario_hash"]
            for item in summaries
            if isinstance(item["estimate"].get("scenario_hash"), str)
            and item["estimate"]["scenario_hash"]
        }
    )

    warnings = []
    if skipped_reports:
        warnings.append(
            "Some reports were skipped; see skipped_reports for reasons."
        )
    evidence_tiers = sorted(
        {
            str((item.get("evidence") or {}).get("measurement_kind"))
            for item in summaries
            if (item.get("evidence") or {}).get("measurement_kind")
        }
    )
    confidence_tiers = sorted(
        {
            str((item.get("evidence") or {}).get("confidence"))
            for item in summaries
            if (item.get("evidence") or {}).get("confidence")
        }
    )
    reason_codes = ["verified_paired_comparison_unavailable"]
    if len(summaries) < 2:
        reason_codes.append("insufficient_reports")
    if len(evidence_tiers) > 1:
        reason_codes.append("mixed_evidence_kinds")
    if len(scenario_hashes) > 1:
        reason_codes.append("mixed_scenario_assumptions")

    return {
        "tool": "pisces-compare",
        "schema_version": "2.0.0",
        "report_count": len(summaries),
        "input_count": len(paths),
        "skipped_reports": skipped_reports,
        "summaries": summaries,
        "ranked_by_rule_risk": ranked_by_rule_risk,
        "comparison": {
            "status": "not_comparable",
            "reason_codes": reason_codes,
        },
        "comparison_context": {
            "evidence_tiers": evidence_tiers,
            "confidence_tiers": confidence_tiers,
            "estimate_scenario_hashes": scenario_hashes,
        },
        "warnings": warnings,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Inventory PISCES JSON reports and compare static rule findings")
    parser.add_argument("reports", nargs="+", help="PISCES JSON report paths")
    parser.add_argument("--out", help="Optional output JSON path")
    args = parser.parse_args()

    result = compare_reports([Path(p) for p in args.reports])
    text = json.dumps(result, indent=2, allow_nan=False)
    if args.out:
        Path(args.out).write_text(text + "\n", encoding="utf-8")
    else:
        print(text)


if __name__ == "__main__":
    main()
