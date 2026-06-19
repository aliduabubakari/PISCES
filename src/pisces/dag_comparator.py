#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List


SEVERITY_WEIGHTS = {
    "critical": 100,
    "major": 30,
    "minor": 10,
    "info": 1,
}


def _load_report(path: Path) -> Dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Report is not a JSON object: {path}")
    return payload


def _is_evaluation_report(report: Dict[str, Any]) -> bool:
    return isinstance(report.get("workflow"), dict) and isinstance(report.get("energy"), dict)


def _rules(report: Dict[str, Any]) -> List[Dict[str, Any]]:
    rules = (((report.get("energy") or {}).get("heuristic") or {}).get("rules") or {}).get("rules")
    return rules if isinstance(rules, list) else []


def _runtime_energy(report: Dict[str, Any]) -> Dict[str, Any]:
    runtime = ((report.get("energy") or {}).get("runtime_measurement") or {})
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


def summarize_report(path: Path) -> Dict[str, Any]:
    report = _load_report(path)
    workflow = report.get("workflow") if isinstance(report.get("workflow"), dict) else {}
    energy = report.get("energy") if isinstance(report.get("energy"), dict) else {}
    rules = _rules(report)
    rule_summary = _score_rules(rules)
    energy_profile = _runtime_energy(report)
    evidence = _evidence(report)
    estimate = _estimate(report)
    yearly = estimate.get("yearly") if isinstance(estimate.get("yearly"), dict) else {}

    return {
        "path": str(path),
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
            "energy_consumed_kwh": energy_profile.get("energy_consumed_kwh"),
            "emissions_kgco2eq": energy_profile.get("emissions_kgco2eq"),
            "measurement_scope": energy_profile.get("measurement_scope"),
        },
        "estimate": {
            "available": estimate.get("status") == "ok",
            "status": estimate.get("status"),
            "scenario_hash": estimate.get("scenario_hash"),
            "scenario_name": ((estimate.get("assumptions") or {}).get("scenario_name") if isinstance(estimate.get("assumptions"), dict) else None),
            "runs_per_year": ((estimate.get("assumptions") or {}).get("runs_per_year") if isinstance(estimate.get("assumptions"), dict) else None),
            "yearly_energy_consumed_kwh": yearly.get("energy_consumed_kwh"),
            "yearly_emissions_kgco2eq": yearly.get("emissions_kgco2eq"),
            "baseline_measurement_kind": ((estimate.get("baseline") or {}).get("measurement_kind") if isinstance(estimate.get("baseline"), dict) else None),
            "confidence": estimate.get("confidence"),
        },
    }


def compare_reports(paths: List[Path]) -> Dict[str, Any]:
    summaries: List[Dict[str, Any]] = []
    skipped_reports: List[Dict[str, Any]] = []
    for path in paths:
        report = _load_report(path)
        if not _is_evaluation_report(report):
            skipped_reports.append(
                {
                    "path": str(path),
                    "reason": "not_pisces_unified_evaluation_report",
                    "tool": report.get("tool"),
                }
            )
            continue
        summaries.append(summarize_report(path))
    ranked_by_rule_risk = sorted(
        summaries,
        key=lambda item: (int(item.get("risk_score") or 0), int(item.get("rule_count") or 0)),
        reverse=True,
    )
    comparable_runtime = [
        item for item in summaries
        if isinstance(item.get("runtime_energy"), dict)
        and isinstance(item["runtime_energy"].get("energy_consumed_kwh"), (int, float))
    ]
    ranked_by_runtime_energy = sorted(
        comparable_runtime,
        key=lambda item: float(item["runtime_energy"]["energy_consumed_kwh"]),
        reverse=True,
    )
    comparable_estimates = [
        item for item in summaries
        if isinstance(item.get("estimate"), dict)
        and isinstance(item["estimate"].get("yearly_emissions_kgco2eq"), (int, float))
    ]
    scenario_hashes = sorted(
        {
            str(item["estimate"].get("scenario_hash"))
            for item in comparable_estimates
            if item["estimate"].get("scenario_hash")
        }
    )
    ranked_by_estimated_yearly_emissions = sorted(
        comparable_estimates,
        key=lambda item: float(item["estimate"]["yearly_emissions_kgco2eq"]),
        reverse=True,
    )

    warnings = []
    if len(comparable_runtime) != len(summaries):
        warnings.append(
            "Some reports do not include measured runtime energy; runtime-energy ranking is partial."
        )
    if skipped_reports:
        warnings.append(
            "Some JSON files were skipped because they are not PISCES unified evaluation reports."
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
    if len(evidence_tiers) > 1 or len(confidence_tiers) > 1:
        warnings.append(
            "Reports include mixed evidence/confidence tiers; compare rankings with caution."
        )
    if comparable_estimates and len(comparable_estimates) != len(summaries):
        warnings.append(
            "Some reports do not include calibrated estimates; estimate ranking is partial."
        )
    if len(scenario_hashes) > 1:
        warnings.append(
            "Calibrated estimates use different scenario assumptions; yearly estimate ranking may be incomparable."
        )

    return {
        "tool": "pisces-compare",
        "schema_version": "1.1.0",
        "report_count": len(summaries),
        "input_count": len(paths),
        "skipped_reports": skipped_reports,
        "summaries": summaries,
        "ranked_by_rule_risk": ranked_by_rule_risk,
        "ranked_by_runtime_energy": ranked_by_runtime_energy,
        "ranked_by_estimated_yearly_emissions": ranked_by_estimated_yearly_emissions,
        "comparison_context": {
            "evidence_tiers": evidence_tiers,
            "confidence_tiers": confidence_tiers,
            "estimate_scenario_hashes": scenario_hashes,
        },
        "warnings": warnings,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare PISCES JSON reports")
    parser.add_argument("reports", nargs="+", help="PISCES JSON report paths")
    parser.add_argument("--out", help="Optional output JSON path")
    args = parser.parse_args()

    result = compare_reports([Path(p) for p in args.reports])
    text = json.dumps(result, indent=2, default=str)
    if args.out:
        Path(args.out).write_text(text + "\n", encoding="utf-8")
    else:
        print(text)


if __name__ == "__main__":
    main()
