"""Read-only presentation of admitted evidence, independent of any web framework."""

from __future__ import annotations

import hashlib
import json
from math import isfinite
from functools import lru_cache
from importlib.resources import files
from typing import Any

from jsonschema import Draft202012Validator

from pisces.report_validation import _validator, validate_report
from .metrics import EXECUTION, MODELED, WORKER, METRIC_REGISTRY_VERSION, observations

VIEW_SCHEMA_VERSION = "1.0.0"
MAX_REPORT_BYTES = 8 * 1024 * 1024


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate_json_key")
        result[key] = value
    return result


def load_report(data: bytes | str) -> dict:
    """Bounded, strict JSON loading shared by the CLI and optional HTTP adapter."""
    if (
        len(data if isinstance(data, bytes) else data.encode("utf-8"))
        > MAX_REPORT_BYTES
    ):
        raise ValueError("report_too_large")
    try:
        report = json.loads(
            data,
            object_pairs_hook=_unique_object,
            parse_constant=lambda _: (_ for _ in ()).throw(
                ValueError("non_finite_json")
            ),
        )
        _check_tree(report)
    except (UnicodeError, json.JSONDecodeError, RecursionError) as exc:
        raise ValueError("invalid_report_json") from exc
    if not isinstance(report, dict):
        raise ValueError("report_must_be_object")
    return report


def _check_tree(value: Any, depth: int = 0) -> None:
    if depth > 64:
        raise ValueError("report_nesting_limit")
    if isinstance(value, dict):
        for child in value.values():
            _check_tree(child, depth + 1)
    elif isinstance(value, list):
        for child in value:
            _check_tree(child, depth + 1)
    elif isinstance(value, float) and not isfinite(value):
        raise ValueError("non_finite_json")


@lru_cache(maxsize=1)
def view_schema() -> dict:
    return json.loads(
        files("pisces")
        .joinpath("schemas", "report-view-1.0.0.schema.json")
        .read_text(encoding="utf-8")
    )


def _suite_admission(report: dict) -> bool:
    # Suite attempts have no top-level version yet. Admit only this inventoried shape.
    schema = {
        "type": "object",
        "required": [
            "pipeline",
            "case",
            "repetition",
            "provenance",
            "output_verified",
            "expected_behavior_matched",
            "result",
        ],
        "properties": {
            "pipeline": {"type": "string"},
            "case": {"type": "string"},
            "repetition": {"type": "integer", "minimum": 1},
            "output_verified": {"type": "boolean"},
            "expected_behavior_matched": {"type": "boolean"},
            "provenance": {
                "type": "object",
                "required": ["artifact_sha256"],
                "properties": {
                    "artifact_sha256": {"type": "string", "pattern": "^[a-f0-9]{64}$"}
                },
            },
            "result": {
                "type": "object",
                "required": ["execution_outcome", "output_assertions"],
                "properties": {
                    "execution_outcome": {"type": "string"},
                    "output_assertions": {"type": "object"},
                    "payload": {
                        "type": ["object", "null"],
                        "properties": {
                            "runtime_measurement": {"$ref": "#/$defs/runtime"}
                        },
                    },
                },
            },
        },
        "$defs": _validator().schema["$defs"],
    }
    return Draft202012Validator(schema).is_valid(report)


def build_report_view(report: dict) -> dict:
    """Map evidence without executing source or granting comparison eligibility.

    Admission validates structure, not authenticity. Unknown/rejected formats are
    inventory-only and never expose metric or verdict claims as admitted evidence.
    """
    if not isinstance(report, dict):
        raise ValueError("report_must_be_object")
    _check_tree(report)
    try:
        encoded = json.dumps(
            report,
            sort_keys=True,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode()
    except (ValueError, TypeError, OverflowError) as exc:
        raise ValueError("invalid_report_json_values") from exc
    if len(encoded) > MAX_REPORT_BYTES:
        raise ValueError("report_too_large")
    view = {
        "view_schema_version": VIEW_SCHEMA_VERSION,
        "metric_registry_version": METRIC_REGISTRY_VERSION,
        "report_id": hashlib.sha256(encoded).hexdigest(),
        "report_kind": "unsupported",
        "source_schema_version": None,
        "admission": {"status": "unsupported", "reason": "unsupported_report_format"},
        "identity": {
            "name": "Unrecognized report",
            "engine": "unknown",
            "case": None,
            "artifact_sha256": None,
            "evidence_kind": "inventory_only",
        },
        "execution": {"status": "not_evaluated", "pointer": None},
        "outputs": {
            "status": "not_evaluated",
            "checks": [],
            "expected_behavior_matched": None,
        },
        "tasks": [],
        "metrics": [],
        "comparison": {
            "resource_eligible": None,
            "pairwise_status": "not_evaluated",
            "screening_eligible": None,
            "reasons": ["unsupported_report_format"],
            "pointer": None,
        },
        "warnings": [
            "Imported report claims are not attested execution.",
            "No conformance association is established by this view.",
        ],
    }
    runtime, base = {}, ""
    if report.get("tool") == "pisces-unified" or "report_schema_version" in report:
        admission = validate_report(report, allow_legacy=True)
        view.update(
            report_kind="unified",
            source_schema_version=str(report.get("report_schema_version"))
            if report.get("report_schema_version") is not None
            else None,
        )
        view["admission"] = {"status": admission.status, "reason": admission.reason}
        if admission.status != "current_inventory":
            view["comparison"]["reasons"] = [admission.reason or "report_not_admitted"]
            return _validated(view)
        workflow = report["workflow"]
        view["identity"].update(
            name=workflow["name"],
            engine=workflow["orchestrator"],
            artifact_sha256=report["metadata"]["artifact_sha256"],
            evidence_kind=(report.get("energy", {}).get("evidence") or {}).get(
                "measurement_kind", "not_collected"
            ),
        )
        runtime = (report.get("energy", {}).get("measurements") or {}).get(
            "runtime"
        ) or {}
        base = "/energy/measurements/runtime"
        if report.get("errors"):
            view["warnings"].append(
                "Evaluation reported errors; inspect the original report."
            )
    elif "pipeline" in report and "result" in report:
        view["report_kind"] = "suite_attempt"
        view["admission"] = {
            "status": "suite_inventory" if _suite_admission(report) else "rejected",
            "reason": None,
        }
        if view["admission"]["status"] == "rejected":
            view["admission"]["reason"] = "invalid_suite_attempt_schema"
            view["comparison"]["reasons"] = ["invalid_suite_attempt_schema"]
            return _validated(view)
        view["warnings"].append(
            "Suite attempts are versionless; admission covers the supported inventory shape only."
        )
        view["identity"].update(
            name=report["pipeline"],
            engine=report["provenance"].get("target", "unknown"),
            case=report["case"],
            artifact_sha256=report["provenance"]["artifact_sha256"],
            evidence_kind="benchmark_attempt",
        )
        view["outputs"]["expected_behavior_matched"] = report[
            "expected_behavior_matched"
        ]
        runtime = (report["result"].get("payload") or {}).get(
            "runtime_measurement"
        ) or {}
        base = "/result/payload/runtime_measurement"
        proof = report["result"]["output_assertions"]
        if runtime and runtime.get("output_assertions") != proof:
            return _reject(view, "inconsistent_output_assertion_alias")
        if (
            type(proof.get("passed")) is bool
            and proof["passed"] != report["output_verified"]
        ):
            return _reject(view, "inconsistent_output_verdict")
        if not runtime:
            if not _valid_proof(proof):
                return _reject(view, "invalid_output_evidence")
            view["execution"] = {
                "status": report["result"]["execution_outcome"],
                "pointer": "/result/execution_outcome",
            }
            _outputs(
                view, report["result"]["output_assertions"], "/result/output_assertions"
            )
    else:
        return _validated(view)
    if runtime:
        if not _valid_runtime_extensions(runtime):
            return _reject(view, "invalid_presentation_evidence")
        view["execution"] = {
            "status": runtime["execution_outcome"],
            "pointer": base + "/execution_outcome",
        }
        _outputs(
            view, runtime.get("output_assertions") or {}, base + "/output_assertions"
        )
        engine = runtime.get("engine_evidence") or {}
        view["tasks"] = [
            {
                "task_id": task["task_id"],
                "state": task["state"],
                "status": task["status"],
                "pointer": base + f"/engine_evidence/task_states/{index}",
            }
            for index, task in enumerate(engine.get("task_states", []))
        ]
        if not view["tasks"]:
            coverage = runtime.get("task_coverage") or {}
            for field, status in (
                ("executed_task_ids", "executed"),
                ("failed_task_ids", "failed"),
                ("disallowed_skipped_task_ids", "skipped"),
            ):
                for index, task_id in enumerate(coverage.get(field, [])):
                    view["tasks"].append(
                        {
                            "task_id": task_id,
                            "state": status,
                            "status": status,
                            "pointer": base + f"/task_coverage/{field}/{index}",
                        }
                    )
        pair = runtime["pairwise_equivalence"]
        view["comparison"] = {
            "resource_eligible": runtime["eligible_for_resource_comparison"],
            "pairwise_status": pair["status"],
            "screening_eligible": pair["screening_eligible"],
            "reasons": runtime.get("ineligibility_reasons") or [],
            "pointer": base + "/eligible_for_resource_comparison",
        }
    else:
        view["comparison"]["reasons"] = ["runtime_evidence_not_collected"]
    worker = runtime.get("container_telemetry") or {}
    diagnostics = runtime.get("attempt_diagnostics") or {}
    operational = (
        runtime.get("operational_metrics")
        or diagnostics.get("operational_metrics")
        or {}
    )
    execution_base = base + (
        "/operational_metrics"
        if runtime.get("operational_metrics")
        else "/attempt_diagnostics/operational_metrics"
    )
    diagnostic = not runtime.get("operational_metrics") and bool(
        diagnostics.get("operational_metrics")
    )
    model = runtime.get("modeled_energy") or diagnostics.get("modeled_energy") or {}
    model_base = base + (
        "/modeled_energy"
        if runtime.get("modeled_energy")
        else "/attempt_diagnostics/modeled_energy"
    )
    view["metrics"] = (
        observations(
            WORKER,
            worker,
            scope="container_worker",
            base=base + "/container_telemetry" if worker else None,
            role="diagnostic" if view["outputs"]["status"] == "failed" else "reported",
            missing_reason=runtime.get("container_telemetry_missing_reason"),
        )
        + observations(
            EXECUTION,
            operational,
            scope="execution_block",
            base=execution_base if operational else None,
            role="diagnostic" if diagnostic else "reported",
        )
        + observations(
            MODELED,
            model,
            scope="modeled",
            base=model_base if model else None,
            role="modeled",
        )
    )
    view["warnings"].append(
        "Task-body resource allocation and dependency edges are not inferred from aggregate counters or task order."
    )
    return _validated(view)


def _outputs(view: dict, proof: dict, base: str) -> None:
    passed = proof.get("passed")
    view["outputs"].update(
        status="passed"
        if passed is True
        else "failed"
        if passed is False
        else "not_evaluated"
    )
    checks = []
    for index, result in enumerate(proof.get("results") or []):
        # Output values and local paths are deliberately not part of the view contract.
        if not isinstance(result, dict):
            continue
        count = result.get("row_count")
        checks.append(
            {
                "name": str(result.get("logical_name", "unknown")),
                "status": "passed"
                if result.get("passed") is True
                else "failed"
                if result.get("passed") is False
                else "not_evaluated",
                "passed": result.get("passed")
                if type(result.get("passed")) is bool
                else None,
                "row_count": count if type(count) is int and count >= 0 else None,
                "reason": str(result["reason"])
                if result.get("reason") is not None
                else None,
                "pointer": f"{base}/results/{index}",
            }
        )
    view["outputs"]["checks"] = checks


def _valid_proof(proof: dict) -> bool:
    if (
        not isinstance(proof, dict)
        or type(proof.get("passed", False)) is not bool
        or not isinstance(proof.get("results", []), list)
    ):
        return False
    for result in proof.get("results", []):
        if not isinstance(result, dict):
            return False
        if result.get("passed") is not None and type(result["passed"]) is not bool:
            return False
        if proof.get("passed") is True and result.get("passed") is False:
            return False
        if result.get("reason") is not None and not isinstance(result["reason"], str):
            return False
    return True


def _valid_runtime_extensions(runtime: dict) -> bool:
    for key in (
        "output_assertions",
        "attempt_diagnostics",
        "engine_evidence",
        "task_coverage",
    ):
        if runtime.get(key) is not None and not isinstance(runtime[key], dict):
            return False
    if not _valid_proof(runtime.get("output_assertions") or {}):
        return False
    reasons = runtime.get("ineligibility_reasons", [])
    if not isinstance(reasons, list) or any(
        not isinstance(reason, str) for reason in reasons
    ):
        return False
    coverage = runtime.get("task_coverage") or {}
    for key in ("executed_task_ids", "failed_task_ids", "disallowed_skipped_task_ids"):
        if not isinstance(coverage.get(key, []), list) or any(
            not isinstance(task, str) for task in coverage.get(key, [])
        ):
            return False
    return True


def _reject(view: dict, reason: str) -> dict:
    view["admission"] = {"status": "rejected", "reason": reason}
    view["execution"] = {"status": "not_evaluated", "pointer": None}
    view["outputs"].update(
        status="not_evaluated", checks=[], expected_behavior_matched=None
    )
    view["metrics"], view["tasks"] = [], []
    view["comparison"]["reasons"] = [reason]
    return _validated(view)


def _validated(view: dict) -> dict:
    Draft202012Validator(view_schema()).validate(view)
    return view


def build_view_bundle(report: dict) -> dict:
    """Portable notebook/CLI output; the original report remains semantically unchanged."""
    return {
        "bundle_format": "pisces-report-view",
        "view": build_workspace_view(report),
        "report": report,
        "report_json": json.dumps(report, indent=2, allow_nan=False),
    }


def build_workspace_view(report: dict) -> dict:
    from .campaigns import build_campaign_view, is_campaign

    return build_campaign_view(report) if isinstance(report, dict) and is_campaign(report) else build_report_view(report)


__all__ = ["build_report_view", "build_workspace_view", "build_view_bundle", "load_report", "view_schema"]
