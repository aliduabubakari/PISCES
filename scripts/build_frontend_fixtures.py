"""Build sanitized, provenance-linked UI fixtures from saved synthetic benchmark attempts."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
from pathlib import Path

from pisces.report_views import build_report_view, load_report


def build(record_dir: Path, source_dir: Path) -> list[dict]:
    examples = []
    for case in ("valid", "same_shape_wrong", "partial"):
        name = f"event_aggregation-{case}-01.json"
        original = load_report((record_dir / name).read_bytes())
        runtime = original["result"]["payload"]["runtime_measurement"]
        safe_runtime = {
            key: copy.deepcopy(runtime[key])
            for key in (
                "status",
                "execution_outcome",
                "execution_adapter_requested",
                "execution_adapter_used",
                "contract_validation",
                "pairwise_equivalence",
                "telemetry",
                "normalized",
                "eligible_for_resource_comparison",
                "ineligibility_reasons",
                "container_telemetry",
                "container_telemetry_missing_reason",
                "engine_evidence",
                "operational_metrics",
                "attempt_diagnostics",
                "output_assertions",
            )
            if key in runtime
        }
        # Keep only structural output facts, never observed cells, file paths or model logs.
        safe_runtime["output_assertions"] = {
            key: copy.deepcopy(runtime["output_assertions"][key])
            for key in ("passed", "status", "results")
        }
        safe_runtime["output_assertions"]["results"] = [
            {
                key: item[key]
                for key in ("logical_name", "passed", "status", "reason", "row_count")
                if key in item
            }
            for item in runtime["output_assertions"]["results"]
        ]
        if safe_runtime.get("attempt_diagnostics"):
            safe_runtime["attempt_diagnostics"] = {
                "operational_metrics": copy.deepcopy(
                    runtime["attempt_diagnostics"].get("operational_metrics")
                )
            }
        report = {
            key: original[key]
            for key in (
                "pipeline",
                "case",
                "repetition",
                "output_verified",
                "expected_behavior_matched",
            )
        }
        report["provenance"] = {
            key: original["provenance"][key] for key in ("artifact_sha256", "target")
        }
        report["result"] = {
            "execution_outcome": original["result"]["execution_outcome"],
            "output_assertions": copy.deepcopy(safe_runtime["output_assertions"]),
            "payload": {"runtime_measurement": safe_runtime},
        }
        source = (source_dir / f"{case}.py").read_bytes()
        digest = hashlib.sha256(source).hexdigest()
        if digest != report["provenance"]["artifact_sha256"]:
            raise ValueError(f"Source does not match saved artifact: {case}")
        examples.append(
            {
                "filename": name,
                "report": report,
                "view": build_report_view(report),
                "source": {
                    "name": f"{case}.py",
                    "text": source.decode("utf-8"),
                    "digest": digest,
                },
            }
        )
    return examples


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("record_dir", type=Path)
    parser.add_argument("source_dir", type=Path)
    parser.add_argument(
        "--out", type=Path, default=Path("frontend/src/test/fixtures/examples.json")
    )
    args = parser.parse_args()
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(
        json.dumps(build(args.record_dir, args.source_dir), indent=2, allow_nan=False)
        + "\n",
        encoding="utf-8",
    )
