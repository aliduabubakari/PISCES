"""Generate immutable fixtures and independent references for five pipeline families."""

from __future__ import annotations

from collections import Counter
from contextlib import contextmanager
import csv
from decimal import Decimal
from importlib.resources import files
import json
from pathlib import Path

from ..energy.workload_contract import sha256_file
from ..energy.runtime_profiles import PROFILES


CASES = ("valid", "equivalent", "same_shape_wrong", "partial")
PIPELINES = {
    "climate_fusion": {"description": "Five-source temperature normalization", "sources": ["bom", "ecmwf", "jma", "metoffice", "noaa"],
                       "header": ["source", "station_id", "temperature_c"], "types": ["string", "integer", "decimal"],
                       "key_indices": [0, 1], "value_index": 2},
    "event_aggregation": {"description": "Reject invalid events; group counts and integer amounts", "header": ["group_id", "count", "total"],
                          "types": ["integer", "integer", "integer"], "key_indices": [0], "value_index": 2},
    "order_enrichment": {"description": "Join orders to customers; calculate discounted net cents", "header": ["order_id", "region", "net_cents"],
                         "types": ["integer", "string", "integer"], "key_indices": [0], "value_index": 2},
    "sensor_windows": {"description": "Quality filter and fixed 60-second sensor windows", "header": ["sensor_id", "window_start_s", "count", "sum_tenths"],
                       "types": ["integer", "integer", "integer", "integer"], "key_indices": [0, 1], "value_index": 3},
    "inventory_latest": {"description": "Latest version per item with tombstones and reversed input", "header": ["item_id", "version", "quantity", "active"],
                         "types": ["integer", "integer", "integer", "boolean"], "key_indices": [0], "value_index": 2},
}


def write_json(path: Path, value: dict) -> None:
    with path.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2, sort_keys=True, allow_nan=False)
        stream.write("\n")


@contextmanager
def _csv(path: Path, header: list[str]):
    with path.open("x", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream, lineterminator="\n")
        writer.writerow(header)
        yield writer


def _fixtures(root: Path, pipeline: str, rows: int, keys: int, truth) -> tuple[int, int]:
    """Build expected facts while generating inputs, never importing a candidate."""
    if pipeline == "climate_fusion":
        for source_index, source in enumerate(PIPELINES[pipeline]["sources"]):
            count = rows // 5 + (source_index < rows % 5)
            with _csv(root / f"{source}.csv", ["station_id", "temperature", "unit"]) as observed:
                for station in range(count):
                    celsius = Decimal((1729 + 17 * station + 43 * source_index) % 601 - 200) / 10
                    truth.writerow([source, station, celsius])
                    fahrenheit = source == "noaa"
                    observed.writerow([station, celsius * Decimal("1.8") + 32 if fahrenheit else celsius, "F" if fahrenheit else "C"])
        return rows, 0

    if pipeline == "event_aggregation":
        counts, totals = Counter(), Counter()
        with _csv(root / "events.csv", ["event_id", "group_id", "amount", "status"]) as observed:
            for index in range(rows):
                key, amount, accepted = index % keys, (index * 17 % 997) + 1, index % 11 != 0
                observed.writerow([index, key, amount, "valid" if accepted else "invalid"])
                if accepted:
                    counts[key] += 1
                    totals[key] += amount
        for key in sorted(counts):
            truth.writerow([key, counts[key], totals[key]])
        accepted = sum(counts.values())
        return accepted, rows - accepted

    if pipeline == "order_enrichment":
        with _csv(root / "customers.csv", ["customer_id", "region", "discount_bps"]) as observed:
            for key in range(keys):
                observed.writerow([key, f"region_{key % 4}", (key % 5) * 250])
        accepted = 0
        with _csv(root / "orders.csv", ["order_id", "customer_id", "quantity", "unit_cents"]) as observed:
            for index in range(rows):
                key, quantity, price = index % keys, 1 + index % 7, 100 + index % 901
                observed.writerow([index, key if index % 13 else -1, quantity, price])
                if index % 13:
                    gross = quantity * price
                    discount = Decimal((key % 5) * 250) / 10_000
                    truth.writerow([index, f"region_{key % 4}", int(Decimal(gross) * (1 - discount))])
                    accepted += 1
        return accepted, rows - accepted

    if pipeline == "sensor_windows":
        counts, sums = Counter(), Counter()
        with _csv(root / "readings.csv", ["sensor_id", "timestamp_s", "temperature_tenths", "quality"]) as observed:
            for index in range(rows):
                sensor, timestamp, value = index % keys, (index // keys) * 17, (index * 7 % 800) - 200
                observed.writerow([sensor, timestamp, value, "good" if index % 17 else "bad"])
                if index % 17:
                    # Bucket endpoints are generated facts, independent of the candidate's grouping code.
                    bucket = timestamp - timestamp % 60
                    counts[sensor, bucket] += 1
                    sums[sensor, bucket] += value
        for (sensor, bucket), count in sorted(counts.items()):
            truth.writerow([sensor, bucket, count, sums[sensor, bucket]])
        accepted = sum(counts.values())
        return accepted, rows - accepted

    if pipeline == "inventory_latest":
        with _csv(root / "updates.csv", ["item_id", "version", "quantity", "deleted"]) as observed:
            for index in reversed(range(rows)):
                observed.writerow([index % keys, index // keys, index * 3 % 101, "true" if index % 19 == 0 else "false"])
        for key in range(keys):
            last_index = key + ((rows - 1 - key) // keys) * keys
            truth.writerow([key, last_index // keys, last_index * 3 % 101, "false" if last_index % 19 == 0 else "true"])
        return rows, 0
    raise ValueError("Unknown benchmark pipeline")


def _output(name: str, header: list[str], types: list[str], key_indices: list[int], reference: Path) -> dict:
    return {"logical_name": name, "format": "csv", "row_order": "key_sorted",
            "primary_key": [header[index] for index in key_indices],
            "columns": [{"name": name, "type": kind, "nullable": False} for name, kind in zip(header, types)],
            "reference": {"path": f"references/{reference.name}", "sha256": sha256_file(reference)}}


def build_suite(root: Path, *, rows: int = 1000, keys: int = 32, target: str = "airflow") -> dict:
    if target not in PROFILES:
        raise ValueError("Unsupported benchmark target")
    if type(rows) is not int or not 10 <= rows <= 1_000_000:
        raise ValueError("--rows must be 10 to 1000000 primary input records per pipeline")
    if type(keys) is not int or not 1 <= keys <= min(rows, 10_000):
        raise ValueError("--keys must be 1 to min(rows, 10000)")
    root.mkdir(parents=True, exist_ok=False)
    template = files(__package__).joinpath("workflow.py.tmpl").read_text(encoding="utf-8")
    template = template.replace("__ENGINE_ENTRYPOINT__", files(__package__).joinpath(f"{target}.py.tmpl").read_text(encoding="utf-8").rstrip())
    for pipeline, config in PIPELINES.items():
        bundle = root / pipeline
        fixtures, references = bundle / "fixtures", bundle / "references"
        fixtures.mkdir(parents=True)
        references.mkdir()
        result_reference, quality_reference = references / "result.csv", references / "quality.csv"
        with _csv(result_reference, config["header"]) as truth:
            accepted, rejected = _fixtures(fixtures, pipeline, rows, keys, truth)
        with _csv(quality_reference, ["metric", "value"]) as truth:
            truth.writerows([["accepted_count", accepted], ["rejected_count", rejected]])
        contract_id = f"{pipeline}-{rows}-{keys}-v1"
        fixture_paths = sorted(fixtures.glob("*.csv"))
        write_json(bundle / "contract.json", {
            "schema_version": "2.0", "contract_id": contract_id, "pipeline_id": pipeline, "functional_unit": "valid_batch",
            "inputs": [{"integration_id": path.stem, "kind": "file", "fixture": f"fixtures/{path.name}", "sha256": sha256_file(path)}
                       for path in fixture_paths],
            "outputs": [_output("result", config["header"], config["types"], config["key_indices"], result_reference),
                        _output("quality", ["metric", "value"], ["string", "integer"], [0], quality_reference)],
        })
        for case in CASES:
            artifact = bundle / f"{case}.py"
            source = template.replace("__PIPELINE__", repr(pipeline)).replace("__VARIANT__", repr(case)).replace("__CONFIG__", repr(config))
            artifact.write_text(source, encoding="utf-8")
            write_json(bundle / f"{case}.binding.json", {
                "schema_version": "2.0", "binding_id": case, "contract_id": contract_id, "target": target,
                "artifact_path": artifact.name, "artifact_sha256": sha256_file(artifact),
                "file_bindings": {path.stem: f"inputs/{path.name}" for path in fixture_paths}, "integration_bindings": {},
                "output_bindings": {name: f"outputs/{name}.csv" for name in ("result", "quality")},
                "expected_task_ids": ["normalize", "transform", "write_outputs"],
            })
    manifest = {"schema_version": "1.0", "suite_id": "pisces-five-pipelines-v1", "primary_rows_per_pipeline": rows,
                "keys": keys, "target": target, "input_origin": "deterministic_synthetic", "pipelines": list(PIPELINES), "cases": list(CASES),
                "files": {str(path.relative_to(root)): sha256_file(path) for path in sorted(root.rglob("*")) if path.is_file()}}
    write_json(root / "suite.json", manifest)
    return manifest
