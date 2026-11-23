from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from pisces.energy.runtime_evaluation import (
    EnergyEvaluationRequest,
    LLMProviderError,
    _recipe_to_rows,
    build_provider,
    evaluate_energy,
    load_sample_dataset,
)


def _write_workflow(tmp_path: Path) -> Path:
    f = tmp_path / "flow.py"
    f.write_text(
        """
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

with DAG(dag_id='x', start_date=datetime(2024,1,1), schedule=None) as dag:
    t1 = PythonOperator(task_id='a', python_callable=lambda: 1)
""".strip()
        + "\n",
        encoding="utf-8",
    )
    return f


def test_mode_precedence_auto_uses_fuzz_fill_without_data_or_llm(tmp_path: Path) -> None:
    wf = _write_workflow(tmp_path)
    req = EnergyEvaluationRequest(
        file_path=wf,
        code_text=wf.read_text(encoding="utf-8"),
        orchestrator="airflow",
        repo_root=Path.cwd(),
        runtime_python=Path(sys.executable),
        mode_selected="auto",
        sample_paths=[],
        llm_provider=None,
        llm_model=None,
    )

    res = evaluate_energy(req).to_dict()
    attempted = res["attempted_modes"]
    # Auto stays local: sample, fuzz_fill, zero_fill, heuristic.
    assert attempted[0]["mode"] == "sample"
    assert attempted[0]["status"] == "skipped"
    assert attempted[1]["mode"] == "fuzz_fill"
    assert res["mode_used"] in {"fuzz_fill", "zero_fill", "heuristic"}
    assert isinstance(res.get("metadata", {}).get("warnings"), list)


def test_synthetic_rows_are_deterministic() -> None:
    recipe = {
        "row_count": 4,
        "columns": [
            {"name": "id", "type": "int"},
            {"name": "score", "type": "float"},
            {"name": "name", "type": "str"},
        ],
    }
    sig = "abcd" * 16
    rows_a = _recipe_to_rows(recipe, signature_hash=sig, seed=1729, max_rows=10)
    rows_b = _recipe_to_rows(recipe, signature_hash=sig, seed=1729, max_rows=10)
    assert rows_a == rows_b


def test_provider_error_normalization() -> None:
    with pytest.raises(LLMProviderError) as e:
        build_provider("unknown_provider", model="x", api_key_env=None, base_url=None, timeout_s=5)
    assert e.value.code == "unsupported_provider"


def test_anthropic_provider_health_check_without_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    provider = build_provider("anthropic", model="claude-test", api_key_env=None, base_url=None, timeout_s=5)
    ok, reason = provider.health_check()
    assert ok is False
    assert reason == "missing_api_key_env:ANTHROPIC_API_KEY"


def test_privacy_no_raw_rows_persisted_for_sample_mode(tmp_path: Path) -> None:
    wf = _write_workflow(tmp_path)
    sample = tmp_path / "sample.csv"
    sample.write_text("id,value\n1,sensitive123\n2,ok\n", encoding="utf-8")

    req = EnergyEvaluationRequest(
        file_path=wf,
        code_text=wf.read_text(encoding="utf-8"),
        orchestrator="airflow",
        repo_root=Path.cwd(),
        runtime_python=Path(sys.executable),
        mode_selected="sample",
        sample_paths=[sample],
        max_rows=100,
        max_tasks=5,
        timeout_s=30,
    )

    res = evaluate_energy(req).to_dict()
    blob = json.dumps(res)
    assert "sensitive123" not in blob
    assert res["privacy"]["raw_rows_persisted"] is False
    assert res["privacy"]["sample_provenance_mode"].startswith("file hashes")
    assert res["evidence"]["measurement_kind"] == "surrogate_benchmark"
    assert res["evidence"]["confidence"] == "low"
    assert res["measurements"]["runtime"] == res["runtime_measurement"]
    sample_summary = res["attempted_modes"][0]["sample_summary"]
    assert sample_summary["schema_summary"]["column_count"] == 2
    assert sample_summary["provenance"][0]["source_name"] == "sample.csv"
    assert "file_sha256" in sample_summary["provenance"][0]


def test_sample_dataset_schema_mapping_and_privacy_provenance() -> None:
    fixture = Path(__file__).parent / "fixtures" / "sample_orders.csv"
    dataset = load_sample_dataset(
        [fixture],
        max_rows=10,
        schema_map={
            "orderIdentifier": "order_id",
            "totalAmount": "amount",
            "isPriority": "priority",
        },
    )

    assert dataset["row_count"] == 3
    assert [sorted(row.keys()) for row in dataset["rows"]] == [["amount", "order_id", "priority"]] * 3
    assert dataset["schema_summary"]["column_count"] == 3
    assert dataset["schema_summary"]["type_counts"] == {"float": 1, "bool": 1, "str": 1}
    assert dataset["mapping"]["enabled"] is True
    assert dataset["mapping"]["mapped_column_count"] == 3
    assert dataset["mapping"]["target_columns"] == ["amount", "order_id", "priority"]
    provenance = dataset["provenance"][0]
    assert provenance["source_name"] == "sample_orders.csv"
    assert provenance["rows_discovered"] == 3
    assert provenance["rows_loaded"] == 3
    assert len(provenance["schema_summary"]["column_name_hashes"]) == 3
    assert "o-100" not in json.dumps(
        {
            "schema_summary": dataset["schema_summary"],
            "provenance": dataset["provenance"],
            "mapping": dataset["mapping"],
        }
    )


def test_sample_mode_invalid_schema_mapping_fails_gracefully(tmp_path: Path) -> None:
    wf = _write_workflow(tmp_path)
    sample = tmp_path / "sample.csv"
    sample.write_text("id,value\n1,2\n", encoding="utf-8")

    req = EnergyEvaluationRequest(
        file_path=wf,
        code_text=wf.read_text(encoding="utf-8"),
        orchestrator="airflow",
        repo_root=Path.cwd(),
        runtime_python=Path(sys.executable),
        mode_selected="sample",
        sample_paths=[sample],
        sample_schema_map={"missing_column": "canonical"},
        max_rows=100,
        max_tasks=5,
        timeout_s=30,
    )

    res = evaluate_energy(req).to_dict()
    assert res["mode_used"] == "heuristic"
    assert res["attempted_modes"][0]["mode"] == "sample"
    assert res["attempted_modes"][0]["status"] == "failed"
    assert res["attempted_modes"][0]["reason"] == "sample_schema_mapping_missing_sources"
    assert res["errors"][0]["error_type"] == "SampleValidationFailed"
    assert res["errors"][0]["details"]["mapping"]["missing_source_count"] == 1


class _FakeProvider:
    provider_name = "openai"

    def health_check(self):
        return True, None

    def generate_synthetic_spec(self, minimized_spec, *, seed: int, max_rows: int):
        return {
            "row_count": min(max_rows, 5),
            "columns": [
                {"name": "id", "type": "int", "example": 1},
                {"name": "amount", "type": "float", "example": 2.2},
                {"name": "category", "type": "str", "example": "A"},
            ],
        }


class _CountingProvider:
    provider_name = "openai"

    def __init__(self) -> None:
        self.calls = 0

    def health_check(self):
        return True, None

    def generate_synthetic_spec(self, minimized_spec, *, seed: int, max_rows: int):
        self.calls += 1
        return {
            "row_count": min(max_rows, 5),
            "columns": [
                {"name": "id", "type": "int", "example": seed},
                {"name": "amount", "type": "float", "example": float(self.calls)},
            ],
        }


@pytest.mark.parametrize("orch", ["airflow", "prefect", "dagster"])
def test_synthetic_mode_runtime_for_supported_orchestrators(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, orch: str) -> None:
    wf = _write_workflow(tmp_path)
    import pisces.energy.runtime_evaluation as reval

    monkeypatch.setattr(reval, "build_provider", lambda *args, **kwargs: _FakeProvider())

    req = EnergyEvaluationRequest(
        file_path=wf,
        code_text=wf.read_text(encoding="utf-8"),
        orchestrator=orch,
        repo_root=Path.cwd(),
        runtime_python=Path(sys.executable),
        mode_selected="synthetic",
        llm_provider="openai",
        llm_model="gpt-test",
        max_rows=100,
        max_tasks=5,
        timeout_s=30,
    )

    res = evaluate_energy(req).to_dict()
    assert res["mode_used"] == "synthetic"
    assert isinstance(res["runtime_measurement"], dict)
    assert res["runtime_measurement"].get("status") == "ok"
    assert res["runtime_measurement"].get("execution_adapter_requested") == "surrogate_benchmark"
    assert res["runtime_measurement"]["execution_adapter_used"] == "surrogate_benchmark"
    assert res["evidence"]["measurement_kind"] == "surrogate_benchmark"
    assert res["evidence"]["data_source"] == "synthetic"
    assert res["synthetic_info"]["signature_hash"]
    diagnostics = res["runtime_measurement"]["attempt_diagnostics"]
    operational = diagnostics["operational_metrics"]
    assert operational["evidence_role"] == "diagnostic"
    assert operational["wall_clock_s"] >= 0
    assert operational["process_cpu_s"] >= 0
    assert operational["peak_rss_mb"] is not None
    assert diagnostics["modeled_energy"]["evidence_role"] == "secondary"
    assert res["runtime_measurement"]["eligible_for_resource_comparison"] is False


def test_synthetic_cache_key_includes_seed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    wf = _write_workflow(tmp_path)
    import pisces.energy.runtime_evaluation as reval

    provider = _CountingProvider()
    monkeypatch.setattr(reval, "build_provider", lambda *args, **kwargs: provider)

    base = dict(
        file_path=wf,
        code_text=wf.read_text(encoding="utf-8"),
        orchestrator="airflow",
        repo_root=Path.cwd(),
        runtime_python=Path(sys.executable),
        mode_selected="synthetic",
        llm_provider="openai",
        llm_model="gpt-test",
        max_rows=100,
        max_tasks=5,
        timeout_s=30,
        artifacts_dir=tmp_path / "artifacts",
    )

    first = evaluate_energy(EnergyEvaluationRequest(**base, seed=1)).to_dict()
    second = evaluate_energy(EnergyEvaluationRequest(**base, seed=1)).to_dict()
    third = evaluate_energy(EnergyEvaluationRequest(**base, seed=2)).to_dict()

    assert provider.calls == 2
    assert first["synthetic_info"]["recipe_source"] == "provider"
    assert second["synthetic_info"]["recipe_source"] == "cache"
    assert third["synthetic_info"]["recipe_source"] == "provider"
    assert first["synthetic_info"]["recipe_cache_key"] != third["synthetic_info"]["recipe_cache_key"]


def test_synthetic_cache_refresh_and_off_modes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    wf = _write_workflow(tmp_path)
    import pisces.energy.runtime_evaluation as reval

    provider = _CountingProvider()
    monkeypatch.setattr(reval, "build_provider", lambda *args, **kwargs: provider)
    artifacts_dir = tmp_path / "artifacts"

    base = dict(
        file_path=wf,
        code_text=wf.read_text(encoding="utf-8"),
        orchestrator="airflow",
        repo_root=Path.cwd(),
        runtime_python=Path(sys.executable),
        mode_selected="synthetic",
        llm_provider="openai",
        llm_model="gpt-test",
        seed=1,
        max_rows=100,
        max_tasks=5,
        timeout_s=30,
        artifacts_dir=artifacts_dir,
    )

    cached = evaluate_energy(EnergyEvaluationRequest(**base)).to_dict()
    refreshed = evaluate_energy(EnergyEvaluationRequest(**base, synthetic_cache_mode="refresh")).to_dict()

    no_cache_dir = tmp_path / "no_cache_artifacts"
    off_first = evaluate_energy(
        EnergyEvaluationRequest(**{**base, "artifacts_dir": no_cache_dir}, synthetic_cache_mode="off")
    ).to_dict()
    off_second = evaluate_energy(
        EnergyEvaluationRequest(**{**base, "artifacts_dir": no_cache_dir}, synthetic_cache_mode="off")
    ).to_dict()

    assert cached["synthetic_info"]["recipe_source"] == "provider"
    assert refreshed["synthetic_info"]["recipe_source"] == "provider"
    assert refreshed["synthetic_info"]["cache_mode"] == "refresh"
    assert off_first["synthetic_info"]["recipe_source"] == "provider"
    assert off_second["synthetic_info"]["recipe_source"] == "provider"
    assert off_first["synthetic_info"]["cache_mode"] == "off"
    assert not (no_cache_dir / "synthetic_recipes.json").exists()
