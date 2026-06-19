from __future__ import annotations

from pathlib import Path

from pisces.diagnostics import DiagnosticsConfig, collect_diagnostics


def test_collect_diagnostics_reports_package_and_workflow_status(tmp_path: Path) -> None:
    workflow = tmp_path / "flow.py"
    workflow.write_text("def task():\n    return 1\n", encoding="utf-8")

    report = collect_diagnostics(
        DiagnosticsConfig(
            workflow_path=workflow,
            validate_codecarbon=False,
            llm_api_key_env="PISCES_TEST_MISSING_KEY",
        )
    )

    assert report["tool"] == "pisces-diagnostics"
    assert report["packages"]["codecarbon"]["available"] in {True, False}
    assert report["workflow"]["exists"] is True
    assert report["checks"]["workflow_exists"] is True
    assert report["llm"]["api_key_env"]["name"] == "PISCES_TEST_MISSING_KEY"
    assert report["llm"]["api_key_env"]["present"] is False
    assert "missing_llm_api_key_env:PISCES_TEST_MISSING_KEY; synthetic provider refresh will fail" in report["warnings"]
