"""
Tests for structural_yaml_analyzer.py — Argo Workflows and Kestra support.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pisces.energy.structural_yaml_analyzer import (
    ArgoParser,
    KestraParser,
    YamlTopology,
    YamlTask,
    analyze_yaml_workflow,
    detect_yaml_orchestrator,
    parse_yaml_manifest,
    _estimate_runs_from_cron,
    _parse_memory_to_mb,
    _parse_duration_to_seconds,
)


# ---------------------------------------------------------------------------
# Sample manifests
# ---------------------------------------------------------------------------

ARGO_SIMPLE = {
    "apiVersion": "argoproj.io/v1alpha1",
    "kind": "Workflow",
    "metadata": {"name": "test-wf"},
    "spec": {
        "entrypoint": "main",
        "templates": [
            {
                "name": "main",
                "steps": [
                    [{"name": "step1", "template": "task-a"}],
                    [{"name": "step2", "template": "task-b"}],
                ],
            },
            {
                "name": "task-a",
                "container": {
                    "image": "python:3.11",
                    "command": ["echo", "hello"],
                    "resources": {
                        "requests": {"cpu": "500m", "memory": "256Mi"},
                    },
                },
            },
            {
                "name": "task-b",
                "container": {
                    "image": "python:3.11",
                    "command": ["echo", "world"],
                    "resources": {
                        "requests": {"cpu": "100m", "memory": "128Mi"},
                    },
                },
            },
        ],
    },
}

ARGO_DAG = {
    "apiVersion": "argoproj.io/v1alpha1",
    "kind": "Workflow",
    "metadata": {"name": "dag-wf"},
    "spec": {
        "entrypoint": "dag-main",
        "templates": [
            {
                "name": "dag-main",
                "dag": {
                    "tasks": [
                        {"name": "A", "template": "echo"},
                        {"name": "B", "template": "echo", "dependencies": ["A"]},
                        {"name": "C", "template": "echo", "dependencies": ["A"]},
                        {"name": "D", "template": "echo", "dependencies": ["B", "C"]},
                    ],
                },
            },
            {"name": "echo", "container": {"image": "alpine", "command": ["echo"]}},
        ],
    },
}

KESTRA_SIMPLE = {
    "id": "test_flow",
    "namespace": "test",
    "description": "A test flow",
    "tasks": [
        {
            "id": "extract",
            "type": "io.kestra.plugin.scripts.python.Script",
            "script": "print('hello')",
        },
        {
            "id": "transform",
            "type": "io.kestra.plugin.scripts.python.Script",
            "inputs": {
                "data": "{{ outputs.extract.vars }}",
            },
            "script": "print('world')",
        },
        {
            "id": "load",
            "type": "io.kestra.plugin.scripts.python.Script",
            "inputs": {
                "data": "{{ outputs.transform.vars }}",
            },
            "script": "print('done')",
        },
    ],
    "triggers": [
        {"id": "daily", "type": "io.kestra.plugin.core.trigger.Schedule", "cron": "0 6 * * *"},
    ],
}


# ---------------------------------------------------------------------------
# Detection tests
# ---------------------------------------------------------------------------

def test_detect_argo_manifest():
    assert detect_yaml_orchestrator(ARGO_SIMPLE) == "argo"


def test_detect_kestra_manifest():
    assert detect_yaml_orchestrator(KESTRA_SIMPLE) == "kestra"


def test_detect_unknown_returns_none():
    assert detect_yaml_orchestrator({"foo": "bar"}) is None


# ---------------------------------------------------------------------------
# Argo parser tests
# ---------------------------------------------------------------------------

def test_argo_can_parse():
    assert ArgoParser.can_parse(ARGO_SIMPLE) is True
    assert ArgoParser.can_parse({"foo": "bar"}) is False


def test_argo_simple_topology():
    topo = ArgoParser.parse(ARGO_SIMPLE)
    assert topo.orchestrator == "argo"
    assert topo.entrypoint == "main"
    assert topo.total_tasks >= 3  # main + task-a + task-b
    assert topo.total_edges >= 1  # step1 → step2 (sequential)


def test_argo_dag_topology():
    topo = ArgoParser.parse(ARGO_DAG)
    # Templates: dag-main + echo = 2 templates
    # DAG tasks A, B, C, D are entries within dag-main
    assert topo.total_tasks >= 2
    # DAG edges: A→B, A→C, B→D, C→D
    assert topo.total_edges >= 4
    assert topo.max_fan_out >= 2  # A has 2 children (B, C)


def test_argo_profile_output():
    topo = ArgoParser.parse(ARGO_SIMPLE)
    profile = topo.to_profile(code_sha256="abc123")
    assert profile["orchestrator"] == "argo"
    assert profile["metrics"]["total_tasks"] >= 3
    assert isinstance(profile["metrics"]["parallelism_index"], (int, float))
    assert "signals" in profile
    assert "schedule" in profile


def test_argo_resource_parsing():
    topo = ArgoParser.parse(ARGO_SIMPLE)
    # task-a has cpu=500m, memory=256Mi
    heavy_tasks = [t for t in topo.tasks if t.is_heavy_compute]
    # 500m = 0.5 CPU, not heavy. Heavy is >= 2 CPU or >= 2048 MiB
    assert len(heavy_tasks) == 0


# ---------------------------------------------------------------------------
# Kestra parser tests
# ---------------------------------------------------------------------------

def test_kestra_can_parse():
    assert KestraParser.can_parse(KESTRA_SIMPLE) is True
    assert KestraParser.can_parse({"foo": "bar"}) is False


def test_kestra_simple_topology():
    topo = KestraParser.parse(KESTRA_SIMPLE)
    assert topo.orchestrator == "kestra"
    assert topo.entrypoint == "test_flow"
    assert topo.total_tasks == 3
    # With template references in inputs, edges should be detected
    assert topo.total_edges >= 1


def test_kestra_schedule_detection():
    topo = KestraParser.parse(KESTRA_SIMPLE)
    assert topo.schedule is not None
    assert "0 6 * * *" in str(topo.schedule)
    # Daily cron → ~365 runs/year
    assert topo.runs_per_year_estimate is not None
    assert topo.runs_per_year_estimate >= 300


def test_kestra_profile_output():
    topo = KestraParser.parse(KESTRA_SIMPLE)
    profile = topo.to_profile(code_sha256="def456")
    assert profile["orchestrator"] == "kestra"
    assert profile["metrics"]["total_tasks"] == 3
    assert profile["schedule"]["enabled"] is True


# ---------------------------------------------------------------------------
# YAML analyzer entry point tests
# ---------------------------------------------------------------------------

def test_analyze_yaml_workflow_argo(tmp_path: Path):
    import yaml
    wf = tmp_path / "test.yaml"
    wf.write_text(yaml.dump(ARGO_SIMPLE), encoding="utf-8")
    profile = analyze_yaml_workflow(wf)
    assert profile["orchestrator"] == "argo"
    assert profile["metrics"]["total_tasks"] >= 3


def test_analyze_yaml_workflow_kestra(tmp_path: Path):
    import yaml
    wf = tmp_path / "test.yaml"
    wf.write_text(yaml.dump(KESTRA_SIMPLE), encoding="utf-8")
    profile = analyze_yaml_workflow(wf)
    assert profile["orchestrator"] == "kestra"
    assert profile["metrics"]["total_tasks"] == 3


def test_analyze_yaml_with_explicit_orchestrator(tmp_path: Path):
    import yaml
    wf = tmp_path / "test.yaml"
    wf.write_text(yaml.dump(ARGO_SIMPLE), encoding="utf-8")
    profile = analyze_yaml_workflow(wf, orchestrator="argo")
    assert profile["orchestrator"] == "argo"


# ---------------------------------------------------------------------------
# Helper function tests
# ---------------------------------------------------------------------------

def test_estimate_runs_from_cron():
    assert _estimate_runs_from_cron("*/5 * * * *") == 105120
    assert _estimate_runs_from_cron("0 * * * *") == 8760
    assert _estimate_runs_from_cron("@daily") == 365
    assert _estimate_runs_from_cron(None) is None


def test_parse_memory_to_mb():
    assert _parse_memory_to_mb("128Mi") == 128.0
    assert _parse_memory_to_mb("1Gi") == 1024.0
    assert _parse_memory_to_mb("256M") == 256.0
    assert _parse_memory_to_mb("512Ki") == 0.5


def test_parse_duration_to_seconds():
    assert _parse_duration_to_seconds("5m") == 300
    assert _parse_duration_to_seconds("1h") == 3600
    assert _parse_duration_to_seconds("1h30m") == 5400
    assert _parse_duration_to_seconds("300s") == 300
    assert _parse_duration_to_seconds(None) is None


# ---------------------------------------------------------------------------
# Zero-fill YAML integration tests
# ---------------------------------------------------------------------------

def test_zero_fill_detects_yaml():
    from pisces.energy.zero_fill import generate_zero_fill_dataset, _is_yaml_content
    import yaml
    yaml_text = yaml.dump(ARGO_SIMPLE)
    assert _is_yaml_content(yaml_text) is True

    py_text = "from airflow import DAG\n\nwith DAG('x'):\n    pass\n"
    assert _is_yaml_content(py_text) is False


def test_zero_fill_generates_for_yaml():
    from pisces.energy.zero_fill import generate_zero_fill_dataset
    import yaml
    yaml_text = yaml.dump(ARGO_SIMPLE)
    dataset = generate_zero_fill_dataset(code_text=yaml_text, max_rows=5, seed=42)
    assert dataset["row_count"] >= 1
    assert len(dataset["schema"]) >= 1
    assert "inference_method" in str(dataset["provenance"])
    # Should detect it's a YAML-based pipeline
    assert dataset["meta"]["tasks_detected"] > 0 or "yaml" in str(dataset["provenance"]).lower()


# ---------------------------------------------------------------------------
# YamlTopology serialization
# ---------------------------------------------------------------------------

def test_yaml_topology_to_dict():
    task = YamlTask(task_id="test", task_type="container", retry_count=2)
    topo = YamlTopology(
        orchestrator="argo",
        entrypoint="main",
        tasks=[task],
        total_tasks=1,
        total_edges=0,
    )
    profile = topo.to_profile()
    assert isinstance(profile, dict)
    assert profile["orchestrator"] == "argo"
