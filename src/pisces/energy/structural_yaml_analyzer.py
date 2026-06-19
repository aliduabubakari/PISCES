#!/usr/bin/env python3
"""
Structural YAML Pipeline Analyzer (v1.0)
========================================

Parses declarative YAML-based workflow definitions (Argo Workflows, Kestra)
and extracts topology, resource, and sustainability signals without executing
any code.

Supports:
- Argo Workflows (Kubernetes-native): templates, steps, DAGs, containers, CronWorkflows
- Kestra: flows, tasks, triggers, plugin types

Design goals:
- Zero runtime execution needed
- Extracts task count, edge count, parallelism, resource estimates
- Feeds into the same EcoDesignRuleEngine used by Python-based pipelines
- Opt-in YAML dependency (falls back gracefully if PyYAML not installed)
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    import yaml  # type: ignore
    HAS_YAML = True
except ImportError:
    HAS_YAML = False

SCHEMA_VERSION = "1.0.0"
YAML_ORCHESTRATORS = {"argo", "kestra"}


# ---------------------------------------------------------------------------
# Supporting data classes
# ---------------------------------------------------------------------------

@dataclass
class YamlTask:
    """A single task/step/template extracted from a YAML pipeline."""
    task_id: str
    task_type: str          # e.g. 'container', 'script', 'steps', 'dag', 'plugin'
    parent_template: Optional[str] = None
    dependencies: List[str] = field(default_factory=list)
    resources: Dict[str, Any] = field(default_factory=dict)
    retry_count: int = 0
    timeout_seconds: Optional[int] = None
    is_sensor: bool = False
    is_heavy_compute: bool = False
    parallelism_hint: int = 1


@dataclass
class YamlTopology:
    """Extracted topology from a YAML pipeline manifest."""
    orchestrator: str
    entrypoint: Optional[str] = None
    tasks: List[YamlTask] = field(default_factory=list)
    total_tasks: int = 0
    total_edges: int = 0
    max_fan_out: int = 0
    max_fan_in: int = 0
    parallelism_index: Optional[float] = None
    schedule: Optional[str] = None
    runs_per_year_estimate: Optional[int] = None
    resources_summary: Dict[str, Any] = field(default_factory=dict)
    signals: Dict[str, Any] = field(default_factory=dict)
    raw_metadata: Dict[str, Any] = field(default_factory=dict)

    def to_profile(self, code_sha256: str = "") -> Dict[str, Any]:
        """Convert to the standard profile dict expected by EcoDesignRuleEngine."""
        return {
            "schema_version": SCHEMA_VERSION,
            "orchestrator": self.orchestrator,
            "code_sha256": code_sha256,
            "metrics": {
                "total_tasks": self.total_tasks,
                "total_edges": self.total_edges,
                "max_fan_out": self.max_fan_out,
                "max_fan_in": self.max_fan_in,
                "parallelism_index": self.parallelism_index,
                "sequential_bottleneck_ratio": (
                    1.0 - (self.parallelism_index or 0.0)
                    if self.parallelism_index is not None
                    else None
                ),
            },
            "schedule": {
                "enabled": self.schedule is not None,
                "schedule_expression": self.schedule,
                "runs_per_year_estimate": self.runs_per_year_estimate,
                "source": "yaml_manifest",
            },
            "signals": {
                "operator_tiers": {
                    "heavy_compute_count": sum(
                        1 for t in self.tasks if t.is_heavy_compute
                    ),
                    "polling_heavy_count": sum(
                        1 for t in self.tasks if t.is_sensor
                    ),
                },
                "sensors": {
                    "has_sensor": any(t.is_sensor for t in self.tasks),
                    "poke_mode_detected": any(t.is_sensor for t in self.tasks),
                },
                "xcom": {
                    "xcom_total_signals": self._estimate_data_exchanges(),
                },
                "dependency_info": {
                    "dependency_syntax_detected": self.total_edges > 0,
                },
            },
            "edges": {
                "effective_source": "yaml_manifest",
                "code_edges_compared_count": self.total_edges,
                "opos_edges_count": self.total_edges,
            },
            "resources": self.resources_summary,
            "yaml_metadata": self.raw_metadata,
        }

    def _estimate_data_exchanges(self) -> int:
        """Estimate data exchanges from task dependencies."""
        # Each edge roughly represents a data exchange
        # Add extra for fan-out / fan-in patterns
        base = self.total_edges
        if self.max_fan_out > 3:
            base += self.max_fan_out
        if self.max_fan_in > 3:
            base += self.max_fan_in
        return base


# ---------------------------------------------------------------------------
# Argo Workflows parser
# ---------------------------------------------------------------------------

class ArgoParser:
    """Parse Argo Workflow YAML manifests into YamlTopology."""

    @staticmethod
    def can_parse(manifest: Dict[str, Any]) -> bool:
        """Check if manifest looks like an Argo Workflow."""
        if not isinstance(manifest, dict):
            return False
        kind = str(manifest.get("kind", "")).lower()
        api = str(manifest.get("apiVersion", ""))
        return kind in {"workflow", "cronworkflow", "workflowtemplate"} and (
            "argoproj.io" in api
        )

    @staticmethod
    def parse(manifest: Dict[str, Any]) -> YamlTopology:
        """Parse an Argo Workflow manifest into topology."""
        kind = str(manifest.get("kind", "Workflow"))
        metadata = manifest.get("metadata", {}) if isinstance(manifest.get("metadata"), dict) else {}
        spec = manifest.get("spec", {}) if isinstance(manifest.get("spec"), dict) else {}

        # CronWorkflow nests the workflow spec under spec.workflowSpec
        if kind.lower() == "cronworkflow":
            workflow_spec = spec.get("workflowSpec", {})
            if isinstance(workflow_spec, dict):
                spec = workflow_spec

        entrypoint = spec.get("entrypoint", "main")
        templates_raw = spec.get("templates", [])
        if not isinstance(templates_raw, list):
            templates_raw = []

        # Parse schedule from CronWorkflow
        schedule = None
        runs_per_year = None
        if kind.lower() == "cronworkflow":
            # Schedule is at spec.schedule (not inside workflowSpec)
            original_spec = manifest.get("spec", {}) if isinstance(manifest.get("spec"), dict) else {}
            schedule = original_spec.get("schedule")
            runs_per_year = _estimate_runs_from_cron(schedule)

        # Parse all templates into tasks
        tasks: List[YamlTask] = []
        name_to_task: Dict[str, YamlTask] = {}
        edges: List[Tuple[str, str]] = []

        for tmpl in templates_raw:
            if not isinstance(tmpl, dict):
                continue
            name = tmpl.get("name", f"template_{len(tasks)}")
            task = ArgoParser._parse_template(tmpl, name)
            tasks.append(task)
            name_to_task[name] = task

        # Extract edges from steps/DAG templates
        for tmpl in templates_raw:
            if not isinstance(tmpl, dict):
                continue
            parent_name = tmpl.get("name", "")
            edges.extend(ArgoParser._extract_edges(tmpl, parent_name, name_to_task))

        # Compute topology metrics
        total_tasks = len(tasks)
        unique_edges = list(set(edges))
        total_edges = len(unique_edges)

        # Fan-out / fan-in
        out_degree: Dict[str, int] = {}
        in_degree: Dict[str, int] = {}
        for src, dst in unique_edges:
            out_degree[src] = out_degree.get(src, 0) + 1
            in_degree[dst] = in_degree.get(dst, 0) + 1

        max_fan_out = max(out_degree.values()) if out_degree else 0
        max_fan_in = max(in_degree.values()) if in_degree else 0

        # Parallelism index: avg out-degree / max out-degree
        avg_out = sum(out_degree.values()) / max(len(out_degree), 1)
        parallelism_index = round(avg_out / max(max_fan_out, 1), 3) if max_fan_out > 0 else 0.0

        # Resource summary
        total_cpu = 0.0
        total_memory_mb = 0.0
        for t in tasks:
            res = t.resources
            if res.get("cpu"):
                total_cpu += float(res["cpu"])
            if res.get("memory_mb"):
                total_memory_mb += float(res["memory_mb"])

        return YamlTopology(
            orchestrator="argo",
            entrypoint=entrypoint,
            tasks=tasks,
            total_tasks=total_tasks,
            total_edges=total_edges,
            max_fan_out=max_fan_out,
            max_fan_in=max_fan_in,
            parallelism_index=parallelism_index,
            schedule=schedule,
            runs_per_year_estimate=runs_per_year,
            resources_summary={
                "total_cpu_cores": total_cpu,
                "total_memory_mb": total_memory_mb,
                "has_resource_requests": total_cpu > 0 or total_memory_mb > 0,
            },
            signals={
                "retry_total": sum(t.retry_count for t in tasks),
                "timeout_total": sum(t.timeout_seconds or 0 for t in tasks),
                "sensor_count": sum(1 for t in tasks if t.is_sensor),
            },
            raw_metadata={
                "kind": kind,
                "name": str(metadata.get("name", "")),
                "namespace": str(metadata.get("namespace", "")),
            },
        )

    @staticmethod
    def _parse_template(tmpl: Dict[str, Any], name: str) -> YamlTask:
        """Parse a single Argo template into a YamlTask."""
        task_type = "unknown"
        resources = {}
        is_heavy = False
        retry_count = 0
        timeout_s = None

        # Determine template type
        if "container" in tmpl:
            task_type = "container"
            container = tmpl.get("container", {}) if isinstance(tmpl.get("container"), dict) else {}
            resources = ArgoParser._parse_resources(container.get("resources", {}))
            if resources.get("cpu", 0) >= 2 or resources.get("memory_mb", 0) >= 2048:
                is_heavy = True
        elif "script" in tmpl:
            task_type = "script"
            script = tmpl.get("script", {}) if isinstance(tmpl.get("script"), dict) else {}
            resources = ArgoParser._parse_resources(script.get("resources", {}))
        elif "steps" in tmpl:
            task_type = "steps"
        elif "dag" in tmpl:
            task_type = "dag"
        elif "resource" in tmpl:
            task_type = "resource"
        elif "suspend" in tmpl:
            task_type = "suspend"

        # Retry strategy
        retry = tmpl.get("retryStrategy", {}) if isinstance(tmpl.get("retryStrategy"), dict) else {}
        if isinstance(retry, dict):
            retry_count = retry.get("limit", 0)

        # Timeout
        timeout_raw = tmpl.get("timeout")
        if isinstance(timeout_raw, str):
            timeout_s = _parse_duration_to_seconds(timeout_raw)

        # Sensor-like: suspend templates wait for external events
        is_sensor = task_type == "suspend"

        return YamlTask(
            task_id=name,
            task_type=task_type,
            resources=resources,
            retry_count=int(retry_count) if retry_count else 0,
            timeout_seconds=timeout_s,
            is_sensor=is_sensor,
            is_heavy_compute=is_heavy,
        )

    @staticmethod
    def _parse_resources(resources: Any) -> Dict[str, Any]:
        """Parse Kubernetes resource requests/limits."""
        if not isinstance(resources, dict):
            return {}
        result: Dict[str, Any] = {}
        for scope in ("requests", "limits"):
            scope_dict = resources.get(scope, {})
            if not isinstance(scope_dict, dict) or not scope_dict:
                continue
            cpu = scope_dict.get("cpu", "")
            if isinstance(cpu, str) and cpu:
                cpu_val = cpu.rstrip("m")
                parsed_cpu = float(cpu_val) / 1000.0 if cpu_val.isdigit() else (float(cpu_val) if cpu_val else 0.0)
                if parsed_cpu > 0:
                    result["cpu"] = parsed_cpu
            memory = scope_dict.get("memory", "")
            if isinstance(memory, str) and memory:
                memory_mb = _parse_memory_to_mb(memory)
                if memory_mb > 0:
                    result["memory_mb"] = memory_mb
        return result

    @staticmethod
    def _extract_edges(
        tmpl: Dict[str, Any],
        parent_name: str,
        name_to_task: Dict[str, YamlTask],
    ) -> List[Tuple[str, str]]:
        """Extract dependency edges from steps or DAG templates."""
        edges: List[Tuple[str, str]] = []

        steps_raw = tmpl.get("steps")
        if isinstance(steps_raw, list):
            prev_names: List[str] = []
            for step_group in steps_raw:
                if not isinstance(step_group, list):
                    continue
                group_names: List[str] = []
                for step in step_group:
                    if not isinstance(step, dict):
                        continue
                    step_name = step.get("name", "")
                    template_ref = step.get("template", "")
                    task_name = f"{parent_name}.{step_name}" if parent_name else step_name

                    # Dependencies from 'dependencies' field or sequential
                    deps = step.get("dependencies", [])
                    if isinstance(deps, list) and deps:
                        for dep in deps:
                            dep_task = f"{parent_name}.{dep}" if parent_name else dep
                            edges.append((dep_task, task_name))
                    elif prev_names:
                        # Sequential: depend on previous group
                        for prev in prev_names:
                            edges.append((prev, task_name))

                    # Also add edge from template ref
                    if template_ref and template_ref != task_name:
                        pass  # Template ref is for execution, not dependency

                    group_names.append(task_name)

                prev_names = group_names

        # Dag mode
        dag_raw = tmpl.get("dag")
        if isinstance(dag_raw, dict):
            dag_tasks = dag_raw.get("tasks", [])
            if isinstance(dag_tasks, list):
                for dag_task in dag_tasks:
                    if not isinstance(dag_task, dict):
                        continue
                    task_name = dag_task.get("name", "")
                    task_name = f"{parent_name}.{task_name}" if parent_name else task_name
                    deps = dag_task.get("dependencies", [])
                    if isinstance(deps, list):
                        for dep in deps:
                            dep_name = f"{parent_name}.{dep}" if parent_name else dep
                            edges.append((dep_name, task_name))

        return edges


# ---------------------------------------------------------------------------
# Kestra parser
# ---------------------------------------------------------------------------

class KestraParser:
    """Parse Kestra flow YAML manifests into YamlTopology."""

    @staticmethod
    def can_parse(manifest: Dict[str, Any]) -> bool:
        """Check if manifest looks like a Kestra flow."""
        if not isinstance(manifest, dict):
            return False
        # Kestra flows have 'id', 'namespace', and 'tasks' keys
        return "id" in manifest and "namespace" in manifest and "tasks" in manifest

    @staticmethod
    def parse(manifest: Dict[str, Any]) -> YamlTopology:
        """Parse a Kestra flow manifest into topology."""
        flow_id = str(manifest.get("id", "unknown"))
        namespace = str(manifest.get("namespace", "default"))
        description = str(manifest.get("description", ""))

        tasks_raw = manifest.get("tasks", [])
        if not isinstance(tasks_raw, list):
            tasks_raw = []

        triggers_raw = manifest.get("triggers", [])
        if not isinstance(triggers_raw, list):
            triggers_raw = []

        # Parse schedule from triggers
        schedule = None
        runs_per_year = None
        for trigger in triggers_raw:
            if not isinstance(trigger, dict):
                continue
            trigger_type = trigger.get("type", "")
            if "schedule" in trigger_type.lower():
                schedule = trigger.get("cron") or trigger.get("schedule")
                if schedule:
                    runs_per_year = _estimate_runs_from_cron(str(schedule))

        # Parse tasks
        tasks: List[YamlTask] = []
        edges: List[Tuple[str, str]] = []

        for i, task in enumerate(tasks_raw):
            if not isinstance(task, dict):
                continue
            parsed = KestraParser._parse_task(task, i)
            tasks.append(parsed)

        # Edge extraction from task IDs and their runAfter references
        for task in tasks_raw:
            if not isinstance(task, dict):
                continue
            task_id = task.get("id", "")
            # Kestra doesn't have explicit dependencies in the basic model -
            # tasks are sequential by default. But some task types have inputs
            # that reference other tasks
            inputs = task.get("inputs", {})
            if isinstance(inputs, dict):
                for key, value in inputs.items():
                    if isinstance(value, str) and "{{ outputs." in value:
                        # Extract referenced task from template syntax
                        m = re.search(r"\{\{\s*outputs\.(\w+)", value)
                        if m:
                            dep_id = m.group(1)
                            edges.append((dep_id, task_id))

        # If no explicit edges, assume sequential
        if not edges and len(tasks) > 1:
            for i in range(len(tasks) - 1):
                edges.append((tasks[i].task_id, tasks[i + 1].task_id))

        total_tasks = len(tasks)
        total_edges = len(edges)

        out_degree: Dict[str, int] = {}
        in_degree: Dict[str, int] = {}
        for src, dst in edges:
            out_degree[src] = out_degree.get(src, 0) + 1
            in_degree[dst] = in_degree.get(dst, 0) + 1

        max_fan_out = max(out_degree.values()) if out_degree else 0
        max_fan_in = max(in_degree.values()) if in_degree else 0

        avg_out = sum(out_degree.values()) / max(len(out_degree), 1)
        parallelism_index = round(avg_out / max(max_fan_out, 1), 3) if max_fan_out > 0 else 0.0

        return YamlTopology(
            orchestrator="kestra",
            entrypoint=flow_id,
            tasks=tasks,
            total_tasks=total_tasks,
            total_edges=total_edges,
            max_fan_out=max_fan_out,
            max_fan_in=max_fan_in,
            parallelism_index=parallelism_index,
            schedule=schedule,
            runs_per_year_estimate=runs_per_year,
            resources_summary={
                "task_count": total_tasks,
                "plugin_types": list(set(t.task_type for t in tasks)),
            },
            signals={
                "retry_total": sum(t.retry_count for t in tasks),
                "timeout_total": sum(t.timeout_seconds or 0 for t in tasks),
            },
            raw_metadata={
                "flow_id": flow_id,
                "namespace": namespace,
                "description": description,
                "trigger_count": len(triggers_raw),
            },
        )

    @staticmethod
    def _parse_task(task: Dict[str, Any], index: int) -> YamlTask:
        """Parse a single Kestra task into a YamlTask."""
        task_id = task.get("id", f"task_{index}")
        task_type = task.get("type", "unknown")

        # Determine if heavy compute based on task type
        is_heavy = any(
            keyword in task_type.lower()
            for keyword in ("spark", "python", "script", "jdbc", "query", "dbt")
        )

        # Parse retry
        retry = task.get("retry", {})
        retry_count = 0
        if isinstance(retry, dict):
            retry_count = retry.get("maxAttempt", 0) or retry.get("max_attempt", 0)

        # Parse timeout
        timeout_raw = task.get("timeout")
        timeout_s = None
        if isinstance(timeout_raw, str):
            timeout_s = _parse_duration_to_seconds(timeout_raw)
        elif isinstance(timeout_raw, (int, float)):
            timeout_s = int(timeout_raw)

        return YamlTask(
            task_id=task_id,
            task_type=task_type,
            retry_count=int(retry_count) if retry_count else 0,
            timeout_seconds=timeout_s,
            is_sensor=False,
            is_heavy_compute=is_heavy,
        )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def detect_yaml_orchestrator(manifest: Dict[str, Any]) -> Optional[str]:
    """Detect which orchestrator a YAML manifest belongs to."""
    if ArgoParser.can_parse(manifest):
        return "argo"
    if KestraParser.can_parse(manifest):
        return "kestra"
    return None


def parse_yaml_manifest(
    file_path: Path,
    orchestrator: Optional[str] = None,
) -> YamlTopology:
    """Parse a YAML manifest file into topology, auto-detecting the orchestrator.

    Args:
        file_path: Path to the YAML manifest file.
        orchestrator: Force a specific orchestrator ('argo' or 'kestra').
                      If None, auto-detects.

    Returns:
        YamlTopology with extracted structure.

    Raises:
        ImportError: If PyYAML is not installed.
        ValueError: If the manifest cannot be parsed.
    """
    if not HAS_YAML:
        raise ImportError(
            "PyYAML is required for YAML pipeline analysis. "
            "Install with: pip install pisces[yaml]"
        )

    text = file_path.read_text(encoding="utf-8", errors="ignore")

    try:
        manifest = yaml.safe_load(text)
    except Exception as e:
        raise ValueError(f"Failed to parse YAML: {e}")

    if not isinstance(manifest, dict):
        raise ValueError("YAML manifest is not a mapping/object")

    if orchestrator:
        orch = orchestrator.strip().lower()
        if orch == "argo":
            topo = ArgoParser.parse(manifest)
        elif orch == "kestra":
            topo = KestraParser.parse(manifest)
        else:
            raise ValueError(f"Unknown orchestrator for YAML: {orchestrator}")
    else:
        detected = detect_yaml_orchestrator(manifest)
        if detected == "argo":
            topo = ArgoParser.parse(manifest)
        elif detected == "kestra":
            topo = KestraParser.parse(manifest)
        else:
            raise ValueError(
                "Could not detect orchestrator from YAML manifest. "
                "Specify --orchestrator argo or --orchestrator kestra."
            )

    return topo


def analyze_yaml_workflow(
    file_path: Path,
    orchestrator: Optional[str] = None,
) -> Dict[str, Any]:
    """Main entry point: analyze a YAML workflow and return the standard profile dict.

    This is the YAML equivalent of StructuralEnergyAnalyzer.analyze().
    """
    topology = parse_yaml_manifest(file_path, orchestrator=orchestrator)
    text = file_path.read_text(encoding="utf-8", errors="ignore")
    code_sha256 = hashlib.sha256(text.encode()).hexdigest()
    return topology.to_profile(code_sha256=code_sha256)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _estimate_runs_from_cron(expression: Optional[str]) -> Optional[int]:
    """Estimate runs per year from a cron expression."""
    if not expression:
        return None

    expr = expression.strip()

    # Minute-based patterns
    if expr.startswith("*/1 ") or expr.startswith("* "):
        return 525600  # Every minute
    if "*/5 " in expr or expr.startswith("*/5 "):
        return 105120
    if "*/15 " in expr or expr.startswith("*/15 "):
        return 35040
    if "*/30 " in expr or expr.startswith("*/30 "):
        return 17520

    # Hourly
    if expr.startswith("0 *") or expr.startswith("@hourly"):
        return 8760
    if expr.startswith("0 */") or "0 */" in expr:
        try:
            hours = int(expr.split("*/")[1].split()[0]) if "*/" in expr else 1
            return 8760 // hours
        except Exception:
            return 8760

    # Daily
    if "@daily" in expr:
        return 365

    # Weekly
    if "@weekly" in expr:
        return 52

    # Default: assume daily
    if len(expr.split()) >= 5:
        return 365

    return 365


def _parse_memory_to_mb(memory: str) -> float:
    """Parse Kubernetes memory string to MB."""
    memory = memory.strip().upper()
    multipliers = {
        "KI": 1 / 1024,
        "MI": 1,
        "GI": 1024,
        "TI": 1024 * 1024,
        "K": 1 / 1024,
        "M": 1,
        "G": 1024,
        "T": 1024 * 1024,
    }

    for suffix, mult in multipliers.items():
        if memory.endswith(suffix):
            try:
                return float(memory[: -len(suffix)]) * mult
            except ValueError:
                return 0.0

    # Plain number (assume bytes)
    try:
        return float(memory) / (1024 * 1024)
    except ValueError:
        return 0.0


def _parse_duration_to_seconds(duration: str) -> Optional[int]:
    """Parse a duration string like '5m', '1h', '300s' to seconds."""
    import re
    if not duration:
        return None

    total = 0
    pattern = re.compile(r"(\d+)\s*(h|m|s|d)")
    for match in pattern.finditer(duration.lower()):
        value = int(match.group(1))
        unit = match.group(2)
        if unit == "d":
            total += value * 86400
        elif unit == "h":
            total += value * 3600
        elif unit == "m":
            total += value * 60
        elif unit == "s":
            total += value
    return total if total > 0 else None
