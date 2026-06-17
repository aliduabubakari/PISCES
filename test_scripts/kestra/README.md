# Kestra — Declarative YAML Flow Support

Kestra is an open-source orchestration platform with a declarative YAML-based
workflow definition language. It supports both Python scripts and plugin-based
tasks.

PISCES supports Kestra flows through its **YAML structural analyzer**, which
parses flow manifests and extracts topology, trigger, and sustainability
signals **without executing any code**.

## Supported Features

- **Task topology** — sequential vs parallel tasks, flow structure
- **Trigger patterns** — schedule frequency (cron), event-driven triggers
- **Task types** — Python scripts, shell commands, plugin tasks
- **Retry and timeout** — Impact on energy estimation
- **Eco-design rules** — Same heuristic engine used for Python pipelines

## Sample Scripts

| Script | Description | Expected Rules |
|--------|-------------|---------------|
| `simple_etl.yaml` | Sequential 3-task ETL, daily schedule | No rules (clean baseline) |
| `iot_analytics.yaml` | 4 device groups in parallel, frequent schedule | `LARGE_FAN_OUT`, `HIGH_FREQUENCY_HEAVY_COMPUTE` |

## Usage

```bash
# Evaluate a Kestra flow manifest
pisces-unified test_scripts/kestra/simple_etl.yaml \
  --orchestrator kestra \
  --energy-mode heuristic --stdout

# With carbon tracking on the parse/evaluation step
pisces-unified test_scripts/kestra/iot_analytics.yaml \
  --orchestrator kestra \
  --track-carbon --carbon-country-iso ITA \
  --energy-mode heuristic --stdout
```

## Flow Format (for reference)

```yaml
id: weather_etl
namespace: data
tasks:
  - id: extract
    type: io.kestra.plugin.scripts.python.Script
    script: |
      import random
      stations = ["KBOS", "KORD", "KCLT"]
      print([{"station": s, "temp": random.uniform(5, 35)} for s in stations])

  - id: transform
    type: io.kestra.plugin.scripts.python.Script
    inputs:
      raw_data: "{{ outputs.extract.vars }}"
    script: |
      import json
      data = json.loads("{{ inputs.raw_data }}")
      print([{**r, "temp_f": r["temp"] * 9/5 + 32} for r in data])

triggers:
  - id: daily_schedule
    type: io.kestra.plugin.core.trigger.Schedule
    cron: "0 6 * * *"
```
