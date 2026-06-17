# Argo Workflows — Declarative YAML Pipeline Support

Argo Workflows is a Kubernetes-native workflow engine for orchestrating
parallel jobs on Kubernetes. Workflows are defined as YAML manifests.

PISCES supports Argo Workflows through its **YAML structural analyzer**,
which parses manifest files and extracts topology, resource, and
sustainability signals **without executing any code**.

## Supported Features

- **DAG/task topology** — step/template count, parallelism, fan-out, fan-in
- **Resource requests** — CPU/memory per container for energy estimation
- **Retry strategies** — Impact on expected runtime energy
- **Schedule frequency** — CronWorkflow recurrence patterns for annualised
  carbon estimates
- **Eco-design rules** — Same heuristic engine used for Python pipelines

## Sample Scripts

| Script | Description | Expected Rules |
|--------|-------------|---------------|
| `simple_etl.yaml` | Sequential 3-step ETL, no schedule | No rules (clean baseline) |
| `multi_city_dag.yaml` | 6 parallel city branches, hourly schedule | `LARGE_FAN_OUT`, `HIGH_FREQUENCY_HEAVY_COMPUTE` |

## Usage

```bash
# Evaluate an Argo workflow manifest
pisces-unified test_scripts/argo/simple_etl.yaml \
  --orchestrator argo \
  --energy-mode heuristic --stdout

# With carbon tracking on the parse/evaluation step
pisces-unified test_scripts/argo/multi_city_dag.yaml \
  --orchestrator argo \
  --track-carbon --carbon-country-iso ITA \
  --energy-mode heuristic --stdout
```

## Manifest Format (for reference)

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  name: weather-pipeline
spec:
  entrypoint: weather-etl
  templates:
  - name: weather-etl
    steps:
    - - name: extract
        template: fetch-weather
    - - name: transform
        template: process-data
  - name: fetch-weather
    container:
      image: python:3.11
      command: [python, -c]
      args: ["print('fetching weather data...')"]
      resources:
        requests:
          cpu: "250m"
          memory: "128Mi"
```
