# PISCES Test Scripts

Sample workflow scripts for testing the PISCES sustainability evaluation
framework across orchestrators. Each script is designed to exercise
specific eco-design patterns and energy characteristics.

## Supported Orchestrators

| Orchestrator | Status | Scripts | Patterns Tested |
|-------------|--------|---------|-----------------|
| **Airflow** | ✅ Full support | 4 scripts | Fan-out, sensors, XCom, heavy compute |
| **Dagster** | ✅ Full support | 2 scripts | Software-defined assets, partitions |
| **Prefect** | ✅ Full support | 2 scripts | Concurrent tasks, subflows, retries |
| **Argo Workflows** | ✅ Full support | 2 scripts | Declarative YAML, CronWorkflow, DAG topology |
| **Kestra** | ✅ Full support | 2 scripts | Declarative YAML, plugin tasks, triggers |

## Directory Structure

```
test_scripts/
├── airflow/
│   ├── simple_etl.py            # Basic ETL (baseline)
│   ├── weather_pipeline.py      # Multi-branch fan-out/fan-in
│   ├── sensor_heavy.py         # Poke-mode sensors, high frequency
│   └── batch_processing.py      # Heavy compute, large fan-out
├── dagster/
│   ├── simple_asset_pipeline.py # Asset-based ETL
│   └── multi_asset_job.py       # Multi-asset with partitions
├── prefect/
│   ├── simple_flow.py           # Task-based ETL
│   └── parameterized_etl.py     # Concurrent fan-out with subflows
├── argo/
│   ├── README.md                # Argo Workflows YAML support
│   ├── simple_etl.yaml          # Sequential 3-step ETL
│   └── multi_city_dag.yaml      # Parallel fan-out DAG
└── kestra/
    ├── README.md                # Kestra flow YAML support
    ├── simple_etl.yaml          # Sequential 3-task flow
    └── iot_analytics.yaml       # Parallel IoT pipeline
```

## Quick Usage

```bash
# Evaluate a single workflow with carbon tracking
pisces-unified test_scripts/airflow/simple_etl.py \
  --track-carbon \
  --enable-energy-eval \
  --energy-mode zero_fill

# Evaluate with real sample input data and a yearly scenario estimate
pisces-unified test_scripts/airflow/simple_etl.py \
  --enable-energy-eval \
  --energy-mode sample \
  --energy-sample-path demo/input/weather_observations.csv \
  --energy-sample-map-file demo/input/sample_schema_map.json \
  --estimate-scenario-file demo/input/scenario_daily.json

# Compare two workflows across orchestrators
pisces-unified test_scripts/airflow/weather_pipeline.py \
  --track-carbon --enable-energy-eval --energy-mode zero_fill --out-dir ./reports

pisces-unified test_scripts/dagster/multi_asset_job.py \
  --track-carbon --enable-energy-eval --energy-mode zero_fill --out-dir ./reports

# Batch evaluate all Airflow scripts
for script in test_scripts/airflow/*.py; do
  pisces-unified "$script" --energy-mode heuristic --stdout | \
    python3 -c "import sys,json; d=json.load(sys.stdin); print(f'{d[\"workflow\"][\"name\"]}: {d[\"energy\"][\"heuristic\"][\"rules\"][\"rule_count\"]} rules')"
done
```

## Expected Eco-Design Profiles

### Airflow

| Script | Expected Rules |
|--------|---------------|
| `simple_etl.py` | No rules (clean baseline) |
| `weather_pipeline.py` | `LARGE_FAN_OUT`, `HIGH_XCOM_INTENSITY` |
| `sensor_heavy.py` | `SENSOR_POKE_MODE` (major), `HIGH_FREQUENCY_HEAVY_COMPUTE`, `DEEP_SEQUENTIAL_CHAIN`, `HIGH_XCOM_INTENSITY` |
| `batch_processing.py` | `HIGH_FREQUENCY_HEAVY_COMPUTE` (major), `LARGE_FAN_OUT`, `HIGH_XCOM_INTENSITY` |

### Dagster

| Script | Expected Rules |
|--------|---------------|
| `simple_asset_pipeline.py` | No rules (clean baseline) |
| `multi_asset_job.py` | `LARGE_FAN_OUT` (multiple downstream assets) |

### Prefect

| Script | Expected Rules |
|--------|---------------|
| `simple_flow.py` | No rules (clean baseline) |
| `parameterized_etl.py` | `LARGE_FAN_OUT` (concurrent city processing) |

### Argo Workflows

| Script | Expected Rules |
|--------|---------------|
| `simple_etl.yaml` | No rules (sequential, no schedule) |
| `multi_city_dag.yaml` | `LARGE_FAN_OUT` (6 parallel cities), `HIGH_FREQUENCY_HEAVY_COMPUTE` (hourly + 6x heavy containers) |

### Kestra

| Script | Expected Rules |
|--------|---------------|
| `simple_etl.yaml` | No rules (sequential, daily schedule) |
| `iot_analytics.yaml` | `LARGE_FAN_OUT` (4 device groups), `HIGH_FREQUENCY_HEAVY_COMPUTE` (every 15 min + Spark/Python tasks) |
