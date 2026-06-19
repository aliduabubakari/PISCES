# PISCES Release Demo

This bundle exercises the release-quality path for PISCES:

- readiness diagnostics
- sample-data runtime measurement
- schema mapping
- calibrated yearly estimates
- static YAML evaluation
- report comparison

Run from the repository root after installing the project:

```bash
python -m pisces.diagnostics test_scripts/airflow/simple_etl.py \
  --carbon-country-iso NOR \
  --out demo/output/readiness.json

python -m pisces.unified_evaluator test_scripts/airflow/simple_etl.py \
  --enable-energy-eval \
  --energy-mode sample \
  --energy-sample-path demo/input/weather_observations.csv \
  --energy-sample-map-file demo/input/sample_schema_map.json \
  --estimate-scenario-file demo/input/scenario_daily.json \
  --out-dir demo/output

python -m pisces.unified_evaluator test_scripts/airflow/simple_etl.py \
  --enable-energy-eval \
  --energy-mode fuzz_fill \
  --estimate-scenario-file demo/input/scenario_daily.json \
  --out-dir demo/output

python -m pisces.unified_evaluator test_scripts/argo/simple_etl.yaml \
  --orchestrator argo \
  --out-dir demo/output

python -m pisces.dag_comparator demo/output/*.json \
  --out demo/output/comparison.json
```

The reports intentionally keep raw input rows out of JSON outputs. Sample provenance includes
file hashes, row counts, column-name hashes, and aggregate schema information.
