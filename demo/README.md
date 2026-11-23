# PISCES Release Demo

This bundle exercises the release-quality path for PISCES:

- readiness diagnostics
- sample-data and fuzz-fill surrogate diagnostics (no workflow tasks executed)
- schema mapping
- an explicit refusal to project annual workflow carbon from a surrogate
- static YAML evaluation
- report inventory and static rule ranking

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
  --out-file demo/output/sample.json

python -m pisces.unified_evaluator test_scripts/airflow/simple_etl.py \
  --enable-energy-eval \
  --energy-mode fuzz_fill \
  --estimate-scenario-file demo/input/scenario_daily.json \
  --out-file demo/output/fuzz_fill.json

python -m pisces.unified_evaluator test_scripts/argo/simple_etl.yaml \
  --orchestrator argo \
  --out-file demo/output/argo.json

python -m pisces.dag_comparator demo/output/sample.json demo/output/fuzz_fill.json demo/output/argo.json \
  --out demo/output/comparison.json
```

The reports intentionally keep raw input rows out of JSON outputs. Sample provenance includes
file hashes, row counts, column-name hashes, and aggregate schema information.
Both diagnostic reports record `surrogate_is_not_workflow_execution` for the
scenario request. The comparison reports `not_comparable` for workflow resource use.
