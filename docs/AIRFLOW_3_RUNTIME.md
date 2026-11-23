# Airflow 3 runtime profile

Use Airflow **3.4.0** for new PISCES experiments. The
[Apache Airflow support table](https://airflow.apache.org/docs/apache-airflow/stable/installation/supported-versions.html)
lists the entire 2.x line, including 2.11.2, as EOL since April 22, 2026.
The 2.8 adapter path and the [frozen protocol](CONTROLLED_EXPERIMENT_PROTOCOL.md)
are retained for historical reproduction, not new studies. No database or
artifact from that study needs to be migrated.

## Reproduce the local profile

Create a separate environment using Python 3.12:

```bash
python3.12 -m venv .venv/airflow-3.4.0
.venv/airflow-3.4.0/bin/python -m pip install \
  "apache-airflow==3.4.0" "apache-airflow-providers-standard==1.21.0" -e . \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.4.0/constraints-3.12.txt
```

Point `--energy-runtime-python` at that interpreter. Run the opt-in tests
from the regular PISCES development environment:

```bash
PISCES_AIRFLOW3_PYTHON=.venv/airflow-3.4.0/bin/python \
  .venv/bin/python -m pytest -q tests/test_airflow_3_integration.py
```

The [climate-fusion bundle](../benchmarks/climate_fusion/README.md) uses
`airflow.sdk.DAG` and the standard provider's `PythonOperator`. The native
runner initializes a fresh SQLite database via `airflow db migrate` outside
the timed block. It stages only the selected DAG in the local bundle,
ignores inherited Airflow deployment configuration, and calls
`DAG.test(logical_date=..., use_executor=False)`. This follows the documented
[Airflow 3.4 DAG test requirements](https://airflow.apache.org/docs/apache-airflow/3.4.0/core-concepts/debug.html).
Task bodies execute for real; no production scheduler or executor is tested.

## Evidence limits

- The reference profile covers self-contained Python DAGs with local fixtures
  and zero declared retries. Airflow 3 reparses the staged file; mutating
  retries on the first imported object cannot enforce a single attempt in
  the reparsed DAG. The adapter rejects nonzero retries instead.
- Existing Airflow 2 scripts may need SDK/provider imports or other migration
  changes. They remain unchanged as historical/static fixtures. Additional
  providers, deferrable tasks, and production executors are not qualified
  by this benchmark.
- The worker is for trusted artifacts. The Python guards and temporary
  bundle are not OS isolation.
- Native output verification does not establish pairwise equivalence or
  eligibility for runtime or carbon comparison. Keep Airflow version,
  Task SDK, and provider versions in the evidence record. Do not pool 2.8 and
  3.4 telemetry as though the measurement environment were unchanged.
