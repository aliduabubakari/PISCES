# Five-pipeline qualification suite

This suite tests whether the same PISCES contract, output oracle, and runner
can distinguish correct and incorrect work across five pipeline families.
It generates deterministic synthetic CSV fixtures, independent references,
Airflow v2 contracts, hash-bound artifacts, and four cases per pipeline.
The previous Climate Fusion commands and historical reports are unchanged.

## Pipelines and cases

| Pipeline ID | Workload | Equivalent implementation |
| --- | --- | --- |
| `climate_fusion` | Normalize temperatures from five sources, including Fahrenheit | Single input read instead of two; reverse intermediate order |
| `event_aggregation` | Reject invalid events; group counts and integer totals | Sorted grouping instead of hash aggregation |
| `order_enrichment` | Join orders to customers; apply discounts in cents; reject unknown customers | Binary-search lookup instead of a hash join |
| `sensor_windows` | Filter bad readings; aggregate each sensor into 60-second windows | Sorted grouping instead of hash aggregation |
| `inventory_latest` | Select the greatest version per item; preserve tombstones | Sorted grouping instead of a latest-version dictionary |

Every artifact has three dependent tasks: `normalize`, `transform`, and
`write_outputs`. They communicate through local intermediate JSON files.
Each declares both `result.csv` and a `quality.csv` accepted/rejected-count
summary. Every output is strictly sorted by its typed primary key and is
verified against its own immutable reference. No workflow can read the
reference through its declared input bindings.

| Case | Expected semantic verdict | Qualification condition |
| --- | --- | --- |
| `valid` | Passed | Both outputs exactly match the references |
| `equivalent` | Passed | Both outputs match; fingerprints agree with `valid` |
| `same_shape_wrong` | Failed | Same schema, keys and row counts; one value is wrong, while quality output passes |
| `partial` | Failed | Result is correct but the required quality output is missing |

The suite succeeds only when all cases behave as expected. An execution
crash, timeout, failed task-coverage check, or unexpected rejection is not a
successful negative test. Incorrect and incomplete cases retain attempt
telemetry, but are not credited as valid useful work.

## Quick start without Airflow or Docker

Run from the repository root with PISCES installed in your Python environment.
The module entry point works with the existing editable installation. To
refresh the optional console entry point, run `python -m pip install -e .`;
then `pisces-benchmarks` is equivalent to `python -m pisces.benchmarks`.

```bash
python -m pisces.benchmarks list
ROOT=$(mktemp -d "$PWD/diagnostics/benchmark-suite-XXXXXX")
python -m pisces.benchmarks build "$ROOT/bundles" --rows 1000 --keys 32
python -m pisces.benchmarks run "$ROOT/bundles" "$ROOT/standalone"
```

This executes all 20 approved task-body artifacts on the host in fresh
subprocesses and workspaces. It verifies semantics, not Airflow orchestration
or workflow performance. Do not use this backend on untrusted or externally
modified code. Hashes provide integrity within the frozen bundle, not a
signature or a trust boundary. It does not require providers or credentials.

## Actual Airflow execution and workload telemetry

Build the existing pinned Airflow OCI profile, resolve its immutable ID, then
run exactly the same bundles with the OCI backend:

```bash
docker build -f containers/airflow/Dockerfile -t pisces-airflow-oci:benchmark-suite .
IMAGE=$(docker image inspect --format '{{.Id}}' pisces-airflow-oci:benchmark-suite)
python -m pisces.benchmarks run "$ROOT/bundles" "$ROOT/oci" \
  --backend oci --oci-image "$IMAGE"
```

This runs 20 fresh containers sequentially. Network is disabled; fixtures
are read-only. Readiness is checked before execution. Each run verifies
workflow task coverage, fixture consumption, provenance, both outputs, and
the OCI profile. Telemetry comes from the existing cgroup collector, with
source, scope, missingness and quality reasons in the saved runtime payload.
See [OCI runtime](OCI_RUNTIME.md) for exact boundaries and conformance checks.
For experimental Prefect and Dagster profiles, generate the suite with
`--target prefect` or `--target dagster` and use its matching image.
See [engine qualification](ENGINE_QUALIFICATION.md) for the limits and CLI.

To select families or collect repeated attempts, use a new result directory:

```bash
python -m pisces.benchmarks run "$ROOT/bundles" "$ROOT/aggregation-oci" \
  --backend oci --oci-image "$IMAGE" --pipeline event_aggregation \
  --pipeline sensor_windows --repetitions 3 --timeout-s 90 --seed 1729
```

## Input size and cardinality

`--rows` means primary input records per pipeline, from 10 to 1,000,000.
For Climate Fusion it is the total across all five sources, not rows per
source. Orders additionally has `--keys` customer lookup records. `--keys`
controls event groups, customer count, sensor count and item count, from
1 to `min(rows, 10000)`. Output row counts differ by pipeline and are never
assumed to equal input row count. Climate Fusion does not use `--keys`.

Build a separate suite for each load or cardinality:

```bash
python -m pisces.benchmarks build "$ROOT/bundles-100k" --rows 100000 --keys 1000
python -m pisces.benchmarks run "$ROOT/bundles-100k" "$ROOT/oci-100k" \
  --backend oci --oci-image "$IMAGE" --repetitions 3
```

Generation supports larger fixtures; this does not guarantee execution under
the fixed 2 GiB memory, 256 MiB scratch, timeout, and 32 MiB-per-output oracle
limits. These new pipelines intentionally materialize intermediate records;
they are not the streaming Climate Fusion scaling profile. Resource-limit
failures must be retained, not treated as oracle successes. A primary row
count alone is not a universal functional unit or a reason to pool results
from different pipeline families.

## Saved evidence and exit codes

- `bundles/suite.json` inventories every artifact, binding, input and reference
  by SHA-256. All declared workload files must be covered and unchanged.
- `manifest.json` in the result directory records the frozen suite hash,
  loaded workload provenance, backend, environment, seed, schedule and bounds
  before attempts begin.
- `<pipeline>-<case>-<repetition>.json` retains each result and exclusion.
  Standalone controller elapsed time includes launch and verification and is
  not labeled as task time. OCI reports retain the actual runtime payload.
- `summary.json` separates semantic behavior from telemetry coverage and
  reports the positive cases' exact equivalence. CPU, wall and peak-memory
  summaries include intentionally invalid attempts, per case, as diagnostic
  observations. There are no speedup rankings or carbon estimates.
- A successful full check exits 0. Unexpected outcomes or incomplete coverage
  exit nonzero. Generation, result directories and JSON reports never
  overwrite an existing campaign. An interrupted run leaves attempts on disk;
  without its final summary it is not a completed qualification run.

Ordinary tests run all 20 standalone cases. The opt-in integration command
also exercises all 20 real Airflow artifacts:

```bash
PISCES_SUITE_OCI_TEST_IMAGE="$IMAGE" python -m pytest -q tests/test_benchmark_suite_oci.py
```

This is a small synthetic qualification set, not proof that arbitrary pipelines,
data types, distributed systems, or production deployments are supported.

## Verified smoke run

At 1,000 primary records and 32 keys, all 20 standalone cases and all 20
Airflow OCI cases matched their expected behavior on Docker Desktop. In the
OCI run, every case completed all three tasks; exactly the ten positive
cases verified both outputs. CPU telemetry was collected in all 20 attempts.
This checks semantic discrimination and the existing execution path at that
load. It does not qualify larger loads, native Linux, or cross-orchestrator
performance comparisons.
