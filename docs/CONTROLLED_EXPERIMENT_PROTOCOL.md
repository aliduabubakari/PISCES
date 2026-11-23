# Controlled experiment protocol

This protocol defines the evidence boundary for confirmatory PISCES studies on
macOS. It does not retroactively change historical reports.

For unified report schema `1.4.0`, the current runtime contract checks still
screen the final repetition only. Passing them marks an artifact as a candidate
for a separate equivalence check; it does not qualify the block for resource
comparison or provide verified units. `telemetry.use` identifies screening
observations versus attempt diagnostics. This change does not rerun or alter
the frozen study.

## Measurement hierarchy

The primary operational outcomes are:

1. wall-clock time for the bounded execution block;
2. process CPU time for the same block;
3. peak resident memory of the dedicated runner process.

CodeCarbon output is secondary modeled-energy evidence on the current Mac. Its
values must not be described as direct hardware energy measurements.

## Execution outcome before resource use

Each assigned artifact receives one execution outcome:

- `completed`: all expected native work completed;
- `partial`: some native work completed and at least one expected task was skipped;
- `zero_work`: no native task completed;
- `failed`: the bounded execution raised an error;
- `surrogate`: PISCES ran its diagnostic benchmark instead of the workflow.
- `mixed_execution`: a measurement block used both native and surrogate execution.

Only complete native executions can contribute primary operational measurements.
Failed, partial, and zero-work attempts retain diagnostic counters under
`attempt_diagnostics`. Their analytical resource values are unavailable. They
must never be encoded as zero time, zero memory, or zero modeled energy.

Execution yield is reported over every assigned attempt. Resource comparisons
use the smaller population of eligible executions and state that denominator.

## Strict command requirements

A confirmatory run uses:

- `--experiment-mode`;
- an explicit `--energy-mode`;
- `--energy-execution-adapter native`;
- an explicit `--energy-runtime-python` from the frozen target environment;
- a frozen input fixture;
- a minimum duration and repetition bound established by a pilot;
- `--carbon-scale-factor 1`;
- an exact immutable `--out-file`.
- a validated `--workload-contract` and `--artifact-binding`.

Strict mode rejects `auto` as either a data mode or adapter, hosted synthesis,
scaled carbon, and targets other than Airflow. The controller checks the
interpreter and contract before execution, and the worker enforces the same
mode and adapter policy. Native task inputs are bound by the workload contract.
The separate import-only probe is skipped; it is not a target acceptance gate.
Use an exact output path for reproducibility. The CLI does not enforce archival
immutability, so the experiment driver must avoid reusing result paths.

## Workload-equivalence requirement

The direct and OPOS-mediated artifacts in a pair must receive the same frozen
workload contract. The contract must identify:

- input files and their SHA-256 hashes;
- deterministic local mocks for external services;
- target-specific fixture wiring;
- expected tasks or components;
- expected output schema, row count, and content assertions;
- cleanup and resource limits.

PISCES validates contract, binding, artifact, and fixture provenance. The
Airflow adapter activates a temporary scoped workspace, deterministic HTTP and
FTP fixtures, contract-aware task coverage, required-input consumption checks,
and format-aware output assertions. Branch contracts may declare alternative
task groups and permitted branch skips. Exact string-literal path bindings are
applied to a recorded sandbox copy; the archived artifact remains unchanged
and hash-verified.

An artifact becomes an operational candidate after its native contract checks
pass. A matched pair becomes eligible for confirmatory resource comparison
only after a separate pairwise check establishes equivalent logical outputs,
formats, row counts, schemas, and deterministic content. Operational success
in both routes is insufficient when the routes perform different
transformations. Prefect and Dagster require equivalent target-specific
integration before confirmatory paired use.

## Frozen Airflow environment

This section describes the archived study. Use the
[Airflow 3 runtime profile](AIRFLOW_3_RUNTIME.md) for new work.

The Airflow study target is Apache Airflow 2.8.1 on Python 3.11.8. PISCES uses
Airflow's `DAG.test()` interface with a temporary SQLite metadata database and
the `SequentialExecutor`. Metadata initialization occurs before the operational
measurement window. Each measured run uses the contract's fixed logical date.
Additional repetitions receive a microsecond run-index suffix to preserve
Airflow metadata-key uniqueness without changing the calendar date.

The confirmatory outcome is first-pass execution. PISCES sets task retries to
zero for `DAG.test()` and records each artifact's declared retry count in the
native metadata. The measured block includes the DAG test and task bodies. It
excludes environment creation, metadata initialization, and output assertion
evaluation.

## Measurement schedule

Use at least two unmeasured warm-ups. Counterbalance the route order within each
pair using AB/BA or ABBA/BAAB. Keep input data, mocks, target version, interpreter,
and machine settings fixed. Record background-state checks and run timestamps.

The pilot determines the minimum measured block duration, repetition count, and
confirmatory sample size. Repetitions are averaged within an artifact; the
matched artifact pair remains the statistical unit.
