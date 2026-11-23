# Prefect and Dagster controlled OCI profiles

PISCES uses the same container isolation, workload contracts, output oracle,
and software counters for three different engine interfaces. This is an
experimental local test profile for approved artifacts, not production or
distributed-engine equivalence.

| Target | Pinned engine | Execution evidence | Local control plane |
| --- | --- | --- | --- |
| Airflow | 3.4.0; standard provider 1.21.0 | `DAG.test` task instances | Fresh SQLite database |
| Prefect | 3.6.18 | Actual flow call plus task states and attempt counts read from its API | Temporary test server and SQLite |
| Dagster | 1.12.8 | `execute_in_process` plus terminal step events | Ephemeral instance |

The new images also pin SQLAlchemy 2.0.55; Prefect pins FastAPI 0.128.0.
The image ID freezes the actual installed dependency set for a campaign.
Other transitive dependencies are not fully locked for future rebuilds:
rebuilding can create a new profile and requires requalification. The probe
checks declared versions, resource limits and isolation before admitting a run.
It does not start the Prefect API or execute a job; readiness is not an engine
execution verdict. Use the four-case smoke check before a larger campaign.

## Build and run separately

Use the existing Python environment from the repository root. Each target
gets its own immutable image, generated bindings and result directory. For
example, the following runs all five pipeline families and four cases for each
target. Start with 100 or 1,000 input rows and one repetition.

```bash
ROOT=$(mktemp -d "$PWD/diagnostics/engine-suite-XXXXXX")
for TARGET in prefect dagster; do
  docker build -f "containers/$TARGET/Dockerfile" -t "pisces-$TARGET-oci:qualification" .
  IMAGE=$(docker image inspect --format '{{.Id}}' "pisces-$TARGET-oci:qualification")
  docker image tag "$IMAGE" "pisces-$TARGET-oci:$(basename "$ROOT")"
  python -m pisces.diagnostics --orchestrator "$TARGET" --oci-image "$IMAGE"
  python -m pisces.benchmarks build "$ROOT/$TARGET" --target "$TARGET" --rows 1000 --keys 32
  python -m pisces.benchmarks run "$ROOT/$TARGET" "$ROOT/$TARGET-results" \
    --backend oci --oci-image "$IMAGE" --timeout-s 120
done
```

`pisces-benchmarks` is an alternative to `python -m pisces.benchmarks`.
Use `--pipeline event_aggregation` for a four-case smoke test or
`--repetitions 3` for repeated attempts. Never reuse result directories.
Changing the suite target changes engine bindings and artifact hashes;
input fixtures and reference contracts remain identical across targets.
The CLI rejects a suite/image target mismatch before execution.
The extra campaign tag retains the local image while development tags are
rebuilt. Do not prune campaign images during a run. An immutable ID identifies
content but cannot make a deleted local image available; PISCES fails closed
rather than substituting a newer image.

For an individual artifact, use the same unified entry point with the
appropriate `--orchestrator`, `--workload-contract`, and `--artifact-binding`:

```bash
pisces-unified "$ROOT/prefect/event_aggregation/valid.py" \
  --orchestrator prefect --enable-energy-eval --energy-execution-adapter oci \
  --oci-image "$PREFECT_IMAGE" --energy-maximum-repetitions 1 --energy-timeout-s 120 \
  --workload-contract "$ROOT/prefect/event_aggregation/contract.json" \
  --artifact-binding "$ROOT/prefect/event_aggregation/valid.binding.json" \
  --out-file "$ROOT/prefect-unified.json"
```

Set `PREFECT_IMAGE` to the immutable ID of the Prefect image before using
this individual example. The orchestration target must match the binding
and image; the worker checks the match again.

## Profile limits and evidence

- Prefect selects a real `Flow` object and invokes its engine, never its
  underlying `.fn` as a substitute. Synchronous tasks need unique names,
  no retries and one observed attempt. The test harness creates a local API
  process inside the container. Task events are drained and the API is polled
  for at most five seconds for final states; missing or inconsistent evidence
  cannot become a successful task. API startup has a 60-second allowance,
  subject to the outer container deadline. Retain startup failures.
- Dagster selects a real `JobDefinition`, executes a flat in-process op job,
  and reads `STEP_SUCCESS`, `STEP_FAILURE` and `STEP_SKIPPED` events. Job
  success alone is insufficient. Declared retries and retry events are
  rejected. General asset graphs, partitions, dynamic execution and remote
  services are not qualified by this profile.
- Both require an unambiguous entry point or an explicitly selected
  `pisces_entrypoint` of the correct engine type. Task/step budgets and exact
  contract coverage are checked independently of output correctness. These
  profiles support declared local file fixtures, not service fixture mocking.
- `engine_evidence` preserves the named engine interface, version, final run
  state, and per-task/step state. The ordinary runtime, task coverage, input
  consumption, output assertions and per-metric quality fields stay separate.
  Missing output, engine failure and missing telemetry are different findings.

Network isolation is enforced by Docker's `--network=none`; loopback remains
available for the Prefect API. The images run without host credentials,
capabilities or a Docker socket, with a read-only root and input mount, two
CPU cores of quota, 2 GiB memory without swap, 128 PIDs and bounded scratch.
Prefect's Python convenience guards allow engine networking and subprocesses
inside that container; they are not a task sandbox. Do not expose this worker
as an arbitrary-code, multi-tenant service.

Legacy host/native Prefect function probes and job-level Dagster probes are
retained for compatibility but explicitly unqualified and ineligible for
workflow runtime credit. A host environment variable cannot unlock the new
profiles; the worker also checks the private cgroup, non-root UID and read-only
root. There is no fallback from an OCI admission failure to a legacy adapter.

## Measurement boundaries

The worker cgroup window includes engine setup, task execution, output
verification and descendants. Prefect API startup, event draining and shutdown
are part of that observed work. Its in-process execution block includes control
plane setup; Airflow database initialization is outside its narrower execution
block. These narrower blocks are not interchangeable. `engine_evidence` states
that setup is included for these two new profiles.

Per-step engine timestamps are not task-body CPU measurements. The existing
single-task Climate Fusion timing pilot remains separate; this extension does
not fabricate per-task CPU, distributed resource accounting or carbon estimates.
Do not rank orchestrators from raw totals collected under different interfaces.

## Qualification tests

Ordinary tests check target policy, matching reference bytes across targets,
entry point selection and the standalone bodies without optional engines.
The opt-in tests execute the real engines:

```bash
PISCES_PREFECT_TEST_IMAGE="$PREFECT_IMAGE" \
PISCES_DAGSTER_TEST_IMAGE="$DAGSTER_IMAGE" \
  python -m pytest -q tests/test_benchmark_suite_oci.py tests/test_engine_failure_integration.py
```

Set each image variable to its immutable ID. Cases include all twenty pipeline
variants per target, a deliberately failed transform, ambiguous entry points,
retry rejection and container deadline cleanup. Run software counter checks
with each image separately, outside benchmark runs:

```bash
python -m pisces.energy.telemetry_check --oci-image "$PREFECT_IMAGE" \
  --repetitions 5 --out "$ROOT/prefect-counter-calibration.json"
```

Calibration is not confirmation. Predeclare tolerances and confirm on fresh
attempts as described in the [OCI protocol](OCI_RUNTIME.md#wall-and-cpu-conformance).
Keep Docker Desktop and native Linux evidence separate.

## Recorded Docker Desktop qualification

At 100 primary input rows and eight keys, each image completed all twenty
cases across the five families. All three engine tasks/steps were observed
in every case. Exactly ten positive cases per engine verified both outputs;
the ten negative cases were rejected for their intended output defect.
All forty attempts retained CPU telemetry. Separate tests passed for a failed
transform, declared retries, ambiguous entry points and container deadlines
on each engine (eight tests in total).

| Profile | Semantic cases matched | Counter checks passed | Max wall disagreement | Max CPU disagreement |
| --- | ---: | ---: | ---: | ---: |
| Prefect | 20/20 | 25/25 | 0.059 ms | 0.991 ms |
| Dagster | 20/20 | 25/25 | 0.089 ms | 0.984 ms |

Counter confirmation used the existing Docker Desktop limits of 1 ms wall
and 3 ms CPU disagreement. This is software reference agreement for the
controlled fixtures, not physical energy accuracy or a certification of
every workload duration. Both unified CLI reports passed report validation.

The frozen image IDs were:

- Prefect: `sha256:d1fd08fe570fc32e9c600cb0f0e877dbe41205709c66102661215f38a009f503`.
- Dagster: `sha256:d51a4c5810de09172d1e7048d9d665aa6c854dd3a32b5ee747c34392635ce8b3`.

The local evidence is under `diagnostics/multi-engine-4w4hOm/`, with the
passing suites in `prefect-frozen-final/` and `dagster-full/`. These ignored
diagnostics are not part of the installable package. Earlier Prefect runs
remain failed evidence: dependency incompatibility, API startup deadlines,
unsettled task records and replacement of a local image during development
were not counted as passes. Corrected runs used new result directories.
Native Linux repeatability, larger data loads, deployed services, remote
workers and cross-engine resource rankings remain outside this qualification.

## Sources

- [Prefect 3.6.18 test harness source](https://github.com/PrefectHQ/prefect/blob/3.6.18/src/prefect/testing/utilities.py)
  documents the local SQLite/API harness and event-worker draining.
- [Dagster execution API](https://docs.dagster.io/api/dagster/execution)
  documents in-process execution; the qualified version is pinned above.
