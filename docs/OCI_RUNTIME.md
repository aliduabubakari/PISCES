# Linux OCI execution

PISCES can run one contract-bound workload in a local Docker Linux engine
with cgroup v2. Airflow 3.4.0 is the reference profile; experimental
[Prefect and Dagster profiles](ENGINE_QUALIFICATION.md) reuse the same isolation
and collector with their own engine-state qualification. This path is for approved,
self-contained workflows, not arbitrary public submissions. It never
falls back to running the artifact on the host.

## Build and probe

From the project root, build and resolve the immutable local image ID.
Image pulls and builds are separate from evaluation.

```bash
docker build -f containers/airflow/Dockerfile -t pisces-airflow-oci:3.4.0 .
IMAGE="$(docker image inspect --format '{{.Id}}' pisces-airflow-oci:3.4.0)"
python -m pisces.diagnostics --oci-image "$IMAGE"
```

The probe checks the local Unix Docker endpoint, Linux cgroup v2, the
pinned engine profile, enforced quotas, private cgroup, disabled
network, read-only root, and available counters. It does not import a
workflow or start CodeCarbon. A repository digest also works if present
locally. Mutable tags are rejected for execution.

## Run a contract

```bash
python -m benchmarks.climate_fusion.benchmark /tmp/pisces-climate-oci --rows-per-source 10000
pisces-unified /tmp/pisces-climate-oci/baseline.py \
  --orchestrator airflow --enable-energy-eval --energy-execution-adapter oci \
  --oci-image "$IMAGE" --energy-maximum-repetitions 1 \
  --workload-contract /tmp/pisces-climate-oci/contract.json \
  --artifact-binding /tmp/pisces-climate-oci/baseline.binding.json \
  --out-file /tmp/pisces-climate-oci/oci-baseline.json
```

The fixed profile uses two CPU cores of quota, 2 GiB of memory without swap,
128 PIDs, and 256 MiB of temporary storage. The worker runs as a non-root
user with no capabilities, no network, and no host credential, home, or
Docker socket mounts. Only hash-verified workload inputs are staged and
mounted read-only. Import, Airflow setup, task execution, and output
verification all run inside the container. Registered fixture mocks can
serve local substitutes without enabling external networking.

## What the counters mean

- `container_telemetry.cpu_s` is the cgroup CPU delta, including descendant
  processes, database initialization, DAG execution, and output validation.
  `wall_clock_s` brackets the operation, excluding counter reads. The CPU
  reads immediately surround that wall interval; `counter_window` retains
  their timestamp bounds and raw microseconds. Container creation, host input
  staging, start-to-exit time, and removal are reported separately under
  `container_lifecycle`. The start-to-exit window overlaps the worker window.
- `memory_peak_bytes` is the cgroup lifetime high-water mark, including
  page cache. It is not process RSS and is not additive.
- Block read/write byte deltas require readable endpoints, the same device
  set, and the corresponding field at both endpoints. PISCES does not
  infer a zero baseline when a device first appears. Missing, empty, or
  invalid counters stay null. These are not logical CSV bytes or network
  traffic. See the [Linux cgroup v2 documentation](https://docs.kernel.org/admin-guide/cgroup-v2.html).
- Final counters are read before normal container exit. A hard timeout or
  OOM can prevent the final snapshot; the report states why it is missing.
  The controller attempts removal even when execution or inspection fails.

`container_telemetry.metric_quality` records `available`, `unavailable`, or
`invalid` for each reading, with endpoint-specific reasons when applicable.
Here, `available` means a reading could be computed, not that it passed
conformance. `telemetry.container` summarizes the core wall, CPU and peak
memory fields; consult per-metric status for I/O and auxiliary counters.
Missing CPU does not invalidate verified output. Inspection failure after
a worker report yields `container_state_unverified` and retains the report
as attempt evidence. Cleanup errors are reported separately under
`container_lifecycle` and exclude the attempt from repeatability summaries.

Docker Desktop runs in a VM; its results are labeled accordingly. Do not
pool them with a Linux reference worker or the native process pilot.
The generic comparator still refuses resource rankings, and this profile
does not produce energy or carbon estimates.

## Peak memory and bounded OOM

```bash
python -m pisces.energy.memory_check --oci-image "$IMAGE" --repetitions 3 \
  --out /tmp/pisces-memory-check.json
```

Fresh containers charge 32, 96, and 256 MiB of anonymous memory by touching
every page. The check compares current usage before, while held, and after
release, and checks the production lifetime peak against those readings
with a fixed 16 MiB allowance. This checks charging and retention of the
high-water mark, not memory attribution to a specific Airflow task.
The separate OOM fixture uses a 128 MiB container with no swap, a child
that attempts no more than 160 MiB, and a timeout. It requires both a
kernel OOM-kill event and termination of the allocating child. None of
these fixtures runs a DAG, mounts inputs, or validates block I/O.

## Wall and CPU conformance

After rebuilding the image, run the collector on empty, sleep (50 and 250
ms), parent CPU, and child CPU fixtures. Every attempt uses a fresh container
with the normal isolation profile and no input mount. It does not import or
execute a DAG.

```bash
python -m pisces.energy.telemetry_check --oci-image "$IMAGE" --repetitions 5 \
  --out /tmp/pisces-telemetry-calibration.json
```

Calibration reports absolute error against a separately instrumented
monotonic clock and the combined self and reaped-child CPU from `getrusage`.
It also reports wall time spent outside the operation within the collector
call. It does not choose a tolerance or issue a confirmation verdict.
Choose both absolute tolerances from calibration and your intended time
resolution, document them, then pass both as `--wall-tolerance-s` and
`--cpu-tolerance-s` in a separate run. Keep the same image and enforcement
profile. A confirmation passes only when all attempts have valid readings
and agree within the predeclared bounds. The two wall reads use the same
kernel clock family on Linux; the CPU interfaces are both OS accounting,
not physical measurements. Results are specific to the tested profile
and durations. This command does not validate peak memory, block I/O, or
workflow repeatability, and does not set resource-comparison eligibility.

### Docker Desktop pilot

The initial calibration collected 25/25 attempts. A separate confirmation
collected and passed 50/50 attempts against limits set after calibration
and before confirmation: 1 ms wall error and 3 ms CPU error. The largest
confirmation disagreements were 0.172 ms wall and 0.983 ms CPU; median
collector overhead outside the empty operation was 0.141 ms. These are
pilot results, not a minimum supported duration or a transferable tolerance.

## Opt-in integration tests

```bash
PISCES_OCI_TEST_IMAGE="$IMAGE" python -m pytest -q tests/test_oci_integration.py
```

The tests cover the valid and wrong-unit outputs, read-only fixtures,
network and credential isolation, descendant CPU accounting, and
timeout cleanup. Docker is not required by the ordinary unit suite.
