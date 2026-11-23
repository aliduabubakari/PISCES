# Native Linux telemetry confirmation

Use a dedicated, approved bare-metal Linux worker with a local Docker
engine, cgroup v2, and the same OCI isolation profile as the Docker Desktop
pilot. Run the PISCES controller on that worker. `local_linux_engine` only
describes the reported Docker profile; it does not prove the host is bare
metal. Confirm the worker type independently and record it before the run.
Do not pool virtualized or shared-runner results with this reference campaign.

## 1. Capture the profile and calibrate

From the repository root, use the same source revision and a Python
environment with PISCES installed. Record the revision and any worktree diff,
worker identifier, host OS/kernel, CPU model and architecture, Docker
version, and other workloads allowed during the campaign. Save the engine
readiness report and immutable image ID; use that exact image for every step.
The Dockerfile pins the base image and Airflow profile; the recorded built
image ID identifies the actual installed dependency set used for this run.

```bash
ROOT=$(mktemp -d "$PWD/linux-telemetry-XXXXXX")
docker build -f containers/airflow/Dockerfile -t pisces-airflow-oci:linux-reference .
IMAGE=$(docker image inspect --format '{{.Id}}' pisces-airflow-oci:linux-reference)
python -m pisces.diagnostics --oci-image "$IMAGE" > "$ROOT/readiness.json"
python -m pisces.energy.telemetry_check --oci-image "$IMAGE" --repetitions 10 \
  --out "$ROOT/calibration.json"
```

Check readiness and require calibration to return `calibration_only` with
all fixture readings. Before opening the confirmation run, record a protocol
with the frozen image and environment, absolute error limits in seconds,
planned repetitions, and shortest wall/CPU windows the experiment needs.
Choose limits based on calibration **and** required resolution. For example,
for a 5% error budget at a 50 ms wall window the limit cannot exceed 2.5 ms;
a 250 ms CPU fixture cannot certify a 50 ms CPU workload. If calibration is
too noisy for the intended resolution, report that limitation or add fixtures
at the required durations before confirming. Do not recycle the Docker
Desktop limits as acceptance criteria.

## 2. Confirm using fresh attempts

Set `WALL_LIMIT_S` and `CPU_LIMIT_S` to the frozen limits and run the
predeclared repetitions. Keep all failed reports. A rerun after changing the
profile or limits is a new campaign, not a replacement for a failed result.

```bash
python -m pisces.energy.telemetry_check --oci-image "$IMAGE" --repetitions 20 \
  --wall-tolerance-s "${WALL_LIMIT_S:?set the frozen wall limit}" \
  --cpu-tolerance-s "${CPU_LIMIT_S:?set the frozen CPU limit}" \
  --out "$ROOT/confirmation.json"
```

## 3. Measure same-baseline repeatability

Use the [Climate Fusion procedure](../benchmarks/climate_fusion/README.md#oci-same-baseline-repeatability)
with `--environment local_linux_engine`. Predeclare session start times,
rounds per session, tolerated background load, and the workload size. Run
separate sequential sessions at those times, from the same bundle and
confirmation report. Prefer sessions on different days for a claim about
day-to-day variation. Retain the whole campaign, including manifest, attempts,
confirmation, and bundle. Report coverage and variation in each session and
the spread of session medians. This is a software measurement check, not a
carbon estimate or a pipeline performance comparison.

Use the same image for the [peak-memory and bounded OOM checks](OCI_RUNTIME.md#peak-memory-and-bounded-oom)
and the [Climate Fusion load sweep](../benchmarks/climate_fusion/README.md#oci-load-sweep).
Keep their reports separate: a controlled page allocation checks the
memory signal, while changing row counts shows how this workload and its
verifier behave under load. Declare repetition count and schedule seed
before running each sweep. Use `--environment local_linux_engine`, save each
session to a new directory, then use the benchmark's `scaling_sessions`
command to compare scopes and readouts without pooling sessions. Record the
worker identifier and background-load policy alongside the comparison; matching
software profiles alone do not establish that runs used the same physical host.
