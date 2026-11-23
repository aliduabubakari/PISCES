# Climate fusion semantic benchmark

Build a fresh deterministic bundle and check the task outputs:

```bash
python -m benchmarks.climate_fusion.benchmark /tmp/pisces-climate-ci
python -m benchmarks.climate_fusion.benchmark /tmp/pisces-climate-large --rows-per-source 10000
```

The generator builds five source fixtures from a fixed temperature sequence.
It writes the expected Celsius records before encoding one source in
Fahrenheit. The baseline reads each fixture twice; the equivalent variant
reads each once and changes output order; the wrong variant skips the
conversion while preserving schema and row count. The verdicts must be
`passed`, `passed`, and `failed`, respectively. The bundle contains the
reference, inputs, contract, artifact bindings, manifest, and verdicts.
Use a new directory for each bundle.

The driver executes the real transformation bodies as standalone Python
processes. It does not measure runtime efficiency or simulate Airflow.
Each generated file also exposes an Airflow 3 SDK DAG. With the
[pinned Airflow 3.4.0 interpreter](../../docs/AIRFLOW_3_RUNTIME.md), the bundle
can be screened using:

```bash
pisces-unified /tmp/pisces-climate-ci/baseline.py --orchestrator airflow \
  --enable-energy-eval --energy-mode zero_fill --energy-execution-adapter native \
  --energy-runtime-python .venv/airflow-3.4.0/bin/python --experiment-mode \
  --energy-maximum-repetitions 1 \
  --workload-contract /tmp/pisces-climate-ci/contract.json \
  --artifact-binding /tmp/pisces-climate-ci/baseline.binding.json \
  --out-file /tmp/pisces-climate-ci/native-baseline.json
```

The actual native inputs come from the contract, not zero-fill rows.
Use only trusted artifacts and keep oracle files outside the candidate
workspace. This harness is not OS isolation.

## Paired resource pilot

Use the pinned interpreter and a fresh result directory. The paired driver
starts one native runner per attempt and checks the output from that attempt:

```bash
python -m benchmarks.climate_fusion.paired /tmp/pisces-climate-large /tmp/pisces-climate-paired \
  --runtime-python .venv/airflow-3.4.0/bin/python --pairs 6
```

The manifest records a seeded, counterbalanced order, wall-time metric,
minimum meaningful reduction (5%), technical interval half-width goal
(10 percentage points), and minimum eligible pair fraction (80%).
Wall time and process CPU are normalized by verified batches from eligible
pairs only. Peak RSS stays a per-process lifetime observation, not a total.
With fewer than five eligible pairs no interval or directional conclusion is
issued. The bootstrap describes technical variation for one artifact pair;
it does not generalize to other pipelines or deployment environments.

The cache policy is fresh runner and database, not cold OS page cache.
No warm-ups, power backend, or carbon projection run in this profile. The
driver requires an exact-output v2 contract; tolerance-based outputs still
need a direct comparison. Run with `--candidate wrong_units --pairs 2` in
a new result directory to check rejection. The generic report comparator
remains inventory-only; it does not merge arbitrary reports into a campaign.

Check measurement boundaries and empty-monitor overhead separately:

```bash
python -m benchmarks.climate_fusion.counter_check
```

## OCI same-baseline repeatability

After [wall/CPU confirmation](../../docs/OCI_RUNTIME.md#wall-and-cpu-conformance),
run the same baseline in fresh containers. Use the same image, generated
bundle, and passing confirmation report in each session. Set `ROOT` to a
new campaign directory and `CONFIRMATION` to that report. For a
[native Linux reference campaign](../../docs/NATIVE_LINUX_TELEMETRY.md),
use `--environment local_linux_engine` instead of `docker_desktop_vm`.

```bash
python -m benchmarks.climate_fusion.benchmark "$ROOT/bundle" --rows-per-source 10000
python -m benchmarks.climate_fusion.repeatability run "$ROOT/bundle" "$ROOT/session-1" \
  --oci-image "$IMAGE" --confirmation "$CONFIRMATION" --environment docker_desktop_vm --repetitions 5
```

Run the same command at the next scheduled session, using `session-2`
as the output directory. Then summarize without pooling observations:

```bash
python -m benchmarks.climate_fusion.repeatability summarize \
  "$ROOT/session-1" "$ROOT/session-2" --out "$ROOT/repeatability.json"
```

The session manifest is written before the attempts. Each attempt is
retained, including missing counters and rejected output. Wall and CPU
coverage use planned attempts as the denominator. Summary statistics include
the median, median absolute deviation (MAD), and range. Between-session
dispersion is shown only for complete sessions with matching profiles and
output fingerprints. Containers and databases are fresh; OS page cache is
uncontrolled. Results quantify technical variation, not counter accuracy,
speedup, carbon savings, or generalizability to other hosts.

### Docker Desktop pilot

Two same-day sessions, five fresh containers per session, ran the same
50,000-row output workload with a 30-second pause between sessions. All ten
outputs matched the same semantic fingerprint and both counters were usable
in all ten runs. Values below are for the whole worker window.

| Signal | Session 1 median (MAD) | Session 2 median (MAD) | Span of medians |
| --- | ---: | ---: | ---: |
| Wall time | 6.549 s (0.177 s) | 6.763 s (0.075 s) | 3.22% |
| CPU time | 6.544 CPU-s (0.178 CPU-s) | 6.747 CPU-s (0.083 CPU-s) | 3.06% |

The percentage is the max-minus-min session median, divided by the median
of session medians. The session shifts occurred without changing the
pipeline. This short pilot does not estimate day-to-day drift or define a
minimum meaningful effect for future comparisons.

## OCI load sweep

Run one unchanged baseline at **10,000, 100,000, 500,000, and 1,000,000
total rows**, evenly split across the five source fixtures. Each round
runs one fresh container per size in a seeded shuffled order. The expected
output is generated independently and verified row by row, including
the row count and output fingerprint. This profile uses a sorted-key
contract; it does not replace the order-insensitive semantic benchmark.

```bash
python -m benchmarks.climate_fusion.scaling "$ROOT/scaling" \
  --oci-image "$IMAGE" --confirmation "$CONFIRMATION" \
  --environment docker_desktop_vm --repetitions 3
```

The four default loads can be overridden with
`--rows 10000 100000 500000 1000000`. Choose 3-8 distinct total counts, multiples
of five between 5 and 1,000,000. Session comparison uses the declared workloads
instead of assuming the default four sizes; different load plans cannot be pooled.

Export a saved sweep for the read-only frontend:

```bash
pisces campaign-view "$ROOT/scaling" --pipeline climate_fusion \
  --out "$ROOT/scaling-view.json"
```

See [multiple-size charts and import](../../docs/FRONTEND_WORKSPACE.md#multiple-data-sizes).
Viewing a campaign does not run it or upgrade its descriptive evidence.

Use a passing wall/CPU confirmation report for the same image and engine.
The report distinguishes the `fuse()` task body, the bounded Airflow
execution block, output verification, and the whole container worker.
The task wall interval includes fixture reads, transformation, and output
close. It excludes Airflow dispatch, timing-record writes, and the output
verifier. Task CPU is current-thread CPU, not process or cgroup CPU; it
does not cover child processes or background threads. This is an opt-in
instrumentation record from a trusted, hash-bound benchmark, not independent
evidence from arbitrary submitted code. The runner bounds the record by
its own execution timestamps and treats missing or invalid records as
missing readings without changing output verification.

The report retains per-attempt
evidence, load-level medians and MAD, the time slope per 100,000 rows,
intercept, residuals, and R-squared. Time fits require complete data for
that signal. Neither a high R-squared over four loads nor a rising peak
establishes a general complexity law; fixed setup can dominate small
loads, and memory high-water can rise with charged files even for a
streaming task. Block I/O and carbon are outside this experiment.

### Docker Desktop load pilot

All 12 attempts verified with complete readings, three per size. Median
times below are seconds; memory is cgroup lifetime peak in MiB.

| Total rows | Airflow execution | Output verification | Whole worker | Peak MiB |
| ---: | ---: | ---: | ---: | ---: |
| 10,000 | 3.63 | 0.073 | 6.58 | 232 |
| 100,000 | 3.60 | 0.690 | 7.04 | 236 |
| 500,000 | 4.25 | 3.793 | 11.00 | 254 |
| 1,000,000 | 5.66 | 7.479 | 16.73 | 280 |

The descriptive wall-time slopes were 0.21 s per 100,000 rows for the
execution block, 0.75 s for verification, and 1.04 s for the worker.
Execution medians were not monotonic at the two smallest loads. Verification
accounts for most of the observed worker slope, so the worker curve should
not be interpreted as pipeline-only scaling. This was a single-host pilot.

### Docker Desktop task-timing pilot

A separate three-round sweep used the opt-in timing wrapper. All 12 runs
verified, and each load produced byte-identical CSV output to the untimed
pilot at the same load. Median seconds:

| Total rows | Task body wall | Task thread CPU | Airflow execution wall | Verification wall | Worker wall |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 10,000 | 0.021 | 0.019 | 3.701 | 0.073 | 7.299 |
| 100,000 | 0.186 | 0.181 | 4.264 | 0.766 | 8.157 |
| 500,000 | 0.915 | 0.905 | 5.153 | 3.992 | 12.537 |
| 1,000,000 | 1.867 | 1.846 | 5.569 | 8.012 | 17.274 |

Task wall time rose by about 0.186 s per 100,000 rows (descriptive
R-squared 0.9999). The enclosing Airflow time did not rise in the same
proportion, and verification took longer than the task itself at every
load. These observations are for this single-threaded transformation and
worker; they do not establish a general scaling law or quantify timing
overhead by comparing the separate pilots.

### Compare scaling sessions

On the same worker, run the instrumented sweep again in a new directory.
Keep the image, confirmation report, load definitions, repetitions and
timeout fixed. Predeclare session start times and a seed for each session;
different seeds are allowed. Use separate days for a day-to-day claim.

```bash
python -m benchmarks.climate_fusion.scaling_sessions "$ROOT/session-1" "$ROOT/session-2" \
  --out "$ROOT/session-comparison.json"
```

Pass directories from the current instrumented sweep (manifest version
1.1). The comparison reads each planned attempt, recomputes coverage from
the saved runtime payloads, and refuses mixed profiles or workload hashes.
Missing attempts or conflicting output fingerprints suppress cross-session
statistics. The report compares independently fitted slopes and per-load
medians, without pooling attempts or deciding what variation is acceptable.

In a same-evening Docker Desktop pilot, two three-round sessions used
different order seeds and a 30-second pause between sweeps. All 24 attempts
verified with complete timing coverage. Task wall slopes were 0.182 and
0.188 seconds per 100,000 rows (3.5% apart relative to their midpoint).
At one million rows, task wall medians were 1.822 and 1.876 seconds. At
10,000 rows, whole-worker medians were 7.049 and 6.289 seconds, 11.4% apart.
The pilot is not evidence of day-to-day drift or a tolerance for comparing
different pipelines.
