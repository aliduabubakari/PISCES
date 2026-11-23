# PISCES Robustness Plan

## Purpose and baseline

Make PISCES a software-only evaluator that can say **what ran, what useful
work it delivered, what resources were observed, and what can be compared**.
When real data is unavailable, provide design feedback or explicitly modeled
predictions instead of substituting a benchmark and calling it workflow runtime.

Baseline: `codex/pisces-mac-measurement-hardening`, commit `3ef82c6`, with 72
tests passing in the implementation review. The starting point includes static
analysis, guarded local runners, an Airflow workload-contract path, operational
counters, and secondary CodeCarbon output. It does not have a validated
energy model, a portable execution boundary, or a strict pairwise comparator.
This plan supersedes the relevant claims in the [original roadmap](../plan.md).
The [controlled experiment protocol](CONTROLLED_EXPERIMENT_PROTOCOL.md) records
the existing Airflow procedure; version its successor rather than reinterpreting
historical reports.

The studies inform the order of work. The corpus yielded 6,452 static profiles,
which shows coverage, not graph fidelity. The Airflow pilot completed 20 runs
under a partial output contract, which shows repeatability for one pair, not
equivalent work. In frozen Study D, 24 screened pairs yielded two pairs that
passed both native contract screens and zero that passed pairwise equivalence.
The earlier optimization result exposed a dependency-parser blind spot; the
reviewed V2 smoke summaries are dry runs, not optimization results. Preserve
those cohorts, reports, and exclusion decisions. New benchmarks use new IDs,
contracts, and result directories.

## Product and claim boundaries

| Stage | What it does | Maximum claim |
|---|---|---|
| Static analysis | Reads code/specs and flags supported patterns | A design risk was detected; no runtime or carbon result |
| Target validation | Loads and checks the artifact in an isolated, pinned target environment | The artifact passed the named target gate, not that its tasks ran |
| Task probe | Executes selected real task bodies on controlled inputs | Those tasks behaved this way on those fixtures; no whole-workflow verdict |
| Simulation | Predicts workflow behavior from task profiles and scenario assumptions | Conditional prediction with coverage and uncertainty; no observed workflow result |
| Controlled execution | Runs the actual workflow with bound inputs and checks valid work | Observed resource use on this workload and environment; comparable only after a pairwise verdict |
| Production observation (later) | Observes a declared deployment and workload | Observed behavior in that deployment; no automatic generalization |
| Diagnostic benchmark | Runs the existing evaluator-owned hash/arithmetic loop | Runner readiness or diagnostic performance only; never workflow efficiency |

Execution mode, input origin, and evidence method are independent. For example,
an Airflow run with deterministic fixtures executes real tasks, but is not
production-equivalent. An actual dataset fed to the diagnostic benchmark still
does not execute the workflow. A dry-run plan or import check is not simulation;
simulation requires an explicit model and inputs. Do not use one `confidence`
label to collapse semantic validity, telemetry quality, and generalizability.

**Scope:** no power meters, privileged power counters, or claims of electrical
accuracy. Observe software counters and optionally model energy and carbon.
Do not add Kubernetes, an automatic rewrite agent, or more native adapters to
the first release. Preserve `pisces-unified`, `pisces-compare`, and the static
no-credentials path; new behavior should fit those entry points before adding
commands. The planned first supported benchmark is Airflow; feature parity for
Prefect, Dagster, Argo, and Kestra must be earned with target-specific tests.

The [platform extension roadmap](PLATFORM_EXTENSION_ROADMAP.md) stages a
second workload family, Prefect and Dagster OCI qualification, and an
explainable report dashboard. These extend the Airflow baseline without
relaxing its measurement or release gates.

## Delivery sequence

| Phase | Deliverable | Depends on | Status |
|---|---|---|---|
| 1 | Fail-closed reporting and comparison | Reviewed baseline | Implemented |
| 2 | Workload contract v2, output oracle, and reference benchmark | 1 | CSV semantic gate validated on Airflow 3.4.0 |
| 3 | Validated repetitions and reproducible paired execution | 2 | Exact-output Airflow pilot implemented |
| 4 | Isolated Linux OCI execution and software telemetry | 3 | Docker Desktop pilot and wall/CPU conformance harness; native Linux confirmation pending |
| 5 | Auditable modeled energy and operational carbon scenarios | 3; 4 for container claims | Planned |
| 6 | Graph fidelity, task probes, and simulation | 2-4 for held-out validation | Planned |
| 7 | Recommendation evaluation, release, and Kubernetes decision | Relevant earlier gates | Planned |

Each phase should land as a reviewable change with its tests, fixtures, and docs.
Update the status after its gate passes; tests of historical compatibility must
remain separate from tests of current behavior.

## Phase 1: Stop unsupported comparisons

Implemented: no runtime or carbon leaderboards; scenario flags record refusals
under the compatibility `estimate` key. Runtime schema 1.4 reports distinct
execution, contract, telemetry, and equivalence states, retains diagnostics,
and withholds normalization. It also distinguishes requested/resolved targets
and adapters and identifies runner versus controller metadata. The packaged
JSON schema is checked at CLI output and comparator admission. Known legacy
reports are inventory only. Controller and worker preflight reject unsupported
strict requests; automatic data selection never invokes a hosted provider.
The frozen historical Airflow benchmark is not requalified by these changes.

**Change** [`dag_comparator.py`](../src/pisces/dag_comparator.py),
[`runtime_evaluation.py`](../src/pisces/energy/runtime_evaluation.py),
[`runtime_runner_cli.py`](../src/pisces/energy/runtime_runner_cli.py), and
[`unified_evaluator.py`](../src/pisces/unified_evaluator.py):

- Keep static findings and an inventory of input reports. Remove unconditional
  runtime and annual carbon leaderboards. For now, return `not_comparable` with
  stable reason codes for reports that do not carry the required evidence; no
  numeric efficiency deltas until Phase 3. Warnings cannot override a gate.
- Suppress per-unit workflow numbers for surrogate, failed, partial, zero-work,
  or unverified runs. Preserve available counters as attempt diagnostics, not
  zeros or successful observations. Suppress scenario projections from these
  sources and from blocks that lack a verified denominator. Rename the public
  `calibrated_estimate` concept to `scenario_projection`; retire fixed tier
  percentage bands as uncertainty intervals. Restore projections in Phase 5.
- Make eligibility distinct from equivalence: `eligible_pending_pairwise_comparison`
  is not a pass and must not upgrade confidence. Separate target acceptance,
  execution outcome, contract verdict, telemetry status, and pairwise verdict.
  Record requested and resolved target and adapter independently; do not use
  controller package versions as proof of the runtime environment.
- Publish a versioned JSON report schema and one shared validation/eligibility
  entry point. Preserve useful legacy fields only through an explicit compatibility
  view; never promote legacy reports to qualified evidence. Define migration
  behavior and reject unsupported future schemas for comparison. Keep JSON
  standard-compliant: reject non-finite numbers and distinguish missing,
  unsupported, not run, and invalid. Do not accept booleans as numeric metrics.
- Keep old CLI entry points during migration. Explicit native and strict modes
  fail on missing prerequisites; no fallback to surrogate or an implicit hosted
  LLM call. Keep the surrogate only as an explicitly named diagnostic result.
  Update README, demo, and optimization experiment integration notes so none
  describe diagnostic values as candidate workflow energy.

**Tests:** Add `tests/test_dag_comparator.py` for mixed evidence, mismatched
scenarios, missing hashes, unknown schema, diagnostics, invalid numbers, and
pending equivalence. Extend unified and golden report tests for static, target
mismatch, missing backend, native failure, and diagnostic sample input. Keep a
legacy golden fixture and assert inventory-only behavior. Assert strict mode
does not invoke the surrogate or a provider when prerequisites fail.

**Exit gate:** No currently unsupported report pairing can produce an efficiency
delta or annual workflow carbon ranking. Static screening still works without
an orchestrator, data, CodeCarbon, or credentials.

## Phase 2: Make intent testable

The [v2 CSV contract](WORKLOAD_CONTRACT_V2.md) and
[climate-fusion benchmark](../benchmarks/climate_fusion/README.md) implement
the initial semantic gate. The standalone benchmark compares actual task
outputs; the opt-in integration suite also runs the DAGs on the
[pinned Airflow 3.4.0 profile](AIRFLOW_3_RUNTIME.md). Arbitrary input schema
rules, deployment constraints, and additional output formats remain outside
this initial result.
No efficiency ranking or verified multi-run accounting is enabled.

**Add** a versioned workload contract and strengthen
[`workload_contract.py`](../src/pisces/energy/workload_contract.py),
[`output_assertions.py`](../src/pisces/energy/output_assertions.py), and the
fixture binding path. Use three distinct records rather than a single file
that mixes requirements with implementation details:

| Record | Owns | Required information |
|---|---|---|
| Workload contract | User/reviewer-approved intent | Input schema, key/units/null rules, fixture hashes, logical outputs, content oracle, functional unit, service effects, and hard limits such as deadline or residency where applicable |
| Artifact binding | Adapter-specific wiring | Artifact hash, target and entry point, fixture mappings, expected task/branch coverage, output locations, and any recorded rewrites |
| Experiment manifest | Reproduction policy | Versions/image digests, run order and seed, scopes, warm-up/cache policy, repetition/budget limits, and assertion/equivalence versions |

- Check in schemas and portable examples. Keep v1 readable as historical or
  diagnostic input; incomplete v1 contracts cannot acquire v2 eligibility.
  Validate types and cross-references before running. Use relative references
  confined to declared roots and verify the staged bytes used for execution.
  Remove thesis-only route restrictions from general artifact bindings; route
  is optional experiment metadata. Emit portable provenance, not host paths,
  raw sample rows, credentials, or unrestricted output previews.
- Define `verify_output` against an independent, reviewed reference or oracle;
  define `compare_outputs` separately for pairs. For the first tabular oracle,
  require a logical output set, typed schema, key uniqueness, row content,
  units, and null behavior. Use key-based comparison with contract-declared
  ordering and numeric tolerances; compare exact deterministic content by
  canonical digest only where applicable. If a contract declares tolerances,
  compare values rather than pretending a byte hash implements tolerance.
  Make large checks bounded or streaming; if the checker cannot complete,
  report `not_verified`. Verify service effects through fixture logs and output
  invariants, not just proof that a file was opened.
- Store a structured verdict (`passed`, `failed`, `not_verified`) with reason
  codes and references to the checks that ran. The pairwise verdict must bind
  both artifacts, the contract, the oracle version, and the actual observations.
  Never let candidate code supply its own oracle or claim its own equivalence.
  A fixed-topology arm may also require identical IDs/edges. That requirement
  is an experiment choice, not the general definition of intent.
- Create `benchmarks/climate_fusion/` with deterministic five-source fixtures
  at small CI scale and a reproducible larger tier, a known-good baseline, a
  known-equivalent variant that avoids a redundant read or materialization, and
  a faster wrong variant that preserves schema and row count but corrupts
  normalization or ignores one source. Generate expected normalized output
  independently of all three implementations. Record fixture seeds and hashes.

**Tests:** Extend `tests/test_workload_contract.py` for schema errors, wrong
hashes, wrong target, missing output/fixture bindings, and portable replay. Add
`tests/test_output_equivalence.py` for equivalent serialization/order, duplicate
keys, null and unit mismatches, tolerance boundaries, equal-shape wrong values,
missing inputs or side effects, and unverified large output. Verify the known
good pair passes and the deliberately wrong variant fails even if it is faster.

**Exit gate:** The semantic gate discriminates the three reference variants
without reading timing or energy. Every verdict points to the exact inputs and
assertions used. The frozen Study D inputs/results remain unchanged.

## Phase 3: Measure verified repetitions

The first [paired pilot](../benchmarks/climate_fusion/README.md#paired-resource-pilot)
uses one fresh Airflow 3.4 runner per attempt, exact CSV output fingerprints,
and software counters only. It writes a predeclared manifest, attempt and
pair records, and an analysis with verified-batch totals and an explicit
inconclusive outcome. This does not yet support warm-state studies,
tolerance-based native pair analysis, or production generalization.

**Change** [`runtime_runner_cli.py`](../src/pisces/energy/runtime_runner_cli.py),
[`operational_metrics.py`](../src/pisces/energy/operational_metrics.py),
[`execution_sandbox.py`](../src/pisces/energy/execution_sandbox.py), and
[`dag_comparator.py`](../src/pisces/dag_comparator.py):

- Separate run planning, execution, validation, and aggregation. Start with one
  measured workflow execution per fresh process for strict benchmarks. Set up
  fixtures before timing, enforce isolated output state, and verify coverage,
  input use, and outputs for **every** measured repetition outside the measured
  window. Record warm-ups separately; cold and warm runs are different profiles.
  If warm state is intended, declare what may persist and require fresh outputs
  so the validator cannot pass a stale result.
- Record status, resource scope, timestamps, raw counters, and credited units
  per attempt. A passed repetition contributes only its declared functional
  unit (start with `valid_batch`). Normalize additive counters by the sum of
  verified units within the same boundary. Do not sum peak memory or call a
  lifetime RSS high-water mark a phase-specific peak. Never divide an energy
  window that includes invalid work by only valid units; treat mixed blocks as
  attempt cost. Retain timeout/failure costs when observable.
- Add a paired-run manifest and analysis path. Predeclare the metric, cache
  policy, minimum meaningful effect, precision goal, and missing-pair rule.
  Counterbalance or randomize AB/BA order using a recorded seed. Use a pilot
  to set duration and repetition bounds, preserve all pairs and failures, and
  report completion yield alongside eligible-pair estimates. For comparisons
  across pipelines, artifact pairs are the experimental units; repetitions
  within one pair quantify technical variation, not corpus sample size.
- Enable comparison only when the pairwise output verdict passes for the
  analyzed runs, workload/fixture and unit identities match, and measurement
  method, environment, and boundary match the declared comparison profile.
  Controlled studies of a changed environment must declare that change as the
  treatment and keep the remaining profile fixed. Return `inconclusive` when
  precision or pair coverage is insufficient, not `no improvement` or a win.
- Check wall and CPU collection against independently invoked OS counters on
  controlled subprocesses with aligned boundaries. Measure monitor overhead
  with an empty run and simple workloads. Pin the actual runner environment
  and record dependency versions, not just the controller interpreter.

**Tests:** Cover fail-then-pass and pass-then-fail, leftover outputs, warm-up
exclusion, zero-work/partial/timeout runs, two valid repetitions with a known
block total, mixed valid/invalid blocks, process-versus-child CPU scope, and
memory high-water semantics. Test scheduling and paired analysis with fake
clocks/metrics, seeded order, missing pairs, and incompatible provenance.
Run the real Airflow three-variant benchmark in a pinned integration job.

**Exit gate:** The wrong variant cannot rank; the valid pair can produce a
bounded resource comparison with verified units and raw observations. CI
asserts correctness and scope, not a brittle percentage speedup. The pinned
benchmark records dispersion and independent counter agreement; neither is
evidence of electrical-energy accuracy.

## Phase 4: Isolate import and execution in Linux OCI

The [OCI runtime profile](OCI_RUNTIME.md) implements explicit Docker
execution for a single Airflow v2 contract run. The initial smoke runs use
Docker Desktop's Linux VM. A pinned native Linux reference campaign and
comparison across container repetitions remain separate work.

**Add** a single whole-workflow OCI adapter behind the existing execution
interface. Run target validation and workflow execution inside the declared
image. Use Linux cgroup v2 for workload CPU time, peak memory, and available
block I/O counters. Record their scope (including descendants, page cache,
and any unsupported controller), plus wall time, exit/timeout state, and
separate setup/teardown costs. Capture final counters before the runtime
destroys the cgroup. Do not treat block I/O as network bytes or container memory
as the same metric as process RSS. Use a pinned Linux worker for reference
measurements; report a Docker Desktop VM as a different environment.

- Pin image digests, entry point, quotas, OS/runtime versions, and mount policy.
  Use non-root execution, no privileged mode, no runtime socket or host-home
  mount, an allowlisted environment, read-only inputs, bounded scratch space,
  and disabled network by default. Use only declared local service fixtures.
  Enforce timeouts and clean up descendants and scratch state. Stage or pull
  images separately from the timed phase; keep cold-start data separate.
- Mark the existing Python guards as trusted-code convenience only. Move both
  native import and execution behind the same isolation policy for generated
  code. Do not silently fall back to a host subprocess if isolation is absent.
  This is initially a controlled worker for approved artifacts, not a public
  multi-tenant sandbox. Define a stronger worker/VM boundary and review it
  before accepting submissions from untrusted users.
- Extend [`diagnostics.py`](../src/pisces/diagnostics.py) with capability checks
  for image identity, kernel controllers, limits, and telemetry. Add per-task
  containers only for declared container-native tasks with explicit artifact
  handoffs. Treat any conversion from native tasks as a new deployment variant,
  including startup, transfer, retry, and shared-service overhead.

**Tests:** Unit-test manifest construction and counter parsing; integration-test
the real workflow and child-process accounting, missing counters, exit and OOM
classification, final counter capture, timeout cleanup, no credential inheritance,
read-only inputs, and blocked undeclared network. Check that a failed isolation
setup prevents the import and runtime from starting. Compare OCI output to the
reference oracle; do not require process and cgroup counter equality.

**Exit gate:** An image-pinned Airflow run delivers the verified benchmark on
Linux without host secrets, live services, or hardware power access. Missing
telemetry stays missing; inability to enforce isolation is a preflight failure.

### Phase 4.1: Validate telemetry before comparing workflows

- [x] Share the production OCI collector with a no-data wall/CPU conformance
  harness. Retain raw CPU endpoints and window timestamps. Run calibration
  separately from confirmation with explicit tolerances. See
  [the OCI conformance protocol](OCI_RUNTIME.md#wall-and-cpu-conformance).
- [ ] Confirm on a pinned native Linux worker, with a dedicated calibration
  session, predeclared tolerances and minimum supported durations, and fresh
  confirmation runs. Follow [the native Linux runbook](NATIVE_LINUX_TELEMETRY.md)
  and keep Docker Desktop results separate.
- [x] Preserve per-metric unavailability and lifecycle failures separately.
  Reject unobserved I/O baselines and preserve available evidence on timeout,
  inspection and cleanup failures without inventing missing values.
- [x] Exercise bounded live OOM and check peak memory against known page
  allocations in the [Docker Desktop memory pilot](OCI_RUNTIME.md#peak-memory-and-bounded-oom).
- [ ] Validate disk-backed I/O before treating it as conformance-tested.
- [x] Run a Docker Desktop same-baseline pilot with the
  [OCI repeatability driver](../benchmarks/climate_fusion/README.md#oci-same-baseline-repeatability)
  and publish coverage and dispersion.
- [ ] Repeat the verified Climate Fusion baseline across native Linux sessions
  and publish coverage and dispersion separately. Do not use this A/A run
  to claim speedup, carbon savings, or collector accuracy in isolation.
- [x] Run the four-load Docker Desktop pilot with exact streaming verification
  and separate measurements for execution and verification.
- [x] Add task-body wall and thread CPU telemetry to the trusted Climate
  Fusion sweep. Preserve the task, Airflow, verifier and worker boundaries.
- [x] Add cross-session comparison of per-load medians and independently
  fitted slopes; require a matching profile, planned coverage and matching
  verified output fingerprints.
- [ ] Repeat the [10,000 to 1,000,000-row load sweep](../benchmarks/climate_fusion/README.md#oci-load-sweep)
  on the native Linux reference worker. Keep workflow and verifier timings
  separate, and report slopes as descriptive observations, not exit criteria.

## Phase 5: Model carbon without a power meter

**Change** [`carbon_tracker.py`](../src/pisces/energy/carbon_tracker.py) and
the scenario path in `runtime_evaluation.py`. Separate telemetry from energy
model output and grid conversion. Make CodeCarbon an optional, pinned backend:
record effective attribution mode, version, model/source, supported components,
and fallback reason. In the software-only profile, use only a verified modeling
path; if that cannot be established for the backend/version, omit the estimate.
Never infer a backend from platform or a configuration flag alone. Avoid
machine-wide attribution to one container on a shared host.

- Store model version, assumptions and valid workload range, the source and
  time/region basis of grid intensity, and included/excluded components. If
  using an OCI telemetry-based model, treat utilization-to-energy coefficients
  as assumptions, disclose idle allocation and provisioned scope, and avoid
  assigning unsupported components zero. Do not substitute CPU time for energy.
- Calculate modeled operational emissions as `sum(E_kWh[t] * I_gCO2e_per_kWh[t])`
  over matching intervals and normalize by verified units. Use a fixed intensity
  in software-efficiency comparisons to isolate the resource change. Evaluate
  time/region shifting separately with an archived intensity series, deadline,
  allowed regions, transfer effects, and missing-data rules. Without an energy
  model, report resource results; without a matching grid factor, omit CO2e.
- Restore scenario projections only for an eligible baseline with an explicit
  mapping from a verified batch to the projected workload. Record schedule,
  volumes, environment, and extrapolation limits. Separate sampling variation,
  model sensitivity, and grid/scenario uncertainty; call parameter ranges
  sensitivity ranges unless calibrated. Don't claim a full SCI score unless
  the declared boundary includes provisioned energy and allocated embodied
  emissions. Keep generation/repair, validation, and operation separate.
  EcoLogits may be used later for a separate hosted inference estimate.

**Tests:** Mock backend variants and failure states; test unknown sources,
non-finite and missing factors, unit conversion, time/region alignment, interval
integration, multiple repetitions, and unsupported extrapolation. Golden cases
include resource-only, modeled operational carbon, and refused projection.
Check that a simulated grid shift can change modeled emissions without changing
energy or automatically qualifying a missed deadline.

**Exit gate:** Every published carbon figure can be reproduced from its model,
inputs, functional unit, and assumptions. Unsupported models or scopes produce
no number. No result is marketed as metered or externally calibrated energy.

## Phase 6: Make no-data evidence useful

**Extend** the static analyzers and data strategies; add a separate task-profile
and simulator interface rather than extending the hash loop.

- Normalize supported graphs from the artifact, target runtime, or user intent
  spec with source provenance and unresolved-edge indicators. Distinguish a
  legitimately disconnected graph from incomplete extraction. Test equivalent
  dependency syntax against pinned Airflow graph snapshots. When the graph is
  incomplete, suppress graph-derived recommendations and simulation rather
  than guessing. Make rules conditional: parallelism can reduce latency while
  increasing provisioned resources; shorter critical path is not carbon proof.
- Prefer deterministic, contract-derived fixtures to LLM recipes. Cover schema,
  size, key distributions, join cardinality, skew, nulls, branches, duplicates,
  and retry/cache scenarios as appropriate. Real sample data stays local; use
  only an approved redacted schema/profile for any opt-in hosted service.
  Record which tasks, branches, and scale ranges each fixture actually covers.
- Profile actual supported task types in the isolated backend at multiple scales
  and cold/warm conditions. Model dependency order, concurrency limits, queueing,
  startup, transfer, retries, and cache behavior with explicit scenario inputs.
  Use a discrete-event simulation library if needed. Start with one batch ETL
  profile and a fan-out/fan-in profile; unknown tasks stay unknown. Output
  predicted makespan and resource ranges with profile sources and coverage;
  estimate carbon only through the separately declared Phase 5 model.
- Pre-register a held-out comparison against controlled execution. Compare
  static-only guidance, task profiles/simulation, and native baselines using
  resource prediction error, ranking agreement, false improvements, abstention,
  and evaluation cost. Hold out workload shapes or scales, not just repeats of
  fitted inputs. Set acceptable error and false-recommendation bounds after the
  pilot, before seeing held-out results. Abstain outside validated ranges or
  when uncertainty changes the decision.

**Tests:** Cover equivalent chain/edge syntax, dynamic or unknown graphs,
deterministic branch and skew fixtures, known critical paths and concurrency
limits, cold versus warm execution, retry overhead, missing profiles, and
out-of-range simulation. Keep fitted and held-out fixture manifests separate.

**Exit gate:** A held-out report states where simulation agrees with verified
execution and where it abstains. Without enough evidence it remains a labeled
experimental feature, not an authoritative workflow ranking.

## Phase 7: Evaluate recommendations and release

- Introduce recommendation records with evidence, preconditions, intent risk,
  expected direction, and the validation step required. Start with eliminating
  redundant I/O, reducing unnecessary materialization, and changing batch size
  under a fixed quality contract. Report trade-offs among runtime, CPU, memory,
  and optional modeled carbon. Do not let a heuristic score compensate for a
  failed semantic gate or a resource regression beyond a declared guardrail.
- Update the external optimization study to consume the paired verdict and
  verified metrics before accepting a candidate. Keep proposals and rejected
  attempts, including missingness. Recheck the final candidate against the
  original baseline in a fresh paired block. Evaluate fixed-topology and
  topology-changing optimizations separately; only the latter may alter edges,
  and only with a sufficiently strong intent oracle. For research reporting,
  separate observed resource outcomes from model-dependent carbon effects.
- Add a release demo that runs the reference benchmark from a clean checkout,
  plus a no-data demo that shows a static result, a labeled simulation if
  available, and a refused comparison. Document schema migration, supported
  target/backend combinations, isolation limits, and known evidence gaps.
- Consider Kubernetes only for a distinct research question such as placement,
  contention, autoscaling, or carbon-aware scheduling. Before implementation,
  define pod/job and shared-node accounting, telemetry for short-lived tasks,
  control-plane and idle allocation, workload placement, and an OCI parity
  test. Basic CPU/memory metrics alone do not satisfy that accounting contract.

**Tests:** Run a clean-install CLI smoke test and full Airflow benchmark with
the valid pair and wrong variant. Verify recommendation rejection for equivalent
outputs with resource regressions, changed intent, no evidence, and mixed
measurement scopes. Archive a reproducible run manifest, raw observations,
versioned reports, and analysis output without sample data or secrets.

**Exit gate:** A reviewer can reproduce the run and trace every conclusion to
the oracle, measurement scope, and source report. Do not claim a general route
or optimizer advantage from one benchmark family or from simulator-only scores.

## Quality gates and first implementation slice

- **Every change:** run the unit suite, Ruff, CLI help/smoke checks for changed
  commands, and report-schema/golden checks. No live API or power backend in
  ordinary CI. Validate the optional dependency matrix separately from base
  installation, and never count an unavailable integration as a passing run.
- **Native/OCI integration:** run on pinned environments with deterministic
  fixtures and no production credentials. Record the image or environment
  digest and retain all verdicts and exclusions. Use scheduled or explicit
  benchmark jobs for noisy timing, not hard speedup checks in ordinary CI.
- **Release blockers:** any path that ranks a surrogate, credits a failed
  repetition, accepts the known wrong variant, bypasses isolation, or reports
  unsupported carbon as zero. Treat these as regressions even when the rest of
  the report looks correct.

Start Phase 1 with one test that demonstrates the current mixed-evidence
ranking bug and one that demonstrates an unsupported surrogate projection.
Make both fail closed, then update the report schema and demo. Do not start
the OCI adapter or simulator until the reference oracle and comparison gates
are in place.

## Methodology references

- [Software Carbon Intensity specification](https://sci.greensoftware.foundation/): boundary, functional unit, operational and embodied emissions.
- [CodeCarbon 3.2 methodology](https://docs.codecarbon.io/3.2/explanation/methodology/): attribution modes, model selection, and component limits.
- [EcoLogits methodology](https://ecologits.ai/dev/methodology/): separate modeled accounting for supported hosted inference.
- [SINTEF SIM-PIPE](https://www.sintef.no/en/software/simpipe/): task profiling and pipeline simulation precedent.
- [Sustainable Quality in Data Preparation](https://doi.org/10.1145/3769120): joint assessment of data quality and sustainability trade-offs.
- [Linux cgroup v2](https://docs.kernel.org/admin-guide/cgroup-v2.html) and [Kubernetes resource metrics](https://kubernetes.io/docs/tasks/debug/debug-cluster/resource-metrics-pipeline/): software telemetry and its scope.
