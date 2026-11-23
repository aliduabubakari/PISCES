# PISCES platform extension roadmap

## Objective

Show which parts of PISCES transfer to other data pipelines and orchestrators,
then make that evidence understandable without a CLI. The success criteria are
correct work verification, credible telemetry, and clear limits. No milestone
requires a speedup, a carbon saving, a linear curve, or indistinguishable numbers
across orchestrators.

| Milestone | Deliverable | Depends on | Status |
| --- | --- | --- | --- |
| G1 | Second workload family and reusable campaign runner | Current Airflow OCI profile | Five-family qualification suite implemented |
| G2 | Shared execution profile and adapter conformance tests | G1; preserve existing Airflow behavior | Shared OCI profile registry and state evidence implemented |
| G3 | Bounded Prefect profile, then bounded Dagster profile | G2; independently gated | Experimental profiles passed separate Docker Desktop qualification |
| U1 | Local, read-only dashboard for validated reports and campaigns | Frozen view contract; can start in parallel with G1 | Report and campaign views, load plots, filters and optional local API implemented; richer task/source evidence pending |
| U2 | Guided execution of approved workloads | U1, worker controls, and at least one qualified OCI profile | Gated on U1 and execution controls |

## G1: Establish workload generalizability first

The [five-pipeline qualification CLI](BENCHMARK_SUITE.md) now implements
this first slice with Climate Fusion and four additional families, each
with valid, equivalent, same-shape-wrong and missing-output cases. The
shared runner supports standalone task bodies and the existing Airflow OCI
profile. This does not qualify the other orchestrators or production data.

The v2 contract/oracle, OCI cgroup collector, and separation of telemetry from
correctness are reusable. Climate Fusion's generator, single task timing hook,
output name and counts, load schedule, and report extraction are not a generic
protocol yet. Existing v2 verification is CSV-based and does not express arbitrary
input semantics, data privacy, or production service effects.

1. Add a deterministic event-aggregation benchmark with three stages: validate
   and normalize events, aggregate by a configurable key, and write sorted totals
   and a rejection summary. Use synthetic records and a separate reference
   implementation. Vary row count and key cardinality independently. This tests
   multiple steps, multiple outputs, and growing state, unlike Climate Fusion's
   single streaming transform. Do not require the same memory curve.
2. Keep Climate Fusion as a regression workload. For the new benchmark include a
   valid baseline, an equivalent variant, and a wrong variant that preserves
   schema and output count but changes an aggregate. Add a deterministic
   failure or missing-output case. Require task coverage, fixture consumption,
   and every declared output to pass independently.
3. Extract the campaign runner around explicit workload definitions: versioned
   benchmark ID, bundle, target binding, load dimensions with units, expected
   per-output invariants, repetition schedule, and phase metrics. Validate
   fingerprints by logical output name, not by the literal name `fusion`. Keep
   benchmark-specific timing hooks optional, and preserve the existing Climate
   Fusion command and historical reports. Do not add a plugin framework yet.
4. Run both families with seeded order, fresh containers, and archived attempts.
   Report coverage and variation by scope and load, not a pooled score. Keep a
   fixed reference environment and repeat on dedicated native Linux when
   available; label Docker Desktop as a pilot.

**Acceptance:** Both families run through the same campaign code without
workload-specific conditionals in the measurement path. Wrong or partial work
is never credited. Identical verified work is reproducible across sessions within
the reported variation; missing counters remain explicit. Two families support a
limited transferability claim, not universal generalizability. A held-out
workflow family, ideally with different file formats or a local service fixture,
is a later validation step and may require a new oracle.

## G2/G3: Extend orchestrator support without changing the claims

The [experimental OCI engine profiles](ENGINE_QUALIFICATION.md) now provide
separate Prefect and Dagster admission and state extraction. Legacy native
probes remain unqualified. Qualification evidence must identify the exact
image and target, and does not establish deployed or cross-target parity.

At the reviewed baseline, Prefect's local adapter invoked the underlying function rather than
proving flow and task execution through the engine. Dagster calls
`execute_in_process` but reduces evidence to job success. Both can flatten
failures into skips. OCI preflight, staging, worker version checks, and the
v2 experiment admission currently assume Airflow. Do not unlock them with a
target allowlist change alone.

1. Extract a small profile registry covering pinned runtime dependencies, entry
   point identity, target probe, allowed execution strategy, and mapping from
   orchestrator state to task/step evidence. Reuse isolation, staging, oracle,
   collector, and report types. Keep native engine names and states alongside
   the common verdict. Keep the Airflow image and evidence regression suite.
2. **Prefect first:** qualify one explicit synchronous flow using the actual
   Prefect engine in a local, isolated profile. Capture flow and task terminal
   states without calling `.fn` as a fallback. Prove the profile works without
   external services, or define an explicit local fixture and account for it.
3. **Dagster second:** qualify one explicit in-process job. Derive op/step
   coverage from its event stream, not from the job success flag. Capture failed
   and skipped steps distinctly. Defer general asset graph selection, dynamic
   partitions, multiprocess execution, and deployed services.
4. For both, pin a tested dependency set and immutable image; record execution
   setup, engine/run interface, process topology, and evidence boundaries. Start
   with no retries, deterministic local file fixtures, and explicit selection
   when there are multiple entry points. Do not infer equivalent task-body CPU
   from engine event durations or compare process CPU against cgroup CPU.
5. Implement both benchmark families for each target with shared fixture and
   oracle bytes, but target-specific bindings and expected task/step evidence.
   Demonstrate within-target repeatability before designing any cross-target
   resource comparison. Different orchestration overhead is part of the named
   profile, not automatically a pipeline efficiency difference.

**Acceptance per target:** An image-pinned OCI run verifies both workload
families; wrong output and a failed step cannot pass; ambiguous binding and
unsupported profiles fail closed; limits and isolation remain in force. Test
skips, retry rejection, timeout, missing counters, and task/step coverage.
Run the software counter fixtures on the pinned profile and publish coverage
by profile. Keep an explicit support matrix for static, import, controlled
runtime, task evidence, and comparison eligibility. No silent host or surrogate
fallback. Keep shipped support marked experimental until these gates pass.

## U1: Explainable dashboard, reports first

The [frontend design](FRONTEND_DESIGN.md) specifies the InLumen-inspired
source/evidence workspace, notebook/CLI parity, and a proposed optional HTTP
adapter. The [first workspace slice](FRONTEND_WORKSPACE.md) now implements the
report viewer, Python-owned view contract, portable CLI export and a stateless
local view API. Campaign adapters now cover suite inventory, load summaries and
independent scaling sessions, including numeric load plots and original planned
denominators. Richer task/source-linked evidence and usability qualification
remain; U1 is not complete.

Build a local, read-only report dashboard before exposing execution. Users
should be able to open a saved report or campaign and see useful results on
the first screen. Do not require an account, API keys, Docker, or access to
raw input fixtures for report viewing. Use the current report validation API
and a small versioned dashboard view contract over individual reports and
campaign summaries; do not have components parse incidental JSON fields.

| View | What a user should be able to answer |
| --- | --- |
| Run inventory | What ran, on which profile and inputs, and what failed or was not checked? |
| Run detail | Did the output verify? Which tasks ran? Which signals were measured and in what scope? |
| Load sweep | How do task, engine, verifier, and worker metrics move by load and session? What is missing? |
| Comparison eligibility | Can these reports be compared? If not, which required evidence or profile differs? |

For every metric, expose the definition, unit, scope, source, availability
reason, conformance context, and link or pointer to the underlying evidence.
Show phase boundaries and overlapping windows, not a stacked chart that
adds a nested interval twice. Keep repeatability spread separate from reference
agreement. Render unsupported/missing as missing, never zero. Distinguish
observed, synthetic, simulated, and modeled results; show carbon only when a
separately validated model provides it. Explain these distinctions with
versioned definitions and reason codes, not generated justifications.

**Acceptance:** Browser tests cover valid, failed, partial, static-only, obsolete,
malformed, and incompatible reports, including a missing carbon value with
available runtime. Test chart units and scopes, keyboard access, responsive
layouts, empty states, and safe handling of report text. Redact local paths and
other sensitive provenance in display and public exports by default. Verify
that a first-time user can identify the output verdict, find a metric's scope,
and explain why a comparison is blocked. No rankings for unsupported evidence.

## U2: Guided approved execution

After U1, add a readiness check, workload/profile selection, progress and
cancellation, then link directly to the resulting report. Reuse the same
backend policy as the CLI. Start with bundled, approved benchmarks on a local
worker. A report upload is never executable. Do not expose the Docker socket
or direct admission of arbitrary workflow files to the browser. Public or
multi-tenant execution requires a separate threat model, isolation review,
resource limits, and credential and retention policies.

**Acceptance:** Test the complete guided path and denial paths, including
profile unavailable, validation failure, cancellation, and cleanup failure.
Keep results exportable in the same report formats used by the CLI.

## First implementation slice

Start G1 with the event-aggregation bundle and exact oracle tests. Prove the
new workload differentiates verified, equivalent, same-shape wrong, and partial
outputs in standalone tests; then run its baseline on the existing Airflow OCI
profile before extracting the shared campaign runner. In parallel, freeze a
small dashboard fixture pack and view contract, but defer execution controls.
Choose concrete Prefect and Dagster version pins against their official docs
and actual integration results when qualifying each profile.
