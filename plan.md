# PISCES Engineering Roadmap

## Purpose

This plan defines the engineering path for turning PISCES from a promising prototype into a reliable carbon and runtime evaluation suite for workflow code. The central design principle is evidence honesty: PISCES should always explain what it measured, what it inferred, what it skipped, and how much confidence a user should place in the result.

PISCES should not claim to measure true production energy for every workflow. That requires production infrastructure, real datasets, external services, orchestration configuration, and workload traces that are often unavailable. Instead, PISCES should provide a layered assessment model:

- Static analysis is always available.
- Import/build carbon measurement is low-risk and broadly available.
- Representative runtime measurement is useful when real data is missing.
- Sample-data runtime measurement is stronger when user data is available.
- Native orchestrator execution is strongest when dependencies can be isolated.
- Calibrated estimates convert measurements into scenario-based impact projections.

The goal is a suite that is useful even with incomplete data, while clearly separating measured evidence from representative, synthetic, and estimated evidence.

## Target Product Contract

PISCES should produce a unified report that answers these questions:

- What workflow was evaluated?
- Which orchestrator was detected or selected?
- Which evidence tiers were attempted?
- Which tiers succeeded, failed, or were skipped?
- What data source was used?
- What execution adapter was used?
- Was CodeCarbon available and did it produce usable values?
- Is the result production-equivalent, representative, estimated, or static-only?
- What is the confidence level?
- What limitations should the user read before acting on the result?
- What design rules fired and why?
- How should the workflow be improved?

Every report should be safe to compare, automate, and archive.

## Evidence Tiers

### Tier 0: Static Heuristic Evaluation

Status: Always available.

Purpose:

- Analyze workflow structure without executing code.
- Detect sustainability design smells.
- Provide fallback results when runtime measurement cannot run.

Inputs:

- Python workflow files for Airflow, Prefect, and Dagster.
- YAML manifests for Argo and Kestra.
- Optional OPOS or PipeSpec sidecars.
- Optional knowledge pack.

Outputs:

- Task count, edge count, fan-out, fan-in, critical path, parallelism index.
- Schedule estimate and yearly run count.
- Operator tier distribution.
- Sensor, polling, retry, XCom, and resource-request signals.
- Fired eco-design rules.
- Static-only confidence classification.

Acceptance criteria:

- `pisces-unified workflow.py --stdout` produces a heuristic report by default.
- `--energy-mode heuristic` never requires optional runtime dependencies.
- YAML workflows produce static profiles without runtime support claims.
- README examples match actual command behavior.

### Tier 1: Import and Build Carbon Measurement

Status: Broadly available for Python workflows.

Purpose:

- Measure energy/carbon for importing workflow code and constructing DAGs, flows, jobs, or definitions.
- Capture parse/registration overhead separately from task runtime.

Inputs:

- Workflow file.
- Optional orchestrator environment.
- Optional country ISO and CodeCarbon settings.

Outputs:

- Import/build duration.
- CodeCarbon energy and emissions when available.
- Fallback availability status when CodeCarbon is missing or fails.
- Explicit measurement scope: `import_build_only`.

Acceptance criteria:

- `--track-carbon` alone produces an import/build measurement or a clear `not_measured` status.
- `carbon_country_iso`, `measure_power_secs`, and `carbon_scale_factor` affect measurement or are reported as unused with a reason.
- Reports distinguish import/build emissions from runtime emissions.
- CodeCarbon failure never crashes the evaluator.

### Tier 2: Representative Runtime Evaluation

Status: Available when no real data exists.

Purpose:

- Exercise bounded workload paths using generated data and local representative computation.
- Provide comparative runtime signals without claiming production equivalence.

Data strategies:

- `zero_fill`: all-zero or empty values inferred from schema hints.
- `fuzz_fill`: deterministic valid-ish values to trigger more branches.
- `branch_probe`: values designed to exercise conditional paths.
- `scale_probe`: repeated runs at multiple row counts to estimate growth.
- `operator_profile`: representative CPU, IO, polling, and orchestration overhead based on detected operator types.

Outputs:

- Rows processed.
- Tasks exercised.
- Duration.
- Energy and emissions when CodeCarbon is available.
- Normalized energy per 1k rows.
- Scaling slope when scale probes run.
- Explicit measurement scope: `representative_runtime`.

Acceptance criteria:

- Runtime measurement works from both installed packages and source checkouts.
- Representative mode does not require Airflow, Prefect, or Dagster imports.
- Generated datasets are deterministic by seed.
- Generated rows are not persisted in the final report.
- The report sets `production_equivalent` to `false`.

### Tier 3: Sample Data Runtime Evaluation

Status: Available when user supplies data.

Purpose:

- Use user-provided CSV, JSON, or JSONL files to run representative or task-level workloads.
- Improve confidence beyond generated data.

Inputs:

- One or more data files or folders.
- Optional schema mapping.
- Optional row limit and task limit.

Outputs:

- Dataset provenance.
- File hashes.
- Row count and schema summary.
- Runtime and carbon metrics.
- Privacy summary.
- Explicit measurement scope: `sample_data_runtime`.

Acceptance criteria:

- Raw sample rows do not appear in final report JSON.
- File hashes and aggregate schema metadata are recorded.
- Empty or incompatible sample data produces a clear skip/failure reason.
- Sample data results have higher confidence than generated representative runs.

### Tier 4: Native Orchestrator Runtime Evaluation

Status: Strongest local evidence when dependencies are available.

Purpose:

- Execute workflow code through the orchestrator where possible.
- Use local mocks and bounded execution to avoid unsafe production effects.

Initial adapters:

- Airflow: import DAGs, run `dag.test()` when safe, or execute Python callables with mocked context.
- Prefect: execute flow functions locally with task limits and patched external calls.
- Dagster: use `execute_in_process()` or materialization helpers where safe.
- Argo and Kestra: static-only initially, with future optional local engine support.

Safety requirements:

- Subprocess isolation.
- Timeout enforcement.
- Temporary working directories.
- No network by default.
- Controlled environment variables.
- Optional allowlist for external calls.
- Mock database, object-store, HTTP, and cloud clients where possible.

Outputs:

- Native adapter attempted.
- Native adapter result.
- Tasks executed and skipped.
- Mocked resources summary.
- Runtime/carbon metrics.
- Explicit measurement scope: `native_local_runtime`.

Acceptance criteria:

- Native execution failures fall back to representative mode in `auto`.
- Native mode never silently executes unbounded external effects.
- Adapter attempts are recorded in the report.
- Native results have the highest local confidence, but still do not claim production equivalence unless explicitly configured with production-like inputs.

### Tier 5: Calibrated Estimates

Status: Derived from measured and static evidence.

Purpose:

- Convert local measurements into scenario-based impact estimates.
- Help users compare workflows and reason about yearly emissions.

Inputs:

- Measured import/build energy.
- Measured runtime energy.
- Static schedule estimate.
- User-provided run frequency override.
- Carbon intensity from CodeCarbon or user config.
- Optional scale factor.

Outputs:

- Per-run estimate.
- Daily/monthly/yearly estimates.
- Confidence interval or uncertainty band.
- Assumptions list.
- Explicit measurement scope: `calibrated_estimate`.

Acceptance criteria:

- Estimates are separated from raw measurements.
- All scaling assumptions appear in the report.
- Estimates are disabled or marked low-confidence when no measured baseline exists.

## Unified Report Schema Direction

Add an evidence block to every report:

```json
{
  "evidence": {
    "measurement_kind": "static|import_build|representative_runtime|sample_runtime|native_runtime|calibrated_estimate",
    "confidence": "none|low|medium|high",
    "production_equivalent": false,
    "data_source": "none|zero_fill|fuzz_fill|branch_probe|scale_probe|sample|synthetic|native",
    "execution_adapter": "none|representative|native|auto",
    "measurement_scope": "static_only",
    "limitations": []
  }
}
```

Add an attempts block:

```json
{
  "attempts": [
    {
      "tier": "representative_runtime",
      "status": "success|failed|skipped",
      "reason": "ok",
      "duration_s": 0.0,
      "next_tier": "sample_runtime"
    }
  ]
}
```

Add separate measurement blocks:

```json
{
  "measurements": {
    "import_build": {},
    "runtime": {},
    "estimate": {}
  }
}
```

Report rules:

- Never mix raw measurement and estimate in the same field.
- Never report `carbon_tracking.enabled: true` as if measurement succeeded.
- Always include `available`, `status`, and `measurement_scope` for CodeCarbon output.
- Always include limitations for representative, synthetic, or estimated results.
- Always include a schema version and migration notes when the report changes.

## Immediate Correctness Fixes

These are the first issues to fix before adding new capability.

### Fix Default CLI Behavior

Problem:

- The README claims `pisces-unified workflow.py` evaluates heuristics, but the code currently emits only metadata unless flags are provided.

Tasks:

- Make heuristic evaluation run by default.
- Keep `--energy-mode heuristic` as an explicit equivalent.
- Add a `--no-evaluation` option only if there is a real need for metadata-only reports.
- Update README examples after behavior is fixed.

Acceptance criteria:

- `pisces-unified test_scripts/airflow/simple_etl.py --stdout` includes `energy.heuristic`.
- `pisces evaluate test_scripts/airflow/simple_etl.py --stdout` includes `energy.heuristic`.

### Fix Carbon Tracking Semantics

Problem:

- `--track-carbon` currently records intent but may not produce a real measurement.
- Carbon config is not fully passed to CodeCarbon.

Tasks:

- Add explicit import/build measurement path.
- Pass `country_iso_code`, `measure_power_secs`, and scale factor into measurement.
- Apply scale factor only in a clearly labeled estimate field, or store both raw and scaled values.
- Add status fields: `measured`, `not_available`, `failed`, `skipped`.

Acceptance criteria:

- `--track-carbon` produces an import/build measurement attempt.
- Reports show whether CodeCarbon was available.
- Reports distinguish raw measured emissions from scaled estimates.

### Fix Runtime Subprocess Import Path

Problem:

- Runtime subprocess execution can fail from source checkout because `PYTHONPATH` is derived from workflow location instead of project/package location.

Tasks:

- Resolve package root reliably.
- Preserve existing `PYTHONPATH` while prepending required paths.
- Prefer installed package imports when available.
- Add source-checkout integration tests.

Acceptance criteria:

- `PYTHONPATH=src python -m pisces.unified_evaluator ... --energy-mode zero_fill` succeeds from the repo root.
- Installed package execution succeeds in a clean virtual environment.

### Fix Missing Comparator

Problem:

- `pisces-compare` is exposed but `pisces.dag_comparator` is missing.

Tasks:

- Either implement `dag_comparator.py` or remove the entry point and documentation.
- Preferred: implement comparator over existing JSON reports first.
- Add support for comparing direct workflow inputs later.

Acceptance criteria:

- `pisces-compare report_a.json report_b.json` works.
- Comparator output ranks workflows by rule severity, measured energy, estimated yearly impact, and confidence.

### Fix Public API Exports

Problem:

- `src/pisces/__init__.py` lists exports that are not imported, and includes a missing `UnifiedEnergyEvaluator`.

Tasks:

- Import actual public API objects.
- Remove nonexistent exports or implement them.
- Add import smoke tests.

Acceptance criteria:

- `from pisces import EnergyEvaluationRequest, evaluate_energy` works.
- Public API import tests pass.

### Fix LLM Provider Bugs

Problem:

- Anthropic provider references `urllib` without importing it.

Tasks:

- Add missing imports or replace urllib code with a small shared HTTP helper.
- Add provider unit tests for failure normalization without making real network calls.

Acceptance criteria:

- `build_provider("anthropic", ...)` can execute health-check and mocked request paths.
- Ruff no longer reports undefined names in runtime code.

## Architecture Changes

### Add MeasurementPlan

Create a planning layer before execution.

Responsibilities:

- Decide which tiers can run.
- Explain why each tier is enabled, skipped, or blocked.
- Select fallback order.
- Assign expected confidence.
- Preserve user intent from CLI flags.

Candidate model:

```python
@dataclass
class MeasurementPlan:
    workflow_path: Path
    orchestrator: str
    requested_mode: str
    tiers: list[MeasurementTierAttempt]
    data_strategy: str
    execution_adapter: str
    expected_confidence: str
    limitations: list[str]
```

Acceptance criteria:

- Reports include the plan.
- Tests can assert planning decisions without running measurement.
- Runtime fallback behavior is driven by the plan, not scattered conditionals.

### Add MeasurementResult Model

Create normalized result objects for all tiers.

Responsibilities:

- Standardize success/failure/skipped states.
- Store measurement scope.
- Store raw metrics and derived metrics separately.
- Store confidence and limitations.

Acceptance criteria:

- Static, import, representative, sample, native, and estimate results share a common shape.
- JSON reports are stable and easier to compare.

### Add DataStrategy Interface

Create a clean interface for generated and supplied data.

Strategies:

- `NoDataStrategy`
- `ZeroFillStrategy`
- `FuzzFillStrategy`
- `BranchProbeStrategy`
- `ScaleProbeStrategy`
- `SampleDataStrategy`
- `LLMSyntheticRecipeStrategy`

Acceptance criteria:

- Each strategy has deterministic tests.
- Each strategy records provenance.
- No raw rows are persisted in final reports by default.

### Add ExecutionAdapter Interface

Create a clean interface for runtime execution.

Adapters:

- `RepresentativeAdapter`
- `AirflowNativeAdapter`
- `PrefectNativeAdapter`
- `DagsterNativeAdapter`
- `YamlStaticAdapter`

Acceptance criteria:

- Adapters can be tested independently.
- Each adapter reports tasks executed, tasks skipped, and safety limits.
- Native adapter failure can fall back to representative adapter.

### Add CarbonMeasurement Service

Move CodeCarbon lifecycle into one service.

Responsibilities:

- Start/stop CodeCarbon.
- Normalize CodeCarbon v2/v3 output.
- Apply macOS powermetrics guard.
- Preserve raw values.
- Optionally compute scaled estimates.

Acceptance criteria:

- All measurement tiers use the same carbon wrapper.
- CodeCarbon config is visible in reports.
- Missing CodeCarbon produces a structured `not_available` result.

## Creative Evaluation Without Real Data

When real data is absent, PISCES should use multiple probes rather than only zero-fill.

### Zero-Fill Probe

Purpose:

- Minimal deterministic payload.
- Exercise import and simple callable paths.
- Establish lower-bound runtime overhead.

Limitations:

- May miss branches.
- May understate CPU and IO work.
- May produce unrealistic behavior for filters and aggregations.

### Fuzz-Fill Probe

Purpose:

- Generate deterministic non-zero values from inferred schema.
- Trigger more transformations and conditionals.

Examples:

- Positive and negative numbers.
- Empty and non-empty strings.
- Boolean true and false.
- Date ranges.
- Category distributions.

Limitations:

- Still synthetic.
- May not match production distribution.

### Branch Probe

Purpose:

- Inspect comparisons and conditional statements.
- Generate values likely to exercise both sides of branches.

Examples:

- Threshold values for `if amount > 100`.
- Empty lists for `if not records`.
- Multiple categories for branching logic.

Limitations:

- Best-effort AST inference.
- Hard to guarantee full branch coverage without instrumentation.

### Scale Probe

Purpose:

- Run generated data at multiple sizes.
- Estimate growth rate.

Default sizes:

- 10 rows.
- 100 rows.
- 1,000 rows, bounded by CLI max.

Outputs:

- Duration slope.
- Energy slope when available.
- Nonlinear growth warning.

Limitations:

- Local hardware and short durations can be noisy.
- Requires repeated runs for stable estimates.

### Operator Profile Probe

Purpose:

- Generate representative work based on operator tiers.

Profiles:

- Control: minimal overhead.
- Light compute: Python transform loops.
- Heavy compute: CPU-bound bounded hashing or numeric work.
- IO-bound: local file read/write simulation.
- Polling-heavy: bounded sleep or polling loop simulation.

Limitations:

- Measures a calibrated surrogate, not real operator internals.

## Testing Strategy

### Unit Tests

Coverage targets:

- Orchestrator detection.
- YAML parser behavior.
- Static analyzer metrics.
- Rule engine thresholds.
- Knowledge pack resolution.
- Data strategies.
- Measurement planning.
- Carbon result normalization.
- Provider error normalization.

### Integration Tests

Coverage targets:

- CLI commands from README.
- Source checkout execution with `PYTHONPATH=src`.
- Editable install execution.
- Clean virtual environment install.
- Zero-fill representative runtime.
- Sample-data runtime.
- CodeCarbon missing.
- CodeCarbon available but failing.
- Native adapter fallback.

### Golden Report Tests

Create golden reports for representative workflows.

Rules:

- Use stable timestamps or remove volatile fields before comparison.
- Assert schema shape.
- Assert evidence classification.
- Assert key metrics and rules.
- Assert limitations.

Workflows:

- Airflow simple ETL.
- Airflow sensor-heavy DAG.
- Airflow fan-out DAG.
- Prefect simple flow.
- Prefect mapped flow.
- Dagster asset job.
- Argo simple workflow.
- Argo fan-out workflow.
- Kestra simple flow.
- Kestra high-frequency flow.

### README Command Tests

Every command in the README should be executable or explicitly marked illustrative.

Acceptance criteria:

- CI runs README smoke commands.
- Broken documentation cannot drift silently.

### Quality Gates

Before calling the suite reliable:

- `pytest` passes.
- `ruff check src tests` passes.
- README command smoke tests pass.
- Report schema golden tests pass.
- CLI import smoke tests pass.
- Source and installed package modes both pass.

## CLI Design

Recommended commands:

```bash
pisces-unified workflow.py
pisces-unified workflow.py --mode static
pisces-unified workflow.py --mode import-carbon
pisces-unified workflow.py --mode representative
pisces-unified workflow.py --mode sample --sample-path data.csv
pisces-unified workflow.py --mode native
pisces-unified workflow.py --mode auto
```

Compatibility:

- Keep existing `--energy-mode` temporarily.
- Map old modes to new modes internally.
- Add deprecation warnings only after README and tests are updated.

Important options:

```bash
--orchestrator auto|airflow|prefect|dagster|argo|kestra
--track-carbon
--carbon-country-iso ITA
--carbon-measure-power-secs 1
--carbon-scale-factor 50
--sample-path data.csv
--max-rows 500
--max-tasks 25
--timeout-s 120
--execution-adapter representative|native|auto
--allow-network
--allow-external-effects
--out-dir reports
--stdout
```

CLI acceptance criteria:

- Defaults are safe.
- Destructive or external-effect behavior is opt-in.
- Output report paths are predictable.
- JSON output remains machine-readable.

## Comparator Roadmap

### Version 1: Compare Existing Reports

Inputs:

- Two or more PISCES JSON reports.

Outputs:

- Summary table.
- Rule severity comparison.
- Measured runtime comparison when evidence is comparable.
- Estimate comparison when assumptions are compatible.
- Confidence warnings.

Rules:

- Do not rank high-confidence measured data against low-confidence estimates without warning.
- Surface incomparable measurements clearly.

### Version 2: Batch Evaluate and Compare

Inputs:

- Workflow files or folders.

Behavior:

- Run selected evaluation mode for each workflow.
- Write individual reports.
- Write comparison report.

### Version 3: Scenario Comparison

Inputs:

- Reports plus scenario config.

Scenario fields:

- Runs per day.
- Country/carbon intensity.
- Row volume.
- Scale factor.
- Confidence policy.

## Documentation Roadmap

### README

Fix:

- Default command behavior.
- Carbon tracking semantics.
- Runtime mode limitations.
- Comparator status.
- Native adapter safety model.

Add:

- Evidence tiers table.
- Confidence definitions.
- Example reports for static, representative, sample, and native modes.
- Clear “what PISCES does not claim” section.

### Developer Docs

Add:

- Architecture overview.
- Report schema.
- Adapter interface.
- Data strategy interface.
- Testing guide.
- Release checklist.

### User Guide

Add:

- How to evaluate without data.
- How to provide sample data.
- How to interpret confidence.
- How to compare workflows.
- How to use results in research.

## Phased Delivery Plan

### Phase 1: Make Current Claims True

Goal:

- Fix the current prototype so README and code agree.

Tasks:

- Run heuristic evaluation by default.
- Fix carbon tracking semantics.
- Fix subprocess import path.
- Fix public API exports.
- Fix missing `urllib` import.
- Remove or implement missing comparator entry point.
- Add README command smoke tests.

Done when:

- Existing tests pass.
- Ruff passes for `src`.
- README quick-start commands work.
- No advertised command crashes because of missing modules.

### Phase 2: Evidence-Tier Report Schema

Goal:

- Introduce honest measurement classification.

Tasks:

- Add evidence block.
- Add attempts block.
- Separate raw measurements from estimates.
- Add confidence calculation.
- Add limitations list.
- Update tests and README.

Done when:

- Every report has evidence metadata.
- Static-only reports are clearly labeled.
- Representative reports cannot be mistaken for production runtime.

### Phase 3: Robust Representative Runtime

Goal:

- Make no-data runtime testing useful.

Status:

- Initial implementation complete: `DataStrategy` interface, `zero_fill`, `fuzz_fill`, `scale_probe`, and `operator_profile` modes are available and covered by tests.

Tasks:

- Implement `DataStrategy` interface.
- Keep zero-fill.
- Add fuzz-fill.
- Add scale-probe.
- Add operator-profile representative workloads.
- Add deterministic tests.

Done when:

- Auto mode can produce a representative runtime report without sample data or LLM keys.
- Reports explain generated data strategy and limitations.
- Runtime metrics are normalized.

### Phase 4: Import/Build Carbon Measurement

Goal:

- Make `--track-carbon` meaningful even without runtime execution.

Tasks:

- Add import/build runner.
- Wrap import in CodeCarbon measurement.
- Normalize CodeCarbon status.
- Add raw and scaled outputs.

Done when:

- `--track-carbon` alone produces a real measurement attempt.
- Missing CodeCarbon is reported clearly.
- Carbon settings appear in measurement config.

### Phase 5: Native Adapter Hardening

Goal:

- Execute supported workflows locally where safe.

Tasks:

- Build adapter interface.
- Implement Airflow native adapter.
- Implement Prefect native adapter.
- Implement Dagster native adapter.
- Add mock/safety layer.
- Add fallback to representative mode.

Done when:

- Native mode works on curated examples.
- Unsafe external effects are disabled by default.
- Adapter results include executed/skipped task counts.

### Phase 6: Sample Data Runtime

Goal:

- Improve confidence when users provide real data.

Tasks:

- Strengthen sample validation.
- Add optional schema mapping.
- Add privacy-preserving provenance.
- Add sample-data golden tests.

Done when:

- Sample mode produces medium-confidence runtime reports.
- Raw sample values are not persisted.
- Invalid sample data fails gracefully.

### Phase 7: Calibrated Estimates and Comparator

Goal:

- Make results useful for decision-making and workflow comparison.

Tasks:

- Add per-run and yearly estimates.
- Add uncertainty bands.
- Implement report comparator.
- Add scenario config.

Done when:

- Reports include estimates only when assumptions are explicit.
- Comparator warns about incomparable confidence tiers.
- Batch comparison works on sample reports.

## Risk Register

### Measurement Noise

Risk:

- Short workloads may produce noisy CodeCarbon values.

Mitigation:

- Warmup runs.
- Repeated runs.
- Median and p95 reporting.
- Minimum-duration warnings.
- Normalize by rows and tasks.

### False Precision

Risk:

- Users may over-trust tiny emissions numbers.

Mitigation:

- Confidence labels.
- Limitations block.
- Raw vs estimated separation.
- Significant figures policy.

### Unsafe Native Execution

Risk:

- Workflows may call external APIs, databases, or cloud services.

Mitigation:

- No network by default.
- Mock common clients.
- Subprocess isolation.
- Timeouts.
- Explicit opt-in flags.

### Data Privacy

Risk:

- Sample data may contain sensitive values.

Mitigation:

- Do not persist raw rows.
- Store file hashes and schema summaries only.
- Redact error payloads.
- Document privacy behavior.

### Dependency Drift

Risk:

- Airflow, Prefect, Dagster, CodeCarbon, and provider SDKs change APIs.

Mitigation:

- Knowledge packs.
- Version-aware adapters.
- Matrix tests for supported versions.
- Clear unsupported-version warnings.

### Documentation Drift

Risk:

- README claims diverge from code.

Mitigation:

- README command smoke tests.
- Release checklist.
- Golden report examples.

## Definition of Reliable

PISCES can be called a reliable carbon/runtime evaluation suite when:

- It does not silently overstate what was measured.
- It always classifies evidence tier and confidence.
- It produces useful static results without optional dependencies.
- It produces real carbon measurement attempts when carbon tracking is requested.
- It can test execution creatively without real data using deterministic representative probes.
- It safely handles sample data without persisting raw values.
- It can run native local execution for supported orchestrators when dependencies are available.
- It falls back gracefully when runtime execution is impossible.
- Its documented commands are covered by tests.
- Its report schema is stable enough for comparison and research use.

## Near-Term Implementation Checklist

- [x] Make heuristic evaluation the default.
- [x] Add import/build carbon measurement.
- [x] Pass carbon config through all measurement paths.
- [x] Fix source-checkout runtime subprocess imports.
- [x] Fix public API exports.
- [x] Fix Anthropic provider import bug.
- [x] Implement or remove `pisces-compare`.
- [ ] Add `MeasurementPlan`.
- [x] Add evidence and attempts blocks to reports.
- [x] Add fuzz-fill data strategy.
- [x] Add scale-probe mode.
- [ ] Add README command smoke tests.
- [ ] Add golden report tests.
- [x] Update README to describe evidence tiers and limitations.
