# PISCES

**P**ipeline **I**ntelligent **S**ustainability & **C**arbon **E**valuation **S**uite

A standalone toolkit for evaluating the energy/carbon impact and eco-design quality of LLM-generated workflow code targeting orchestrators like Airflow, Prefect, and Dagster.

## What PISCES Does

PISCES provides a comprehensive suite for sustainability assessment of generated pipeline code:

| Feature | Description |
|---------|-------------|
| **Operational Measurement** | Records wall-clock time, process CPU time, and peak resident memory in a dedicated runner process |
| **Modeled Energy Evidence** | Records CodeCarbon estimates as secondary evidence and identifies the power-source basis |
| **Runtime Evaluation** | Runs bounded native or surrogate workloads with explicit completion and comparison-eligibility status |
| **No-Data Data Generation** | Produces AST-guided zero-fill and deterministic fuzz-fill rows for bounded diagnostic workloads |
| **Eco-Design Heuristics** | Static analysis rules that flag energy-inefficient patterns (poke-mode sensors, sequential chains, high XCom, etc.) |
| **Unified Reports** | Combines all assessments into a single JSON report artifact |
| **DAG Comparator** | Compares sustainability profiles across multiple workflows and orchestrators |

## Installation

Base install:

```bash
pip install pisces
```

With carbon tracking support:

```bash
pip install "pisces[energy]"
```

With CLI enhancements (recommended):

```bash
pip install "pisces[energy,cli]"
```

With LLM synthetic data generation:

```bash
pip install "pisces[llm]"
```

With YAML support (for Argo/Kestra manifests):

```bash
pip install "pisces[energy,cli,yaml]"
```

All optional features:

```bash
pip install "pisces[energy,cli,yaml,llm,dotenv]"
```

## Quick Start

For a complete release demo with sample input data, schema mapping, scenario estimates, and
comparison output, see [demo/README.md](demo/README.md).

### Setup API Keys

Create a `.env` file in the project root:

```bash
# OpenRouter (for LLM synthetic data generation)
OPENROUTER_API_KEY=sk-or-v1-your-key-here

# Optional: site info for OpenRouter rankings
OPENROUTER_HTTP_REFERER=https://github.com/your-repo
OPENROUTER_TITLE=PISCES
```

PISCES auto-loads `.env` via `python-dotenv` if installed.

### Basic Sustainability Evaluation

```bash
# Check local readiness before running measurements
pisces diagnostics path/to/generated_workflow.py

# Write diagnostics to JSON for CI or experiment logs
pisces-diagnostics path/to/generated_workflow.py --out ./diagnostics/readiness.json
```

```bash
# Evaluate a workflow with eco-design heuristics only
pisces-unified path/to/generated_workflow.py

# Specify orchestrator explicitly (otherwise auto-detected)
pisces-unified path/to/pipeline.py --orchestrator airflow
```

### Carbon Tracking

```bash
# Track carbon emissions during workflow import
pisces-unified path/to/pipeline.py \
  --track-carbon \
  --carbon-country-iso ITA

# Keep raw per-run output unscaled; place repeated-use assumptions in a scenario
pisces-unified path/to/pipeline.py \
  --track-carbon \
  --carbon-country-iso GBR
```

### Scenario Estimates

Calibrated estimates are disabled unless you provide explicit scenario assumptions. Estimates are
stored under `energy.measurements.estimate` and never replace raw measured values.

```bash
# Project one measured runtime into an explicit annual scenario
pisces-unified path/to/pipeline.py \
  --enable-energy-eval \
  --energy-mode sample \
  --energy-sample-path ./my_data.csv \
  --estimate-scenario-name daily-production \
  --estimate-runs-per-year 365 \
  --estimate-years 1 \
  --estimate-repetitions-per-run 1

# Or load the scenario from JSON:
# {"name": "daily-production", "runs_per_year": 365, "years": 1, "repetitions_per_run": 1}
pisces-unified path/to/pipeline.py \
  --enable-energy-eval \
  --energy-mode sample \
  --energy-sample-path ./my_data.csv \
  --estimate-scenario-file ./scenario.json
```

Each estimate includes assumptions, a scenario hash, per-run/yearly/period totals, the measured
baseline used, and uncertainty bands derived from the evidence tier.

### Runtime Energy Evaluation

```bash
# Auto-mode: sample if provided, otherwise fuzz_fill → zero_fill → synthetic → heuristic
pisces-unified path/to/pipeline.py \
  --enable-energy-eval \
  --energy-mode auto

# Zero-fill mode: AST-inferred trivial data, with no LLM call
pisces-unified path/to/pipeline.py \
  --track-carbon \
  --enable-energy-eval \
  --energy-mode zero_fill

# Fuzz-fill mode: AST-inferred deterministic non-zero data, with no LLM call
pisces-unified path/to/pipeline.py \
  --track-carbon \
  --enable-energy-eval \
  --energy-mode fuzz_fill

# Scale-probe mode: runs generated workloads at multiple row counts
# to estimate growth trends under bounded local execution.
pisces-unified path/to/pipeline.py \
  --enable-energy-eval \
  --energy-mode scale_probe \
  --energy-max-rows 1000

# Operator-profile mode: uses static operator tiers to weight
# a bounded representative workload.
pisces-unified path/to/pipeline.py \
  --enable-energy-eval \
  --energy-mode operator_profile

# Sample mode: provide real data files
pisces-unified path/to/pipeline.py \
  --enable-energy-eval \
  --energy-mode sample \
  --energy-sample-path ./my_data.csv

# Sample mode with schema mapping into canonical runtime columns
pisces-unified path/to/pipeline.py \
  --enable-energy-eval \
  --energy-mode sample \
  --energy-sample-path ./orders.csv \
  --energy-sample-map orderIdentifier=order_id \
  --energy-sample-map totalAmount=amount

# Or provide a JSON object mapping file: {"sourceColumn": "target_column"}
pisces-unified path/to/pipeline.py \
  --enable-energy-eval \
  --energy-mode sample \
  --energy-sample-path ./orders.csv \
  --energy-sample-map-file ./sample_schema_map.json

# Synthetic mode: LLM generates representative data
# (defaults: OpenRouter + openai/gpt-oss-120b — set OPENROUTER_API_KEY in .env)
pisces-unified path/to/pipeline.py \
  --enable-energy-eval \
  --energy-mode synthetic

# Synthetic with custom model/provider
pisces-unified path/to/pipeline.py \
  --enable-energy-eval \
  --energy-mode synthetic \
  --llm-provider openrouter \
  --llm-model openai/gpt-oss-120b

# Force a fresh provider call instead of reusing a cached recipe
pisces-unified path/to/pipeline.py \
  --enable-energy-eval \
  --energy-mode synthetic \
  --synthetic-cache refresh

# Disable synthetic recipe cache reads and writes
pisces-unified path/to/pipeline.py \
  --enable-energy-eval \
  --energy-mode synthetic \
  --synthetic-cache off
```

Synthetic recipe caching defaults to `use`. Cache keys include the minimized workflow signature,
provider, model, seed, max rows, and recipe schema version. Use `refresh` for live API validation
runs, and `off` when an experiment must avoid writing cache artifacts.

For controlled studies, use an explicit mode, native execution, a stable measurement
window, and an exact output path:

```bash
pisces-unified path/to/pipeline.py \
  --enable-energy-eval \
  --energy-mode sample \
  --energy-sample-path ./frozen_fixture.csv \
  --energy-execution-adapter native \
  --energy-runtime-python /path/to/airflow-2.8.1/bin/python \
  --experiment-mode \
  --workload-contract ./contracts/pipeline_v1.json \
  --artifact-binding ./bindings/artifact.json \
  --energy-minimum-duration-s 30 \
  --energy-minimum-repetitions 3 \
  --energy-maximum-repetitions 1000 \
  --out-file ./reports/assigned_run_id.json
```

Strict experiment mode disables automatic surrogate fallback. A failed, zero-work, or
partial native execution has no primary resource measurement. Its counters remain under
`attempt_diagnostics`; they are not encoded as numerical zero.

The Airflow adapter executes the full DAG through `DAG.test()`. Contract-bound
runs use temporary scoped storage, registered HTTP and FTP fixtures, expected-task
coverage, required-input consumption checks, and output assertions. Declared
branch alternatives may skip the unselected path. The Airflow metadata
database is initialized before measurement. Task retries are disabled for the
first-pass outcome and their declared values remain in report metadata. A
separate pairwise output-equivalence check is required before resource
comparison. Prefect and Dagster still require corresponding contract-aware
adapters before confirmatory paired use.

### Combined: Full Assessment

```bash
# Full assessment with defaults (OpenRouter + gpt-oss-120b for synthetic)
pisces-unified path/to/pipeline.py \
  --orchestrator dagster \
  --track-carbon \
  --carbon-country-iso ITA \
  --carbon-scale-factor 50 \
  --enable-energy-eval \
  --energy-mode auto \
  --out-dir ./sustainability_reports
```

### Interactive CLI

```bash
# Guided command builder walks you through all options
pisces agent

# Quick evaluate command
pisces evaluate path/to/pipeline.py --track-carbon --stdout
```

## Output: Unified Report

Each evaluation produces a structured JSON report:

```json
{
  "report_schema_version": "1.2.0",
  "tool": "pisces-unified",
  "timestamp": "2026-06-16T12:00:00Z",
  "workflow": {
    "path": "/path/to/workflow.py",
    "orchestrator": "airflow",
    "runtime_supported": true
  },
  "carbon_tracking": {
    "enabled": true,
    "country_iso": "ITA",
    "scale_factor": 50.0
  },
  "energy": {
    "schema_version": "1.2.0",
    "mode_used": "sample",
    "evidence": {
      "measurement_kind": "native_runtime",
      "confidence": "medium",
      "production_equivalent": false,
      "data_source": "sample",
      "execution_adapter": "native",
      "measurement_scope": "native_local_runtime",
      "limitations": [
        "Wall-clock time, process CPU time, and peak resident memory are the primary operational measurements."
      ]
    },
    "attempts": [
      {
        "tier": "sample_runtime",
        "mode": "sample",
        "status": "success",
        "reason": "ok"
      }
    ],
    "measurements": {
      "import_build": null,
      "runtime": {
        "execution_outcome": "completed",
        "eligible_for_resource_comparison": true,
        "eligible_for_paired_equivalence_comparison": false,
        "operational_metrics": {
          "evidence_role": "primary",
          "wall_clock_s": 30.4,
          "process_cpu_s": 21.8,
          "peak_rss_mb": 412.3
        },
        "modeled_energy": {
          "evidence_role": "secondary",
          "interpretation": "modeled_energy",
          "hardware_power_source_verified": false
        },
        "energy_profile": {
          "energy_consumed_kwh": 0.00042,
          "emissions_kgco2eq": 0.00012
        }
      },
      "estimate": null
    },
    "heuristic": {
      "rules": [
        {
          "rule_id": "SENSOR_POKE_MODE",
          "severity": "major",
          "title": "Poke-mode sensor detected",
          "recommendation": "For Airflow sensors: set mode='reschedule'..."
        }
      ]
    },
    "runtime_measurement": { "...": "legacy-compatible runtime block" }
  }
}
```

### Evidence Tiers and Confidence

PISCES labels every result by evidence strength so reports do not overstate what was measured:

| Evidence kind | Confidence | Meaning |
|---------------|------------|---------|
| `static` | Low | Static heuristic analysis only; no workflow execution or hardware measurement |
| `import_build` | Low | Workflow import/DAG-build measurement only; task runtime is not included |
| `surrogate_benchmark` | Low | PISCES diagnostic workload; it does not execute the workflow and is ineligible for route comparison |
| `sample_runtime` | Medium | Legacy evidence label for bounded sample-data runs |
| `native_runtime` | Medium | Complete local native execution; paired equivalence still requires output assertions |
| `native_runtime` with verified equivalent output | High | Complete native execution of the frozen workload contract with passing output assertions |

`production_equivalent` is `false` by default. PISCES separates raw measurements from scaled estimates, and limitations are included in the `evidence.limitations` list.

### Comparing Reports

```bash
pisces-compare ./reports/*.json --out ./reports/comparison.json
```

The comparator ranks workflows by static rule risk, measured runtime energy when present, and
estimated yearly emissions when reports contain calibrated estimates. It warns when reports mix
confidence tiers or use incompatible scenario assumptions.

## Eco-Design Rules

PISCES ships with static heuristic rules that flag sustainability concerns **without running the workflow**:

| Rule ID | Severity | What It Detects |
|---------|----------|-----------------|
| `SENSOR_POKE_MODE` | Major | Poke-mode sensors wasting resources |
| `DEEP_SEQUENTIAL_CHAIN` | Minor | Limited parallelism increasing wall-clock |
| `HIGH_XCOM_INTENSITY` | Minor | Excessive metadata DB traffic |
| `HIGH_FREQUENCY_HEAVY_COMPUTE` | Major | Hourly-or-faster heavy compute |
| `LARGE_FAN_OUT` | Info | Scheduling overhead from large fan-out |
| `NO_DEPENDENCIES_DETECTED` | Minor | Missing dependency edges |
| `SPEC_CODE_EDGE_MISMATCH` | Info | OPOS vs code graph divergence |

## Project Structure

```text
PISCES/
├── pyproject.toml
├── README.md
├── LICENSE
├── demo/                       # Release demo inputs and commands
├── src/pisces/
│   ├── __init__.py
│   ├── unified_evaluator.py       # Main unified evaluation entry point
│   ├── cli.py                     # Interactive CLI
│   ├── dag_comparator.py          # Batch comparison reports
│   ├── diagnostics.py             # Environment readiness checks
│   ├── base_evaluator.py          # Core types (Orchestrator, Severity, Issue)
│   ├── subprocess_json_runner.py  # Subprocess management for venvs
│   ├── knowledge_pack.py          # Version-aware capability resolution
│   ├── knowledge_packs/
│   │   └── default_pack_v1.json   # Bundled knowledge pack
│   └── energy/
│       ├── __init__.py
│       ├── carbon_tracker.py      # CodeCarbon wrapper
│       ├── runtime_evaluation.py  # Main energy evaluation engine
│       ├── runtime_runner_cli.py  # Subprocess workload runner
│       ├── import_runner_cli.py   # Import/build carbon runner
│       ├── data_strategies.py     # No-data runtime strategies
│       ├── adapters/              # Native Airflow/Prefect/Dagster adapters
│       ├── structural_energy_analyzer.py  # Static code structure analysis (Python)
│       ├── structural_yaml_analyzer.py    # Static manifest analysis (Argo, Kestra)
│       ├── design_rules.py                # Eco-design heuristic rule engine
│       └── zero_fill.py                   # AST-powered trivial data generator (no LLM)
└── tests/
    ├── __init__.py
    └── test_runtime_evaluation.py
```

## Concepts

### Energy Evaluation Modes

```
 ┌────────────┐   no deps   ┌──────────┐   needs data   ┌──────────┐   needs LLM   ┌───────────┐
 │fuzz_fill   │────────────▶│zero_fill │───────────────▶│synthetic │──────────────▶│ heuristic │
 │ AST-infer  │             │ AST-infer│                │ LLM-gen  │               │  static   │
 │ non-zero   │             │ all zero │                │   data   │               │  rules    │
 └────────────┘             └──────────┘                └──────────┘               └───────────┘
      ↓                          ↓                          ↓                          ↓
 executes code             executes code              executes code                no runtime
 no LLM needed             needs data files           needs API key               always works
```

- **`auto`** — Uses sample data first if supplied; otherwise cascades through fuzz_fill → zero_fill → synthetic → heuristic
- **`zero_fill`** — AST-powered trivial data (all zeros/empty strings) for a bounded diagnostic workload. **No LLM or data files needed.**
- **`fuzz_fill`** — AST-powered deterministic non-zero data for a bounded diagnostic workload. **No LLM or data files needed.**
- **`scale_probe`** — Runs deterministic generated workloads at multiple row counts to estimate runtime growth trends.
- **`operator_profile`** — Uses static operator-tier signals to scale a bounded representative workload.
- **`sample`** — Uses provided CSV/JSON/JSONL data to exercise the workflow
- **`synthetic`** — Uses an LLM to generate representative synthetic test data
- **`heuristic`** — Static eco-design rules only (no runtime measurement needed)

Sample mode persists only privacy-preserving metadata in reports: file hashes, row counts,
column-name hashes, type counts, mapping summaries, and validation status. Raw sample rows and
raw cell values are passed only to the bounded runtime subprocess and are not written into the
PISCES report.

### Execution Adapters

- **`surrogate_benchmark`** — Target-neutral PISCES diagnostic workload; it is not workflow execution
- **`representative`** — Deprecated alias for `surrogate_benchmark`
- **`native`** — Loads and executes supported workflow objects through a guarded local adapter
- **`auto`** — Tries native first, then uses the surrogate benchmark outside strict experiment mode

Native execution is intentionally conservative. PISCES runs native adapters under a safety guard
that blocks unregistered network calls, subprocess creation, and file writes outside approved
temporary roots. Registered contract fixtures serve approved external reads. Strict experiment mode
marks any failed or skipped task run ineligible. Native reports include
`native_tasks_executed`, `native_tasks_skipped`, per-task status, and the active safety policy.

### Carbon Scale Factor

Keep `--carbon-scale-factor` at `1.0` for experimental raw records. Express repeated
execution through an explicit scenario so measured and projected quantities remain separate.

## Troubleshooting

### macOS Password Prompt (`Password:`)

When running with `--track-carbon` on Apple Silicon Macs, PISCES may trigger a
system `Password:` prompt. This comes from CodeCarbon's powermetrics capability
check, which runs `sudo powermetrics` to detect Apple hardware power metrics.

**PISCES disables this check by default** via the `PISCES_SKIP_POWERMETRICS`
environment variable (defaults to `1`). CodeCarbon then uses a TDP-based model.
PISCES labels this output as secondary modeled-energy evidence. Wall-clock time,
process CPU time, and peak resident memory are the primary Mac measurements.

If you need hardware-level power metrics, configure passwordless sudo for
powermetrics (see [CodeCarbon docs](https://docs.codecarbon.io/latest/explanation/methodology/#power-usage))
and then re-enable the check:

```bash
PISCES_SKIP_POWERMETRICS=0 pisces-unified ... --track-carbon
```

## Development

```bash
# Clone and install in editable mode with dev dependencies
pip install -e ".[dev,energy,cli]"

# Run tests
pytest tests/

# Run a single test
pytest tests/test_runtime_evaluation.py -v

# Run the release demo end to end
python -m pisces.diagnostics test_scripts/airflow/simple_etl.py --out demo/output/readiness.json
python -m pisces.unified_evaluator test_scripts/airflow/simple_etl.py \
  --enable-energy-eval \
  --energy-mode sample \
  --energy-sample-path demo/input/weather_observations.csv \
  --energy-sample-map-file demo/input/sample_schema_map.json \
  --estimate-scenario-file demo/input/scenario_daily.json \
  --out-dir demo/output
```

## Citation

If you use PISCES in your research, please cite it as:

```
@software{pisces2026,
  author = {Abubakari, Alidu},
  title  = {PISCES: Pipeline Intelligent Sustainability & Carbon Evaluation Suite},
  year   = {2026},
  url    = {https://github.com/aliduabubakari/pisces}
}
```
