# PISCES

**P**ipeline **I**ntelligent **S**ustainability & **C**arbon **E**valuation **S**uite

A standalone toolkit for evaluating the energy/carbon impact and eco-design quality of LLM-generated workflow code targeting orchestrators like Airflow, Prefect, and Dagster.

## What PISCES Does

PISCES provides a comprehensive suite for sustainability assessment of generated pipeline code:

| Feature | Description |
|---------|-------------|
| **Carbon Tracking** | Measures CO₂ emissions during workflow import/execution using [CodeCarbon](https://codecarbon.io/) |
| **Runtime Energy Evaluation** | Runs workflows with zero-fill, sample, or synthetic data to measure real energy consumption |
| **Zero-Fill Data Generation** | AST-powered trivial data generator — no LLM needed, exercises real code paths for baseline energy measurement |
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

# Apply a scale factor for more conservative estimates
pisces-unified path/to/pipeline.py \
  --track-carbon \
  --carbon-country-iso GBR \
  --carbon-scale-factor 50
```

### Runtime Energy Evaluation

```bash
# Auto-mode: cascade zero_fill → sample → synthetic → heuristic
pisces-unified path/to/pipeline.py \
  --enable-energy-eval \
  --energy-mode auto

# Zero-fill mode: AST-inferred trivial data — NO LLM needed!
# Analyses pipeline code to infer input schemas, generates all-zero data,
# and measures actual energy/carbon during real code execution.
pisces-unified path/to/pipeline.py \
  --track-carbon \
  --enable-energy-eval \
  --energy-mode zero_fill

# Sample mode: provide real data files
pisces-unified path/to/pipeline.py \
  --enable-energy-eval \
  --energy-mode sample \
  --energy-sample-path ./my_data.csv

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
```

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
  "report_schema_version": "1.0.0",
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
    "mode_used": "sample",
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
    "runtime_measurement": {
      "energy_profile": {
        "energy_consumed_kwh": 0.00042,
        "emissions_kgco2eq": 0.00012
      }
    }
  }
}
```

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
├── src/pisces/
│   ├── __init__.py
│   ├── unified_evaluator.py       # Main unified evaluation entry point
│   ├── cli.py                     # Interactive CLI
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
 │ zero_fill  │────────────▶│  sample  │───────────────▶│synthetic │──────────────▶│ heuristic │
 │ AST-infer  │             │ real CSV │                │ LLM-gen  │               │  static   │
 │ all zeros  │             │ or JSON  │                │   data   │               │  rules    │
 └────────────┘             └──────────┘                └──────────┘               └───────────┘
      ↓                          ↓                          ↓                          ↓
 executes code             executes code              executes code                no runtime
 no LLM needed             needs data files           needs API key               always works
```

- **`auto`** — Cascades through zero_fill → sample → synthetic → heuristic
- **`zero_fill`** — AST-powered trivial data (all zeros/empty strings). Exercises real code paths for baseline measurement. **No LLM, no data files needed.**
- **`sample`** — Uses provided CSV/JSON/JSONL data to exercise the workflow
- **`synthetic`** — Uses an LLM to generate representative synthetic test data
- **`heuristic`** — Static eco-design rules only (no runtime measurement needed)

### Execution Adapters

- **`representative`** — Bounded synthetic workload (safe, no orchestrator import required)
- **`native`** — Loads and executes the workflow natively in the orchestration framework
- **`auto`** — Tries native first, falls back to representative

### Carbon Scale Factor

Amplifies measured carbon values to estimate the impact of repeated execution. For example, `--carbon-scale-factor 50` would multiply the per-run emissions by 50 — useful when a DAG runs thousands of times.

## Troubleshooting

### macOS Password Prompt (`Password:`)

When running with `--track-carbon` on Apple Silicon Macs, PISCES may trigger a
system `Password:` prompt. This comes from CodeCarbon's powermetrics capability
check, which runs `sudo powermetrics` to detect Apple hardware power metrics.

**PISCES disables this check by default** via the `PISCES_SKIP_POWERMETRICS`
environment variable (defaults to `1`). CodeCarbon falls back to TDP-based
estimation, which requires no elevated privileges.

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
