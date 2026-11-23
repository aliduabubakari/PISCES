# CLI Reference

The CLI calls the same Python evaluation core used by the HTTP service. It does
not need Node.js or the frontend. Run commands from the repository root after
installing this checkout. This guide describes the current source, not an
assumed PyPI release.

## Install

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[web,dev,yaml]'
pisces --help
```

Python 3.10+ is declared; 3.12 is the tested container runtime. Core evaluation
does not require optional engine packages on the host when using OCI images.

| Extra | Purpose |
| --- | --- |
| `web` | FastAPI/Uvicorn report API and service dependencies |
| `dev` | Pytest, Ruff and the HTTP test client |
| `yaml` | Argo/Kestra YAML input parsing; not native execution |
| `energy` | CodeCarbon software estimates; never a hardware power measurement |
| `llm` | Opt-in OpenRouter-compatible synthetic recipe generation |
| `dotenv` | Optional `.env` loading |
| `cli` | Optional terminal dependencies; the main CLI itself uses argparse |

## Command Map

| Command | Purpose |
| --- | --- |
| `pisces evaluate WORKFLOW` | Static analysis; optional runtime/diagnostic evidence |
| `pisces diagnostics [WORKFLOW]` | Dependency, model and optional OCI readiness JSON |
| `pisces report-view REPORT` | Export a validated portable report view |
| `pisces campaign-view DIRECTORY` | Export an existing supported campaign bundle |
| `pisces serve-api` | Loopback report API; alias `serve-reports` |
| `pisces agent` | Interactive command builder with confirmation before evaluation |
| `pisces-unified WORKFLOW` | Direct evaluator, including explicit `--orchestrator` |
| `pisces-compare REPORT...` | Report inventory and static rule comparison |
| `pisces-diagnostics` | Direct diagnostics command |
| `pisces-benchmarks` | Generate/run the five-family benchmark suite |
| `pisces-energy` | Low-level worker runner; not the recommended user entry point |

Every command has `--help`. The committed [generated help](reference/cli-help.md)
lists **every current flag and default**, including lower-level entry points.

## Select The Evidence You Need

| Path | How to request it | What it establishes |
| --- | --- | --- |
| Static | Default `evaluate` | Design findings without running workflow code |
| Surrogate | `--enable-energy-eval --energy-mode fuzz_fill` | Bounded diagnostic work, not original pipeline execution |
| Sample diagnostic | `--energy-mode sample` plus sample paths/map | Validated sample-driven surrogate, not native engine execution |
| Native Linux OCI | `--energy-execution-adapter oci`, immutable image, v2 contract and binding | Approved engine execution, scoped counters and contract-bound output checks |
| Host native/representative | Explicit adapter and supporting contract/profile | Specialized trusted local experiments; not container isolation |

Do not execute untrusted artifacts through host adapters. Default surrogate
diagnostics do not import/execute the supplied pipeline. Synthetic recipe
generation is data preparation, not evidence of equivalent pipeline work.

### Static Report

```bash
pisces evaluate test_scripts/airflow/simple_etl.py \
  --out-file reports/static.json

# YAML inputs use the direct evaluator's explicit orchestrator option.
pisces-unified test_scripts/argo/simple_etl.yaml \
  --orchestrator argo --out-file reports/argo.json
```

With no `--out-file`, a timestamped report goes to `pisces_reports/` beside the
workflow, or `--out-dir`. Parents are created. Explicit `--out-file` can overwrite
an existing report; use fresh paths for experiments. `--stdout` prints the JSON.

### Deterministic No-Data Diagnostic

```bash
pisces evaluate test_scripts/airflow/simple_etl.py \
  --enable-energy-eval --energy-mode fuzz_fill --energy-seed 1729 \
  --energy-max-rows 500 --energy-timeout-s 60 \
  --out-file reports/fuzz-fill.json
```

Available modes include `auto`, `zero_fill`, `fuzz_fill`, `scale_probe`,
`operator_profile`, `sample`, `synthetic` and `heuristic`. These names select
diagnostic strategies; none alone requests native execution. `auto` is not a
permission to silently claim native engine evidence.

For sample validation and schema mapping:

```bash
pisces evaluate test_scripts/airflow/simple_etl.py \
  --enable-energy-eval --energy-mode sample \
  --energy-sample-path demo/input/weather_observations.csv \
  --energy-sample-map-file demo/input/sample_schema_map.json \
  --out-file reports/sample.json
```

`--energy-sample-path` may repeat. Mappings also accept repeated
`--energy-sample-map SOURCE=TARGET`. Reports retain hashes/aggregate provenance,
not raw sample rows. This does not make the uploaded dataset anonymous: source,
paths and metadata can still be sensitive, and native outputs can contain data.
Review artifacts before sharing them.

### Native OCI Run With Verified Outputs

Requires a trusted local Docker Linux engine, cgroup v2 and the enforced
isolation profile. Build/pull approval is separate; execution will not pull a
missing image or accept a mutable image tag.

```bash
docker build -f containers/airflow/Dockerfile -t pisces-airflow-oci:local .
IMAGE="$(docker image inspect --format '{{.Id}}' pisces-airflow-oci:local)"
pisces diagnostics --oci-image "$IMAGE"

python -m benchmarks.climate_fusion.benchmark benchmark-runs/climate \
  --rows-per-source 10000

pisces-unified benchmark-runs/climate/baseline.py \
  --orchestrator airflow --enable-energy-eval \
  --energy-execution-adapter oci --oci-image "$IMAGE" \
  --energy-maximum-repetitions 1 \
  --workload-contract benchmark-runs/climate/contract.json \
  --artifact-binding benchmark-runs/climate/baseline.binding.json \
  --out-file reports/native.json
```

Contract v2 binds the intended work, inputs, output oracle and artifact hash.
Native/OCI v2 runs require a maximum of one execution per attempt; repetitions
belong in the controlled campaign runner, not an unreported inner loop. Use
fresh directories. Prefect/Dagster require their own approved image and generated
script, not an Airflow artifact relabeled as another engine.

See [OCI runtime](OCI_RUNTIME.md), [contract v2](WORKLOAD_CONTRACT_V2.md),
[engine qualification](ENGINE_QUALIFICATION.md) and
[benchmark suite](BENCHMARK_SUITE.md). A standalone run remains ineligible for
resource ranking even when outputs pass and counters are available.

### Optional Modeled Energy And Recipes

Install `.[energy]` before using `--track-carbon`. Set an explicit country with
`--carbon-country-iso` where needed. `--carbon-measure-power-secs` is a polling
interval, not a promise of precision. CodeCarbon's source/assumptions must be
inspected; short workloads can be below useful model resolution. Do not treat a
process estimate as whole-host or task energy. The OCI/service profiles do not
automatically enable this model.

`--estimate-scenario-file` (or `--estimate-runs-per-year` with its documented
parameters) requests a projection; it does not override eligibility. Surrogates
explicitly refuse workflow annualization. See [the demo](../demo/README.md).

For opt-in `--energy-mode synthetic`, install `.[llm]`, configure the API-key
environment variable named by `--llm-api-key-env`, and choose provider/model
explicitly when appropriate. LLM calls may incur cost and disclose the selected
workflow/schema context. Never put secrets or private records into prompts.

`--synthetic-cache use` reuses admitted cached recipes, `refresh` regenerates,
and `off` avoids cache reuse/writes. Cache policy does not alter oracle or
measurement qualification. Deterministic fuzz-fill needs no LLM credential.

## Reports And Comparisons

```bash
pisces report-view reports/native.json --out reports/native.view.json
pisces campaign-view benchmark-runs/results --pipeline "Suite" \
  --out reports/suite.view.json
pisces-compare reports/static.json reports/sample.json --out reports/comparison.json
```

View exporters refuse existing output files and require an existing output
parent. They admit evidence and do not execute code or fetch report-supplied
filesystem paths. Report imports are bounded to 8 MiB. Supported campaign
directories must contain the expected manifest/summary files; arbitrary folders
of JSON reports are not a campaign.

The generic comparator ranks **static rule findings**, not CPU, memory, energy
or carbon. Different native profiles, environments, input identities or
surrogate workloads must not be pooled as equivalent work. Use the controlled
campaign tools and recorded qualification evidence instead.

## Diagnostics And Automation

```bash
pisces diagnostics test_scripts/airflow/simple_etl.py \
  --skip-codecarbon-smoke --out reports/readiness.json
pisces serve-api --port 8000
```

Diagnostics always prints JSON and optionally writes it. Missing dependencies
can be reported without a nonzero process status: **inspect readiness fields**,
not just `$?`. OCI readiness probes images/isolation/counters; it does not
execute your workflow or establish telemetry conformance. The default CodeCarbon
smoke check can be skipped, as above.

`serve-api` binds loopback only; it is not an implicit job worker. See the
[HTTP API](API.md) and [service deployment](EXECUTION_SERVICE.md) for execution.
Avoid serving sensitive imported evidence from an unauthenticated shared host.

| Outcome | CLI behavior |
| --- | --- |
| `evaluate` succeeds | Exit 0; inspect output verdict and eligibility separately |
| Evaluator/runtime reports failure | Exit 2; a diagnostic/failure JSON may still be written |
| Invalid arguments/admission | Nonzero (argparse usage errors use 2) |
| Diagnostics has unavailable capabilities | May exit 0; inspect returned JSON |

Other low-level/campaign tools have command-specific exits. Do not interpret
exit 0, `runtime.status`, output validity and resource-comparison eligibility as
the same signal. Keep canonical report JSON alongside experiment metadata.
