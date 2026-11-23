# Documentation

Start with the [project README](../README.md) for the current support boundary
and a choice of browser, CLI or HTTP workflows.

## Use And Integrate

| Guide | Purpose |
| --- | --- |
| [CLI reference](CLI_REFERENCE.md) | Installation, modes, examples, exit codes and full generated help |
| [HTTP API](API.md) | Authentication, submissions, polling, reports, schemas, errors and limits |
| [Execution service](EXECUTION_SERVICE.md) | Compose prod/dev/test, native profiles, security and persistence |
| [Workspace](FRONTEND_WORKSPACE.md) | Local frontend, imports, API connection and campaign charts |
| [Release demo](../demo/README.md) | Small committed data samples and explicit diagnostic limitations |
| [OpenAPI snapshot](reference/openapi.json) | Machine-readable API routes and transport schemas |
| [Generated CLI help](reference/cli-help.md) | Exact flags exported from the installed source |

## Evidence And Validation

| Guide | Purpose |
| --- | --- |
| [Controlled experiments](CONTROLLED_EXPERIMENT_PROTOCOL.md) | Measurement scope, repeats, comparisons and refusal conditions |
| [Workload contract v2](WORKLOAD_CONTRACT_V2.md) | Intent, bindings, fixtures and output equivalence |
| [OCI runtime](OCI_RUNTIME.md) | Linux container execution and telemetry boundary |
| [Native Linux telemetry](NATIVE_LINUX_TELEMETRY.md) | Counter semantics and environment qualification |
| [Airflow 3 runtime](AIRFLOW_3_RUNTIME.md) | Bounded Airflow execution and version assumptions |
| [Engine qualification](ENGINE_QUALIFICATION.md) | Separate Airflow/Prefect/Dagster success and failure evidence |
| [Benchmark suite](BENCHMARK_SUITE.md) | Five pipeline families and four output cases |
| [Climate Fusion](../benchmarks/climate_fusion/README.md) | Paired experiments, load sweeps and session diagnostics |

## Design And Roadmaps

The guides above describe current behavior. Roadmap items are **not** support
claims; in particular general per-task resource telemetry remains future work.

- [Robustness roadmap](PISCES_ROBUSTNESS_PLAN.md)
- [Platform extension roadmap](PLATFORM_EXTENSION_ROADMAP.md)
- [Frontend design](FRONTEND_DESIGN.md)
- [Original engineering plan](../plan.md)
- [Brand and screenshot assets](../assets/README.md)

After CLI/API/schema changes, run `python scripts/export_reference_docs.py` and
commit the updated reference files. `--check` detects drift without rewriting.
