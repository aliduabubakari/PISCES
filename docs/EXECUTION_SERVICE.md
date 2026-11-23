# PISCES execution service

## Scope and Architecture

The CLI remains a first-class entry point. The browser is another client of the
same Python evaluator and versioned JSON reports, not a separate evaluation
implementation. Existing library entry points are unchanged; no new library
facade is required for this deployment.

```text
Browser -> frontend (Vite in dev, Nginx in prod) -> FastAPI
                                                    |
                                             SQLite job queue
                                                    |
                                              worker controller
                                                    |
                                       disposable evaluation container
```

- `pisces.web`: factory, security, report admission and job routes.
- `pisces.service`: typed submissions, durable queue, worker and sandbox entry point.
- `frontend/src/api`: API client and schema-generated types; credentials in memory only.
- `frontend/src/features/evaluations`: upload, readiness, job history and report handoff.

The API **never imports or executes uploaded source** and has no Docker socket.
The worker alone has Docker access. Source, contracts and fixtures enter each
disposable container through bounded stdin, not host-path mounts. This works
with a local Linux engine or Docker Desktop without translating host paths into
paths inside the API container. Reports retain source hashes and controller-added
image IDs and environment classification. They are **not signed or attested**.

Unlike the CLI's `/input` read-only mount, uploaded bundles are private temporary
files inside the sandbox. They are made non-writable and hash-checked before
and after execution, but their owning UID could change permissions. Native
reports explicitly label `input_delivery: private_stdin_bundle` and include a
warning; this is a distinct trusted-workload profile, not proof against hostile
input tampering. No read-only input-mount or inherited campaign qualification
claim is made. The same native runner, task evidence and output oracle are reused.

## Start the Stack

Requirements: Docker Engine/Desktop with Linux containers, cgroup v2 and default
seccomp; recent Compose v2+ (`!reset` support requires 2.24.4+). Docker must be
reachable through a local Unix socket. Windows users need a Linux/WSL2 Docker
environment; native Windows execution is not supported.

From the repository root:

```bash
python3 scripts/prepare_compose.py
docker compose -f compose.yaml -f compose.prod.yaml up --build -d
docker compose ps
```

Open <http://127.0.0.1:8080/>. Open **Deployment connection**, enter the token from
`.secrets/api-token`, and select **Connect API**. **Runs** shows retained jobs;
polling continues across all screens. Choose **New evaluation** and complete
Script, Inputs, Settings and Review, then explicitly authorize submission.
The job page opens its report when available. Source inspection is an optional
pane; output verdicts, scoped metrics and task states remain separate. A report
is downloadable as the original JSON.

Run, report, tab and selected-task navigation use URL fragments. Reloading a
deployment job URL requires reconnecting but retrieves the saved report again.
Imported reports, source attachments and credentials are not saved to browser
storage or placed in URLs. Closing a report copy does not delete its backend job.

The token is never included in an image or browser storage. The preparation
script creates its parent directory with mode `0700` and a file with mode `0644`
so the non-root container can read a local Compose bind-backed secret. The
private parent directory is essential on the host. Do not commit or log tokens.
Use an operator-managed secret path with `PISCES_API_TOKEN_FILE` if preferred.

The shared named volume stores uploaded source, fixture contents and reports.
`docker compose down` retains it. Deleting a terminal job removes its logical
records; SQLite/WAL and backups are **not secure erasure**. Apply host disk
encryption and your own backup/retention policy. Never run `down --volumes`
against a deployment whose jobs you need to keep.

## Development and Tests

```bash
# Vite source reload; API reload; restart worker after controller changes.
docker compose -f compose.yaml -f compose.dev.yaml up --build -d

# Separate test volume and a deliberately public test-only token.
PISCES_HTTP_PORT=18080 docker compose -p pisces-test \
  -f compose.yaml -f compose.test.yaml up --build -d api worker frontend
PISCES_HTTP_PORT=18080 docker compose -p pisces-test \
  -f compose.yaml -f compose.test.yaml build tests
PISCES_HTTP_PORT=18080 docker compose -p pisces-test \
  -f compose.yaml -f compose.test.yaml run --no-deps --rm tests

# Python tests inside the service image (optional integration tests stay opt-in).
docker compose -p pisces-test -f compose.yaml -f compose.test.yaml \
  run --no-deps --rm tests python -m pytest -q

# Remove ONLY the disposable test deployment, including its data.
docker compose -p pisces-test -f compose.yaml -f compose.test.yaml down --volumes
```

The smoke test executes actual Python, checks nonzero CPU and memory evidence,
verifies report/view/hash parity, and exercises timeout, cancellation and deletion.
It also proves static analysis does not execute a script that raises immediately.
Production never uses the committed test token. Do not expose the test stack.
These are integration checks, not a calibration of counter accuracy, a scaling
linearity claim or a replacement for full engine/campaign qualification.

Build the `tests` target separately and use `run --no-deps`: rebuilding executor
dependencies while a worker is running can invalidate its approved image ID on
Docker Desktop. If images were rebuilt, recreate the worker before testing; do
not rebuild the same executor tag during a live browser or smoke run.

For live browser coverage, start the test deployment, then from `frontend/`:

```bash
PISCES_BROWSER_CHANNEL=chrome \
PISCES_E2E_EXECUTION_URL=http://127.0.0.1:18080 \
PISCES_E2E_TOKEN_FILE=../containers/service/test-token.txt \
  npm run test:e2e
```

Credential-bearing browser tests disable trace, screenshot and video recording.
Other browser tests retain the existing local report API/Vite setup. Regenerate
transport types after model changes:

```bash
.venv/bin/python scripts/export_job_schema.py
cd frontend
npm run generate:jobs
npm run generate:types
```

The dev override mounts API/controller source and frontend source. Uploaded
scripts still run in immutable images: rebuild `executor-image` (or approved
native images) and restart the worker after changing evaluator code. Source
reload must not silently change an image already approved by a running worker.

The overrides use separate default image tags: `dev`, `test` and `local` for the
production baseline. Set `PISCES_IMAGE_TAG` consistently for custom releases.
Do not rebuild/prune a tag used by another live deployment: some Docker image
stores remove its old index ID when the tag moves. Restart the affected worker
after rebuilding its images; readiness is a startup probe, not an image lease.

## Profiles and Evidence

| Profile | Execution | Output claim |
| --- | --- | --- |
| `static` | Static evaluator only; no uploaded script execution | No runtime/output verification |
| `python` | Self-contained Python process inside the sandbox | Exit status and telemetry, not useful-work verification |
| `airflow` | Qualified native Airflow adapter in an approved image | Requires v2 workload contract, artifact binding and fixtures/references |
| `prefect` | Qualified native Prefect adapter in an approved image | Same contract/oracle boundary; engine-specific evidence |
| `dagster` | Qualified native Dagster adapter in an approved image | Same contract/oracle boundary; engine-specific evidence |

Only static/Python images are enabled by default. Native profiles remain disabled
until the worker probes a configured, operator-approved image with the correct
engine/version. Generic Python rejects detected orchestrator scripts rather
than claiming it evaluated the native engine. It does not install dependencies,
provide remote data access or supply arbitrary arguments/environment variables.

`completed` means a schema-admitted JSON report was produced, not that the
workflow's outputs are correct. `evaluation_succeeded` indicates the evaluator
did not report failure; the output verdict is separate. An exit-zero Python
script has **unverified outputs**. Failed jobs, timeouts and cancellations never
receive invented zero metrics. A killed container can lose the final snapshot.

Resource-comparison eligibility remains false for standalone jobs. A submitted
job is not a controlled paired campaign or telemetry qualification. Containers
on Docker Desktop report VM-scoped Linux counters, not native host electrical
measurements. CPU/memory observations do not become energy or carbon estimates
without an explicit qualified model and its assumptions. No hardware power
measurement or new carbon-savings claim is introduced by this service.

### Enable Native Engines

Build an existing qualified engine image first, then overlay the service code:

```bash
docker build -f containers/service/Dockerfile.engine \
  --build-arg BASE_ENGINE_IMAGE=pisces-dagster-oci:qualification \
  -t pisces-dagster-service:local .

export PISCES_EXECUTION_IMAGES='{"static":"pisces-executor:local","python":"pisces-executor:local","dagster":"pisces-dagster-service:local"}'
docker compose -f compose.yaml -f compose.prod.yaml up -d --force-recreate worker
```

The base image must be trusted, locally present and already qualified using the
engine procedures in [ENGINE_QUALIFICATION.md](ENGINE_QUALIFICATION.md). Equivalent
overlays work for qualified Prefect/Airflow images. Rebuilding from their current
Dockerfiles includes the sandbox marker; older images need the overlay above.
The mapping replaces the defaults, so include static/Python if desired.
Tags resolve to immutable IDs when the worker starts; image pulls are forbidden
at job execution. Restart the worker deliberately to adopt rebuilt images.

Native uploads require the script, its matching `*.binding.json`, the v2
`contract.json`, and every declared input/reference file. Contract and binding
are uploaded as exact UTF-8 text, preserving hash-bound file bytes. The UI offers
contract-relative paths for fixtures; check them before submission. It never
loads report-supplied paths from the server or exposes host directories. Existing
benchmark bundle generators can prepare compatible synthetic bundles.

Exercise valid and same-shape-wrong cases through the live API using a prepared
synthetic pipeline directory (against the disposable test deployment):

```bash
.venv/bin/python scripts/compose_engine_smoke.py path/to/prepared/event_aggregation \
  --profile dagster --origin http://127.0.0.1:18080 \
  --token-file containers/service/test-token.txt
```

The check requires an enabled native image, checks both output verdicts, source
hashes and CPU/memory evidence, and deletes its submitted jobs afterward.

## API and Independent Services

For complete request/response examples, transport constraints, errors and job
lifecycle semantics, see [the HTTP API guide](API.md) and
[generated OpenAPI](reference/openapi.json). The CLI is documented separately
in [CLI_REFERENCE.md](CLI_REFERENCE.md).

All `/api` endpoints require `Authorization: Bearer TOKEN` when authentication is
configured. `/healthz` is intentionally unauthenticated. Swagger is available at
`/docs` in development only; `/openapi.json` remains authenticated in production.
The UI and API share an origin through Nginx/Vite; permissive CORS is not used.

| Method / Path | Purpose |
| --- | --- |
| `GET /api/v1/capabilities` | Report formats and enabled execution/persistence |
| `POST /api/v1/reports/view` | Admit a supplied report; never execute it |
| `GET /api/v1/jobs/profiles` | Fresh worker/image readiness, including disabled engines |
| `POST /api/v1/jobs` | Validate bounded submission and enqueue; returns 202 |
| `GET /api/v1/jobs` | Up to 100 retained jobs, without source/fixture contents |
| `GET /api/v1/jobs/{id}` | State, timestamps, hash and report availability |
| `POST /api/v1/jobs/{id}/cancel` | Cancel queued/running evaluation |
| `GET /api/v1/jobs/{id}/report` | Canonical comprehensive JSON report |
| `GET /api/v1/jobs/{id}/view` | Python-owned presentation contract |
| `GET /api/v1/jobs/{id}/source` | Explicitly retrieve submitted source |
| `DELETE /api/v1/jobs/{id}` | Remove terminal job and retained payloads |

Submission fields are specified by the generated `JobSubmission` OpenAPI schema.
`source`, `contract` and `binding` are text strings; fixtures use confined `path`
and `content_base64`. The client cannot choose an image, shell, Docker flags,
host path, credentials or dependency install command.

Run only the backend and expose it on loopback port 8001:

```bash
docker compose -f compose.yaml -f compose.prod.yaml -f compose.backend.yaml \
  up --build -d api worker
```

Run the frontend independently against a reachable backend/proxy:

```bash
PISCES_API_UPSTREAM=backend.internal:8001 \
  docker compose -f compose.frontend.yaml up --build -d
```

Configure backend routing, `PISCES_ALLOWED_HOSTS` and `PISCES_ALLOWED_ORIGINS` for
your actual deployment. A loopback-only API is not reachable from another host;
set `PISCES_API_BIND_ADDRESS` to an appropriate private address only with network
controls. Nginx resolves its upstream through Docker DNS so API recreation does
not require a frontend rebuild. The CLI still works without any of these services:

```bash
pisces --help
pisces diagnostics path/to/workflow.py
pisces serve-api --port 8000   # report-only unless execution settings are explicit
```

## Limits and Production Boundary

- Single-host SQLite queue and one worker, serialized evaluations; not a cluster scheduler.
- 8 queued/running jobs, 100 retained records, 128 MiB logical payload budget;
  queue/storage exhaustion returns 429 until terminal jobs are deleted.
- 8 MiB total HTTP request/report, 1 MiB UTF-8 script, at most 16 fixture/reference
  files and 6 MiB decoded fixtures. Base64/JSON overhead also counts toward HTTP limits.
- 5-120 second execution deadline, 2 CPU quota, 2 GiB memory with no swap,
  128 processes, 256 MiB temporary scratch per disposable evaluation.
- Non-root evaluation user, read-only root, no bind mounts/network/capabilities,
  seccomp and no-new-privileges. Failed probes disable the profile.
- Cancel/timeout removes the container. Worker restart marks interrupted work
  failed and removes only that deployment's labeled containers; no automatic replay.
- No secrets injection, network data access, arbitrary packages or automatic load sweeps
  in uploaded jobs. Existing CLI controlled campaigns and browser plots remain available.

`compose.prod.yaml` is a hardened **single trusted-operator baseline**, not a
complete public SaaS deployment. Anyone holding the shared token can access all
jobs. Container isolation does not make hostile uploaded code safe, and an
uploaded script can falsify its own software-visible evidence. The controller's
Docker socket effectively grants host-level control; keep it off untrusted hosts
and do not use it for hostile multi-tenant uploads. Prefer a dedicated execution
machine/VM for approved research workloads.

Before exposing it beyond loopback: add a TLS reverse proxy, real user identity
and authorization, rate limits, audit/backup policy, managed secrets, pinned and
scanned release images/dependency locks, and stronger isolation for hostile code.
Production disables reload and interactive docs and requires a 32+ character
token, but does not implement those external services. No Kubernetes support or
distributed queue is claimed by this release.
