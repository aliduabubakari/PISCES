# HTTP API Guide

The frontend is an API client, not a second evaluator. Programmers can submit
the same bounded evaluations over HTTP and retrieve comprehensive JSON without
using the UI. For local files without a server, use [the CLI](CLI_REFERENCE.md).

API version: **0.2.0**. Routes are namespaced `/api/v1`; the package version,
canonical report schema (currently 1.4.0), and view schema (1.0.0) are separate
version domains. Treat unknown future schemas as unsupported, not equivalent.

## Deployment And Authentication

Start the [Compose service](EXECUTION_SERVICE.md#start-the-stack). The combined
frontend proxy exposes API routes at `http://127.0.0.1:8080`. For an independent
backend on port 8001, use the backend override in the deployment guide.

All routes except `/healthz` require `Authorization: Bearer <token>` when a token
is configured. Production requires a secret of at least 32 characters. Prefer
`PISCES_API_TOKEN_FILE` to a plaintext environment variable. Tokens are shared
operator credentials, not per-user accounts/scopes. Never commit or log them.

The default loopback `pisces serve-api --port 8000` is report-only and may have
no token. It does not start a worker or enable jobs. Do not expose that default
to a network. Remote use requires TLS, an operator access boundary and explicit
allowed hosts/origins. The service deliberately does not enable permissive CORS.

`GET /openapi.json` returns the live, authenticated OpenAPI document. `/docs` is
development-only (and still subject to authentication); production disables it.
The [committed OpenAPI snapshot](reference/openapi.json) is generated from the
same routes, Pydantic models and view schemas with authentication enabled. It
does not imply that any engine profile is ready on your deployment. Some raw
report/source responses intentionally have loose OpenAPI response types; use
the canonical schemas and shapes below, not an assumed generated SDK model.

## Routes

| Method | Path | Success response |
| --- | --- | --- |
| GET | `/healthz` | 200 `{"status":"ok"}`; process health, not worker readiness |
| GET | `/api/v1/capabilities` | 200 report formats, size bound, execution/persistence booleans |
| POST | `/api/v1/reports/view` | 200 normalized `ReportView` or `CampaignView`; no execution/persistence |
| GET | `/api/v1/jobs/profiles` | 200 array of profile readiness records |
| POST | `/api/v1/jobs` | 202 `JobStatus`; accepted into the queue, not completed |
| GET | `/api/v1/jobs` | 200 up to 100 retained job metadata records |
| GET | `/api/v1/jobs/{id}` | 200 `JobStatus` |
| POST | `/api/v1/jobs/{id}/cancel` | 200 current `JobStatus` |
| DELETE | `/api/v1/jobs/{id}` | 204 for a terminal job; no response body |
| GET | `/api/v1/jobs/{id}/report` | 200 original canonical report JSON |
| GET | `/api/v1/jobs/{id}/view` | 200 derived, validated report view |
| GET | `/api/v1/jobs/{id}/source` | 200 `{"filename":"...","source":"..."}` |

Job IDs are 32 lowercase hexadecimal characters. Job listing returns metadata,
not fixture/source contents. It is bounded, not a paginated archival API.

### Capabilities And Readiness

Capabilities include `view_schema_version`, `report_types`, `execution`,
`persistence` and `max_report_bytes` (8388608). Execution being enabled means a
queue is configured, **not** that a worker or requested image is ready.

Profile records have `name`, `ready`, nullable `image_id` and nullable `reason`.
The five names are `static`, `python`, `airflow`, `prefect`, `dagster`. A worker
heartbeat older than 15 seconds makes readiness unavailable. Image probing
checks the configured environment; it is not engine/counter qualification.
Only static/Python are configured by default. Native engines require approved
images on the worker; clients cannot select arbitrary images or Docker flags.

## Submit, Poll And Download

This standard-library Python example runs a self-contained script on the local
Compose deployment. Run from the repository root. It reads the token from a
file, refuses HTTP redirects, preserves exact source bytes as UTF-8, uses a
bounded polling deadline and refuses to overwrite the downloaded report.
It never retries `POST` automatically: the API has **no idempotency key**.

```python
import hashlib
import json
from pathlib import Path
import time
import urllib.request

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None

origin = "http://127.0.0.1:8080"  # Use HTTPS for non-loopback deployments.
token = Path(".secrets/api-token").read_text().strip()
opener = urllib.request.build_opener(NoRedirect)

def request(method, path, body=None):
    data = None if body is None else json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        origin + path, data=data, method=method,
        headers={"Authorization": "Bearer " + token,
                 "Content-Type": "application/json"},
    )
    with opener.open(req, timeout=15) as response:
        raw = response.read()
    return json.loads(raw) if raw else None

profiles = request("GET", "/api/v1/jobs/profiles")
assert any(p["name"] == "python" and p["ready"] for p in profiles)
source = b"total = sum(i * i for i in range(1000000))\nassert total > 0\n"
job = request("POST", "/api/v1/jobs", {
    "filename": "example.py", "source": source.decode("utf-8"),
    "profile": "python", "timeout_s": 60,
})
identity = job["id"]
print("Submitted job", identity)  # Retain this ID if the client disconnects.
deadline = time.monotonic() + 180
while job["state"] in {"queued", "running"}:
    if time.monotonic() >= deadline:
        raise TimeoutError("Client stopped waiting; the server job may still be active")
    time.sleep(1)
    job = request("GET", f"/api/v1/jobs/{identity}")
assert job["artifact_sha256"] == hashlib.sha256(source).hexdigest()
if job["has_report"]:
    report = request("GET", f"/api/v1/jobs/{identity}/report")
    with Path("evaluation-report.json").open("x", encoding="utf-8") as output:
        json.dump(report, output, indent=2)
        output.write("\n")
print(job["state"], "evaluation_succeeded:", job["evaluation_succeeded"])
```

This example's internal assertion is **not** a PISCES output oracle. Generic
Python returns execution/telemetry evidence, with unverified useful-work outputs.
A client timeout does not cancel a server job. Retrieve by ID or explicitly
cancel it; a lost submission response must be reconciled with the job inventory
before submitting duplicate work. The current API has no submission deduplication.

### Submission Contract

JSON only; unknown fields are rejected. Defaults shown below. Exact constraints
also live in `JobSubmission` in [OpenAPI](reference/openapi.json) and
[the source models](../src/pisces/service/models.py).

| Field | Type / constraint |
| --- | --- |
| `filename` | ASCII `.py` basename matching `^[A-Za-z0-9][A-Za-z0-9_.-]{0,95}\.py$` |
| `source` | Nonempty UTF-8 Python text, at most 1 MiB of UTF-8 bytes, no NUL |
| `profile` | `static` (default), `python`, `airflow`, `prefect`, `dagster` |
| `timeout_s` | Integer 5..120, default 60 |
| `contract` | Null/default, or exact UTF-8 v2 contract text; max 1,048,576 characters |
| `binding` | Null/default, or exact UTF-8 artifact-binding text; same character bound |
| `files` | Default `[]`; at most 16 `{path, content_base64}` fixture records |

The complete HTTP JSON body is limited to **8 MiB**, including base64 overhead.
Decoded fixture bytes are limited to **6 MiB combined**. Each base64 string has
an additional 8 MiB character bound. Fixture paths must be canonical relative
CSV/JSON/TXT paths of at most 160 characters and five components. No leading
dot, absolute path, `..`, empty/noncanonical component, backslash, colon, NUL,
duplicate path, or replacement of `contract.json` is allowed. Only declared
inputs/references needed for the contract should be included.

`static`/`python` reject contracts, bindings and fixtures. Native profiles
require both contract and binding; the worker validates their contents and
hashes before native execution. Source bytes must match the binding. Preserve
CRLF/newline bytes when preparing text: use `read_bytes().decode("utf-8")`, not
universal-newline file reads or parse/re-serialized contract objects.

### Native Bundle Preparation

Build the five-family fixtures with `pisces-benchmarks build` for your selected
engine. Submit one pipeline/case with its matching binding, contract and all
contract-declared input/reference files. Extend the example payload explicitly:

```python
import base64

def exact_text(path):
    return Path(path).read_bytes().decode("utf-8")

# Paths here are operator-selected, relative to the generated pipeline directory.
# Enumerate every declared input/reference; do not crawl paths from a remote report.
payload = {
    "filename": "valid.py",
    "source": exact_text("benchmark-runs/suite/event_aggregation/valid.py"),
    "profile": "dagster", "timeout_s": 120,
    "contract": exact_text("benchmark-runs/suite/event_aggregation/contract.json"),
    "binding": exact_text("benchmark-runs/suite/event_aggregation/valid.binding.json"),
    "files": [
        {"path": relative, "content_base64": base64.b64encode(
            (Path("benchmark-runs/suite/event_aggregation") / relative).read_bytes()
        ).decode("ascii")}
        for relative in declared_fixture_paths  # Explicitly fill from your contract.
    ],
}
```

This is a payload template; `declared_fixture_paths` must be supplied for your
contract. For a complete executable native valid/wrong-case check, use
[compose_engine_smoke.py](../scripts/compose_engine_smoke.py) as documented in
[native service setup](EXECUTION_SERVICE.md#enable-native-engines).

Service uploads use hash-checked **private temporary files**, not CLI read-only
input mounts. Reports label this `private_stdin_bundle`; an owning UID could
change file permissions. These are approved-workload experiments, not hostile
input attestation. Do not inherit campaign qualification from another delivery
profile without re-testing it.

## Job Lifecycle And Interpretation

`queued` becomes `running`, then `completed`, `failed`, `cancelled` or
`timed_out`. `POST .../cancel` cancels queued jobs immediately; a running job may
first show `cancel_requested: true` while the worker terminates it. Terminal
jobs can be deleted; active jobs return 409. Cancellation does not establish
successful output, and hard termination can lose final counters.

`JobStatus` includes source `artifact_sha256`, profile/filename, UTC Unix epoch
timestamps (`created_at`, nullable `started_at`/`finished_at`), `state`,
`cancel_requested`, nullable `error`, `has_report` and nullable
`evaluation_succeeded`. These fields are metadata, not resource measurements.

**`completed` means an admitted report was produced.** It can have
`evaluation_succeeded: false`. Inspect execution status, output verdicts,
per-metric availability/scope and comparison eligibility separately. A standalone
job never establishes a controlled resource ranking. Task states do not imply
per-task CPU/memory readings; absent metrics remain missing, not zero.

There is one worker controller per queue. Restart recovery marks interrupted
running jobs failed rather than silently replaying them. No SSE/WebSocket or
automatic retry/replay contract exists; poll at a modest interval and back off
after connection errors. Do not run multiple controllers against one store.

## Admit Saved Reports Without Execution

`POST /api/v1/reports/view` accepts an existing canonical unified report, suite
attempt/summary, load sweep or scaling sessions object. Send the JSON object
itself, not `{"report": ...}` or multipart form data. It returns a Python-owned
`ReportView`/`CampaignView`. It does not enqueue, retain, execute, fetch URLs or
follow evidence paths on the server.

Well-formed but unsupported/rejected evidence may return HTTP 200 with an
explicit admission verdict in the view. Malformed/invalid evidence can return
422. HTTP success is therefore not a scientific-validity verdict.

Portable offline bundles from `pisces report-view`/`campaign-view` contain
`view` plus original `report`; those bundles are for frontend import. They are
not a new canonical report schema and must not be submitted as a job.

## Errors, Limits And Operations

Most application errors return `{"detail":"machine_readable_reason"}`.
FastAPI parameter validation can instead return a `detail` array. Do not assume
one error body shape or expose raw errors/tokens in public logs.

| Status | Typical cause / action |
| --- | --- |
| 400 | Invalid Host header; check deployment allowlist |
| 401 | Missing/incorrect bearer token; do not retry with logged credentials |
| 403 | Origin not allowed; configure the intended proxy/origin |
| 404 | Unknown/deleted job or unavailable route |
| 409 | `profile_not_ready`, `job_still_active`, `job_report_not_available`, `job_source_not_retained` |
| 413 | Body over 8 MiB (`report_too_large`, also used for submissions) |
| 415 | `application_json_required` |
| 422 | `invalid_job_submission`, `invalid_report_evidence`, invalid ID/path parameters |
| 429 | `job_queue_or_storage_full`; clean retained jobs or wait, do not blindly resubmit |
| 503 | `execution_service_disabled`; use the authenticated execution deployment |

The queue allows eight active jobs (queued plus running), 100 retained jobs and 128 MiB of logical
stored payloads. These are current implementation limits, not tuning parameters
accepted by the client. Service containers also enforce resource limits; disk,
SQLite/WAL growth, logs, backups and host Docker resources need operator care.
See [deployment](EXECUTION_SERVICE.md) for the exact container profile.

Source, inputs and reports are sensitive retained data. Only terminal job
deletion removes their logical records; backups/WAL can still contain them.
Queued cancellation can discard the submitted source, making `/source` return
409. Tokens provide full deployment access, not tenant isolation. Reports carry
hash provenance but are not signed/attested. Before a public/multi-user service,
add a separate identity/authorization design, quotas, operational monitoring and
stronger hostile-code isolation; this release does not claim those properties.

## Maintain The Contract

```bash
python scripts/export_reference_docs.py
python scripts/export_reference_docs.py --check
```

The exporter starts no server, creates no execution store and uses no real
secret. It regenerates [OpenAPI](reference/openapi.json) and
[CLI help](reference/cli-help.md). Runtime live schemas still take precedence for
a deployment built from a different revision.
