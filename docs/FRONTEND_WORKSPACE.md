# Evaluation workspace

The frontend is implemented in `frontend/`: React, TypeScript and Vite, with
Python-owned versioned report-view and job contracts. Report import never runs
scripts, reads report-supplied paths or calls external services. **New evaluation**
is a separate, explicitly authorized upload workflow for the Compose API/worker.
See [execution service setup and security boundaries](EXECUTION_SERVICE.md).
The examples include sanitized saved Dagster attempts, a five-pipeline suite
summary and two independent Climate Fusion scaling sessions. They are saved
observations, not new evaluations. No accounts, analytics or browser persistence
are enabled. Execution jobs are retained in the backend until deleted; source
attachment to an imported report remains browser-local.

## Workspace Navigation

- **Runs** is the initial screen. Deployment history is loaded only after an
  explicit global connection; disconnected state never claims there are no jobs.
- **Campaigns** groups load studies and suite inventories. **Examples** separates
  sanitized saved evidence from new deployment evaluations and imported reports.
- **New evaluation** is a dedicated four-step route: Script, Inputs, Settings,
  Review. Submission requires a ready profile and fresh explicit authorization.
- Reports open on **Overview**, with execution/output/worker-telemetry status,
  scoped resource observations and output checks. **Source** toggles a resizable
  inspector; **Tasks** provides a selectable task-state table and provenance.
- **Metrics** explains definitions and missingness; source pointers and raw
  readings are under expandable provenance. **Compare** preserves backend policy
  and links back to the available observations instead of calculating rankings.

Job polling and profile readiness updates are application-level, not modal-owned.
Connection loss keeps the last observations visibly stale and retries with bounded
backoff. Run/report/tab/task selections use URL fragments and browser back/forward;
source, datasets and credentials never enter URLs or browser storage. A deployment
job can be restored after reconnecting. Imported reports must be reopened after a
reload. Up to 20 non-example reports may be held in memory; local copies can be
closed from Runs without deleting backend jobs.

The task inspector does **not** introduce new measurements. The current report-view
task contract supplies states, not arbitrary task duration, CPU or memory. Those
fields are explicitly marked **Not supplied**; worker totals are never allocated
to tasks. General task collectors, attempt-level timing, dependency timelines and
parallelization experiments remain a separate backend milestone. Existing trusted
benchmark timing remains available through its saved campaign signals.

## Run locally

From the repository root, install the optional local API and start it:

```bash
.venv/bin/python -m pip install -e '.[web]'
.venv/bin/python -m pisces.cli serve-api --port 8000
```

In a second terminal:

```bash
cd frontend
nvm use
npm ci
npm run dev -- --port 5173
```

Open <http://127.0.0.1:5173/>. The Vite development proxy sends `/api` requests
to `127.0.0.1:8000`. The API command binds to loopback only. If port 5173 is
occupied, Vite prints the next available port. If port 8000 is occupied, choose
another API port and set `PISCES_API_PROXY=http://127.0.0.1:PORT` when starting Vite.
`serve-reports` remains a compatibility alias. Without `PISCES_STATE_DIR`, this
local API is report-only; use Compose for execution rather than running uploaded
source inside an API process.

Node.js 22.12+ is required; `.nvmrc` pins the tested Node.js release. Versions
are pinned in `package.json` and `package-lock.json`. Fonts are served locally.
The API is optional for saved examples and portable view bundles. Python
library and CLI imports do not load FastAPI, Vite, React or Node.js.

## Notebook and CLI use

The original evaluator and JSON report remain unchanged. Map an existing report
without executing it or its source:

```python
from pisces.report_views import build_workspace_view, build_view_bundle

view = build_workspace_view(report)
portable_bundle = build_view_bundle(report)
```

Export an existing JSON report for offline viewing:

```bash
.venv/bin/python -m pisces.cli report-view path/to/report.json --out path/to/view.json
```

The command refuses to overwrite an existing output file. Open the resulting
bundle with **Open report** in the workspace. Raw reports need the optional
local API; portable bundles do not. Imports are explicit, bounded to 8 MiB per
file, with at most 10 selected files and 20 loaded reports. A bundle embeds its
original report and can be larger than the raw report; very large reports must
use raw import. Exported `report_json` preserves numeric JSON values that a
JavaScript object cannot represent exactly. Report download asks for confirmation
because the original artifact may contain local paths or private data.

## Supported evidence

- Unified reports: schema 1.4.0 with existing Python admission and alias checks.
- Benchmark attempts: the current versionless suite-attempt inventory shape,
  including OCI runtime evidence and standalone output checks. This admission
  does not invent a top-level suite schema version or attest execution.
- Suite summaries: pipeline/case inventory with original planned denominators;
  expectation matching is separate from output correctness.
- Scaling summaries, manifest/summary campaign envelopes and session comparisons:
  per-load medians, MAD, coverage and source descriptive fits. Separate sessions
  are never pooled. Memory lifetime peaks have no fitted scaling claim.
- Legacy, malformed and unknown report types: inventory-only, with no admitted
  metrics or verdicts.

The campaign contract is independently versioned in
`src/pisces/schemas/campaign-view-1.0.0.schema.json`; existing report views retain
their 1.0.0 contract. The same optional endpoint admits both formats.

## Multiple Data Sizes

Select **Scaling sessions** in the saved inventory to inspect the measured
10,000, 100,000, 500,000 and 1,000,000 total-row sweep. Use the session and metric
selectors to inspect task, execution-block, verification, worker and cgroup
lifetime-memory signals separately. **Visible loads** filters presentation
only; it does not recompute denominators, fits or medians. The x axis is numeric,
not evenly spaced categories. Missing readings remain gaps, not zeroes.

MAD whiskers show dispersion, not a confidence interval. **Source fit** displays
the supplied descriptive regression only when the complete planned signal has
full coverage and verified matching outputs, with at least three loads. The
mapper checks the supplied coefficients/R-squared against the medians; it does
not attest their underlying executions or invent a new fit. A raw `summary.json`
without its manifest can show reported medians, but not fit eligibility.
Non-monotonic readings remain non-monotonic. A high R-squared is not proof of
linear complexity, accurate counters, or carbon efficiency.

To run a new load sweep in this repository, use the existing approved Climate
Fusion OCI workflow and its matching successful telemetry confirmation:

```bash
.venv/bin/python -m benchmarks.climate_fusion.scaling diagnostics/my-load-sweep \
  --oci-image "$IMAGE" --confirmation "$CONFIRMATION" \
  --environment docker_desktop_vm --repetitions 3 \
  --rows 10000 100000 500000 1000000
```

The four sizes above remain the default. `--rows` accepts 3-8 distinct total row
counts, multiples of five between 5 and 1,000,000, because this fixture splits
rows evenly over five sources. Counts are sorted for reporting and randomized
within each repetition for execution. The container/profile/oracle checks are
unchanged; this is not new arbitrary-dataset execution support in the browser.
On a qualified native Linux engine use `--environment local_linux_engine`.

Export the saved manifest and summary for offline viewing without running them:

```bash
.venv/bin/python -m pisces.cli campaign-view diagnostics/my-load-sweep \
  --pipeline climate_fusion --out diagnostics/my-load-sweep-view.json
```

Open that file with **Open report**. It bundles only the explicitly selected
`manifest.json` and `summary.json`; it never follows report-supplied paths.
Snapshots of the raw attempts remain the authoritative detailed evidence.
Do not mix different data sizes by concatenating unrelated run reports into a
curve: prepare a declared campaign with a consistent workflow and profile.

Existing session-comparison and suite-summary JSON files can be imported through
the local API or exported with `pisces report-view`. Multiple files can be opened
at once. Inventory search and suite pipeline/case filters preserve all original
planned counts. Session selection changes the displayed session, not a pooled
average. Different profiles remain independently labeled by profile digest.

The versioned schema is `src/pisces/schemas/report-view-1.0.0.schema.json`.
Python metric definitions live in `src/pisces/report_views/metrics.py`.
TypeScript types are generated from that schema, not maintained independently:

```bash
cd frontend
npm run generate:types
npm run generate:validators
```

Each view includes admission, metric registry version, original JSON pointers,
availability and reason codes. The report ID hashes compact UTF-8 JSON with
sorted keys, not the original file bytes. It is an inventory identifier, not an
execution attestation or a cryptographic signature. Source association uses
SHA-256 of the explicitly attached file's exact bytes, without newline
normalization. Source attached to an imported report stays browser-local and is
never executed. Source explicitly submitted through New evaluation is uploaded,
retained and evaluated according to the selected profile.

Browser validators are compiled from these schemas at build time using Ajv
standalone output. Production does not require CSP `unsafe-eval`; `npm test`
checks that generated validators are current.

## Local HTTP boundary

Implemented endpoints:

| Endpoint | Behavior |
| --- | --- |
| `GET /api/v1/capabilities` | Supported report formats, byte limit, execution/persistence based on configuration |
| `POST /api/v1/reports/view` | Bounded JSON body, Python admission, canonical report view |
| `GET /openapi.json` | Local transport description including the response view schema |

The default local configuration is stateless and report-only. The Compose
configuration adds authenticated `/api/v1/jobs` submission, status, cancellation,
report/view/source retrieval and terminal-job deletion. It has no URL fetch,
directory scanner or arbitrary server-path input. Host/Origin allowlists,
bounded JSON uploads and `Cache-Control: no-store` apply; CORS is not enabled.
The frontend reverse-proxies `/api` on the same origin. See the execution-service
guide for the endpoint table, retention, limits and production requirements.

Offline bundles undergo browser structural admission only and are labeled
**not revalidated by Python**. Python schema admission also validates structure,
not authenticity. A third-party report is never turned into an attested run.

## Current limitations

The workspace shows output checks, task states, container counters, bounded
execution-block metrics and attached runtime energy/carbon model fields. It
does not yet display import/build profiles, heuristic findings, arbitrary per-task
timing, graph edges or source-location links. Trusted task timing in scaling
campaigns is supported. Task coverage can supply
observed task IDs when no engine-event list is attached; it cannot establish
dependencies. Python remains authoritative for comparison policy. The viewer
does not compute rankings or infer task resource shares, model carbon from CPU,
or treat missing counters as zero. A lifetime peak is labeled as such even
when listed next to execution-block observations.

## Verification

```bash
.venv/bin/python -m pytest -q
cd frontend
npm test
npm run build
npx playwright install chromium
npm run test:e2e
```

The browser tests start/reuse the local API and Vite servers. Set
`PISCES_BROWSER_CHANNEL=chrome` to use an installed Chrome instead of downloaded
Chromium; `PISCES_PYTHON` overrides the default `../.venv/bin/python` interpreter.
Tests cover saved-view snapshots, pointer/value parity, raw import, offline
bundles, negative cases, exact-byte source association, missing versus zero,
unknown reason codes, lost snapshots, unsupported schemas, XSS text rendering,
upload controls, keyboard tabs and five widths from 320 to 1440 pixels.

The committed fixture set is reproducible from the saved synthetic evidence:

```bash
.venv/bin/python scripts/build_frontend_fixtures.py \
  diagnostics/multi-engine-4w4hOm/dagster-full \
  diagnostics/multi-engine-4w4hOm/dagster/event_aggregation
```

Saved diagnostics are not required to run the app or its tests; the sanitized
fixtures are committed. Rebuild campaign fixtures with:

```bash
.venv/bin/python scripts/build_campaign_fixtures.py \
  diagnostics/climate-scaling-sessions-vSaTww/session-comparison.json \
  diagnostics/engine-suite-Ei48dh/dagster-results/summary.json
```

Next are richer task timing and source-linked static findings. Guided execution
remains a separate U2 milestone; no browser action starts a load sweep.
