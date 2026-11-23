# PISCES frontend design

## Product contract

PISCES remains one Python evaluation library with multiple clients: notebooks,
the CLI, and a browser workspace. Each evaluation produces the same versioned
JSON evidence. The frontend presents that evidence; it must not implement a
second evaluator, invent explanations, or change eligibility policy.

This document defines the workspace design. Report viewing, saved campaigns and
an authenticated upload/queue workflow are implemented; see
[workspace setup and current limits](FRONTEND_WORKSPACE.md) and
[the execution service](EXECUTION_SERVICE.md).
The current public Python entry points are `UnifiedEnergyConfig` and
`run_unified_energy_evaluation`; `validate_report` admits unified reports.
The HTTP transport is modular: report admission is stateless; enabling a state
directory and authentication adds jobs backed by a separate container worker.
CLI and notebook imports remain independent of Node.js and FastAPI.

## Main experience: evaluation workspace

Borrow InLumen's resizable main surface and contextual inspector, not its entire
pipeline-authoring application. Keep a script and its evidence visible together.
The first screen is the workspace, not a marketing page or a wall of score cards.

```text
PISCES | Workspace  Campaigns  Compare  Metric guide       Open report  Export
-------------------------------------------------------------------------
event_aggregation / valid.py           Dagster 1.12.8 / OCI / Docker Desktop
-------------------------------------------------------------------------
Source / Declared graph                Verdicts / Metrics / Compare / Evidence
                                      |
Read-only, syntax-highlighted code     | Output and task checks
or an explicitly sourced graph        | Scope-aware metrics and definitions
                                      | Missingness reasons and limitations
                                      | Comparison gates and evidence pointers
-------------------------------------------------------------------------
Source association / artifact hash     Imported report / schema / evidence type
```

### Workspace layout

- A compact top bar identifies the workspace, selected run and engine profile.
  Import, export and New evaluation are distinct commands. Evaluation profiles
  are enabled only when a live worker has successfully probed an approved image.
- The source pane occupies about 42% of a wide workspace, with a keyboard-
  operable splitter and a reset-to-default action. Both pane widths are bounded.
  Pane collapse/focus controls must not hide the selected report or verdict.
- The evidence pane uses stable tabs: **Verdicts**, **Metrics**, **Compare**,
  **Evidence**. Keep execution outcome, useful-work verification, and telemetry
  availability separate. A completed engine run can have incorrect output.
- On narrower tablets use a single pane with Source/Evidence switching. On
  mobile stack the primary verdict and source sections; retain all details.
  Avoid forcing a desktop graph or JSON tree into a narrow panel.
- Prefer restrained white/neutral surfaces, a teal active accent, thin dividers,
  and explicit green/amber/red status text. Use color plus icons and labels,
  never color alone. Keep corners modest, text compact but readable, and
  operational data in tables rather than nested cards.

### Source and graph

The unified report includes `metadata.artifact_sha256`, but not script contents.
Viewing a report must work without source. Offer optional source attachment,
hash the exact file bytes without newline normalization, and show **matched**,
**mismatched**, or **not attached**. A filename or path is not an association.
Never open `workflow.path` automatically or let a server fetch arbitrary paths.

Use a read-only code viewer with Python/JSON/YAML highlighting, line numbers,
find, wrap toggle, copy and download. Editing later creates a new draft and
never modifies the source associated with a historical report. Dirty drafts
cannot inherit the original hash or verdict. Do not imply that the current
schema provides source locations for every finding; use line links only when
backend evidence supplies a validated span against the matching artifact.

Graph rendering is optional. InLumen's graph interaction is a useful reference,
but task-state lists do not establish dependency edges or timing. Distinguish
**declared/static graph**, **observed nodes**, and **runtime dependency trace**.
If no qualified graph exists, show observed tasks as a list, not an invented DAG.
Selecting a node may filter its actual evidence, but cannot assign a share of
cgroup CPU to that task. A graph/source mapping requires its own versioned
backend representation and tests; do not infer it inside React.

### Evidence tabs

| Tab | Primary contents | Important behavior |
| --- | --- | --- |
| Verdicts | Engine state, declared task coverage, fixture consumption, each output check | Separate expected rejection of a negative fixture from a successful workflow output |
| Metrics | Scope selector; metric/value/unit/availability table; definition inspector | Worker, execution block and task body remain distinct; unavailable task metrics stay unavailable |
| Compare | Candidate selection and policy results | Show not evaluated, blocked, or permitted evidence types; no local ranking calculation |
| Evidence | Version, image ID, hashes, environment, origin, raw JSON pointers | Hide sensitive local paths by default; raw download is a separately disclosed action |

Each metric definition contains its stable ID, display unit, measurement source,
interval/scope, inclusions/exclusions, evidence role, availability reason and
pointer to the exact source field. Definitions are versioned Python-owned data,
exported to the UI with generated types. Avoid a second manually curated
registry in TypeScript. Formatting bytes as MiB is presentation; converting
missing values to zero or dividing a container peak among tasks is not.

Missingness distinguishes not requested, unsupported, not collected, unreadable,
invalid, and a lost final snapshot. Unknown reason codes display the original
code with a neutral explanation, not an invented cause. Modeled carbon requires
its own model metadata and is never inferred from available CPU or runtime.

Do not stack task, execution and worker intervals as if they are additive.
Use separate aligned scope views. Conformance/reference agreement and
run-to-run dispersion are different observations and require different labels.
An imported run is not conformance-tested merely because a similar image passed
a counter fixture. Conformance association requires matching profile provenance.

## Navigation and workflows

1. **Open a CLI/notebook report:** validate and identify the report type, then
   open the workspace even if source is absent. Let the user attach source
   explicitly. Preserve the original report unchanged.
2. **Inspect a result:** select an output or task; show the verdict, reason,
   available evidence and limitations. Never show sensitive output cell values
   merely to explain an error. A false output verdict is not a server error.
3. **Inspect a metric:** select a row for source, scope and missingness details.
   Let users inspect raw evidence without needing to understand the whole JSON.
4. **Open a campaign:** show a dense run inventory filtered by pipeline, engine,
   case, load and session. Case expectation, output verification and collection
   coverage are independent columns. Filtering never changes the planned
   denominator or recomputes a favorable pass fraction.
5. **Request a comparison:** inventory side-by-side observations first. Invoke
   the existing policy or campaign analysis; resource ranking remains blocked
   where the backend does not support it. Matching image IDs are necessary for
   some comparisons, not sufficient to establish equivalent work. Current
   generic reports explicitly set resource eligibility to false.

Keep URL state for selected run/tab/metric and presentation filters. Keep
source text and raw datasets out of URLs, analytics and browser logs. No
automatic account requirement for a local report viewer. Later multi-user
workspaces need explicit access control and retention policy.

### Evaluation flow

The implemented **New evaluation** route replaces the original all-in-one modal.
Global deployment connection and persistent job monitoring live outside that flow.
The route has Script, Inputs, Settings and Review steps with a request summary.
The following remain the design requirements for expanding the supported profiles:

1. **Artifact:** explicitly attach source or choose an approved artifact;
   identify engine and entry point, display its byte hash, and attach an
   approved dependency/image profile. Selecting a source file never imports it
   into the web server's Python process.
2. **Intent and inputs:** select or attach the workload contract, expected
   outputs and permitted sample/fixture inputs. Separate supplied facts from
   generated assumptions. Display privacy/provenance policy before upload.
3. **Evidence request:** choose supported static, simulated/sample, or isolated
   execution modes using backend capability labels. Show which metrics the
   selected mode cannot provide; a dry run cannot promise execution telemetry.
   A readiness check returns structured blockers before submission.
4. **Review and submit:** show immutable artifact/contract/profile IDs,
   requested metrics, limits and evidence scope. Backend admission decides what
   can run. Stream or poll job progress, allow cancellation, and open the
   finished canonical report in the same workspace used for notebook imports.

Keep this request distinct from a historical report. Reopening an old report
must not submit it again. Retry creates a new attempt with linked provenance,
not a replacement result. Unsupported modes stay disabled with a backend
reason; do not silently substitute simulation for execution. Live logs remain
separate from durable evidence and must be bounded and redacted.

## Architecture

```text
Notebook -------- Python library ------ versioned JSON evidence
CLI ------------- Python library ------ versioned JSON evidence
Browser --------- report/view HTTP adapter ----- same Python library
   |                       |
   +-- local report view   +-- authenticated job API --> isolated worker
```

**Frontend:** React + TypeScript + Vite on a pinned Node.js toolchain, following
the established InLumen stack. Use Radix/shadcn primitives, Lucide icons,
resizable panels, React Router and TanStack Query. Prefer Monaco for substantial
source viewing, loaded lazily and locally, with a plain-text accessible fallback.
Use React Flow only for a sourced graph, not for report layout or evaluation
logic. Adopt a chart library only when a campaign plot is actually needed.
Freeze versions and a lockfile during implementation rather than copying
InLumen's dependency ranges or its application state wholesale.

**Python transport:** a small optional FastAPI adapter around library services,
not CLI output scraping and not a Node evaluator. React can be deployed as
static assets; Node.js is the frontend development/build runtime. A separate
Node server is unnecessary unless a concrete deployment requirement needs one.
Serving the SPA and API under one origin avoids an unnecessary CORS surface.
Libraries and CLI imports must not import a web framework or require a server.

**View contract:** introduce a versioned normalized presentation object covering
unified reports, suite attempts, suite summaries, load sweeps and session
comparisons. They are different formats today, not interchangeable JSON files.
Keep `view_schema_version` distinct from each source's schema version. Include
report admission, origin, source digest, original JSON pointer, capability and
missingness metadata. Components consume this contract, not ad hoc nested JSON
paths. Unsupported versions remain an inventory or raw-JSON view.

**Trust:** JSON schema admission is not proof that uploaded claims are true.
Keep report origin, schema admission, source hash match and actual service-
observed execution as independent facts. In a fully offline browser, structural
checks can display report-provided verdicts with an imported/not-revalidated
label. Python admission is authoritative when the adapter is connected. Neither
path should turn a third-party JSON upload into an attested execution.

### Proposed read-only HTTP surface

The initial stateless slice implements `GET /api/v1/capabilities` and
`POST /api/v1/reports/view`, with OpenAPI and generated view types. The stored
report/source endpoints below remain proposed, not implemented.

| Endpoint | Responsibility |
| --- | --- |
| `GET /api/v1/capabilities` | Available report types, schemas and admission features; execution capability defaults off |
| `POST /api/v1/reports/import` | Bounded JSON upload, format-specific Python admission, report ID and view result; never execute |
| `GET /api/v1/reports/{id}` | Authorized original report, unchanged |
| `GET /api/v1/reports/{id}/view` | Canonical view model, metric definitions and evidence pointers |
| `POST /api/v1/reports/{id}/source` | Optional explicit source upload and byte-hash association; never read a client-specified server path |

Local browser-only imports need no upload at all. Server uploads need explicit
consent, size limits, storage policy and deletion support. Do not add arbitrary
URL fetching, ZIP extraction or local-directory crawling in the first slice.

### Suggested ownership boundaries

```text
frontend/src/
  app/                  routing, providers and workspace shell
  features/imports/     import dialog and source association
  features/workspace/   source/graph/evidence layout
  features/reports/     verdicts, metric inspector and provenance
  features/campaigns/   inventory, loads and sessions
  features/comparison/  backend gate results, not new policy
  components/ui/        accessible shared primitives
  api/generated/        generated transport types and client
  api/adapters/         offline and HTTP adapters behind one interface
  test/fixtures/        sanitized, provenance-linked reports
src/pisces/report_views/ presentation mapping and metric registry (proposed)
src/pisces/web/          optional HTTP transport only (proposed)
```

Do not introduce a global mutable evaluation object in React. Keep selection
state separate from immutable imported evidence and from future run requests.
Use query caching for server resources and local component state for panel and
tab controls. Add shared state only when multiple independent views need it.

## Delivery and acceptance

**Design prototype (this slice):** explore a split workspace with real saved
Dagster evidence and explicit preview labeling. Case selection, evidence tabs,
source/declared graph views and metric details are local interactions. It does
not import arbitrary files, connect to a service or execute code.

**Viewer slice:** implement typed view adapters and metric definitions in Python,
sanitized fixtures and the read-only React workspace. Test schema admission,
inconsistent aliases, unknown report formats, exact-byte source matching,
missing source, wrong/partial output, unavailable carbon and timeout snapshots.
Add import/export and optional local persistence with explicit consent.

**Connected slice:** add the read-only transport and generated client; prove
that offline report, CLI and HTTP views preserve the same original values and
reason codes. Test upload limits, XSS, path traversal, malformed JSON, polling
errors, and access controls for the chosen deployment.

**Execution slice (implemented baseline):** evaluate approved artifacts only after readiness,
contract and profile admission. Separate queued/importing/running/verifying/
finished job progress from workflow correctness. Use a bounded queue, service-
controlled paths and images, cancellation, cleanup and event streaming or
polling. A report upload is never a request to run code; a new draft requires a
new evaluation. Notebooks and CLI continue using the same library and formats.
The implemented job states, profiles, trusted-input boundary and current limits
are documented in [EXECUTION_SERVICE.md](EXECUTION_SERVICE.md). Per-phase progress,
live log streaming and automatic retry are not implemented.

Run component and adapter tests plus browser tests at desktop, tablet and mobile
widths, with keyboard navigation, focus management, resize controls, empty and
error states, and accessible metric names. Test that missing readings are never
rendered as zero and nested intervals are not double-counted. User acceptance:
someone unfamiliar with PISCES can find the output verdict, identify a metric's
scope, explain a missing value and understand why a comparison is blocked.
