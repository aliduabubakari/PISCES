import { lazy, Suspense, useEffect, useState } from "react";
import * as Tooltip from "@radix-ui/react-tooltip";
import { Panel, PanelGroup, PanelResizeHandle } from "react-resizable-panels";
import {
  Activity,
  BookOpen,
  ChartNoAxesCombined,
  ChevronLeft,
  Code2,
  Download,
  FolderOpen,
  GripVertical,
  LoaderCircle,
  Play,
  PlugZap,
  ShieldCheck,
  Square,
} from "lucide-react";
import { apiJSON } from "../api/client";
import {
  download,
  isCampaignEntry,
  type ReportEntry,
  type WorkspaceEntry,
} from "../api/reports";
import type { ReportView } from "../api/generated/report-view";
import type { CampaignView } from "../api/generated/campaign-view";
import { IconButton, Status } from "../components/ui";
import { Evidence } from "../features/reports/Evidence";
import { ImportDialog } from "../features/reports/ImportDialog";
import { ReportLibrary } from "../features/reports/ReportLibrary";
import {
  SourcePane,
  type SourceAttachment,
} from "../features/workspace/SourcePane";
import {
  ServiceProvider,
  useService,
} from "../features/evaluations/ServiceProvider";
import { ConnectionDialog } from "../features/evaluations/ConnectionDialog";
import { RunsPage } from "../features/evaluations/RunsPage";
import { loadServiceReport } from "../features/evaluations/serviceReports";
import { navigate, useRoute, type ReportTab } from "./navigation";
import { explain, profileNames, timestamp } from "../lib/presentation";
import examplesData from "../test/fixtures/examples.json";
import brandMark from "../../../assets/brand/pisces-mark.svg";

const CampaignWorkspace = lazy(
  () => import("../features/campaigns/CampaignWorkspace"),
);
const EvaluationPage = lazy(
  () => import("../features/evaluations/EvaluationPage"),
);
const examples: WorkspaceEntry[] = examplesData.map((example) => ({
  view: example.view as ReportView,
  raw: JSON.stringify(example.report, null, 2),
  filename: example.filename,
  origin: "example",
}));
const initialSources: Record<string, SourceAttachment> = {};
examplesData.forEach((example) => {
  if (example.source)
    initialSources[example.view.report_id] = {
      name: example.source.name,
      text: example.source.text,
      digest: example.source.digest,
      bytes: new TextEncoder().encode(example.source.text).buffer,
    };
});

export default function App() {
  return (
    <ServiceProvider>
      <Shell />
    </ServiceProvider>
  );
}
function Shell() {
  const service = useService();
  const route = useRoute();
  const [entries, setEntries] = useState(examples);
  const [sources, setSources] = useState(initialSources);
  const [importOpen, setImportOpen] = useState(false);
  const [connectionOpen, setConnectionOpen] = useState(false);
  const [sourceOpen, setSourceOpen] = useState(false);
  const [exampleError, setExampleError] = useState(false);
  const [jobReports, setJobReports] = useState<Record<string, string>>({});
  const [jobError, setJobError] = useState("");
  const [cancelling, setCancelling] = useState(false);
  const [retry, setRetry] = useState(0);
  useEffect(() => {
    let active = true;
    import("../test/fixtures/campaigns.json")
      .then(({ default: data }) => {
        if (active)
          setEntries((previous) => [
            ...previous,
            ...data
              .filter(
                (item) =>
                  !previous.some(
                    (entry) => entry.view.report_id === item.view.report_id,
                  ),
              )
              .map((item) => ({
                view: item.view as CampaignView,
                raw: JSON.stringify(item.report, null, 2),
                filename: item.filename,
                origin: "example" as const,
              })),
          ]);
      })
      .catch(() => {
        if (active) setExampleError(true);
      });
    return () => {
      active = false;
    };
  }, []);
  const job =
    route.page === "job"
      ? service.jobs.find((item) => item.id === route.id)
      : undefined;
  const selected = route.page === "job" ? jobReports[route.id ?? ""] : route.id;
  const entry = entries.find((item) => item.view.report_id === selected);
  useEffect(() => {
    setJobError("");
    if (
      route.page !== "job" ||
      !job?.has_report ||
      entry ||
      service.status !== "connected"
    )
      return;
    let active = true;
    loadServiceReport(job.id)
      .then(({ entry: loaded, source }) => {
        if (!active) return;
        if (
          entries.filter((item) => item.origin !== "example").length >= 20 &&
          !entries.some((item) => item.view.report_id === loaded.view.report_id)
        )
          throw new Error(
            "Close an opened report from Runs before loading more. The browser limit is 20 reports.",
          );
        setEntries((previous) => [
          ...previous.filter(
            (item) => item.view.report_id !== loaded.view.report_id,
          ),
          loaded,
        ]);
        setSources((previous) => ({
          ...previous,
          [loaded.view.report_id]: source,
        }));
        setJobReports((previous) => ({
          ...previous,
          [job.id]: loaded.view.report_id,
        }));
      })
      .catch((cause) => {
        if (active)
          setJobError(
            cause instanceof Error
              ? cause.message
              : "Could not open the job report.",
          );
      });
    return () => {
      active = false;
    };
  }, [
    route.page,
    route.id,
    job?.has_report,
    entry?.view.report_id,
    service.status,
    retry,
  ]);
  function closeReport(id: string) {
    setEntries((previous) =>
      previous.filter((item) => item.view.report_id !== id),
    );
    setSources((previous) => {
      const next = { ...previous };
      delete next[id];
      return next;
    });
    setJobReports((previous) =>
      Object.fromEntries(
        Object.entries(previous).filter(([, value]) => value !== id),
      ),
    );
  }
  function imported(incoming: WorkspaceEntry[]) {
    const merged = [
      ...entries.filter(
        (entry) =>
          !incoming.some(
            (item) => item.view.report_id === entry.view.report_id,
          ),
      ),
      ...incoming.filter(
        (item, index) =>
          incoming.findIndex(
            (other) => other.view.report_id === item.view.report_id,
          ) === index,
      ),
    ];
    if (merged.filter((item) => item.origin !== "example").length > 20)
      throw new Error(
        "Close an opened report before adding more. The browser limit is 20 reports.",
      );
    setEntries(merged);
    navigate(`report/${incoming[0].view.report_id}`);
  }
  function setTab(tab: ReportTab, task?: string) {
    navigate(
      `${route.page}/${route.id}?${new URLSearchParams({ tab, ...(task ? { task } : {}) })}`,
    );
  }
  const section =
    route.page === "report"
      ? entry?.origin === "example"
        ? "examples"
        : entry && isCampaignEntry(entry)
          ? "campaigns"
          : "runs"
      : route.page === "job"
        ? "runs"
        : route.page;
  const activeCount = service.jobs.filter((item) =>
    ["queued", "running"].includes(item.state),
  ).length;
  const connected = service.capabilities !== null;
  return (
    <Tooltip.Provider delayDuration={250}>
      <div className="app-shell clean-shell">
        <header className="app-header clean-header">
          <a className="brand" href="#/runs">
            <img
              className="brand-logo"
              src={brandMark}
              width="28"
              height="28"
              alt=""
              aria-hidden="true"
            />
            <h1>PISCES</h1>
          </a>
          <nav className="primary-nav" aria-label="Main navigation">
            {[
              ["runs", "Runs", Activity],
              ["campaigns", "Campaigns", ChartNoAxesCombined],
              ["examples", "Examples", BookOpen],
            ].map(([value, label, Icon]) => {
              const Symbol = Icon as typeof Activity;
              return (
                <a
                  key={String(value)}
                  href={`#/${value}`}
                  aria-current={section === value ? "page" : undefined}
                >
                  <Symbol />
                  {String(label)}
                  {value === "runs" && activeCount > 0 && (
                    <span className="count">{activeCount}</span>
                  )}
                </a>
              );
            })}
          </nav>
          <div className="header-actions">
            <button
              className={`button connection-button ${service.status === "reconnecting" ? "is-stale" : connected ? "is-connected" : ""}`}
              aria-label="Deployment connection"
              title="Deployment connection"
              onClick={() => setConnectionOpen(true)}
            >
              {service.status === "connecting" ||
              service.status === "reconnecting" ? (
                <LoaderCircle className="spin" />
              ) : (
                <PlugZap />
              )}
              <span className="action-label">
                {service.status === "reconnecting"
                  ? "Reconnecting"
                  : connected
                    ? "Connected"
                    : "Connect deployment"}
              </span>
            </button>
            {!["runs", "campaigns", "examples"].includes(route.page) && (
              <IconButton
                label="Open report"
                onClick={() => setImportOpen(true)}
              >
                <FolderOpen />
              </IconButton>
            )}
            <button
              className="button primary"
              aria-label="New evaluation"
              title="New evaluation"
              onClick={() => navigate("evaluate")}
            >
              <Play />
              <span className="action-label">New evaluation</span>
            </button>
          </div>
        </header>
        <main className="shell-main">
          {service.capabilities && service.status === "reconnecting" && (
            <p role="status" className="connection-notice">
              {service.error}
            </p>
          )}
          {exampleError && (
            <p role="status" className="admission-banner">
              Saved campaign examples could not be loaded.
            </p>
          )}
          {route.page === "runs" && (
            <RunsPage
              entries={entries}
              onImport={() => setImportOpen(true)}
              onConnect={() => setConnectionOpen(true)}
              onClose={closeReport}
            />
          )}
          {(route.page === "campaigns" || route.page === "examples") && (
            <ReportLibrary
              entries={entries}
              kind={route.page}
              onImport={() => setImportOpen(true)}
            />
          )}
          {route.page === "evaluate" && (
            <Suspense
              fallback={
                <p className="empty-line">Loading evaluation form...</p>
              }
            >
              <EvaluationPage onConnect={() => setConnectionOpen(true)} />
            </Suspense>
          )}
          {route.page === "job" &&
            (!connected || !job || !job.has_report || !entry) && (
              <div className="page-content">
                <div className="page-heading">
                  <div>
                    <span className="eyebrow">Deployment evaluation</span>
                    <h2>{job?.filename ?? "Evaluation"}</h2>
                  </div>
                  <button className="button" onClick={() => navigate("runs")}>
                    All runs
                  </button>
                </div>
                {!connected ? (
                  <div className="empty-state">
                    <PlugZap />
                    <h3>Connect to retrieve this evaluation</h3>
                    <button
                      className="button"
                      onClick={() => setConnectionOpen(true)}
                    >
                      <PlugZap />
                      Connect deployment
                    </button>
                  </div>
                ) : !job ? (
                  <div className="empty-state">
                    <Activity />
                    <h3>Evaluation not found</h3>
                    <p>The deployment did not return this job.</p>
                  </div>
                ) : (
                  <section className="job-progress">
                    <div className="job-progress-title">
                      {["queued", "running"].includes(job.state) && (
                        <LoaderCircle className="spin" />
                      )}
                      <Status value={job.state} />
                    </div>
                    <dl className="review-list">
                      <div>
                        <dt>Profile</dt>
                        <dd>{profileNames[job.profile]}</dd>
                      </div>
                      <div>
                        <dt>Submitted</dt>
                        <dd>{timestamp(job.created_at)}</dd>
                      </div>
                      <div>
                        <dt>Started</dt>
                        <dd>{timestamp(job.started_at)}</dd>
                      </div>
                      <div>
                        <dt>Report</dt>
                        <dd>
                          {job.has_report
                            ? "Loading report"
                            : ["queued", "running"].includes(job.state)
                              ? "Not yet available"
                              : "No report produced"}
                        </dd>
                      </div>
                    </dl>
                    {job.error && (
                      <p className="import-error">
                        {explain(job.error) ?? job.error.replaceAll("_", " ")}
                      </p>
                    )}
                    {jobError && (
                      <>
                        <p role="alert" className="import-error">
                          {jobError}
                        </p>
                        <button
                          className="button"
                          onClick={() => setRetry((value) => value + 1)}
                        >
                          Retry report retrieval
                        </button>
                      </>
                    )}
                    {["queued", "running"].includes(job.state) && (
                      <button
                        className="button"
                        disabled={cancelling || job.cancel_requested}
                        onClick={async () => {
                          setCancelling(true);
                          setJobError("");
                          try {
                            await apiJSON(`/api/v1/jobs/${job.id}/cancel`, {
                              method: "POST",
                            });
                            await service.refresh();
                          } catch (cause) {
                            setJobError(
                              cause instanceof Error
                                ? cause.message
                                : "Could not request cancellation.",
                            );
                          } finally {
                            setCancelling(false);
                          }
                        }}
                      >
                        <Square />
                        {job.cancel_requested
                          ? "Cancellation requested"
                          : "Cancel evaluation"}
                      </button>
                    )}
                  </section>
                )}
              </div>
            )}
          {(route.page === "report" ||
            (route.page === "job" && job?.has_report)) &&
            entry && (
              <>
                <div className="run-heading">
                  <div className="run-heading-title">
                    <IconButton
                      label={`Back to ${entry.origin === "example" ? "examples" : "runs"}`}
                      onClick={() =>
                        navigate(
                          entry.origin === "example" ? "examples" : "runs",
                        )
                      }
                    >
                      <ChevronLeft />
                    </IconButton>
                    <div>
                      <div className="run-title">
                        <h2>{entry.view.identity.name.replaceAll("_", " ")}</h2>
                        <span className="engine-label">
                          {entry.view.identity.engine}
                        </span>
                      </div>
                      <div className="run-meta">
                        <span>
                          {entry.view.identity.case?.replaceAll("_", " ") ??
                            entry.view.identity.evidence_kind.replaceAll(
                              "_",
                              " ",
                            )}
                        </span>
                        <span>/</span>
                        <span>
                          {entry.origin === "example"
                            ? "Saved example"
                            : entry.origin === "service"
                              ? "Deployment evaluation"
                              : entry.origin === "local-api"
                                ? "Python-admitted import"
                                : "Imported view"}
                        </span>
                      </div>
                    </div>
                  </div>
                  <div className="run-actions">
                    {!isCampaignEntry(entry) && (
                      <button
                        className="button"
                        aria-expanded={sourceOpen}
                        aria-label={sourceOpen ? "Hide source" : "Show source"}
                        onClick={() => setSourceOpen((value) => !value)}
                      >
                        <Code2 />
                        Source
                      </button>
                    )}
                    <IconButton
                      label="Download original report"
                      onClick={() => {
                        if (
                          window.confirm(
                            "The original report may contain local paths, environment details or private data. Download it?",
                          )
                        )
                          download(entry.filename, entry.raw);
                      }}
                    >
                      <Download />
                    </IconButton>
                  </div>
                </div>
                {![
                  "current_inventory",
                  "suite_inventory",
                  "campaign_inventory",
                  "summary_inventory",
                ].includes(entry.view.admission.status) && (
                  <p className="admission-banner" role="status">
                    Inventory only.{" "}
                    {entry.view.admission.reason ??
                      "Verdicts and metrics have not been admitted."}
                  </p>
                )}
                {isCampaignEntry(entry) ? (
                  <Suspense
                    fallback={<p className="empty-line">Loading campaign...</p>}
                  >
                    <CampaignWorkspace
                      key={entry.view.report_id}
                      entry={entry}
                    />
                  </Suspense>
                ) : (
                  <PanelGroup
                    direction="horizontal"
                    className={`workspace-panels report-workspace ${sourceOpen ? "with-source" : ""}`}
                    aria-label="Results and source workspace"
                  >
                    <Panel minSize={40} className="report-panel">
                      <Evidence
                        entry={entry as ReportEntry}
                        tab={route.tab}
                        onTabChange={(tab) => setTab(tab)}
                        selectedTask={route.task}
                        onTaskChange={(task) => setTab("tasks", task)}
                      />
                    </Panel>
                    {sourceOpen && (
                      <>
                        <PanelResizeHandle
                          className="resize-handle"
                          aria-label="Resize results and source"
                        >
                          <GripVertical />
                        </PanelResizeHandle>
                        <Panel
                          defaultSize={36}
                          minSize={25}
                          maxSize={55}
                          className="source-panel"
                        >
                          <SourcePane
                            key={entry.view.report_id}
                            view={entry.view}
                            source={sources[entry.view.report_id]}
                            onAttach={(source) =>
                              setSources((previous) => ({
                                ...previous,
                                [entry.view.report_id]: source,
                              }))
                            }
                            onRemove={() =>
                              setSources((previous) => {
                                const next = { ...previous };
                                delete next[entry.view.report_id];
                                return next;
                              })
                            }
                          />
                        </Panel>
                      </>
                    )}
                  </PanelGroup>
                )}
                <footer className="workspace-footer">
                  <span>
                    <ShieldCheck />
                    Software evidence, not attested execution
                  </span>
                  <span>View {entry.view.view_schema_version}</span>
                </footer>
              </>
            )}
          {route.page === "report" && !entry && (
            <div className="page-content empty-state">
              <FolderOpen />
              <h2>This report is not loaded</h2>
              <p>
                Imported reports and source attachments stay in browser memory.
              </p>
              <button className="button" onClick={() => setImportOpen(true)}>
                <FolderOpen />
                Open report
              </button>
            </div>
          )}
        </main>
        <ConnectionDialog
          open={connectionOpen}
          onOpenChange={setConnectionOpen}
        />
        <ImportDialog
          open={importOpen}
          onOpenChange={setImportOpen}
          onImport={imported}
        />
      </div>
    </Tooltip.Provider>
  );
}
