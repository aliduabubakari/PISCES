import { useState } from "react";
import * as Tabs from "@radix-ui/react-tabs";
import {
  Activity,
  CircleAlert,
  CircleCheck,
  Clock3,
  Cpu,
  FileCheck2,
  Fingerprint,
  Gauge,
  GitCompareArrows,
  MemoryStick,
  Workflow,
} from "lucide-react";
import type { ReportView } from "../../api/generated/report-view";
import type { ReportEntry } from "../../api/reports";
import type { ReportTab } from "../../app/navigation";
import { Status } from "../../components/ui";
import { formatNumber } from "../../lib/format";
import { explain, scopeNames } from "../../lib/presentation";

type Metric = ReportView["metrics"][number];
export function formatMetric(metric: Metric) {
  return metric.value === null
    ? "\u2014"
    : `${formatNumber(metric.value, metric.unit === "bytes" ? 0 : 3)} ${metric.unit}`;
}
function Reason({ code }: { code: string }) {
  const message = explain(code);
  return (
    <>
      {message && <span>{message}</span>}
      <code className="reason-code">{code}</code>
    </>
  );
}
function ResourceSummary({
  view,
  onMetrics,
}: {
  view: ReportView;
  onMetrics: () => void;
}) {
  const prefix = view.metrics.some(
    (metric) =>
      metric.scope === "container_worker" &&
      metric.availability === "available",
  )
    ? "worker"
    : "execution";
  const values = ["wall", "cpu", "memory"].map((id) =>
    view.metrics.find((metric) => metric.id === `${prefix}.${id}`),
  );
  return (
    <section className="resource-summary" aria-label="Resource summary">
      <div className="section-heading">
        <h3>Resource observations</h3>
        <button className="text-button" onClick={onMetrics}>
          All metrics
        </button>
      </div>
      <div className="summary-readings">
        {values.map((metric, index) => {
          const Icon = [Clock3, Cpu, MemoryStick][index];
          return (
            <div key={index} className={`summary-reading signal-${index}`}>
              <span>
                <Icon />
                {metric?.label ??
                  ["Wall time", "CPU time", "Peak memory"][index]}
              </span>
              <strong>
                {metric && metric.value !== null
                  ? formatMetric(metric)
                  : "Not supplied"}
              </strong>
              <small>
                {metric?.scope === "container_worker"
                  ? index === 2
                    ? "Container lifetime peak"
                    : "Container worker window"
                  : metric?.scope === "execution_block"
                    ? index === 2
                      ? "Process lifetime peak"
                      : "Execution block"
                    : "No admitted observation"}
              </small>
            </div>
          );
        })}
      </div>
    </section>
  );
}
function Overview({
  view,
  onTabChange,
}: {
  view: ReportView;
  onTabChange: (value: ReportTab) => void;
}) {
  const output = view.outputs;
  return (
    <>
      <section
        className={`verdict-band ${output.status === "passed" ? "success" : output.status === "failed" ? "failure" : ""}`}
      >
        {output.status === "passed" ? <CircleCheck /> : <CircleAlert />}
        <div>
          <span className="eyebrow">Output verification</span>
          <h2>
            {output.status === "passed"
              ? "Required outputs verified"
              : output.status === "failed"
                ? "Output verification failed"
                : "Outputs not verified"}
          </h2>
        </div>
      </section>
      <ResourceSummary view={view} onMetrics={() => onTabChange("metrics")} />
      <section className="evidence-section">
        <h3>
          Output checks <span className="count">{output.checks.length}</span>
        </h3>
        {output.checks.length ? (
          <div className="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>Output</th>
                  <th className="numeric">Rows</th>
                  <th>Verdict</th>
                </tr>
              </thead>
              <tbody>
                {output.checks.map((check) => (
                  <tr key={check.pointer}>
                    <td>
                      {check.name}
                      {check.reason && (
                        <span className="reason bad">
                          <Reason code={check.reason} />
                        </span>
                      )}
                    </td>
                    <td className="numeric">{check.row_count ?? "\u2014"}</td>
                    <td>
                      <Status value={check.status} />
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <p className="empty-line">No output assertion evidence attached.</p>
        )}
      </section>
      <section className="task-overview">
        <div>
          <Workflow />
          <div>
            <h3>
              {view.tasks.length} observed{" "}
              {view.tasks.length === 1 ? "task" : "tasks"}
            </h3>
            <p>
              {view.tasks.filter((task) => task.status === "executed").length}{" "}
              reported as executed
            </p>
          </div>
        </div>
        <button className="button" onClick={() => onTabChange("tasks")}>
          Inspect tasks
        </button>
      </section>
      {output.expected_behavior_matched !== null && (
        <details className="technical-details">
          <summary>
            Benchmark expectation{" "}
            {output.expected_behavior_matched ? "matched" : "not matched"}
          </summary>
          <p>
            This describes the expected benchmark case, independently of output
            correctness.
          </p>
        </details>
      )}
    </>
  );
}
function TaskInspector({
  view,
  selectedTask,
  onTaskChange,
}: {
  view: ReportView;
  selectedTask?: string;
  onTaskChange: (id: string) => void;
}) {
  const task =
    view.tasks.find((item) => item.task_id === selectedTask) ?? view.tasks[0];
  return (
    <>
      <div className="section-heading">
        <div>
          <h2>Task observations</h2>
          <p className="page-subtitle">{view.tasks.length} observed tasks</p>
        </div>
        <span className="scope-badge">State evidence</span>
      </div>
      {!task ? (
        <div className="empty-state">
          <Workflow />
          <h3>No task observations supplied</h3>
        </div>
      ) : (
        <div className="task-layout">
          <div className="table-scroll">
            <table className="task-table">
              <thead>
                <tr>
                  <th>Task</th>
                  <th>State</th>
                  <th>Performance coverage</th>
                </tr>
              </thead>
              <tbody>
                {view.tasks.map((item) => (
                  <tr
                    className={
                      task.pointer === item.pointer ? "selected-row" : ""
                    }
                    key={item.pointer}
                  >
                    <td>
                      <button
                        className="row-link"
                        aria-pressed={task.pointer === item.pointer}
                        onClick={() => onTaskChange(item.task_id)}
                      >
                        {item.task_id}
                      </button>
                    </td>
                    <td>
                      <Status value={item.status} />
                    </td>
                    <td>
                      <span className="coverage-label">State only</span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <section className="task-inspector" aria-label="Selected task">
            <div className="section-heading">
              <h3>{task.task_id}</h3>
              <Workflow />
            </div>
            <dl className="task-readings">
              <div>
                <dt>Task status</dt>
                <dd>
                  <Status value={task.status} />
                </dd>
              </div>
              <div>
                <dt>Active duration</dt>
                <dd>Not supplied</dd>
              </div>
              <div>
                <dt>Task CPU time</dt>
                <dd>Not supplied</dd>
              </div>
              <div>
                <dt>Task peak memory</dt>
                <dd>Not supplied</dd>
              </div>
            </dl>
            <p className="notice">
              Task performance fields are not supplied by this view. Pipeline
              totals are not allocated to individual tasks.
            </p>
            <details className="technical-details">
              <summary>Task provenance</summary>
              <dl className="provenance">
                <dt>Task ID</dt>
                <dd>{task.task_id}</dd>
                <dt>Engine state</dt>
                <dd>
                  <code>{task.state}</code>
                </dd>
                <dt>Observation</dt>
                <dd>{task.status}</dd>
                <dt>JSON pointer</dt>
                <dd>
                  <code>{task.pointer}</code>
                </dd>
              </dl>
            </details>
          </section>
        </div>
      )}
      <p className="footnote">
        Dependency edges and source locations are not available in this report.
        No dependency graph or critical path has been inferred.
      </p>
    </>
  );
}
function Metrics({ view }: { view: ReportView }) {
  const [scope, setScope] = useState("container_worker");
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const metrics = view.metrics.filter((metric) => metric.scope === scope);
  const selected =
    metrics.find((metric) => metric.id === selectedId) ?? metrics[0];
  return (
    <>
      <div className="section-heading">
        <h2>Metrics</h2>
        <label className="select-label">
          Scope
          <select
            aria-label="Measurement scope"
            value={scope}
            onChange={(event) => setScope(event.target.value)}
          >
            {Object.entries(scopeNames).map(([value, label]) => (
              <option key={value} value={value}>
                {label}
              </option>
            ))}
          </select>
        </label>
      </div>
      <p className="notice">
        Separate measurement scopes. Values are not additive.
      </p>
      {metrics.length ? (
        <div className="table-scroll">
          <table className="metric-table">
            <thead>
              <tr>
                <th>Metric</th>
                <th className="numeric">Value</th>
                <th>Availability</th>
              </tr>
            </thead>
            <tbody>
              {metrics.map((metric) => (
                <tr
                  key={metric.id}
                  className={metric.id === selected?.id ? "selected-row" : ""}
                >
                  <td>
                    <button
                      className="text-button"
                      aria-pressed={metric.id === selected?.id}
                      onClick={() => setSelectedId(metric.id)}
                    >
                      {metric.label}
                    </button>
                  </td>
                  <td className="numeric">{formatMetric(metric)}</td>
                  <td>
                    <Status value={metric.availability} />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <p className="empty-line">No admitted metrics in this scope.</p>
      )}
      {selected && (
        <section className="metric-inspector" aria-live="polite">
          <h3>{selected.label}</h3>
          <p>{selected.definition}</p>
          <dl>
            <dt>Scope</dt>
            <dd>{scopeNames[selected.scope] ?? selected.scope}</dd>
            <dt>Includes</dt>
            <dd>{selected.includes}</dd>
            <dt>Evidence role</dt>
            <dd>{selected.evidence_role}</dd>
          </dl>
          {selected.reason_codes.length > 0 && (
            <div className="missing-reasons">
              {selected.reason_codes.map((code) => (
                <p key={code}>
                  <Reason code={code} />
                </p>
              ))}
            </div>
          )}
          <details className="technical-details">
            <summary>Measurement provenance</summary>
            <dl className="provenance">
              <dt>Source</dt>
              <dd>{selected.source}</dd>
              <dt>JSON pointer</dt>
              <dd>
                <code>{selected.pointer ?? "No field collected"}</code>
              </dd>
              {selected.raw_value !== null && (
                <>
                  <dt>Raw reading</dt>
                  <dd>
                    {selected.raw_value} {selected.raw_unit}
                  </dd>
                </>
              )}
            </dl>
          </details>
        </section>
      )}
      <p className="footnote">
        Task-body resource readings are not inferred from worker totals.
      </p>
    </>
  );
}
function Compare({
  view,
  onMetrics,
}: {
  view: ReportView;
  onMetrics: () => void;
}) {
  const comparison = view.comparison;
  return (
    <>
      <div className="comparison-heading">
        <GitCompareArrows />
        <div>
          <span className="eyebrow">Comparison eligibility</span>
          <h2>
            {comparison.resource_eligible === true
              ? "Report claims resource eligibility"
              : comparison.resource_eligible === false
                ? "Resource ranking blocked"
                : "Resource eligibility not established"}
          </h2>
        </div>
      </div>
      <p className="notice">No pair is evaluated or ranked by this viewer.</p>
      <dl className="gate-list">
        {[
          ["Output verification", view.outputs.status],
          ["Pairwise equivalence", comparison.pairwise_status],
          [
            "Screening eligibility",
            comparison.screening_eligible === null
              ? "Not reported"
              : comparison.screening_eligible
                ? "Eligible for screening"
                : "Not eligible",
          ],
          [
            "Resource eligibility",
            comparison.resource_eligible === null
              ? "Not reported"
              : comparison.resource_eligible
                ? "Report claims eligible"
                : "Not eligible",
          ],
        ].map(([label, value]) => (
          <div key={label}>
            <dt>{label}</dt>
            <dd>{value.replaceAll("_", " ")}</dd>
          </div>
        ))}
      </dl>
      <section className="evidence-section">
        <h3>Requirements and blockers</h3>
        {comparison.reasons.length ? (
          <ul className="reason-list">
            {comparison.reasons.map((code) => (
              <li key={code}>
                <CircleAlert />
                <div>
                  <Reason code={code} />
                </div>
              </li>
            ))}
          </ul>
        ) : (
          <p className="empty-line">No reason code supplied.</p>
        )}
      </section>
      <button className="button" onClick={onMetrics}>
        <Gauge />
        Inspect available metrics
      </button>
      {comparison.pointer && (
        <details className="technical-details">
          <summary>Eligibility provenance</summary>
          <code>{comparison.pointer}</code>
        </details>
      )}
    </>
  );
}
export function Evidence({
  entry,
  tab,
  onTabChange,
  selectedTask,
  onTaskChange,
}: {
  entry: ReportEntry;
  tab: ReportTab;
  onTabChange: (value: ReportTab) => void;
  selectedTask?: string;
  onTaskChange: (id: string) => void;
}) {
  const { view } = entry;
  const worker = view.metrics.filter(
    (metric) => metric.scope === "container_worker",
  );
  const count = worker.filter(
    (metric) => metric.availability === "available",
  ).length;
  return (
    <Tabs.Root
      className="evidence-pane"
      value={tab}
      onValueChange={(value) => onTabChange(value as ReportTab)}
    >
      <div className="run-status-strip">
        <span>
          <Activity />
          Execution <Status value={view.execution.status} />
        </span>
        <span>
          <FileCheck2 />
          Outputs <Status value={view.outputs.status} />
        </span>
        <span>
          <Gauge />
          Worker telemetry{" "}
          <strong>
            {worker.length
              ? `${count}/${worker.length} readings`
              : "Not supplied"}
          </strong>
        </span>
      </div>
      <Tabs.List className="tabs" aria-label="Report view">
        {[
          ["overview", "Overview", FileCheck2],
          ["tasks", "Tasks", Workflow],
          ["metrics", "Metrics", Gauge],
          ["compare", "Compare", GitCompareArrows],
          ["evidence", "Evidence", Fingerprint],
        ].map(([value, label, Icon]) => {
          const Symbol = Icon as typeof FileCheck2;
          return (
            <Tabs.Trigger
              key={String(value)}
              className="tab"
              value={String(value)}
            >
              <Symbol />
              {String(label)}
              {value === "tasks" && (
                <span className="tab-count">{view.tasks.length}</span>
              )}
            </Tabs.Trigger>
          );
        })}
      </Tabs.List>
      <div className="evidence-scroll">
        <Tabs.Content value="overview" className="tab-content">
          <Overview view={view} onTabChange={onTabChange} />
        </Tabs.Content>
        <Tabs.Content value="tasks" className="tab-content">
          <TaskInspector
            view={view}
            selectedTask={selectedTask}
            onTaskChange={onTaskChange}
          />
        </Tabs.Content>
        <Tabs.Content value="metrics" className="tab-content">
          <Metrics view={view} />
        </Tabs.Content>
        <Tabs.Content value="compare" className="tab-content">
          <Compare view={view} onMetrics={() => onTabChange("metrics")} />
        </Tabs.Content>
        <Tabs.Content value="evidence" className="tab-content">
          <h2>Evidence provenance</h2>
          <dl className="provenance">
            <dt>Origin</dt>
            <dd>
              {entry.origin === "example"
                ? "Sanitized saved example"
                : entry.origin === "service"
                  ? "Service-produced container report; not attested"
                  : entry.origin === "local-api"
                    ? "Local Python admission"
                    : "Imported view; not revalidated by Python"}
            </dd>
            <dt>Admission</dt>
            <dd>
              {view.admission.status.replaceAll("_", " ")}
              {view.admission.reason && `: ${view.admission.reason}`}
            </dd>
            <dt>Report format</dt>
            <dd>{view.report_kind.replaceAll("_", " ")}</dd>
          </dl>
          <details className="technical-details">
            <summary>Schema versions and artifact identity</summary>
            <dl className="provenance">
              <dt>Source schema</dt>
              <dd>{view.source_schema_version ?? "Not versioned"}</dd>
              <dt>View schema</dt>
              <dd>{view.view_schema_version}</dd>
              <dt>Metric registry</dt>
              <dd>{view.metric_registry_version}</dd>
              <dt>Artifact SHA-256</dt>
              <dd>
                <code>{view.identity.artifact_sha256 ?? "Not supplied"}</code>
              </dd>
              <dt>Report ID</dt>
              <dd>
                <code>{view.report_id}</code>
              </dd>
            </dl>
          </details>
          <section className="evidence-section">
            <h3>Limitations</h3>
            <ul className="warning-list">
              {view.warnings.map((warning) => (
                <li key={warning}>{warning}</li>
              ))}
            </ul>
          </section>
        </Tabs.Content>
      </div>
    </Tabs.Root>
  );
}
