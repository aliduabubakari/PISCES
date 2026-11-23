import { useState } from "react";
import {
  Activity,
  Clock3,
  FileJson2,
  FolderOpen,
  LoaderCircle,
  Play,
  PlugZap,
  RefreshCw,
  Search,
  Square,
  Trash2,
} from "lucide-react";
import { apiJSON } from "../../api/client";
import type { WorkspaceEntry } from "../../api/reports";
import { isCampaignEntry } from "../../api/reports";
import { IconButton, Status } from "../../components/ui";
import { navigate } from "../../app/navigation";
import { explain, profileNames, timestamp } from "../../lib/presentation";
import { useService } from "./ServiceProvider";

export function RunsPage({
  entries,
  onImport,
  onConnect,
  onClose,
}: {
  entries: WorkspaceEntry[];
  onImport: () => void;
  onConnect: () => void;
  onClose: (id: string) => void;
}) {
  const service = useService();
  const [query, setQuery] = useState("");
  const [state, setState] = useState("all");
  const [busy, setBusy] = useState<string | null>(null);
  const [error, setError] = useState("");
  const active = service.jobs.filter((job) =>
    ["queued", "running"].includes(job.state),
  );
  const visible = service.jobs.filter(
    (job) =>
      (state === "all" ||
        (state === "active"
          ? ["queued", "running"].includes(job.state)
          : job.state === state)) &&
      `${job.filename} ${job.profile} ${job.id}`
        .toLowerCase()
        .includes(query.toLowerCase()),
  );
  const local = entries.filter(
    (entry) => entry.origin !== "example" && !isCampaignEntry(entry),
  );
  async function action(id: string, kind: "cancel" | "delete") {
    if (
      kind === "delete" &&
      !window.confirm("Delete this job, its uploaded source and saved report?")
    )
      return;
    setBusy(id);
    setError("");
    try {
      await apiJSON(`/api/v1/jobs/${id}${kind === "cancel" ? "/cancel" : ""}`, {
        method: kind === "cancel" ? "POST" : "DELETE",
      });
      await service.refresh();
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Job action failed.");
    } finally {
      setBusy(null);
    }
  }
  return (
    <div className="page-content">
      <div className="page-heading">
        <div>
          <span className="eyebrow">Evaluation workspace</span>
          <h2>Runs</h2>
          <p className="page-subtitle">
            {service.capabilities?.execution
              ? `${service.jobs.length} retained evaluations`
              : "Evaluation history and imported reports"}
          </p>
        </div>
        <button className="button" onClick={onImport}>
          <FolderOpen />
          Open report
        </button>
      </div>
      <div className="run-totals" aria-label="Run inventory summary">
        <div>
          <Activity />
          <span>Active evaluations</span>
          <strong>
            {service.capabilities?.execution
              ? active.length
              : service.capabilities
                ? "Not supported"
                : "Not connected"}
          </strong>
        </div>
        <div>
          <FileJson2 />
          <span>Reports available</span>
          <strong>
            {service.capabilities?.execution
              ? service.jobs.filter((job) => job.has_report).length
              : service.capabilities
                ? "Not supported"
                : "Not connected"}
          </strong>
        </div>
        <div>
          <Clock3 />
          <span>Last synchronized</span>
          <strong>
            {service.updatedAt
              ? new Intl.DateTimeFormat(undefined, {
                  hour: "2-digit",
                  minute: "2-digit",
                  second: "2-digit",
                }).format(service.updatedAt)
              : "Not synchronized"}
          </strong>
        </div>
      </div>
      <section className="list-section">
        <div className="section-heading">
          <h3>Deployment evaluations</h3>
          <IconButton
            label="Refresh jobs and profiles"
            disabled={!service.capabilities || busy !== null}
            onClick={() =>
              void service
                .refresh()
                .catch(() =>
                  setError("Could not refresh deployment evaluations."),
                )
            }
          >
            <RefreshCw />
          </IconButton>
        </div>
        {!service.capabilities ? (
          <div className="empty-state">
            <PlugZap />
            <h3>Connect to your deployment</h3>
            <p>Job history has not been loaded.</p>
            <button className="button" onClick={onConnect}>
              <PlugZap />
              Connect deployment
            </button>
          </div>
        ) : !service.capabilities.execution ? (
          <div className="empty-state">
            <FileJson2 />
            <h3>Report-only API</h3>
            <p>This deployment does not accept execution jobs.</p>
          </div>
        ) : (
          <>
            <div className="list-toolbar">
              <label className="search-field">
                <Search />
                <input
                  type="search"
                  aria-label="Search evaluations"
                  placeholder="Search by script, engine or job ID"
                  value={query}
                  onChange={(event) => setQuery(event.target.value)}
                />
              </label>
              <select
                aria-label="Job status filter"
                value={state}
                onChange={(event) => setState(event.target.value)}
              >
                <option value="all">All states</option>
                <option value="active">Active</option>
                {["completed", "failed", "cancelled", "timed_out"].map(
                  (item) => (
                    <option key={item} value={item}>
                      {item.replaceAll("_", " ")}
                    </option>
                  ),
                )}
              </select>
            </div>
            {error && (
              <p role="alert" className="import-error">
                {error}
              </p>
            )}
            {!service.jobs.length ? (
              <div className="empty-state">
                <Play />
                <h3>No evaluations yet</h3>
                <button
                  className="button primary"
                  onClick={() => navigate("evaluate")}
                >
                  <Play />
                  New evaluation
                </button>
              </div>
            ) : !visible.length ? (
              <p className="empty-line">No evaluations match these filters.</p>
            ) : (
              <div className="table-scroll">
                <table className="runs-table">
                  <thead>
                    <tr>
                      <th>Script / profile</th>
                      <th>Job state</th>
                      <th>Report</th>
                      <th>Submitted</th>
                      <th>
                        <span className="visually-hidden">Actions</span>
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {visible.map((job) => (
                      <tr key={job.id}>
                        <td>
                          <button
                            className="row-link"
                            onClick={() => navigate(`job/${job.id}`)}
                          >
                            {job.filename}
                          </button>
                          <span className="reason">
                            {profileNames[job.profile] ?? job.profile}
                          </span>
                          <code className="job-id">{job.id.slice(0, 12)}</code>
                        </td>
                        <td>
                          <span className="inline-status">
                            {job.state === "running" && (
                              <LoaderCircle className="spin" />
                            )}
                            <Status value={job.state} />
                          </span>
                          {job.cancel_requested &&
                            ["queued", "running"].includes(job.state) && (
                              <span className="reason">
                                Cancellation requested
                              </span>
                            )}
                          {job.error && (
                            <span className="reason bad">
                              {explain(job.error) ??
                                job.error.replaceAll("_", " ")}
                            </span>
                          )}
                        </td>
                        <td>
                          {job.has_report ? (
                            <button
                              className="text-button"
                              aria-label={`View report for ${job.filename}`}
                              onClick={() => navigate(`job/${job.id}`)}
                            >
                              Open report
                            </button>
                          ) : (
                            "Not available"
                          )}
                          {job.evaluation_succeeded === false && (
                            <span className="reason bad">
                              Evaluation reported failure
                            </span>
                          )}
                        </td>
                        <td>
                          <time
                            dateTime={new Date(
                              job.created_at * 1000,
                            ).toISOString()}
                          >
                            {timestamp(job.created_at)}
                          </time>
                        </td>
                        <td>
                          {["queued", "running"].includes(job.state) ? (
                            <IconButton
                              label={`Cancel ${job.filename}`}
                              disabled={busy === job.id || job.cancel_requested}
                              onClick={() => void action(job.id, "cancel")}
                            >
                              <Square />
                            </IconButton>
                          ) : (
                            <IconButton
                              label={`Delete ${job.filename}`}
                              disabled={busy === job.id}
                              onClick={() => void action(job.id, "delete")}
                            >
                              <Trash2 />
                            </IconButton>
                          )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </>
        )}
      </section>
      <section className="list-section">
        <div className="section-heading">
          <h3>
            Opened reports <span className="count">{local.length}</span>
          </h3>
        </div>
        {local.length ? (
          <div className="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>Report</th>
                  <th>Engine</th>
                  <th>Origin</th>
                  <th>
                    <span className="visually-hidden">Actions</span>
                  </th>
                </tr>
              </thead>
              <tbody>
                {local.map((entry) => (
                  <tr key={entry.view.report_id}>
                    <td>
                      <button
                        className="row-link"
                        onClick={() =>
                          navigate(`report/${entry.view.report_id}`)
                        }
                      >
                        {entry.view.identity.name.replaceAll("_", " ")}
                      </button>
                    </td>
                    <td>{entry.view.identity.engine}</td>
                    <td>
                      {entry.origin === "service"
                        ? "Deployment copy"
                        : "Imported report"}
                    </td>
                    <td>
                      <IconButton
                        label={`Close ${entry.view.identity.name}`}
                        onClick={() => onClose(entry.view.report_id)}
                      >
                        <Trash2 />
                      </IconButton>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <p className="empty-line">
            No reports opened in this browser session.
          </p>
        )}
      </section>
    </div>
  );
}
