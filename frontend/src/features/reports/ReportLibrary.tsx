import { useState } from "react";
import {
  ChartNoAxesCombined,
  FileJson2,
  FolderOpen,
  Search,
} from "lucide-react";
import { isCampaignEntry, type WorkspaceEntry } from "../../api/reports";
import { navigate } from "../../app/navigation";
import { Status } from "../../components/ui";

export function ReportLibrary({
  entries,
  kind,
  onImport,
}: {
  entries: WorkspaceEntry[];
  kind: "campaigns" | "examples";
  onImport: () => void;
}) {
  const [query, setQuery] = useState("");
  const choices = entries.filter((entry) =>
    kind === "examples" ? entry.origin === "example" : isCampaignEntry(entry),
  );
  const visible = choices.filter((entry) =>
    `${entry.view.identity.name} ${entry.view.identity.engine} ${entry.view.identity.case ?? ""}`
      .toLowerCase()
      .includes(query.toLowerCase()),
  );
  return (
    <div className="page-content">
      <div className="page-heading">
        <div>
          <span className="eyebrow">
            {kind === "examples" ? "Saved evidence" : "Load and suite studies"}
          </span>
          <h2>{kind === "examples" ? "Examples" : "Campaigns"}</h2>
          <p className="page-subtitle">
            {kind === "examples"
              ? "Sanitized reports from previous benchmark executions"
              : "Independent sessions, data loads and planned coverage"}
          </p>
        </div>
        <button className="button" onClick={onImport}>
          <FolderOpen />
          Open report
        </button>
      </div>
      <label className="search-field library-search">
        <Search />
        <input
          type="search"
          aria-label="Filter reports and campaigns"
          placeholder="Filter by pipeline, engine or case"
          value={query}
          onChange={(event) => setQuery(event.target.value)}
        />
      </label>
      {!visible.length ? (
        <div className="empty-state">
          <ChartNoAxesCombined />
          <h3>No matching reports</h3>
        </div>
      ) : (
        <div className="table-scroll">
          <table className="library-table">
            <thead>
              <tr>
                <th>Pipeline / report</th>
                <th>Engine</th>
                <th>Case or scope</th>
                <th>Origin / output verdict</th>
              </tr>
            </thead>
            <tbody>
              {visible.map((entry) => (
                <tr key={entry.view.report_id}>
                  <td>
                    <button
                      className="row-link library-link"
                      aria-label={`${entry.view.identity.name.replaceAll("_", " ")}${entry.view.identity.case ? ` ${entry.view.identity.case.replaceAll("_", " ")}` : ""}`}
                      onClick={() => navigate(`report/${entry.view.report_id}`)}
                    >
                      {isCampaignEntry(entry) ? (
                        <ChartNoAxesCombined />
                      ) : (
                        <FileJson2 />
                      )}
                      {entry.view.identity.name.replaceAll("_", " ")}
                    </button>
                  </td>
                  <td>{entry.view.identity.engine}</td>
                  <td>
                    {isCampaignEntry(entry)
                      ? entry.view.sessions.length
                        ? `${entry.view.sessions.length} sessions`
                        : `${entry.view.inventory.length} case groups`
                      : (entry.view.identity.case?.replaceAll("_", " ") ??
                        entry.view.identity.evidence_kind.replaceAll("_", " "))}
                  </td>
                  <td>
                    <span className="reason">
                      {entry.origin === "example"
                        ? "Saved example"
                        : "Imported report"}
                    </span>
                    {!isCampaignEntry(entry) && (
                      <Status value={entry.view.outputs.status} />
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
