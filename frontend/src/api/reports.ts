import type { ReportView } from "./generated/report-view";
import type { CampaignView } from "./generated/campaign-view";
import { apiFetch } from "./client";

export type ReportEntry = {
  view: ReportView;
  raw: string;
  filename: string;
  origin: "example" | "local-api" | "imported-view" | "service";
};
export type CampaignEntry = Omit<ReportEntry, "view"> & { view: CampaignView };
export type WorkspaceEntry = ReportEntry | CampaignEntry;
export function isCampaignEntry(entry: WorkspaceEntry): entry is CampaignEntry {
  return "view_type" in entry.view && entry.view.view_type === "campaign";
}
function withView(
  view: ReportView | CampaignView,
  metadata: Omit<ReportEntry, "view">,
): WorkspaceEntry {
  return "view_type" in view ? { view, ...metadata } : { view, ...metadata };
}
export const MAX_REPORT_BYTES = 8 * 1024 * 1024;
export async function openReport(file: File): Promise<WorkspaceEntry> {
  const { admitWorkspaceView } = await import("./contract");
  if (file.size > MAX_REPORT_BYTES)
    throw new Error("Report exceeds the 8 MiB limit.");
  const raw = await file.text();
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new Error("The selected file is not valid JSON.");
  }
  if (
    parsed &&
    typeof parsed === "object" &&
    "bundle_format" in parsed &&
    parsed.bundle_format === "pisces-report-view"
  ) {
    const bundle = parsed as {
      view?: unknown;
      report?: unknown;
      report_json?: unknown;
    };
    if (!bundle.report || typeof bundle.report !== "object")
      throw new Error("View bundle is missing its original report.");
    return withView(admitWorkspaceView(bundle.view), {
      raw:
        typeof bundle.report_json === "string"
          ? bundle.report_json
          : JSON.stringify(bundle.report, null, 2),
      filename: file.name,
      origin: "imported-view",
    });
  }
  let response: Response;
  try {
    response = await apiFetch("/api/v1/reports/view", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: raw,
      signal: AbortSignal.timeout(15000),
    });
  } catch {
    throw new Error(
      "Local report API unavailable. Start pisces serve-reports or open a report-view bundle.",
    );
  }
  if (!response.ok)
    throw new Error(
      response.status === 422
        ? "The Python API rejected malformed report evidence."
        : "Local report API unavailable or rejected the upload.",
    );
  return withView(admitWorkspaceView(await response.json()), {
    raw,
    filename: file.name,
    origin: "local-api",
  });
}

export function download(
  name: string,
  contents: string | Blob,
  type = "application/json",
) {
  const url = URL.createObjectURL(
    typeof contents === "string" ? new Blob([contents], { type }) : contents,
  );
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = name;
  anchor.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

export async function sourceDigest(bytes: ArrayBuffer): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return Array.from(new Uint8Array(digest), (value) =>
    value.toString(16).padStart(2, "0"),
  ).join("");
}
