import { apiFetch, apiJSON } from "../../api/client";
import { sourceDigest, type ReportEntry } from "../../api/reports";
import type { SourceAttachment } from "../workspace/SourcePane";

export async function loadServiceReport(
  id: string,
): Promise<{ entry: ReportEntry; source: SourceAttachment }> {
  const { admitView } = await import("../../api/contract");
  const [response, view, attachment] = await Promise.all([
    apiFetch(`/api/v1/jobs/${id}/report`),
    apiJSON<unknown>(`/api/v1/jobs/${id}/view`),
    apiJSON<{ filename: string; source: string }>(`/api/v1/jobs/${id}/source`),
  ]);
  if (!response.ok) throw new Error("The job report is not available.");
  const bytes = new TextEncoder().encode(attachment.source).buffer;
  return {
    entry: {
      view: admitView(view),
      raw: await response.text(),
      filename: `${id}.json`,
      origin: "service",
    },
    source: {
      name: attachment.filename,
      text: attachment.source,
      bytes,
      digest: await sourceDigest(bytes),
    },
  };
}
