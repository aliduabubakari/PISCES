import { useEffect, useState } from "react";

export const reportTabs = [
  "overview",
  "tasks",
  "metrics",
  "compare",
  "evidence",
] as const;
export type ReportTab = (typeof reportTabs)[number];
export type Route = {
  page: "runs" | "campaigns" | "examples" | "evaluate" | "report" | "job";
  id?: string;
  tab: ReportTab;
  task?: string;
};
export function readRoute(hash = window.location.hash): Route {
  const [path, query] = hash.replace(/^#\/?/, "").split("?");
  const [page, id] = path.split("/");
  const params = new URLSearchParams(query);
  const tab = params.get("tab");
  return {
    page:
      page === "report" && /^[a-f0-9]{64}$/.test(id ?? "")
        ? "report"
        : page === "job" && /^[a-f0-9]{32}$/.test(id ?? "")
          ? "job"
          : ["campaigns", "examples", "evaluate"].includes(page)
            ? (page as Route["page"])
            : "runs",
    id,
    tab: reportTabs.includes(tab as ReportTab)
      ? (tab as ReportTab)
      : "overview",
    task: params.get("task") ?? undefined,
  };
}
export function navigate(path: string) {
  window.location.hash = "/" + path;
}
export function useRoute() {
  const [route, setRoute] = useState(readRoute);
  useEffect(() => {
    const changed = () => setRoute(readRoute());
    window.addEventListener("hashchange", changed);
    return () => window.removeEventListener("hashchange", changed);
  }, []);
  return route;
}
