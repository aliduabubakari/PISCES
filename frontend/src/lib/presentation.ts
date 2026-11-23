export const profileNames: Record<string, string> = {
  static: "Static analysis",
  python: "Python script process",
  airflow: "Airflow workflow",
  prefect: "Prefect workflow",
  dagster: "Dagster workflow",
};
export const scopeNames: Record<string, string> = {
  container_worker: "Container worker",
  execution_block: "Execution block",
  modeled: "Modeled estimates",
};
const reasons: Record<string, string> = {
  counter_file_empty: "The operating system supplied an empty counter file.",
  counter_file_unreadable: "The operating system counter could not be read.",
  worker_snapshot_lost_to_timeout:
    "The time limit was reached before the worker returned its final measurements.",
  verified_workload_units_unavailable:
    "Verified workload units are not available for resource ranking.",
  runtime_evidence_not_collected:
    "Runtime evidence was not collected for this report.",
  output_file_missing: "A required output file was not produced.",
  output_content_mismatch:
    "The output content does not match the declared reference.",
  profile_not_ready: "The selected execution profile is not ready.",
  worker_restarted:
    "The worker restarted before this evaluation finished. It was not automatically replayed.",
};
export function explain(code: string) {
  return reasons[code] ?? null;
}
export function timestamp(value: number | null) {
  return value === null
    ? "Not started"
    : new Intl.DateTimeFormat(undefined, {
        month: "short",
        day: "numeric",
        hour: "2-digit",
        minute: "2-digit",
      }).format(new Date(value * 1000));
}
