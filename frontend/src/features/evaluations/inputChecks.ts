import type { FixtureFile } from "../../api/generated/jobs";

export function requiredPaths(contract: string): string[] {
  const value = JSON.parse(contract);
  return [
    ...(Array.isArray(value.inputs) ? value.inputs : []).map(
      (item: { fixture?: unknown }) => item.fixture,
    ),
    ...(Array.isArray(value.outputs) ? value.outputs : []).map(
      (item: { reference?: { path?: unknown } }) => item.reference?.path,
    ),
  ].filter((path): path is string => typeof path === "string");
}

// Early form feedback only; backend contract and byte-hash validation is authoritative.
export function inputProblems({
  profile,
  artifact,
  contract,
  binding,
  files,
}: {
  profile: string;
  artifact: { filename: string; digest: string } | null;
  contract: string | null;
  binding: string | null;
  files: FixtureFile[];
}): string[] {
  if (["static", "python"].includes(profile) || !contract || !binding)
    return [];
  try {
    const intent = JSON.parse(contract),
      association = JSON.parse(binding);
    const problems: string[] = [];
    if (intent.schema_version !== "2.0" || association.schema_version !== "2.0")
      problems.push(
        "Native execution requires a version 2 workload contract and binding.",
      );
    if (association.target !== profile)
      problems.push("The binding targets a different execution engine.");
    if (intent.contract_id !== association.contract_id)
      problems.push("The contract and binding identifiers do not match.");
    if (
      artifact &&
      (association.artifact_path !== artifact.filename ||
        association.artifact_sha256 !== artifact.digest)
    )
      problems.push(
        "The binding does not match the selected script's filename and exact bytes.",
      );
    if (new Set(files.map((file) => file.path)).size !== files.length)
      problems.push(
        "Each uploaded file needs a unique contract-relative path.",
      );
    if (
      files.some(
        (file) =>
          file.path.startsWith(".") ||
          file.path.includes("\\") ||
          file.path.includes(":") ||
          file.path.includes("\0") ||
          file.path
            .split("/")
            .some((part) => !part || part === "." || part === "..") ||
          file.path.split("/").length > 5 ||
          !/\.(csv|json|txt)$/i.test(file.path) ||
          file.path === "contract.json" ||
          file.path.length > 160,
      )
    )
      problems.push(
        "Fixture paths must be confined relative CSV, JSON or text paths, not contract.json.",
      );
    const supplied = new Set(files.map((file) => file.path));
    const missing = requiredPaths(contract).filter(
      (path) => !supplied.has(path),
    );
    if (missing.length)
      problems.push(
        `Missing declared input/reference files: ${missing.join(", ")}.`,
      );
    return problems;
  } catch {
    return ["The contract or binding could not be read as valid JSON."];
  }
}
