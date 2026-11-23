import { describe, it, expect } from "vitest";
import {
  inputProblems,
  requiredPaths,
} from "../features/evaluations/inputChecks";

const contract = JSON.stringify({
  schema_version: "2.0",
  contract_id: "events",
  inputs: [{ fixture: "fixtures/events.csv" }],
  outputs: [{ reference: { path: "references/result.csv" } }],
});
const binding = JSON.stringify({
  schema_version: "2.0",
  contract_id: "events",
  target: "dagster",
  artifact_path: "valid.py",
  artifact_sha256: "a".repeat(64),
});
const bundle = {
  profile: "dagster",
  artifact: { filename: "valid.py", digest: "a".repeat(64) },
  contract,
  binding,
  files: [
    { path: "fixtures/events.csv", content_base64: "eA==" },
    { path: "references/result.csv", content_base64: "eA==" },
  ],
};
describe("evaluation input feedback", () => {
  it("preserves declared paths without resolving host files", () =>
    expect(requiredPaths(contract)).toEqual([
      "fixtures/events.csv",
      "references/result.csv",
    ]));
  it("accepts matching identities without claiming output verification", () =>
    expect(inputProblems(bundle)).toEqual([]));
  it("explains mismatched source and missing inputs before submission", () => {
    expect(
      inputProblems({
        ...bundle,
        artifact: { filename: "wrong.py", digest: "b".repeat(64) },
        files: [],
      }),
    ).toEqual(
      expect.arrayContaining([
        expect.stringContaining("exact bytes"),
        expect.stringContaining("Missing declared"),
      ]),
    );
  });
  it("rejects ambiguous or escaping fixture paths", () => {
    expect(
      inputProblems({
        ...bundle,
        files: [{ path: "../private.csv", content_base64: "eA==" }],
      }),
    ).toEqual(
      expect.arrayContaining([expect.stringContaining("confined relative")]),
    );
    expect(
      inputProblems({ ...bundle, files: [...bundle.files, bundle.files[0]] }),
    ).toEqual(expect.arrayContaining([expect.stringContaining("unique")]));
  });
  it("requires the selected engine and treats malformed documents as errors", () => {
    expect(inputProblems({ ...bundle, profile: "prefect" })).toEqual(
      expect.arrayContaining([
        expect.stringContaining("different execution engine"),
      ]),
    );
    expect(inputProblems({ ...bundle, binding: "{" })).toEqual([
      expect.stringContaining("valid JSON"),
    ]);
  });
});
