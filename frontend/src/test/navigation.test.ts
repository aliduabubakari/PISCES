import { describe, expect, it } from "vitest";
import { readRoute } from "../app/navigation";
import { explain } from "../lib/presentation";

describe("workspace navigation", () => {
  it("opens the real run inventory by default", () => {
    expect(readRoute("").page).toBe("runs");
  });
  it("restores report, task and tab without storing source", () => {
    const id = "a".repeat(64);
    const route = readRoute(`#/report/${id}?tab=tasks&task=transform`);
    expect(route).toEqual({
      page: "report",
      id,
      tab: "tasks",
      task: "transform",
    });
  });
  it("admits only bounded report and job identifiers", () => {
    expect(readRoute("#/report/../../etc").page).toBe("runs");
    expect(readRoute("#/job/" + "b".repeat(32)).page).toBe("job");
    expect(readRoute("#/report/" + "b".repeat(64) + "?tab=unknown").tab).toBe(
      "overview",
    );
  });
  it("does not turn unknown reason codes into invented causes", () => {
    expect(explain("new_reason_from_a_future_collector")).toBeNull();
    expect(explain("counter_file_empty")).toContain("empty counter file");
  });
});
