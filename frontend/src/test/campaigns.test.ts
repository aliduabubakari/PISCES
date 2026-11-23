import { describe, expect, it } from "vitest";
import campaigns from "./fixtures/campaigns.json";
import { admitWorkspaceView } from "../api/contract";
import { chartPoints } from "../features/campaigns/CampaignWorkspace";
import { formatNumber } from "../lib/format";

describe("campaign view contract", () => {
  it("admits independent saved sessions and suite inventory", () => {
    const views = campaigns.map((example) => admitWorkspaceView(example.view));
    expect(views.map((view) => view.report_kind)).toEqual([
      "scaling_sessions",
      "suite_summary",
    ]);
  });
  it("preserves missing points as gaps rather than zeros", () => {
    const view = admitWorkspaceView(campaigns[0].view);
    if (!("view_type" in view)) throw new Error("Expected campaign");
    const metric = structuredClone(view.sessions[0].metrics[0]);
    metric.points[1].median = null;
    const data = chartPoints(
      metric,
      metric.points.map((point) => point.rows),
      false,
    );
    expect(data.map((point) => point.rows)).toEqual([
      10000, 100000, 500000, 1000000,
    ]);
    expect(data[1].median).toBeNull();
    expect(data.every((point) => point.fitted === null)).toBe(true);
  });
  it("filters visibility without changing source denominators or fitting data", () => {
    const view = admitWorkspaceView(campaigns[0].view);
    if (!("view_type" in view)) throw new Error("Expected campaign");
    const metric = view.sessions[0].metrics[0];
    const filtered = chartPoints(metric, [10000, 1000000], true);
    expect(filtered).toHaveLength(2);
    expect(filtered.every((point) => point.planned === 3)).toBe(true);
    expect(metric.points).toHaveLength(4);
    expect(filtered[1].fitted).toBe(metric.points[3].fitted);
  });
  it("keeps nonzero dispersion distinct from an actual zero", () => {
    expect(formatNumber(0.00026225)).toBe("0.000262");
    expect(formatNumber(0)).toBe("0");
    expect(formatNumber(null)).toBe("\u2014");
    expect(formatNumber(1e-10)).toBe("1.00e-10");
  });
  it("rejects malformed versions and non-finite values", () => {
    expect(() =>
      admitWorkspaceView({ ...campaigns[0].view, view_schema_version: "99" }),
    ).toThrow();
    const view = structuredClone(campaigns[0].view);
    view.sessions[0].metrics[0].points[0].median = Infinity;
    expect(() => admitWorkspaceView(view)).toThrow();
  });
});
