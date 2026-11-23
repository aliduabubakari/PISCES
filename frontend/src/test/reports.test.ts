import { describe, expect, it, vi, afterEach } from "vitest";
import examples from "./fixtures/examples.json";
import { admitView } from "../api/contract";
import { openReport, sourceDigest } from "../api/reports";
import { formatMetric } from "../features/reports/Evidence";

afterEach(() => vi.unstubAllGlobals());

describe("Python-owned view contract", () => {
  it("admits saved view snapshots without reinterpreting verdicts", () => {
    expect(
      examples.map((example) => admitView(example.view).outputs.status),
    ).toEqual(["passed", "failed", "failed"]);
  });
  it("rejects unsupported versions and missing fields", () => {
    expect(() =>
      admitView({ ...examples[0].view, view_schema_version: "99" }),
    ).toThrow();
    expect(() => admitView({})).toThrow();
  });
  it("keeps a real zero distinct from missing I/O", () => {
    const metrics = admitView(examples[0].view).metrics;
    expect(
      formatMetric(
        metrics.find((metric) => metric.id === "worker.throttling")!,
      ),
    ).toBe("0 ms");
    expect(
      formatMetric(metrics.find((metric) => metric.id === "worker.read")!),
    ).toBe("\u2014");
  });
  it("uses exact source bytes, including newline differences", async () => {
    const source = examples[0].source;
    expect(
      await sourceDigest(new TextEncoder().encode(source.text).buffer),
    ).toBe(source.digest);
    expect(
      await sourceDigest(
        new TextEncoder().encode(source.text.replaceAll("\n", "\r\n")).buffer,
      ),
    ).not.toBe(source.digest);
  });
});

describe("imports", () => {
  it("opens portable bundles without calling the API", async () => {
    const fetch = vi.fn();
    vi.stubGlobal("fetch", fetch);
    const file = new File(
      [JSON.stringify({ bundle_format: "pisces-report-view", ...examples[0] })],
      "view.json",
    );
    const entry = await openReport(file);
    expect(entry.origin).toBe("imported-view");
    expect(fetch).not.toHaveBeenCalled();
  });
  it("sends original raw JSON to Python and preserves its text", async () => {
    const fetch = vi
      .fn()
      .mockResolvedValue({ ok: true, json: async () => examples[0].view });
    vi.stubGlobal("fetch", fetch);
    const raw = JSON.stringify(examples[0].report, null, 4) + "\n";
    const entry = await openReport(new File([raw], "raw.json"));
    expect(entry.raw).toBe(raw);
    expect(fetch.mock.calls[0][1].body).toBe(raw);
  });
  it("keeps API errors distinct from false output verdicts", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({ ok: false, status: 422 }),
    );
    await expect(openReport(new File(["{}"], "bad.json"))).rejects.toThrow(
      "Python API rejected",
    );
  });
  it("rejects malformed and oversized files before uploading", async () => {
    const fetch = vi.fn();
    vi.stubGlobal("fetch", fetch);
    await expect(openReport(new File(["{"], "bad.json"))).rejects.toThrow(
      "not valid JSON",
    );
    await expect(
      openReport(new File([" ".repeat(8 * 1024 * 1024 + 1)], "big.json")),
    ).rejects.toThrow("8 MiB");
    expect(fetch).not.toHaveBeenCalled();
  });
});
