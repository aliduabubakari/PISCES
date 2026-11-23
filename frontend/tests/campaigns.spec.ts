import { test, expect } from "@playwright/test";
import campaigns from "../src/test/fixtures/campaigns.json" with { type: "json" };

test("real load progression, numeric spacing, source fits and independent sessions", async ({
  page,
}) => {
  const errors: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await page.goto("/#/campaigns");
  await page.getByRole("button", { name: /Scaling sessions/i }).click();
  await expect(page.getByLabel("Scaling metric")).toBeVisible();
  await expect(page.locator(".load-table tbody tr")).toHaveCount(4);
  await expect(page.locator(".load-table")).toContainText("0.000262");
  await expect(page.locator(".fit-readout")).toContainText("0.1815");
  const circles = page.locator(".recharts-line-dots circle");
  await expect(circles).toHaveCount(4);
  const xs = await circles.evaluateAll((nodes) =>
    nodes.map((node) => Number(node.getAttribute("cx"))),
  );
  expect((xs[2] - xs[1]) / (xs[1] - xs[0])).toBeCloseTo(400000 / 90000, 2);
  await page.getByLabel("Source fit", { exact: true }).check();
  await expect(page.locator(".recharts-line")).toHaveCount(2);
  await page.getByLabel("Scaling session").selectOption("session-2");
  await expect(page.locator(".load-table")).toContainText("1.876");
  await page
    .getByLabel("Scaling metric")
    .selectOption("cgroup_lifetime_peak_bytes");
  await expect(page.getByLabel("Source fit", { exact: true })).toBeDisabled();
  await expect(page.locator(".chart-heading")).toContainText("MiB");
  await expect(page.locator(".fit-readout")).toContainText(
    "memory_high_water_not_fitted",
  );
  expect(errors).toEqual([]);
});

test("visible loads and suite filters preserve planned denominators", async ({
  page,
}) => {
  await page.goto("/#/campaigns");
  await page.getByRole("button", { name: /Scaling sessions/i }).click();
  await page.getByLabel("100,000", { exact: true }).uncheck();
  await expect(page.locator(".load-table-caption")).toContainText(
    "3 of 4 planned loads",
  );
  await expect(page.locator(".load-table tbody tr")).toHaveCount(3);
  await page.getByRole("link", { name: "Campaigns", exact: true }).click();
  await page.getByRole("button", { name: /Benchmark suite/i }).click();
  await expect(page.locator(".load-table-wrap tbody tr")).toHaveCount(20);
  await page.getByLabel("Inventory pipeline").selectOption("event_aggregation");
  await page.getByLabel("Inventory case").selectOption("same_shape_wrong");
  await expect(page.locator(".load-table-wrap tbody tr")).toHaveCount(1);
  await expect(page.locator(".campaign-toolbar")).toContainText(
    "20 planned attempts",
  );
  await expect(page.locator(".load-table-wrap tbody tr")).toContainText(
    "0 / 1",
  );
});

test("raw campaign imports use Python; partial coverage remains a gap", async ({
  page,
}) => {
  const report = structuredClone(campaigns[0].report);
  if (!report.sessions) throw new Error("Expected sessions fixture");
  Object.assign(
    report.sessions[0].summary.loads["100000"].metrics.task_wall_s,
    { median: null, mad: null, coverage: 0 },
  );
  await page.goto("/");
  await page.getByRole("button", { name: "Open report", exact: true }).click();
  await page.getByLabel("Report files").setInputFiles({
    name: "partial-campaign.json",
    mimeType: "application/json",
    buffer: Buffer.from(JSON.stringify(report)),
  });
  await page.getByRole("button", { name: "Open locally", exact: true }).click();
  await expect(page.getByLabel("Scaling metric")).toBeVisible();
  await expect(page.locator(".load-table")).toContainText(
    "reading_not_available",
  );
  await expect(page.locator(".recharts-line-dots circle")).toHaveCount(3);
  await expect(page.getByLabel("Source fit", { exact: true })).toBeDisabled();
  await expect(page.locator(".load-table tbody tr")).toHaveCount(4);
});

for (const width of [1440, 768, 390, 320]) {
  test(`campaign chart and controls at ${width}px`, async ({ page }) => {
    await page.setViewportSize({ width, height: 1000 });
    await page.goto(`/#/report/${campaigns[0].view.report_id}`);
    await expect(page.getByLabel("Scaling metric")).toBeVisible();
    await expect(page.locator(".load-chart svg")).toBeVisible();
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= window.innerWidth + 1,
      ),
    ).toBe(true);
    await page.screenshot({
      path: `test-results/campaign-${width}.png`,
      fullPage: true,
    });
  });
}
