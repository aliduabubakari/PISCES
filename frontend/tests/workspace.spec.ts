import { test, expect } from "@playwright/test";
import examples from "../src/test/fixtures/examples.json" with { type: "json" };

test("saved cases, zero versus missing, scope details and comparison gates", async ({
  page,
}) => {
  const errors: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await page.goto(`/#/report/${examples[0].view.report_id}`);
  await expect(
    page.getByRole("heading", { name: "Required outputs verified" }),
  ).toBeVisible();
  await page.getByRole("tab", { name: "Metrics", exact: true }).click();
  await expect(
    page.locator("tr").filter({ hasText: "CPU throttling" }),
  ).toContainText("0 ms");
  await page
    .getByRole("button", { name: "Block read bytes", exact: true })
    .click();
  await expect(page.locator(".metric-inspector")).toContainText(
    "counter_file_empty",
  );
  await page.getByLabel("Measurement scope").selectOption("execution_block");
  await page
    .getByRole("button", { name: "Peak process RSS", exact: true })
    .click();
  await expect(page.locator(".metric-inspector")).toContainText("lifetime");
  await page.getByRole("tab", { name: "Compare", exact: true }).click();
  await expect(
    page.getByRole("heading", { name: "Resource ranking blocked" }),
  ).toBeVisible();
  await page.goto(`/#/report/${examples[1].view.report_id}`);
  await expect(
    page.getByRole("heading", { name: "Output verification failed" }),
  ).toBeVisible();
  await expect(page.locator("table")).toContainText("output_content_mismatch");
  await page.goto(`/#/report/${examples[2].view.report_id}`);
  await expect(page.locator("table")).toContainText("output_file_missing");
  expect(errors).toEqual([]);
});

test("source association rejects different bytes and graph does not invent dependencies", async ({
  page,
}) => {
  await page.goto(`/#/report/${examples[0].view.report_id}`);
  await page.getByRole("button", { name: "Show source" }).click();
  await page.getByLabel("Source file", { exact: true }).setInputFiles({
    name: "wrong.py",
    mimeType: "text/plain",
    buffer: Buffer.from('print("wrong")\r\n'),
  });
  await expect(page.locator(".source-association")).toContainText("mismatched");
  await page.getByLabel("Source file", { exact: true }).setInputFiles({
    name: "valid.py",
    mimeType: "text/plain",
    buffer: Buffer.from(examples[0].source.text),
  });
  await expect(page.locator(".source-association")).toContainText("matched");
  await page.getByRole("tab", { name: /^Tasks/ }).click();
  await expect(page.locator(".task-table tbody tr")).toHaveCount(3);
  await expect(page.locator(".tab-content[data-state='active']")).toContainText(
    "Dependency edges and source locations are not available",
  );
});

test("raw import uses Python and offline bundles remain explicitly untrusted", async ({
  page,
}) => {
  await page.goto("/");
  await page.getByRole("button", { name: "Open report", exact: true }).click();
  await page.getByLabel("Report files", { exact: true }).setInputFiles({
    name: "real-report.json",
    mimeType: "application/json",
    buffer: Buffer.from(JSON.stringify(examples[1].report)),
  });
  const response = page.waitForResponse(
    (url) =>
      url.url().endsWith("/api/v1/reports/view") &&
      url.request().method() === "POST",
  );
  await page.getByRole("button", { name: "Open locally", exact: true }).click();
  expect((await response).status()).toBe(200);
  await expect(
    page.getByRole("heading", { name: "Output verification failed" }),
  ).toBeVisible();
  await page.getByRole("tab", { name: "Evidence", exact: true }).click();
  await expect(page.locator(".provenance").first()).toContainText(
    "Local Python admission",
  );
  await page.getByRole("button", { name: "Open report", exact: true }).click();
  await page.getByLabel("Report files", { exact: true }).setInputFiles({
    name: "bundle.json",
    mimeType: "application/json",
    buffer: Buffer.from(
      JSON.stringify({ bundle_format: "pisces-report-view", ...examples[2] }),
    ),
  });
  await page.getByRole("button", { name: "Open locally", exact: true }).click();
  await page.getByRole("tab", { name: "Evidence", exact: true }).click();
  await expect(page.locator(".provenance").first()).toContainText(
    "not revalidated by Python",
  );
});

test("rejected imports keep the previous report and focus inside the dialog", async ({
  page,
}) => {
  await page.goto(`/#/report/${examples[0].view.report_id}`);
  await page.getByRole("button", { name: "Open report", exact: true }).click();
  await page.getByLabel("Report files", { exact: true }).setInputFiles({
    name: "bad.json",
    mimeType: "application/json",
    buffer: Buffer.from("{"),
  });
  await page.getByRole("button", { name: "Open locally", exact: true }).click();
  await expect(page.getByRole("alert")).toContainText("not valid JSON");
  await page.keyboard.press("Escape");
  await expect(page.getByRole("dialog")).not.toBeVisible();
  await expect(
    page.getByRole("heading", { name: "Required outputs verified" }),
  ).toBeVisible();
});

test("untrusted strings remain text rather than executable markup", async ({
  page,
}) => {
  const bundle = structuredClone(examples[0]);
  bundle.view.identity.name = '<img src=x onerror="window.injected=true">';
  await page.goto("/");
  await page.getByRole("button", { name: "Open report", exact: true }).click();
  await page.getByLabel("Report files", { exact: true }).setInputFiles({
    name: "untrusted.json",
    mimeType: "application/json",
    buffer: Buffer.from(
      JSON.stringify({ bundle_format: "pisces-report-view", ...bundle }),
    ),
  });
  await page.getByRole("button", { name: "Open locally", exact: true }).click();
  await expect(page.locator(".run-title")).toContainText("<img");
  expect(await page.locator(".run-title img").count()).toBe(0);
  expect(await page.locator("img:not(.brand-logo)").count()).toBe(0);
  expect(await page.evaluate(() => "injected" in window)).toBe(false);
});

for (const width of [1440, 1024, 768, 390, 320]) {
  test(`responsive evidence at ${width}px`, async ({ page }) => {
    await page.setViewportSize({ width, height: 900 });
    await page.goto(`/#/report/${examples[0].view.report_id}`);
    for (const tab of ["Overview", "Metrics", "Compare", "Evidence"]) {
      await page.getByRole("tab", { name: tab, exact: true }).click();
      const overflow = await page.evaluate(
        () => document.documentElement.scrollWidth > window.innerWidth + 1,
      );
      expect(overflow, `horizontal overflow in ${tab}`).toBe(false);
    }
    await page.getByRole("tab", { name: "Metrics", exact: true }).focus();
    await page.keyboard.press("ArrowRight");
    await expect(
      page.getByRole("tab", { name: "Compare", exact: true }),
    ).toHaveAttribute("data-state", "active");
    await page.screenshot({
      path: `test-results/workspace-${width}.png`,
      fullPage: true,
    });
  });
}
