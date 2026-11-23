import { test, expect } from "@playwright/test";
import examples from "../src/test/fixtures/examples.json" with { type: "json" };

test("project logo and favicon are served as real SVG assets", async ({
  page,
  request,
}) => {
  await page.goto("/");
  const mark = page.locator(".brand-logo");
  await expect(mark).toBeVisible();
  expect(
    await mark.evaluate(
      (node: HTMLImageElement) => node.complete && node.naturalWidth > 0,
    ),
  ).toBe(true);
  const favicon = await page.locator('link[rel="icon"]').getAttribute("href");
  expect(favicon).toBeTruthy();
  expect(new URL(favicon!, page.url()).href).toBe(
    new URL((await mark.getAttribute("src"))!, page.url()).href,
  );
  if (favicon!.startsWith("data:")) {
    expect(favicon).toMatch(/^data:image\/svg\+xml[;,]/);
    const [header, data] = favicon!.split(",", 2);
    const svg = header.includes(";base64")
      ? Buffer.from(data, "base64").toString("utf8")
      : decodeURIComponent(data);
    expect(svg).toContain("PISCES pipeline mark");
  } else {
    const response = await request.get(new URL(favicon!, page.url()).href);
    expect(response.ok()).toBe(true);
    expect(response.headers()["content-type"]).toContain("image/svg+xml");
    expect(await response.text()).toContain("PISCES pipeline mark");
  }
});

test("unconnected inventory never claims empty deployment history", async ({
  page,
}) => {
  await page.goto("/");
  await expect(
    page.getByRole("heading", { name: "Runs", exact: true }),
  ).toBeVisible();
  await expect(
    page.getByText("Job history has not been loaded."),
  ).toBeVisible();
  await expect(
    page.getByText("No evaluations yet", { exact: true }),
  ).not.toBeVisible();
  await page.getByRole("button", { name: "Deployment connection" }).click();
  await page.keyboard.press("Escape");
  await expect(page.getByRole("dialog")).not.toBeVisible();
});

test("task selection and report tabs survive URL navigation without invented metrics", async ({
  page,
}) => {
  await page.goto(
    `/#/report/${examples[0].view.report_id}?tab=tasks&task=transform`,
  );
  await expect(
    page.getByRole("region", { name: "Selected task" }),
  ).toContainText("transform");
  await expect(page.locator(".task-table tbody tr")).toHaveCount(3);
  await expect(page.locator(".task-readings")).toContainText("Not supplied");
  await expect(page.locator(".task-readings")).not.toContainText("7.286");
  await page.getByRole("button", { name: "normalize", exact: true }).click();
  await expect(page).toHaveURL(/task=normalize/);
  await page.getByRole("tab", { name: "Metrics", exact: true }).click();
  await expect(page).toHaveURL(/tab=metrics/);
  await page.goBack();
  await expect(
    page.getByRole("region", { name: "Selected task" }),
  ).toContainText("normalize");
  await page.reload();
  await expect(
    page.getByRole("region", { name: "Selected task" }),
  ).toContainText("normalize");
});

test("global polling continues while examples are open and profile readiness refreshes", async ({
  page,
}) => {
  let calls = 0;
  const job = {
    id: "a".repeat(32),
    filename: "pending.py",
    profile: "python",
    artifact_sha256: "b".repeat(64),
    state: "queued",
    created_at: 1795430000,
    started_at: null,
    finished_at: null,
    cancel_requested: false,
    error: null,
    has_report: false,
    evaluation_succeeded: null,
  };
  await page.route("**/api/v1/capabilities", (route) =>
    route.fulfill({ json: { execution: true, persistence: true } }),
  );
  await page.route("**/api/v1/jobs/profiles", (route) =>
    route.fulfill({
      json: [
        {
          name: "static",
          ready: true,
          image_id: "sha256:" + "c".repeat(64),
          reason: null,
        },
      ],
    }),
  );
  await page.route("**/api/v1/jobs", (route) => {
    calls++;
    return route.fulfill({
      json: [
        {
          ...job,
          state: calls >= 3 ? "completed" : calls >= 2 ? "running" : "queued",
        },
      ],
    });
  });
  await page.goto("/");
  await page.getByRole("button", { name: "Deployment connection" }).click();
  await page.getByRole("button", { name: "Connect API", exact: true }).click();
  await expect(
    page.getByRole("row").filter({ hasText: "pending.py" }),
  ).toContainText("queued");
  await page.getByRole("link", { name: "Examples", exact: true }).click();
  await expect.poll(() => calls, { timeout: 12000 }).toBeGreaterThanOrEqual(3);
  await page.getByRole("link", { name: "Runs", exact: true }).click();
  await expect(
    page.getByRole("row").filter({ hasText: "pending.py" }),
  ).toContainText("completed");
  await page.getByRole("button", { name: "Deployment connection" }).click();
  await page.getByRole("button", { name: "Disconnect", exact: true }).click();
  await page.keyboard.press("Escape");
  await expect(
    page.getByText("Job history has not been loaded."),
  ).toBeVisible();
});

test("a failed new file read clears the previous executable draft", async ({
  page,
}) => {
  await page.goto("/#/evaluate");
  const input = page.getByLabel("Evaluation script", { exact: true });
  await input.setInputFiles({
    name: "valid.py",
    mimeType: "text/plain",
    buffer: Buffer.from("print(1)\n"),
  });
  await expect(
    page.getByRole("button", { name: "Continue", exact: true }),
  ).toBeEnabled();
  await input.setInputFiles({
    name: "wrong.txt",
    mimeType: "text/plain",
    buffer: Buffer.from("print(2)\n"),
  });
  await expect(page.getByRole("alert")).toContainText("Python filename");
  await expect(
    page.getByRole("button", { name: "Continue", exact: true }),
  ).toBeDisabled();
  await expect(page.locator(".request-summary")).toContainText("Not selected");
});

test("connection interruption keeps cached job states explicitly stale", async ({
  page,
}) => {
  let failed = false;
  const job = {
    id: "d".repeat(32),
    filename: "retained.py",
    profile: "python",
    artifact_sha256: "e".repeat(64),
    state: "queued",
    created_at: 1795430000,
    started_at: null,
    finished_at: null,
    cancel_requested: false,
    error: null,
    has_report: false,
    evaluation_succeeded: null,
  };
  await page.route("**/api/v1/capabilities", (route) =>
    route.fulfill({ json: { execution: true, persistence: true } }),
  );
  await page.route("**/api/v1/jobs/profiles", (route) =>
    route.fulfill({ json: [] }),
  );
  await page.route("**/api/v1/jobs", (route) =>
    failed
      ? route.fulfill({
          status: 503,
          json: { detail: "temporarily_unavailable" },
        })
      : route.fulfill({ json: [job] }),
  );
  await page.goto("/");
  await page.getByRole("button", { name: "Deployment connection" }).click();
  await page.getByRole("button", { name: "Connect API", exact: true }).click();
  await expect(
    page.getByRole("row").filter({ hasText: "retained.py" }),
  ).toContainText("queued");
  failed = true;
  await expect(page.getByRole("status")).toContainText(
    "last received job states",
    { timeout: 8000 },
  );
  await expect(page.locator(".connection-button")).toContainText(
    "Reconnecting",
  );
  await expect(
    page.getByRole("row").filter({ hasText: "retained.py" }),
  ).toContainText("queued");
  failed = false;
  await expect(page.locator(".connection-button")).toContainText("Connected", {
    timeout: 10000,
  });
  await expect(page.getByRole("status")).not.toBeVisible();
});
