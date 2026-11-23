import { test, expect } from "@playwright/test";
import { readFileSync } from "node:fs";

test.use({ trace: "off", screenshot: "off", video: "off" });
test("Compose wizard executes a script and restores its report after reconnecting", async ({
  page,
  request,
}) => {
  test.skip(
    !process.env.PISCES_E2E_EXECUTION_URL || !process.env.PISCES_E2E_TOKEN_FILE,
    "Requires an explicitly started Compose deployment and token file",
  );
  test.setTimeout(90000);
  const origin = process.env.PISCES_E2E_EXECUTION_URL!;
  const token = readFileSync(process.env.PISCES_E2E_TOKEN_FILE!, "utf8").trim();
  const errors: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  async function connect() {
    await page
      .getByRole("button", { name: "Deployment connection", exact: true })
      .click();
    await page.getByLabel("API token", { exact: true }).fill(token);
    await page
      .getByRole("button", { name: "Connect API", exact: true })
      .click();
    await expect(page.getByRole("dialog")).not.toBeVisible();
    await expect(page.locator(".connection-button")).toContainText("Connected");
  }
  let jobId: string | undefined;
  try {
    await page.goto(origin);
    await connect();
    await page
      .getByRole("banner")
      .getByRole("button", { name: "New evaluation", exact: true })
      .click();
    await page
      .getByLabel("Evaluation script", { exact: true })
      .setInputFiles({
        name: "browser_smoke.py",
        mimeType: "text/plain",
        buffer: Buffer.from(
          "total = sum(i * i for i in range(1000000))\nassert total > 0\n",
        ),
      });
    await page
      .getByLabel("Execution profile", { exact: true })
      .selectOption("python");
    for (let step = 0; step < 3; step++)
      await page.getByRole("button", { name: "Continue", exact: true }).click();
    await page.getByRole("checkbox").check();
    const response = page.waitForResponse(
      (result) =>
        result.url().endsWith("/api/v1/jobs") &&
        result.request().method() === "POST",
    );
    await page
      .getByRole("button", { name: "Queue evaluation", exact: true })
      .click();
    const submitted = await response;
    expect(submitted.status()).toBe(202);
    jobId = (await submitted.json()).id;
    await expect(page).toHaveURL(new RegExp(`/job/${jobId}`));
    await expect(
      page.getByRole("heading", { name: "Outputs not verified", exact: true }),
    ).toBeVisible({ timeout: 45000 });
    await page.getByRole("button", { name: "Show source" }).click();
    await expect(page.locator(".source-association")).toContainText("matched");
    await page.getByRole("tab", { name: "Evidence", exact: true }).click();
    await expect(page.locator(".provenance").first()).toContainText(
      "Service-produced container report; not attested",
    );
    await page.reload();
    await expect(
      page.getByRole("heading", {
        name: "Connect to retrieve this evaluation",
      }),
    ).toBeVisible();
    await connect();
    await expect(
      page.getByRole("heading", { name: "Evidence provenance" }),
    ).toBeVisible({ timeout: 15000 });
    expect(await page.evaluate(() => localStorage.length)).toBe(0);
    expect(errors).toEqual([]);
  } finally {
    if (jobId) {
      const headers = { Authorization: `Bearer ${token}` };
      await request.post(`${origin}/api/v1/jobs/${jobId}/cancel`, { headers });
      await request.delete(`${origin}/api/v1/jobs/${jobId}`, { headers });
    }
  }
});

for (const width of [1440, 390, 320]) {
  test(`evaluation wizard fits at ${width}px without a backend`, async ({
    page,
  }) => {
    await page.setViewportSize({ width, height: 900 });
    await page.goto("/#/evaluate");
    await expect(
      page.getByRole("button", { name: "Continue", exact: true }),
    ).toBeDisabled();
    await expect(page.locator(".request-summary")).toContainText("Not checked");
    await expect(page.locator(".request-summary")).not.toContainText(
      "unavailable",
    );
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth > innerWidth + 1,
      ),
    ).toBe(false);
    await page.screenshot({
      path: `test-results/upload-${width}.png`,
      fullPage: true,
    });
    await page.getByRole("button", { name: "Cancel", exact: true }).click();
    await expect(
      page.getByRole("heading", { name: "Runs", exact: true }),
    ).toBeVisible();
  });
}
