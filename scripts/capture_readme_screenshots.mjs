// Capture public examples only; never connect to a deployment or load a token.
import { chromium } from "../frontend/node_modules/@playwright/test/index.mjs";
import { readFile, mkdir } from "node:fs/promises";
import { fileURLToPath } from "node:url";

const root = new URL("../", import.meta.url);
const examples = JSON.parse(
  await readFile(
    new URL("frontend/src/test/fixtures/examples.json", root),
    "utf8",
  ),
);
const campaigns = JSON.parse(
  await readFile(
    new URL("frontend/src/test/fixtures/campaigns.json", root),
    "utf8",
  ),
);
const output = new URL("assets/screenshots/", root);
await mkdir(output, { recursive: true });
const browser = await chromium.launch({
  channel: process.env.PISCES_BROWSER_CHANNEL ?? "chrome",
});
const page = await browser.newPage({
  viewport: { width: 1440, height: 1000 },
  deviceScaleFactor: 1,
});
const errors = [];
page.on("pageerror", (error) => errors.push(error.message));
const origin = "http://127.0.0.1:5173";
async function capture(filename) {
  await page.evaluate(() => document.fonts.ready);
  await page.mouse.move(2, 2);
  await page.screenshot({
    path: fileURLToPath(new URL(filename, output)),
    fullPage: true,
    animations: "disabled",
  });
}
try {
  await page.goto(`${origin}/#/report/${examples[0].view.report_id}`);
  await page.getByRole("button", { name: "Show source", exact: true }).click();
  await page.getByRole("region", { name: "Source inspector" }).waitFor();
  await capture("report-overview.png");
  await page.getByRole("tab", { name: /^Tasks/ }).click();
  await page.getByRole("button", { name: "transform", exact: true }).click();
  await capture("task-inspection.png");
  await page.goto(`${origin}/#/report/${campaigns[0].view.report_id}`);
  await page.getByLabel("Scaling metric").waitFor();
  await page.locator(".recharts-line-dots circle").first().waitFor();
  await capture("load-campaign.png");
  await page.goto(`${origin}/#/evaluate`);
  await page.getByLabel("Evaluation script", { exact: true }).setInputFiles({
    name: examples[0].source.name,
    mimeType: "text/plain",
    buffer: Buffer.from(examples[0].source.text),
  });
  await page.getByText("Source preview", { exact: true }).click();
  await page.getByLabel("Uploaded source preview").waitFor();
  await capture("new-evaluation.png");
  if (errors.length) throw new Error(errors.join("\n"));
  console.log("Captured four sanitized README screenshots.");
} finally {
  await browser.close();
}
