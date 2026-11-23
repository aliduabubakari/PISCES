import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./tests",
  use: {
    baseURL: "http://127.0.0.1:5173",
    channel: process.env.PISCES_BROWSER_CHANNEL,
    trace: "retain-on-failure",
  },
  webServer: [
    {
      command:
        "node node_modules/vite/bin/vite.js --host 127.0.0.1 --port 5173 --strictPort",
      url: "http://127.0.0.1:5173",
      reuseExistingServer: !process.env.CI,
    },
    {
      command: `${process.env.PISCES_PYTHON ?? "../.venv/bin/python"} -m pisces.cli serve-reports --port 8000`,
      url: "http://127.0.0.1:8000/api/v1/capabilities",
      reuseExistingServer: !process.env.CI,
    },
  ],
});
