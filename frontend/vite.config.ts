import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    host: "127.0.0.1",
    proxy: {
      "/api": {
        target: process.env.PISCES_API_PROXY ?? "http://127.0.0.1:8000",
        changeOrigin: true,
      },
    },
    fs: { allow: [".."] },
  },
});
