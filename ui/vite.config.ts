import { sveltekit } from "@sveltejs/kit/vite";
import { defineConfig } from "vite";

export default defineConfig({
  plugins: [sveltekit()],
  server: {
    port: 5173,
    strictPort: true,
    proxy: {
      "/api": {
        target: "http://127.0.0.1:19198",
        changeOrigin: true,
      },
      "/api/v1/events": {
        target: "ws://127.0.0.1:19198",
        ws: true,
        changeOrigin: true,
      },
    },
  },
  test: {
    include: ["src/**/*.{test,spec}.{js,ts}"],
    environment: "jsdom",
  },
});
