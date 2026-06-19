import { defineConfig } from "vitest/config";

export default defineConfig({
  resolve: {
    alias: {
      "@frankensqlite/worker": new URL("../worker/src/index.ts", import.meta.url)
        .pathname,
    },
  },
  test: {
    environment: "node",
    include: ["tests/**/*.test.ts"],
  },
});
