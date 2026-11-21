import { defineConfig } from "tsdown";

export default defineConfig({
  cwd: process.cwd(),
  format: ["esm", "cjs"],
  platform: "neutral",
  target: "es2022",
  tsconfig: "tsconfig.build.json",
  outDir: "dist/npm",
  dts: true,
  sourcemap: true,
  shims: true,
  unbundle: true,
  clean: false,
});
