import { randomUUID } from "node:crypto";
import { rename, writeFile } from "node:fs/promises";
import { join, posix } from "node:path";
import { defineConfig } from "tsdown";

const formats = ["esm", "cjs"] as const;
const emitted = new Map<string, string[]>();

export default defineConfig({
  cwd: process.cwd(),
  format: [...formats],
  platform: "neutral",
  target: "es2022",
  tsconfig: "tsconfig.build.json",
  outDir: "dist/npm",
  dts: true,
  sourcemap: true,
  shims: true,
  unbundle: true,
  clean: false,
  hooks: {
    async "build:done"({ chunks, options }) {
      // Only this build's emitted modules, declarations and maps belong in
      // the tarball. Leave older output on disk for incremental consumers.
      emitted.set(options.format, chunks.map(chunk => chunk.fileName));
      if (emitted.size !== formats.length) return;
      const paths = new Set<string>();
      for (const files of emitted.values()) {
        for (const file of files) {
          for (let path = file; path !== "."; path = posix.dirname(path)) {
            paths.add(path);
          }
        }
      }
      // .npmignore interprets glob patterns. Each exception names a literal
      // emitted path. Concurrent pack operations must see a complete list.
      const rules = [...paths].sort().map(path => "!/" + path.replace(/[^\w./-]/g, "\\$&"));
      const temporary = join(options.outDir, "..", `.npmignore-${randomUUID()}`);
      await writeFile(temporary, ["*", ...rules, ""].join("\n"));
      await rename(temporary, join(options.outDir, ".npmignore"));
    },
  },
});
