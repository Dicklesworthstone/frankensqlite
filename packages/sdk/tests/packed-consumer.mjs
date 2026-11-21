import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { copyFile, cp, mkdtemp, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../../../", import.meta.url));
const consumer = await mkdtemp(join(tmpdir(), "fsqlite-packed-"));
const core = process.env.FSQLITE_WASM_CORE_TARBALL;
const require = createRequire(import.meta.url);
const npm = (args, cwd) => execFileSync(process.execPath, [process.env.npm_execpath, ...args], {
  cwd, encoding: "utf8", stdio: ["ignore", "pipe", "inherit"],
});
console.log("Packed consumer", consumer);
const packs = JSON.parse(npm(["pack", "--json", "--ignore-scripts", "--pack-destination", consumer,
  "--workspace", "@frankensqlite/worker", "--workspace", "@frankensqlite/sdk"], root));
assert.equal(packs.length, 2);
const archives = packs.map(pack => join(consumer, pack.filename));
for (const pack of packs) {
  const name = pack.name.split("/")[1];
  const source = join(root, "packages", name);
  const staged = join(consumer, "incremental", name);
  const output = join(staged, "dist/npm");
  await cp(join(source, "dist/npm"), output, { recursive: true });
  await copyFile(join(source, "package.json"), join(staged, "package.json"));
  const obsolete = await mkdtemp(join(output, "previous-build-"));
  await writeFile(obsolete + ".js", "export default 0;");
  await writeFile(join(obsolete, "old.d.ts"), "export {};");
  const [incremental] = JSON.parse(npm(["pack", "--dry-run", "--json", "--ignore-scripts"], staged));
  const outputs = ({ files }) => files.map(f => f.path).filter(path => path.startsWith("dist/npm/")).sort();
  assert.deepEqual(outputs(incremental), outputs(pack), `${pack.name} packed stale build output`);
}
await writeFile(join(consumer, "package.json"), JSON.stringify({ private: true }));
npm(["install", "--offline", "--ignore-scripts", "--no-audit", "--no-fund",
  ...(core ? [resolve(core)] : ["--legacy-peer-deps"]), ...archives], consumer);

for (const [extension, load, resolveEntry] of [
  ["mjs", "await import", "import.meta.resolve"],
  ["cjs", "require", "require.resolve"],
]) {
  const entry = join(consumer, "consumer." + extension);
  await writeFile(entry, `
    const sdk = ${load}("@frankensqlite/sdk");
    const worker = ${load}("@frankensqlite/worker");
    const protocol = ${load}("@frankensqlite/worker/protocol");
    const assert = ${load}("node:assert/strict");
    const path = ${load}("node:path");
    assert.equal(typeof sdk.FrankenDB.open, "function");
    assert.equal(protocol.MAX_EXECUTE_MANY_ROWS, 10000);
    assert.equal(sdk.MAX_EXECUTE_MANY_ROWS, protocol.MAX_EXECUTE_MANY_ROWS);
    assert.equal(worker.isSnapshotPersistenceMode, protocol.isSnapshotPersistenceMode);
    for (const name of ["@frankensqlite/worker/worker", "@frankensqlite/sdk/worker"])
      assert.equal(path.basename(${resolveEntry}(name)), "worker.${extension === "mjs" ? "js" : "cjs"}");
  `);
  execFileSync(process.execPath, ["--no-experimental-require-module", entry], { cwd: consumer, stdio: "inherit" });
}
const types = `
  import { FrankenDB } from "@frankensqlite/sdk";
  import { MAX_EXECUTE_MANY_ROWS, type WorkerRequest } from "@frankensqlite/worker/protocol";
  import "@frankensqlite/sdk/worker";
  import "@frankensqlite/worker/worker";
  const close: WorkerRequest = { kind: "close", requestId: MAX_EXECUTE_MANY_ROWS };
  async function read(db: FrankenDB): Promise<number> {
    // @ts-expect-error the public query requires SQL text
    await db.query(close);
    return (await db.query<{ n: number }>("SELECT 1 AS n")).rows[0].n;
  }
`;
await Promise.all(["mts", "cts"].map(ext => writeFile(join(consumer, "consumer." + ext), types)));
execFileSync(process.execPath, [join(dirname(require.resolve("typescript/package.json")), "bin/tsc"),
  "--strict", "--noEmit", "--module", "NodeNext", "--target", "ES2022", "--lib", "ES2022,DOM,ESNext.Disposable",
  "consumer.mts", "consumer.cts"], { cwd: consumer, stdio: "inherit" });
console.log("Packed ESM, CommonJS and both strict TypeScript consumers passed");

if (core) {
  const { build, preview } = await import("vite");
  const { chromium } = await import("@playwright/test");
  await writeFile(join(consumer, "index.html"), '<script type="module" src="/app.js"></script>');
  await writeFile(join(consumer, "writer.cjs"), `
    const { FrankenDB } = require("@frankensqlite/sdk");
    module.exports = options => FrankenDB.open(options);
  `);
  await writeFile(join(consumer, "app.js"), `
    import { FrankenDB } from "@frankensqlite/sdk";
    import { createFrankenSqliteWorker } from "@frankensqlite/worker";
    import workerAsset from "@frankensqlite/sdk/worker?worker&url";
    import openWriter from "./writer.cjs";
    async function reopen() {
      const options = { dbName: "packed", persistence: "indexeddb-snapshot" };
      const writer = await openWriter(options);
      try {
        await writer.execute("CREATE TABLE t (n INTEGER)");
        await writer.execute("INSERT INTO t VALUES (?)", [42]);
        await writer.checkpoint();
      } finally { await writer.close(); }
      let reads = 0;
      const worker = createFrankenSqliteWorker({
        get workerUrl() {
          if (++reads > 1) throw new Error("workerUrl was read twice");
          return new URL(workerAsset, location.href);
        },
      });
      const reader = await FrankenDB.open({ ...options, worker });
      try { return (await reader.query("SELECT n FROM t")).rows; }
      finally { await reader.close(); }
    }
    reopen().then(rows => window.result = {rows}, error => window.result = {error: String(error)});
  `);
  const config = { root: consumer, configFile: false, worker: { format: "es" } };
  await build(config);
  const server = await preview({ ...config, preview: { host: "127.0.0.1", port: 0 } });
  try {
    const browser = await chromium.launch({ executablePath: process.env.FSQLITE_CHROMIUM_PATH });
    try {
      const page = await browser.newPage();
      await page.goto(server.resolvedUrls.local[0]);
      await page.waitForFunction(() => window.result !== undefined);
      assert.deepEqual(await page.evaluate(() => window.result), { rows: [{ n: 42 }] });
      console.log("Packaged browser worker reopened committed SQL", browser.version());
    } finally { await browser.close(); }
  } finally { await server.close(); }
}
console.log("Tested package archives", ...archives);
