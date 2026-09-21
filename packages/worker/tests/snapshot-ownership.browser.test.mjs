// Actual Chromium Web Locks across dedicated workers; no lock-manager model.
// node --test packages/worker/tests/snapshot-ownership.browser.test.mjs

import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { createServer } from "node:http";
import { after, before, test } from "node:test";

const { chromium } = await import(process.env.FSQLITE_PLAYWRIGHT_MODULE ?? "@playwright/test");
const { default: ts } = await import(process.env.FSQLITE_TYPESCRIPT_MODULE ?? "typescript");
let server, browser, context, url;
const source = new URL("../src/snapshot-ownership.ts", import.meta.url);
const worker = `
import { SnapshotSessionLease } from '/ownership.js';
let lease;
onmessage = async ({data}) => {
  try {
    if (data.action === 'acquire') {
      lease = await SnapshotSessionLease.acquire(data.backend, data.name, data.mode);
      postMessage({id:data.id, mode:lease?.mode ?? null});
    } else if (data.action === 'close') {
      await lease?.close(); lease = undefined;
      postMessage({id:data.id, closed:true});
    }
  } catch(e) { postMessage({id:data.id, code:e.code, message:e.message}); }
};`;
before(async () => {
  const module = ts.transpileModule(await readFile(source, "utf8"), {
    compilerOptions: {
      target: ts.ScriptTarget.ES2022,
      module: ts.ModuleKind.ESNext,
    },
  }).outputText;
  server = createServer((request, response) => {
    response.setHeader("Cache-Control", "no-store");
    response.setHeader("Content-Type", request.url === "/" ? "text/html" : "text/javascript");
    response.end(
      request.url === "/ownership.js"
        ? module
        : request.url === "/worker.js"
          ? worker
          : "<!doctype html><title>Snapshot session ownership</title>",
    );
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  url = `http://127.0.0.1:${server.address().port}`;
  browser = await chromium.launch({
    headless: true,
    ...(process.env.FSQLITE_CHROMIUM_PATH
      ? { executablePath: process.env.FSQLITE_CHROMIUM_PATH }
      : {}),
  });
  context = await browser.newContext();
  console.log(`Real Web Locks in Chromium ${browser.version()}`);
});
after(async () => {
  await context?.close();
  await browser?.close();
  if (server) await new Promise((resolve) => server.close(resolve));
});
async function run(body) {
  const page = await context.newPage();
  try {
    await page.goto(url);
    await page.evaluate(() => {
      globalThis.startOwner = () => {
        const worker = new Worker("/worker.js", { type: "module" });
        let sequence = 0;
        return {
          worker,
          ask(data) {
            const id = ++sequence;
            return new Promise((resolve, reject) => {
              const receive = ({ data: reply }) => {
                if (reply.id !== id) return;
                worker.removeEventListener("message", receive);
                resolve(reply);
              };
              worker.addEventListener("message", receive);
              worker.addEventListener("error", reject, { once: true });
              worker.postMessage({ ...data, id });
            });
          },
        };
      };
    });
    return await page.evaluate(body);
  } finally {
    await page.close();
  }
}

test("shared sessions coexist; exclusive admission fails without waiting", async () => {
  const result = await run(async () => {
    const { SnapshotSessionLease: L } = await import("/ownership.js");
    const name = crypto.randomUUID();
    const a = await L.acquire("indexeddb-snapshot", name);
    const b = await L.acquire("indexeddb-snapshot", name, "shared");
    const blocked = await L.acquire("indexeddb-snapshot", name, "exclusive").catch((e) => e.code);
    await a.close();
    const stillBlocked = await L.acquire("indexeddb-snapshot", name, "exclusive").catch(
      (e) => e.code,
    );
    await b.close();
    const c = await L.acquire("indexeddb-snapshot", name, "exclusive");
    const modes = [a.mode, b.mode, c.mode];
    await c.close();
    return { modes, blocked, stillBlocked };
  });
  assert.deepEqual(result, {
    modes: ["shared", "shared", "exclusive"],
    blocked: "ERR_FSQLITE_SNAPSHOT_OWNED",
    stillBlocked: "ERR_FSQLITE_SNAPSHOT_OWNED",
  });
});

test("exclusive ownership excludes both kinds of independently running worker", async () => {
  const result = await run(async () => {
    const a = startOwner(),
      b = startOwner(),
      c = startOwner();
    const config = { action: "acquire", backend: "opfs-snapshot", name: crypto.randomUUID() };
    const first = await a.ask({ ...config, mode: "exclusive" });
    const shared = await b.ask(config);
    const exclusive = await c.ask({ ...config, mode: "exclusive" });
    await a.ask({ action: "close" });
    const next = await b.ask({ ...config, mode: "exclusive" });
    await b.ask({ action: "close" });
    a.worker.terminate();
    b.worker.terminate();
    c.worker.terminate();
    return { first: first.mode, shared: shared.code, exclusive: exclusive.code, next: next.mode };
  });
  assert.deepEqual(result, {
    first: "exclusive",
    shared: "ERR_FSQLITE_SNAPSHOT_OWNED",
    exclusive: "ERR_FSQLITE_SNAPSHOT_OWNED",
    next: "exclusive",
  });
});

test("termination releases ownership; no stale callback has to cooperate", async () => {
  const result = await run(async () => {
    const a = startOwner(),
      b = startOwner();
    const config = {
      action: "acquire",
      backend: "opfs-snapshot",
      name: crypto.randomUUID(),
      mode: "exclusive",
    };
    await a.ask(config);
    const blocked = await b.ask(config);
    a.worker.terminate();
    // Browser realm destruction is asynchronous. Retry admission only; no SQL
    // or application callback is repeated by this test.
    let acquired;
    for (let i = 0; i < 100; i++) {
      acquired = await b.ask(config);
      if (acquired.mode) break;
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    await b.ask({ action: "close" });
    b.worker.terminate();
    return { blocked: blocked.code, acquired: acquired.mode };
  });
  assert.deepEqual(result, { blocked: "ERR_FSQLITE_SNAPSHOT_OWNED", acquired: "exclusive" });
});

test("namespaces distinguish backends, escaping, and lone surrogates", async () => {
  const result = await run(async () => {
    const { SnapshotSessionLease: L } = await import("/ownership.js");
    const prefix = crypto.randomUUID();
    const names = [
      prefix,
      prefix + "\\",
      prefix + '"',
      prefix + "\ud800",
      prefix + "\ud801",
      prefix + "\ufffd",
    ];
    const leases = [];
    for (const backend of ["opfs-snapshot", "indexeddb-snapshot"]) {
      for (const name of names) leases.push(await L.acquire(backend, name, "exclusive"));
    }
    const count = leases.length;
    await Promise.all(leases.map((lease) => lease.close()));
    return count;
  });
  assert.equal(result, 12);
});

test("close joins release, is idempotent, and retires reusable ownership", async () => {
  const result = await run(async () => {
    const { SnapshotSessionLease: L } = await import("/ownership.js");
    const name = crypto.randomUUID();
    const lease = await L.acquire("opfs-snapshot", name, "exclusive");
    const matches = lease.matches("opfs-snapshot", name, "exclusive");
    const mismatches = [
      lease.matches("indexeddb-snapshot", name, "exclusive"),
      lease.matches("opfs-snapshot", name, undefined),
    ];
    const first = lease.close(),
      same = first === lease.close();
    const retired = lease.matches("opfs-snapshot", name, "exclusive");
    await first;
    const next = await L.acquire("opfs-snapshot", name, "exclusive");
    await next.close();
    return { matches, mismatches, same, retired, frozen: Object.isFrozen(lease) };
  });
  assert.deepEqual(result, {
    matches: true,
    mismatches: [false, false],
    same: true,
    retired: false,
    frozen: true,
  });
});

test("invalid policies and names never create a held or pending lock", async () => {
  const result = await run(async () => {
    const { SnapshotSessionLease: L } = await import("/ownership.js");
    const before = await navigator.locks.query();
    const errors = [];
    for (const args of [
      ["memory", "x", "exclusive"],
      ["opfs-snapshot", "", undefined],
      ["opfs-snapshot", ":memory:", undefined],
      ["opfs-snapshot", "x", "invalid"],
      ["opfs-snapshot", "a\0b", "shared"],
      ["opfs-snapshot", "x".repeat(257), "shared"],
    ]) {
      errors.push(await L.acquire(...args).catch((e) => e.code));
    }
    const after = await navigator.locks.query();
    return { errors, same: JSON.stringify(before) === JSON.stringify(after) };
  });
  assert.deepEqual(result, {
    errors: Array(6).fill("ERR_FSQLITE_SNAPSHOT_OWNERSHIP_INPUT"),
    same: true,
  });
});
