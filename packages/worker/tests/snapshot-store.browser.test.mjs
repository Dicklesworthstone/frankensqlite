// Run with: node --test packages/worker/tests/snapshot-store.browser.test.mjs
// Uses Chromium's IndexedDB, origin-private filesystem, Web Locks and Web Crypto.

import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { createServer } from "node:http";
import { basename, dirname, resolve, sep } from "node:path";
import { after, before, test } from "node:test";
import { fileURLToPath } from "node:url";

const { chromium } = await import(process.env.FSQLITE_PLAYWRIGHT_MODULE ?? "@playwright/test");
const { default: ts } = await import(process.env.FSQLITE_TYPESCRIPT_MODULE ?? "typescript");
const root = fileURLToPath(new URL("../../../", import.meta.url));
const wasmModule = process.env.FSQLITE_WASM_BROWSER_MODULE;
let server;
let browser;
let context;
let url;

const fixture = `
export class SqlWorker extends Worker {
  requests = [];
  constructor(fault) {
    super(fault ? "/fault-worker.js?" + fault : "/packages/worker/src/worker.ts", { type: "module" });
  }
  postMessage(request, ...rest) {
    this.requests.push([request.requestId, request.kind, request.action]);
    super.postMessage(request, ...rest);
  }
}
export async function rejection(operation) {
  try { await operation; } catch (error) { return error; }
  throw new Error("Expected the SDK operation to reject");
}
export * from "/packages/worker/src/snapshot-store.ts";
export { OpfsSnapshotStore } from "/packages/worker/src/opfs-snapshot-store.ts";
export function image(marker = 7, pageSize = 512) {
  const bytes = new Uint8Array(pageSize);
  bytes.set(new TextEncoder().encode("SQLite format 3\\0"));
  bytes[16] = pageSize === 65536 ? 0 : pageSize >> 8;
  bytes[17] = pageSize === 65536 ? 1 : pageSize & 255;
  bytes[100] = marker;
  return bytes;
}
export async function raw(name, mutate) {
  const db = await new Promise((yes, no) => {
    const request = indexedDB.open("frankensqlite:snapshot:v1:" + name);
    request.onsuccess = () => yes(request.result);
    request.onerror = () => no(request.error);
  });
  try {
    return await new Promise((yes, no) => {
      const tx = db.transaction("snapshots", mutate ? "readwrite" : "readonly");
      const store = tx.objectStore("snapshots");
      const request = store.get("head");
      let result;
      request.onsuccess = () => {
        result = request.result;
        if (mutate) store.put(mutate(result), "head");
      };
      tx.oncomplete = () => yes(result);
      tx.onabort = () => no(tx.error);
    });
  } finally { db.close(); }
}
`;

before(async () => {
  const wasmAssets = new Map();
  if (wasmModule) {
    const binary = basename(wasmModule, ".js") + "_bg.wasm";
    wasmAssets.set("/wasm/core.js", ["text/javascript", await readFile(wasmModule)]);
    wasmAssets.set("/wasm/" + binary, [
      "application/wasm", await readFile(resolve(dirname(wasmModule), binary)),
    ]);
  }
  server = createServer(async (request, response) => {
    try {
      const pathname = new URL(request.url, "http://localhost").pathname;
      response.setHeader("Cache-Control", "no-store");
      const asset = wasmAssets.get(pathname);
      if (asset) {
        response.setHeader("Content-Type", asset[0]);
        response.end(asset[1]);
        return;
      }
      if (pathname === "/") {
        response.setHeader("Content-Type", "text/html");
        response.end("<!doctype html><title>Snapshot storage verification</title>");
        return;
      }
      response.setHeader("Content-Type", "text/javascript");
      if (pathname === "/fault-worker.js") {
        response.end('import "/fault-listener.js"; import "/packages/worker/src/worker.ts";');
        return;
      }
      if (pathname === "/fault-listener.js") {
        response.end(`
          let failed = false;
          addEventListener("message", event => {
            if (failed) {
              event.stopImmediatePropagation();
              return;
            }
            const r = event.data;
            if ((location.search === "?query" && r.kind === "query" && r.sql === "SELECT 99")
                || (location.search === "?rollback" && r.kind === "transaction" && r.action === "rollback")) {
              failed = true;
              event.stopImmediatePropagation();
              throw new Error("intentional worker failure on " + r.kind);
            }
          });`);
        return;
      }
      if (pathname === "/fixture.js") {
        response.end(fixture);
        return;
      }
      let filename = resolve(root, `.${decodeURIComponent(pathname)}`);
      if (!filename.startsWith(root.endsWith(sep) ? root : root + sep))
        throw new Error("Invalid path");
      if (!/\.[a-z]+$/i.test(filename)) filename += ".ts";
      let source = await readFile(filename, "utf8");
      if (filename.endsWith(".ts")) {
        source = ts.transpileModule(source, {
          fileName: filename,
          compilerOptions: {
            target: ts.ScriptTarget.ES2022,
            module: ts.ModuleKind.ESNext,
            verbatimModuleSyntax: true,
          },
        }).outputText
          .replaceAll('"@frankensqlite/worker"', '"/packages/worker/src/index.ts"')
          .replaceAll('"@frankensqlite/core"', '"/wasm/core.js"');
      }
      response.end(source);
    } catch (error) {
      response.statusCode = 404;
      response.end(String(error));
    }
  });
  await new Promise((yes) => server.listen(0, "127.0.0.1", yes));
  url = `http://127.0.0.1:${server.address().port}`;
  browser = await chromium.launch({
    headless: true,
    ...(process.env.FSQLITE_CHROMIUM_PATH
      ? { executablePath: process.env.FSQLITE_CHROMIUM_PATH }
      : {}),
  });
  context = await browser.newContext();
  console.log(`Browser: ${browser.version()}; snapshot storage on ${url}`);
});

after(async () => {
  await context?.close();
  await browser?.close();
  if (server) await new Promise((yes) => server.close(yes));
});

async function run(body, arg) {
  const page = await context.newPage();
  try {
    await page.goto(url);
    return await page.evaluate(body, arg);
  } finally {
    await page.close();
  }
}

const wasmLifecycle = {
  skip: wasmModule ? false : "Set FSQLITE_WASM_BROWSER_MODULE to a web-target WASM binding",
  timeout: 30_000,
};

test("SDK close drains posted WASM SQL and refuses retained handles", wasmLifecycle, async (t) => {
  const result = await run(async () => {
    const { FrankenDB } = await import("/packages/sdk/src/database.ts");
    const { SqlWorker, rejection } = await import("/fixture.js");
    const worker = new SqlWorker();
    const db = await FrankenDB.open({ worker });
    await db.execute("CREATE TABLE t(v)");
    const statement = await db.prepare("SELECT v FROM t ORDER BY v");
    const write = db.execute("INSERT INTO t VALUES (?)", [17]);
    const read = statement.query();
    const close = db.close();
    const repeated = db.close() === close;
    const count = worker.requests.length;
    const late = await rejection(db.execute("INSERT INTO t VALUES (18)"));
    const [changes, rows] = await Promise.all([write, read, close]);
    const retained = await rejection(statement.query());
    return {
      changes, rows: rows.rowArrays, repeated: repeated && db.close() === close,
      refused: [late.message, retained.message], noExtraPosts: count === worker.requests.length,
      requests: worker.requests,
    };
  });
  t.diagnostic(JSON.stringify(result.requests));
  assert.equal(result.changes, 1);
  assert.deepEqual(result.rows, [[17]]);
  assert.equal(result.repeated, true);
  assert.equal(result.noExtraPosts, true);
  assert.match(result.refused[0], /clos/i);
  assert.match(result.refused[1], /dispos|clos/i);
});

test("SDK transaction ownership isolates foreign writes and nested WASM savepoints", wasmLifecycle, async (t) => {
  const result = await run(async () => {
    const { FrankenDB } = await import("/packages/sdk/src/database.ts");
    const { SqlWorker, rejection } = await import("/fixture.js");
    const worker = new SqlWorker();
    const db = await FrankenDB.open({ worker });
    try {
      await db.execute("CREATE TABLE events(id INTEGER PRIMARY KEY)");
      const entered = Promise.withResolvers();
      const resume = Promise.withResolvers();
      const marker = new Error("rollback root");
      let escaped;
      const rollback = rejection(db.transaction(async (tx) => {
        escaped = tx;
        await tx.execute("INSERT INTO events VALUES (1)");
        entered.resolve();
        await resume.promise;
        throw marker;
      }));
      await Promise.race([entered.promise, rollback.then(error => { throw error; })]);
      const before = worker.requests.length;
      let foreign, refusedBeforePosting;
      try {
        foreign = await rejection(db.execute("INSERT INTO events VALUES (2)"));
        refusedBeforePosting = worker.requests.length === before;
      } finally {
        resume.resolve();
      }
      const original = (await rollback) === marker;
      const expired = await rejection(escaped.execute("INSERT INTO events VALUES (3)"));
      let parentBlocked, childExpired;
      await db.transaction(async (parent) => {
        await parent.transaction(async (child) => {
          await child.execute("INSERT INTO events VALUES (4)");
          parentBlocked = (await rejection(parent.execute("INSERT INTO events VALUES (5)"))).code;
        });
        let savedChild;
        const childError = await rejection(parent.transaction(async (child) => {
          savedChild = child;
          await child.execute("INSERT INTO events VALUES (6)");
          throw marker;
        }));
        if (childError !== marker) throw childError;
        childExpired = (await rejection(savedChild.query("SELECT 1"))).code;
        await parent.transaction(child => child.execute("INSERT INTO events VALUES (7)"));
      });
      return {
        rows: (await db.query("SELECT id FROM events ORDER BY id")).rowArrays,
        original, refusedBeforePosting, codes: [foreign.code, expired.code, parentBlocked, childExpired],
        requests: worker.requests,
      };
    } finally {
      await db.close();
    }
  });
  t.diagnostic(JSON.stringify(result.requests));
  assert.equal(result.original, true);
  assert.equal(result.refusedBeforePosting, true);
  assert.deepEqual(result.codes, ["ERR_FSQLITE_TRANSACTION_OWNERSHIP", "ERR_FSQLITE_TRANSACTION_CLOSED",
    "ERR_FSQLITE_TRANSACTION_OWNERSHIP", "ERR_FSQLITE_TRANSACTION_CLOSED"]);
  assert.deepEqual(result.rows, [[4], [7]]);
});

test("SDK worker crash settles pending requests and releases exclusive snapshot ownership", wasmLifecycle, async (t) => {
  const result = await run(async () => {
    const { FrankenDB } = await import("/packages/sdk/src/database.ts");
    const { SqlWorker, rejection } = await import("/fixture.js");
    const worker = new SqlWorker("query");
    const options = { dbName: crypto.randomUUID(), persistence: "indexeddb-snapshot", snapshotOwnership: "exclusive" };
    const db = await FrankenDB.open({ ...options, worker });
    try {
      await db.execute("CREATE TABLE saved(v)");
      await db.execute("INSERT INTO saved VALUES (13)");
      await db.checkpoint();
      const stmt = await db.prepare("SELECT v FROM saved");
      const { held } = await navigator.locks.query();
      const lease = held.find(lock => lock.name.includes(options.dbName));
      if (lease?.mode !== "exclusive") throw new Error("Missing exclusive session before crash");
      const pending = await Promise.all([rejection(db.query("SELECT 99")), rejection(stmt.query())]);
      const count = worker.requests.length;
      const later = await rejection(db.query("SELECT 1"));
      const retained = await rejection(stmt.query());
      // An uncaught error in a browser worker does not itself terminate its realm.
      // Wait on the same lock before doing any explicit close or termination.
      await navigator.locks.request(lease.name, { signal: AbortSignal.timeout(5_000) }, () => {});
      const close = db.close();
      const repeated = close === db.close();
      const failedClose = await rejection(close);
      const reopened = await FrankenDB.open({ ...options, worker: new SqlWorker() });
      try {
        return {
          message: pending[0].message,
          sameCause: [pending[1], later, retained, failedClose].every(e => e === pending[0]),
          repeated, noExtraPosts: count === worker.requests.length, requests: worker.requests,
          rows: (await reopened.query("SELECT v FROM saved")).rowArrays,
        };
      } finally {
        await reopened.close();
      }
    } finally {
      worker.terminate();
    }
  });
  t.diagnostic(JSON.stringify(result.requests));
  assert.match(result.message, /intentional worker failure on query/);
  assert.equal(result.sameCause, true);
  assert.equal(result.repeated, true);
  assert.equal(result.noExtraPosts, true);
  assert.deepEqual(result.rows, [[13]]);
});

test("SDK keeps the callback error when its WASM worker crashes during rollback", wasmLifecycle, async (t) => {
  const result = await run(async () => {
    const { FrankenDB } = await import("/packages/sdk/src/database.ts");
    const { SqlWorker, rejection } = await import("/fixture.js");
    const worker = new SqlWorker("rollback");
    const db = await FrankenDB.open({ worker });
    try {
      await db.execute("CREATE TABLE t(v)");
      const original = new Error("callback refused");
      const failure = await rejection(db.transaction(async (tx) => {
        await tx.execute("INSERT INTO t VALUES (1)");
        throw original;
      }));
      return {
        originalCause: failure instanceof AggregateError && failure.cause === original && failure.errors[0] === original,
        rollbackError: failure.errors?.[1]?.message,
        terminal: (await rejection(db.query("SELECT 1"))) === failure,
        requests: worker.requests,
      };
    } finally {
      worker.terminate();
    }
  });
  t.diagnostic(JSON.stringify(result.requests));
  assert.equal(result.originalCause, true);
  assert.match(result.rollbackError, /intentional worker failure on transaction/);
  assert.equal(result.terminal, true);
});

test("SDK binds parameters across recursive and sibling CTEs in the WASM worker", {
  skip: wasmModule ? false : "Set FSQLITE_WASM_BROWSER_MODULE to a web-target WASM binding",
}, async () => {
  const rows = await run(async () => {
    const { FrankenDB } = await import("/packages/sdk/src/database.ts");
    const worker = new Worker("/packages/worker/src/worker.ts", { type: "module" });
    const db = await FrankenDB.open({ worker, persistence: "memory" });
    try {
      const batch = [];
      for (const [low, high] of [[2, 4], [6, 8]]) {
        const recursive = await db.query(
          "WITH RECURSIVE ids(n) AS (SELECT :low UNION ALL SELECT n+1 FROM ids WHERE n<:high) SELECT n FROM ids ORDER BY n",
          { low, high },
        );
        const siblings = await db.query(
          "WITH unused AS (SELECT ?), a(x) AS (SELECT ?), b(y) AS (SELECT ?) SELECT x,y FROM a,b",
          [-1, low, high],
        );
        batch.push([recursive.rowArrays, siblings.rowArrays]);
      }
      return batch;
    } finally {
      await db.close();
    }
  });
  assert.deepEqual(rows, [
    [[[2], [3], [4]], [[2, 4]]],
    [[[6], [7], [8]], [[6, 8]]],
  ]);
});

for (const persistence of ["indexeddb-snapshot", "opfs-snapshot"]) {
  test(`SDK reopens checkpointed WASM SQL rows with ${persistence}`, {
    skip: wasmModule ? false : "Set FSQLITE_WASM_BROWSER_MODULE to a web-target WASM binding",
  }, async () => {
    const config = { dbName: crypto.randomUUID(), persistence };
    const saved = await run(async (config) => {
      const { FrankenDB } = await import("/packages/sdk/src/database.ts");
      const worker = new Worker("/packages/worker/src/worker.ts", { type: "module" });
      const db = await FrankenDB.open({ ...config, worker });
      await db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)");
      await db.execute("INSERT INTO t VALUES (1)");
      const receipt = await db.checkpoint();
      await db.execute("INSERT INTO t VALUES (2)");
      const live = (await db.query("SELECT id FROM t ORDER BY id")).rowArrays;
      let sqlError;
      try {
        await db.query("SELECT * FROM missing_checkpoint_table");
      } catch (error) {
        sqlError = error.message;
      }
      return { path: db.path, receipt, live, sqlError };
    }, config);
    assert.equal(saved.path, config.dbName);
    assert.equal(saved.receipt.parentRevision, null);
    assert.equal(typeof saved.receipt.revision, "string");
    assert.deepEqual(saved.live, [[1], [2]]);
    assert.match(saved.sqlError, /missing_checkpoint_table/i);

    const reopened = await run(async (config) => {
      const { FrankenDB } = await import("/packages/sdk/src/database.ts");
      const worker = new Worker("/packages/worker/src/worker.ts", { type: "module" });
      const db = await FrankenDB.open({ ...config, worker });
      const rows = (await db.query("SELECT id FROM t ORDER BY id")).rowArrays;
      const integrity = (await db.query("PRAGMA integrity_check")).rowArrays;
      const revision = db.snapshotRevision;
      await db.close();
      return { rows, integrity, revision };
    }, config);
    assert.equal(reopened.revision, saved.receipt.revision);
    assert.deepEqual(reopened.rows, [[1]]);
    assert.deepEqual(reopened.integrity, [["ok"]]);
  });
}

async function inFreshWorker(body, arg) {
  const page = await context.newPage();
  try {
    await page.goto(url);
    const [worker] = await Promise.all([
      page.waitForEvent("worker"),
      page.evaluateHandle(() => new Worker("/fixture.js", { type: "module" })),
    ]);
    return await worker.evaluate(body, arg);
  } finally {
    await page.close();
  }
}

for (const backend of ["IndexedDbSnapshotStore", "OpfsSnapshotStore"]) {
  test(`${backend} reopens the acknowledged bytes in a fresh worker`, async () => {
    const name = crypto.randomUUID();
    const saved = await inFreshWorker(async ({ backend, name }) => {
      const api = await import(self.location.href);
      const store = await api[backend].open(name);
      return store.save(api.image(31), null);
    }, { backend, name });

    const restored = await inFreshWorker(async ({ backend, name }) => {
      const api = await import(self.location.href);
      const store = await api[backend].open(name);
      const head = await store.load();
      return { revision: head.revision, bytes: Array.from(head.bytes) };
    }, { backend, name });

    const expected = new Uint8Array(512);
    expected.set(new TextEncoder().encode("SQLite format 3\0"));
    expected[16] = 2;
    expected[100] = 31;
    assert.equal(restored.revision, saved.revision);
    assert.deepEqual(Uint8Array.from(restored.bytes), expected);
  });
}

test("empty storage, explicit close, and invalid names fail without inventing data", async () => {
  const result = await run(async () => {
    const { IndexedDbSnapshotStore: Store } = await import("/fixture.js");
    const store = await Store.open(crypto.randomUUID());
    const empty = await store.load();
    store.close();
    store.close();
    const closed = await store.load().catch((e) => e.code);
    const bad = [];
    for (const name of ["", " ", ":memory:", "a\0b", "x".repeat(257), null]) {
      bad.push(await Store.open(name).catch((e) => e.code));
    }
    return { empty, closed, bad };
  });
  assert.equal(result.empty, null);
  assert.equal(result.closed, "ERR_FSQLITE_SNAPSHOT_CLOSED");
  assert.deepEqual(result.bad, Array(6).fill("ERR_FSQLITE_SNAPSHOT_INPUT"));
});

test("save owns an exact copy before hashing; reopen returns matching bytes and checksum", async () => {
  const result = await run(async () => {
    const { IndexedDbSnapshotStore: Store, image } = await import("/fixture.js");
    const name = crypto.randomUUID();
    const store = await Store.open(name);
    const backing = new Uint8Array(640);
    const bytes = backing.subarray(64, 576);
    bytes.set(image(19));
    const pending = store.save(bytes, null);
    bytes.fill(2); // The caller still owns its buffer, but cannot mutate the pending save.
    const saved = await pending;
    store.close();
    const reopened = await Store.open(name);
    const loaded = await reopened.load();
    loaded.bytes[100] = 88;
    const again = await reopened.load();
    reopened.close();
    return {
      saved,
      size: again.bytes.length,
      marker: again.bytes[100],
      revision: again.revision,
      checksum: again.sha256,
      inputSize: backing.byteLength,
    };
  });
  assert.equal(result.size, 512);
  assert.equal(result.marker, 19);
  assert.equal(result.saved.parentRevision, null);
  assert.equal(result.saved.revision, result.revision);
  assert.equal(result.saved.sha256, result.checksum);
  assert.match(result.checksum, /^[a-f0-9]{64}$/);
  assert.equal(result.inputSize, 640);
});

test("compare-and-swap rejects both stale updates and accidental create-overwrite", async () => {
  const result = await run(async () => {
    const { IndexedDbSnapshotStore: Store, image } = await import("/fixture.js");
    const store = await Store.open(crypto.randomUUID());
    const first = await store.save(image(1), null);
    const second = await store.save(image(2), first.revision);
    const stale = await store.save(image(3), first.revision).catch((e) => e.code);
    const create = await store.save(image(4), null).catch((e) => e.code);
    const current = await store.load();
    store.close();
    return { first, second, stale, create, marker: current.bytes[100] };
  });
  assert.notEqual(result.first.revision, result.second.revision);
  assert.equal(result.second.parentRevision, result.first.revision);
  assert.equal(result.stale, "ERR_FSQLITE_SNAPSHOT_CONFLICT");
  assert.equal(result.create, "ERR_FSQLITE_SNAPSHOT_CONFLICT");
  assert.equal(result.marker, 2);
});

test("independent IndexedDB connections have exactly one winner on a shared revision", async () => {
  const result = await run(async () => {
    const { IndexedDbSnapshotStore: Store, image } = await import("/fixture.js");
    const name = crypto.randomUUID();
    const stores = await Promise.all([Store.open(name), Store.open(name)]);
    const settled = await Promise.allSettled(
      stores.map((store, i) => store.save(image(i + 1), null)),
    );
    const current = await stores[0].load();
    stores.forEach((s) => s.close());
    return {
      settled: settled.map((r) =>
        r.status === "fulfilled" ? { ok: r.value.revision } : { error: r.reason.code },
      ),
      marker: current.bytes[100],
      revision: current.revision,
    };
  });
  assert.equal(result.settled.filter((x) => x.ok).length, 1);
  assert.equal(result.settled.filter((x) => x.error === "ERR_FSQLITE_SNAPSHOT_CONFLICT").length, 1);
  assert.equal(result.settled[result.marker - 1].ok, result.revision);
});

test("different database names persist independently", async () => {
  const result = await run(async () => {
    const { IndexedDbSnapshotStore: Store, image } = await import("/fixture.js");
    const stores = await Promise.all([
      Store.open(crypto.randomUUID()),
      Store.open(crypto.randomUUID()),
    ]);
    await Promise.all(stores.map((s, i) => s.save(image(10 + i), null)));
    const markers = await Promise.all(stores.map(async (s) => (await s.load()).bytes[100]));
    stores.forEach((s) => s.close());
    return markers;
  });
  assert.deepEqual(result, [10, 11]);
});

for (const field of [
  "format",
  "name",
  "revision",
  "parentRevision",
  "byteLength",
  "sha256",
  "bytes",
]) {
  test(`corrupt ${field} is rejected and never reset to an empty database`, async () => {
    const page = await context.newPage();
    try {
      await page.goto(url);
      const result = await page.evaluate(async (field) => {
        const { IndexedDbSnapshotStore: Store, image, raw } = await import("/fixture.js");
        const name = crypto.randomUUID();
        const store = await Store.open(name);
        await store.save(image(), null);
        await raw(name, (value) => ({
          ...value,
          [field]: field === "bytes" ? new Uint8Array(value.bytes) : "invalid",
        }));
        const error = await store.load().catch((e) => e.code);
        const retained = await raw(name);
        store.close();
        return {
          error,
          retained: retained[field] === "invalid" || retained[field] instanceof Uint8Array,
        };
      }, field);
      assert.equal(result.error, "ERR_FSQLITE_SNAPSHOT_CORRUPT");
      assert.equal(result.retained, true);
    } finally {
      await page.close();
    }
  });
}

test("bit corruption with an intact SQLite header is detected by the checksum", async () => {
  const result = await run(async () => {
    const { IndexedDbSnapshotStore: Store, image, raw } = await import("/fixture.js");
    const name = crypto.randomUUID();
    const store = await Store.open(name);
    await store.save(image(), null);
    await raw(name, (record) => {
      new Uint8Array(record.bytes)[200] ^= 1;
      return record;
    });
    const error = await store.load().catch((e) => e.code);
    store.close();
    return error;
  });
  assert.equal(result, "ERR_FSQLITE_SNAPSHOT_CORRUPT");
});

test("invalid images, oversized snapshots, and bad revisions cannot change the saved head", async () => {
  const result = await run(async () => {
    const {
      IndexedDbSnapshotStore: Store,
      image,
      MAX_SNAPSHOT_BYTES,
    } = await import("/fixture.js");
    const store = await Store.open(crypto.randomUUID());
    const baseline = await store.save(image(8), null);
    const invalidPage = image();
    invalidPage[16] = 3;
    const errors = [];
    for (const bytes of [
      new Uint8Array(),
      new Uint8Array(513),
      invalidPage,
      new Uint8Array(MAX_SNAPSHOT_BYTES + 1),
    ]) {
      errors.push(await store.save(bytes, baseline.revision).catch((e) => e.code));
    }
    errors.push(await store.save(image(), "bad-token").catch((e) => e.code));
    const after = await store.load();
    store.close();
    return { errors, same: after.revision === baseline.revision, marker: after.bytes[100] };
  });
  assert.deepEqual(result.errors, [
    "ERR_FSQLITE_SNAPSHOT_CORRUPT",
    "ERR_FSQLITE_SNAPSHOT_CORRUPT",
    "ERR_FSQLITE_SNAPSHOT_CORRUPT",
    "ERR_FSQLITE_SNAPSHOT_TOO_LARGE",
    "ERR_FSQLITE_SNAPSHOT_INPUT",
  ]);
  assert.equal(result.same, true);
  assert.equal(result.marker, 8);
});

test("the SQLite 65536-byte page-size sentinel is supported", async () => {
  const result = await run(async () => {
    const { IndexedDbSnapshotStore: Store, image } = await import("/fixture.js");
    const store = await Store.open(crypto.randomUUID());
    const saved = await store.save(image(9, 65536), null);
    const loaded = await store.load();
    store.close();
    return { length: saved.byteLength, marker: loaded.bytes[100] };
  });
  assert.deepEqual(result, { length: 65536, marker: 9 });
});

test("a write request success is not a durable acknowledgement if its transaction aborts", async () => {
  const result = await run(async () => {
    const { IndexedDbSnapshotStore: Store, image } = await import("/fixture.js");
    const store = await Store.open(crypto.randomUUID());
    const baseline = await store.save(image(3), null);
    const original = IDBObjectStore.prototype.put;
    let putSucceeded = false;
    IDBObjectStore.prototype.put = function (...args) {
      const request = original.apply(this, args);
      request.addEventListener("success", () => {
        putSucceeded = true;
        this.transaction.abort();
      });
      return request;
    };
    let rejected;
    try {
      rejected = await store.save(image(4), baseline.revision).then(
        () => false,
        () => true,
      );
    } finally {
      IDBObjectStore.prototype.put = original;
    }
    const loaded = await store.load();
    store.close();
    return {
      putSucceeded,
      rejected,
      same: baseline.revision === loaded.revision,
      marker: loaded.bytes[100],
    };
  });
  assert.deepEqual(result, { putSucceeded: true, rejected: true, same: true, marker: 3 });
});

test("quota failure aborts publication, keeps the previous image, and allows retry", async () => {
  const result = await run(async () => {
    const { IndexedDbSnapshotStore: Store, image } = await import("/fixture.js");
    const store = await Store.open(crypto.randomUUID());
    const baseline = await store.save(image(1), null);
    const original = IDBObjectStore.prototype.put;
    IDBObjectStore.prototype.put = () => {
      throw new DOMException("Injected quota failure", "QuotaExceededError");
    };
    let error;
    try {
      error = await store.save(image(2), baseline.revision).catch((e) => e.name);
    } finally {
      IDBObjectStore.prototype.put = original;
    }
    const beforeRetry = await store.load();
    await store.save(image(3), baseline.revision);
    const afterRetry = await store.load();
    store.close();
    return { error, before: beforeRetry.bytes[100], after: afterRetry.bytes[100] };
  });
  assert.deepEqual(result, { error: "QuotaExceededError", before: 1, after: 3 });
});

test("close during asynchronous hashing prevents later write admission", async () => {
  const result = await run(async () => {
    const { IndexedDbSnapshotStore: Store, image } = await import("/fixture.js");
    const name = crypto.randomUUID();
    const store = await Store.open(name);
    const original = crypto.subtle.digest;
    let resume;
    crypto.subtle.digest = async function (...args) {
      await new Promise((resolve) => {
        resume = resolve;
      });
      return original.apply(this, args);
    };
    const pending = store.save(image(), null);
    store.close();
    resume();
    let error;
    try {
      error = await pending.catch((e) => e.code);
    } finally {
      crypto.subtle.digest = original;
    }
    const reopened = await Store.open(name);
    const saved = await reopened.load();
    reopened.close();
    return { error, saved };
  });
  assert.deepEqual(result, { error: "ERR_FSQLITE_SNAPSHOT_CLOSED", saved: null });
});

test("schema upgrades close old handles; unknown newer schemas never downgrade", async () => {
  const result = await run(async () => {
    const { IndexedDbSnapshotStore: Store, image } = await import("/fixture.js");
    const name = crypto.randomUUID();
    const store = await Store.open(name);
    await store.save(image(), null);
    const upgraded = await new Promise((yes, no) => {
      const request = indexedDB.open(`frankensqlite:snapshot:v1:${name}`, 2);
      request.onsuccess = () => yes(request.result);
      request.onerror = () => no(request.error);
    });
    upgraded.close();
    return {
      closed: await store.load().catch((e) => e.code),
      open: await Store.open(name).catch((e) => e.name),
    };
  });
  assert.deepEqual(result, { closed: "ERR_FSQLITE_SNAPSHOT_CLOSED", open: "VersionError" });
});
