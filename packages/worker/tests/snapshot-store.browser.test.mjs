// Run with: node --test packages/worker/tests/snapshot-store.browser.test.mjs
// Uses actual Chromium IndexedDB/Web Crypto, not an in-memory IDB replacement.

import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { createServer } from "node:http";
import { resolve, sep } from "node:path";
import { after, before, test } from "node:test";
import { fileURLToPath } from "node:url";

const { chromium } = await import(process.env.FSQLITE_PLAYWRIGHT_MODULE ?? "@playwright/test");
const { default: ts } = await import(process.env.FSQLITE_TYPESCRIPT_MODULE ?? "typescript");
const root = fileURLToPath(new URL("../../../", import.meta.url));
let server;
let browser;
let context;
let url;

const fixture = `
export * from "/packages/worker/src/snapshot-store.ts";
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
  server = createServer(async (request, response) => {
    try {
      const pathname = new URL(request.url, "http://localhost").pathname;
      response.setHeader("Cache-Control", "no-store");
      if (pathname === "/") {
        response.setHeader("Content-Type", "text/html");
        response.end("<!doctype html><title>Snapshot storage verification</title>");
        return;
      }
      response.setHeader("Content-Type", "text/javascript");
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
        }).outputText;
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
  console.log(`Browser: ${browser.version()}; real IndexedDB on ${url}`);
});

after(async () => {
  await context?.close();
  await browser?.close();
  if (server) await new Promise((yes) => server.close(yes));
});

async function run(body) {
  const page = await context.newPage();
  try {
    await page.goto(url);
    return await page.evaluate(body);
  } finally {
    await page.close();
  }
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
