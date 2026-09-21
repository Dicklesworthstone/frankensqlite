// Production SDK/host tests plus actual worker.ts in Node threads. Node SQLite
// and a Web Worker shim are reference infrastructure, NOT browser/WASM proof.

import assert from "node:assert/strict";
import { once } from "node:events";
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import { Worker } from "node:worker_threads";
import { tryEncodeQueryResult } from "../../worker/src/result-codec.ts";
import { sqliteSnapshotWorker } from "../../worker/tests/helpers/snapshot-sqlite-core.mjs";
import { FrankenDB } from "../src/database.ts";
import { FrankenSQLiteError } from "../src/errors.ts";
import { FrankenWorkerClient } from "../src/worker-client.ts";
import { ControlledWorker } from "./helpers/controlled-worker.ts";

function data(value = 1) {
  return {
    columns: ["value"],
    columnCount: 1,
    columnTypes: ["integer"],
    rows: [{ value }],
    rowArrays: [[value]],
    changes: 0,
  };
}
async function controlled(mode = "binary", accepted = mode) {
  const worker = new ControlledWorker();
  worker.onPost = (request) => {
    if (request.kind === "init")
      queueMicrotask(() =>
        worker.reply({
          kind: "ready",
          requestId: request.requestId,
          data: {
            path: ":memory:",
            persistence: "memory",
            ...(accepted === null ? {} : { resultEncoding: accepted }),
          },
        }),
      );
    if (request.kind === "close")
      queueMicrotask(() => worker.reply({ kind: "close-result", requestId: request.requestId }));
  };
  const client = new FrankenWorkerClient(worker, { maxPendingRequests: 1 });
  await client.init({ resultEncoding: mode });
  return { client, worker };
}

test("SDK rejects an invalid encoding before creating a worker or detaching snapshots", async () => {
  let created = 0;
  const snapshot = new Uint8Array(128);
  await assert.rejects(
    FrankenDB.open({
      resultEncoding: "typo",
      snapshot,
      worker: () => {
        created++;
        throw new Error("created");
      },
    }),
    { code: "ERR_FSQLITE_RESULT_ENCODING" },
  );
  assert.equal(created, 0);
  assert.equal(snapshot.byteLength, 128);
});

test("SDK exposes the acknowledged mode and preserves an older structured worker", async () => {
  for (const accepted of ["binary", "structured-clone", null]) {
    const { client, worker } = await controlled("binary", accepted);
    assert.equal(client.resultEncoding, accepted ?? "structured-clone");
    const pending = client.query("SELECT 1");
    worker.reply({
      kind: "query-result",
      requestId: worker.requests.at(-1).requestId,
      data: data(),
    });
    assert.deepEqual(await pending, data());
    await client.close();
  }
});

test("SDK refuses unsolicited encoding enablement during initialization", async () => {
  const worker = new ControlledWorker();
  worker.onPost = (request) =>
    queueMicrotask(() =>
      worker.reply({
        kind: "ready",
        requestId: request.requestId,
        data: { path: ":memory:", persistence: "memory", resultEncoding: "binary" },
      }),
    );
  await assert.rejects(FrankenDB.open({ worker }), { code: "ERR_FSQLITE_RESULT_ENCODING" });
  assert.equal(worker.terminateCount, 1);
});

test("both client query methods decode transferable frames to the existing public shape", async () => {
  const { client, worker } = await controlled();
  for (const run of [() => client.query("SELECT ?", [1]), () => client.queryPrepared("1", [1])]) {
    const pending = run();
    worker.reply({
      kind: "query-binary-result",
      encoding: "fqr1",
      requestId: worker.requests.at(-1).requestId,
      data: tryEncodeQueryResult(data(9223372036854775807n)),
    });
    assert.deepEqual(await pending, data(9223372036854775807n));
    assert.equal(client.requestQueue.pendingRequests, 0);
    assert.equal(client.requestQueue.pendingBytes, 0);
  }
  await client.close();
});

test("SDK retains embedded NUL and lone UTF-16 surrogates from a typed core result", async () => {
  const { client, worker } = await controlled();
  const pending = client.query("typed fixture");
  const expected = data("a\0\ud800\udfff😀");
  worker.reply({
    kind: "query-binary-result",
    encoding: "fqr1",
    requestId: worker.requests.at(-1).requestId,
    data: tryEncodeQueryResult(expected),
  });
  assert.deepEqual(await pending, expected);
  await client.close();
});

for (const damage of ["magic", "version", "truncated", "detached", "envelope"]) {
  test(`malformed ${damage} frames reject promptly, release capacity, and never resend SQL`, async () => {
    const { client, worker } = await controlled();
    const pending = client.query("INSERT RETURNING value");
    let buffer = tryEncodeQueryResult(data());
    let encoding = "fqr1";
    if (damage === "magic") new Uint8Array(buffer)[0] = 0;
    if (damage === "version") new DataView(buffer).setUint16(4, 99, true);
    if (damage === "truncated") buffer = buffer.slice(0, -1);
    if (damage === "detached") structuredClone(buffer, { transfer: [buffer] });
    if (damage === "envelope") encoding = "fqr2";
    const id = worker.requests.at(-1).requestId;
    worker.reply({ kind: "query-binary-result", encoding, requestId: id, data: buffer });
    await assert.rejects(
      pending,
      (error) =>
        error instanceof FrankenSQLiteError &&
        error.code === "ERR_FSQLITE_RESULT_DECODE" &&
        error.transient === false &&
        /may have executed/.test(error.suggestion),
    );
    assert.equal(client.requestQueue.pendingRequests, 0);
    assert.equal(worker.requests.filter((r) => r.kind === "query").length, 1);
    // Ignore even malformed late/duplicate responses after settlement.
    worker.reply({ kind: "query-binary-result", encoding: "fqr2", requestId: id, data: null });
    const next = client.query("SELECT 1");
    worker.reply({
      kind: "query-result",
      requestId: worker.requests.at(-1).requestId,
      data: data(),
    });
    assert.deepEqual(await next, data());
    await client.close();
  });
}

test("unnegotiated binary replies fail without decoding or leaking a reservation", async () => {
  const { client, worker } = await controlled("structured-clone");
  const pending = client.query("SELECT 1");
  worker.reply({
    kind: "query-binary-result",
    encoding: "fqr1",
    requestId: worker.requests.at(-1).requestId,
    data: tryEncodeQueryResult(data()),
  });
  await assert.rejects(pending, { code: "ERR_FSQLITE_RESULT_DECODE" });
  assert.equal(client.requestQueue.pendingRequests, 0);
  await client.close();
});

test("decode failure in a managed callback rolls back successfully executed SQL", async () => {
  const f = sqliteSnapshotWorker();
  const handle = f.host.handle.bind(f.host);
  f.host.handle = async (request) => {
    const reply = await handle(request);
    if (reply.kind === "query-binary-result") new Uint8Array(reply.data)[0] = 0;
    return reply;
  };
  const db = await FrankenDB.open({ worker: f.worker, resultEncoding: "binary" });
  await db.execute("CREATE TABLE items(id INTEGER PRIMARY KEY)");
  await assert.rejects(
    db.transaction(async (tx) => {
      await tx.query("INSERT INTO items VALUES(1) RETURNING id");
    }),
    { code: "ERR_FSQLITE_RESULT_DECODE" },
  );
  const reader = new DatabaseSync(f.handles[0].path, { readOnly: true });
  assert.equal(reader.prepare("SELECT count(*) n FROM items").get().n, 0);
  reader.close();
  await db.close();
});

async function realWorker(t, mode, workerData = {}) {
  const raw = new Worker(new URL("../../worker/tests/helpers/result-worker.mjs", import.meta.url), {
    workerData,
  });
  t.after(() => raw.terminate());
  const audits = new Map(),
    waiters = new Map();
  raw.on("message", (message) => {
    if (!message.audit) return;
    audits.set(message.id, message);
    waiters.get(message.id)?.(message);
    waiters.delete(message.id);
  });
  const [boot] = await once(raw, "message");
  assert.equal(boot.booted, true);
  const wrappers = new Map();
  const requests = [];
  const worker = {
    addEventListener(type, listener) {
      const wrap =
        type === "message"
          ? (data) => {
              if (!data.audit && !data.booted) listener({ data });
            }
          : (error) => listener({ message: error.message });
      wrappers.set(listener, wrap);
      raw.on(type, wrap);
    },
    removeEventListener(type, listener) {
      raw.off(type, wrappers.get(listener));
      wrappers.delete(listener);
    },
    postMessage(request, transfer = []) {
      requests.push(request);
      raw.postMessage(request, transfer);
    },
    terminate() {
      void raw.terminate();
    },
  };
  const db = await FrankenDB.open({
    worker,
    ...(mode === undefined ? {} : { resultEncoding: mode }),
  });
  return {
    db,
    requests,
    audit(id) {
      return audits.has(id)
        ? Promise.resolve(audits.get(id))
        : new Promise((resolve) => waiters.set(id, resolve));
    },
  };
}

for (const mode of ["binary", "auto", undefined]) {
  test(`real worker entry: ${mode ?? "default"} mode preserves exact large and small query values`, {
    timeout: 15000,
  }, async (t) => {
    const f = await realWorker(t, mode);
    assert.equal(f.db.resultEncoding, mode ?? "structured-clone");
    const blob = Uint8Array.of(0, 7, 255);
    // Node 22's SQL text result conversion truncates embedded NUL independently
    // of transport. The typed core test above covers exact NUL/surrogate values.
    const small = await f.db.query("SELECT ? AS id, ? AS body, ? AS bytes", [
      (1n << 63n) - 1n,
      "a😀",
      blob,
    ]);
    assert.deepEqual(small.rowArrays, [[(1n << 63n) - 1n, "a😀", blob]]);
    const smallAudit = await f.audit(f.requests.at(-1).requestId);
    assert.equal(smallAudit.kind, mode === "binary" ? "query-binary-result" : "query-result");
    const large =
      await f.db.query(`WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<12000)
      SELECT i AS id, printf('row-%05d',i) AS body FROM n`);
    assert.equal(large.rows.length, 12000);
    for (let i = 0; i < 12000; i++)
      assert.deepEqual(large.rowArrays[i], [
        BigInt(i + 1),
        `row-${String(i + 1).padStart(5, "0")}`,
      ]);
    const audit = await f.audit(f.requests.at(-1).requestId);
    assert.equal(audit.kind, mode === undefined ? "query-result" : "query-binary-result");
    assert.equal(audit.before.length, mode === undefined ? 0 : 1);
    if (mode !== undefined) {
      assert.ok(audit.before[0] > 65536);
      assert.deepEqual(audit.after, [0]);
    }
    // Receiver blobs do not retain the frame or source parameter allocation.
    small.rows[0].bytes[0] = 9;
    assert.equal(blob[0], 0);
    await f.db.close();
  });
}

test("real worker entry: prepared binary reads and RETURNING preserve rollback and persisted state", {
  timeout: 15000,
}, async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "fsqlite-result-reopen-"));
  const path = join(directory, "db.sqlite");
  const f = await realWorker(t, "binary", { path });
  await f.db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT)");
  await f.db.transaction(async (tx) => {
    await tx.executeMany("INSERT INTO t VALUES(?,?)", [
      [1, "kept"],
      [2, "also kept"],
    ]);
    const stmt = await tx.prepare("SELECT id,v FROM t WHERE id>? ORDER BY id");
    assert.deepEqual((await stmt.query([0])).rowArrays, [
      [1n, "kept"],
      [2n, "also kept"],
    ]);
  });
  await assert.rejects(
    f.db.transaction(async (tx) => {
      assert.deepEqual(
        (await tx.query("INSERT INTO t VALUES(3,'discarded') RETURNING id")).rowArrays,
        [[3n]],
      );
      throw new Error("rollback");
    }),
    /rollback/,
  );
  await f.db.close();
  const reopened = new DatabaseSync(path, { readOnly: true });
  assert.deepEqual(
    reopened
      .prepare("SELECT id FROM t ORDER BY id")
      .all()
      .map((r) => r.id),
    [1, 2],
  );
  assert.equal(reopened.prepare("PRAGMA integrity_check").get().integrity_check, "ok");
  reopened.close();
});

test("real worker entry: queued binary replies settle before close disposes the transport", {
  timeout: 15000,
}, async (t) => {
  const f = await realWorker(t, "binary");
  const first = f.db.query("SELECT 1 AS n"),
    second = f.db.query("SELECT 2 AS n");
  const closing = f.db.close();
  const [a, b] = await Promise.all([first, second, closing]);
  assert.deepEqual(a.rowArrays, [[1n]]);
  assert.deepEqual(b.rowArrays, [[2n]]);
  assert.equal(f.db.requestQueue.pendingRequests, 0);
  await assert.rejects(f.db.query("SELECT 3"));
});

test("real worker entry: an extension result stays structured even after binary negotiation", {
  timeout: 15000,
}, async (t) => {
  const f = await realWorker(t, "binary", { extraMetadata: true });
  const value = await f.db.query("SELECT 1 AS n");
  assert.deepEqual(value.extension, { keep: true });
  assert.deepEqual(value.rowArrays, [[1n]]);
  const audit = await f.audit(f.requests.at(-1).requestId);
  assert.equal(audit.kind, "query-result");
  assert.deepEqual(audit.before, []);
  await f.db.close();
});
