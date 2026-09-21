// Real Node workers load the production worker.ts entry point and transfer path.
// Only the unavailable FrankenSQLite WASM core is replaced by native SQLite.

import assert from "node:assert/strict";
import { mkdtemp, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import { Worker } from "node:worker_threads";
import { FrankenSnapshotPool } from "../src/index.ts";

const options = { timeout: 20000 };
function observe(promise) {
  const record = { settled: false };
  record.promise = promise.then(
    (value) => {
      record.settled = true;
      return { value };
    },
    (error) => {
      record.settled = true;
      return { error };
    },
  );
  return record;
}
async function until(predicate) {
  const end = performance.now() + 10000;
  while (!predicate()) {
    if (performance.now() > end) throw new Error("Timed out waiting for actual worker evidence");
    await new Promise((resolve) => setTimeout(resolve, 2));
  }
}
async function databaseImage(count) {
  const directory = await mkdtemp(join(tmpdir(), "fsqlite-pool-thread-source-"));
  const path = join(directory, "source.sqlite"),
    db = new DatabaseSync(path);
  try {
    db.exec("CREATE TABLE records(id INTEGER PRIMARY KEY,label TEXT,b BLOB,large INTEGER);BEGIN");
    const insert = db.prepare("INSERT INTO records VALUES(?,?,?,?)");
    for (let id = 1; id <= count; id++)
      insert.run(id, `row-${id}-λ`, Uint8Array.of(id % 251, (id + 1) % 251), 9223372036854775807n);
    db.exec("COMMIT");
  } finally {
    db.close();
  }
  return new Uint8Array(await readFile(path));
}
async function setup(t, { workers = 2, rows = 10, resultEncoding = "binary" } = {}) {
  const gate = new Int32Array(new SharedArrayBuffer(8)),
    threads = [];
  let pool;
  t.after(async () => {
    Atomics.store(gate, 0, 1);
    Atomics.notify(gate, 0);
    if (pool) await pool.close().catch(() => {});
    await Promise.all(threads.map((thread) => thread.native.terminate()));
  });
  const worker = () => {
    const native = new Worker(
      new URL("../../worker/tests/helpers/result-worker.mjs", import.meta.url),
      {
        execArgv: process.execArgv,
        workerData: { poolBarrier: gate.buffer },
      },
    );
    const listeners = { message: new Set(), error: new Set() },
      audit = [];
    let terminating = false,
      hadError = false,
      path;
    const exited = new Promise((resolve) =>
      native.once("exit", (code) => {
        if (!terminating && !hadError)
          for (const fn of [...listeners.error]) fn({ message: `Native worker exited ${code}` });
        resolve(code);
      }),
    );
    native.on("message", (data) => {
      if (data.audit) {
        audit.push(data);
        return;
      }
      if (data.kind === "ready") path = data.data.path;
      if (data.kind) for (const fn of [...listeners.message]) fn({ data });
    });
    native.on("error", (error) => {
      hadError = true;
      for (const fn of [...listeners.error]) fn({ message: error.message });
    });
    const bridge = {
      addEventListener(type, fn) {
        listeners[type].add(fn);
      },
      removeEventListener(type, fn) {
        listeners[type].delete(fn);
      },
      postMessage(message, transfer) {
        native.postMessage(message, transfer);
      },
      terminate() {
        terminating = true;
        void native.terminate();
      },
    };
    threads.push({
      native,
      bridge,
      audit,
      exited,
      listeners,
      get path() {
        return path;
      },
    });
    return bridge;
  };
  const image = await databaseImage(rows);
  pool = await FrankenSnapshotPool.open(image, {
    workers,
    maxPendingQueries: 32,
    resultEncoding,
    worker,
  });
  return {
    pool,
    threads,
    gate,
    image,
    release() {
      Atomics.store(gate, 0, 1);
      Atomics.notify(gate, 0);
    },
  };
}

test(
  "three actual SQLite executions rendezvous concurrently before any is allowed to finish",
  options,
  async (t) => {
    const f = await setup(t, { workers: 3, rows: 20 });
    const reads = Array.from({ length: 20 }, (_, i) =>
      f.pool.query("SELECT __pool_hold(id) AS id FROM records WHERE id=?", [i + 1]),
    );
    await until(() => Atomics.load(f.gate, 1) === 3);
    assert.equal(f.pool.stats.activeQueries, 3);
    assert.equal(f.pool.stats.waitingQueries, 17);
    assert.equal(new Set(f.threads.map((thread) => thread.path)).size, 3);
    f.release();
    assert.deepEqual(
      (await Promise.all(reads)).map((result) => result.rows[0].id),
      Array.from({ length: 20 }, (_, i) => BigInt(i + 1)),
    );
    assert.equal(Atomics.load(f.gate, 1), 20);
    await f.pool.close();
    await Promise.all(f.threads.map((thread) => thread.exited));
    assert.ok(
      f.threads.every(
        (thread) => thread.listeners.message.size === 0 && thread.listeners.error.size === 0,
      ),
    );
  },
);

test(
  "20,000 ordered rows retain exact int64, Unicode and blobs through real transferable results",
  options,
  async (t) => {
    const f = await setup(t, { workers: 2, rows: 20000 });
    const result = await f.pool.query("SELECT id,label,b,large FROM records ORDER BY id");
    assert.equal(result.rows.length, 20000);
    assert.equal(result.snapshot.sha256, f.pool.snapshot.sha256);
    for (let i = 0; i < result.rows.length; i++) {
      const row = result.rows[i],
        id = i + 1;
      assert.equal(row.id, BigInt(id));
      assert.equal(row.label, `row-${id}-λ`);
      assert.equal(row.large, 9223372036854775807n);
      assert.deepEqual(row.b, Uint8Array.of(id % 251, (id + 1) % 251));
      assert.deepEqual(result.rowArrays[i], [row.id, row.label, row.b, row.large]);
    }
    // Initialization also queries query_only and produces a small binary reply.
    // Wait for this large result's audit, which is posted after its response.
    await until(() =>
      f.threads.some((thread) =>
        thread.audit.some(
          (record) => record.kind === "query-binary-result" && record.before[0] > 65536,
        ),
      ),
    );
    const packets = f.threads
      .flatMap((thread) => thread.audit)
      .filter((record) => record.kind === "query-binary-result");
    assert.ok(packets.some((packet) => packet.before[0] > 65536));
    assert.ok(packets.every((packet) => packet.after[0] === 0));
  },
);

test(
  "active cancellation drains real SQLite execution before close and successor completion",
  options,
  async (t) => {
    const f = await setup(t, { workers: 1 }),
      c = new AbortController();
    const first = observe(
      f.pool.query("SELECT __pool_hold(id) AS id FROM records WHERE id=1", [], {
        signal: c.signal,
      }),
    );
    const next = observe(f.pool.query("SELECT id FROM records WHERE id=2"));
    await until(() => Atomics.load(f.gate, 1) === 1);
    c.abort("stop");
    const close = observe(f.pool.close());
    await new Promise((resolve) => setImmediate(resolve));
    assert.equal(first.settled, false);
    assert.equal(next.settled, false);
    assert.equal(close.settled, false);
    assert.equal(f.pool.stats.activeQueries, 1);
    f.release();
    assert.equal((await first.promise).error.code, "ERR_FSQLITE_POOL_CANCELLED");
    assert.equal((await next.promise).value.rows[0].id, 2n);
    await close.promise;
    assert.equal(f.pool.stats.pendingQueries, 0);
  },
);

test(
  "actual worker termination fails waiters without replay while another active replica drains",
  options,
  async (t) => {
    const f = await setup(t, { workers: 2 });
    const a = observe(f.pool.query("SELECT __pool_hold(id) AS id FROM records WHERE id=1"));
    const b = observe(f.pool.query("SELECT __pool_hold(id) AS id FROM records WHERE id=2"));
    const waiting = observe(f.pool.query("SELECT id FROM records WHERE id=3"));
    await until(() => Atomics.load(f.gate, 1) === 2);
    // The bridge converts Node's unexpected exit to the browser-style error event.
    await f.threads[0].native.terminate();
    assert.match((await a.promise).error.message, /exited/);
    assert.equal((await waiting.promise).error.code, "ERR_FSQLITE_POOL_UNUSABLE");
    await assert.rejects(f.pool.query("SELECT 4"), { code: "ERR_FSQLITE_POOL_UNUSABLE" });
    assert.equal(b.settled, false);
    f.release();
    assert.equal((await b.promise).value.rows[0].id, 2n);
    await f.pool.close().catch(() => {});
    assert.equal(f.pool.stats.pendingQueries, 0);
    for (const thread of f.threads) {
      const db = new DatabaseSync(thread.path, { readOnly: true });
      try {
        assert.deepEqual(db.prepare("PRAGMA integrity_check").all().map(Object.values), [["ok"]]);
        assert.equal(db.prepare("SELECT count(*) AS n FROM records").get().n, 10);
      } finally {
        db.close();
      }
    }
  },
);

test(
  "actual workers switch complete generations behind the refresh barrier",
  options,
  async (t) => {
    const f = await setup(t, { workers: 2, rows: 10 });
    const before = f.pool.snapshot;
    const image = await databaseImage(25);
    const old = [
      f.pool.query("SELECT __pool_hold(id) AS id FROM records WHERE id=1"),
      f.pool.query("SELECT __pool_hold(id) AS id FROM records WHERE id=2"),
    ];
    await until(() => Atomics.load(f.gate, 1) === 2);
    const refresh = f.pool.refresh(image),
      reads = Array.from({ length: 8 }, () => f.pool.query("SELECT count(*) AS n FROM records"));
    assert.equal(f.threads.length, 2);
    f.release();
    const changed = await refresh;
    assert.equal(changed.snapshot.generation, 2);
    assert.equal(changed.cleanupErrors.length, 0);
    assert.ok((await Promise.all(old)).every((result) => result.snapshot.sha256 === before.sha256));
    assert.ok(
      (await Promise.all(reads)).every(
        (result) => result.rows[0].n === 25n && result.snapshot.sha256 === changed.snapshot.sha256,
      ),
    );
    assert.equal(f.threads.length, 4);
    assert.equal(new Set(f.threads.map((thread) => thread.path)).size, 4);
  },
);

test(
  "real production worker path rejects WITH writes and cannot disable read-only through EXPLAIN",
  options,
  async (t) => {
    const f = await setup(t, { workers: 1 });
    await assert.rejects(f.pool.query("EXPLAIN PRAGMA query_only=OFF"), {
      code: "ERR_FSQLITE_POOL_READ_ONLY",
    });
    await assert.rejects(
      f.pool.query("WITH x AS (SELECT 1) DELETE FROM records RETURNING id"),
      /readonly/i,
    );
    assert.equal((await f.pool.query("SELECT count(*) AS n FROM records")).rows[0].n, 10n);
  },
);
