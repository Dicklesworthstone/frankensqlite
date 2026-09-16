// Production SDK and worker host, actual Node SQLite, structured-clone transport.
// This is an SQL reference integration, NOT FrankenSQLite WASM or a browser.
import { test } from "node:test";
import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { FrankenDB, FrankenStreamError } from "../src/index.ts";
import { executeRowStream, streamOptions } from "../src/stream.ts";
import { sqliteSnapshotWorker } from "../../worker/tests/helpers/snapshot-sqlite-core.mjs";

const INSERT = "INSERT INTO t VALUES (?, ?)";
function deferred() {
  let resolve;
  const promise = new Promise(yes => { resolve = yes; });
  return { promise, resolve };
}
async function fixture(hooks) {
  const f = sqliteSnapshotWorker(hooks);
  const db = await FrankenDB.open({ worker: f.worker });
  await db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, value TEXT UNIQUE)");
  return { ...f, db, read: async () => (await db.query("SELECT * FROM t ORDER BY id")).rowArrays };
}
function chunks(f) { return f.worker.requests.filter(r => r.kind === "statement-execute-many"); }

test("25,001 async rows use bounded chunks, one prepared handle and one outer commit", async () => {
  const f = await fixture();
  let pulled = 0;
  async function* rows() {
    for (let i = 1; i <= 25001; i++) { pulled++; yield [i, `v${i}`]; }
  }
  const result = await f.db.executeStream(INSERT, rows(), { batchSize: 257 });
  assert.deepEqual(result, { executions: 25001, changes: 25001, batches: 98 });
  assert.equal(pulled, 25001);
  assert.equal(f.worker.requests.filter(r => r.kind === "prepare").length, 1);
  assert.equal(f.worker.requests.filter(r => r.kind === "statement-finalize").length, 1);
  assert.equal(f.events.filter(sql => sql === "BEGIN").length, 1);
  assert.equal(f.events.filter(sql => sql === "COMMIT").length, 1);
  assert.equal(chunks(f).length, 98);
  assert.ok(chunks(f).every(r => r.parameterSets.length <= 257));
  assert.deepEqual((await f.db.query("SELECT count(*), sum(id) FROM t")).rowArrays, [[25001, 312537501]]);
  assert.deepEqual((await f.db.query("PRAGMA integrity_check")).rowArrays, [["ok"]]);
  await f.db.close();
});

test("a later chunk constraint failure rolls back earlier chunks and trigger effects", async () => {
  const f = await fixture();
  await f.db.execute("CREATE TABLE audit(id INTEGER)");
  await f.db.execute("CREATE TRIGGER inserted AFTER INSERT ON t BEGIN INSERT INTO audit VALUES(new.id); END");
  await f.db.execute(INSERT, [90, "keep"]);
  const rows = Array.from({ length: 8 }, (_, i) => [i + 1, i === 6 ? "v1" : `v${i}`]);
  await assert.rejects(f.db.executeStream(INSERT, rows, { batchSize: 3 }), e => {
    assert.ok(e instanceof FrankenStreamError);
    assert.equal(e.phase, "execute"); assert.equal(e.rowIndex, 6);
    assert.equal(e.cause.batchIndex, 0);
    assert.match(e.cause.cause.message, /UNIQUE/);
    return true;
  });
  assert.deepEqual(await f.read(), [[90, "keep"]]);
  assert.deepEqual((await f.db.query("SELECT * FROM audit")).rowArrays, [[90]]);
  assert.equal(f.events.filter(sql => sql === "COMMIT").length, 0);
  await f.db.close();
});

test("a synchronous generator failure rolls back all chunks and preserves its cause", async () => {
  const f = await fixture();
  const cause = new Error("input file checksum failed");
  let returned = 0;
  function* rows() {
    try { yield [1, "a"]; yield [2, "b"]; yield [3, "c"]; throw cause; }
    finally { returned++; }
  }
  await assert.rejects(f.db.executeStream(INSERT, rows(), { batchSize: 1 }), e => {
    assert.equal(e.phase, "source"); assert.equal(e.rowIndex, 3); assert.equal(e.cause, cause); return true;
  });
  assert.equal(returned, 1); assert.deepEqual(await f.read(), []); await f.db.close();
});

test("an async producer failure closes its iterator once before releasing ownership", async () => {
  const f = await fixture();
  let pulls = 0, returns = 0;
  const cause = new Error("network source failed");
  const closing = deferred(), release = deferred();
  const rows = { [Symbol.asyncIterator]() { return {
    async next() { if (++pulls === 3) throw cause; return { done: false, value: [pulls, `v${pulls}`] }; },
    async return() { returns++; closing.resolve(); await release.promise; return { done: true }; },
  }; } };
  const pending = f.db.executeStream(INSERT, rows, { batchSize: 1 });
  const rejection = assert.rejects(pending, e => e.cause === cause && e.rowIndex === 2);
  await closing.promise;
  await assert.rejects(f.db.close(), e => e.code === "ERR_FSQLITE_TRANSACTION_OWNERSHIP");
  release.resolve(); await rejection;
  assert.equal(returns, 1); assert.deepEqual(await f.read(), []); await f.db.close();
});

test("nested stream rollback preserves parent and previously successful siblings", async () => {
  const f = await fixture();
  await f.db.transaction(async parent => {
    await parent.execute(INSERT, [90, "parent"]);
    await parent.executeStream(INSERT, [[91, "sibling"]]);
    await assert.rejects(parent.executeStream(INSERT, [[1, "a"], [2, "b"], [3, "a"]], { batchSize: 1 }));
    await parent.execute(INSERT, [92, "after"]);
  });
  assert.deepEqual(await f.read(), [[90, "parent"], [91, "sibling"], [92, "after"]]);
  await f.db.close();
});

test("a parent rollback also undoes a successful stream's released chunks", async () => {
  const f = await fixture();
  await assert.rejects(f.db.transaction(async parent => {
    await parent.executeStream(INSERT, [[1, "a"], [2, "b"]], { batchSize: 1 });
    throw new Error("parent abort");
  }), /parent abort/);
  assert.deepEqual(await f.read(), []); await f.db.close();
});

test("source pulls stop while a worker chunk is suspended; foreign work is refused", async () => {
  const blocked = deferred(), release = deferred();
  const f = await fixture({ async beforeBatch(sql) {
    if (sql.startsWith("SAVEPOINT fsqlite_bulk_")) { blocked.resolve(); await release.promise; }
  } });
  let pulls = 0;
  function* rows() { for (let i = 1; i <= 5; i++) { pulls++; yield [i, `v${i}`]; } }
  const pending = f.db.executeStream(INSERT, rows(), { batchSize: 2 });
  await blocked.promise;
  assert.equal(pulls, 2);
  await assert.rejects(f.db.execute(INSERT, [90, "foreign"]), e => e.code === "ERR_FSQLITE_TRANSACTION_OWNERSHIP");
  assert.equal(pulls, 2); release.resolve();
  assert.deepEqual(await pending, { executions: 5, changes: 5, batches: 3 });
  await f.db.close();
});

test("byte budget flushes before row limit and copies reused row and blob buffers", async () => {
  const f = await fixture();
  await f.db.execute("CREATE TABLE blobs(id INTEGER PRIMARY KEY, data BLOB)");
  const buffer = new Uint8Array(12), row = [0, buffer];
  function* rows() {
    for (let i = 1; i <= 7; i++) { row[0] = i; buffer.fill(i); yield row; }
    buffer.fill(99);
  }
  const result = await f.db.executeStream("INSERT INTO blobs VALUES (?, ?)", rows(), { batchSize: 100, maxBatchBytes: 121 });
  assert.equal(result.batches, 4); // 16 + 16*2 + 12 = 60 accounted bytes per row.
  assert.deepEqual(chunks(f).map(r => r.parameterSets.length), [2, 2, 2, 1]);
  const read = (await f.db.query("SELECT id, hex(data) FROM blobs ORDER BY id")).rowArrays;
  assert.deepEqual(read, Array.from({ length: 7 }, (_, i) => [i + 1, (i + 1).toString(16).padStart(2, "0").repeat(12)]));
  await f.db.close();
});

test("oversized or invalid later rows fail with their global index and undo earlier work", async () => {
  for (const value of ["x".repeat(100), undefined, { invalid: true }, [1]]) {
    const f = await fixture(); let returned = 0;
    function* rows() { try { yield [1, "a"]; yield [2, "b"]; yield [3, value]; } finally { returned++; } }
    await assert.rejects(f.db.executeStream(INSERT, rows(), { batchSize: 1, maxBatchBytes: 80 }), e => {
      assert.equal(e.phase, "input"); assert.equal(e.rowIndex, 2); return true;
    });
    assert.equal(returned, 1); assert.deepEqual(await f.read(), []); await f.db.close();
  }
});

test("invalid options and transaction-control scripts reject before source/BEGIN", async () => {
  const f = await fixture(); let accessed = false;
  const rows = { [Symbol.iterator]() { accessed = true; throw new Error("must not iterate"); } };
  for (const options of [{ batchSize: 0 }, { batchSize: 10001 }, { batchSize: NaN },
    { batchSize: 1.5 }, { maxBatchBytes: 0 }, { maxBatchBytes: Infinity }, { maxBatchBytes: 67108865 }]) {
    await assert.rejects(f.db.executeStream(INSERT, rows, options), e => e.phase === "input");
  }
  for (const sql of ["COMMIT", "INSERT INTO t VALUES(1,'a'); COMMIT", "SELECT * FROM t"]) {
    await assert.rejects(f.db.executeStream(sql, rows), e => e.phase === "input");
  }
  assert.equal(accessed, false); assert.equal(f.events.includes("BEGIN"), false); await f.db.close();
});

test("WITH and RETURNING statements that produce rows reject without consuming a source", async () => {
  const f = await fixture(); let accessed = false;
  const rows = { [Symbol.iterator]() { accessed = true; return [][Symbol.iterator](); } };
  for (const sql of ["WITH x AS (SELECT 1) SELECT * FROM x", "INSERT INTO t VALUES(1,'a') RETURNING id"]) {
    await assert.rejects(f.db.executeStream(sql, rows), e => e.phase === "prepare");
  }
  assert.equal(accessed, false); assert.deepEqual(await f.read(), []); await f.db.close();
});

test("empty sources finalize once, bind empty rows explicitly, and count ignored rows correctly", async () => {
  const f = await fixture();
  assert.deepEqual(await f.db.executeStream(INSERT, []), { executions: 0, changes: 0, batches: 0 });
  assert.equal(chunks(f).length, 0);
  assert.deepEqual(await f.db.executeStream("INSERT OR IGNORE INTO t VALUES(1,'a')", [[], [], []], { batchSize: 2 }),
    { executions: 3, changes: 1, batches: 2 });
  assert.equal(f.worker.requests.filter(r => r.kind === "statement-finalize").length, 2);
  assert.deepEqual(await f.read(), [[1, "a"]]); await f.db.close();
});

test("a failed BEGIN does not consume rows or roll back an existing manual transaction", async () => {
  const f = await fixture(); let accessed = false;
  await f.db.executeBatch("BEGIN; INSERT INTO t VALUES(90,'manual');");
  const rows = { [Symbol.iterator]() { accessed = true; return [][Symbol.iterator](); } };
  await assert.rejects(f.db.executeStream(INSERT, rows));
  assert.equal(accessed, false); await f.db.executeBatch("COMMIT");
  assert.deepEqual(await f.read(), [[90, "manual"]]); await f.db.close();
});

test("a parent returning with an unawaited stream drains it and rolls back instead of committing", async () => {
  const f = await fixture(); const gate = deferred(); let child;
  const parent = f.db.transaction(tx => {
    child = tx.executeStream(INSERT, (async function*() { await gate.promise; yield [1, "a"]; })());
  });
  const rejection = assert.rejects(parent, e => e.code === "ERR_FSQLITE_TRANSACTION_UNAWAITED");
  await new Promise(resolve => setImmediate(resolve));
  gate.resolve(); await rejection; await child;
  assert.deepEqual(await f.read(), []); await f.db.close();
});

test("iterator and statement cleanup failures do not mask the primary SQL error", async () => {
  const f = await fixture();
  const prepare = f.handles[0].prepare.bind(f.handles[0]);
  f.handles[0].prepare = async sql => {
    const stmt = await prepare(sql); const free = stmt.free.bind(stmt);
    stmt.free = () => { free(); throw new Error("finalize failed"); }; return stmt;
  };
  const iterator = { [Symbol.iterator]() { return this; }, next() { return { value: [1, "a"], done: false }; },
    return() { throw new Error("source close failed"); } };
  await assert.rejects(f.db.executeStream(INSERT, iterator, { batchSize: 1 }), e => {
    assert.equal(e.phase, "execute"); assert.equal(e.rowIndex, 1);
    assert.match(e.cause.cause.message, /UNIQUE/);
    assert.deepEqual(e.cleanupErrors.map(x => x.message), ["source close failed", "finalize failed"]); return true;
  });
  assert.deepEqual(await f.read(), []); await f.db.close();
});

test("malformed iterator results close once, finalize and roll back", async () => {
  const f = await fixture(); let returns = 0;
  const rows = { [Symbol.iterator]() { return { next() { return undefined; }, return() { returns++; return {}; } }; } };
  await assert.rejects(f.db.executeStream(INSERT, rows), e => e.phase === "source" && e.rowIndex === 0);
  assert.equal(returns, 1); assert.deepEqual(await f.read(), []); await f.db.close();
});

test("safe-integer count overflow is rejected rather than returning a rounded total", async () => {
  let frees = 0;
  const client = { async prepare() { return { statementId: "1", columnCount: 0 }; },
    async executePreparedMany() { return { executions: 1, changes: Number.MAX_SAFE_INTEGER }; },
    async finalizePrepared() { frees++; } };
  await assert.rejects(executeRowStream(client, INSERT, [[1, "a"], [2, "b"]], streamOptions(INSERT, { batchSize: 1 })),
    e => e.phase === "execute" && e.cause instanceof RangeError);
  assert.equal(frees, 1);
});

test("independent connections make progress while an import waits for source data", async () => {
  const a = await fixture(), b = await fixture(); const started = deferred(), release = deferred();
  const pending = a.db.executeStream(INSERT, (async function*() { started.resolve(); await release.promise; yield [1, "a"]; })());
  await started.promise;
  await b.db.executeStream(INSERT, [[2, "b"]]); assert.deepEqual(await b.read(), [[2, "b"]]);
  release.resolve(); await pending; assert.deepEqual(await a.read(), [[1, "a"]]);
  await a.db.close(); await b.db.close();
});

test("a deferred foreign-key failure at outer COMMIT undoes every released chunk", async () => {
  const f = await fixture();
  await f.db.executeBatch("PRAGMA foreign_keys=ON; CREATE TABLE parent(id PRIMARY KEY); CREATE TABLE child(id REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)");
  await assert.rejects(f.db.executeStream("INSERT INTO child VALUES (?)", [[1], [2], [3]], { batchSize: 1 }), /FOREIGN KEY/);
  assert.deepEqual((await f.db.query("SELECT * FROM child")).rowArrays, []);
  assert.ok(f.events.includes("ROLLBACK")); await f.db.close();
});

test("committed streamed data reopens with exact values and clean SQLite integrity", async () => {
  const f = await fixture(); const path = f.handles[0].path;
  await f.db.executeStream(INSERT, Array.from({ length: 513 }, (_, i) => [i, `row ${i}`]));
  await f.db.close();
  const reopened = new DatabaseSync(path);
  try {
    assert.deepEqual(reopened.prepare("SELECT * FROM t ORDER BY id").all().map(r => [r.id, r.value]),
      Array.from({ length: 513 }, (_, i) => [i, `row ${i}`]));
    assert.equal(Object.values(reopened.prepare("PRAGMA integrity_check").get())[0], "ok");
  } finally { reopened.close(); }
});

test("streamed integers preserve signed 64-bit limits and reject wider bigint payloads", async () => {
  const f = await fixture();
  await f.db.executeStream(INSERT, [[-(1n << 63n), "minimum"], [(1n << 63n) - 1n, "maximum"]], { batchSize: 1 });
  assert.deepEqual((await f.db.query("SELECT CAST(id AS TEXT), value FROM t ORDER BY id")).rowArrays,
    [["-9223372036854775808", "minimum"], ["9223372036854775807", "maximum"]]);
  await assert.rejects(f.db.executeStream(INSERT, [[1, "discard"], [1n << 1000n, "too wide"]], { batchSize: 1 }),
    e => e.phase === "input" && e.rowIndex === 1 && e.cause instanceof RangeError);
  assert.deepEqual((await f.db.query("SELECT count(*) FROM t WHERE id=1")).rowArrays, [[0]]);
  await f.db.close();
});
const isStreamCancelled = e => e instanceof FrankenStreamError && e.code === "ERR_FSQLITE_STREAM_CANCELLED";

test("pre-aborted imports reject before BEGIN or acquiring an iterator", async () => {
  const f = await fixture(); const controller = new AbortController(); const reason = new Error("cancel requested");
  controller.abort(reason); let acquired = false; const before = f.worker.requests.length;
  const rows = { [Symbol.iterator]() { acquired = true; return [][Symbol.iterator](); } };
  await assert.rejects(f.db.executeStream(INSERT, rows, { signal: controller.signal }), e => {
    assert.ok(isStreamCancelled(e)); assert.equal(e.cause, reason); return true;
  });
  assert.equal(acquired, false); assert.equal(f.worker.requests.length, before); await f.db.close();
});

test("progress is immutable, cumulative and explicitly uncommitted", async () => {
  const f = await fixture(); const progress = [];
  const result = await f.db.executeStream(INSERT, [[1, "a"], [2, "b"], [3, "c"], [4, "d"], [5, "e"]], {
    batchSize: 2,
    onProgress(p) {
      assert.ok(Object.isFrozen(p)); assert.throws(() => { p.changes = 999; }, TypeError);
      progress.push(p); assert.equal(f.events.includes("COMMIT"), false);
    },
  });
  assert.deepEqual(progress, [
    { executions: 2, changes: 2, batches: 1, committed: false },
    { executions: 4, changes: 4, batches: 2, committed: false },
    { executions: 5, changes: 5, batches: 3, committed: false },
  ]);
  assert.deepEqual(result, { executions: 5, changes: 5, batches: 3 }); await f.db.close();
});

test("async progress applies backpressure and no additional input is prefetched", async () => {
  const f = await fixture(); const started = deferred(), release = deferred(); let pulls = 0;
  function* rows() { for (let i = 1; i <= 5; i++) { pulls++; yield [i, `v${i}`]; } }
  const pending = f.db.executeStream(INSERT, rows(), {
    batchSize: 2, async onProgress(p) { if (p.batches === 1) { started.resolve(); await release.promise; } },
  });
  await started.promise; assert.equal(pulls, 2); assert.equal(chunks(f).length, 1);
  release.resolve(); await pending; assert.equal(pulls, 5); await f.db.close();
});

test("a progress rejection closes the source and rolls back already reported chunks", async () => {
  const f = await fixture(); const cause = new Error("validation rejected imported rows"); let returned = 0;
  function* rows() { try { for (let i = 1; i <= 10; i++) yield [i, `v${i}`]; } finally { returned++; } }
  await assert.rejects(f.db.executeStream(INSERT, rows(), { batchSize: 2, async onProgress(p) {
    if (p.batches === 2) throw cause;
  } }), e => e.phase === "progress" && e.cause === cause);
  assert.equal(returned, 1); assert.equal(chunks(f).length, 2); assert.deepEqual(await f.read(), []); await f.db.close();
});

test("cancellation between chunks undoes every previous chunk and trigger effect", async () => {
  const f = await fixture(); const controller = new AbortController(); let returned = 0;
  await f.db.executeBatch("CREATE TABLE audit(id); CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO audit VALUES(new.id); END;");
  function* rows() { try { for (let i = 1; i <= 10; i++) yield [i, `v${i}`]; } finally { returned++; } }
  await assert.rejects(f.db.executeStream(INSERT, rows(), { batchSize: 2, signal: controller.signal,
    onProgress(p) { if (p.batches === 2) controller.abort("stop importing"); },
  }), e => isStreamCancelled(e) && e.cause === "stop importing");
  assert.equal(returned, 1); assert.equal(chunks(f).length, 2); assert.deepEqual(await f.read(), []);
  assert.deepEqual((await f.db.query("SELECT * FROM audit")).rowArrays, []); await f.db.close();
});

test("in-flight cancellation waits for the current SQL then undoes earlier successful chunks", async () => {
  const f = await fixture(); const blocked = deferred(), release = deferred(); const controller = new AbortController();
  const prepare = f.handles[0].prepare.bind(f.handles[0]);
  f.handles[0].prepare = async sql => {
    const stmt = await prepare(sql), execute = stmt.executeWithParams.bind(stmt);
    stmt.executeWithParams = async params => { if (params[0] === 5) { blocked.resolve(); await release.promise; } return execute(params); };
    return stmt;
  };
  let settled = false;
  const pending = f.db.executeStream(INSERT, Array.from({ length: 10 }, (_, i) => [i + 1, `v${i}`]),
    { batchSize: 3, signal: controller.signal });
  void pending.then(() => { settled = true; }, () => { settled = true; });
  const rejection = assert.rejects(pending, e => isStreamCancelled(e) && e.cause.code === "ERR_FSQLITE_BULK_CANCELLED");
  await blocked.promise; controller.abort();
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(settled, false); assert.equal(f.worker.requests.filter(r => r.kind === "cancel-bulk").length, 1);
  release.resolve(); await rejection; assert.equal(chunks(f).length, 2);
  assert.deepEqual(await f.read(), []); await f.db.close();
});

test("a real SQL failure takes precedence over a coincident abort", async () => {
  const f = await fixture(); const controller = new AbortController();
  const prepare = f.handles[0].prepare.bind(f.handles[0]);
  f.handles[0].prepare = async sql => {
    const stmt = await prepare(sql), execute = stmt.executeWithParams.bind(stmt);
    stmt.executeWithParams = async params => {
      try { return await execute(params); } finally { if (params[0] === 5) controller.abort(); }
    }; return stmt;
  };
  const rows = [[1, "a"], [2, "b"], [3, "c"], [4, "d"], [5, "a"]];
  await assert.rejects(f.db.executeStream(INSERT, rows, { batchSize: 3, signal: controller.signal }), e => {
    assert.equal(e.phase, "execute"); assert.equal(e.rowIndex, 4); assert.match(e.cause.cause.message, /UNIQUE/); return true;
  });
  assert.deepEqual(await f.read(), []); await f.db.close();
});

test("source cancellation is cooperative: no next() races and no post-abort rows are written", async () => {
  const f = await fixture(); const blocked = deferred(), release = deferred(); const controller = new AbortController();
  let pulls = 0, returned = 0, settled = false;
  const rows = { [Symbol.asyncIterator]() { return {
    async next() { pulls++; if (pulls === 2) { blocked.resolve(); await release.promise; } return { done: false, value: [pulls, `v${pulls}`] }; },
    async return() { returned++; return { done: true }; },
  }; } };
  const pending = f.db.executeStream(INSERT, rows, { batchSize: 1, signal: controller.signal });
  void pending.then(() => { settled = true; }, () => { settled = true; });
  const rejection = assert.rejects(pending, isStreamCancelled);
  await blocked.promise; controller.abort(); await new Promise(resolve => setImmediate(resolve));
  assert.equal(settled, false); assert.equal(pulls, 2); assert.equal(returned, 0);
  release.resolve(); await rejection; assert.equal(returned, 1); assert.equal(chunks(f).length, 1);
  assert.deepEqual(await f.read(), []); await f.db.close();
});

test("abort after COMMIT dispatch cannot turn the real committed result into cancellation", async () => {
  const controller = new AbortController();
  const f = await fixture({ beforeBatch(sql) { if (sql === "COMMIT") controller.abort("too late"); } });
  const result = await f.db.executeStream(INSERT, [[1, "a"], [2, "b"]], { batchSize: 1, signal: controller.signal });
  assert.equal(controller.signal.aborted, true); assert.equal(result.changes, 2);
  assert.deepEqual(await f.read(), [[1, "a"], [2, "b"]]);
  const path = f.handles[0].path; await f.db.close(); const reopened = new DatabaseSync(path);
  try { assert.equal(reopened.prepare("SELECT count(*) AS n FROM t").get().n, 2); } finally { reopened.close(); }
});

test("abort during finalization is still observed before COMMIT dispatch", async () => {
  const f = await fixture(); const controller = new AbortController();
  const prepare = f.handles[0].prepare.bind(f.handles[0]);
  f.handles[0].prepare = async sql => {
    const stmt = await prepare(sql), free = stmt.free.bind(stmt);
    stmt.free = () => { free(); controller.abort(); }; return stmt;
  };
  await assert.rejects(f.db.executeStream(INSERT, [[1, "a"], [2, "b"]], { signal: controller.signal }), isStreamCancelled);
  assert.equal(f.events.includes("COMMIT"), false); assert.deepEqual(await f.read(), []); await f.db.close();
});

test("a cancelled child import can be caught without discarding parent and sibling work", async () => {
  const f = await fixture(); const controller = new AbortController();
  await f.db.transaction(async parent => {
    await parent.execute(INSERT, [90, "parent"]);
    await parent.executeStream(INSERT, [[91, "sibling"]]);
    await assert.rejects(parent.executeStream(INSERT, [[1, "a"], [2, "b"]], {
      batchSize: 1, signal: controller.signal, onProgress() { controller.abort(); },
    }), isStreamCancelled);
    await parent.execute(INSERT, [92, "after"]);
  });
  assert.deepEqual(await f.read(), [[90, "parent"], [91, "sibling"], [92, "after"]]); await f.db.close();
});

test("cancellation preserves iterator cleanup failures without losing the abort cause", async () => {
  const f = await fixture(); const controller = new AbortController(); const reason = new Error("stop");
  const cleanup = new Error("source disposal failed");
  const source = { [Symbol.iterator]() { return this; }, next() { return { done: false, value: [1, "a"] }; },
    return() { throw cleanup; } };
  await assert.rejects(f.db.executeStream(INSERT, source, { batchSize: 1, signal: controller.signal,
    onProgress() { controller.abort(reason); },
  }), e => {
    assert.ok(isStreamCancelled(e)); assert.equal(e.cause, reason); assert.deepEqual(e.cleanupErrors, [cleanup]); return true;
  });
  assert.deepEqual(await f.read(), []); await f.db.close();
});

test("a failed cancellation rollback makes the connection terminal, not silently reusable", async () => {
  const controller = new AbortController();
  const f = await fixture({ beforeBatch(sql) { if (sql === "ROLLBACK") throw new Error("rollback unavailable"); } });
  await assert.rejects(f.db.executeStream(INSERT, [[1, "a"]], { signal: controller.signal, onProgress() { controller.abort(); } }), e => {
    assert.ok(e instanceof AggregateError); assert.ok(isStreamCancelled(e.cause));
    assert.match(e.errors[1].message, /rollback unavailable/); return true;
  });
  assert.equal(f.worker.terminateCount, 1);
  await assert.rejects(f.db.execute(INSERT, [2, "must not write"]), AggregateError);
  f.handles[0].close(); // The loopback transport does not own a real worker process.
});

test("bulk abort listeners are removed after each chunk and after an aborted import", async () => {
  for (const abort of [false, true]) {
    const f = await fixture(); const controller = new AbortController(); const active = new Set();
    const add = controller.signal.addEventListener.bind(controller.signal);
    const remove = controller.signal.removeEventListener.bind(controller.signal);
    controller.signal.addEventListener = (type, listener, options) => { if (type === "abort") active.add(listener); return add(type, listener, options); };
    controller.signal.removeEventListener = (type, listener, options) => { if (type === "abort") active.delete(listener); return remove(type, listener, options); };
    const pending = f.db.executeStream(INSERT, [[1, "a"], [2, "b"], [3, "c"]], { batchSize: 1, signal: controller.signal,
      onProgress(p) { assert.equal(active.size, 0); if (abort && p.batches === 2) controller.abort(); },
    });
    if (abort) await assert.rejects(pending, isStreamCancelled); else await pending;
    assert.equal(active.size, 0); await f.db.close();
  }
});

test("invalid cancellation/progress options reject before entering a transaction", async () => {
  const f = await fixture(); const before = f.worker.requests.length;
  for (const options of [{ signal: null }, { signal: {} }, { onProgress: "not a callback" }]) {
    await assert.rejects(f.db.executeStream(INSERT, [], options), e => e.phase === "input");
  }
  assert.equal(f.worker.requests.length, before); await f.db.close();
});

test("options and callback identity are captured once for the entire import", async () => {
  const f = await fixture(); let notifications = 0;
  const options = { batchSize: 2, onProgress() { notifications++; options.batchSize = 1000; options.onProgress = () => { throw new Error("changed callback"); }; } };
  const result = await f.db.executeStream(INSERT, [[1, "a"], [2, "b"], [3, "c"], [4, "d"], [5, "e"]], options);
  assert.equal(result.batches, 3); assert.equal(notifications, 3); await f.db.close();
});

test("progress does not claim success when a deferred constraint later rejects commit", async () => {
  const f = await fixture(); const progress = [];
  await f.db.executeBatch("PRAGMA foreign_keys=ON; CREATE TABLE p(id PRIMARY KEY); CREATE TABLE c(id REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)");
  await assert.rejects(f.db.executeStream("INSERT INTO c VALUES (?)", [[1], [2]], { batchSize: 1, onProgress(p) { progress.push(p); } }), /FOREIGN KEY/);
  assert.deepEqual(progress, [{ executions: 1, changes: 1, batches: 1, committed: false }, { executions: 2, changes: 2, batches: 2, committed: false }]);
  assert.deepEqual((await f.db.query("SELECT * FROM c")).rowArrays, []); await f.db.close();
});
