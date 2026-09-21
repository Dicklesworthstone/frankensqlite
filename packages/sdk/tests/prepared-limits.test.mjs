// Exercises production SDK/worker client/host over real Node worker messages.
// The SQL engine and browser-Worker adapter are test-only; no WASM claim.
import assert from "node:assert/strict";
import test from "node:test";
import { Worker } from "node:worker_threads";
import { RequestBudget } from "../../worker/src/admission.ts";
import { sqliteSnapshotWorker } from "../../worker/tests/helpers/snapshot-sqlite-core.mjs";
import { FrankenDB } from "../src/database.ts";
import { FrankenSQLiteError } from "../src/errors.ts";
import { FrankenDBQueue } from "../src/queue.ts";

function threadTransport(t, workerData = {}) {
  const thread = new Worker(
    new URL("../../worker/tests/helpers/statement-thread.mjs", import.meta.url),
    { workerData },
  );
  const listeners = { message: new Set(), error: new Set(), messageerror: new Set() };
  const controls = new Map();
  let nextControl = 0,
    termination;
  const worker = {
    sent: [],
    terminateCount: 0,
    addEventListener(type, listener) {
      listeners[type].add(listener);
    },
    removeEventListener(type, listener) {
      listeners[type].delete(listener);
    },
    postMessage(message, transfer) {
      worker.sent.push(message.kind);
      thread.postMessage(message, transfer);
    },
    terminate() {
      worker.terminateCount++;
      termination ??= thread.terminate();
    },
    stats() {
      return new Promise((resolve, reject) => {
        const id = ++nextControl;
        controls.set(id, { resolve, reject });
        thread.postMessage({ testControl: "stats", id });
      });
    },
  };
  thread.on("message", (data) => {
    if (data.testControl === "stats") {
      controls.get(data.id)?.resolve(data);
      controls.delete(data.id);
      return;
    }
    for (const listener of listeners.message) listener({ data });
  });
  thread.on("error", (error) => {
    for (const control of controls.values()) control.reject(error);
    controls.clear();
    for (const listener of listeners.error) listener({ message: error.message });
  });
  thread.on("messageerror", () => {
    for (const listener of listeners.messageerror) listener();
  });
  t.after(async () => {
    try {
      await worker.closeForTest?.();
    } finally {
      termination ??= thread.terminate();
      await termination;
    }
  });
  return worker;
}
async function open(t, config = {}, workerData) {
  const worker = threadTransport(t, workerData);
  const db = await FrankenDB.open({ worker, ...config });
  worker.closeForTest = () => db.close();
  return { worker, db };
}
const saturated = (e) =>
  e instanceof FrankenSQLiteError && e.code === "ERR_FSQLITE_STATEMENT_LIMIT";
const policyFailure = (e) =>
  e instanceof FrankenSQLiteError && e.code === "ERR_FSQLITE_STATEMENT_POLICY";
const requested = { maxStatements: 2, maxBytes: 4096 };

for (const encoding of ["structured-clone", "binary"]) {
  test(`real thread enforces acknowledged policy and preserves existing ${encoding} prepared queries`, async (t) => {
    const { db, worker } = await open(t, {
      preparedStatementLimits: requested,
      resultEncoding: encoding,
    });
    assert.deepEqual(db.preparedStatementLimits, requested);
    assert.ok(Object.isFrozen(db.preparedStatementLimits));
    const a = await db.prepare("SELECT ? AS value"),
      b = await db.prepare("SELECT 2 AS n");
    await assert.rejects(db.prepare("SELECT 3"), saturated);
    assert.deepEqual((await a.query([new Uint8Array([1, 2, 3])])).rowArrays, [
      [new Uint8Array([1, 2, 3])],
    ]);
    assert.deepEqual((await b.query()).rowArrays, [[2]]);
    assert.equal((await worker.stats()).prepared.statements, 2);
    await a.finalize();
    const c = await db.prepare("SELECT 3 AS n");
    assert.deepEqual((await c.query()).rowArrays, [[3]]);
    await b.finalize();
    await c.finalize();
    const stats = await worker.stats();
    assert.equal(stats.prepared.statements, 0);
    assert.equal(stats.prepared.bytes, 0);
    assert.equal(stats.requests.pendingRequests, 0);
  });
}

test("concurrent prepares serialize under one real host budget without oversubscription", async (t) => {
  const { db, worker } = await open(t, { preparedStatementLimits: requested });
  const results = await Promise.allSettled(
    Array.from({ length: 8 }, (_, i) => db.prepare(`SELECT ${i}`)),
  );
  assert.equal(results.filter((r) => r.status === "fulfilled").length, 2);
  for (const r of results) if (r.status === "rejected") assert.ok(saturated(r.reason));
  assert.equal((await worker.stats()).prepared.statements, 2);
  for (const r of results) if (r.status === "fulfilled") await r.value.finalize();
  assert.equal((await worker.stats()).prepared.statements, 0);
});

test("real thread churn releases all capacity without retaining failed metadata or ids", async (t) => {
  const { db, worker } = await open(t, {
    preparedStatementLimits: { maxStatements: 1, maxBytes: 4096 },
  });
  for (let i = 0; i < 100; i++) {
    const statement = await db.prepare("SELECT ? AS value");
    assert.equal((await statement.query([i])).rows[0].value, i);
    await statement.finalize();
  }
  const stats = await worker.stats();
  assert.equal(stats.prepared.statements, 0);
  assert.equal(stats.prepared.bytes, 0);
  assert.equal(stats.prepared.rejectedStatements, 0);
});

test("retained byte limit crosses the actual serialization boundary as a nonretryable input error", async (t) => {
  const { db, worker } = await open(t, { preparedStatementLimits: { maxBytes: 512 } });
  await assert.rejects(
    db.prepare("SELECT ?100"),
    (e) => e.code === "ERR_FSQLITE_STATEMENT_TOO_LARGE" && e.transient === false,
  );
  assert.equal((await worker.stats()).prepared.statements, 0);
  const s = await db.prepare("SELECT 1");
  await s.finalize();
});

for (const rollback of [false, true]) {
  test(`actual SDK/host ${rollback ? "rollback" : "commit"} releases scoped handles`, async (t) => {
    const { db, worker } = await open(t, { preparedStatementLimits: { maxStatements: 1 } });
    let escaped;
    const pending = db.transaction(async (tx) => {
      escaped = await tx.prepare("SELECT 1");
      await escaped.query();
      if (rollback) throw new Error("application rejected");
    });
    if (rollback) await assert.rejects(pending, /application rejected/);
    else await pending;
    assert.equal((await worker.stats()).prepared.statements, 0);
    await assert.rejects(escaped.query(), (e) => e.code === "ERR_FSQLITE_TRANSACTION_CLOSED");
    const next = await db.prepare("SELECT 2");
    await next.finalize();
  });
}

test("real SDK recovers a capacity-refused child without losing parent handles or writes", async (t) => {
  const { db, worker } = await open(t, { preparedStatementLimits: { maxStatements: 1 } });
  await db.execute("CREATE TABLE items(value)");
  await db.transaction(async (tx) => {
    const statement = await tx.prepare("INSERT INTO items VALUES(?)");
    await statement.execute([1]);
    await assert.rejects(
      tx.transaction((child) => child.prepare("SELECT 1")),
      saturated,
    );
    await statement.execute([2]);
  });
  assert.deepEqual((await db.query("SELECT value FROM items ORDER BY value")).rowArrays, [
    [1],
    [2],
  ]);
  assert.equal((await worker.stats()).prepared.statements, 0);
});

test("resource pressure never authorizes automatic SQL replay", async (t) => {
  const { db, worker } = await open(t, { preparedStatementLimits: { maxStatements: 1 } });
  await db.execute("CREATE TABLE items(value)");
  let callbacks = 0;
  await assert.rejects(
    db.transactionWithRetry(
      async (tx) => {
        callbacks++;
        await tx.execute("INSERT INTO items VALUES(1)");
        await tx.prepare("SELECT 1");
        await tx.prepare("SELECT 2");
      },
      { maxAttempts: 3 },
    ),
    saturated,
  );
  assert.equal(callbacks, 1);
  assert.deepEqual((await db.query("SELECT * FROM items")).rowArrays, []);
  assert.equal((await worker.stats()).prepared.statements, 0);
});

test("queued streaming imports reuse a single admitted handle and release it before the next job", async (t) => {
  const worker = threadTransport(t);
  const queue = await FrankenDBQueue.open({
    worker,
    preparedStatementLimits: { maxStatements: 1, maxBytes: 4096 },
  });
  worker.closeForTest = () => queue.close();
  await queue.transaction((tx) => tx.execute("CREATE TABLE items(value)"));
  const imported = await queue.transaction((tx) =>
    tx.executeStream("INSERT INTO items VALUES(?)", [[1], [2], [3]], { batchSize: 1 }),
  );
  assert.equal(imported.executions, 3);
  const count = await queue.transaction(async (tx) => {
    const s = await tx.prepare("SELECT count(*) n FROM items");
    return (await s.query()).rows[0].n;
  });
  assert.equal(count, 3);
  assert.equal((await worker.stats()).prepared.statements, 0);
});

test("real worker finalization failure reaches the client fatal fence and cannot be retried", async (t) => {
  const { db, worker } = await open(t, {}, { freeFailure: true });
  const statement = await db.prepare("SELECT 1 /* fail_free */");
  await assert.rejects(
    statement.finalize(),
    (e) => e.code === "ERR_FSQLITE_TRANSACTION_CONNECTION_UNUSABLE",
  );
  const sent = worker.sent.length;
  await assert.rejects(
    db.execute("CREATE TABLE must_not_run(x)"),
    (e) => e.code === "ERR_FSQLITE_TRANSACTION_CONNECTION_UNUSABLE",
  );
  assert.equal(worker.sent.length, sent);
  await db.close();
  assert.equal(worker.terminateCount, 1);
});

test("host ceilings may tighten but never increase requested limits", async (t) => {
  const { db } = await open(t, {
    preparedStatementLimits: { maxStatements: 512, maxBytes: 32 * 1024 * 1024 },
  });
  assert.deepEqual(db.preparedStatementLimits, { maxStatements: 256, maxBytes: 16 * 1024 * 1024 });
});

test("new worker acknowledges its default limits without explicit options", async (t) => {
  const { db } = await open(t);
  assert.deepEqual(db.preparedStatementLimits, { maxStatements: 256, maxBytes: 16 * 1024 * 1024 });
});

test("old worker omission is compatible only when the application did not request a policy", async (t) => {
  const { db } = await open(t, {}, { omitPolicy: true });
  assert.equal(db.preparedStatementLimits, null);
  const worker = threadTransport(t, { omitPolicy: true });
  await assert.rejects(
    FrankenDB.open({ worker, preparedStatementLimits: requested }),
    policyFailure,
  );
  assert.equal(worker.terminateCount, 1);
  assert.deepEqual(worker.sent, ["init"]);
});

for (const responsePolicy of [
  null,
  {},
  { maxStatements: 2 },
  { maxStatements: 3, maxBytes: 4096 },
  { maxStatements: 2, maxBytes: 4097 },
  { maxStatements: 0, maxBytes: 4096 },
  { maxStatements: 2.5, maxBytes: 4096 },
  { maxStatements: "2", maxBytes: 4096 },
  { maxStatements: 2, maxBytes: 255 },
]) {
  test(`malformed or weakened real-thread policy ${JSON.stringify(responsePolicy)} cannot initialize the SDK`, async (t) => {
    const worker = threadTransport(t, { responsePolicy });
    await assert.rejects(
      FrankenDB.open({ worker, preparedStatementLimits: requested }),
      policyFailure,
    );
    assert.deepEqual(worker.sent, ["init"]);
    assert.equal(worker.terminateCount, 1);
  });
}

for (const policy of [
  { maxStatements: 0 },
  { maxBytes: Infinity },
  null,
  [],
  { maxStatements: 4097 },
]) {
  test(`invalid requested policy ${JSON.stringify(policy)} fails before allocating a worker or detaching an image`, async () => {
    let workers = 0;
    const snapshot = new Uint8Array(512);
    await assert.rejects(
      FrankenDB.open({
        snapshot,
        preparedStatementLimits: policy,
        worker: () => {
          workers++;
          throw new Error("must not create worker");
        },
      }),
      (e) => e.code === "ERR_FSQLITE_STATEMENT_INPUT",
    );
    assert.equal(workers, 0);
    assert.equal(snapshot.byteLength, 512);
  });
}

test("caller policy getters are captured once before IPC and later mutation cannot change the accepted policy", async (t) => {
  const worker = threadTransport(t);
  let reads = 0;
  const policy = {
    get maxStatements() {
      reads++;
      return 1;
    },
    maxBytes: 4096,
    unrelated: { huge: new Uint8Array(1000000) },
  };
  const pending = FrankenDB.open({ worker, preparedStatementLimits: policy });
  policy.maxBytes = 999999;
  const db = await pending;
  worker.closeForTest = () => db.close();
  assert.equal(reads, 1);
  assert.deepEqual(db.preparedStatementLimits, { maxStatements: 1, maxBytes: 4096 });
});

test("request capture carries only known numeric policy fields and charges the policy payload", () => {
  const budget = new RequestBudget({ maxPendingBytes: 256 });
  const lease = budget.admit({
    kind: "init",
    requestId: 1,
    config: { preparedStatementLimits: { ...requested, irrelevant: new Uint8Array(1000000) } },
  });
  assert.deepEqual(lease.request.config.preparedStatementLimits, requested);
  assert.equal(budget.stats.pendingBytes, 176);
  lease.release();
});

test("failed reinitialization keeps the old statement policy and handles; successful replacement can change the policy", async (t) => {
  let rejectImport = false;
  const f = sqliteSnapshotWorker({
    beforeImport() {
      if (rejectImport) throw new Error("import failed");
    },
  });
  let id = 0;
  const send = (kind, fields = {}) => f.host.handle({ kind, requestId: ++id, ...fields });
  t.after(async () => {
    await send("close");
  });
  assert.equal(
    (await send("init", { config: { preparedStatementLimits: { maxStatements: 1 } } })).kind,
    "ready",
  );
  const prepared = await send("prepare", { sql: "SELECT 1" });
  assert.equal(prepared.kind, "prepare-result");
  rejectImport = true;
  const bad = await send("init", {
    config: { snapshot: new Uint8Array([0]), preparedStatementLimits: { maxStatements: 3 } },
  });
  assert.equal(bad.kind, "error");
  assert.equal(f.host.preparedStatements.maxStatements, 1);
  assert.equal(f.host.preparedStatements.statements, 1);
  assert.equal(
    (await send("statement-query", { statementId: prepared.data.statementId })).kind,
    "query-result",
  );
  const good = await send("init", { config: { preparedStatementLimits: { maxStatements: 3 } } });
  assert.equal(good.kind, "ready");
  assert.equal(f.host.preparedStatements.maxStatements, 3);
  assert.equal(f.host.preparedStatements.statements, 0);
});

for (const location of ["envelope accessor", "field accessor", "inherited fields"]) {
  test(`custom transport ${location} is not accepted as a policy acknowledgement`, async () => {
    let getters = 0,
      terminations = 0;
    const listeners = new Set();
    const data = { path: ":memory:", persistence: "memory" };
    if (location === "envelope accessor")
      Object.defineProperty(data, "preparedStatementLimits", {
        get() {
          getters++;
          return requested;
        },
        enumerable: true,
      });
    if (location === "field accessor")
      data.preparedStatementLimits = {
        get maxStatements() {
          getters++;
          return 2;
        },
        maxBytes: 4096,
      };
    if (location === "inherited fields") data.preparedStatementLimits = Object.create(requested);
    const worker = {
      addEventListener(type, listener) {
        if (type === "message") listeners.add(listener);
      },
      removeEventListener(type, listener) {
        if (type === "message") listeners.delete(listener);
      },
      postMessage(request) {
        assert.equal(request.kind, "init");
        for (const listener of listeners)
          listener({ data: { kind: "ready", requestId: request.requestId, data } });
      },
      terminate() {
        terminations++;
      },
    };
    await assert.rejects(
      FrankenDB.open({ worker, preparedStatementLimits: requested }),
      policyFailure,
    );
    assert.equal(getters, 0);
    assert.equal(terminations, 1);
  });
}
