// Actual production SDK/worker classes. Controlled transport tests are not WASM
// or browser execution; SQL integrations below use the Node SQLite reference.

import assert from "node:assert/strict";
import { test } from "node:test";
import { RequestBudget, resolveRequestLimits } from "../../worker/src/admission.ts";
import { sqliteSnapshotWorker } from "../../worker/tests/helpers/snapshot-sqlite-core.mjs";
import { FrankenDB } from "../src/database.ts";
import { FrankenWorkerClient } from "../src/worker-client.ts";
import {
  ControlledWorker,
  deferred,
  drain,
  observe,
  rejected,
} from "./helpers/controlled-worker.ts";

const code = (expected) => (error) => error?.code === expected;
const full = code("ERR_FSQLITE_QUEUE_FULL");
const large = code("ERR_FSQLITE_REQUEST_TOO_LARGE");
function fixture(limits = {}) {
  const worker = new ControlledWorker();
  const client = new FrankenWorkerClient(worker, limits);
  return { worker, client };
}
function success(worker, request, changes = 1) {
  worker.reply({ kind: "execute-result", requestId: request.requestId, changes });
}
const execute = (requestId, sql = "SELECT 1", params = []) => ({
  kind: "execute",
  requestId,
  sql,
  params,
});

test("limits are validated, captured and exposed only as frozen snapshots", () => {
  const options = { maxPendingRequests: 2, maxPendingBytes: 1024 };
  const budget = new RequestBudget(options);
  options.maxPendingRequests = 999;
  assert.equal(budget.stats.maxPendingRequests, 2);
  assert.ok(Object.isFrozen(budget.stats));
  for (const maxPendingRequests of [0, -1, 1.5, NaN, Infinity, 4097]) {
    assert.throws(() => resolveRequestLimits({ maxPendingRequests }), RangeError);
  }
  for (const maxPendingBytes of [0, 255, 1.5, NaN, Infinity, 1073741825]) {
    assert.throws(() => resolveRequestLimits({ maxPendingBytes }), RangeError);
  }
});

test("invalid public options reject before creating a worker", async () => {
  let spawned = 0;
  await assert.rejects(
    FrankenDB.open({
      requestLimits: { maxPendingRequests: 0 },
      worker() {
        spawned++;
        return new ControlledWorker();
      },
    }),
    RangeError,
  );
  assert.equal(spawned, 0);
});

test("full count budget rejects before IPC and admits again after settlement", async () => {
  const { client, worker } = fixture({ maxPendingRequests: 2 });
  const first = client.execute("first"),
    second = client.execute("second");
  await assert.rejects(
    client.execute("never posted"),
    (error) => full(error) && error.transient === true,
  );
  assert.equal(worker.requests.length, 2);
  assert.equal(client.requestQueue.pendingRequests, 2);
  success(worker, worker.requests[0]);
  await first;
  const third = client.execute("third");
  assert.equal(worker.requests.length, 3);
  success(worker, worker.requests[1]);
  success(worker, worker.requests[2]);
  await Promise.all([second, third]);
  assert.equal(client.requestQueue.pendingRequests, 0);
  assert.equal(client.requestQueue.pendingBytes, 0);
  assert.equal(client.requestQueue.rejectedRequests, 1);
  client.dispose();
});

test("cumulative byte pressure and permanently oversized requests are distinct", async () => {
  const { client, worker } = fixture({ maxPendingBytes: 512 });
  const first = client.execute("x".repeat(120)); // 400 accounted bytes.
  assert.equal(client.requestQueue.pendingBytes, 400);
  await assert.rejects(client.execute("second"), full);
  await assert.rejects(
    client.execute("x".repeat(300)),
    (error) => large(error) && !error.transient,
  );
  assert.equal(worker.requests.length, 1);
  assert.equal(client.requestQueue.pendingBytes, 400);
  success(worker, worker.requests[0]);
  await first;
  const next = client.execute("next");
  success(worker, worker.requests[1]);
  await next;
  assert.equal(client.requestQueue.pendingBytes, 0);
  client.dispose();
});

test("tiny subarrays charge the entire backing store; aliases charge it once", () => {
  const small = new RequestBudget({ maxPendingBytes: 512 });
  const buffer = new ArrayBuffer(4096),
    view = new Uint8Array(buffer, 100, 1);
  assert.throws(() => small.admit(execute(1, "q", [view])), large);
  const budget = new RequestBudget({ maxPendingBytes: 8192 });
  const lease = budget.admit(execute(1, "q", [view, new Uint8Array(buffer, 200, 3)]));
  assert.equal(budget.stats.pendingBytes, 128 + 18 + 16 + 32 + 4096);
  assert.equal(lease.request.params[0].buffer, lease.request.params[1].buffer);
  const cloned = structuredClone(lease.request);
  assert.equal(cloned.params[0].buffer.byteLength, 4096);
  assert.equal(cloned.params[0].buffer, cloned.params[1].buffer);
  lease.release();
  lease.release();
  assert.equal(budget.stats.pendingBytes, 0);
});

test("resizable and shared growable buffers account their maximum capacity", () => {
  const budget = new RequestBudget({ maxPendingBytes: 512 });
  for (const buffer of [
    new ArrayBuffer(1, { maxByteLength: 4096 }),
    new SharedArrayBuffer(1, { maxByteLength: 4096 }),
  ]) {
    assert.throws(() => budget.admit(execute(1, "q", [new Uint8Array(buffer)])), large);
  }
});

test("snapshot rejection precedes transfer and leaves all sender views intact", async () => {
  const { client, worker } = fixture({ maxPendingBytes: 512 });
  let posts = 0;
  worker.postMessage = (request, transfer) => {
    posts++;
    structuredClone(request, { transfer });
  };
  const buffer = new ArrayBuffer(4096),
    snapshot = new Uint8Array(buffer, 0, 100);
  await assert.rejects(client.init({ snapshot }), large);
  assert.equal(posts, 0);
  assert.equal(snapshot.byteLength, 100);
  assert.equal(buffer.byteLength, 4096);
  assert.equal(client.requestQueue.pendingBytes, 0);
  client.dispose();
});

test("huge sparse arrays are rejected without traversing their entries", async () => {
  const { client, worker } = fixture({ maxPendingBytes: 512 });
  const params = [];
  params.length = 1_000_000_000;
  Object.defineProperty(params, "0", {
    get() {
      throw new Error("must not inspect");
    },
  });
  await assert.rejects(client.execute("q", params), large);
  assert.equal(worker.requests.length, 0);
  client.dispose();
});

test("capture removes extra properties, ignores array iterators and reads each scalar once", async () => {
  const { client, worker } = fixture({ maxPendingBytes: 512 });
  let reads = 0;
  const row = [0];
  Object.defineProperty(row, "0", {
    enumerable: true,
    get() {
      reads++;
      return 42;
    },
  });
  row.extra = new Uint8Array(4096);
  row[Symbol.iterator] = () => {
    throw new Error("must not iterate");
  };
  const pending = client.execute("q", row);
  assert.equal(reads, 1);
  assert.deepEqual(worker.requests[0].params, [42]);
  assert.equal(Object.hasOwn(worker.requests[0].params, "extra"), false);
  success(worker, worker.requests[0]);
  await pending;
  const initialized = client.init({ dbName: "small", extra: new Uint8Array(4096) });
  assert.deepEqual(worker.requests[1].config, { dbName: "small" });
  worker.reply({
    kind: "ready",
    requestId: worker.requests[1].requestId,
    data: { path: "small", persistence: "memory" },
  });
  await initialized;
  client.dispose();
});

test("invalid scalars and malformed bulk rows reject without posting and preserve batchIndex", async () => {
  const { client, worker } = fixture();
  for (const value of [undefined, {}, [], () => {}, Symbol(), 1n << 1000n]) {
    await assert.rejects(client.execute("q", [value]), code("ERR_FSQLITE_REQUEST_INPUT"));
  }
  await assert.rejects(
    client.executeMany("q", [[1], null]),
    (e) => e.code === "ERR_FSQLITE_BULK_INPUT" && e.batchIndex === 1,
  );
  await assert.rejects(
    client.executeMany(
      "q",
      Array.from({ length: 10001 }, () => []),
    ),
    code("ERR_FSQLITE_BULK_INPUT"),
  );
  assert.equal(worker.requests.length, 0);
  client.dispose();
});

test("synchronous transport failures release capacity and keep later requests usable", async () => {
  const { client, worker } = fixture({ maxPendingRequests: 1 });
  const error = new Error("clone failed");
  worker.onPost = () => {
    throw error;
  };
  await assert.rejects(client.execute("q"), (e) => e === error);
  assert.equal(client.requestQueue.pendingRequests, 0);
  assert.equal(client.requestQueue.pendingBytes, 0);
  worker.onPost = undefined;
  const next = client.execute("q");
  success(worker, worker.requests[1]);
  await next;
  client.dispose();
});

test("duplicate and late responses cannot release another request's reservation", async () => {
  const { client, worker } = fixture({ maxPendingRequests: 1 });
  const first = client.execute("one");
  success(worker, worker.requests[0]);
  await first;
  const second = client.execute("two");
  const before = client.requestQueue;
  success(worker, worker.requests[0]);
  worker.reply({ kind: "execute-result", requestId: 999, changes: 1 });
  assert.deepEqual(client.requestQueue, before);
  success(worker, worker.requests[1]);
  await second;
  client.dispose();
});

test("worker SQL errors, crashes and disposal settle leases exactly once", async () => {
  const { client, worker } = fixture({ maxPendingRequests: 2 });
  const a = observe(client.execute("a")),
    b = observe(client.execute("b"));
  worker.reply({
    kind: "error",
    requestId: worker.requests[0].requestId,
    error: { code: "SQLITE_ERROR", message: "bad SQL" },
  });
  await a.settled;
  assert.equal(rejected(a).code, "SQLITE_ERROR");
  assert.equal(client.requestQueue.pendingRequests, 1);
  worker.crash("crash");
  await b.settled;
  assert.match(rejected(b).message, /crash/);
  assert.equal(client.requestQueue.pendingBytes, 0);
  assert.equal(client.requestQueue.pendingRequests, 0);
  await assert.rejects(client.execute("future"), /crash/);
  client.dispose();
  client.dispose();
  assert.equal(worker.terminateCount, 1);
});

test("close uses one independent slot even with all ordinary capacity occupied", async () => {
  const { client, worker } = fixture({ maxPendingRequests: 1, maxPendingBytes: 256 });
  const query = client.execute("q");
  const close = client.close();
  assert.equal(client.close(), close);
  assert.deepEqual(
    worker.requests.map((r) => r.kind),
    ["execute", "close"],
  );
  assert.equal(client.requestQueue.pendingRequests, 1);
  await assert.rejects(client.execute("not admitted"), /closing/);
  success(worker, worker.requests[0]);
  await query;
  worker.reply({ kind: "close-result", requestId: worker.requests[1].requestId });
  await close;
  assert.equal(client.requestQueue.pendingBytes, 0);
  assert.equal(worker.terminateCount, 1);
});

test("cancel control bypasses saturation; missing cancel acks never occupy ordinary capacity", async () => {
  const { client, worker } = fixture({ maxPendingRequests: 1 });
  for (let i = 0; i < 30; i++) {
    const controller = new AbortController();
    const pending = client.executeMany("INSERT INTO t VALUES (?)", [[1]], {
      signal: controller.signal,
    });
    const request = worker.requests.at(-1);
    assert.equal(client.requestQueue.pendingRequests, 1);
    controller.abort();
    assert.equal(worker.requests.at(-1).kind, "cancel-bulk");
    worker.reply({
      kind: "error",
      requestId: request.requestId,
      error: { code: "ERR_FSQLITE_BULK_CANCELLED", message: "rolled back" },
    });
    await assert.rejects(pending, code("ERR_FSQLITE_BULK_CANCELLED"));
    assert.equal(client.requestQueue.pendingRequests, 0);
    assert.equal(client.requestQueue.pendingBytes, 0);
  }
  assert.equal(worker.requests.filter((r) => r.kind === "cancel-bulk").length, 30);
  const close = client.close();
  worker.reply({ kind: "close-result", requestId: worker.requests.at(-1).requestId });
  await close;
});

test("rejected bulk requests remove abort listeners and never send stray cancellations", async () => {
  const { client, worker } = fixture({ maxPendingRequests: 1 });
  const controller = new AbortController();
  const listeners = new Set();
  const add = controller.signal.addEventListener.bind(controller.signal),
    remove = controller.signal.removeEventListener.bind(controller.signal);
  controller.signal.addEventListener = (type, fn, opts) => {
    listeners.add(fn);
    return add(type, fn, opts);
  };
  controller.signal.removeEventListener = (type, fn, opts) => {
    listeners.delete(fn);
    return remove(type, fn, opts);
  };
  const occupied = client.execute("held");
  const batch = client.executeMany("INSERT INTO t VALUES (?)", [[1]], {
    signal: controller.signal,
  });
  const rejection = assert.rejects(batch, full);
  controller.abort();
  await rejection;
  assert.equal(listeners.size, 0);
  assert.equal(worker.requests.length, 1);
  success(worker, worker.requests[0]);
  await occupied;
  client.dispose();
});

test("reentrant parameter getters cannot overbook the queue", async () => {
  const { client, worker } = fixture({ maxPendingRequests: 1 });
  let nested;
  const row = [0];
  Object.defineProperty(row, "0", {
    get() {
      nested = client.execute("nested");
      return 1;
    },
  });
  await assert.rejects(client.execute("outer", row), full);
  assert.equal(worker.requests.length, 1);
  assert.equal(worker.requests[0].sql, "nested");
  assert.equal(client.requestQueue.pendingRequests, 1);
  success(worker, worker.requests[0]);
  await nested;
  client.dispose();
});

test("a close invoked by a parameter getter prevents posting the captured request", async () => {
  const { client, worker } = fixture();
  let closing;
  const row = [0];
  Object.defineProperty(row, "0", {
    get() {
      closing = client.close();
      return 1;
    },
  });
  await assert.rejects(client.execute("must not post", row), /closing/);
  assert.deepEqual(
    worker.requests.map((r) => r.kind),
    ["close"],
  );
  assert.equal(client.requestQueue.pendingBytes, 0);
  worker.reply({ kind: "close-result", requestId: worker.requests[0].requestId });
  await closing;
});

test("an admission error inside a managed transaction rolls back admitted writes", async () => {
  const f = sqliteSnapshotWorker();
  const db = await FrankenDB.open({ worker: f.worker, requestLimits: { maxPendingRequests: 1 } });
  await db.execute("CREATE TABLE t(id INTEGER)");
  const original = f.handles[0].executeWithParams.bind(f.handles[0]);
  const entered = deferred(),
    release = deferred();
  f.handles[0].executeWithParams = async (...args) => {
    entered.resolve();
    await release.promise;
    return original(...args);
  };
  const pending = db.transaction(async (tx) => {
    const first = tx.execute("INSERT INTO t VALUES (?)", [1]);
    await entered.promise;
    await assert.rejects(tx.execute("INSERT INTO t VALUES (?)", [2]), full);
    release.resolve();
    await first;
  });
  await assert.rejects(pending, full);
  assert.deepEqual((await db.query("SELECT * FROM t")).rowArrays, []);
  assert.equal(
    f.worker.requests.filter((r) => r.kind === "execute" && r.params?.[0] === 2).length,
    0,
  );
  assert.equal(db.requestQueue.pendingRequests, 0);
  await db.close();
});

test("streaming imports cooperate with one-request admission and small payload budgets", async () => {
  const f = sqliteSnapshotWorker();
  const db = await FrankenDB.open({
    worker: f.worker,
    requestLimits: { maxPendingRequests: 1, maxPendingBytes: 2048 },
  });
  await db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, value TEXT)");
  const result = await db.executeStream(
    "INSERT INTO t VALUES (?, ?)",
    (function* () {
      for (let i = 0; i < 1001; i++) yield [i, `v${i}`];
    })(),
    { batchSize: 8 },
  );
  assert.equal(result.executions, 1001);
  assert.equal(db.requestQueue.pendingBytes, 0);
  assert.deepEqual((await db.query("SELECT count(*) FROM t")).rowArrays, [[1001]]);
  await assert.rejects(
    db.executeStream(
      "INSERT INTO t VALUES (?, ?)",
      [
        [2000, "ok"],
        [2001, "x".repeat(3000)],
      ],
      { batchSize: 1 },
    ),
    (e) => e.phase === "execute" && e.cause.code === "ERR_FSQLITE_REQUEST_TOO_LARGE",
  );
  assert.deepEqual((await db.query("SELECT count(*) FROM t")).rowArrays, [[1001]]);
  await db.close();
});

test("independent clients do not share admission limits", async () => {
  const a = fixture({ maxPendingRequests: 1 }),
    b = fixture({ maxPendingRequests: 1 });
  const pa = a.client.execute("a"),
    pb = b.client.execute("b");
  assert.equal(a.worker.requests.length, 1);
  assert.equal(b.worker.requests.length, 1);
  success(b.worker, b.worker.requests[0]);
  await pb;
  assert.equal(a.client.requestQueue.pendingRequests, 1);
  success(a.worker, a.worker.requests[0]);
  await pa;
  a.client.dispose();
  b.client.dispose();
});

test("locally refused finalization remains retryable without losing the prepared handle", async () => {
  const f = sqliteSnapshotWorker();
  const db = await FrankenDB.open({ worker: f.worker, requestLimits: { maxPendingRequests: 1 } });
  await db.execute("CREATE TABLE t(id INTEGER)");
  const stmt = await db.prepare("INSERT INTO t VALUES (?)");
  const query = f.handles[0].query.bind(f.handles[0]),
    entered = deferred(),
    release = deferred();
  f.handles[0].query = async (...args) => {
    entered.resolve();
    await release.promise;
    return query(...args);
  };
  const held = db.query("SELECT * FROM t");
  await entered.promise;
  await assert.rejects(stmt.finalize(), full);
  assert.equal(f.worker.requests.filter((r) => r.kind === "statement-finalize").length, 0);
  release.resolve();
  await held;
  assert.equal(await stmt.execute([1]), 1);
  const finalization = stmt.finalize();
  assert.equal(stmt.finalize(), finalization);
  await finalization;
  assert.equal(f.worker.requests.filter((r) => r.kind === "statement-finalize").length, 1);
  assert.deepEqual((await db.query("SELECT * FROM t")).rowArrays, [[1]]);
  assert.equal(db.requestQueue.pendingRequests, 0);
  await db.close();
});

test("receiver-refused finalization is retried and scoped cleanup still frees the handle", async () => {
  const f = sqliteSnapshotWorker({ requestLimits: { maxPendingRequests: 1 } });
  const db = await FrankenDB.open({ worker: f.worker });
  await db.execute("CREATE TABLE t(id INTEGER)");
  let frees = 0;
  const prepare = f.handles[0].prepare.bind(f.handles[0]);
  f.handles[0].prepare = async (sql) => {
    const stmt = await prepare(sql),
      free = stmt.free.bind(stmt);
    stmt.free = () => {
      frees++;
      free();
    };
    return stmt;
  };
  const query = f.handles[0].query.bind(f.handles[0]),
    entered = deferred(),
    release = deferred();
  f.handles[0].query = async (...args) => {
    entered.resolve();
    await release.promise;
    return query(...args);
  };
  const txn = db.transaction(async (tx) => {
    const stmt = await tx.prepare("INSERT INTO t VALUES (?)");
    await stmt.execute([1]);
    const held = tx.query("SELECT * FROM t");
    await entered.promise;
    await assert.rejects(stmt.finalize(), (error) => full(error) && error.transient);
    assert.equal(frees, 0);
    release.resolve();
    await held;
    // No explicit retry: scope cleanup must still own the unfinalized handle.
  });
  await assert.rejects(txn, full);
  assert.equal(frees, 1);
  assert.deepEqual((await db.query("SELECT * FROM t")).rowArrays, []);
  assert.equal(f.host.requestQueue.pendingRequests, 0);
  assert.equal(db.requestQueue.pendingRequests, 0);
  await db.close();
  assert.equal(frees, 1);
});

// Run the unchanged production SDK and host in different JS agents. Only the
// core SQL implementation is replaced with Node SQLite, plus an explicit gate
// around parameter 2 so overload/cancel/close interleavings are deterministic.
async function threaded(t, hostLimits = {}, clientLimits = {}, pause = true) {
  const { Worker } = await import("node:worker_threads");
  const coreUrl = new URL("../../worker/tests/helpers/snapshot-sqlite-core.mjs", import.meta.url)
    .href;
  const script = `
    import { parentPort, workerData } from 'node:worker_threads';
    import { sqliteSnapshotWorker } from ${JSON.stringify(coreUrl)};
    let resume;
    const gate = new Promise(resolve => { resume = resolve; });
    const f = sqliteSnapshotWorker({ requestLimits: workerData.hostLimits });
    parentPort.on('message', request => {
      if (request.fixture === 'resume') { resume(); return; }
      void f.host.handle(request).then(response => {
        if (request.kind === 'init' && response.kind === 'ready' && workerData.pause) {
          const core = f.handles[0], prepare = core.prepare.bind(core);
          core.prepare = async sql => {
            const stmt = await prepare(sql), run = stmt.executeWithParams.bind(stmt);
            stmt.executeWithParams = async params => {
              if (params[0] === 2) { parentPort.postMessage({ fixture: 'entered' }); await gate; }
              return run(params);
            };
            return stmt;
          };
        }
        parentPort.postMessage(response);
      }, error => { throw error; });
    });
  `;
  const native = new Worker(new URL(`data:text/javascript,${encodeURIComponent(script)}`), {
    execArgv: process.execArgv,
    workerData: { hostLimits, pause },
  });
  t.after(async () => {
    await native.terminate();
  });
  const wrappers = new Map(),
    requests = [],
    entered = deferred(),
    cancelled = deferred();
  native.on("message", (data) => {
    if (data.fixture === "entered") entered.resolve();
    if (data.kind === "cancel-bulk-result") cancelled.resolve(data.accepted);
  });
  const worker = {
    addEventListener(type, listener) {
      const wrap =
        type === "message"
          ? (data) => {
              if (data.kind) listener({ data });
            }
          : (error) => listener({ message: error.message });
      wrappers.set(listener, wrap);
      native.on(type, wrap);
    },
    removeEventListener(type, listener) {
      const wrap = wrappers.get(listener);
      if (wrap) native.off(type, wrap);
      wrappers.delete(listener);
    },
    postMessage(request, transfer) {
      requests.push(request);
      native.postMessage(request, transfer);
    },
    terminate() {
      void native.terminate();
    },
  };
  const db = await FrankenDB.open({ worker, requestLimits: clientLimits });
  await db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, value TEXT)");
  return {
    db,
    requests,
    entered: entered.promise,
    cancelled: cancelled.promise,
    resume: () => native.postMessage({ fixture: "resume" }),
  };
}

test("real thread: saturated SDK still delivers cancellation and orderly close", {
  timeout: 10000,
}, async (t) => {
  const f = await threaded(t, { maxPendingRequests: 1 }, { maxPendingRequests: 1 });
  const path = f.db.path,
    controller = new AbortController();
  const bulk = f.db.executeMany(
    "INSERT INTO t VALUES(?,?)",
    [
      [1, "one"],
      [2, "two"],
      [3, "three"],
    ],
    { signal: controller.signal },
  );
  const rejection = assert.rejects(bulk, code("ERR_FSQLITE_BULK_CANCELLED"));
  await f.entered;
  await assert.rejects(f.db.execute("INSERT INTO t VALUES(99,'not sent')"), full);
  const close = f.db.close();
  controller.abort();
  assert.equal(await f.cancelled, true);
  assert.equal(f.db.requestQueue.pendingRequests, 1);
  f.resume();
  await rejection;
  await close;
  assert.equal(f.db.requestQueue.pendingBytes, 0);
  assert.equal(
    f.requests.some((r) => r.sql?.includes("not sent")),
    false,
  );
  const { DatabaseSync } = await import("node:sqlite");
  const oracle = new DatabaseSync(path);
  try {
    assert.deepEqual(oracle.prepare("SELECT * FROM t").all(), []);
    assert.equal(Object.values(oracle.prepare("PRAGMA integrity_check").get())[0], "ok");
  } finally {
    oracle.close();
  }
});

test("real thread: stricter receiver refuses work accepted by the client's larger budget", {
  timeout: 10000,
}, async (t) => {
  const f = await threaded(t, { maxPendingRequests: 1 }, { maxPendingRequests: 8 });
  const bulk = f.db.executeMany("INSERT INTO t VALUES(?,?)", [
    [1, "one"],
    [2, "two"],
  ]);
  await f.entered;
  await assert.rejects(
    f.db.execute("INSERT INTO t VALUES(99,'receiver refused')"),
    (e) => full(e) && e.transient,
  );
  assert.equal(
    f.requests.some((r) => r.sql?.includes("receiver refused")),
    true,
  );
  assert.equal(f.db.requestQueue.pendingRequests, 1);
  f.resume();
  await bulk;
  assert.deepEqual((await f.db.query("SELECT * FROM t ORDER BY id")).rowArrays, [
    [1, "one"],
    [2, "two"],
  ]);
  assert.equal(f.db.requestQueue.pendingRequests, 0);
  await f.db.close();
});

test("real thread: bounded streams roll back prior chunks after request-size refusal", {
  timeout: 10000,
}, async (t) => {
  const f = await threaded(
    t,
    { maxPendingRequests: 1, maxPendingBytes: 2048 },
    { maxPendingRequests: 1, maxPendingBytes: 2048 },
    false,
  );
  const inserted = await f.db.executeStream(
    "INSERT INTO t VALUES(?,?)",
    (function* () {
      for (let i = 0; i < 1001; i++) yield [i, `v${i}`];
    })(),
    { batchSize: 8 },
  );
  assert.equal(inserted.executions, 1001);
  await assert.rejects(
    f.db.executeStream(
      "INSERT INTO t VALUES(?,?)",
      [
        [2000, "discard"],
        [2001, "x".repeat(4096)],
      ],
      { batchSize: 1 },
    ),
    (e) => e.phase === "execute" && e.cause.code === "ERR_FSQLITE_REQUEST_TOO_LARGE",
  );
  assert.deepEqual((await f.db.query("SELECT count(*) FROM t")).rowArrays, [[1001]]);
  assert.equal(f.db.requestQueue.pendingBytes, 0);
  await f.db.close();
});
