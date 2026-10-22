// Actual SDK database/client, worker host/admission/protocol/budget/codec,
// errors/utils and lease/bulk modules over Node MessageChannel + reference SQLite.
// Bindings, prepared-budget, storage, transaction and stream ports are explicit
// test doubles. This is not a browser/WASM/FrankenSQLite Rust qualification.
// FSQLITE_TYPESCRIPT_MODULE=/path/to/typescript/lib/typescript.js \
// node --experimental-vm-modules --test <file>
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { SourceTextModule, SyntheticModule } from "node:vm";
import { MessageChannel } from "node:worker_threads";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
const { default: ts } = await import(process.env.FSQLITE_TYPESCRIPT_MODULE ?? "typescript");
const encodedSources = new Map();
async function source(area, name) {
  const key = `${area}/${name}`;
  if (!encodedSources.has(key)) {
    let url = new URL(`../../${area}/src/${name}.ts`, import.meta.url);
    const baseline = process.env.FSQLITE_BASELINE_NEGOTIATION;
    if (baseline && ["sdk/database", "sdk/worker-client", "worker/admission", "worker/protocol", "worker/connection"].includes(key))
      url = new URL(`file://${baseline}/${area === "sdk" ? "sdk-" : ""}${name}.ts`);
    encodedSources.set(key, ts.transpileModule(await readFile(url, "utf8"), {
      compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ESNext },
    }).outputText);
  }
  return encodedSources.get(key);
}
const code = suffix => ({ code: `ERR_FSQLITE_RESULT_${suffix}` });
const three = "SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3";
const deferred = () => { let resolve; const promise = new Promise(r => { resolve = r; }); return { promise, resolve }; };

async function fixture(t, { ceiling, mutateReady, beforeLoad } = {}) {
  const modules = new Map(), ports = new Map(), calls = [], received = [], databases = [];
  const stats = { creates: 0, closes: 0, terminations: 0, loads: 0 };
  class PortError extends Error { constructor(code, message, options, connectionUnusable = false) { super(message, options); this.code = code; this.connectionUnusable = connectionUnusable; this.cleanupErrors = []; } }
  class Statements { limits = { maxStatements: 32, maxBytes: 1_000_000 }; stats = {}; reserve() { return { grow() {}, release() {} }; } }
  const unused = () => { throw Error("unexpected test-double path"); };
  const portValues = {
    "worker/bindings": { BindingError: class extends PortError {}, isNamedBindings: x => typeof x === "object" && x !== null && !Array.isArray(x), namedParameterEnd: unused, parameterLayout: () => ({ count: 0, names: [] }), resolveBindings: (_l, x) => x },
    "worker/statement-budget": { PreparedStatementBudget: Statements, PreparedStatementError: class extends PortError {}, resolvePreparedStatementLimits: x => x },
    "worker/snapshot-store": { IndexedDbSnapshotStore: class {}, SnapshotStoreError: class extends PortError {}, validateSnapshotBytes: unused, validateSnapshotName: unused },
    "worker/opfs-snapshot-store": { OpfsSnapshotStore: class {} },
    "worker/transactions": { ManagedTransactionError: class extends PortError {}, ManagedTransactions: class { assertOwner() {} fail() {} clear() {} cancel() { return true; } }, validateTransactionId: id => { if (!/^[1-9][0-9]*$/.test(id)) throw Error("invalid id"); }, validateManagedSql() {}, executeManagedBatch: unused },
    "worker/vfs-init": { UnsupportedPersistenceModeError: class extends PortError {}, assertSupportedPersistenceMode() {}, createReadyResult: () => ({ path: ":memory:", persistence: "memory" }), resolveDatabasePath: () => ":memory:", resolvePersistenceMode: () => "memory" },
    "sdk/statement": { FrankenPreparedStatement: class {} },
    "sdk/stream": { checkStreamCancellation: unused, executeRowStream: unused, streamOptions: unused },
    "sdk/transaction": { captureTransactionOptions: unused, combineTransactionSignals: unused, FrankenTransaction: class {}, TransactionBudget: class {} },
    "sdk/transaction-retry": { isTransactionConflict: unused, resolveTransactionRetryOptions: unused, runTransactionRetry: unused },
  };
  async function load(key) {
    if (modules.has(key)) return modules.get(key);
    if (portValues[key]) {
      const values = portValues[key];
      const module = new SyntheticModule(Object.keys(values), function () { for (const [k, v] of Object.entries(values)) this.setExport(k, v); });
      modules.set(key, module); await module.link(unused); await module.evaluate(); return module;
    }
    if (key === "@frankensqlite/worker") {
      if (ports.has(key)) return ports.get(key);
      const exports = { createFrankenSqliteWorker: unused };
      for (const name of ["admission", "bindings", "protocol", "result-budget", "result-codec", "snapshot-ownership", "statement-budget"]) {
        Object.assign(exports, (await load(`worker/${name}`)).namespace);
      }
      const module = new SyntheticModule(Object.keys(exports), function () { for (const [k, v] of Object.entries(exports)) this.setExport(k, v); });
      ports.set(key, module); await module.link(unused); await module.evaluate(); return module;
    }
    const [area, name] = key.split("/");
    const module = new SourceTextModule(await source(area, name), { identifier: key });
    modules.set(key, module);
    await module.link(specifier => load(specifier === "@frankensqlite/worker" ? specifier : `${area}/${specifier.slice(2)}`));
    await module.evaluate(); return module;
  }
  const Host = (await load("worker/connection")).namespace.WorkerConnectionHost;
  const DB = (await load("sdk/database")).namespace.FrankenDB;
  const Client = (await load("sdk/worker-client")).namespace.FrankenWorkerClient;
  const budget = (await load("worker/result-budget")).namespace;
  const Admission = (await load("worker/admission")).namespace;
  const codec = (await load("worker/result-codec")).namespace;
  function createCore() {
    stats.creates++;
    const db = new DatabaseSync(":memory:"); databases.push(db);
    let closed = false;
    function query(sql, params = []) {
      calls.push(sql); const stmt = db.prepare(sql);
      stmt.setReadBigInts(true); stmt.setReturnArrays(true);
      const metadata = stmt.columns(), columns = metadata.map(c => c.name), rowArrays = stmt.all(...params);
      return { columns, columnCount: columns.length, columnTypes: metadata.map(c => c.type ?? ""), rowArrays,
        rows: rowArrays.map(values => Object.fromEntries(columns.map((name, i) => [name, values[i]]))), changes: 0 };
    }
    const core = { path: ":memory:", free() {}, close() { if (!closed) { closed = true; stats.closes++; db.close(); } },
      async execute(sql) { calls.push(sql); return Number(db.prepare(sql).run().changes); },
      async executeWithParams(sql, params) { calls.push(sql); return Number(db.prepare(sql).run(...params).changes); },
      async executeBatch(sql) { calls.push(sql); db.exec(sql); },
      async query(sql) { return query(sql); }, async queryWithParams(sql, params) { return query(sql, params); },
      async prepare(sql) { const meta = db.prepare(sql).columns(); return { sql, columnCount: meta.length, columnNames: () => meta.map(c => c.name), free() {}, query: () => core.query(sql), queryWithParams: p => core.queryWithParams(sql, p), execute: () => core.execute(sql), executeWithParams: p => core.executeWithParams(sql, p) }; },
    };
    return core;
  }
  const host = new Host({ async load() { stats.loads++; await beforeLoad?.(stats.loads); return { FrankenDB: { create: async () => createCore() } }; } }, {}, {}, ceiling);
  const { port1, port2 } = new MessageChannel(), listeners = new Map();
  let terminated = false, dispatchError;
  const pending = new Set();
  port1.on("message", data => { for (const cb of listeners.get("message") ?? []) cb({ data }); });
  port2.on("message", request => {
    received.push(request);
    const task = (async () => {
      const response = await host.handle(request);
      if (response.kind === "ready") mutateReady?.(response.data);
      if (!terminated) port2.postMessage(response, codec.responseTransferList(response));
    })();
    pending.add(task); void task.then(() => pending.delete(task), error => { pending.delete(task); dispatchError = error; for (const cb of listeners.get("error") ?? []) cb({ message: String(error) }); });
  });
  const worker = {
    addEventListener(kind, listener) { if (!listeners.has(kind)) listeners.set(kind, new Set()); listeners.get(kind).add(listener); },
    removeEventListener(kind, listener) { listeners.get(kind)?.delete(listener); },
    postMessage(request, transfer) { if (terminated) throw Error("terminated"); port1.postMessage(request, transfer); },
    terminate() { if (!terminated) { terminated = true; stats.terminations++; port1.close(); port2.close(); } },
  };
  let id = 1000;
  const direct = (kind, extra = {}) => host.handle({ kind, requestId: id++, ...extra });
  t.after(async () => {
    await Promise.allSettled(pending);
    await direct("close"); worker.terminate();
    assert.equal(dispatchError, undefined);
  });
  return { DB, Client, budget, Admission, host, worker, direct, calls, received, stats, databases };
}

for (const resultEncoding of ["structured-clone", "auto", "binary"]) {
  test(`public open negotiates and enforces row policy through MessageChannel: ${resultEncoding}`, { timeout: 5000 }, async t => {
    const f = await fixture(t);
    const db = await f.DB.open({ worker: f.worker, resultEncoding, resultLimits: { maxRows: 2 } });
    assert.deepEqual(db.resultLimits, { maxRows: 2, maxCells: 1_000_000, maxBytes: 64 * 1024 * 1024 });
    assert.ok(Object.isFrozen(db.resultLimits));
    assert.equal(f.received[0].config.resultLimits.maxRows, 2);
    await assert.rejects(db.query(three), code("LIMIT"));
    assert.deepEqual((await db.query("SELECT 9 AS x")).rowArrays, [[9n]]);
    assert.equal(f.calls.filter(sql => sql === three).length, 1);
    await db.close();
  });
}
for (const key of ["maxRows", "maxCells", "maxBytes"]) {
  test(`host constructor ceiling cannot be relaxed or disabled: ${key}`, { timeout: 5000 }, async t => {
    const ceiling = { [key]: key === "maxBytes" ? 1024 : 2 };
    const f = await fixture(t, { ceiling });
    const ready = await f.direct("init", { config: { resultLimits: {} } });
    assert.equal(ready.data.resultLimits[key], ceiling[key]);
    const next = await f.direct("init", { config: {} });
    assert.equal(next.data.resultLimits[key], ceiling[key]);
    const sql = key === "maxBytes" ? "SELECT hex(zeroblob(2000)) AS x" : three;
    assert.equal((await f.direct("query", { sql })).error?.code, code("LIMIT").code);
  });
}
test("public open captures nested getters once before allocating a worker", async t => {
  const f = await fixture(t); let calls = 0, factories = 0;
  const policy = { get maxRows() { calls++; return 2; } };
  const pending = f.DB.open({ resultLimits: policy, worker: () => { factories++; Object.defineProperty(policy, "maxRows", { value: 100 }); return f.worker; } });
  const db = await pending;
  assert.equal(calls, 1); assert.equal(factories, 1); assert.equal(db.resultLimits.maxRows, 2);
  await assert.rejects(db.query(three), code("LIMIT")); await db.close();
});

test("invalid public policy refuses before worker allocation or SQL", async t => {
  const f = await fixture(t); let factories = 0;
  for (const resultLimits of [null, [], { maxRows: 0 }, { maxCells: NaN }, { maxBytes: Infinity }, { maxRows: "2" }])
    await assert.rejects(f.DB.open({ resultLimits, worker: () => { factories++; return f.worker; } }), code("INPUT"));
  assert.equal(factories, 0); assert.equal(f.stats.loads, 0); assert.deepEqual(f.calls, []);
});

test("actual admission captures, charges and releases result policy atomically", async t => {
  const f = await fixture(t), b = new f.Admission.RequestBudget({ maxPendingBytes: 256 }); let captures = 0;
  const input = { kind: "init", requestId: 1, config: { resultLimits: { get maxRows() { captures++; return 2; } } } };
  const admitted = b.admit(input);
  input.config.resultLimits = { maxRows: 100 };
  assert.equal(captures, 1); assert.equal(admitted.request.config.resultLimits.maxRows, 2);
  assert.ok(Object.isFrozen(admitted.request.config.resultLimits)); assert.equal(b.stats.pendingBytes, 192);
  assert.throws(() => b.admit({ kind: "init", requestId: 2, config: {} }), { code: "ERR_FSQLITE_QUEUE_FULL" });
  admitted.release(); admitted.release(); assert.equal(b.stats.pendingBytes, 0);
  assert.throws(() => b.admit({ kind: "init", requestId: 3, config: { resultLimits: { maxRows: 0 } } }), code("INPUT"));
  assert.equal(b.stats.pendingRequests, 0);
});

test("queued host init cannot observe later input mutation", async t => {
  const gate = deferred(), entered = deferred();
  const f = await fixture(t, { beforeLoad: async n => { if (n === 1) { entered.resolve(); await gate.promise; } } });
  const one = f.direct("init", { config: {} }); await entered.promise;
  const config = { resultLimits: { maxRows: 2 } };
  const two = f.direct("init", { config }); config.resultLimits.maxRows = 200;
  gate.resolve(); await one;
  assert.equal((await two).data.resultLimits.maxRows, 2);
  assert.equal((await f.direct("query", { sql: three })).error?.code, code("LIMIT").code);
});

test("failed candidate initialization preserves the previous policy and database", async t => {
  const failure = new Error("candidate failed");
  const f = await fixture(t, { beforeLoad: n => { if (n === 2) throw failure; } });
  await f.direct("init", { config: { resultLimits: { maxRows: 2 } } });
  await f.direct("execute-batch", { sql: "CREATE TABLE t(x); INSERT INTO t VALUES(10)" });
  const failed = await f.direct("init", { config: { resultLimits: { maxRows: 50 } } });
  assert.equal(failed.kind, "error"); assert.equal(f.stats.closes, 0);
  assert.equal((await f.direct("query", { sql: three })).error?.code, code("LIMIT").code);
  assert.deepEqual((await f.direct("query", { sql: "SELECT x FROM t" })).data.rowArrays, [[10n]]);
});

test("successful replacement publishes its own policy, including explicit legacy omission", async t => {
  const f = await fixture(t);
  await f.direct("init", { config: { resultLimits: { maxRows: 2 } } });
  await f.direct("init", { config: {} });
  assert.equal(f.stats.closes, 1);
  assert.equal((await f.direct("query", { sql: three })).data.rowArrays.length, 3);
});

for (const mutation of ["missing", "partial", "null", "zero", "looser-rows", "looser-cells", "looser-bytes"]) {
  test(`public open refuses ${mutation} result-policy acknowledgement before exposing SQL`, { timeout: 5000 }, async t => {
    const f = await fixture(t, { mutateReady: ready => {
      if (mutation === "missing") delete ready.resultLimits;
      else if (mutation === "partial") ready.resultLimits = { maxRows: 2 };
      else if (mutation === "null") ready.resultLimits = null;
      else if (mutation === "zero") ready.resultLimits = { ...ready.resultLimits, maxRows: 0 };
      else { const key = { "looser-rows": "maxRows", "looser-cells": "maxCells", "looser-bytes": "maxBytes" }[mutation]; ready.resultLimits = { ...ready.resultLimits, [key]: ready.resultLimits[key] + 1 }; }
    } });
    await assert.rejects(f.DB.open({ worker: f.worker, resultLimits: { maxRows: 2, maxCells: 20, maxBytes: 4096 } }), code("POLICY"));
    assert.deepEqual(f.calls, []); assert.equal(f.stats.terminations, 1);
  });
}

test("raw SDK client fences itself after an absent required acknowledgement", async t => {
  const f = await fixture(t, { mutateReady: ready => { delete ready.resultLimits; } });
  const client = new f.Client(f.worker);
  await assert.rejects(client.init({ resultLimits: {} }), code("POLICY"));
  await assert.rejects(client.query("SELECT 1"), code("POLICY"));
  assert.deepEqual(f.calls, []); assert.equal(client.requestQueue.pendingRequests, 0);
  await client.close();
});

test("legacy omission is accepted only when no result policy was requested", async t => {
  const f = await fixture(t, { mutateReady: ready => { delete ready.resultLimits; } });
  const db = await f.DB.open({ worker: f.worker });
  assert.equal(db.resultLimits, null); assert.equal((await db.query(three)).rowArrays.length, 3); await db.close();
});

test("empty explicit policy activates defaults and tighter host limits are acknowledged", async t => {
  const f = await fixture(t, { ceiling: { maxRows: 2 } });
  const db = await f.DB.open({ worker: f.worker, resultLimits: {} });
  assert.equal(db.resultLimits.maxRows, 2); assert.equal(db.resultLimits.maxBytes, 64 * 1024 * 1024);
  await assert.rejects(db.query(three), code("LIMIT")); await db.close();
});

test("ack helper refuses accessors, inherited or missing fields without invoking code", async t => {
  const f = await fixture(t), capture = f.budget.captureResultLimitsAcknowledgement;
  const request = f.budget.resolveResultLimits({ maxRows: 3 }); let touched = false;
  const accessor = { ...request }; Object.defineProperty(accessor, "maxRows", { get() { touched = true; return 1; } });
  for (const input of [accessor, Object.create(request), { ...request, maxRows: undefined }, [], null, undefined])
    assert.throws(() => capture(input, request), code("POLICY"));
  assert.equal(touched, false);
  const supplied = { ...request }, accepted = capture(supplied, request); supplied.maxRows = 99;
  assert.equal(accepted.maxRows, 3); assert.ok(Object.isFrozen(accepted));
});

test("prepared client reads enforce negotiated policy and release normal request reservations", async t => {
  const f = await fixture(t), client = new f.Client(f.worker);
  await client.init({ resultLimits: { maxRows: 2 }, resultEncoding: "binary" });
  const prepared = await client.prepare(three);
  await assert.rejects(client.queryPrepared(prepared.statementId), code("LIMIT"));
  assert.equal(client.requestQueue.pendingRequests, 0); assert.equal(client.requestQueue.pendingBytes, 0);
  await client.finalizePrepared(prepared.statementId); await client.close();
});

test("SDK delivery failure remains non-retryable after a real committed INSERT RETURNING", async t => {
  const f = await fixture(t), db = await f.DB.open({ worker: f.worker, resultLimits: { maxRows: 2 } });
  await db.executeBatch("CREATE TABLE t(x)");
  const sql = "INSERT INTO t VALUES(1),(2),(3) RETURNING x";
  await assert.rejects(db.query(sql), error => { assert.equal(error.code, code("LIMIT").code); assert.equal(error.transient, false); assert.match(error.message, /writes may have committed/); return true; });
  assert.deepEqual((await db.query("SELECT count(*) AS n FROM t")).rowArrays, [[3n]]);
  assert.equal(f.calls.filter(x => x === sql).length, 1); await db.close();
});
