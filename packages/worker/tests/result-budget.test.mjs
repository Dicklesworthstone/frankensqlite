// Actual budget + WorkerConnectionHost TypeScript, with native reference SQLite
// as the core adapter. Admission, storage and managed-txn ports below
// are explicit doubles. No WASM/browser/FrankenSQLite execution-memory claim.
// FSQLITE_TYPESCRIPT_MODULE=/path/to/typescript/lib/typescript.js \
// node --experimental-vm-modules --experimental-transform-types --test <file>
import assert from "node:assert/strict";
import { readFile, mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { SourceTextModule, SyntheticModule, runInNewContext } from "node:vm";
import { DatabaseSync } from "node:sqlite";
import { DEFAULT_RESULT_LIMITS, ResultBudgetError, enforceResultLimits, resolveResultLimits } from "../src/result-budget.ts";
import * as resultCodec from "../src/result-codec.ts";
const { default: ts } = await import(process.env.FSQLITE_TYPESCRIPT_MODULE ?? "typescript");
const source = async name => ts.transpileModule(
  await readFile(new URL(`../src/${name}.ts`, import.meta.url), "utf8"),
  { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ESNext } },
).outputText;
const hostSource = process.env.FSQLITE_BASELINE_HOST ? ts.transpileModule(await readFile(process.env.FSQLITE_BASELINE_HOST, "utf8"), { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ESNext } }).outputText : await source("connection"), leaseSource = await source("snapshot-ownership"), bulkSource = await source("bulk");
const code = suffix => ({ code: `ERR_FSQLITE_RESULT_${suffix}` });
const result = value => ({ columns: ["x"], columnCount: 1, columnTypes: ["TEXT"], rows: [{ x: value }], rowArrays: [[value]], changes: 0 });
const check = (value, policy = {}) => enforceResultLimits(value, resolveResultLimits(policy));

test("policies are captured once, bounded and immutable", () => {
  let calls = 0;
  const limits = resolveResultLimits({ get maxRows() { calls++; return 2; } });
  assert.equal(calls, 1); assert.equal(limits.maxRows, 2); assert.ok(Object.isFrozen(limits));
  assert.ok(Object.isFrozen(DEFAULT_RESULT_LIMITS));
  for (const key of Object.keys(limits)) for (const value of [null, 0, -1, 1.5, NaN, Infinity, "2", DEFAULT_RESULT_LIMITS[key] + 1]) {
    assert.throws(() => resolveResultLimits({ [key]: value }), code("INPUT"));
  }
  for (const value of [null, [], 7]) assert.throws(() => resolveResultLimits(value), code("INPUT"));
});

test("row and cell limits are independent, including zero-column rows", () => {
  const data = result(1);
  data.rows.push({ x: 2 }); data.rowArrays.push([2]);
  check(data, { maxRows: 2, maxCells: 2 });
  assert.throws(() => check(data, { maxRows: 1 }), code("LIMIT"));
  assert.throws(() => check(data, { maxCells: 1 }), code("LIMIT"));
  assert.throws(() => check({ columns: [], columnCount: 0, columnTypes: [], rows: [{}, {}], rowArrays: [[], []], changes: 0 }, { maxRows: 1 }), code("LIMIT"));
});

test("exact logical byte boundary counts named AND positional values", () => {
  // Root=16; property/value pairs: columns=30+68, columnCount=38+16,
  // columnTypes=38+74, rows=24+100, rowArrays=34+116, changes=30+16.
  // This includes 16 bytes for each of the five array indices: total=600.
  check(result(1), { maxBytes: 600 });
  assert.throws(() => check(result(1), { maxBytes: 599 }), code("LIMIT"));
  assert.throws(() => check(result("x".repeat(2000)), { maxBytes: 1024 }), code("LIMIT"));
});

test("tiny BLOB views account for the entire cloned buffer, without detaching it", () => {
  const backing = new Uint8Array(8192), view = backing.subarray(4000, 4001);
  assert.throws(() => check(result(view), { maxBytes: 1024 }), code("LIMIT"));
  assert.equal(backing.byteLength, 8192);
  assert.equal(structuredClone(view).buffer.byteLength, 8192);
  check(result(view.slice()), { maxBytes: 1024 });
});

test("shared aliases are charged once and supported cross-realm values survive", () => {
  const value = new Uint8Array(1024);
  check(result(value), { maxBytes: 1700 });
  assert.throws(() => check({ ...result(value), extra: value.slice() }, { maxBytes: 1700 }), code("LIMIT"));
  const cross = runInNewContext('({ columns:["x"], columnCount:1, columnTypes:["BLOB"], rows:[{x:new Uint8Array([1,2])}], rowArrays:[[new Uint8Array([1,2])]], changes:0 })');
  check(cross); check(result(Buffer.from([1, 2])));
});

test("additional metadata and cycles cannot bypass the byte budget", () => {
  const data = result(1); data.cycle = data; check(data);
  data.metadata = "x".repeat(2000);
  assert.throws(() => check(data, { maxBytes: 1024 }), code("LIMIT"));
  const array = [1]; array.extra = "x".repeat(2000);
  assert.throws(() => check({ ...result(1), array }, { maxBytes: 1024 }), code("LIMIT"));
});

test("sparse metadata and column arrays cannot evade the byte bound", () => {
  const sparse = Array(1_000_000);
  assert.throws(() => check({ ...result(1), sparse }, { maxBytes: 4096 }), code("LIMIT"));
  assert.throws(() => check({ columns: sparse, columnCount: sparse.length, columnTypes: [], rows: [], rowArrays: [], changes: 0 }, { maxBytes: 4096 }), code("LIMIT"));
});

test("accessors, malformed rows, unstable buffers and unsupported values fail closed", () => {
  let touched = false;
  const data = result(1);
  Object.defineProperty(data.rows[0], "x", { enumerable: true, get() { touched = true; throw new Error("called"); } });
  assert.throws(() => check(data), code("SHAPE")); assert.equal(touched, false);
  for (const value of [() => {}, new Map(), new Date(), 1n << 100n, Symbol("x"), new Uint8Array(new SharedArrayBuffer(8)), new Uint8Array(new ArrayBuffer(8, { maxByteLength: 16 }))]) {
    assert.throws(() => check(result(value)), code("SHAPE"));
  }
  const detached = new Uint8Array(8); structuredClone(detached, { transfer: [detached.buffer] });
  assert.throws(() => check(result(detached)), code("SHAPE"));
  assert.throws(() => check({ ...result(1), rowArrays: [[1, 2]] }), code("SHAPE"));
  assert.throws(() => check({ ...result(1), rowArrays: Array(1) }), code("SHAPE"));
  const deep = result(1); let link = deep;
  for (let i = 0; i < 70; i++) link = link.next = {};
  assert.throws(() => check(deep), code("SHAPE"));
});

async function fixture(t, policy, encoding = "structured-clone", path = ":memory:") {
  const db = new DatabaseSync(path), executed = [], encoded = [];
  let closed = false;
  class PortError extends Error { constructor(code, message, options, connectionUnusable = false) { super(message, options); this.code = code; this.connectionUnusable = connectionUnusable; this.cleanupErrors = []; } }
  class Budget { limits = { maxStatements: 32, maxBytes: 1_000_000 }; stats = {}; admit(request) { return { request, release() {} }; } reserve() { return { grow() {}, release() {} }; } }
  class Transactions { assertOwner() {} clear() {} fail() {} cancel() { return true; } }
  const ports = {
    "./admission": { RequestAdmissionError: class extends PortError {}, RequestBudget: Budget, validateRequestId() {} },
    "./bindings": { namedParameterEnd() { throw Error("unused"); }, parameterLayout: () => ({ count: 0, names: [] }), resolveBindings: (_l, p) => p },
    "./opfs-snapshot-store": { OpfsSnapshotStore: class {} },
    "./protocol": { MAX_EXECUTE_MANY_ROWS: 10_000, isSnapshotPersistenceMode: () => false },
    "./result-budget": { enforceResultLimits, resolveResultLimits },
    "./result-codec": { resolveResultEncoding: resultCodec.resolveResultEncoding, encodeQueryResponse: (id, data, mode) => { encoded.push(mode); return resultCodec.encodeQueryResponse(id, data, mode); } },
    "./snapshot-store": { IndexedDbSnapshotStore: class {}, SnapshotStoreError: class extends PortError {}, validateSnapshotBytes() {}, validateSnapshotName() {} },
    "./statement-budget": { PreparedStatementBudget: Budget, PreparedStatementError: class extends PortError {}, resolvePreparedStatementLimits: x => x },
    "./transactions": { ManagedTransactionError: class extends PortError {}, ManagedTransactions: Transactions, validateManagedSql() {}, executeManagedBatch: (core, sql) => core.executeBatch(sql) },
    "./vfs-init": { UnsupportedPersistenceModeError: class extends PortError {}, assertSupportedPersistenceMode() {}, createReadyResult: () => ({ persistence: "memory", path }), resolveDatabasePath: () => path, resolvePersistenceMode: () => "memory" },
  };
  const port = name => { assert.ok(ports[name], name); return new SyntheticModule(Object.keys(ports[name]), function () { for (const [k, v] of Object.entries(ports[name])) this.setExport(k, v); }); };
  const lease = new SourceTextModule(leaseSource); await lease.link(port); await lease.evaluate();
  const bulk = new SourceTextModule(bulkSource); await bulk.link(port); await bulk.evaluate();
  const host = new SourceTextModule(hostSource); await host.link(name => name === "./snapshot-ownership" ? lease : name === "./bulk" ? bulk : port(name)); await host.evaluate();
  const query = (sql, params = []) => {
    executed.push(sql);
    const stmt = db.prepare(sql); stmt.setReadBigInts(true); stmt.setReturnArrays(true);
    const columns = stmt.columns().map(c => c.name), columnTypes = stmt.columns().map(c => c.type ?? "");
    const rowArrays = stmt.all(...params);
    const rows = rowArrays.map(values => Object.fromEntries(columns.map((name, i) => [name, values[i]])));
    return { columns, columnCount: columns.length, columnTypes, rows, rowArrays, changes: 0 };
  };
  const core = {
    path, free() {}, close() { if (!closed) { closed = true; db.close(); } },
    async executeBatch(sql) { executed.push(sql); db.exec(sql); },
    async execute(sql) { executed.push(sql); return Number(db.prepare(sql).run().changes); },
    async executeWithParams(sql, params) { executed.push(sql); return Number(db.prepare(sql).run(...params).changes); },
    async query(sql) { return query(sql); }, async queryWithParams(sql, params) { return query(sql, params); },
    async prepare(sql) { const count = db.prepare(sql).columns(); return {
      sql, columnCount: count.length, columnNames: () => count.map(c => c.name), free() {},
      query: () => core.query(sql), queryWithParams: p => core.queryWithParams(sql, p),
      execute: () => core.execute(sql), executeWithParams: p => core.executeWithParams(sql, p),
    }; },
  };
  const instance = new host.namespace.WorkerConnectionHost({ async load() { return { FrankenDB: { create: async () => core } }; } }, {}, {}, policy);
  let id = 0; const call = (kind, extra = {}) => instance.handle({ kind, requestId: ++id, ...extra });
  assert.equal((await call("init", { config: { resultEncoding: encoding } })).kind, "ready");
  t.after(async () => { await call("close"); });
  return { call, executed, encoded, db };
}

for (const encoding of ["structured-clone", "auto", "binary"]) for (const prepared of [false, true]) {
  test(`real SQL row limit precedes ${encoding} encoding, prepared=${prepared}`, async t => {
    const f = await fixture(t, { maxRows: 2 }, encoding);
    const sql = "SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3";
    const response = prepared
      ? await f.call("statement-query", { statementId: (await f.call("prepare", { sql })).data.statementId })
      : await f.call("query", { sql });
    assert.equal(response.error?.code, code("LIMIT").code);
    assert.equal(response.error.transient, false); assert.match(response.error.message, /already executed/);
    assert.equal(f.executed.filter(x => x === sql).length, 1); assert.deepEqual(f.encoded, []);
    const small = await f.call("query", { sql: "SELECT 42 AS x" });
    assert.deepEqual((small.kind === "query-binary-result" ? resultCodec.decodeQueryResult(small.data) : small.data).rowArrays, [[42n]]); assert.deepEqual(f.encoded, [encoding]);
  });
}

for (const fileBacked of [false, true]) {
  test(`oversized INSERT RETURNING is never replayed or falsely reported rolled back, file=${fileBacked}`, async t => {
    const path = fileBacked ? join(await mkdtemp(join(tmpdir(), "fsqlite-result-budget-")), "result.db") : ":memory:";
    const f = await fixture(t, { maxRows: 2 }, "auto", path);
    await f.call("execute-batch", { sql: "CREATE TABLE t(x INTEGER)" });
    const sql = "INSERT INTO t VALUES(1),(2),(3) RETURNING x";
    const response = await f.call("query", { sql });
    assert.equal(response.error?.code, code("LIMIT").code); assert.equal(response.error.transient, false);
    assert.equal(f.db.prepare("SELECT count(*) AS n FROM t").get().n, 3);
    assert.equal(f.executed.filter(x => x === sql).length, 1);
    if (fileBacked) {
      await f.call("close"); const reopened = new DatabaseSync(path);
      assert.equal(reopened.prepare("SELECT count(*) AS n FROM t").get().n, 3); reopened.close();
    }
  });
}

test("real SQL cell and byte budgets reject independently", async t => {
  const f = await fixture(t, { maxCells: 3, maxBytes: 2048 });
  assert.equal((await f.call("query", { sql: "SELECT 1 AS a, 2 AS b UNION ALL SELECT 3,4" })).error?.code, code("LIMIT").code);
  assert.equal((await f.call("query", { sql: "SELECT hex(zeroblob(2000)) AS text" })).error?.code, code("LIMIT").code);
  assert.deepEqual(f.encoded, []);
});

test("omitted host policy preserves legacy result behavior", async t => {
  const f = await fixture(t, undefined);
  const response = await f.call("query", { sql: "SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3" });
  assert.equal(response.data.rowArrays.length, 3); assert.equal(f.encoded.length, 1);
});
