// Isolated lifecycle tests: execute the actual host, lease and bulk TypeScript.
// Core SQL, stores, admission/binding/codec and transaction ports are test doubles;
// this is not a WASM, browser, SQL rollback or storage-durability qualification.
// node --experimental-vm-modules --test packages/worker/tests/snapshot-ownership-host.test.mjs
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { test } from "node:test";
import { SourceTextModule, SyntheticModule } from "node:vm";

const { default: ts } = await import(process.env.FSQLITE_TYPESCRIPT_MODULE ?? "typescript");
const transpile = async name => ts.transpileModule(
  await readFile(new URL(`../src/${name}.ts`, import.meta.url), "utf8"),
  { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ESNext } },
).outputText;
const hostSource = await transpile("connection");
const leaseSource = await transpile("snapshot-ownership");
const bulkSource = await transpile("bulk");
const LOST = "ERR_FSQLITE_SNAPSHOT_OWNERSHIP_LOST";
const deferred = () => {
  let resolve, reject;
  const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
};
const turn = () => new Promise(resolve => setImmediate(resolve));

async function fixture(t) {
  const state = { calls: [], stores: [], databases: [], locks: [], saves: 0, exports: 0 };
  const original = Object.getOwnPropertyDescriptor(globalThis, "navigator");
  Object.defineProperty(globalThis, "navigator", { configurable: true, value: {
    locks: { request(name, options, callback) {
      const released = deferred();
      const held = callback({ name, mode: options.mode });
      void Promise.resolve(held).then(released.resolve, released.reject);
      state.locks.push({ ...released, held, name });
      return released.promise;
    } },
  } });
  t.after(() => {
    if (original) Object.defineProperty(globalThis, "navigator", original);
    else Reflect.deleteProperty(globalThis, "navigator");
  });
  class PortError extends Error {
    constructor(code, message, options, connectionUnusable = false) {
      super(message, options); this.code = code;
      this.connectionUnusable = connectionUnusable; this.cleanupErrors = [];
    }
  }
  class Budget {
    limits = { maxStatements: 16, maxBytes: 1_000_000 };
    stats = {};
    admit(request) { return { request, release() {} }; }
    reserve() { return { grow() {}, release() {} }; }
  }
  class Transactions {
    assertOwner() {}
    clear() {}
    fail() {}
    cancel() { return true; }
    async boundary(db, request, finalize) {
      finalize(request.transactionId);
      await db.executeBatch(request.action.toUpperCase());
    }
  }
  class Store {
    static async open(name) {
      const store = new Store(); store.name = name; store.closed = 0;
      state.stores.push(store);
      await state.open?.(store);
      return store;
    }
    async load() { await state.load?.(this); return null; }
    async save() {
      state.saves++; await state.save?.();
      return { revision: "saved", parentRevision: null, byteLength: 1, sha256: "digest" };
    }
    async confirmPublication() { await state.confirm?.(); return { revision: "saved" }; }
    close() { this.closed++; state.calls.push("store.close"); }
  }
  const ports = {
    "./admission": { RequestAdmissionError: class extends PortError {}, RequestBudget: Budget, validateRequestId() {} },
    "./bindings": { namedParameterEnd() { throw new Error("unexpected named bind"); }, parameterLayout: () => ({ count: 0, names: [] }), resolveBindings: (_layout, values) => values },
    "./opfs-snapshot-store": { OpfsSnapshotStore: Store },
    "./protocol": { MAX_EXECUTE_MANY_ROWS: 10_000, isSnapshotPersistenceMode: mode => ["opfs-snapshot", "indexeddb-snapshot"].includes(mode) },
    "./result-codec": {
      resolveResultEncoding: value => value ?? "structured-clone",
      encodeQueryResponse: (requestId, data) => ({ kind: "query-result", requestId, data }),
    },
    "./snapshot-store": { IndexedDbSnapshotStore: Store, SnapshotStoreError: class extends PortError {}, validateSnapshotBytes() {}, validateSnapshotName() {} },
    "./statement-budget": { PreparedStatementBudget: Budget, PreparedStatementError: class extends PortError {}, resolvePreparedStatementLimits: value => value },
    "./transactions": {
      ManagedTransactionError: class extends PortError {}, ManagedTransactions: Transactions,
      validateManagedSql() {},
      async executeManagedBatch(db, sql, check) {
        for (const statement of sql.split(";")) { check(); await db.executeBatch(statement); check(); }
      },
    },
    "./vfs-init": {
      UnsupportedPersistenceModeError: class extends PortError {}, assertSupportedPersistenceMode() {},
      createReadyResult: config => ({ persistence: config.persistence ?? "memory", path: config.dbName ?? ":memory:" }),
      resolveDatabasePath: config => config.dbName ?? ":memory:", resolvePersistenceMode: mode => mode ?? "memory",
    },
  };
  const lease = new SourceTextModule(leaseSource);
  await lease.link(() => { throw new Error("unexpected lease dependency"); });
  await lease.evaluate();
  const port = name => {
    const exports = ports[name];
    assert.ok(exports, `unexpected host dependency ${name}`);
    return new SyntheticModule(Object.keys(exports), function () {
      for (const [key, value] of Object.entries(exports)) this.setExport(key, value);
    });
  };
  const bulk = new SourceTextModule(bulkSource);
  await bulk.link(port); await bulk.evaluate();
  const module = new SourceTextModule(hostSource);
  await module.link(name => name === "./snapshot-ownership" ? lease : name === "./bulk" ? bulk : port(name));
  await module.evaluate();
  const create = async path => {
    const db = {
      path, closed: 0, freed: 0,
      close() { this.closed++; state.calls.push("db.close"); },
      free() { this.freed++; state.calls.push("db.free"); },
      async execute(sql) { state.calls.push(sql); await state.execute?.(sql); return 1; },
      async executeWithParams(sql, _params) { return this.execute(sql); },
      async executeBatch(sql) { state.calls.push(sql); await state.batch?.(sql); },
      async query(sql) { state.calls.push(sql); await state.query?.(sql); return { columns: [], rows: [] }; },
      async queryWithParams(sql, _params) { return this.query(sql); },
      async export() { state.exports++; await state.export?.(); return new Uint8Array([1]); },
      async prepare(sql) {
        const statement = {
          sql, columnCount: 0, free() { state.calls.push("stmt.free"); }, columnNames: () => [],
          execute: () => db.execute(sql), executeWithParams: params => db.executeWithParams(sql, params),
          query: () => db.query(sql), queryWithParams: params => db.queryWithParams(sql, params),
        };
        await state.prepare?.();
        return statement;
      },
    };
    state.databases.push(db);
    await state.create?.(db);
    return db;
  };
  const host = new module.namespace.WorkerConnectionHost({
    async load() { await state.loader?.(); return { FrankenDB: { create, import: () => create("import") } }; },
  });
  let id = 0;
  const call = (kind, extra = {}) => host.handle({ kind, requestId: ++id, ...extra });
  const open = (extra = {}) => call("init", { config: { persistence: "opfs-snapshot", dbName: "owned", snapshotOwnership: "exclusive", ...extra } });
  const lose = async (index = state.locks.length - 1) => {
    state.locks[index].reject(new DOMException("stolen", "AbortError"));
    await turn();
  };
  t.after(async () => { await call("close"); });
  return { state, host, call, open, lose };
}

for (const kind of ["execute", "query", "execute-batch", "export"]) {
  test(`lost ownership drains active ${kind} and refuses queued/new work`, async t => {
    const f = await fixture(t), entered = deferred(), complete = deferred();
    assert.equal((await f.open()).kind, "ready");
    const hook = { execute: "execute", query: "query", "execute-batch": "batch", export: "export" }[kind];
    f.state[hook] = async () => { entered.resolve(); await complete.promise; };
    const running = f.call(kind, { sql: "active" });
    await entered.promise;
    const queued = f.call("execute", { sql: "queued" });
    await f.lose();
    const db = f.state.databases[0];
    const closedWhileRunning = db.closed;
    const late = f.call("execute", { sql: "late" });
    complete.resolve();
    const result = await running;
    assert.equal(closedWhileRunning, 0, "must not destroy a running handle");
    assert.equal((await late).error?.code, LOST);
    assert.equal(result.error?.code, LOST);
    assert.equal(result.error.userRecoverable, false);
    assert.equal(result.error.transient, false);
    assert.match(result.error.cause.message, /stolen/);
    assert.equal((await queued).error?.code, LOST);
    await f.call("close");
    assert.equal(db.closed, 1); assert.equal(db.freed, 1);
    assert.ok(!f.state.calls.includes("queued") && !f.state.calls.includes("late"));
  });
}

for (const phase of ["batch", "export", "save"]) {
  test(`checkpoint loss during ${phase} never advances unstarted publication or abandons started work`, async t => {
    const f = await fixture(t), entered = deferred(), complete = deferred();
    await f.open();
    f.state[phase] = async sql => {
      if (phase === "batch" && sql !== "BEGIN") return;
      entered.resolve(); await complete.promise;
    };
    const pending = f.call("checkpoint", { publicationId: "pub" });
    await entered.promise;
    await f.lose();
    assert.equal(f.state.databases[0].closed, 0);
    let settled = false; void pending.then(() => { settled = true; });
    await turn(); assert.equal(settled, false, "must await actual operation outcome");
    complete.resolve();
    assert.equal((await pending).error?.code, LOST);
    assert.equal(f.state.saves, phase === "save" ? 1 : 0);
    assert.equal(f.state.exports, phase === "batch" ? 0 : 1);
    assert.deepEqual(f.state.calls.filter(x => x === "BEGIN" || x === "ROLLBACK"), ["BEGIN", "ROLLBACK"]);
    await f.call("close");
  });
}

for (const phase of ["open", "load", "loader", "create"]) {
  test(`candidate lease lost during ${phase} is not published and does not retire the old memory session`, async t => {
    const f = await fixture(t), entered = deferred(), complete = deferred();
    await f.open({ persistence: "memory", snapshotOwnership: undefined });
    f.state[phase] = async () => { entered.resolve(); await complete.promise; };
    const pending = f.open();
    await entered.promise; await f.lose(); complete.resolve();
    assert.equal((await pending).kind, "error");
    const old = f.state.databases[0];
    assert.equal(old.closed, 0); assert.equal(old.freed, 0);
    assert.equal((await f.call("execute", { sql: "old still usable" })).kind, "execute-result");
    assert.equal(f.state.stores[0].closed, 1);
    if (phase !== "create") assert.equal(f.state.databases.length, 1, "no core allocation after loss");
    else { assert.equal(f.state.databases[1].closed, 1); assert.equal(f.state.databases[1].freed, 1); }
  });
}

test("same-namespace replacement retains one lease and keeps its loss observer", async t => {
  const f = await fixture(t);
  await f.open(); await f.open();
  assert.equal(f.state.locks.length, 1);
  assert.equal(f.state.databases[0].closed, 1);
  await f.lose();
  assert.equal((await f.call("execute", { sql: "forbidden" })).error?.code, LOST);
  await f.call("close");
  assert.equal(f.state.databases[1].closed, 1);
});

test("normal retirement never fences a replacement namespace", async t => {
  const f = await fixture(t);
  await f.open(); await f.open({ dbName: "second" });
  assert.equal(f.state.locks.length, 2);
  await turn();
  assert.equal((await f.call("execute", { sql: "healthy" })).kind, "execute-result");
  assert.equal((await f.call("close")).kind, "close-result");
  assert.equal(f.state.databases[1].closed, 1);
});

test("lost ownership never publishes a newly prepared handle", async t => {
  const f = await fixture(t), entered = deferred(), complete = deferred();
  await f.open();
  f.state.prepare = async () => { entered.resolve(); await complete.promise; };
  const pending = f.call("prepare", { sql: "SELECT 1" });
  await entered.promise; await f.lose();
  assert.ok(!f.state.calls.includes("stmt.free"));
  complete.resolve();
  assert.equal((await pending).error?.code, LOST);
  await f.call("close");
  assert.equal(f.state.calls.filter(x => x === "stmt.free").length, 1);
});

test("all bulk requests pass a loss guard even when init was still queued at admission", async t => {
  const f = await fixture(t), entered = deferred(), complete = deferred();
  const opening = f.open();
  f.state.execute = async () => { entered.resolve(); await complete.promise; };
  const batch = f.call("execute-many", { sql: "INSERT", parameterSets: [[1], [2]], cancellable: false });
  await opening; await entered.promise; await f.lose(); complete.resolve();
  assert.equal((await batch).error?.code, LOST);
  assert.equal(f.state.calls.filter(x => x === "INSERT").length, 1, "second row cannot start");
  assert.ok(f.state.calls.some(sql => sql.startsWith("ROLLBACK TO SAVEPOINT")));
});

test("an operation's error is retained but never advertised retryable after loss", async t => {
  const f = await fixture(t), entered = deferred(), complete = deferred();
  await f.open();
  f.state.execute = async () => { entered.resolve(); await complete.promise; throw Object.assign(new Error("uncertain commit"), { code: "BUSY", transient: true, userRecoverable: true }); };
  const pending = f.call("execute", { sql: "write" });
  await entered.promise; await f.lose(); complete.resolve();
  const result = await pending;
  assert.equal(result.error.code, "BUSY");
  assert.equal(result.error.message, "uncertain commit");
  assert.equal(result.error.transient, false); assert.equal(result.error.userRecoverable, false);
});

test("idle ownership loss retires resources without waiting for another SQL request", async t => {
  const f = await fixture(t);
  await f.open(); await f.lose();
  assert.equal(f.state.databases[0].closed, 1);
  assert.equal(f.state.databases[0].freed, 1);
  assert.equal(f.state.stores[0].closed, 1);
  assert.equal((await f.call("init", { config: { persistence: "memory" } })).error?.code, LOST);
});

for (const kind of ["statement-query", "statement-execute", "transaction"]) {
  test(`loss during ${kind} refuses its acknowledgement without freeing its active handle`, async t => {
    const f = await fixture(t), entered = deferred(), complete = deferred();
    await f.open();
    const prepared = await f.call("prepare", { sql: "SELECT 1" });
    const hook = kind === "statement-query" ? "query" : kind === "transaction" ? "batch" : "execute";
    f.state[hook] = async () => { entered.resolve(); await complete.promise; };
    const running = f.call(kind, kind === "transaction"
      ? { action: "commit", transactionId: "1" }
      : { statementId: prepared.data.statementId });
    await entered.promise; await f.lose();
    const prematureFree = f.state.calls.includes("stmt.free");
    complete.resolve();
    assert.equal((await running).error?.code, LOST);
    assert.equal(prematureFree, false);
    await f.call("close");
    assert.equal(f.state.calls.filter(x => x === "stmt.free").length, 1);
  });
}

test("bulk rollback failure is drained and retained; loss does not authorize RELEASE or early free", async t => {
  const f = await fixture(t), entered = deferred(), complete = deferred();
  const rollbackEntered = deferred(), rollbackDone = deferred();
  await f.open();
  f.state.execute = async () => { entered.resolve(); await complete.promise; };
  f.state.batch = async sql => {
    if (sql.startsWith("ROLLBACK TO")) { rollbackEntered.resolve(); await rollbackDone.promise; throw new Error("rollback failed"); }
  };
  const running = f.call("execute-many", { sql: "INSERT", parameterSets: [[1], [2]] });
  await entered.promise; await f.lose(); complete.resolve();
  await rollbackEntered.promise;
  const prematureClose = f.state.databases[0].closed;
  rollbackDone.resolve();
  const result = await running;
  assert.equal(result.error.code, "ERR_FSQLITE_BULK_CONNECTION_UNUSABLE");
  assert.equal(result.error.userRecoverable, false);
  assert.equal(result.error.cause.code, LOST);
  assert.match(result.error.cleanupErrors[0].message, /rollback failed/);
  assert.equal(prematureClose, 0);
  assert.ok(!f.state.calls.some(sql => sql.startsWith("RELEASE")));
  await f.call("close");
  assert.equal(f.state.databases[0].closed, 1);
});

test("ordinary transport failure retains the prior active-result and close-admission contract", async t => {
  const f = await fixture(t), entered = deferred(), complete = deferred();
  await f.open({ persistence: "memory", snapshotOwnership: undefined });
  f.state.execute = async () => { entered.resolve(); await complete.promise; };
  const running = f.call("execute", { sql: "already running" });
  await entered.promise;
  const closing = f.host.failTransport(new Error("transport failed"));
  const refused = f.call("execute", { sql: "later" });
  complete.resolve();
  assert.equal((await running).kind, "execute-result");
  assert.equal((await refused).error?.code, "ERR_FSQLITE_CONNECTION_CLOSED");
  assert.equal((await closing).kind, "close-result");
});
