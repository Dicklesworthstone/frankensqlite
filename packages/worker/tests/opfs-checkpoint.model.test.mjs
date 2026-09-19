// Production worker + OPFS store, with the existing real Node SQLite reference
// adapter and an OPFS/Web Locks model. NOT FrankenSQLite WASM/browser evidence.
import { test } from "node:test";
import assert from "node:assert/strict";
import { register } from "node:module";
import { installOpfsModel, deferred } from "./helpers/opfs-model.mjs";
register(`data:text/javascript,${encodeURIComponent(`
  export async function resolve(specifier, context, next) {
    try { return await next(specifier, context); }
    catch (error) {
      if (error.code === "ERR_MODULE_NOT_FOUND" && specifier.startsWith(".") && !/\\.[^/]+$/.test(specifier)) {
        return next(specifier + ".ts", context);
      }
      throw error;
    }
  }
`)}`);
const { sqliteSnapshotWorker } = await import("./helpers/snapshot-sqlite-core.mjs");
const schema = "CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT UNIQUE)";
const code = suffix => ({ code: `ERR_FSQLITE_${suffix}` });
function fixture(t) { const m = installOpfsModel(); t.after(() => m.restore()); return m; }
async function open(t, name = crypto.randomUUID(), hooks = {}, extra = {}) {
  const f = sqliteSnapshotWorker(hooks);
  let requestId = 0;
  const send = async request => {
    const response = await f.host.handle(structuredClone({ ...request, requestId: ++requestId }));
    if (response.kind === "error") throw Object.assign(new Error(response.error.message), response.error);
    return response;
  };
  t.after(() => f.host.handle({ kind: "close", requestId: ++requestId }));
  const ready = await send({ kind: "init", config: { dbName: name, persistence: "opfs-snapshot", ...extra } });
  return { ...f, send, ready, name,
    batch: sql => send({ kind: "execute-batch", sql }),
    query: async sql => (await send({ kind: "query", sql })).data.rowArrays,
    checkpoint: async publicationId => (await send({ kind: "checkpoint", publicationId })).data,
    close: () => send({ kind: "close" }),
  };
}

test("OPFS worker checkpoints reopen real SQLite schema, indexes, rows and blobs", async t => {
  fixture(t); const a = await open(t, "offline/notes");
  assert.equal(a.ready.data.path, a.name); assert.equal(a.ready.data.persistence, "opfs-snapshot");
  assert.equal(a.ready.data.snapshot, null); assert.equal(a.ready.data.checkpointRecovery, 1);
  await a.batch(`${schema}; CREATE INDEX by_name ON items(name); CREATE TABLE payloads(b BLOB)`);
  await a.send({ kind: "execute-many", sql: "INSERT INTO items VALUES (?, ?)", parameterSets: [[1, "Ada"], [2, "Grace"]] });
  await a.send({ kind: "execute", sql: "INSERT INTO payloads VALUES (?)", params: [Uint8Array.of(0, 255, 8)] });
  const saved = await a.checkpoint(); await a.close(); const b = await open(t, a.name);
  assert.equal(b.ready.data.snapshot.revision, saved.revision);
  assert.deepEqual(b.counts(), { creates: 0, imports: 1 });
  assert.deepEqual(await b.query("SELECT * FROM items ORDER BY id"), [[1, "Ada"], [2, "Grace"]]);
  assert.deepEqual([...(await b.query("SELECT b FROM payloads"))[0][0]], [0, 255, 8]);
  assert.deepEqual(await b.query("PRAGMA integrity_check"), [["ok"]]);
  assert.deepEqual(await b.query("SELECT name FROM sqlite_master WHERE type='index' AND name='by_name'"), [["by_name"]]);
});

test("OPFS worker close does not publish unsaved SQL or a new empty database", async t => {
  const m = fixture(t); const empty = await open(t, "empty"); await empty.batch(schema); await empty.close();
  assert.equal(m.files.size, 0);
  const fresh = await open(t, "empty"); assert.equal(fresh.ready.data.snapshot, null);
  assert.deepEqual(await fresh.query("SELECT name FROM sqlite_master WHERE type='table'"), []);
  const a = await open(t); await a.batch(`${schema}; INSERT INTO items VALUES(1,'saved')`);
  await a.checkpoint(); await a.batch("INSERT INTO items VALUES(2,'unsaved')"); await a.close();
  const b = await open(t, a.name); assert.deepEqual(await b.query("SELECT * FROM items"), [[1, "saved"]]);
});

test("OPFS worker stale conflicts preserve local data and the winning durable image", async t => {
  fixture(t); const a = await open(t); await a.batch(schema); const first = await a.checkpoint();
  const b = await open(t, a.name); assert.equal(b.ready.data.snapshot.revision, first.revision);
  await a.batch("INSERT INTO items VALUES(1,'winner')"); const winner = await a.checkpoint();
  await b.batch("INSERT INTO items VALUES(2,'local')");
  await assert.rejects(b.checkpoint(), code("SNAPSHOT_CONFLICT"));
  assert.deepEqual(await b.query("SELECT * FROM items"), [[2, "local"]]);
  assert.ok((await b.send({ kind: "export" })).data.byteLength >= 512);
  const c = await open(t, a.name); assert.equal(c.ready.data.snapshot.revision, winner.revision);
  assert.deepEqual(await c.query("SELECT * FROM items"), [[1, "winner"]]);
});

test("OPFS worker checkpoint does not roll back or persist a manual transaction", async t => {
  fixture(t); const a = await open(t); await a.batch(schema); const saved = await a.checkpoint();
  await a.batch("BEGIN; INSERT INTO items VALUES(1,'tentative')"); const start = a.events.length;
  await assert.rejects(a.checkpoint(), code("SNAPSHOT_TRANSACTION"));
  assert.deepEqual(a.events.slice(start), ["BEGIN"]);
  assert.deepEqual(await a.query("SELECT * FROM items"), [[1, "tentative"]]);
  await a.batch("ROLLBACK"); const b = await open(t, a.name);
  assert.equal(b.ready.data.snapshot.revision, saved.revision); assert.deepEqual(await b.query("SELECT * FROM items"), []);
});

test("OPFS worker managed ownership remains enforced through checkpoint boundaries", async t => {
  fixture(t); const a = await open(t); await a.batch(schema);
  await a.send({ kind: "transaction", transactionId: "1", action: "begin" });
  await a.send({ kind: "execute", transactionId: "1", sql: "INSERT INTO items VALUES(1,'owned')" });
  await assert.rejects(a.checkpoint(), code("TRANSACTION_OWNERSHIP"));
  await assert.rejects(a.send({ kind: "checkpoint", transactionId: "1" }), code("TRANSACTION_OWNERSHIP"));
  // The rejected owned operation fences this transaction, so roll it back.
  await a.send({ kind: "transaction", transactionId: "1", action: "rollback" });
  await a.send({ kind: "transaction", transactionId: "2", action: "begin" });
  await a.send({ kind: "execute", transactionId: "2", sql: "INSERT INTO items VALUES(2,'committed')" });
  await a.send({ kind: "transaction", transactionId: "2", action: "commit" });
  await a.checkpoint(); const b = await open(t, a.name);
  assert.deepEqual(await b.query("SELECT * FROM items"), [[2, "committed"]]);
});

test("OPFS worker reconstructs a lost publication receipt without exporting or replaying SQL", async t => {
  const m = fixture(t), a = await open(t); await a.batch(schema); const parent = await a.checkpoint();
  await a.batch("INSERT INTO items VALUES(1,'once')"); const publicationId = crypto.randomUUID();
  m.hooks.afterClose = () => { throw new Error("lost close acknowledgement"); };
  await assert.rejects(a.checkpoint(publicationId), /lost close acknowledgement/);
  const count = a.events.length, publications = m.counts.publications;
  const recovered = await a.send({ kind: "checkpoint-recover", publicationId, parentRevision: parent.revision });
  assert.equal(recovered.data.revision, publicationId); assert.equal(a.events.length, count);
  assert.equal(m.counts.publications, publications); delete m.hooks.afterClose;
  const next = await a.checkpoint(); assert.equal(next.parentRevision, publicationId);
  const b = await open(t, a.name); assert.deepEqual(await b.query("SELECT * FROM items"), [[1, "once"]]);
  await assert.rejects(a.send({ kind: "checkpoint-recover", publicationId, parentRevision: parent.revision }), code("SNAPSHOT_NOT_CONFIRMED"));
});

test("OPFS worker publication failures retain the previous revision and permit a safe retry", async t => {
  const m = fixture(t), a = await open(t); await a.batch(schema); const parent = await a.checkpoint();
  await a.batch("INSERT INTO items VALUES(1,'retained')");
  m.hooks.beforeClose = () => { throw new DOMException("quota exhausted", "QuotaExceededError"); };
  await assert.rejects(a.checkpoint(), /quota exhausted/); delete m.hooks.beforeClose;
  const b = await open(t, a.name); assert.equal(b.ready.data.snapshot.revision, parent.revision);
  assert.deepEqual(await b.query("SELECT * FROM items"), []);
  const saved = await a.checkpoint(); assert.equal(saved.parentRevision, parent.revision);
  assert.deepEqual(await a.query("SELECT * FROM items"), [[1, "retained"]]);
});

test("OPFS worker FIFO holds later SQL and close until publication settles", async t => {
  const m = fixture(t), a = await open(t); await a.batch(`${schema}; INSERT INTO items VALUES(1,'before')`);
  const entered = deferred(), release = deferred();
  m.hooks.beforeClose = async () => { entered.resolve(); await release.promise; };
  const saving = a.checkpoint(); await entered.promise;
  let wrote = false;
  const writing = a.batch("INSERT INTO items VALUES(2,'after')").then(() => { wrote = true; });
  const closing = a.close(); await new Promise(resolve => setImmediate(resolve)); assert.equal(wrote, false);
  release.resolve(); await saving; await writing; await closing; delete m.hooks.beforeClose;
  const b = await open(t, a.name); assert.deepEqual(await b.query("SELECT * FROM items"), [[1, "before"]]);
});

test("OPFS failed reinitialization preserves the previous session and checkpoint lineage", async t => {
  fixture(t); const a = await open(t, "original"); await a.batch(`${schema}; INSERT INTO items VALUES(1,'live')`);
  const parent = await a.checkpoint(); const image = (await a.send({ kind: "export" })).data;
  await assert.rejects(a.send({ kind: "init", config: { dbName: a.name, persistence: "opfs-snapshot", snapshot: image } }), code("SNAPSHOT_EXISTS"));
  assert.deepEqual(await a.query("SELECT * FROM items"), [[1, "live"]]);
  const next = await a.checkpoint(); assert.equal(next.parentRevision, parent.revision);
  const b = await open(t, "seeded", {}, { snapshot: image });
  assert.equal(b.ready.data.snapshot, null); await b.checkpoint();
  const c = await open(t, "seeded"); assert.deepEqual(await c.query("SELECT * FROM items"), [[1, "live"]]);
});

test("OPFS worker import failure does not dispose a usable previous database", async t => {
  fixture(t); const source = await open(t, "saved"); await source.batch(schema); await source.checkpoint();
  const hooks = {}, a = await open(t, "live", hooks); await a.batch(`${schema}; INSERT INTO items VALUES(1,'local')`);
  hooks.beforeImport = () => { throw new Error("import failed"); };
  await assert.rejects(a.send({ kind: "init", config: { dbName: "saved", persistence: "opfs-snapshot" } }), /import failed/);
  assert.deepEqual(await a.query("SELECT * FROM items"), [[1, "local"]]);
  assert.equal(a.events.includes("close"), false);
});

test("OPFS checkpoint rollback-probe failure fences the connection before queued SQL", async t => {
  const m = fixture(t), hooks = {}, a = await open(t, "probe", hooks); await a.batch(schema);
  hooks.beforeBatch = sql => { if (sql === "ROLLBACK") throw new Error("rollback failed"); };
  const checkpoint = assert.rejects(a.checkpoint(), code("SNAPSHOT_CONNECTION_UNUSABLE"));
  const write = assert.rejects(a.batch("INSERT INTO items VALUES(1,'must not run')"), code("SNAPSHOT_CONNECTION_UNUSABLE"));
  await Promise.all([checkpoint, write]); assert.equal(m.counts.publications, 0);
  assert.equal(a.events.some(sql => sql.includes("must not run")), false);
});

test("OPFS capability failure and unsupported page-VFS modes never allocate a core", async t => {
  fixture(t); Object.defineProperty(globalThis, "navigator", { configurable: true, value: {} });
  const f = sqliteSnapshotWorker();
  for (const [persistence, suffix] of [["opfs-snapshot", "SNAPSHOT_UNAVAILABLE"], ["opfs", "UNSUPPORTED_PERSISTENCE"], ["indexeddb", "UNSUPPORTED_PERSISTENCE"]]) {
    const result = await f.host.handle({ kind: "init", requestId: 1, config: { persistence, dbName: "test" } });
    assert.equal(result.kind, "error"); assert.equal(result.error.code, code(suffix).code);
  }
  assert.deepEqual(f.counts(), { creates: 0, imports: 0 });
});
