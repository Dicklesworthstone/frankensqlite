// Production SDK + worker + snapshot store; transactional IDB model, real Node
// SQLite file exports/imports and structured-clone transport. NOT browser/WASM.
import { test } from "node:test";
import assert from "node:assert/strict";
import { FrankenDB } from "../../sdk/src/database.ts";
import { installIndexedDbModel, ModelObjectStore } from "./helpers/indexeddb-model.mjs";
import { sqliteSnapshotWorker } from "./helpers/snapshot-sqlite-core.mjs";
const { databases } = installIndexedDbModel();
const state = name => databases.get(`frankensqlite:snapshot:v1:${name}`);
const code = value => ({ code: value });
async function open(name = crypto.randomUUID(), hooks) {
  const f = sqliteSnapshotWorker(hooks);
  const db = await FrankenDB.open({ worker: f.worker, persistence: "indexeddb-snapshot", dbName: name });
  return { ...f, db, name };
}
const rows = async db => (await db.query("SELECT id, name FROM items ORDER BY id")).rowArrays;
const schema = "CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT UNIQUE)";

// This import path is relative to packages/worker/tests, hence ../../sdk.
test("checkpoint: real SQLite data, blobs, indexes and schema survive reopen", async () => {
  const a = await open();
  assert.equal(a.db.path, a.name); assert.equal(a.db.persistence, "indexeddb-snapshot");
  assert.equal(a.db.snapshotRevision, null);
  await a.db.executeBatch(schema + "; CREATE INDEX by_name ON items(name); CREATE TABLE payloads(b BLOB)");
  await a.db.executeMany("INSERT INTO items VALUES (?, ?)", [[1, "Ada"], [2, "Grace"]]);
  await a.db.execute("INSERT INTO payloads VALUES (?)", [Uint8Array.of(0, 255, 8)]);
  const saved = await a.db.checkpoint();
  assert.equal(a.db.snapshotRevision, saved.revision); assert.match(saved.sha256, /^[a-f0-9]{64}$/);
  await a.db.close();
  const b = await open(a.name);
  assert.deepEqual(await rows(b.db), [[1, "Ada"], [2, "Grace"]]);
  assert.deepEqual([...((await b.db.query("SELECT b FROM payloads")).rowArrays[0][0])], [0, 255, 8]);
  assert.deepEqual((await b.db.query("PRAGMA integrity_check")).rowArrays, [["ok"]]);
  assert.equal(b.db.snapshotRevision, saved.revision); assert.deepEqual(b.counts(), { creates: 0, imports: 1 });
  await b.db.close();
});

test("checkpoint: close does not silently publish uncheckpointed memory writes", async () => {
  const a = await open(); await a.db.executeBatch(schema);
  await a.db.execute("INSERT INTO items VALUES (1,'saved')"); await a.db.checkpoint();
  await a.db.execute("INSERT INTO items VALUES (2,'not saved')"); await a.db.close();
  const b = await open(a.name); assert.deepEqual(await rows(b.db), [[1, "saved"]]); await b.db.close();
});

test("checkpoint: a new unsaved database stays absent", async () => {
  const a = await open(); await a.db.executeBatch(schema); await a.db.close();
  const b = await open(a.name);
  assert.equal(b.db.snapshotRevision, null);
  assert.deepEqual((await b.db.query("SELECT name FROM sqlite_master WHERE type='table'")).rowArrays, []);
  await b.db.close();
});

test("checkpoint: stale sessions cannot overwrite another session and keep their local work", async () => {
  const a = await open(); await a.db.executeBatch(schema); const first = await a.db.checkpoint();
  const b = await open(a.name); assert.equal(b.db.snapshotRevision, first.revision);
  await a.db.execute("INSERT INTO items VALUES (1,'winner')"); const winner = await a.db.checkpoint();
  await b.db.execute("INSERT INTO items VALUES (2,'local')");
  await assert.rejects(b.db.checkpoint(), code("ERR_FSQLITE_SNAPSHOT_CONFLICT"));
  await assert.rejects(b.db.checkpoint(), code("ERR_FSQLITE_SNAPSHOT_CONFLICT"));
  assert.equal(b.db.snapshotRevision, first.revision);
  assert.deepEqual(await rows(b.db), [[2, "local"]]);
  assert.ok((await b.db.export()).byteLength >= 512);
  const c = await open(a.name); assert.equal(c.db.snapshotRevision, winner.revision);
  assert.deepEqual(await rows(c.db), [[1, "winner"]]);
  await Promise.all([a.db.close(), b.db.close(), c.db.close()]);
});

test("checkpoint: managed transactions reject foreign checkpoint admission", async () => {
  const a = await open(); await a.db.executeBatch(schema);
  await a.db.transaction(async tx => {
    await tx.execute("INSERT INTO items VALUES (1,'inside')");
    await assert.rejects(a.db.checkpoint(), code("ERR_FSQLITE_TRANSACTION_OWNERSHIP"));
  });
  assert.equal(a.worker.requests.filter(r => r.kind === "checkpoint").length, 0);
  await a.db.checkpoint(); await a.db.close();
  const b = await open(a.name); assert.deepEqual(await rows(b.db), [[1, "inside"]]); await b.db.close();
});

test("checkpoint: manual BEGIN is not rolled back or persisted by a failed probe", async () => {
  const a = await open(); await a.db.executeBatch(schema); const saved = await a.db.checkpoint();
  await a.db.executeBatch("BEGIN; INSERT INTO items VALUES (1,'tentative')");
  const start = a.events.length;
  await assert.rejects(a.db.checkpoint(), error => error.code === "ERR_FSQLITE_SNAPSHOT_TRANSACTION" && !!error.cause);
  assert.deepEqual(a.events.slice(start), ["BEGIN"]);
  assert.equal(a.db.snapshotRevision, saved.revision);
  assert.deepEqual(await rows(a.db), [[1, "tentative"]]);
  await a.db.executeBatch("ROLLBACK"); assert.deepEqual(await rows(a.db), []);
  await a.db.checkpoint(); await a.db.close();
});

test("checkpoint: a SAVEPOINT opened via a prepared statement is detected without SQL guessing", async () => {
  const a = await open(); await a.db.executeBatch(schema);
  const statement = await a.db.prepare("SAVEPOINT manual"); await statement.execute();
  await a.db.execute("INSERT INTO items VALUES (1,'pending')");
  await assert.rejects(a.db.checkpoint(), code("ERR_FSQLITE_SNAPSHOT_TRANSACTION"));
  await a.db.executeBatch("RELEASE manual");
  await statement.finalize(); await a.db.checkpoint(); await a.db.close();
  const b = await open(a.name); assert.deepEqual(await rows(b.db), [[1, "pending"]]); await b.db.close();
});

test("checkpoint: quota failure keeps the old revision and all in-memory work for retry", async () => {
  const a = await open(); await a.db.executeBatch(schema); const before = await a.db.checkpoint();
  await a.db.execute("INSERT INTO items VALUES (1,'retry')");
  const original = ModelObjectStore.prototype.put;
  ModelObjectStore.prototype.put = () => { throw new DOMException("quota exhausted", "QuotaExceededError"); };
  try { await assert.rejects(a.db.checkpoint(), /quota exhausted/); }
  finally { ModelObjectStore.prototype.put = original; }
  assert.equal(a.db.snapshotRevision, before.revision); assert.deepEqual(await rows(a.db), [[1, "retry"]]);
  const old = await open(a.name); assert.deepEqual(await rows(old.db), []); await old.db.close();
  const saved = await a.db.checkpoint(); assert.equal(saved.parentRevision, before.revision);
  await a.db.close(); const b = await open(a.name);
  assert.deepEqual(await rows(b.db), [[1, "retry"]]); await b.db.close();
});

test("checkpoint: corrupt saved bytes reject initialization and never create an empty core", async () => {
  const a = await open(); await a.db.executeBatch(schema); await a.db.checkpoint(); await a.db.close();
  const head = state(a.name).values.get("head"); new Uint8Array(head.bytes)[200] ^= 1;
  const f = sqliteSnapshotWorker();
  await assert.rejects(FrankenDB.open({ dbName: a.name, persistence: "indexeddb-snapshot", worker: f.worker }),
    code("ERR_FSQLITE_SNAPSHOT_CORRUPT"));
  assert.deepEqual(f.counts(), { creates: 0, imports: 0 }); assert.equal(f.worker.terminateCount, 1);
  assert.equal(state(a.name).connections.size, 0); assert.equal(state(a.name).values.get("head").revision, head.revision);
});

test("checkpoint: failed core import releases storage without a create fallback", async () => {
  const a = await open(); await a.db.executeBatch(schema); const saved = await a.db.checkpoint(); await a.db.close();
  const f = sqliteSnapshotWorker({ beforeImport() { throw new Error("core import refused"); } });
  await assert.rejects(FrankenDB.open({ dbName: a.name, persistence: "indexeddb-snapshot", worker: f.worker }), /core import refused/);
  assert.deepEqual(f.counts(), { creates: 0, imports: 1 }); assert.equal(state(a.name).connections.size, 0);
  assert.equal(state(a.name).values.get("head").revision, saved.revision);
});

test("checkpoint: an initialization image seeds only an absent snapshot name", async () => {
  const a = await open(); await a.db.executeBatch(schema); await a.db.execute("INSERT INTO items VALUES (1,'imported')");
  const bytes = await a.db.export(); await a.db.close();
  const f = sqliteSnapshotWorker(); const name = crypto.randomUUID();
  const db = await FrankenDB.import(new Uint8Array(bytes), { worker: f.worker, persistence: "indexeddb-snapshot", dbName: name });
  assert.deepEqual(await rows(db), [[1, "imported"]]); await db.checkpoint(); await db.close();
  const g = sqliteSnapshotWorker();
  await assert.rejects(FrankenDB.import(new Uint8Array(bytes), { worker: g.worker, persistence: "indexeddb-snapshot", dbName: name }),
    code("ERR_FSQLITE_SNAPSHOT_EXISTS"));
  const b = await open(name); assert.deepEqual(await rows(b.db), [[1, "imported"]]); await b.db.close();
});

test("checkpoint: failed probe rollback makes the host terminal and releases all handles", async () => {
  let fail = false;
  const a = await open(undefined, { beforeBatch(sql) { if (fail && sql === "ROLLBACK") throw new Error("rollback failed"); } });
  await a.db.executeBatch(schema); const saved = await a.db.checkpoint();
  fail = true;
  await assert.rejects(a.db.checkpoint(), error => error.code === "ERR_FSQLITE_SNAPSHOT_CONNECTION_UNUSABLE" && !!error.cause);
  await assert.rejects(a.db.query("SELECT 1"), code("ERR_FSQLITE_SNAPSHOT_CONNECTION_UNUSABLE"));
  assert.equal(a.db.snapshotRevision, saved.revision); assert.equal(state(a.name).connections.size, 0);
  assert.ok(a.events.includes("free")); await a.db.close();
});

test("checkpoint: queued close waits for export and durable publication", async () => {
  let resume; let entered;
  const started = new Promise(resolve => { entered = resolve; });
  const blocked = new Promise(resolve => { resume = resolve; });
  const a = await open(undefined, { async beforeExport() { entered(); await blocked; } });
  await a.db.executeBatch(schema); await a.db.execute("INSERT INTO items VALUES (1,'saved')");
  const pending = a.db.checkpoint(); await started;
  let closed = false; const closing = a.db.close().then(() => { closed = true; });
  await new Promise(resolve => setImmediate(resolve)); assert.equal(closed, false);
  resume(); const saved = await pending; await closing;
  const b = await open(a.name); assert.equal(b.db.snapshotRevision, saved.revision);
  assert.deepEqual(await rows(b.db), [[1, "saved"]]); await b.db.close();
});

test("checkpoint: ordinary memory mode remains supported and does not claim persistence", async () => {
  const f = sqliteSnapshotWorker(); const db = await FrankenDB.open({ worker: f.worker });
  assert.equal(db.persistence, "memory"); await assert.rejects(db.checkpoint(), code("ERR_FSQLITE_SNAPSHOT_MODE"));
  await db.executeBatch(schema); await db.close();
});

test("checkpoint: 10000-row import and failed bulk trigger effects round-trip correctly", async () => {
  const a = await open();
  await a.db.executeBatch(schema + "; CREATE TABLE audit(id INTEGER); CREATE TRIGGER log_insert AFTER INSERT ON items BEGIN INSERT INTO audit VALUES(new.id); END;");
  const values = Array.from({ length: 10000 }, (_, i) => [i, `row-${i}`]);
  const inserted = await a.db.executeMany("INSERT INTO items VALUES (?, ?)", values);
  assert.equal(inserted.changes, 10000);
  await a.db.checkpoint();
  await assert.rejects(a.db.executeMany("INSERT INTO items VALUES (?, ?)", [[10001, "valid"], [0, "conflict"]]));
  await a.db.checkpoint(); await a.db.close();
  const b = await open(a.name);
  assert.deepEqual((await b.db.query("SELECT COUNT(*), SUM(id) FROM items")).rowArrays, [[10000, 49995000]]);
  assert.deepEqual((await b.db.query("SELECT COUNT(*) FROM audit")).rowArrays, [[10000]]);
  assert.deepEqual((await b.db.query("PRAGMA integrity_check")).rowArrays, [["ok"]]); await b.db.close();
});


test("checkpoint: export failure cannot advance storage or poison an otherwise usable connection", async () => {
  let fail = false;
  const a = await open(undefined, { beforeExport() { if (fail) throw new Error("export failed"); } });
  await a.db.executeBatch(schema); const baseline = await a.db.checkpoint();
  await a.db.execute("INSERT INTO items VALUES (1,'memory')"); fail = true;
  await assert.rejects(a.db.checkpoint(), /export failed/);
  assert.equal(a.db.snapshotRevision, baseline.revision); assert.deepEqual(await rows(a.db), [[1, "memory"]]);
  assert.equal(state(a.name).values.get("head").revision, baseline.revision);
  fail = false; await a.db.checkpoint(); await a.db.close();
});

test("checkpoint: queued SQL cannot modify the exported image before publication", async () => {
  let resume; let entered;
  const started = new Promise(resolve => { entered = resolve; });
  const blocked = new Promise(resolve => { resume = resolve; });
  const a = await open(undefined, { async beforeExport() { entered(); await blocked; } });
  await a.db.executeBatch(schema); await a.db.execute("INSERT INTO items VALUES (1,'first')");
  const pending = a.db.checkpoint(); await started;
  const later = a.db.execute("INSERT INTO items VALUES (2,'later')");
  resume(); await pending; await later;
  assert.deepEqual(await rows(a.db), [[1, "first"], [2, "later"]]);
  const b = await open(a.name); assert.deepEqual(await rows(b.db), [[1, "first"]]);
  await Promise.all([a.db.close(), b.db.close()]);
});

test("checkpoint: eviction/recreation cannot turn an old revision into a matching token", async () => {
  const a = await open(); await a.db.executeBatch(schema); const baseline = await a.db.checkpoint();
  // Storage eviction is modeled as removing the published head, not as a
  // browser durability receipt. A stale owner must not recreate it silently.
  state(a.name).values.clear();
  await assert.rejects(a.db.checkpoint(), code("ERR_FSQLITE_SNAPSHOT_CONFLICT"));
  assert.equal(a.db.snapshotRevision, baseline.revision);
  const b = await open(a.name); await b.db.executeBatch(schema);
  const fresh = await b.db.checkpoint(); assert.notEqual(fresh.revision, baseline.revision);
  await assert.rejects(a.db.checkpoint(), code("ERR_FSQLITE_SNAPSHOT_CONFLICT"));
  await Promise.all([a.db.close(), b.db.close()]);
});
