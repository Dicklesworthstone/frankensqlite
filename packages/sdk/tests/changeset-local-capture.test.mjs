// Production journal/capture SQL over real Node SQLite. Not a Rust/WASM or
// browser/power-loss certificate. Retain test-owned files for inspection.
import assert from "node:assert/strict";
import { test } from "node:test";
import { DatabaseSync } from "node:sqlite";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createHash } from "node:crypto";
import {
  ChangesetRebaseJournal as Journal,
  REBASE_JOURNAL_LOCALS_TABLE as LOCAL,
  REBASE_JOURNAL_ENTRIES_TABLE as ENTRIES,
} from "../src/changeset-rebase-journal.ts";
import { decodeChangeset, encodeChangeset } from "../src/changeset-codec.ts";

const table = `main."${LOCAL}"`;
const opts = { tables: ["t"] };
const sha = bytes => createHash("sha256").update(bytes).digest("hex");
const code = kind => ({ code: `ERR_FSQLITE_REBASE_JOURNAL_${kind}` });
function fixture(t, path = ":memory:", schema = "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT);", limits = {}) {
  const db = new DatabaseSync(path);
  db.exec("PRAGMA recursive_triggers=ON; PRAGMA foreign_keys=ON;");
  if (schema) db.exec(schema);
  let closed = false, depth = 0, next = 0;
  const cleanup = [];
  const control = { beforeCommit: null, afterCommit: null, afterExecute: null };
  const tx = {
    async execute(sql, params = []) {
      const changed = Number(db.prepare(sql).run(...params).changes);
      await control.afterExecute?.(sql);
      return changed;
    },
    async query(sql, params = []) {
      const stmt = db.prepare(sql); stmt.setReadBigInts(true);
      return { rowArrays: stmt.all(...params).map(row => Object.values(row)) };
    },
  };
  const target = {
    async transaction(work) {
      const nested = depth !== 0, name = `test_owned_${++next}`;
      db.exec(nested ? `SAVEPOINT ${name}` : "BEGIN"); depth++;
      let committed = false;
      try {
        const result = await work(tx);
        await control.beforeCommit?.();
        db.exec(nested ? `RELEASE ${name}` : "COMMIT"); committed = true;
        await control.afterCommit?.();
        return result;
      } catch (cause) {
        if (!committed) db.exec(nested ? `ROLLBACK TO ${name}; RELEASE ${name}` : "ROLLBACK");
        throw cause;
      } finally { depth--; }
    },
  };
  const journal = new Journal(target, { journalId: "local-device", ...limits });
  const close = () => { if (!closed) { for (const free of cleanup.splice(0).reverse()) free(); db.close(); closed = true; } };
  t.after(close);
  return { db, target, tx, journal, control, close, cleanup };
}
const readRows = f => f.db.prepare("SELECT * FROM t ORDER BY id").all().map(row => Object.values(row));
const normal = bytes => decodeChangeset(bytes).map(t => ({ ...t, changes: [...t.changes].sort((a,b) => {
  const left = a.operation === "insert" ? a.new[0] : a.old[0];
  const right = b.operation === "insert" ? b.new[0] : b.old[0];
  return left < right ? -1 : left > right ? 1 : 0;
}) }));
async function remote(f, id = "remote:1", key = 9n) {
  return f.journal.apply(encodeChangeset([{ name: "t", primaryKey: [1,0], changes: [{ operation: "insert", indirect: false, new: [key, "remote"] }] }]), { tables: ["t"], deliveryId: id });
}

test("captures SQL, original bytes and current verified basis in one transaction", async t => {
  const f = fixture(t); await remote(f);
  const basis = await f.journal.bookmark();
  const result = await f.journal.captureLocal("op:1", async tx => {
    await tx.execute("INSERT INTO t VALUES (?,?)", [1n,"local"]); return 42;
  }, opts);
  assert.equal(result.replayed, false); assert.equal(result.value, 42);
  assert.deepEqual(result.record.basis, basis);
  assert.equal(result.record.changes, 1); assert.equal(result.record.touchedRows, 1);
  assert.equal(result.record.sha256, sha(result.record.changeset));
  assert.equal(result.record.byteLength, result.record.changeset.length);
  assert.equal(decodeChangeset(result.record.changeset)[0].changes[0].new[1], "local");
  assert.deepEqual(await f.journal.bookmark(), basis);
  assert.deepEqual(await f.journal.readLocal("op:1"), result.record);
  assert.ok(Object.isFrozen(result)); assert.ok(Object.isFrozen(result.record));
  assert.ok(Object.isFrozen(result.record.basis));
  assert.deepEqual(f.db.prepare("SELECT name FROM temp.sqlite_schema WHERE name GLOB '__fsqlite_capture_*'").all(), []);
});

test("lost commit ACK plus file reopen returns the original without replaying work", async t => {
  const file = join(mkdtempSync(join(tmpdir(), "fsqlite-local-")), "source.db");
  const f = fixture(t, file); await remote(f);
  let calls = 0;
  f.control.afterCommit = () => { throw new Error("lost ACK after real COMMIT"); };
  await assert.rejects(f.journal.captureLocal("op:lost", async tx => {
    calls++; await tx.execute("INSERT INTO t VALUES (1,'saved')"); return "unpersisted result";
  }, opts), /lost ACK/);
  f.close();
  const next = fixture(t, file, "");
  await remote(next, "remote:2", 10n);
  const replay = await next.journal.captureLocal("op:lost", () => { calls++; throw new Error("replayed SQL"); }, opts);
  assert.equal(calls, 1); assert.equal(replay.replayed, true);
  assert.equal(Object.hasOwn(replay, "value"), false);
  assert.equal(replay.record.basis.position, 1);
  assert.deepEqual(readRows(next), [[1,"saved"],[9,"remote"],[10,"remote"]]);
  assert.equal(next.db.prepare("PRAGMA integrity_check").get().integrity_check, "ok");
});

test("callback failure rolls back rows, local schema and capture TEMP state", async t => {
  const f = fixture(t);
  await assert.rejects(f.journal.captureLocal("failed", async tx => {
    await tx.execute("INSERT INTO t VALUES (1,'x')"); throw new Error("user failure");
  }, opts), /user failure/);
  assert.deepEqual(readRows(f), []);
  assert.equal(await f.journal.readLocal("failed"), null);
  assert.equal(f.db.prepare("SELECT count(*) n FROM temp.sqlite_schema").get().n, 0);
  assert.equal(f.db.prepare("SELECT count(*) n FROM sqlite_schema WHERE name=?").get(LOCAL).n, 0);
});

test("outer rollback removes provisional local rows and record", async t => {
  const f = fixture(t); let provisional;
  await assert.rejects(f.target.transaction(async () => {
    provisional = await f.journal.captureLocal("nested", tx => tx.execute("INSERT INTO t VALUES (1,'x')"), opts);
    assert.ok(await f.journal.readLocal("nested")); throw new Error("outer rollback");
  }), /outer rollback/);
  assert.equal(provisional.replayed, false);
  assert.deepEqual(readRows(f), []); assert.equal(await f.journal.readLocal("nested"), null);
});

test("real deferred foreign-key COMMIT failure rolls back local record and rows", async t => {
  const f = fixture(t, ":memory:", "CREATE TABLE parent(id INTEGER PRIMARY KEY); CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT,p INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED);");
  await assert.rejects(f.journal.captureLocal("fk", tx => tx.execute("INSERT INTO t VALUES (1,'x',99)"), opts), /FOREIGN KEY/);
  assert.deepEqual(readRows(f), []); assert.equal(await f.journal.readLocal("fk"), null);
});

for (const stage of ["before", "callback", "record"]) test(`cancellation at ${stage} leaves no acknowledged partial work`, async t => {
  const f = fixture(t); const ac = new AbortController();
  if (stage === "before") ac.abort();
  if (stage === "record") f.control.afterExecute = sql => { if (sql.startsWith(`INSERT OR ABORT INTO ${table}`)) ac.abort(); };
  await assert.rejects(f.journal.captureLocal("cancel", async tx => {
    await tx.execute("INSERT INTO t VALUES (1,'x')"); if (stage === "callback") ac.abort();
  }, { ...opts, signal: ac.signal }), error => /CANCELLED/.test(error.code));
  assert.deepEqual(readRows(f), []); assert.equal(await f.journal.readLocal("cancel"), null);
});

test("cancellation after real COMMIT does not relabel success as rollback", async t => {
  const f = fixture(t); const ac = new AbortController(); f.control.afterCommit = () => ac.abort();
  const result = await f.journal.captureLocal("committed", tx => tx.execute("INSERT INTO t VALUES (1,'x')"), { ...opts, signal: ac.signal });
  assert.equal(result.replayed, false); assert.deepEqual(readRows(f), [[1,"x"]]);
});

test("empty and reverted work retain IDs and consume entry capacity", async t => {
  const f = fixture(t, ":memory:", undefined, { maxLocalEntries: 1 });
  const empty = await f.journal.captureLocal("empty", async tx => {
    await tx.execute("INSERT INTO t VALUES (1,'temporary')"); await tx.execute("DELETE FROM t WHERE id=1");
  }, opts);
  assert.equal(empty.record.changeset.length, 0); assert.equal(empty.record.changes, 0);
  assert.equal(empty.record.touchedRows, 1);
  assert.equal(f.db.prepare(`SELECT typeof(changeset) t,length(changeset) n FROM ${table}`).get().t, "blob");
  let ran = false;
  await assert.rejects(f.journal.captureLocal("second", () => { ran = true; }, opts), code("LIMIT"));
  assert.equal(ran, false);
  assert.equal((await f.journal.captureLocal("empty", () => { throw new Error("replay"); }, opts)).replayed, true);
});

test("byte-limit failure after capture rolls back SQL without discarding prior records", async t => {
  const f = fixture(t); const first = await f.journal.captureLocal("one", tx => tx.execute("INSERT INTO t VALUES (1,'a')"), opts);
  const bounded = new Journal(f.target, { journalId: "local-device", maxLocalBytes: first.record.byteLength });
  await assert.rejects(bounded.captureLocal("two", tx => tx.execute("INSERT INTO t VALUES (2,'b')"), opts), code("LIMIT"));
  assert.deepEqual(readRows(f), [[1,"a"]]); assert.deepEqual(await bounded.readLocal("one"), first.record);
});

test("per-message codec limit rolls back local SQL", async t => {
  const f = fixture(t, ":memory:", undefined, { limits: { maxBytes: 10 } });
  await assert.rejects(f.journal.captureLocal("small", tx => tx.execute("INSERT INTO t VALUES (1,'oversize')"), opts), code("LIMIT"));
  assert.deepEqual(readRows(f), []);
});

test("capture scope is bound to ID but option table order/case may vary", async t => {
  const f = fixture(t, ":memory:", "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT); CREATE TABLE u(id INTEGER PRIMARY KEY,v TEXT);");
  await f.journal.captureLocal("id", tx => tx.execute("INSERT INTO t VALUES (1,'a')"), { tables: ["t","u"] });
  assert.equal((await f.journal.captureLocal("id", () => assert.fail("replay"), { tables: ["U","T"] })).replayed, true);
  await assert.rejects(f.journal.captureLocal("id", () => assert.fail("replay"), opts), code("HISTORY"));
  await assert.rejects(f.journal.captureLocal("id", () => assert.fail("replay"), { tables: ["t","u"], indirect: true }), code("HISTORY"));
});

test("input options are captured before asynchronous admission", async t => {
  const f = fixture(t); let accesses = 0;
  const options = { get tables() { accesses++; return ["t"]; }, indirect: false };
  const pending = f.journal.captureLocal("owned", tx => tx.execute("INSERT INTO t VALUES (1,'a')"), options);
  options.indirect = true;
  const result = await pending; assert.equal(accesses, 1);
  assert.equal(decodeChangeset(result.record.changeset)[0].changes[0].indirect, false);
});

test("read returns detached ownership, and retains original after app schema changes", async t => {
  const f = fixture(t);
  const result = await f.journal.captureLocal("copy", tx => tx.execute("INSERT INTO t VALUES (1,'a')"), opts);
  const exact = new Uint8Array(result.record.changeset); result.record.changeset.fill(255);
  f.db.exec("ALTER TABLE t ADD COLUMN extra TEXT");
  assert.deepEqual((await f.journal.readLocal("copy")).changeset, exact);
  assert.equal((await f.journal.captureLocal("copy", () => assert.fail("replay"), opts)).replayed, true);
});

test("read absence is read-only; journal namespaces and quoted IDs are distinct", async t => {
  const f = fixture(t); assert.equal(await f.journal.readLocal("missing"), null);
  assert.equal(f.db.prepare("SELECT count(*) n FROM sqlite_schema WHERE name=?").get(LOCAL).n, 0);
  const id = "'🦆:operation";
  await f.journal.captureLocal(id, () => undefined, opts);
  const other = new Journal(f.target, { journalId: "other" });
  assert.equal(await other.readLocal(id), null);
  assert.equal((await other.captureLocal(id, () => undefined, opts)).replayed, false);
});

for (const field of ["scope_sha256","basis_position","basis_sha256","sha256","record_sha256","byte_length","change_count","touched_rows","changeset"]) {
  test(`corrupt ${field} refuses read and replay, retaining corrupt evidence`, async t => {
    const f = fixture(t); await f.journal.captureLocal("corrupt", tx => tx.execute("INSERT INTO t VALUES (1,'a')"), opts);
    f.db.prepare(`UPDATE ${table} SET ${field}=? WHERE operation_id='corrupt'`).run(new Uint8Array(1024*1024));
    await assert.rejects(f.journal.readLocal("corrupt"));
    await assert.rejects(f.journal.captureLocal("corrupt", () => assert.fail("reran corrupt work"), opts));
    assert.equal(f.db.prepare(`SELECT length(${field}) n FROM ${table}`).get().n, 1024*1024);
    assert.deepEqual(readRows(f), [[1,"a"]]);
  });
}

for (const namespace of ["main","temp"]) test(`${namespace} metadata trigger rejects before local SQL`, async t => {
  const f = fixture(t); await f.journal.captureLocal("seed", () => undefined, opts);
  f.db.exec(`CREATE ${namespace === "temp" ? "TEMP" : ""} TRIGGER interfere AFTER INSERT ON ${table} BEGIN SELECT 1; END`);
  await assert.rejects(f.journal.captureLocal("blocked", () => assert.fail("executed"), opts), code("SCHEMA"));
  assert.deepEqual(readRows(f), []);
});

test("changed remote history during the local callback rolls back all changes", async t => {
  const f = fixture(t); await remote(f); const before = await f.journal.bookmark();
  await assert.rejects(f.journal.captureLocal("rewrite", async tx => {
    await tx.execute("INSERT INTO t VALUES (1,'a')");
    await tx.execute(`UPDATE main."${ENTRIES}" SET sha256=?`, ["0".repeat(64)]);
  }, opts), code("CORRUPT"));
  assert.deepEqual(readRows(f), [[9,"remote"]]); assert.deepEqual(await f.journal.bookmark(), before);
});

for (let seed=1; seed<=20; seed++) test(`native SQLite session comparison for captured original workload ${seed}`, async t => {
  const f = fixture(t);
  f.db.exec("INSERT INTO t VALUES (1,'base'),(2,'remove')");
  const session = f.db.createSession({ table: "t" }); f.cleanup.push(() => session.close());
  const result = await f.journal.captureLocal(`seed:${seed}`, async tx => {
    await tx.execute("UPDATE t SET v=? WHERE id=1", [`local-${seed}`]);
    await tx.execute("DELETE FROM t WHERE id=2");
    for (let k=3;k<6;k++) await tx.execute("INSERT INTO t VALUES (?,?)", [BigInt(k), `s${seed}:${k}`]);
    if (seed%2) await tx.execute("UPDATE t SET v='second' WHERE id=4");
    if (seed%3) await tx.execute("DELETE FROM t WHERE id=5");
  }, opts);
  assert.deepEqual(normal(result.record.changeset), normal(new Uint8Array(session.changeset())));
});
