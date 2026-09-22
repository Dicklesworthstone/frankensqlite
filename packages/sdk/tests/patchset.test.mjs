// Production codec/application with SQLite session.patchset() as an independent
// wire and SQL oracle. Not FrankenSQLite WASM or browser durability evidence.
import assert from "node:assert/strict";
import { constants, DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  ChangesetError, decodeChangeset, decodePatchset, encodeChangeset,
  encodePatchset, invertChangeset,
} from "../src/changeset-codec.ts";
import {
  applyChangeset, applyPatchset, CHANGESET_RECEIPTS_TABLE,
} from "../src/changeset-apply.ts";

class Target {
  log = [];
  scopes = 0;
  afterExecute;
  constructor(sql = "", path = ":memory:") {
    this.db = new DatabaseSync(path);
    this.db.exec(sql);
  }
  async query(sql, params = []) {
    this.log.push(sql);
    const s = this.db.prepare(sql);
    s.setReadBigInts(true);
    const names = s.columns().map((c) => c.name);
    return { rowArrays: s.all(...params).map((row) => names.map((name) => row[name])) };
  }
  async execute(sql, params = []) {
    this.log.push(sql);
    const n = Number(this.db.prepare(sql).run(...params).changes);
    await this.afterExecute?.(sql);
    return n;
  }
  async transaction(work, options = {}) {
    const savepoint = `patchset_${++this.scopes}`;
    this.db.exec(`SAVEPOINT ${savepoint}`);
    try {
      const result = await work(this);
      if (options.signal?.aborted) throw options.signal.reason;
      this.db.exec(`RELEASE ${savepoint}`);
      return result;
    } catch (cause) {
      this.db.exec(`ROLLBACK TO ${savepoint}; RELEASE ${savepoint}`);
      throw cause;
    }
  }
  rows(sql = "SELECT * FROM t ORDER BY 1") {
    const s = this.db.prepare(sql);
    s.setReadBigInts(true);
    return s.all().map((row) => Object.values(row));
  }
}
function target(t, sql) {
  const value = new Target(sql);
  t.after(() => value.db.close());
  return value;
}
const insert = (...values) => ({ operation: "insert", indirect: false, new: values });
const remove = (...keys) => ({ operation: "delete", indirect: false, old: keys });
const update = (keys, next) => ({ operation: "update", indirect: false, old: keys, new: next });
const table = (changes, primaryKey = [1, 0], name = "t") => ({ name, primaryKey, changes });
const wire = (changes) => encodePatchset([table(changes)]);
const policy = { tables: ["t"] };
const inputError = { code: "ERR_FSQLITE_CHANGESET_INPUT" };
const formatError = { code: "ERR_FSQLITE_CHANGESET_FORMAT" };
const limitError = { code: "ERR_FSQLITE_CHANGESET_LIMIT" };
const conflictError = { code: "ERR_FSQLITE_CHANGESET_CONFLICT" };
const assertNoInbox = (actual) => assert.equal(actual.db.prepare(
  "SELECT count(*) AS n FROM sqlite_schema WHERE name=?",
).get(CHANGESET_RECEIPTS_TABLE).n, 0);

for (const composite of [false, true]) {
  test(`native patchset exact bytes and SQL results, composite=${composite}`, async (t) => {
    const schema = composite
      ? "CREATE TABLE t(b TEXT,a INTEGER,v,local,PRIMARY KEY(a,b)) WITHOUT ROWID;"
      : "CREATE TABLE t(b TEXT,a INTEGER PRIMARY KEY,v,local);";
    const initial = "INSERT INTO t VALUES('x',1,'old','keep'),('y',2,'gone','keep');";
    const author = target(t, schema + initial), actual = target(t, schema + initial), oracle = target(t, schema + initial);
    const s = author.db.createSession();
    author.db.exec("UPDATE t SET v=NULL WHERE a=1; DELETE FROM t WHERE a=2; INSERT INTO t VALUES('z',3,x'00ff','new')");
    const bytes = s.patchset(), full = s.changeset();
    s.close();
    const decoded = decodePatchset(bytes);
    assert.deepEqual(encodePatchset(decoded), bytes);
    assert.ok(bytes.length < full.length);
    assert.deepEqual(decoded[0].primaryKey, composite ? [2, 1, 0, 0] : [0, 1, 0, 0]);
    for (const change of decoded[0].changes) {
      assert.ok(Object.isFrozen(change));
      if (change.operation !== "insert")
        assert.ok(change.old.every((value, i) => decoded[0].primaryKey[i] !== 0 || value === undefined));
    }
    assert.equal(oracle.db.applyChangeset(bytes), true);
    assert.deepEqual(await applyPatchset(actual, bytes, policy), { applied: 3, omitted: 0, replayed: false });
    assert.deepEqual(actual.rows(), oracle.rows());
    assert.deepEqual(actual.rows(), author.rows());
  });
}

test("patchset updates ignore absent before-images without overriding untouched local columns", async (t) => {
  const schema = "CREATE TABLE t(id PRIMARY KEY,v,local);";
  const author = target(t, schema + "INSERT INTO t VALUES(1,'old','sender'),(2,'gone','sender')");
  const actual = target(t, schema + "INSERT INTO t VALUES(1,'diverged','receiver'),(2,'diverged','receiver')");
  const oracle = target(t, schema + "INSERT INTO t VALUES(1,'diverged','receiver'),(2,'diverged','receiver')");
  const s = author.db.createSession();
  author.db.exec("UPDATE t SET v='new' WHERE id=1; DELETE FROM t WHERE id=2");
  const bytes = s.patchset(), full = s.changeset(); s.close();
  await assert.rejects(applyChangeset(actual, full, policy), conflictError);
  assert.equal(oracle.db.applyChangeset(bytes, { onConflict() { assert.fail("no DATA in patchset"); } }), true);
  await applyPatchset(actual, bytes, { ...policy, onConflict() { assert.fail("no invented DATA conflict"); } });
  assert.deepEqual(actual.rows(), [[1n, "new", "receiver"]]);
  assert.deepEqual(actual.rows(), oracle.rows());
});

for (const value of [null, -(1n << 63n), (1n << 63n) - 1n, 1, 1.5, Infinity, -Infinity, "a\0世界", "\uFEFF", Uint8Array.of(0, 255), new Uint8Array()]) {
  test(`authored patchset roundtrip and native storage ${String(value)}`, async (t) => {
    const schema = "CREATE TABLE t(id INTEGER PRIMARY KEY,v);";
    const actual = target(t, schema), oracle = target(t, schema);
    const bytes = wire([insert(1n, value)]);
    assert.deepEqual(encodePatchset(decodePatchset(bytes)), bytes);
    assert.equal(oracle.db.applyChangeset(bytes), true);
    await applyPatchset(actual, bytes, policy);
    assert.deepEqual(actual.rows("SELECT id,typeof(v),CAST(v AS BLOB) FROM t"), oracle.rows("SELECT id,typeof(v),CAST(v AS BLOB) FROM t"));
  });
}

test("NULL differs from an omitted update field and key changes remain delete/insert", async (t) => {
  const actual = target(t, "CREATE TABLE t(id PRIMARY KEY,v,local); INSERT INTO t VALUES(1,'old','keep')");
  const bytes = encodePatchset([table([
    update([1n, undefined, undefined], [undefined, null, undefined]),
    remove(1n, undefined, undefined), insert(2n, "new", "new"),
  ], [1, 0, 0])]);
  await applyPatchset(actual, bytes, policy);
  assert.deepEqual(actual.rows(), [[2n, "new", "new"]]);
});

test("indirect flags, UTF-8 names, quotes and reordered composite key bytes survive", async (t) => {
  const name = '引号"🚀';
  const quoted = `"${name.replaceAll('"', '""')}"`;
  const actual = target(t, `CREATE TABLE ${quoted}(a TEXT,b INTEGER,v,PRIMARY KEY(b,a)) WITHOUT ROWID`);
  const tables = [table([{ ...insert("key", 42n, "\uFEFF"), indirect: true }], [2, 1, 0], name)];
  const bytes = encodePatchset(tables);
  assert.deepEqual(decodePatchset(bytes), tables);
  assert.equal(actual.db.applyChangeset(bytes), true);
  assert.deepEqual(actual.rows(`SELECT * FROM ${quoted}`), [["key", 42n, "\uFEFF"]]);
});

test("empty format is valid without SQL changes; optional receipt still records delivery", async (t) => {
  const actual = target(t, "");
  assert.deepEqual(decodePatchset(new Uint8Array()), []);
  assert.deepEqual(encodePatchset([]), new Uint8Array());
  assert.deepEqual(await applyPatchset(actual, new Uint8Array(), { tables: [] }), { applied: 0, omitted: 0, replayed: false });
  assertNoInbox(actual);
  await applyPatchset(actual, new Uint8Array(), { tables: [], deliveryId: "empty" });
  assert.equal((await applyPatchset(actual, new Uint8Array(), { tables: [], deliveryId: "empty" })).replayed, true);
});

test("explicit APIs reject format substitution, mixed streams, and patchset inversion before SQL", async (t) => {
  const actual = target(t, "CREATE TABLE t(id PRIMARY KEY,v)");
  const compact = wire([insert(1n, "value")]);
  const full = encodeChangeset([table([insert(1n, "value")])]);
  assert.throws(() => decodeChangeset(compact), formatError);
  assert.throws(() => decodePatchset(full), formatError);
  assert.throws(() => invertChangeset(compact), formatError);
  for (const mixed of [new Uint8Array([...full, ...compact]), new Uint8Array([...compact, ...full])]) {
    assert.throws(() => decodePatchset(mixed), formatError);
    assert.throws(() => decodeChangeset(mixed), formatError);
  }
  await assert.rejects(applyChangeset(actual, compact, policy), formatError);
  await assert.rejects(applyPatchset(actual, full, policy), formatError);
  assert.equal(actual.scopes, 0);
  assert.deepEqual(actual.log, []);
});

test("encoding never silently discards full before-images, key edits, or incomplete records", () => {
  for (const change of [
    remove(1n, "old"), update([1n, "old"], [undefined, "new"]),
    update([1n, undefined], [2n, "new"]),
    update([1n, undefined], [undefined, undefined]),
    insert(null, "bad"), insert(1n, undefined), remove(null, undefined),
    update([undefined, undefined], [undefined, "new"]),
    remove(1n),
  ]) assert.throws(() => wire([change]), ChangesetError);
});

for (const operation of ["insert", "update", "delete"]) {
  test(`every truncated single ${operation} record fails closed except empty input`, () => {
    const changes = operation === "insert" ? [insert(1n, "long value")]
      : operation === "update" ? [update([1n, undefined], [undefined, "long value"])]
      : [remove(1n, undefined)];
    const bytes = wire(changes);
    for (let n = 1; n < bytes.length; n++) assert.throws(() => decodePatchset(bytes.slice(0, n)), ChangesetError, `cut=${n}`);
  });
}

test("sparse DELETE and UPDATE charge their full normalized slot count before allocation", () => {
  const width = 1200, pk = Array(width).fill(0); pk[width - 1] = 1;
  const keys = Array(width).fill(undefined); keys[width - 1] = 7n;
  const change = remove(...keys);
  const bytes = encodePatchset([table([change], pk)]);
  assert.throws(() => decodePatchset(bytes, { maxCells: width - 1 }), limitError);
  assert.throws(() => encodePatchset([table([change], pk)], { maxCells: width - 1 }), limitError);
  assert.equal(decodePatchset(bytes, { maxCells: width })[0].changes[0].old.length, width);
  const next = Array(width).fill(undefined); next[0] = "modified";
  const updating = encodePatchset([table([update(keys, next)], pk)]);
  assert.throws(() => decodePatchset(updating, { maxCells: 2 * width - 1 }), limitError);
  assert.equal(decodePatchset(updating, { maxCells: 2 * width })[0].changes[0].new[0], "modified");
});

test("all codec bounds still apply in both patchset directions", () => {
  const tables = [table([insert(1n, "a"), insert(2n, "b")]), table([insert(3n, "c")], [1, 0], "u")];
  const bytes = encodePatchset(tables);
  for (const limits of [{ maxBytes: 8 }, { maxTables: 1 }, { maxColumns: 1 }, { maxChanges: 2 }, { maxCells: 5 }]) {
    assert.throws(() => decodePatchset(bytes, limits), limitError);
    assert.throws(() => encodePatchset(tables, limits), ChangesetError);
  }
  for (const limits of [{ maxBytes: 0 }, { maxTables: Infinity }, { maxCells: 1.5 }])
    assert.throws(() => decodePatchset(bytes, limits), inputError);
});

test("corrupt headers, masks, tags, flags, UTF-8 and mixed table identities reject", () => {
  for (const bytes of [
    [80], [80, 0], [80, 2, 0, 0, 116, 0], [80, 2, 1, 0, 255, 0],
    [80, 128, 2], [80, 2, 1, 0, 116, 0, 18, 2],
    [80, 2, 1, 0, 116, 0, 18, 0, 6, 5],
    [80, 2, 1, 0, 116, 0, 9, 0, 5],
    [80, 2, 1, 0, 116, 0, 23, 0, 5, 5],
  ]) assert.throws(() => decodePatchset(Uint8Array.from(bytes)), ChangesetError);
  assert.throws(() => encodePatchset([table([insert(1n, "x")]), table([insert(2n, "y")], [1, 0], "T")]), formatError);
});

test("decoding uses intrinsic typed-array slots and owns blob slices", () => {
  const bytes = wire([insert(1n, Uint8Array.of(1, 2, 3))]);
  class Hostile extends Uint8Array {
    get byteLength() { throw Error("shadow length"); }
    get buffer() { throw Error("shadow buffer"); }
    [Symbol.iterator]() { throw Error("iterator"); }
  }
  const backing = new Uint8Array(bytes.length + 32); backing.set(bytes, 7);
  const hostile = new Hostile(backing.buffer, 7, bytes.length);
  const decoded = decodePatchset(hostile); backing.fill(0);
  assert.deepEqual(decoded[0].changes[0].new[1], Uint8Array.of(1, 2, 3));
  assert.equal(decoded[0].changes[0].new[1].buffer.byteLength, 3);
  for (const value of [new Uint8Array(new SharedArrayBuffer(0)), new Uint8Array(new ArrayBuffer(1, { maxByteLength: 4 }))])
    assert.throws(() => decodePatchset(value), ChangesetError);
  const detached = new Uint8Array(0); structuredClone(detached.buffer, { transfer: [detached.buffer] });
  assert.throws(() => decodePatchset(detached), ChangesetError);
});

test("patchset input and allowlist are captured before asynchronous transaction admission", async (t) => {
  const actual = target(t, "CREATE TABLE t(id PRIMARY KEY,v)");
  const bytes = wire([insert(1n, Uint8Array.of(0, 255))]), tables = ["t"];
  const pending = applyPatchset(actual, bytes, { tables });
  bytes.fill(0); tables.length = 0;
  await pending;
  assert.deepEqual(actual.rows(), [[1n, Uint8Array.of(0, 255)]]);
});

test("missing and duplicate keys abort or omit explicitly; replacement retains strict uniqueness", async (t) => {
  const actual = target(t, "CREATE TABLE t(id PRIMARY KEY,v UNIQUE ON CONFLICT REPLACE); INSERT INTO t VALUES(1,'local'),(2,'occupied')");
  const bytes = wire([insert(1n, "remote"), remove(99n, undefined), update([98n, undefined], [undefined, "missing"])]);
  await assert.rejects(applyPatchset(actual, bytes, policy), conflictError);
  const kinds = [];
  assert.deepEqual(await applyPatchset(actual, bytes, { ...policy, onConflict(c) { kinds.push(c.kind); return "omit"; } }), { applied: 0, omitted: 3, replayed: false });
  assert.deepEqual(kinds, ["conflict", "not-found", "not-found"]);
  await applyPatchset(actual, wire([insert(1n, "remote")]), { ...policy, onConflict: () => "replace" });
  await assert.rejects(applyPatchset(actual, wire([insert(1n, "occupied")]), { ...policy, onConflict: () => "replace" }), /UNIQUE constraint/);
  await assert.rejects(applyPatchset(actual, wire([remove(99n, undefined)]), { ...policy, onConflict: () => "replace" }), inputError);
  assert.deepEqual(actual.rows(), [[1n, "remote"], [2n, "occupied"]]);
});

test("constraint failure, deferred commit failure and ignored write roll back patchset plus receipt", async (t) => {
  const actual = target(t, "PRAGMA foreign_keys=ON; CREATE TABLE parent(id PRIMARY KEY); INSERT INTO parent VALUES(1); CREATE TABLE t(id PRIMARY KEY,v REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)");
  await assert.rejects(applyPatchset(actual, wire([insert(1n, 1n), insert(2n, 2n)]), { ...policy, deliveryId: "deferred" }), /FOREIGN KEY constraint/);
  assert.deepEqual(actual.rows(), []); assertNoInbox(actual);
  actual.db.exec("CREATE TRIGGER ignore_row BEFORE INSERT ON t WHEN new.id=2 BEGIN SELECT RAISE(IGNORE); END");
  await assert.rejects(applyPatchset(actual, wire([insert(1n, 1n), insert(2n, 1n)]), { ...policy, deliveryId: "ignored" }), { code: "ERR_FSQLITE_CHANGESET_RESULT" });
  assert.deepEqual(actual.rows(), []); assertNoInbox(actual);
});

test("cancellation after patchset write rolls back every row and receipt", async (t) => {
  const actual = target(t, "CREATE TABLE t(id PRIMARY KEY,v)");
  const abort = new AbortController();
  actual.afterExecute = (sql) => { if (sql.startsWith('INSERT OR ABORT INTO main."t"')) abort.abort("stop"); };
  await assert.rejects(applyPatchset(actual, wire([insert(1n, "a"), insert(2n, "b")]), {
    ...policy, deliveryId: "cancel", signal: abort.signal,
  }), { code: "ERR_FSQLITE_CHANGESET_CANCELLED" });
  assert.deepEqual(actual.rows(), []); assertNoInbox(actual);
});

test("before-image-free updates preserve trailing columns and main cannot be shadowed by temp", async (t) => {
  const actual = target(t, "CREATE TABLE t(id TEXT PRIMARY KEY COLLATE NOCASE,v,extra DEFAULT 'default'); CREATE TEMP TABLE t(id PRIMARY KEY,v); INSERT INTO main.t VALUES('a','local','keep'); INSERT INTO temp.t VALUES('A','temp')");
  await applyPatchset(actual, wire([update(["A", undefined], [undefined, "new"]), insert("B", "added")]), policy);
  assert.deepEqual(actual.rows("SELECT * FROM main.t ORDER BY id"), [["a", "new", "keep"], ["B", "added", "default"]]);
  assert.deepEqual(actual.rows("SELECT * FROM temp.t"), [["A", "temp"]]);
});

test("all table schemas are preflighted and unauthorized targets never begin SQL", async (t) => {
  const actual = target(t, "CREATE TABLE t(id PRIMARY KEY,v)");
  const bytes = encodePatchset([table([insert(1n, "first")]), table([insert(2n, "bad")], [1, 0], "missing")]);
  await assert.rejects(applyPatchset(actual, bytes, policy), inputError);
  assert.equal(actual.scopes, 0);
  await assert.rejects(applyPatchset(actual, bytes, { tables: ["t", "missing"] }), { code: "ERR_FSQLITE_CHANGESET_SCHEMA" });
  assert.ok(!actual.log.some((sql) => /^(INSERT|UPDATE|DELETE)/.test(sql)));
});

test("patchset receipt binds exact format bytes and does not repeat writes after lost ACK/reopen", async () => {
  const path = join(mkdtempSync(join(tmpdir(), "fsqlite-patchset-")), "receiver.db");
  const actual = new Target("CREATE TABLE t(id PRIMARY KEY,v); CREATE TABLE audit(id); CREATE TRIGGER seen AFTER INSERT ON t BEGIN INSERT INTO audit VALUES(new.id); END", path);
  const bytes = wire([insert(1n, "new")]);
  const opts = { ...policy, deliveryId: "source:patch:1" };
  try {
    await assert.rejects(applyPatchset({ async transaction(work, settings) {
      await actual.transaction(work, settings); throw Error("lost ACK");
    } }, bytes, opts), /lost ACK/);
  } finally { actual.db.close(); }
  const reopened = new Target("", path);
  try {
    assert.deepEqual(await applyPatchset(reopened, bytes, { ...opts, onConflict() { assert.fail("no replay callback"); } }), { applied: 1, omitted: 0, replayed: true });
    await assert.rejects(applyChangeset(reopened, encodeChangeset([table([insert(1n, "new")])]), opts), { code: "ERR_FSQLITE_CHANGESET_DELIVERY_REUSE" });
    assert.deepEqual(reopened.rows(), [[1n, "new"]]);
    assert.deepEqual(reopened.rows("SELECT * FROM audit"), [[1n]]);
    assert.equal(reopened.db.prepare("PRAGMA integrity_check").get().integrity_check, "ok");
  } finally { reopened.db.close(); }
});

for (let seed = 1; seed <= 20; seed++) {
  test(`deterministic native patchset workload ${seed}`, async (t) => {
    const definition = "CREATE TABLE t(id INTEGER PRIMARY KEY,v);" + Array.from({ length: 20 }, (_, i) => `INSERT INTO t VALUES(${i},'old${i}');`).join("");
    const author = target(t, definition), actual = target(t, definition), oracle = target(t, definition);
    const s = author.db.createSession();
    let random = seed;
    for (let i = 0; i < 80; i++) {
      random = (Math.imul(random, 1664525) + 1013904223) >>> 0;
      const id = random % 35;
      if (random % 3 === 0) author.db.prepare("INSERT OR REPLACE INTO t VALUES(?,?)").run(id, `new${random}`);
      else if (random % 3 === 1) author.db.prepare("UPDATE t SET v=? WHERE id=?").run(BigInt(random), id);
      else author.db.prepare("DELETE FROM t WHERE id=?").run(id);
    }
    const bytes = s.patchset(), full = s.changeset(); s.close();
    assert.deepEqual(encodePatchset(decodePatchset(bytes)), bytes);
    // Shared codec changes cannot regress the strict changeset lane or inversion.
    assert.deepEqual(encodeChangeset(decodeChangeset(full)), full);
    assert.deepEqual(invertChangeset(invertChangeset(full)), full);
    assert.equal(oracle.db.applyChangeset(bytes), true);
    await applyPatchset(actual, bytes, policy);
    assert.deepEqual(actual.rows(), oracle.rows());
    assert.deepEqual(actual.rows(), author.rows());
  });
}

test("invalid SQL probe cannot invent a DATA conflict for absent before-images", async (t) => {
  const actual = target(t, "CREATE TABLE t(id PRIMARY KEY,v); INSERT INTO t VALUES(1,'local')");
  const query = actual.query.bind(actual);
  actual.query = async (sql, params) => sql.startsWith("SELECT CASE")
    ? { rowArrays: [[0n]] } : query(sql, params);
  await assert.rejects(applyPatchset(actual, wire([update([1n, undefined], [undefined, "new"])]), {
    ...policy, onConflict() { assert.fail("no DATA may be inferred"); },
  }), { code: "ERR_FSQLITE_CHANGESET_RESULT" });
  assert.deepEqual(actual.rows(), [[1n, "local"]]);
});
