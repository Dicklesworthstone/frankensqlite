// Production codec against SQLite's native session extension, not a WASM test.
import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import {
  ChangesetError,
  decodeChangeset,
  encodeChangeset,
  invertChangeset,
} from "../src/changeset-codec.ts";

const insert = (values) => ({ operation: "insert", indirect: false, new: values });
const table = (changes, primaryKey = [1, 0], name = "t") => ({ name, primaryKey, changes });
function db(t, schema = "CREATE TABLE t(id INTEGER PRIMARY KEY, v)") {
  const d = new DatabaseSync(":memory:");
  d.exec(schema);
  t.after(() => d.close());
  return d;
}
function rows(d, name = "t") {
  const s = d.prepare(`SELECT * FROM "${name.replaceAll('"', '""')}" ORDER BY 1`);
  s.setReadBigInts(true);
  return s
    .all()
    .map((row) =>
      Object.values(row).map((value) => (value instanceof Uint8Array ? [...value] : value)),
    );
}
function roundtrip(bytes) {
  assert.deepEqual(encodeChangeset(decodeChangeset(bytes)), bytes);
}
const formatError = (e) => e instanceof ChangesetError && e.code === "ERR_FSQLITE_CHANGESET_FORMAT";
const limitError = (e) => e instanceof ChangesetError && e.code === "ERR_FSQLITE_CHANGESET_LIMIT";

test("empty SQLite session is an empty valid changeset", (t) => {
  const d = db(t),
    s = d.createSession();
  assert.equal(s.changeset().length, 0);
  assert.deepEqual(decodeChangeset(s.changeset()), []);
  assert.deepEqual(encodeChangeset([]), new Uint8Array());
  assert.deepEqual(invertChangeset(s.changeset()), new Uint8Array());
  s.close();
});
test("native inserts round-trip exactly across every SQLite storage class", (t) => {
  const d = db(t),
    s = d.createSession();
  const values = [
    null,
    "",
    "hello\0world",
    "\uFEFFUnicode λ🚀",
    Buffer.from([0, 255]),
    -(1n << 63n),
    (1n << 63n) - 1n,
    1.5,
    Infinity,
    -Infinity,
  ];
  values.forEach((v, i) => d.prepare("INSERT INTO t VALUES (?,?)").run(i + 1, v));
  const bytes = s.changeset();
  s.close();
  roundtrip(bytes);
  const restored = db(t);
  assert.equal(restored.applyChangeset(encodeChangeset(decodeChangeset(bytes))), true);
  assert.deepEqual(rows(restored), rows(d));
});
test("generated UPDATE distinguishes untouched fields, SQL NULL and empty blobs", (t) => {
  const d = db(t, "CREATE TABLE t(id INTEGER PRIMARY KEY, a, b, c)");
  d.exec("INSERT INTO t VALUES (1,'old','untouched',X'0102')");
  const s = d.createSession();
  d.exec("UPDATE t SET a=NULL,c=X''");
  const bytes = s.changeset();
  s.close();
  roundtrip(bytes);
  const change = decodeChangeset(bytes)[0].changes[0];
  assert.deepEqual(change.old, [1n, "old", undefined, new Uint8Array([1, 2])]);
  assert.deepEqual(change.new, [undefined, null, undefined, new Uint8Array()]);
  assert.equal(d.applyChangeset(invertChangeset(bytes)), true);
  assert.deepEqual(rows(d), [[1n, "old", "untouched", [1, 2]]]);
});
test("native delete inversion restores all fields", (t) => {
  const d = db(t);
  d.exec("INSERT INTO t VALUES (1,'gone')");
  const s = d.createSession();
  d.exec("DELETE FROM t");
  const bytes = s.changeset();
  s.close();
  roundtrip(bytes);
  assert.equal(decodeChangeset(bytes)[0].changes[0].operation, "delete");
  assert.equal(d.applyChangeset(invertChangeset(bytes)), true);
  assert.deepEqual(rows(d), [[1n, "gone"]]);
});
test("composite WITHOUT ROWID key order bytes survive decoding and inversion", (t) => {
  const d = db(t, "CREATE TABLE t(a TEXT,b INTEGER,c,PRIMARY KEY(b,a)) WITHOUT ROWID");
  d.exec("INSERT INTO t VALUES('a',1,'old')");
  const s = d.createSession();
  d.exec("UPDATE t SET c='new'");
  const bytes = s.changeset();
  s.close();
  roundtrip(bytes);
  assert.deepEqual(decodeChangeset(bytes)[0].primaryKey, [2, 1, 0]);
  assert.equal(d.applyChangeset(invertChangeset(bytes)), true);
  assert.deepEqual(rows(d), [["a", 1n, "old"]]);
});
test("multiple tables and mixed operations round-trip and undo a native session", (t) => {
  const schema = "CREATE TABLE t(id INTEGER PRIMARY KEY,v); CREATE TABLE u(a TEXT PRIMARY KEY,b)";
  const d = db(t, schema);
  d.exec("INSERT INTO t VALUES(1,'update'),(2,'delete'); INSERT INTO u VALUES('x',7)");
  const initial = [rows(d), rows(d, "u")],
    s = d.createSession();
  d.exec(
    "UPDATE t SET v='updated' WHERE id=1; DELETE FROM t WHERE id=2; INSERT INTO t VALUES(3,'new'); UPDATE u SET b=8",
  );
  const bytes = s.changeset();
  s.close();
  roundtrip(bytes);
  assert.equal(decodeChangeset(bytes).length, 2);
  assert.equal(d.applyChangeset(invertChangeset(bytes)), true);
  assert.deepEqual([rows(d), rows(d, "u")], initial);
  assert.deepEqual(invertChangeset(invertChangeset(bytes)), bytes);
});
test("primary-key UPDATE generated as DELETE/INSERT remains interoperable", (t) => {
  const d = db(t);
  d.exec("INSERT INTO t VALUES(1,'v')");
  const s = d.createSession();
  d.exec("UPDATE t SET id=9");
  const bytes = s.changeset();
  s.close();
  roundtrip(bytes);
  assert.equal(decodeChangeset(bytes)[0].changes.length, 2);
  assert.equal(d.applyChangeset(invertChangeset(bytes)), true);
  assert.deepEqual(rows(d), [[1n, "v"]]);
});
test("JavaScript-authored typed records apply using SQLite native applyChangeset", (t) => {
  const d = db(t, "CREATE TABLE t(id INTEGER PRIMARY KEY, i, r, v, b)");
  const bytes = encodeChangeset([
    table([insert([7n, 42n, 42, "data", new Uint8Array([8])])], [1, 0, 0, 0, 0]),
  ]);
  assert.equal(d.applyChangeset(bytes), true);
  const row = d.prepare("SELECT typeof(i) AS i, typeof(r) AS r, hex(b) AS b FROM t").get();
  assert.deepEqual({ ...row }, { i: "integer", r: "real", b: "08" });
});
test("indirect marker is preserved, including double inversion", () => {
  const change = { ...insert([1n, "value"]), indirect: true };
  const bytes = encodeChangeset([table([change])]);
  assert.equal(decodeChangeset(bytes)[0].changes[0].indirect, true);
  assert.deepEqual(invertChangeset(invertChangeset(bytes)), bytes);
});
test("UTF-8 table names, quotes and BOM text survive exact serialization", (t) => {
  const name = '引号"🚀';
  const d = db(t, `CREATE TABLE "${name.replaceAll('"', '""')}"(id PRIMARY KEY,v)`);
  const bytes = encodeChangeset([table([insert(["key", "\uFEFFdata"])], [1, 0], name)]);
  roundtrip(bytes);
  assert.equal(d.applyChangeset(bytes), true);
  assert.deepEqual(rows(d, name), [["key", "\uFEFFdata"]]);
});
test("blob decode owns a tight copy and sliced input uses correct offsets", () => {
  const bytes = encodeChangeset([table([insert([1n, new Uint8Array([1, 2, 3])])])]);
  const backing = new Uint8Array(bytes.length + 16);
  backing.set(bytes, 7);
  const decoded = decodeChangeset(backing.subarray(7, 7 + bytes.length));
  backing.fill(0);
  const blob = decoded[0].changes[0].new[1];
  assert.deepEqual(blob, new Uint8Array([1, 2, 3]));
  assert.equal(blob.buffer.byteLength, 3);
  assert.equal(Object.isFrozen(decoded), true);
  assert.equal(Object.isFrozen(decoded[0].changes[0].new), true);
});
test("native patchsets are rejected rather than misread as complete before images", (t) => {
  const d = db(t),
    s = d.createSession();
  d.exec("INSERT INTO t VALUES(1,2)");
  assert.throws(() => decodeChangeset(s.patchset()), formatError);
  assert.throws(() => invertChangeset(s.patchset()), formatError);
  s.close();
});
test("all truncated prefixes of a one-change record reject (except empty stream)", () => {
  const bytes = encodeChangeset([table([insert([1n, "longer payload"])])]);
  for (let n = 1; n < bytes.length; n++)
    assert.throws(() => decodeChangeset(bytes.slice(0, n)), ChangesetError);
});
test("malformed tags, headers, UTF-8, varints and missing primary keys fail closed", () => {
  for (const bytes of [
    [83],
    [84, 0],
    [84, 2, 0, 0, 116, 0],
    [84, 2, 1, 0, 255, 0],
    [84, 128, 2],
    [84, 2, 1, 0, 116, 0, 255, 0],
    [84, 2, 1, 0, 116, 0, 18, 2],
    [84, 2, 1, 0, 116, 0, 18, 0, 6, 0],
    [84, 2, 1, 0, 116, 0, 18, 0, 5, 5],
  ])
    assert.throws(() => decodeChangeset(new Uint8Array(bytes)), ChangesetError);
});
test("invalid UPDATE/INSERT record semantics cannot be encoded", () => {
  for (const change of [
    insert([1n, undefined]),
    insert([null, "bad"]),
    { operation: "update", indirect: false, old: [1n, "a"], new: [2n, "b"] },
    { operation: "update", indirect: false, old: [1n, undefined], new: [undefined, "b"] },
    { operation: "update", indirect: false, old: [1n, undefined], new: [undefined, undefined] },
  ])
    assert.throws(() => encodeChangeset([table([change])]), ChangesetError);
});
test("unsupported numbers, ill-formed strings and conflicting table headers reject", () => {
  for (const value of [NaN, 1n << 63n, -(1n << 63n) - 1n, true, {}, "\ud800"]) {
    assert.throws(() => encodeChangeset([table([insert([1n, value])])]), ChangesetError);
  }
  assert.throws(
    () => encodeChangeset([table([insert([1n, "x"])]), table([insert([2n, "y"])], [1, 0], "T")]),
    formatError,
  );
  assert.throws(
    () => encodeChangeset([table([insert([1n, "x"])], [1, 0], "bad\0name")]),
    ChangesetError,
  );
});
test("byte, table, column, change and field-slot bounds apply to both directions", () => {
  const source = [
    table([insert([1n, "a"]), insert([2n, "b"])]),
    table([insert([3n, "c"])], [1, 0], "u"),
  ];
  const bytes = encodeChangeset(source);
  for (const policy of [
    { maxBytes: 8 },
    { maxTables: 1 },
    { maxColumns: 1 },
    { maxChanges: 2 },
    { maxCells: 5 },
  ]) {
    assert.throws(() => decodeChangeset(bytes, policy), limitError);
    assert.throws(() => encodeChangeset(source, policy), ChangesetError);
  }
  for (const policy of [
    { maxBytes: 0 },
    { maxTables: Infinity },
    { maxCells: 1.5 },
    { maxChanges: 100001 },
  ]) {
    assert.throws(() => decodeChangeset(bytes, policy), { code: "ERR_FSQLITE_CHANGESET_INPUT" });
  }
});
test("length varint boundaries 127/128/16383/16384 and large blobs match native", (t) => {
  const d = db(t),
    s = d.createSession();
  for (const n of [0, 127, 128, 16383, 16384])
    d.prepare("INSERT INTO t VALUES(?,?)").run(n, Buffer.alloc(n, 13));
  const bytes = s.changeset();
  s.close();
  roundtrip(bytes);
  assert.equal(d.applyChangeset(invertChangeset(bytes)), true);
  assert.deepEqual(rows(d), []);
});
test("shared, detached and resizable buffers cannot race decoding", () => {
  assert.throws(() => decodeChangeset(new Uint8Array(new SharedArrayBuffer(0))), ChangesetError);
  const view = new Uint8Array(1);
  structuredClone(view.buffer, { transfer: [view.buffer] });
  assert.throws(() => decodeChangeset(view));
  assert.throws(
    () => decodeChangeset(new Uint8Array(new ArrayBuffer(1, { maxByteLength: 4 }))),
    ChangesetError,
  );
});
test("deterministic native session workloads preserve bytes and round-trip database state", (t) => {
  for (let seed = 1; seed <= 30; seed++) {
    const d = new DatabaseSync(":memory:");
    try {
      d.exec("CREATE TABLE t(id INTEGER PRIMARY KEY,v)");
      for (let i = 1; i <= 20; i++) d.prepare("INSERT INTO t VALUES(?,?)").run(i, `old${i}`);
      const initial = rows(d),
        s = d.createSession();
      let state = seed;
      for (let i = 0; i < 80; i++) {
        state = (Math.imul(state, 1664525) + 1013904223) >>> 0;
        const id = (state % 35) + 1;
        const mode = (state >>> 12) % 3;
        if (mode === 0) d.prepare("INSERT OR REPLACE INTO t VALUES(?,?)").run(id, `new${state}`);
        else if (mode === 1) d.prepare("UPDATE t SET v=? WHERE id=?").run(state, id);
        else d.prepare("DELETE FROM t WHERE id=?").run(id);
      }
      const bytes = s.changeset();
      s.close();
      roundtrip(bytes);
      assert.equal(d.applyChangeset(invertChangeset(bytes)), true);
      assert.deepEqual(rows(d), initial);
    } finally {
      d.close();
    }
  }
});
