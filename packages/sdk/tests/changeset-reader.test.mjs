// Execute the production Session parser against native SQLite wire/application.
// No worker, WASM engine, network stream, or physical durability is simulated.
import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import * as codec from "../src/changeset-codec.ts";

const formats = [
  { name: "changeset", prepare: (...args) => codec.prepareChangesetReader(...args),
    decode: codec.decodeChangeset, encode: codec.encodeChangeset },
  { name: "patchset", prepare: (...args) => codec.preparePatchsetReader(...args),
    decode: codec.decodePatchset, encode: codec.encodePatchset },
];
const insert = (...values) => ({ operation: "insert", indirect: false, new: values });
const table = (changes, name = "t", primaryKey = [1, 0]) => ({ name, primaryKey, changes });
function collect(reader) {
  const tables = reader.tables.map(({ name, primaryKey }) => ({ name, primaryKey, changes: [] }));
  let index = 0;
  for (const entry of reader) {
    assert.equal(entry.changeIndex, index++);
    assert.ok(Object.isFrozen(entry));
    assert.ok(Object.isFrozen(entry.change));
    tables[entry.tableIndex].changes.push(entry.change);
  }
  assert.equal(index, reader.changeCount);
  assert.deepEqual(tables.map(t => t.changes.length), reader.tables.map(t => t.changeCount));
  return tables;
}
function rows(db, name) {
  const s = db.prepare(`SELECT * FROM "${name}" ORDER BY 1, 2`);
  s.setReadBigInts(true);
  s.setReturnArrays(true);
  return s.all();
}
function errorOf(work) {
  try { work(); return null; }
  catch (e) { return { code: e.code, message: e.message, offset: e.offset }; }
}

for (const f of formats) {
  test(`${f.name}: prepared rows preserve native multi-table Session wire and SQL results`, () => {
    const schema = `CREATE TABLE t(id INTEGER PRIMARY KEY, v, unchanged);
      CREATE TABLE other(b TEXT, a INTEGER, v BLOB, PRIMARY KEY(a,b)) WITHOUT ROWID;
      INSERT INTO t VALUES(1,'old','keep'),(2,'delete me',5.0);
      INSERT INTO other VALUES('x',1,x'00ff');`;
    const author = new DatabaseSync(":memory:"), actual = new DatabaseSync(":memory:");
    try {
      author.exec(schema); actual.exec(schema);
      const session = author.createSession();
      try {
        author.exec(`UPDATE t SET v=x'00ffff' WHERE id=1;
          DELETE FROM t WHERE id=2;
          INSERT INTO t VALUES(3,9223372036854775807,NULL),(4,-9223372036854775808,1.5);
          UPDATE other SET v=x'' WHERE a=1;
          INSERT INTO other VALUES('y',2,x'123400');`);
        const native = new Uint8Array(session[f.name]());
        const reader = f.prepare(native);
        assert.equal(reader.byteLength, native.length);
        assert.equal(reader.changeCount, 6);
        assert.equal(reader.tables.length, 2);
        assert.ok(Object.isFrozen(reader) && Object.isFrozen(reader.tables));
        for (const info of reader.tables) {
          assert.ok(Object.isFrozen(info) && Object.isFrozen(info.primaryKey));
          assert.equal(Object.hasOwn(info, "changes"), false);
        }
        const reconstructed = collect(reader);
        assert.deepEqual(reconstructed, f.decode(native));
        const roundtrip = f.encode(reconstructed);
        assert.deepEqual(roundtrip, native, "preserve table/row order and every wire field");
        assert.equal(actual.applyChangeset(roundtrip), true);
        assert.deepEqual(rows(actual, "t"), rows(author, "t"));
        assert.deepEqual(rows(actual, "other"), rows(author, "other"));
      } finally { session.close(); }
    } finally { author.close(); actual.close(); }
  });

  test(`${f.name}: input, option and output mutation cannot affect repeatable independent cursors`, () => {
    const wire = f.encode([table([insert(1n, new Uint8Array([0, 255])), insert(2n, "second")]),
      table([insert(3n, new Uint8Array([2, 3]))], "other")]);
    const original = wire.slice(), options = { maxChanges: 3, maxTables: 2 };
    const reader = f.prepare(wire, options);
    wire.fill(0); options.maxChanges = 1; options.maxTables = 1;
    const a = reader[Symbol.iterator](), b = reader[Symbol.iterator]();
    const first = a.next().value;
    first.change.new[1].fill(8);
    assert.deepEqual(b.next().value.change.new[1], new Uint8Array([0, 255]));
    assert.equal(a.next().value.change.new[1], "second");
    a.return();
    assert.equal(b.next().value.changeIndex, 1);
    assert.equal(b.next().value.tableIndex, 1);
    assert.equal(b.next().done, true);
    assert.deepEqual(collect(reader), f.decode(original));
    assert.throws(() => { reader.tables[0].primaryKey[0] = 0; }, TypeError);
    assert.throws(() => { reader.tables.length = 0; }, TypeError);
    assert.deepEqual(f.encode(collect(reader)), original);
  });

  test(`${f.name}: malformed final records and headers reject at preparation, not cursor consumption`, () => {
    const valid = f.encode([table([insert(1n, "prefix"), insert(2n, "last payload")])]);
    for (let missing = 1; missing <= 12; missing++) {
      const truncated = valid.subarray(0, valid.length - missing);
      const expected = errorOf(() => f.decode(truncated));
      assert.ok(expected, `fixture ${missing} must be invalid`);
      assert.deepEqual(errorOf(() => f.prepare(truncated)), expected);
    }
    const other = f.encode([table([insert(3n, "x")], "T")]);
    const repeated = new Uint8Array([...valid, ...other]);
    assert.deepEqual(errorOf(() => f.prepare(repeated)), errorOf(() => f.decode(repeated)));
    const emptyHeader = new Uint8Array([f.name === "changeset" ? 84 : 80, 2, 1, 0, 117, 0]);
    const emptyLast = new Uint8Array([...valid, ...emptyHeader]);
    assert.throws(() => f.prepare(emptyLast), { code: "ERR_FSQLITE_CHANGESET_FORMAT" });
    assert.deepEqual(errorOf(() => f.prepare(emptyLast)), errorOf(() => f.decode(emptyLast)));
  });

  test(`${f.name}: budgets apply cumulatively to the whole message, not the current table or row`, () => {
    const wire = f.encode([table([insert(1n, "a"), insert(2n, "b")]), table([insert(3n, "c")], "u")]);
    const exact = { maxBytes: wire.length, maxChanges: 3, maxCells: 6, maxTables: 2, maxColumns: 2 };
    assert.equal(f.prepare(wire, exact).changeCount, 3);
    for (const key of Object.keys(exact)) {
      const small = { ...exact, [key]: exact[key] - 1 };
      assert.throws(() => f.prepare(wire, small), { code: "ERR_FSQLITE_CHANGESET_LIMIT" }, key);
      assert.deepEqual(errorOf(() => f.prepare(wire, small)), errorOf(() => f.decode(wire, small)), key);
    }
    assert.deepEqual(collect(f.prepare(new Uint8Array())), []);
    assert.equal(f.prepare(new Uint8Array()).changeCount, 0);
  });

  test(`${f.name}: capture uses intrinsic slots, rejects unsafe storage and survives later detachment`, () => {
    const wire = f.encode([table([insert(1n, new Uint8Array([5, 6]))])]);
    const storage = new Uint8Array(wire.length + 4); storage.set(wire, 2);
    const input = storage.subarray(2, -2);
    let calls = 0;
    for (const name of ["length", "byteLength", "byteOffset", "buffer", "slice", "subarray"])
      Object.defineProperty(input, name, { get() { calls++; throw new Error(name); } });
    const reader = f.prepare(input);
    assert.equal(calls, 0);
    structuredClone(storage.buffer, { transfer: [storage.buffer] });
    assert.deepEqual(f.encode(collect(reader)), wire);
    for (const bad of [new Uint8Array(new SharedArrayBuffer(wire.length)),
      new Uint8Array(new ArrayBuffer(wire.length, { maxByteLength: wire.length + 1 })),
      input, new Proxy(wire, {})]) {
      assert.throws(() => f.prepare(bad), { code: "ERR_FSQLITE_CHANGESET_INPUT" });
    }
  });

  test(`${f.name}: native multi-step histories agree with eager decoding across repeated cursors`, () => {
    for (let seed = 0; seed < 16; seed++) {
      const db = new DatabaseSync(":memory:");
      try {
        db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY,v,keep); INSERT INTO t VALUES(1,'initial','fixed')");
        const session = db.createSession();
        try {
          for (let step = 0; step < 12; step++) {
            const key = (seed + step) % 7;
            if ((step + seed) % 3 === 0) db.prepare("DELETE FROM t WHERE id=?").run(key);
            else db.prepare("INSERT INTO t VALUES(?,?,?) ON CONFLICT(id) DO UPDATE SET v=excluded.v")
              .run(key, new Uint8Array([seed, step, 0, 255]), `s${seed}`);
            const wire = new Uint8Array(session[f.name]()), reader = f.prepare(wire);
            assert.deepEqual(collect(reader), f.decode(wire), `seed=${seed}, step=${step}`);
            assert.deepEqual(f.encode(collect(reader)), wire);
          }
        } finally { session.close(); }
      } finally { db.close(); }
    }
  });
}

test("readers keep full changesets and compact patchsets explicitly distinct", () => {
  const full = codec.encodeChangeset([table([insert(1n, "x")])]);
  const compact = codec.encodePatchset([table([insert(1n, "x")])]);
  assert.throws(() => codec.prepareChangesetReader(compact), { code: "ERR_FSQLITE_CHANGESET_FORMAT" });
  assert.throws(() => codec.preparePatchsetReader(full), { code: "ERR_FSQLITE_CHANGESET_FORMAT" });
  for (const mixed of [new Uint8Array([...full, ...compact]), new Uint8Array([...compact, ...full])]) {
    assert.throws(() => codec.prepareChangesetReader(mixed), { code: "ERR_FSQLITE_CHANGESET_FORMAT" });
    assert.throws(() => codec.preparePatchsetReader(mixed), { code: "ERR_FSQLITE_CHANGESET_FORMAT" });
  }
  const wire = codec.encodePatchset([table([
    { operation: "update", indirect: true, old: [1n, undefined], new: [undefined, null] },
    { operation: "delete", indirect: false, old: [2n, undefined] },
  ])]);
  const entries = [...codec.preparePatchsetReader(wire)];
  assert.equal(entries[0].change.old[1], undefined);
  assert.equal(entries[0].change.new[1], null);
  assert.equal(entries[1].change.old[1], undefined);
  assert.equal(entries[0].change.indirect, true);
});

test("prepared and eager validation have identical errors for deterministic corrupted native payloads", () => {
  for (const f of formats) {
    const wire = f.encode([table([insert(1n, "hello 世界"), insert(2n, new Uint8Array([1, 2, 3]))])]);
    for (let offset = 0; offset < wire.length; offset++) for (const mask of [1, 64, 128, 255]) {
      const changed = wire.slice(); changed[offset] ^= mask;
      const eagerError = errorOf(() => f.decode(changed));
      const readerError = errorOf(() => f.prepare(changed));
      assert.deepEqual(readerError, eagerError, `${f.name}/${offset}/${mask}`);
      if (readerError === null) assert.deepEqual(collect(f.prepare(changed)), f.decode(changed));
    }
  }
});
