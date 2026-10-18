// Execute the production Session parser against native SQLite wire/application.
// No worker, WASM engine, network stream, or physical durability is simulated.
import assert from "node:assert/strict";
import { constants, DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import * as codec from "../src/changeset-codec.ts";
import { applyChangeset, applyPatchset } from "../src/changeset-apply.ts";

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

// Actual SQL and savepoints: the only injected scheduling boundary is an
// awaited hook after a completed SQL statement, before its promise settles.
class SqlTarget {
  constructor(t, schema = "CREATE TABLE t(id INTEGER PRIMARY KEY,v)") {
    this.db = new DatabaseSync(":memory:"); this.db.exec(schema);
    this.log = []; this.scopes = 0; this.afterExecute = null;
    t.after(() => this.db.close());
  }
  async transaction(work, options = {}) {
    const name = `reader_scope_${++this.scopes}`;
    this.db.exec(`SAVEPOINT ${name}`);
    const tx = {
      query: async (sql, params = []) => {
        this.log.push(sql);
        const s = this.db.prepare(sql); s.setReadBigInts(true); s.setReturnArrays(true);
        return { rowArrays: s.all(...params) };
      },
      execute: async (sql, params = []) => {
        this.log.push(sql);
        const changes = Number(this.db.prepare(sql).run(...params).changes);
        await this.afterExecute?.(sql, params);
        return changes;
      },
      transaction: (work, options) => this.transaction(work, options),
    };
    try {
      const value = await work(tx);
      if (options.signal?.aborted) throw options.signal.reason;
      this.db.exec(`RELEASE ${name}`);
      return value;
    } catch (error) {
      this.db.exec(`ROLLBACK TO ${name}; RELEASE ${name}`);
      throw error;
    }
  }
}
function deferred() {
  let resolve;
  const promise = new Promise(r => { resolve = r; });
  return { promise, resolve };
}

for (const f of formats) {
  const apply = f.name === "changeset" ? applyChangeset : applyPatchset;
  test(`${f.name}: row-at-a-time SQL preserves later rows after policy mutates caller input`, async t => {
    const schema = `CREATE TABLE t(id INTEGER PRIMARY KEY,v); CREATE TABLE u(id INTEGER PRIMARY KEY,v);
      CREATE TABLE decisions(info BLOB); INSERT INTO t VALUES(0,'old'); INSERT INTO u VALUES(7,'keep');`;
    const target = new SqlTarget(t, schema), native = new DatabaseSync(":memory:");
    t.after(() => native.close()); native.exec(schema);
    const changes = Array.from({ length: 1001 }, (_, i) => insert(BigInt(i), new Uint8Array([i % 251, 0, 255])));
    const wire = f.encode([table(changes), table([insert(7n, "ignored")], "u")]);
    const original = wire.slice(), seen = [];
    const options = { tables: ["t", "u"], deliveryId: `reader:${f.name}`,
      onConflict(c) {
        seen.push([c.table, c.changeIndex]);
        if (c.table === "u") return "omit";
        assert.deepEqual(c.current, [0n, "old"]);
        c.change.new[1].fill(9);
        wire.fill(0); // Later SQL rows must come from the private validated wire.
        return "replace";
      },
    };
    if (f.name === "changeset") options.onRebase = (tx, info) => tx.execute("INSERT INTO decisions VALUES(?)", [info]);
    assert.deepEqual(await apply(target, wire, options), { applied: 1001, omitted: 1, replayed: false });
    assert.deepEqual(seen, [["t", 0], ["u", 1001]]);
    let nativeConflicts = 0;
    assert.equal(native.applyChangeset(original, { onConflict: () => ++nativeConflicts === 1
      ? constants.SQLITE_CHANGESET_REPLACE : constants.SQLITE_CHANGESET_OMIT }), true);
    assert.deepEqual(rows(target.db, "t"), rows(native, "t"));
    assert.deepEqual(rows(target.db, "u"), rows(native, "u"));
    if (f.name === "changeset") {
      const info = target.db.prepare("SELECT info FROM decisions").get().info;
      assert.deepEqual(codec.decodeRebaseInfo(info).map(t => [t.name, t.changes[0].replace]), [["t", true], ["u", false]]);
    }
    const offset = target.log.length;
    options.onConflict = () => assert.fail("receipt replay must not invoke policy");
    options.onRebase = undefined;
    assert.deepEqual(await apply(target, original, options), { applied: 1001, omitted: 1, replayed: true });
    assert.equal(target.log.slice(offset).some(sql => /^(INSERT|UPDATE|DELETE|CREATE)/.test(sql)), false);
  });

  test(`${f.name}: invalid final wire and unauthorized final tables never enter SQL`, async t => {
    const target = new SqlTarget(t);
    const valid = f.encode([table([insert(1n, "valid"), insert(2n, "last payload")])]);
    await assert.rejects(apply(target, valid.subarray(0, -1), { tables: ["t"], deliveryId: "invalid" }),
      { code: "ERR_FSQLITE_CHANGESET_FORMAT" });
    const late = f.encode([table([insert(1n, "valid")]), table([insert(2n, "last")], "unauthorized")]);
    await assert.rejects(apply(target, late, { tables: ["t"] }), { code: "ERR_FSQLITE_CHANGESET_INPUT" });
    assert.equal(target.scopes, 0); assert.deepEqual(target.log, []);
    await assert.rejects(apply(target, late, { tables: ["t", "unauthorized"] }), { code: "ERR_FSQLITE_CHANGESET_SCHEMA" });
    assert.equal(target.log.some(sql => /^(INSERT|UPDATE|DELETE)/.test(sql)), false);
    assert.deepEqual(rows(target.db, "t"), []);
  });

  test(`${f.name}: a late conflict rolls back every preceding row but preserves the caller's outer work`, async t => {
    const target = new SqlTarget(t, "CREATE TABLE t(id INTEGER PRIMARY KEY,v); INSERT INTO t VALUES(999,'old')");
    const changes = Array.from({ length: 512 }, (_, i) => insert(BigInt(i), `row ${i}`));
    changes.push(insert(999n, "conflict"));
    await target.transaction(async tx => {
      await tx.execute("INSERT INTO t VALUES(2000,'outer')");
      await assert.rejects(apply(tx, f.encode([table(changes)]), { tables: ["t"], deliveryId: "abort" }),
        e => e.code === "ERR_FSQLITE_CHANGESET_CONFLICT" && e.conflict.changeIndex === 512);
      assert.deepEqual(rows(target.db, "t"), [[999n, "old"], [2000n, "outer"]]);
      assert.equal(target.db.prepare("SELECT count(*) n FROM sqlite_schema WHERE name='__fsqlite_changeset_receipts'").get().n, 0);
      await tx.execute("INSERT INTO t VALUES(2001,'after')");
    });
    assert.deepEqual(rows(target.db, "t"), [[999n, "old"], [2000n, "outer"], [2001n, "after"]]);
  });

  test(`${f.name}: cancellation drains admitted SQL before abandoning the cursor and rolling back`, async t => {
    const target = new SqlTarget(t), entered = deferred(), release = deferred(), controller = new AbortController();
    let writes = 0, settled = false;
    target.afterExecute = async sql => {
      if (sql.startsWith('INSERT OR ABORT INTO main."t"') && ++writes === 5) {
        entered.resolve(); await release.promise;
      }
    };
    const wire = f.encode([table(Array.from({ length: 100 }, (_, i) => insert(BigInt(i), `row ${i}`)))]);
    const pending = apply(target, wire, { tables: ["t"], deliveryId: "cancelled", signal: controller.signal });
    void pending.then(() => { settled = true; }, () => { settled = true; });
    await Promise.race([entered.promise, pending.then(() => assert.fail("held SQL was not entered"))]);
    controller.abort(new Error("stop"));
    await new Promise(r => setImmediate(r));
    assert.equal(settled, false, "cancellation cannot abandon admitted work");
    assert.equal(target.db.prepare("SELECT count(*) n FROM t").get().n, 5);
    release.resolve();
    await assert.rejects(pending, { code: "ERR_FSQLITE_CHANGESET_CANCELLED" });
    assert.equal(writes, 5);
    assert.deepEqual(rows(target.db, "t"), []);
    assert.equal(target.db.prepare("SELECT count(*) n FROM sqlite_schema WHERE name='__fsqlite_changeset_receipts'").get().n, 0);
  });
}
