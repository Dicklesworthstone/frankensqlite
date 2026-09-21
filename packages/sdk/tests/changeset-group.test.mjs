// Production composition against native SQLite Session capture/apply, not WASM.
import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import { decodeChangeset, encodeChangeset, invertChangeset } from "../src/changeset-codec.ts";
import { ChangesetGroupError, concatChangesets } from "../src/changeset-group.ts";

const empty = () => new Uint8Array();
const table = (changes, primaryKey = [1, 0], name = "t") => ({ name, primaryKey, changes });
const insert = (values, indirect = false) => ({ operation: "insert", indirect, new: values });
const remove = (values, indirect = false) => ({ operation: "delete", indirect, old: values });
const update = (old, next, indirect = false) => ({ operation: "update", indirect, old, new: next });
const bytes = (change, pk = [1, 0], name = "t") => encodeChangeset([table([change], pk, name)]);
const changes = (value) => decodeChangeset(value).flatMap((t) => t.changes);
const limitError = { code: "ERR_FSQLITE_CHANGESET_LIMIT" };

// Compare every wire field and flag, ignoring ONLY native hash iteration order.
function normalized(value) {
  return decodeChangeset(value)
    .map((t) => ({
      name: t.name,
      primaryKey: t.primaryKey,
      rows: t.changes.map((c) => Buffer.from(encodeChangeset([{ ...t, changes: [c] }])).toString("hex")).sort(),
    }))
    .sort((a, b) => a.name.localeCompare(b.name));
}
function open(t, schema, seed = "") {
  const db = new DatabaseSync(":memory:");
  db.exec(schema);
  if (seed) db.exec(seed);
  t.after(() => db.close());
  return db;
}
function snapshot(db) {
  const tables = db.prepare("SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name").all();
  return tables.map(({ name }) => {
    const statement = db.prepare(`SELECT * FROM "${name.replaceAll('"', '""')}" ORDER BY 1,2`);
    statement.setReadBigInts(true);
    return [name, statement.all().map((r) => Object.values(r))];
  });
}
function nativeHistory(t, schema, seed, steps) {
  const source = open(t, schema, seed);
  const initial = snapshot(source);
  const whole = source.createSession();
  const parts = [];
  let composed = empty();
  try {
    for (const step of steps) {
      const session = source.createSession();
      try {
        if (typeof step === "function") step(source);
        else source.exec(step);
        const part = session.changeset();
        parts.push(part);
        composed = concatChangesets(composed, part);
      } finally {
        session.close();
      }
    }
    assert.deepEqual(normalized(composed), normalized(whole.changeset()));
  } finally {
    whole.close();
  }
  const target = open(t, schema, seed);
  assert.equal(target.applyChangeset(composed), true);
  assert.deepEqual(snapshot(target), snapshot(source));
  assert.equal(target.applyChangeset(invertChangeset(composed)), true);
  assert.deepEqual(snapshot(target), initial);
  return { composed, parts, source };
}

test("SQLite oracle version is observable and both empty inputs are identities", (t) => {
  const db = open(t, "CREATE TABLE t(id PRIMARY KEY,v)");
  t.diagnostic(`SQLite Session oracle ${db.prepare("SELECT sqlite_version() AS version").get().version}; Node ${process.version}`);
  const a = bytes(insert([1n, "payload"]));
  assert.deepEqual(concatChangesets(empty(), empty()), empty());
  assert.deepEqual(concatChangesets(empty(), a), a);
  assert.deepEqual(concatChangesets(a, empty()), a);
});

test("all nine operation transitions preserve original images and indirect flags", () => {
  for (const ai of [false, true]) for (const bi of [false, true]) {
    const first = {
      insert: insert([1n, "before"], ai),
      update: update([1n, "before"], [undefined, "middle"], ai),
      delete: remove([1n, "before"], ai),
    };
    const second = {
      insert: insert([1n, "after"], bi),
      update: update([1n, "middle"], [undefined, "after"], bi),
      delete: remove([1n, "middle"], bi),
    };
    const expected = {
      "insert/insert": first.insert,
      "insert/update": insert([1n, "after"], ai && bi),
      "insert/delete": null,
      "update/insert": first.update,
      "update/update": update([1n, "before"], [undefined, "after"], ai && bi),
      "update/delete": remove([1n, "before"], ai && bi),
      "delete/insert": update([1n, "before"], [undefined, "after"], ai && bi),
      "delete/update": first.delete,
      "delete/delete": first.delete,
    };
    for (const [a, ca] of Object.entries(first)) for (const [b, cb] of Object.entries(second)) {
      const want = expected[`${a}/${b}`];
      assert.deepEqual(changes(concatChangesets(bytes(ca), bytes(cb))), want === null ? [] : [want], `${a}/${b}/${ai}/${bi}`);
    }
  }
});

test("disjoint updates, deletion and new inserts match a single native session and undo", (t) => {
  nativeHistory(t,
    "CREATE TABLE t(id INTEGER PRIMARY KEY,a,b); CREATE TABLE u(k TEXT PRIMARY KEY,v)",
    "INSERT INTO t VALUES(1,'original',X'01'),(2,'deleted',NULL),(3,'unchanged',42); INSERT INTO u VALUES('x','old')",
    [
      "UPDATE t SET a=NULL WHERE id=1; UPDATE t SET b=X'02' WHERE id=2; INSERT INTO t VALUES(4,'new',1.5)",
      "UPDATE t SET b=X'00ff' WHERE id=1; DELETE FROM t WHERE id=2; UPDATE t SET a='changed' WHERE id=4; UPDATE u SET v=9",
      "UPDATE t SET a='original' WHERE id=1; INSERT INTO t VALUES(5,'last',NULL)",
    ]);
});

test("insert/delete, delete/reinsert and reverted updates disappear without empty table headers", (t) => {
  const { composed } = nativeHistory(t,
    "CREATE TABLE t(id INTEGER PRIMARY KEY,a,b)", "INSERT INTO t VALUES(1,'a',X'00ff'),(2,NULL,7)",
    [
      "INSERT INTO t VALUES(3,'temporary',1); DELETE FROM t WHERE id=1; UPDATE t SET a='b',b=8 WHERE id=2",
      "DELETE FROM t WHERE id=3; INSERT INTO t VALUES(1,'a',X'00ff'); UPDATE t SET a=NULL,b=7 WHERE id=2",
    ]);
  assert.deepEqual(composed, empty());
});

test("primary-key changes across several sessions retain native delete/insert identity", (t) => {
  nativeHistory(t, "CREATE TABLE t(id INTEGER PRIMARY KEY,v)", "INSERT INTO t VALUES(1,'a')", [
    "UPDATE t SET id=2,v='b'", "UPDATE t SET id=3", "UPDATE t SET v='c'",
  ]);
});

test("composite blob/text keys, arbitrary key order and 64-bit values stay exact", (t) => {
  nativeHistory(t,
    "CREATE TABLE t(a,b,c,PRIMARY KEY(b,a)) WITHOUT ROWID", "", [
      (db) => {
        const s = db.prepare("INSERT INTO t VALUES(?,?,?)");
        s.run("a:b;", new Uint8Array([0,255]), -(1n << 63n));
        s.run("a:b;\0", new Uint8Array([0,255,0]), (1n << 63n) - 1n);
        s.run("λ🚀", new Uint8Array(), "\uFEFFtext\0tail");
      },
      (db) => db.prepare("UPDATE t SET c=? WHERE a=? AND b=?").run(null, "a:b;", new Uint8Array([0,255])),
      "DELETE FROM t WHERE a='λ🚀'",
    ]);
});

test("typed keys never conflate integer/real/text/blob or positive and negative zero", () => {
  // Native sqlite3changeset_concat uses the serialized PK representation, not
  // SQL numeric equality. These can be distinct group entries even though a
  // particular destination schema might subsequently reject them as conflicts.
  const keys = [1n, 1, "1", new Uint8Array([49]), 0, -0, "", new Uint8Array()];
  const a = encodeChangeset([table(keys.map((key) => insert([key, "old"])))]);
  const b = encodeChangeset([table(keys.map((key) => update([key, "old"], [undefined, "new"])))]);
  const result = changes(concatChangesets(a, b));
  assert.equal(result.length, keys.length);
  result.forEach((change, i) => assert.deepEqual(change.new, [keys[i], "new"]));
});

test("composite key tokens are unambiguous and blobs compare by content", () => {
  const keys = [["a", "b:c"], ["a:b", "c"], ["", "t0:"], ["t0:", ""], [new Uint8Array([1,2]), "x"]];
  const a = encodeChangeset([table(keys.map((k) => insert([...k, "old"])), [1,2,0])]);
  const b = encodeChangeset([table(keys.map((k) => update([...k, "old"], [undefined,undefined,"new"])), [1,2,0])]);
  assert.equal(changes(concatChangesets(a, b)).length, keys.length);
  assert.ok(changes(concatChangesets(a, b)).every((c) => c.new[2] === "new"));
});

test("NULL, untouched, integer/real and blob equality retain distinct update semantics", () => {
  const a = bytes(remove([1n, null, 7n, new Uint8Array([1]), -0]), [1,0,0,0,0]);
  const b = bytes(insert([1n, null, 7, new Uint8Array([1]), 0]), [1,0,0,0,0]);
  assert.deepEqual(changes(concatChangesets(a,b)), [
    update([1n,undefined,7n,undefined,-0], [undefined,undefined,7,undefined,0]),
  ]);
});

test("case-insensitive table matching retains first spelling and rejects incompatible schemas", () => {
  const a = bytes(insert([1n,"a"]), [1,0], "TaBle");
  const b = bytes(update([1n,"a"], [undefined,"b"]), [1,0], "table");
  const result = decodeChangeset(concatChangesets(a,b));
  assert.equal(result[0].name, "TaBle");
  assert.deepEqual(result[0].changes, [insert([1n,"b"])]);
  for (const other of [
    bytes(insert([1n,"b",null]), [1,0,0], "table"),
    bytes(insert([1n,"b"]), [0,1], "table"),
    bytes(insert([1n,"b"]), [2,0], "table"),
  ]) assert.throws(() => concatChangesets(a,other), ChangesetGroupError);
});

test("duplicate key records within one input also follow changegroup composition", () => {
  const a = encodeChangeset([table([
    insert([1n,"a"]), update([1n,"a"], [undefined,"b"]), remove([1n,"b"]), insert([1n,"final"]),
  ])]);
  assert.deepEqual(changes(concatChangesets(a,empty())), [insert([1n,"final"])]);
});

test("input and final-output budgets are enforced without mutating either input", () => {
  const a = bytes(insert([1n,"a"]));
  const b = bytes(insert([2n,"b"]));
  const originalA = a.slice(), originalB = b.slice();
  for (const policy of [{maxChanges:1}, {maxCells:3}, {maxBytes:a.length}]) {
    assert.throws(() => concatChangesets(a,b,policy), limitError);
  }
  assert.throws(() => concatChangesets(a, bytes(insert([2n,"b"]),[1,0],"u"), {maxTables:1}), limitError);
  assert.throws(() => concatChangesets(a,b,{maxBytes:1}), limitError);
  assert.throws(() => concatChangesets(a,b,{maxCells:0}), {code:"ERR_FSQLITE_CHANGESET_INPUT"});
  // A cancellation can fit the output budget without charging the sum of both
  // input lengths as if it were retained network output.
  assert.deepEqual(concatChangesets(a,bytes(remove([1n,"a"])),{maxBytes:a.length,maxChanges:1}),empty());
  assert.deepEqual(a,originalA);
  assert.deepEqual(b,originalB);
});

test("limits getters are captured once and malformed second inputs never yield partial output", () => {
  const counts = new Map();
  const policy = {};
  for (const name of ["maxBytes","maxTables","maxColumns","maxChanges","maxCells"]) {
    Object.defineProperty(policy,name,{get(){counts.set(name,(counts.get(name) ?? 0)+1); return undefined;}});
  }
  const a=bytes(insert([1n,"a"]));
  assert.deepEqual(concatChangesets(a,empty(),policy),a);
  assert.ok([...counts.values()].every((n)=>n===1));
  for (const b of [a.slice(0,-1),new Uint8Array([80]),new Uint8Array(new SharedArrayBuffer(0))]) {
    assert.throws(()=>concatChangesets(a,b));
  }
  const storage=new Uint8Array(a.length+10);
  storage.set(a,5);
  const output=concatChangesets(storage.subarray(5,5+a.length),empty());
  storage.fill(0);
  assert.deepEqual(output,a);
});

test("deterministic multi-session histories match native Session aggregation, application and inversion", (t) => {
  let state=0x63a5c91f;
  const random=()=>{state^=state<<13;state^=state>>>17;state^=state<<5;return state>>>0;};
  const values=[null,"", "λ\0🚀",new Uint8Array(),new Uint8Array([0,255]),0n,1n,-1n,(1n<<63n)-1n,1.5,-Infinity];
  for(let run=0;run<24;run++) {
    const steps=Array.from({length:12},()=>db=>{
      for(let n=0;n<8;n++) {
        const id=1+random()%16;
        const value=values[random()%values.length];
        switch(random()%4) {
          case 0:db.prepare("INSERT OR IGNORE INTO t VALUES(?,?,?)").run(id,value,value);break;
          case 1:db.prepare("UPDATE t SET a=? WHERE id=?").run(value,id);break;
          case 2:db.prepare("UPDATE t SET b=? WHERE id=?").run(value,id);break;
          case 3:db.prepare("DELETE FROM t WHERE id=?").run(id);break;
        }
      }
    });
    nativeHistory(t,"CREATE TABLE t(id INTEGER PRIMARY KEY,a,b)","INSERT INTO t VALUES(1,'initial',7),(2,NULL,X'00')",steps);
  }
});
