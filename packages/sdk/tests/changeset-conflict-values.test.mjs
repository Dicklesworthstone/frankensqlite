// Production applyChangeset/applyPatchset against actual SQLite transactions.
// The independent C Session oracle reads sqlite3changeset_conflict, not SDK state.
// This does not qualify the Rust engine, the browser worker, or physical durability.
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import { applyChangeset, applyPatchset, CHANGESET_RECEIPTS_TABLE } from "../src/changeset-apply.ts";
import { encodeChangeset, encodePatchset, decodeRebaseInfo } from "../src/changeset-codec.ts";

const insert = (...values) => ({ operation: "insert", indirect: false, new: values });
const update = (old, next) => ({ operation: "update", indirect: false, old, new: next });
const remove = (...values) => ({ operation: "delete", indirect: false, old: values });
const wire = (changes, pk = [1, 0], name = "t", encode = encodeChangeset) =>
  encode([{ name, primaryKey: pk, changes }]);
const limitError = { code: "ERR_FSQLITE_CHANGESET_LIMIT" };
const resultError = { code: "ERR_FSQLITE_CHANGESET_RESULT" };
const conflictError = { code: "ERR_FSQLITE_CHANGESET_CONFLICT" };
const quote = name => `"${name.replaceAll('"', '""')}"`;
const costQuery = sql => sql.includes(" AS __fsqlite_conflict_cost");
const valueQuery = sql => sql.startsWith("SELECT typeof(");

function target(t, schema, seed = "") {
  const db = new DatabaseSync(":memory:");
  db.exec(schema + ";" + seed);
  t.after(() => db.close());
  const owner = {
    db, active: false, commits: 0, rollbacks: 0, queries: [],
    afterQuery: undefined,
    async transaction(work) {
      assert.equal(this.active, false);
      this.active = true;
      db.exec("BEGIN");
      const tx = {
        execute: async (sql, params = []) => {
          assert.equal(this.active, true);
          return Number(db.prepare(sql).run(...params).changes);
        },
        query: async (sql, params = []) => {
          assert.equal(this.active, true);
          this.queries.push(sql);
          const statement = db.prepare(sql);
          statement.setReadBigInts(true);
          const rows = statement.all(...params).map(row => Object.values(row));
          return { rowArrays: this.afterQuery ? await this.afterQuery(sql, rows) : rows };
        },
      };
      try {
        const value = await work(tx);
        db.exec("COMMIT"); this.commits++;
        return value;
      } catch (error) {
        db.exec("ROLLBACK"); this.rollbacks++;
        throw error;
      } finally { this.active = false; }
    },
  };
  return owner;
}
function rows(owner, sql = "SELECT * FROM t ORDER BY 1") {
  const statement = owner.db.prepare(sql);
  statement.setReadBigInts(true);
  return statement.all().map(row => Object.values(row));
}
function receiptCount(owner) {
  const exists = owner.db.prepare("SELECT 1 FROM sqlite_schema WHERE name=?").get(CHANGESET_RECEIPTS_TABLE);
  return exists ? owner.db.prepare(`SELECT count(*) n FROM ${CHANGESET_RECEIPTS_TABLE}`).get().n : 0;
}
function typed(value) {
  if (value === null) return { null: true };
  if (typeof value === "bigint") return { integer: String(value) };
  if (typeof value === "number") {
    const bits = new DataView(new ArrayBuffer(8)); bits.setFloat64(0, value);
    return { real: Buffer.from(bits.buffer).toString("hex") };
  }
  if (typeof value === "string") return { text: value };
  if (value instanceof Uint8Array) return { blob: Buffer.from(value).toString("hex") };
  assert.fail("Current receiver image contains an undefined or unsupported value");
}
function normalized(conflict) {
  return { kind: conflict.kind, current: conflict.current === null ? null : conflict.current.map(typed) };
}

const ORACLE = String.raw`
import ctypes as C, ctypes.util, json, struct, sys
lib = C.CDLL(ctypes.util.find_library('sqlite3'))
P, I, S = C.c_void_p, C.c_int, C.c_char_p
for name, result, args in [
 ('sqlite3_libversion', S, []), ('sqlite3_open', I, [S,C.POINTER(P)]),
 ('sqlite3_close', I, [P]), ('sqlite3_errmsg', S, [P]),
 ('sqlite3_exec', I, [P,S,P,P,P]), ('sqlite3changeset_apply', I, [P,I,P,P,P,P]),
 ('sqlite3changeset_op', I, [P,C.POINTER(S),C.POINTER(I),C.POINTER(I),C.POINTER(I)]),
 ('sqlite3changeset_conflict', I, [P,I,C.POINTER(P)]),
 ('sqlite3_value_type', I, [P]), ('sqlite3_value_int64', C.c_int64, [P]),
 ('sqlite3_value_double', C.c_double, [P]), ('sqlite3_value_text', P, [P]),
 ('sqlite3_value_blob', P, [P]), ('sqlite3_value_bytes', I, [P]),
]:
 f = getattr(lib, name); f.restype = result; f.argtypes = args

def check(rc, db=None):
 if rc: raise RuntimeError(f'SQLite {rc}: {lib.sqlite3_errmsg(db).decode() if db else ""}')
def value(it, index):
 p = P(); check(lib.sqlite3changeset_conflict(it, index, C.byref(p)))
 k = lib.sqlite3_value_type(p)
 if k == 1: return {'integer': str(lib.sqlite3_value_int64(p))}
 if k == 2: return {'real': struct.pack('>d',lib.sqlite3_value_double(p)).hex()}
 if k == 5: return {'null': True}
 data = lib.sqlite3_value_text(p) if k == 3 else lib.sqlite3_value_blob(p)
 n = lib.sqlite3_value_bytes(p); b = C.string_at(data,n) if n else b''
 return {'text': b.decode('utf-8')} if k == 3 else {'blob': b.hex()}
results = []
for case in json.load(sys.stdin):
 db = P(); check(lib.sqlite3_open(b':memory:',C.byref(db)))
 try:
  check(lib.sqlite3_exec(db,case['sql'].encode(),None,None,None),db)
  seen = []; failures = []
  @C.CFUNCTYPE(I,P,I,P)
  def callback(_, kind, it):
   try:
    name,n,op,indirect = S(),I(),I(),I()
    check(lib.sqlite3changeset_op(it,C.byref(name),C.byref(n),C.byref(op),C.byref(indirect)))
    seen.append({'kind':{1:'data',2:'not-found',3:'conflict'}[kind],
                 'current':None if kind == 2 else [value(it,i) for i in range(n.value)]})
    return 0
   except Exception as error: failures.append(str(error)); return 2
  data = bytes.fromhex(case['wire']); buf = C.create_string_buffer(data)
  check(lib.sqlite3changeset_apply(db,len(data),buf,None,callback,None),db)
  if failures: raise RuntimeError(failures)
  results.append(seen)
 finally: check(lib.sqlite3_close(db))
print(json.dumps({'sqlite':lib.sqlite3_libversion().decode(),'results':results}))
`;
function nativeOracle(t, cases) {
  const result = spawnSync("python3", ["-c", ORACLE], {
    input: JSON.stringify(cases.map(c => ({ sql: c.sql, wire: Buffer.from(c.wire).toString("hex") }))),
    encoding: "utf8", timeout: 30_000, maxBuffer: 4 * 1024 * 1024,
  });
  assert.ifError(result.error);
  assert.equal(result.status, 0, result.stderr);
  const output = JSON.parse(result.stdout);
  t.diagnostic(`Independent sqlite3changeset_conflict oracle: SQLite ${output.sqlite}`);
  return output.results;
}

test("replica policy can compare its actual version with the incoming revision and receipt replay does not run it again", async t => {
  const schema = "CREATE TABLE t(id INTEGER PRIMARY KEY, body TEXT, revision INTEGER)";
  const seed = "INSERT INTO t VALUES(1,'local-newer',12),(2,'local-older',2)";
  const owner = target(t, schema, seed);
  const payload = wire([
    update([1n, "base", 0n], [undefined, "remote", 4n]),
    update([2n, "base", 0n], [undefined, "remote", 4n]),
  ], [1, 0, 0]);
  const observed = [];
  const options = { tables: ["t"], deliveryId: "source:revision-4", onConflict: async conflict => {
    assert.equal(owner.active, true);
    assert.deepEqual(conflict.columns, ["id", "body", "revision"]);
    observed.push(normalized(conflict));
    await Promise.resolve();
    return conflict.current[2] > conflict.change.new[2] ? "omit" : "replace";
  } };
  assert.deepEqual(await applyChangeset(owner, payload, options), { applied: 1, omitted: 1, replayed: false });
  assert.deepEqual(rows(owner), [[1n, "local-newer", 12n], [2n, "remote", 4n]]);
  assert.equal(receiptCount(owner), 1);
  const count = owner.queries.filter(costQuery).length;
  assert.deepEqual(await applyChangeset(owner, payload, options), { applied: 1, omitted: 1, replayed: true });
  assert.equal(observed.length, 2);
  assert.equal(owner.queries.filter(costQuery).length, count);
  assert.deepEqual(observed, nativeOracle(t, [{ sql: `${schema};${seed}`, wire: payload }])[0]);
});

test("current images preserve exact storage classes, untouched values and match native Session conflict inspection", async t => {
  const schema = "CREATE TABLE t(id PRIMARY KEY, changed, huge, integral_real, text_value, bytes, null_value, infinity)";
  const seed = "INSERT INTO t VALUES(-9223372036854775808,'local',9223372036854775807,1.0," +
    "CAST(x'efbbbfcebb00f09f9a80' AS TEXT),x'00ff',NULL,1e999)";
  const owner = target(t, schema, seed);
  const payload = wire([update([-(1n << 63n), "expected", undefined, undefined, undefined, undefined, undefined, undefined],
    [undefined, "remote", undefined, undefined, undefined, undefined, undefined, undefined])], [1,0,0,0,0,0,0,0]);
  const seen = [];
  await applyChangeset(owner, payload, { tables: ["t"], onConflict: conflict => {
    assert.equal(Object.isFrozen(conflict), true);
    assert.equal(Object.isFrozen(conflict.current), true);
    assert.deepEqual(conflict.current, [-(1n << 63n), "local", (1n << 63n)-1n, 1, "\uFEFFλ\0🚀", new Uint8Array([0,255]), null, Infinity]);
    assert.equal(typeof conflict.current[3], "number");
    assert.equal(typeof conflict.current[2], "bigint");
    seen.push(normalized(conflict));
    return "omit";
  } });
  assert.deepEqual(seen, nativeOracle(t, [{ sql: `${schema};${seed}`, wire: payload }])[0]);
});

test("all three full-changeset conflict kinds and both patchset kinds expose native current-row semantics", async t => {
  const cases = [
    { encode: encodeChangeset, apply: applyChangeset, change: insert(1n, "incoming"), seed: "INSERT INTO t VALUES(1,'local')" },
    { encode: encodeChangeset, apply: applyChangeset, change: remove(1n, "expected"), seed: "INSERT INTO t VALUES(1,'local')" },
    { encode: encodeChangeset, apply: applyChangeset, change: update([1n,"expected"],[undefined,"incoming"]), seed: "" },
    { encode: encodeChangeset, apply: applyChangeset, change: remove(1n,"expected"), seed: "" },
    { encode: encodePatchset, apply: applyPatchset, change: insert(1n, "incoming"), seed: "INSERT INTO t VALUES(1,'local')" },
    { encode: encodePatchset, apply: applyPatchset, change: remove(1n,undefined), seed: "" },
    { encode: encodePatchset, apply: applyPatchset, change: update([1n,undefined],[undefined,"incoming"]), seed: "" },
  ];
  const schema = "CREATE TABLE t(id PRIMARY KEY, v)";
  const observed = [];
  for (const scenario of cases) {
    const owner = target(t, schema, scenario.seed), seen = [];
    scenario.wire = wire([scenario.change], [1,0], "t", scenario.encode);
    await scenario.apply(owner, scenario.wire, { tables: ["t"], onConflict: conflict => {
      seen.push(normalized(conflict)); return "omit";
    } });
    assert.equal(seen.length, 1);
    if (scenario.seed === "") {
      assert.deepEqual(seen, [{ kind: "not-found", current: null }]);
      assert.equal(owner.queries.some(costQuery), false);
    }
    observed.push(seen);
  }
  assert.deepEqual(observed, nativeOracle(t, cases.map(c => ({ sql: `${schema};${c.seed}`, wire: c.wire }))));
});

test("callback mutation of owned receiver/incoming BLOBs cannot alter the replacement key or rebase evidence", async t => {
  const owner = target(t, "CREATE TABLE t(id BLOB PRIMARY KEY,v BLOB)", "INSERT INTO t VALUES(x'0102',x'03')");
  let saved;
  const payload = wire([insert(new Uint8Array([1,2]), new Uint8Array([8,9]))]);
  const result = await applyChangeset(owner, payload, { tables: ["t"], deliveryId: "blobs:1",
    onConflict: async conflict => {
      saved = conflict.current;
      assert.deepEqual(saved, [new Uint8Array([1,2]), new Uint8Array([3])]);
      assert.throws(() => saved.push(null), TypeError);
      saved[0].fill(255); saved[1].fill(255);
      conflict.change.new[0].fill(0); conflict.change.new[1].fill(0);
      await Promise.resolve(); return "replace";
    }, onRebase: async (tx, bytes) => {
      const records = decodeRebaseInfo(bytes);
      assert.deepEqual(records[0].changes[0].values, [new Uint8Array([1,2]),new Uint8Array([8,9])]);
      const current = await tx.query("SELECT hex(id),hex(v) FROM t");
      assert.deepEqual(current.rowArrays, [["0102","0809"]]);
    },
  });
  assert.deepEqual(result, { applied: 1, omitted: 0, replayed: false });
  assert.deepEqual(rows(owner, "SELECT hex(id),hex(v) FROM t"), [["0102","0809"]]);
  assert.equal(receiptCount(owner), 1);
  assert.deepEqual(saved, [new Uint8Array([255,255]),new Uint8Array([255])]);
});

test("oversized receiver rows are refused before their payload is fetched and roll back earlier writes and receipts", async t => {
  const owner = target(t, "CREATE TABLE t(id PRIMARY KEY,v)", "INSERT INTO t VALUES(2,zeroblob(4096))");
  const payload = wire([insert(1n,"first"),insert(2n,"conflict")]);
  assert.ok(payload.length < 256);
  let callbacks = 0;
  await assert.rejects(applyChangeset(owner, payload, { tables: ["t"], deliveryId: "bounded:1",
    limits: { maxBytes: 256 }, onConflict: () => { callbacks++; return "omit"; },
  }), limitError);
  assert.equal(callbacks, 0);
  assert.equal(owner.queries.some(costQuery), true);
  assert.equal(owner.queries.some(valueQuery), false, "no unbounded receiver payload result");
  assert.deepEqual(rows(owner, "SELECT id,length(v) FROM t"), [[2n,4096n]]);
  assert.equal(receiptCount(owner), 0);
  assert.equal(owner.rollbacks, 1);
});

test("receiver image accounting admits the exact byte boundary and rejects one byte less", async t => {
  const payload = wire([insert(1n,"remote")]);
  for (const maximum of [136,135]) {
    const owner = target(t, "CREATE TABLE t(id PRIMARY KEY,v)", "INSERT INTO t VALUES(1,zeroblob(128))");
    let seen = false;
    const attempt = applyChangeset(owner, payload, { tables: ["t"], limits: { maxBytes: maximum },
      onConflict: conflict => { seen = true; assert.equal(conflict.current[1].length,128); return "omit"; },
    });
    if (maximum === 136) { await attempt; assert.equal(seen,true); }
    else { await assert.rejects(attempt,limitError); assert.equal(seen,false); }
  }
});

test("wide receiver rows are complete even when a single doubled projection would exceed SQLite's result-column limit", async t => {
  const count = 1100;
  const columns = Array.from({length: count}, (_, i) => `c${i}`);
  const owner = target(t, `CREATE TABLE t(${columns.map((c,i) => `${c}${i === 0 ? " PRIMARY KEY" : ""}`).join(",")})`,
    `INSERT INTO t VALUES(${columns.map((_,i) => i).join(",")})`);
  // Point at the existing key; retain every otherwise conflicting non-key field.
  const input = wire([insert(0n,...columns.slice(1).map(() => "new"))], columns.map((_,i) => i === 0 ? 1 : 0));
  let seen = false;
  await applyChangeset(owner, input, { tables: ["t"], onConflict: conflict => {
    seen = true;
    assert.deepEqual(conflict.columns, columns);
    assert.deepEqual(conflict.current, columns.map((_,i) => BigInt(i)));
    return "omit";
  } });
  assert.equal(seen, true);
  assert.equal(owner.queries.filter(valueQuery).length, Math.ceil(count/128));
});

test("UTF-8 and both UTF-16 databases retain BOMs, embedded NULs and supplementary characters", async t => {
  for (const encoding of ["UTF-8","UTF-16le","UTF-16be"]) {
    const owner = target(t, `PRAGMA encoding='${encoding}'; CREATE TABLE t(id PRIMARY KEY,v TEXT)`);
    const expected = "\uFEFFλ\0🚀";
    owner.db.prepare("INSERT INTO t VALUES(?,?)").run(1,expected);
    let seen;
    await applyChangeset(owner, wire([insert(1n,"new")]), { tables: ["t"], onConflict: c => {
      seen = c.current; return "omit";
    } });
    // Node bound the typeless key as REAL. Numeric PK matching must not coerce
    // the receiver's actual REAL into the incoming changeset's INTEGER key.
    assert.deepEqual(seen,[1,expected],encoding);
  }
});

test("malformed receiver TEXT is not replaced with lossy text in policy or acknowledged", async t => {
  const owner = target(t, "CREATE TABLE t(id PRIMARY KEY,v)", "INSERT INTO t VALUES(1,CAST(x'ff' AS TEXT))");
  let seen = false;
  await assert.rejects(applyChangeset(owner, wire([insert(1n,"new")]), {
    tables: ["t"], deliveryId: "invalid-text:1", onConflict: () => { seen = true; return "omit"; },
  }), resultError);
  assert.equal(seen,false); assert.equal(receiptCount(owner),0);
  assert.deepEqual(rows(owner,"SELECT id,hex(v) FROM t"),[[1n,"FF"]]);
});

test("typed receiver capture rejects missing, malformed, inconsistent and unsafe adapter results before invoking policy", async t => {
  const faults = [
    (sql,r) => costQuery(sql) ? [] : r,
    (sql,r) => costQuery(sql) ? [[-1]] : r,
    (sql,r) => valueQuery(sql) ? [] : r,
    (sql,r) => valueQuery(sql) ? [r[0],r[0]] : r,
    (sql,r) => valueQuery(sql) ? [r[0].slice(1)] : r,
    (sql,r) => valueQuery(sql) ? [["integer",1n,...r[0].slice(2)]] : r,
    (sql,r) => valueQuery(sql) ? [["integer","9223372036854775808",...r[0].slice(2)]] : r,
    (sql,r) => valueQuery(sql) ? [["real",NaN,...r[0].slice(2)]] : r,
    (sql,r) => valueQuery(sql) ? [["made-up","1",...r[0].slice(2)]] : r,
    (sql,r) => valueQuery(sql) ? [[...r[0].slice(0,2),"blob",new Uint8Array(2000)]] : r,
    (sql,r) => valueQuery(sql) ? [[...r[0].slice(0,2),"blob",new Uint8Array(new SharedArrayBuffer(1))]] : r,
    (sql,r) => valueQuery(sql) ? [[...r[0].slice(0,2),"blob",new Uint8Array(new ArrayBuffer(1,{maxByteLength:2}))]] : r,
    (sql,r) => sql === "PRAGMA main.encoding" ? [["unknown"]] : r,
  ];
  for (const fault of faults) {
    const owner = target(t,"CREATE TABLE t(id PRIMARY KEY,v)","INSERT INTO t VALUES(1,x'03')");
    owner.afterQuery = fault;
    let seen = false;
    await assert.rejects(applyChangeset(owner,wire([insert(1n,"new")]),{
      tables:["t"],deliveryId:"adapter:1",onConflict:()=>{seen=true;return "omit";},
    }),resultError);
    assert.equal(seen,false); assert.equal(receiptCount(owner),0);
    assert.deepEqual(rows(owner,"SELECT id,hex(v) FROM t"),[[1n,"03"]]);
  }
});

test("shadowed result-buffer accessors never control receiver-image lengths or returned bytes", async t => {
  const owner=target(t,"CREATE TABLE t(id PRIMARY KEY,v)","INSERT INTO t VALUES(1,x'00ff07')");
  let accesses=0, seen=false;
  owner.afterQuery=(sql,r)=>{
    if(valueQuery(sql)) {
      const value=r[0][3];
      for(const name of ["length","buffer","byteOffset","byteLength","slice","subarray"])
        Object.defineProperty(value,name,{get(){accesses++;throw new Error("not storage authority");}});
      Object.defineProperty(value,Symbol.iterator,{get(){accesses++;throw new Error("not an array iterator");}});
    }
    return r;
  };
  await applyChangeset(owner,wire([insert(1n,"new")]),{tables:["t"],onConflict:c=>{
    seen=true;assert.deepEqual(c.current,[1n,new Uint8Array([0,255,7])]);return "omit";
  }});
  assert.equal(seen,true);assert.equal(accesses,0);
});

test("cancellation between receiver image queries rolls back instead of calling policy or recording delivery", async t => {
  for(const checkpoint of [costQuery,valueQuery]) {
    const owner=target(t,"CREATE TABLE t(id PRIMARY KEY,v)","INSERT INTO t VALUES(1,'local')");
    const controller=new AbortController();let seen=false;
    owner.afterQuery=(sql,r)=>{if(checkpoint(sql))controller.abort("cancel conflict capture");return r;};
    await assert.rejects(applyChangeset(owner,wire([insert(1n,"new")]),{
      tables:["t"],deliveryId:"cancel:1",signal:controller.signal,onConflict:()=>{seen=true;return "replace";},
    }),{code:"ERR_FSQLITE_CHANGESET_CANCELLED"});
    assert.equal(seen,false);assert.equal(receiptCount(owner),0);
    assert.deepEqual(rows(owner),[[1n,"local"]]);
  }
});

test("default conflict errors own useful receiver values while conflict-free writes perform no receiver-image queries", async t => {
  const owner=target(t,"CREATE TABLE t(id PRIMARY KEY,v)","INSERT INTO t VALUES(1,'local')");
  await applyChangeset(owner,wire([insert(2n,"new")]),{tables:["t"]});
  assert.equal(owner.queries.some(costQuery),false);
  assert.equal(owner.queries.some(valueQuery),false);
  let error;
  try { await applyChangeset(owner,wire([insert(1n,"new")]),{tables:["t"]}); }
  catch(e){error=e;}
  assert.equal(error?.code,conflictError.code);
  assert.deepEqual(error.conflict.current,[1n,"local"]);
  owner.db.exec("UPDATE t SET v='later'");
  assert.deepEqual(error.conflict.current,[1n,"local"]);
});

test("generated and receiver-only columns do not shift the incoming projection or leak into current values", async t => {
  const name='quoted "table';
  const owner=target(t,`CREATE TABLE ${quote(name)}(id TEXT PRIMARY KEY COLLATE NOCASE, computed AS (length(id)), value, extra DEFAULT 'local-only')`,
    `INSERT INTO ${quote(name)}(id,value) VALUES('KEY','local')`);
  let seen;
  await applyChangeset(owner,wire([insert("key","remote")],[1,0],name),{
    tables:[name],generatedColumns:"recompute",onConflict:c=>{
      seen=c.current;assert.deepEqual(c.columns,["id","value"]);return "omit";
    },
  });
  assert.deepEqual(seen,["KEY","local"]);
});

test("a later constraint failure rolls back a policy-selected replacement and its pending receipt", async t => {
  const owner=target(t,"CREATE TABLE t(id PRIMARY KEY,v UNIQUE)","INSERT INTO t VALUES(1,'local'),(2,'taken')");
  let called=0;
  await assert.rejects(applyChangeset(owner,wire([insert(1n,"replacement"),insert(3n,"taken")]),{
    tables:["t"],deliveryId:"rollback:1",onConflict:c=>{
      called++;assert.deepEqual(c.current,[1n,"local"]);return "replace";
    },
  }),/UNIQUE constraint failed/);
  assert.equal(called,1);assert.equal(receiptCount(owner),0);
  assert.deepEqual(rows(owner),[[1n,"local"],[2n,"taken"]]);
});
