// Production SDK/worker with an independent native SQLite slot-binding oracle.
// Python ctypes supplies the reference core, NOT FrankenSQLite WASM.

import assert from "node:assert/strict";
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { sqliteBindingFixture } from "../../worker/tests/helpers/bindings-core.mjs";
import { FrankenDB } from "../src/database.ts";
import { FrankenSQLiteError } from "../src/errors.ts";

async function fixture(t, options = {}, path = ":memory:") {
  const f = sqliteBindingFixture(path);
  t.after(() => f.shutdown());
  const db = await FrankenDB.open({ worker: f.worker, ...options });
  return { ...f, db };
}
function causes(error) {
  return [error, ...(error?.errors ?? []).flatMap(causes), ...(error?.cause ? [error.cause] : [])];
}
const code = (expected) => (error) =>
  causes(error).some((e) => e instanceof FrankenSQLiteError && e.code === expected);
const arity = code("ERR_FSQLITE_BINDING_ARITY");
const ownership = code("ERR_FSQLITE_TRANSACTION_OWNERSHIP");

test("named execute/query work through the public SDK and preserve original labels", async (t) => {
  const f = await fixture(t);
  await f.db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY,v)");
  assert.equal(await f.db.execute("INSERT INTO t VALUES(:id,@v)", { id: 1, v: "hello" }), 1);
  const r = await f.db.query("SELECT :x,:x,@x,?5,$tail", { ":x": 7, "@x": 9, "?5": 11, tail: 13 });
  assert.deepEqual(r.rowArrays, [[7, 7, 9, 11, 13]]);
  assert.deepEqual(r.columns, [":x", ":x", "@x", "?5", "$tail"]);
  assert.deepEqual((await f.db.query("SELECT * FROM t")).rowArrays, [[1, "hello"]]);
  await f.db.close();
});

test("bound statements can be reused, overridden once, rebound and cleared", async (t) => {
  const f = await fixture(t),
    s = await f.db.prepare("SELECT :v AS v,?3 AS n");
  assert.equal(s.parameterCount, 3);
  assert.deepEqual(s.parameterNames, [":v", null, "?3"]);
  assert.ok(Object.isFrozen(s.parameterNames));
  assert.equal(await s.bind({ v: "saved", "?3": 8 }), s);
  assert.deepEqual(await s.get(), { v: "saved", n: 8 });
  assert.deepEqual(await s.all(["override", null, 10]), [{ v: "override", n: 10 }]);
  assert.deepEqual((await s.query()).rowArrays, [["saved", 8]]);
  assert.deepEqual((await s.query({ v: "once", "?3": 11 })).rowArrays, [["once", 11]]);
  assert.deepEqual(await s.get(), { v: "saved", n: 8 });
  await s.bind(["next", null, 12]);
  assert.deepEqual(await s.get(), { v: "next", n: 12 });
  await s.clearBindings();
  await assert.rejects(s.get(), arity);
  // Legacy query/execute retain the core's unbound-NULL behavior.
  assert.deepEqual((await s.query()).rowArrays, [[null, null]]);
  await s.finalize();
  await f.db.close();
});

test("get is undefined for no rows, all is an array and run returns affected rows", async (t) => {
  const f = await fixture(t);
  await f.db.execute("CREATE TABLE t(v)");
  const insert = await f.db.prepare("INSERT INTO t VALUES(:v)"),
    read = await f.db.prepare("SELECT v FROM t WHERE v>=:min ORDER BY v");
  await insert.bind({ v: 1 });
  assert.equal(await insert.run(), 1);
  assert.equal(await insert.execute({ v: 2 }), 1);
  assert.deepEqual(await read.all({ min: 0 }), [{ v: 1 }, { v: 2 }]);
  assert.deepEqual(await read.get({ min: 0 }), { v: 1 });
  assert.equal(await read.get({ min: 3 }), undefined);
  assert.deepEqual(await read.all({ min: 3 }), []);
  await Promise.all([insert.finalize(), read.finalize()]);
  await f.db.close();
});

test("get consumes RETURNING exactly once to completion, not just the first write", async (t) => {
  const f = await fixture(t);
  await f.db.execute("CREATE TABLE t(v)");
  const s = await f.db.prepare("INSERT INTO t VALUES(:a),(:b) RETURNING v");
  const before = f.requests.length;
  assert.deepEqual(await s.get({ a: 1, b: 2 }), { v: 1 });
  assert.equal(f.requests.slice(before).filter((r) => r.op === "query").length, 1);
  assert.deepEqual((await f.db.query("SELECT v FROM t ORDER BY v")).rowArrays, [[1], [2]]);
  await s.finalize();
  await f.db.close();
});

test("strict APIs reject missing/extra slots and invalid types before executing SQL", async (t) => {
  const f = await fixture(t),
    s = await f.db.prepare("SELECT :v");
  for (const values of [[], [1, 2], {}]) {
    for (const method of ["bind", "get", "all", "run"]) {
      const before = f.requests.length;
      await assert.rejects(s[method](values), arity);
      assert.equal(f.requests.length, before);
    }
  }
  for (const values of [[undefined], [{}], [1n << 63n], null]) {
    await assert.rejects(s.bind(values), (e) => e instanceof FrankenSQLiteError && !e.transient);
  }
  await s.bind({ v: 4 });
  await assert.rejects(s.bind({ wrong: 8 }), code("ERR_FSQLITE_BINDING_NAME"));
  assert.deepEqual(await s.get(), { ":v": 4 });
  await s.finalize();
  await f.db.close();
});

test("ambiguous prefixes and duplicate aliases reject without replacing prior values", async (t) => {
  const f = await fixture(t),
    s = await f.db.prepare("SELECT :v AS a,@v AS b");
  await s.bind({ ":v": 1, "@v": 2 });
  await assert.rejects(s.bind({ v: 3 }), code("ERR_FSQLITE_BINDING_NAME"));
  await assert.rejects(s.bind({ ":v": 1, v: 3, "@v": 2 }), code("ERR_FSQLITE_BINDING_NAME"));
  assert.deepEqual(await s.get(), { a: 1, b: 2 });
  await s.finalize();
  await f.db.close();
});

test("bind copies reused blob buffers and source maps without later mutation leakage", async (t) => {
  const f = await fixture(t),
    s = await f.db.prepare("SELECT :b AS b,:v AS v");
  const backing = Uint8Array.of(99, 1, 2, 99),
    values = { b: backing.subarray(1, 3), v: "before" };
  await s.bind(values);
  backing.fill(8);
  values.v = "after";
  const row = await s.get();
  assert.deepEqual([...row.b], [1, 2]);
  assert.equal(row.v, "before");
  row.b[0] = 77;
  assert.deepEqual([...(await s.get()).b], [1, 2]);
  await s.finalize();
  await f.db.close();
});

test("an already-dispatched call retains its binding across immediate rebind", async (t) => {
  const f = await fixture(t),
    s = await f.db.prepare("SELECT :v AS v");
  await s.bind({ v: "old" });
  const first = s.get();
  const changed = s.bind({ v: "new" });
  assert.deepEqual(await first, { v: "old" });
  await changed;
  assert.deepEqual(await s.get(), { v: "new" });
  await s.finalize();
  await f.db.close();
});

test("aliased blobs are copied once and numbered expansion remains byte-bounded", async (t) => {
  const f = await fixture(t, { requestLimits: { maxPendingBytes: 2048 } });
  const s = await f.db.prepare("SELECT :a AS a,:b AS b"),
    raw = new Uint8Array(1024);
  raw[0] = 42;
  await s.bind({ a: raw, b: raw });
  raw.fill(7);
  const row = await s.get();
  assert.equal(row.a[0], 42);
  assert.equal(row.b[0], 42);
  await assert.rejects(
    s.bind({ a: new Uint8Array(4096).subarray(0, 1), b: null }),
    code("ERR_FSQLITE_REQUEST_TOO_LARGE"),
  );
  assert.equal((await s.get()).a[0], 42);
  const high = await f.db.prepare("SELECT ?1000");
  await assert.rejects(high.bind({ "?1000": 1 }), code("ERR_FSQLITE_REQUEST_TOO_LARGE"));
  assert.equal(f.db.requestQueue.pendingRequests, 0);
  await s.finalize();
  await high.finalize();
  await f.db.close();
});

test("prototype keys, Unicode, embedded NUL, i64 and empty blobs retain exact SQL values", async (t) => {
  const f = await fixture(t, { resultEncoding: "binary" }),
    s = await f.db.prepare('SELECT :__proto__ AS "__proto__", :世界 AS v, :n AS n, :b AS b');
  const values = Object.create(null);
  Object.assign(values, {
    __proto__: "unused",
    世界: "🦀\0tail",
    n: 9223372036854775807n,
    b: new Uint8Array(),
  });
  values.__proto__ = "own";
  await s.bind(values);
  const row = await s.get();
  assert.ok(Object.hasOwn(row, "__proto__"));
  assert.equal(row.__proto__, "own");
  assert.equal(Object.getPrototypeOf(row), Object.prototype);
  assert.equal(row.v, "🦀\0tail");
  assert.equal(row.n, 9223372036854775807n);
  assert.deepEqual(row.b, new Uint8Array());
  await s.finalize();
  await f.db.close();
});

test("scoped binding validation participates in rollback even when the caller catches it", async (t) => {
  const f = await fixture(t);
  await f.db.execute("CREATE TABLE t(v)");
  await assert.rejects(
    f.db.transaction(async (tx) => {
      const s = await tx.prepare("INSERT INTO t VALUES(:v)");
      await s.bind({ v: 1 });
      await s.run();
      await assert.rejects(s.bind({}), arity);
    }),
    arity,
  );
  assert.deepEqual((await f.db.query("SELECT * FROM t")).rowArrays, []);
  await f.db.close();
});

test("child rollback preserves parent bindings and blocks parent bind/clear/get until release", async (t) => {
  const f = await fixture(t);
  await f.db.execute("CREATE TABLE t(v)");
  await f.db.transaction(async (parent) => {
    const s = await parent.prepare("INSERT INTO t VALUES(:v)");
    await s.bind({ v: 1 });
    await s.run();
    await assert.rejects(
      parent.transaction(async (child) => {
        await assert.rejects(s.bind({ v: 9 }), ownership);
        await assert.rejects(s.clearBindings(), ownership);
        await assert.rejects(s.run(), ownership);
        await child.execute("INSERT INTO t VALUES(:v)", { v: 2 });
        throw new Error("child rollback");
      }),
      /child rollback/,
    );
    await s.run();
  });
  assert.deepEqual((await f.db.query("SELECT * FROM t")).rowArrays, [[1], [1]]);
  await f.db.close();
});

test("escaped, finalized, cancelled and closed handles reject local bind/clear as well as SQL", async (t) => {
  const f = await fixture(t),
    escaped = await f.db.transaction((tx) => tx.prepare("SELECT :v"));
  for (const method of ["bind", "clearBindings", "get", "all", "run"])
    await assert.rejects(escaped[method]({ v: 1 }), code("ERR_FSQLITE_TRANSACTION_CLOSED"));
  const s = await f.db.prepare("SELECT :v");
  await s.bind({ v: 1 });
  await s.finalize();
  for (const method of ["bind", "clearBindings", "get"])
    await assert.rejects(s[method]({ v: 2 }), /finalized/);
  const controller = new AbortController();
  await assert.rejects(
    f.db.transaction(
      async (tx) => {
        const owned = await tx.prepare("SELECT :v");
        controller.abort();
        await assert.rejects(owned.bind({ v: 1 }), code("ERR_FSQLITE_TRANSACTION_CANCELLED"));
      },
      { signal: controller.signal },
    ),
    code("ERR_FSQLITE_TRANSACTION_CANCELLED"),
  );
  const live = await f.db.prepare("SELECT :v");
  await f.db.close();
  await assert.rejects(live.bind({ v: 1 }), /disposed|closing/);
  await assert.rejects(live.clearBindings(), /disposed|closing/);
});

test("binding getters cannot change ownership before a local binding is installed", async (t) => {
  const f = await fixture(t),
    s = await f.db.prepare("SELECT :v AS v");
  await s.bind({ v: 1 });
  let release;
  const gate = new Promise((resolve) => {
    release = resolve;
  });
  let transaction;
  await assert.rejects(
    s.bind({
      get v() {
        transaction = f.db.transaction(() => gate);
        return 9;
      },
    }),
    ownership,
  );
  release();
  await transaction;
  assert.deepEqual(await s.get(), { v: 1 });
  await s.finalize();
  await f.db.close();
});

test("getters that finalize or close during capture never dispatch an extra query", async (t) => {
  for (const closing of [false, true]) {
    const f = await fixture(t),
      s = await f.db.prepare("SELECT :v");
    let cleanup;
    const before = f.requests.filter((r) => r.op === "query").length;
    await assert.rejects(
      s.get({
        get v() {
          cleanup = closing ? f.db.close() : s.finalize();
          return 1;
        },
      }),
      /finalized|closing|disposed/,
    );
    await cleanup;
    assert.equal(f.requests.filter((r) => r.op === "query").length, before);
    if (!closing) await f.db.close();
  }
});

test("named SQL works in atomic streaming imports and scoped prepared batches", async (t) => {
  const f = await fixture(t);
  await f.db.execute("CREATE TABLE t(id PRIMARY KEY,v)");
  const inserted = await f.db.executeStream(
    "INSERT INTO t VALUES(:id,:v)",
    Array.from({ length: 600 }, (_, i) => [i, `v${i}`]),
    { batchSize: 17 },
  );
  assert.equal(inserted.executions, 600);
  await f.db.transaction(async (tx) => {
    const s = await tx.prepare("INSERT INTO t VALUES(:id,:v)");
    await s.executeMany([
      [600, "v600"],
      [601, "v601"],
    ]);
    assert.equal(
      (await tx.query("SELECT count(*) n FROM t WHERE id>=:min", { min: 0 })).rows[0].n,
      602,
    );
  });
  await assert.rejects(
    f.db.executeStream(
      "INSERT INTO t VALUES(:id,:v)",
      [
        [700, "discard"],
        [0, "duplicate"],
      ],
      { batchSize: 1 },
    ),
  );
  assert.equal((await f.db.query("SELECT count(*) n FROM t")).rows[0].n, 602);
  await f.db.close();
});

test("named writes and rollback are verified by a fresh SQLite connection to the same file", async (t) => {
  const path = join(await mkdtemp(join(tmpdir(), "fsqlite-bindings-")), "database.sqlite");
  const f = await fixture(t, {}, path);
  await f.db.execute("CREATE TABLE t(v)");
  await f.db.transaction((tx) => tx.execute("INSERT INTO t VALUES(:v)", { v: "committed" }));
  await assert.rejects(
    f.db.transaction(async (tx) => {
      await tx.execute("INSERT INTO t VALUES(:v)", { v: "rolled back" });
      throw new Error("stop");
    }),
  );
  const reader = await fixture(t, {}, path);
  assert.deepEqual((await reader.db.query("SELECT * FROM t")).rowArrays, [["committed"]]);
  assert.deepEqual((await reader.db.query("PRAGMA integrity_check")).rowArrays, [["ok"]]);
  await reader.db.close();
  await f.db.close();
});

test("opaque parameter suffixes and BOM do not confuse managed transaction preflight", async (t) => {
  const f = await fixture(t);
  const values = { "$a(semi;colon)": 7, "@b('quote)": 8, x: 9 };
  await f.db.transaction(async (tx) => {
    const sql = "SELECT $a(semi;colon) AS a,@b('quote) AS b,\ufeff$x AS x";
    assert.deepEqual((await tx.query(sql, values)).rowArrays, [[7, 8, 9]]);
    const s = await tx.prepare(sql);
    await s.bind(values);
    assert.deepEqual(await s.get(), { a: 7, b: 8, x: 9 });
  });
  await assert.rejects(
    f.db.transaction((tx) => tx.executeBatch("SELECT $a(ignored;COMMIT); COMMIT;")),
    code("ERR_FSQLITE_TRANSACTION_SQL"),
  );
  await f.db.close();
});
