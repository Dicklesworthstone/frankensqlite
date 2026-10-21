// Production order-admission SQL on reference SQLite, not Rust/WASM certification.
import assert from "node:assert/strict";
import { test } from "node:test";
import { DatabaseSync } from "node:sqlite";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createHash } from "node:crypto";
import { ChangesetOrder, CHANGESET_ORDER_TABLE } from "../src/changeset-order.ts";

const T = `"${CHANGESET_ORDER_TABLE}"`;
const HASH = bytes => createHash("sha256").update(bytes).digest("hex");
function message(sequence = 1n, value = "first", deliveryId = `source/${sequence}`) {
  const changeset = new TextEncoder().encode(value);
  return { sequence, deliveryId, sha256: HASH(changeset), changeset };
}
class SqliteTarget {
  constructor(path = ":memory:") {
    this.db = new DatabaseSync(path);
    this.db.exec("PRAGMA foreign_keys=ON; CREATE TABLE IF NOT EXISTS items(id INTEGER PRIMARY KEY, value TEXT NOT NULL)");
    this.transactions = 0;
  }
  executor() {
    return {
      execute: async (sql, params = []) => Number(this.db.prepare(sql).run(...params).changes),
      query: async (sql, params = []) => ({ rowArrays: this.db.prepare(sql).all(...params).map(row => Object.values(row)) }),
    };
  }
  async transaction(work) {
    this.transactions++;
    this.db.exec("BEGIN");
    try {
      const value = await work(this.executor());
      this.db.exec("COMMIT");
      return value;
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }
  rows() { return this.db.prepare("SELECT id,value FROM items ORDER BY id").all().map(row => Object.values(row)); }
  close() { this.db.close(); }
}
const opts = { receiverId: "replica/a", sourceId: "source/generation-1" };
async function setup(config = {}) {
  const target = new SqliteTarget();
  const order = new ChangesetOrder(target, { ...opts, ...config });
  await order.initialize();
  return { target, order };
}
function writer(id, beforeCommit = async () => {}) {
  return (scoped, bytes) => scoped.transaction(async tx => {
    await tx.execute("INSERT INTO items VALUES (?,?)", [BigInt(id), new TextDecoder().decode(bytes)]);
    await beforeCommit(tx, bytes);
    return { applied: 1, omitted: 0, replayed: false };
  });
}
const code = expected => error => error?.code === `ERR_FSQLITE_ORDER_${expected}`;

test("explicit enrollment; no automatic state creation during delivery", async () => {
  const target = new SqliteTarget(), order = new ChangesetOrder(target, opts);
  await assert.rejects(order.apply(message(), writer(1)), code("UNINITIALIZED"));
  assert.deepEqual(target.rows(), []);
  assert.equal((await order.initialize()).sequence, 0n);
  assert.equal((await order.initialize()).sequence, 0n);
  target.close();
});
test("reject reordered writes before application; recover by delivering the missing predecessor", async () => {
  const { target, order } = await setup();
  let calls = 0;
  await assert.rejects(order.apply(message(2n), async () => { calls++; return { applied: 0, omitted: 0, replayed: false }; }), code("GAP"));
  assert.equal(calls, 0);
  await order.apply(message(), writer(1));
  await order.apply(message(2n, "second"), writer(2));
  assert.deepEqual(target.rows(), [[1, "first"], [2, "second"]]);
  assert.equal((await order.head()).sequence, 2n);
  target.close();
});
test("old delivery retry returns original decisions without invoking callbacks", async () => {
  const { target, order } = await setup();
  await order.apply(message(), writer(1));
  await order.apply(message(2n, "second"), writer(2));
  const replay = await order.apply(message(), () => { throw new Error("must not run"); });
  assert.equal(replay.replayed, true);
  assert.equal(replay.applied, 1);
  assert.equal(replay.sequence, 1n);
  assert.equal((await order.head()).sequence, 2n);
  target.close();
});
for (const changed of [message(1n, "different"), message(1n, "first", "another-id"), message(2n, "first", "source/1")]) {
  test(`sequence/identity reuse is refused (${changed.sequence}/${changed.deliveryId}/${new TextDecoder().decode(changed.changeset)})`, async () => {
    const { target, order } = await setup();
    await order.apply(message(), writer(1));
    await assert.rejects(order.apply(changed, writer(2)), code("REUSE"));
    assert.deepEqual(target.rows(), [[1, "first"]]);
    target.close();
  });
}
test("lost ACK and file reopen retain exact admission and replay state", async () => {
  const path = join(mkdtempSync(join(tmpdir(), "fsqlite-order-")), "receiver.db");
  let target = new SqliteTarget(path), order = new ChangesetOrder(target, opts);
  await order.initialize();
  await order.apply(message(), writer(1));
  target.close();
  target = new SqliteTarget(path); order = new ChangesetOrder(target, opts);
  assert.equal((await order.initialize()).sequence, 1n);
  assert.equal((await order.apply(message(), () => { throw new Error("replayed writes"); })).replayed, true);
  await order.apply(message(2n, "second"), writer(2));
  assert.deepEqual(target.rows(), [[1, "first"], [2, "second"]]);
  target.close();
});
test("receiver and source bindings survive reconstruction of the owner", async () => {
  const { target, order } = await setup();
  await order.apply(message(), writer(1));
  for (const override of [{ sourceId: "other-source" }, { receiverId: "other-receiver" }]) {
    const wrong = new ChangesetOrder(target, { ...opts, ...override });
    await assert.rejects(wrong.initialize(), code("BINDING"));
    await assert.rejects(wrong.apply(message(2n), writer(2)), code("BINDING"));
  }
  assert.deepEqual(target.rows(), [[1, "first"]]);
  target.close();
});
test("row and side-effect journal failures roll back the sequence marker and all SQL", async () => {
  const { target, order } = await setup();
  target.db.exec("CREATE TABLE proof(value TEXT)");
  await assert.rejects(order.apply(message(), writer(1, async tx => {
    await tx.execute("INSERT INTO proof VALUES ('journal')");
    throw new Error("policy failure");
  })), /policy failure/);
  assert.deepEqual(target.rows(), []);
  assert.equal(target.db.prepare("SELECT count(*) AS n FROM proof").get().n, 0);
  assert.equal((await order.head()).sequence, 0n);
  await order.apply(message(), writer(1));
  target.close();
});
test("deferred COMMIT failure rolls back data AND ordering; same delivery can retry", async () => {
  const { target, order } = await setup();
  target.db.exec("CREATE TABLE parent(id INTEGER PRIMARY KEY); CREATE TABLE child(id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)");
  await assert.rejects(order.apply(message(), writer(1, tx => tx.execute("INSERT INTO child VALUES(1)"))), /FOREIGN KEY/);
  assert.deepEqual(target.rows(), []);
  assert.equal((await order.head()).sequence, 0n);
  target.db.exec("INSERT INTO parent VALUES(1)");
  await order.apply(message(), writer(1, tx => tx.execute("INSERT INTO child VALUES(1)")));
  assert.equal((await order.head()).sequence, 1n);
  target.close();
});
test("an unordered receipt cannot be retrospectively blessed into the stream", async () => {
  const { target, order } = await setup();
  await assert.rejects(order.apply(message(), async scoped => scoped.transaction(async tx => {
    await tx.execute("INSERT INTO items VALUES(1,'bad')");
    return { applied: 1, omitted: 0, replayed: true };
  })), code("REUSE"));
  assert.deepEqual(target.rows(), []);
  assert.equal((await order.head()).sequence, 0n);
  target.close();
});
test("cancellation before SQL and inside the callback never advances order", async () => {
  const { target, order } = await setup();
  const controller = new AbortController(); controller.abort();
  const before = target.transactions;
  await assert.rejects(order.apply(message(), writer(1), { signal: controller.signal }), code("CANCELLED"));
  assert.equal(target.transactions, before);
  const active = new AbortController();
  await assert.rejects(order.apply(message(), writer(1, async () => active.abort()), { signal: active.signal }), code("CANCELLED"));
  assert.deepEqual(target.rows(), []);
  assert.equal((await order.head()).sequence, 0n);
  target.close();
});
test("deadline covers application and is checked before commit", async () => {
  const { target, order } = await setup();
  await assert.rejects(order.apply(message(), writer(1, () => new Promise(resolve => setTimeout(resolve, 30))), { timeoutMs: 15 }), code("TIMEOUT"));
  assert.deepEqual(target.rows(), []);
  assert.equal((await order.head()).sequence, 0n);
  target.close();
});
test("retention limit refuses new work but still acknowledges exact retries", async () => {
  const { target, order } = await setup({ maxEntries: 1 });
  await order.apply(message(), writer(1));
  await assert.rejects(order.apply(message(2n), writer(2)), code("FULL"));
  assert.equal((await order.apply(message(), writer(2))).replayed, true);
  target.close();
});
test("payload is copied before asynchronous hashing and caller mutation", async () => {
  const { target, order } = await setup();
  const input = message();
  const pending = order.apply(input, writer(1));
  input.changeset.fill(0); input.deliveryId = "mutated";
  const saved = await pending;
  assert.equal(saved.deliveryId, "source/1");
  assert.deepEqual(target.rows(), [[1, "first"]]);
  target.close();
});
test("bad digest and oversized message are refused before entering SQL", async () => {
  const { target, order } = await setup({ maxMessageBytes: 5 });
  const before = target.transactions;
  await assert.rejects(order.apply({ ...message(), sha256: "0".repeat(64) }, writer(1)), code("INPUT"));
  await assert.rejects(order.apply(message(1n, "123456"), writer(1)), code("INPUT"));
  assert.equal(target.transactions, before);
  target.close();
});
test("shared, detached, resizable and accessor-controlled input is refused", async () => {
  const { target, order } = await setup();
  const detached = new Uint8Array([1]); structuredClone(detached.buffer, { transfer: [detached.buffer] });
  const arrays = [detached, new Uint8Array(new SharedArrayBuffer(1)), new Uint8Array(new ArrayBuffer(1, { maxByteLength: 10 }))];
  for (const changeset of arrays) await assert.rejects(order.apply({ ...message(), changeset }, writer(1)), code("INPUT"));
  let called = false;
  const input = message(); Object.defineProperty(input, "sequence", { get() { called = true; return 1n; } });
  await assert.rejects(order.apply(input, writer(1)), code("INPUT"));
  assert.equal(called, false);
  target.close();
});
test("missing middle receipt, binding row, or corrupted receipt fails closed", async () => {
  for (const sql of [`DELETE FROM ${T} WHERE seq=0`, `DELETE FROM ${T} WHERE seq=1`, `UPDATE ${T} SET sha256='bad' WHERE seq=2`]) {
    const { target, order } = await setup();
    await order.apply(message(), writer(1)); await order.apply(message(2n), writer(2));
    target.db.exec(sql);
    await assert.rejects(order.apply(message(3n), writer(3)), code("CORRUPT"));
    assert.equal(target.rows().length, 2);
    target.close();
  }
});
test("truncated last receipt cannot turn committed work into fresh work after reopen", async () => {
  const { target, order } = await setup();
  await order.apply(message(), writer(1));
  target.db.exec(`DELETE FROM ${T} WHERE seq=1`);
  const reopened = new ChangesetOrder(target, opts);
  await assert.rejects(reopened.apply(message(), writer(2)), code("CORRUPT"));
  assert.deepEqual(target.rows(), [[1, "first"]]);
  target.close();
});
test("application cannot mutate its admitted payload and retain the old digest", async () => {
  const { target, order } = await setup();
  await assert.rejects(order.apply(message(), writer(1, async (_tx, bytes) => bytes.fill(0))), code("CORRUPT"));
  assert.deepEqual(target.rows(), []);
  target.close();
});
test("application cannot tamper with the ledger or introduce metadata triggers", async () => {
  for (const sql of [`DELETE FROM ${T}`, `CREATE TRIGGER bad AFTER INSERT ON ${T} BEGIN DELETE FROM items; END`]) {
    const { target, order } = await setup();
    await assert.rejects(order.apply(message(), writer(1, tx => tx.execute(sql))));
    assert.deepEqual(target.rows(), []);
    assert.equal((await order.head()).sequence, 0n);
    target.close();
  }
});
test("concurrent calls on one owner are rejected, not silently queued or globally serialized", async () => {
  const { target, order } = await setup();
  let release, entered;
  const started = new Promise(resolve => { entered = resolve; });
  const gate = new Promise(resolve => { release = resolve; });
  const pending = order.apply(message(), writer(1, async () => { entered(); await gate; }));
  await started;
  await assert.rejects(order.apply(message(2n), writer(2)), code("BUSY"));
  release(); await pending;
  await order.apply(message(2n), writer(2));
  target.close();
});
test("escaped application target closes when the callback ends", async () => {
  const { target, order } = await setup();
  let escaped;
  await order.apply(message(), async (scoped, bytes) => { escaped = scoped; return writer(1)(scoped, bytes); });
  await assert.rejects(escaped.transaction(tx => tx.execute("INSERT INTO items VALUES(2,'late')")), code("INPUT"));
  assert.deepEqual(target.rows(), [[1, "first"]]);
  target.close();
});
test("unawaited rejected application work aborts instead of escaping past COMMIT", async () => {
  const { target, order } = await setup();
  await assert.rejects(order.apply(message(), async scoped => {
    void scoped.transaction(async tx => {
      await new Promise(resolve => setTimeout(resolve, 10));
      await tx.execute("INSERT INTO items VALUES(1,'late')");
      throw new Error("unobserved child failure");
    }).catch(() => {});
    return { applied: 1, omitted: 0, replayed: false };
  }), /unobserved child failure/);
  assert.deepEqual(target.rows(), []);
  target.close();
});

// Real owned savepoints through the ordered ledger, application and rebase hook.
// Fault hooks hold/reenter operations; query results always come from SQLite.
import { applyChangeset, applyPatchset, CHANGESET_RECEIPTS_TABLE } from "../src/changeset-apply.ts";
import { encodeChangeset, decodeRebaseInfo } from "../src/changeset-codec.ts";
import { FrankenSQLiteError } from "../src/errors.ts";
const ownSchema = "CREATE TABLE IF NOT EXISTS t(id INTEGER PRIMARY KEY,v TEXT UNIQUE);" +
  "CREATE TABLE IF NOT EXISTS decisions(id INTEGER PRIMARY KEY,info BLOB)";
const pause = () => { let resolve; const promise = new Promise(r => { resolve = r; }); return { promise, resolve }; };
const tick = () => new Promise(resolve => setImmediate(resolve));
const success = { applied: 1, omitted: 0, replayed: false };
const sqlRows = (db, sql = "SELECT * FROM t ORDER BY id") => {
  const stmt = db.prepare(sql); stmt.setReadBigInts(true); stmt.setReturnArrays(true); return stmt.all();
};
class OrderedOwner {
  constructor(ddl = ownSchema, path = ":memory:") {
    this.db = new DatabaseSync(path);
    this.db.exec("PRAGMA foreign_keys=ON; PRAGMA recursive_triggers=ON;" + ddl);
    this.depth = 0; this.serial = 0; this.events = []; this.capable = true;
    this.beforeSql = null; this.beforeRelease = null; this.beforeRollback = null;
    this.afterRollback = null; this.afterCommit = null;
  }
  async transaction(work) {
    const depth = this.depth++, name = `order_owned_${++this.serial}`;
    this.db.exec(depth ? `SAVEPOINT ${name}` : "BEGIN");
    this.events.push(["begin", depth]);
    let active = true, released = false;
    const check = () => assert.ok(active, "expired native owner");
    const tx = {
      execute: async (sql, params = []) => {
        check(); this.events.push(["execute", depth, sql]);
        await this.beforeSql?.("execute", sql, depth);
        try { return Number(this.db.prepare(sql).run(...params).changes); }
        catch (error) {
          if (error.code === "ERR_SQLITE_ERROR" && (error.errcode & 255) === 19)
            throw new FrankenSQLiteError({ code: "SQLITE_CONSTRAINT", sqliteCode: 19,
              extendedCode: error.errcode, message: error.message });
          throw error;
        }
      },
      query: async (sql, params = []) => {
        check(); this.events.push(["query", depth, sql]);
        await this.beforeSql?.("query", sql, depth);
        const stmt = this.db.prepare(sql); stmt.setReadBigInts(true); stmt.setReturnArrays(true);
        return { rowArrays: stmt.all(...params) };
      },
    };
    if (this.capable) tx.transaction = child => { check(); return this.transaction(child); };
    try {
      const result = await work(tx); active = false;
      await this.beforeRelease?.(depth);
      this.db.exec(depth ? `RELEASE ${name}` : "COMMIT"); released = true;
      this.events.push(["release", depth]);
      if (!depth) await this.afterCommit?.();
      return result;
    } catch (error) {
      active = false;
      if (released) throw error; // A lost commit response must not become rollback.
      try {
        await this.beforeRollback?.(depth, error);
        this.db.exec(depth ? `ROLLBACK TO ${name}; RELEASE ${name}` : "ROLLBACK");
        this.events.push(["rollback", depth]);
        await this.afterRollback?.(depth, error);
      } catch (cleanup) {
        throw new AggregateError([error, cleanup], "unconfirmed ordered cleanup", { cause: error });
      }
      throw error;
    } finally { active = false; this.depth--; }
  }
  rows(sql) { return sqlRows(this.db, sql); }
  close() { this.db.close(); }
}
async function ownedSetup(t, ddl = ownSchema) {
  const owner = new OrderedOwner(ddl); t.after(() => owner.close());
  const order = new ChangesetOrder(owner, opts); await order.initialize();
  owner.events.length = 0;
  return { owner, order };
}
function nativeWire(ddl, statements, compact = false) {
  const db = new DatabaseSync(":memory:"); db.exec(ddl);
  const session = db.createSession();
  try { db.exec(statements); return compact ? session.patchset() : session.changeset(); }
  finally { session.close(); db.close(); }
}
const wireMessage = (changeset, sequence = 1n) => ({ sequence,
  deliveryId: `owned/${sequence}`, sha256: HASH(changeset), changeset });
const remote = () => encodeChangeset([{ name: "t", primaryKey: [1, 0], changes: [
  { operation: "insert", indirect: false, new: [1n, "remote"] },
  { operation: "insert", indirect: false, new: [2n, "new"] },
] }]);
function noInbox(owner) {
  if (owner.rows(`SELECT name FROM sqlite_schema WHERE name='${CHANGESET_RECEIPTS_TABLE}'`).length)
    assert.deepEqual(owner.rows(`SELECT * FROM "${CHANGESET_RECEIPTS_TABLE}"`), []);
}
function atGenesis(owner) {
  assert.deepEqual(owner.rows(`SELECT seq FROM ${T}`), [[0n]]);
  noInbox(owner); assert.equal(owner.depth, 0);
}

for (const compact of [false, true]) test(`ordered ${compact ? "patchset" : "changeset"}: isolate constraints in the same ledger transaction`, async t => {
  const ddl = "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT NOT NULL UNIQUE CHECK(v!='bad'))";
  const { owner, order } = await ownedSetup(t, ddl);
  owner.db.exec("INSERT INTO t VALUES(9,'taken')");
  const bytes = nativeWire(ddl, "INSERT INTO t VALUES(1,'first'),(2,'taken'),(3,'last')", compact);
  const msg = wireMessage(bytes), apply = compact ? applyPatchset : applyChangeset;
  let calls = 0;
  const result = await order.apply(msg, (target, wire) => apply(target, wire, {
    tables: ["t"], deliveryId: msg.deliveryId, constraintConflicts: "isolate",
    onConflict: conflict => {
      calls++; assert.equal(conflict.kind, "constraint");
      assert.equal(conflict.error.extendedCode, 2067);
      assert.deepEqual(owner.events.at(-1), ["rollback", 1]); return "omit";
    },
  }));
  assert.equal(result.applied, 2); assert.equal(result.omitted, 1); assert.equal(calls, 1);
  assert.equal(owner.events.filter(([kind, depth]) => kind === "begin" && depth === 0).length, 1);
  assert.equal(owner.events.filter(([kind, depth]) => kind === "begin" && depth === 1).length, 3);
  const oracle = new DatabaseSync(":memory:");
  try {
    oracle.exec(ddl + ";INSERT INTO t VALUES(9,'taken')");
    assert.equal(oracle.applyChangeset(bytes, { onConflict: () => 0 }), true);
    assert.deepEqual(owner.rows(), sqlRows(oracle));
  } finally { oracle.close(); }
  assert.equal((await order.apply(msg, () => assert.fail("ordered replay applied rows"))).replayed, true);
  assert.equal(calls, 1); assert.equal((await order.head()).sequence, 1n);
});

for (const defer of [false, true]) test(`ordered rebase hook${defer ? " with deferred FKs" : ""}: nested decisions and exact replay`, async t => {
  const { owner, order } = await ownedSetup(t);
  owner.db.exec("INSERT INTO t VALUES(1,'local')");
  const msg = wireMessage(remote()), rejected = new Error("discard optional journal path");
  let oldTarget, oldParent, oldChild, info, calls = 0;
  const result = await order.apply(msg, (target, bytes) => {
    oldTarget = target;
    return applyChangeset(target, bytes, { tables: ["t"], deliveryId: msg.deliveryId,
      ...(defer ? { foreignKeys: "defer" } : {}), onConflict: () => "replace",
      onRebase: async (tx, decisions) => {
        calls++; oldParent = tx; info = new Uint8Array(decisions);
        await assert.rejects(tx.transaction(async child => {
          await child.execute("INSERT INTO decisions VALUES(99,?)", [decisions]); throw rejected;
        }), error => error === rejected);
        assert.equal(await tx.transaction(async child => {
          oldChild = child;
          await child.transaction(last => last.execute("INSERT INTO decisions VALUES(1,?)", [decisions]));
          return 42;
        }), 42);
      },
    });
  });
  assert.deepEqual(owner.rows(), [[1n, "remote"], [2n, "new"]]);
  assert.deepEqual(owner.rows("SELECT id FROM decisions"), [[1n]]);
  assert.deepEqual(new Uint8Array(owner.rows("SELECT info FROM decisions")[0][0]), info);
  assert.equal(decodeRebaseInfo(info)[0].changes[0].replace, true);
  assert.equal(result.applied, 2); assert.equal(result.sequence, 1n);
  const replay = await order.apply(msg, () => assert.fail("replayed journal"));
  assert.equal(replay.replayed, true); assert.equal(calls, 1);
  const before = owner.events.length;
  await assert.rejects(oldTarget.transaction(async () => 1), code("INPUT"));
  for (const old of [oldParent, oldChild]) {
    await assert.rejects(old.query("SELECT 1"), /scope has ended/);
    await assert.rejects(old.transaction(async () => 1), /scope has ended/);
  }
  assert.equal(owner.events.length, before);
});

test("ordered isolation allows later FK parents but atomically rejects unresolved references", async t => {
  const ddl = "CREATE TABLE p(id INTEGER PRIMARY KEY);" +
    "CREATE TABLE t(id INTEGER PRIMARY KEY,pid REFERENCES p(id),v TEXT UNIQUE);" +
    "CREATE TABLE decisions(id INTEGER PRIMARY KEY,info BLOB)";
  const { owner, order } = await ownedSetup(t, ddl);
  owner.db.exec("INSERT INTO p VALUES(9); INSERT INTO t VALUES(9,9,'taken')");
  const build = includeParent => encodeChangeset([
    { name: "t", primaryKey: [1,0,0], changes: [
      { operation: "insert", indirect: false, new: [1n,1n,"kept"] },
      { operation: "insert", indirect: false, new: [2n,1n,"taken"] },
    ] },
    ...(includeParent ? [{ name: "p", primaryKey: [1], changes: [
      { operation: "insert", indirect: false, new: [1n] },
    ] }] : []),
  ]);
  const settings = { tables: ["t","p"], deliveryId: "ordered-fk-batch",
    foreignKeys: "defer", constraintConflicts: "isolate",
    onConflict: c => { assert.equal(c.kind,"constraint"); return "omit"; },
    onRebase: (tx, info) => tx.transaction(child => child.execute("INSERT INTO decisions VALUES(1,?)", [info])),
  };
  await assert.rejects(order.apply(wireMessage(build(false)), (target, bytes) =>
    applyChangeset(target, bytes, settings)), /foreign|violation/i);
  atGenesis(owner); assert.deepEqual(owner.rows(), [[9n,9n,"taken"]]);
  assert.deepEqual(owner.rows("SELECT * FROM decisions"), []);
  const result = await order.apply(wireMessage(build(true)), (target, bytes) => applyChangeset(target, bytes, settings));
  assert.equal(result.applied, 2); assert.equal(result.omitted, 1);
  assert.deepEqual(owner.rows(), [[1n,1n,"kept"],[9n,9n,"taken"]]);
  assert.deepEqual(owner.rows("PRAGMA foreign_key_check"), []);
  assert.deepEqual(owner.rows("PRAGMA defer_foreign_keys"), [[0n]]);
  assert.deepEqual(owner.rows("SELECT id FROM decisions"), [[1n]]);
  assert.deepEqual(owner.rows(`SELECT applied,omitted FROM "${CHANGESET_RECEIPTS_TABLE}"`), [[2n,1n]]);
  assert.equal((await order.apply(wireMessage(build(true)), () => assert.fail("replayed composite batch"))).replayed, true);
});

for (const ending of ["commit", "throw", "cancel"]) test(`ordered ${ending}: drain dropped descendant before ledger publication`, async t => {
  const { owner, order } = await ownedSetup(t), entered = pause(), release = pause(), abort = new AbortController();
  let alias, child, write, operation, done = false;
  const sql = "INSERT INTO t VALUES(1,'held')";
  owner.beforeSql = async (kind, text) => {
    if (kind === "execute" && text === sql) { entered.resolve(); await release.promise; }
  };
  operation = order.apply(message(), async target => {
    alias = target.transaction(async tx => {
      child = tx.transaction(async inner => { write = inner.execute(sql); });
    });
    if (ending === "throw") throw new Error("application failed separately");
    return success;
  }, { signal: abort.signal });
  void operation.then(() => { done = true; }, () => { done = true; });
  try {
    await Promise.race([entered.promise, operation.then(() => assert.fail("no held descendant"))]);
    if (ending === "cancel") abort.abort("cancel ordered delivery");
    await tick(); assert.equal(done, false); assert.equal(owner.depth, 2);
    assert.deepEqual(owner.rows(`SELECT seq FROM ${T}`), [[0n]]);
    release.resolve();
    if (ending === "commit") {
      await operation; assert.deepEqual(owner.rows(), [[1n,"held"]]);
      assert.equal((await order.head()).sequence, 1n);
    } else {
      await assert.rejects(operation); assert.deepEqual(owner.rows(), []); atGenesis(owner);
    }
    await Promise.allSettled([alias, child, write]);
  } finally { release.resolve(); await Promise.allSettled([operation, alias, child, write]); }
});

for (const boundary of ["release", "rollback"]) test(`ordered caught ${boundary} uncertainty blocks hooks and sequence advancement`, async t => {
  const { owner, order } = await ownedSetup(t), failure = new Error(`${boundary} not confirmed`);
  let blocked = 0;
  if (boundary === "release") owner.beforeRelease = depth => { if (depth === 2) throw failure; };
  else owner.afterRollback = depth => { if (depth === 2) throw failure; };
  await assert.rejects(order.apply(message(), target => target.transaction(async tx => {
    await tx.transaction(async parent => {
      await parent.transaction(async child => {
        await child.execute("INSERT INTO t VALUES(2,'uncertain')");
        if (boundary === "rollback") throw new Error("rollback request");
      }).catch(() => {});
      const before = owner.events.length;
      for (const attempt of [() => parent.execute("INSERT INTO t VALUES(3,'bad')"),
        () => parent.query("SELECT 1"), () => parent.transaction(async () => { blocked++; })])
        await assert.rejects(attempt(), code("STATE"));
      assert.equal(owner.events.length, before);
    }).catch(() => {});
    await assert.rejects(tx.execute("INSERT INTO t VALUES(4,'bad')"), code("STATE"));
    return success;
  })), code("STATE"));
  assert.equal(blocked,0); assert.deepEqual(owner.rows(), []); atGenesis(owner);
  owner.beforeRelease = null; owner.afterRollback = null;
  await order.apply(message(), target => target.transaction(async tx => {
    await tx.execute("INSERT INTO t VALUES(1,'retry')"); return success;
  }));
  assert.deepEqual(owner.rows(), [[1n,"retry"]]);
});

test("ordered callback failure retains independently failed child settlement", async t => {
  const { owner, order } = await ownedSetup(t);
  const callback = new Error("outer callback"), cleanup = new Error("child release failed");
  owner.beforeRelease = depth => { if (depth === 1) throw cleanup; };
  let alias;
  await assert.rejects(order.apply(message(), async target => {
    alias = target.transaction(tx => tx.transaction(async child => {
      await child.execute("INSERT INTO t VALUES(1,'discarded')");
    }));
    throw callback;
  }), error => {
    assert.equal(error.code,"ERR_FSQLITE_ORDER_STATE"); assert.equal(error.cause,callback);
    assert.equal(error.cleanupErrors.length,1);
    assert.equal(error.cleanupErrors[0].cleanupErrors[0],cleanup); return true;
  });
  await Promise.allSettled([alias]); assert.deepEqual(owner.rows(), []); atGenesis(owner);
});

test("ordered logical application aliases are not independent rollback boundaries", async t => {
  const { owner, order } = await ownedSetup(t), reason = new Error("logical callback failed");
  let calls = 0;
  await assert.rejects(order.apply(message(), async target => {
    await target.transaction(async tx => {
      await tx.execute("INSERT INTO t VALUES(1,'provisional')"); throw reason;
    }).catch(() => {});
    await assert.rejects(target.transaction(async tx => {
      calls++; await tx.execute("INSERT INTO t VALUES(2,'must not enter')");
    }), error => error === reason);
    return success;
  }), error => error === reason);
  assert.equal(calls,0); assert.deepEqual(owner.rows(),[]); atGenesis(owner);
});

for (const method of ["execute", "query"]) test(`ordered ${method}: synchronous reentry cannot open a second application alias`, async t => {
  const { owner, order } = await ownedSetup(t);
  let refused, calls = 0;
  const sql = "INSERT INTO t VALUES(1,'parent')" + (method === "query" ? " RETURNING id" : "");
  await order.apply(message(), target => target.transaction(async tx => {
    owner.beforeSql = (kind,text) => {
      if (kind !== method || text !== sql) return;
      owner.beforeSql = null;
      refused = target.transaction(async () => { calls++; });
      void refused.catch(() => {});
    };
    await tx[method](sql); await assert.rejects(refused,code("BUSY")); return success;
  }));
  assert.equal(calls,0); assert.deepEqual(owner.rows(),[[1n,"parent"]]);
  assert.equal(owner.events.filter(([kind]) => kind === "begin").length,1);
});

test("ordered savepoint ownership rejects parent and alias writes inside a live child", async t => {
  const { owner, order } = await ownedSetup(t), entered = pause(), release = pause();
  let alias, child, operation, calls=0;
  operation = order.apply(message(), async target => {
    alias = target.transaction(async tx => {
      child = tx.transaction(async inner => {
        entered.resolve(); await release.promise; await inner.execute("INSERT INTO t VALUES(1,'child')");
      });
      await entered.promise;
      await assert.rejects(tx.query("SELECT 1"),code("BUSY"));
      await assert.rejects(tx.transaction(async () => { calls++; }),code("BUSY"));
      await child; return success;
    });
    await Promise.race([entered.promise, alias.then(() => assert.fail("child did not start"))]);
    await assert.rejects(target.transaction(async () => { calls++; }),code("BUSY"));
    release.resolve(); return alias;
  });
  try { await operation; assert.equal(calls,0); assert.deepEqual(owner.rows(),[[1n,"child"]]); }
  finally { release.resolve(); await Promise.allSettled([operation,alias,child]); }
});

test("ordered lost response and WAL reopen replay rows, decisions and sequence together", async () => {
  const path = join(mkdtempSync(join(tmpdir(),"fsqlite-order-owned-")),"receiver.db");
  let owner = new OrderedOwner(ownSchema,path), order = new ChangesetOrder(owner,opts), calls=0;
  const msg=wireMessage(remote());
  try {
    owner.db.exec("PRAGMA journal_mode=WAL"); await order.initialize();
    owner.afterCommit=()=>{throw new Error("lost committed response");};
    await assert.rejects(order.apply(msg,(target,bytes)=>applyChangeset(target,bytes,{
      tables:["t"],deliveryId:msg.deliveryId,onRebase:async(tx,info)=>{
        calls++; await tx.transaction(child=>child.execute("INSERT INTO decisions VALUES(1,?)",[info]));
      },
    })),/lost committed response/);
    owner.close(); owner = new OrderedOwner(ownSchema,path); order = new ChangesetOrder(owner,opts);
    const replay=await order.apply(msg,()=>assert.fail("replayed callback"));
    assert.equal(replay.replayed,true);assert.equal(replay.sequence,1n);assert.equal(calls,1);
    assert.deepEqual(owner.rows(),[[1n,"remote"],[2n,"new"]]);
    assert.deepEqual(owner.rows("SELECT id FROM decisions"),[[1n]]);
    assert.deepEqual(owner.rows(`SELECT applied FROM "${CHANGESET_RECEIPTS_TABLE}"`),[[2n]]);
  } finally { owner.close(); }
});

test("ordered targets without child capability retain default application but cannot claim isolation", async t => {
  const { owner, order }=await ownedSetup(t);owner.capable=false;
  const msg=wireMessage(remote());
  await assert.rejects(order.apply(msg,(target,bytes)=>applyChangeset(target,bytes,{
    tables:["t"],constraintConflicts:"isolate",
  })),{code:"ERR_FSQLITE_CHANGESET_INPUT"});
  atGenesis(owner);
  await order.apply(msg,(target,bytes)=>applyChangeset(target,bytes,{tables:["t"]}));
  assert.deepEqual(owner.rows(),[[1n,"remote"],[2n,"new"]]);
  assert.equal(owner.events.some(([kind,depth])=>kind==="begin"&&depth>0),false);
});

// Atomic ordered windows retain per-message identities without publishing a prefix.
function batchMessages(compact = false) {
  return [1n, 2n, 3n].map(id => wireMessage(nativeWire(ownSchema,
    `INSERT INTO t VALUES(${id},'batch-${id}')`, compact), id));
}
const batchApplication = (extra = {}) => (target, bytes, info) => applyChangeset(target, bytes, {
  tables: ["t"], deliveryId: info.deliveryId, ...extra,
});
function batchReceipts(owner) {
  return owner.rows(`SELECT delivery_id,applied,omitted FROM "${CHANGESET_RECEIPTS_TABLE}" ORDER BY delivery_id`);
}

for (const compact of [false, true]) test(`atomic batch ${compact ? "compact" : "full"}: one owner, exact identities and native SQL results`, async t => {
  const { owner, order } = await ownedSetup(t), messages = batchMessages(compact), seen = [];
  const apply = compact ? applyPatchset : applyChangeset;
  const result = await order.applyBatch(messages, (target, bytes, info) => {
    assert.ok(Object.isFrozen(info));
    assert.deepEqual(Object.keys(info).sort(), ["byteLength", "deliveryId", "receiverId", "sequence", "sha256", "sourceId"]);
    assert.equal(info.receiverId, opts.receiverId); assert.equal(info.sourceId, opts.sourceId);
    assert.equal(info.sha256, HASH(bytes)); assert.equal(info.byteLength, bytes.length);
    seen.push(info.sequence);
    return apply(target, bytes, { tables: ["t"], deliveryId: info.deliveryId });
  });
  assert.ok(Object.isFrozen(result)); assert.ok(result.every(Object.isFrozen));
  assert.deepEqual(seen, [1n, 2n, 3n]);
  assert.deepEqual(result.map(r => [r.sequence, r.applied, r.omitted, r.replayed]),
    [[1n, 1, 0, false], [2n, 1, 0, false], [3n, 1, 0, false]]);
  assert.deepEqual(batchReceipts(owner), [["owned/1", 1n, 0n], ["owned/2", 1n, 0n], ["owned/3", 1n, 0n]]);
  assert.deepEqual(owner.events.filter(([kind]) => kind === "begin" || kind === "release"),
    [["begin", 0], ["release", 0]], "no per-message transaction or automatic savepoint");
  const reference = new DatabaseSync(":memory:");
  try {
    reference.exec(ownSchema + ";BEGIN");
    for (const msg of messages) assert.equal(reference.applyChangeset(msg.changeset), true);
    reference.exec("COMMIT"); assert.deepEqual(owner.rows(), sqlRows(reference));
  } finally { reference.close(); }
  assert.equal((await order.head()).sequence, 3n);
});

test("atomic batch: late SQL failure rolls back earlier rows, decisions, inbox and sequence", async t => {
  const { owner, order } = await ownedSetup(t), messages = batchMessages();
  owner.db.exec("INSERT INTO t VALUES(9,'batch-2')");
  let calls = 0;
  await assert.rejects(order.applyBatch(messages, (target, bytes, info) => {
    calls++;
    return applyChangeset(target, bytes, { tables: ["t"], deliveryId: info.deliveryId,
      onRebase: (tx, value) => tx.transaction(child =>
        child.execute("INSERT INTO decisions VALUES(?,?)", [info.sequence, value])),
    });
  }), /UNIQUE/);
  assert.equal(calls, 2); assert.deepEqual(owner.rows(), [[9n, "batch-2"]]);
  assert.deepEqual(owner.rows("SELECT * FROM decisions"), []); atGenesis(owner);
});

test("atomic batch: handled omissions and nested rebase hooks retain each message's decisions", async t => {
  const { owner, order } = await ownedSetup(t), messages = batchMessages();
  owner.db.exec("INSERT INTO t VALUES(9,'batch-2')");
  let omissions = 0;
  const result = await order.applyBatch(messages, batchApplication({ constraintConflicts: "isolate",
    onConflict: conflict => { omissions++; assert.equal(conflict.kind, "constraint"); return "omit"; },
    onRebase: (tx, bytes) => tx.transaction(child => child.execute("INSERT INTO decisions(info) VALUES(?)", [bytes])),
  }));
  assert.equal(omissions, 1); assert.deepEqual(result.map(r => [r.applied, r.omitted]), [[1, 0], [0, 1], [1, 0]]);
  assert.deepEqual(owner.rows(), [[1n, "batch-1"], [3n, "batch-3"], [9n, "batch-2"]]);
  assert.deepEqual(batchReceipts(owner), [["owned/1", 1n, 0n], ["owned/2", 0n, 1n], ["owned/3", 1n, 0n]]);
  const saved = owner.rows("SELECT info FROM decisions ORDER BY id");
  assert.equal(saved.length, 3); assert.equal(decodeRebaseInfo(saved[1][0])[0].changes[0].replace, false);
  assert.equal(owner.events.filter(([kind, depth]) => kind === "release" && depth === 0).length, 1);
});

for (const resolveParent of [false, true]) test(`atomic batch: cross-message foreign key ${resolveParent ? "resolves" : "rejects whole window"}`, async t => {
  const ddl = "CREATE TABLE p(id INTEGER PRIMARY KEY);CREATE TABLE t(id INTEGER PRIMARY KEY,pid REFERENCES p);" +
    "CREATE TABLE decisions(id INTEGER PRIMARY KEY,info BLOB)";
  const { owner, order } = await ownedSetup(t, ddl);
  const child = encodeChangeset([{ name: "t", primaryKey: [1, 0], changes: [
    { operation: "insert", indirect: false, new: [1n, 7n] },
  ] }]);
  const parent = encodeChangeset([{ name: "p", primaryKey: [1], changes: [
    { operation: "insert", indirect: false, new: [resolveParent ? 7n : 8n] },
  ] }]);
  const messages = [wireMessage(child), wireMessage(parent, 2n)];
  const operation = order.applyBatch(messages, (target, bytes, info) => applyChangeset(target, bytes, {
    tables: ["t", "p"], deliveryId: info.deliveryId, constraintConflicts: "isolate",
    onRebase: (tx, value) => tx.transaction(inner => inner.execute("INSERT INTO decisions VALUES(?,?)", [info.sequence, value])),
  }), { foreignKeys: "defer" });
  if (resolveParent) {
    const result = await operation; assert.equal(result.length, 2);
    assert.deepEqual(owner.rows(), [[1n, 7n]]); assert.deepEqual(owner.rows("SELECT * FROM p"), [[7n]]);
    assert.deepEqual(owner.rows("PRAGMA foreign_key_check"), []);
    assert.equal(batchReceipts(owner).length, 2); assert.equal((await order.head()).sequence, 2n);
  } else {
    await assert.rejects(operation, /foreign.key/i);
    assert.deepEqual(owner.rows(), []); assert.deepEqual(owner.rows("SELECT * FROM p"), []);
    assert.deepEqual(owner.rows("SELECT * FROM decisions"), []); atGenesis(owner);
  }
  assert.deepEqual(owner.rows("PRAGMA defer_foreign_keys"), [[0n]]);
});

test("atomic batch: replay prefix is skipped and fresh callbacks receive their actual identity", async t => {
  const { owner, order } = await ownedSetup(t), messages = batchMessages();
  await order.applyBatch(messages.slice(0, 2), batchApplication());
  const called = [];
  const result = await order.applyBatch(messages, (target, bytes, info) => {
    called.push(info); return batchApplication()(target, bytes, info);
  });
  assert.deepEqual(result.map(r => r.replayed), [true, true, false]);
  assert.deepEqual(called.map(r => [r.sequence, r.deliveryId]), [[3n, "owned/3"]]);
  assert.equal((await order.head()).sequence, 3n); assert.equal(batchReceipts(owner).length, 3);
  const lowered = new ChangesetOrder(owner, { ...opts, maxEntries: 1 });
  const old = await lowered.applyBatch(messages.slice(0, 2), () => assert.fail("replayed callback"));
  assert.ok(old.every(r => r.replayed)); assert.equal((await lowered.head()).sequence, 3n);
});

test("atomic batch: admission captures all future payloads, identities and options before hashing yields", async t => {
  const { owner, order } = await ownedSetup(t), messages = batchMessages(), originals = [...messages];
  const settings = { maxBatchBytes: messages.reduce((n, m) => n + m.changeset.length, 0) };
  const operation = order.applyBatch(messages, batchApplication(), settings);
  for (const msg of originals) { msg.changeset.fill(0); msg.deliveryId = "mutated"; msg.sequence = 99n; }
  messages.reverse(); settings.maxBatchBytes = 1; settings.foreignKeys = "invalid";
  const result = await operation;
  assert.deepEqual(result.map(r => r.deliveryId), ["owned/1", "owned/2", "owned/3"]);
  assert.deepEqual(owner.rows(), [[1n, "batch-1"], [2n, "batch-2"], [3n, "batch-3"]]);
});

test("atomic batch: malformed envelopes, late digests and aggregate budgets reject before SQL", async t => {
  const { owner, order } = await ownedSetup(t);
  const messages = batchMessages(), total = messages.reduce((n, m) => n + m.changeset.length, 0);
  let getters = 0;
  const accessor = [...messages]; Object.defineProperty(accessor, "1", { get() { getters++; return messages[1]; } });
  const sparse = Array(2); sparse[0] = messages[0];
  const cases = [
    [[], {}], [Array(257).fill(messages[0]), {}], [sparse, {}], [accessor, {}],
    [[messages[0], messages[2]], {}], [[messages[0], messages[0]], {}],
    [[messages[0], { ...messages[1], deliveryId: messages[0].deliveryId }], {}],
    [[messages[0], { ...messages[1], sha256: "0".repeat(64) }], {}],
    [[messages[0], { ...messages[1], sequence: 1n << 63n }], {}],
    [messages, { maxBatchBytes: total - 1 }], [messages, { maxBatchBytes: 0 }],
    [messages, { maxBatchBytes: 64 * 1024 * 1024 + 1 }], [messages, { foreignKeys: "off" }],
  ];
  for (const [input, options] of cases) {
    const before = owner.events.length;
    await assert.rejects(order.applyBatch(input, () => assert.fail("unadmitted callback"), options));
    assert.equal(owner.events.length, before, "invalid batch must not open a SQL transaction");
  }
  assert.equal(getters, 0);
  await order.applyBatch(messages, batchApplication(), { maxBatchBytes: total });
  assert.equal((await order.head()).sequence, 3n);
});

test("atomic batch: cumulative byte budget accepts empty wire at its exact boundary", async t => {
  const { owner, order } = await ownedSetup(t), first = batchMessages()[0];
  const messages = [first, wireMessage(new Uint8Array(), 2n), wireMessage(new Uint8Array(), 3n)];
  const result = await order.applyBatch(messages, batchApplication(), { maxBatchBytes: first.changeset.length });
  assert.deepEqual(result.map(r => r.applied), [1, 0, 0]); assert.equal((await order.head()).sequence, 3n);
  assert.equal(batchReceipts(owner).length, 3);
});

test("atomic batch: gaps, retired replays, reused suffix IDs and capacity fail before callbacks", async t => {
  const { owner, order } = await ownedSetup(t), messages = batchMessages();
  await assert.rejects(order.applyBatch(messages.slice(1), () => assert.fail("gap callback")), code("GAP"));
  await order.applyBatch(messages.slice(0, 1), batchApplication());
  const reused = [messages[1], { ...messages[2], deliveryId: messages[0].deliveryId }];
  await assert.rejects(order.applyBatch(reused, () => assert.fail("doomed suffix callback")), code("REUSE"));
  const narrow = new ChangesetOrder(owner, { ...opts, maxEntries: 2 });
  await assert.rejects(narrow.applyBatch(messages.slice(1), () => assert.fail("overfull callback")), code("FULL"));
  const changed = [wireMessage(nativeWire(ownSchema, "INSERT INTO t VALUES(1,'different')")), messages[1]];
  await assert.rejects(order.applyBatch(changed, () => assert.fail("changed prefix callback")), code("REUSE"));
  assert.deepEqual(owner.rows(), [[1n, "batch-1"]]);
  await order.applyBatch(messages, batchApplication()); await order.retireBefore(2n);
  await assert.rejects(order.applyBatch(messages, () => assert.fail("retired callback")), code("EXPIRED"));
  assert.equal((await order.head()).sequence, 3n);
});

test("atomic batch: a prior replay remains committed when the fresh suffix fails", async t => {
  const { owner, order } = await ownedSetup(t), messages = batchMessages();
  await order.applyBatch(messages.slice(0, 1), batchApplication());
  const failure = new Error("late new message");
  await assert.rejects(order.applyBatch(messages, async (target, bytes, info) => {
    const result = await batchApplication()(target, bytes, info);
    if (info.sequence === 3n) throw failure; return result;
  }), error => error === failure);
  assert.deepEqual(owner.rows(), [[1n, "batch-1"]]); assert.equal((await order.head()).sequence, 1n);
  assert.deepEqual(batchReceipts(owner), [["owned/1", 1n, 0n]]);
});

test("atomic batch: dropped descendants finish before the next message begins", async t => {
  const { owner, order } = await ownedSetup(t), entered = pause(), release = pause();
  let operation, child, sql, calls = 0, done = false;
  owner.beforeSql = async (_kind, text) => { if (text === "INSERT INTO t VALUES(1,'held')") { entered.resolve(); await release.promise; } };
  operation = order.applyBatch([message(), message(2n)], target => {
    calls++;
    return target.transaction(async tx => {
      if (calls === 1) child = tx.transaction(async inner => { sql = inner.execute("INSERT INTO t VALUES(1,'held')"); });
      else await tx.execute("INSERT INTO t VALUES(2,'next')");
      return success;
    });
  });
  void operation.then(() => { done = true; }, () => { done = true; });
  try {
    await Promise.race([entered.promise, operation.then(() => assert.fail("no held write"))]);
    await tick(); assert.equal(done, false); assert.equal(calls, 1); assert.equal(owner.depth, 2);
    release.resolve(); await operation; await Promise.all([child, sql]);
    assert.deepEqual(owner.rows(), [[1n, "held"], [2n, "next"]]); assert.equal(calls, 2);
  } finally { release.resolve(); await Promise.allSettled([operation, child, sql]); }
});

for (const ending of ["cancel", "timeout"]) test(`atomic batch: ${ending} during late admitted work drains before complete rollback`, async t => {
  const { owner, order } = await ownedSetup(t), entered = pause(), release = pause(), abort = new AbortController();
  const messages = batchMessages(); let operation, done = false;
  owner.beforeSql = async (kind, sql) => {
    if (kind === "execute" && sql === "INSERT INTO decisions VALUES(2,?)") { entered.resolve(); await release.promise; }
  };
  operation = order.applyBatch(messages, (target, bytes, info) => applyChangeset(target, bytes, {
    tables: ["t"], deliveryId: info.deliveryId,
    onRebase: (tx, decisions) => tx.transaction(child => child.execute(`INSERT INTO decisions VALUES(${info.sequence},?)`, [decisions])),
  }), ending === "cancel" ? { signal: abort.signal } : { timeoutMs: 200 });
  void operation.then(() => { done = true; }, () => { done = true; });
  try {
    await Promise.race([entered.promise, operation.then(() => assert.fail("late write was not held"))]);
    if (ending === "cancel") abort.abort("batch cancelled");
    else await new Promise(resolve => setTimeout(resolve, 220));
    await tick(); assert.equal(done, false);
    release.resolve(); await assert.rejects(operation, code(ending === "cancel" ? "CANCELLED" : "TIMEOUT"));
    assert.deepEqual(owner.rows(), []); assert.deepEqual(owner.rows("SELECT * FROM decisions"), []); atGenesis(owner);
  } finally { release.resolve(); await Promise.allSettled([operation]); }
});

test("atomic batch: uncertain later child cleanup retains its cause and prevents prefix publication", async t => {
  const { owner, order } = await ownedSetup(t), cleanup = new Error("uncertain release");
  let calls = 0;
  await assert.rejects(order.applyBatch(batchMessages(), (target, bytes, info) => {
    calls++;
    return applyChangeset(target, bytes, { tables: ["t"], deliveryId: info.deliveryId, onRebase: async (tx, decisions) => {
      if (info.sequence === 2n) owner.beforeRelease = depth => { if (depth === 1) throw cleanup; };
      await tx.transaction(child => child.execute("INSERT INTO decisions VALUES(?,?)", [info.sequence, decisions]));
    } });
  }), error => {
    assert.equal(error.code, "ERR_FSQLITE_ORDER_STATE");
    assert.equal(error.cleanupErrors[0], cleanup); return true;
  });
  assert.equal(calls, 2); assert.deepEqual(owner.rows(), []); assert.deepEqual(owner.rows("SELECT * FROM decisions"), []); atGenesis(owner);
});

test("atomic batch: synchronous reentry cannot submit another batch or single delivery", async t => {
  const { owner, order } = await ownedSetup(t); let getters = 0;
  const blocked = { get signal() { getters++; throw new Error("must not inspect rejected work"); } };
  await order.applyBatch(batchMessages(), async (target, bytes, info) => {
    await assert.rejects(order.applyBatch([], () => assert.fail("reentry"), blocked), code("BUSY"));
    await assert.rejects(order.apply(message(), () => assert.fail("reentry"), blocked), code("BUSY"));
    await assert.rejects(order.retireBefore(1n, blocked), code("BUSY"));
    return batchApplication()(target, bytes, info);
  });
  assert.equal(getters, 0); assert.equal((await order.head()).sequence, 3n);
});

test("atomic batch: mutation or invalid results in a late application roll back all fresh messages", async t => {
  for (const mutate of [false, true]) {
    const { owner, order } = await ownedSetup(t);
    await assert.rejects(order.applyBatch(batchMessages(), async (target, bytes, info) => {
      const result = await batchApplication()(target, bytes, info);
      if (info.sequence === 2n) {
        if (mutate) bytes.fill(0); else return { ...result, replayed: true };
      }
      return result;
    }), code(mutate ? "CORRUPT" : "REUSE"));
    assert.deepEqual(owner.rows(), []); atGenesis(owner);
  }
});

test("atomic batch: final COMMIT failure rolls back every receipt and permits an exact retry", async t => {
  const { owner, order } = await ownedSetup(t), reason = new Error("commit refused"), messages = batchMessages();
  owner.beforeRelease = depth => { if (depth === 0) throw reason; };
  await assert.rejects(order.applyBatch(messages, batchApplication()), error => error === reason);
  assert.deepEqual(owner.rows(), []); atGenesis(owner);
  owner.beforeRelease = null;
  const result = await order.applyBatch(messages, batchApplication());
  assert.ok(result.every(r => !r.replayed)); assert.equal((await order.head()).sequence, 3n);
});

test("atomic batch: WAL reopen after lost response replays all per-message receipts without callbacks", async () => {
  const path = join(mkdtempSync(join(tmpdir(), "fsqlite-order-batch-")), "receiver.db");
  let owner = new OrderedOwner(ownSchema, path), order = new ChangesetOrder(owner, opts);
  const messages = batchMessages(); let calls = 0;
  try {
    owner.db.exec("PRAGMA journal_mode=WAL"); await order.initialize();
    owner.afterCommit = () => { throw new Error("lost batch response"); };
    await assert.rejects(order.applyBatch(messages, (target, bytes, info) => {
      calls++;
      return applyChangeset(target, bytes, { tables: ["t"], deliveryId: info.deliveryId,
        onRebase: (tx, decisions) => tx.transaction(child => child.execute("INSERT INTO decisions VALUES(?,?)", [info.sequence, decisions])),
      });
    }), /lost batch response/);
    owner.close(); owner = new OrderedOwner(ownSchema, path); order = new ChangesetOrder(owner, opts);
    const replay = await order.applyBatch(messages, () => assert.fail("replayed application"));
    assert.ok(replay.every(r => r.replayed)); assert.equal(calls, 3);
    assert.equal((await order.head()).sequence, 3n); assert.equal(batchReceipts(owner).length, 3);
    assert.deepEqual(owner.rows("SELECT id FROM decisions ORDER BY id"), [[1n], [2n], [3n]]);
    assert.deepEqual(owner.rows(), [[1n, "batch-1"], [2n, "batch-2"], [3n, "batch-3"]]);
  } finally { owner.close(); }
});

test("atomic batch: separate owners progress independently and unsupported savepoints are not invented", async t => {
  const first = await ownedSetup(t), second = await ownedSetup(t), entered = pause(), release = pause();
  first.owner.capable = false; second.owner.capable = false;
  let operation;
  operation = first.order.applyBatch(batchMessages(), async (target, bytes, info) => {
    if (info.sequence === 1n) { entered.resolve(); await release.promise; }
    return batchApplication()(target, bytes, info);
  });
  try {
    await Promise.race([entered.promise, operation.then(() => assert.fail("first was not held"))]);
    await second.order.applyBatch(batchMessages(), batchApplication());
    assert.equal((await second.order.head()).sequence, 3n);
    release.resolve(); await operation;
    for (const { owner } of [first, second])
      assert.equal(owner.events.some(([kind, depth]) => kind === "begin" && depth > 0), false);
  } finally { release.resolve(); await Promise.allSettled([operation]); }
});

test("atomic batch: maximum 256 empty messages retain distinct receipts in one owner", async t => {
  const { owner, order } = await ownedSetup(t);
  const messages = Array.from({ length: 256 }, (_, i) => wireMessage(new Uint8Array(), BigInt(i + 1)));
  const results = await order.applyBatch(messages, batchApplication(), { maxBatchBytes: 1 });
  assert.equal(results.length, 256); assert.ok(results.every(r => r.applied === 0 && !r.replayed));
  assert.equal(batchReceipts(owner).length, 256); assert.equal((await order.head()).sequence, 256n);
  assert.equal(owner.events.filter(([kind, depth]) => kind === "release" && depth === 0).length, 2,
    "one batch commit and the later head read; never a commit per message");
});

test("atomic batch: results inside a caller-owned transaction remain provisional until that caller commits", async t => {
  const { owner, order } = await ownedSetup(t), reason = new Error("caller rollback");
  await assert.rejects(owner.transaction(async tx => {
    const nested = new ChangesetOrder({ transaction: work => tx.transaction(work) }, opts);
    const result = await nested.applyBatch(batchMessages(), batchApplication());
    assert.equal(result.length, 3); assert.equal(owner.rows().length, 3);
    throw reason;
  }), error => error === reason);
  assert.deepEqual(owner.rows(), []); atGenesis(owner); assert.equal((await order.head()).sequence, 0n);
});

test("atomic batch: cancellation arriving after physical COMMIT cannot relabel its successful result", async t => {
  const { owner, order } = await ownedSetup(t), abort = new AbortController();
  owner.afterCommit = () => abort.abort("already committed");
  const result = await order.applyBatch(batchMessages(), batchApplication(), { signal: abort.signal });
  assert.equal(abort.signal.aborted, true); assert.equal(result.length, 3); assert.ok(result.every(r => !r.replayed));
  assert.deepEqual(owner.rows(), [[1n, "batch-1"], [2n, "batch-2"], [3n, "batch-3"]]);
  owner.afterCommit = null; assert.equal((await order.head()).sequence, 3n);
});

test("atomic batch: late unsafe storage rejects before SQL while sliced exact buffers ignore shadowed getters", async t => {
  const { owner, order } = await ownedSetup(t), first = batchMessages()[0], second = batchMessages()[1];
  const detached = second.changeset.slice(); structuredClone(detached.buffer, { transfer: [detached.buffer] });
  for (const bytes of [detached, new Uint8Array(new SharedArrayBuffer(4)),
    new Uint8Array(new ArrayBuffer(4, { maxByteLength: 8 })), new Proxy(second.changeset, {})]) {
    const before = owner.events.length;
    await assert.rejects(order.applyBatch([first, { ...second, changeset: bytes }],
      () => assert.fail("unsafe callback")), code("INPUT"));
    assert.equal(owner.events.length, before);
  }
  const storage = new Uint8Array(second.changeset.length + 6); storage.set(second.changeset, 3);
  const view = storage.subarray(3, -3); let getters = 0;
  for (const property of ["length", "byteLength", "byteOffset", "buffer"])
    Object.defineProperty(view, property, { get() { getters++; throw new Error(property); } });
  await order.applyBatch([first, { ...second, changeset: view }], batchApplication());
  assert.equal(getters, 0); assert.deepEqual(owner.rows(), [[1n, "batch-1"], [2n, "batch-2"]]);
});
