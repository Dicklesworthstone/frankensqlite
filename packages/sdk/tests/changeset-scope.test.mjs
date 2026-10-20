// Shared callback ownership through real SQLite transactions and savepoints.
// Adapter hooks reenter or hold operations; they do not fabricate SQL results.
import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import { captureChangeset } from "../src/changeset-capture.ts";
import { withDeferredForeignKeys } from "../src/changeset-foreign-keys.ts";
import { decodeChangeset, invertChangeset } from "../src/changeset-codec.ts";

const schema = "CREATE TABLE t(id INTEGER PRIMARY KEY,value TEXT)";
const gate = () => {
  let resolve;
  const promise = new Promise(r => { resolve = r; });
  return { promise, resolve };
};
const tick = () => new Promise(resolve => setImmediate(resolve));
const modes = ["deferred", "capture", "deferred-capture"];
class Owner {
  constructor(ddl = schema) {
    this.db = new DatabaseSync(":memory:");
    this.db.exec("PRAGMA foreign_keys=ON; PRAGMA recursive_triggers=ON;" + ddl);
    this.depth = 0; this.serial = 0; this.events = [];
    this.beforeSql = null; this.beforeRelease = null; this.afterRollback = null;
  }
  async transaction(work) {
    const depth = this.depth++, name = `owned_${++this.serial}`;
    this.db.exec(depth ? `SAVEPOINT ${name}` : "BEGIN");
    this.events.push(["begin", depth]);
    let active = true;
    const check = () => assert.ok(active, "adapter scope expired");
    const tx = {
      execute: async (sql, params = []) => {
        check(); this.events.push(["execute", depth, sql]);
        await this.beforeSql?.("execute", sql, depth);
        return Number(this.db.prepare(sql).run(...params).changes);
      },
      query: async (sql, params = []) => {
        check(); this.events.push(["query", depth, sql]);
        await this.beforeSql?.("query", sql, depth);
        const stmt = this.db.prepare(sql); stmt.setReadBigInts(true); stmt.setReturnArrays(true);
        return { rowArrays: stmt.all(...params) };
      },
      transaction: child => { check(); return this.transaction(child); },
    };
    try {
      const value = await work(tx); active = false;
      await this.beforeRelease?.(depth);
      this.db.exec(depth ? `RELEASE ${name}` : "COMMIT");
      this.events.push(["release", depth]);
      return value;
    } catch (error) {
      active = false;
      try {
        this.db.exec(depth ? `ROLLBACK TO ${name}; RELEASE ${name}` : "ROLLBACK");
        this.events.push(["rollback", depth]);
        await this.afterRollback?.(depth, error);
      } catch (cleanup) {
        throw new AggregateError([error, cleanup], "uncertain child cleanup", { cause: error });
      }
      throw error;
    } finally { active = false; this.depth--; }
  }
  rows(sql = "SELECT * FROM t ORDER BY id") {
    const stmt = this.db.prepare(sql); stmt.setReadBigInts(true); stmt.setReturnArrays(true);
    return stmt.all();
  }
  close() { this.db.close(); }
}
async function run(mode, target, work, options = {}) {
  if (mode === "deferred")
    return { value: await withDeferredForeignKeys(target).transaction(work, options) };
  const owner = mode === "deferred-capture" ? withDeferredForeignKeys(target) : target;
  return captureChangeset(owner, work, { tables: ["t"], ...options });
}
function clean(owner) {
  assert.deepEqual(owner.rows("SELECT name FROM temp.sqlite_schema WHERE name GLOB '__fsqlite_capture_*'"), []);
  assert.deepEqual(owner.rows("PRAGMA defer_foreign_keys"), [[0n]]);
  assert.equal(owner.depth, 0);
}
function verifyCapture(result, source, ddl = schema) {
  if (!result.changeset) return;
  const receiver = new DatabaseSync(":memory:");
  try {
    receiver.exec(ddl);
    assert.equal(receiver.applyChangeset(result.changeset), true);
    const stmt = receiver.prepare("SELECT * FROM t ORDER BY id");
    stmt.setReadBigInts(true); stmt.setReturnArrays(true);
    assert.deepEqual(stmt.all(), source.rows());
    assert.equal(receiver.applyChangeset(invertChangeset(result.changeset)), true);
    assert.deepEqual(stmt.all(), []);
  } finally { receiver.close(); }
}

for (const mode of modes) {
  for (const method of ["execute", "query"]) {
    test(`${mode}: reserve parent ${method} before synchronous adapter reentry`, async () => {
      const source = new Owner(), release = gate();
      const marker = new Error("reentered child rollback");
      const sql = "INSERT INTO t VALUES(1,'parent')" + (method === "query" ? " RETURNING id" : "");
      let child, childError, childCalls = 0, operation;
      try {
        operation = run(mode, source, async tx => {
          source.beforeSql = (kind, statement) => {
            if (kind !== method || statement !== sql) return;
            source.beforeSql = null;
            // Happens before the adapter returns the promise to the caller.
            child = tx.transaction(async () => {
              childCalls++; await release.promise; throw marker;
            });
            void child.catch(() => {});
          };
          const affected = await tx[method](sql);
          release.resolve();
          try { await child; } catch (error) { childError = error; }
          return affected;
        });
        const result = await operation;
        assert.equal(childCalls, 0, "child must not open inside admitted parent SQL");
        assert.equal(childError.code, mode === "deferred"
          ? "ERR_FSQLITE_FOREIGN_KEY_BUSY" : "ERR_FSQLITE_CAPTURE_BUSY");
        if (method === "execute") assert.equal(result.value, 1);
        else assert.deepEqual(result.value.rowArrays, [[1n]]);
        assert.deepEqual(source.rows(), [[1n, "parent"]], "acknowledged parent write must survive");
        assert.equal(source.events.filter(([kind]) => kind === "begin").length, 1);
        clean(source); verifyCapture(result, source);
      } finally {
        release.resolve(); await Promise.allSettled([operation, child]); source.close();
      }
    });
  }

  test(`${mode}: reentry cannot move a parent read into a child snapshot`, async () => {
    const source = new Owner(); let child, calls = 0;
    try {
      const result = await run(mode, source, async tx => {
        source.beforeSql = (kind, sql) => {
          if (kind !== "query" || sql !== "SELECT count(*) FROM t") return;
          source.beforeSql = null;
          child = tx.transaction(async inner => {
            calls++;
            await inner.execute("INSERT INTO t VALUES(2,'not parent work')");
          });
          void child.catch(() => {});
        };
        const read = await tx.query("SELECT count(*) FROM t");
        let error;
        try { await child; } catch (cause) { error = cause; }
        return { read, error };
      });
      assert.equal(calls, 0);
      assert.match(result.value.error.code, /_BUSY$/);
      assert.deepEqual(result.value.read.rowArrays, [[0n]]);
      assert.deepEqual(source.rows(), []); clean(source);
    } finally { await Promise.allSettled([child]); source.close(); }
  });

  test(`${mode}: unawaited descendants settle before capture or deferral cleanup`, async () => {
    const source = new Owner(), entered = gate(), release = gate();
    let child, grandchild, write, operation, finished = false;
    source.beforeSql = async (_kind, sql) => {
      if (sql === "INSERT INTO t VALUES(1,'late')") { entered.resolve(); await release.promise; }
    };
    operation = run(mode, source, tx => {
      child = tx.transaction(async inner => {
        grandchild = inner.transaction(async last => {
          write = last.execute("INSERT INTO t VALUES(1,'late')");
        });
      });
      return Promise.resolve(42);
    });
    void operation.then(() => { finished = true; }, () => { finished = true; });
    try {
      await Promise.race([entered.promise, operation.then(() => assert.fail("write was not held"))]);
      await tick();
      assert.equal(finished, false); assert.equal(source.depth, 3);
      assert.equal(source.events.some(([kind]) => kind === "release"), false);
      if (mode !== "capture") assert.deepEqual(source.rows("PRAGMA defer_foreign_keys"), [[1n]]);
      release.resolve();
      const result = await operation; await Promise.all([child, grandchild, write]);
      assert.equal(result.value, 42); assert.deepEqual(source.rows(), [[1n, "late"]]);
      clean(source); verifyCapture(result, source);
    } finally {
      release.resolve(); await Promise.allSettled([operation, child, grandchild, write]); source.close();
    }
  });

  test(`${mode}: independent callback trees do not share a global ownership gate`, async () => {
    const first = new Owner(), second = new Owner(), entered = gate(), release = gate();
    let operation;
    try {
      operation = run(mode, first, async tx => {
        await tx.transaction(async child => {
          await child.execute("INSERT INTO t VALUES(1,'first')");
          entered.resolve(); await release.promise;
        });
      });
      await entered.promise;
      const result = await run(mode, second, async tx => {
        await tx.transaction(async child => {
          await child.execute("INSERT INTO t VALUES(2,'second')");
        });
      });
      assert.deepEqual(second.rows(), [[2n, "second"]]); clean(second); verifyCapture(result, second);
      release.resolve(); await operation; clean(first);
    } finally {
      release.resolve(); await Promise.allSettled([operation]); first.close(); second.close();
    }
  });
}

test("deferred capture retains only released child rows with later FK parents", async () => {
  const ddl = "CREATE TABLE p(id INTEGER PRIMARY KEY);CREATE TABLE t(id INTEGER PRIMARY KEY,pid REFERENCES p(id))";
  const source = new Owner(ddl), receiver = new DatabaseSync(":memory:");
  try {
    const result = await captureChangeset(withDeferredForeignKeys(source), async tx => {
      await assert.rejects(tx.transaction(async child => {
        await child.execute("INSERT INTO t VALUES(9,99)"); throw new Error("discard");
      }), /discard/);
      await tx.transaction(async child => {
        await child.execute("INSERT INTO t VALUES(1,10)");
      });
      await tx.execute("INSERT INTO p VALUES(10)");
      return 17;
    }, { tables: ["p", "t"], maxRows: 2 });
    assert.equal(result.value, 17); assert.equal(result.changes, 2); assert.equal(result.touchedRows, 2);
    assert.deepEqual(source.rows(), [[1n, 10n]]); clean(source);
    assert.equal(decodeChangeset(result.changeset).flatMap(t => t.changes).length, 2);
    receiver.exec("PRAGMA foreign_keys=ON;" + ddl);
    assert.equal(receiver.applyChangeset(result.changeset), true);
    assert.deepEqual(receiver.prepare("SELECT * FROM t").all(), [{ id: 1, pid: 10 }].map(row => Object.assign(Object.create(null), row)));
    assert.deepEqual(receiver.prepare("PRAGMA foreign_key_check").all(), []);
  } finally { source.close(); receiver.close(); }
});

// Rebase decisions are stored with the received rows and deduplication receipt.
// A nested hook may discard optional work, but never an uncertain boundary.
import { applyChangeset, CHANGESET_RECEIPTS_TABLE } from "../src/changeset-apply.ts";
import { encodeChangeset, decodeRebaseInfo } from "../src/changeset-codec.ts";
const hookSchema = schema + ";CREATE TABLE decisions(id INTEGER PRIMARY KEY,info BLOB);" +
  "CREATE TABLE audit(id INTEGER PRIMARY KEY,value TEXT)";
const incoming = () => encodeChangeset([{ name: "t", primaryKey: [1, 0], changes: [
  { operation: "insert", indirect: false, new: [1n, "remote"] },
  { operation: "insert", indirect: false, new: [2n, "new"] },
] }]);
function hookOwner(ddl = hookSchema) {
  const owner = new Owner(ddl);
  owner.db.exec("INSERT INTO t VALUES(1,'local')");
  return owner;
}
function noReceipt(owner) {
  const present = owner.rows(`SELECT name FROM sqlite_schema WHERE name='${CHANGESET_RECEIPTS_TABLE}'`);
  if (present.length) assert.deepEqual(owner.rows(`SELECT * FROM ${CHANGESET_RECEIPTS_TABLE}`), []);
}
for (const defer of [false, true]) {
  const settings = { tables: ["t"], deliveryId: `hook:${defer}`, onConflict: () => "replace",
    ...(defer ? { foreignKeys: "defer" } : {}) };
  const label = `rebase hook${defer ? " with deferred FKs" : ""}`;
  test(`${label}: handled child rollback retains exact decisions and replay runs no hook`, async () => {
    const owner = hookOwner(); const discarded = new Error("optional decision path");
    let hooks = 0, oldChild, oldParent, savedBytes;
    try {
      const result = await applyChangeset(owner, incoming(), { ...settings, onRebase: async (tx, bytes) => {
        hooks++; oldParent = tx; savedBytes = new Uint8Array(bytes);
        assert.equal(typeof tx.transaction, "function");
        await assert.rejects(tx.transaction(async child => {
          await child.execute("INSERT INTO audit VALUES(9,'discard')");
          throw discarded;
        }), error => error === discarded);
        const answer = await tx.transaction(async child => {
          oldChild = child;
          await child.execute("INSERT INTO decisions VALUES(1,?)", [bytes]);
          await child.transaction(async last => {
            await last.execute("INSERT INTO audit VALUES(1,'kept')");
          });
          return 42;
        });
        assert.equal(answer, 42);
      } });
      assert.deepEqual(result, { applied: 2, omitted: 0, replayed: false });
      assert.deepEqual(owner.rows(), [[1n, "remote"], [2n, "new"]]);
      assert.deepEqual(owner.rows("SELECT * FROM audit"), [[1n, "kept"]]);
      const record = owner.rows("SELECT info FROM decisions")[0][0];
      assert.deepEqual(new Uint8Array(record), savedBytes);
      assert.equal(decodeRebaseInfo(record)[0].changes[0].replace, true);
      for (const saved of [oldParent, oldChild]) {
        const before = owner.events.length;
        await assert.rejects(saved.query("SELECT 1"), /scope has ended/);
        await assert.rejects(saved.transaction(async () => 1), /scope has ended/);
        assert.equal(owner.events.length, before);
      }
      const replay = await applyChangeset(owner, incoming(), { ...settings, onConflict: () => assert.fail("replayed policy"),
        onRebase: () => { hooks++; assert.fail("replayed hook"); } });
      assert.deepEqual(replay, { ...result, replayed: true }); assert.equal(hooks, 1); clean(owner);
    } finally { owner.close(); }
  });

  test(`${label}: dropped descendant write settles before receipt and commit`, async () => {
    const owner = hookOwner(), entered = gate(), release = gate();
    let child, grandchild, write, done = false, operation;
    const sql = "INSERT INTO audit VALUES(1,'held')";
    owner.beforeSql = async (_kind, statement) => { if (statement === sql) { entered.resolve(); await release.promise; } };
    operation = applyChangeset(owner, incoming(), { ...settings, onRebase: tx => {
      child = tx.transaction(async inner => {
        grandchild = inner.transaction(async last => { write = last.execute(sql); });
      });
    } });
    void operation.then(() => { done = true; }, () => { done = true; });
    try {
      await Promise.race([entered.promise, operation.then(() => assert.fail("hook SQL never started"))]);
      await tick(); assert.equal(done, false); assert.equal(owner.depth, 3);
      assert.equal(owner.events.some(([kind, , text]) => kind === "execute" && text.startsWith(`INSERT OR ABORT INTO main."${CHANGESET_RECEIPTS_TABLE}"`)), false);
      release.resolve(); await operation; await Promise.all([child, grandchild, write]);
      assert.deepEqual(owner.rows("SELECT * FROM audit"), [[1n, "held"]]); clean(owner);
    } finally { release.resolve(); await Promise.allSettled([operation, child, grandchild, write]); owner.close(); }
  });

  test(`${label}: cancellation waits for admitted child SQL then rolls everything back`, async () => {
    const owner = hookOwner(), entered = gate(), release = gate(), abort = new AbortController();
    let operation, child, write, done = false;
    owner.beforeSql = async (_kind, sql) => { if (sql === "INSERT INTO audit VALUES(1,'held')") { entered.resolve(); await release.promise; } };
    operation = applyChangeset(owner, incoming(), { ...settings, signal: abort.signal, onRebase: tx => {
      child = tx.transaction(async inner => { write = inner.execute("INSERT INTO audit VALUES(1,'held')"); });
    } });
    void operation.then(() => { done = true; }, () => { done = true; });
    try {
      await Promise.race([entered.promise, operation.then(() => assert.fail("no held write"))]);
      abort.abort("stop"); await tick(); assert.equal(done, false);
      release.resolve(); await assert.rejects(operation, /cancel/i);
      await Promise.allSettled([child, write]);
      assert.deepEqual(owner.rows(), [[1n, "local"]]);
      assert.deepEqual(owner.rows("SELECT * FROM audit"), []); noReceipt(owner); clean(owner);
    } finally { release.resolve(); await Promise.allSettled([operation, child, write]); owner.close(); }
  });

  test(`${label}: parent SQL cannot enter a live child; reentry cannot capture parent writes`, async () => {
    const owner = hookOwner(); let rejected, calls = 0, attempted;
    try {
      await applyChangeset(owner, incoming(), { ...settings, onRebase: async tx => {
        owner.beforeSql = (kind, sql) => {
          if (kind !== "execute" || sql !== "INSERT INTO audit VALUES(1,'parent')") return;
          owner.beforeSql = null;
          attempted = tx.transaction(async () => { calls++; });
          void attempted.catch(() => {});
        };
        assert.equal(await tx.execute("INSERT INTO audit VALUES(1,'parent')"), 1);
        await assert.rejects(attempted, error => { rejected = error; return true; });
        await tx.transaction(async child => {
          const before = owner.events.length;
          await assert.rejects(tx.execute("DELETE FROM t"), /owns this SQL scope/);
          await assert.rejects(tx.query("SELECT * FROM t"), /owns this SQL scope/);
          await assert.rejects(tx.transaction(async () => assert.fail("sibling entered")), /owns this SQL scope/);
          assert.equal(owner.events.length, before);
          await child.execute("INSERT INTO audit VALUES(2,'child')");
        });
      } });
      assert.equal(calls, 0); assert.equal(rejected.code, "ERR_FSQLITE_CHANGESET_BUSY");
      assert.deepEqual(owner.rows("SELECT * FROM audit ORDER BY id"), [[1n, "parent"], [2n, "child"]]); clean(owner);
    } finally { await Promise.allSettled([attempted]); owner.close(); }
  });

  test(`${label}: recovered child SQL failure is local but direct caught SQL failure aborts`, async () => {
    const owner = hookOwner();
    try {
      await applyChangeset(owner, incoming(), { ...settings, onRebase: async tx => {
        await assert.rejects(tx.transaction(async child => {
          await child.execute("INSERT INTO audit VALUES(1,'first')");
          await child.execute("INSERT INTO audit VALUES(1,'duplicate')").catch(() => {});
        }), /UNIQUE/);
        await tx.execute("INSERT INTO audit VALUES(2,'kept')");
      } });
      assert.deepEqual(owner.rows("SELECT * FROM audit"), [[2n, "kept"]]); clean(owner);
      await assert.rejects(applyChangeset(owner, incoming(), { ...settings, deliveryId: "direct-failure", onRebase: async tx => {
        await tx.execute("INSERT INTO audit VALUES(3,'rolled-back')");
        await tx.execute("INSERT INTO audit VALUES(2,'duplicate')").catch(() => {});
      } }), /UNIQUE/);
      assert.deepEqual(owner.rows("SELECT * FROM audit"), [[2n, "kept"]]);
      assert.deepEqual(owner.rows(`SELECT delivery_id FROM ${CHANGESET_RECEIPTS_TABLE}`), [[settings.deliveryId]]);
    } finally { owner.close(); }
  });

  test(`${label}: caught uncertain grandchild cleanup cannot publish rows or a receipt`, async () => {
    const owner = hookOwner(), cleanup = new Error("cleanup lost"), callback = new Error("independent hook error");
    let failure, blocked, nestedError;
    owner.afterRollback = depth => { if (depth === 2) throw cleanup; };
    try {
      try {
        await applyChangeset(owner, incoming(), { ...settings, onRebase: async tx => {
          await tx.transaction(async child => {
            await child.transaction(async last => {
              await last.execute("INSERT INTO audit VALUES(9,'uncertain')");
              throw new Error("grandchild failed");
            }).catch(error => { nestedError = error; });
          }).catch(() => {});
          const before = owner.events.length;
          await tx.query("SELECT 1").catch(error => { blocked = error; });
          assert.equal(owner.events.length, before, "uncertainty must fence even reads");
          throw callback;
        } });
      } catch (error) { failure = error; }
      assert.equal(failure.code, defer ? "ERR_FSQLITE_FOREIGN_KEY_STATE" : "ERR_FSQLITE_CHANGESET_STATE");
      assert.equal(blocked.code, "ERR_FSQLITE_CHANGESET_STATE");
      // The enclosing FK owner also retains its own cleanup obligation rather
      // than hiding it. The hook's failure is preserved as that owner's cause.
      const hookFailure = defer ? failure.cause : failure;
      assert.equal(hookFailure.code, "ERR_FSQLITE_CHANGESET_STATE");
      assert.equal(hookFailure.cause, callback); assert.equal(hookFailure.cleanupErrors.length, 1);
      assert.equal(failure.cleanupErrors[0], hookFailure.cleanupErrors[0]);
      assert.ok(Object.isFrozen(failure.cleanupErrors));
      assert.ok(nestedError); assert.equal(failure.cleanupErrors[0].errors[1], cleanup);
      assert.deepEqual(owner.rows(), [[1n, "local"]]);
      assert.deepEqual(owner.rows("SELECT * FROM audit"), []); noReceipt(owner); clean(owner);
    } finally { owner.close(); }
  });
}

test("rebase hook without child capability retains plain SQL and does not invent savepoints", async () => {
  const owner = hookOwner();
  const target = { transaction: work => owner.transaction(tx => work({ execute: tx.execute, query: tx.query })) };
  try {
    await applyChangeset(target, incoming(), { tables: ["t"], deliveryId: "plain-hook", onConflict: () => "replace",
      onRebase: async (tx, bytes) => {
        assert.equal(tx.transaction, undefined);
        await tx.execute("INSERT INTO decisions VALUES(1,?)", [bytes]);
      } });
    assert.equal(owner.events.filter(([kind]) => kind === "begin").length, 1);
    assert.equal(owner.rows("SELECT * FROM decisions").length, 1); clean(owner);
  } finally { owner.close(); }
});

test("rebase hook child keeps FK deferral until its parent arrives in later hook work", async () => {
  const owner = hookOwner(hookSchema + ";CREATE TABLE p(id INTEGER PRIMARY KEY);" +
    "CREATE TABLE child(id INTEGER PRIMARY KEY,pid REFERENCES p(id))");
  try {
    await applyChangeset(owner, incoming(), { tables: ["t"], deliveryId: "later-parent",
      foreignKeys: "defer", onConflict: () => "replace", onRebase: async (tx, bytes) => {
        await tx.transaction(async nested => {
          await nested.execute("INSERT INTO child VALUES(1,10)");
          await nested.execute("INSERT INTO decisions VALUES(1,?)", [bytes]);
        });
        await tx.execute("INSERT INTO p VALUES(10)");
      } });
    assert.deepEqual(owner.rows("SELECT * FROM child"), [[1n, 10n]]);
    assert.deepEqual(owner.rows("PRAGMA foreign_key_check"), []); clean(owner);
  } finally { owner.close(); }
});
