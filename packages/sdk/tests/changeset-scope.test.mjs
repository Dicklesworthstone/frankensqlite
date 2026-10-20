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
