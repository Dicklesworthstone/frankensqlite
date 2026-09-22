// Use the existing SDK source-loader.mjs. Production SDK/worker with a Node
// SQLite reference core; not native FrankenSQLite or browser certification.
import assert from "node:assert/strict";
import test from "node:test";
import { sqliteSnapshotWorker } from "../../worker/tests/helpers/snapshot-sqlite-core.mjs";
import { FrankenDB } from "../src/database.ts";
import { FrankenSnapshotPool } from "../src/snapshot-pool.ts";

async function fixture(t) {
  const source = sqliteSnapshotWorker();
  const db = await FrankenDB.open({ worker: source.worker });
  let snapshot;
  try {
    await db.executeBatch(
      "CREATE TABLE items(id INTEGER PRIMARY KEY,n); INSERT INTO items VALUES(1,0); " +
      "CREATE TABLE audit(n); CREATE TRIGGER log AFTER UPDATE ON items " +
      "BEGIN INSERT INTO audit VALUES(new.n); END;",
    );
    snapshot = await db.export();
  } finally {
    await db.close();
  }
  const replica = sqliteSnapshotWorker();
  const pool = await FrankenSnapshotPool.open(snapshot, {
    workers: 1,
    worker: () => replica.worker,
  });
  t.after(() => pool.close());
  return { pool, replica };
}

test("CTE writes are refused before transport; query_only independently still rejects them", { timeout: 10000 }, async (t) => {
  const { pool, replica } = await fixture(t);
  const writes = [
    "WITH x AS (SELECT 1) INSERT INTO items VALUES(2,9) RETURNING n",
    "WITH x AS (SELECT 1) REPLACE INTO items VALUES(1,9) RETURNING n",
    "WITH x AS (SELECT 1) UPDATE items SET n=9 RETURNING n",
    "WITH x AS (SELECT 1) DELETE FROM items RETURNING n",
  ];
  for (const sql of writes) {
    const before = replica.worker.requests.length;
    await assert.rejects(pool.query(sql), { code: "ERR_FSQLITE_POOL_READ_ONLY" });
    assert.equal(replica.worker.requests.length, before);
    assert.equal(pool.stats.pendingBytes, 0);
    assert.equal(pool.stats.pendingQueries, 0);
    // Deliberately bypass SDK admission to retain independent engine-policy
    // coverage. A passing preflight test cannot prove query_only enforcement.
    await assert.rejects(replica.handles[0].query(sql), /readonly/i);
  }
  assert.equal(pool.stats.rejectedQueries, writes.length);
  assert.deepEqual((await pool.query("SELECT * FROM items")).rowArrays, [[1, 0]]);
  assert.deepEqual((await pool.query("SELECT * FROM audit")).rowArrays, []);
});

test("plans of writable CTEs and misleading keyword prefixes never reach replicas", { timeout: 10000 }, async (t) => {
  const { pool, replica } = await fixture(t);
  for (const sql of [
    "EXPLAIN WITH x AS (SELECT 1) UPDATE items SET n=9",
    "EXPLAIN QUERY PLAN WITH x AS (SELECT 1) DELETE FROM items",
    "EXPLAIN PRAGMA query_only=OFF",
    "EXPLAIN QUERY PLAN PRAGMA query_only=OFF",
    "SELECT_not_a_keyword 1", "WITHnot_a_keyword x AS (SELECT 1) SELECT 1",
    "EXPLAIN QUERY PLAN SELECTx", "EXPLAIN QUERY PLAN EXPLAIN SELECT 1",
  ]) {
    const before = replica.worker.requests.length;
    await assert.rejects(pool.query(sql), { code: "ERR_FSQLITE_POOL_READ_ONLY" });
    assert.equal(replica.worker.requests.length, before, sql);
    assert.equal(pool.stats.pendingBytes, 0);
  }
});

test("ordinary/recursive CTEs and their plans preserve snapshot identity", { timeout: 10000 }, async (t) => {
  const { pool } = await fixture(t);
  const identity = pool.snapshot;
  for (const sql of [
    "WITH x AS (SELECT 42 AS n) SELECT n FROM x",
    "WITH x(n) AS MATERIALIZED (VALUES(42)), y AS NOT MATERIALIZED (SELECT n FROM x) SELECT n FROM y",
    "WITH x AS (WITH y AS (SELECT 42 AS n) SELECT n FROM y) SELECT n FROM x",
    "WITH RECURSIVE x(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM x WHERE n<42) SELECT max(n) AS n FROM x",
  ]) {
    const result = await pool.query(sql);
    assert.deepEqual(result.rowArrays, [[42]]);
    assert.equal(result.snapshot, identity);
    for (const prefix of ["EXPLAIN ", "EXPLAIN /*x*/ QUERY /*y*/ PLAN "]) {
      const plan = await pool.query(prefix + sql);
      assert.ok(plan.rowArrays.length > 0);
      assert.equal(plan.snapshot, identity);
    }
  }
  assert.deepEqual((await pool.query("WITH x(n) AS (VALUES(?)) SELECT n FROM x", [42])).rowArrays, [[42]]);
  assert.equal(pool.stats.pendingBytes, 0);
  assert.equal(pool.stats.pendingQueries, 0);
});
