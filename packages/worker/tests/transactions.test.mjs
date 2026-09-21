// Direct production host ownership tests; Node SQLite is the reference core.

import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import { validateManagedSql } from "../src/transactions.ts";
import { sqliteSnapshotWorker } from "./helpers/snapshot-sqlite-core.mjs";

async function fixture(hooks = {}) {
  const f = sqliteSnapshotWorker(hooks);
  let next = 1;
  const send = (r) => f.host.handle({ requestId: next++, ...r });
  const ok = async (r) => {
    const response = await send(r);
    assert.notEqual(response.kind, "error", JSON.stringify(response));
    return response;
  };
  const fail = async (r, code) => {
    const response = await send(r);
    assert.equal(response.kind, "error");
    if (code) assert.equal(response.error.code, code);
    return response.error;
  };
  const boundary = (action, transactionId, parentId) =>
    ok({
      kind: "transaction",
      action,
      transactionId,
      ...(parentId === undefined ? {} : { parentId }),
    });
  const execute = (sql, transactionId) =>
    ok({ kind: "execute", sql, ...(transactionId ? { transactionId } : {}) });
  const rows = async () =>
    (await ok({ kind: "query", sql: "SELECT id,v FROM t ORDER BY id" })).data.rowArrays;
  await ok({ kind: "init", config: {} });
  await execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT UNIQUE)");
  await execute("INSERT INTO t VALUES(1,'seed')");
  return { ...f, send, ok, fail, boundary, execute, rows };
}

const fenced = "ERR_FSQLITE_TRANSACTION_ABORTED";
const ownership = "ERR_FSQLITE_TRANSACTION_OWNERSHIP";

test("raw worker scopes commit nested work and roll back a failed child only", async () => {
  const f = await fixture();
  await f.boundary("begin", "1");
  await f.execute("INSERT INTO t VALUES(2,'parent')", "1");
  await f.boundary("begin", "2", "1");
  await f.execute("INSERT INTO t VALUES(3,'sibling')", "2");
  await f.boundary("commit", "2");
  await f.boundary("begin", "3", "1");
  await f.execute("INSERT INTO t VALUES(4,'child')", "3");
  await f.fail({ kind: "execute", transactionId: "3", sql: "INSERT INTO t VALUES(5,'seed')" });
  await f.fail({ kind: "transaction", action: "commit", transactionId: "3" }, fenced);
  await f.boundary("rollback", "3");
  await f.execute("INSERT INTO t VALUES(6,'after')", "1");
  await f.boundary("commit", "1");
  assert.deepEqual(await f.rows(), [
    [1, "seed"],
    [2, "parent"],
    [3, "sibling"],
    [6, "after"],
  ]);
  await f.ok({ kind: "close" });
});

test("untagged foreign queries, SQL, init and checkpoint cannot enter an owned scope", async () => {
  const f = await fixture();
  await f.boundary("begin", "1");
  for (const request of [
    { kind: "execute", sql: "INSERT INTO t VALUES(2,'foreign')" },
    { kind: "query", sql: "SELECT * FROM t" },
    { kind: "init", config: {} },
    { kind: "checkpoint" },
    { kind: "export" },
    { kind: "execute-batch", sql: "COMMIT" },
  ])
    await f.fail(request, ownership);
  await f.execute("INSERT INTO t VALUES(3,'owner')", "1");
  await f.boundary("commit", "1");
  assert.deepEqual(await f.rows(), [
    [1, "seed"],
    [3, "owner"],
  ]);
  await f.ok({ kind: "close" });
});

test("worker failure fence precedes already-queued autocommit escape attempts", async () => {
  const f = await fixture();
  await f.boundary("begin", "1");
  const failed = f.send({
    kind: "execute",
    transactionId: "1",
    sql: "INSERT OR ROLLBACK INTO t VALUES(2,'seed')",
  });
  const queued = f.send({
    kind: "execute",
    transactionId: "1",
    sql: "INSERT INTO t VALUES(3,'escaped')",
  });
  assert.equal((await failed).kind, "error");
  assert.equal((await queued).error.code, fenced);
  await f.fail(
    { kind: "transaction", action: "rollback", transactionId: "1" },
    "ERR_FSQLITE_TRANSACTION_CONNECTION_UNUSABLE",
  );
  await f.fail(
    { kind: "execute", sql: "INSERT INTO t VALUES(4,'later')" },
    "ERR_FSQLITE_TRANSACTION_CONNECTION_UNUSABLE",
  );
  const reader = new DatabaseSync(f.handles[0].path);
  assert.deepEqual(
    reader
      .prepare("SELECT id FROM t ORDER BY id")
      .all()
      .map((r) => r.id),
    [1],
  );
  reader.close();
});

test("stale scope ids never attach to a later transaction or reinitialization", async () => {
  const f = await fixture();
  await f.boundary("begin", "1");
  await f.boundary("commit", "1");
  await f.boundary("begin", "2");
  await f.fail(
    { kind: "execute", transactionId: "1", sql: "INSERT INTO t VALUES(2,'stale')" },
    ownership,
  );
  await f.execute("INSERT INTO t VALUES(3,'current')", "2");
  await f.boundary("commit", "2");
  await f.fail(
    { kind: "transaction", action: "begin", transactionId: "1" },
    "ERR_FSQLITE_TRANSACTION_INPUT",
  );
  await f.ok({ kind: "init", config: {} });
  await f.fail(
    { kind: "transaction", action: "begin", transactionId: "2" },
    "ERR_FSQLITE_TRANSACTION_INPUT",
  );
  await f.ok({ kind: "close" });
});

test("raw owned prepared handles expire and finalize at scope boundaries", async () => {
  const f = await fixture();
  await f.boundary("begin", "1");
  const prepared = await f.ok({
    kind: "prepare",
    transactionId: "1",
    sql: "INSERT INTO t VALUES(?,?)",
  });
  await f.ok({
    kind: "statement-execute",
    transactionId: "1",
    statementId: prepared.data.statementId,
    params: [2, "prepared"],
  });
  await f.boundary("commit", "1");
  await f.fail(
    {
      kind: "statement-execute",
      transactionId: "1",
      statementId: prepared.data.statementId,
      params: [3, "stale"],
    },
    "ERR_FSQLITE_TRANSACTION_CLOSED",
  );
  await f.fail({
    kind: "statement-execute",
    statementId: prepared.data.statementId,
    params: [3, "stale"],
  });
  assert.deepEqual(await f.rows(), [
    [1, "seed"],
    [2, "prepared"],
  ]);
  await f.ok({ kind: "close" });
});

test("failed scopes still permit handle finalization and rollback", async () => {
  const f = await fixture();
  await f.boundary("begin", "1");
  const prepared = await f.ok({ kind: "prepare", transactionId: "1", sql: "SELECT * FROM t" });
  await f.fail({ kind: "execute", transactionId: "1", sql: "invalid SQL" });
  await f.fail({ kind: "query", transactionId: "1", sql: "SELECT * FROM t" }, fenced);
  await f.ok({
    kind: "statement-finalize",
    transactionId: "1",
    statementId: prepared.data.statementId,
  });
  await f.boundary("rollback", "1");
  assert.deepEqual(await f.rows(), [[1, "seed"]]);
  await f.ok({ kind: "close" });
});

test("failed top-level BEGIN never rolls back an existing manual transaction", async () => {
  const f = await fixture();
  await f.ok({ kind: "execute-batch", sql: "BEGIN; INSERT INTO t VALUES(2,'manual')" });
  await f.fail({ kind: "transaction", action: "begin", transactionId: "1" });
  await f.fail(
    { kind: "transaction", action: "rollback", transactionId: "1" },
    "ERR_FSQLITE_TRANSACTION_CLOSED",
  );
  await f.ok({ kind: "execute-batch", sql: "COMMIT" });
  assert.deepEqual(await f.rows(), [
    [1, "seed"],
    [2, "manual"],
  ]);
  await f.ok({ kind: "close" });
});

test("a rejected deferred commit keeps ownership for explicit rollback", async () => {
  const f = await fixture();
  await f.ok({
    kind: "execute-batch",
    sql: "PRAGMA foreign_keys=ON; CREATE TABLE p(id PRIMARY KEY); CREATE TABLE c(v REFERENCES p DEFERRABLE INITIALLY DEFERRED)",
  });
  await f.boundary("begin", "1");
  await f.execute("INSERT INTO c VALUES(1)", "1");
  await f.fail({ kind: "transaction", action: "commit", transactionId: "1" });
  await f.fail({ kind: "execute", sql: "INSERT INTO p VALUES(1)" }, ownership);
  await f.fail({ kind: "execute", transactionId: "1", sql: "INSERT INTO p VALUES(1)" }, fenced);
  await f.boundary("rollback", "1");
  assert.deepEqual((await f.ok({ kind: "query", sql: "SELECT * FROM c" })).data.rowArrays, []);
  await f.ok({ kind: "close" });
});

test("preflight rejects an entire script before its first write when a tail commits", async () => {
  const f = await fixture();
  await f.boundary("begin", "1");
  f.events.length = 0;
  await f.fail(
    {
      kind: "execute-batch",
      transactionId: "1",
      sql: "INSERT INTO t VALUES(2,'tail'); /*x*/ END TRANSACTION",
    },
    "ERR_FSQLITE_TRANSACTION_SQL",
  );
  assert.equal(f.events.length, 0);
  await f.boundary("rollback", "1");
  assert.deepEqual(await f.rows(), [[1, "seed"]]);
  await f.ok({ kind: "close" });
});

test("managed scripts accept multi-statement trigger bodies with CASE END and quoted delimiters", async () => {
  const f = await fixture();
  await f.boundary("begin", "1");
  await f.ok({
    kind: "execute-batch",
    transactionId: "1",
    sql: `
    CREATE TABLE audit(v);
    CREATE TEMP TRIGGER [log;entry] AFTER INSERT ON t BEGIN
      INSERT INTO audit VALUES(CASE WHEN new.id>0 THEN 'COMMIT; END' ELSE 'ROLLBACK' END);
      UPDATE audit SET v=v || ';';
    END;
    INSERT INTO t VALUES(2,'trigger');
  `,
  });
  await f.boundary("commit", "1");
  assert.deepEqual((await f.ok({ kind: "query", sql: "SELECT * FROM audit" })).data.rowArrays, [
    ["COMMIT; END;"],
  ]);
  await f.ok({ kind: "close" });
});

test("parent SQL cannot enter a live child and does not poison the child", async () => {
  const f = await fixture();
  await f.boundary("begin", "1");
  await f.boundary("begin", "2", "1");
  await f.fail(
    { kind: "execute", transactionId: "1", sql: "INSERT INTO t VALUES(2,'wrong')" },
    ownership,
  );
  await f.execute("INSERT INTO t VALUES(3,'child')", "2");
  await f.boundary("commit", "2");
  await f.boundary("commit", "1");
  assert.deepEqual(await f.rows(), [
    [1, "seed"],
    [3, "child"],
  ]);
  await f.ok({ kind: "close" });
});

test("rollback failure is terminal before the next queued request", async () => {
  const f = await fixture({
    beforeBatch(sql) {
      if (sql === "ROLLBACK") throw new Error("rollback injected");
    },
  });
  await f.boundary("begin", "1");
  await f.execute("INSERT INTO t VALUES(2,'uncommitted')", "1");
  const rollback = f.send({ kind: "transaction", action: "rollback", transactionId: "1" });
  const queued = f.send({ kind: "execute", sql: "INSERT INTO t VALUES(3,'late')" });
  const failed = await rollback;
  assert.equal(failed.error.code, "ERR_FSQLITE_TRANSACTION_CONNECTION_UNUSABLE");
  assert.equal(failed.error.cause.message, "rollback injected");
  assert.equal((await queued).error.code, failed.error.code);
  const reader = new DatabaseSync(f.handles[0].path);
  assert.equal(reader.prepare("SELECT count(*) n FROM t").get().n, 1);
  reader.close();
});

test("nesting is bounded and admitted scopes still unwind after an over-limit begin", async () => {
  const f = await fixture();
  await f.boundary("begin", "1");
  for (let n = 2; n <= 64; n++) await f.boundary("begin", String(n), String(n - 1));
  await f.fail(
    { kind: "transaction", action: "begin", transactionId: "65", parentId: "64" },
    "ERR_FSQLITE_TRANSACTION_INPUT",
  );
  for (let n = 64; n >= 1; n--) await f.boundary("rollback", String(n));
  assert.deepEqual(await f.rows(), [[1, "seed"]]);
  await f.ok({ kind: "close" });
});

test("transaction SQL preflight handles boundary keywords and quote/comment variants", () => {
  for (const word of ["BEGIN", "COMMIT", "END", "ROLLBACK", "SAVEPOINT", "RELEASE"]) {
    for (const prefix of ["", " ; ; ", "\ufeff--x\n/*y*/ "]) {
      assert.throws(() => validateManagedSql(prefix + word.toLowerCase() + ";"), {
        code: "ERR_FSQLITE_TRANSACTION_SQL",
      });
      assert.throws(() => validateManagedSql("SELECT 1;" + prefix + word, true), {
        code: "ERR_FSQLITE_TRANSACTION_SQL",
      });
    }
  }
  for (const sql of [
    "SELECT 'COMMIT;''END'",
    'SELECT "x;""y"',
    "SELECT `x;``y`",
    "SELECT [END;]",
    "WITH t AS (SELECT 1) SELECT * FROM t",
    "SELECT CASE WHEN 1 THEN 'x' END; -- END",
    "EXPLAIN COMMIT",
  ]) {
    assert.doesNotThrow(() => validateManagedSql(sql));
  }
  assert.throws(() => validateManagedSql("SELECT 1; SELECT 2"), {
    code: "ERR_FSQLITE_TRANSACTION_SQL",
  });
  assert.throws(() => validateManagedSql("SELECT 'x\0'"), { code: "ERR_FSQLITE_TRANSACTION_SQL" });
  assert.throws(() => validateManagedSql("SELECT 'unterminated"), {
    code: "ERR_FSQLITE_TRANSACTION_SQL",
  });
  assert.throws(() => validateManagedSql("/*comment*/ ; "), {
    code: "ERR_FSQLITE_TRANSACTION_SQL",
  });
});
