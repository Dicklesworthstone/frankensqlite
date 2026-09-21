// Exercises the production SDK AND worker guard, with Node SQLite as the core.
// This is a SQL-reference integration, not FrankenSQLite WASM certification.

import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import { sqliteSnapshotWorker } from "../../worker/tests/helpers/snapshot-sqlite-core.mjs";
import { FrankenDB } from "../src/database.ts";

function causes(error) {
  return [
    error,
    ...(error?.errors ?? []).flatMap(causes),
    ...(error?.cause ? causes(error.cause) : []),
  ];
}
const hasCode = (expected) => (error) => causes(error).some((e) => e?.code === expected);
async function fixture(options = {}) {
  const f = sqliteSnapshotWorker(options);
  const db = await FrankenDB.open({ worker: f.worker, requestLimits: options.requestLimits });
  await db.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)");
  return { ...f, db };
}
function stored(f) {
  const oracle = new DatabaseSync(f.handles[0].path, { readOnly: true });
  try {
    return oracle
      .prepare("SELECT id FROM items ORDER BY id")
      .all()
      .map((r) => r.id);
  } finally {
    oracle.close();
  }
}

for (const sql of [
  "COMMIT",
  "  -- end early\nEND TRANSACTION;",
  "ROLLBACK",
  "SAVEPOINT manual",
  "RELEASE manual",
]) {
  test(`managed execute refuses transaction escape: ${sql}`, async () => {
    const f = await fixture();
    await assert.rejects(
      f.db.transaction(async (tx) => {
        await tx.execute("INSERT INTO items VALUES (1,'before')");
        await tx.execute(sql);
      }),
      hasCode("ERR_FSQLITE_TRANSACTION_SQL"),
    );
    assert.deepEqual(stored(f), []);
    await f.db.close();
  });
}

test("queued SQL cannot escape an implicit OR ROLLBACK into autocommit", async () => {
  const f = await fixture();
  const results = [];
  await assert.rejects(
    f.db.transaction(async (tx) => {
      await tx.execute("INSERT INTO items VALUES (1,'first')");
      const failure = tx.execute("INSERT OR ROLLBACK INTO items VALUES (1,'duplicate')");
      const escape = tx.execute("INSERT INTO items VALUES (2,'must not commit')");
      results.push(...(await Promise.allSettled([failure, escape])));
    }),
  );
  assert.equal(results[0].status, "rejected");
  assert.equal(results[1].status, "rejected");
  assert.ok(hasCode("ERR_FSQLITE_TRANSACTION_ABORTED")(results[1].reason));
  assert.deepEqual(stored(f), []);
  assert.equal(f.worker.terminateCount, 1);
});

test("every managed database and prepared operation carries its immutable owner", async () => {
  const f = await fixture();
  await f.db.transaction(async (tx) => {
    await tx.execute("INSERT INTO items VALUES (1,'direct')");
    await tx.executeMany("INSERT INTO items VALUES (?,?)", [[2, "many"]]);
    await tx.query("SELECT * FROM items");
    const stmt = await tx.prepare("INSERT INTO items VALUES (?,?)");
    await stmt.execute([3, "prepared"]);
    await stmt.executeMany([[4, "prepared many"]]);
    await stmt.finalize();
    const select = await tx.prepare("SELECT * FROM items");
    await select.query(); // Automatic finalization must carry the owner too.
  });
  const requests = f.worker.requests;
  const begin = requests.findIndex((r) => r.kind === "transaction" && r.action === "begin");
  assert.ok(begin >= 0, "SDK must use explicit worker transaction protocol");
  for (const r of requests.slice(begin)) assert.equal(r.transactionId, "1");
  assert.deepEqual(stored(f), [1, 2, 3, 4]);
  await f.db.close();
});

test("unowned raw worker request is rejected while the public callback is suspended", async () => {
  const f = await fixture();
  await f.db.transaction(async (tx) => {
    await tx.execute("INSERT INTO items VALUES (1,'owned')");
    const foreign = await f.host.handle({
      kind: "execute",
      requestId: 999,
      sql: "INSERT INTO items VALUES (2,'foreign')",
    });
    assert.equal(foreign.kind, "error");
    assert.equal(foreign.error.code, "ERR_FSQLITE_TRANSACTION_OWNERSHIP");
    await tx.execute("INSERT INTO items VALUES (3,'still owned')");
  });
  assert.deepEqual(stored(f), [1, 3]);
  await f.db.close();
});

test("nested transactions use distinct parent-linked scopes and preserve siblings", async () => {
  const f = await fixture();
  await f.db.transaction(async (parent) => {
    await parent.execute("INSERT INTO items VALUES (1,'parent')");
    await assert.rejects(
      parent.transaction(async (child) => {
        await child.execute("INSERT INTO items VALUES (2,'child')");
        await child.execute("END");
      }),
      hasCode("ERR_FSQLITE_TRANSACTION_SQL"),
    );
    await parent.transaction(async (child) => {
      const stmt = await child.prepare("INSERT INTO items VALUES (?,?)");
      await stmt.execute([3, "sibling"]);
    });
  });
  assert.deepEqual(stored(f), [1, 3]);
  assert.deepEqual(
    f.worker.requests
      .filter((r) => r.kind === "transaction")
      .map(({ action, transactionId, parentId }) => [action, transactionId, parentId]),
    [
      ["begin", "1", undefined],
      ["begin", "2", "1"],
      ["rollback", "2", undefined],
      ["begin", "3", "1"],
      ["commit", "3", undefined],
      ["commit", "1", undefined],
    ],
  );
  await f.db.close();
});

test("transaction script creates triggers atomically and preflights all top-level statements", async () => {
  const f = await fixture();
  await f.db.transaction(async (tx) => {
    await tx.executeBatch(`CREATE TABLE audit(id INTEGER); CREATE TRIGGER record_item AFTER INSERT ON items
      BEGIN INSERT INTO audit VALUES (CASE WHEN NEW.id > 0 THEN NEW.id ELSE 0 END); END;
      INSERT INTO items VALUES (1,'semi;colon');`);
    assert.equal((await tx.query("SELECT count(*) AS n FROM audit")).rows[0].n, 1);
  });
  await assert.rejects(
    f.db.transaction((tx) => tx.executeBatch("INSERT INTO items VALUES(2,'no'); /*tail*/ COMMIT;")),
    hasCode("ERR_FSQLITE_TRANSACTION_SQL"),
  );
  assert.deepEqual(stored(f), [1]);
  await f.db.close();
});

test("stream chunks, prepare and cleanup are bound to the stream child scope", async () => {
  const f = await fixture({ requestLimits: { maxPendingRequests: 1, maxPendingBytes: 4096 } });
  const rows = Array.from({ length: 600 }, (_, i) => [i, `v${i}`]);
  await f.db.transaction(async (parent) => {
    await parent.executeStream("INSERT INTO items VALUES (?,?)", rows, { batchSize: 10 });
    const owned = f.worker.requests.filter((r) => r.transactionId === "2");
    assert.equal(owned.filter((r) => r.kind === "statement-execute-many").length, 60);
    assert.ok(owned.some((r) => r.kind === "prepare"));
    assert.ok(owned.some((r) => r.kind === "statement-finalize"));
    await assert.rejects(
      parent.executeStream(
        "INSERT INTO items VALUES (?,?)",
        [
          [700, "x"],
          [0, "duplicate"],
        ],
        { batchSize: 1 },
      ),
    );
    assert.equal((await parent.query("SELECT count(*) AS n FROM items")).rows[0].n, 600);
  });
  assert.equal(stored(f).length, 600);
  await f.db.close();
});

test("failed managed BEGIN leaves an existing manual transaction intact", async () => {
  const f = await fixture();
  await f.db.executeBatch("BEGIN; INSERT INTO items VALUES (1,'manual')");
  await assert.rejects(f.db.transaction(() => assert.fail("callback must not run")));
  assert.equal((await f.db.query("SELECT count(*) AS n FROM items")).rows[0].n, 1);
  await f.db.executeBatch("ROLLBACK");
  assert.deepEqual(stored(f), []);
  await f.db.transaction((tx) => tx.execute("INSERT INTO items VALUES (2,'new')"));
  assert.deepEqual(stored(f), [2]);
  await f.db.close();
});

for (const operation of ["query", "prepare"]) {
  test(`${operation} cannot hide a manual commit inside a managed callback`, async () => {
    const f = await fixture();
    await assert.rejects(
      f.db.transaction(async (tx) => {
        await tx.execute("INSERT INTO items VALUES (1,'before')");
        await tx[operation]("/* control */ END;");
      }),
      hasCode("ERR_FSQLITE_TRANSACTION_SQL"),
    );
    assert.deepEqual(stored(f), []);
    await f.db.close();
  });
}

test("caught SQL failures fence every remaining operation but still permit cleanup", async () => {
  const f = await fixture();
  await assert.rejects(
    f.db.transaction(async (tx) => {
      const stmt = await tx.prepare("INSERT INTO items VALUES (?,?)");
      await stmt.execute([1, "before"]);
      await assert.rejects(stmt.execute([1, "duplicate"]));
      await assert.rejects(
        tx.execute("INSERT INTO items VALUES (2,'later')"),
        hasCode("ERR_FSQLITE_TRANSACTION_ABORTED"),
      );
      await stmt.finalize();
    }),
  );
  assert.deepEqual(stored(f), []);
  await f.db.transaction((tx) => tx.execute("INSERT INTO items VALUES (3,'next transaction')"));
  assert.deepEqual(stored(f), [3]);
  await f.db.close();
});

test("public callbacks receive only their handle, never the internal mutable scope", async () => {
  const f = await fixture();
  await f.db.transaction(async function (tx) {
    assert.equal(arguments.length, 1);
    await tx.transaction(function (child) {
      assert.equal(arguments.length, 1);
      return child.query("SELECT 1");
    });
  });
  await f.db.close();
});

test("prepared handles cannot be retagged to enter a child and expired ids cannot be reused", async () => {
  const f = await fixture();
  await f.db.transaction(async (parent) => {
    const stmt = await parent.prepare("INSERT INTO items VALUES (?,?)");
    const prepared = f.worker.requests.find((r) => r.kind === "prepare");
    await parent
      .transaction(async (child) => {
        const forged = await f.host.handle({
          kind: "statement-execute",
          requestId: 999,
          statementId: "1",
          transactionId: "2",
          params: [8, "bad"],
        });
        assert.equal(forged.kind, "error");
        assert.equal(forged.error.code, "ERR_FSQLITE_TRANSACTION_OWNERSHIP");
        // An owner-tagged protocol error fails this child; its rollback is explicit.
        await assert.rejects(child.query("SELECT 1"), hasCode("ERR_FSQLITE_TRANSACTION_ABORTED"));
      })
      .catch((error) => assert.ok(hasCode("ERR_FSQLITE_TRANSACTION_ABORTED")(error)));
    assert.equal(prepared.transactionId, "1");
    await stmt.execute([1, "parent"]);
  });
  const stale = await f.host.handle({
    kind: "execute",
    requestId: 1000,
    transactionId: "1",
    sql: "INSERT INTO items VALUES (2,'stale')",
  });
  assert.equal(stale.kind, "error");
  assert.equal(stale.error.code, "ERR_FSQLITE_TRANSACTION_CLOSED");
  assert.deepEqual(stored(f), [1]);
  await f.db.close();
});

test("deferred commit rejection rolls back schema and data before returning control", async () => {
  const f = await fixture();
  await f.db.executeBatch("PRAGMA foreign_keys=ON; CREATE TABLE parents(id PRIMARY KEY);");
  await assert.rejects(
    f.db.transaction(async (tx) => {
      await tx.executeBatch(`CREATE TABLE children(id REFERENCES parents(id) DEFERRABLE INITIALLY DEFERRED);
      INSERT INTO children VALUES(99); INSERT INTO items VALUES(1,'rollback');`);
    }),
  );
  assert.deepEqual(stored(f), []);
  assert.equal(
    (await f.db.query("SELECT count(*) AS n FROM sqlite_master WHERE name='children'")).rows[0].n,
    0,
  );
  await f.db.close();
});
