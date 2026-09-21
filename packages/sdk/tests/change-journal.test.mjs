// Production SDK/worker with file-backed Node SQLite, not FrankenSQLite WASM.

import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import { sqliteSnapshotWorker } from "../../worker/tests/helpers/snapshot-sqlite-core.mjs";
import { captureTables, TableChangeJournal } from "../src/change-journal.ts";
import { FrankenDB } from "../src/database.ts";
import { deferred, observe } from "./helpers/controlled-worker.ts";

const options = { timeout: 10000 };
async function fixture(t, hooks = {}) {
  const f = sqliteSnapshotWorker(hooks);
  const db = await FrankenDB.open({ worker: f.worker });
  t.after(() => db.close().catch(() => {}));
  await db.executeBatch(
    "CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT UNIQUE); CREATE TABLE audit(id); CREATE TABLE other(id);",
  );
  const journal = new TableChangeJournal();
  await journal.configure(db, ["items", "audit"]);
  return { ...f, db, journal };
}
const code = (name) => (error) =>
  error?.code === name || error?.cause?.code === name || error?.errors?.some(code(name));

test(
  "journal records actual direct/trigger changes once per committed table, not reads/no-op writes",
  options,
  async (t) => {
    const { db, journal } = await fixture(t);
    await db.execute(
      "CREATE TRIGGER track AFTER INSERT ON items BEGIN INSERT INTO audit VALUES(new.id); END;",
    );
    const result = await journal.run(db, async (tx) => {
      await tx.executeMany("INSERT INTO items VALUES(?,?)", [
        [1, "a"],
        [2, "b"],
        [3, "c"],
      ]);
      await tx.execute("UPDATE items SET value='x' WHERE id=1");
      await tx.execute("DELETE FROM items WHERE id=2");
      return 42;
    });
    assert.equal(result.value, 42);
    assert.deepEqual(result.tables, ["items", "audit"]);
    assert.ok(Object.isFrozen(result.tables));
    assert.deepEqual((await journal.run(db, (tx) => tx.query("SELECT * FROM items"))).tables, []);
    assert.deepEqual(
      (await journal.run(db, (tx) => tx.execute("DELETE FROM items WHERE id=100"))).tables,
      [],
    );
    assert.deepEqual(
      (await journal.run(db, (tx) => tx.execute("INSERT INTO other VALUES(1)"))).tables,
      [],
    );
  },
);

test(
  "journal rolls back child dirty bits and released children with a failing outer callback",
  options,
  async (t) => {
    const { db, journal } = await fixture(t);
    const result = await journal.run(db, async (tx) => {
      await tx.execute("INSERT INTO items VALUES(1,'keep')");
      await assert.rejects(
        tx.transaction(async (child) => {
          await child.execute("INSERT INTO audit VALUES(1)");
          throw Error("discard");
        }),
        /discard/,
      );
    });
    assert.deepEqual(result.tables, ["items"]);
    await assert.rejects(
      journal.run(db, async (tx) => {
        await tx.transaction((child) => child.execute("INSERT INTO audit VALUES(2)"));
        throw Error("outer");
      }),
      /outer/,
    );
    assert.deepEqual((await journal.run(db, (tx) => tx.query("SELECT * FROM audit"))).tables, []);
    assert.deepEqual((await db.query("SELECT * FROM audit")).rowArrays, []);
  },
);

test(
  "journal gives no result before COMMIT, and deferred commit failure leaves no pending event",
  options,
  async (t) => {
    let armed = false;
    const entered = deferred(),
      release = deferred();
    const f = await fixture(t, {
      beforeBatch: async (sql) => {
        if (armed && sql === "COMMIT") {
          entered.resolve();
          await release.promise;
        }
      },
    });
    armed = true;
    const run = observe(
      f.journal.run(f.db, (tx) => tx.execute("INSERT INTO items VALUES(1,'committed')")),
    );
    await entered.promise;
    assert.equal(run.outcome.status, "pending");
    const other = new DatabaseSync(f.db.path);
    t.after(() => other.close());
    assert.equal(other.prepare("SELECT count(*) AS n FROM items").get().n, 0);
    release.resolve();
    await run.settled;
    armed = false;
    assert.equal(run.outcome.status, "fulfilled");
    assert.deepEqual(run.outcome.value.tables, ["items"]);
    await f.db.execute("PRAGMA foreign_keys=ON");
    await f.db.execute("CREATE TABLE child(id REFERENCES items(id) DEFERRABLE INITIALLY DEFERRED)");
    await f.journal.configure(f.db, ["items", "child"]);
    await assert.rejects(f.journal.run(f.db, (tx) => tx.execute("INSERT INTO child VALUES(99)")));
    assert.deepEqual(
      (await f.journal.run(f.db, (tx) => tx.query("SELECT * FROM child"))).tables,
      [],
    );
  },
);

test(
  "journal expires user handles before asynchronous metadata/collection postlude",
  options,
  async (t) => {
    let armed = false,
      reads = 0;
    const entered = deferred(),
      release = deferred();
    const f = await fixture(t);
    const core = f.handles[0],
      query = core.queryWithParams.bind(core);
    core.queryWithParams = async (sql, params) => {
      if (armed && sql.startsWith("SELECT name, sql FROM main.sqlite_master") && ++reads === 2) {
        entered.resolve();
        await release.promise;
      }
      return query(sql, params);
    };
    let escaped;
    armed = true;
    const job = f.journal.run(f.db, async (tx) => {
      escaped = tx;
      await tx.execute("INSERT INTO items VALUES(1,'safe')");
    });
    await entered.promise;
    await assert.rejects(
      escaped.execute("DELETE FROM items"),
      code("ERR_FSQLITE_TRANSACTION_CLOSED"),
    );
    release.resolve();
    await job;
    assert.deepEqual((await f.db.query("SELECT id FROM items")).rowArrays, [[1]]);
  },
);

test(
  "journal rejects and rolls back watched-schema changes but permits unrelated DDL",
  options,
  async (t) => {
    const { db, journal } = await fixture(t);
    for (const sql of [
      "DROP TABLE items",
      "ALTER TABLE items RENAME TO lost",
      "ALTER TABLE items ADD COLUMN extra",
    ]) {
      await assert.rejects(
        journal.run(db, (tx) => tx.execute(sql)),
        code("ERR_FSQLITE_SUBSCRIPTION_SCHEMA"),
      );
    }
    assert.deepEqual(
      (await journal.run(db, (tx) => tx.execute("CREATE INDEX idx ON items(value)"))).tables,
      [],
    );
    assert.deepEqual(
      (await journal.run(db, (tx) => tx.execute("INSERT INTO items VALUES(1,'ok')"))).tables,
      ["items"],
    );
    await journal.configure(db, []);
    await journal.run(db, (tx) => tx.execute("DROP TABLE items"));
  },
);

test(
  "journal safely quotes identifiers and handles WITHOUT ROWID and outer conflict policies",
  options,
  async (t) => {
    const { db, journal } = await fixture(t);
    await db.execute('CREATE TABLE "a""; DROP TABLE items;--"(id TEXT PRIMARY KEY) WITHOUT ROWID');
    await journal.configure(db, ['a"; DROP TABLE items;--']);
    const result = await journal.run(db, async (tx) => {
      await tx.execute('INSERT OR FAIL INTO "a""; DROP TABLE items;--" VALUES(\'one\')');
      await tx.execute('INSERT OR REPLACE INTO "a""; DROP TABLE items;--" VALUES(\'two\')');
      await tx.execute('INSERT OR REPLACE INTO "a""; DROP TABLE items;--" VALUES(\'one\')');
    });
    assert.deepEqual(result.tables, ['a"; DROP TABLE items;--']);
    assert.equal((await db.query("SELECT count(*) AS n FROM items")).rows[0].n, 0);
  },
);

test(
  "journal tracks cascade effects and excludes child table writes that are rolled back",
  options,
  async (t) => {
    const { db, journal } = await fixture(t);
    await db.execute("PRAGMA foreign_keys=ON");
    await db.execute(
      "CREATE TABLE children(id INTEGER PRIMARY KEY, parent REFERENCES items(id) ON DELETE CASCADE)",
    );
    await journal.configure(db, ["items", "children"]);
    await journal.run(db, async (tx) => {
      await tx.execute("INSERT INTO items VALUES(1,'p')");
      await tx.execute("INSERT INTO children VALUES(1,1)");
    });
    assert.deepEqual((await journal.run(db, (tx) => tx.execute("DELETE FROM items"))).tables, [
      "items",
      "children",
    ]);
  },
);

test(
  "journal configuration failure is atomic and removal leaves exports free of instrumentation",
  options,
  async (t) => {
    const { db, journal } = await fixture(t);
    await assert.rejects(
      journal.configure(db, ["other", "does not exist"]),
      code("ERR_FSQLITE_SUBSCRIPTION_INPUT"),
    );
    assert.deepEqual(journal.tables, ["items", "audit"]);
    assert.deepEqual(
      (await journal.run(db, (tx) => tx.execute("INSERT INTO items VALUES(1,'ok')"))).tables,
      ["items"],
    );
    const file = new DatabaseSync(db.path);
    t.after(() => file.close());
    assert.deepEqual(
      file.prepare("SELECT name FROM sqlite_master WHERE name LIKE '__fsqlite_watch_%'").all(),
      [],
    );
    assert.deepEqual(file.prepare("PRAGMA integrity_check").all().map(Object.values), [["ok"]]);
    await journal.configure(db, []);
    assert.equal(
      (await db.query("SELECT count(*) AS n FROM temp.sqlite_master WHERE type='trigger'")).rows[0]
        .n,
      0,
    );
  },
);

test(
  "journal rejects views and virtual tables without changing the previous subscription",
  options,
  async (t) => {
    const { db, journal } = await fixture(t);
    await db.execute("CREATE VIEW v AS SELECT * FROM items");
    await db.execute("CREATE VIRTUAL TABLE f USING fts5(body)");
    for (const table of ["v", "f"])
      await assert.rejects(journal.configure(db, [table]), code("ERR_FSQLITE_SUBSCRIPTION_INPUT"));
    assert.deepEqual(journal.tables, ["items", "audit"]);
  },
);

test(
  "journal catches trigger removal within a callback and rolls back earlier successful writes",
  options,
  async (t) => {
    const { db, journal } = await fixture(t);
    const trigger = (
      await db.query("SELECT name FROM temp.sqlite_master WHERE type='trigger' LIMIT 1")
    ).rows[0].name;
    await assert.rejects(
      journal.run(db, async (tx) => {
        await tx.execute("INSERT INTO items VALUES(1,'lost')");
        await tx.execute(`DROP TRIGGER temp."${trigger}"`);
      }),
      code("ERR_FSQLITE_SUBSCRIPTION_SCHEMA"),
    );
    assert.deepEqual((await db.query("SELECT * FROM items")).rowArrays, []);
    assert.deepEqual(
      (await journal.run(db, (tx) => tx.execute("INSERT INTO items VALUES(2,'keep')"))).tables,
      ["items"],
    );
  },
);

test("table capture is bounded, owns its array, folds only ASCII and never uses a custom iterator", () => {
  const names = ["Items", "items", "İtems"];
  names[Symbol.iterator] = () => {
    throw Error("iterator");
  };
  const result = captureTables(names);
  names[0] = "changed";
  assert.deepEqual(result, ["items", "İtems"]);
  for (const value of [
    [],
    [""],
    ["a\0b"],
    ["sqlite_master"],
    ["__fsqlite_watch_x"],
    new Array(65),
    null,
  ])
    assert.throws(() => captureTables(value), code("ERR_FSQLITE_SUBSCRIPTION_INPUT"));
});

test(
  "journal refuses competing TEMP triggers that could suppress change tracking with RAISE IGNORE",
  options,
  async (t) => {
    const f = sqliteSnapshotWorker(),
      db = await FrankenDB.open({ worker: f.worker });
    t.after(() => db.close());
    await db.execute("CREATE TABLE items(id)");
    await db.execute(
      "CREATE TEMP TRIGGER skip AFTER INSERT ON main.items BEGIN SELECT RAISE(IGNORE); END;",
    );
    const journal = new TableChangeJournal();
    await assert.rejects(journal.configure(db, ["items"]), code("ERR_FSQLITE_SUBSCRIPTION_SCHEMA"));
    assert.deepEqual(journal.tables, []);
    await db.execute("DROP TRIGGER temp.skip");
    await journal.configure(db, ["items"]);
    await assert.rejects(
      journal.run(db, (tx) =>
        tx.execute(
          "CREATE TEMP TRIGGER skip AFTER INSERT ON main.items BEGIN SELECT RAISE(IGNORE); END;",
        ),
      ),
      code("ERR_FSQLITE_SUBSCRIPTION_SCHEMA"),
    );
    assert.deepEqual(
      (await journal.run(db, (tx) => tx.execute("INSERT INTO items VALUES(1)"))).tables,
      ["items"],
    );
  },
);
