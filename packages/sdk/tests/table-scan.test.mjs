// Production SDK/worker, SQLite references. Not a FrankenSQLite WASM certificate.

import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import { sqliteBindingFixture } from "../../worker/tests/helpers/bindings-core.mjs";
import { sqliteSnapshotWorker } from "../../worker/tests/helpers/snapshot-sqlite-core.mjs";
import { FrankenDB } from "../src/database.ts";
import { FrankenDBQueue, scanTable } from "../src/index.ts";
import { captureTableScan, createTablePageReader } from "../src/table-scan.ts";
import { deferred, drain, observe } from "./helpers/controlled-worker.ts";

const limits = { timeout: 20000 };
const hasCode = (code) => (error) =>
  error?.code === code || error?.cause?.code === code || error?.errors?.some(hasCode(code));
async function fixture(t) {
  const f = sqliteSnapshotWorker();
  const db = await FrankenDB.open({ worker: f.worker });
  t.after(() => db.close().catch(() => {}));
  return { ...f, db };
}
async function all(db, table, options = {}) {
  const settings = captureTableScan(table, options);
  return db.transaction(async (tx) => {
    const reader = await createTablePageReader(tx, settings),
      rows = [],
      sizes = [];
    while (!reader.exhausted) {
      const page = await reader.read();
      sizes.push(page.length);
      rows.push(...page);
    }
    const queries = reader.queries;
    assert.deepEqual(await reader.read(), []);
    assert.equal(reader.queries, queries);
    return { rows, sizes, queries, columns: reader.columns };
  });
}
const dataQueries = (f) =>
  f.worker.requests.filter((r) => r.kind === "query" && r.sql.startsWith("SELECT s."));

test(
  "scan pages: 10001 rows in bounded rowid pages, generated columns and exact order without OFFSET",
  limits,
  async (t) => {
    const f = await fixture(t);
    await f.db.executeBatch(
      "CREATE TABLE items(id INTEGER PRIMARY KEY,value TEXT,g TEXT GENERATED ALWAYS AS (value || '!') VIRTUAL);" +
        "WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<10001) INSERT INTO items(id,value) SELECT x,printf('row-%d',x) FROM n;",
    );
    const result = await all(f.db, "items", { batchSize: 127 });
    assert.equal(result.rows.length, 10001);
    assert.equal(result.queries, 79);
    assert.deepEqual(result.columns, ["id", "value", "g"]);
    for (let i = 0; i < result.rows.length; i++)
      assert.deepEqual(result.rows[i], { id: i + 1, value: `row-${i + 1}`, g: `row-${i + 1}!` });
    assert.ok(result.sizes.every((n) => n <= 127));
    const queries = dataQueries(f);
    assert.equal(queries.length, 79);
    for (const q of queries) {
      assert.ok(q.sql.endsWith("LIMIT ?"));
      assert.ok(!q.sql.includes("OFFSET"));
      assert.equal(q.params.at(-1), 127);
    }
    assert.ok(queries.slice(1).every((q) => q.sql.includes(' WHERE s."_rowid_" > ?')));
    const native = new DatabaseSync(f.db.path);
    t.after(() => native.close());
    for (const q of [queries[1], queries.at(-1)]) {
      const plan = native
        .prepare("EXPLAIN QUERY PLAN " + q.sql)
        .all(...q.params)
        .map((row) => row.detail)
        .join(";");
      assert.match(plan, /SEARCH s USING INTEGER PRIMARY KEY/);
      assert.doesNotMatch(plan, /TEMP B-TREE/);
    }
    assert.deepEqual(native.prepare("PRAGMA integrity_check").all().map(Object.values), [["ok"]]);
  },
);

test(
  "scan pages: empty, single-row, exact-sized final page and reversed rowid ranges",
  limits,
  async (t) => {
    const { db } = await fixture(t);
    await db.execute("CREATE TABLE items(id INTEGER PRIMARY KEY)");
    assert.deepEqual((await all(db, "items", { batchSize: 2 })).sizes, [0]);
    await db.execute("INSERT INTO items VALUES(-2)");
    assert.deepEqual((await all(db, "items", { batchSize: 2 })).sizes, [1]);
    await db.execute("INSERT INTO items VALUES(0),(7),(9)");
    const result = await all(db, "items", { batchSize: 2, reverse: true });
    assert.deepEqual(result.rows, [{ id: 9 }, { id: 7 }, { id: 0 }, { id: -2 }]);
    assert.deepEqual(result.sizes, [2, 2, 0]);
  },
);

test(
  "scan pages: aliases cannot overwrite user data and projections are canonical validated identifiers",
  limits,
  async (t) => {
    const { db } = await fixture(t);
    await db.execute(
      'CREATE TABLE items(id INTEGER PRIMARY KEY,"__fsqlite_scan_key_0", "__proto__", "constructor",data BLOB)',
    );
    await db.execute("INSERT INTO items VALUES(1,'visible','ordinary',NULL,x'00ff')");
    const result = await all(db, "ITEMS", {
      columns: ["DATA", "__PROTO__", "__fsqlite_scan_key_0", "constructor"],
      batchSize: 1,
    });
    assert.deepEqual(result.columns, ["data", "__proto__", "__fsqlite_scan_key_0", "constructor"]);
    assert.ok(Object.hasOwn(result.rows[0], "__proto__"));
    assert.equal(result.rows[0].__proto__, "ordinary");
    assert.deepEqual(result.rows[0].data, Uint8Array.of(0, 255));
    assert.equal(result.rows[0].__fsqlite_scan_key_0, "visible");
    await assert.rejects(
      all(db, "items", { columns: ["missing"] }),
      hasCode("ERR_FSQLITE_SCAN_INPUT"),
    );
  },
);

test(
  "scan pages: signed 64-bit rowids including both extremes are bound without arithmetic or rounding",
  limits,
  async (t) => {
    const f = sqliteBindingFixture();
    t.after(() => f.shutdown());
    const db = await FrankenDB.open({ worker: f.worker });
    t.after(() => db.close());
    await db.execute("CREATE TABLE items(id INTEGER PRIMARY KEY,value)");
    const keys = [-(2n ** 63n), -9007199254740993n, -1n, 0n, 9007199254740993n, 2n ** 63n - 1n];
    for (const key of keys) await db.execute("INSERT INTO items VALUES(?,?)", [key, String(key)]);
    assert.deepEqual(
      (await all(db, "items", { batchSize: 1 })).rows.map((r) => BigInt(r.id)),
      keys,
    );
    assert.deepEqual(
      (await all(db, "items", { batchSize: 2, reverse: true })).rows.map((r) => BigInt(r.id)),
      keys.toReversed(),
    );
  },
);

test(
  "scan pages: declared rowid names select an unshadowed hidden alias and all-shadowed tables reject",
  limits,
  async (t) => {
    const { db } = await fixture(t);
    await db.execute('CREATE TABLE items("_ROWID_" TEXT,"rowid" TEXT,value)');
    await db.execute("INSERT INTO items VALUES('z','z',1),('a','a',2)");
    assert.deepEqual((await all(db, "items", { columns: ["value"], batchSize: 1 })).rows, [
      { value: 1 },
      { value: 2 },
    ]);
    await db.execute("CREATE TABLE blocked(rowid, _rowid_, oid, value)");
    await assert.rejects(all(db, "blocked"), hasCode("ERR_FSQLITE_SCAN_SCHEMA"));
  },
);

test(
  "scan pages: INTEGER PRIMARY KEY DESC uses the real rowid, not an incorrectly inferred key alias",
  limits,
  async (t) => {
    const { db } = await fixture(t);
    await db.execute("CREATE TABLE items(id INTEGER PRIMARY KEY DESC,value)");
    await db.execute("INSERT INTO items VALUES(7,'first'),(2,'second'),(NULL,'third')");
    assert.deepEqual(
      (await all(db, "items", { batchSize: 1 })).rows.map((row) => row.value),
      ["first", "second", "third"],
    );
  },
);

test(
  "scan pages: WITHOUT ROWID mixed-direction composite keys match independent SQL, seeking every continuation",
  limits,
  async (t) => {
    const f = await fixture(t);
    await f.db.execute(
      "CREATE TABLE items(a TEXT COLLATE NOCASE,b INTEGER,c TEXT COLLATE RTRIM,value,PRIMARY KEY(a DESC,b ASC,c DESC)) WITHOUT ROWID",
    );
    await f.db.executeBatch(
      "WITH RECURSIVE n(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM n WHERE x<1000) " +
        "INSERT INTO items SELECT CASE x%4 WHEN 0 THEN 'A' WHEN 1 THEN 'b' WHEN 2 THEN 'C' ELSE 'd' END,x%17,printf('%06d ',x),x FROM n;",
    );
    const native = new DatabaseSync(f.db.path);
    t.after(() => native.close());
    const expected = native
      .prepare(
        "SELECT a,b,c,value FROM items ORDER BY a COLLATE NOCASE DESC,b ASC,c COLLATE RTRIM DESC",
      )
      .all()
      .map((r) => ({ ...r }));
    const result = await all(f.db, "items", { batchSize: 13 });
    assert.deepEqual(result.rows, expected);
    assert.ok(result.queries <= Math.ceil(1001 / 13) * 3 + 3);
    for (const q of dataQueries(f).filter((q) => q.sql.includes(" WHERE "))) {
      const plan = native
        .prepare("EXPLAIN QUERY PLAN " + q.sql)
        .all(...q.params)
        .map((row) => row.detail)
        .join(";");
      assert.match(plan, /SEARCH s USING PRIMARY KEY/);
      assert.doesNotMatch(plan, /TEMP B-TREE/);
      assert.ok(!q.sql.includes(" OR ") && !q.sql.includes("OFFSET"));
      assert.ok(q.params.at(-1) <= 13);
    }
    assert.deepEqual(
      (await all(f.db, "items", { batchSize: 16, reverse: true })).rows,
      expected.toReversed(),
    );
  },
);

test(
  "scan pages: composite blob and numeric keys retain SQLite storage-class ordering",
  limits,
  async (t) => {
    const { db } = await fixture(t);
    await db.execute("CREATE TABLE items(a,b,c,value,PRIMARY KEY(a,b DESC,c)) WITHOUT ROWID");
    const values = [
      [2, 1, "a", "integer"],
      [2.5, 1, "a", "real"],
      ["2", 1, "a", "text"],
      ["a", 0, "a", "one"],
      ["a", 2, "a", "two"],
      [Uint8Array.of(0), 1, "a", "blob0"],
      [Uint8Array.of(255), 1, "a", "blobff"],
    ];
    for (const row of values) await db.execute("INSERT INTO items VALUES(?,?,?,?)", row);
    const expected = (await db.query("SELECT a,b,c,value FROM items ORDER BY a,b DESC,c")).rows;
    assert.deepEqual((await all(db, "items", { batchSize: 1 })).rows, expected);
  },
);

test(
  "scan pages: consumer mutation of projected primary-key blobs never changes the continuation key",
  limits,
  async (t) => {
    const { db } = await fixture(t);
    await db.execute("CREATE TABLE items(k BLOB PRIMARY KEY,value) WITHOUT ROWID");
    await db.execute("INSERT INTO items VALUES(x'01','a'),(x'02','b'),(x'03','c')");
    await db.transaction(async (tx) => {
      const reader = await createTablePageReader(tx, captureTableScan("items", { batchSize: 1 }));
      const first = await reader.read();
      assert.equal(first[0].value, "a");
      first[0].k.fill(255);
      assert.equal((await reader.read())[0].value, "b");
      assert.equal((await reader.read())[0].value, "c");
    });
  },
);

test(
  "scan pages: quoted table and column names are never treated as SQL fragments",
  limits,
  async (t) => {
    const { db } = await fixture(t);
    const table = "a'\"; DROP TABLE other;--";
    await db.execute("CREATE TABLE other(id)");
    await db.execute(
      `CREATE TABLE "${table.replaceAll('"', '""')}"("v"";--",id INTEGER PRIMARY KEY)`,
    );
    await db.execute(`INSERT INTO "${table.replaceAll('"', '""')}" VALUES(7,1)`);
    assert.deepEqual((await all(db, table, { columns: ['v";--'] })).rows, [{ 'v";--': 7 }]);
    assert.deepEqual((await db.query("SELECT * FROM other")).rowArrays, []);
  },
);

test(
  "scan pages: views, virtual/shadow tables and absent tables reject before a data query",
  limits,
  async (t) => {
    const f = await fixture(t);
    await f.db.executeBatch(
      "CREATE TABLE items(id);CREATE VIEW v AS SELECT * FROM items;CREATE VIRTUAL TABLE f USING fts5(body);",
    );
    for (const table of ["v", "f", "f_data", "missing"])
      await assert.rejects(all(f.db, table), hasCode("ERR_FSQLITE_SCAN_SCHEMA"));
    assert.equal(dataQueries(f).length, 0);
  },
);

test("scan pages: capture bounds and snapshots columns without invoking an array iterator", () => {
  const columns = ["Value"];
  columns[Symbol.iterator] = () => {
    throw Error("iterator");
  };
  const captured = captureTableScan("items", { columns, batchSize: 1 });
  columns[0] = "changed";
  assert.deepEqual(captured.columns, ["Value"]);
  assert.ok(Object.isFrozen(captured.columns));
  for (const options of [
    { batchSize: 0 },
    { batchSize: 4097 },
    { batchSize: 1.5 },
    { reverse: "yes" },
    { columns: [] },
    { columns: ["id", "ID"] },
    { columns: ["x\0"] },
    { columns: new Array(1025) },
  ])
    assert.throws(() => captureTableScan("items", options), hasCode("ERR_FSQLITE_SCAN_INPUT"));
  for (const table of ["", "x\0", "x".repeat(1025), "sqlite_master", null])
    assert.throws(() => captureTableScan(table), hasCode("ERR_FSQLITE_SCAN_INPUT"));
});

test(
  "scan pages: malformed/unsupported metadata fails closed rather than falling back to unbounded SELECT",
  limits,
  async (t) => {
    const f = await fixture(t);
    await f.db.execute("CREATE TABLE items(id)");
    const core = f.handles[0],
      query = core.query.bind(core);
    core.query = async (sql) =>
      sql.startsWith("PRAGMA main.table_list")
        ? { columns: [], columnCount: 0, columnTypes: [], rows: [], rowArrays: [], changes: 0 }
        : query(sql);
    await assert.rejects(all(f.db, "items"), hasCode("ERR_FSQLITE_SCAN_SCHEMA"));
    assert.equal(dataQueries(f).length, 0);
  },
);

test(
  "scan pages: oversized and non-progressing core results reject without transparent replay",
  limits,
  async (t) => {
    const f = await fixture(t);
    await f.db.executeBatch(
      "CREATE TABLE items(id INTEGER PRIMARY KEY);INSERT INTO items VALUES(1),(2);",
    );
    const core = f.handles[0],
      query = core.queryWithParams.bind(core);
    let first;
    core.queryWithParams = async (sql, params) => {
      const result = await query(sql, params);
      if (sql.startsWith("SELECT s.")) {
        if (!first) first = result;
        else return first;
      }
      return result;
    };
    await assert.rejects(all(f.db, "items", { batchSize: 1 }), hasCode("ERR_FSQLITE_SCAN_RESULT"));
    assert.equal(dataQueries(f).length, 2);
    core.queryWithParams = async (sql, params) =>
      query(sql, sql.startsWith("SELECT s.") ? [...params.slice(0, -1), 2] : params);
    await assert.rejects(all(f.db, "items", { batchSize: 1 }), hasCode("ERR_FSQLITE_SCAN_RESULT"));
  },
);

async function queued(t, hooks = {}) {
  const f = sqliteSnapshotWorker(hooks),
    q = await FrankenDBQueue.open({ worker: f.worker });
  t.after(() => q.close().catch(() => {}));
  await q.transaction((tx) =>
    tx.executeBatch(
      "CREATE TABLE items(id INTEGER PRIMARY KEY,value TEXT);" +
        "WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<10) INSERT INTO items SELECT x,printf('row-%d',x) FROM n;",
    ),
  );
  return { ...f, q };
}
async function collect(iterator) {
  const rows = [];
  for await (const row of iterator) rows.push(row);
  return rows;
}

test(
  "scan iterator: lazy start, one bounded page, no prefetch, releasing consumed rows and exact completion",
  limits,
  async (t) => {
    const f = await queued(t),
      scan = scanTable(f.q, "items", { batchSize: 3 });
    assert.equal(dataQueries(f).length, 0);
    assert.equal(f.q.stats.activeJobs, 0);
    assert.equal((await scan.next()).value.id, 1);
    assert.equal(scan.stats.bufferedRows, 2);
    const completion = observe(scan.done);
    await drain();
    assert.equal(completion.outcome.status, "pending");
    assert.equal(dataQueries(f).length, 1);
    await scan.next();
    assert.equal(scan.stats.bufferedRows, 1);
    await scan.next();
    await drain();
    assert.equal(dataQueries(f).length, 1);
    assert.equal(scan.stats.bufferedRows, 0);
    const rest = await collect(scan);
    await scan.done;
    assert.deepEqual(
      rest.map((row) => row.id),
      [4, 5, 6, 7, 8, 9, 10],
    );
    assert.ok(scan.closed);
    assert.deepEqual(scan.stats, {
      pagesRead: 4,
      pageQueries: 4,
      rowsRead: 10,
      rowsYielded: 10,
      bufferedRows: 0,
      maxBufferedRows: 3,
    });
    assert.ok(Object.isFrozen(scan.stats));
    assert.equal(f.q.stats.pendingJobs, 0);
  },
);

test(
  "scan iterator: rejects concurrent unresolved next without queuing hidden demand",
  limits,
  async (t) => {
    const f = await queued(t),
      core = f.handles[0],
      query = core.queryWithParams.bind(core),
      entered = deferred(),
      release = deferred();
    core.queryWithParams = async (sql, params) => {
      if (sql.startsWith("SELECT s.")) {
        entered.resolve();
        await release.promise;
      }
      return query(sql, params);
    };
    const scan = scanTable(f.q, "items"),
      first = scan.next();
    await entered.promise;
    await assert.rejects(scan.next(), hasCode("ERR_FSQLITE_SCAN_NEXT_PENDING"));
    release.resolve();
    assert.equal((await first).value.id, 1);
    await scan.return();
    assert.equal(dataQueries(f).length, 1);
    assert.equal(scan.stats.rowsYielded, 1);
  },
);

test(
  "scan iterator: queue successor waits for snapshot release and loop break joins rollback first",
  limits,
  async (t) => {
    const f = await queued(t),
      scan = scanTable(f.q, "items", { batchSize: 2 });
    assert.equal((await scan.next()).value.id, 1);
    const write = observe(
      f.q.transaction((tx) => tx.execute("INSERT INTO items VALUES(11,'after')")),
    );
    await drain();
    assert.equal(write.outcome.status, "pending");
    for await (const row of scan) {
      assert.equal(row.id, 2);
      break;
    }
    await scan.done;
    await write.settled;
    assert.equal(write.outcome.status, "fulfilled");
    const rollback = f.events.lastIndexOf("ROLLBACK"),
      insert = f.events.indexOf("INSERT INTO items VALUES(11,'after')");
    assert.ok(rollback >= 0 && insert > rollback);
    assert.equal(scan.stats.bufferedRows, 0);
  },
);

test(
  "scan iterator: one snapshot remains stable while another real WAL connection updates, inserts and deletes",
  limits,
  async (t) => {
    const f = await queued(t);
    await f.handles[0].query("PRAGMA journal_mode=WAL");
    const other = new DatabaseSync(f.q.path);
    t.after(() => other.close());
    const scan = scanTable(f.q, "items", { batchSize: 2 }),
      first = (await scan.next()).value;
    other.exec(
      "BEGIN;UPDATE items SET value='new' WHERE id=5;DELETE FROM items WHERE id=6;INSERT INTO items VALUES(11,'new');COMMIT;",
    );
    const rows = [first, ...(await collect(scan))];
    await scan.done;
    assert.deepEqual(
      rows,
      Array.from({ length: 10 }, (_, i) => ({ id: i + 1, value: `row-${i + 1}` })),
    );
    const current = await f.q.transaction((tx) =>
      tx.query("SELECT id,value FROM items ORDER BY id"),
    );
    assert.equal(current.rows.find((row) => row.id === 5).value, "new");
    assert.equal(
      current.rows.some((row) => row.id === 6),
      false,
    );
    assert.equal(current.rows.at(-1).id, 11);
  },
);

test(
  "scan iterator: early return and pre-abort before demand perform no SQL or admission",
  limits,
  async (t) => {
    const f = await queued(t),
      before = f.q.stats.acceptedJobs;
    const scan = scanTable(f.q, "items");
    await scan.return();
    assert.equal((await scan.next()).done, true);
    const controller = new AbortController();
    controller.abort(Error("stop"));
    const cancelled = scanTable(f.q, "items", { signal: controller.signal });
    await assert.rejects(cancelled.done, hasCode("ERR_FSQLITE_SCAN_CANCELLED"));
    await assert.rejects(cancelled.next(), hasCode("ERR_FSQLITE_SCAN_CANCELLED"));
    assert.equal(f.q.stats.acceptedJobs, before);
    assert.equal(dataQueries(f).length, 0);
  },
);

test(
  "scan iterator: queue close wakes an idle snapshot owner and completes without requiring another next",
  limits,
  async (t) => {
    const f = await queued(t),
      scan = scanTable(f.q, "items", { batchSize: 3 });
    await scan.next();
    await f.q.close();
    await assert.rejects(scan.done, hasCode("ERR_FSQLITE_SCAN_CANCELLED"));
    assert.ok(scan.closed);
    await assert.rejects(scan.next(), hasCode("ERR_FSQLITE_SCAN_CANCELLED"));
    assert.equal(f.q.stats.state, "closed");
    assert.equal(f.worker.terminateCount, 1);
  },
);

test(
  "scan iterator: starting against a closed queue is an error, never successful empty data",
  limits,
  async (t) => {
    const f = await queued(t);
    await f.q.close();
    const scan = scanTable(f.q, "items");
    await assert.rejects(scan.next(), hasCode("ERR_FSQLITE_JOB_QUEUE_CLOSED"));
    await assert.rejects(scan.done, hasCode("ERR_FSQLITE_JOB_QUEUE_CLOSED"));
  },
);

test(
  "scan iterator: waiting cancellation and queue admission limits reject or remove work before BEGIN",
  limits,
  async (t) => {
    const f = await queued(t),
      entered = deferred(),
      release = deferred();
    const hold = f.q.transaction(async () => {
      entered.resolve();
      await release.promise;
    });
    await entered.promise;
    try {
      const controller = new AbortController(),
        scan = scanTable(f.q, "items", { signal: controller.signal });
      const next = scan.next();
      controller.abort();
      await assert.rejects(next, hasCode("ERR_FSQLITE_SCAN_CANCELLED"));
      await assert.rejects(scan.done, hasCode("ERR_FSQLITE_SCAN_CANCELLED"));
      assert.equal(dataQueries(f).length, 0);
      const timed = scanTable(f.q, "items", { waitTimeoutMs: 1 });
      await assert.rejects(timed.next(), hasCode("ERR_FSQLITE_JOB_WAIT_TIMEOUT"));
      await assert.rejects(timed.done, hasCode("ERR_FSQLITE_JOB_WAIT_TIMEOUT"));
      assert.equal(dataQueries(f).length, 0);
    } finally {
      release.resolve();
      await hold;
    }
  },
);

test(
  "scan iterator: active cancellation joins actual SQL and rollback, then releases the queue slot",
  limits,
  async (t) => {
    const f = await queued(t),
      core = f.handles[0],
      query = core.queryWithParams.bind(core),
      entered = deferred(),
      release = deferred();
    core.queryWithParams = async (sql, params) => {
      if (sql.startsWith("SELECT s.")) {
        entered.resolve();
        await release.promise;
      }
      return query(sql, params);
    };
    const controller = new AbortController(),
      scan = scanTable(f.q, "items", { signal: controller.signal }),
      next = observe(scan.next());
    await entered.promise;
    controller.abort();
    const done = observe(scan.done);
    const write = observe(
      f.q.transaction((tx) => tx.execute("INSERT INTO items VALUES(11,'after')")),
    );
    await drain();
    assert.equal(next.outcome.status, "pending");
    assert.equal(done.outcome.status, "pending");
    assert.equal(write.outcome.status, "pending");
    release.resolve();
    await Promise.all([next.settled, done.settled, write.settled]);
    assert.equal(next.outcome.reason.code, "ERR_FSQLITE_SCAN_CANCELLED");
    assert.equal(done.outcome.status, "rejected");
    assert.equal(write.outcome.status, "fulfilled");
    assert.equal(scan.stats.rowsYielded, 0);
    assert.equal(dataQueries(f).length, 1);
  },
);

test(
  "scan iterator: a real SQL failure arriving with cancellation is preserved, not hidden as EOF",
  limits,
  async (t) => {
    const f = await queued(t),
      core = f.handles[0],
      query = core.queryWithParams.bind(core),
      entered = deferred(),
      release = deferred();
    core.queryWithParams = async (sql, params) => {
      if (sql.startsWith("SELECT s.")) {
        entered.resolve();
        await release.promise;
        throw Error("read fault");
      }
      return query(sql, params);
    };
    const scan = scanTable(f.q, "items"),
      next = observe(scan.next());
    await entered.promise;
    const returned = observe(scan.return());
    release.resolve();
    await Promise.all([next.settled, returned.settled]);
    assert.equal(next.outcome.status, "rejected");
    assert.equal(returned.outcome.status, "rejected");
    const contains = (error, text) =>
      String(error?.message).includes(text) ||
      containsSafe(error?.cause, text) ||
      error?.errors?.some((e) => containsSafe(e, text));
    const containsSafe = (error, text) => error !== undefined && contains(error, text);
    assert.ok(contains(next.outcome.reason, "read fault"));
  },
);

test(
  "scan iterator: consumer throw retains its exact cause and rollback failure makes the queue unusable",
  limits,
  async (t) => {
    let armed = false;
    const f = await queued(t, {
      beforeBatch: (sql) => {
        if (armed && sql === "ROLLBACK") throw Error("rollback fault");
      },
    });
    const scan = scanTable(f.q, "items", { batchSize: 1 });
    await scan.next();
    const reason = Error("consumer fault");
    armed = true;
    const hasReason = (error) =>
      error === reason || error?.cause === reason || error?.errors?.some(hasReason);
    await assert.rejects(scan.throw(reason), hasReason);
    await assert.rejects(scan.done, hasReason);
    await assert.rejects(f.q.transaction((tx) => tx.query("SELECT 1")));
    assert.ok(scan.closed);
  },
);

test(
  "scan iterator: idle timeout is a failure and releases a snapshot even with no outstanding next",
  limits,
  async (t) => {
    const f = await queued(t),
      scan = scanTable(f.q, "items", { batchSize: 3, idleTimeoutMs: 10 });
    await scan.next();
    await assert.rejects(scan.done, hasCode("ERR_FSQLITE_SCAN_IDLE_TIMEOUT"));
    await assert.rejects(scan.next(), hasCode("ERR_FSQLITE_SCAN_IDLE_TIMEOUT"));
    assert.equal(scan.stats.rowsYielded, 1);
    assert.equal(scan.stats.bufferedRows, 0);
    assert.equal(
      await f.q.transaction((tx) => tx.execute("INSERT INTO items VALUES(11,'after')")),
      1,
    );
  },
);

test(
  "scan iterator: delayed idle timers cannot let a late next silently resume an expired snapshot",
  limits,
  async (t) => {
    const f = await queued(t),
      scan = scanTable(f.q, "items", { batchSize: 3, idleTimeoutMs: 10 });
    await scan.next();
    await drain();
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 30);
    await assert.rejects(scan.next(), hasCode("ERR_FSQLITE_SCAN_IDLE_TIMEOUT"));
    await assert.rejects(scan.done, hasCode("ERR_FSQLITE_SCAN_IDLE_TIMEOUT"));
  },
);

test(
  "scan iterator: disabled idle timeout and per-row activity preserve demand-driven delivery",
  limits,
  async (t) => {
    const f = await queued(t),
      scan = scanTable(f.q, "items", { batchSize: 3, idleTimeoutMs: 0 });
    await scan.next();
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.equal(scan.closed, false);
    assert.equal(dataQueries(f).length, 1);
    await scan.return();
    assert.equal(scan.stats.rowsYielded, 1);
  },
);

test(
  "scan iterator: subscribed queue reads do not emit writes or create a notification backlog",
  limits,
  async (t) => {
    const f = await queued(t),
      changes = [];
    const sub = await f.q.subscribe(["items"], (change) => {
      changes.push(change);
    });
    const scan = scanTable(f.q, "items", { batchSize: 2 });
    assert.equal((await collect(scan)).length, 10);
    await scan.done;
    await drain();
    assert.deepEqual(changes, []);
    assert.equal(f.q.changeSequence, 0n);
    sub.unsubscribe();
    await sub.done;
  },
);

test(
  "scan iterator: projection capture and composite key mutation preserve complete results",
  limits,
  async (t) => {
    const f = await queued(t);
    await f.q.transaction((tx) =>
      tx.executeBatch(
        "CREATE TABLE composite(k BLOB,n,PRIMARY KEY(k,n DESC)) WITHOUT ROWID;INSERT INTO composite VALUES(x'01',1),(x'01',2),(x'02',1);",
      ),
    );
    const columns = ["k", "n"];
    const scan = scanTable(f.q, "composite", { batchSize: 1, columns });
    columns[0] = "missing";
    const first = (await scan.next()).value;
    assert.equal(first.n, 2);
    first.k.fill(255);
    const rest = await collect(scan);
    assert.deepEqual(rest, [
      { k: Uint8Array.of(1), n: 1 },
      { k: Uint8Array.of(2), n: 1 },
    ]);
  },
);

test(
  "scan iterator: complete 25001-row read retains at most one requested page",
  limits,
  async (t) => {
    const f = await queued(t);
    await f.q.transaction((tx) =>
      tx.executeBatch(
        "DELETE FROM items;WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<25001) INSERT INTO items SELECT x,'payload' FROM n;",
      ),
    );
    const scan = scanTable(f.q, "items", { batchSize: 113 });
    let expected = 1;
    for await (const row of scan) {
      assert.equal(row.id, expected++);
      assert.equal(row.value, "payload");
      assert.ok(scan.stats.bufferedRows <= 113);
    }
    await scan.done;
    assert.equal(expected, 25002);
    assert.equal(scan.stats.maxBufferedRows, 113);
    assert.equal(scan.stats.bufferedRows, 0);
    assert.equal(scan.stats.pageQueries, 222);
    assert.equal(f.q.stats.pendingJobs, 0);
  },
);

test(
  "scan iterator: empty tables and invalid projections settle done with the real outcome",
  limits,
  async (t) => {
    const f = await queued(t);
    await f.q.transaction((tx) => tx.execute("DELETE FROM items"));
    const empty = scanTable(f.q, "items");
    assert.deepEqual(await collect(empty), []);
    await empty.done;
    const invalid = scanTable(f.q, "items", { columns: ["missing"] });
    await assert.rejects(invalid.next(), hasCode("ERR_FSQLITE_SCAN_INPUT"));
    await assert.rejects(invalid.done, hasCode("ERR_FSQLITE_SCAN_INPUT"));
    const before = f.q.stats.acceptedJobs;
    for (const options of [
      { signal: {} },
      { idleTimeoutMs: -1 },
      { idleTimeoutMs: Infinity },
      { idleTimeoutMs: 0.5 },
      { waitTimeoutMs: -1 },
    ])
      assert.throws(() => scanTable(f.q, "items", options));
    assert.equal(f.q.stats.acceptedJobs, before);
  },
);

test(
  "scan iterator: late abort after complete delivery and COMMIT dispatch retains the real successful outcome",
  limits,
  async (t) => {
    let armed = false;
    const entered = deferred(),
      release = deferred();
    const f = await queued(t, {
      beforeBatch: async (sql) => {
        if (armed && sql === "COMMIT") {
          entered.resolve();
          await release.promise;
        }
      },
    });
    const controller = new AbortController(),
      scan = scanTable(f.q, "items", { batchSize: 20, signal: controller.signal });
    armed = true;
    const read = observe(collect(scan));
    await entered.promise;
    controller.abort(Error("late"));
    await drain();
    assert.equal(read.outcome.status, "pending");
    release.resolve();
    await read.settled;
    await scan.done;
    assert.equal(read.outcome.status, "fulfilled");
    assert.equal(read.outcome.value.length, 10);
    assert.ok(scan.closed);
  },
);

test(
  "scan iterator: final transaction failure rejects completion after a delivered prefix without replay",
  limits,
  async (t) => {
    let armed = false;
    const f = await queued(t, {
      beforeBatch: (sql) => {
        if (armed && sql === "COMMIT") throw Error("read commit failure");
      },
    });
    const scan = scanTable(f.q, "items", { batchSize: 20 });
    armed = true;
    let delivered = 0;
    await assert.rejects(async () => {
      for await (const row of scan) {
        assert.equal(row.id, ++delivered);
      }
    }, /read commit failure/);
    await assert.rejects(scan.done, /read commit failure/);
    assert.equal(delivered, 10);
    assert.equal(dataQueries(f).length, 1);
  },
);

test(
  "scan pages: primary-key collation overrides column collation and nullable metadata never enables null keys",
  limits,
  async (t) => {
    const { db } = await fixture(t);
    await db.execute(
      "CREATE TABLE items(a TEXT COLLATE BINARY,b,value,PRIMARY KEY(a COLLATE NOCASE DESC,b)) WITHOUT ROWID",
    );
    await db.execute("INSERT INTO items VALUES('a',1,1),('A',2,2),('B',1,3),('b',2,4)");
    const expected = (
      await db.query("SELECT a,b,value FROM items ORDER BY a COLLATE NOCASE DESC,b")
    ).rows;
    assert.deepEqual((await all(db, "items", { batchSize: 1 })).rows, expected);
  },
);

test(
  "scan pages: continuation keys larger than the retained-key budget fail without another page query",
  limits,
  async (t) => {
    const f = await fixture(t);
    await f.db.execute("CREATE TABLE items(k TEXT PRIMARY KEY) WITHOUT ROWID");
    await f.db.execute("INSERT INTO items VALUES(?)", ["x".repeat(524289)]);
    await assert.rejects(all(f.db, "items"), hasCode("ERR_FSQLITE_SCAN_SCHEMA"));
    assert.equal(dataQueries(f).length, 1);
  },
);

test(
  "scan iterator: full queue rejects before a scan snapshot starts and retained close observer is removed",
  limits,
  async (t) => {
    const f = sqliteSnapshotWorker(),
      q = await FrankenDBQueue.open({ worker: f.worker }, { maxPendingJobs: 1 });
    t.after(() => q.close());
    const entered = deferred(),
      release = deferred(),
      hold = q.transaction(async () => {
        entered.resolve();
        await release.promise;
      });
    await entered.promise;
    try {
      const scan = scanTable(q, "items");
      await assert.rejects(scan.next(), hasCode("ERR_FSQLITE_JOB_QUEUE_FULL"));
      await assert.rejects(scan.done, hasCode("ERR_FSQLITE_JOB_QUEUE_FULL"));
      assert.equal(dataQueries(f).length, 0);
      assert.equal(q.stats.pendingJobs, 1);
    } finally {
      release.resolve();
      await hold;
    }
  },
);
