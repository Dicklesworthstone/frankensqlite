import assert from "node:assert/strict";
import { constants, DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import { captureChangeset } from "../src/changeset-capture.ts";
import { decodeChangeset, invertChangeset } from "../src/changeset-codec.ts";

// Real SQLite is the SQL and binary-session oracle, not a mocked engine.
class SqlTarget {
  constructor(schema = "", seed = "") {
    this.db = new DatabaseSync(":memory:");
    this.db.exec("PRAGMA recursive_triggers=ON; PRAGMA foreign_keys=ON;" + schema + seed);
    this.depth = 0;
    this.serial = 0;
    this.statements = [];
  }
  async execute(sql, params = []) {
    this.statements.push(sql);
    return Number(this.db.prepare(sql).run(...params).changes);
  }
  async query(sql, params = []) {
    this.statements.push(sql);
    const stmt = this.db.prepare(sql);
    stmt.setReadBigInts(true);
    stmt.setReturnArrays(true);
    return { rowArrays: stmt.all(...params) };
  }
  async transaction(work) {
    const nested = this.depth++ > 0,
      savepoint = `test_${++this.serial}`;
    this.db.exec(nested ? `SAVEPOINT ${savepoint}` : "BEGIN");
    try {
      const value = await work(this);
      this.db.exec(nested ? `RELEASE ${savepoint}` : "COMMIT");
      return value;
    } catch (error) {
      this.db.exec(nested ? `ROLLBACK TO ${savepoint}; RELEASE ${savepoint}` : "ROLLBACK");
      throw error;
    } finally {
      this.depth--;
    }
  }
  close() {
    this.db.close();
  }
}
const schema = "CREATE TABLE t(id INTEGER PRIMARY KEY, value, extra);";
const seed = "INSERT INTO t VALUES (1,'old',NULL),(2,'delete',X'0102');";
function rows(target, table = "t") {
  const stmt = target.db.prepare(`SELECT * FROM "${table.replaceAll('"', '""')}" ORDER BY 1`);
  stmt.setReadBigInts(true);
  stmt.setReturnArrays(true);
  return stmt.all();
}
function noArtifacts(target) {
  assert.deepEqual(
    target.db
      .prepare("SELECT name FROM temp.sqlite_schema WHERE name GLOB '__fsqlite_capture_*'")
      .all(),
    [],
  );
}
function normalized(bytes) {
  return decodeChangeset(bytes)
    .flatMap((t) => t.changes.map((change) => ({ table: t.name, pk: t.primaryKey, ...change })))
    .sort(
      (a, b) =>
        a.table.localeCompare(b.table) ||
        a.operation.localeCompare(b.operation) ||
        JSON.stringify(a.old ?? a.new, (_, v) =>
          typeof v === "bigint" ? `${v}n` : v,
        ).localeCompare(
          JSON.stringify(b.old ?? b.new, (_, v) => (typeof v === "bigint" ? `${v}n` : v)),
        ),
    );
}
async function oracle(
  work,
  ddl = schema,
  initial = seed,
  tables = ["t"],
  options = {},
  compareNative = true,
) {
  const source = new SqlTarget(ddl, initial),
    receiver = new SqlTarget(ddl, initial);
  const session = source.db.createSession();
  try {
    const result = await captureChangeset(source, work, { tables, ...options });
    assert.equal(receiver.db.applyChangeset(result.changeset), true);
    for (const table of tables) assert.deepEqual(rows(receiver, table), rows(source, table));
    if (compareNative)
      assert.deepEqual(normalized(result.changeset), normalized(session.changeset()));
    assert.equal(receiver.db.applyChangeset(invertChangeset(result.changeset)), true);
    const before = new SqlTarget(ddl, initial);
    try {
      for (const table of tables) assert.deepEqual(rows(receiver, table), rows(before, table));
    } finally {
      before.close();
    }
    noArtifacts(source);
    return result;
  } finally {
    session.close();
    source.close();
    receiver.close();
  }
}

test("capture real INSERT/UPDATE/DELETE, generated integer keys and callback result", async () => {
  const result = await oracle(async (tx) => {
    await tx.execute("UPDATE t SET value='new' WHERE id=1");
    await tx.execute("INSERT INTO t(value,extra) VALUES ('fresh',17)");
    await tx.execute("DELETE FROM t WHERE id=2");
    return 42;
  });
  assert.equal(result.value, 42);
  assert.equal(result.changes, 3);
  assert.equal(result.touchedRows, 3);
});
test("first-touch state coalesces repeated writes and net-zero insert/delete", async () => {
  const result = await oracle(async (tx) => {
    await tx.execute("UPDATE t SET value='step' WHERE id=1");
    await tx.execute("UPDATE t SET value='final',extra=22 WHERE id=1");
    await tx.execute("INSERT INTO t VALUES(3,'temporary',NULL)");
    await tx.execute("DELETE FROM t WHERE id=3");
  });
  assert.equal(result.changes, 1);
  assert.equal(result.touchedRows, 2);
});
test("delete/reinsert coalesces and restoring original values yields no changes", async () => {
  await oracle(async (tx) => {
    await tx.execute("DELETE FROM t WHERE id=1");
    await tx.execute("INSERT INTO t VALUES (1,'replaced',NULL)");
    await tx.execute("UPDATE t SET value='step' WHERE id=2");
    await tx.execute("UPDATE t SET value='delete' WHERE id=2");
  });
});
test("primary-key update is a DELETE and INSERT, including NOCASE spelling", async () => {
  const ddl = "CREATE TABLE t(id TEXT PRIMARY KEY COLLATE NOCASE, value);",
    initial = "INSERT INTO t VALUES('a','old');";
  const source = new SqlTarget(ddl, initial),
    receiver = new SqlTarget(ddl, initial);
  try {
    const result = await captureChangeset(
      source,
      (tx) => tx.execute("UPDATE t SET id='A' WHERE id='a'"),
      { tables: ["t"] },
    );
    assert.deepEqual(
      decodeChangeset(result.changeset)[0].changes.map((c) => c.operation),
      ["delete", "insert"],
    );
    assert.equal(receiver.db.applyChangeset(result.changeset), true);
    assert.deepEqual(rows(receiver), rows(source));
    // Native session 3.49.1 can emit a key-slot UPDATE here, which the codec
    // rejects. Inversion can also encounter the receiver's NOCASE uniqueness
    // constraint. Prove forward application, not those stronger guarantees.
  } finally {
    source.close();
    receiver.close();
  }
});
test("composite WITHOUT ROWID keys preserve key ordinals and mixed storage types", async () => {
  await oracle(
    async (tx) => {
      await tx.execute("UPDATE t SET value='new' WHERE a=1 AND b='x'");
      await tx.execute("INSERT INTO t VALUES(2,'y',X'ABCDEF')");
      await tx.execute("UPDATE t SET a=3 WHERE a=2");
    },
    "CREATE TABLE t(a,b,value, PRIMARY KEY(b DESC,a)) WITHOUT ROWID;",
    "INSERT INTO t VALUES(1,'x','old');",
  );
});
test("NULL key rows are omitted; transitions across NULL become INSERT/DELETE", async () => {
  const ddl = "CREATE TABLE t(id TEXT PRIMARY KEY,value);",
    initial = "INSERT INTO t VALUES(NULL,1),('old',2);";
  const source = new SqlTarget(ddl, initial),
    receiver = new SqlTarget(ddl, initial),
    session = source.db.createSession();
  try {
    const result = await captureChangeset(
      source,
      async (tx) => {
        await tx.execute("UPDATE t SET id='new' WHERE value=1");
        await tx.execute("UPDATE t SET id=NULL WHERE id='old'");
        await tx.execute("INSERT INTO t VALUES(NULL,3)");
      },
      { tables: ["t"] },
    );
    assert.equal(result.changes, 2);
    assert.deepEqual(normalized(result.changeset), normalized(session.changeset()));
    assert.equal(receiver.db.applyChangeset(result.changeset), true);
    assert.deepEqual(
      rows(receiver).filter((row) => row[0] !== null),
      rows(source).filter((row) => row[0] !== null),
    );
    // NULL-key rows are explicitly outside session capture, not replicated.
    assert.deepEqual(
      rows(receiver).filter((row) => row[0] === null),
      [[null, 1n]],
    );
    noArtifacts(source);
  } finally {
    session.close();
    source.close();
    receiver.close();
  }
});
test("typed projection preserves int64 extremes, REAL, NUL text and owned blobs", async () => {
  await oracle(async (tx) => {
    await tx.execute("INSERT INTO t VALUES(-9223372036854775808,CAST(2 AS REAL),X'0000FF')");
    await tx.execute("INSERT INTO t VALUES(9223372036854775807,'a'||char(0)||'b','😀')");
    await tx.execute("UPDATE t SET value=CAST(3 AS REAL),extra=9223372036854775807 WHERE id=1");
  });
});
test("storage-class-only changes are retained in before-images", async () => {
  await oracle(
    async (tx) => {
      await tx.execute("UPDATE t SET value=CAST(7 AS REAL) WHERE id=1");
    },
    schema,
    "INSERT INTO t VALUES(1,7,NULL);",
  );
});
test("REPLACE captures deletions caused by both primary and secondary unique keys", async () => {
  await oracle(
    async (tx) => {
      await tx.execute("INSERT OR REPLACE INTO t VALUES(3,'taken')");
    },
    "CREATE TABLE t(id INTEGER PRIMARY KEY,value UNIQUE);",
    "INSERT INTO t VALUES(1,'taken'),(2,'left');",
  );
});
test("rolled-back SQL savepoints also undo first-touch records and budget charges", async () => {
  const result = await oracle(
    async (tx) => {
      await tx.execute("SAVEPOINT inner_work");
      await tx.execute("INSERT INTO t VALUES(3,'gone',NULL)");
      await tx.execute("ROLLBACK TO inner_work");
      await tx.execute("RELEASE inner_work");
      await tx.execute("UPDATE t SET value='kept' WHERE id=1");
    },
    schema,
    seed,
    ["t"],
    { maxRows: 1 },
  );
  assert.equal(result.touchedRows, 1);
});

test("owned nested transactions roll back capture state and preserve released work", async () => {
  const result = await oracle(
    async (tx) => {
      assert.equal(typeof tx.transaction, "function");

      await assert.rejects(
        tx.transaction(async (child) => {
          await child.execute("INSERT INTO t VALUES(3,'rolled-back',NULL)");
          await child.execute("UPDATE t SET value='discarded' WHERE id=1");
          throw new Error("rollback child");
        }),
        /rollback child/,
      );

      await tx.transaction(async (child) => {
        await child.execute("UPDATE t SET value='released' WHERE id=1");
        await child.execute("INSERT INTO t VALUES(4,'kept',NULL)");
      });
    },
    schema,
    seed,
    ["t"],
    { maxRows: 2 },
  );

  assert.equal(result.touchedRows, 2);
  assert.equal(result.changes, 2);
});
test("callback errors roll back all tables and every TEMP capture object", async () => {
  const target = new SqlTarget(schema, seed),
    error = new Error("callback");
  try {
    const original = rows(target);
    await assert.rejects(
      captureChangeset(
        target,
        async (tx) => {
          await tx.execute("DELETE FROM t");
          throw error;
        },
        { tables: ["t"] },
      ),
      (e) => e === error,
    );
    assert.deepEqual(rows(target), original);
    noArtifacts(target);
    await captureChangeset(target, (tx) => tx.execute("UPDATE t SET value='retry' WHERE id=1"), {
      tables: ["t"],
    });
    noArtifacts(target);
  } finally {
    target.close();
  }
});
test("capture within an outer transaction remains provisional until its commit", async () => {
  const target = new SqlTarget(schema, seed);
  try {
    const before = rows(target);
    await assert.rejects(
      target.transaction(async () => {
        const result = await captureChangeset(target, (tx) => tx.execute("DELETE FROM t"), {
          tables: ["t"],
        });
        assert.equal(result.changes, 2);
        throw new Error("outer");
      }),
      /outer/,
    );
    assert.deepEqual(rows(target), before);
    noArtifacts(target);
  } finally {
    target.close();
  }
});
test("deferred foreign-key commit failure rejects capture and restores source", async () => {
  const target = new SqlTarget(
    "CREATE TABLE p(id PRIMARY KEY); CREATE TABLE t(id INTEGER PRIMARY KEY,pid REFERENCES p DEFERRABLE INITIALLY DEFERRED);",
  );
  try {
    await assert.rejects(
      captureChangeset(target, (tx) => tx.execute("INSERT INTO t VALUES(1,99)"), { tables: ["t"] }),
      /FOREIGN KEY/,
    );
    assert.deepEqual(rows(target), []);
    noArtifacts(target);
  } finally {
    target.close();
  }
});
test("DML and capture state roll back on SQL row, byte and slot budgets", async () => {
  for (const budget of [{ maxRows: 1 }, { maxBytes: 100 }, { maxCells: 2 }]) {
    const target = new SqlTarget(schema, seed);
    try {
      const before = rows(target);
      await assert.rejects(
        captureChangeset(target, (tx) => tx.execute("UPDATE t SET value='both'"), {
          tables: ["t"],
          ...budget,
        }),
        /CAPTURE_LIMIT/,
      );
      assert.deepEqual(rows(target), before);
      noArtifacts(target);
    } finally {
      target.close();
    }
  }
});
test("huge postimages are rejected by SQL length before the value is fetched", async () => {
  const target = new SqlTarget(schema);
  try {
    await assert.rejects(
      captureChangeset(
        target,
        (tx) => tx.execute("INSERT INTO t VALUES(1,zeroblob(100000),NULL)"),
        { tables: ["t"], maxBytes: 1000 },
      ),
      /CAPTURE_LIMIT/,
    );
    assert.deepEqual(rows(target), []);
    noArtifacts(target);
    assert.equal(
      target.statements.some((sql) => sql.startsWith('SELECT typeof("id")')),
      false,
    );
  } finally {
    target.close();
  }
});
test("output codec limits reject before source commit", async () => {
  const target = new SqlTarget(schema, seed);
  try {
    const before = rows(target);
    await assert.rejects(
      captureChangeset(target, (tx) => tx.execute("DELETE FROM t"), {
        tables: ["t"],
        limits: { maxChanges: 1 },
      }),
      /maxChanges/,
    );
    assert.deepEqual(rows(target), before);
    noArtifacts(target);
  } finally {
    target.close();
  }
});
test("cancellation after DML rolls back instead of publishing a partial capture", async () => {
  const target = new SqlTarget(schema, seed),
    abort = new AbortController();
  try {
    const before = rows(target);
    await assert.rejects(
      captureChangeset(
        target,
        async (tx) => {
          await tx.execute("DELETE FROM t");
          abort.abort("stop");
        },
        { tables: ["t"], signal: abort.signal },
      ),
      { code: "ERR_FSQLITE_CAPTURE_CANCELLED" },
    );
    assert.deepEqual(rows(target), before);
    noArtifacts(target);
  } finally {
    target.close();
  }
});
test("deadline includes awaited callback time and cleanup", async () => {
  const target = new SqlTarget(schema, seed);
  try {
    await assert.rejects(
      captureChangeset(
        target,
        async (tx) => {
          await tx.execute("DELETE FROM t");
          await new Promise((r) => setTimeout(r, 15));
        },
        { tables: ["t"], timeoutMs: 5 },
      ),
      { code: "ERR_FSQLITE_CAPTURE_TIMEOUT" },
    );
    assert.equal(rows(target).length, 2);
    noArtifacts(target);
  } finally {
    target.close();
  }
});
for (const [title, ddl, tables] of [
  ["missing table", schema, ["missing"]],
  ["view", schema + "CREATE VIEW v AS SELECT * FROM t;", ["v"]],
  ["no primary key", "CREATE TABLE t(value);", ["t"]],
  ["generated column", "CREATE TABLE t(id PRIMARY KEY,v GENERATED ALWAYS AS(id+1));", ["t"]],
  [
    "application trigger",
    schema +
      "CREATE TRIGGER app AFTER INSERT ON t BEGIN UPDATE t SET value=1 WHERE id=NEW.id; END;",
    ["t"],
  ],
])
  test(`preflight rejects ${title} without invoking callback`, async () => {
    const target = new SqlTarget(ddl);
    let called = false;
    try {
      await assert.rejects(
        captureChangeset(
          target,
          () => {
            called = true;
          },
          { tables },
        ),
        { code: "ERR_FSQLITE_CAPTURE_SCHEMA" },
      );
      assert.equal(called, false);
      noArtifacts(target);
    } finally {
      target.close();
    }
  });
test("recursive_triggers must be enabled explicitly; helper never changes connection policy", async () => {
  const target = new SqlTarget(schema);
  target.db.exec("PRAGMA recursive_triggers=OFF");
  try {
    await assert.rejects(
      captureChangeset(target, () => {}, { tables: ["t"] }),
      /recursive_triggers/,
    );
    assert.equal(target.db.prepare("PRAGMA recursive_triggers").get().recursive_triggers, 0);
    noArtifacts(target);
  } finally {
    target.close();
  }
});
test("schema mutation rolls back with application writes", async () => {
  const target = new SqlTarget(schema, seed);
  try {
    await assert.rejects(
      captureChangeset(
        target,
        async (tx) => {
          await tx.execute("DELETE FROM t");
          await tx.execute("ALTER TABLE t ADD COLUMN other");
        },
        { tables: ["t"] },
      ),
      { code: "ERR_FSQLITE_CAPTURE_SCHEMA" },
    );
    assert.equal(rows(target).length, 2);
    assert.equal(target.db.prepare("PRAGMA table_info('t')").all().length, 3);
    noArtifacts(target);
  } finally {
    target.close();
  }
});
test("input options are validated before any transaction begins", async () => {
  const target = {
    transaction: () => {
      throw new Error("must not enter");
    },
  };
  for (const options of [
    { tables: [] },
    { tables: ["T", "t"] },
    { tables: ["sqlite_schema"] },
    { tables: ["__fsqlite_changeset_receipts"] },
    { tables: ["t"], maxRows: 0 },
    { tables: ["t"], timeoutMs: 0 },
    { tables: ["t"], limits: { maxBytes: 0 } },
  ]) {
    await assert.rejects(
      captureChangeset(target, () => {}, options),
      (e) => !e.message.includes("must not enter"),
    );
  }
});
test("table and column quoting handles hostile-looking valid identifiers", async () => {
  const table = 'odd";--';
  const target = new SqlTarget('CREATE TABLE "odd"";--"("key""" INTEGER PRIMARY KEY,"select");');
  try {
    const result = await captureChangeset(
      target,
      (tx) => tx.execute('INSERT INTO "odd"";--" VALUES(1,2)'),
      { tables: [table] },
    );
    assert.equal(decodeChangeset(result.changeset)[0].name, table);
    noArtifacts(target);
  } finally {
    target.close();
  }
});
test("whole-scope indirect flag is explicit rather than invented trigger-depth evidence", async () => {
  const target = new SqlTarget(schema);
  try {
    const result = await captureChangeset(
      target,
      (tx) => tx.execute("INSERT INTO t VALUES(1,2,3)"),
      { tables: ["t"], indirect: true },
    );
    assert.equal(decodeChangeset(result.changeset)[0].changes[0].indirect, true);
  } finally {
    target.close();
  }
});
for (let seedValue = 1; seedValue <= 20; seedValue++)
  test(`native session equivalence for deterministic DML workload ${seedValue}`, async () => {
    let state = seedValue;
    const random = () => {
      state = (Math.imul(state, 1664525) + 1013904223) >>> 0;
      return state;
    };
    await oracle(async (tx) => {
      for (let i = 0; i < 50; i++) {
        const id = BigInt(random() % 12),
          op = random() % 4,
          value = BigInt(random() % 1000);
        if (op === 0) await tx.execute("INSERT OR IGNORE INTO t VALUES(?,?,NULL)", [id, value]);
        else if (op === 1) await tx.execute("UPDATE t SET value=? WHERE id=?", [value, id]);
        else if (op === 2) await tx.execute("DELETE FROM t WHERE id=?", [id]);
        else await tx.execute("INSERT OR REPLACE INTO t VALUES(?,?,X'1234')", [id, value]);
      }
    });
  });

test("no-op capture emits a valid empty changeset and leaves no TEMP objects", async () => {
  const target = new SqlTarget(schema, seed);
  try {
    const result = await captureChangeset(target, () => "no-op", { tables: ["t"] });
    assert.deepEqual(result, {
      value: "no-op",
      changeset: new Uint8Array(),
      changes: 0,
      touchedRows: 0,
    });
    noArtifacts(target);
  } finally {
    target.close();
  }
});
for (const encoding of ["UTF-16le", "UTF-16be"])
  test(`typed bytes preserve ${encoding} text, including NUL and BOM`, async () => {
    await oracle(
      (tx) => tx.execute("INSERT INTO t VALUES(1,'a'||char(0)||'中😀',char(65279)||'BOM')"),
      `PRAGMA encoding='${encoding}';` + schema,
      "",
    );
  });
test("one changed row in a large table uses only bounded primary-key data reads", async () => {
  const target = new SqlTarget(
    schema,
    "WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<10000) INSERT INTO t SELECT x,'value',NULL FROM n;",
  );
  try {
    const result = await captureChangeset(
      target,
      (tx) => tx.execute("UPDATE t SET value='changed' WHERE id=5678"),
      { tables: ["t"], maxRows: 1, maxBytes: 1000 },
    );
    assert.equal(result.changes, 1);
    assert.equal(result.touchedRows, 1);
    const reads = target.statements.filter((sql) => sql.includes('FROM main."t"'));
    assert.equal(reads.length, 2);
    for (const sql of reads) assert.match(sql, /WHERE "id" = \? LIMIT 2$/);
    for (const sql of reads) {
      const plans = target.db.prepare(`EXPLAIN QUERY PLAN ${sql}`).all(5678n);
      assert.ok(plans.some((p) => /SEARCH (main\.)?t USING INTEGER PRIMARY KEY/.test(p.detail)));
    }
  } finally {
    target.close();
  }
});
test("existing reserved TEMP objects are preserved and block capture", async () => {
  const target = new SqlTarget(schema);
  target.db.exec(
    "CREATE TEMP TABLE __fsqlite_capture_budget(precious); INSERT INTO __fsqlite_capture_budget VALUES(42)",
  );
  try {
    await assert.rejects(
      captureChangeset(target, () => {}, { tables: ["t"] }),
      { code: "ERR_FSQLITE_CAPTURE_SCHEMA" },
    );
    assert.equal(
      target.db.prepare("SELECT precious FROM __fsqlite_capture_budget").get().precious,
      42,
    );
  } finally {
    target.close();
  }
});
test("foreign-key cascade changes are captured in every selected table", async () => {
  const ddl =
    "CREATE TABLE p(id INTEGER PRIMARY KEY); CREATE TABLE t(id INTEGER PRIMARY KEY,pid REFERENCES p(id) ON DELETE CASCADE);";
  const initial = "INSERT INTO p VALUES(1),(2); INSERT INTO t VALUES(10,1),(11,2);";
  const source = new SqlTarget(ddl, initial),
    receiver = new SqlTarget(ddl, initial);
  try {
    const result = await captureChangeset(source, (tx) => tx.execute("DELETE FROM p WHERE id=1"), {
      tables: ["p", "t"],
    });
    let cascaded = 0;
    assert.equal(result.changes, 2);
    assert.equal(
      receiver.db.applyChangeset(result.changeset, {
        onConflict(reason) {
          // The receiving FK action has already deleted this child. This policy
          // is explicit: capture does not silently turn off receiver constraints.
          assert.equal(reason, constants.SQLITE_CHANGESET_NOTFOUND);
          cascaded++;
          return constants.SQLITE_CHANGESET_OMIT;
        },
      }),
      true,
    );
    assert.equal(cascaded, 1);
    assert.deepEqual(rows(receiver, "p"), rows(source, "p"));
    assert.deepEqual(rows(receiver), rows(source));
    noArtifacts(source);
  } finally {
    source.close();
    receiver.close();
  }
});
test("captured table names and limits are owned before asynchronous transaction admission", async () => {
  const target = new SqlTarget(schema),
    tables = ["t"],
    limits = { maxChanges: 1 };
  const wrapper = {
    transaction: async (work) => {
      tables[0] = "wrong";
      limits.maxChanges = 0;
      return target.transaction(work);
    },
  };
  try {
    const result = await captureChangeset(
      wrapper,
      (tx) => tx.execute("INSERT INTO t VALUES(1,2,3)"),
      { tables, limits },
    );
    assert.equal(result.changes, 1);
  } finally {
    target.close();
  }
});

// The owner executes real SQLite savepoints; hooks delay or fail their actual
// settlement boundaries. No SQL result or capture journal is synthesized.
class CaptureOwner extends SqlTarget {
  constructor(ddl = schema, initial = seed) {
    super(ddl, initial);
    this.boundaries = [];
  }
  async execute(sql, params = []) {
    await this.beforeSql?.("execute", sql, this.depth);
    return super.execute(sql, params);
  }
  async query(sql, params = []) {
    await this.beforeSql?.("query", sql, this.depth);
    return super.query(sql, params);
  }
  async transaction(work) {
    const depth = this.depth++, key = `owner_${++this.serial}`;
    let begun = false;
    try {
      await this.beforeBegin?.(depth);
      this.db.exec(depth ? `SAVEPOINT ${key}` : "BEGIN");
      begun = true;
      this.boundaries.push(["begin", depth]);
      const scoped = {
        execute: (sql, params) => this.execute(sql, params),
        query: (sql, params) => this.query(sql, params),
        transaction: (child) => this.transaction(child),
      };
      const result = await work(scoped);
      await this.beforeRelease?.(depth);
      this.db.exec(depth ? `RELEASE ${key}` : "COMMIT");
      this.boundaries.push(["release", depth]);
      return result;
    } catch (error) {
      if (begun) {
        try {
          await this.beforeRollback?.(depth, error);
          this.db.exec(depth ? `ROLLBACK TO ${key}; RELEASE ${key}` : "ROLLBACK");
          this.boundaries.push(["rollback", depth]);
          await this.afterRollback?.(depth, error);
        } catch (cleanup) {
          throw new AggregateError([error, cleanup], "capture owner cleanup failed", { cause: error });
        }
      }
      throw error;
    } finally {
      this.depth--;
    }
  }
}
const captureGate = () => {
  let resolve;
  const promise = new Promise((r) => { resolve = r; });
  return { promise, resolve };
};
const nextCaptureTurn = () => new Promise((r) => setImmediate(r));
const captureBusy = { code: "ERR_FSQLITE_CAPTURE_BUSY" };
const captureState = { code: "ERR_FSQLITE_CAPTURE_STATE" };

for (const cause of [undefined, null, NaN, "rollback", new Error("child abort")]) {
  test(`capture preserves exact handled child rejection: ${String(cause)}`, async () => {
    const target = new CaptureOwner();
    try {
      const result = await captureChangeset(target, async (tx) => {
        await assert.rejects(tx.transaction(async (child) => {
          await child.execute("DELETE FROM t");
          throw cause;
        }), (error) => Object.is(error, cause));
        return tx.transaction(async (child) => {
          await child.execute("UPDATE t SET value='kept' WHERE id=1");
          return { answer: 42 };
        });
      }, { tables: ["t"], maxRows: 2 });
      assert.deepEqual(result.value, { answer: 42 });
      assert.equal(result.changes, 1);
      assert.equal(result.touchedRows, 1);
      assert.deepEqual(rows(target), [[1n, "kept", null], [2n, "delete", new Uint8Array([1, 2])]]);
      noArtifacts(target);
    } finally { target.close(); }
  });
}

test("released grandchildren disappear with their parent rollback and restore capture budgets", async () => {
  const result = await oracle(async (tx) => {
    await assert.rejects(tx.transaction(async (child) => {
      await child.transaction(async (grandchild) => {
        await grandchild.execute("DELETE FROM t WHERE id=1");
        await grandchild.execute("INSERT INTO t VALUES(3,'discard',NULL)");
      });
      throw new Error("discard parent");
    }), /discard parent/);
    await tx.transaction(async (child) => {
      await child.execute("UPDATE t SET value='final' WHERE id=1");
      await child.transaction(async (grandchild) => {
        await grandchild.execute("INSERT INTO t VALUES(4,'retained',NULL)");
      });
    });
  }, schema, seed, ["t"], { maxRows: 2 });
  assert.equal(result.touchedRows, 2);
  assert.equal(result.changes, 2);
});

test("caught child SQL failures force child rollback, not outer rollback after it is confirmed", async () => {
  const result = await oracle(async (tx) => {
    await assert.rejects(tx.transaction(async (child) => {
      await child.execute("INSERT INTO t VALUES(3,'discard',NULL)");
      await child.execute("INSERT INTO t VALUES(4,'over budget',NULL)").catch(() => {});
    }), /CAPTURE_LIMIT/);
    await tx.execute("UPDATE t SET value='survives' WHERE id=1");
  }, schema, seed, ["t"], { maxRows: 1 });
  assert.equal(result.touchedRows, 1);
  assert.equal(result.changes, 1);
});

test("capture refuses parent SQL and sibling savepoints while a child owns the scope", async () => {
  const target = new CaptureOwner(), entered = captureGate(), release = captureGate();
  try {
    const result = await captureChangeset(target, async (tx) => {
      const child = tx.transaction(async (inner) => {
        await inner.execute("INSERT INTO t VALUES(3,'child',NULL)");
        entered.resolve(); await release.promise;
        return "child result";
      });
      await entered.promise;
      const before = [target.statements.length, target.boundaries.length];
      try {
        await assert.rejects(tx.execute("DELETE FROM t"), captureBusy);
        await assert.rejects(tx.query("SELECT * FROM t"), captureBusy);
        await assert.rejects(tx.transaction(async () => {}), captureBusy);
        assert.deepEqual([target.statements.length, target.boundaries.length], before);
      } finally { release.resolve(); await child; }
      assert.equal(await child, "child result");
      await tx.execute("UPDATE t SET value='parent' WHERE id=1");
    }, { tables: ["t"] });
    assert.equal(result.changes, 2);
    assert.deepEqual(rows(target).map((r) => r[0]), [1n, 2n, 3n]);
    noArtifacts(target);
  } finally { release.resolve(); target.close(); }
});

test("capture will not enter a child while previously admitted parent SQL is unsettled", async () => {
  const target = new CaptureOwner(), entered = captureGate(), release = captureGate();
  target.beforeSql = async (kind, sql) => {
    if (sql === "UPDATE t SET value='held' WHERE id=1") { entered.resolve(); await release.promise; }
  };
  try {
    const result = await captureChangeset(target, async (tx) => {
      const write = tx.execute("UPDATE t SET value='held' WHERE id=1");
      await entered.promise;
      const count = target.boundaries.length;
      try {
        await assert.rejects(tx.transaction(async () => {}), captureBusy);
        assert.equal(target.boundaries.length, count);
      } finally { release.resolve(); await write; }
      return tx.transaction(async () => 19);
    }, { tables: ["t"] });
    assert.equal(result.value, 19); assert.equal(result.changes, 1);
    noArtifacts(target);
  } finally { release.resolve(); target.close(); }
});

test("capture joins unawaited child and grandchild SQL before collecting the final source image", async () => {
  const target = new CaptureOwner(), receiver = new SqlTarget(schema, seed);
  const entered = captureGate(), release = captureGate();
  let child, grandchild, sql, finished = false;
  target.beforeSql = async (_kind, statement) => {
    if (statement === "INSERT INTO t VALUES(3,'late',NULL)") { entered.resolve(); await release.promise; }
  };
  const operation = captureChangeset(target, (tx) => {
    child = tx.transaction(async (inner) => {
      grandchild = inner.transaction(async (last) => {
        sql = last.execute("INSERT INTO t VALUES(3,'late',NULL)");
      });
    });
    return "outer result";
  }, { tables: ["t"] });
  void operation.then(() => { finished = true; }, () => { finished = true; });
  try {
    await Promise.race([entered.promise, operation.then(() => assert.fail("missed held write"))]);
    await nextCaptureTurn();
    assert.equal(finished, false); assert.equal(target.depth, 3);
    assert.equal(target.boundaries.some(([kind]) => kind === "release"), false);
    release.resolve();
    const result = await operation;
    await Promise.all([child, grandchild, sql]);
    assert.equal(result.value, "outer result"); assert.equal(result.changes, 1);
    assert.equal(receiver.db.applyChangeset(result.changeset), true);
    assert.deepEqual(rows(receiver), rows(target)); noArtifacts(target);
  } finally {
    release.resolve(); await Promise.allSettled([operation, child, grandchild, sql]);
    target.close(); receiver.close();
  }
});

test("saved child executors expire before owner RELEASE and cannot enter later capture work", async () => {
  const target = new CaptureOwner(); let saved, checked = 0;
  target.beforeRelease = async (depth) => {
    if (depth !== 1) return;
    const before = [target.statements.length, target.boundaries.length];
    await assert.rejects(saved.execute("DELETE FROM t"), /scope has ended/);
    await assert.rejects(saved.query("SELECT 1"), /scope has ended/);
    await assert.rejects(saved.transaction(async () => {}), /scope has ended/);
    assert.deepEqual([target.statements.length, target.boundaries.length], before);
    checked++;
  };
  try {
    const result = await captureChangeset(target, (tx) => tx.transaction(async (child) => {
      saved = child;
      await child.execute("UPDATE t SET value='kept' WHERE id=1");
    }), { tables: ["t"] });
    assert.equal(result.changes, 1); assert.equal(checked, 1);
    target.beforeRelease = null;
    await captureChangeset(target, async (tx) => {
      await assert.rejects(saved.transaction(async () => {}), /scope has ended/);
      await tx.execute("UPDATE t SET value='later' WHERE id=2");
    }, { tables: ["t"] });
    assert.equal(rows(target)[1][1], "later"); noArtifacts(target);
  } finally { target.close(); }
});

for (const stage of ["begin", "release", "rollback", "metadata", "getter"]) {
  test(`uncertain child ${stage} blocks more capture SQL even when its rejection is caught`, async () => {
    const target = new CaptureOwner(), cause = new Error(`uncertain ${stage}`);
    let refusals = 0, observed;
    if (stage === "begin") target.beforeBegin = (depth) => { if (depth === 1) throw cause; };
    if (stage === "release") target.beforeRelease = (depth) => { if (depth === 1) throw cause; };
    if (stage === "rollback") target.afterRollback = (depth) => { if (depth === 1) throw cause; };
    if (stage === "metadata") target.afterRollback = (depth, error) => {
      if (depth === 1) error.cleanupErrors = [cause];
    };
    if (stage === "getter") target.afterRollback = (depth, error) => {
      if (depth === 1) Object.defineProperty(error, "cleanupErrors", { get() { throw cause; } });
    };
    try {
      await assert.rejects(captureChangeset(target, async (tx) => {
        await tx.execute("UPDATE t SET value='outer' WHERE id=1");
        await tx.transaction(async (child) => {
          await child.execute("DELETE FROM t WHERE id=2");
          if (stage !== "release") throw new Error("child callback");
        }).catch((error) => { observed = error; });
        const before = [target.statements.length, target.boundaries.length];
        for (const attempt of [
          () => tx.execute("DELETE FROM t"), () => tx.query("SELECT 1"),
          () => tx.transaction(async () => {}),
        ]) {
          await assert.rejects(attempt(), captureState); refusals++;
        }
        assert.deepEqual([target.statements.length, target.boundaries.length], before);
      }, { tables: ["t"] }), (error) => {
        assert.equal(error.code, captureState.code);
        assert.equal(error.cleanupErrors.length, 1);
        assert.equal(error.cleanupErrors[0], observed);
        return true;
      });
      assert.equal(refusals, 3, "all admission refusals must actually execute");
      assert.deepEqual(rows(target).map((r) => r[1]), ["old", "delete"]);
      noArtifacts(target);
    } finally { target.close(); }
  });
}

test("caught grandchild uncertainty survives confirmed parent rollback and fences the root", async () => {
  const target = new CaptureOwner(), failure = new Error("grandchild release failed");
  let refusals = 0;
  target.beforeRelease = (depth) => { if (depth === 2) throw failure; };
  try {
    await assert.rejects(captureChangeset(target, async (tx) => {
      await tx.transaction(async (child) => {
        await child.transaction(async (grandchild) => { await grandchild.execute("DELETE FROM t"); }).catch(() => {});
        await assert.rejects(child.query("SELECT 1"), captureState); refusals++;
        throw new Error("parent callback failed too");
      }).catch(() => {});
      await assert.rejects(tx.execute("DELETE FROM t"), captureState); refusals++;
    }, { tables: ["t"] }), (error) => error.code === captureState.code && error.cleanupErrors[0] === failure);
    assert.equal(refusals, 2);
    assert.deepEqual(rows(target).map((r) => r[1]), ["old", "delete"]); noArtifacts(target);
  } finally { target.close(); }
});

test("cancellation waits for child rollback before rejecting capture without a payload", async () => {
  const target = new CaptureOwner(), controller = new AbortController();
  const entered = captureGate(), release = captureGate(); let finished = false;
  target.beforeRollback = async (depth) => { if (depth === 1) { entered.resolve(); await release.promise; } };
  const operation = captureChangeset(target, async (tx) => {
    await tx.transaction(async (child) => {
      await child.execute("DELETE FROM t"); throw new Error("roll back first");
    }).catch(() => {});
  }, { tables: ["t"], signal: controller.signal });
  void operation.then(() => { finished = true; }, () => { finished = true; });
  try {
    await Promise.race([entered.promise, operation.then(() => assert.fail("missing rollback"))]);
    controller.abort("cancel"); await nextCaptureTurn();
    assert.equal(finished, false); assert.equal(target.depth, 2);
    release.resolve();
    await assert.rejects(operation, { code: "ERR_FSQLITE_CAPTURE_CANCELLED" });
    assert.equal(target.depth, 0);
    assert.deepEqual(rows(target).map((r) => r[1]), ["old", "delete"]); noArtifacts(target);
  } finally { release.resolve(); await Promise.allSettled([operation]); target.close(); }
});

test("uncertain cleanup from unawaited children retains a separately thrown callback cause", async () => {
  const target = new CaptureOwner(), entered = captureGate(), release = captureGate();
  const callback = new Error("outer callback failed"), cleanup = new Error("child cleanup uncertain");
  let child, finished = false;
  target.afterRollback = (depth) => { if (depth === 1) throw cleanup; };
  const operation = captureChangeset(target, async (tx) => {
    child = tx.transaction(async (inner) => {
      await inner.execute("DELETE FROM t"); entered.resolve(); await release.promise;
      throw new Error("child rollback");
    });
    await entered.promise;
    throw callback;
  }, { tables: ["t"] });
  void operation.then(() => { finished = true; }, () => { finished = true; });
  try {
    await entered.promise; await nextCaptureTurn(); assert.equal(finished, false);
    release.resolve();
    await assert.rejects(operation, (error) => error.code === captureState.code &&
      error.cause === callback && error.cleanupErrors[0].errors.includes(cleanup));
    assert.deepEqual(rows(target).map((r) => r[1]), ["old", "delete"]); noArtifacts(target);
  } finally { release.resolve(); await Promise.allSettled([operation, child]); target.close(); }
});

test("invalid child callbacks are rejected before SQL; executors without children remain valid", async () => {
  const target = new CaptureOwner();
  try {
    const result = await captureChangeset(target, async (tx) => {
      const before = target.boundaries.length;
      for (const work of [null, undefined, 42]) {
        await assert.rejects(tx.transaction(work), { code: "ERR_FSQLITE_CAPTURE_INPUT" });
      }
      assert.equal(target.boundaries.length, before);
      await tx.execute("UPDATE t SET value='valid' WHERE id=1");
    }, { tables: ["t"] });
    assert.equal(result.changes, 1);
    const targetWithoutChildren = {
      transaction: (work) => target.transaction((tx) => work({ execute: tx.execute, query: tx.query })),
    };
    await captureChangeset(targetWithoutChildren, async (tx) => {
      assert.equal(tx.transaction, undefined);
      await tx.execute("UPDATE t SET value='also valid' WHERE id=2");
    }, { tables: ["t"] });
    noArtifacts(target);
  } finally { target.close(); }
});
