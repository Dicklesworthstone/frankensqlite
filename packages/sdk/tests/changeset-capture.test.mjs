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
