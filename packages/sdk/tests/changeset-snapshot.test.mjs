import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import { applyChangeset } from "../src/changeset-apply.ts";
import { captureChangeset, snapshotChangeset } from "../src/changeset-capture.ts";
import { decodeChangeset, encodeChangeset, invertChangeset } from "../src/changeset-codec.ts";
import { CHANGESET_OUTBOX_TABLE, ChangesetOutbox } from "../src/changeset-outbox.ts";

// Real SQLite SQL/session oracle. Hooks inject interleavings or corrupt adapter
// replies; none of the SQL or binary changeset semantics are mocked.
class Target {
  constructor(ddl = "", path = ":memory:") {
    this.db = new DatabaseSync(path);
    this.db.exec(ddl);
    this.depth = 0;
    this.serial = 0;
    this.log = [];
    this.hook = undefined;
  }
  async execute(sql, params = []) {
    this.log.push(sql);
    return Number(this.db.prepare(sql).run(...params).changes);
  }
  async query(sql, params = []) {
    this.log.push(sql);
    const stmt = this.db.prepare(sql);
    stmt.setReadBigInts(true);
    stmt.setReturnArrays(true);
    const result = { rowArrays: stmt.all(...params) };
    await this.hook?.(sql, params, result);
    return result;
  }
  async transaction(work) {
    const nested = this.depth++ !== 0,
      name = `snapshot_test_${++this.serial}`;
    this.db.exec(nested ? `SAVEPOINT ${name}` : "BEGIN");
    try {
      const result = await work(this);
      this.db.exec(nested ? `RELEASE ${name}` : "COMMIT");
      return result;
    } catch (error) {
      try {
        this.db.exec(nested ? `ROLLBACK TO ${name}; RELEASE ${name}` : "ROLLBACK");
      } catch (cleanup) {
        throw new AggregateError([error, cleanup], "Test transaction and rollback failed");
      }
      throw error;
    } finally {
      this.depth--;
    }
  }
  close() {
    this.db.close();
  }
}
const quote = (s) => `"${s.replaceAll('"', '""')}"`;
function rows(db, table) {
  const s = db.prepare(`SELECT * FROM ${quote(table)} ORDER BY 1,2`);
  s.setReadBigInts(true);
  s.setReturnArrays(true);
  return s.all();
}
function normalized(bytes) {
  return decodeChangeset(bytes)
    .flatMap((t) => t.changes.map((c) => ({ table: t.name, pk: t.primaryKey, ...c })))
    .map((c) => JSON.stringify(c, (_, v) => (typeof v === "bigint" ? `${v}n` : v)))
    .sort();
}
const images = (sql) => sql.startsWith("SELECT typeof(") && sql.includes(" ORDER BY ");
async function oracle(ddl, seed, options = {}) {
  const source = new Target(ddl),
    receiver = new Target(ddl),
    session = source.db.createSession();
  try {
    seed(source.db);
    const tables = options.tables ?? ["t"],
      before = tables.map((t) => rows(source.db, t));
    const native = session.changeset();
    const snapshot = await snapshotChangeset(source, { tables, ...options });
    assert.equal(receiver.db.applyChangeset(snapshot.changeset), true);
    assert.deepEqual(
      tables.map((t) => rows(receiver.db, t)),
      before,
    );
    assert.deepEqual(
      tables.map((t) => rows(source.db, t)),
      before,
    );
    assert.deepEqual(normalized(snapshot.changeset), normalized(native));
    assert.equal(
      snapshot.changes,
      before.reduce((n, r) => n + r.length, 0),
    );
    assert.equal(source.db.prepare("SELECT count(*) AS n FROM temp.sqlite_schema").get().n, 0);
    assert(
      source.log.every((sql) => /^(SELECT|PRAGMA) /.test(sql)),
      "snapshot must be read-only",
    );
    assert.equal(receiver.db.applyChangeset(invertChangeset(snapshot.changeset)), true);
    assert(tables.every((t) => rows(receiver.db, t).length === 0));
    return snapshot;
  } finally {
    session.close();
    receiver.close();
    source.close();
  }
}
for (const n of [0, 1, 31, 32, 33, 64, 65, 127])
  test(`rowid snapshot/native session parity across ${n} rows`, async () => {
    await oracle("CREATE TABLE t(id INTEGER PRIMARY KEY, value)", (db) => {
      const s = db.prepare("INSERT INTO t VALUES(?,?)");
      for (let i = 0; i < n; i++) s.run(BigInt(i) - 70n, `value-${i}`);
    });
  });
for (const suffix of ["", " WITHOUT ROWID"])
  test(`mixed index order and explicit index collations${suffix}`, async () => {
    await oracle(
      "CREATE TABLE t(a TEXT COLLATE NOCASE,b INTEGER,c BLOB,v,PRIMARY KEY(b DESC,a COLLATE RTRIM ASC,c DESC))" +
        suffix,
      (db) => {
        const s = db.prepare("INSERT INTO t VALUES(?,?,?,?)");
        for (let b = 6; b >= 0; b--)
          for (let a = 5; a >= 0; a--)
            for (let c = 4; c >= 0; c--) s.run(`A${a}`, b, new Uint8Array([c]), `${b}:${a}:${c}`);
      },
    );
  });
for (const encoding of ["UTF-8", "UTF-16le", "UTF-16be"])
  test(`typed values survive ${encoding} snapshot and native apply`, async () => {
    await oracle(
      `PRAGMA encoding='${encoding}'; CREATE TABLE t(id INTEGER PRIMARY KEY, a, b, c, d)`,
      (db) => {
        db.prepare("INSERT INTO t VALUES(?,?,?,?,?)").run(
          -(1n << 63n),
          9007199254740993n,
          "\uFEFFa\0😀é",
          new Uint8Array([0, 255]),
          null,
        );
        db.exec("INSERT INTO t VALUES(9223372036854775807,CAST(2 AS REAL),1e999,-1e999,X'')");
      },
    );
  });
test("mixed storage classes in a non-affinity primary key paginate without coercion", async () => {
  await oracle("CREATE TABLE t(k BLOB PRIMARY KEY, v) WITHOUT ROWID", (db) => {
    const s = db.prepare("INSERT INTO t VALUES(?,?)");
    for (let i = 0; i < 80; i++) {
      s.run(BigInt(i) * 3n, `int${i}`);
      s.run(i * 3 + 0.5, `real${i}`);
      s.run(String(i), `text${i}`);
      s.run(new Uint8Array([i]), `blob${i}`);
    }
  });
});
test("declared INTEGER PRIMARY KEY DESC uses its index, not a presumed rowid alias", async () => {
  await oracle("CREATE TABLE t(id INTEGER PRIMARY KEY DESC, v)", (db) => {
    const s = db.prepare("INSERT INTO t VALUES(?,?)");
    for (let i = 0; i < 83; i++) s.run(i, `r${i}`);
  });
});
test("tables and columns with quotes and shadowed rowid names stay in main", async () => {
  const ddl =
    'CREATE TABLE "weird\'table" ("rowid" TEXT, "_rowid_" TEXT, "oid" TEXT, "i""d" INTEGER PRIMARY KEY)';
  await oracle(
    ddl,
    (db) => {
      db.exec(`INSERT INTO "weird'table" VALUES('a','b','c',5)`);
    },
    { tables: ["weird'table"] },
  );
  const s = new Target(
    "CREATE TABLE t(id INTEGER PRIMARY KEY,v);INSERT INTO t VALUES(1,2);CREATE TEMP TABLE t(id,value,other)",
  );
  try {
    assert.equal((await snapshotChangeset(s, { tables: ["T"] })).changes, 1);
  } finally {
    s.close();
  }
});
test("snapshot permits source triggers, needs no recursive-trigger policy and fires no writes", async () => {
  const s = new Target(
    "CREATE TABLE t(id INTEGER PRIMARY KEY,v);CREATE TABLE audit(n);INSERT INTO t VALUES(1,'v');CREATE TRIGGER tap AFTER UPDATE ON t BEGIN INSERT INTO audit VALUES(1);END",
  );
  try {
    s.db.exec("PRAGMA recursive_triggers=OFF;PRAGMA query_only=ON");
    assert.equal((await snapshotChangeset(s, { tables: ["t"] })).changes, 1);
    assert.equal(s.db.prepare("SELECT count(*) AS n FROM audit").get().n, 0);
    assert.equal(s.db.prepare("PRAGMA recursive_triggers").get().recursive_triggers, 0);
  } finally {
    s.close();
  }
});
test("parent-first multi-table seed includes existing data, not empty table headers", async () => {
  await oracle(
    "PRAGMA foreign_keys=ON;CREATE TABLE p(id INTEGER PRIMARY KEY,v);CREATE TABLE t(id INTEGER PRIMARY KEY,p REFERENCES p(id));CREATE TABLE empty(id INTEGER PRIMARY KEY,v)",
    (db) => {
      db.exec("INSERT INTO p VALUES(1,'p');INSERT INTO t VALUES(2,1)");
    },
    { tables: ["p", "empty", "t"] },
  );
});
for (const ddl of [
  "CREATE TABLE t(id,v)",
  "CREATE TABLE t(id PRIMARY KEY,v GENERATED ALWAYS AS(id+1))",
  "CREATE VIEW t AS SELECT 1 AS id,2 AS v",
  "CREATE VIRTUAL TABLE t USING fts5(v)",
  "CREATE TABLE t(id PRIMARY KEY,v);INSERT INTO t VALUES(NULL,5)",
  "CREATE TABLE t(a,b,v,PRIMARY KEY(a,b));INSERT INTO t VALUES(1,NULL,5)",
]) {
  test(`unsupported snapshot schema fails closed: ${ddl}`, async () => {
    const s = new Target(ddl);
    try {
      await assert.rejects(snapshotChangeset(s, { tables: ["t"] }), {
        code: "ERR_FSQLITE_CAPTURE_SCHEMA",
      });
      assert(!s.log.some(images));
    } finally {
      s.close();
    }
  });
}
test("all schemas preflight before any row image, and duplicate/system names reject before admission", async () => {
  const s = new Target("CREATE TABLE t(id INTEGER PRIMARY KEY,v);INSERT INTO t VALUES(1,2)");
  try {
    await assert.rejects(snapshotChangeset(s, { tables: ["t", "missing"] }), {
      code: "ERR_FSQLITE_CAPTURE_SCHEMA",
    });
    assert(!s.log.some(images));
    for (const tables of [[], ["t", "T"], ["sqlite_schema"], ["__fsqlite_changeset_outbox"]]) {
      s.log = [];
      await assert.rejects(snapshotChangeset(s, { tables }), { code: "ERR_FSQLITE_CAPTURE_INPUT" });
      assert.equal(s.log.length, 0);
    }
  } finally {
    s.close();
  }
});
for (const opts of [{ maxRows: 1 }, { maxCells: 3 }, { maxBytes: 100 }])
  test(`snapshot budget rejects before row-image transfer: ${JSON.stringify(opts)}`, async () => {
    const s = new Target(
      "CREATE TABLE t(id INTEGER PRIMARY KEY,v);INSERT INTO t VALUES(1,'x'),(2,'y')",
    );
    try {
      await assert.rejects(snapshotChangeset(s, { tables: ["t"], ...opts }), {
        code: "ERR_FSQLITE_CAPTURE_LIMIT",
      });
      assert(!s.log.some(images));
    } finally {
      s.close();
    }
  });
test("oversized individual postimage is length-checked before transferring its bytes", async () => {
  const s = new Target(
    "CREATE TABLE t(id INTEGER PRIMARY KEY,v);INSERT INTO t VALUES(1,zeroblob(1000000))",
  );
  try {
    await assert.rejects(snapshotChangeset(s, { tables: ["t"], maxBytes: 1024 }), {
      code: "ERR_FSQLITE_CAPTURE_LIMIT",
    });
    assert(!s.log.some(images));
  } finally {
    s.close();
  }
});
test("budgets are global across tables; wire budget remains independent", async () => {
  const s = new Target(
    "CREATE TABLE t(id INTEGER PRIMARY KEY,v);CREATE TABLE u(id INTEGER PRIMARY KEY,v);INSERT INTO t VALUES(1,2);INSERT INTO u VALUES(1,2)",
  );
  try {
    await assert.rejects(snapshotChangeset(s, { tables: ["t", "u"], maxRows: 1 }), {
      code: "ERR_FSQLITE_CAPTURE_LIMIT",
    });
    await assert.rejects(snapshotChangeset(s, { tables: ["t"], limits: { maxBytes: 10 } }), {
      code: "ERR_FSQLITE_CHANGESET_LIMIT",
    });
  } finally {
    s.close();
  }
});
test("options and table list are captured before asynchronous transaction admission", async () => {
  const s = new Target("CREATE TABLE t(id INTEGER PRIMARY KEY,v);INSERT INTO t VALUES(1,2)");
  try {
    const tables = ["t"],
      limits = { maxBytes: 1000 };
    const p = snapshotChangeset(s, { tables, limits });
    tables[0] = "missing";
    limits.maxBytes = 1;
    assert.equal((await p).changes, 1);
  } finally {
    s.close();
  }
});
test("pre-cancelled and mid-read cancelled snapshots reject and drain transaction", async () => {
  const s = new Target("CREATE TABLE t(id INTEGER PRIMARY KEY,v);INSERT INTO t VALUES(1,2)");
  try {
    const before = AbortSignal.abort(new Error("before"));
    await assert.rejects(snapshotChangeset(s, { tables: ["t"], signal: before }), {
      code: "ERR_FSQLITE_CAPTURE_CANCELLED",
    });
    assert.equal(s.log.length, 0);
    const controller = new AbortController();
    s.hook = (sql) => {
      if (sql.includes(" ORDER BY ")) controller.abort();
    };
    await assert.rejects(snapshotChangeset(s, { tables: ["t"], signal: controller.signal }), {
      code: "ERR_FSQLITE_CAPTURE_CANCELLED",
    });
    assert.equal(s.depth, 0);
    assert(!s.log.some(images));
  } finally {
    s.close();
  }
});
test("deadline is monotonic through awaited reads", async () => {
  const s = new Target("CREATE TABLE t(id INTEGER PRIMARY KEY,v)");
  try {
    s.hook = () => new Promise((r) => setTimeout(r, 15));
    await assert.rejects(snapshotChangeset(s, { tables: ["t"], timeoutMs: 5 }), {
      code: "ERR_FSQLITE_CAPTURE_TIMEOUT",
    });
    assert.equal(s.depth, 0);
  } finally {
    s.close();
  }
});
test("a file-backed snapshot retains one view across pages and tables despite another commit", async () => {
  const path = join(mkdtempSync(join(tmpdir(), "fsqlite-snapshot-")), "db.sqlite");
  const s = new Target(
      "PRAGMA journal_mode=WAL;CREATE TABLE t(id INTEGER PRIMARY KEY,v);CREATE TABLE u(id INTEGER PRIMARY KEY,v);INSERT INTO u VALUES(1,10)",
      path,
    ),
    writer = new DatabaseSync(path);
  try {
    for (let i = 0; i < 90; i++) s.db.prepare("INSERT INTO t VALUES(?,?)").run(i, i);
    const expected = [rows(s.db, "t"), rows(s.db, "u")];
    let wrote = false;
    s.hook = (sql) => {
      if (images(sql) && !wrote) {
        wrote = true;
        writer.exec(
          "BEGIN;UPDATE t SET v=999 WHERE id>=32;INSERT INTO t VALUES(99,99);UPDATE u SET v=999;COMMIT",
        );
      }
    };
    const result = await snapshotChangeset(s, { tables: ["t", "u"] });
    assert(wrote);
    const r = new DatabaseSync(":memory:");
    try {
      r.exec("CREATE TABLE t(id INTEGER PRIMARY KEY,v);CREATE TABLE u(id INTEGER PRIMARY KEY,v)");
      assert(r.applyChangeset(result.changeset));
      assert.deepEqual([rows(r, "t"), rows(r, "u")], expected);
    } finally {
      r.close();
    }
    assert.equal(s.db.prepare("SELECT v FROM u").get().v, 999);
  } finally {
    writer.close();
    s.close();
  }
});
test("ten-thousand-row mixed-key snapshot uses indexed continuation, not OFFSET or sorting", async () => {
  const s = new Target(
    "CREATE TABLE t(a INTEGER,b TEXT COLLATE NOCASE,v,PRIMARY KEY(a DESC,b ASC)) WITHOUT ROWID",
  );
  try {
    s.db.exec("BEGIN");
    const stmt = s.db.prepare("INSERT INTO t VALUES(?,?,?)");
    for (let i = 0; i < 10000; i++)
      stmt.run(
        i % 100,
        `key${Math.floor(i / 100)
          .toString()
          .padStart(3, "0")}`,
        i,
      );
    s.db.exec("COMMIT");
    let pages = 0;
    s.hook = (sql, params, result) => {
      if (!sql.includes(" ORDER BY ")) return;
      pages++;
      assert(result.rowArrays.length <= 32);
      assert(!sql.includes("OFFSET"));
      const plan = s.db.prepare("EXPLAIN QUERY PLAN " + sql).all(...params);
      assert(!plan.some((r) => r.detail.includes("TEMP B-TREE")), JSON.stringify(plan));
    };
    assert.equal((await snapshotChangeset(s, { tables: ["t"] })).changes, 10000);
    assert(pages > 600);
  } finally {
    s.close();
  }
});
test("snapshot result validation rejects oversized, inconsistent and repeating adapter pages", async () => {
  for (const corrupt of ["sizes", "width", "duplicate", "missing"]) {
    const s = new Target(
      "CREATE TABLE t(id INTEGER PRIMARY KEY,v);INSERT INTO t VALUES(1,2),(2,3)",
    );
    try {
      s.hook = (sql, _params, r) => {
        if (sql.startsWith("SELECT 96") && corrupt === "sizes")
          r.rowArrays = Array.from({ length: 33 }, () => [100n]);
        if (images(sql)) {
          if (corrupt === "width") r.rowArrays[0] = [];
          if (corrupt === "duplicate") r.rowArrays[1] = r.rowArrays[0];
          if (corrupt === "missing") r.rowArrays.pop();
        }
      };
      await assert.rejects(snapshotChangeset(s, { tables: ["t"] }), {
        code: "ERR_FSQLITE_CAPTURE_RESULT",
      });
    } finally {
      s.close();
    }
  }
});
test("existing mutation capture still coalesces, rolls back and leaves no TEMP artifacts", async () => {
  const s = new Target(
    "PRAGMA recursive_triggers=ON;CREATE TABLE t(id INTEGER PRIMARY KEY,v);INSERT INTO t VALUES(1,'old')",
  );
  try {
    const r = await captureChangeset(
      s,
      async (tx) => {
        await tx.execute("UPDATE t SET v='middle'");
        await tx.execute("UPDATE t SET v='last'");
        await tx.execute("INSERT INTO t VALUES(2,'gone')");
        await tx.execute("DELETE FROM t WHERE id=2");
      },
      { tables: ["t"] },
    );
    assert.equal(r.changes, 1);
    assert.equal(r.touchedRows, 2);
    assert.equal(decodeChangeset(r.changeset)[0].changes[0].operation, "update");
    await assert.rejects(
      captureChangeset(
        s,
        async (tx) => {
          await tx.execute("UPDATE t SET v='bad'");
          throw Error("rollback");
        },
        { tables: ["t"] },
      ),
      /rollback/,
    );
    assert.equal(s.db.prepare("SELECT v FROM t").get().v, "last");
    assert.equal(s.db.prepare("SELECT count(*) AS n FROM temp.sqlite_schema").get().n, 0);
  } finally {
    s.close();
  }
});

const appSchema = "PRAGMA recursive_triggers=ON;CREATE TABLE t(id INTEGER PRIMARY KEY,v);";
const outboxSchema = `CREATE TABLE ${CHANGESET_OUTBOX_TABLE} (seq INTEGER PRIMARY KEY AUTOINCREMENT, delivery_id TEXT NOT NULL UNIQUE COLLATE BINARY, sha256 TEXT NOT NULL, byte_length INTEGER NOT NULL, change_count INTEGER NOT NULL, scope TEXT NOT NULL, acknowledged INTEGER NOT NULL, payload BLOB NOT NULL)`;
const bootstrapOptions = { tables: ["t"], deliveryId: "source-42:bootstrap" };
async function deliver(outbox, target, id) {
  const message = await outbox.read(id);
  assert(message?.changeset instanceof Uint8Array);
  const result = await applyChangeset(target, message.changeset, { tables: ["t"], deliveryId: id });
  await outbox.acknowledge(id, message.delivery.sha256);
  return result;
}
function noOutbox(s) {
  assert.equal(
    s.db
      .prepare("SELECT count(*) AS n FROM main.sqlite_schema WHERE name=?")
      .get(CHANGESET_OUTBOX_TABLE).n,
    0,
  );
}

test("bootstrap persists baseline first; real SDK apply then incremental delivery converges", async () => {
  const s = new Target(appSchema + "INSERT INTO t VALUES(1,'old'),(2,'delete')"),
    r = new Target(appSchema),
    outbox = new ChangesetOutbox(s);
  try {
    const base = await outbox.bootstrap(bootstrapOptions);
    assert.equal(base.delivery.sequence, 1n);
    assert.equal(base.delivery.changes, 2);
    assert(!base.replayed);
    const change = await outbox.record(
      async (tx) => {
        await tx.execute("UPDATE t SET v='new' WHERE id=1");
        await tx.execute("DELETE FROM t WHERE id=2");
        await tx.execute("INSERT INTO t VALUES(3,'fresh')");
        return 42;
      },
      { tables: ["t"], deliveryId: "source-42:update" },
    );
    assert.equal(change.value, 42);
    assert.equal(change.delivery.sequence, 2n);
    assert.deepEqual(
      (await outbox.pending()).map((v) => v.deliveryId),
      ["source-42:bootstrap", "source-42:update"],
    );
    assert.equal((await deliver(outbox, r, base.delivery.deliveryId)).applied, 2);
    assert.equal((await deliver(outbox, r, change.delivery.deliveryId)).applied, 3);
    assert.deepEqual(rows(s.db, "t"), rows(r.db, "t"));
    assert.deepEqual(await outbox.pending(), []);
    const replay = await outbox.bootstrap(bootstrapOptions);
    assert(replay.replayed);
    assert(replay.delivery.acknowledged);
    assert.equal((await outbox.read(replay.delivery.deliveryId)).changeset, null);
  } finally {
    s.close();
    r.close();
  }
});
test("lost baseline receiver acknowledgement replays the inbox without duplicating rows", async () => {
  const s = new Target(appSchema + "INSERT INTO t VALUES(1,'seed')"),
    r = new Target(appSchema),
    outbox = new ChangesetOutbox(s);
  try {
    await outbox.bootstrap(bootstrapOptions);
    const msg = await outbox.read(bootstrapOptions.deliveryId);
    assert.equal(
      (
        await applyChangeset(r, msg.changeset, {
          tables: ["t"],
          deliveryId: msg.delivery.deliveryId,
        })
      ).replayed,
      false,
    );
    // Deliberately lose that result before source acknowledgement.
    assert.equal((await outbox.pending()).length, 1);
    const replay = await deliver(outbox, r, msg.delivery.deliveryId);
    assert(replay.replayed);
    assert.equal(replay.applied, 1);
    assert.deepEqual(rows(r.db, "t"), [[1n, "seed"]]);
  } finally {
    s.close();
    r.close();
  }
});
test("seed ID reuses original bytes after source changes; method/scope collisions never run callbacks", async () => {
  const s = new Target(appSchema + "INSERT INTO t VALUES(1,'seed')"),
    outbox = new ChangesetOutbox(s);
  try {
    const base = await outbox.bootstrap(bootstrapOptions),
      before = (await outbox.read(base.delivery.deliveryId)).changeset;
    await outbox.record((tx) => tx.execute("UPDATE t SET v='later'"), {
      tables: ["t"],
      deliveryId: "source-42:next",
    });
    s.log = [];
    const replay = await outbox.bootstrap(bootstrapOptions);
    assert(replay.replayed);
    assert(!s.log.some(images));
    assert.deepEqual((await outbox.read(base.delivery.deliveryId)).changeset, before);
    let called = false;
    await assert.rejects(
      outbox.record(
        () => {
          called = true;
        },
        { ...bootstrapOptions },
      ),
      { code: "ERR_FSQLITE_OUTBOX_REUSE" },
    );
    assert(!called);
    await assert.rejects(outbox.bootstrap({ ...bootstrapOptions, indirect: true }), {
      code: "ERR_FSQLITE_OUTBOX_REUSE",
    });
    await assert.rejects(outbox.bootstrap({ ...bootstrapOptions, tables: ["other"] }), {
      code: "ERR_FSQLITE_OUTBOX_REUSE",
    });
    await assert.rejects(outbox.bootstrap({ ...bootstrapOptions, deliveryId: "source-42:next" }), {
      code: "ERR_FSQLITE_OUTBOX_REUSE",
    });
  } finally {
    s.close();
  }
});
test("a used, acknowledged or entirely forgotten outbox cannot append a new baseline", async () => {
  const s = new Target(appSchema),
    outbox = new ChangesetOutbox(s);
  try {
    const first = await outbox.record((tx) => tx.execute("INSERT INTO t VALUES(1,'v')"), {
      tables: ["t"],
      deliveryId: "first",
    });
    await assert.rejects(outbox.bootstrap(bootstrapOptions), { code: "ERR_FSQLITE_OUTBOX_STATE" });
    await outbox.acknowledge("first", first.delivery.sha256);
    await assert.rejects(outbox.bootstrap(bootstrapOptions), { code: "ERR_FSQLITE_OUTBOX_STATE" });
    await outbox.forgetAcknowledged("first", first.delivery.sha256);
    assert.deepEqual(await outbox.pending(), []);
    await assert.rejects(outbox.bootstrap(bootstrapOptions), { code: "ERR_FSQLITE_OUTBOX_STATE" });
    assert.equal(s.db.prepare(`SELECT count(*) AS n FROM ${CHANGESET_OUTBOX_TABLE}`).get().n, 0);
  } finally {
    s.close();
  }
});
test("forgetting the seed does not authorize silently generating a replacement seed", async () => {
  const s = new Target(appSchema),
    outbox = new ChangesetOutbox(s);
  try {
    const base = await outbox.bootstrap(bootstrapOptions);
    await outbox.acknowledge(base.delivery.deliveryId, base.delivery.sha256);
    await outbox.forgetAcknowledged(base.delivery.deliveryId, base.delivery.sha256);
    await assert.rejects(outbox.bootstrap(bootstrapOptions), { code: "ERR_FSQLITE_OUTBOX_STATE" });
  } finally {
    s.close();
  }
});
test("empty bootstrap is a retained sequence boundary, followed by ordinary incremental inserts", async () => {
  const s = new Target(appSchema),
    r = new Target(appSchema),
    outbox = new ChangesetOutbox(s);
  try {
    const base = await outbox.bootstrap(bootstrapOptions);
    assert.equal(base.delivery.byteLength, 0);
    assert.equal(base.delivery.changes, 0);
    assert.equal((await deliver(outbox, r, base.delivery.deliveryId)).applied, 0);
    await outbox.record((tx) => tx.execute("INSERT INTO t VALUES(1,2)"), {
      tables: ["t"],
      deliveryId: "next",
    });
    assert.equal((await deliver(outbox, r, "next")).applied, 1);
    assert.deepEqual(rows(r.db, "t"), rows(s.db, "t"));
  } finally {
    s.close();
    r.close();
  }
});
for (const options of [
  { maxRows: 1 },
  { maxCells: 2 },
  { maxBytes: 10 },
  { limits: { maxBytes: 10 } },
])
  test(`failed bootstrap leaves no seed or created inbox: ${JSON.stringify(options)}`, async () => {
    const s = new Target(appSchema + "INSERT INTO t VALUES(1,'a'),(2,'b')"),
      outbox = new ChangesetOutbox(s);
    try {
      const before = rows(s.db, "t");
      await assert.rejects(outbox.bootstrap({ ...bootstrapOptions, ...options }));
      noOutbox(s);
      assert.deepEqual(rows(s.db, "t"), before);
      assert.equal((await outbox.bootstrap(bootstrapOptions)).delivery.sequence, 1n);
    } finally {
      s.close();
    }
  });
test("outbox capacity and cancellation reject the whole seed before publication", async () => {
  const s = new Target(appSchema + "INSERT INTO t VALUES(1,'a')");
  try {
    await assert.rejects(
      new ChangesetOutbox(s, { maxPayloadBytes: 1 }).bootstrap(bootstrapOptions),
      { code: "ERR_FSQLITE_OUTBOX_FULL" },
    );
    noOutbox(s);
    const controller = new AbortController();
    s.hook = (sql) => {
      if (images(sql)) controller.abort();
    };
    await assert.rejects(
      new ChangesetOutbox(s).bootstrap({ ...bootstrapOptions, signal: controller.signal }),
      { code: "ERR_FSQLITE_CAPTURE_CANCELLED" },
    );
    noOutbox(s);
  } finally {
    s.close();
  }
});
test("bootstrap consumes retention capacity and nested rollback removes the entire provisional boundary", async () => {
  const s = new Target(appSchema + "INSERT INTO t VALUES(1,'a')"),
    outbox = new ChangesetOutbox(s, { maxEntries: 1 });
  try {
    await assert.rejects(
      s.transaction(async () => {
        const base = await outbox.bootstrap(bootstrapOptions);
        assert.equal(base.delivery.sequence, 1n);
        throw Error("outer rollback");
      }),
      /outer rollback/,
    );
    noOutbox(s);
    const base = await outbox.bootstrap(bootstrapOptions);
    await outbox.acknowledge(base.delivery.deliveryId, base.delivery.sha256);
    let called = false;
    await assert.rejects(
      outbox.record(
        () => {
          called = true;
        },
        { tables: ["t"], deliveryId: "next" },
      ),
      { code: "ERR_FSQLITE_OUTBOX_FULL" },
    );
    assert(!called);
  } finally {
    s.close();
  }
});
test("lost source commit response recovers retained seed without a new scan", async () => {
  const s = new Target(appSchema + "INSERT INTO t VALUES(1,'seed')");
  let lose = true;
  const target = {
    transaction: async (work) => {
      const result = await s.transaction(work);
      if (lose) {
        lose = false;
        throw Error("lost source commit response");
      }
      return result;
    },
  };
  const outbox = new ChangesetOutbox(target);
  try {
    await assert.rejects(outbox.bootstrap(bootstrapOptions), /lost source commit response/);
    assert.equal((await new ChangesetOutbox(s).pending()).length, 1);
    s.log = [];
    const base = await outbox.bootstrap(bootstrapOptions);
    assert(base.replayed);
    assert(!s.log.some(images));
    assert.equal(base.delivery.sequence, 1n);
  } finally {
    s.close();
  }
});
test("two overlapping file-backed bootstraps cannot publish two baselines", async () => {
  const path = join(mkdtempSync(join(tmpdir(), "fsqlite-bootstrap-overlap-")), "db.sqlite");
  const a = new Target(
      "PRAGMA journal_mode=WAL;" + appSchema + "INSERT INTO t VALUES(1,2);" + outboxSchema,
      path,
    ),
    b = new Target("", path);
  const abox = new ChangesetOutbox(a),
    bbox = new ChangesetOutbox(b);
  let release,
    arrived = 0;
  const barrier = new Promise((r) => {
    release = r;
  });
  for (const target of [a, b])
    target.hook = async (sql) => {
      if (images(sql)) {
        if (++arrived === 2) release();
        await barrier;
      }
    };
  try {
    const results = await Promise.allSettled([
      abox.bootstrap(bootstrapOptions),
      bbox.bootstrap({ ...bootstrapOptions, deliveryId: "other-seed" }),
    ]);
    assert.equal(arrived, 2);
    assert.equal(results.filter((r) => r.status === "fulfilled").length, 1);
    assert.equal(results.filter((r) => r.status === "rejected").length, 1);
    a.hook = b.hook = undefined;
    const pending = await abox.pending();
    assert.equal(pending.length, 1);
    assert.equal(pending[0].sequence, 1n);
    const winner = pending[0].deliveryId;
    assert((await abox.bootstrap({ ...bootstrapOptions, deliveryId: winner })).replayed);
    await assert.rejects(
      bbox.bootstrap({
        ...bootstrapOptions,
        deliveryId:
          winner === bootstrapOptions.deliveryId ? "other-seed" : bootstrapOptions.deliveryId,
      }),
      { code: "ERR_FSQLITE_OUTBOX_STATE" },
    );
  } finally {
    a.close();
    b.close();
  }
});
test("an incremental writer winning during bootstrap prevents a stale baseline from committing", async () => {
  const path = join(mkdtempSync(join(tmpdir(), "fsqlite-bootstrap-writer-")), "db.sqlite");
  const a = new Target(
      "PRAGMA journal_mode=WAL;" + appSchema + "INSERT INTO t VALUES(1,2);" + outboxSchema,
      path,
    ),
    b = new Target("PRAGMA recursive_triggers=ON", path);
  const abox = new ChangesetOutbox(a),
    bbox = new ChangesetOutbox(b);
  let wrote = false;
  a.hook = async (sql) => {
    if (images(sql) && !wrote) {
      wrote = true;
      await bbox.record((tx) => tx.execute("UPDATE t SET v=3"), {
        tables: ["t"],
        deliveryId: "increment-won",
      });
    }
  };
  try {
    await assert.rejects(
      abox.bootstrap(bootstrapOptions),
      (error) => error.code === "ERR_SQLITE_ERROR",
    );
    assert(wrote);
    a.hook = undefined;
    assert.deepEqual(
      (await abox.pending()).map((d) => d.deliveryId),
      ["increment-won"],
    );
    assert.equal(await abox.read(bootstrapOptions.deliveryId), null);
    await assert.rejects(abox.bootstrap(bootstrapOptions), { code: "ERR_FSQLITE_OUTBOX_STATE" });
  } finally {
    a.close();
    b.close();
  }
});
test("bootstrap scope rejects malformed metadata and non-INSERT payloads even with matching hashes", async () => {
  const s = new Target(appSchema + "INSERT INTO t VALUES(1,2)"),
    outbox = new ChangesetOutbox(s);
  try {
    const base = await outbox.bootstrap(bootstrapOptions);
    const change = encodeChangeset([
      {
        name: "t",
        primaryKey: [1, 0],
        changes: [{ operation: "update", indirect: false, old: [1n, 2n], new: [undefined, 3n] }],
      },
    ]);
    const digest = Buffer.from(await crypto.subtle.digest("SHA-256", change)).toString("hex");
    s.db
      .prepare(`UPDATE ${CHANGESET_OUTBOX_TABLE} SET payload=?,byte_length=?,sha256=?`)
      .run(change, change.length, digest);
    await assert.rejects(outbox.read(base.delivery.deliveryId), {
      code: "ERR_FSQLITE_OUTBOX_CORRUPT",
    });
    s.db
      .prepare(`UPDATE ${CHANGESET_OUTBOX_TABLE} SET scope=?`)
      .run(JSON.stringify({ tables: ["t"], indirect: false, snapshot: false }));
    await assert.rejects(outbox.pending(), { code: "ERR_FSQLITE_OUTBOX_CORRUPT" });
  } finally {
    s.close();
  }
});
for (let seed = 1; seed <= 12; seed++)
  test(`deterministic snapshot-to-incremental convergence workload ${seed}`, async () => {
    const s = new Target(appSchema),
      r = new Target(appSchema),
      outbox = new ChangesetOutbox(s);
    let random = seed;
    const next = () => {
      random = (Math.imul(random, 1664525) + 1013904223) >>> 0;
      return random;
    };
    try {
      for (let i = 0; i < 50; i++) s.db.prepare("INSERT INTO t VALUES(?,?)").run(i, String(next()));
      await outbox.bootstrap(bootstrapOptions);
      for (let batch = 0; batch < 3; batch++)
        await outbox.record(
          async (tx) => {
            for (let j = 0; j < 20; j++) {
              const k = next() % 70;
              await tx.execute(
                "INSERT INTO t VALUES(?,?) ON CONFLICT(id) DO UPDATE SET v=excluded.v",
                [BigInt(k), String(next())],
              );
            }
          },
          { tables: ["t"], deliveryId: `change-${batch}` },
        );
      for (const d of await outbox.pending()) await deliver(outbox, r, d.deliveryId);
      assert.deepEqual(rows(r.db, "t"), rows(s.db, "t"));
    } finally {
      s.close();
      r.close();
    }
  });

async function killedBootstrap(path, cut) {
  const code = `import {DatabaseSync} from 'node:sqlite';
import {ChangesetOutbox} from ${JSON.stringify(new URL("../src/changeset-outbox.ts", import.meta.url).href)};
${Target.toString()}
const s=new Target('',process.argv[1]);const original=s.transaction.bind(s);
const hold=()=>new Promise(()=>{});
s.transaction=async work=>{
  if(process.argv[2]==='before')return original(async tx=>{await work(tx);process.send('cut');await hold();});
  const result=await original(work);process.send('cut');await hold();return result;
};
await new ChangesetOutbox(s).bootstrap(${JSON.stringify(bootstrapOptions)});`;
  const child = spawn(
    process.execPath,
    [
      "--experimental-loader",
      new URL("./helpers/source-loader.mjs", import.meta.url).pathname,
      "--input-type=module",
      "-e",
      code,
      path,
      cut,
    ],
    { stdio: ["ignore", "ignore", "pipe", "ipc"], env: process.env },
  );
  let stderr = "";
  child.stderr.on("data", (chunk) => {
    stderr += chunk;
  });
  await new Promise((resolve, reject) => {
    let atCut = false;
    const timer = setTimeout(() => {
      child.kill("SIGKILL");
      reject(Error("Child did not reach bootstrap cut: " + stderr));
    }, 15000);
    child.on("message", (message) => {
      if (message === "cut") {
        atCut = true;
        child.kill("SIGKILL");
      }
    });
    child.on("error", (error) => {
      clearTimeout(timer);
      reject(error);
    });
    child.on("exit", (code, signal) => {
      clearTimeout(timer);
      if (atCut && signal === "SIGKILL") resolve();
      else reject(Error(`Child exited ${code}/${signal}: ${stderr}`));
    });
  });
}
for (const cut of ["before", "after"])
  test(`SIGKILL ${cut} source seed commit: reopen recovers one atomic bootstrap decision`, async () => {
    const path = join(mkdtempSync(join(tmpdir(), "fsqlite-bootstrap-crash-")), "db.sqlite");
    const setup = new Target(
      "PRAGMA journal_mode=WAL;" + appSchema + "INSERT INTO t VALUES(1,'retained')",
      path,
    );
    setup.close();
    await killedBootstrap(path, cut);
    const source = new Target("", path),
      receiver = new Target(appSchema),
      outbox = new ChangesetOutbox(source);
    try {
      assert.deepEqual(rows(source.db, "t"), [[1n, "retained"]]);
      if (cut === "before") noOutbox(source);
      else assert.equal((await outbox.pending()).length, 1);
      const result = await outbox.bootstrap(bootstrapOptions);
      assert.equal(result.replayed, cut === "after");
      assert.equal(result.delivery.sequence, 1n);
      assert.equal((await deliver(outbox, receiver, result.delivery.deliveryId)).applied, 1);
      assert.deepEqual(rows(receiver.db, "t"), rows(source.db, "t"));
    } finally {
      source.close();
      receiver.close();
    }
  });

test("nested source work, seed and later captured changes commit as one ordered unit", async () => {
  const s = new Target(appSchema),
    r = new Target(appSchema),
    outbox = new ChangesetOutbox(s);
  try {
    await s.transaction(async (tx) => {
      await tx.execute("INSERT INTO t VALUES(1,'before seed')");
      await outbox.bootstrap(bootstrapOptions);
      await outbox.record((t) => t.execute("UPDATE t SET v='after seed'"), {
        tables: ["t"],
        deliveryId: "nested-after",
      });
    });
    for (const d of await outbox.pending()) await deliver(outbox, r, d.deliveryId);
    assert.deepEqual(rows(r.db, "t"), [[1n, "after seed"]]);
  } finally {
    s.close();
    r.close();
  }
});
test("receiver constraint failure rolls back the entire seed and keeps source deliveries pending", async () => {
  const s = new Target(appSchema + "INSERT INTO t VALUES(1,1),(2,100)"),
    r = new Target("CREATE TABLE t(id INTEGER PRIMARY KEY,v CHECK(v<10))"),
    outbox = new ChangesetOutbox(s);
  try {
    const seed = await outbox.bootstrap(bootstrapOptions);
    await outbox.record((tx) => tx.execute("UPDATE t SET v=2 WHERE id=1"), {
      tables: ["t"],
      deliveryId: "later",
    });
    await assert.rejects(deliver(outbox, r, seed.delivery.deliveryId));
    assert.deepEqual(rows(r.db, "t"), []);
    assert.equal((await outbox.pending()).length, 2);
    assert.equal(
      r.db
        .prepare(
          "SELECT count(*) AS n FROM sqlite_schema WHERE name='__fsqlite_changeset_receipts'",
        )
        .get().n,
      0,
    );
  } finally {
    s.close();
    r.close();
  }
});
