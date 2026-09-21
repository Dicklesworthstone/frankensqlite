import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { once } from "node:events";
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import { applyChangeset } from "../src/changeset-apply.ts";
import {
  CHANGESET_BOOTSTRAP_CHUNKS_TABLE as C,
  ChangesetBootstrapReceiver,
  createBootstrapManifest,
  CHANGESET_BOOTSTRAP_STATE_TABLE as S,
} from "../src/changeset-bootstrap.ts";
import { encodeChangeset } from "../src/changeset-codec.ts";

// Actual SQLite transactions. These are SQL-reference tests, not WASM mocks.
class Target {
  constructor(
    path = ":memory:",
    schema = "CREATE TABLE IF NOT EXISTS t(id INTEGER PRIMARY KEY, value);",
  ) {
    this.db = new DatabaseSync(path);
    this.db.exec("PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL;" + schema);
    this.depth = 0;
    this.serial = 0;
    this.sql = [];
    this.hook = null;
    this.commitHook = null;
  }
  async execute(sql, params = []) {
    this.sql.push(sql);
    const result = Number(this.db.prepare(sql).run(...params).changes);
    await this.hook?.(sql, params);
    return result;
  }
  async query(sql, params = []) {
    this.sql.push(sql);
    const q = this.db.prepare(sql);
    q.setReadBigInts(true);
    q.setReturnArrays(true);
    const rowArrays = q.all(...params);
    await this.hook?.(sql, params);
    return { rowArrays };
  }
  async transaction(work) {
    const nested = this.depth++ > 0,
      id = `nested_${++this.serial}`;
    let committed = false;
    this.db.exec(nested ? `SAVEPOINT ${id}` : "BEGIN");
    try {
      const result = await work(this);
      await this.commitHook?.("before");
      this.db.exec(nested ? `RELEASE ${id}` : "COMMIT");
      committed = true;
      await this.commitHook?.("after");
      return result;
    } catch (cause) {
      if (!committed) this.db.exec(nested ? `ROLLBACK TO ${id}; RELEASE ${id}` : "ROLLBACK");
      throw cause;
    } finally {
      this.depth--;
    }
  }
  rows(table = "t") {
    const q = this.db.prepare(`SELECT * FROM "${table}" ORDER BY 1`);
    q.setReadBigInts(true);
    q.setReturnArrays(true);
    return q.all();
  }
  close() {
    this.db.close();
  }
}
const insert = (rows, table = "t", pk = [1, 0]) =>
  encodeChangeset([
    {
      name: table,
      primaryKey: pk,
      changes: rows.map((row) => ({ operation: "insert", indirect: false, new: row })),
    },
  ]);
const chunks = () => [insert([[1n, "one"]]), insert([[2n, "two"]]), insert([[3n, "three"]])];
async function manifest(parts, tables = ["t"], extra = {}) {
  return createBootstrapManifest(
    {
      receiverId: "replica",
      deliveryId: "source:baseline",
      tables,
      chunks: parts.length,
      changes: parts.reduce((n, p) => n + countChanges(p), 0),
      byteLength: parts.reduce((n, p) => n + p.length, 0),
      ...extra,
    },
    async (i) => parts[i],
  );
}

// Native SQLite verifies codec output; the count is deliberately obtained with
// the same public decoder used by callers, not a hand-written binary parser.
import { decodeChangeset } from "../src/changeset-codec.ts";

function countChanges(b) {
  return decodeChangeset(b).reduce((n, t) => n + t.changes.length, 0);
}
const receiver = (target, extra = {}) =>
  new ChangesetBootstrapReceiver(target, {
    receiverId: "replica",
    tables: ["t"],
    confirmCommit: async () => {},
    ...extra,
  });
async function staged(target, parts = chunks(), extra = {}) {
  const m = await manifest(parts, extra.tables ?? ["t"]),
    r = receiver(target, extra);
  for (let i = 0; i < parts.length; i++) await r.stage(m, i, parts[i]);
  return { m, r, parts };
}
function noUserWrites(t) {
  return t.sql.every((sql) => !sql.startsWith('INSERT OR ABORT INTO main."t"'));
}
const isCode = (suffix) => (error) => error?.code === `ERR_FSQLITE_BOOTSTRAP_${suffix}`;
const gate = () => {
  let resolve;
  const promise = new Promise((r) => {
    resolve = r;
  });
  return { promise, resolve };
};

test("partial and complete staging do not touch application rows; one install publishes all", async () => {
  const t = new Target();
  let confirms = 0;
  try {
    const p = chunks(),
      m = await manifest(p),
      r = receiver(t, {
        confirmCommit: async () => {
          confirms++;
        },
      });
    assert.equal(await r.status(m), null);
    assert.equal(
      t.db
        .prepare("SELECT count(*) n FROM sqlite_schema WHERE name LIKE '__fsqlite_bootstrap_%'")
        .get().n,
      0,
    );
    for (let i = 0; i < p.length; i++) {
      assert.deepEqual(await r.stage(m, i, p[i]), {
        receivedChunks: i + 1,
        receivedBytes: p.slice(0, i + 1).reduce((a, b) => a + b.length, 0),
        receivedChanges: i + 1,
        installed: false,
      });
      assert.deepEqual(t.rows(), []);
      assert.equal(confirms, 0);
    }
    assert.ok(noUserWrites(t));
    const ack = await r.install(m);
    assert.equal(ack.confirmed, true);
    assert.equal(ack.replayed, false);
    assert.equal(ack.sha256, m.sha256);
    assert.deepEqual(t.rows(), [
      [1n, "one"],
      [2n, "two"],
      [3n, "three"],
    ]);
    assert.equal(confirms, 1);
    assert.equal(t.db.prepare(`SELECT sum(length(payload)) n FROM "${C}"`).get().n, 0);
    assert.equal((await r.status(m)).installed, true);
    assert.equal(
      t.db
        .prepare("SELECT count(*) n FROM sqlite_schema WHERE name='__fsqlite_changeset_receipts'")
        .get().n,
      0,
    );
  } finally {
    t.close();
  }
});
test("manifest chain is independently reproducible and binds recipient, scope, order and exact payload", async () => {
  const p = chunks(),
    m = await manifest(p);
  const sha = (v) => createHash("sha256").update(v).digest("hex");
  let h = sha(
    JSON.stringify([
      m.protocol,
      m.receiverId,
      m.deliveryId,
      m.tables,
      m.chunks,
      m.changes,
      m.byteLength,
    ]),
  );
  for (let i = 0; i < p.length; i++)
    h = sha(JSON.stringify([m.protocol, h, i, sha(p[i]), p[i].length, countChanges(p[i])]));
  assert.equal(m.sha256, h);
  for (const change of [
    { receiverId: "other" },
    { deliveryId: "other" },
    { tables: ["t", "unused"] },
  ])
    assert.notEqual((await manifest(p, change.tables ?? ["t"], change)).sha256, m.sha256);
  assert.notEqual((await manifest([...p].reverse())).sha256, m.sha256);
});
test("staging resumes after reopen; duplicate chunks do not consume quota or insert rows twice", async () => {
  const dir = await mkdtemp(join(tmpdir(), "fsqlite-stage-")),
    path = join(dir, "receiver.db");
  const p = chunks(),
    m = await manifest(p);
  let t = new Target(path);
  await receiver(t).stage(m, 0, p[0]);
  t.close();
  t = new Target(path);
  try {
    const r = receiver(t);
    assert.equal((await r.status(m)).receivedChunks, 1);
    await r.stage(m, 0, p[0]);
    await r.stage(m, 1, p[1]);
    await r.stage(m, 2, p[2]);
    await r.install(m);
    assert.equal((await r.install(m)).replayed, true);
    assert.equal((await r.stage(m, 0, p[0])).installed, true);
    assert.equal(t.rows().length, 3);
  } finally {
    t.close();
  }
});
test("missing, out-of-order and changed duplicate chunks fail without partial publication", async () => {
  const t = new Target();
  try {
    const p = chunks(),
      m = await manifest(p),
      r = receiver(t);
    await assert.rejects(r.stage(m, 1, p[1]), isCode("STATE"));
    assert.equal(await r.status(m), null);
    await r.stage(m, 0, p[0]);
    await assert.rejects(r.install(m), isCode("STATE"));
    await assert.rejects(r.stage(m, 2, p[2]), isCode("STATE"));
    await assert.rejects(r.stage(m, 0, insert([[1n, "wrong"]])), isCode("CORRUPT"));
    assert.equal((await r.status(m)).receivedChunks, 1);
    assert.deepEqual(t.rows(), []);
  } finally {
    t.close();
  }
});
test("wrong final hash rolls back only the invalid stage; correct preceding chunks remain recoverable", async () => {
  const t = new Target();
  try {
    const p = chunks(),
      m = await manifest(p),
      r = receiver(t),
      wrong = { ...m, sha256: "0".repeat(64) };
    await r.stage(wrong, 0, p[0]);
    await r.stage(wrong, 1, p[1]);
    await assert.rejects(r.stage(wrong, 2, p[2]), isCode("CORRUPT"));
    assert.equal((await r.status(wrong)).receivedChunks, 2);
    assert.deepEqual(t.rows(), []);
    await assert.rejects(r.status(m), isCode("STATE"));
  } finally {
    t.close();
  }
});
test("all selected tables must be empty, including tables with no chunk records", async () => {
  const t = new Target(
    ":memory:",
    "CREATE TABLE t(id PRIMARY KEY,value);CREATE TABLE empty(id PRIMARY KEY);INSERT INTO empty VALUES(8)",
  );
  try {
    const p = chunks(),
      m = await manifest(p, ["t", "empty"]),
      r = receiver(t, { tables: ["t", "empty"] });
    for (let i = 0; i < p.length; i++) await r.stage(m, i, p[i]);
    await assert.rejects(r.install(m), isCode("STATE"));
    assert.deepEqual(t.rows(), []);
    assert.ok(noUserWrites(t));
    assert.equal((await r.status(m)).installed, false);
  } finally {
    t.close();
  }
});
test("last-chunk unique conflict rolls back prior chunks and retains staged payloads", async () => {
  const t = new Target(":memory:", "CREATE TABLE t(id INTEGER PRIMARY KEY,value UNIQUE)");
  try {
    const { r, m } = await staged(t, [insert([[1n, "same"]]), insert([[2n, "same"]])]);
    await assert.rejects(r.install(m), /UNIQUE/);
    assert.deepEqual(t.rows(), []);
    assert.equal((await r.status(m)).installed, false);
    assert.ok(t.db.prepare(`SELECT sum(length(payload)) n FROM "${C}"`).get().n > 0);
  } finally {
    t.close();
  }
});
test("cross-table deferred constraints are evaluated at the atomic installation commit", async () => {
  const ddl =
    "CREATE TABLE t(id INTEGER PRIMARY KEY,value REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED);CREATE TABLE p(id INTEGER PRIMARY KEY,value);";
  const t = new Target(":memory:", ddl);
  try {
    const p = [insert([[1n, 7n]]), insert([[7n, "parent"]], "p")],
      { r, m } = await staged(t, p, { tables: ["t", "p"] });
    await r.install(m);
    assert.deepEqual(t.rows(), [[1n, 7n]]);
    assert.deepEqual(t.rows("p"), [[7n, "parent"]]);
  } finally {
    t.close();
  }
});
test("deferred COMMIT failure rolls back installed decision and payload reclamation", async () => {
  const ddl =
    "CREATE TABLE t(id INTEGER PRIMARY KEY,value REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED);CREATE TABLE p(id INTEGER PRIMARY KEY);";
  const t = new Target(":memory:", ddl);
  try {
    const { r, m } = await staged(t, [insert([[1n, 7n]])]);
    await assert.rejects(r.install(m), /FOREIGN KEY/);
    assert.deepEqual(t.rows(), []);
    assert.equal((await r.status(m)).installed, false);
    assert.ok(t.db.prepare(`SELECT length(payload) n FROM "${C}"`).get().n > 0);
  } finally {
    t.close();
  }
});
test("separate SQL readers never observe a partial install across tables", async () => {
  const dir = await mkdtemp(join(tmpdir(), "fsqlite-visible-")),
    path = join(dir, "db");
  const ddl =
    "CREATE TABLE IF NOT EXISTS t(id INTEGER PRIMARY KEY,value);CREATE TABLE IF NOT EXISTS p(id INTEGER PRIMARY KEY,value);";
  const t = new Target(path, ddl),
    observer = new Target(path, ddl);
  try {
    const p = [insert([[1n, "a"]]), insert([[2n, "b"]], "p")],
      { r, m } = await staged(t, p, { tables: ["t", "p"] });
    let reads = 0;
    t.hook = async (sql) => {
      if (
        sql.startsWith('INSERT OR ABORT INTO main."t"') ||
        sql.startsWith('INSERT OR ABORT INTO main."p"')
      ) {
        assert.deepEqual(observer.rows(), []);
        assert.deepEqual(observer.rows("p"), []);
        reads++;
      }
    };
    await r.install(m);
    assert.equal(reads, 2);
    assert.deepEqual(observer.rows(), [[1n, "a"]]);
    assert.deepEqual(observer.rows("p"), [[2n, "b"]]);
  } finally {
    observer.close();
    t.close();
  }
});
test("lost SQL commit response returns no receipt; replay confirms without repeating inserts", async () => {
  const t = new Target();
  let confirmations = 0;
  try {
    const { r, m } = await staged(t, chunks(), {
      confirmCommit: async () => {
        confirmations++;
      },
    });
    t.commitHook = async (phase) => {
      if (phase === "after") {
        t.commitHook = null;
        throw new Error("lost commit response");
      }
    };
    await assert.rejects(r.install(m), /lost commit/);
    assert.equal(t.rows().length, 3);
    assert.equal(confirmations, 0);
    const before = t.sql.filter((s) => s.startsWith('INSERT OR ABORT INTO main."t"')).length;
    assert.equal((await r.install(m)).replayed, true);
    assert.equal(confirmations, 1);
    assert.equal(t.sql.filter((s) => s.startsWith('INSERT OR ABORT INTO main."t"')).length, before);
  } finally {
    t.close();
  }
});
test("failed storage confirmation cannot ACK; replay reconfirms even after later user writes", async () => {
  const t = new Target();
  let calls = 0;
  try {
    const { r, m } = await staged(t, chunks(), {
      confirmCommit: async () => {
        if (++calls === 1) throw new Error("lost checkpoint");
      },
    });
    await assert.rejects(r.install(m), isCode("CONFIRM"));
    assert.equal(t.rows().length, 3);
    await t.execute("UPDATE t SET value='later' WHERE id=1");
    assert.equal((await r.install(m)).replayed, true);
    assert.equal(calls, 2);
    assert.equal(t.rows()[0][1], "later");
  } finally {
    t.close();
  }
});
test("cancel after successful SQL commit drains confirmation before rejecting and releasing admission", async () => {
  const t = new Target(),
    controller = new AbortController(),
    started = gate(),
    finish = gate();
  try {
    const { r, m } = await staged(t, chunks(), {
      confirmCommit: async () => {
        started.resolve();
        await finish.promise;
      },
    });
    t.commitHook = async (phase) => {
      if (phase === "after") {
        t.commitHook = null;
        controller.abort("cancel after commit");
      }
    };
    let settled = false;
    const work = r.install(m, { signal: controller.signal }).finally(() => {
      settled = true;
    });
    const rejected = assert.rejects(work, isCode("CANCELLED"));
    await started.promise;
    assert.equal(settled, false);
    await assert.rejects(r.status(m), isCode("BUSY"));
    finish.resolve();
    await rejected;
    assert.equal((await r.status(m)).installed, true);
  } finally {
    t.close();
  }
});
test("mid-install cancellation rolls back every row and leaves complete staging for retry", async () => {
  const t = new Target(),
    controller = new AbortController();
  try {
    const { r, m } = await staged(t);
    let n = 0;
    t.hook = async (sql) => {
      if (sql.startsWith('INSERT OR ABORT INTO main."t"') && ++n === 2) controller.abort("stop");
    };
    await assert.rejects(r.install(m, { signal: controller.signal }));
    t.hook = null;
    assert.deepEqual(t.rows(), []);
    assert.equal((await r.status(m)).installed, false);
    await r.install(m);
    assert.equal(t.rows().length, 3);
  } finally {
    t.close();
  }
});
test("monotonic deadline rejects when CPU work prevents timeout timer delivery", async () => {
  const t = new Target();
  try {
    const { r, m } = await staged(t);
    t.hook = async (sql) => {
      if (sql.startsWith('INSERT OR ABORT INTO main."t"')) {
        const end = performance.now() + 30;
        while (performance.now() < end) {}
      }
    };
    await assert.rejects(r.install(m, { timeoutMs: 10 }));
    t.hook = null;
    assert.deepEqual(t.rows(), []);
    await r.install(m);
    assert.equal(t.rows().length, 3);
  } finally {
    t.close();
  }
});
test("caller mutation after stage dispatch cannot alter stored payload or manifest", async () => {
  const t = new Target();
  try {
    const p = [insert([[1n, "good"]])],
      m = await manifest(p),
      r = receiver(t),
      mutable = { ...m, tables: [...m.tables] };
    const work = r.stage(mutable, 0, p[0]);
    p[0].fill(0);
    mutable.tables[0] = "wrong";
    mutable.deliveryId = "wrong";
    await work;
    await r.install(m);
    assert.deepEqual(t.rows(), [[1n, "good"]]);
  } finally {
    t.close();
  }
});
for (const [name, mutate] of [
  ["payload", (t) => t.db.exec(`UPDATE "${C}" SET payload=zeroblob(length(payload)) WHERE idx=1`)],
  ["digest", (t) => t.db.exec(`UPDATE "${C}" SET sha256='${"0".repeat(64)}' WHERE idx=1`)],
  ["missing chunk", (t) => t.db.exec(`DELETE FROM "${C}" WHERE idx=1`)],
  [
    "extra chunk",
    (t) =>
      t.db.exec(
        `INSERT INTO "${C}" SELECT 99,sha256,byte_length,change_count,payload FROM "${C}" WHERE idx=0`,
      ),
  ],
  ["manifest total", (t) => t.db.exec(`UPDATE "${S}" SET bytes=0`)],
  [
    "installed without complete upload",
    (t) => t.db.exec(`UPDATE "${S}" SET received=1,installed=1`),
  ],
])
  test(`damaged ${name} fails closed without partial installation`, async () => {
    const t = new Target();
    try {
      const { r, m } = await staged(t);
      mutate(t);
      await assert.rejects(r.install(m));
      assert.deepEqual(t.rows(), []);
    } finally {
      t.close();
    }
  });
for (const suffix of [
  ";CREATE INDEX unexpected ON __fsqlite_bootstrap_chunks(sha256)",
  ";CREATE TRIGGER unexpected AFTER INSERT ON __fsqlite_bootstrap_chunks BEGIN SELECT 1; END",
  ";CREATE TEMP TRIGGER unexpected AFTER INSERT ON main.t BEGIN SELECT 1; END",
]) {
  test("unexpected storage index or target/storage trigger rejects: " + suffix, async () => {
    const t = new Target();
    try {
      const { r, m } = await staged(t);
      t.db.exec(suffix);
      await assert.rejects(r.install(m), isCode("SCHEMA"));
      assert.deepEqual(t.rows(), []);
    } finally {
      t.close();
    }
  });
}
for (const [key, value] of [
  ["chunks", 0],
  ["chunks", 100001],
  ["chunks", 1.5],
  ["changes", -1],
  ["changes", 10000001],
  ["byteLength", 1073741825],
  ["tables", []],
  ["tables", ["sqlite_schema"]],
  ["tables", ["__fsqlite_bootstrap_state"]],
  ["tables", ["t", "T"]],
  ["receiverId", ""],
  ["deliveryId", "\ud800"],
]) {
  test(`invalid manifest ${key} rejects before SQL`, async () => {
    const t = new Target();
    try {
      const m = await manifest(chunks());
      await assert.rejects(receiver(t).status({ ...m, [key]: value }));
      assert.equal(t.sql.length, 0);
    } finally {
      t.close();
    }
  });
}
test("receiver bounds, routing and authorization precede payload copying or SQL", async () => {
  const t = new Target();
  try {
    const p = chunks(),
      m = await manifest(p);
    for (const options of [
      { receiverId: "other" },
      { tables: ["other"] },
      { maxChunks: 2 },
      { maxChanges: 2 },
      { maxBytes: 1 },
    ]) {
      await assert.rejects(receiver(t, options).stage(m, 0, p[0]));
      assert.equal(t.sql.length, 0);
    }
    await assert.rejects(receiver(t, { maxChunkBytes: 1 }).stage(m, 0, p[0]), isCode("LIMIT"));
    assert.equal(t.sql.length, 0);
  } finally {
    t.close();
  }
});
test("shared, resizable, detached and misleading Uint8Array subclass inputs reject or obey intrinsic bounds", async () => {
  const t = new Target();
  try {
    const p = chunks(),
      m = await manifest(p),
      r = receiver(t, { maxChunkBytes: 100 });
    for (const b of [
      new Uint8Array(new SharedArrayBuffer(10)),
      new Uint8Array(new ArrayBuffer(10, { maxByteLength: 20 })),
    ])
      await assert.rejects(r.stage(m, 0, b), isCode("INPUT"));
    const detached = new Uint8Array(10);
    structuredClone(detached, { transfer: [detached.buffer] });
    await assert.rejects(r.stage(m, 0, detached));
    class Liar extends Uint8Array {
      get byteLength() {
        return 1;
      }
      get length() {
        return 1;
      }
      *[Symbol.iterator]() {
        throw new Error("not used");
      }
    }
    await assert.rejects(r.stage(m, 0, new Liar(200)), isCode("LIMIT"));
    assert.equal(t.sql.length, 0);
  } finally {
    t.close();
  }
});
test("UPDATE and unauthorized table payloads cannot be staged as a seed", async () => {
  const t = new Target();
  try {
    const p = chunks(),
      m = await manifest(p),
      r = receiver(t);
    const update = encodeChangeset([
      {
        name: "t",
        primaryKey: [1, 0],
        changes: [{ operation: "update", indirect: false, old: [1n, "x"], new: [undefined, "y"] }],
      },
    ]);
    await assert.rejects(r.stage(m, 0, update), isCode("INPUT"));
    await assert.rejects(r.stage(m, 0, insert([[1n, "x"]], "not_allowed")), isCode("INPUT"));
    assert.equal(t.sql.length, 0);
  } finally {
    t.close();
  }
});
test("empty seed still installs atomically once and retains its decision", async () => {
  const t = new Target();
  try {
    const { r, m } = await staged(t, [new Uint8Array()]);
    assert.equal((await r.install(m)).changes, 0);
    assert.equal((await r.install(m)).replayed, true);
    assert.deepEqual(t.rows(), []);
  } finally {
    t.close();
  }
});
test("actual scalar storage classes survive atomic installation and native session comparison", async () => {
  const ddl = "CREATE TABLE t(id INTEGER PRIMARY KEY,value);",
    s = new Target(":memory:", ddl),
    t = new Target(":memory:", ddl);
  try {
    const session = s.db.createSession();
    s.db.exec(
      "INSERT INTO t VALUES(-9223372036854775808,1.0),(9223372036854775807,X'00ff'),(2,NULL)",
    );
    s.db.prepare("INSERT INTO t VALUES(?,?)").run(3, "\ufeffA\0😀");
    const p = [session.changeset()];
    session.close();
    const { r, m } = await staged(t, p);
    await r.install(m);
    const projection = (d) =>
      d.db.prepare("SELECT id,typeof(value),CAST(value AS BLOB) AS bytes FROM t ORDER BY id");
    const a = projection(s),
      b = projection(t);
    a.setReadBigInts(true);
    b.setReadBigInts(true);
    assert.deepEqual(b.all(), a.all());
  } finally {
    s.close();
    t.close();
  }
});
test("large many-chunk install keeps payload queries one chunk wide", async () => {
  const t = new Target();
  try {
    const p = Array.from({ length: 96 }, (_, i) =>
        insert([[BigInt(i), new Uint8Array(16 * 1024).fill(i)]]),
      ),
      { r, m } = await staged(t, p);
    let loaded = 0;
    t.hook = async (sql) => {
      if (sql.startsWith("SELECT payload")) {
        loaded++;
        assert.match(sql, /WHERE idx=\?/);
      }
    };
    await r.install(m);
    assert.equal(loaded, 96);
    assert.equal(t.rows().length, 96);
  } finally {
    t.close();
  }
});
for (let workload = 0; workload < 12; workload++)
  test(`deterministic native-session baseline ${workload}`, async () => {
    const s = new Target(),
      t = new Target();
    try {
      const p = [];
      for (let group = 0; group < 7; group++) {
        const session = s.db.createSession();
        for (let row = 0; row < 5; row++)
          s.db
            .prepare("INSERT INTO t VALUES(?,?)")
            .run(BigInt(group * 5 + row), `${workload}:${row}:☃`);
        p.push(session.changeset());
        session.close();
      }
      const { r, m } = await staged(t, p);
      await r.install(m);
      assert.deepEqual(t.rows(), s.rows());
      const session = s.db.createSession();
      s.db.exec("UPDATE t SET value='incremental' WHERE id%3=0;DELETE FROM t WHERE id%7=0");
      await applyChangeset(t, session.changeset(), {
        tables: ["t"],
        deliveryId: `change:${workload}`,
      });
      session.close();
      assert.deepEqual(t.rows(), s.rows());
    } finally {
      s.close();
      t.close();
    }
  });

// Separate children load only the implementation, not this test module.
for (const cut of ["mid-install", "before-commit", "after-commit", "during-confirm"])
  test(`SIGKILL ${cut}: reopen exposes all or none and retries retained decision`, async () => {
    const dir = await mkdtemp(join(tmpdir(), "fsqlite-bootstrap-kill-")),
      path = join(dir, "db");
    let target = new Target(path);
    const { m } = await staged(target);
    target.close();
    const moduleUrl = new URL("../src/changeset-bootstrap.ts", import.meta.url).href;
    const code = `import {DatabaseSync} from 'node:sqlite';
    import {ChangesetBootstrapReceiver} from ${JSON.stringify(moduleUrl)};
    ${Target.toString()}
    const t=new Target(${JSON.stringify(path)}),m=${JSON.stringify(m)};
    const hold=async()=>{process.send({cut:${JSON.stringify(cut)}});await new Promise(()=>{});};
    let inserts=0;
    t.hook=async sql=>{if(${JSON.stringify(cut)}==='mid-install'&&sql.startsWith('INSERT OR ABORT INTO main."t"')&&++inserts===2)await hold();};
    t.commitHook=async phase=>{if(${JSON.stringify(cut)}===phase+'-commit')await hold();};
    const r=new ChangesetBootstrapReceiver(t,{receiverId:'replica',tables:['t'],confirmCommit:async()=>{if(${JSON.stringify(cut)}==='during-confirm')await hold();}});
    try{await r.install(m);process.send({unexpected:'completed'});}catch(e){process.send({error:e.stack});}
  `;
    const child = spawn(
      process.execPath,
      [
        "--experimental-loader=./packages/sdk/tests/helpers/source-loader.mjs",
        "--input-type=module",
        "-e",
        code,
      ],
      { cwd: process.cwd(), env: process.env, stdio: ["ignore", "pipe", "pipe", "ipc"] },
    );
    let stderr = "";
    child.stderr.on("data", (b) => {
      stderr += b;
    });
    const timer = setTimeout(() => child.kill("SIGKILL"), 10000);
    try {
      const [message] = await once(child, "message");
      assert.deepEqual(message, { cut }, stderr);
      const exited = once(child, "exit");
      child.kill("SIGKILL");
      const [, signal] = await exited;
      assert.equal(signal, "SIGKILL");
    } finally {
      clearTimeout(timer);
      if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
    }
    target = new Target(path);
    try {
      const committed = cut === "after-commit" || cut === "during-confirm";
      assert.equal(target.rows().length, committed ? 3 : 0);
      const r = receiver(target);
      assert.equal((await r.status(m)).installed, committed);
      const receipt = await r.install(m);
      assert.equal(receipt.replayed, committed);
      assert.equal(target.rows().length, 3);
    } finally {
      target.close();
    }
  });
test("concurrent file-backed installs cannot both apply; loser replays after winner commit", async () => {
  const dir = await mkdtemp(join(tmpdir(), "fsqlite-bootstrap-race-")),
    path = join(dir, "db");
  const a = new Target(path),
    b = new Target(path),
    paused = gate(),
    resume = gate();
  try {
    const { m, r: ra } = await staged(a),
      rb = receiver(b);
    let held = false;
    a.hook = async (sql) => {
      if (!held && sql.startsWith('INSERT OR ABORT INTO main."t"')) {
        held = true;
        paused.resolve();
        await resume.promise;
      }
    };
    const running = ra.install(m);
    await paused.promise;
    await assert.rejects(rb.install(m), /locked|BUSY/i);
    assert.deepEqual(b.rows(), []);
    resume.resolve();
    await running;
    assert.equal((await rb.install(m)).replayed, true);
    assert.equal(b.rows().length, 3);
  } finally {
    resume.resolve();
    b.close();
    a.close();
  }
});
test("concurrent staging upgrades reject stale snapshots instead of corrupting progress", async () => {
  const dir = await mkdtemp(join(tmpdir(), "fsqlite-bootstrap-stage-race-")),
    path = join(dir, "db");
  const a = new Target(path),
    b = new Target(path),
    paused = gate(),
    resume = gate();
  try {
    const p = chunks(),
      m = await manifest(p),
      ra = receiver(a),
      rb = receiver(b);
    await ra.stage(m, 0, p[0]);
    let held = false;
    a.hook = async (sql) => {
      if (!held && sql.startsWith("SELECT id, CASE")) {
        held = true;
        paused.resolve();
        await resume.promise;
      }
    };
    const running = ra.stage(m, 1, p[1]);
    const rejection = assert.rejects(running, /locked|BUSY/i);
    await paused.promise;
    await rb.stage(m, 1, p[1]);
    resume.resolve();
    await rejection;
    a.hook = null;
    assert.equal((await ra.stage(m, 1, p[1])).receivedChunks, 2);
    await ra.stage(m, 2, p[2]);
    await ra.install(m);
    assert.equal(b.rows().length, 3);
  } finally {
    resume.resolve();
    b.close();
    a.close();
  }
});
