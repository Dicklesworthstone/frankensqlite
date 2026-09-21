// SQL-reference tests: execute the production job queue against Node's SQLite.
// This is not a claim of FrankenSQLite engine or browser crash certification.
// node --experimental-loader=./packages/sdk/tests/helpers/source-loader.mjs \
//   --test packages/sdk/tests/durable-jobs.test.mjs
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import { DURABLE_JOBS_TABLE, DurableJobError, DurableJobQueue } from "../src/durable-jobs.ts";

class SqlDatabase {
  tail = Promise.resolve();
  calls = 0;
  postCommitFailure = null;
  beforeStart = null;
  beforeExecute = null;
  constructor(path = ":memory:") {
    this.sql = new DatabaseSync(path);
  }
  transaction(work) {
    this.calls++;
    const result = this.tail.then(async () => {
      if (this.beforeStart) await this.beforeStart();
      this.sql.exec("BEGIN IMMEDIATE");
      let committed = false;
      try {
        const value = await work({
          execute: async (sql, params = []) => {
            this.beforeExecute?.(sql, params);
            return Number(this.sql.prepare(sql).run(...params).changes);
          },
          query: async (sql, params = []) => ({ rows: this.sql.prepare(sql).all(...params) }),
        });
        this.sql.exec("COMMIT");
        committed = true;
        if (this.postCommitFailure) {
          const error = this.postCommitFailure;
          this.postCommitFailure = null;
          throw error;
        }
        return value;
      } catch (error) {
        if (!committed) this.sql.exec("ROLLBACK");
        throw error;
      }
    });
    this.tail = result.catch(() => {});
    return result;
  }
  close() {
    this.sql.close();
  }
}

async function fixture(t, name = "work") {
  const db = new SqlDatabase();
  t.after(() => db.close());
  let now = 1000;
  const clock = () => now;
  const queue = await DurableJobQueue.open(db, name, { clock });
  return {
    db,
    queue,
    clock,
    time: (value) => {
      now = value;
    },
  };
}
const lost = (error) =>
  error instanceof DurableJobError && error.code === "ERR_FSQLITE_JOB_LEASE_LOST";
const job = (id, extra = {}) => ({ id, payload: `payload:${id}`, ...extra });

test("SQL schema installation is repeatable and jobs start ready", async (t) => {
  const { db, queue, clock } = await fixture(t);
  await DurableJobQueue.open(db, "work", { clock });
  const result = await queue.enqueue(job("one"));
  assert.equal(result.inserted, true);
  assert.equal(result.job.state, "ready");
  assert.equal(result.job.attempts, 0);
  assert.equal(result.job.maxAttempts, 3);
  assert.equal(result.job.leaseExpiresAt, null);
  assert.equal(Object.isFrozen(result.job), true);
  assert.equal((await queue.get("one")).payload, "payload:one");
  assert.equal(await queue.get("missing"), null);
});

test("stable job ids deduplicate retries without resetting delivery state", async (t) => {
  const { queue, time } = await fixture(t);
  await queue.enqueue(job("one", { availableAt: 1000 }));
  const lease = await queue.claim("worker");
  await queue.fail(lease, "retry", 500);
  time(1100);
  const duplicate = await queue.enqueue(job("one", { availableAt: 1000 }));
  assert.equal(duplicate.inserted, false);
  assert.equal(duplicate.job.attempts, 1);
  assert.equal(duplicate.job.availableAt, 1500);
  await assert.rejects(queue.enqueue(job("one", { payload: "different" })), {
    code: "ERR_FSQLITE_JOB_ID_CONFLICT",
  });
  await assert.rejects(queue.enqueue(job("one", { priority: 4 })), {
    code: "ERR_FSQLITE_JOB_ID_CONFLICT",
  });
  await assert.rejects(queue.enqueue(job("one", { maxAttempts: 7 })), {
    code: "ERR_FSQLITE_JOB_ID_CONFLICT",
  });
  await assert.rejects(queue.enqueue(job("one", { availableAt: 2000 })), {
    code: "ERR_FSQLITE_JOB_ID_CONFLICT",
  });
});

test("claim respects schedule, priority, deterministic ties, and queue isolation", async (t) => {
  const { db, queue, clock, time } = await fixture(t);
  const other = await DurableJobQueue.open(db, "other", { clock });
  await other.enqueue(job("foreign", { priority: 100 }));
  await queue.enqueue(job("later", { priority: 100, availableAt: 2000 }));
  await queue.enqueue(job("low", { priority: -1 }));
  await queue.enqueue(job("b", { priority: 10 }));
  await queue.enqueue(job("a", { priority: 10 }));
  for (const id of ["a", "b", "low"]) {
    const lease = await queue.claim("worker");
    assert.equal(lease.id, id);
    await queue.complete(lease);
  }
  assert.equal(await queue.claim("worker"), null);
  time(2000);
  assert.equal((await queue.claim("worker")).id, "later");
  assert.equal((await other.claim("worker")).id, "foreign");
});

test("claims return immutable ownership receipts and consume an attempt", async (t) => {
  const { queue } = await fixture(t);
  await queue.enqueue(job("one"));
  const lease = await queue.claim("worker", 100);
  assert.equal(lease.attempt, 1);
  assert.equal(lease.expiresAt, 1100);
  assert.equal(lease.payload, "payload:one");
  assert.equal(Object.isFrozen(lease), true);
  assert.match(lease.token, /^[0-9a-f-]{36}$/);
  assert.equal(await queue.claim("another"), null);
});

test("expired leases are reclaimed with new tokens even for the same worker id", async (t) => {
  const { queue, time } = await fixture(t);
  await queue.enqueue(job("one"));
  const first = await queue.claim("worker", 100);
  time(1100);
  const second = await queue.claim("worker", 100);
  assert.equal(second.attempt, 2);
  assert.notEqual(first.token, second.token);
  await assert.rejects(queue.complete(first), lost);
  await assert.rejects(queue.fail(first, "late"), lost);
  await assert.rejects(queue.renew(first), lost);
  await queue.complete(second, "done");
  assert.equal((await queue.get("one")).result, "done");
});

test("expiry is enforced without a competing claimant, including the exact boundary", async (t) => {
  const { queue, time } = await fixture(t);
  await queue.enqueue(job("one"));
  const lease = await queue.claim("worker", 10);
  time(1010);
  await assert.rejects(queue.complete(lease), lost);
  await assert.rejects(queue.fail(lease, "late"), lost);
  await assert.rejects(queue.renew(lease), lost);
});

test("renew extends the persisted lease but cannot shorten it", async (t) => {
  const { queue, time } = await fixture(t);
  await queue.enqueue(job("one"));
  const lease = await queue.claim("worker", 100);
  time(1050);
  const shorter = await queue.renew(lease, 10);
  assert.equal(shorter.expiresAt, 1100);
  const renewed = await queue.renew(lease, 100);
  assert.equal(renewed.expiresAt, 1150);
  assert.equal(renewed.token, lease.token);
  time(1120);
  // An old receipt's displayed deadline is not authoritative after renewal.
  await queue.complete(lease);
  assert.equal((await queue.get("one")).state, "completed");
});

test("owner, token, attempt, id, and queue each fence mutations", async (t) => {
  const { queue } = await fixture(t);
  await queue.enqueue(job("one"));
  const lease = await queue.claim("worker");
  for (const changed of [
    { owner: "other" },
    { token: crypto.randomUUID() },
    { attempt: 2 },
    { id: "missing" },
  ]) {
    await assert.rejects(queue.complete({ ...lease, ...changed }), lost);
  }
  await assert.rejects(queue.complete({ ...lease, queue: "other" }), TypeError);
  await queue.complete(lease);
});

test("retry delay and max attempts lead to a durable dead letter", async (t) => {
  const { queue, time } = await fixture(t);
  await queue.enqueue(job("one", { maxAttempts: 2 }));
  await queue.fail(await queue.claim("worker"), "first failure", 50);
  assert.equal(await queue.claim("worker"), null);
  time(1050);
  const lease = await queue.claim("worker");
  assert.equal(lease.attempt, 2);
  await queue.fail(lease, "last failure");
  const saved = await queue.get("one");
  assert.equal(saved.state, "dead");
  assert.equal(saved.lastError, "last failure");
  assert.equal(saved.owner, null);
  assert.equal(await queue.claim("worker"), null);
});

test("bounded recovery dead-letters final crashed claims and requeues other expirations", async (t) => {
  const { queue, time } = await fixture(t);
  await queue.enqueue(job("a", { maxAttempts: 1 }));
  await queue.enqueue(job("b", { maxAttempts: 2 }));
  const a = await queue.claim("worker", 10);
  const b = await queue.claim("worker", 10);
  assert.equal(await queue.reapExpired(1), 0);
  time(1010);
  assert.equal(await queue.reapExpired(1), 1);
  assert.equal((await queue.get("a")).state, "dead");
  assert.equal((await queue.get("b")).state, "leased");
  assert.equal(await queue.reapExpired(1), 1);
  assert.equal((await queue.get("b")).state, "ready");
  await assert.rejects(queue.complete(a), lost);
  await assert.rejects(queue.complete(b), lost);
  assert.equal((await queue.claim("worker")).attempt, 2);
});

test("cancellation fences running handlers and preserves terminal jobs", async (t) => {
  const { queue } = await fixture(t);
  await queue.enqueue(job("one"));
  const lease = await queue.claim("worker");
  assert.equal(await queue.cancel("one"), true);
  assert.equal(await queue.cancel("one"), false);
  await assert.rejects(queue.complete(lease), lost);
  assert.equal(await queue.claim("worker"), null);
  assert.equal(await queue.cancel("missing"), false);
  await queue.enqueue(job("done"));
  await queue.complete(await queue.claim("worker"));
  assert.equal(await queue.cancel("done"), false);
});

test("a reused job id cannot accept an earlier receipt after SQL deletion/recreation", async (t) => {
  const { db, queue } = await fixture(t);
  await queue.enqueue(job("one"));
  const old = await queue.claim("worker");
  db.sql
    .prepare(`DELETE FROM ${DURABLE_JOBS_TABLE} WHERE queue_name=? AND job_id=?`)
    .run("work", "one");
  await queue.enqueue(job("one"));
  const fresh = await queue.claim("worker");
  assert.equal(fresh.attempt, old.attempt);
  assert.notEqual(fresh.token, old.token);
  await assert.rejects(queue.complete(old), lost);
  await queue.complete(fresh);
});

test("enqueue copies caller properties before transaction admission", async (t) => {
  const { db, queue } = await fixture(t);
  let release;
  db.beforeStart = () =>
    new Promise((resolve) => {
      release = resolve;
    });
  const input = job("one");
  const pending = queue.enqueue(input);
  await Promise.resolve();
  input.id = "changed";
  input.payload = "changed";
  db.beforeStart = null;
  release();
  await pending;
  assert.equal((await queue.get("one")).payload, "payload:one");
  assert.equal(await queue.get("changed"), null);
});

test("lease clock is sampled when the transaction starts, not before queue wait", async (t) => {
  const { db, queue, time } = await fixture(t);
  await queue.enqueue(job("one"));
  let release;
  db.beforeStart = () =>
    new Promise((resolve) => {
      release = resolve;
    });
  const pending = queue.claim("worker", 100);
  await Promise.resolve();
  time(5000);
  db.beforeStart = null;
  release();
  assert.equal((await pending).expiresAt, 5100);
});

test("invalid inputs fail before starting transactions or touching SQL", async (t) => {
  const { db, queue } = await fixture(t);
  const count = db.calls;
  for (const input of [
    job(""),
    job("x", { priority: 1.5 }),
    job("x", { maxAttempts: 0 }),
    job("x", { availableAt: -1 }),
    job("x", { payload: "\u{1f600}".repeat(262145) }),
  ]) {
    await assert.rejects(queue.enqueue(input));
  }
  await assert.rejects(queue.claim("", 10));
  await assert.rejects(queue.claim("worker", 0));
  await assert.rejects(queue.claim("worker", Infinity));
  await assert.rejects(queue.reapExpired(1001));
  assert.equal(db.calls, count);
});

test("invalid clocks and overflowing deadlines roll back without consuming attempts", async (t) => {
  const { queue, time } = await fixture(t);
  await queue.enqueue(job("one"));
  time(Number.MAX_SAFE_INTEGER);
  await assert.rejects(queue.claim("worker", 1), RangeError);
  time(NaN);
  await assert.rejects(queue.claim("worker"), RangeError);
  time(1000);
  assert.equal((await queue.get("one")).attempts, 0);
});

test("SQL injection text is bound as data, including queue names and errors", async (t) => {
  const { queue } = await fixture(t, "x'; DROP TABLE users; --");
  const id = "id'); DROP TABLE x; --";
  await queue.enqueue(job(id, { payload: JSON.stringify({ text: "data'\0; -- 🚀" }) }));
  const lease = await queue.claim("worker' --");
  assert.equal(lease.id, id);
  await queue.fail(lease, "error' --");
  assert.equal((await queue.get(id)).lastError, "error' --");
});

test("100 concurrent callers of one transaction owner get distinct jobs", async (t) => {
  const { queue } = await fixture(t);
  for (let i = 0; i < 100; i++) await queue.enqueue(job(String(i)));
  const leases = await Promise.all(
    Array.from({ length: 100 }, (_, i) => queue.claim(`worker-${i}`)),
  );
  assert.equal(new Set(leases.map((lease) => lease.id)).size, 100);
  assert.equal(new Set(leases.map((lease) => lease.token)).size, 100);
  assert.equal(await queue.claim("empty"), null);
});

test("post-commit publication errors propagate unchanged and never replay a claim", async (t) => {
  const { db, queue } = await fixture(t);
  await queue.enqueue(job("one"));
  const error = new Error("checkpoint receipt lost");
  error.sqlCommitted = true;
  db.postCommitFailure = error;
  const calls = db.calls;
  await assert.rejects(queue.claim("worker"), (actual) => actual === error);
  assert.equal(db.calls, calls + 1);
  const saved = await queue.get("one");
  assert.equal(saved.state, "leased");
  assert.equal(saved.attempts, 1);
  assert.equal(await queue.claim("other"), null);
});

test("jobs, results, and outstanding leases survive a real file close/reopen", async (t) => {
  const directory = mkdtempSync(join(tmpdir(), "fsqlite-durable-jobs-"));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const path = join(directory, "jobs.sqlite");
  let db = new SqlDatabase(path);
  t.after(() => db.close());
  let now = 1000;
  const clock = () => now;
  let queue = await DurableJobQueue.open(db, "work", { clock });
  await queue.enqueue(job("done", { priority: 10 }));
  await queue.complete(await queue.claim("worker"), "stored-result");
  await queue.enqueue(job("pending"));
  const stale = await queue.claim("crashed-worker", 10);
  db.close();
  db = new SqlDatabase(path);
  queue = await DurableJobQueue.open(db, "work", { clock });
  assert.equal((await queue.get("done")).result, "stored-result");
  assert.equal(await queue.claim("restart"), null);
  now = 1010;
  const recovered = await queue.claim("restart");
  assert.equal(recovered.id, "pending");
  assert.equal(recovered.attempt, 2);
  await assert.rejects(queue.complete(stale), lost);
  await queue.complete(recovered);
  assert.deepEqual(
    db.sql
      .prepare("PRAGMA integrity_check")
      .all()
      .map((row) => row.integrity_check),
    ["ok"],
  );
});

test("enqueueWith commits application writes and the outbox entry together", async (t) => {
  const { db, queue } = await fixture(t);
  db.sql.exec("CREATE TABLE documents(id TEXT PRIMARY KEY, body TEXT NOT NULL)");
  const result = await queue.enqueueWith(job("index:one"), async (tx) => {
    await tx.execute("INSERT INTO documents VALUES (?,?)", ["one", "original"]);
    const rows = (await tx.query("SELECT body FROM documents WHERE id=?", ["one"])).rows;
    return rows[0].body;
  });
  assert.equal(result.inserted, true);
  assert.equal(result.value, "original");
  assert.equal(result.job.state, "ready");
  assert.equal(Object.isFrozen(result), true);
  assert.equal(db.sql.prepare("SELECT COUNT(*) AS n FROM documents").get().n, 1);
  assert.equal((await queue.get("index:one")).state, "ready");
});

test("enqueueWith rolls back every application write and the job on callback failure", async (t) => {
  const { db, queue } = await fixture(t);
  db.sql.exec("CREATE TABLE documents(id TEXT PRIMARY KEY, body TEXT NOT NULL)");
  const failure = new Error("application write failed");
  await assert.rejects(
    queue.enqueueWith(job("index:one"), async (tx) => {
      await tx.execute("INSERT INTO documents VALUES (?,?)", ["one", "original"]);
      throw failure;
    }),
    (error) => error === failure,
  );
  assert.equal(db.sql.prepare("SELECT COUNT(*) AS n FROM documents").get().n, 0);
  assert.equal(await queue.get("index:one"), null);
  assert.equal((await queue.enqueue(job("index:one"))).inserted, true);
});

test("enqueueWith deduplication never repeats application work, including terminal jobs", async (t) => {
  const { queue } = await fixture(t);
  let calls = 0;
  const work = async () => ++calls;
  assert.equal((await queue.enqueueWith(job("one"), work)).value, 1);
  let duplicate = await queue.enqueueWith(job("one"), work);
  assert.equal(duplicate.inserted, false);
  assert.equal(duplicate.value, undefined);
  await queue.complete(await queue.claim("worker"));
  duplicate = await queue.enqueueWith(job("one"), work);
  assert.equal(duplicate.job.state, "completed");
  assert.equal(duplicate.value, undefined);
  await assert.rejects(queue.enqueueWith(job("one", { payload: "new input" }), work), {
    code: "ERR_FSQLITE_JOB_ID_CONFLICT",
  });
  assert.equal(calls, 1);
});

test("lost enqueueWith commit receipt does not justify replaying the application callback", async (t) => {
  const { db, queue } = await fixture(t);
  db.sql.exec("CREATE TABLE effects(id TEXT PRIMARY KEY)");
  let calls = 0;
  const work = async (tx) => {
    calls++;
    await tx.execute("INSERT INTO effects VALUES (?)", ["one"]);
    return "created";
  };
  const failure = new Error("checkpoint was committed but its receipt was lost");
  failure.sqlCommitted = true;
  db.postCommitFailure = failure;
  await assert.rejects(queue.enqueueWith(job("one"), work), (error) => error === failure);
  const duplicate = await queue.enqueueWith(job("one"), work);
  assert.equal(duplicate.inserted, false);
  assert.equal(duplicate.value, undefined);
  assert.equal(calls, 1);
  assert.equal(db.sql.prepare("SELECT COUNT(*) AS n FROM effects").get().n, 1);
});

test("completeWith atomically commits application SQL, result and completion", async (t) => {
  const { db, queue } = await fixture(t);
  db.sql.exec("CREATE TABLE effects(id TEXT PRIMARY KEY, value INTEGER NOT NULL)");
  await queue.enqueue(job("one"));
  const lease = await queue.claim("worker");
  const value = await queue.completeWith(
    lease,
    async (tx) => {
      await tx.execute("INSERT INTO effects VALUES (?,?)", ["one", 17]);
      return (await tx.query("SELECT value FROM effects WHERE id=?", ["one"])).rows[0].value;
    },
    "indexed",
  );
  assert.equal(value, 17);
  const saved = await queue.get("one");
  assert.equal(saved.state, "completed");
  assert.equal(saved.result, "indexed");
  assert.equal(saved.owner, null);
  assert.equal(db.sql.prepare("SELECT value FROM effects").get().value, 17);
});

test("completeWith fences stale and expired receipts before running the callback", async (t) => {
  const { queue, time } = await fixture(t);
  await queue.enqueue(job("one"));
  const first = await queue.claim("worker", 10);
  let calls = 0;
  const work = async () => ++calls;
  time(1010);
  await assert.rejects(queue.completeWith(first, work), lost);
  const current = await queue.claim("worker", 10);
  await assert.rejects(queue.completeWith(first, work), lost);
  await queue.cancel("one");
  await assert.rejects(queue.completeWith(current, work), lost);
  assert.equal(calls, 0);
});

test("completeWith callback failure rolls back writes and leaves the current lease usable", async (t) => {
  const { db, queue, time } = await fixture(t);
  db.sql.exec("CREATE TABLE effects(id TEXT PRIMARY KEY)");
  await queue.enqueue(job("one"));
  const lease = await queue.claim("worker", 100);
  const before = await queue.get("one");
  time(1010);
  const failure = new Error("application failed");
  await assert.rejects(
    queue.completeWith(lease, async (tx) => {
      await tx.execute("INSERT INTO effects VALUES (?)", ["one"]);
      throw failure;
    }),
    (error) => error === failure,
  );
  assert.deepEqual(await queue.get("one"), before);
  assert.equal(db.sql.prepare("SELECT COUNT(*) AS n FROM effects").get().n, 0);
  await queue.completeWith(lease, async (tx) =>
    tx.execute("INSERT INTO effects VALUES (?)", ["one"]),
  );
  assert.equal((await queue.get("one")).state, "completed");
});

test("completeWith rechecks lease expiry after application SQL and undoes all effects", async (t) => {
  const { db, queue, time } = await fixture(t);
  db.sql.exec("CREATE TABLE effects(id TEXT PRIMARY KEY)");
  await queue.enqueue(job("one"));
  const lease = await queue.claim("worker", 10);
  await assert.rejects(
    queue.completeWith(lease, async (tx) => {
      await tx.execute("INSERT INTO effects VALUES (?)", ["one"]);
      time(1010);
    }),
    lost,
  );
  assert.equal(db.sql.prepare("SELECT COUNT(*) AS n FROM effects").get().n, 0);
  assert.equal((await queue.get("one")).state, "leased");
  const reclaimed = await queue.claim("other");
  assert.equal(reclaimed.attempt, 2);
  await queue.completeWith(reclaimed, async (tx) =>
    tx.execute("INSERT INTO effects VALUES (?)", ["one"]),
  );
  assert.equal(db.sql.prepare("SELECT COUNT(*) AS n FROM effects").get().n, 1);
});

test("lost completeWith commit receipt propagates and an old lease cannot repeat SQL", async (t) => {
  const { db, queue } = await fixture(t);
  db.sql.exec("CREATE TABLE effects(id TEXT PRIMARY KEY)");
  await queue.enqueue(job("one"));
  const lease = await queue.claim("worker");
  let calls = 0;
  const work = async (tx) => {
    calls++;
    await tx.execute("INSERT INTO effects VALUES (?)", ["one"]);
  };
  const failure = new Error("committed checkpoint acknowledgement lost");
  failure.sqlCommitted = true;
  db.postCommitFailure = failure;
  await assert.rejects(queue.completeWith(lease, work), (error) => error === failure);
  assert.equal((await queue.get("one")).state, "completed");
  await assert.rejects(queue.completeWith(lease, work), lost);
  assert.equal(calls, 1);
  assert.equal(db.sql.prepare("SELECT COUNT(*) AS n FROM effects").get().n, 1);
});

test("callback methods validate before transaction admission and capture lease identity", async (t) => {
  const { db, queue } = await fixture(t);
  await queue.enqueue(job("one"));
  const lease = { ...(await queue.claim("worker")) };
  const calls = db.calls;
  await assert.rejects(queue.enqueueWith(job("two"), null), TypeError);
  await assert.rejects(queue.completeWith(lease, null), TypeError);
  await assert.rejects(
    queue.completeWith(lease, async () => {}, "x".repeat(1024 * 1024 + 1)),
    RangeError,
  );
  assert.equal(db.calls, calls);
  let release;
  db.beforeStart = () =>
    new Promise((resolve) => {
      release = resolve;
    });
  const pending = queue.completeWith(lease, async () => "done");
  await Promise.resolve();
  lease.token = "caller-mutated";
  lease.id = "changed";
  db.beforeStart = null;
  release();
  assert.equal(await pending, "done");
  assert.equal((await queue.get("one")).state, "completed");
});

test("SQL diagnostics distinguish scheduled work, expired claims and terminal states", async (t) => {
  const { db, queue, clock, time } = await fixture(t);
  const zero = {
    ready: 0,
    leased: 0,
    completed: 0,
    dead: 0,
    cancelled: 0,
    available: 0,
    expired: 0,
    total: 0,
  };
  assert.deepEqual(await queue.stats(), zero);
  const other = await DurableJobQueue.open(db, "other", { clock });
  await other.enqueue(job("other"));
  for (const id of ["completed", "dead", "expired", "live"]) {
    await queue.enqueue(job(id, { maxAttempts: 1 }));
    const lease = await queue.claim("worker", id === "live" ? 100 : 10);
    if (id === "completed") await queue.complete(lease);
    if (id === "dead") await queue.fail(lease, "terminal failure");
  }
  await queue.enqueue(job("cancelled"));
  await queue.cancel("cancelled");
  await queue.enqueue(job("scheduled", { availableAt: 2000 }));
  await queue.enqueue(job("available"));
  time(1010);
  const counts = await queue.stats();
  assert.deepEqual(counts, {
    ready: 2,
    leased: 2,
    completed: 1,
    dead: 1,
    cancelled: 1,
    available: 1,
    expired: 1,
    total: 7,
  });
  assert.equal(Object.isFrozen(counts), true);
  assert.equal(await queue.reapExpired(), 1);
  assert.deepEqual(await queue.stats(), { ...counts, leased: 1, dead: 2, expired: 0 });
  assert.deepEqual(await other.stats(), { ...zero, ready: 1, available: 1, total: 1 });
});

test("SQL constraint failure rolls back application effects and the new outbox job", async (t) => {
  const { db, queue } = await fixture(t);
  db.sql.exec("CREATE TABLE effects(id TEXT PRIMARY KEY); INSERT INTO effects VALUES ('existing')");
  await assert.rejects(
    queue.enqueueWith(job("one"), async (tx) => {
      await tx.execute("INSERT INTO effects VALUES (?)", ["new"]);
      await tx.execute("INSERT INTO effects VALUES (?)", ["existing"]);
    }),
  );
  assert.deepEqual(
    db.sql
      .prepare("SELECT id FROM effects ORDER BY id")
      .all()
      .map((row) => row.id),
    ["existing"],
  );
  assert.equal(await queue.get("one"), null);
});

test("batch claims return an ordered immutable prefix in one host transaction", async (t) => {
  const { db, queue } = await fixture(t);
  for (let i = 0; i < 20; i++) await queue.enqueue(job(`job-${i}`, { priority: i }));
  const calls = db.calls;
  const batch = await queue.claimBatch("worker");
  assert.equal(db.calls, calls + 1);
  assert.equal(batch.length, 16);
  assert.deepEqual(
    batch.map((lease) => lease.id),
    Array.from({ length: 16 }, (_, i) => `job-${19 - i}`),
  );
  assert.equal(Object.isFrozen(batch), true);
  assert.equal(
    batch.every((lease) => Object.isFrozen(lease) && lease.attempt === 1),
    true,
  );
  assert.equal(new Set(batch.map((lease) => lease.token)).size, 16);
  assert.equal((await queue.claimBatch("other")).length, 4);
  assert.deepEqual(await queue.claimBatch("empty"), []);
});

test("batch payload budget counts UTF-8 bytes and leaves unclaimed attempts untouched", async (t) => {
  const { queue } = await fixture(t);
  const payload = "\u{1f600}".repeat(131072); // 512 KiB, not 256 KiB.
  for (const id of ["a", "b", "c"]) await queue.enqueue(job(id, { payload }));
  const batch = await queue.claimBatch("worker", { limit: 3, maxPayloadBytes: 1024 * 1024 });
  assert.deepEqual(
    batch.map((lease) => lease.id),
    ["a", "b"],
  );
  assert.equal(
    batch.reduce((bytes, lease) => bytes + Buffer.byteLength(lease.payload), 0),
    1024 * 1024,
  );
  assert.equal((await queue.get("c")).attempts, 0);
  assert.equal((await queue.get("c")).state, "ready");
  assert.equal((await queue.claimBatch("worker", { maxPayloadBytes: 1024 * 1024 }))[0].id, "c");
});

test("batch byte pressure stops at the priority prefix instead of skipping large jobs", async (t) => {
  const { queue } = await fixture(t);
  await queue.enqueue(job("a", { payload: "a".repeat(786432), priority: 3 }));
  await queue.enqueue(job("b", { payload: "b".repeat(524288), priority: 2 }));
  await queue.enqueue(job("c", { payload: "c".repeat(104857), priority: 1 }));
  const options = { maxPayloadBytes: 1024 * 1024 };
  assert.deepEqual(
    (await queue.claimBatch("worker", options)).map((lease) => lease.id),
    ["a"],
  );
  assert.deepEqual(
    (await queue.claimBatch("worker", options)).map((lease) => lease.id),
    ["b", "c"],
  );
});

test("batch reclaim renews tokens and fences the old receipts as one transaction", async (t) => {
  const { queue, time } = await fixture(t);
  for (const id of ["a", "b"]) await queue.enqueue(job(id));
  const old = await queue.claimBatch("worker", { leaseMs: 10 });
  time(1010);
  const fresh = await queue.claimBatch("worker", { leaseMs: 20 });
  assert.deepEqual(
    fresh.map((lease) => lease.attempt),
    [2, 2],
  );
  assert.equal(
    fresh.every((lease) => lease.expiresAt === 1030),
    true,
  );
  for (let i = 0; i < old.length; i++) {
    assert.notEqual(old[i].token, fresh[i].token);
    await assert.rejects(queue.complete(old[i]), lost);
    await queue.complete(fresh[i]);
  }
});

test("SQL failure midway through claiming rolls back the entire batch", async (t) => {
  const { db, queue } = await fixture(t);
  for (const id of ["a", "b", "c"]) await queue.enqueue(job(id));
  const failure = new Error("injected write failure");
  let updates = 0;
  db.beforeExecute = (sql) => {
    if (sql.startsWith("UPDATE") && ++updates === 2) throw failure;
  };
  await assert.rejects(queue.claimBatch("worker"), (error) => error === failure);
  db.beforeExecute = null;
  for (const id of ["a", "b", "c"]) {
    const saved = await queue.get(id);
    assert.equal(saved.state, "ready");
    assert.equal(saved.attempts, 0);
    assert.equal(saved.owner, null);
  }
  assert.equal((await queue.claimBatch("worker")).length, 3);
});

test("batch commit ambiguity propagates without replaying claims or returning partial receipts", async (t) => {
  const { db, queue } = await fixture(t);
  for (const id of ["a", "b", "c"]) await queue.enqueue(job(id));
  const failure = new Error("batch checkpoint receipt lost");
  failure.sqlCommitted = true;
  db.postCommitFailure = failure;
  const calls = db.calls;
  await assert.rejects(queue.claimBatch("worker"), (error) => error === failure);
  assert.equal(db.calls, calls + 1);
  assert.equal((await queue.stats()).leased, 3);
  assert.deepEqual(await queue.claimBatch("other"), []);
});

test("batch admission validates limits and captures options before waiting", async (t) => {
  const { db, queue } = await fixture(t);
  for (const id of ["a", "b"]) await queue.enqueue(job(id));
  const calls = db.calls;
  for (const options of [
    { limit: 0 },
    { limit: 129 },
    { limit: 1.5 },
    { leaseMs: 0 },
    { maxPayloadBytes: 1024 * 1024 - 1 },
    { maxPayloadBytes: 64 * 1024 * 1024 + 1 },
  ]) {
    await assert.rejects(queue.claimBatch("worker", options), RangeError);
  }
  assert.equal(db.calls, calls);
  let release;
  db.beforeStart = () =>
    new Promise((resolve) => {
      release = resolve;
    });
  const options = { limit: 1, leaseMs: 10 };
  const pending = queue.claimBatch("worker", options);
  await Promise.resolve();
  options.limit = 2;
  options.leaseMs = 100;
  db.beforeStart = null;
  release();
  const batch = await pending;
  assert.equal(batch.length, 1);
  assert.equal(batch[0].expiresAt, 1010);
});
