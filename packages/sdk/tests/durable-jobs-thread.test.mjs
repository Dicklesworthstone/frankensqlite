// Real competing SQLite connections and forced worker termination. This checks
// the production job protocol against SQLite, not FrankenSQLite native SSI.
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import { Worker } from "node:worker_threads";
import { DURABLE_JOBS_TABLE, DurableJobQueue } from "../src/durable-jobs.ts";

async function fixture(t) {
  const directory = mkdtempSync(join(tmpdir(), "fsqlite-jobs-thread-"));
  const path = join(directory, "jobs.sqlite");
  const sql = new DatabaseSync(path);
  const workers = [];
  t.after(async () => {
    await Promise.all(workers.map((worker) => worker.terminate()));
    sql.close();
    rmSync(directory, { recursive: true, force: true });
  });
  sql.exec("PRAGMA journal_mode = WAL; PRAGMA busy_timeout = 5000");
  sql.exec("CREATE TABLE effects(job_id TEXT PRIMARY KEY, owner TEXT NOT NULL)");
  const database = {
    async transaction(work) {
      sql.exec("BEGIN IMMEDIATE");
      try {
        const value = await work({
          execute: async (statement, params = []) =>
            Number(sql.prepare(statement).run(...params).changes),
          query: async (statement, params = []) => ({
            rows: sql.prepare(statement).all(...params),
          }),
        });
        sql.exec("COMMIT");
        return value;
      } catch (error) {
        sql.exec("ROLLBACK");
        throw error;
      }
    },
  };
  let now = 1000;
  const queue = await DurableJobQueue.open(database, "work", { clock: () => now });
  function start(mode, owner, barrier) {
    const worker = new Worker(new URL("./helpers/durable-jobs-thread.mjs", import.meta.url), {
      workerData: { mode, owner, path, barrier },
      execArgv: [
        "--experimental-loader",
        new URL("./helpers/source-loader.mjs", import.meta.url).href,
      ],
    });
    workers.push(worker);
    let readyResolve, readyReject, resultResolve, resultReject;
    let received = false;
    const ready = new Promise((resolve, reject) => {
      readyResolve = resolve;
      readyReject = reject;
    });
    const result = new Promise((resolve, reject) => {
      resultResolve = resolve;
      resultReject = reject;
    });
    // Observe early errors even before the test reaches its corresponding await.
    void ready.catch(() => {});
    void result.catch(() => {});
    const fail = (error) => {
      readyReject(error);
      resultReject(error);
    };
    worker.on("error", fail);
    worker.on("message", (message) => {
      if (message.type === "ready") readyResolve();
      if (message.type === "result" || message.type === "held") {
        received = true;
        resultResolve(message);
      }
    });
    worker.on("exit", (code) => {
      if (!received) fail(new Error(`Worker exited without a result (code ${code})`));
    });
    return { worker, ready, result };
  }
  return {
    sql,
    queue,
    start,
    time: (value) => {
      now = value;
    },
  };
}

test("four independent SQLite connections claim and complete jobs without duplicate effects", {
  timeout: 30_000,
}, async (t) => {
  const { sql, queue, start } = await fixture(t);
  for (let i = 0; i < 128; i++) await queue.enqueue({ id: `job-${i}`, payload: String(i) });
  const barrier = new SharedArrayBuffer(4);
  const running = Array.from({ length: 4 }, (_, i) => start("consume", `worker-${i}`, barrier));
  await Promise.all(running.map((worker) => worker.ready));
  Atomics.store(new Int32Array(barrier), 0, 1);
  Atomics.notify(new Int32Array(barrier), 0);
  const results = await Promise.all(running.map((worker) => worker.result));
  const ids = results.flatMap((result) => result.ids);
  assert.equal(ids.length, 128);
  assert.equal(new Set(ids).size, 128);
  assert.equal(sql.prepare("SELECT COUNT(*) AS n FROM effects").get().n, 128);
  assert.equal(sql.prepare(`SELECT SUM(attempts) AS n FROM ${DURABLE_JOBS_TABLE}`).get().n, 128);
  assert.equal((await queue.stats()).completed, 128);
  assert.deepEqual(
    sql
      .prepare("PRAGMA integrity_check")
      .all()
      .map((row) => row.integrity_check),
    ["ok"],
  );
});

test("a committed lease survives forced worker termination and is reclaimed at expiry", {
  timeout: 30_000,
}, async (t) => {
  const { queue, start, time } = await fixture(t);
  await queue.enqueue({ id: "one", payload: "one" });
  const running = start("hold-claim", "crashed");
  const { lease } = await running.result;
  await running.worker.terminate();
  assert.equal((await queue.get("one")).state, "leased");
  assert.equal(await queue.claim("restart"), null);
  time(1010);
  const reclaimed = await queue.claim("restart");
  assert.equal(reclaimed.attempt, 2);
  assert.notEqual(reclaimed.token, lease.token);
  await assert.rejects(queue.complete(lease), { code: "ERR_FSQLITE_JOB_LEASE_LOST" });
  await queue.complete(reclaimed);
});

test("termination between application SQL and completion cannot leave partial effects", {
  timeout: 30_000,
}, async (t) => {
  const { sql, queue, start, time } = await fixture(t);
  await queue.enqueue({ id: "one", payload: "one" });
  const running = start("hold-effects", "crashed");
  const { lease } = await running.result;
  // WAL readers must not see the uncommitted application effect.
  assert.equal(sql.prepare("SELECT COUNT(*) AS n FROM effects").get().n, 0);
  await running.worker.terminate();
  assert.equal(sql.prepare("SELECT COUNT(*) AS n FROM effects").get().n, 0);
  assert.equal((await queue.get("one")).state, "leased");
  time(1010);
  const reclaimed = await queue.claim("restart");
  await assert.rejects(queue.complete(lease), { code: "ERR_FSQLITE_JOB_LEASE_LOST" });
  await queue.completeWith(
    reclaimed,
    async (tx) => {
      await tx.execute("INSERT INTO effects VALUES (?,?)", [reclaimed.id, reclaimed.owner]);
    },
    "done",
  );
  assert.equal(sql.prepare("SELECT COUNT(*) AS n FROM effects").get().n, 1);
  assert.equal((await queue.get("one")).result, "done");
  assert.deepEqual(
    sql
      .prepare("PRAGMA integrity_check")
      .all()
      .map((row) => row.integrity_check),
    ["ok"],
  );
});

test("managed runners on four connections drain 128 jobs with atomic unique effects", {
  timeout: 30_000,
}, async (t) => {
  const { sql, queue, start } = await fixture(t);
  for (let i = 0; i < 128; i++) await queue.enqueue({ id: `job-${i}`, payload: String(i) });
  const barrier = new SharedArrayBuffer(4);
  const running = Array.from({ length: 4 }, (_, i) =>
    start("managed-consume", `worker-${i}`, barrier),
  );
  await Promise.all(running.map((worker) => worker.ready));
  Atomics.store(new Int32Array(barrier), 0, 1);
  Atomics.notify(new Int32Array(barrier), 0);
  const results = await Promise.all(running.map((worker) => worker.result));
  assert.equal(
    results.reduce((total, result) => total + result.stats.completed, 0),
    128,
  );
  for (const { stats } of results) {
    assert.equal(stats.activeJobs, 0);
    assert.equal(stats.pendingClaims, 0);
    assert.equal(stats.state, "stopped");
    assert.equal(stats.lostLeases, 0);
  }
  assert.equal(sql.prepare("SELECT COUNT(*) AS n FROM effects").get().n, 128);
  assert.equal(sql.prepare(`SELECT SUM(attempts) AS n FROM ${DURABLE_JOBS_TABLE}`).get().n, 128);
  assert.equal((await queue.stats()).completed, 128);
  assert.deepEqual(
    sql
      .prepare("PRAGMA integrity_check")
      .all()
      .map((row) => row.integrity_check),
    ["ok"],
  );
});

for (const mode of ["managed-hold-claim", "managed-hold-effects"]) {
  test(`forced termination during ${mode} preserves reclaimable work without partial effects`, {
    timeout: 30_000,
  }, async (t) => {
    const { sql, queue, start, time } = await fixture(t);
    await queue.enqueue({ id: "one", payload: "one" });
    const running = start(mode, "crashed");
    const { lease } = await running.result;
    assert.equal(sql.prepare("SELECT COUNT(*) AS n FROM effects").get().n, 0);
    await running.worker.terminate();
    assert.equal(sql.prepare("SELECT COUNT(*) AS n FROM effects").get().n, 0);
    assert.equal((await queue.get("one")).state, "leased");
    assert.equal(await queue.claim("restart"), null);
    time(31000);
    const reclaimed = await queue.claim("restart");
    assert.equal(reclaimed.attempt, 2);
    assert.notEqual(reclaimed.token, lease.token);
    await assert.rejects(queue.complete(lease), { code: "ERR_FSQLITE_JOB_LEASE_LOST" });
    await queue.completeWith(
      reclaimed,
      async (tx) => {
        await tx.execute("INSERT INTO effects VALUES (?,?)", [reclaimed.id, reclaimed.owner]);
      },
      "recovered",
    );
    assert.equal(sql.prepare("SELECT COUNT(*) AS n FROM effects").get().n, 1);
    assert.equal((await queue.get("one")).result, "recovered");
    assert.deepEqual(
      sql
        .prepare("PRAGMA integrity_check")
        .all()
        .map((row) => row.integrity_check),
      ["ok"],
    );
  });
}
