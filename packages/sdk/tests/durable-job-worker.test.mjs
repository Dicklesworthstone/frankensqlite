// Production runner + production job SQL against Node SQLite. This is not
// FrankenSQLite native/WASM, browser durability, or power-loss certification.
import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";
import { DurableJobWorker, DurableJobWorkerError } from "../src/durable-job-worker.ts";
import { DURABLE_JOBS_TABLE, DurableJobError, DurableJobQueue } from "../src/durable-jobs.ts";

function deferred() {
  let resolve, reject;
  const promise = new Promise((yes, no) => {
    resolve = yes;
    reject = no;
  });
  return { promise, resolve, reject };
}
async function until(check) {
  const deadline = performance.now() + 3000;
  while (!(await check())) {
    if (performance.now() >= deadline) throw new Error("Condition did not become true");
    await sleep(2);
  }
}

async function fixture(t) {
  const sql = new DatabaseSync(":memory:");
  let now = 1000;
  let tail = Promise.resolve();
  let calls = 0;
  const gates = [],
    workers = [];
  const database = {
    transaction(work) {
      calls++;
      const pending = tail.then(async () => {
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
      });
      tail = pending.catch(() => {});
      return pending;
    },
  };
  const queue = await DurableJobQueue.open(database, "work", { clock: () => now });
  t.after(async () => {
    for (const gate of gates) gate.resolve();
    await Promise.all(workers.map((worker) => worker.stop({ abort: true }).catch(() => {})));
    await tail;
    sql.close();
  });
  return {
    queue,
    sql,
    database,
    calls: () => calls,
    time: (value) => {
      now = value;
    },
    gate() {
      const gate = deferred();
      gates.push(gate);
      return gate;
    },
    start(handler, options = {}, overrides = {}) {
      const wrapped = {};
      for (const key of ["claim", "renew", "complete", "completeWith", "fail", "reapExpired"]) {
        wrapped[key] = overrides[key] ?? queue[key].bind(queue);
      }
      const worker = DurableJobWorker.start(wrapped, handler, {
        owner: "worker",
        leaseMs: 3000,
        heartbeatMs: 10,
        pollIntervalMs: 5,
        retryDelayMs: 0,
        reapIntervalMs: 1000,
        clock: () => now,
        ...options,
      });
      workers.push(worker);
      return worker;
    },
  };
}
const enqueue = (queue, id, extra = {}) =>
  queue.enqueue({ id, payload: `payload:${id}`, ...extra });

test("bounded handlers overlap without prefetching extra leases", async (t) => {
  const f = await fixture(t);
  for (let i = 0; i < 12; i++) await enqueue(f.queue, String(i));
  const gate = f.gate();
  let running = 0,
    peak = 0;
  const ids = new Set();
  const worker = f.start(
    async (lease) => {
      running++;
      peak = Math.max(peak, running);
      ids.add(lease.id);
      await gate.promise;
      running--;
      return `processed:${lease.id}`;
    },
    { concurrency: 3 },
  );
  await until(() => worker.stats.started === 3);
  assert.equal(worker.stats.activeJobs, 3);
  assert.equal(worker.stats.claimed, 3);
  assert.equal((await f.queue.stats()).leased, 3);
  assert.equal((await f.queue.stats()).ready, 9);
  gate.resolve();
  await until(() => worker.stats.completed === 12);
  await worker.stop();
  assert.equal(peak, 3);
  assert.equal(ids.size, 12);
  assert.equal(worker.stats.activeJobs, 0);
  assert.equal(worker.stats.pendingClaims, 0);
  assert.equal(worker.stats.state, "stopped");
  assert.equal(Object.isFrozen(worker.stats), true);
  assert.equal((await f.queue.get("0")).result, "processed:0");
});

test("a failing handler retries within the durable attempt limit and then dead-letters", async (t) => {
  const f = await fixture(t);
  await enqueue(f.queue, "one", { maxAttempts: 3 });
  const attempts = [];
  const worker = f.start((lease) => {
    attempts.push(lease.attempt);
    throw new Error("compute failed");
  });
  await until(() => worker.stats.failedJobs === 3);
  await worker.stop();
  assert.deepEqual(attempts, [1, 2, 3]);
  assert.equal((await f.queue.get("one")).state, "dead");
  assert.equal((await f.queue.get("one")).lastError, "compute failed");
});

test("failure delay is persisted and scheduled jobs are not run early", async (t) => {
  const f = await fixture(t);
  await enqueue(f.queue, "one");
  const worker = f.start(
    (lease) => {
      if (lease.attempt === 1) throw new Error("later");
      return "success";
    },
    { retryDelayMs: 200 },
  );
  await until(() => worker.stats.failedJobs === 1);
  assert.equal((await f.queue.get("one")).availableAt, 1200);
  assert.equal(worker.stats.started, 1);
  f.time(1200);
  await until(() => worker.stats.completed === 1);
  await worker.stop();
  assert.equal((await f.queue.get("one")).attempts, 2);
});

test("automatic heartbeat extends SQL ownership while a handler remains active", async (t) => {
  const f = await fixture(t);
  await enqueue(f.queue, "one");
  const gate = f.gate();
  const worker = f.start(
    async () => {
      await gate.promise;
      return "done";
    },
    { leaseMs: 300 },
  );
  await until(() => worker.stats.started === 1);
  f.time(1250);
  await until(async () => (await f.queue.get("one")).leaseExpiresAt === 1550);
  assert.ok(worker.stats.renewals >= 1);
  f.time(1350);
  assert.equal(await f.queue.claim("competitor"), null);
  gate.resolve();
  await until(() => worker.stats.completed === 1);
  await worker.stop();
});

test("graceful stop drains accepted work and keeps heartbeats alive", async (t) => {
  const f = await fixture(t);
  await enqueue(f.queue, "a");
  await enqueue(f.queue, "b");
  const gate = f.gate();
  let signal;
  const worker = f.start(async (_, context) => {
    signal = context.signal;
    await gate.promise;
    return "done";
  });
  await until(() => worker.stats.started === 1);
  const stopped = worker.stop();
  assert.equal(stopped, worker.stop());
  assert.equal(worker.stats.state, "draining");
  assert.equal(signal.aborted, false);
  const renewals = worker.stats.renewals;
  await until(() => worker.stats.renewals > renewals);
  assert.equal(worker.stats.claimed, 1);
  gate.resolve();
  await stopped;
  assert.equal((await f.queue.get("a")).state, "completed");
  assert.equal((await f.queue.get("b")).state, "ready");
  const calls = f.calls();
  await sleep(30);
  assert.equal(f.calls(), calls);
});

test("abort stop signals the handler and waits before releasing its lease", async (t) => {
  const f = await fixture(t);
  await enqueue(f.queue, "one");
  const cleanup = f.gate();
  let signal;
  const reason = new Error("application stopping");
  const worker = f.start(async (_, context) => {
    signal = context.signal;
    await new Promise((resolve) => signal.addEventListener("abort", resolve, { once: true }));
    await cleanup.promise;
    return "must-not-complete";
  });
  await until(() => worker.stats.started === 1);
  const stopped = worker.stop({ abort: true, reason });
  let settled = false;
  void stopped.then(() => {
    settled = true;
  });
  await sleep(5);
  assert.equal(signal.reason, reason);
  assert.equal(settled, false);
  assert.equal((await f.queue.get("one")).state, "leased");
  cleanup.resolve();
  await stopped;
  assert.equal(worker.stats.cancelledJobs, 1);
  assert.equal((await f.queue.get("one")).state, "ready");
  assert.equal((await f.queue.get("one")).result, null);
});

test("draining can escalate to abort without replacing the join promise", async (t) => {
  const f = await fixture(t);
  await enqueue(f.queue, "one");
  const worker = f.start(async (_, { signal }) => {
    await new Promise((resolve) => signal.addEventListener("abort", resolve, { once: true }));
  });
  await until(() => worker.stats.started === 1);
  const drain = worker.stop();
  assert.equal(drain, worker.stop({ abort: true }));
  await drain;
  assert.equal(worker.stats.cancelledJobs, 1);
});

test("an already aborted signal admits no SQL and no handler", async (t) => {
  const f = await fixture(t);
  const controller = new AbortController();
  controller.abort("before start");
  const before = f.calls();
  const worker = f.start(() => assert.fail("handler ran"), { signal: controller.signal });
  await worker.done;
  assert.equal(f.calls(), before);
  assert.equal(worker.stats.state, "stopped");
});

test("external abort wakes a long idle poll and releases its listener", async (t) => {
  const f = await fixture(t);
  const controller = new AbortController();
  let adds = 0,
    removes = 0;
  const signal = controller.signal;
  const add = signal.addEventListener.bind(signal),
    remove = signal.removeEventListener.bind(signal);
  signal.addEventListener = (...args) => {
    adds++;
    return add(...args);
  };
  signal.removeEventListener = (...args) => {
    removes++;
    return remove(...args);
  };
  const worker = f.start(() => assert.fail("handler ran"), {
    signal,
    pollIntervalMs: 2_147_483_647,
  });
  await until(() => f.calls() >= 3);
  controller.abort();
  await worker.done;
  assert.equal(adds, 1);
  assert.equal(removes, 1);
});

test("graceful stop during a pending claim drains that claim exactly once", async (t) => {
  const f = await fixture(t);
  await enqueue(f.queue, "one");
  const gate = f.gate();
  let admitted = false;
  const worker = f.start(
    () => "done",
    {},
    {
      claim: async (...args) => {
        admitted = true;
        const lease = await f.queue.claim(...args);
        await gate.promise;
        return lease;
      },
    },
  );
  await until(() => admitted);
  const stopped = worker.stop();
  assert.equal(worker.stats.pendingClaims, 1);
  gate.resolve();
  await stopped;
  assert.equal(worker.stats.completed, 1);
  assert.equal(worker.stats.started, 1);
});

test("abort during a pending claim never starts its handler and joins release", async (t) => {
  const f = await fixture(t);
  await enqueue(f.queue, "one");
  const gate = f.gate();
  let admitted = false;
  const worker = f.start(
    () => assert.fail("handler ran"),
    {},
    {
      claim: async (...args) => {
        const lease = await f.queue.claim(...args);
        admitted = true;
        await gate.promise;
        return lease;
      },
    },
  );
  await until(() => admitted);
  const stopped = worker.stop({ abort: true });
  gate.resolve();
  await stopped;
  assert.equal(worker.stats.started, 0);
  assert.equal(worker.stats.cancelledJobs, 1);
  assert.equal((await f.queue.get("one")).state, "ready");
});

test("lease loss aborts and drains a handler without completing or failing the new owner", async (t) => {
  const f = await fixture(t);
  await enqueue(f.queue, "one");
  const cleanup = f.gate();
  let signal;
  let completed = 0,
    failed = 0;
  const worker = f.start(
    async (_, context) => {
      signal = context.signal;
      await new Promise((resolve) => signal.addEventListener("abort", resolve, { once: true }));
      await cleanup.promise;
      return "stale";
    },
    { leaseMs: 300 },
    {
      complete: async (...args) => {
        completed++;
        return f.queue.complete(...args);
      },
      fail: async (...args) => {
        failed++;
        return f.queue.fail(...args);
      },
    },
  );
  await until(() => worker.stats.started === 1);
  f.time(1300);
  const newer = await f.queue.claim("replacement");
  await until(() => signal.aborted);
  assert.equal(worker.stats.lostLeases, 1);
  const stopped = worker.stop();
  cleanup.resolve();
  await stopped;
  assert.equal(completed, 0);
  assert.equal(failed, 0);
  await f.queue.complete(newer, "current");
  assert.equal((await f.queue.get("one")).result, "current");
});

test("completion waits for an in-flight heartbeat before touching SQL again", async (t) => {
  const f = await fixture(t);
  await enqueue(f.queue, "one");
  const work = f.gate(),
    renew = f.gate();
  let renewing = false,
    completed = 0;
  const worker = f.start(
    async () => {
      await work.promise;
      return "done";
    },
    {},
    {
      renew: async (...args) => {
        renewing = true;
        await renew.promise;
        return f.queue.renew(...args);
      },
      complete: async (...args) => {
        completed++;
        return f.queue.complete(...args);
      },
    },
  );
  await until(() => renewing);
  work.resolve();
  const stopped = worker.stop();
  await sleep(5);
  assert.equal(completed, 0);
  renew.resolve();
  await stopped;
  assert.equal(completed, 1);
});

for (const phase of ["claim", "renew", "complete", "fail", "reapExpired"]) {
  test(`storage failure in ${phase} stops without replaying operations or handlers`, async (t) => {
    const f = await fixture(t);
    await enqueue(f.queue, "one");
    const cause = new Error(`ambiguous ${phase}`);
    cause.sqlCommitted = true;
    let invoked = 0,
      handlers = 0;
    const worker = f.start(
      async (_, { signal }) => {
        handlers++;
        if (phase === "renew")
          await new Promise((resolve) => signal.addEventListener("abort", resolve, { once: true }));
        if (phase === "fail") throw new Error("handler failed");
        return "done";
      },
      {},
      {
        [phase]: async () => {
          invoked++;
          throw cause;
        },
      },
    );
    await assert.rejects(
      worker.done,
      (error) =>
        error instanceof DurableJobWorkerError &&
        error.cause === cause &&
        error.phase === (phase === "reapExpired" ? "reap" : phase),
    );
    assert.equal(invoked, 1);
    assert.equal(handlers, ["claim", "reapExpired"].includes(phase) ? 0 : 1);
    assert.equal(worker.stats.state, "failed");
    assert.equal(worker.stats.activeJobs, 0);
    assert.equal(worker.stats.pendingClaims, 0);
    assert.equal(worker.stats.failedJobs, 0);
    assert.equal(worker.stats.completed, 0);
    assert.equal(worker.stats.cancelledJobs, 0);
  });
}

test("lost completion acknowledgement does not turn a committed job into a retry", async (t) => {
  const f = await fixture(t);
  await enqueue(f.queue, "one");
  const cause = new Error("checkpoint acknowledgement lost");
  cause.sqlCommitted = true;
  let fails = 0;
  const worker = f.start(
    () => "done",
    {},
    {
      complete: async (...args) => {
        await f.queue.complete(...args);
        throw cause;
      },
      fail: async (...args) => {
        fails++;
        return f.queue.fail(...args);
      },
    },
  );
  await assert.rejects(worker.done, (error) => error.cause === cause);
  assert.equal((await f.queue.get("one")).state, "completed");
  assert.equal((await f.queue.get("one")).result, "done");
  assert.equal(fails, 0);
  assert.equal(worker.stats.completed, 0);
});

test("one fatal operation aborts sibling handlers and joins their cleanup", async (t) => {
  const f = await fixture(t);
  await enqueue(f.queue, "a");
  await enqueue(f.queue, "b");
  const releaseA = f.gate(),
    cleanupB = f.gate();
  let abortedB = false;
  const cause = new Error("unknown completion outcome");
  const worker = f.start(
    async (lease, { signal }) => {
      if (lease.id === "a") {
        await releaseA.promise;
        return "a";
      }
      await new Promise((resolve) =>
        signal.addEventListener(
          "abort",
          () => {
            abortedB = true;
            resolve();
          },
          { once: true },
        ),
      );
      await cleanupB.promise;
      return "b";
    },
    { concurrency: 2 },
    {
      complete: async () => {
        throw cause;
      },
    },
  );
  await until(() => worker.stats.started === 2);
  releaseA.resolve();
  await until(() => abortedB);
  let settled = false;
  void worker.done.catch(() => {
    settled = true;
  });
  await sleep(5);
  assert.equal(settled, false);
  cleanupB.resolve();
  await assert.rejects(worker.done, (error) => error.cause === cause);
  assert.equal(worker.stats.activeJobs, 0);
  assert.equal((await f.queue.get("b")).state, "leased");
});

test("startup and periodic bounded reaping recover expired final attempts", async (t) => {
  const f = await fixture(t);
  for (const id of ["a", "b", "c"]) {
    await enqueue(f.queue, id, { maxAttempts: 1 });
    await f.queue.claim("crashed", 10);
  }
  f.time(1010);
  const limits = [];
  const worker = f.start(
    () => assert.fail("exhausted work ran"),
    { reapLimit: 1, reapIntervalMs: 5 },
    {
      reapExpired: async (limit) => {
        limits.push(limit);
        return f.queue.reapExpired(limit);
      },
    },
  );
  await until(() => worker.stats.reapedLeases === 3);
  await worker.stop();
  assert.ok(limits.every((limit) => limit === 1));
  assert.equal((await f.queue.stats()).dead, 3);
});

test("invalid handler results consume attempts but never enter completion", async (t) => {
  const f = await fixture(t);
  await enqueue(f.queue, "invalid", { maxAttempts: 1 });
  await enqueue(f.queue, "large", { maxAttempts: 1 });
  const worker = f.start((lease) => (lease.id === "invalid" ? {} : "😀".repeat(262145)));
  await until(() => worker.stats.failedJobs === 2);
  await worker.stop();
  assert.equal((await f.queue.stats()).dead, 2);
  assert.equal(worker.stats.completed, 0);
});

test("policy is captured and invalid input starts no SQL", async (t) => {
  const f = await fixture(t);
  const base = { owner: "worker" };
  for (const bad of [
    { owner: "" },
    { owner: "x\0y" },
    { concurrency: 65 },
    { concurrency: 0 },
    { leaseMs: 2 },
    { leaseMs: Infinity },
    { heartbeatMs: 10001 },
    { pollIntervalMs: 0 },
    { retryDelayMs: -1 },
    { reapLimit: 1001 },
    { signal: {} },
  ]) {
    assert.throws(() => DurableJobWorker.start(f.queue, () => {}, { ...base, ...bad }));
  }
  const before = f.calls();
  await sleep(1);
  assert.equal(f.calls(), before);
  const options = { owner: "captured", concurrency: 1, pollIntervalMs: 5 };
  const worker = DurableJobWorker.start(f.queue, () => {}, options);
  options.concurrency = 64;
  options.owner = "";
  assert.equal(worker.stats.concurrency, 1);
  await worker.stop();
});

test("successful immediate handlers yield so application timers can stop intake", async (t) => {
  const f = await fixture(t);
  for (let i = 0; i < 100; i++) await enqueue(f.queue, String(i));
  const worker = f.start(() => "done");
  setTimeout(() => {
    void worker.stop();
  }, 0);
  await worker.done;
  assert.ok(worker.stats.completed < 100);
});

test("all 64 slots can stop idle without retaining timers or polling again", async (t) => {
  const f = await fixture(t);
  const worker = f.start(() => assert.fail("unexpected job"), {
    concurrency: 64,
    pollIntervalMs: 100000,
  });
  await until(() => f.calls() >= 66);
  await worker.stop();
  const calls = f.calls();
  await sleep(10);
  assert.equal(f.calls(), calls);
  assert.equal(worker.stats.activeJobs, 0);
  assert.equal(worker.stats.pendingClaims, 0);
  assert.equal(f.sql.prepare(`SELECT COUNT(*) AS n FROM ${DURABLE_JOBS_TABLE}`).get().n, 0);
});

test("a delayed claim receipt cannot start a handler after its lease deadline", async (t) => {
  const f = await fixture(t);
  await enqueue(f.queue, "one");
  const receipt = f.gate();
  let saved = false;
  const worker = f.start(
    () => assert.fail("expired handler started"),
    { leaseMs: 300 },
    {
      claim: async (...args) => {
        const lease = await f.queue.claim(...args);
        saved = true;
        await receipt.promise;
        return lease;
      },
    },
  );
  await until(() => saved);
  const stopped = worker.stop();
  f.time(1300);
  receipt.resolve();
  await stopped;
  assert.equal(worker.stats.started, 0);
  assert.equal(worker.stats.lostLeases, 1);
  assert.equal(worker.stats.completed, 0);
});

test("expiry aborts a handler even while its renewal is stuck, and stop joins that renewal", async (t) => {
  const f = await fixture(t);
  await enqueue(f.queue, "one");
  const receipt = f.gate();
  let renewing = false,
    signal;
  const worker = f.start(
    async (_, context) => {
      signal = context.signal;
      await new Promise((resolve) => signal.addEventListener("abort", resolve, { once: true }));
    },
    { leaseMs: 300 },
    {
      renew: async (...args) => {
        renewing = true;
        await receipt.promise;
        return f.queue.renew(...args);
      },
    },
  );
  await until(() => renewing);
  f.time(1300);
  await until(() => signal.aborted);
  assert.equal(signal.reason.code, "ERR_FSQLITE_JOB_LEASE_LOST");
  const stopped = worker.stop();
  let settled = false;
  void stopped.then(() => {
    settled = true;
  });
  await sleep(5);
  assert.equal(settled, false);
  receipt.resolve();
  await stopped;
  assert.equal(worker.stats.lostLeases, 1);
  assert.equal(worker.stats.completed, 0);
  assert.equal(worker.stats.failedJobs, 0);
});

test("a late successful renewal receipt cannot resurrect an expired handler", async (t) => {
  const f = await fixture(t);
  await enqueue(f.queue, "one");
  const receipt = f.gate();
  let renewed = false,
    signal;
  const worker = f.start(
    async (_, context) => {
      signal = context.signal;
      await new Promise((resolve) => signal.addEventListener("abort", resolve, { once: true }));
      return "stale";
    },
    { leaseMs: 300 },
    {
      renew: async (...args) => {
        f.time(1200);
        const lease = await f.queue.renew(...args);
        renewed = true;
        await receipt.promise;
        return lease;
      },
    },
  );
  await until(() => renewed);
  assert.equal((await f.queue.get("one")).leaseExpiresAt, 1500);
  f.time(1300);
  await until(() => signal.aborted);
  const stopped = worker.stop();
  receipt.resolve();
  await stopped;
  assert.equal(worker.stats.lostLeases, 1);
  assert.equal(worker.stats.completed, 0);
  assert.equal((await f.queue.get("one")).state, "leased");
});

test("backward wall-clock jumps cannot extend the monotonic acknowledged lease budget", async (t) => {
  const f = await fixture(t);
  await enqueue(f.queue, "one");
  const renewal = f.gate();
  let renewing = false,
    signal;
  const worker = f.start(
    async (_, context) => {
      signal = context.signal;
      await new Promise((resolve) => signal.addEventListener("abort", resolve, { once: true }));
    },
    { leaseMs: 90, heartbeatMs: 5 },
    {
      renew: async (...args) => {
        renewing = true;
        await renewal.promise;
        return f.queue.renew(...args);
      },
    },
  );
  await until(() => renewing);
  f.time(900);
  await until(() => signal.aborted);
  const stopped = worker.stop();
  renewal.resolve();
  await stopped;
  assert.equal(worker.stats.lostLeases, 1);
});

test("cooperative checkpoints detect expiry without waiting for any timer callback", async (t) => {
  const f = await fixture(t);
  await enqueue(f.queue, "one");
  let caught;
  const worker = f.start((_, context) => {
    f.time(4000);
    try {
      context.checkpoint();
    } catch (error) {
      caught = error;
    }
    void worker.stop();
    return "must-not-complete";
  });
  await worker.done;
  assert.ok(caught instanceof DurableJobError);
  assert.equal(caught.code, "ERR_FSQLITE_JOB_LEASE_LOST");
  assert.equal(worker.stats.lostLeases, 1);
  assert.equal(worker.stats.completed, 0);
});

test("mismatched renewal acknowledgements stop rather than acquiring another identity", async (t) => {
  const f = await fixture(t);
  await enqueue(f.queue, "one");
  const worker = f.start(
    async (_, { signal }) => {
      await new Promise((resolve) => signal.addEventListener("abort", resolve, { once: true }));
    },
    {},
    { renew: async (...args) => ({ ...(await f.queue.renew(...args)), token: "wrong-token" }) },
  );
  await assert.rejects(
    worker.done,
    (error) => error.phase === "lease" && error.cause instanceof TypeError,
  );
  assert.equal(worker.stats.completed, 0);
});

test("invalid clocks stop supervision without throwing from detached timer callbacks", async (t) => {
  const f = await fixture(t);
  await enqueue(f.queue, "one");
  const worker = f.start(async (_, { signal }) => {
    f.time(NaN);
    await new Promise((resolve) => signal.addEventListener("abort", resolve, { once: true }));
  });
  await assert.rejects(
    worker.done,
    (error) => error.phase === "lease" && error.cause instanceof RangeError,
  );
  assert.equal(worker.stats.activeJobs, 0);
});

test("managed completion commits computed application effects and the job result atomically", async (t) => {
  const f = await fixture(t);
  f.sql.exec("CREATE TABLE effects(id TEXT PRIMARY KEY, value TEXT)");
  await enqueue(f.queue, "one");
  const worker = f.start(async (lease, context) => {
    context.checkpoint();
    const computed = lease.payload.toUpperCase();
    return {
      result: "indexed",
      apply: async (tx, applying) => {
        applying.checkpoint();
        await tx.execute("INSERT INTO effects VALUES (?, ?)", [lease.id, computed]);
      },
    };
  });
  await until(() => worker.stats.completed === 1);
  await worker.stop();
  assert.equal(f.sql.prepare("SELECT value FROM effects").get().value, "PAYLOAD:ONE");
  assert.equal((await f.queue.get("one")).result, "indexed");
  assert.equal((await f.queue.get("one")).state, "completed");
});

test("SQL application failure rolls back effects and stops without rerunning computation", async (t) => {
  const f = await fixture(t);
  f.sql.exec("CREATE TABLE effects(id TEXT PRIMARY KEY)");
  await enqueue(f.queue, "one");
  let computations = 0,
    applications = 0;
  const worker = f.start(() => {
    computations++;
    return {
      apply: async (tx) => {
        applications++;
        await tx.execute("INSERT INTO effects VALUES (?)", ["one"]);
        await tx.execute("INSERT INTO effects VALUES (?)", ["one"]);
      },
    };
  });
  await assert.rejects(worker.done, (error) => error.phase === "complete");
  assert.equal(computations, 1);
  assert.equal(applications, 1);
  assert.equal(f.sql.prepare("SELECT COUNT(*) AS n FROM effects").get().n, 0);
  assert.equal((await f.queue.get("one")).state, "leased");
  assert.equal(worker.stats.failedJobs, 0);
});

test("expiry during SQL application rolls back every write and job completion", async (t) => {
  const f = await fixture(t);
  f.sql.exec("CREATE TABLE effects(id TEXT PRIMARY KEY)");
  await enqueue(f.queue, "one");
  const worker = f.start((lease) => ({
    result: "no",
    apply: async (tx) => {
      await tx.execute("INSERT INTO effects VALUES (?)", [lease.id]);
      f.time(4000);
      void worker.stop();
    },
  }));
  await worker.done;
  assert.equal(f.sql.prepare("SELECT COUNT(*) AS n FROM effects").get().n, 0);
  assert.equal((await f.queue.get("one")).state, "leased");
  assert.equal(worker.stats.lostLeases, 1);
});

test("abort during SQL application drains rollback rather than acknowledging partial effects", async (t) => {
  const f = await fixture(t);
  f.sql.exec("CREATE TABLE effects(id TEXT PRIMARY KEY)");
  await enqueue(f.queue, "one");
  let applying = false;
  const reason = new Error("stop during SQL application");
  const worker = f.start((lease) => ({
    apply: async (tx, { signal }) => {
      await tx.execute("INSERT INTO effects VALUES (?)", [lease.id]);
      applying = true;
      await new Promise((resolve) => signal.addEventListener("abort", resolve, { once: true }));
    },
  }));
  await until(() => applying);
  await assert.rejects(
    worker.stop({ abort: true, reason }),
    (error) => error.phase === "complete" && error.cause === reason,
  );
  assert.equal(f.sql.prepare("SELECT COUNT(*) AS n FROM effects").get().n, 0);
  assert.equal((await f.queue.get("one")).state, "leased");
  assert.equal(worker.stats.activeJobs, 0);
});

test("lost atomic completion receipt preserves committed effects without replay", async (t) => {
  const f = await fixture(t);
  f.sql.exec("CREATE TABLE effects(id TEXT PRIMARY KEY)");
  await enqueue(f.queue, "one");
  const cause = new Error("atomic checkpoint acknowledgement lost");
  cause.sqlCommitted = true;
  let computations = 0,
    applications = 0;
  const worker = f.start(
    (lease) => {
      computations++;
      return {
        result: "done",
        apply: async (tx) => {
          applications++;
          await tx.execute("INSERT INTO effects VALUES (?)", [lease.id]);
        },
      };
    },
    {},
    {
      completeWith: async (...args) => {
        await f.queue.completeWith(...args);
        throw cause;
      },
    },
  );
  await assert.rejects(worker.done, (error) => error.phase === "complete" && error.cause === cause);
  assert.equal(computations, 1);
  assert.equal(applications, 1);
  assert.equal(f.sql.prepare("SELECT COUNT(*) AS n FROM effects").get().n, 1);
  assert.equal((await f.queue.get("one")).state, "completed");
  assert.equal(worker.stats.completed, 0);
  assert.equal(worker.stats.failedJobs, 0);
});

test("completion callbacks and results are captured before queued transaction admission", async (t) => {
  const f = await fixture(t);
  f.sql.exec("CREATE TABLE effects(value TEXT)");
  await enqueue(f.queue, "one");
  const gate = f.gate();
  let admitted = false;
  const completion = {
    result: "original",
    apply: async (tx) => tx.execute("INSERT INTO effects VALUES (?)", ["original"]),
  };
  const worker = f.start(
    () => completion,
    {},
    {
      completeWith: async (...args) => {
        admitted = true;
        await gate.promise;
        return f.queue.completeWith(...args);
      },
    },
  );
  await until(() => admitted);
  const stopped = worker.stop();
  completion.result = "changed";
  completion.apply = () => assert.fail("mutated callback ran");
  gate.resolve();
  await stopped;
  assert.equal((await f.queue.get("one")).result, "original");
  assert.equal(f.sql.prepare("SELECT value FROM effects").get().value, "original");
});

for (const wrap of [
  (error) => error,
  (error) => new Error("wrapped", { cause: error }),
  (error) => new AggregateError([new Error("other"), error], "aggregate"),
]) {
  test("explicit uncertain application commit errors are never recorded as retryable handler failures", async (t) => {
    const f = await fixture(t);
    await enqueue(f.queue, "one");
    const committed = new Error("committed checkpoint failed");
    committed.sqlCommitted = true;
    const cause = wrap(committed);
    const worker = f.start(() => {
      throw cause;
    });
    await assert.rejects(
      worker.done,
      (error) => error.phase === "handler" && error.cause === cause,
    );
    assert.equal(worker.stats.started, 1);
    assert.equal(worker.stats.failedJobs, 0);
    assert.equal((await f.queue.get("one")).state, "leased");
  });
}

test("unreadable outcome getters fail closed without invoking application getters", async (t) => {
  const f = await fixture(t);
  await enqueue(f.queue, "one");
  let read = false;
  const cause = new Error("unreadable");
  Object.defineProperty(cause, "sqlCommitted", {
    get() {
      read = true;
      return false;
    },
  });
  const worker = f.start(() => {
    throw cause;
  });
  await assert.rejects(worker.done, (error) => error.phase === "handler" && error.cause === cause);
  assert.equal(read, false);
  assert.equal(worker.stats.failedJobs, 0);
});

test("finite draining joins every slot and atomic completion before done resolves", async (t) => {
  const f = await fixture(t);
  f.sql.exec("CREATE TABLE effects(id TEXT PRIMARY KEY)");
  for (let i = 0; i < 17; i++) await enqueue(f.queue, String(i));
  const worker = f.start(
    (lease) => ({ apply: (tx) => tx.execute("INSERT INTO effects VALUES (?)", [lease.id]) }),
    { concurrency: 4, stopWhenIdle: true },
  );
  await worker.done;
  assert.equal(worker.stats.completed, 17);
  assert.equal(worker.stats.state, "stopped");
  assert.equal(worker.stats.pendingClaims, 0);
  assert.equal(worker.stats.activeJobs, 0);
  assert.equal(f.sql.prepare("SELECT COUNT(*) AS n FROM effects").get().n, 17);
});

test("finite draining of an empty queue finishes without leaving a reaper behind", async (t) => {
  const f = await fixture(t);
  const worker = f.start(() => assert.fail("handler ran"), { concurrency: 8, stopWhenIdle: true });
  await worker.done;
  const calls = f.calls();
  await sleep(20);
  assert.equal(f.calls(), calls);
  assert.equal(worker.stats.started, 0);
});

test("finite draining preserves future schedules instead of waiting forever for them", async (t) => {
  const f = await fixture(t);
  await enqueue(f.queue, "future", { availableAt: 10000 });
  const worker = f.start(() => assert.fail("future work ran"), { stopWhenIdle: true });
  await worker.done;
  assert.equal((await f.queue.get("future")).state, "ready");
  assert.equal((await f.queue.get("future")).attempts, 0);
});

test("finite draining can exhaust immediate retries without orphaning handlers", async (t) => {
  const f = await fixture(t);
  await enqueue(f.queue, "one", { maxAttempts: 2 });
  const worker = f.start(
    () => {
      throw new Error("failed");
    },
    { stopWhenIdle: true, retryDelayMs: 0 },
  );
  await worker.done;
  assert.equal(worker.stats.failedJobs, 2);
  assert.equal((await f.queue.get("one")).state, "dead");
});

test("retained contexts cannot change worker state after their scope has finished", async (t) => {
  const f = await fixture(t);
  await enqueue(f.queue, "one");
  let retained;
  const worker = f.start(
    (_, context) => {
      retained = context;
    },
    { stopWhenIdle: true },
  );
  await worker.done;
  const before = worker.stats;
  f.time(10000);
  assert.throws(() => retained.checkpoint(), { code: "ERR_FSQLITE_JOB_CONTEXT_CLOSED" });
  assert.deepEqual(worker.stats, before);
});
