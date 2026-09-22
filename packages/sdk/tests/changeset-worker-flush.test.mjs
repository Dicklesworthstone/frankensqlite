import assert from "node:assert/strict";
import test from "node:test";
import { getEventListeners } from "node:events";
import { setTimeout as delay } from "node:timers/promises";
import {
  ChangesetDeliveryFlushError,
  ChangesetDeliveryWorker,
} from "../src/changeset-worker.ts";

const empty = () => ({
  deliveries: 0, bytes: 0, applied: 0, omitted: 0, replays: 0,
  alreadyAcknowledged: 0, stopped: "empty",
});
const page = () => ({ ...empty(), deliveries: 1, bytes: 32, applied: 1, stopped: "limit" });
function deferred() {
  let resolve, reject;
  const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}
async function until(predicate) {
  const deadline = performance.now() + 2000;
  while (!predicate()) {
    assert.ok(performance.now() < deadline, "worker did not reach the expected state");
    await delay(1);
  }
}
const hasCode = (suffix) => (error) =>
  error instanceof ChangesetDeliveryFlushError &&
  error.code === `ERR_FSQLITE_DELIVERY_FLUSH_${suffix}`;

// Production supervisor, explicit drivers: these are lifecycle/barrier tests,
// not a substitute for exercising SQL, receiver persistence or HTTP delivery.
test("flush registered before startup waits through limited pages to a fresh empty read", async () => {
  const replies = [page(), page(), empty()];
  const worker = ChangesetDeliveryWorker.start({ async run() { return replies.shift(); } },
    { stopWhenIdle: true });
  const flushed = worker.flush();
  assert.equal(worker.stats.pendingFlushes, 1);
  const stats = await flushed;
  assert.equal(stats.deliveries, 2);
  assert.equal(stats.attempts, 3);
  assert.equal(stats.pendingFlushes, 0);
  assert.ok(Object.isFrozen(stats));
  await worker.done;
});

test("an already active empty observation cannot certify a later flush", async () => {
  const first = deferred(), releaseFirst = deferred(), second = deferred(), releaseSecond = deferred();
  let calls = 0;
  const worker = ChangesetDeliveryWorker.start({ async run() {
    if (++calls === 1) { first.resolve(); await releaseFirst.promise; }
    else { second.resolve(); await releaseSecond.promise; }
    return empty();
  } }, { stopWhenIdle: true });
  try {
    await first.promise;
    let settled = false;
    const flushed = worker.flush().then((stats) => { settled = true; return stats; });
    releaseFirst.resolve();
    await second.promise;
    assert.equal(settled, false, "the first empty read predates this barrier");
    assert.equal(worker.stats.pendingFlushes, 1);
    releaseSecond.resolve();
    assert.equal((await flushed).attempts, 2);
    await worker.done;
    assert.equal(calls, 2);
  } finally { releaseFirst.resolve(); releaseSecond.resolve(); await worker.stop(); }
});

test("many flush callers coalesce into one fresh drain and clean up listeners", async () => {
  const gate = deferred();
  let calls = 0;
  const worker = ChangesetDeliveryWorker.start({ async run() { calls++; await gate.promise; return empty(); } },
    { stopWhenIdle: true });
  const signals = Array.from({ length: 20 }, () => new AbortController());
  const flushes = signals.map(({ signal }) => worker.flush({ signal, timeoutMs: 2000 }));
  assert.equal(worker.stats.pendingFlushes, 20);
  gate.resolve();
  const stats = await Promise.all(flushes);
  await worker.done;
  assert.equal(calls, 1);
  for (const snapshot of stats) assert.equal(snapshot.pendingFlushes, 0);
  for (const { signal } of signals) assert.equal(getEventListeners(signal, "abort").length, 0);
});

test("flush wakes an idle long poll instead of accepting cached emptiness", async () => {
  let calls = 0;
  const worker = ChangesetDeliveryWorker.start({ async run() { calls++; return empty(); } },
    { pollIntervalMs: 2_147_483_647 });
  try {
    await until(() => worker.stats.state === "idle");
    const stats = await worker.flush();
    assert.equal(stats.attempts, 2);
    assert.equal(calls, 2);
  } finally { await worker.stop(); }
});

test("cancelling a waiter does not cancel the shared run or another waiter", async () => {
  const entered = deferred(), release = deferred();
  const cancel = new AbortController();
  let signal;
  const worker = ChangesetDeliveryWorker.start({ async run(options) {
    signal = options.signal; entered.resolve(); await release.promise; return empty();
  } }, { stopWhenIdle: true });
  const first = worker.flush({ signal: cancel.signal });
  const second = worker.flush();
  const failed = assert.rejects(first, (error) => hasCode("CANCELLED")(error) && error.cause === "not waiting");
  await entered.promise;
  cancel.abort("not waiting");
  await failed;
  assert.equal(signal.aborted, false);
  assert.equal(worker.stats.pendingFlushes, 1);
  assert.equal(getEventListeners(cancel.signal, "abort").length, 0);
  release.resolve();
  await second; await worker.done;
});

test("a waiter timeout releases capacity without abandoning confirmation", async () => {
  const entered = deferred(), release = deferred();
  let signal;
  const worker = ChangesetDeliveryWorker.start({ async run(options) {
    signal = options.signal; entered.resolve(); await release.promise; return empty();
  } }, { stopWhenIdle: true, maxPendingFlushes: 1 });
  const flushed = worker.flush({ timeoutMs: 5 });
  const failed = assert.rejects(flushed, hasCode("TIMEOUT"));
  await entered.promise; await failed;
  assert.equal(worker.stats.pendingFlushes, 0);
  assert.equal(worker.stats.active, true);
  assert.equal(signal.aborted, false);
  release.resolve(); await worker.done;
});

test("an expired monotonic deadline cannot succeed just because its timer was starved", async (t) => {
  let now = 100;
  t.mock.method(performance, "now", () => now);
  const worker = ChangesetDeliveryWorker.start({ async run() { now = 111; return empty(); } },
    { stopWhenIdle: true });
  await assert.rejects(worker.flush({ timeoutMs: 10 }), hasCode("TIMEOUT"));
  await worker.done;
  assert.equal(worker.stats.pendingFlushes, 0);
});

test("bounded waiters reject overload and become available after cancellation", async () => {
  const gate = deferred(), cancel = new AbortController();
  const worker = ChangesetDeliveryWorker.start({ async run() { await gate.promise; return empty(); } },
    { stopWhenIdle: true, maxPendingFlushes: 1 });
  const first = worker.flush({ signal: cancel.signal });
  const firstRejected = assert.rejects(first, hasCode("CANCELLED"));
  await assert.rejects(worker.flush(), hasCode("LIMIT"));
  assert.equal(worker.stats.pendingFlushes, 1);
  cancel.abort(); await firstRejected;
  const replacement = worker.flush();
  gate.resolve();
  await replacement; await worker.done;
  assert.equal(worker.stats.pendingFlushes, 0);
});

test("source or transport failure rejects every waiter and retains original uncertainty", async () => {
  const cause = Object.assign(new Error("checkpoint acknowledgement lost"), { phase: "source-confirm" });
  const worker = ChangesetDeliveryWorker.start({ async run() { throw cause; } });
  const pending = [worker.flush(), worker.flush()];
  let stopped;
  await assert.rejects(worker.done, (error) => { stopped = error; return error.cause === cause; });
  for (const flushed of pending) await assert.rejects(flushed,
    (error) => hasCode("STOPPED")(error) && error.cause === stopped);
  await assert.rejects(worker.flush(), (error) => error.cause === stopped);
  assert.equal(worker.stats.attempts, 1);
  assert.equal(worker.stats.pendingFlushes, 0);
});

test("stop joins its run but rejects barriers requiring a run not yet admitted", async () => {
  const entered = deferred(), release = deferred();
  let calls = 0;
  const worker = ChangesetDeliveryWorker.start({ async run() {
    calls++; entered.resolve(); await release.promise; return empty();
  } });
  await entered.promise;
  const flushed = worker.flush();
  let settled = false;
  const failed = assert.rejects(flushed, hasCode("STOPPED")).then(() => { settled = true; });
  const stopped = worker.stop();
  await Promise.resolve();
  assert.equal(settled, false, "shutdown still owns the in-flight outcome");
  await assert.rejects(worker.flush(), hasCode("STOPPED"));
  release.resolve(); await stopped; await failed;
  assert.equal(calls, 1);
  assert.equal(worker.stats.pendingFlushes, 0);
});

test("graceful stop preserves a qualifying empty result already admitted after flush", async () => {
  const entered = deferred(), release = deferred();
  const worker = ChangesetDeliveryWorker.start({ async run() {
    entered.resolve(); await release.promise; return empty();
  } });
  const flushed = worker.flush();
  await entered.promise;
  const stopped = worker.stop();
  release.resolve();
  assert.equal((await flushed).attempts, 1);
  await stopped;
});

test("invalid flush inputs reserve no waiter and do not poison delivery", async () => {
  const gate = deferred();
  const worker = ChangesetDeliveryWorker.start({ async run() { await gate.promise; return empty(); } },
    { stopWhenIdle: true });
  for (const options of [null, { signal: {} }, { timeoutMs: 0 }, { timeoutMs: 0.5 },
    { timeoutMs: 2_147_483_648 }, { timeoutMs: NaN }]) {
    await assert.rejects(worker.flush(options), hasCode("INPUT"));
    assert.equal(worker.stats.pendingFlushes, 0);
  }
  gate.resolve(); await worker.done;
});

test("pre-aborted waiter performs no wakeup and ignores overridden signal getters", async () => {
  const cancel = new AbortController(); cancel.abort("already cancelled");
  let calls = 0;
  const worker = ChangesetDeliveryWorker.start({ async run() { calls++; return empty(); } },
    { pollIntervalMs: 2_147_483_647 });
  try {
    await until(() => worker.stats.state === "idle");
    Object.defineProperty(cancel.signal, "aborted", { get() { assert.fail("not an intrinsic"); } });
    Object.defineProperty(cancel.signal, "reason", { get() { assert.fail("not an intrinsic"); } });
    await assert.rejects(worker.flush({ signal: cancel.signal }),
      (error) => hasCode("CANCELLED")(error) && error.cause === "already cancelled");
    assert.equal(calls, 1);
    assert.equal(worker.stats.pendingFlushes, 0);
    assert.equal(getEventListeners(cancel.signal, "abort").length, 0);
  } finally { await worker.stop(); }
});

test("option getters cannot reserve a waiter after re-entering stop", async () => {
  const worker = ChangesetDeliveryWorker.start({ run() { assert.fail("must not run"); } });
  let signalReads = 0, timeoutReads = 0;
  const flushed = worker.flush({
    get signal() { signalReads++; worker.stop(); return undefined; },
    get timeoutMs() { timeoutReads++; return 100; },
  });
  await assert.rejects(flushed, hasCode("STOPPED"));
  await worker.done;
  assert.equal(signalReads, 1); assert.equal(timeoutReads, 1);
  assert.equal(worker.stats.pendingFlushes, 0);
});

test("stopping before startup rejects pending flushes and releases timers/listeners", async () => {
  const controller = new AbortController();
  controller.signal.addEventListener = () => assert.fail("use native listener intrinsic");
  controller.signal.removeEventListener = () => assert.fail("use native listener intrinsic");
  const worker = ChangesetDeliveryWorker.start({ run() { assert.fail("must not run"); } });
  const flushed = worker.flush({ signal: controller.signal, timeoutMs: 2_147_483_647 });
  await worker.stop();
  await assert.rejects(flushed, hasCode("STOPPED"));
  assert.equal(getEventListeners(controller.signal, "abort").length, 0);
  assert.equal(worker.stats.pendingFlushes, 0);
});

test("malformed empty results never satisfy a flush", async () => {
  const worker = ChangesetDeliveryWorker.start({ async run() { return { ...empty(), bytes: -1 }; } });
  const flushed = worker.flush();
  await assert.rejects(worker.done, (error) => error.phase === "result");
  await assert.rejects(flushed, (error) => hasCode("STOPPED")(error) && error.cause.phase === "result");
});

test("overridden public conveniences cannot intercept a flush wakeup or forge its receipt", async () => {
  let calls = 0;
  const worker = ChangesetDeliveryWorker.start({ async run() { calls++; return empty(); } },
    { pollIntervalMs: 2_147_483_647 });
  try {
    await until(() => worker.stats.state === "idle");
    worker.notify = () => assert.fail("flush must not invoke an override");
    Object.defineProperty(worker, "stats", { get() { assert.fail("receipt must be an internal snapshot"); } });
    const stats = await worker.flush();
    assert.equal(calls, 2);
    assert.equal(stats.attempts, 2);
    assert.equal(stats.pendingFlushes, 0);
    assert.ok(Object.isFrozen(stats));
  } finally { await worker.stop(); }
});
