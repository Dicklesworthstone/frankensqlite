import assert from "node:assert/strict";
import test from "node:test";
import { getEventListeners } from "node:events";
import { setTimeout as delay } from "node:timers/promises";
import {
  ChangesetDeliveryWorker,
  ChangesetDeliveryWorkerError,
} from "../src/changeset-worker.ts";

const empty = () => ({
  deliveries: 0, bytes: 0, applied: 0, omitted: 0, replays: 0,
  alreadyAcknowledged: 0, stopped: "empty",
});
const delivered = (stopped = "limit") => ({
  ...empty(), deliveries: 1, bytes: 32, applied: 1, stopped,
});
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

// These tests run the production supervisor, with explicit pump drivers. They
// certify scheduling/ownership contracts, not SQLite or browser durability.
test("drains bounded pages sequentially and stops only after an empty read", async () => {
  const settings = [];
  const replies = [delivered(), { ...delivered(), replays: 1 }, empty()];
  let returned = false;
  const worker = ChangesetDeliveryWorker.start({
    async run(options) {
      assert.equal(returned, true, "start must return before invoking the pump");
      settings.push(options);
      return replies.shift();
    },
  }, { stopWhenIdle: true, maxDeliveriesPerRun: 2, maxBytesPerRun: 64, runTimeoutMs: 250 });
  returned = true;
  await worker.done;
  assert.equal(settings.length, 3);
  for (const options of settings) {
    assert.equal(options.maxDeliveries, 2);
    assert.equal(options.maxBytes, 64);
    assert.equal(options.timeoutMs, 250);
    assert.equal(options.signal.aborted, false);
  }
  assert.deepEqual(worker.stats, {
    state: "stopped", active: false, attempts: 3, successfulRuns: 3, pendingFlushes: 0,
    deliveries: 2, bytes: 64, applied: 2, omitted: 0, replays: 1, alreadyAcknowledged: 0,
  });
  assert.equal(Object.isFrozen(worker.stats), true);
  assert.equal(worker.notify(), false);
});

test("captures option getters and the pump method once", async () => {
  const reads = new Map();
  const values = { pollIntervalMs: 10, maxDeliveriesPerRun: 1, maxBytesPerRun: 50,
    runTimeoutMs: 100, maxPendingFlushes: 7, stopWhenIdle: true, signal: undefined };
  const options = Object.fromEntries([]);
  for (const [key, value] of Object.entries(values)) {
    Object.defineProperty(options, key, { get() {
      reads.set(key, (reads.get(key) ?? 0) + 1);
      return value;
    } });
  }
  let methodReads = 0;
  const pump = { get run() {
    methodReads++;
    return function () { assert.equal(this, pump); return Promise.resolve(empty()); };
  } };
  const worker = ChangesetDeliveryWorker.start(pump, options);
  await worker.done;
  assert.equal(methodReads, 1);
  assert.deepEqual([...reads.values()], [1, 1, 1, 1, 1, 1, 1]);
});

test("notifications during an active empty read coalesce into one follow-up", async () => {
  const entered = deferred(), release = deferred();
  let calls = 0, active = 0, maximum = 0;
  const worker = ChangesetDeliveryWorker.start({ async run() {
    calls++; maximum = Math.max(maximum, ++active);
    if (calls === 1) { entered.resolve(); await release.promise; }
    active--;
    return empty();
  } }, { stopWhenIdle: true });
  await entered.promise;
  assert.equal(worker.stats.active, true);
  for (let i = 0; i < 100; i++) assert.equal(worker.notify(), true);
  release.resolve();
  await worker.done;
  assert.equal(calls, 2);
  assert.equal(maximum, 1);
});

test("notify wakes an idle long poll without creating overlapping drains", async () => {
  const second = deferred(), release = deferred();
  let calls = 0;
  const worker = ChangesetDeliveryWorker.start({ async run() {
    if (++calls === 2) { second.resolve(); await release.promise; }
    return empty();
  } }, { pollIntervalMs: 2_147_483_647 });
  try {
    await until(() => worker.stats.state === "idle");
    for (let i = 0; i < 100; i++) worker.notify();
    await second.promise;
    assert.equal(calls, 2);
    const stopped = worker.stop();
    release.resolve();
    await stopped;
  } finally { release.resolve(); await worker.stop(); }
});

test("graceful stop joins the active run and does not drain a further page", async () => {
  const entered = deferred(), release = deferred();
  let calls = 0, signal;
  const worker = ChangesetDeliveryWorker.start({ async run(options) {
    calls++; signal = options.signal; entered.resolve(); await release.promise;
    return delivered();
  } });
  await entered.promise;
  let settled = false;
  const stopping = worker.stop();
  assert.equal(stopping, worker.done);
  void stopping.then(() => { settled = true; });
  await Promise.resolve();
  assert.equal(settled, false);
  assert.equal(worker.stats.state, "draining");
  assert.equal(worker.stats.active, true);
  assert.equal(signal.aborted, false);
  release.resolve();
  await stopping;
  assert.equal(calls, 1);
  assert.equal(worker.stats.deliveries, 1);
});

test("abort signals an active run but never abandons its confirmation", async () => {
  const entered = deferred(), release = deferred();
  const cause = new Error("confirmation failed after cancellation");
  const reason = new Error("owner stopped");
  let signal;
  const worker = ChangesetDeliveryWorker.start({ async run(options) {
    signal = options.signal; entered.resolve(); await release.promise; throw cause;
  } });
  await entered.promise;
  let settled = false;
  const stopping = worker.stop({ abort: true, reason });
  void stopping.catch(() => { settled = true; });
  assert.equal(signal.aborted, true);
  assert.equal(signal.reason, reason);
  await Promise.resolve();
  assert.equal(settled, false);
  release.resolve();
  await assert.rejects(stopping, (error) => {
    assert.ok(error instanceof ChangesetDeliveryWorkerError);
    assert.equal(error.phase, "run");
    assert.equal(error.cause, cause);
    return true;
  });
  assert.equal(worker.stats.state, "failed");
  assert.equal(worker.stats.successfulRuns, 0);
});

test("a successful late commit remains successful even after abort", async () => {
  const entered = deferred(), release = deferred();
  const worker = ChangesetDeliveryWorker.start({ async run() {
    entered.resolve(); await release.promise; return delivered();
  } });
  await entered.promise;
  const stopped = worker.stop({ abort: true });
  release.resolve();
  await stopped;
  assert.equal(worker.stats.state, "stopped");
  assert.equal(worker.stats.deliveries, 1);
});

test("pre-aborted ownership never invokes the pump", async () => {
  const controller = new AbortController(); controller.abort("closed");
  const worker = ChangesetDeliveryWorker.start({ run() { assert.fail("must not run"); } },
    { signal: controller.signal });
  await worker.done;
  assert.equal(worker.stats.attempts, 0);
  assert.equal(worker.stats.state, "stopped");
});

test("stop before the startup microtask admits no work", async () => {
  const worker = ChangesetDeliveryWorker.start({ run() { assert.fail("must not run"); } });
  await worker.stop();
  assert.equal(worker.stats.attempts, 0);
});

test("external cancellation interrupts idle polling and removes its listener", async () => {
  const controller = new AbortController();
  let calls = 0;
  // These overrides must never acquire or strand unowned startup work.
  controller.signal.addEventListener = () => assert.fail("must use the native listener intrinsic");
  controller.signal.removeEventListener = () => assert.fail("must use the native listener intrinsic");
  const worker = ChangesetDeliveryWorker.start({ async run() { calls++; return empty(); } },
    { signal: controller.signal, pollIntervalMs: 2_147_483_647 });
  await until(() => worker.stats.state === "idle");
  assert.equal(getEventListeners(controller.signal, "abort").length, 1);
  controller.abort("shutdown");
  await worker.done;
  assert.equal(calls, 1);
  assert.equal(getEventListeners(controller.signal, "abort").length, 0);
});

test("a pump failure stops admission without retrying uncertain work", async () => {
  let calls = 0;
  const cause = Object.assign(new Error("lost acknowledgement"), {
    phase: "source-ack", deliveryId: "origin:17", code: "ERR_FSQLITE_DELIVERY_FAILED",
  });
  const worker = ChangesetDeliveryWorker.start({ async run() { calls++; throw cause; } });
  await assert.rejects(worker.done, (error) => error.cause === cause && error.phase === "run");
  assert.equal(calls, 1);
  assert.equal(worker.notify(), false);
  assert.equal(worker.stop(), worker.done);
});

test("result validation never invokes caller-controlled getters", async () => {
  let reads = 0;
  const result = empty();
  Object.defineProperty(result, "bytes", { get() { reads++; return 0; } });
  const worker = ChangesetDeliveryWorker.start({ async run() { return result; } });
  await assert.rejects(worker.done, (error) => error.phase === "result");
  assert.equal(reads, 0);
  assert.equal(worker.stats.successfulRuns, 0);
});

test("rejects malformed results and non-progressing limits without another run", async (t) => {
  const invalid = [null, {}, { ...empty(), deliveries: -1 }, { ...empty(), bytes: 0.5 },
    { ...empty(), applied: Infinity }, { ...empty(), replays: 1 },
    { ...empty(), stopped: "limit" }, { ...empty(), stopped: "finished" },
    { ...delivered(), deliveries: 101 }, { ...delivered(), bytes: 64 * 1024 * 1024 + 1 },
    { ...empty(), bytes: 1 }, { ...empty(), omitted: 1 },
    { ...delivered(), alreadyAcknowledged: 100 }, Object.create(empty())];
  for (let i = 0; i < invalid.length; i++) await t.test(`case ${i}`, async () => {
    let calls = 0;
    const worker = ChangesetDeliveryWorker.start({ async run() { calls++; return invalid[i]; } });
    await assert.rejects(worker.done, (error) => error.phase === "result");
    assert.equal(calls, 1);
    assert.equal(worker.stats.successfulRuns, 0);
  });
});

test("acknowledgement races count as progress without inventing new deliveries", async () => {
  const replies = [{ ...empty(), alreadyAcknowledged: 1, stopped: "limit" }, empty()];
  const worker = ChangesetDeliveryWorker.start({ async run() { return replies.shift(); } },
    { stopWhenIdle: true });
  await worker.done;
  assert.equal(worker.stats.attempts, 2);
  assert.equal(worker.stats.deliveries, 0);
  assert.equal(worker.stats.alreadyAcknowledged, 1);
});

test("continuous immediate work yields to task-level cancellation", async () => {
  const controller = new AbortController();
  let calls = 0;
  const worker = ChangesetDeliveryWorker.start({ async run() {
    assert.ok(++calls < 100, "immediate drains starved the task queue");
    worker.notify();
    return delivered();
  } }, { signal: controller.signal });
  const timer = setTimeout(() => controller.abort("task turn"), 0);
  try { await worker.done; } finally { clearTimeout(timer); }
  assert.equal(calls, 1);
});

test("invalid options fail before reading the pump or admitting any work", () => {
  const pump = { get run() { assert.fail("pump must not be read"); } };
  for (const options of [
    { pollIntervalMs: 0 }, { pollIntervalMs: 2_147_483_648 },
    { maxDeliveriesPerRun: 10_001 }, { maxDeliveriesPerRun: 0.5 },
    { maxBytesPerRun: 0 }, { maxBytesPerRun: 1_073_741_825 },
    { runTimeoutMs: 0 }, { runTimeoutMs: NaN },
    { maxPendingFlushes: 0 }, { maxPendingFlushes: 65_537 },
    { stopWhenIdle: "true" }, { signal: {} },
  ]) assert.throws(() => ChangesetDeliveryWorker.start(pump, options));
  assert.throws(() => ChangesetDeliveryWorker.start(null), TypeError);
});

test("graceful stop can be escalated without replacing the owned completion", async () => {
  const entered = deferred(), release = deferred();
  let signal;
  const worker = ChangesetDeliveryWorker.start({ async run(options) {
    signal = options.signal; entered.resolve(); await release.promise; return empty();
  } });
  await entered.promise;
  const first = worker.stop();
  assert.equal(worker.stop({ abort: true, reason: "escalated" }), first);
  assert.equal(worker.stop(), first);
  assert.equal(worker.stats.state, "aborting");
  assert.equal(signal.reason, "escalated");
  release.resolve(); await first;
});

test("polling eventually observes work without notify", async () => {
  let calls = 0;
  const worker = ChangesetDeliveryWorker.start({ async run() {
    if (++calls === 2) worker.stop();
    return calls === 1 ? empty() : delivered();
  } }, { pollIntervalMs: 1 });
  await worker.done;
  assert.equal(calls, 2);
  assert.equal(worker.stats.deliveries, 1);
});

test("statistics overflow fails closed without partially updating counters", async () => {
  const replies = [{ ...delivered(), applied: Number.MAX_SAFE_INTEGER }, delivered()];
  const worker = ChangesetDeliveryWorker.start({ async run() { return replies.shift(); } });
  await assert.rejects(worker.done, (error) => error.phase === "result");
  assert.equal(worker.stats.successfulRuns, 1);
  assert.equal(worker.stats.deliveries, 1);
  assert.equal(worker.stats.applied, Number.MAX_SAFE_INTEGER);
});
