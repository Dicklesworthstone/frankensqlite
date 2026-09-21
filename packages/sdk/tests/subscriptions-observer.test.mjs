// Unit ownership/coalescing tests: no SQL engine or browser is substituted here.

import assert from "node:assert/strict";
import { test } from "node:test";
import { ChangeObserver, createChangeStream } from "../src/subscriptions.ts";

function deferred() {
  let resolve;
  const promise = new Promise((yes) => {
    resolve = yes;
  });
  return { promise, resolve };
}
function fixture(listener) {
  let detached = 0,
    released = 0;
  const observer = new ChangeObserver(
    ["items", "audit"],
    listener,
    () => {
      detached++;
    },
    () => {
      released++;
    },
  );
  observer.activate(undefined);
  return {
    observer,
    get detached() {
      return detached;
    },
    get released() {
      return released;
    },
  };
}
const options = { timeout: 3000 };

test("observer coalesces 10000 commits while one listener owns delivery", options, async () => {
  const entered = deferred(),
    release = deferred(),
    second = deferred(),
    events = [];
  const f = fixture(async (change) => {
    events.push(change);
    if (events.length === 1) {
      entered.resolve();
      await release.promise;
    } else second.resolve();
  });
  f.observer.publish(["items", "unwatched"], 1n);
  await entered.promise;
  for (let n = 2n; n <= 10001n; n++) f.observer.publish(["audit", "items"], n);
  assert.equal(events.length, 1);
  assert.equal(f.observer.running, true);
  assert.equal(f.observer.pending, true);
  release.resolve();
  await second.promise;
  assert.deepEqual(events, [
    { tables: ["items"], firstSequence: 1n, lastSequence: 1n, commits: 1n },
    { tables: ["audit", "items"], firstSequence: 2n, lastSequence: 10001n, commits: 10000n },
  ]);
  assert.ok(Object.isFrozen(events[1]));
  assert.ok(Object.isFrozen(events[1].tables));
  f.observer.stop();
  await f.observer.handle.done;
  assert.equal(f.detached, 1);
  assert.equal(f.released, 1);
});

test("unsubscribe drops scheduled callbacks and releases exactly once", options, async () => {
  const f = fixture(() => assert.fail("stopped callback executed"));
  f.observer.publish(["items"], 1n);
  f.observer.stop();
  f.observer.stop();
  await f.observer.handle.done;
  f.observer.publish(["items"], 2n);
  assert.equal(f.observer.pending, false);
  assert.equal(f.detached, 1);
  assert.equal(f.released, 1);
});

test("stopped active callback retains its reservation until it settles", options, async () => {
  const entered = deferred(),
    release = deferred();
  const f = fixture(async () => {
    entered.resolve();
    await release.promise;
  });
  f.observer.publish(["items"], 1n);
  await entered.promise;
  f.observer.stop();
  assert.equal(f.detached, 1);
  assert.equal(f.released, 0);
  release.resolve();
  await f.observer.handle.done;
  assert.equal(f.released, 1);
});

test("concurrent connection and listener failures preserve both causes", options, async () => {
  const entered = deferred(),
    release = deferred(),
    connection = Error("connection"),
    listener = Error("listener");
  const f = fixture(async () => {
    entered.resolve();
    await release.promise;
    throw listener;
  });
  f.observer.publish(["items"], 1n);
  await entered.promise;
  f.observer.fail(connection);
  release.resolve();
  await assert.rejects(f.observer.handle.done, (error) => {
    assert.equal(error.cause, connection);
    assert.deepEqual(error.errors, [connection, listener]);
    return true;
  });
  assert.equal(f.observer.handle.state, "failed");
  assert.equal(f.released, 1);
});

test("throwing undefined remains a failure rather than a successful stop", options, async () => {
  const f = fixture(() => {
    throw undefined;
  });
  f.observer.publish(["items"], 1n);
  let rejected = false;
  await f.observer.handle.done.catch((error) => {
    rejected = true;
    assert.equal(error, undefined);
  });
  assert.equal(rejected, true);
  assert.equal(f.observer.handle.state, "failed");
});

test(
  "pull stream bounds pending next and return settles its existing waiter",
  options,
  async () => {
    let f;
    const stream = await createChangeStream(async (listener) => {
      f = fixture(listener);
      return f.observer.handle;
    });
    const waiting = stream.next();
    await assert.rejects(stream.next(), { code: "ERR_FSQLITE_SUBSCRIPTION_NEXT_PENDING" });
    assert.deepEqual(await stream.return(), { done: true, value: undefined });
    assert.deepEqual(await waiting, { done: true, value: undefined });
    await f.observer.handle.done;
    assert.equal(f.released, 1);
    assert.equal(stream.closed, true);
  },
);

test("idle pull stream rejects on terminal failure without another publish", options, async () => {
  let f;
  const failure = Error("terminal");
  const stream = await createChangeStream(async (listener) => {
    f = fixture(listener);
    return f.observer.handle;
  });
  const waiting = stream.next();
  f.observer.fail(failure);
  await assert.rejects(waiting, (error) => error === failure);
  await assert.rejects(stream.next(), (error) => error === failure);
  assert.equal(stream.closed, true);
  assert.equal(f.released, 1);
});
