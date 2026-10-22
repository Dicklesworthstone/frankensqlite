// Deterministic Web Locks model; NOT a browser termination/storage test.

import assert from "node:assert/strict";
import { test } from "node:test";
import {
  SnapshotSessionLease as Lease,
  resolveSnapshotOwnership,
} from "../src/snapshot-ownership.ts";

function install(t, manager) {
  const original = Object.getOwnPropertyDescriptor(globalThis, "navigator");
  Object.defineProperty(globalThis, "navigator", {
    configurable: true,
    value: manager === undefined ? {} : { locks: manager },
  });
  t.after(() => {
    if (original) Object.defineProperty(globalThis, "navigator", original);
    else Reflect.deleteProperty(globalThis, "navigator");
  });
}
function model(t) {
  const held = new Set();
  const calls = [];
  const manager = {
    async request(name, options, callback) {
      calls.push({ name, ...options });
      assert.equal(options.ifAvailable, true);
      assert.equal(options.steal, undefined);
      await Promise.resolve();
      const blocked = [...held].some(
        (lock) => lock.name === name && (options.mode === "exclusive" || lock.mode === "exclusive"),
      );
      if (blocked) return callback(null);
      const lock = { name, mode: options.mode };
      held.add(lock);
      try {
        return await callback(lock);
      } finally {
        held.delete(lock);
      }
    },
  };
  install(t, manager);
  return { held, calls };
}
const denied = { code: "ERR_FSQLITE_SNAPSHOT_OWNED" };

test("default sessions share ownership and exclude exclusive peers", async (t) => {
  const { held } = model(t);
  const a = await Lease.acquire("opfs-snapshot", "db");
  const b = await Lease.acquire("opfs-snapshot", "db", "shared");
  assert.equal(a.mode, "shared");
  assert.equal(held.size, 2);
  await assert.rejects(Lease.acquire("opfs-snapshot", "db", "exclusive"), denied);
  await a.close();
  await assert.rejects(Lease.acquire("opfs-snapshot", "db", "exclusive"), denied);
  await b.close();
  assert.equal(held.size, 0);
  const exclusive = await Lease.acquire("opfs-snapshot", "db", "exclusive");
  await exclusive.close();
});

test("an exclusive owner excludes default/shared/exclusive sessions", async (t) => {
  model(t);
  const a = await Lease.acquire("indexeddb-snapshot", "db", "exclusive");
  for (const mode of [undefined, "shared", "exclusive"]) {
    await assert.rejects(Lease.acquire("indexeddb-snapshot", "db", mode), denied);
  }
  await a.close();
  const b = await Lease.acquire("indexeddb-snapshot", "db");
  await b.close();
});

test("unrelated names and storage backends have separate authority", async (t) => {
  const { calls, held } = model(t);
  const leases = [];
  for (const backend of ["indexeddb-snapshot", "opfs-snapshot"]) {
    for (const name of ["db", 'db"', "db\\", "db\ud800", "db\ud801", "db\ufffd"]) {
      leases.push(await Lease.acquire(backend, name, "exclusive"));
    }
  }
  assert.equal(held.size, 12);
  assert.equal(new Set(calls.map((call) => call.name)).size, 12);
  await Promise.all(leases.map((lease) => lease.close()));
  assert.equal(held.size, 0);
});

test("close returns one joined promise and retires transfer eligibility immediately", async (t) => {
  const { held } = model(t);
  const lease = await Lease.acquire("opfs-snapshot", "db", "exclusive");
  assert.equal(lease.matches("opfs-snapshot", "db", "exclusive"), true);
  assert.equal(lease.matches("opfs-snapshot", "db", undefined), false);
  assert.equal(lease.matches("indexeddb-snapshot", "db", "exclusive"), false);
  const first = lease.close();
  assert.equal(first, lease.close());
  assert.equal(lease.matches("opfs-snapshot", "db", "exclusive"), false);
  await first;
  assert.equal(held.size, 0);
  assert.equal(Object.isFrozen(lease), true);
});

test("only an unspecified default may operate without Web Locks", async (t) => {
  install(t);
  assert.equal(await Lease.acquire("indexeddb-snapshot", "db"), null);
  for (const mode of ["shared", "exclusive"]) {
    await assert.rejects(Lease.acquire("indexeddb-snapshot", "db", mode), {
      code: "ERR_FSQLITE_SNAPSHOT_OWNERSHIP_UNAVAILABLE",
    });
  }
});

test("invalid identities or explicit policies fail before contacting the manager", async (t) => {
  const { calls } = model(t);
  for (const args of [
    ["memory", "db", "shared"],
    ["opfs-snapshot", "", undefined],
    ["opfs-snapshot", ":memory:", undefined],
    ["opfs-snapshot", "a\0b", "exclusive"],
    ["opfs-snapshot", "x".repeat(257), "exclusive"],
    ["opfs-snapshot", "db", null],
    ["opfs-snapshot", "db", false],
    ["opfs-snapshot", "db", "typo"],
  ]) {
    await assert.rejects(Lease.acquire(...args), { code: "ERR_FSQLITE_SNAPSHOT_OWNERSHIP_INPUT" });
  }
  assert.equal(calls.length, 0);
  assert.equal(resolveSnapshotOwnership(undefined), undefined);
});

test("concurrent exclusive attempts grant exactly one lease without waiters", async (t) => {
  const { held } = model(t);
  const attempts = await Promise.allSettled(
    Array.from({ length: 100 }, () => Lease.acquire("opfs-snapshot", "db", "exclusive")),
  );
  assert.equal(attempts.filter((v) => v.status === "fulfilled").length, 1);
  assert.equal(
    attempts.filter((v) => v.status === "rejected" && v.reason.code === denied.code).length,
    99,
  );
  assert.equal(held.size, 1);
  await attempts.find((v) => v.status === "fulfilled").value.close();
  assert.equal(held.size, 0);
});

test("lock-manager refusal and thrown errors never become a granted lease", async (t) => {
  const failure = new Error("lock manager rejected");
  install(t, {
    request() {
      throw failure;
    },
  });
  await assert.rejects(Lease.acquire("opfs-snapshot", "db", "exclusive"), (e) => e === failure);
  globalThis.navigator.locks.request = () => Promise.reject(failure);
  await assert.rejects(Lease.acquire("opfs-snapshot", "db", "exclusive"), (e) => e === failure);
  globalThis.navigator.locks.request = () => Promise.resolve();
  await assert.rejects(Lease.acquire("opfs-snapshot", "db", "exclusive"), {
    code: "ERR_FSQLITE_SNAPSHOT_OWNERSHIP_UNAVAILABLE",
  });
});

function breakable(t) {
  let reject, finish, callbackDone;
  install(t, {
    request(name, options, callback) {
      const released = new Promise((resolve, fail) => { finish = resolve; reject = fail; });
      callbackDone = Promise.resolve(callback({ name, mode: options.mode }));
      void callbackDone.then(finish, reject);
      return released;
    },
  });
  return { reject: reason => reject(reason), finish: () => finish(), joined: () => callbackDone };
}

test("a granted but subsequently lost lease is never transferable", async (t) => {
  const manager = breakable(t);
  const lease = await Lease.acquire("opfs-snapshot", "db", "exclusive");
  const failure = new DOMException("Lock stolen", "AbortError");
  manager.reject(failure);
  await Promise.resolve();
  assert.equal(lease.matches("opfs-snapshot", "db", "exclusive"), false);
  await assert.rejects(lease.close());
  await manager.joined();
});

test("loss signals one non-retryable failure and releases the orphaned callback", async (t) => {
  for (const mode of ["shared", "exclusive"]) {
    const manager = breakable(t);
    const lease = await Lease.acquire("indexeddb-snapshot", "db", mode);
    let notices = 0;
    lease.lossSignal.addEventListener("abort", () => { notices++; });
    lease.assertHeld();
    const cause = new DOMException("stolen", "AbortError");
    manager.reject(cause);
    await Promise.resolve();
    const failure = lease.lossSignal.reason;
    assert.equal(lease.lossSignal.aborted, true);
    assert.equal(failure.code, "ERR_FSQLITE_SNAPSHOT_OWNERSHIP_LOST");
    assert.equal(failure.cause, cause);
    assert.equal(failure.transient, false);
    assert.equal(failure.userRecoverable, false);
    assert.throws(() => lease.assertHeld(), error => error === failure);
    // This joins WITHOUT close(): the unexpected loss releases its own wait.
    await manager.joined();
    assert.equal(notices, 1);
    const closing = lease.close();
    assert.equal(closing, lease.close());
    await assert.rejects(closing, error => error === failure);
    assert.equal(notices, 1);
  }
});

test("premature successful manager settlement is also loss, not a clean close", async (t) => {
  const manager = breakable(t);
  const lease = await Lease.acquire("opfs-snapshot", "db", "exclusive");
  manager.finish();
  await Promise.resolve();
  assert.equal(lease.lossSignal.aborted, true);
  assert.throws(() => lease.assertHeld(), { code: "ERR_FSQLITE_SNAPSHOT_OWNERSHIP_LOST" });
  await assert.rejects(lease.close(), { code: "ERR_FSQLITE_SNAPSHOT_OWNERSHIP_LOST" });
  await manager.joined();
});

test("ordinary joined close never reports an unexpected loss", async (t) => {
  model(t);
  const lease = await Lease.acquire("opfs-snapshot", "db", "exclusive");
  let notices = 0;
  lease.lossSignal.addEventListener("abort", () => { notices++; });
  const closing = lease.close();
  assert.throws(() => lease.assertHeld(), { code: "ERR_FSQLITE_SNAPSHOT_OWNERSHIP_UNAVAILABLE" });
  await closing;
  assert.equal(lease.lossSignal.aborted, false);
  assert.equal(notices, 0);
});

test("an already-lost grant cannot escape acquire", async (t) => {
  let done;
  install(t, {
    request(name, options, callback) {
      done = callback({ name, mode: options.mode });
      return Promise.reject(new DOMException("immediate theft", "AbortError"));
    },
  });
  await assert.rejects(Lease.acquire("opfs-snapshot", "db", "exclusive"), {
    code: "ERR_FSQLITE_SNAPSHOT_OWNERSHIP_LOST",
  });
  await done;
});

test("loss while close is settling remains a failure", async (t) => {
  let reject, done;
  install(t, {
    request(name, options, callback) {
      done = callback({ name, mode: options.mode });
      return new Promise((_resolve, fail) => { reject = fail; });
    },
  });
  const lease = await Lease.acquire("opfs-snapshot", "db", "exclusive");
  const closing = lease.close();
  const cause = new Error("release could not be acknowledged");
  reject(cause);
  await assert.rejects(closing, error => error === lease.lossSignal.reason && error.cause === cause);
  await done;
  assert.equal(lease.matches("opfs-snapshot", "db", "exclusive"), false);
});
