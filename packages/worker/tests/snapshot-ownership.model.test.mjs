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
