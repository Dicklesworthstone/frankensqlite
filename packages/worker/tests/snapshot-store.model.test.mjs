// Transactional reference-model tests. These do NOT certify browser behavior.
// Run with Node's TypeScript loader or a runner supporting .ts imports.

import assert from "node:assert/strict";
import { test } from "node:test";
import { MAX_SNAPSHOT_BYTES, IndexedDbSnapshotStore as Store } from "../src/snapshot-store.ts";
import {
  snapshotImage as image,
  installIndexedDbModel,
  ModelObjectStore,
} from "./helpers/indexeddb-model.mjs";

const { databases } = installIndexedDbModel();
const state = (name) => databases.get(`frankensqlite:snapshot:v1:${name}`);

async function fixture() {
  const name = crypto.randomUUID();
  return { name, store: await Store.open(name) };
}
const code = (value) => ({ code: value });

test("model: empty/open/close and invalid name admission", async () => {
  const { store } = await fixture();
  assert.equal(await store.load(), null);
  store.close();
  store.close();
  await assert.rejects(store.load(), code("ERR_FSQLITE_SNAPSHOT_CLOSED"));
  for (const name of ["", " ", ":memory:", "\0", "x".repeat(257), null]) {
    await assert.rejects(Store.open(name), code("ERR_FSQLITE_SNAPSHOT_INPUT"));
  }
});

test("model: detached copies and exact-sized subarrays survive close/reopen", async () => {
  const { name, store } = await fixture();
  const backing = new Uint8Array(1024);
  const view = backing.subarray(256, 768);
  view.set(image(99));
  const pending = store.save(view, null);
  view.fill(1);
  const saved = await pending;
  store.close();
  const reopened = await Store.open(name);
  const loaded = await reopened.load();
  assert.equal(loaded.bytes.length, 512);
  assert.equal(loaded.bytes[100], 99);
  assert.equal(saved.revision, loaded.revision);
  assert.equal(saved.sha256, loaded.sha256);
  loaded.bytes[100] = 2;
  assert.equal((await reopened.load()).bytes[100], 99);
  reopened.close();
});

test("model: compare-and-swap rejects stale/null revisions and preserves the winner", async () => {
  const { name, store } = await fixture();
  const peer = await Store.open(name);
  const initial = await store.save(image(1), null);
  const results = await Promise.allSettled([
    store.save(image(2), initial.revision),
    peer.save(image(3), initial.revision),
  ]);
  assert.equal(results.filter((r) => r.status === "fulfilled").length, 1);
  assert.equal(
    results.filter(
      (r) => r.status === "rejected" && r.reason.code === "ERR_FSQLITE_SNAPSHOT_CONFLICT",
    ).length,
    1,
  );
  const loaded = await store.load();
  const winner = results.find((r) => r.status === "fulfilled").value;
  assert.equal(loaded.revision, winner.revision);
  assert.equal(loaded.parentRevision, initial.revision);
  await assert.rejects(peer.save(image(4), null), code("ERR_FSQLITE_SNAPSHOT_CONFLICT"));
  await assert.rejects(
    peer.save(image(4), initial.revision),
    code("ERR_FSQLITE_SNAPSHOT_CONFLICT"),
  );
  store.close();
  peer.close();
});

for (const field of [
  "format",
  "name",
  "revision",
  "parentRevision",
  "byteLength",
  "sha256",
  "bytes",
]) {
  test(`model: malformed ${field} fails closed without rewriting storage`, async () => {
    const { name, store } = await fixture();
    await store.save(image(), null);
    state(name).values.get("head")[field] = "invalid";
    await assert.rejects(store.load(), code("ERR_FSQLITE_SNAPSHOT_CORRUPT"));
    assert.equal(state(name).values.get("head")[field], "invalid");
    store.close();
  });
}

test("model: byte corruption is found by the actual Web Crypto digest", async () => {
  const { name, store } = await fixture();
  await store.save(image(), null);
  new Uint8Array(state(name).values.get("head").bytes)[200] ^= 1;
  await assert.rejects(store.load(), code("ERR_FSQLITE_SNAPSHOT_CORRUPT"));
  store.close();
});

test("model: invalid and over-limit inputs never enter a write transaction", async () => {
  const { store } = await fixture();
  const baseline = await store.save(image(2), null);
  for (const bytes of [new Uint8Array(), new Uint8Array(513), image().subarray(0, 511)]) {
    await assert.rejects(
      store.save(bytes, baseline.revision),
      code("ERR_FSQLITE_SNAPSHOT_CORRUPT"),
    );
  }
  await assert.rejects(
    store.save(new Uint8Array(MAX_SNAPSHOT_BYTES + 1), baseline.revision),
    code("ERR_FSQLITE_SNAPSHOT_TOO_LARGE"),
  );
  await assert.rejects(store.save(image(), "bad"), code("ERR_FSQLITE_SNAPSHOT_INPUT"));
  assert.equal((await store.load()).revision, baseline.revision);
  store.close();
});

test("model: request success followed by transaction abort never acknowledges a save", async () => {
  const { store } = await fixture();
  const baseline = await store.save(image(2), null);
  const original = ModelObjectStore.prototype.put;
  let succeeded = false;
  ModelObjectStore.prototype.put = function (...args) {
    const request = original.apply(this, args);
    request.addEventListener("success", () => {
      succeeded = true;
      this.transaction.abort();
    });
    return request;
  };
  try {
    await assert.rejects(store.save(image(3), baseline.revision));
  } finally {
    ModelObjectStore.prototype.put = original;
  }
  assert.equal(succeeded, true);
  assert.equal((await store.load()).revision, baseline.revision);
  store.close();
});

test("model: quota exception aborts replacement and permits an unchanged-revision retry", async () => {
  const { store } = await fixture();
  const baseline = await store.save(image(2), null);
  const original = ModelObjectStore.prototype.put;
  ModelObjectStore.prototype.put = () => {
    throw new DOMException("quota", "QuotaExceededError");
  };
  try {
    await assert.rejects(store.save(image(3), baseline.revision), { name: "QuotaExceededError" });
  } finally {
    ModelObjectStore.prototype.put = original;
  }
  assert.equal((await store.load()).bytes[100], 2);
  await store.save(image(4), baseline.revision);
  assert.equal((await store.load()).bytes[100], 4);
  store.close();
});

test("model: close during hashing prevents delayed write admission", async () => {
  const { name, store } = await fixture();
  const pending = store.save(image(), null);
  store.close();
  await assert.rejects(pending, code("ERR_FSQLITE_SNAPSHOT_CLOSED"));
  const next = await Store.open(name);
  assert.equal(await next.load(), null);
  next.close();
});

test("model: valid maximum-size page and different names have independent heads", async () => {
  const a = await fixture();
  const b = await fixture();
  await Promise.all([a.store.save(image(1, 65536), null), b.store.save(image(2), null)]);
  assert.equal((await a.store.load()).bytes.byteLength, 65536);
  assert.equal((await b.store.load()).bytes[100], 2);
  a.store.close();
  b.store.close();
});

test("model: unsupported stored schema rejects open without deleting it", async () => {
  const { name, store } = await fixture();
  store.close();
  state(name).stores.clear();
  await assert.rejects(Store.open(name), code("ERR_FSQLITE_SNAPSHOT_CORRUPT"));
  assert.equal(state(name).stores.size, 0);
});
