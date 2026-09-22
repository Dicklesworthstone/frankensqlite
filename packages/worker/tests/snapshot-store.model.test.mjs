// Transactional reference-model tests. These do NOT certify browser behavior.
// Run with Node's TypeScript loader or a runner supporting .ts imports.

import assert from "node:assert/strict";
import { test } from "node:test";
import {
  MAX_SNAPSHOT_BYTES,
  IndexedDbSnapshotStore as Store,
  validateSnapshotBytes,
} from "../src/snapshot-store.ts";
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

for (const [label, damage] of [
  [
    "payload",
    (record) => {
      new Uint8Array(record.bytes)[200] ^= 1;
    },
  ],
  [
    "digest",
    (record) => {
      record.sha256 = "0".repeat(64);
    },
  ],
  [
    "self-parent lineage",
    (record) => {
      record.parentRevision = record.revision;
    },
  ],
]) {
  test(`model: ${label} corruption cannot be hidden by a replacement checkpoint`, async () => {
    const { name, store } = await fixture();
    try {
      const saved = await store.save(image(1), null);
      damage(state(name).values.get("head"));
      const damaged = structuredClone(state(name).values.get("head"));
      await assert.rejects(store.load(), code("ERR_FSQLITE_SNAPSHOT_CORRUPT"));
      await assert.rejects(
        store.save(image(2), saved.revision),
        code("ERR_FSQLITE_SNAPSHOT_CORRUPT"),
      );
      assert.deepEqual(state(name).values.get("head"), damaged);
    } finally {
      store.close();
    }
  });
}

for (const [label, damage] of [
  [
    "payload",
    (record) => {
      new Uint8Array(record.bytes)[200] ^= 1;
    },
  ],
  [
    "digest",
    (record) => {
      record.sha256 = "0".repeat(64);
    },
  ],
  [
    "parent",
    (record) => {
      record.parentRevision = crypto.randomUUID();
    },
  ],
]) {
  test(`model: ${label} mutation between verification and CAS aborts publication`, async () => {
    const { name, store } = await fixture();
    const saved = await store.save(image(1), null);
    const original = ModelObjectStore.prototype.get;
    let damaged;
    ModelObjectStore.prototype.get = function (key) {
      if (this.transaction.mode === "readwrite") {
        damage(state(name).values.get("head"));
        damaged = structuredClone(state(name).values.get("head"));
      }
      return original.call(this, key);
    };
    try {
      await assert.rejects(
        store.save(image(2), saved.revision),
        code("ERR_FSQLITE_SNAPSHOT_CORRUPT"),
      );
      assert.ok(damaged, "the write-time mutation must have been exercised");
      assert.deepEqual(state(name).values.get("head"), damaged);
    } finally {
      ModelObjectStore.prototype.get = original;
      store.close();
    }
  });
}

test("model: a new revision between verification and CAS preserves the competing head", async () => {
  const { name, store } = await fixture();
  const saved = await store.save(image(1), null);
  const original = ModelObjectStore.prototype.get;
  let winner;
  ModelObjectStore.prototype.get = function (key) {
    if (this.transaction.mode === "readwrite") {
      const head = state(name).values.get("head");
      head.parentRevision = saved.revision;
      head.revision = crypto.randomUUID();
      winner = structuredClone(head);
    }
    return original.call(this, key);
  };
  try {
    await assert.rejects(
      store.save(image(2), saved.revision),
      code("ERR_FSQLITE_SNAPSHOT_CONFLICT"),
    );
    assert.ok(winner, "the competing publication must have been exercised");
    assert.deepEqual(state(name).values.get("head"), winner);
  } finally {
    ModelObjectStore.prototype.get = original;
    store.close();
  }
});

test("model: close during head verification prevents delayed publication", async () => {
  const { name, store } = await fixture();
  const saved = await store.save(image(1), null);
  const original = ModelObjectStore.prototype.get;
  let closed = false;
  ModelObjectStore.prototype.get = function (key) {
    const request = original.call(this, key);
    if (this.transaction.mode === "readonly") {
      request.addEventListener("success", () => {
        closed = true;
        store.close();
      });
    }
    return request;
  };
  try {
    await assert.rejects(
      store.save(image(2), saved.revision),
      code("ERR_FSQLITE_SNAPSHOT_CLOSED"),
    );
    assert.equal(closed, true);
    assert.equal(state(name).values.get("head").revision, saved.revision);
  } finally {
    ModelObjectStore.prototype.get = original;
    store.close();
  }
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

// Header fixtures, not complete B-trees. Exercise the shared image validator
// used by both persistence backends without claiming SQL integrity checking.
function countedImage(pageSize, pages = 3) {
  const bytes = new Uint8Array(pageSize * pages);
  bytes.set(image(17, pageSize));
  const header = new DataView(bytes.buffer);
  header.setUint32(24, 0x12345678);
  header.setUint32(28, pages);
  header.setUint32(92, 0x12345678);
  return bytes;
}

for (const pageSize of [512, 1024, 2048, 4096, 8192, 16384, 32768, 65536]) {
  test(`model: ${pageSize}-byte pages cannot publish a whole-page-truncated image`, async () => {
    const { store } = await fixture();
    try {
      const bytes = countedImage(pageSize);
      // The backing buffer still contains the last page; this view does not.
      const truncated = bytes.subarray(0, bytes.byteLength - pageSize);
      await assert.rejects(store.save(truncated, null), code("ERR_FSQLITE_SNAPSHOT_CORRUPT"));
      assert.equal(await store.load(), null);
      const saved = await store.save(bytes, null);
      await assert.rejects(
        store.save(truncated, saved.revision),
        code("ERR_FSQLITE_SNAPSHOT_CORRUPT"),
      );
      const retained = await store.load();
      assert.equal(retained.revision, saved.revision);
      assert.deepEqual(retained.bytes, bytes);
    } finally {
      store.close();
    }
  });

  test(`model: ${pageSize}-byte pages retain SQLite's legacy size fallback`, async () => {
    const { store } = await fixture();
    try {
      const bytes = countedImage(pageSize);
      const header = new DataView(bytes.buffer);
      let revision = null;
      // An unavailable/stale count falls back to the actual file size. A
      // smaller authoritative count may ignore trailing complete pages.
      for (const [pages, validFor] of [
        [0, 0x12345678],
        [0xffffffff, 0x12345679],
        [1, 0x12345678],
        [3, 0x12345678],
      ]) {
        header.setUint32(28, pages);
        header.setUint32(92, validFor);
        const saved = await store.save(bytes, revision);
        revision = saved.revision;
        assert.deepEqual((await store.load()).bytes, bytes);
      }
    } finally {
      store.close();
    }
  });
}

test("image validation reads the view's header and length, not its backing buffer", () => {
  const backing = new Uint8Array(512 * 3 + 256).fill(255);
  const bytes = backing.subarray(128, 128 + 512 * 3);
  bytes.set(countedImage(512));
  assert.doesNotThrow(() => validateSnapshotBytes(bytes));
  assert.throws(
    () => validateSnapshotBytes(bytes.subarray(0, 1024)),
    code("ERR_FSQLITE_SNAPSHOT_CORRUPT"),
  );
});

test("image page counts remain unsigned and equal zero change counters are authoritative", () => {
  const bytes = countedImage(512);
  const header = new DataView(bytes.buffer);
  header.setUint32(24, 0);
  header.setUint32(92, 0);
  assert.doesNotThrow(() => validateSnapshotBytes(bytes));
  for (const pages of [4, 0x80000000, 0xffffffff]) {
    header.setUint32(28, pages);
    assert.throws(() => validateSnapshotBytes(bytes), code("ERR_FSQLITE_SNAPSHOT_CORRUPT"));
  }
});

test("model: matching envelope checksum cannot confirm or replace a truncated database", async () => {
  const { name, store } = await fixture();
  try {
    const bytes = countedImage(512);
    const saved = await store.save(bytes, null);
    const truncated = bytes.slice(0, 1024);
    const record = state(name).values.get("head");
    record.bytes = truncated.buffer;
    record.byteLength = truncated.byteLength;
    const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", truncated));
    record.sha256 = [...digest].map((byte) => byte.toString(16).padStart(2, "0")).join("");
    const damaged = structuredClone(record);
    await assert.rejects(store.load(), code("ERR_FSQLITE_SNAPSHOT_CORRUPT"));
    await assert.rejects(
      store.confirmPublication(saved.revision, saved.parentRevision),
      code("ERR_FSQLITE_SNAPSHOT_CORRUPT"),
    );
    await assert.rejects(
      store.save(bytes, saved.revision),
      code("ERR_FSQLITE_SNAPSHOT_CORRUPT"),
    );
    assert.deepEqual(state(name).values.get("head"), damaged);
  } finally {
    store.close();
  }
});
