// Actual snapshot stores and validation with the retained storage models.
// Tests input ownership/admission, not SQLite integrity or browser durability.
import assert from "node:assert/strict";
import { register } from "node:module";
import { test } from "node:test";
import { runInNewContext } from "node:vm";
import { databaseImage, installOpfsModel } from "./helpers/opfs-model.mjs";
import { installIndexedDbModel } from "./helpers/indexeddb-model.mjs";

register(`data:text/javascript,${encodeURIComponent(`
  export async function resolve(specifier, context, next) {
    try { return await next(specifier, context); }
    catch (error) {
      if (error.code === "ERR_MODULE_NOT_FOUND" && specifier.startsWith(".") && !/\\.[^/]+$/.test(specifier))
        return next(specifier + ".ts", context);
      throw error;
    }
  }
`)}`);
const { IndexedDbSnapshotStore, MAX_SNAPSHOT_BYTES, validateSnapshotBytes } = await import("../src/snapshot-store.ts");
const { OpfsSnapshotStore } = await import("../src/opfs-snapshot-store.ts");
const code = suffix => ({ code: `ERR_FSQLITE_SNAPSHOT_${suffix}` });

async function fixture(t, kind) {
  if (kind === "OPFS") {
    const model = installOpfsModel();
    const store = await OpfsSnapshotStore.open(crypto.randomUUID());
    t.after(() => { store.close(); model.restore(); });
    return { store, count: () => model.counts.publications };
  }
  const original = ["indexedDB", "IDBObjectStore"].map(key => [key, Object.getOwnPropertyDescriptor(globalThis, key)]);
  const model = installIndexedDbModel(), store = await IndexedDbSnapshotStore.open(crypto.randomUUID());
  t.after(() => {
    store.close();
    for (const [key, descriptor] of original) {
      if (descriptor) Object.defineProperty(globalThis, key, descriptor);
      else Reflect.deleteProperty(globalThis, key);
    }
  });
  return { store, count: () => [...model.databases.values()].reduce((n, db) => n + db.values.size, 0) };
}

for (const kind of ["OPFS", "IDB"]) {
  test(`${kind} ignores shadowed typed-array metadata without invoking getters`, async t => {
    const { store } = await fixture(t, kind);
    const backing = new Uint8Array(2048), expected = databaseImage(37);
    backing.set(expected, 123);
    const input = backing.subarray(123, 635);
    let invoked = 0;
    for (const key of ["buffer", "byteOffset", "byteLength", "length", Symbol.toStringTag, Symbol.iterator]) {
      Object.defineProperty(input, key, { get() { invoked++; throw new Error("application getter invoked"); } });
    }
    const pending = store.save(input, null);
    backing.fill(0);
    const receipt = await pending;
    assert.equal(invoked, 0);
    assert.equal(receipt.byteLength, 512);
    assert.deepEqual((await store.load()).bytes, expected);
  });

  for (const [label, make] of [
    ["shared", () => new Uint8Array(new SharedArrayBuffer(512))],
    ["resizable", () => new Uint8Array(new ArrayBuffer(512, { maxByteLength: 1024 }))],
    ["detached", () => { const b = databaseImage(); structuredClone(b.buffer, { transfer: [b.buffer] }); return b; }],
    ["proxy", () => new Proxy(databaseImage(), {})],
    ["forged", () => Object.create(Uint8Array.prototype)],
  ]) {
    test(`${kind} rejects ${label} snapshot storage before publication`, async t => {
      const { store, count } = await fixture(t, kind), input = make();
      if (label === "shared" || label === "resizable") input.set(databaseImage());
      await assert.rejects(store.save(input, null), code("INPUT"));
      assert.equal(count(), 0);
      const valid = await store.save(databaseImage(8), null);
      assert.equal((await store.load()).revision, valid.revision);
    });
  }

  test(`${kind} cannot validate a fake buffer and then persist truncated real bytes`, async t => {
    const { store, count } = await fixture(t, kind), input = databaseImage();
    new DataView(input.buffer).setUint32(28, 2); // the second page is absent
    Object.defineProperty(input, "buffer", { value: databaseImage().buffer });
    await assert.rejects(store.save(input, null), code("CORRUPT"));
    assert.equal(count(), 0);
  });

  test(`${kind} supports real cross-realm Uint8Array and ordinary Buffer views`, async t => {
    const { store } = await fixture(t, kind);
    const other = runInNewContext("new Uint8Array(512)");
    other.set(databaseImage(3));
    const first = await store.save(other, null);
    const bytes = Buffer.from(databaseImage(9));
    const saved = await store.save(bytes.subarray(0), first.revision);
    assert.deepEqual((await store.load()).bytes, databaseImage(9));
    assert.equal(saved.parentRevision, first.revision);
  });
}

test("validator enforces the true size before copying even with shadowed length", () => {
  const input = new Uint8Array(MAX_SNAPSHOT_BYTES + 512);
  input.set(databaseImage());
  Object.defineProperty(input, "byteLength", { value: 512 });
  assert.throws(() => validateSnapshotBytes(input), code("TOO_LARGE"));
});

test("IDB cannot store an oversized image that its own load will reject", async t => {
  const { store, count } = await fixture(t, "IDB");
  const input = new Uint8Array(MAX_SNAPSHOT_BYTES + 512);
  input.set(databaseImage());
  Object.defineProperty(input, "byteLength", { value: 512 });
  await assert.rejects(store.save(input, null), code("TOO_LARGE"));
  assert.equal(count(), 0);
});

test("validator rejects other typed-array brands despite a forged Uint8Array tag", () => {
  for (const input of [new Int8Array(512), new Uint8ClampedArray(512), new DataView(new ArrayBuffer(512))]) {
    Object.defineProperty(input, Symbol.toStringTag, { value: "Uint8Array" });
    assert.throws(() => validateSnapshotBytes(input), code("INPUT"));
  }
});
