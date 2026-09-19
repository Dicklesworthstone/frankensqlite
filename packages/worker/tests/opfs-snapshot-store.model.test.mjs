// Production storage implementation against a controllable OPFS/Web Locks
// model. Run: node --experimental-transform-types --test <this file>.
import { test } from "node:test";
import assert from "node:assert/strict";
import { register } from "node:module";
import { installOpfsModel, databaseImage, deferred } from "./helpers/opfs-model.mjs";

// Match the package's bundler-style extensionless TypeScript imports in Node.
register(`data:text/javascript,${encodeURIComponent(`
  export async function resolve(specifier, context, next) {
    try { return await next(specifier, context); }
    catch (error) {
      if (error.code === "ERR_MODULE_NOT_FOUND" && specifier.startsWith(".") && !/\\.[^/]+$/.test(specifier)) {
        return next(specifier + ".ts", context);
      }
      throw error;
    }
  }
`)}`);
const { OpfsSnapshotStore } = await import("../src/opfs-snapshot-store.ts");
const { MAX_SNAPSHOT_BYTES } = await import("../src/snapshot-store.ts");
const code = suffix => ({ code: `ERR_FSQLITE_SNAPSHOT_${suffix}` });
const fixture = t => { const model = installOpfsModel(); t.after(() => model.restore()); return model; };
const fileKey = model => { assert.equal(model.files.size, 1); return [...model.files.keys()][0]; };

function rewriteEnvelope(bytes, change) {
  const length = new DataView(bytes.buffer, bytes.byteOffset).getUint32(8);
  const value = JSON.parse(new TextDecoder().decode(bytes.slice(12, 12 + length)));
  change(value);
  const header = new TextEncoder().encode(JSON.stringify(value));
  const result = new Uint8Array(12 + header.length + bytes.length - 12 - length);
  result.set(bytes.slice(0, 8)); new DataView(result.buffer).setUint32(8, header.length);
  result.set(header, 12); result.set(bytes.slice(12 + length), 12 + header.length);
  return result;
}

test("OPFS checkpoints reopen complete images and revision lineage", async t => {
  const m = fixture(t), a = await OpfsSnapshotStore.open("notes/日本語");
  assert.equal(await a.load(), null);
  const one = await a.save(databaseImage(3), null);
  const two = await a.save(databaseImage(7, 65536), one.revision);
  assert.equal(two.parentRevision, one.revision); assert.equal(two.byteLength, 65536);
  assert.match(two.sha256, /^[a-f0-9]{64}$/);
  a.close();
  const b = await OpfsSnapshotStore.open("notes/日本語");
  const saved = await b.load();
  assert.deepEqual(saved.bytes, databaseImage(7, 65536)); assert.equal(saved.revision, two.revision);
  assert.equal(m.counts.publications, 2); b.close();
});

test("OPFS stale and create-only writers cannot overwrite the winning checkpoint", async t => {
  const m = fixture(t), a = await OpfsSnapshotStore.open("shared"), b = await OpfsSnapshotStore.open("shared");
  const first = await a.save(databaseImage(), null);
  const next = await a.save(databaseImage(2), first.revision);
  await assert.rejects(b.save(databaseImage(3), first.revision), code("CONFLICT"));
  await assert.rejects(b.save(databaseImage(3), null), code("CONFLICT"));
  assert.equal((await b.load()).revision, next.revision); assert.equal(m.counts.publications, 2);
});

test("OPFS concurrent publications have exactly one CAS winner", async t => {
  const m = fixture(t);
  const stores = await Promise.all(Array.from({ length: 12 }, () => OpfsSnapshotStore.open("race")));
  const results = await Promise.allSettled(stores.map((s, i) => s.save(databaseImage(i), null)));
  assert.equal(results.filter(r => r.status === "fulfilled").length, 1);
  for (const r of results.filter(r => r.status === "rejected")) assert.equal(r.reason.code, code("CONFLICT").code);
  assert.equal(m.counts.publications, 1);
});

test("OPFS lock spans stream close but unrelated names stay independent", async t => {
  const m = fixture(t), a = await OpfsSnapshotStore.open("a"), b = await OpfsSnapshotStore.open("b");
  const entered = deferred(), release = deferred();
  m.hooks.beforeClose = async () => { delete m.hooks.beforeClose; entered.resolve(); await release.promise; };
  const saving = a.save(databaseImage(1), null); await entered.promise;
  let loaded = false;
  const reading = a.load().then(value => { loaded = true; return value; });
  await b.save(databaseImage(2), null);
  assert.equal(loaded, false);
  release.resolve(); await saving;
  assert.equal((await reading).bytes[100], 1);
});

test("OPFS captures caller-owned views before asynchronous hashing", async t => {
  fixture(t); const a = await OpfsSnapshotStore.open("copy");
  const backing = new Uint8Array(1024); backing.set(databaseImage(9), 100);
  const view = backing.subarray(100, 612), expected = new Uint8Array(view);
  const pending = a.save(view, null); backing.fill(255); await pending;
  assert.deepEqual((await a.load()).bytes, expected); assert.equal(backing.byteLength, 1024);
});

for (const phase of ["create", "write", "beforeClose"]) {
  test(`OPFS ${phase} failure preserves an existing head and allows retry`, async t => {
    const m = fixture(t), a = await OpfsSnapshotStore.open(phase);
    const saved = await a.save(databaseImage(4), null), key = fileKey(m), original = m.files.get(key).slice();
    const failure = new DOMException("Storage full", "QuotaExceededError");
    m.hooks[phase] = () => { throw failure; };
    await assert.rejects(a.save(databaseImage(5), saved.revision), error => error === failure);
    assert.deepEqual(m.files.get(key), original); delete m.hooks[phase];
    await a.save(databaseImage(6), saved.revision); assert.equal((await a.load()).bytes[100], 6);
  });
  test(`OPFS initial ${phase} failure removes only its new empty staging entry`, async t => {
    const m = fixture(t), a = await OpfsSnapshotStore.open(phase);
    m.hooks[phase] = () => { throw new Error("injected"); };
    await assert.rejects(a.save(databaseImage(), null), /injected/);
    assert.equal(m.files.size, 0); delete m.hooks[phase];
    await a.save(databaseImage(8), null); assert.equal((await a.load()).bytes[100], 8);
  });
}

test("OPFS lost close acknowledgement is confirmed without rewriting or replay", async t => {
  const m = fixture(t), a = await OpfsSnapshotStore.open("receipt");
  const previous = await a.save(databaseImage(1), null), revision = crypto.randomUUID();
  m.hooks.afterClose = () => { throw new Error("lost acknowledgement"); };
  await assert.rejects(a.save(databaseImage(2), previous.revision, revision), /lost acknowledgement/);
  const count = m.counts.publications;
  const receipt = await a.confirmPublication(revision, previous.revision);
  assert.equal(receipt.revision, revision); assert.ok(Object.isFrozen(receipt));
  assert.equal(m.counts.publications, count); assert.equal((await a.load()).bytes[100], 2);
  await assert.rejects(a.confirmPublication(crypto.randomUUID(), previous.revision), code("NOT_CONFIRMED"));
  await assert.rejects(a.confirmPublication(revision, null), code("NOT_CONFIRMED"));
});

test("OPFS lost initial receipt is retained rather than deleted", async t => {
  const m = fixture(t), a = await OpfsSnapshotStore.open("first-receipt"), revision = crypto.randomUUID();
  m.hooks.afterClose = () => { throw new Error("lost acknowledgement"); };
  await assert.rejects(a.save(databaseImage(8), null, revision));
  assert.equal(m.counts.removals, 0);
  assert.equal((await a.confirmPublication(revision, null)).revision, revision);
});

test("OPFS corruption and malformed envelopes cannot be loaded or overwritten", async t => {
  const m = fixture(t), a = await OpfsSnapshotStore.open("corrupt");
  const saved = await a.save(databaseImage(), null), key = fileKey(m), original = m.files.get(key).slice();
  const badHash = original.slice(); badHash[badHash.length - 1] ^= 1;
  const badLength = original.slice(); new DataView(badLength.buffer).setUint32(8, 0xFFFFFFFF);
  const badMagic = original.slice(); badMagic[0] ^= 1;
  const cases = [new Uint8Array(), original.slice(0, 10), original.slice(0, -1), badHash, badLength, badMagic,
    ...[e => { e.name = "other"; }, e => { e.format = 2; }, e => { e.byteLength++; },
      e => { e.parentRevision = e.revision; }, e => { e.sha256 = "x"; }, e => { e.revision = "bad"; }]
      .map(change => rewriteEnvelope(original, change))];
  for (const bytes of cases) {
    m.files.set(key, bytes);
    await assert.rejects(a.load(), code("CORRUPT"));
    await assert.rejects(a.save(databaseImage(9), saved.revision), code("CORRUPT"));
    assert.equal(m.files.get(key), bytes);
  }
  assert.equal(m.counts.publications, 1); assert.equal(m.counts.removals, 0);
});

test("OPFS bounds and identities reject before any file publication", async t => {
  const m = fixture(t), a = await OpfsSnapshotStore.open("validation");
  for (const name of ["", " ", ":memory:", "a\0b", "x".repeat(257), null]) {
    await assert.rejects(OpfsSnapshotStore.open(name), code("INPUT"));
  }
  for (const [parent, revision] of [[undefined, undefined], ["bad", undefined], [null, "bad"]]) {
    await assert.rejects(a.save(databaseImage(), parent, revision), code("INPUT"));
  }
  const id = crypto.randomUUID();
  await assert.rejects(a.save(databaseImage(), id, id), code("INPUT"));
  await assert.rejects(a.confirmPublication(undefined, null), code("INPUT"));
  await assert.rejects(a.confirmPublication(id, id), code("INPUT"));
  await assert.rejects(a.save(new Uint8Array(MAX_SNAPSHOT_BYTES + 1), null), code("TOO_LARGE"));
  await assert.rejects(a.save(new Uint8Array(512), null), code("CORRUPT"));
  await assert.rejects(a.save([1, 2], null), code("INPUT"));
  assert.equal(m.files.size, 0);
});

test("OPFS names preserve distinct unpaired UTF-16 surrogates", async t => {
  const m = fixture(t), a = await OpfsSnapshotStore.open("\ud800"), b = await OpfsSnapshotStore.open("\ud801");
  await a.save(databaseImage(1), null); await b.save(databaseImage(2), null);
  assert.equal(m.files.size, 2); assert.equal((await a.load()).bytes[100], 1); assert.equal((await b.load()).bytes[100], 2);
});

test("OPFS close fences queued work without interrupting an admitted publication", async t => {
  const m = fixture(t), a = await OpfsSnapshotStore.open("closed"), entered = deferred(), release = deferred();
  m.hooks.beforeClose = async () => { entered.resolve(); await release.promise; };
  const saving = a.save(databaseImage(2), null); await entered.promise;
  const reading = a.load(); const rejected = assert.rejects(reading, code("CLOSED"));
  a.close(); a.close(); release.resolve(); const saved = await saving; await rejected;
  await assert.rejects(a.load(), code("CLOSED")); await assert.rejects(a.save(databaseImage(), saved.revision), code("CLOSED"));
  const b = await OpfsSnapshotStore.open("closed"); assert.equal((await b.load()).revision, saved.revision);
});

test("OPFS missing browser capabilities fail explicitly without memory fallback", async t => {
  fixture(t); Object.defineProperty(globalThis, "navigator", { configurable: true, value: {} });
  await assert.rejects(OpfsSnapshotStore.open("unavailable"), code("UNAVAILABLE"));
});
