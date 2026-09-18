// Storage-level publication identity and read-only receipt reconstruction.
// Uses production store + the explicit IndexedDB model, NOT browser storage.
import assert from 'node:assert/strict';
import test from 'node:test';
import { setImmediate as turn } from 'node:timers/promises';
import { IndexedDbSnapshotStore } from '../src/snapshot-store.ts';
import { installIndexedDbModel, ModelObjectStore, snapshotImage } from './helpers/indexeddb-model.mjs';
const token = () => crypto.randomUUID();
async function fixture(t) {
  const model = installIndexedDbModel(), name = token();
  const store = await IndexedDbSnapshotStore.open(name); t.after(() => store.close());
  return { store, state: model.databases.get(`frankensqlite:snapshot:v1:${name}`), name };
}
test('provided publication identity persists and reconstructs immutable metadata without writes', async t => {
  const { store, state } = await fixture(t), id = token();
  const saved = await store.save(snapshotImage(7), null, id);
  assert.equal(saved.revision, id); assert.equal(saved.parentRevision, null);
  const before = structuredClone(state.values);
  t.mock.method(ModelObjectStore.prototype, 'put', () => { throw new Error('no writes during confirmation'); });
  const confirmed = await store.confirmPublication(id, null);
  assert.deepEqual(confirmed, saved); assert.ok(Object.isFrozen(confirmed));
  assert.deepEqual(state.values, before);
});
test('legacy calls still generate identities and preserve compare-and-swap', async t => {
  const { store } = await fixture(t);
  const first = await store.save(snapshotImage(), null);
  const second = await store.save(snapshotImage(8), first.revision);
  assert.notEqual(first.revision, second.revision);
  assert.deepEqual(await store.confirmPublication(second.revision, first.revision), second);
  await assert.rejects(store.save(snapshotImage(), first.revision, token()), e => e.code === 'ERR_FSQLITE_SNAPSHOT_CONFLICT');
});
for (const mutation of ['absent', 'identity', 'parent', 'name', 'bytes', 'checksum']) {
  test(`confirmation refuses ${mutation} without replacing the stored head`, async t => {
    const { store, state } = await fixture(t), id = token(); await store.save(snapshotImage(), null, id);
    const record = state.values.get('head');
    if (mutation === 'absent') state.values.clear();
    if (mutation === 'identity') record.revision = token();
    if (mutation === 'parent') record.parentRevision = token();
    if (mutation === 'name') record.name = 'another-database';
    if (mutation === 'bytes') new Uint8Array(record.bytes)[100] ^= 1;
    if (mutation === 'checksum') record.sha256 = 'f'.repeat(64);
    const before = structuredClone(state.values);
    await assert.rejects(store.confirmPublication(id, null), e => e.code ===
      (['name', 'bytes', 'checksum'].includes(mutation) ? 'ERR_FSQLITE_SNAPSHOT_CORRUPT' : 'ERR_FSQLITE_SNAPSHOT_NOT_CONFIRMED'));
    assert.deepEqual(state.values, before);
  });
}
for (const invalid of ['', null, 3, 'not-a-token', '00000000-0000-1000-8000-000000000000']) {
  test(`invalid identity ${JSON.stringify(invalid)} cannot write`, async t => {
    const { store, state } = await fixture(t);
    await assert.rejects(store.save(snapshotImage(), null, invalid), e => e.code === 'ERR_FSQLITE_SNAPSHOT_INPUT');
    await assert.rejects(store.confirmPublication(invalid, null), e => e.code === 'ERR_FSQLITE_SNAPSHOT_INPUT');
    assert.equal(state.values.size, 0);
  });
}
test('publication must not reuse its parent identity', async t => {
  const { store } = await fixture(t), id = token(); await store.save(snapshotImage(), null, id);
  await assert.rejects(store.save(snapshotImage(), id, id), e => e.code === 'ERR_FSQLITE_SNAPSHOT_INPUT');
  await assert.rejects(store.confirmPublication(id, id), e => e.code === 'ERR_FSQLITE_SNAPSHOT_INPUT');
});
test('put success followed by abort does not produce a confirmable publication', async t => {
  const { store, state } = await fixture(t), original = ModelObjectStore.prototype.put, id = token();
  t.mock.method(ModelObjectStore.prototype, 'put', function(...args) {
    const request = original.apply(this, args);
    request.onsuccess = () => this.transaction.abort();
    return request;
  });
  await assert.rejects(store.save(snapshotImage(), null, id));
  await assert.rejects(store.confirmPublication(id, null), e => e.code === 'ERR_FSQLITE_SNAPSHOT_NOT_CONFIRMED');
  assert.equal(state.values.size, 0);
});
test('confirmation waits for the complete read and asynchronous digest', async t => {
  const { store } = await fixture(t), id = token(); await store.save(snapshotImage(), null, id);
  let release, entered;
  const blocked = new Promise(r => { release = r; }), ready = new Promise(r => { entered = r; });
  const digest = crypto.subtle.digest.bind(crypto.subtle);
  t.mock.method(crypto.subtle, 'digest', async (...args) => { entered(); await blocked; return digest(...args); });
  let settled = false; const pending = store.confirmPublication(id, null).then(r => { settled = true; return r; });
  await ready; await turn(); assert.equal(settled, false); release(); assert.equal((await pending).revision, id);
});
test('a superseding writer is not mistaken for the original publication', async t => {
  const { store, name } = await fixture(t), id = token(); await store.save(snapshotImage(), null, id);
  const peer = await IndexedDbSnapshotStore.open(name); t.after(() => peer.close());
  const winner = await peer.save(snapshotImage(42), id, token());
  await assert.rejects(store.confirmPublication(id, null), e => e.code === 'ERR_FSQLITE_SNAPSHOT_NOT_CONFIRMED');
  assert.equal((await peer.load()).revision, winner.revision);
});
