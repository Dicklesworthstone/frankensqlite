// Production storage/admission/host; Node SQLite and the explicit IDB model are
// test-only. This is not browser storage or a FrankenSQLite WASM certificate.
import assert from 'node:assert/strict';
import test from 'node:test';
import { setImmediate as turn } from 'node:timers/promises';
import { IndexedDbSnapshotStore } from '../src/snapshot-store.ts';
import { RequestBudget } from '../src/admission.ts';
import { installIndexedDbModel, ModelObjectStore, snapshotImage } from './helpers/indexeddb-model.mjs';
import { sqliteSnapshotWorker } from './helpers/snapshot-sqlite-core.mjs';

const token = () => crypto.randomUUID();
function ok(response, kind) { assert.equal(response.kind, kind, JSON.stringify(response)); return response.data; }
function error(response, code) { assert.equal(response.kind, 'error', JSON.stringify(response)); assert.equal(response.error.code, code); }
function deferred() { let resolve; const promise = new Promise(r => { resolve = r; }); return { promise, resolve }; }
async function fixture(t, hooks = {}, name = token()) {
  const model = installIndexedDbModel(), f = sqliteSnapshotWorker(hooks);
  let id = 0;
  const send = request => f.host.handle({ requestId: ++id, ...request });
  const ready = ok(await send({ kind: 'init', config: { persistence: 'indexeddb-snapshot', dbName: name } }), 'ready');
  t.after(() => send({ kind: 'close' }));
  await send({ kind: 'execute', sql: 'CREATE TABLE items(value INTEGER)' });
  const state = model.databases.get(`frankensqlite:snapshot:v1:${name}`);
  return { ...f, send, state, ready, name };
}
const publish = (send, publicationId) => send({ kind: 'checkpoint', publicationId });
const confirm = (send, publicationId, parentRevision) => send({ kind: 'checkpoint-recover', publicationId, parentRevision });

test('storage confirms an exact publication only after verifying stored bytes; no write or export', async t => {
  const f = await fixture(t); assert.equal(f.ready.checkpointRecovery, 1);
  await f.send({ kind: 'execute', sql: 'INSERT INTO items VALUES(7)' });
  const id = token(), saved = ok(await publish(f.send, id), 'checkpoint-result');
  assert.equal(saved.revision, id);
  const head = structuredClone(f.state.values.get('head')), before = [...f.events];
  t.mock.method(ModelObjectStore.prototype, 'put', () => { throw new Error('recovery may not write'); });
  const receipt = ok(await confirm(f.send, id, null), 'checkpoint-result');
  assert.deepEqual(receipt, saved); assert.deepEqual(f.events, before);
  assert.deepEqual(f.state.values.get('head'), head);
  assert.equal(f.host.requestQueue.pendingRequests, 0);
});

test('lost storage acknowledgement recovers the parent-held host and permits the next CAS', async t => {
  const f = await fixture(t), original = IndexedDbSnapshotStore.prototype.save;
  let once = true;
  t.mock.method(IndexedDbSnapshotStore.prototype, 'save', async function(...args) {
    const result = await original.apply(this, args);
    if (once) { once = false; throw new Error('committed but response lost'); }
    return result;
  });
  const id = token(); error(await publish(f.send, id), 'ERR_FSQLITE_WORKER');
  assert.equal(f.state.values.get('head').revision, id);
  ok(await confirm(f.send, id, null), 'checkpoint-result');
  const next = ok(await publish(f.send, token()), 'checkpoint-result');
  assert.equal(next.parentRevision, id);
});

test('recovering an acknowledged publication is read-only and repeatable', async t => {
  const f = await fixture(t), id = token();
  const saved = ok(await publish(f.send, id), 'checkpoint-result');
  for (let i = 0; i < 3; i++) assert.deepEqual(ok(await confirm(f.send, id, null), 'checkpoint-result'), saved);
  assert.equal(f.events.filter(e => e === 'export').length, 1);
});

test('readback cannot claim SQL executed after the captured checkpoint was saved', async t => {
  const f = await fixture(t), id = token();
  const saved = ok(await publish(f.send, id), 'checkpoint-result');
  await f.send({ kind: 'execute', sql: 'INSERT INTO items VALUES(8)' });
  const head = structuredClone(f.state.values.get('head'));
  assert.deepEqual(ok(await confirm(f.send, id, null), 'checkpoint-result'), saved);
  assert.deepEqual(f.state.values.get('head'), head);
  const rows = ok(await f.send({ kind: 'query', sql: 'SELECT * FROM items' }), 'query-result');
  assert.deepEqual(rows.rowArrays, [[8]]);
});

for (const mutation of ['absent', 'wrong-token', 'wrong-parent', 'bad-hash', 'bad-bytes', 'wrong-name']) {
  test(`recovery refuses ${mutation} without changing storage or host revision`, async t => {
    const f = await fixture(t), id = token();
    await publish(f.send, id);
    const head = f.state.values.get('head');
    if (mutation === 'absent') f.state.values.clear();
    if (mutation === 'wrong-token') head.revision = token();
    if (mutation === 'wrong-parent') head.parentRevision = token();
    if (mutation === 'bad-hash') head.sha256 = 'f'.repeat(64);
    if (mutation === 'bad-bytes') new Uint8Array(head.bytes)[100] ^= 1;
    if (mutation === 'wrong-name') head.name = 'different';
    const before = structuredClone(f.state.values), events = [...f.events];
    error(await confirm(f.send, id, null), ['bad-hash', 'bad-bytes', 'wrong-name'].includes(mutation)
      ? 'ERR_FSQLITE_SNAPSHOT_CORRUPT' : 'ERR_FSQLITE_SNAPSHOT_NOT_CONFIRMED');
    assert.deepEqual(f.state.values, before); assert.deepEqual(f.events, events);
  });
}

test('a newer local publication cannot be regressed by an old recovery request', async t => {
  const f = await fixture(t), first = token(), second = token();
  await publish(f.send, first); await publish(f.send, second);
  error(await confirm(f.send, first, null), 'ERR_FSQLITE_SNAPSHOT_NOT_CONFIRMED');
  const next = ok(await publish(f.send, token()), 'checkpoint-result'); assert.equal(next.parentRevision, second);
});

test('a different writer winning CAS is never adopted as this worker publication', async t => {
  const f = await fixture(t), mine = token();
  await publish(f.send, mine);
  const peer = await IndexedDbSnapshotStore.open(f.name); t.after(() => peer.close());
  const other = await peer.save(snapshotImage(), mine);
  error(await confirm(f.send, mine, null), 'ERR_FSQLITE_SNAPSHOT_NOT_CONFIRMED');
  error(await publish(f.send, token()), 'ERR_FSQLITE_SNAPSHOT_CONFLICT');
  assert.equal(f.state.values.get('head').revision, other.revision);
});

test('failed publication does not become success just because the parent still exists', async t => {
  const f = await fixture(t), parent = token(); await publish(f.send, parent);
  t.mock.method(ModelObjectStore.prototype, 'put', () => { throw new DOMException('full', 'QuotaExceededError'); });
  const id = token(); error(await publish(f.send, id), 'ERR_FSQLITE_WORKER');
  error(await confirm(f.send, id, parent), 'ERR_FSQLITE_SNAPSHOT_NOT_CONFIRMED');
  assert.equal(f.state.values.get('head').revision, parent);
});

test('recover holds the FIFO through asynchronous hashing and close', async t => {
  const f = await fixture(t), id = token(); await publish(f.send, id);
  const reached = deferred(), release = deferred(), original = crypto.subtle.digest.bind(crypto.subtle);
  t.mock.method(crypto.subtle, 'digest', async (...args) => { reached.resolve(); await release.promise; return original(...args); });
  const recovering = confirm(f.send, id, null); await reached.promise;
  const closed = f.send({ kind: 'close' }); let settled = false; closed.then(() => { settled = true; });
  await turn(); assert.equal(settled, false); assert.ok(!f.events.includes('close'));
  release.resolve(); ok(await recovering, 'checkpoint-result'); ok(await closed, 'close-result');
});

test('transport failure while hashing cannot resurrect a checkpoint revision', async t => {
  const f = await fixture(t), id = token(); await publish(f.send, id);
  const reached = deferred(), release = deferred(), original = crypto.subtle.digest.bind(crypto.subtle);
  t.mock.method(crypto.subtle, 'digest', async (...args) => { reached.resolve(); await release.promise; return original(...args); });
  const recovering = confirm(f.send, id, null); await reached.promise;
  const closing = f.host.failTransport(new Error('lost transport'));
  release.resolve(); error(await recovering, 'ERR_FSQLITE_WORKER'); await closing;
  assert.equal(f.state.values.get('head').revision, id);
});

test('managed ownership refuses recovery, including recovery using its own scope id', async t => {
  const f = await fixture(t), id = token(); await publish(f.send, id);
  ok(await f.send({ kind: 'transaction', action: 'begin', transactionId: '1' }), 'transaction-result');
  error(await confirm(f.send, id, null), 'ERR_FSQLITE_TRANSACTION_OWNERSHIP');
  error(await f.send({ kind: 'checkpoint-recover', publicationId: id, parentRevision: null, transactionId: '1' }),
    'ERR_FSQLITE_TRANSACTION_OWNERSHIP');
  ok(await f.send({ kind: 'transaction', action: 'rollback', transactionId: '1' }), 'transaction-result');
});

for (const input of [undefined, '', 'not-uuid', 7, null, 'a'.repeat(1000)]) {
  test(`admission refuses malformed recovery identity ${JSON.stringify(input)}`, () => {
    const budget = new RequestBudget();
    assert.throws(() => budget.admit({ kind: 'checkpoint-recover', requestId: 1, publicationId: input, parentRevision: null }));
    assert.equal(budget.stats.pendingRequests, 0); assert.equal(budget.stats.pendingBytes, 0);
  });
}

test('admission captures identity getters once and accounts only known fields', () => {
  const budget = new RequestBudget(), id = token(); let reads = 0;
  const lease = budget.admit({ kind: 'checkpoint-recover', requestId: 1,
    get publicationId() { reads++; return id; }, parentRevision: null, extra: new Uint8Array(10000) });
  assert.equal(reads, 1); assert.deepEqual(lease.request, { kind: 'checkpoint-recover', requestId: 1, publicationId: id, parentRevision: null });
  assert.ok(budget.stats.pendingBytes > 128); lease.release(); assert.equal(budget.stats.pendingBytes, 0);
});

test('ordinary legacy save still generates its own identity', async t => {
  installIndexedDbModel(); const store = await IndexedDbSnapshotStore.open(token()); t.after(() => store.close());
  const saved = await store.save(snapshotImage(), null);
  assert.deepEqual(await store.confirmPublication(saved.revision, null), saved);
  await assert.rejects(store.save(snapshotImage(), saved.revision, saved.revision), e => e.code === 'ERR_FSQLITE_SNAPSHOT_INPUT');
});
