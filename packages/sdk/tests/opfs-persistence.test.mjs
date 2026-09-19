// Production SDK + WorkerConnectionHost + snapshot stores. SQL runs against
// Node SQLite and browser storage uses deterministic models, NOT WASM/OPFS.
// node --experimental-loader=./packages/sdk/tests/helpers/source-loader.mjs \
//   --test packages/sdk/tests/opfs-persistence.test.mjs
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { FrankenDB } from '../src/database.ts';
import { FrankenDBQueue, FrankenCheckpointCommitError } from '../src/queue.ts';
import { FrankenWorkerClient } from '../src/worker-client.ts';
import { sqliteSnapshotWorker } from '../../worker/tests/helpers/snapshot-sqlite-core.mjs';
import { installOpfsModel, deferred } from '../../worker/tests/helpers/opfs-model.mjs';

function fixture(t) {
  const model = installOpfsModel();
  const connections = [];
  t.after(async () => {
    // Close through the host even when an invalid ready response made the SDK
    // dispose its transport. A test conduit is not an actual browser worker.
    for (const connection of connections) {
      await connection.host.handle({ kind: 'close', requestId: 0 });
      connection.worker.terminate();
    }
    model.restore();
  });
  function connection(hooks) {
    const value = sqliteSnapshotWorker(hooks);
    connections.push(value);
    return value;
  }
  async function open(name = 'documents', options = {}, hooks) {
    const value = connection(hooks);
    return { ...value, db: await FrankenDB.open({
      dbName: name, persistence: 'opfs-snapshot', ...options, worker: value.worker,
    }) };
  }
  async function queue(name = 'documents', options = {}, hooks) {
    const value = connection(hooks);
    return { ...value, db: await FrankenDBQueue.open({
      dbName: name, persistence: 'opfs-snapshot', worker: value.worker,
    }, { checkpointOnCommit: true, ...options }) };
  }
  return { model, connection, open, queue };
}

const schema = 'CREATE TABLE documents(id INTEGER PRIMARY KEY, body TEXT NOT NULL, data BLOB)';
const rows = db => db.query('SELECT id, body, data FROM documents ORDER BY id');
const sqlCounts = events => events.filter(sql => sql.startsWith('INSERT')).length;

test('OPFS public database opens, publishes and restores real SQLite bytes', async t => {
  const { model, open } = fixture(t);
  const first = await open('folder/name 🚀', { resultEncoding: 'binary' });
  assert.equal(first.db.persistence, 'opfs-snapshot');
  assert.equal(first.db.path, 'folder/name 🚀');
  assert.equal(first.db.snapshotRevision, null);
  assert.equal(first.db.checkpointRecoverySupported, true);
  await first.db.execute(schema);
  await first.db.execute('INSERT INTO documents VALUES (?, ?, ?)', [1, 'saved 🚀', new Uint8Array([0, 1, 255])]);
  const receipt = await first.db.checkpoint();
  assert.equal(receipt.parentRevision, null);
  assert.equal(first.db.snapshotRevision, receipt.revision);
  assert.equal(model.counts.publications, 1);
  await first.db.close();
  const reopened = await open('folder/name 🚀');
  assert.equal(reopened.db.snapshotRevision, receipt.revision);
  assert.deepEqual(reopened.counts(), { creates: 0, imports: 1 });
  assert.deepEqual((await rows(reopened.db)).rowArrays, [[1, 'saved 🚀', new Uint8Array([0, 1, 255])]]);
  assert.deepEqual((await reopened.db.query('PRAGMA integrity_check')).rowArrays, [['ok']]);
});

test('OPFS queued transactions acknowledge only their published commit', async t => {
  const { model, queue, open } = fixture(t);
  const first = await queue();
  assert.equal(first.db.checkpointOnCommit, true);
  await first.db.transaction(async tx => {
    await tx.execute(schema);
    await tx.execute('INSERT INTO documents VALUES (1, ?, NULL)', ['first']);
  });
  const revision = first.db.snapshotRevision;
  assert.equal(model.counts.publications, 1);
  const failed = new Error('rollback the entire callback');
  await assert.rejects(first.db.transaction(async tx => {
    await tx.execute('INSERT INTO documents VALUES (2, ?, NULL)', ['not saved']);
    throw failed;
  }), error => error === failed);
  assert.equal(model.counts.publications, 1);
  await first.db.close();
  const reopened = await open();
  assert.equal(reopened.db.snapshotRevision, revision);
  assert.deepEqual((await rows(reopened.db)).rowArrays, [[1, 'first', null]]);
});

test('OPFS queue holds later SQL and close behind an in-flight publication', async t => {
  const { model, queue } = fixture(t);
  const first = await queue();
  await first.db.transaction(tx => tx.execute(schema));
  const entered = deferred(), release = deferred();
  model.hooks.beforeClose = async () => { entered.resolve(); await release.promise; };
  let accepted = false, later = false, closed = false;
  const write = first.db.transaction(tx => tx.execute('INSERT INTO documents VALUES (1, ?, NULL)', ['first']))
    .then(() => { accepted = true; });
  await entered.promise;
  const following = first.db.transaction(async tx => { later = true; await tx.execute('INSERT INTO documents VALUES (2, ?, NULL)', ['second']); });
  const close = first.db.close().then(() => { closed = true; });
  assert.equal(accepted, false);
  assert.equal(later, false);
  assert.equal(closed, false);
  assert.equal(first.worker.terminateCount, 0);
  model.hooks.beforeClose = undefined;
  release.resolve();
  await Promise.all([write, following, close]);
  assert.equal(model.counts.publications, 3);
  assert.equal(first.worker.terminateCount, 1);
});

test('OPFS store close acknowledgement loss is confirmed without replay or export', async t => {
  const { model, queue, open } = fixture(t);
  const first = await queue();
  await first.db.transaction(tx => tx.execute(schema));
  const parent = first.db.snapshotRevision;
  let callbacks = 0;
  model.hooks.afterClose = () => { throw new Error('published, acknowledgement lost'); };
  await assert.rejects(first.db.transaction(async tx => {
    callbacks++;
    await tx.execute('INSERT INTO documents VALUES (1, ?, NULL)', ['committed']);
    return 73;
  }), error => error instanceof FrankenCheckpointCommitError && error.value === 73 && error.sqlCommitted);
  assert.equal(first.db.snapshotRevision, parent);
  assert.equal(first.db.stats.checkpointRecoveryRequired, true);
  await assert.rejects(first.db.transaction(() => { callbacks++; }), { code: 'ERR_FSQLITE_CHECKPOINT_RECOVERY_REQUIRED' });
  const exports = first.events.filter(event => event === 'export').length;
  const inserts = sqlCounts(first.events);
  const publications = model.counts.publications;
  model.hooks.afterClose = undefined;
  const recovered = await first.db.recoverCheckpoint();
  assert.equal(recovered.parentRevision, parent);
  assert.equal(first.db.snapshotRevision, recovered.revision);
  assert.equal(first.db.stats.checkpointRecoveryRequired, false);
  assert.equal(callbacks, 1);
  assert.equal(sqlCounts(first.events), inserts);
  assert.equal(first.events.filter(event => event === 'export').length, exports);
  assert.equal(model.counts.publications, publications);
  await first.db.transaction(tx => tx.execute('INSERT INTO documents VALUES (2, ?, NULL)', ['next']));
  await first.db.close();
  const reopened = await open();
  assert.deepEqual((await rows(reopened.db)).rowArrays, [[1, 'committed', null], [2, 'next', null]]);
});

test('OPFS corrupt worker receipt can be recovered by reading authoritative bytes', async t => {
  const { connection } = fixture(t);
  const value = connection();
  const handle = value.host.handle.bind(value.host);
  let corrupt = false;
  value.host.handle = async request => {
    const response = await handle(request);
    if (corrupt && request.kind === 'checkpoint' && response.kind === 'checkpoint-result') {
      return { ...response, data: { ...response.data, sha256: 'bad' } };
    }
    return response;
  };
  const db = await FrankenDB.open({ dbName: 'documents', persistence: 'opfs-snapshot', worker: value.worker });
  await db.execute(schema);
  corrupt = true;
  await assert.rejects(db.checkpoint(), { code: 'ERR_FSQLITE_SNAPSHOT_RECEIPT' });
  const requests = value.worker.requests.length;
  await assert.rejects(db.checkpoint(), { code: 'ERR_FSQLITE_SNAPSHOT_RECEIPT' });
  assert.equal(value.worker.requests.length, requests);
  corrupt = false;
  const receipt = await db.recoverCheckpoint();
  assert.equal(receipt.revision, db.snapshotRevision);
  await db.checkpoint();
});

test('OPFS failed staging preserves saved data and queue recovery never replays SQL', async t => {
  const { model, queue, open } = fixture(t);
  const first = await queue();
  await first.db.transaction(tx => tx.execute(schema));
  const parent = first.db.snapshotRevision;
  let callbacks = 0;
  model.hooks.write = () => { throw new DOMException('full', 'QuotaExceededError'); };
  await assert.rejects(first.db.transaction(async tx => {
    callbacks++;
    await tx.execute('INSERT INTO documents VALUES (1, ?, NULL)', ['pending']);
  }), FrankenCheckpointCommitError);
  assert.equal(first.db.snapshotRevision, parent);
  assert.equal(model.counts.publications, 1);
  const reader = await open();
  assert.deepEqual((await rows(reader.db)).rowArrays, []);
  await reader.db.close();
  await assert.rejects(first.db.recoverCheckpoint(), { code: 'ERR_FSQLITE_SNAPSHOT_NOT_CONFIRMED' });
  assert.equal(first.db.stats.checkpointRecoveryRequired, true);
  model.hooks.write = undefined;
  const saved = await first.db.checkpoint();
  assert.equal(saved.parentRevision, parent);
  assert.equal(callbacks, 1);
  assert.equal(sqlCounts(first.events), 1);
  await first.db.close();
  const reopened = await open();
  assert.deepEqual((await rows(reopened.db)).rowArrays, [[1, 'pending', null]]);
});

test('independent OPFS sessions reject stale publication and keep local SQL exportable', async t => {
  const { open, queue } = fixture(t);
  const seed = await open();
  await seed.db.execute(schema);
  await seed.db.checkpoint();
  await seed.db.close();
  const winner = await queue(), stale = await queue();
  await winner.db.transaction(tx => tx.execute('INSERT INTO documents VALUES (1, ?, NULL)', ['winner']));
  await assert.rejects(stale.db.transaction(tx => tx.execute('INSERT INTO documents VALUES (2, ?, NULL)', ['local'])),
    error => error instanceof FrankenCheckpointCommitError && error.cause.code === 'ERR_FSQLITE_SNAPSHOT_CONFLICT');
  await assert.rejects(stale.db.recoverCheckpoint(), { code: 'ERR_FSQLITE_SNAPSHOT_NOT_CONFIRMED' });
  const image = await stale.db.export();
  assert.ok(image.byteLength >= 512);
  assert.equal(stale.db.stats.checkpointRecoveryRequired, true);
  await assert.rejects(stale.db.close(), FrankenCheckpointCommitError);
  await winner.db.close();
  const reopened = await open();
  assert.deepEqual((await rows(reopened.db)).rowArrays, [[1, 'winner', null]]);
});

test('OPFS export/import supports new names but refuses replacing existing storage', async t => {
  const { open } = fixture(t);
  const source = await open('source');
  await source.db.execute(schema);
  await source.db.execute('INSERT INTO documents VALUES (1, ?, NULL)', ['copied']);
  const bytes = await source.db.export();
  const target = await open('target', { snapshot: bytes });
  assert.equal(target.db.snapshotRevision, null);
  assert.deepEqual((await rows(target.db)).rowArrays, [[1, 'copied', null]]);
  await target.db.checkpoint();
  await target.db.close();
  await assert.rejects(open('target', { snapshot: await source.db.export() }), { code: 'ERR_FSQLITE_SNAPSHOT_EXISTS' });
});

test('OPFS named namespaces isolate unrelated databases', async t => {
  const { open } = fixture(t);
  const one = await open('one'), two = await open('two');
  for (const [value, body] of [[one, 'first'], [two, 'second']]) {
    await value.db.execute(schema);
    await value.db.execute('INSERT INTO documents VALUES (1, ?, NULL)', [body]);
    await value.db.checkpoint();
    await value.db.close();
  }
  const first = await open('one'), second = await open('two');
  assert.notEqual(first.db.snapshotRevision, second.db.snapshotRevision);
  assert.deepEqual((await rows(first.db)).rowArrays, [[1, 'first', null]]);
  assert.deepEqual((await rows(second.db)).rowArrays, [[1, 'second', null]]);
});

test('OPFS publication refuses active manual and managed transactions', async t => {
  const { model, open } = fixture(t);
  const { db } = await open();
  await db.execute(schema);
  await db.executeBatch('BEGIN; INSERT INTO documents VALUES (1, \'uncommitted\', NULL)');
  await assert.rejects(db.checkpoint(), { code: 'ERR_FSQLITE_SNAPSHOT_TRANSACTION' });
  assert.equal(model.counts.publications, 0);
  await db.executeBatch('ROLLBACK');
  await db.transaction(async tx => {
    await tx.execute('INSERT INTO documents VALUES (2, ?, NULL)', ['committed']);
    await assert.rejects(db.checkpoint(), { code: 'ERR_FSQLITE_TRANSACTION_OWNERSHIP' });
  });
  await db.checkpoint();
  assert.deepEqual((await rows(db)).rowArrays, [[2, 'committed', null]]);
});

test('OPFS unavailable APIs never silently fall back to volatile memory', async t => {
  const { connection } = fixture(t);
  Object.defineProperty(globalThis, 'navigator', { configurable: true, value: {} });
  const value = connection();
  await assert.rejects(FrankenDB.open({ dbName: 'documents', persistence: 'opfs-snapshot', worker: value.worker }),
    { code: 'ERR_FSQLITE_SNAPSHOT_UNAVAILABLE' });
  assert.deepEqual(value.counts(), { creates: 0, imports: 0 });
  assert.equal(value.worker.terminateCount, 1);
});

test('unsupported page-VFS modes still fail before creating a core database', async t => {
  const { connection } = fixture(t);
  for (const persistence of ['opfs', 'indexeddb']) {
    const value = connection();
    await assert.rejects(FrankenDB.open({ dbName: 'documents', persistence, worker: value.worker }),
      { code: 'ERR_FSQLITE_UNSUPPORTED_PERSISTENCE' });
    assert.deepEqual(value.counts(), { creates: 0, imports: 0 });
  }
});

test('memory import retains its core-assigned path without inventing persistence', async t => {
  const { connection, open } = fixture(t);
  const source = await open();
  await source.db.execute(schema);
  const target = connection();
  const db = await FrankenDB.import(await source.db.export(), { dbName: 'display-name', persistence: 'memory', worker: target.worker });
  assert.equal(db.path, target.handles[0].path);
  assert.equal(db.persistence, 'memory');
  assert.equal(db.checkpointRecoverySupported, false);
  await assert.rejects(db.checkpoint(), { code: 'ERR_FSQLITE_SNAPSHOT_MODE' });
  await db.close();
});

function peer(reply) {
  const listeners = new Map();
  const worker = {
    requests: [], terminated: 0,
    addEventListener(kind, listener) { (listeners.get(kind) ?? listeners.set(kind, new Set()).get(kind)).add(listener); },
    removeEventListener(kind, listener) { listeners.get(kind)?.delete(listener); },
    postMessage(request) {
      this.requests.push(request);
      const data = request.kind === 'init' ? { kind: 'ready', requestId: request.requestId, data: reply(request.config) }
        : { kind: 'close-result', requestId: request.requestId };
      for (const listener of listeners.get('message') ?? []) listener({ data });
    },
    terminate() { this.terminated++; },
  };
  return worker;
}

for (const [requested, accepted] of [
  ['opfs-snapshot', 'memory'], ['opfs-snapshot', 'indexeddb-snapshot'],
  ['indexeddb-snapshot', 'memory'], ['indexeddb-snapshot', 'opfs-snapshot'], ['memory', 'opfs-snapshot'],
]) {
  test(`init rejects ${requested} being acknowledged as ${accepted}`, async () => {
    const worker = peer(config => ({ path: config.dbName, persistence: accepted }));
    await assert.rejects(FrankenDB.open({ dbName: 'documents', persistence: requested, worker }),
      { code: 'ERR_FSQLITE_PERSISTENCE_POLICY' });
    assert.equal(worker.terminated, 1);
    assert.deepEqual(worker.requests.map(request => request.kind), ['init']);
  });
}

for (const persistence of ['opfs-snapshot', 'indexeddb-snapshot']) {
  test(`init rejects another ${persistence} namespace`, async () => {
    const worker = peer(() => ({ path: 'other-database', persistence }));
    await assert.rejects(FrankenDB.open({ dbName: 'documents', persistence, worker }), { code: 'ERR_FSQLITE_PERSISTENCE_POLICY' });
    assert.equal(worker.terminated, 1);
  });
}

test('direct client captures caller config once and fences a mismatched acknowledgement', async () => {
  let reads = 0;
  const worker = peer(() => ({ path: 'documents', persistence: 'memory' }));
  const client = new FrankenWorkerClient(worker);
  await assert.rejects(client.init({ dbName: 'documents', get persistence() { reads++; return 'opfs-snapshot'; } }),
    { code: 'ERR_FSQLITE_PERSISTENCE_POLICY' });
  assert.equal(reads, 1);
  await assert.rejects(client.execute('INSERT INTO anything VALUES (1)'), { code: 'ERR_FSQLITE_PERSISTENCE_POLICY' });
  assert.equal(worker.requests.length, 1);
  await client.close();
});

for (const key of ['path', 'persistence', 'snapshot', 'checkpointRecovery', 'preparedStatementLimits', 'resultEncoding']) {
  test(`ready ${key} accessors cannot grant storage authority`, async () => {
    let reads = 0;
    const worker = peer(() => Object.defineProperty({ path: 'documents', persistence: 'opfs-snapshot' }, key,
      { enumerable: true, get() { reads++; return key === 'path' ? 'documents' : 'opfs-snapshot'; } }));
    await assert.rejects(FrankenDB.open({ dbName: 'documents', persistence: 'opfs-snapshot', worker }),
      { code: 'ERR_FSQLITE_WORKER_RESPONSE' });
    assert.equal(reads, 0);
    assert.equal(worker.terminated, 1);
  });
}

test('ready identity is copied before a custom transport can mutate it', async () => {
  const ready = { path: 'documents', persistence: 'opfs-snapshot', snapshot: null, checkpointRecovery: 1 };
  const worker = peer(() => ready);
  const post = worker.postMessage.bind(worker);
  worker.postMessage = request => {
    post(request);
    ready.path = 'other'; ready.persistence = 'memory'; ready.checkpointRecovery = undefined;
  };
  const db = await FrankenDB.open({ dbName: 'documents', persistence: 'opfs-snapshot', worker });
  assert.equal(db.path, 'documents');
  assert.equal(db.persistence, 'opfs-snapshot');
  assert.equal(db.checkpointRecoverySupported, true);
  await db.close();
});
