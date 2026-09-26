import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';
import { SQLiteTarget, nativeChanges } from './helpers/sqlite-integration-target.mjs';
import { ChangesetBootstrapReceiver } from '../src/changeset-bootstrap.ts';
import { ChangesetBootstrapTransfer } from '../src/changeset-bootstrap-transfer.ts';
import { ensure, store, chunkId } from '../src/changeset-outbox-store.ts';

const schema = 'CREATE TABLE notes(id INTEGER PRIMARY KEY, value TEXT)';
const route = { receiverId: 'replica', deliveryId: 'source:baseline', tables: ['notes'] };
const nativeChunk = id => nativeChanges(db => db.prepare('INSERT INTO notes VALUES (?,?)').run(id, `seed-${id}`));
async function retain(source, chunks) {
  const summary = { chunks: chunks.length, changes: chunks.filter(b => b.length).length, byteLength: chunks.reduce((n, b) => n + b.length, 0) };
  await source.transaction(async tx => {
    await ensure(tx, true);
    for (const [i, bytes] of chunks.entries()) {
      const scope = JSON.stringify({ tables: route.tables, indirect: false, snapshot: true,
        stream: { id: route.deliveryId, index: i, ...(i === 0 ? { summary } : {}) } });
      await store(tx, chunkId(route.deliveryId, i), scope, { changeset: bytes, changes: bytes.length ? 1 : 0 }, () => {});
    }
  });
}
async function pending(source) {
  return (await source.query('SELECT sum(1-acknowledged), sum(length(payload)) FROM __fsqlite_changeset_outbox')).rowArrays[0];
}
async function setup(encoding = 'UTF-8', ordered = false, chunks = [nativeChunk(1), nativeChunk(2), nativeChunk(3)]) {
  const source = new SQLiteTarget(':memory:', encoding), destination = new SQLiteTarget(':memory:', encoding);
  destination.db.exec(schema);
  await retain(source, chunks);
  const policy = ordered ? { orderedSourceId: 'source:incarnation' } : {};
  const counts = { source: 0, receiver: 0 };
  const receiver = new ChangesetBootstrapReceiver(destination, {
    receiverId: route.receiverId, tables: route.tables, ...policy,
    confirmCommit: async () => { assert.equal(destination.depth, 0); counts.receiver++; },
  });
  const options = { ...route, ...policy,
    confirmSource: async () => { assert.equal(source.depth, 0); counts.source++; } };
  return { source, destination, receiver, counts, options,
    close() { source.close(); destination.close(); } };
}
function lostInstall(receiver) {
  return { status: receiver.status.bind(receiver), stage: receiver.stage.bind(receiver),
    async install(...args) { await receiver.install(...args); throw Error('lost installation response'); } };
}
const corruptions = [
  ['missing first chunk', 'DELETE FROM __fsqlite_bootstrap_chunks WHERE idx=0'],
  ['different chunk digest', "UPDATE __fsqlite_bootstrap_chunks SET sha256=printf('%064d',0) WHERE idx=1"],
  ['different byte count', 'UPDATE __fsqlite_bootstrap_chunks SET byte_length=byte_length+1 WHERE idx=0'],
  ['different row count', 'UPDATE __fsqlite_bootstrap_chunks SET change_count=change_count+1 WHERE idx=0'],
  ['unexpected retained body', "UPDATE __fsqlite_bootstrap_chunks SET payload=X'FF' WHERE idx=0"],
  ['extra metadata record', 'INSERT INTO __fsqlite_bootstrap_chunks SELECT 99,sha256,byte_length,change_count,payload FROM __fsqlite_bootstrap_chunks WHERE idx=0'],
];
for (const encoding of ['UTF-8', 'UTF-16le', 'UTF-16be']) {
  for (const ordered of [false, true]) for (const [kind, sql] of corruptions) {
    test(`${encoding} ${ordered ? 'ordered' : 'unordered'}: damaged installed replay retains source (${kind})`, async () => {
      const h = await setup(encoding, ordered);
      try {
        const first = new ChangesetBootstrapTransfer(h.source, { ...h.options, transport: lostInstall(h.receiver) });
        await assert.rejects(first.run(), { phase: 'install' });
        assert.equal((await h.destination.query('SELECT count(*) FROM notes')).rowArrays[0][0], 3n);
        const retained = await pending(h.source), confirmations = h.counts.receiver;
        assert.equal(retained[0], 3n); assert.ok(retained[1] > 0n);
        await h.destination.execute(sql);
        const retry = new ChangesetBootstrapTransfer(h.source, { ...h.options, transport: h.receiver });
        await assert.rejects(retry.run(), error => error.phase === 'install' && (error.cause?.code === 'ERR_FSQLITE_BOOTSTRAP_CORRUPT' || (ordered && error.cause?.code === 'ERR_FSQLITE_ORDER_CORRUPT')));
        assert.deepEqual(await pending(h.source), retained);
        assert.equal(h.counts.receiver, confirmations, 'unverified history must not reach storage confirmation');
        assert.equal((await h.destination.query('SELECT count(*) FROM notes')).rowArrays[0][0], 3n);
      } finally { h.close(); }
    });
  }

  test(`${encoding}: installed replay survives later row writes and lost source ACK without replaying SQL`, async () => {
    const h = await setup(encoding);
    try {
      const first = new ChangesetBootstrapTransfer(h.source, { ...h.options, transport: lostInstall(h.receiver) });
      await assert.rejects(first.run());
      await h.destination.execute("UPDATE notes SET value='later' WHERE id=1");
      h.destination.onExecute = sql => { assert.ok(!/INSERT OR ABORT INTO main\."notes"/.test(sql), 'must not reinstall'); };
      h.source.onCommit = async () => {
        if ((await pending(h.source))[0] === 0n) { h.source.onCommit = null; throw Error('lost source ACK'); }
      };
      const retry = new ChangesetBootstrapTransfer(h.source, { ...h.options, transport: h.receiver });
      await assert.rejects(retry.run(), { phase: 'source-ack' });
      assert.equal((await pending(h.source))[0], 0n);
      const result = await retry.run();
      assert.equal(result.receipt.replayed, true); assert.equal(result.newlyAcknowledged, 0);
      assert.equal(result.uploadedChunks, 0);
      assert.deepEqual((await h.destination.query('SELECT value FROM notes WHERE id=1')).rowArrays, [['later']]);
    } finally { h.close(); }
  });
}

test('an empty installed baseline also requires its retained chunk identity', async () => {
  const h = await setup('UTF-8', false, [new Uint8Array()]);
  try {
    await assert.rejects(new ChangesetBootstrapTransfer(h.source, { ...h.options, transport: lostInstall(h.receiver) }).run());
    await h.destination.execute('DELETE FROM __fsqlite_bootstrap_chunks');
    await assert.rejects(new ChangesetBootstrapTransfer(h.source, { ...h.options, transport: h.receiver }).run(), { phase: 'install' });
    assert.equal((await pending(h.source))[0], 1n);
  } finally { h.close(); }
});

test('installed replay reads bounded metadata only, and cancellation prevents ACK', async () => {
  const h = await setup('UTF-8', false, Array.from({ length: 80 }, (_, i) => nativeChunk(i + 1)));
  try {
    await assert.rejects(new ChangesetBootstrapTransfer(h.source, { ...h.options, transport: lostInstall(h.receiver) }).run());
    const abort = new AbortController(); let reads = 0;
    h.destination.onQuery = (sql, params, rows) => {
      assert.ok(!/^SELECT payload FROM/.test(sql), 'replay must not load reclaimed bodies');
      if (sql.includes('WHERE idx=?')) { assert.ok(rows.length <= 1); if (++reads === 5) abort.abort(); }
    };
    const retry = new ChangesetBootstrapTransfer(h.source, { ...h.options, transport: h.receiver });
    await assert.rejects(retry.run({ signal: abort.signal }), { phase: 'install' });
    assert.equal((await pending(h.source))[0], 80n);
    h.destination.onQuery = null;
    const result = await retry.run();
    assert.equal(result.receipt.replayed, true); assert.equal(result.newlyAcknowledged, 80);
  } finally { h.close(); }
});

for (const journal of ['WAL', 'DELETE']) test(`${journal}: actual receiver process dies after installation commit; production transfer recovers`, async () => {
  const directory = mkdtempSync(join(tmpdir(), 'fsqlite-replay-'));
  const sourcePath = join(directory, 'source.db'), receiverPath = join(directory, 'receiver.db');
  const source = new SQLiteTarget(sourcePath, 'UTF-16le', journal);
  const destination = new SQLiteTarget(receiverPath, 'UTF-16le', journal);
  await retain(source, [nativeChunk(1), nativeChunk(2)]); destination.db.exec(schema);
  source.close(); destination.close();
  const child = spawnSync(process.execPath, [
    '--experimental-loader=./packages/sdk/tests/helpers/bootstrap-integration-loader.mjs',
    './packages/sdk/tests/helpers/bootstrap-replay-integration-child.mjs', sourcePath, receiverPath, journal,
  ], { encoding: 'utf8', timeout: 15000 });
  assert.equal(child.signal, 'SIGKILL', child.stderr);
  const reopened = new SQLiteTarget(sourcePath, 'UTF-16le', journal), replica = new SQLiteTarget(receiverPath, 'UTF-16le', journal);
  try {
    assert.equal((await pending(reopened))[0], 2n);
    assert.equal((await replica.query('SELECT count(*) FROM notes')).rowArrays[0][0], 2n);
    replica.onExecute = sql => { assert.ok(!/INSERT OR ABORT INTO main\."notes"/.test(sql)); };
    const receiver = new ChangesetBootstrapReceiver(replica, { receiverId: route.receiverId, tables: route.tables, confirmCommit: async () => {} });
    const result = await new ChangesetBootstrapTransfer(reopened, { ...route, transport: receiver, confirmSource: async () => {} }).run();
    assert.equal(result.receipt.replayed, true); assert.equal(result.uploadedChunks, 0); assert.equal(result.newlyAcknowledged, 2);
    assert.deepEqual(await pending(reopened), [0n, 0n]);
    assert.deepEqual((await replica.query('PRAGMA integrity_check')).rowArrays, [['ok']]);
  } finally { reopened.close(); replica.close(); }
});

for (const encoding of ['UTF-8', 'UTF-16le', 'UTF-16be']) for (const field of ['sha256', 'chain']) {
  test(`${encoding}: ${field} NUL tails are refused before SQL metadata transfer`, async () => {
    const h = await setup(encoding);
    try {
      let manifest;
      const transport = { status: h.receiver.status.bind(h.receiver), stage: h.receiver.stage.bind(h.receiver),
        async install(m, controls) { manifest = m; await h.receiver.install(m, controls); throw Error('lost response'); } };
      await assert.rejects(new ChangesetBootstrapTransfer(h.source, { ...h.options, transport }).run());
      const table = field === 'sha256' ? '__fsqlite_bootstrap_chunks' : '__fsqlite_bootstrap_state';
      await h.destination.execute(`UPDATE ${table} SET ${field}=?`, ['0'.repeat(64) + '\0' + 'x'.repeat(1024 * 1024)]);
      let checked = false;
      h.destination.onQuery = (sql, _params, rows) => {
        if (field === 'sha256' && sql.includes('WHERE idx=?')) { assert.equal(rows[0][0], null); checked = true; }
        if (field === 'chain' && sql.includes('received, bytes, changes')) { assert.equal(rows[0][5], null); checked = true; }
      };
      const confirmations = h.counts.receiver;
      await assert.rejects(h.receiver.install(manifest), { code: 'ERR_FSQLITE_BOOTSTRAP_CORRUPT' });
      assert.equal(checked, true); assert.equal(h.counts.receiver, confirmations);
      assert.equal((await pending(h.source))[0], 3n);
    } finally { h.close(); }
  });
}
