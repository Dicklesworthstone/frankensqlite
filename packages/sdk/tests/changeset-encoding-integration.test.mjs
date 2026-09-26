import assert from 'node:assert/strict';
import { test } from 'node:test';
import { createHash } from 'node:crypto';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { SQLiteTarget, nativeChanges } from './helpers/sqlite-integration-target.mjs';
import { ChangesetOrder } from '../src/changeset-order.ts';
import { ChangesetFanout } from '../src/changeset-fanout.ts';
import { applyChangeset } from '../src/changeset-apply.ts';
import { decodeChangeset } from '../src/changeset-codec.ts';
import { ensure, store, load, find, chunkId } from '../src/changeset-outbox-store.ts';
import { ChangesetBootstrapReceiver, readBootstrapManifest, acknowledgeFanoutBootstrapInstall } from '../src/changeset-bootstrap.ts';

// No loader substitutions: order, fanout, bootstrap, apply, codec and outbox
// storage are the real source modules. Only the SQL owner is reference SQLite.
const encodings = ['UTF-8', 'UTF-16le', 'UTF-16be'];
const hash = bytes => createHash('sha256').update(bytes).digest('hex');
const schema = 'CREATE TABLE notes(id INTEGER PRIMARY KEY, value TEXT)';
const seed = nativeChanges(db => db.exec("INSERT INTO notes VALUES(1,'seed')"));
const update = nativeChanges(db => db.exec("UPDATE notes SET value='next' WHERE id=1"), schema, "INSERT INTO notes VALUES(1,'seed');");
const changes = bytes => decodeChangeset(bytes).reduce((n, table) => n + table.changes.length, 0);
const message = (sequence, deliveryId, bytes = seed) => ({ sequence, deliveryId, changeset: bytes, sha256: hash(bytes) });
const apply = (target, bytes) => applyChangeset(target, bytes, { tables: ['notes'] });
async function append(target, id, bytes, scope = JSON.stringify({ tables: ['notes'], indirect: false })) {
  return target.transaction(async tx => {
    await ensure(tx, true);
    return store(tx, id, scope, { changeset: bytes, changes: changes(bytes) }, () => {});
  });
}
async function appendSeed(target, id, chunks) {
  const summary = { chunks: chunks.length, changes: chunks.reduce((n, b) => n + changes(b), 0),
    byteLength: chunks.reduce((n, b) => n + b.length, 0) };
  return target.transaction(async tx => {
    await ensure(tx, true);
    for (let i = 0; i < chunks.length; i++) {
      const scope = JSON.stringify({ tables: ['notes'], indirect: false, snapshot: true,
        stream: { id, index: i, ...(i === 0 ? { summary } : {}) } });
      await store(tx, chunkId(id, i), scope, { changeset: chunks[i], changes: changes(chunks[i]) }, () => {});
    }
  });
}

for (const encoding of encodings) {
  for (const ids of [
    { name: 'short', receiverId: 'east', sourceId: 'source', deliveryId: 'op' },
    { name: 'ASCII limits', receiverId: 'r'.repeat(256), sourceId: 's'.repeat(256), deliveryId: 'd'.repeat(512) },
    { name: 'Unicode limits', receiverId: '😀'.repeat(64), sourceId: 'é'.repeat(128), deliveryId: '界'.repeat(170) + 'dd' },
    { name: 'JSON-escaped limits', receiverId: '\u0001'.repeat(256), sourceId: '\u0002'.repeat(256), deliveryId: '\u0003'.repeat(512) },
  ]) test(`${encoding}: ordered native-session application, replay and reopen (${ids.name})`, async () => {
    const path = join(mkdtempSync(join(tmpdir(), 'fsqlite-encoding-')), 'replica.db');
    let target = new SQLiteTarget(path, encoding);
    try {
      target.db.exec(schema);
      let order = new ChangesetOrder(target, ids);
      assert.equal((await order.initialize()).sequence, 0n);
      assert.equal((await order.apply(message(1n, ids.deliveryId), apply)).applied, 1);
      assert.equal((await order.apply(message(1n, ids.deliveryId), () => { throw Error('replayed SQL'); })).replayed, true);
      target.close(); target = new SQLiteTarget(path, encoding);
      assert.equal((await target.query('PRAGMA encoding')).rowArrays[0][0], encoding);
      order = new ChangesetOrder(target, ids);
      assert.equal((await order.head()).sequence, 1n);
      assert.equal((await order.apply(message(2n, 'update', update), apply)).applied, 1);
      assert.deepEqual((await target.query('SELECT * FROM notes')).rowArrays, [[1n, 'next']]);
      assert.equal((await order.apply(message(1n, ids.deliveryId), () => { throw Error('old replay'); })).sequence, 1n);
      assert.equal((await order.head()).sequence, 2n);
      assert.deepEqual((await target.query('PRAGMA integrity_check')).rowArrays, [['ok']]);
    } finally { target.close(); }
  });

  test(`${encoding}: all-replica retention and atomic ordered bootstrap handoff`, async () => {
    const source = new SQLiteTarget(':memory:', encoding);
    const east = new SQLiteTarget(':memory:', encoding), west = new SQLiteTarget(':memory:', encoding);
    const ids = ['e'.repeat(256), 'w'.repeat(256)], root = 'b'.repeat(480), sourceId = 's'.repeat(256);
    const second = nativeChanges(db => db.exec("INSERT INTO notes VALUES(2,'two')"));
    try {
      const fanout = await ChangesetFanout.open(source, ids);
      await appendSeed(source, root, [seed, second]);
      const incremental = await append(source, 'i'.repeat(512), update);
      for (const [index, target] of [east, west].entries()) {
        target.db.exec(schema);
        const receiverId = ids[index];
        const manifest = await readBootstrapManifest(source, { receiverId, deliveryId: root, tables: ['notes'] });
        const receiver = new ChangesetBootstrapReceiver(target, {
          receiverId, orderedSourceId: sourceId, tables: ['notes'], confirmCommit: async () => {},
        });
        for (const [i, bytes] of [seed, second].entries()) await receiver.stage(manifest, i, bytes);
        assert.deepEqual((await target.query('SELECT * FROM notes')).rowArrays, []);
        const receipt = await receiver.install(manifest);
        assert.equal(receipt.order.sequence, '2');
        assert.equal(await acknowledgeFanoutBootstrapInstall(source, manifest, receipt, { receiverId, orderedSourceId: sourceId }), 2);
        assert.equal((await fanout.progress()).acknowledgedThrough, index === 0 ? 0n : 2n);
        const rootRow = await source.transaction(tx => find(tx, root));
        assert.equal(rootRow.delivery.acknowledged, index !== 0);
        if (index === 0) assert.deepEqual(await source.transaction(tx => load(tx, rootRow)), seed);
        const order = new ChangesetOrder(target, { receiverId, sourceId });
        await order.apply(message(3n, incremental.deliveryId, update), apply);
        await fanout.forReplica(receiverId).acknowledge(incremental.deliveryId, incremental.sha256);
        assert.equal((await receiver.install(manifest)).replayed, true);
        assert.equal((await order.head()).sequence, 3n);
      }
      assert.equal((await fanout.progress()).acknowledgedThrough, 3n);
      assert.deepEqual((await east.query('SELECT * FROM notes ORDER BY id')).rowArrays, [[1n, 'next'], [2n, 'two']]);
      assert.deepEqual((await west.query('SELECT * FROM notes ORDER BY id')).rowArrays, [[1n, 'next'], [2n, 'two']]);
    } finally { source.close(); east.close(); west.close(); }
  });

  test(`${encoding}: maximum escaped roster stays bounded and reopens`, async () => {
    const target = new SQLiteTarget(':memory:', encoding);
    const replicas = Array.from({ length: 256 }, (_, i) => `${i}`.padStart(3, '0') + '\u0001'.repeat(253));
    try {
      await ChangesetFanout.open(target, replicas);
      const reopened = await ChangesetFanout.open(target, [...replicas].reverse());
      assert.equal((await reopened.progress()).replicas.length, 256);
      const [[bytes]] = (await target.query('SELECT length(CAST(roster AS BLOB)) FROM __fsqlite_changeset_fanout')).rowArrays;
      assert.ok(bytes > 262144n && bytes < 1024n * 1024n);
    } finally { target.close(); }
  });

  for (const [field, value] of [
    ['sha256', 'f'.repeat(64) + '\0' + 'x'.repeat(10000)],
    ['sha256', new Uint8Array(64).fill(48)],
    ['receiver_id', 'r'.repeat(257)],
    ['source_id', '界'.repeat(86)],
    ['delivery_id', 'd'.repeat(513)],
    ['delivery_id', 'ok\0bad'],
  ]) test(`${encoding}: malformed retained ${field} refuses replay (${typeof value}:${value.length})`, async () => {
    const target = new SQLiteTarget(':memory:', encoding);
    try {
      target.db.exec(schema);
      const order = new ChangesetOrder(target, { receiverId: 'east', sourceId: 'source' });
      await order.initialize(); await order.apply(message(1n, 'op'), apply);
      await target.execute(`UPDATE __fsqlite_changeset_order SET ${field}=? WHERE seq=1`, [value]);
      await assert.rejects(order.apply(message(1n, 'op'), () => { throw Error('must not run'); }), { code: 'ERR_FSQLITE_ORDER_CORRUPT' });
      assert.deepEqual((await target.query('SELECT * FROM notes')).rowArrays, [[1n, 'seed']]);
    } finally { target.close(); }
  });

  test(`${encoding}: fanout refuses a forged over-limit cursor identity`, async () => {
    const target = new SQLiteTarget(':memory:', encoding);
    try {
      const fanout = await ChangesetFanout.open(target, ['east', 'west']);
      const delivery = await append(target, 'op', seed);
      await fanout.forReplica('east').acknowledge('op', delivery.sha256);
      await target.execute("UPDATE __fsqlite_changeset_fanout_progress SET delivery_id=? WHERE replica_id='east'", ['x'.repeat(513)]);
      await assert.rejects(fanout.progress());
      assert.equal((await target.query('SELECT acknowledged FROM __fsqlite_changeset_outbox')).rowArrays[0][0], 0n);
    } finally { target.close(); }
  });
}
