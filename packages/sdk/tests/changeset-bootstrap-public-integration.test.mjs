import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { SQLiteTarget } from './helpers/sqlite-integration-target.mjs';
import { ChangesetOutbox } from '../src/changeset-outbox.ts';
import { ChangesetFanout } from '../src/changeset-fanout.ts';
import { ChangesetBootstrapReceiver } from '../src/changeset-bootstrap.ts';
import { ChangesetBootstrapTransfer } from '../src/changeset-bootstrap-transfer.ts';
import { ChangesetOrder } from '../src/changeset-order.ts';
import { applyChangeset } from '../src/changeset-apply.ts';
import { decodeChangeset } from '../src/changeset-codec.ts';

// All source creation, capture, manifests, delivery, receipt and fanout logic
// come from production modules. SQLiteTarget supplies reference SQL ownership.
const encodings = ['UTF-8', 'UTF-16le', 'UTF-16be'];
const tables = ['parents', 'children'];
const schema = `CREATE TABLE parents(id INTEGER PRIMARY KEY, label TEXT);
CREATE TABLE children(parent_id INTEGER NOT NULL REFERENCES parents(id), position INTEGER NOT NULL,
 body TEXT, payload BLOB, weight REAL, PRIMARY KEY(parent_id DESC,position ASC)) WITHOUT ROWID;`;
const sourceId = 'source:incarnation', root = 'source:baseline';
async function populate(source) {
  source.db.exec(schema + 'PRAGMA recursive_triggers=ON');
  for (let parent = 1; parent <= 3; parent++) {
    await source.execute('INSERT INTO parents VALUES(?,?)', [BigInt(parent), `parent-${parent}`]);
    for (let position = 0; position < 3; position++) await source.execute('INSERT INTO children VALUES(?,?,?,?,?)',
      [BigInt(parent), BigInt(position), `\uFEFFbefore\0界😀-${parent}-${position}`, new Uint8Array([0, parent, 255]), 1.25]);
  }
}
async function contents(target) {
  return [
    (await target.query('SELECT * FROM parents ORDER BY id')).rowArrays,
    (await target.query('SELECT parent_id,position,hex(CAST(body AS BLOB)),hex(payload),typeof(weight),weight FROM children ORDER BY parent_id,position')).rowArrays,
  ];
}
function normalized(bytes) {
  return decodeChangeset(bytes).flatMap(table => table.changes.map(change =>
    JSON.stringify({ name: table.name, pk: table.primaryKey, ...change }, (_, value) =>
      typeof value === 'bigint' ? `${value}n` : value instanceof Uint8Array ? Array.from(value) : value))).sort();
}
const totalPayload = async source => (await source.query('SELECT sum(length(payload)) FROM __fsqlite_changeset_outbox')).rowArrays[0][0];

for (const encoding of encodings) for (const fanoutMode of [false, true]) {
  test(`${encoding}: public capture -> resumable atomic transfer -> ordered incremental (${fanoutMode ? 'fanout' : 'single'})`, async () => {
    const directory = mkdtempSync(join(tmpdir(), 'fsqlite-public-bootstrap-'));
    let source = new SQLiteTarget(join(directory, 'source.db'), encoding);
    const targets = [];
    try {
      await populate(source);
      const recipients = fanoutMode ? ['east', 'west'] : ['east'];
      let fanout = fanoutMode ? await ChangesetFanout.open(source, recipients) : null;
      let outbox = new ChangesetOutbox(source);
      const options = { deliveryId: root, tables, chunkRows: 2, chunkBytes: 2048 };
      const baseline = await contents(source);
      const seed = await outbox.bootstrapChunks(options);
      assert.equal(seed.changes, 12); assert.ok(seed.chunks >= 7);
      const native = source.db.createSession({ table: 'children' });
      let callbackRuns = 0, delta, nativeBytes;
      try {
        delta = await outbox.record(async tx => {
          callbackRuns++;
          await tx.execute('UPDATE children SET body=?,payload=?,weight=? WHERE parent_id=1 AND position=0',
            ['after\0\uFEFF😀', new Uint8Array([9, 0, 1]), 42.5]);
          await tx.execute('DELETE FROM children WHERE parent_id=2 AND position=1');
          await tx.execute('INSERT INTO children VALUES(3,4,?,?,?)', ['new界', new Uint8Array([7]), 8.75]);
        }, { deliveryId: 'source:delta-1', tables });
        nativeBytes = new Uint8Array(native.changeset());
      } finally { native.close(); }
      const retainedDelta = await outbox.read(delta.delivery.deliveryId);
      assert.deepEqual(normalized(retainedDelta.changeset), normalized(nativeBytes));
      assert.equal(delta.delivery.sequence, BigInt(seed.chunks + 1));
      assert.equal((await outbox.record(() => { callbackRuns++; throw Error('must not recapture'); },
        { deliveryId: delta.delivery.deliveryId, tables })).replayed, true);
      assert.equal(callbackRuns, 1);
      const finalContents = await contents(source);
      source.close(); source = new SQLiteTarget(join(directory, 'source.db'), encoding);
      outbox = new ChangesetOutbox(source);
      fanout = fanoutMode ? await ChangesetFanout.open(source, recipients) : null;
      assert.equal((await outbox.bootstrapChunks(options)).replayed, true, 'recover original source seed after restart');
      for (const [replicaIndex, receiverId] of recipients.entries()) {
        const destination = new SQLiteTarget(join(directory, `${receiverId}.db`), encoding);
        targets.push(destination); destination.db.exec(schema);
        const receiver = new ChangesetBootstrapReceiver(destination, { receiverId, orderedSourceId: sourceId, tables,
          confirmCommit: async () => { assert.equal(destination.depth, 0); } });
        let loseInstall = true;
        const transport = {
          status: receiver.status.bind(receiver), stage: receiver.stage.bind(receiver),
          async install(...args) {
            const receipt = await receiver.install(...args);
            if (loseInstall) { loseInstall = false; throw Error('lost install response'); }
            return receipt;
          },
        };
        const transfer = new ChangesetBootstrapTransfer(source, { receiverId, deliveryId: root, tables,
          orderedSourceId: sourceId, acknowledgement: fanoutMode ? 'fanout' : 'single-recipient', transport,
          confirmSource: async () => { assert.equal(source.depth, 0); } });
        assert.equal((await transfer.run({ maxChunks: 2 })).stopped, 'limit');
        assert.deepEqual(await contents(destination), [[], []], 'staging cannot expose application rows');
        const retained = await totalPayload(source);
        await assert.rejects(transfer.run(), { phase: 'install' });
        assert.deepEqual(await contents(destination), baseline);
        assert.equal(await totalPayload(source), retained, 'lost receiver response cannot reclaim source bytes');
        const installed = await transfer.run();
        assert.equal(installed.uploadedChunks, 0); assert.equal(installed.receipt.replayed, true);
        assert.equal(installed.newlyAcknowledged, seed.chunks);
        if (fanoutMode && replicaIndex === 0) assert.notEqual((await outbox.read(root)).changeset, null, 'slow replica retains baseline');
        else assert.equal((await outbox.read(root)).changeset, null);
        const entry = await outbox.read(delta.delivery.deliveryId);
        assert.notEqual(entry.changeset, null, 'seed ACK preserves the next incremental payload');
        const order = new ChangesetOrder(destination, { receiverId, sourceId });
        assert.equal((await order.head()).sequence, BigInt(seed.chunks));
        await order.apply({ ...entry.delivery, changeset: entry.changeset },
          (inside, bytes) => applyChangeset(inside, bytes, { tables }));
        const ackSource = fanout?.forReplica(receiverId) ?? outbox;
        await ackSource.acknowledge(entry.delivery.deliveryId, entry.delivery.sha256);
        assert.deepEqual(await contents(destination), finalContents);
        const replay = await transfer.run();
        assert.equal(replay.newlyAcknowledged, 0); assert.equal(replay.receipt.replayed, true);
        assert.equal((await order.head()).sequence, BigInt(seed.chunks + 1), 'historical bootstrap cannot rewind incremental tip');
      }
      assert.equal(await totalPayload(source), 0n);
      assert.deepEqual(await outbox.pending(), []);
      for (const target of targets) assert.deepEqual((await target.query('PRAGMA integrity_check')).rowArrays, [['ok']]);
    } finally { source.close(); for (const target of targets) target.close(); }
  });
}

for (const encoding of encodings) {
  test(`${encoding}: corrupted installed history keeps a public bootstrap pending after lost ACK`, async () => {
    const source = new SQLiteTarget(':memory:', encoding), destination = new SQLiteTarget(':memory:', encoding);
    try {
      await populate(source); destination.db.exec(schema);
      const outbox = new ChangesetOutbox(source);
      const seed = await outbox.bootstrapChunks({ deliveryId: root, tables, chunkRows: 2, chunkBytes: 2048 });
      const receiver = new ChangesetBootstrapReceiver(destination, { receiverId: 'east', tables, confirmCommit: async () => {} });
      const common = { receiverId: 'east', deliveryId: root, tables, confirmSource: async () => {} };
      const lost = new ChangesetBootstrapTransfer(source, { ...common, transport: {
        status: receiver.status.bind(receiver), stage: receiver.stage.bind(receiver),
        async install(...args) { await receiver.install(...args); throw Error('lost ACK'); },
      } });
      await assert.rejects(lost.run());
      const before = await totalPayload(source);
      await destination.execute('DELETE FROM __fsqlite_bootstrap_chunks WHERE idx=0');
      await assert.rejects(new ChangesetBootstrapTransfer(source, { ...common, transport: receiver }).run(),
        error => error.phase === 'install' && error.cause.code === 'ERR_FSQLITE_BOOTSTRAP_CORRUPT');
      assert.equal(await totalPayload(source), before);
      assert.equal((await outbox.pending()).length, seed.chunks);
    } finally { source.close(); destination.close(); }
  });

  test(`${encoding}: empty public bootstrap installs once and hands off sequence two`, async () => {
    const source = new SQLiteTarget(':memory:', encoding), destination = new SQLiteTarget(':memory:', encoding);
    try {
      source.db.exec(schema + 'PRAGMA recursive_triggers=ON'); destination.db.exec(schema);
      const outbox = new ChangesetOutbox(source);
      const seed = await outbox.bootstrapChunks({ deliveryId: root, tables });
      assert.equal(seed.chunks, 1); assert.equal(seed.changes, 0);
      const delta = await outbox.record(tx => tx.execute("INSERT INTO parents VALUES(1,'first')"),
        { deliveryId: 'source:first', tables });
      const receiver = new ChangesetBootstrapReceiver(destination, { receiverId: 'east', tables,
        orderedSourceId: sourceId, confirmCommit: async () => {} });
      const result = await new ChangesetBootstrapTransfer(source, { receiverId: 'east', deliveryId: root, tables,
        transport: receiver, orderedSourceId: sourceId, confirmSource: async () => {} }).run();
      assert.equal(result.newlyAcknowledged, 1);
      const entry = await outbox.read(delta.delivery.deliveryId);
      assert.equal(entry.delivery.sequence, 2n);
      const order = new ChangesetOrder(destination, { receiverId: 'east', sourceId });
      await order.apply({ ...entry.delivery, changeset: entry.changeset }, (inside, bytes) => applyChangeset(inside, bytes, { tables }));
      assert.deepEqual((await destination.query('SELECT * FROM parents')).rowArrays, [[1n, 'first']]);
    } finally { source.close(); destination.close(); }
  });
}
