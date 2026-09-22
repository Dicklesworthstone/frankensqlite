import assert from 'node:assert/strict';
import { test } from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  ChangesetFanout, CHANGESET_FANOUT_TABLE, CHANGESET_FANOUT_PROGRESS_TABLE,
  assertSingleRecipient, captureFanoutGuard,
} from '../src/changeset-fanout.ts';
import {
  TABLE, ensure, find, load, store, acknowledgeDelivery, forgetDelivery,
} from '../src/changeset-outbox-store.ts';

// Real reference SQLite SQL, sessions and application. No SQL parser/memory-store mock.
// This adapter is not the FrankenSQLite Rust/WASM engine or its MVCC implementation.
class SqliteTarget {
  constructor(path = ':memory:') {
    this.db = new DatabaseSync(path);
    this.db.exec('PRAGMA foreign_keys=ON; CREATE TABLE IF NOT EXISTS items(id INTEGER PRIMARY KEY, value TEXT)');
    this.failCommit = false;
  }
  async execute(sql, params = []) {
    return Number(this.db.prepare(sql).run(...params).changes);
  }
  async query(sql, params = []) {
    const stmt = this.db.prepare(sql);
    stmt.setReadBigInts(true);
    return { rowArrays: stmt.all(...params).map(Object.values) };
  }
  async transaction(work) {
    this.db.exec('BEGIN');
    try {
      const result = await work(this);
      if (this.failCommit) throw new Error('injected pre-commit failure');
      this.db.exec('COMMIT');
      return result;
    } catch (error) {
      this.db.exec('ROLLBACK');
      throw error;
    }
  }
  close() { this.db.close(); }
}
const scope = JSON.stringify({ tables: ['items'], indirect: false });
async function append(target, key, work) {
  return target.transaction(async (tx) => {
    await ensure(tx, true);
    const guard = await captureFanoutGuard(tx);
    const session = target.db.createSession({ table: 'items' });
    try {
      await tx.execute('INSERT INTO items VALUES (?,?)', [BigInt(key), `value-${key}`]);
      if (work) await work(tx);
      const changeset = session.changeset();
      const saved = await store(tx, `source:${key}`, scope, { changeset, changes: 1 }, () => {});
      assert.equal(await captureFanoutGuard(tx), guard, 'source callback changed fanout');
      return saved;
    } finally { session.close(); }
  });
}
async function setup(t, replicas = ['a', 'b', 'c'], count = 3) {
  const target = new SqliteTarget();
  t.after(() => target.close());
  const group = await ChangesetFanout.open(target, replicas), records = [];
  for (let i = 1; i <= count; i++) records.push(await append(target, i));
  return { target, group, records };
}
const ack = (outbox, record) => outbox.acknowledge(record.deliveryId, record.sha256);
const rejected = (promise, suffix) => assert.rejects(promise, (e) => e.code?.endsWith(suffix));
async function payload(target, record) {
  return target.transaction(async (tx) => load(tx, await find(tx, record.deliveryId)));
}

test('three actual SQLite replicas advance independently; last member owns reclamation', async (t) => {
  const { target, group, records } = await setup(t, ['a', 'b', 'c'], 12);
  const replicas = ['a', 'b', 'c'].map(() => new SqliteTarget());
  t.after(() => replicas.forEach((r) => r.close()));
  for (let i = 0; i < 3; i++) {
    const source = group.forReplica(['a', 'b', 'c'][i]);
    assert.deepEqual((await source.pending()).map((d) => d.sequence), records.map((d) => d.sequence));
    for (const record of records) {
      const entry = await source.read(record.deliveryId);
      assert.equal(entry.delivery.acknowledged, false);
      assert.equal(replicas[i].db.applyChangeset(entry.changeset), true);
      assert.equal(await ack(source, record), true);
      assert.equal((await source.read(record.deliveryId)).changeset, null);
      assert.equal((await payload(target, record)) === null, i === 2);
    }
    assert.equal((await source.pending()).length, 0);
    assert.deepEqual(replicas[i].db.prepare('SELECT * FROM items ORDER BY id').all(),
      target.db.prepare('SELECT * FROM items ORDER BY id').all());
  }
  const progress = await group.progress();
  assert.equal(progress.acknowledgedThrough, 12n);
  assert.equal(progress.sourceSequence, 12n);
  assert.deepEqual(progress.replicas.map((r) => r.sequence), [12n, 12n, 12n]);
});

test('reject skipped, forged and unknown acknowledgements without changing progress', async (t) => {
  const { group, records } = await setup(t);
  const a = group.forReplica('a');
  await rejected(ack(a, records[1]), '_ACK');
  await rejected(a.acknowledge(records[0].deliveryId, '0'.repeat(64)), '_ACK');
  await rejected(a.acknowledge('missing', records[0].sha256), '_ACK');
  assert.throws(() => group.forReplica('foreign'), /not a member/);
  assert.deepEqual((await group.progress()).replicas.map((r) => r.sequence), [0n, 0n, 0n]);
});

test('lost ACK response retries are idempotent before and after global reclamation', async (t) => {
  const { target, group, records } = await setup(t);
  for (const id of group.replicas) {
    const r = group.forReplica(id);
    assert.equal(await ack(r, records[0]), true);
    assert.equal(await ack(r, records[0]), false);
  }
  assert.equal(await payload(target, records[0]), null);
  assert.equal(await ack(group.forReplica('a'), records[0]), false);
});

test('failed commit rolls back both the last cursor and reclaimed payload', async (t) => {
  const { target, group, records } = await setup(t, ['a', 'b'], 1);
  await ack(group.forReplica('a'), records[0]);
  target.failCommit = true;
  await assert.rejects(ack(group.forReplica('b'), records[0]), /pre-commit/);
  target.failCommit = false;
  assert.deepEqual((await group.progress()).replicas.map((r) => r.sequence), [1n, 0n]);
  assert.ok(await payload(target, records[0]));
  await ack(group.forReplica('b'), records[0]);
  assert.equal(await payload(target, records[0]), null);
});

test('file reopen recovers exact roster, independent cursors and pending payloads', async () => {
  const path = join(mkdtempSync(join(tmpdir(), 'fsqlite-fanout-')), 'source.sqlite');
  let target = new SqliteTarget(path);
  let group = await ChangesetFanout.open(target, ['b', 'a']);
  const record = await append(target, 1);
  await ack(group.forReplica('a'), record);
  target.close();
  target = new SqliteTarget(path);
  try {
    group = await ChangesetFanout.open(target, ['a', 'b']);
    assert.deepEqual((await group.progress()).replicas.map((r) => r.sequence), [1n, 0n]);
    assert.equal((await group.forReplica('a').pending()).length, 0);
    assert.equal((await group.forReplica('b').pending()).length, 1);
    await ack(group.forReplica('b'), record);
    assert.equal(await payload(target, record), null);
  } finally { target.close(); }
});

test('initialization is immutable, validates input and snapshots roster before admission', async (t) => {
  const target = new SqliteTarget(); t.after(() => target.close());
  for (const invalid of [[], ['a','a'], [''], ['\0'], ['\ud800'], ['é'.repeat(129)], Array(257).fill('a')])
    await rejected(ChangesetFanout.open(target, invalid), '_INPUT');
  assert.equal(target.db.prepare("SELECT count(*) AS n FROM sqlite_schema WHERE name LIKE '__fsqlite%'").get().n, 0);
  const names = ['a', 'A'];
  const promise = ChangesetFanout.open(target, names);
  names[0] = 'changed';
  const group = await promise;
  assert.deepEqual(group.replicas, ['A', 'a']);
  assert.ok(Object.isFrozen(group.replicas));
  await rejected(ChangesetFanout.open(target, ['a']), '_STATE');
  await rejected(ChangesetFanout.open(target, ['a', 'A', 'new']), '_STATE');
});

test('cannot enroll after any prior outbox operation, including explicitly forgotten ACK', async (t) => {
  const target = new SqliteTarget(); t.after(() => target.close());
  const record = await append(target, 1);
  await rejected(ChangesetFanout.open(target, ['a']), '_STATE');
  await target.transaction(async (tx) => {
    await acknowledgeDelivery(tx, record.deliveryId, record.sha256);
    await forgetDelivery(tx, record.deliveryId, record.sha256);
  });
  await rejected(ChangesetFanout.open(target, ['a']), '_STATE');
});

test('legacy acknowledgement/cleanup guard refuses active fanout', async (t) => {
  const { target } = await setup(t);
  await rejected(target.transaction((tx) => assertSingleRecipient(tx)), '_STATE');
});

for (const [name, sql] of [
  ['missing member', `DELETE FROM "${CHANGESET_FANOUT_PROGRESS_TABLE}" WHERE replica_id='b'`],
  ['foreign member', `UPDATE "${CHANGESET_FANOUT_PROGRESS_TABLE}" SET replica_id='foreign' WHERE replica_id='b'`],
  ['missing manifest', `DELETE FROM "${CHANGESET_FANOUT_TABLE}"`],
  ['missing progress table', `DROP TABLE "${CHANGESET_FANOUT_PROGRESS_TABLE}"`],
  ['missing manifest table', `DROP TABLE "${CHANGESET_FANOUT_TABLE}"`],
  ['missing pending row', `DELETE FROM ${TABLE} WHERE seq=2`],
  ['premature global ack', `UPDATE ${TABLE} SET acknowledged=1,payload=X'' WHERE seq=1`],
  ['forged cursor', `UPDATE "${CHANGESET_FANOUT_PROGRESS_TABLE}" SET sequence=1,delivery_id='foreign',sha256='${'0'.repeat(64)}' WHERE replica_id='b'`],
  ['oversized manifest', `UPDATE "${CHANGESET_FANOUT_TABLE}" SET roster=printf('%300000s','x')`],
  ['outbox trigger', `CREATE TRIGGER bypass AFTER UPDATE ON __fsqlite_changeset_outbox BEGIN SELECT 1; END`],
  ['progress trigger', `CREATE TRIGGER bypass AFTER UPDATE ON "${CHANGESET_FANOUT_PROGRESS_TABLE}" BEGIN SELECT 1; END`],
  ['temp progress trigger', `CREATE TEMP TRIGGER bypass AFTER UPDATE ON main."${CHANGESET_FANOUT_PROGRESS_TABLE}" BEGIN SELECT 1; END`],
]) {
  test(`fail closed on ${name}`, async (t) => {
    const { target, group, records } = await setup(t);
    target.db.exec(sql);
    await assert.rejects(group.progress());
    await assert.rejects(ack(group.forReplica('a'), records[0]));
  });
}

test('source metadata tampering rolls back application rows and source append', async (t) => {
  const { target, group } = await setup(t, ['a', 'b'], 0);
  await assert.rejects(append(target, 1, (tx) => tx.execute(`DELETE FROM "${CHANGESET_FANOUT_PROGRESS_TABLE}" WHERE replica_id='b'`)));
  assert.equal(target.db.prepare('SELECT count(*) AS n FROM items').get().n, 0);
  assert.equal((await group.progress()).sourceSequence, 0n);
  await append(target, 1);
});

test('corrupted native payload cannot be acknowledged or returned', async (t) => {
  const { target, group, records } = await setup(t);
  target.db.exec(`UPDATE ${TABLE} SET payload=zeroblob(byte_length) WHERE seq=1`);
  await assert.rejects(group.forReplica('a').read(records[0].deliveryId), /digest/);
  await assert.rejects(ack(group.forReplica('a'), records[0]), /digest/);
  assert.equal((await group.progress()).replicas[0].sequence, 0n);
});

test('explicit identity cleanup retains every current cursor and never skips pending work', async (t) => {
  const { target, group, records } = await setup(t, ['a', 'b']);
  for (const id of group.replicas) await ack(group.forReplica(id), records[0]);
  await rejected(group.forgetAcknowledged(records[0].deliveryId, records[0].sha256), '_STATE');
  for (const id of group.replicas) await ack(group.forReplica(id), records[1]);
  assert.equal(await group.forgetAcknowledged(records[0].deliveryId, records[0].sha256), true);
  assert.equal(await group.forgetAcknowledged(records[0].deliveryId, records[0].sha256), false);
  assert.equal((await group.forReplica('a').pending())[0].sequence, 3n);
  await rejected(ack(group.forReplica('a'), records[0]), '_ACK');
  assert.equal(await target.transaction((tx) => find(tx, records[0].deliveryId)), null);
});

test('owned payload bytes cannot corrupt later deliveries and metadata pages stay bounded', async (t) => {
  const { group, records } = await setup(t);
  const a = group.forReplica('a');
  const read = await a.read(records[0].deliveryId);
  read.changeset.fill(0);
  assert.notEqual((await a.read(records[0].deliveryId)).changeset[0], 0);
  assert.deepEqual((await a.pending({ after: 1n, limit: 1 })).map((r) => r.sequence), [2n]);
  await assert.rejects(a.pending({ after: 1 }));
  await assert.rejects(a.pending({ limit: 257 }));
});
