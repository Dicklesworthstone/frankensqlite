import assert from 'node:assert/strict';
import { test } from 'node:test';
import { spawn } from 'node:child_process';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { SqliteTarget } from './helpers/production-sqlite-target.mjs';
import { ChangesetRebaseJournal } from '../src/changeset-rebase-journal.ts';
import { ChangesetFanout } from '../src/changeset-fanout.ts';
import { applyChangeset } from '../src/changeset-apply.ts';
import { find, load, acknowledgeDelivery, forgetDelivery } from '../src/changeset-outbox-store.ts';

const schema = 'CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT);';
const journalId = 'source:local-history';
const options = { tables: ['t'] };
const LOCAL = 'main."__fsqlite_rebase_journal_locals"';
const OUTBOX = 'main."__fsqlite_changeset_outbox"';
const localCount = s => s.rows('SELECT count(*), coalesce(sum(length(changeset)),0) FROM __fsqlite_rebase_journal_locals')[0];
const ack = (s, d) => s.transaction(tx => acknowledgeDelivery(tx, d.deliveryId, d.sha256));
async function record(s, j, id = 'one', key = 1n) {
  const original = await j.captureLocal(id, tx => tx.execute('INSERT INTO t VALUES(?,?)', [key, 'value']), options);
  const published = await j.enqueueLocal(id, options);
  return { original: original.record, delivery: published.delivery };
}
async function deliver(s, destination, d) {
  const bytes = await s.transaction(async tx => load(tx, await find(tx, d.deliveryId)));
  return applyChangeset(destination, bytes, { ...options, deliveryId: d.deliveryId });
}

test('acknowledged originals can be retired repeatedly under a one-entry local cap', async () => {
  const s = new SqliteTarget(':memory:', schema), destination = new SqliteTarget(':memory:', schema);
  const j = new ChangesetRebaseJournal(s, { journalId, maxLocalEntries: 1 });
  try {
    for (let i = 1; i <= 40; i++) {
      const id = `operation-${i}`, { original, delivery: d } = await record(s, j, id, BigInt(i));
      assert.equal(d.sequence, BigInt(i));
      await assert.rejects(j.captureLocal(`blocked-${i}`, () => assert.fail('full capture ran'), options), { code: 'ERR_FSQLITE_REBASE_JOURNAL_LIMIT' });
      await deliver(s, destination, d); await ack(s, d);
      const released = await j.retireLocal(id, original.recordSha256);
      assert.equal(released.removed, true); assert.equal(released.byteLength, original.byteLength);
      assert.equal(released.delivery.acknowledged, true); assert.equal(released.recordSha256, original.recordSha256);
      assert.deepEqual(localCount(s), [0n, 0n]);
      assert.equal(await j.readLocal(id), null);
      assert.equal((await j.retireLocal(id, original.recordSha256)).removed, false);
      await assert.rejects(j.captureLocal(id, () => assert.fail('retired callback ran'), options), { code: 'ERR_FSQLITE_REBASE_JOURNAL_HISTORY' });
      await assert.rejects(j.enqueueLocal(id, options), { code: 'ERR_FSQLITE_REBASE_JOURNAL_MISSING' });
      assert.equal(await s.transaction(tx => forgetDelivery(tx, d.deliveryId, d.sha256)), true);
    }
    assert.deepEqual(s.rows(), destination.rows());
  } finally { s.close(); destination.close(); }
});

test('a slow required replica prevents retirement even after another replica acknowledges', async () => {
  const s = new SqliteTarget(':memory:', schema);
  const j = new ChangesetRebaseJournal(s, { journalId });
  try {
    const fanout = await ChangesetFanout.open(s, ['east', 'west']);
    const { original, delivery: d } = await record(s, j);
    await fanout.forReplica('east').acknowledge(d.deliveryId, d.sha256);
    await assert.rejects(j.retireLocal('one', original.recordSha256));
    assert.deepEqual((await j.readLocal('one')).changeset, original.changeset);
    const progress = await fanout.progress();
    await fanout.forReplica('west').acknowledge(d.deliveryId, d.sha256);
    const finalProgress = await fanout.progress();
    assert.equal(progress.acknowledgedThrough, 0n); assert.equal(finalProgress.acknowledgedThrough, 1n);
    assert.equal((await j.retireLocal('one', original.recordSha256)).removed, true);
    assert.deepEqual(await fanout.progress(), finalProgress);
  } finally { s.close(); }
});

test('unpublished and unacknowledged originals cannot be retired or silently enqueued', async () => {
  const s = new SqliteTarget(':memory:', schema), j = new ChangesetRebaseJournal(s, { journalId });
  try {
    const { record: original } = await j.captureLocal('one', tx => tx.execute("INSERT INTO t VALUES(1,'v')"), options);
    await assert.rejects(j.retireLocal('one', original.recordSha256));
    assert.equal(s.rows("SELECT count(*) FROM sqlite_schema WHERE name='__fsqlite_changeset_outbox'")[0][0], 0n);
    await j.enqueueLocal('one', options);
    await assert.rejects(j.retireLocal('one', original.recordSha256));
    assert.deepEqual((await j.readLocal('one')).changeset, original.changeset);
  } finally { s.close(); }
});

for (const encoding of ['UTF-8', 'UTF-16le', 'UTF-16be']) {
  test(`${encoding}: typed original and publication survive reopen, then retire without changing user rows`, async () => {
    const directory = mkdtempSync(join(tmpdir(), 'fsqlite-local-retire-'));
    const path = join(directory, 'source.db');
    let s = new SqliteTarget(path, `PRAGMA encoding='${encoding}'; CREATE TABLE t(id INTEGER PRIMARY KEY,v,payload,weight);`);
    const id = '\uFEFF' + '界'.repeat(100), jid = 'j'.repeat(512);
    let j = new ChangesetRebaseJournal(s, { journalId: jid });
    try {
      const native = s.db.createSession({ table: 't' });
      const { record: original } = await j.captureLocal(id, tx => tx.execute('INSERT INTO t VALUES(?,?,?,CAST(? AS REAL))',
        [9007199254740993n, '\uFEFFa\0😀', new Uint8Array(8192).fill(173), 2]), options);
      assert.deepEqual(original.changeset, new Uint8Array(native.changeset())); native.close();
      const { delivery: d } = await j.enqueueLocal(id, options); await ack(s, d);
      const rows = s.rows('SELECT id,hex(CAST(v AS BLOB)),hex(payload),typeof(weight),weight FROM t');
      s.close(); s = new SqliteTarget(path); j = new ChangesetRebaseJournal(s, { journalId: jid });
      assert.equal((await j.retireLocal(id, original.recordSha256)).byteLength, original.byteLength);
      assert.deepEqual(s.rows('SELECT id,hex(CAST(v AS BLOB)),hex(payload),typeof(weight),weight FROM t'), rows);
      s.close(); s = new SqliteTarget(path); j = new ChangesetRebaseJournal(s, { journalId: jid });
      assert.equal((await j.retireLocal(id, original.recordSha256)).removed, false);
      await assert.rejects(j.captureLocal(id, () => assert.fail('reopen repeated work'), options));
    } finally { s.close(); }
  });
}

const snapshot = s => [s.rows(`SELECT * FROM ${LOCAL} ORDER BY journal_id,operation_id`), s.rows(`SELECT * FROM ${OUTBOX} ORDER BY seq`), s.rows()];
for (const [label, sql] of [
  ['changed original bytes', `UPDATE ${LOCAL} SET changeset=zeroblob(byte_length)`],
  ['changed original checksum', `UPDATE ${LOCAL} SET sha256=replace(sha256,'a','z') || '0'`],
  ['changed original basis', `UPDATE ${LOCAL} SET basis_position=1`],
  ['changed original record identity', `UPDATE ${LOCAL} SET record_sha256=printf('%064d',1)`],
  ['changed original byte accounting', `UPDATE ${LOCAL} SET byte_length=byte_length+1`],
  ['changed original row accounting', `UPDATE ${LOCAL} SET touched_rows=touched_rows+1`],
]) test(`refuses ${label} without erasing evidence`, async () => {
  const s = new SqliteTarget(':memory:', schema), j = new ChangesetRebaseJournal(s, { journalId });
  try {
    const { original, delivery: d } = await record(s, j); await ack(s, d); s.db.exec(sql);
    const before = snapshot(s);
    await assert.rejects(j.retireLocal('one', original.recordSha256));
    assert.deepEqual(snapshot(s), before);
  } finally { s.close(); }
});

for (const [label, change] of [
  ['table scope', r => { r.tables = ['other']; }],
  ['indirect policy', r => { r.indirect = !r.indirect; }],
  ['format', r => { r.rebasedLocal.format = 'wrong'; }],
  ['operation identity', r => { r.rebasedLocal.operationId = 'different'; }],
  ['journal identity', r => { r.rebasedLocal.journalId = 'different'; }],
  ['record identity', r => { r.rebasedLocal.recordSha256 = '0'.repeat(64); }],
  ['basis digest', r => { r.rebasedLocal.afterBookmark.sha256 = '0'.repeat(64); }],
  ['selected history', r => { r.rebasedLocal.throughBookmark.position++; }],
  ['non-bookmark boundary', r => { r.rebasedLocal.afterBookmark = 0; }],
  ['null choice', r => { r.rebasedLocal = null; }],
  ['seal', r => { r.rebasedLocal.seal = '0'.repeat(64); }],
  ['extra fields', r => { r.accepted = true; }],
]) test(`refuses publication with altered ${label}`, async () => {
  const s = new SqliteTarget(':memory:', schema), j = new ChangesetRebaseJournal(s, { journalId });
  try {
    const { original, delivery: d } = await record(s, j); await ack(s, d);
    const r = JSON.parse(s.rows(`SELECT scope FROM ${OUTBOX}`)[0][0]); change(r);
    s.db.prepare(`UPDATE ${OUTBOX} SET scope=?`).run(JSON.stringify(r));
    const before = snapshot(s);
    await assert.rejects(j.retireLocal('one', original.recordSha256));
    assert.deepEqual(snapshot(s), before);
  } finally { s.close(); }
});

for (const sql of [
  `UPDATE ${OUTBOX} SET sha256=printf('%064d',1)`,
  `UPDATE ${OUTBOX} SET byte_length=byte_length+1`,
  `UPDATE ${OUTBOX} SET change_count=change_count+1`,
  `UPDATE ${OUTBOX} SET payload=X'01'`,
]) test(`refuses changed acknowledged payload evidence: ${sql}`, async () => {
  const s = new SqliteTarget(':memory:', schema), j = new ChangesetRebaseJournal(s, { journalId });
  try {
    const { original, delivery: d } = await record(s, j); await ack(s, d); s.db.exec(sql);
    const before = snapshot(s);
    await assert.rejects(j.retireLocal('one', original.recordSha256)); assert.deepEqual(snapshot(s), before);
  } finally { s.close(); }
});

test('missing publication and wrong expected record cannot authorize retirement', async () => {
  const s = new SqliteTarget(':memory:', schema), j = new ChangesetRebaseJournal(s, { journalId });
  try {
    const { original, delivery: d } = await record(s, j); await ack(s, d);
    await assert.rejects(j.retireLocal('one', '0'.repeat(64)), { code: 'ERR_FSQLITE_REBASE_JOURNAL_HISTORY' });
    await s.transaction(tx => forgetDelivery(tx, d.deliveryId, d.sha256));
    await assert.rejects(j.retireLocal('one', original.recordSha256), { code: 'ERR_FSQLITE_REBASE_JOURNAL_MISSING' });
    assert.deepEqual((await j.readLocal('one')).changeset, original.changeset);
  } finally { s.close(); }
});

test('missing original still requires the correct acknowledged sealed publication', async () => {
  const s = new SqliteTarget(':memory:', schema), j = new ChangesetRebaseJournal(s, { journalId });
  try {
    const { original, delivery: d } = await record(s, j); await ack(s, d);
    await j.retireLocal('one', original.recordSha256);
    await assert.rejects(j.retireLocal('one', 'f'.repeat(64)), { code: 'ERR_FSQLITE_REBASE_JOURNAL_HISTORY' });
    s.db.prepare(`UPDATE ${OUTBOX} SET scope=?`).run(JSON.stringify({ tables:['t'], indirect:false }));
    await assert.rejects(j.retireLocal('one', original.recordSha256));
    assert.deepEqual(localCount(s), [0n, 0n]);
  } finally { s.close(); }
});

test('cleanup works above reduced aggregate limits and does not release other journals', async () => {
  const s = new SqliteTarget(':memory:', schema), j = new ChangesetRebaseJournal(s, { journalId });
  try {
    const a = await record(s, j), b = await record(s, j, 'two', 2n);
    const other = new ChangesetRebaseJournal(s, { journalId:'other' });
    const c = await record(s, other, 'one', 3n);
    await ack(s, a.delivery); await ack(s, b.delivery); await ack(s, c.delivery);
    const limited = new ChangesetRebaseJournal(s, { journalId, maxLocalEntries:1, maxLocalBytes:1 });
    assert.equal((await limited.retireLocal('one', a.original.recordSha256)).removed, true);
    assert.equal((await limited.retireLocal('two', b.original.recordSha256)).removed, true);
    assert.deepEqual((await other.readLocal('one')).changeset, c.original.changeset);
    assert.equal(localCount(s)[0], 1n);
  } finally { s.close(); }
});

test('fully superseded empty publication still requires acknowledgement, then frees original bytes', async () => {
  const s = new SqliteTarget(':memory:', schema + "INSERT INTO t VALUES(1,'old');");
  const j = new ChangesetRebaseJournal(s, { journalId }), peer = new SqliteTarget(':memory:', schema + "INSERT INTO t VALUES(1,'old');");
  try {
    const { record: original } = await j.captureLocal('one', tx => tx.execute("UPDATE t SET v='local'"), options);
    const native = peer.db.createSession(); peer.db.exec("UPDATE t SET v='remote'");
    await j.apply(native.changeset(), { ...options, deliveryId:'remote:1', onConflict:()=>'replace' }); native.close();
    const { delivery: d } = await j.enqueueLocal('one', options); assert.equal(d.byteLength, 0);
    await assert.rejects(j.retireLocal('one', original.recordSha256));
    await ack(s, d); assert.equal((await j.retireLocal('one', original.recordSha256)).byteLength, original.byteLength);
    assert.equal(s.rows()[0][1], 'remote');
    assert.equal((await j.read('remote:1')).position, 1);
  } finally { s.close(); peer.close(); }
});

test('net-zero originals can be retired, but empty bytes alone are not a retirement marker', async () => {
  const s = new SqliteTarget(':memory:', schema), j = new ChangesetRebaseJournal(s, { journalId });
  try {
    const { record: original } = await j.captureLocal('empty', () => 7, options);
    const { delivery: d } = await j.enqueueLocal('empty', options);
    assert.equal(original.byteLength, 0); await assert.rejects(j.retireLocal('empty', original.recordSha256));
    await ack(s, d); assert.equal((await j.retireLocal('empty', original.recordSha256)).removed, true);
    assert.equal((await j.retireLocal('empty', original.recordSha256)).removed, false);
  } finally { s.close(); }
});

test('retirement neither traverses nor repairs remote history after publication', async () => {
  const s = new SqliteTarget(':memory:', schema), j = new ChangesetRebaseJournal(s, { journalId });
  try {
    await j.apply(new Uint8Array(), { ...options, deliveryId:'remote:1' });
    const { original, delivery: d } = await record(s, j); await ack(s, d);
    s.db.exec('DELETE FROM __fsqlite_rebase_journal_entries');
    await assert.rejects(j.bookmark()); s.statements.length = 0;
    await j.retireLocal('one', original.recordSha256);
    assert.equal(s.statements.some(sql => /FROM main\."__fsqlite_rebase_journal_(heads|entries)"/.test(sql)), false);
    await assert.rejects(j.bookmark());
  } finally { s.close(); }
});

test('metadata triggers cannot execute during retirement', async () => {
  const s = new SqliteTarget(':memory:', schema), j = new ChangesetRebaseJournal(s, { journalId });
  try {
    const { original, delivery: d } = await record(s, j); await ack(s, d);
    s.db.exec(`CREATE TRIGGER surprise AFTER DELETE ON __fsqlite_rebase_journal_locals BEGIN DELETE FROM t; END;`);
    await assert.rejects(j.retireLocal('one', original.recordSha256), { code:'ERR_FSQLITE_REBASE_JOURNAL_SCHEMA' });
    assert.equal(localCount(s)[0], 1n); assert.equal(s.rows().length, 1);
  } finally { s.close(); }
});

for (const advertised of [0, 1, 2]) test(`incorrect deletion result ${advertised} cannot commit partial cleanup`, async () => {
  const s = new SqliteTarget(':memory:', schema), j = new ChangesetRebaseJournal(s, { journalId });
  try {
    const { original, delivery: d } = await record(s, j); await ack(s, d);
    const target = { transaction: work => s.transaction(tx => work({ query:tx.query,
      execute: async (sql, params) => {
        if (!sql.startsWith(`DELETE FROM ${LOCAL}`)) return tx.execute(sql, params);
        if (advertised !== 1) await tx.execute(sql, params); // 1 lies without deleting.
        return advertised;
      },
    })) };
    await assert.rejects(new ChangesetRebaseJournal(target, { journalId }).retireLocal('one', original.recordSha256), { code:'ERR_FSQLITE_REBASE_JOURNAL_CORRUPT' });
    assert.deepEqual((await j.readLocal('one')).changeset, original.changeset);
  } finally { s.close(); }
});

for (const mode of ['cancel', 'timeout']) test(`${mode} after actual DELETE rolls back original removal`, async () => {
  const s = new SqliteTarget(':memory:', schema), j = new ChangesetRebaseJournal(s, { journalId }), abort = new AbortController();
  try {
    const { original, delivery: d } = await record(s, j); await ack(s, d);
    s.after = async (kind, sql) => {
      if (kind === 'execute' && sql.startsWith(`DELETE FROM ${LOCAL}`)) {
        if (mode === 'cancel') abort.abort('stop'); else await new Promise(resolve => setTimeout(resolve, 40));
      }
    };
    await assert.rejects(j.retireLocal('one', original.recordSha256,
      mode === 'cancel' ? { signal:abort.signal } : { timeoutMs:25 }),
      { code:`ERR_FSQLITE_REBASE_JOURNAL_${mode === 'cancel' ? 'CANCELLED' : 'TIMEOUT'}` });
    s.after = null;
    assert.deepEqual((await j.readLocal('one')).changeset, original.changeset);
    assert.equal((await j.retireLocal('one', original.recordSha256)).removed, true);
  } finally { s.close(); }
});

test('cancellation waits for an admitted delayed DELETE before rollback', async () => {
  const s = new SqliteTarget(':memory:', schema), j = new ChangesetRebaseJournal(s, { journalId }), abort = new AbortController();
  let release, entered;
  const gate = new Promise(r => { release=r; }), started = new Promise(r => { entered=r; });
  try {
    const { original, delivery: d } = await record(s, j); await ack(s, d);
    s.before = async (kind, sql) => { if (kind === 'execute' && sql.startsWith(`DELETE FROM ${LOCAL}`)) { entered(); await gate; } };
    let settled = false;
    const work = j.retireLocal('one', original.recordSha256, { signal:abort.signal });
    const rejected = assert.rejects(work, { code:'ERR_FSQLITE_REBASE_JOURNAL_CANCELLED' });
    void work.then(() => { settled=true; }, () => { settled=true; });
    await started; abort.abort(); await new Promise(r => setImmediate(r));
    assert.equal(settled, false); assert.equal(s.active, true);
    release(); await rejected; s.before=null;
    assert.equal(s.active, false); assert.deepEqual((await j.readLocal('one')).changeset, original.changeset);
  } finally { release?.(); s.close(); }
});

test('lost COMMIT reply is recovered without recapturing or regenerating acknowledged work', async () => {
  const s = new SqliteTarget(':memory:', schema), j = new ChangesetRebaseJournal(s, { journalId });
  try {
    const { original, delivery: d } = await record(s, j); await ack(s, d);
    s.afterCommit = async () => { throw new Error('response lost'); };
    await assert.rejects(j.retireLocal('one', original.recordSha256), /response lost/); s.afterCommit=null;
    const replay = await j.retireLocal('one', original.recordSha256);
    assert.equal(replay.removed, false); assert.equal(replay.byteLength, 0);
    await assert.rejects(j.captureLocal('one', () => assert.fail('business callback repeated'), options));
    assert.deepEqual(s.rows(), [[1n,'value']]);
  } finally { s.close(); }
});

test('outer rollback preserves the original after provisional cleanup', async () => {
  const s = new SqliteTarget(':memory:', schema), j = new ChangesetRebaseJournal(s, { journalId });
  try {
    const { original, delivery: d } = await record(s, j); await ack(s, d);
    await assert.rejects(s.transaction(async tx => {
      const nested = new ChangesetRebaseJournal({ transaction:work=>work(tx) }, { journalId });
      assert.equal((await nested.retireLocal('one', original.recordSha256)).removed, true);
      throw new Error('outer failure');
    }), /outer failure/);
    assert.deepEqual((await j.readLocal('one')).changeset, original.changeset);
  } finally { s.close(); }
});

test('deferred foreign-key COMMIT failure rolls back cleanup', async () => {
  const s = new SqliteTarget(':memory:', schema + 'CREATE TABLE p(id PRIMARY KEY); CREATE TABLE child(pid REFERENCES p DEFERRABLE INITIALLY DEFERRED);');
  const j = new ChangesetRebaseJournal(s, { journalId });
  try {
    const { original, delivery: d } = await record(s, j); await ack(s, d);
    s.beforeCommit = async () => s.db.exec('INSERT INTO child VALUES(9)');
    await assert.rejects(j.retireLocal('one', original.recordSha256), /FOREIGN KEY/); s.beforeCommit=null;
    assert.deepEqual((await j.readLocal('one')).changeset, original.changeset);
    assert.deepEqual(s.rows('SELECT * FROM child'), []);
  } finally { s.close(); }
});

test('post-deletion publication meddling is detected and rolled back', async () => {
  const s = new SqliteTarget(':memory:', schema), j = new ChangesetRebaseJournal(s, { journalId });
  try {
    const { original, delivery: d } = await record(s, j); await ack(s, d);
    const before = snapshot(s);
    s.after = async (kind, sql) => { if (kind === 'execute' && sql.startsWith(`DELETE FROM ${LOCAL}`)) s.db.exec(`UPDATE ${OUTBOX} SET change_count=0`); };
    await assert.rejects(j.retireLocal('one', original.recordSha256)); s.after=null;
    assert.deepEqual(snapshot(s), before);
  } finally { s.close(); }
});

test('invalid identities, checksums and controls reject before SQL admission', async () => {
  let calls = 0;
  const j = new ChangesetRebaseJournal({ transaction:() => { calls++; throw new Error('entered'); } }, { journalId });
  for (const [id, hash, controls] of [
    ['', '0'.repeat(64), {}], ['x\0', '0'.repeat(64), {}], ['x', 'bad', {}], ['x', 'A'.repeat(64), {}],
    ['x', '0'.repeat(64), { timeoutMs:0 }], ['x', '0'.repeat(64), { signal:{} }],
    ['x', '0'.repeat(64), { signal:AbortSignal.abort('cancel') }],
  ]) await assert.rejects(j.retireLocal(id, hash, controls), e => e.message !== 'entered');
  assert.equal(calls, 0);
});

test('missing original with a surviving publication cannot execute a capture callback', async () => {
  const s = new SqliteTarget(':memory:', schema), j = new ChangesetRebaseJournal(s, { journalId });
  try {
    const { delivery: d } = await record(s, j);
    s.db.exec(`DELETE FROM ${LOCAL}`); // Corruption, not legitimate retirement: still pending.
    await assert.rejects(j.captureLocal('one', tx => tx.execute("INSERT INTO t VALUES(2,'wrong')"), options), { code:'ERR_FSQLITE_REBASE_JOURNAL_HISTORY' });
    assert.deepEqual(s.rows(), [[1n,'value']]);
    assert.equal((await s.transaction(tx=>find(tx,d.deliveryId))).delivery.acknowledged, false);
  } finally { s.close(); }
});

for (const damage of ['premature global acknowledgement', 'missing replica', 'changed roster']) {
  test(`fanout ${damage} never authorizes local retirement`, async () => {
    const s = new SqliteTarget(':memory:', schema), j = new ChangesetRebaseJournal(s, { journalId });
    try {
      const fanout = await ChangesetFanout.open(s, ['east', 'west']);
      const { original, delivery: d } = await record(s, j);
      await fanout.forReplica('east').acknowledge(d.deliveryId, d.sha256);
      if (damage === 'premature global acknowledgement') s.db.exec(`UPDATE ${OUTBOX} SET acknowledged=1,payload=X''`);
      else if (damage === 'missing replica') s.db.exec("DELETE FROM __fsqlite_changeset_fanout_progress WHERE replica_id='west'");
      else s.db.exec("UPDATE __fsqlite_changeset_fanout SET roster='{}'");
      await assert.rejects(j.retireLocal('one', original.recordSha256));
      assert.deepEqual((await j.readLocal('one')).changeset, original.changeset);
    } finally { s.close(); }
  });
}

test('retirement does not rescan application tables or require their original schema', async () => {
  const s = new SqliteTarget(':memory:', schema), j = new ChangesetRebaseJournal(s, { journalId });
  try {
    const { original, delivery: d } = await record(s, j); await ack(s, d);
    s.db.exec('ALTER TABLE t RENAME TO archived'); s.statements.length=0;
    assert.equal((await j.retireLocal('one', original.recordSha256)).removed, true);
    assert.deepEqual(s.rows('SELECT * FROM archived'), [[1n,'value']]);
    assert.equal(s.statements.some(sql => /FROM (?:main\.)?"?(?:t|archived)"?(?: |$)/.test(sql)), false);
  } finally { s.close(); }
});

test('retirement bounds original transfer before loading an oversized message', async () => {
  const s = new SqliteTarget(':memory:', schema), j = new ChangesetRebaseJournal(s, { journalId });
  try {
    const { record: original } = await j.captureLocal('large', tx => tx.execute('INSERT INTO t VALUES(1,?)', ['a'.repeat(8192)]), options);
    const { delivery: d } = await j.enqueueLocal('large', options); await ack(s,d);
    const small = new ChangesetRebaseJournal(s, { journalId, limits:{maxBytes:1024} });
    let observed = false;
    s.after = async (kind, sql, params, result) => {
      if (kind === 'query' && sql.includes(' AS BLOB') && sql.includes(`FROM ${LOCAL} WHERE`)) {
        observed=true; assert.equal(result.rowArrays[0][8], null);
      }
    };
    await assert.rejects(small.retireLocal('large', original.recordSha256));
    assert.equal(observed,true); s.after=null;
    assert.equal((await j.readLocal('large')).byteLength,original.byteLength);
  } finally { s.close(); }
});

test('input controls are captured before asynchronous identity hashing/admission', async () => {
  const s = new SqliteTarget(':memory:', schema), j = new ChangesetRebaseJournal(s, { journalId });
  try {
    const { original, delivery:d } = await record(s,j); await ack(s,d);
    const control={timeoutMs:5000};
    const running=j.retireLocal('one',original.recordSha256,control);
    control.timeoutMs=0; control.signal=AbortSignal.abort();
    assert.equal((await running).removed,true);
  } finally { s.close(); }
});

test('two independent WAL owners overlap before deletion; loser reconciles without duplicate cleanup', async () => {
  const directory=mkdtempSync(join(tmpdir(),'fsqlite-retirement-race-')), path=join(directory,'source.db');
  const a=new SqliteTarget(path,schema); a.db.exec('PRAGMA journal_mode=WAL');
  const ja=new ChangesetRebaseJournal(a,{journalId});
  const {original,delivery:d}=await record(a,ja); await ack(a,d);
  const b=new SqliteTarget(path), jb=new ChangesetRebaseJournal(b,{journalId});
  let open; const gate=new Promise(r=>{open=r;}); let arrivals=0;
  for(const s of [a,b]) {
    let first=true;
    s.after=async(kind,sql)=>{
      if(first && kind==='query' && sql.includes(`FROM ${LOCAL} WHERE`) && sql.startsWith('SELECT count(*)')) {
        first=false; if(++arrivals===2) open(); await gate;
      }
    };
  }
  try {
    const results=await Promise.allSettled([ja.retireLocal('one',original.recordSha256),jb.retireLocal('one',original.recordSha256)]);
    assert.equal(arrivals,2);
    assert.equal(results.filter(r=>r.status==='fulfilled' && r.value.removed).length,1);
    assert.equal(results.filter(r=>r.status==='rejected').length,1);
    a.after=b.after=null;
    assert.equal((await ja.retireLocal('one',original.recordSha256)).removed,false);
    assert.equal((await jb.retireLocal('one',original.recordSha256)).removed,false);
    assert.deepEqual(a.rows(),[[1n,'value']]);
  } finally { open();a.close();b.close(); }
});

async function crash(config) {
  const child=spawn(process.execPath,[
    '--experimental-transform-types', '--experimental-loader=./packages/sdk/tests/helpers/production-source-loader.mjs',
    './packages/sdk/tests/helpers/local-retirement-child.mjs', JSON.stringify(config),
  ],{stdio:['ignore','pipe','pipe','ipc']});
  let error='', reached=false, watchdog=false;
  child.stdout.on('data',b=>{error+=b;});child.stderr.on('data',b=>{error+=b;});
  child.on('message',message=>{if(message.cut===config.cut) reached=true;});
  const timer=setTimeout(()=>{watchdog=true;child.kill('SIGKILL');},10000);
  try {
    const status=await new Promise((resolve,reject)=>{child.once('error',reject);child.once('close',(code,signal)=>resolve({code,signal}));});
    assert.equal(watchdog,false,`watchdog cannot stand in for the requested cut: ${error}`);
    assert.equal(reached,true,`cut not reached: ${error}`);
    assert.equal(status.signal,'SIGKILL',error);assert.equal(status.code,null,error);
  } finally {clearTimeout(timer);}
}
for(const mode of ['WAL','DELETE']) for(const cut of ['before-delete','after-delete','before-commit','after-commit']) {
  test(`${mode}: SIGKILL at ${cut} recovers original retention and keeps acknowledged publication`,async()=>{
    const directory=mkdtempSync(join(tmpdir(),'fsqlite-retire-crash-')), path=join(directory,'source.db');
    let s=new SqliteTarget(path,schema);s.db.exec(`PRAGMA journal_mode=${mode}; PRAGMA synchronous=FULL;`);
    let j=new ChangesetRebaseJournal(s,{journalId});
    const {original,delivery:d}=await record(s,j);await ack(s,d);s.close();
    await crash({path,cut,journalId,recordSha256:original.recordSha256});
    s=new SqliteTarget(path);j=new ChangesetRebaseJournal(s,{journalId});
    try {
      const afterCommit=cut==='after-commit';
      assert.equal((await j.readLocal('one'))===null,afterCommit);
      assert.equal((await j.retireLocal('one',original.recordSha256)).removed,!afterCommit);
      assert.equal((await j.retireLocal('one',original.recordSha256)).removed,false);
      assert.equal((await s.transaction(tx=>find(tx,d.deliveryId))).delivery.acknowledged,true);
      await assert.rejects(j.captureLocal('one',()=>assert.fail('process recovery reran business work'),options));
      assert.deepEqual(s.rows(),[[1n,'value']]);
    } finally {s.close();}
  });
}
