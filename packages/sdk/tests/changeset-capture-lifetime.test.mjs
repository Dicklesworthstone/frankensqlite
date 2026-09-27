import assert from 'node:assert/strict';
import { test } from 'node:test';
import { setTimeout as delay } from 'node:timers/promises';
import { captureChangeset } from '../src/changeset-capture.ts';
import { applyChangeset } from '../src/changeset-apply.ts';
import { SqliteTarget } from './helpers/production-sqlite-target.mjs';

const schema = 'CREATE TABLE t(id INTEGER PRIMARY KEY,value TEXT)';
const gate = () => { let resolve; const promise = new Promise(r => { resolve = r; }); return { promise, resolve }; };

test('journal capture drains unawaited admitted SQL before collecting or committing', async () => {
  const source = new SqliteTarget(':memory:', schema), receiver = new SqliteTarget(':memory:', schema);
  const started = gate(), release = gate();
  let sql, done = false;
  source.before = async (kind, statement) => {
    if (kind === 'execute' && statement === "INSERT INTO t VALUES(1,'late')") { started.resolve(); await release.promise; }
  };
  const operation = captureChangeset(source, tx => { sql = tx.execute("INSERT INTO t VALUES(1,'late')"); return 42; }, { tables: ['t'] });
  void operation.then(() => { done = true; }, () => { done = true; });
  try {
    await started.promise; await delay(15);
    assert.equal(done, false, 'capture ended while its admitted INSERT was still active');
    assert.equal(source.active, true);
    release.resolve();
    const captured = await operation; await sql;
    assert.equal(captured.value, 42); assert.equal(captured.changes, 1);
    await applyChangeset(receiver, captured.changeset, { tables: ['t'] });
    assert.deepEqual(receiver.rows(), source.rows());
  } finally { release.resolve(); await Promise.allSettled([operation, sql]); source.close(); receiver.close(); }
});

test('a retained journal executor cannot write after its capture commits', async () => {
  const source = new SqliteTarget(':memory:', schema); let saved;
  try {
    await captureChangeset(source, tx => { saved = tx; }, { tables: ['t'] });
    await assert.rejects(saved.execute("INSERT INTO t VALUES(1,'escaped')"), /scope has ended/);
    assert.deepEqual(source.rows(), []);
  } finally { source.close(); }
});

test('a caught SQL failure cannot publish partial captured work', async () => {
  const source = new SqliteTarget(':memory:', schema);
  try {
    await assert.rejects(captureChangeset(source, async tx => {
      await tx.execute("INSERT INTO t VALUES(1,'first')");
      await tx.execute("INSERT INTO t VALUES(1,'duplicate')").catch(() => {});
    }, { tables: ['t'] }), /UNIQUE/);
    assert.deepEqual(source.rows(), []);
  } finally { source.close(); }
});

import { captureSnapshotChangeset } from '../src/changeset-snapshot-capture.ts';
import { ChangesetOutbox } from '../src/changeset-outbox.ts';
import { ChangesetFanout } from '../src/changeset-fanout.ts';
import { decodeChangeset } from '../src/changeset-codec.ts';
const modes = ['journal', 'snapshot', 'outbox', 'snapshot-outbox'];
async function run(mode, target, work, controls = {}, id = 'source:operation') {
  const options = { tables: ['t'], ...controls };
  if (mode === 'journal') return captureChangeset(target, work, options);
  if (mode === 'snapshot') return captureSnapshotChangeset(target, work, options);
  return new ChangesetOutbox(target)[mode === 'outbox' ? 'record' : 'recordSnapshot'](work, { ...options, deliveryId: id });
}
async function payload(mode, target, result) {
  return mode.includes('outbox') ? (await new ChangesetOutbox(target).read(result.delivery.deliveryId)).changeset : result.changeset;
}
function noArtifacts(source) {
  assert.deepEqual(source.rows("SELECT name FROM temp.sqlite_schema WHERE name GLOB '__fsqlite_capture_*'"), []);
}
for (const mode of modes) {
  for (const method of ['execute', 'query']) test(`${mode}: await admitted ${method} even without callback await`, async () => {
    const source = new SqliteTarget(':memory:', schema), receiver = new SqliteTarget(':memory:', schema);
    const started = gate(), release = gate(); let sql, done = false;
    const statement = `INSERT INTO t VALUES(1,'late')${method === 'query' ? ' RETURNING id' : ''}`;
    source.before = async (kind, text) => { if (kind === method && text === statement) { started.resolve(); await release.promise; } };
    const operation = run(mode, source, tx => { sql = tx[method](statement); return 41; });
    void operation.then(() => { done = true; }, () => { done = true; });
    try {
      await started.promise; await delay(10); assert.equal(done, false); assert.equal(source.active, true);
      release.resolve(); const result = await operation; await sql;
      assert.equal(result.value, 41);
      const bytes = await payload(mode, source, result);
      assert.equal(decodeChangeset(bytes)[0].changes.length, 1);
      await applyChangeset(receiver, bytes, { tables: ['t'] });
      assert.deepEqual(receiver.rows(), source.rows()); noArtifacts(source);
    } finally { release.resolve(); await Promise.allSettled([operation, sql]); source.close(); receiver.close(); }
  });

  for (const ending of ['commit', 'throw']) test(`${mode}: retained executor rejects after ${ending}, even inside later work`, async () => {
    const source = new SqliteTarget(':memory:', schema); let saved;
    try {
      const first = run(mode, source, tx => { saved = tx; if (ending === 'throw') throw new Error('callback failed'); });
      if (ending === 'throw') await assert.rejects(first, /callback failed/); else await first;
      const before = source.statements.length;
      await assert.rejects(saved.execute("INSERT INTO t VALUES(1,'escape')"), /scope has ended/);
      await assert.rejects(saved.query('SELECT 1'), /scope has ended/);
      assert.equal(source.statements.length, before);
      await run(mode, source, async tx => {
        await assert.rejects(saved.execute("INSERT INTO t VALUES(2,'escape')"), /scope has ended/);
        await tx.execute("INSERT INTO t VALUES(3,'valid')");
      }, {}, 'source:next');
      assert.deepEqual(source.rows(), [[3n, 'valid']]); noArtifacts(source);
    } finally { source.close(); }
  });

  for (const ending of ['throw', 'cancel', 'timeout']) test(`${mode}: ${ending} drains delayed SQL before rolling back`, async () => {
    const source = new SqliteTarget(':memory:', schema);
    const entered = gate(), release = gate(); const abort = new AbortController(); let sql, done = false;
    source.before = async (kind, text) => { if (kind === 'execute' && text === "INSERT INTO t VALUES(1,'held')") { entered.resolve(); await release.promise; } };
    const operation = run(mode, source, tx => {
      sql = tx.execute("INSERT INTO t VALUES(1,'held')");
      if (ending === 'throw') throw new Error('callback failed');
    }, ending === 'timeout' ? { timeoutMs: 100 } : { signal: abort.signal });
    void operation.then(() => { done = true; }, () => { done = true; });
    try {
      await entered.promise; if (ending === 'cancel') abort.abort(new Error('cancelled by test'));
      await delay(ending === 'timeout' ? 120 : 10); assert.equal(done, false); assert.equal(source.active, true);
      release.resolve(); await assert.rejects(operation); await Promise.allSettled([sql]);
      assert.deepEqual(source.rows(), []); noArtifacts(source);
      if (mode.includes('outbox')) assert.deepEqual(await new ChangesetOutbox(source).pending(), []);
    } finally { release.resolve(); await Promise.allSettled([operation, sql]); source.close(); }
  });

  test(`${mode}: every admitted failure poisons capture, even if caught`, async () => {
    const source = new SqliteTarget(':memory:', schema); let saved;
    try {
      await assert.rejects(run(mode, source, async tx => {
        saved = tx; await tx.execute("INSERT INTO t VALUES(1,'first')");
        for (let i = 0; i < 20; i++) await tx.execute("INSERT INTO t VALUES(1,'duplicate')").catch(() => {});
      }), /UNIQUE/);
      assert.deepEqual(source.rows(), []); noArtifacts(source);
      await assert.rejects(saved.query('SELECT 1'), /scope has ended/);
    } finally { source.close(); }
  });
}

for (const mode of ['outbox', 'snapshot-outbox']) test(`${mode}: lost source response retains all submitted writes and retry runs no callback`, async () => {
  const source = new SqliteTarget(':memory:', schema), receiver = new SqliteTarget(':memory:', schema);
  let calls = 0;
  try {
    source.afterCommit = () => { throw new Error('lost commit response'); };
    await assert.rejects(run(mode, source, tx => { calls++; void tx.execute("INSERT INTO t VALUES(1,'retained')"); }), /lost commit response/);
    source.afterCommit = null;
    const replay = await run(mode, source, () => { calls++; throw new Error('must not run'); });
    assert.equal(replay.replayed, true); assert.equal(calls, 1);
    await applyChangeset(receiver, await payload(mode, source, replay), { tables: ['t'] });
    assert.deepEqual(receiver.rows(), source.rows());
  } finally { source.afterCommit = null; source.close(); receiver.close(); }
});

for (const encoding of ['UTF-8', 'UTF-16le', 'UTF-16be']) test(`journal: ${encoding} deferred work matches native Session and required-replica retention`, async () => {
  const source = new SqliteTarget(':memory:', `PRAGMA encoding='${encoding}';${schema}`);
  const targets = [new SqliteTarget(':memory:', schema), new SqliteTarget(':memory:', schema)];
  let session;
  try {
    const fanout = await ChangesetFanout.open(source, ['east', 'west']);
    session = source.db.createSession({ table: 't' });
    const result = await run('outbox', source, tx => {
      void tx.execute('INSERT INTO t VALUES(?,?)', [9223372036854775807n, '\uFEFFlarge\0界']);
    });
    const native = session.changeset(); const bytes = await payload('outbox', source, result);
    assert.deepEqual(decodeChangeset(bytes), decodeChangeset(native));
    for (const [i, id] of ['east', 'west'].entries()) {
      const replica = fanout.forReplica(id);
      const delivery = await replica.read(result.delivery.deliveryId);
      await applyChangeset(targets[i], delivery.changeset, { tables: ['t'], deliveryId: result.delivery.deliveryId });
      await replica.acknowledge(result.delivery.deliveryId, result.delivery.sha256);
      const retained = await new ChangesetOutbox(source).read(result.delivery.deliveryId);
      assert.equal(retained.changeset === null, i === 1);
      assert.deepEqual(targets[i].rows(), source.rows());
    }
  } finally { session?.close(); source.close(); targets.forEach(t => t.close()); }
});
