import assert from 'node:assert/strict';
import { test } from 'node:test';
import { DurableJobQueue } from '../src/durable-jobs.ts';
import { JobSqliteTarget, gate, tick } from './helpers/durable-jobs-sqlite-target.mjs';

const input = { id: 'job-1', payload: '{"operation":"write"}' };
const schema = 'CREATE TABLE effects(id INTEGER PRIMARY KEY, value TEXT);';
const ended = { code: 'ERR_FSQLITE_JOB_SCOPE_ENDED' };
async function setup(mode, encoding = 'UTF-8') {
  const db = new JobSqliteTarget(':memory:', `PRAGMA encoding='${encoding}';` + schema);
  const clock = { now: 100 };
  const q = await DurableJobQueue.open(db, 'jobs', { clock: () => clock.now });
  let lease;
  if (mode === 'complete') { await q.enqueue(input); lease = await q.claim('worker', 1000); }
  return { db, clock, q, lease, run: work => mode === 'complete'
    ? q.completeWith(lease, work, 'done') : q.enqueueWith(input, work) };
}
const state = async (p, mode, failed) => {
  const job = await p.q.get(input.id);
  assert.equal(job?.state ?? null, mode === 'complete' ? (failed ? 'leased' : 'completed') : (failed ? null : 'ready'));
};

for (const mode of ['enqueue', 'complete']) {
  for (const api of ['execute', 'query']) test(`${mode}: drain unawaited ${api} before publishing`, async () => {
    const p = await setup(mode), entered = gate(), release = gate();
    let child, settled = false;
    p.db.beforeSql = async sql => { if (sql.startsWith('INSERT INTO effects')) { entered.resolve(); await release.promise; } };
    const call = p.run(async tx => {
      child = tx[api]("INSERT INTO effects VALUES(1,'recorded')" + (api === 'query' ? ' RETURNING *' : ''));
      return 42;
    });
    void call.then(() => { settled = true; }, () => { settled = true; });
    try {
      await entered.promise; await tick();
      assert.equal(settled, false, 'job publication must wait for admitted SQL');
      release.resolve(); const value = await call; await child;
      assert.equal(mode === 'complete' ? value : value.value, 42);
      assert.deepEqual(p.db.rows(), [{ id: 1, value: 'recorded' }]);
      await state(p, mode, false);
    } finally { release.resolve(); await Promise.allSettled([call, child]); p.db.close(); }
  });
  test(`${mode}: caught statement failure rolls back the complete operation`, async () => {
    const p = await setup(mode);
    try {
      await assert.rejects(p.run(async tx => {
        await tx.execute("INSERT INTO effects VALUES(1,'partial')");
        await tx.execute("INSERT INTO effects VALUES(1,'duplicate')").catch(() => {});
      }), /UNIQUE/);
      assert.deepEqual(p.db.rows(), []); await state(p, mode, true);
    } finally { p.db.close(); }
  });
  test(`${mode}: retained executor cannot enter a later transaction`, async () => {
    const p = await setup(mode); let saved;
    try {
      await p.run(async tx => { saved = tx; });
      const n = p.db.statements.length;
      await p.db.transaction(async tx => {
        await assert.rejects(saved.execute("INSERT INTO effects VALUES(1,'escaped')"), ended);
        await assert.rejects(saved.query('SELECT 1'), ended);
        assert.equal(p.db.statements.length, n);
        await tx.execute("INSERT INTO effects VALUES(2,'next')");
      });
      assert.deepEqual(p.db.rows(), [{ id: 2, value: 'next' }]);
    } finally { p.db.close(); }
  });
  test(`${mode}: callback failure waits for admitted writes before rollback`, async () => {
    const p = await setup(mode), entered = gate(), release = gate(), marker = new Error('callback');
    let child, settled = false;
    p.db.beforeSql = async sql => { if (sql.startsWith('INSERT INTO effects')) { entered.resolve(); await release.promise; } };
    const call = p.run(async tx => { child = tx.execute("INSERT INTO effects VALUES(1,'rollback')"); throw marker; });
    void call.then(() => { settled = true; }, () => { settled = true; });
    try {
      await entered.promise; await tick(); assert.equal(settled, false);
      release.resolve(); await assert.rejects(call, e => e === marker); await child;
      assert.deepEqual(p.db.rows(), []); await state(p, mode, true);
    } finally { release.resolve(); await Promise.allSettled([call, child]); p.db.close(); }
  });
  for (const cause of [undefined, null, false]) test(`${mode}: swallowed ${String(cause)} rejection still aborts`, async () => {
    const p = await setup(mode);
    p.db.beforeSql = sql => { if (sql === 'SELECT failing') return Promise.reject(cause); };
    try {
      let rejected = false;
      try { await p.run(async tx => { await tx.execute("INSERT INTO effects VALUES(1,'partial')"); await tx.query('SELECT failing').catch(() => {}); }); }
      catch (e) { rejected = true; assert.equal(e, cause); }
      assert.equal(rejected, true); assert.deepEqual(p.db.rows(), []); await state(p, mode, true);
    } finally { p.db.close(); }
  });
  test(`${mode}: callback error takes precedence over an admitted SQL error`, async () => {
    const p = await setup(mode), callback = new Error('callback wins');
    try {
      await assert.rejects(p.run(async tx => {
        void tx.execute('INSERT INTO absent VALUES(1)'); throw callback;
      }), e => e === callback);
      await state(p, mode, true);
    } finally { p.db.close(); }
  });
  for (const encoding of ['UTF-8','UTF-16le','UTF-16be']) test(`${mode}: real ${encoding} callback data and frozen scope`, async () => {
    const p = await setup(mode, encoding), value = { original: true };
    try {
      const result = await p.run(async tx => {
        assert.equal(Object.isFrozen(tx), true);
        assert.deepEqual(Object.keys(tx).sort(), ['execute','query']);
        await tx.execute('INSERT INTO effects VALUES(?,?)', [1, '日本語😀']);
        return value;
      });
      assert.equal(mode === 'complete' ? result : result.value, value);
      assert.deepEqual(p.db.rows(), [{ id: 1, value: '日本語😀' }]); await state(p, mode, false);
    } finally { p.db.close(); }
  });
  test(`${mode}: deferred COMMIT failure restores job and application rows`, async () => {
    const p = await setup(mode);
    p.db.db.exec('CREATE TABLE parent(id PRIMARY KEY); CREATE TABLE dependent(id PRIMARY KEY,p REFERENCES parent DEFERRABLE INITIALLY DEFERRED);');
    try {
      await assert.rejects(p.run(async tx => {
        await tx.execute("INSERT INTO effects VALUES(1,'rollback')");
        await tx.execute('INSERT INTO dependent VALUES(1,99)');
      }), /FOREIGN KEY/);
      assert.deepEqual(p.db.rows(), []); await state(p, mode, true);
    } finally { p.db.close(); }
  });
}

test('completion lease expiry is rechecked after unawaited SQL drains', async () => {
  const p = await setup('complete'), entered = gate(), release = gate();
  let child;
  p.db.beforeSql = async sql => { if (sql.startsWith('INSERT INTO effects')) { entered.resolve(); await release.promise; } };
  const call = p.run(async tx => { child = tx.execute("INSERT INTO effects VALUES(1,'expired')"); });
  void call.catch(() => {});
  try {
    await entered.promise; await tick(); p.clock.now = 1100; release.resolve();
    await assert.rejects(call, { code: 'ERR_FSQLITE_JOB_LEASE_LOST' }); await child;
    assert.deepEqual(p.db.rows(), []); await state(p, 'complete', true);
    const successor = await p.q.claim('next-worker',1000); assert.equal(successor.attempt,2);
    await assert.rejects(p.q.completeWith(p.lease,async () => { throw new Error('must not enter'); }), {code:'ERR_FSQLITE_JOB_LEASE_LOST'});
  } finally { release.resolve(); await Promise.allSettled([call,child]); p.db.close(); }
});
test('completion handle closes before the final ownership check starts', async () => {
  const p = await setup('complete'), entered = gate(), release = gate(); let saved;
  p.db.beforeSql = async sql => { if (sql.includes("state = 'completed'")) { entered.resolve(); await release.promise; } };
  const call = p.run(async tx => { saved = tx; });
  try {
    await entered.promise;
    await assert.rejects(saved.execute("INSERT INTO effects VALUES(1,'late')"), ended);
    release.resolve(); await call; assert.deepEqual(p.db.rows(),[]);
  } finally {release.resolve(); await Promise.allSettled([call]); p.db.close();}
});
test('duplicate enqueue never enters work, including after lost COMMIT acknowledgement',async()=>{
  const p=await setup('enqueue'); let calls=0;
  try {
    p.db.afterCommit=()=>{throw new Error('lost response');};
    await assert.rejects(p.run(async tx=>{calls++;await tx.execute("INSERT INTO effects VALUES(1,'once')");}),/lost response/);
    p.db.afterCommit=null;
    const retry=await p.run(async()=>{calls++;throw new Error('duplicate work');});
    assert.equal(retry.inserted,false);assert.equal(calls,1);assert.deepEqual(p.db.rows(),[{id:1,value:'once'}]);
  }finally{p.db.close();}
});
test('lost completion acknowledgement cannot rerun application work',async()=>{
  const p=await setup('complete');let calls=0;
  try{
    p.db.afterCommit=()=>{throw new Error('lost response');};
    await assert.rejects(p.run(async tx=>{calls++;await tx.execute("INSERT INTO effects VALUES(1,'once')");}),/lost response/);
    p.db.afterCommit=null;
    await assert.rejects(p.run(async()=>{calls++;}),{code:'ERR_FSQLITE_JOB_LEASE_LOST'});
    assert.equal(calls,1);await state(p,'complete',false);
  }finally{p.db.close();}
});
