import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtempSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { DurableJobQueue, DURABLE_JOBS_TABLE } from '../src/durable-jobs.ts';
import { DurableJobWorker } from '../src/durable-job-worker.ts';
import { JobSqliteTarget } from './helpers/durable-jobs-sqlite-target.mjs';

const name = DURABLE_JOBS_TABLE;
const main = `main."${name}"`;
const temporary = `temp."${name}"`;
const ddl = 'CREATE TABLE effects(id INTEGER PRIMARY KEY,value TEXT);';
const clock = () => 100;
const file = () => join(mkdtempSync(join(tmpdir(), 'job-storage-')), 'queue.db');
const job = { id: 'parent', payload: 'real work' };
const next = [{ queue: 'children', id: 'child', payload: 'follow-up' }];
function shadowSql(db, namespace) {
  // Real schemas with the SAME constraints, not mock rows or an SQL interpreter.
  const sql = db.rows('SELECT sql FROM main.sqlite_schema WHERE name=?', [name])[0].sql;
  return `CREATE TABLE ${namespace}."${name}" ${sql.slice(sql.indexOf('('))}`;
}
function shadow(db, namespace = 'temp', copy = false) {
  db.db.exec(shadowSql(db, namespace));
  if (copy) db.db.exec(`INSERT INTO ${namespace}."${name}" SELECT * FROM ${main}`);
}
function rows(db, namespace = 'main') {
  return db.rows(`SELECT * FROM ${namespace}."${name}" ORDER BY queue_name,job_id`);
}

for (const journal of ['WAL', 'DELETE']) {
  for (const encoding of ['UTF-8', 'UTF-16le', 'UTF-16be']) {
    test(`${journal}/${encoding}: acknowledged enqueue survives TEMP shadow and file reopen`, async () => {
      const path = file();
      const db = new JobSqliteTarget(path, `PRAGMA encoding='${encoding}'; PRAGMA journal_mode=${journal};`);
      try {
        const queue = await DurableJobQueue.open(db, 'jobs', { clock });
        shadow(db);
        const result = await queue.enqueue({ id: 'persisted', payload: '日本語😀' });
        assert.equal(result.inserted, true);
        assert.equal(rows(db).length, 1, 'enqueue must write main, not connection-local TEMP');
        assert.deepEqual(rows(db, 'temp'), []);
      } finally { db.close(); }
      const reopened = new JobSqliteTarget(path);
      try {
        const queue = await DurableJobQueue.open(reopened, 'jobs', { clock });
        assert.equal((await queue.get('persisted')).payload, '日本語😀');
        assert.equal((await queue.claim('new-owner')).id, 'persisted');
      } finally { reopened.close(); }
    });
  }
}

test('open creates main table/indexes despite same-named incompatible TEMP objects', async () => {
  const db = new JobSqliteTarget();
  try {
    db.db.exec(`CREATE TEMP TABLE "${name}"(precious); INSERT INTO ${temporary} VALUES(77);
      CREATE INDEX temp.__fsqlite_jobs_ready_v1 ON "${name}"(precious);
      CREATE INDEX temp.__fsqlite_jobs_expiry_v1 ON "${name}"(precious);`);
    const before = db.rows('SELECT name,sql FROM temp.sqlite_schema ORDER BY name');
    const queue = await DurableJobQueue.open(db, 'jobs', { clock });
    await queue.enqueue(job);
    assert.equal(rows(db).length, 1);
    const indexes = db.rows(`PRAGMA main.index_list('${name}')`).map(r => r.name);
    assert(indexes.includes('__fsqlite_jobs_ready_v1'));
    assert(indexes.includes('__fsqlite_jobs_expiry_v1'));
    assert.deepEqual(db.rows('SELECT name,sql FROM temp.sqlite_schema ORDER BY name'), before);
    assert.deepEqual(db.rows(`SELECT * FROM ${temporary}`), [{ precious: 77 }]);
  } finally { db.close(); }
});

test('TEMP view cannot redirect reads, counts or job publication', async () => {
  const db = new JobSqliteTarget();
  try {
    const queue = await DurableJobQueue.open(db, 'jobs', { clock });
    db.db.exec(`CREATE TEMP VIEW "${name}" AS SELECT 'untouched' AS precious`);
    await queue.enqueue(job);
    assert.equal((await queue.get(job.id)).payload, job.payload);
    assert.equal((await queue.stats()).available, 1);
    assert.equal((await queue.claim('worker')).id, job.id);
    assert.deepEqual(db.rows(`SELECT * FROM ${temporary}`), [{ precious: 'untouched' }]);
  } finally { db.close(); }
});

test('claim, renew, fail, complete, cancel and reaping mutate only main', async () => {
  const db = new JobSqliteTarget();
  let now = 100;
  try {
    const queue = await DurableJobQueue.open(db, 'jobs', { clock: () => now });
    for (const [id, priority] of [['a', 4], ['b', 3], ['c', 2], ['d', 1]])
      await queue.enqueue({ id, payload: id, priority });
    shadow(db, 'temp', true);
    const before = rows(db, 'temp');
    const [a, b] = await queue.claimBatch('worker', { limit: 2, leaseMs: 100 });
    assert.deepEqual([a.id, b.id], ['a', 'b']);
    assert.equal((await queue.renew(a, 200)).expiresAt, 300);
    await queue.fail(a, 'try later', 50);
    await queue.complete(b, 'done');
    assert.equal(await queue.cancel('c'), true);
    assert.equal((await queue.claim('worker', 100)).id, 'd');
    now = 1000;
    assert.equal(await queue.reapExpired(), 1);
    const summary = await queue.stats();
    assert.equal(summary.ready, 2);
    assert.equal(summary.completed, 1);
    assert.equal(summary.cancelled, 1);
    assert.equal(summary.leased, 0);
    assert.equal(summary.available, 2);
    assert.equal(rows(db).find(r => r.job_id === 'b').result, 'done');
    assert.deepEqual(rows(db, 'temp'), before);
  } finally { db.close(); }
});

for (const method of ['renew', 'fail', 'complete', 'completeWith', 'completeAndEnqueue']) {
  test(`a TEMP copy of a cancelled lease cannot authorize ${method}`, async () => {
    const path = file();
    const db = new JobSqliteTarget(path, 'PRAGMA journal_mode=WAL;' + ddl);
    const peer = new JobSqliteTarget(path);
    try {
      const queue = await DurableJobQueue.open(db, 'jobs', { clock });
      await queue.enqueue(job);
      const lease = await queue.claim('worker');
      shadow(db, 'temp', true);
      const before = rows(db, 'temp');
      const other = await DurableJobQueue.open(peer, 'jobs', { clock });
      await other.cancel(job.id);
      let called = 0;
      const work = async tx => { called++; await tx.execute("INSERT INTO effects VALUES(1,'forbidden')"); };
      const call = method === 'completeAndEnqueue' ? () => queue.completeAndEnqueue(lease, next, work)
        : method === 'completeWith' ? () => queue.completeWith(lease, work)
        : method === 'fail' ? () => queue.fail(lease, 'forbidden') : () => queue[method](lease);
      await assert.rejects(call(), { code: 'ERR_FSQLITE_JOB_LEASE_LOST' });
      assert.equal(called, 0);
      assert.deepEqual(db.rows(), []);
      assert.equal(rows(db)[0].state, 'cancelled');
      assert.equal(rows(db).length, 1);
      assert.deepEqual(rows(db, 'temp'), before);
    } finally { peer.close(); db.close(); }
  });
}

test('a same-named TEMP table created during callback cannot split continuations from parent', async () => {
  const db = new JobSqliteTarget(':memory:', ddl);
  try {
    const queue = await DurableJobQueue.open(db, 'jobs', { clock });
    await queue.enqueue(job);
    const lease = await queue.claim('worker');
    const tempDdl = shadowSql(db, 'temp');
    const result = await queue.completeAndEnqueue(lease, next, async tx => {
      await tx.execute(tempDdl);
      await tx.execute(`INSERT INTO ${temporary} SELECT * FROM ${main}`);
      await tx.execute("INSERT INTO effects VALUES(1,'main effects')");
    });
    assert.equal(result.jobs[0].inserted, true);
    assert.equal(rows(db).find(r => r.job_id === job.id).state, 'completed');
    assert.equal(rows(db).find(r => r.job_id === 'child').state, 'ready');
    assert.equal(rows(db, 'temp').length, 1);
    assert.equal(rows(db, 'temp')[0].state, 'leased');
  } finally { db.close(); }
});

for (const method of ['get', 'enqueue', 'claim', 'stats']) {
  test(`missing main storage rejects ${method} instead of using an attached copy`, async () => {
    const db = new JobSqliteTarget();
    try {
      const queue = await DurableJobQueue.open(db, 'jobs', { clock });
      await queue.enqueue(job);
      db.db.exec("ATTACH ':memory:' AS other");
      shadow(db, 'other', true);
      const before = rows(db, 'other');
      // Preserve the old state; no file or table is deleted for this fixture.
      db.db.exec(`ALTER TABLE ${main} RENAME TO saved_jobs`);
      const call = method === 'get' ? () => queue.get(job.id)
        : method === 'enqueue' ? () => queue.enqueue({ id: 'different', payload: 'x' })
        : method === 'claim' ? () => queue.claim('worker') : () => queue.stats();
      await assert.rejects(call(), /no such table/);
      assert.deepEqual(rows(db, 'other'), before);
      assert.equal(db.rows('SELECT * FROM main.saved_jobs').length, 1);
    } finally { db.close(); }
  });
}

test('existing attached queue stays independent when main queue is initialized', async () => {
  const db = new JobSqliteTarget();
  const template = new JobSqliteTarget();
  try {
    await DurableJobQueue.open(template, 'template');
    db.db.exec("ATTACH ':memory:' AS other");
    db.db.exec(shadowSql(template, 'other'));
    const queue = await DurableJobQueue.open(db, 'jobs', { clock });
    await queue.enqueue(job);
    assert.equal(rows(db).length, 1);
    assert.deepEqual(rows(db, 'other'), []);
  } finally { template.close(); db.close(); }
});

for (const journal of ['WAL', 'DELETE']) {
  test(`${journal}: lost continuation reply recovers main state after TEMP disappears`, async () => {
    const path = file();
    const db = new JobSqliteTarget(path, `PRAGMA journal_mode=${journal};` + ddl);
    let lease;
    try {
      const queue = await DurableJobQueue.open(db, 'jobs', { clock });
      await queue.enqueue(job);
      lease = await queue.claim('worker');
      shadow(db, 'temp', true);
      db.afterCommit = () => { throw new Error('lost acknowledgement'); };
      await assert.rejects(queue.completeAndEnqueue(lease, next,
        tx => tx.execute("INSERT INTO effects VALUES(1,'once')")), /lost acknowledgement/);
    } finally { db.close(); }
    const recovered = new JobSqliteTarget(path);
    try {
      const queue = await DurableJobQueue.open(recovered, 'jobs', { clock });
      assert.equal((await queue.get(job.id)).state, 'completed');
      assert.equal(rows(recovered).length, 2);
      assert.deepEqual(recovered.rows(), [{ id: 1, value: 'once' }]);
      let called = 0;
      await assert.rejects(queue.completeAndEnqueue(lease, next, async () => { called++; }),
        { code: 'ERR_FSQLITE_JOB_LEASE_LOST' });
      assert.equal(called, 0);
    } finally { recovered.close(); }
  });
}

test('production worker consumes main continuations without touching a TEMP queue copy', async () => {
  const db = new JobSqliteTarget(':memory:', ddl);
  let worker;
  try {
    const queue = await DurableJobQueue.open(db, 'jobs', { clock });
    await queue.enqueue(job);
    shadow(db, 'temp', true);
    const before = rows(db, 'temp');
    worker = DurableJobWorker.start(queue, lease => ({
      ...(lease.id === 'parent' ? { next: [{ queue: 'jobs', id: 'child', payload: 'follow-up' }] } : {}),
      apply: tx => tx.execute('INSERT INTO effects VALUES(?,?)', [lease.id === 'parent' ? 1 : 2, lease.id]),
    }), { owner: 'worker', clock, stopWhenIdle: true });
    await worker.done;
    assert.equal(worker.stats.completed, 2);
    assert(rows(db).every(r => r.state === 'completed'));
    assert.equal(rows(db).length, 2);
    assert.deepEqual(rows(db, 'temp'), before);
    assert.equal(db.rows().length, 2);
  } finally { await worker?.stop().catch(() => {}); db.close(); }
});
