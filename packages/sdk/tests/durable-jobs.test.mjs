// SQL-reference tests: execute the production job queue against Node's SQLite.
// This is not a claim of FrankenSQLite engine or browser crash certification.
// node --experimental-loader=./packages/sdk/tests/helpers/source-loader.mjs \
//   --test packages/sdk/tests/durable-jobs.test.mjs
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { DurableJobQueue, DurableJobError, DURABLE_JOBS_TABLE } from '../src/durable-jobs.ts';

class SqlDatabase {
  tail = Promise.resolve();
  calls = 0;
  postCommitFailure = null;
  beforeStart = null;
  constructor(path = ':memory:') { this.sql = new DatabaseSync(path); }
  transaction(work) {
    this.calls++;
    const result = this.tail.then(async () => {
      if (this.beforeStart) await this.beforeStart();
      this.sql.exec('BEGIN IMMEDIATE');
      let committed = false;
      try {
        const value = await work({
          execute: async (sql, params = []) => Number(this.sql.prepare(sql).run(...params).changes),
          query: async (sql, params = []) => ({ rows: this.sql.prepare(sql).all(...params) }),
        });
        this.sql.exec('COMMIT');
        committed = true;
        if (this.postCommitFailure) {
          const error = this.postCommitFailure;
          this.postCommitFailure = null;
          throw error;
        }
        return value;
      } catch (error) {
        if (!committed) this.sql.exec('ROLLBACK');
        throw error;
      }
    });
    this.tail = result.catch(() => {});
    return result;
  }
  close() { this.sql.close(); }
}

async function fixture(t, name = 'work') {
  const db = new SqlDatabase();
  t.after(() => db.close());
  let now = 1000;
  const clock = () => now;
  const queue = await DurableJobQueue.open(db, name, { clock });
  return { db, queue, clock, time: value => { now = value; } };
}
const lost = error => error instanceof DurableJobError && error.code === 'ERR_FSQLITE_JOB_LEASE_LOST';
const job = (id, extra = {}) => ({ id, payload: `payload:${id}`, ...extra });

test('SQL schema installation is repeatable and jobs start ready', async t => {
  const { db, queue, clock } = await fixture(t);
  await DurableJobQueue.open(db, 'work', { clock });
  const result = await queue.enqueue(job('one'));
  assert.equal(result.inserted, true);
  assert.equal(result.job.state, 'ready');
  assert.equal(result.job.attempts, 0);
  assert.equal(result.job.maxAttempts, 3);
  assert.equal(result.job.leaseExpiresAt, null);
  assert.equal(Object.isFrozen(result.job), true);
  assert.equal((await queue.get('one')).payload, 'payload:one');
  assert.equal(await queue.get('missing'), null);
});

test('stable job ids deduplicate retries without resetting delivery state', async t => {
  const { queue, time } = await fixture(t);
  await queue.enqueue(job('one', { availableAt: 1000 }));
  const lease = await queue.claim('worker');
  await queue.fail(lease, 'retry', 500);
  time(1100);
  const duplicate = await queue.enqueue(job('one', { availableAt: 1000 }));
  assert.equal(duplicate.inserted, false);
  assert.equal(duplicate.job.attempts, 1);
  assert.equal(duplicate.job.availableAt, 1500);
  await assert.rejects(queue.enqueue(job('one', { payload: 'different' })), { code: 'ERR_FSQLITE_JOB_ID_CONFLICT' });
  await assert.rejects(queue.enqueue(job('one', { priority: 4 })), { code: 'ERR_FSQLITE_JOB_ID_CONFLICT' });
  await assert.rejects(queue.enqueue(job('one', { maxAttempts: 7 })), { code: 'ERR_FSQLITE_JOB_ID_CONFLICT' });
  await assert.rejects(queue.enqueue(job('one', { availableAt: 2000 })), { code: 'ERR_FSQLITE_JOB_ID_CONFLICT' });
});

test('claim respects schedule, priority, deterministic ties, and queue isolation', async t => {
  const { db, queue, clock, time } = await fixture(t);
  const other = await DurableJobQueue.open(db, 'other', { clock });
  await other.enqueue(job('foreign', { priority: 100 }));
  await queue.enqueue(job('later', { priority: 100, availableAt: 2000 }));
  await queue.enqueue(job('low', { priority: -1 }));
  await queue.enqueue(job('b', { priority: 10 }));
  await queue.enqueue(job('a', { priority: 10 }));
  for (const id of ['a', 'b', 'low']) {
    const lease = await queue.claim('worker');
    assert.equal(lease.id, id);
    await queue.complete(lease);
  }
  assert.equal(await queue.claim('worker'), null);
  time(2000);
  assert.equal((await queue.claim('worker')).id, 'later');
  assert.equal((await other.claim('worker')).id, 'foreign');
});

test('claims return immutable ownership receipts and consume an attempt', async t => {
  const { queue } = await fixture(t);
  await queue.enqueue(job('one'));
  const lease = await queue.claim('worker', 100);
  assert.equal(lease.attempt, 1);
  assert.equal(lease.expiresAt, 1100);
  assert.equal(lease.payload, 'payload:one');
  assert.equal(Object.isFrozen(lease), true);
  assert.match(lease.token, /^[0-9a-f-]{36}$/);
  assert.equal(await queue.claim('another'), null);
});

test('expired leases are reclaimed with new tokens even for the same worker id', async t => {
  const { queue, time } = await fixture(t);
  await queue.enqueue(job('one'));
  const first = await queue.claim('worker', 100);
  time(1100);
  const second = await queue.claim('worker', 100);
  assert.equal(second.attempt, 2);
  assert.notEqual(first.token, second.token);
  await assert.rejects(queue.complete(first), lost);
  await assert.rejects(queue.fail(first, 'late'), lost);
  await assert.rejects(queue.renew(first), lost);
  await queue.complete(second, 'done');
  assert.equal((await queue.get('one')).result, 'done');
});

test('expiry is enforced without a competing claimant, including the exact boundary', async t => {
  const { queue, time } = await fixture(t);
  await queue.enqueue(job('one'));
  const lease = await queue.claim('worker', 10);
  time(1010);
  await assert.rejects(queue.complete(lease), lost);
  await assert.rejects(queue.fail(lease, 'late'), lost);
  await assert.rejects(queue.renew(lease), lost);
});

test('renew extends the persisted lease but cannot shorten it', async t => {
  const { queue, time } = await fixture(t);
  await queue.enqueue(job('one'));
  const lease = await queue.claim('worker', 100);
  time(1050);
  const shorter = await queue.renew(lease, 10);
  assert.equal(shorter.expiresAt, 1100);
  const renewed = await queue.renew(lease, 100);
  assert.equal(renewed.expiresAt, 1150);
  assert.equal(renewed.token, lease.token);
  time(1120);
  // An old receipt's displayed deadline is not authoritative after renewal.
  await queue.complete(lease);
  assert.equal((await queue.get('one')).state, 'completed');
});

test('owner, token, attempt, id, and queue each fence mutations', async t => {
  const { queue } = await fixture(t);
  await queue.enqueue(job('one'));
  const lease = await queue.claim('worker');
  for (const changed of [{ owner: 'other' }, { token: crypto.randomUUID() }, { attempt: 2 }, { id: 'missing' }]) {
    await assert.rejects(queue.complete({ ...lease, ...changed }), lost);
  }
  await assert.rejects(queue.complete({ ...lease, queue: 'other' }), TypeError);
  await queue.complete(lease);
});

test('retry delay and max attempts lead to a durable dead letter', async t => {
  const { queue, time } = await fixture(t);
  await queue.enqueue(job('one', { maxAttempts: 2 }));
  await queue.fail(await queue.claim('worker'), 'first failure', 50);
  assert.equal(await queue.claim('worker'), null);
  time(1050);
  const lease = await queue.claim('worker');
  assert.equal(lease.attempt, 2);
  await queue.fail(lease, 'last failure');
  const saved = await queue.get('one');
  assert.equal(saved.state, 'dead');
  assert.equal(saved.lastError, 'last failure');
  assert.equal(saved.owner, null);
  assert.equal(await queue.claim('worker'), null);
});

test('bounded recovery dead-letters final crashed claims and requeues other expirations', async t => {
  const { queue, time } = await fixture(t);
  await queue.enqueue(job('a', { maxAttempts: 1 }));
  await queue.enqueue(job('b', { maxAttempts: 2 }));
  const a = await queue.claim('worker', 10);
  const b = await queue.claim('worker', 10);
  assert.equal(await queue.reapExpired(1), 0);
  time(1010);
  assert.equal(await queue.reapExpired(1), 1);
  assert.equal((await queue.get('a')).state, 'dead');
  assert.equal((await queue.get('b')).state, 'leased');
  assert.equal(await queue.reapExpired(1), 1);
  assert.equal((await queue.get('b')).state, 'ready');
  await assert.rejects(queue.complete(a), lost);
  await assert.rejects(queue.complete(b), lost);
  assert.equal((await queue.claim('worker')).attempt, 2);
});

test('cancellation fences running handlers and preserves terminal jobs', async t => {
  const { queue } = await fixture(t);
  await queue.enqueue(job('one'));
  const lease = await queue.claim('worker');
  assert.equal(await queue.cancel('one'), true);
  assert.equal(await queue.cancel('one'), false);
  await assert.rejects(queue.complete(lease), lost);
  assert.equal(await queue.claim('worker'), null);
  assert.equal(await queue.cancel('missing'), false);
  await queue.enqueue(job('done'));
  await queue.complete(await queue.claim('worker'));
  assert.equal(await queue.cancel('done'), false);
});

test('a reused job id cannot accept an earlier receipt after SQL deletion/recreation', async t => {
  const { db, queue } = await fixture(t);
  await queue.enqueue(job('one'));
  const old = await queue.claim('worker');
  db.sql.prepare(`DELETE FROM ${DURABLE_JOBS_TABLE} WHERE queue_name=? AND job_id=?`).run('work', 'one');
  await queue.enqueue(job('one'));
  const fresh = await queue.claim('worker');
  assert.equal(fresh.attempt, old.attempt);
  assert.notEqual(fresh.token, old.token);
  await assert.rejects(queue.complete(old), lost);
  await queue.complete(fresh);
});

test('enqueue copies caller properties before transaction admission', async t => {
  const { db, queue } = await fixture(t);
  let release;
  db.beforeStart = () => new Promise(resolve => { release = resolve; });
  const input = job('one');
  const pending = queue.enqueue(input);
  await Promise.resolve();
  input.id = 'changed'; input.payload = 'changed';
  db.beforeStart = null;
  release();
  await pending;
  assert.equal((await queue.get('one')).payload, 'payload:one');
  assert.equal(await queue.get('changed'), null);
});

test('lease clock is sampled when the transaction starts, not before queue wait', async t => {
  const { db, queue, time } = await fixture(t);
  await queue.enqueue(job('one'));
  let release;
  db.beforeStart = () => new Promise(resolve => { release = resolve; });
  const pending = queue.claim('worker', 100);
  await Promise.resolve();
  time(5000);
  db.beforeStart = null;
  release();
  assert.equal((await pending).expiresAt, 5100);
});

test('invalid inputs fail before starting transactions or touching SQL', async t => {
  const { db, queue } = await fixture(t);
  const count = db.calls;
  for (const input of [job(''), job('x', { priority: 1.5 }), job('x', { maxAttempts: 0 }),
    job('x', { availableAt: -1 }), job('x', { payload: '\u{1f600}'.repeat(262145) })]) {
    await assert.rejects(queue.enqueue(input));
  }
  await assert.rejects(queue.claim('', 10));
  await assert.rejects(queue.claim('worker', 0));
  await assert.rejects(queue.claim('worker', Infinity));
  await assert.rejects(queue.reapExpired(1001));
  assert.equal(db.calls, count);
});

test('invalid clocks and overflowing deadlines roll back without consuming attempts', async t => {
  const { queue, time } = await fixture(t);
  await queue.enqueue(job('one'));
  time(Number.MAX_SAFE_INTEGER);
  await assert.rejects(queue.claim('worker', 1), RangeError);
  time(NaN);
  await assert.rejects(queue.claim('worker'), RangeError);
  time(1000);
  assert.equal((await queue.get('one')).attempts, 0);
});

test('SQL injection text is bound as data, including queue names and errors', async t => {
  const { queue } = await fixture(t, "x'; DROP TABLE users; --");
  const id = "id'); DROP TABLE x; --";
  await queue.enqueue(job(id, { payload: JSON.stringify({ text: "data'\0; -- 🚀" }) }));
  const lease = await queue.claim("worker' --");
  assert.equal(lease.id, id);
  await queue.fail(lease, "error' --");
  assert.equal((await queue.get(id)).lastError, "error' --");
});

test('100 concurrent callers of one transaction owner get distinct jobs', async t => {
  const { queue } = await fixture(t);
  for (let i = 0; i < 100; i++) await queue.enqueue(job(String(i)));
  const leases = await Promise.all(Array.from({ length: 100 }, (_, i) => queue.claim(`worker-${i}`)));
  assert.equal(new Set(leases.map(lease => lease.id)).size, 100);
  assert.equal(new Set(leases.map(lease => lease.token)).size, 100);
  assert.equal(await queue.claim('empty'), null);
});

test('post-commit publication errors propagate unchanged and never replay a claim', async t => {
  const { db, queue } = await fixture(t);
  await queue.enqueue(job('one'));
  const error = new Error('checkpoint receipt lost');
  error.sqlCommitted = true;
  db.postCommitFailure = error;
  const calls = db.calls;
  await assert.rejects(queue.claim('worker'), actual => actual === error);
  assert.equal(db.calls, calls + 1);
  const saved = await queue.get('one');
  assert.equal(saved.state, 'leased');
  assert.equal(saved.attempts, 1);
  assert.equal(await queue.claim('other'), null);
});

test('jobs, results, and outstanding leases survive a real file close/reopen', async t => {
  const directory = mkdtempSync(join(tmpdir(), 'fsqlite-durable-jobs-'));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const path = join(directory, 'jobs.sqlite');
  let db = new SqlDatabase(path);
  t.after(() => db.close());
  let now = 1000;
  const clock = () => now;
  let queue = await DurableJobQueue.open(db, 'work', { clock });
  await queue.enqueue(job('done', { priority: 10 }));
  await queue.complete(await queue.claim('worker'), 'stored-result');
  await queue.enqueue(job('pending'));
  const stale = await queue.claim('crashed-worker', 10);
  db.close();
  db = new SqlDatabase(path);
  queue = await DurableJobQueue.open(db, 'work', { clock });
  assert.equal((await queue.get('done')).result, 'stored-result');
  assert.equal(await queue.claim('restart'), null);
  now = 1010;
  const recovered = await queue.claim('restart');
  assert.equal(recovered.id, 'pending');
  assert.equal(recovered.attempt, 2);
  await assert.rejects(queue.complete(stale), lost);
  await queue.complete(recovered);
  assert.deepEqual(db.sql.prepare('PRAGMA integrity_check').all().map(row => row.integrity_check), ['ok']);
});
