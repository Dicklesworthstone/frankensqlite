// Production SDK + worker host with real SQLite images. Native reference, not WASM.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { createHash } from 'node:crypto';
import { FrankenDB, FrankenSnapshotPool } from '../src/index.ts';
import { sqliteSnapshotWorker } from '../../worker/tests/helpers/snapshot-sqlite-core.mjs';

const opts = { timeout: 15000 };
const turn = () => new Promise(resolve => setImmediate(resolve));
function deferred() { let resolve; const promise = new Promise(r => { resolve = r; }); return { promise, resolve }; }
function observed(promise) { return promise.then(value => ({ value }), error => ({ error })); }
async function image() {
  const f = sqliteSnapshotWorker();
  const db = await FrankenDB.open({ worker: f.worker });
  await db.executeBatch("CREATE TABLE items(id INTEGER PRIMARY KEY,value TEXT);INSERT INTO items VALUES(1,'first'),(2,'second');CREATE TABLE audit(id);CREATE TRIGGER a AFTER INSERT ON items BEGIN INSERT INTO audit VALUES(new.id);END;");
  const bytes = await db.export(); await db.close(); return bytes;
}
async function fixture(t, options = {}) {
  const bytes = await image(), fixtures = [];
  const pool = await FrankenSnapshotPool.open(bytes, { ...options, worker: () => {
    const f = sqliteSnapshotWorker(); fixtures.push(f); return f.worker;
  } });
  t.after(() => pool.close().catch(() => {}));
  return { bytes, pool, fixtures };
}
function block(fixture) {
  const started = deferred(), gate = deferred(), seen = [];
  const core = fixture.handles[0], query = core.query, queryWithParams = core.queryWithParams;
  core.query = async sql => { seen.push(sql); started.resolve(); await gate.promise; return query(sql); };
  core.queryWithParams = async (sql, params) => { seen.push(sql); started.resolve(); await gate.promise; return queryWithParams(sql, params); };
  return { started: started.promise, release: gate.resolve, seen };
}

test('replicas import the same owned image without detaching the caller buffer', opts, async t => {
  const { pool, bytes, fixtures } = await fixture(t, { workers: 3 });
  assert.equal(pool.snapshot.sha256, createHash('sha256').update(bytes).digest('hex'));
  assert.equal(pool.snapshot.byteLength, bytes.length);
  assert.equal(new Set(fixtures.map(f => f.handles[0].path)).size, 3);
  for (const f of fixtures) {
    assert.deepEqual(f.worker.requests.find(r => r.kind === 'init').config.snapshot, bytes);
    assert.deepEqual(f.counts(), { creates: 0, imports: 1 });
  }
  assert.equal((await pool.query('SELECT value FROM items WHERE id=?', [2])).rows[0].value, 'second');
  bytes.fill(0);
  assert.equal((await pool.query('SELECT count(*) AS n FROM items')).rows[0].n, 2);
  assert.ok(Object.isFrozen(pool.snapshot)); assert.ok(Object.isFrozen(pool.stats));
});

test('separate replicas execute overlapping queries; FIFO starts more callers than slots', opts, async t => {
  const { pool, fixtures } = await fixture(t, { workers: 2, maxPendingQueries: 8 });
  const a = block(fixtures[0]), b = block(fixtures[1]);
  const pending = Array.from({ length: 8 }, (_, n) => pool.query('SELECT ? AS n', [n]));
  await Promise.all([a.started, b.started]);
  assert.equal(pool.stats.activeQueries, 2); assert.equal(pool.stats.waitingQueries, 6);
  b.release();
  assert.equal((await pending[1]).rows[0].n, 1);
  // One stalled worker must not hold up the other worker or its successors.
  assert.equal((await pending[7]).rows[0].n, 7);
  assert.equal(pool.stats.activeQueries, 1);
  a.release();
  assert.deepEqual((await Promise.all(pending)).map(result => result.rows[0].n), [0,1,2,3,4,5,6,7]);
  assert.equal(pool.stats.pendingQueries, 0); assert.equal(pool.stats.pendingBytes, 0);
});

test('overload refuses work before transport, then released capacity can be reused', opts, async t => {
  const { pool, fixtures } = await fixture(t, { workers: 1, maxPendingQueries: 2 });
  const gate = block(fixtures[0]);
  const first = pool.query('SELECT 1'), second = pool.query('SELECT 2');
  await gate.started;
  const before = fixtures[0].worker.requests.length;
  await assert.rejects(pool.query('SELECT 3'), { code: 'ERR_FSQLITE_QUEUE_FULL' });
  assert.equal(fixtures[0].worker.requests.length, before);
  gate.release(); await Promise.all([first, second]);
  assert.equal((await pool.query('SELECT 3 AS n')).rows[0].n, 3);
});

test('waiting bindings and blob aliases are captured before application mutation', opts, async t => {
  const { pool, fixtures } = await fixture(t, { workers: 1 });
  const gate = block(fixtures[0]), first = pool.query('SELECT 1');
  await gate.started;
  const backing = Uint8Array.of(0,10,20,30), params = [backing.subarray(1,3), 'original'];
  const second = pool.query('SELECT hex(?) AS b, ? AS t', params);
  backing.fill(255); params[1] = 'changed';
  gate.release(); await first;
  assert.deepEqual((await second).rowArrays, [['0A14', 'original']]);
});

test('read-only admission refuses settings, scripts, maintenance and EXPLAIN pragma escape', opts, async t => {
  const { pool, fixtures } = await fixture(t);
  for (const sql of [
    'PRAGMA query_only=OFF', 'EXPLAIN PRAGMA query_only=OFF',
    'EXPLAIN QUERY PLAN PRAGMA query_only=OFF', 'ATTACH ":memory:" AS another',
    'CREATE TEMP TABLE altered(x)', 'VACUUM', 'BEGIN', 'SELECT 1; DELETE FROM items',
    'SELECT 1; PRAGMA query_only=OFF', '\0SELECT 1',
  ]) {
    const before = fixtures.reduce((n, f) => n + f.worker.requests.length, 0);
    await assert.rejects(pool.query(sql));
    assert.equal(fixtures.reduce((n, f) => n + f.worker.requests.length, 0), before, sql);
  }
  assert.equal((await pool.query('SELECT count(*) AS n FROM items')).rows[0].n, 2);
});

test('WITH DML is rejected by the actual core query-only policy; data and triggers stay unchanged', opts, async t => {
  const { pool, fixtures } = await fixture(t, { workers: 1 });
  for (const sql of [
    "WITH x AS (SELECT 1) INSERT INTO items VALUES(3,'bad') RETURNING id",
    'WITH x AS (SELECT 1) DELETE FROM items RETURNING id',
    "WITH x AS (SELECT 1) UPDATE items SET value='bad' RETURNING id",
  ]) await assert.rejects(pool.query(sql), /readonly/i);
  assert.deepEqual((await pool.query('SELECT * FROM items ORDER BY id')).rowArrays, [[1,'first'],[2,'second']]);
  assert.deepEqual((await pool.query('SELECT * FROM audit')).rows, []);
  await pool.close();
  const reference = new DatabaseSync(fixtures[0].handles[0].path, { readOnly: true });
  try { assert.deepEqual(reference.prepare('PRAGMA integrity_check').all().map(Object.values), [['ok']]); }
  finally { reference.close(); }
});

test('SELECT, recursive CTE, EXPLAIN SELECT, comments and quoted semicolons work', opts, async t => {
  const { pool } = await fixture(t);
  assert.equal((await pool.query("; /*lead*/ SELECT ';PRAGMA query_only=OFF' AS value -- tail")).rows[0].value, ';PRAGMA query_only=OFF');
  assert.deepEqual((await pool.query('WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM x WHERE n<1000) SELECT sum(n) AS n FROM x')).rowArrays, [[500500]]);
  assert.ok((await pool.query('EXPLAIN /*a*/ QUERY /*b*/ PLAN SELECT * FROM items')).rows.length > 0);
});

test('SQL failures settle one job, restore accounting, and do not poison successors', opts, async t => {
  const { pool } = await fixture(t, { workers: 1 });
  const bad = assert.rejects(pool.query('SELECT * FROM nonexistent'));
  const good = pool.query('SELECT 42 AS n');
  await bad; assert.equal((await good).rows[0].n, 42);
  assert.equal(pool.stats.failedQueries, 1); assert.equal(pool.stats.pendingBytes, 0);
});

test('waiting cancellation removes a query without executing it', opts, async t => {
  const { pool, fixtures } = await fixture(t, { workers: 1 });
  const gate = block(fixtures[0]), first = pool.query('SELECT 1'), controller = new AbortController();
  await gate.started;
  const cancelled = assert.rejects(pool.query('SELECT 2', [], { signal: controller.signal }), { code: 'ERR_FSQLITE_POOL_CANCELLED', cause: 'stop' });
  controller.abort('stop'); await cancelled;
  assert.equal(pool.stats.waitingQueries, 0); assert.equal(pool.stats.pendingQueries, 1);
  gate.release(); await first;
  assert.deepEqual(gate.seen, ['SELECT 1']);
});

test('active cancellation retains its slot until SQL settles; unrelated replicas continue', opts, async t => {
  const { pool, fixtures } = await fixture(t, { workers: 2 });
  const gate = block(fixtures[0]), controller = new AbortController();
  const active = observed(pool.query('SELECT 1', [], { signal: controller.signal }));
  await gate.started; controller.abort(); await turn();
  assert.equal(pool.stats.activeQueries, 1); assert.equal(pool.stats.pendingQueries, 1);
  assert.deepEqual((await pool.query('SELECT 2')).rowArrays, [[2]]);
  gate.release(); assert.equal((await active).error.code, 'ERR_FSQLITE_POOL_CANCELLED');
  assert.equal(pool.stats.pendingQueries, 0);
});

test('an active SQL failure is not hidden by simultaneous cancellation', opts, async t => {
  const { pool, fixtures } = await fixture(t, { workers: 1 });
  const gate = block(fixtures[0]), c = new AbortController();
  const pending = observed(pool.query('SELECT * FROM nonexistent', [], { signal: c.signal }));
  await gate.started; c.abort(); gate.release();
  assert.match((await pending).error.message, /no such table/);
});

test('waiting deadline is checked again at dispatch even when its timer cannot run', opts, async t => {
  const { pool, fixtures } = await fixture(t, { workers: 1 });
  const gate = block(fixtures[0]), first = pool.query('SELECT 1'); await gate.started;
  const timed = observed(pool.query('SELECT 2', [], { waitTimeoutMs: 1 }));
  const until = performance.now() + 5; while (performance.now() < until) { /* Deliberately hold task delivery. */ }
  gate.release(); await first;
  assert.equal((await timed).error.code, 'ERR_FSQLITE_POOL_TIMEOUT'); assert.deepEqual(gate.seen, ['SELECT 1']);
});

test('close is idempotent, drains active and accepted queued work, then closes every replica', opts, async t => {
  const { pool, fixtures } = await fixture(t, { workers: 1 });
  const gate = block(fixtures[0]), first = pool.query('SELECT 1'), second = pool.query('SELECT 2');
  await gate.started;
  const close = pool.close(); assert.equal(pool.close(), close);
  await assert.rejects(pool.query('SELECT 3'), { code: 'ERR_FSQLITE_POOL_CLOSED' });
  assert.equal(fixtures[0].worker.terminateCount, 0);
  gate.release(); await Promise.all([first, second, close]);
  assert.equal(fixtures[0].worker.terminateCount, 1); assert.equal(pool.stats.state, 'closed');
  assert.equal(pool.stats.pendingQueries, 0);
});

test('reentrant parameter getters cannot enqueue after close or evade capacity', opts, async t => {
  const { pool } = await fixture(t, { workers: 1 });
  let closed;
  const params = [0]; Object.defineProperty(params, 0, { get() { closed = pool.close(); return 1; } });
  await assert.rejects(pool.query('SELECT ?', params), { code: 'ERR_FSQLITE_POOL_CLOSED' });
  await closed; assert.equal(pool.stats.pendingBytes, 0);
});

test('pre-aborted, invalid and oversized input never reaches a replica', opts, async t => {
  const { pool, fixtures } = await fixture(t, { workers: 1, maxPendingBytes: 512 });
  const before = fixtures[0].worker.requests.length;
  const c = new AbortController(); c.abort('already');
  await assert.rejects(pool.query('SELECT 1', [], { signal: c.signal }), { code: 'ERR_FSQLITE_POOL_CANCELLED' });
  await assert.rejects(pool.query('SELECT ?', ['x'.repeat(1000)]), { code: 'ERR_FSQLITE_REQUEST_TOO_LARGE' });
  await assert.rejects(pool.query('SELECT ?', [new Uint8Array(new ArrayBuffer(1000),0,1)]), { code: 'ERR_FSQLITE_REQUEST_TOO_LARGE' });
  await assert.rejects(pool.query('SELECT ?', [new Uint8Array(new SharedArrayBuffer(1))]), { code: 'ERR_FSQLITE_POOL_INPUT' });
  await assert.rejects(pool.query('SELECT 1', [], { waitTimeoutMs: -1 }), RangeError);
  assert.equal(fixtures[0].worker.requests.length, before); assert.equal(pool.stats.pendingBytes, 0);
});

test('partial initialization failure waits for and closes successfully opened replicas', opts, async () => {
  const bytes = await image(), f = sqliteSnapshotWorker(); let calls = 0;
  await assert.rejects(FrankenSnapshotPool.open(bytes, { workers: 2, worker() {
    if (++calls === 2) throw new Error('factory failed'); return f.worker;
  } }), AggregateError);
  assert.equal(f.worker.terminateCount, 1); assert.deepEqual(f.events.slice(-2), ['close','free']);
});

test('duplicate worker factories are rejected rather than sharing one mutable connection', opts, async () => {
  const bytes = await image(), f = sqliteSnapshotWorker();
  await assert.rejects(FrankenSnapshotPool.open(bytes, { workers: 2, worker: () => f.worker }), AggregateError);
  assert.equal(f.worker.terminateCount, 1);
});

test('invalid images and worker limits reject before invoking a worker factory', opts, async () => {
  const bytes = await image(); let calls = 0;
  const worker = () => { calls++; throw new Error('must not allocate'); };
  for (const workers of [0,9,NaN,1.5]) await assert.rejects(FrankenSnapshotPool.open(bytes,{workers,worker}));
  await assert.rejects(FrankenSnapshotPool.open(new Uint8Array(512),{worker}));
  await assert.rejects(FrankenSnapshotPool.open(bytes,{worker,maxPendingQueries:0}));
  const shared = new Uint8Array(new SharedArrayBuffer(bytes.length)); shared.set(bytes);
  await assert.rejects(FrankenSnapshotPool.open(shared,{worker}));
  assert.equal(calls, 0);
});

test('missing query_only enforcement fails initialization rather than admitting mutable replicas', opts, async () => {
  const bytes = await image(), f = sqliteSnapshotWorker();
  const original = f.worker.postMessage.bind(f.worker);
  f.worker.postMessage = (request, transfer) => original(
    request.kind === 'execute' && request.sql === 'PRAGMA query_only = ON'
      ? { ...request, sql: 'SELECT 1' } : request, transfer);
  const error = (await observed(FrankenSnapshotPool.open(bytes, { workers: 1, worker: () => f.worker }))).error;
  assert.ok(error instanceof AggregateError);
  assert.equal(error.cause.code, 'ERR_FSQLITE_POOL_READ_ONLY');
  assert.equal(f.worker.terminateCount, 1); assert.deepEqual(f.events.slice(-2), ['close','free']);
});
