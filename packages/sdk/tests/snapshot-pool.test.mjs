// Production SDK + worker host with real SQLite images. Native reference, not WASM.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { createHash } from 'node:crypto';
import { mkdtemp, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { FrankenDB, FrankenSnapshotPool } from '../src/index.ts';
import { sqliteSnapshotWorker } from '../../worker/tests/helpers/snapshot-sqlite-core.mjs';
import { sqliteBindingFixture } from '../../worker/tests/helpers/bindings-core.mjs';
import { WorkerConnectionHost } from '../../worker/src/connection.ts';

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
    const f = sqliteSnapshotWorker(options.hooks?.(fixtures.length) ?? {});
    fixtures.push(f); options.configure?.(f, fixtures.length - 1); return f.worker;
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

async function changedImage() {
  const f = sqliteSnapshotWorker();
  const db = await FrankenDB.open({ worker: f.worker });
  await db.executeBatch("CREATE TABLE items(id INTEGER PRIMARY KEY,value TEXT);INSERT INTO items VALUES(10,'new'),(20,'generation'),(30,'three');");
  const bytes = await db.export(); await db.close(); return bytes;
}

test('refresh waits for every old read and atomically publishes all replacement replicas', opts, async t => {
  const { pool, fixtures } = await fixture(t, { workers: 2 });
  const replacement = await changedImage(), initial = pool.snapshot;
  const a = block(fixtures[0]), b = block(fixtures[1]);
  const old = [pool.query('SELECT count(*) AS n FROM items'), pool.query('SELECT count(*) AS n FROM items')];
  await Promise.all([a.started,b.started]);
  const refresh = pool.refresh(replacement);
  const later = Array.from({length:6}, () => pool.query('SELECT count(*) AS n FROM items'));
  await turn(); assert.equal(fixtures.length,2); assert.equal(pool.stats.refreshing,true);
  b.release(); await old[1]; assert.equal(fixtures.length,2); assert.equal(pool.snapshot,initial);
  a.release();
  const refreshed = await refresh;
  assert.equal(refreshed.snapshot.generation,2); assert.equal(refreshed.cleanupErrors.length,0);
  assert.equal(refreshed.snapshot.sha256,createHash('sha256').update(replacement).digest('hex'));
  for(const result of await Promise.all(old)) { assert.equal(result.rows[0].n,2); assert.equal(result.snapshot,initial); }
  for(const result of await Promise.all(later)) { assert.equal(result.rows[0].n,3); assert.equal(result.snapshot,refreshed.snapshot); }
  assert.equal(fixtures.length,4); assert.ok(fixtures.slice(0,2).every(f=>f.worker.terminateCount===1));
  assert.equal(pool.stats.pendingSnapshotBytes,0); assert.equal(pool.stats.refreshing,false);
});

test('pending refresh owns its copied image and only one refresh may be admitted', opts, async t => {
  const { pool, fixtures } = await fixture(t,{workers:1});
  const replacement = await changedImage(), expected = createHash('sha256').update(replacement).digest('hex');
  const gate = block(fixtures[0]), old = pool.query('SELECT 1'); await gate.started;
  const pending = pool.refresh(replacement); replacement.fill(0);
  await assert.rejects(pool.refresh(await image()), {code:'ERR_FSQLITE_POOL_REFRESH_BUSY'});
  gate.release(); await old;
  assert.equal((await pending).snapshot.sha256,expected);
  assert.equal((await pool.query('SELECT count(*) AS n FROM items')).rows[0].n,3);
});

test('failed refresh preserves the old generation but refuses queries waiting for the new one', opts, async t => {
  const { pool, fixtures } = await fixture(t,{workers:2,hooks:index=>index===3?{
    beforeImport(){throw new Error('injected import failure');},
  }:{}});
  const old = pool.snapshot;
  const refresh = observed(pool.refresh(await changedImage()));
  const later = observed(pool.query('SELECT count(*) AS n FROM items'));
  const failure = (await refresh).error; assert.ok(failure instanceof AggregateError);
  assert.equal((await later).error.code,'ERR_FSQLITE_POOL_REFRESH_FAILED');
  assert.equal(pool.snapshot,old); assert.equal(pool.stats.pendingQueries,0);
  assert.equal((await pool.query('SELECT count(*) AS n FROM items')).rows[0].n,2);
  assert.ok(fixtures.slice(2).every(f=>f.worker.terminateCount===1));
  assert.ok(fixtures.slice(0,2).every(f=>f.worker.terminateCount===0));
});

test('successful refresh reports old cleanup errors without pretending publication failed', opts, async t => {
  const { pool } = await fixture(t,{workers:2,hooks:index=>index<2?{
    beforeClose(){throw new Error('old close failed');},
  }:{}});
  const result = await pool.refresh(await changedImage());
  assert.equal(result.snapshot.generation,2); assert.equal(result.cleanupErrors.length,2);
  assert.ok(result.cleanupErrors.every(error=>/old close failed/.test(error.message)));
  assert.ok(Object.isFrozen(result)); assert.ok(Object.isFrozen(result.cleanupErrors));
  assert.equal((await pool.query('SELECT count(*) AS n FROM items')).rows[0].n,3);
});

test('close drains an accepted refresh, then closes its new workers too', opts, async t => {
  const stage = deferred(), started = deferred();
  const { pool, fixtures } = await fixture(t,{workers:1,hooks:index=>index===1?{
    async beforeImport(){started.resolve();await stage.promise;},
  }:{}});
  const refreshed = pool.refresh(await changedImage()), read = pool.query('SELECT count(*) AS n FROM items');
  await started.promise;
  const closed = pool.close(); assert.equal(pool.close(),closed);
  await assert.rejects(pool.refresh(await image()),{code:'ERR_FSQLITE_POOL_CLOSED'});
  assert.equal(pool.stats.state,'closing'); assert.equal(fixtures[0].worker.terminateCount,0);
  stage.resolve(); await refreshed; assert.equal((await read).rows[0].n,3); await closed;
  assert.ok(fixtures.every(f=>f.worker.terminateCount===1)); assert.equal(pool.stats.state,'closed');
});

test('queries can expire or cancel behind an in-flight refresh without cancelling the refresh', opts, async t => {
  const stage = deferred(), started = deferred();
  const { pool } = await fixture(t,{workers:1,hooks:index=>index===1?{
    async beforeImport(){started.resolve();await stage.promise;},
  }:{}});
  const refresh=pool.refresh(await changedImage()); await started.promise;
  const c=new AbortController(), cancelled=observed(pool.query('SELECT 9',[],{signal:c.signal}));
  const expired=observed(pool.query('SELECT 8',[],{waitTimeoutMs:1})); c.abort();
  assert.equal((await cancelled).error.code,'ERR_FSQLITE_POOL_CANCELLED');
  assert.equal((await expired).error.code,'ERR_FSQLITE_POOL_TIMEOUT');
  assert.equal(pool.stats.pendingQueries,0);assert.equal(pool.stats.refreshing,true);
  stage.resolve();assert.equal((await refresh).snapshot.generation,2);
});

test('a reused worker on refresh is refused before disturbing its old connection', opts, async () => {
  const bytes=await image(), f=sqliteSnapshotWorker();
  const pool=await FrankenSnapshotPool.open(bytes,{workers:1,worker:()=>f.worker});
  try {
    await assert.rejects(pool.refresh(await changedImage()),AggregateError);
    assert.equal(f.counts().imports,1); assert.equal(f.worker.terminateCount,0);
    assert.equal((await pool.query('SELECT count(*) AS n FROM items')).rows[0].n,2);
  } finally { await pool.close(); }
});

test('invalid refresh input creates no barrier and leaves later queries usable', opts, async t => {
  const {pool}=await fixture(t,{workers:1});const identity=pool.snapshot;
  await assert.rejects(pool.refresh(new Uint8Array(100)));
  const result=await pool.query('SELECT 1');assert.equal(result.snapshot,identity);
  assert.equal(pool.stats.refreshing,false);assert.equal(pool.stats.pendingSnapshotBytes,0);
});

test('repeated successful refreshes advance identity even for byte-identical images', opts, async t => {
  const {pool,bytes,fixtures}=await fixture(t,{workers:2});const hash=pool.snapshot.sha256;
  for(let generation=2;generation<=4;generation++) {
    const result=await pool.refresh(bytes);
    assert.equal(result.snapshot.generation,generation);assert.equal(result.snapshot.sha256,hash);
  }
  await pool.close(); assert.equal(fixtures.length,8);assert.ok(fixtures.every(f=>f.worker.terminateCount===1));
});

test('a replica crashing while another replica initializes cannot produce a successful open', opts, async () => {
  const bytes=await image(), gate=deferred(), ready=deferred(), fixtures=[], listeners=[];
  const pending=observed(FrankenSnapshotPool.open(bytes,{workers:2,worker(){
    const index=fixtures.length, f=sqliteSnapshotWorker(index===1?{beforeImport:()=>gate.promise}:{});
    const add=f.worker.addEventListener.bind(f.worker),remove=f.worker.removeEventListener.bind(f.worker);
    const errors=new Set();listeners.push(errors);
    f.worker.addEventListener=(type,fn)=>{if(type==='error')errors.add(fn);add(type,fn);};
    f.worker.removeEventListener=(type,fn)=>{if(type==='error')errors.delete(fn);remove(type,fn);};
    if(index===0)f.worker.addEventListener('message',event=>{if(event.data.kind==='query-result')ready.resolve();});
    fixtures.push(f);return f.worker;
  }}));
  await ready.promise;await turn();
  for(const listener of [...listeners[0]])listener({message:'crashed during peer init'});
  gate.resolve();const result=await pending;
  if(result.value)await result.value.close().catch(()=>{}); // Also bound the old-code negative control.
  assert.ok(result.error instanceof AggregateError);
  assert.ok(fixtures.every(f=>f.worker.terminateCount===1));assert.ok(listeners.every(set=>set.size===0));
});

test('named bindings execute unchanged SQL by native SQLite slots across snapshot generations', opts, async t => {
  const references=[], stops=[];
  let pool;
  t.after(async()=>{if(pool)await pool.close().catch(()=>{});await Promise.all(stops);});
  const worker=()=>{
    const listeners={message:new Set(),error:new Set()};
    let reference, stopped;
    const host=new WorkerConnectionHost({async load(){return {FrankenDB:{
      async import(bytes){
        const path=join(await mkdtemp(join(tmpdir(),'fsqlite-pool-bindings-')),'db.sqlite');
        await writeFile(path,bytes);
        reference=sqliteBindingFixture(path);references.push(reference);return reference.core;
      },
    }};}});
    return {
      addEventListener(type,fn){listeners[type].add(fn);},
      removeEventListener(type,fn){listeners[type].delete(fn);},
      postMessage(request){void host.handle(structuredClone(request)).then(response=>{
        for(const listener of listeners.message)listener({data:structuredClone(response)});
      },cause=>{for(const listener of listeners.error)listener({message:String(cause)});});},
      terminate(){if(reference&&!stopped){stopped=reference.shutdown();stops.push(stopped);}},
    };
  };
  pool=await FrankenSnapshotPool.open(await image(),{workers:2,worker,resultEncoding:'binary'});
  const sql='SELECT :id AS id, :id AS again, :text AS text, @blob AS blob, $large AS large';
  const params={id:2,text:'NUL\0λ',blob:Uint8Array.of(0,255),large:9223372036854775807n};
  const results=await Promise.all(Array.from({length:8},()=>pool.query(sql,params)));
  for(const result of results)assert.deepEqual(result.rowArrays,[[2,2,'NUL\0λ',Uint8Array.of(0,255),9223372036854775807n]]);
  assert.equal(references.length,2);
  assert.ok(references.every(reference=>reference.requests.some(request=>request.sql===sql)));
  await assert.rejects(pool.query('SELECT :x, @x',{x:1}),/ambiguous/i);
  const refresh=await pool.refresh(await changedImage());
  const next=await pool.query('SELECT value FROM items WHERE id=:id',{id:20});
  assert.deepEqual(next.rowArrays,[['generation']]);assert.equal(next.snapshot,refresh.snapshot);
  await pool.close();await Promise.all(stops);assert.equal(stops.length,4);
});

test('an old worker crashing during staged refresh closes both generations without publishing', opts, async t => {
  const staged=deferred(), release=deferred(), errors=[];
  const {pool,fixtures}=await fixture(t,{workers:1,
    hooks:index=>index===1?{async beforeImport(){staged.resolve();await release.promise;}}:{},
    configure(f,index){
      const add=f.worker.addEventListener.bind(f.worker),remove=f.worker.removeEventListener.bind(f.worker);
      errors[index]=new Set();
      f.worker.addEventListener=(type,fn)=>{if(type==='error')errors[index].add(fn);add(type,fn);};
      f.worker.removeEventListener=(type,fn)=>{if(type==='error')errors[index].delete(fn);remove(type,fn);};
    },
  });
  const initial=pool.snapshot;
  const refresh=observed(pool.refresh(await changedImage()));await staged.promise;
  const queued=observed(pool.query('SELECT count(*) AS n FROM items'));
  for(const listener of [...errors[0]])listener({message:'old worker failed during refresh'});
  assert.equal((await queued).error.code,'ERR_FSQLITE_POOL_UNUSABLE');
  release.resolve();assert.ok((await refresh).error);await pool.close().catch(()=>{});
  assert.equal(pool.snapshot,initial);assert.equal(pool.stats.state,'closed');
  assert.equal(pool.stats.pendingQueries,0);assert.equal(pool.stats.pendingSnapshotBytes,0);
  assert.ok(fixtures.every(f=>f.worker.terminateCount===1));assert.ok(errors.every(set=>set.size===0));
});
