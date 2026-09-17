// Production SDK + worker with real, file-backed Node SQLite. NOT a browser,
// a native FrankenSQLite engine, or a FrankenSQLite WASM integration certificate.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { FrankenDBQueue, FrankenSQLiteError } from '../src/index.ts';
import { sqliteSnapshotWorker } from '../../worker/tests/helpers/snapshot-sqlite-core.mjs';
import { sqliteBindingFixture } from '../../worker/tests/helpers/bindings-core.mjs';
import { deferred, observe, drain } from './helpers/controlled-worker.ts';

const limits = { timeout: 5000 };
const isCode = code => error => error instanceof FrankenSQLiteError && error.code === code;
const tick = () => new Promise(resolve => setImmediate(resolve));
async function fixture(hooks = {}, queueOptions = {}) {
  const f = sqliteSnapshotWorker(hooks);
  const queue = await FrankenDBQueue.open({ worker: f.worker }, queueOptions);
  await queue.transaction(tx => tx.execute('CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT UNIQUE)'));
  return { ...f, queue };
}
function diskRows(f) {
  const db = new DatabaseSync(f.handles[0].path);
  try {
    assert.deepEqual(db.prepare('PRAGMA integrity_check').all().map(r => Object.values(r)), [['ok']]);
    return db.prepare('SELECT id,value FROM items ORDER BY id').all().map(r => [r.id,r.value]);
  } finally { db.close(); }
}
function accounting(q) {
  const s = q.stats;
  assert.ok(Object.isFrozen(s));
  assert.equal(s.pendingJobs, s.waitingJobs + s.activeJobs);
  assert.ok(s.activeJobs <= 1);
  assert.ok(s.pendingJobs <= s.maxPendingJobs);
  assert.equal(s.acceptedJobs, s.pendingJobs + s.completedJobs + s.failedJobs + s.cancelledJobs + s.timedOutJobs);
  return s;
}

// Scheduling tests assert committed files independently, not merely request order.
test('whole callbacks are FIFO and retain ownership across awaits; queued reads see committed writes', limits, async () => {
  const f = await fixture(), q = f.queue, held = deferred(), entered = deferred(), order = [];
  const first = q.transaction(async tx => {
    order.push('start1'); entered.resolve();
    await tx.execute("INSERT INTO items VALUES(1,'first')");
    await held.promise; order.push('end1'); return 11;
  });
  const second = q.transaction(async tx => {
    order.push('start2');
    assert.deepEqual((await tx.query('SELECT id FROM items')).rowArrays, [[1]]);
    await tx.execute("INSERT INTO items VALUES(2,'second')"); return 22;
  });
  await entered.promise; await drain();
  assert.deepEqual(order, ['start1']);
  assert.deepEqual(diskRows(f), []); // Uncommitted row is not visible in another connection.
  assert.equal(accounting(q).activeJobs, 1); assert.equal(q.stats.waitingJobs, 1);
  held.resolve(); assert.deepEqual(await Promise.all([first,second]), [11,22]);
  assert.deepEqual(order, ['start1','end1','start2']);
  assert.deepEqual(diskRows(f), [[1,'first'],[2,'second']]);
  await q.close(); assert.equal(accounting(q).state, 'closed');
});

test('48 competing callers complete through a four-job bound without write interleaving', {timeout:15000}, async () => {
  const f = await fixture({}, {maxPendingJobs:4}), q = f.queue;
  const started = [], committed = []; let refusals = 0, active = 0;
  async function caller(id) {
    for (;;) {
      try {
        await q.transaction(async tx => {
          assert.equal(++active, 1); started.push(id);
          await tx.execute('INSERT INTO items VALUES(?,?)', [id, `v${id}`]);
          await tick(); assert.equal(--active, 0); return id;
        }).then(value => { committed.push(value); });
        accounting(q); return;
      } catch (error) {
        assert.ok(isCode('ERR_FSQLITE_JOB_QUEUE_FULL')(error));
        refusals++; accounting(q); await tick();
      }
    }
  }
  await Promise.all(Array.from({length:48}, (_,i) => caller(i+1)));
  assert.ok(refusals > 0); assert.deepEqual(committed, started);
  assert.deepEqual(diskRows(f), Array.from({length:48},(_,i)=>[i+1,`v${i+1}`]));
  assert.equal(q.stats.acceptedJobs,49); assert.equal(q.stats.completedJobs,49);
  assert.equal(q.stats.rejectedJobs,refusals); await q.close();
});

test('overload is immediate, invokes no rejected callback and returns capacity only after commit', limits, async () => {
  let armed = false; const committing = deferred(), release = deferred();
  const f = await fixture({beforeBatch: async sql => {
    if (armed && sql === 'COMMIT') { committing.resolve(); await release.promise; }
  }}, {maxPendingJobs:1}); const q=f.queue;
  armed=true;
  const first=q.transaction(tx=>tx.execute("INSERT INTO items VALUES(1,'first')"));
  await committing.promise;
  let called=false;
  await assert.rejects(q.transaction(()=>{called=true;}), error =>
    isCode('ERR_FSQLITE_JOB_QUEUE_FULL')(error) && error.transient === true);
  assert.equal(called,false); assert.equal(accounting(q).pendingJobs,1);
  assert.deepEqual(diskRows(f),[]);
  release.resolve(); await first; armed=false;
  await q.transaction(tx=>tx.execute("INSERT INTO items VALUES(2,'next')"));
  assert.deepEqual(diskRows(f),[[1,'first'],[2,'next']]); await q.close();
});

test('callback and SQL failures roll back their own job and do not poison successful siblings', limits, async () => {
  const f=await fixture(), q=f.queue, original={reason:'producer'};
  const a=q.transaction(async tx=>{await tx.execute("INSERT INTO items VALUES(1,'lost')");throw original;});
  const aResult=observe(a);
  const b=q.transaction(tx=>tx.execute("INSERT INTO items VALUES(2,'kept')"));
  const c=q.transaction(async tx=>{
    await tx.execute("INSERT INTO items VALUES(3,'lost-too')");
    try {await tx.execute("INSERT INTO items VALUES(4,'kept')");} catch {} // Scope still fails.
  }); const cResult=observe(c);
  const d=q.transaction(tx=>tx.execute("INSERT INTO items VALUES(5,'last')"));
  await Promise.all([aResult.settled,b,cResult.settled,d]);
  assert.equal(aResult.outcome.reason,original); assert.equal(cResult.outcome.status,'rejected');
  assert.deepEqual(diskRows(f),[[2,'kept'],[5,'last']]);
  assert.equal(accounting(q).failedJobs,2); await q.close();
});

test('queue drains ignored admitted SQL and finalizes prepared handles before releasing a slot', limits, async () => {
  const f=await fixture(), q=f.queue; let statement, txHandle;
  const a=q.transaction(async tx=>{
    txHandle=tx; statement=await tx.prepare('INSERT INTO items VALUES(?,?)');
    await statement.bind([1,'saved']); void statement.run();
  });
  const b=q.transaction(async tx=>{
    assert.deepEqual((await tx.query('SELECT * FROM items')).rowArrays,[[1,'saved']]);
    await assert.rejects(statement.run(),isCode('ERR_FSQLITE_TRANSACTION_CLOSED'));
    await assert.rejects(txHandle.execute('DELETE FROM items'),isCode('ERR_FSQLITE_TRANSACTION_CLOSED'));
  });
  await Promise.all([a,b]); assert.deepEqual(diskRows(f),[[1,'saved']]); await q.close();
});

test('nested savepoints recover child failures without giving another queued job the connection', limits, async () => {
  const f=await fixture(), q=f.queue;
  const first=q.transaction(async tx=>{
    await tx.execute("INSERT INTO items VALUES(1,'parent')");
    await assert.rejects(tx.transaction(async child=>{
      await child.execute("INSERT INTO items VALUES(2,'child-lost')");throw new Error('child');
    }),/child/);
    await tx.transaction(child=>child.execute("INSERT INTO items VALUES(3,'child-kept')"));
  });
  const next=q.transaction(tx=>tx.query('SELECT * FROM items ORDER BY id'));
  await first; assert.deepEqual((await next).rowArrays,[[1,'parent'],[3,'child-kept']]);
  assert.deepEqual(diskRows(f),[[1,'parent'],[3,'child-kept']]); await q.close();
});

test('queued atomic streaming import rolls back earlier chunks on producer failure', limits, async () => {
  const f=await fixture(), q=f.queue;
  async function* source() {yield [1,'a']; yield [2,'b'];throw new Error('source failed');}
  const importing=observe(q.transaction(tx=>tx.executeStream('INSERT INTO items VALUES(?,?)',source(),{batchSize:1})));
  const next=q.transaction(tx=>tx.execute("INSERT INTO items VALUES(3,'survivor')"));
  await Promise.all([importing.settled,next]);
  assert.equal(importing.outcome.status,'rejected');
  assert.deepEqual(diskRows(f),[[3,'survivor']]); await q.close();
});

test('pre-aborted work is not admitted and preserves the exact local reason', limits, async () => {
  const f=await fixture(), q=f.queue, c=new AbortController(), reason={local:true};c.abort(reason);
  let called=false; const count=f.worker.requests.length;
  await assert.rejects(q.transaction(()=>{called=true;},{signal:c.signal}),e=>
    isCode('ERR_FSQLITE_JOB_CANCELLED')(e) && e.cause===reason);
  assert.equal(called,false); assert.equal(f.worker.requests.length,count);
  assert.equal(accounting(q).acceptedJobs,1); assert.equal(q.stats.rejectedJobs,1); await q.close();
});

test('immediate abort before the pump prevents even BEGIN', limits, async () => {
  const f=await fixture(), q=f.queue, c=new AbortController(), count=f.worker.requests.length;
  const outcome=observe(q.transaction(()=>assert.fail('must not run'),{signal:c.signal})); c.abort('stop');
  await outcome.settled; assert.ok(isCode('ERR_FSQLITE_JOB_CANCELLED')(outcome.outcome.reason));
  assert.equal(f.worker.requests.length,count); assert.equal(accounting(q).cancelledJobs,1); await q.close();
});

test('waiting cancellation removes the middle job and promptly restores capacity', limits, async () => {
  const f=await fixture({}, {maxPendingJobs:3}), q=f.queue, held=deferred(), entered=deferred(), c=new AbortController(), starts=[];
  const a=q.transaction(async tx=>{entered.resolve(); await held.promise; await tx.execute("INSERT INTO items VALUES(1,'a')");});
  const b=observe(q.transaction(()=>{starts.push('cancelled');},{signal:c.signal}));
  const d=q.transaction(tx=>{starts.push('d');return tx.execute("INSERT INTO items VALUES(4,'d')");});
  await entered.promise;c.signal.addEventListener('abort',e=>e.stopImmediatePropagation());c.abort('middle');
  await b.settled; assert.ok(isCode('ERR_FSQLITE_JOB_CANCELLED')(b.outcome.reason));
  assert.equal(accounting(q).pendingJobs,2);
  const e=q.transaction(tx=>{starts.push('e');return tx.execute("INSERT INTO items VALUES(5,'e')");});
  held.resolve(); await Promise.all([a,d,e]);
  assert.deepEqual(starts,['d','e']);assert.deepEqual(diskRows(f),[[1,'a'],[4,'d'],[5,'e']]);await q.close();
});

test('active abort waits for callback and rollback, then lets the next caller proceed', limits, async () => {
  const f=await fixture(),q=f.queue,c=new AbortController(),entered=deferred(),held=deferred();let nextStarted=false;
  const first=observe(q.transaction(async tx=>{
    await tx.execute("INSERT INTO items VALUES(1,'cancelled')");entered.resolve();await held.promise;
  },{signal:c.signal}));
  const next=q.transaction(tx=>{nextStarted=true;return tx.execute("INSERT INTO items VALUES(2,'kept')");});
  await entered.promise;c.abort('cancel active');await drain();
  assert.equal(first.outcome.status,'pending');assert.equal(nextStarted,false);
  assert.equal(accounting(q).pendingJobs,2);held.resolve();await first.settled;await next;
  assert.ok(isCode('ERR_FSQLITE_TRANSACTION_CANCELLED')(first.outcome.reason));
  assert.deepEqual(diskRows(f),[[2,'kept']]);assert.equal(q.stats.failedJobs,1);assert.equal(q.stats.cancelledJobs,0);await q.close();
});

test('abort during dispatched COMMIT does not relabel committed work as cancelled', limits, async () => {
  const c=new AbortController(), committing=deferred(), release=deferred();let armed=false;
  const f=await fixture({beforeBatch:async sql=>{if(armed&&sql==='COMMIT'){committing.resolve();await release.promise;}}});
  const q=f.queue;armed=true;
  const first=q.transaction(async tx=>{await tx.execute("INSERT INTO items VALUES(1,'committed')");return 7;},{signal:c.signal});
  await committing.promise;c.abort('too late');release.resolve();assert.equal(await first,7);
  assert.deepEqual(diskRows(f),[[1,'committed']]);assert.equal(accounting(q).failedJobs,0);await q.close();
});

test('a queued start timeout rejects without touching the database or callback', limits, async () => {
  const f=await fixture(),q=f.queue,entered=deferred(),held=deferred();let called=false;
  const active=q.transaction(async()=>{entered.resolve();await held.promise;});await entered.promise;
  const count=f.worker.requests.length;
  await assert.rejects(q.transaction(()=>{called=true;},{waitTimeoutMs:5}),isCode('ERR_FSQLITE_JOB_WAIT_TIMEOUT'));
  assert.equal(called,false);assert.equal(f.worker.requests.length,count);assert.equal(accounting(q).timedOutJobs,1);
  held.resolve();await active;await q.close();
});

test('deadline is checked before starting even when the timer task has not run', limits, async () => {
  const f=await fixture(),q=f.queue;const count=f.worker.requests.length;
  const outcome=observe(q.transaction(()=>assert.fail('expired callback'),{waitTimeoutMs:5}));
  // Block this test's event loop so the queued pump runs before the timer task.
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)),0,0,20);
  await outcome.settled;assert.ok(isCode('ERR_FSQLITE_JOB_WAIT_TIMEOUT')(outcome.outcome.reason));
  assert.equal(f.worker.requests.length,count);assert.equal(accounting(q).timedOutJobs,1);await q.close();
});

test('start timeout never abandons an already-running transaction', limits, async () => {
  const f=await fixture(),q=f.queue,entered=deferred(),held=deferred();
  const first=observe(q.transaction(async tx=>{entered.resolve();await held.promise;
    await tx.execute("INSERT INTO items VALUES(1,'long-lived')");return 19;
  },{waitTimeoutMs:100}));await entered.promise;
  await new Promise(resolve=>setTimeout(resolve,130));
  assert.equal(first.outcome.status,'pending');assert.equal(accounting(q).activeJobs,1);
  held.resolve();await first.settled;assert.equal(first.outcome.value,19);
  assert.equal(q.stats.timedOutJobs,0);assert.deepEqual(diskRows(f),[[1,'long-lived']]);await q.close();
});

test('abort and timeout settle only once and do not affect subsequent jobs', limits, async () => {
  const f=await fixture(),q=f.queue,held=deferred(),c=new AbortController();
  const active=q.transaction(()=>held.promise);const stopped=observe(q.transaction(()=>assert.fail(),{signal:c.signal,waitTimeoutMs:10}));
  c.abort('one');await stopped.settled;await new Promise(resolve=>setTimeout(resolve,20));
  assert.equal(accounting(q).cancelledJobs,1);assert.equal(q.stats.timedOutJobs,0);
  held.resolve();await active;await q.transaction(tx=>tx.execute("INSERT INTO items VALUES(1,'ok')"));
  assert.deepEqual(diskRows(f),[[1,'ok']]);await q.close();
});

test('close is a deduplicated drain fence, not a cancellation or premature resource release', limits, async () => {
  const f=await fixture(),q=f.queue,entered=deferred(),held=deferred();
  const a=q.transaction(async tx=>{entered.resolve();await held.promise;await tx.execute("INSERT INTO items VALUES(1,'a')");});
  const b=q.transaction(tx=>tx.execute("INSERT INTO items VALUES(2,'b')"));await entered.promise;
  const closing=q.close(), observed=observe(closing);assert.equal(q.close(),closing);
  await assert.rejects(q.transaction(()=>assert.fail('late')),isCode('ERR_FSQLITE_JOB_QUEUE_CLOSED'));
  await drain();assert.equal(observed.outcome.status,'pending');assert.equal(f.worker.terminateCount,0);
  assert.equal(accounting(q).state,'closing');held.resolve();await Promise.all([a,b,closing]);
  assert.equal(f.worker.terminateCount,1);assert.equal(f.events.filter(x=>x==='close').length,1);
  assert.deepEqual(diskRows(f),[[1,'a'],[2,'b']]);assert.equal(accounting(q).state,'closed');assert.equal(q.close(),closing);
});

test('a failed job does not prevent close from draining later accepted work', limits, async () => {
  const f=await fixture(),q=f.queue;
  const failed=observe(q.transaction(async tx=>{await tx.execute("INSERT INTO items VALUES(1,'no')");throw new Error('rollback');}));
  const good=q.transaction(tx=>tx.execute("INSERT INTO items VALUES(2,'yes')"));
  await Promise.all([failed.settled,good,q.close()]);
  assert.deepEqual(diskRows(f),[[2,'yes']]);assert.equal(accounting(q).failedJobs,1);
});

test('waiting cancellation can remove the last backlog entry while closing', limits, async () => {
  const f=await fixture(),q=f.queue,held=deferred(),c=new AbortController();
  const active=q.transaction(()=>held.promise);
  const waiting=observe(q.transaction(()=>assert.fail(),{signal:c.signal}));const closing=q.close();
  await drain();c.abort();await waiting.settled;held.resolve();await Promise.all([active,closing]);
  assert.equal(accounting(q).pendingJobs,0);assert.equal(q.stats.cancelledJobs,1);assert.equal(f.worker.terminateCount,1);
});

test('rollback failure preserves its cause, prevents queued callbacks from running, and drains their promises', limits, async () => {
  const rollback=new Error('injected rollback failure');let armed=false;
  const f=await fixture({beforeBatch:sql=>{if(armed&&sql==='ROLLBACK')throw rollback;}}),q=f.queue;
  armed=true;const original=new Error('callback');let ran=false;
  const first=observe(q.transaction(async tx=>{await tx.execute("INSERT INTO items VALUES(1,'not-committed')");throw original;}));
  const next=observe(q.transaction(()=>{ran=true;}));const closed=observe(q.close());
  await Promise.all([first.settled,next.settled,closed.settled]);
  assert.equal(first.outcome.status,'rejected');assert.equal(first.outcome.reason.cause,original);
  assert.ok(first.outcome.reason instanceof AggregateError);assert.equal(first.outcome.reason.errors.length,2);
  assert.equal(ran,false);assert.equal(next.outcome.status,'rejected');assert.equal(f.worker.terminateCount,1);
  assert.equal(accounting(q).pendingJobs,0);assert.equal(q.stats.state,'closed');assert.deepEqual(diskRows(f),[]);
});

test('undelivered BEGIN rejects that job without poisoning a still-live worker', limits, async () => {
  const f=await fixture(),q=f.queue;const post=f.worker.postMessage.bind(f.worker);let first=true,called=false;
  f.worker.postMessage=(request,transfer)=>{if(first){first=false;throw new Error('transport broken');}return post(request,transfer);};
  const a=observe(q.transaction(()=>{called=true;}));
  const b=observe(q.transaction(tx=>tx.execute("INSERT INTO items VALUES(1,'recoverable')")));
  await Promise.all([a.settled,b.settled]);
  assert.equal(a.outcome.status,'rejected');assert.equal(called,false);
  // Synchronous delivery failure is not a proven crash: no BEGIN was delivered,
  // and the still-live worker may legitimately accept the next queued job.
  assert.equal(b.outcome.status,'fulfilled');assert.deepEqual(diskRows(f),[[1,'recoverable']]);
  assert.equal(accounting(q).failedJobs,1);await q.close();
});

test('close failure is preserved and shared, with no retry or leaked admission', limits, async () => {
  const failure=new Error('close failed');const f=await fixture({beforeClose:()=>{throw failure;}}),q=f.queue;
  const close=q.close();await assert.rejects(close,/close failed/);assert.equal(q.close(),close);
  assert.equal(accounting(q).state,'closed');assert.equal(f.worker.terminateCount,1);
  await assert.rejects(q.transaction(()=>1),isCode('ERR_FSQLITE_JOB_QUEUE_CLOSED'));
});

test('configuration is captured once and invalid limits fail before allocating a worker', limits, async () => {
  let spawned=0;
  for(const maxPendingJobs of [0,-1,1.2,4097,NaN,Infinity,'4']) {
    await assert.rejects(FrankenDBQueue.open({worker:()=>{spawned++;assert.fail();}},{maxPendingJobs}),RangeError);
  }
  assert.equal(spawned,0);const options={maxPendingJobs:3};const f=await fixture({},options);options.maxPendingJobs=1;
  assert.equal(f.queue.stats.maxPendingJobs,3);assert.equal(f.queue.persistence,'memory');assert.equal(f.queue.snapshotRevision,null);
  assert.equal(f.queue.path,f.handles[0].path);await f.queue.close();
});

test('invalid callbacks, deadlines and signal lookalikes reject without reserving or executing', limits, async () => {
  const f=await fixture(),q=f.queue,count=f.worker.requests.length;
  await assert.rejects(q.transaction(null),TypeError);
  for(const waitTimeoutMs of [0,-1,0.5,Infinity,NaN,2147483648,'10'])
    await assert.rejects(q.transaction(()=>{}, {waitTimeoutMs}),RangeError);
  for(const signal of [null,{}, {aborted:false}])await assert.rejects(q.transaction(()=>{}, {signal}),TypeError);
  assert.equal(f.worker.requests.length,count);assert.equal(accounting(q).acceptedJobs,1);await q.close();
});

test('reentrant option getters cannot sneak work past a close fence', limits, async () => {
  const f=await fixture(),q=f.queue,count=f.worker.requests.length;let closing,reads=0,called=false;
  await assert.rejects(q.transaction(()=>{called=true;},{get signal(){reads++;closing=q.close();return undefined;}}),
    isCode('ERR_FSQLITE_JOB_QUEUE_CLOSED'));
  await closing;assert.equal(reads,1);assert.equal(called,false);
  assert.equal(f.worker.requests.slice(count).some(r=>r.kind==='transaction'),false);
  assert.equal(accounting(q).acceptedJobs,1);
});

test('reentrant admission cannot overbook the configured limit', limits, async () => {
  const f=await fixture({}, {maxPendingJobs:1}),q=f.queue;let admitted,called=false;
  await assert.rejects(q.transaction(()=>{called=true;},{get waitTimeoutMs(){
    admitted=q.transaction(tx=>tx.execute("INSERT INTO items VALUES(1,'inner')"));return undefined;
  }}),isCode('ERR_FSQLITE_JOB_QUEUE_FULL'));
  await admitted;assert.equal(called,false);assert.deepEqual(diskRows(f),[[1,'inner']]);accounting(q);await q.close();
});

test('independently opened queues do not share a scheduler or live database', limits, async () => {
  const a=await fixture(),b=await fixture(),held=deferred(),entered=deferred();
  const first=a.queue.transaction(async tx=>{entered.resolve();await held.promise;await tx.execute("INSERT INTO items VALUES(1,'a')");});
  await entered.promise;await b.queue.transaction(tx=>tx.execute("INSERT INTO items VALUES(2,'b')"));
  assert.deepEqual(diskRows(b),[[2,'b']]);assert.deepEqual(diskRows(a),[]);
  held.resolve();await first;assert.deepEqual(diskRows(a),[[1,'a']]);await Promise.all([a.queue.close(),b.queue.close()]);
});


test('queued named prepared bindings use real native slot resolution and preserve values between jobs', limits, async t => {
  const f=sqliteBindingFixture();t.after(()=>f.shutdown());
  const q=await FrankenDBQueue.open({worker:f.worker});
  await q.transaction(tx=>tx.execute('CREATE TABLE items(id INTEGER PRIMARY KEY,value)'));
  const jobs=Array.from({length:8},(_,id)=>q.transaction(async tx=>{
    const insert=await tx.prepare('INSERT INTO items VALUES(:id,@value)');
    await insert.bind({id,value:`v${id}`});await insert.run();
    const read=await tx.prepare('SELECT value FROM items WHERE id=:id');
    return read.get({id});
  }));
  assert.deepEqual(await Promise.all(jobs),Array.from({length:8},(_,id)=>({value:`v${id}`})));
  assert.deepEqual((await q.transaction(tx=>tx.query('SELECT * FROM items ORDER BY id'))).rowArrays,
    Array.from({length:8},(_,id)=>[id,`v${id}`]));
  accounting(q);await q.close();
});

test('a terminal worker error settles admitted jobs without running further callbacks', limits, async () => {
  const f=sqliteSnapshotWorker();const add=f.worker.addEventListener.bind(f.worker);let crash;
  f.worker.addEventListener=(type,listener)=>{if(type==='error')crash=listener;add(type,listener);};
  const q=await FrankenDBQueue.open({worker:f.worker});
  await q.transaction(tx=>tx.execute('CREATE TABLE items(id INTEGER PRIMARY KEY,value)'));
  const entered=deferred(),release=deferred();let nextRan=false;
  const active=observe(q.transaction(async tx=>{
    await tx.execute("INSERT INTO items VALUES(1,'not committed')");entered.resolve();await release.promise;
  }));
  const waiting=observe(q.transaction(()=>{nextRan=true;}));await entered.promise;
  // Transport error injection, not an actual worker-thread crash.
  crash({message:'terminal worker crash'});await drain();assert.equal(nextRan,false);
  assert.equal(active.outcome.status,'pending');release.resolve();
  await Promise.all([active.settled,waiting.settled]);
  assert.equal(active.outcome.status,'rejected');assert.equal(waiting.outcome.status,'rejected');assert.equal(nextRan,false);
  assert.equal(f.worker.terminateCount,1);assert.deepEqual(diskRows(f),[]);accounting(q);
  await observe(q.close()).settled;
  // The local transport fixture does not own a browser thread whose termination
  // closes SQLite. Explicitly release the reference connection, not product state.
  f.handles[0].close();
});
