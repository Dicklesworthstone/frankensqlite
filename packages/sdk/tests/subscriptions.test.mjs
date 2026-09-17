// Public queue + production worker host; Node SQLite is the independent SQL core.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { setTimeout as delay } from 'node:timers/promises';
import { FrankenDBQueue } from '../src/queue.ts';
import { sqliteSnapshotWorker } from '../../worker/tests/helpers/snapshot-sqlite-core.mjs';
import { deferred, observe } from './helpers/controlled-worker.ts';

const limits = { timeout: 15000 };
const code = name => error => error?.code === name || error?.cause?.code === name || error?.errors?.some(code(name));
async function fixture(t, hooks = {}, queueOptions = {}) {
  const f = sqliteSnapshotWorker(hooks);
  const queue = await FrankenDBQueue.open({ worker: f.worker }, queueOptions);
  t.after(() => queue.close().catch(() => {}));
  await queue.transaction(tx => tx.executeBatch(
    'CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT UNIQUE); CREATE TABLE audit(id); CREATE TABLE other(id);'));
  return { ...f, queue };
}
function disk(q, sql = 'SELECT id, value FROM items ORDER BY id') {
  const db = new DatabaseSync(q.path);
  try { return db.prepare(sql).all().map(row => Object.values(row)); } finally { db.close(); }
}
function capture(q, tables = ['items']) {
  const events = [], first = deferred();
  return q.subscribe(tables, event => { events.push(event); first.resolve(event); })
    .then(handle => ({ events, first: first.promise, handle }));
}

test('publishes immutable table invalidations after the real COMMIT, never before', limits, async t => {
  let armed = false; const entered = deferred(), release = deferred();
  const {queue:q} = await fixture(t, {beforeBatch: async sql => {
    if (armed && sql === 'COMMIT') { entered.resolve(); await release.promise; }
  }});
  const watch = await capture(q);
  armed = true;
  const write = observe(q.transaction(tx => tx.execute("INSERT INTO items VALUES(1,'committed')")));
  await entered.promise; await delay(5);
  assert.equal(write.outcome.status,'pending'); assert.deepEqual(watch.events,[]);
  assert.deepEqual(disk(q),[]); assert.equal(q.changeSequence,0n);
  release.resolve(); await write.settled; armed = false;
  const event = await watch.first;
  assert.deepEqual(event, {tables:['items'],firstSequence:1n,lastSequence:1n,commits:1n});
  assert.deepEqual(disk(q),[[1,'committed']]);
  assert.ok(Object.isFrozen(event)); assert.ok(Object.isFrozen(event.tables));
  assert.ok(Object.isFrozen(watch.handle));
  assert.equal(q.changeSequence,1n);
});

test('subscription registration is a FIFO barrier, not a replay of preceding writes', limits, async t => {
  const {queue:q}=await fixture(t);
  const first=q.transaction(tx=>tx.execute("INSERT INTO items VALUES(1,'before')"));
  const pending=capture(q);
  const next=q.transaction(tx=>tx.execute("INSERT INTO items VALUES(2,'after')"));
  await first; const watch=await pending; await next;
  assert.deepEqual(await watch.first,{tables:['items'],firstSequence:1n,lastSequence:1n,commits:1n});
  assert.deepEqual(disk(q),[[1,'before'],[2,'after']]);
});

test('filters by table, sees trigger effects and never notifies reads, no-op writes or unrelated work', limits, async t => {
  const {queue:q}=await fixture(t);
  await q.transaction(tx=>tx.execute('CREATE TRIGGER log AFTER INSERT ON items BEGIN INSERT INTO audit VALUES(new.id); END'));
  const items=await capture(q),audit=await capture(q,['audit']);
  await q.transaction(tx=>tx.executeMany('INSERT INTO items VALUES(?,?)',[[1,'a'],[2,'b'],[3,'c']]));
  await Promise.all([items.first,audit.first]);
  assert.deepEqual(items.events[0].tables,['items']);assert.deepEqual(audit.events[0].tables,['audit']);
  assert.equal(items.events[0].commits,1n);assert.equal(audit.events[0].lastSequence,1n);
  await q.transaction(async tx=>{
    await tx.query('SELECT * FROM items');await tx.execute('DELETE FROM items WHERE id=100');
    await tx.execute('INSERT INTO other VALUES(1)');
  });
  await delay(5);assert.equal(items.events.length,1);assert.equal(audit.events.length,1);
  assert.equal(q.changeSequence,1n);
});

test('rollback and failed later stream chunks do not publish earlier writes or trigger effects', limits, async t => {
  const {queue:q}=await fixture(t);const watch=await capture(q,['items','audit']);
  await q.transaction(tx=>tx.execute('CREATE TRIGGER log AFTER INSERT ON items BEGIN INSERT INTO audit VALUES(new.id); END'));
  await assert.rejects(q.transaction(async tx=>{
    await tx.execute("INSERT INTO items VALUES(1,'discard')");throw Error('rollback');
  }),/rollback/);
  await assert.rejects(q.transaction(tx=>tx.executeStream('INSERT INTO items VALUES(?,?)',[[1,'a'],[2,'b'],[1,'duplicate']],{batchSize:1})));
  await delay(5);assert.deepEqual(watch.events,[]);assert.equal(q.changeSequence,0n);
  assert.deepEqual(disk(q),[]);assert.deepEqual(disk(q,'SELECT * FROM audit'),[]);
  await q.transaction(tx=>tx.execute("INSERT INTO items VALUES(1,'keep')"));
  assert.deepEqual((await watch.first).tables,['audit','items']);
});

test('child-only rollback removes its dirty bits while released successful siblings notify', limits, async t => {
  const {queue:q}=await fixture(t);const watch=await capture(q,['items','audit','other']);
  await q.transaction(async parent=>{
    await parent.execute("INSERT INTO items VALUES(1,'keep')");
    await assert.rejects(parent.transaction(async child=>{await child.execute('INSERT INTO audit VALUES(1)');throw Error('child');}),/child/);
    await parent.transaction(child=>child.execute('INSERT INTO other VALUES(2)'));
  });
  assert.deepEqual((await watch.first).tables,['items','other']);
  assert.deepEqual(disk(q,'SELECT * FROM audit'),[]);
});

test('uncommitted active cancellation emits nothing; the next transaction still delivers', limits, async t=>{
  const {queue:q}=await fixture(t),c=new AbortController();const watch=await capture(q);
  const entered=deferred(),release=deferred();
  const job=observe(q.transaction(async tx=>{await tx.execute("INSERT INTO items VALUES(1,'lost')");entered.resolve();await release.promise;},{signal:c.signal}));
  await entered.promise;c.abort('cancel');await delay(5);assert.equal(job.outcome.status,'pending');
  assert.deepEqual(watch.events,[]);release.resolve();await job.settled;
  assert.equal(job.outcome.status,'rejected');assert.deepEqual(disk(q),[]);
  await q.transaction(tx=>tx.execute("INSERT INTO items VALUES(2,'keep')"));
  assert.equal((await watch.first).commits,1n);assert.deepEqual(disk(q),[[2,'keep']]);
});

test('late cancellation after commit dispatch preserves both the result and its notification', limits, async t=>{
  let armed=false;const entered=deferred(),release=deferred(),c=new AbortController();
  const {queue:q}=await fixture(t,{beforeBatch:async sql=>{if(armed&&sql==='COMMIT'){entered.resolve();await release.promise;}}});
  const watch=await capture(q);armed=true;
  const job=q.transaction(async tx=>{await tx.execute("INSERT INTO items VALUES(1,'keep')");return 42;},{signal:c.signal});
  await entered.promise;c.abort('late');release.resolve();assert.equal(await job,42);armed=false;
  assert.equal((await watch.first).lastSequence,1n);assert.deepEqual(disk(q),[[1,'keep']]);
});

test('slow listeners do not block writers and retain only one coalesced pending record', limits, async t=>{
  const {queue:q}=await fixture(t);const events=[],entered=deferred(),release=deferred(),second=deferred();
  let running=0,peak=0;
  const subscription=await q.subscribe(['items','other'],async event=>{
    running++;peak=Math.max(peak,running);events.push(event);
    if(events.length===1){entered.resolve();await release.promise;}else second.resolve();
    running--;
  });
  await q.transaction(tx=>tx.execute("INSERT INTO items VALUES(1,'first')"));await entered.promise;
  for(let i=2;i<=81;i++) await q.transaction(tx=>tx.execute('INSERT INTO items VALUES(?,?)',[i,`row-${i}`]));
  await q.transaction(tx=>tx.execute('INSERT INTO other VALUES(1)'));
  assert.equal(q.stats.pendingJobs,0);assert.equal(q.stats.pendingNotifications,1);assert.equal(q.stats.activeListeners,1);
  assert.equal(events.length,1);assert.equal(disk(q).length,81);
  release.resolve();await second.promise;
  assert.equal(peak,1);assert.equal(events.length,2);
  assert.deepEqual(events[1],{tables:['items','other'],firstSequence:2n,lastSequence:82n,commits:81n});
  subscription.unsubscribe();await subscription.done;
});

test('a listener may safely await new queued SQL and unsubscribe without self-deadlock', limits, async t=>{
  const {queue:q}=await fixture(t);const notified=deferred();let subscription;
  subscription=await q.subscribe(['items'],async()=>{
    const result=await q.transaction(tx=>tx.query('SELECT value FROM items'));
    subscription.unsubscribe();notified.resolve(result.rows[0].value);
  });
  await q.transaction(tx=>tx.execute("INSERT INTO items VALUES(1,'read from listener')"));
  assert.equal(await notified.promise,'read from listener');await subscription.done;
  assert.equal(subscription.state,'stopped');assert.equal(q.stats.reservedSubscriptions,0);
});

test('listener failure is isolated, retains its cause, and never rejects the committed writer', limits, async t=>{
  const {queue:q}=await fixture(t);const failure=Error('listener failed');
  const bad=await q.subscribe(['items'],async()=>{throw failure;});const good=await capture(q);
  assert.equal(await q.transaction(tx=>tx.execute("INSERT INTO items VALUES(1,'kept')")),1);
  await assert.rejects(bad.done,e=>e===failure);await good.first;
  assert.equal(bad.state,'failed');assert.equal(bad.failure,failure);assert.equal(q.stats.subscriptions,1);
  assert.deepEqual(disk(q),[[1,'kept']]);
  await q.transaction(tx=>tx.execute("INSERT INTO items VALUES(2,'also kept')"));
});

test('throwing undefined from a listener still rejects done and releases capacity', limits, async t=>{
  const {queue:q}=await fixture(t);const bad=await q.subscribe(['items'],()=>{throw undefined;});
  await q.transaction(tx=>tx.execute("INSERT INTO items VALUES(1,'keep')"));
  const observed=await bad.done.then(()=>({ok:true}),cause=>({ok:false,cause}));
  assert.deepEqual(observed,{ok:false,cause:undefined});assert.equal(bad.state,'failed');
  assert.equal(q.stats.reservedSubscriptions,0);
});

test('unsubscribe is immediate under saturation and removes TEMP triggers before the next job', limits, async t=>{
  const {queue:q}=await fixture(t,{}, {maxPendingJobs:1});const watch=await capture(q);
  const entered=deferred(),release=deferred();
  const job=q.transaction(async tx=>{await tx.execute("INSERT INTO items VALUES(1,'kept')");entered.resolve();await release.promise;});
  await entered.promise;watch.handle.unsubscribe();watch.handle.unsubscribe();await watch.handle.done;
  assert.equal(watch.handle.state,'stopped');assert.equal(q.stats.subscriptions,0);
  release.resolve();await job;await delay(5);assert.deepEqual(watch.events,[]);
  const triggers=await q.transaction(tx=>tx.query("SELECT name FROM temp.sqlite_master WHERE type='trigger'"));
  assert.deepEqual(triggers.rowArrays,[]);
  await q.transaction(tx=>tx.execute('DROP TABLE items'));
});

test('stopped in-flight listeners retain reservations until done, bounding resubscription churn', limits, async t=>{
  const {queue:q}=await fixture(t,{}, {maxSubscriptions:1});const entered=deferred(),release=deferred();
  const sub=await q.subscribe(['items'],async()=>{entered.resolve();await release.promise;});
  await q.transaction(tx=>tx.execute("INSERT INTO items VALUES(1,'keep')"));await entered.promise;
  sub.unsubscribe();assert.equal(q.stats.subscriptions,0);assert.equal(q.stats.reservedSubscriptions,1);
  await assert.rejects(q.subscribe(['items'],()=>{}),code('ERR_FSQLITE_SUBSCRIPTION_LIMIT'));
  release.resolve();await sub.done;
  const replacement=await q.subscribe(['items'],()=>{});assert.equal(replacement.state,'active');
});

test('close stops pending deliveries without awaiting a listener that itself awaits close', limits, async t=>{
  const {queue:q}=await fixture(t);const entered=deferred(),release=deferred();let sub;
  sub=await q.subscribe(['items'],async()=>{entered.resolve();await release.promise;await q.close();});
  await q.transaction(tx=>tx.execute("INSERT INTO items VALUES(1,'keep')"));await entered.promise;
  const close=q.close();assert.equal(close,q.close());await close;
  assert.equal(sub.state,'stopped');assert.equal(q.stats.activeListeners,1);
  release.resolve();await sub.done;assert.equal(q.stats.reservedSubscriptions,0);
  assert.deepEqual(disk(q),[[1,'keep']]);
});

test('registration validates names atomically, canonicalizes aliases, and owns mutable arrays', limits, async t=>{
  const {queue:q}=await fixture(t);const names=['ITEMS','items'];
  names[Symbol.iterator]=()=>{throw Error('must not iterate caller');};
  const got=deferred();const pending=q.subscribe(names,e=>got.resolve(e));names[0]='audit';names[1]='other';
  const sub=await pending;assert.deepEqual(sub.tables,['items']);
  await assert.rejects(q.subscribe(['audit','missing'],()=>{}),code('ERR_FSQLITE_SUBSCRIPTION_INPUT'));
  assert.equal(q.stats.subscriptions,1);
  await q.transaction(tx=>tx.execute("INSERT INTO items VALUES(1,'keep')"));assert.deepEqual((await got.promise).tables,['items']);
});

test('invalid inputs and reentrant getters cannot register work after close or beyond capacity', limits, async t=>{
  const {queue:q}=await fixture(t);
  for(const names of [[],[''],['sqlite_master'],['__fsqlite_watch_x'],['a\0b'],new Array(65),null])
    await assert.rejects(q.subscribe(names,()=>{}),code('ERR_FSQLITE_SUBSCRIPTION_INPUT'));
  await assert.rejects(q.subscribe(['items'],null),TypeError);
  const names=[];Object.defineProperty(names,0,{get(){void q.close();return 'items';}});
  await assert.rejects(q.subscribe(names,()=>{}),code('ERR_FSQLITE_JOB_QUEUE_CLOSED'));
  await q.close();assert.equal(q.stats.subscriptions,0);
});

test('watch sets are union-bounded and duplicate subscriptions reuse table triggers', limits, async t=>{
  const {queue:q}=await fixture(t,{}, {maxSubscriptions:80});
  await q.transaction(tx=>tx.executeBatch(Array.from({length:63},(_,i)=>`CREATE TABLE t${i}(id);`).join('')));
  const names=['items',...Array.from({length:63},(_,i)=>`t${i}`)];
  const broad=await q.subscribe(names,()=>{});
  for(let i=0;i<65;i++)await q.subscribe(['ITEMS'],()=>{});
  const count=await q.transaction(tx=>tx.query("SELECT count(*) n FROM temp.sqlite_master WHERE type='trigger'"));
  assert.equal(count.rows[0].n,192);assert.equal(q.stats.subscriptions,66);
  await assert.rejects(q.subscribe(['audit'],()=>{}),code('ERR_FSQLITE_SUBSCRIPTION_INPUT'));
  broad.unsubscribe();await broad.done;
  await q.subscribe(['audit'],()=>{});
  const after=await q.transaction(tx=>tx.query("SELECT count(*) n FROM temp.sqlite_master WHERE type='trigger'"));
  assert.equal(after.rows[0].n,6);
});

test('watched schema changes fail before commit; unsubscribe permits migration', limits, async t=>{
  const {queue:q}=await fixture(t);const watch=await capture(q);
  await assert.rejects(q.transaction(async tx=>{
    await tx.execute("INSERT INTO items VALUES(1,'rollback')");await tx.execute('ALTER TABLE items ADD COLUMN extra');
  }),code('ERR_FSQLITE_SUBSCRIPTION_SCHEMA'));
  assert.deepEqual(disk(q),[]);await delay(5);assert.deepEqual(watch.events,[]);
  watch.handle.unsubscribe();await watch.handle.done;
  await q.transaction(tx=>tx.execute('ALTER TABLE items ADD COLUMN extra'));
  const next=await capture(q);await q.transaction(tx=>tx.execute("INSERT INTO items VALUES(2,'keep',NULL)"));
  await next.first;assert.deepEqual(disk(q),[[2,'keep']]);
});

test('registration cancellation has both waiting and active rollback boundaries', limits, async t=>{
  let armed=false;const configuring=deferred(),allow=deferred();
  const {queue:q}=await fixture(t,{beforeExecute:async sql=>{
    if(armed&&sql.startsWith('CREATE TEMP TRIGGER')){configuring.resolve();await allow.promise;}
  }});
  const held=deferred(),started=deferred();const active=q.transaction(async()=>{started.resolve();await held.promise;});
  await started.promise;const c=new AbortController();const waiting=observe(q.subscribe(['items'],()=>{},{signal:c.signal}));
  c.abort();await waiting.settled;held.resolve();await active;
  assert.equal(waiting.outcome.status,'rejected');assert.equal(q.stats.subscriptions,0);
  const c2=new AbortController();armed=true;
  const registration=observe(q.subscribe(['items'],()=>{},{signal:c2.signal}));
  await configuring.promise;c2.abort('during configure');allow.resolve();await registration.settled;armed=false;
  assert.equal(registration.outcome.status,'rejected');assert.equal(q.stats.subscriptions,0);
  assert.deepEqual((await q.transaction(tx=>tx.query("SELECT name FROM temp.sqlite_master WHERE type='trigger'"))).rowArrays,[]);
});

test('lifetime abort is independent of transaction cancellation and cannot be hidden by an earlier listener', limits, async t=>{
  const {queue:q}=await fixture(t),c=new AbortController();const events=[];
  c.signal.addEventListener('abort',event=>event.stopImmediatePropagation());
  const sub=await q.subscribe(['items'],event=>events.push(event),{signal:c.signal});
  c.abort('stop');await sub.done;
  await q.transaction(tx=>tx.execute("INSERT INTO items VALUES(1,'committed')"));
  await delay(5);assert.deepEqual(events,[]);assert.equal(sub.state,'stopped');assert.deepEqual(disk(q),[[1,'committed']]);
});

test('without subscribers, no journal SQL or extra transaction boundary is introduced', limits, async t=>{
  const {queue:q,events}=await fixture(t);events.length=0;
  await q.transaction(tx=>tx.execute("INSERT INTO items VALUES(1,'plain')"));
  assert.equal(events.filter(sql=>sql==='BEGIN').length,1);assert.equal(events.filter(sql=>sql==='COMMIT').length,1);
  assert.ok(!events.some(sql=>sql.includes('__fsqlite_watch_')));
});

test('change streams implement for-await with automatic unsubscribe on break', limits, async t=>{
  const {queue:q}=await fixture(t);const stream=await q.changes(['items']);const ready=deferred();
  const consumer=(async()=>{for await(const event of stream){ready.resolve(event);break;}})();
  await q.transaction(tx=>tx.execute("INSERT INTO items VALUES(1,'stream')"));
  assert.deepEqual((await ready.promise).tables,['items']);await consumer;
  assert.equal(stream.closed,true);assert.equal(q.stats.subscriptions,0);
  assert.deepEqual(await stream.next(),{done:true,value:undefined});
  assert.deepEqual((await q.transaction(tx=>tx.query("SELECT name FROM temp.sqlite_master WHERE type='trigger'"))).rowArrays,[]);
});

test('a slow change-stream consumer retains one union/range rather than one event per commit', limits, async t=>{
  const {queue:q}=await fixture(t);const stream=await q.changes(['items','audit']);
  for(let i=1;i<=30;i++){
    await q.transaction(tx=>tx.execute('INSERT INTO items VALUES(?,?)',[i,`v${i}`]));
    await delay(1); // Force separate callback deliveries into the same iterator slot.
  }
  await q.transaction(tx=>tx.execute('INSERT INTO audit VALUES(1)'));await delay(5);
  const {value,done}=await stream.next();assert.equal(done,false);
  assert.deepEqual(value,{tables:['audit','items'],firstSequence:1n,lastSequence:31n,commits:31n});
  assert.equal(q.stats.pendingNotifications,0);assert.equal(q.stats.pendingJobs,0);
  await stream.return();
});

test('change-stream next admission is bounded and return settles an outstanding next', limits, async t=>{
  const {queue:q}=await fixture(t);const stream=await q.changes(['items']);
  const pending=stream.next();
  for(let i=0;i<20;i++)await assert.rejects(stream.next(),code('ERR_FSQLITE_SUBSCRIPTION_NEXT_PENDING'));
  await stream.return();assert.deepEqual(await pending,{value:undefined,done:true});
  assert.equal(q.stats.subscriptions,0);assert.equal(stream.closed,true);
});

test('iterator throw retains the exact error and releases the subscription', limits, async t=>{
  const {queue:q}=await fixture(t);const stream=await q.changes(['items']);
  const pending=observe(stream.next()),error=Error('consumer failed');
  await assert.rejects(stream.throw(error),e=>e===error);await pending.settled;
  assert.equal(pending.outcome.reason,error);assert.equal(stream.closed,true);
  await assert.rejects(stream.next(),e=>e===error);assert.equal(q.stats.subscriptions,0);
  await q.transaction(tx=>tx.execute("INSERT INTO items VALUES(1,'independent')"));
});

test('lifetime abort or queue close terminates a stream even without any writes', limits, async t=>{
  const {queue:q}=await fixture(t);const c=new AbortController();
  const first=await q.changes(['items'],{signal:c.signal}),second=await q.changes(['audit']);
  const waiting=first.next(),other=second.next();c.abort('unmount');
  assert.deepEqual(await waiting,{done:true,value:undefined});
  await q.close();assert.deepEqual(await other,{done:true,value:undefined});
  assert.equal(first.closed,true);assert.equal(second.closed,true);
});

async function faultFixture(t,hooks={}) {
  const f=sqliteSnapshotWorker(hooks),errors=new Set();
  const add=f.worker.addEventListener.bind(f.worker),remove=f.worker.removeEventListener.bind(f.worker);
  f.worker.addEventListener=(type,listener)=>{if(type==='error')errors.add(listener);add(type,listener);};
  f.worker.removeEventListener=(type,listener)=>{if(type==='error')errors.delete(listener);remove(type,listener);};
  const q=await FrankenDBQueue.open({worker:f.worker});
  t.after(async()=>{await q.close().catch(()=>{});for(const core of f.handles)core.close();});
  await q.transaction(tx=>tx.execute('CREATE TABLE items(id INTEGER PRIMARY KEY,value TEXT)'));
  return {...f,queue:q,crash:()=>{for(const listener of [...errors])listener({message:'fixture worker failure'});},errors};
}

test('idle transport failure rejects pending iteration and callback done without another SQL request', limits, async t=>{
  const {queue:q,crash,errors}=await faultFixture(t);const stream=await q.changes(['items']);
  const sub=await q.subscribe(['items'],()=>{}),waiting=observe(stream.next());
  crash();await waiting.settled;
  assert.equal(waiting.outcome.status,'rejected');assert.match(waiting.outcome.reason.message,/fixture worker failure/);
  await assert.rejects(sub.done,e=>e===waiting.outcome.reason);
  await q.close().catch(()=>{});assert.equal(q.stats.state,'closed');assert.equal(q.stats.reservedSubscriptions,0);
  assert.equal(errors.size,0);assert.equal(stream.closed,true);
  await assert.rejects(q.transaction(()=>{}),e=>e===waiting.outcome.reason);
});

test('callback failure coinciding with a connection fault retains both errors', limits, async t=>{
  const {queue:q,crash}=await faultFixture(t);const entered=deferred(),release=deferred(),error=Error('callback failure');
  const sub=await q.subscribe(['items'],async()=>{entered.resolve();await release.promise;throw error;});
  await q.transaction(tx=>tx.execute("INSERT INTO items VALUES(1,'kept')"));await entered.promise;
  crash();release.resolve();
  await assert.rejects(sub.done,e=>e instanceof AggregateError&&e.errors.includes(error)&&/fixture worker failure/.test(e.cause.message));
  assert.equal(q.stats.reservedSubscriptions,0);
});

test('failed SQL rollback invalidates idle streams even when no transport error event occurs', limits, async t=>{
  let armed=false;
  const {queue:q}=await faultFixture(t,{beforeBatch:sql=>{if(armed&&sql.startsWith('ROLLBACK'))throw Error('rollback fault');}});
  const stream=await q.changes(['items']),next=observe(stream.next());armed=true;
  await assert.rejects(q.transaction(async tx=>{await tx.execute("INSERT INTO items VALUES(1,'lost')");throw Error('callback');}));
  await next.settled;assert.equal(next.outcome.status,'rejected');assert.equal(stream.closed,true);
  assert.equal(q.stats.subscriptions,0);armed=false;
});

test('a ready-then-crashed worker cannot escape queue opening as a usable queue', limits, async()=>{
  const {ControlledWorker}=await import('./helpers/controlled-worker.ts');const worker=new ControlledWorker();
  worker.onPost=request=>{
    if(request.kind==='init')queueMicrotask(()=>{
      worker.reply({kind:'ready',requestId:request.requestId,data:{path:':memory:',persistence:'memory'}});
      worker.crash('crash immediately after ready');
    });
  };
  await assert.rejects(FrankenDBQueue.open({worker}),/crash immediately after ready/);
  assert.equal(worker.terminateCount,1);assert.equal(worker.messages.size,0);assert.equal(worker.errors.size,0);
});

test('deferred constraint failure at final COMMIT emits no invalidation', limits, async t=>{
  const {queue:q,handles}=await fixture(t);const core=handles[0],batch=core.executeBatch.bind(core);let initialize=true;
  core.executeBatch=async sql=>{if(initialize&&sql==='BEGIN'){initialize=false;await core.execute('PRAGMA foreign_keys=ON');}await batch(sql);};
  await q.transaction(tx=>tx.execute('CREATE TABLE child(id REFERENCES items(id) DEFERRABLE INITIALLY DEFERRED)'));
  const stream=await q.changes(['items','child']);const next=observe(stream.next());
  await assert.rejects(q.transaction(tx=>tx.execute('INSERT INTO child VALUES(123)')));
  await delay(5);assert.equal(next.outcome.status,'pending');assert.equal(q.changeSequence,0n);
  await q.transaction(async tx=>{await tx.execute("INSERT INTO items VALUES(123,'parent')");await tx.execute('INSERT INTO child VALUES(123)');});
  await next.settled;assert.deepEqual(next.outcome.value.value.tables,['child','items']);
  await stream.return();
});

test('worker-reported unusable checkpoint state rejects idle iteration without a transport crash', limits, async t=>{
  const {installIndexedDbModel}=await import('../../worker/tests/helpers/indexeddb-model.mjs');installIndexedDbModel();
  let armed=false;
  const f=sqliteSnapshotWorker({beforeBatch:sql=>{if(armed&&sql==='ROLLBACK')throw Error('checkpoint rollback failed');}});
  const q=await FrankenDBQueue.open({worker:f.worker,persistence:'indexeddb-snapshot',dbName:crypto.randomUUID()});
  t.after(()=>q.close().catch(()=>{}));
  await q.transaction(tx=>tx.execute('CREATE TABLE items(id)'));
  const stream=await q.changes(['items']),waiting=observe(stream.next());armed=true;
  let failure;await assert.rejects(q.checkpoint(),error=>{failure=error;return error.code==='ERR_FSQLITE_SNAPSHOT_CONNECTION_UNUSABLE';});
  await delay(5);assert.equal(waiting.outcome.status,'rejected');assert.equal(waiting.outcome.reason,failure);
  assert.equal(stream.closed,true);assert.equal(q.stats.subscriptions,0);
});

test('a close already admitted behind a fatal host result keeps its real acknowledgement', limits, async t=>{
  const {installIndexedDbModel}=await import('../../worker/tests/helpers/indexeddb-model.mjs');installIndexedDbModel();
  const {FrankenDB}=await import('../src/database.ts');let armed=false;
  const entered=deferred(),release=deferred();
  const f=sqliteSnapshotWorker({beforeBatch:async sql=>{if(armed&&sql==='ROLLBACK'){entered.resolve();await release.promise;throw Error('failed probe');}}});
  const db=await FrankenDB.open({worker:f.worker,persistence:'indexeddb-snapshot',dbName:crypto.randomUUID()});
  t.after(()=>db.close().catch(()=>{}));await db.execute('CREATE TABLE items(id)');armed=true;
  const checkpoint=observe(db.checkpoint());await entered.promise;
  const close=observe(db.close());release.resolve();await Promise.all([checkpoint.settled,close.settled]);
  assert.equal(checkpoint.outcome.status,'rejected');assert.equal(close.outcome.status,'fulfilled');
  assert.equal(f.worker.terminateCount,1);assert.equal(db.requestQueue.pendingRequests,0);
  assert.equal(f.worker.requests.filter(request=>request.kind==='close').length,1);
});
