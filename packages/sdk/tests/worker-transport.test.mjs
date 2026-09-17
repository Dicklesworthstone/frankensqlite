// Production worker.ts in real Node threads, with a native SQLite reference.
// messageerror injection models the browser event, not actual browser decoding.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { Worker } from 'node:worker_threads';
import { DatabaseSync } from 'node:sqlite';
import { FrankenDB, FrankenDBQueue, FrankenSnapshotPool, scanTable, watchQuery } from '../src/index.ts';
import { observe, drain } from './helpers/controlled-worker.ts';
const limits={timeout:15000};
const code=name=>error=>error?.code===name||error?.cause&&code(name)(error.cause)||error?.errors?.some(code(name));

async function thread(t) {
  const helper=new URL('../../worker/tests/helpers/result-worker.mjs',import.meta.url).href;
  const script=`
    import {parentPort} from 'node:worker_threads';
    await import(${JSON.stringify(helper)});
    let config={},resume;
    const send=globalThis.postMessage;
    globalThis.postMessage=(response,transfer)=>{
      if(config.sendFailure==='all' || config.sendFailure==='responses' &&
          response.kind!=='worker-fatal' && response.kind!=='close-result') throw Error('injected send failure');
      return send(response,transfer);
    };
    const constructor=globalThis.__resultReferenceCore;
    for(const method of ['create','import']){
      const original=constructor[method];
      constructor[method]=async(...args)=>{
        const core=await original(...args);
        if(config.pauseInit)await new Promise(resolve=>{resume=resolve;parentPort.postMessage({fixture:'paused'});});
        for(const operation of ['execute','executeWithParams','executeBatch','query','queryWithParams']){
          const invoke=core[operation].bind(core);
          core[operation]=async(sql,...params)=>{
            parentPort.postMessage({fixture:'sql',sql});
            if(config.pause===sql)await new Promise(resolve=>{resume=resolve;parentPort.postMessage({fixture:'paused'});});
            if(config.badError===sql){const bad={message:'unserializable error',get code(){throw bad;}};throw bad;}
            const result=await invoke(sql,...params);
            if(config.uncloneable===sql) result.extension=()=>{}; // Actual structured-clone failure.
            return result;
          };
        }
        const close=core.close.bind(core);
        core.close=()=>{close();parentPort.postMessage({fixture:'closed'});};
        return core;
      };
    }
    parentPort.on('message',message=>{
      if(message?.fixture==='configure'){config=message.config;parentPort.postMessage({fixture:'configured'});}
      if(message?.fixture==='resume'){config=message.config??{};resume?.();}
      if(message?.fixture==='decode-error')parentPort.emit('messageerror',new Error('injected receiver decode failure'));
    });
    parentPort.postMessage({fixture:'ready'});
  `;
  const native=new Worker(new URL('data:text/javascript,'+encodeURIComponent(script)),{execArgv:process.execArgv});
  t.after(()=>native.terminate());
  const messages=[],waiters=[],wrappers=new Map();let terminated=0;
  const received=message=>{
    messages.push(message);
    for(const waiter of [...waiters])if(waiter.predicate(message)){
      waiters.splice(waiters.indexOf(waiter),1);waiter.resolve(message);
    }
  };
  native.on('message',received);
  native.on('error',error=>received({fixture:'error',message:error.message}));
  native.on('exit',exit=>received({fixture:'exit',exit}));
  const until=predicate=>{
    const old=messages.find(predicate);if(old)return Promise.resolve(old);
    return new Promise(resolve=>waiters.push({predicate,resolve}));
  };
  const worker={
    addEventListener(type,listener){
      const wrapped=type==='message'?data=>{if(data?.kind&&!data.audit)listener({data});}:
        type==='messageerror'?()=>listener():error=>listener({message:error.message});
      wrappers.set(listener,wrapped);native.on(type,wrapped);
    },
    removeEventListener(type,listener){const wrapped=wrappers.get(listener);if(wrapped)native.off(type,wrapped);wrappers.delete(listener);},
    postMessage(request,transfer){native.postMessage(request,transfer);},
    terminate(){terminated++;void native.terminate();},
  };
  await until(m=>m.fixture==='ready');
  return {worker,native,messages,wrappers,until,get terminated(){return terminated;},
    async configure(config){messages.length=0;native.postMessage({fixture:'configure',config});await until(m=>m.fixture==='configured');},
    paused:()=>until(m=>m.fixture==='paused'),
    resume:(config={})=>native.postMessage({fixture:'resume',config}),
    decode:()=>native.postMessage({fixture:'decode-error'}),
    messageError:()=>native.emit('messageerror',new Error('injected owner decode failure')),
  };
}
function disk(path,expected){
  const db=new DatabaseSync(path);
  try{
    assert.deepEqual(db.prepare('SELECT id FROM items ORDER BY id').all().map(Object.values),expected.map(id=>[id]));
    assert.deepEqual(db.prepare('PRAGMA integrity_check').all().map(Object.values),[['ok']]);
  }finally{db.close();}
}

test('worker transport: actual clone failure after autocommit RETURNING reports uncertainty without rerunning SQL',limits,async t=>{
  const f=await thread(t),db=await FrankenDB.open({worker:f.worker});
  await db.execute('CREATE TABLE items(id INTEGER PRIMARY KEY)');
  const sql='INSERT INTO items VALUES(1) RETURNING id';await f.configure({uncloneable:sql});
  await assert.rejects(db.query(sql),error=>code('ERR_FSQLITE_RESPONSE_TRANSFER')(error)&&error.transient===false);
  assert.equal(f.messages.filter(m=>m.fixture==='sql'&&m.sql===sql).length,1);
  assert.deepEqual((await db.query('SELECT id FROM items')).rowArrays,[[1n]]);
  await db.close();disk(db.path,[1]);assert.equal(f.terminated,1);
});

test('worker transport: actual clone failure inside a managed transaction rolls back and releases the connection',limits,async t=>{
  const f=await thread(t),db=await FrankenDB.open({worker:f.worker});
  await db.execute('CREATE TABLE items(id INTEGER PRIMARY KEY)');
  const sql='INSERT INTO items VALUES(1) RETURNING id';await f.configure({uncloneable:sql});
  await assert.rejects(db.transaction(tx=>tx.query(sql)),code('ERR_FSQLITE_RESPONSE_TRANSFER'));
  assert.equal(f.messages.filter(m=>m.fixture==='sql'&&m.sql===sql).length,1);
  await db.execute('INSERT INTO items VALUES(2)');await db.close();disk(db.path,[2]);
});

test('worker transport: even an error serializer that repeatedly throws gets a correlated primitive failure',limits,async t=>{
  const f=await thread(t),db=await FrankenDB.open({worker:f.worker});
  await db.execute('CREATE TABLE items(id INTEGER PRIMARY KEY)');await f.configure({badError:'SELECT 1'});
  await assert.rejects(db.query('SELECT 1'),code('ERR_FSQLITE_WORKER_DISPATCH'));
  assert.deepEqual((await db.query('SELECT 2 AS n')).rowArrays,[[2n]]);await db.close();disk(db.path,[]);
});

test('worker transport: response and fallback failure produce one global fatal notice, then real acknowledged close',limits,async t=>{
  const f=await thread(t),db=await FrankenDB.open({worker:f.worker});
  await db.execute('CREATE TABLE items(id INTEGER PRIMARY KEY)');await f.configure({sendFailure:'responses'});
  await assert.rejects(db.execute('INSERT INTO items VALUES(1)'),code('ERR_FSQLITE_WORKER_TRANSPORT'));
  await assert.rejects(db.execute('INSERT INTO items VALUES(2)'),code('ERR_FSQLITE_WORKER_TRANSPORT'));
  await db.close();
  assert.equal(f.messages.filter(m=>m.kind==='worker-fatal'&&!m.audit).length,1);
  assert.equal(f.messages.filter(m=>m.fixture==='sql'&&m.sql==='INSERT INTO items VALUES(2)').length,0);
  assert.ok(f.messages.some(m=>m.fixture==='closed'));disk(db.path,[1]);
});

test('worker transport: when even fatal delivery fails a real worker error event settles the original request',limits,async t=>{
  const f=await thread(t),db=await FrankenDB.open({worker:f.worker});
  await db.execute('CREATE TABLE items(id INTEGER PRIMARY KEY)');await f.configure({sendFailure:'all'});
  const write=observe(db.execute('INSERT INTO items VALUES(1)'));
  const fault=await f.until(m=>m.fixture==='error');await write.settled;
  assert.match(fault.message,/outcomes are unknown/);assert.equal(write.outcome.status,'rejected');
  assert.equal(f.messages.filter(m=>m.fixture==='sql'&&m.sql==='INSERT INTO items VALUES(1)').length,1);
  await db.close().catch(()=>{});assert.equal(f.terminated,1);disk(db.path,[1]);
});

test('worker transport: receiver decode fault fences queued writes and close waits for active SQL',limits,async t=>{
  const f=await thread(t),db=await FrankenDB.open({worker:f.worker});
  await db.execute('CREATE TABLE items(id INTEGER PRIMARY KEY)');const sql='INSERT INTO items VALUES(1)';await f.configure({pause:sql});
  const active=observe(db.execute(sql));await f.paused();
  const queued=observe(db.execute('INSERT INTO items VALUES(2)'));f.decode();
  await f.until(m=>m.kind==='worker-fatal'&&!m.audit);
  const close=observe(db.close());await drain();assert.equal(close.outcome.status,'pending');
  assert.equal(f.messages.some(m=>m.fixture==='closed'),false);
  assert.equal(active.outcome.status,'pending');assert.equal(queued.outcome.status,'pending');
  f.resume();await Promise.all([close.settled,active.settled,queued.settled]);
  assert.equal(close.outcome.status,'fulfilled');assert.equal(active.outcome.status,'rejected');assert.equal(queued.outcome.status,'rejected');
  assert.equal(f.messages.some(m=>m.fixture==='sql'&&m.sql==='INSERT INTO items VALUES(2)'),false);
  disk(db.path,[1]); // Active autocommit finished; the failure never claimed rollback.
});

test('worker transport: malformed request without an id stops acceptance but preserves later close acknowledgement',limits,async t=>{
  const f=await thread(t),db=await FrankenDB.open({worker:f.worker});
  await db.execute('CREATE TABLE items(id INTEGER PRIMARY KEY)');f.native.postMessage(null);
  await f.until(m=>m.kind==='worker-fatal'&&!m.audit);
  await assert.rejects(db.query('SELECT 1'),code('ERR_FSQLITE_WORKER_TRANSPORT'));
  await db.close();assert.ok(f.messages.some(m=>m.fixture==='closed'));disk(db.path,[]);
});

test('worker transport: lost close reply after a fatal notice surfaces an error instead of waiting forever',limits,async t=>{
  const f=await thread(t),db=await FrankenDB.open({worker:f.worker});
  await db.execute('CREATE TABLE items(id INTEGER PRIMARY KEY)');await f.configure({pause:'SELECT 1'});
  const call=observe(db.query('SELECT 1'));await f.paused();f.decode();await f.until(m=>m.kind==='worker-fatal'&&!m.audit);
  const closed=observe(db.close());
  // Let the operation drain but fail its later close/fallback acknowledgements.
  f.resume({sendFailure:'all'});
  const fault=await f.until(m=>m.fixture==='error');await Promise.all([closed.settled,call.settled]);
  assert.match(fault.message,/outcomes are unknown/);assert.equal(closed.outcome.status,'rejected');
  assert.equal(f.terminated,1);disk(db.path,[]);
});

test('worker transport: idle subscriptions and live query demands fail on owner messageerror without another SQL call',limits,async t=>{
  const f=await thread(t),q=await FrankenDBQueue.open({worker:f.worker});
  await q.transaction(tx=>tx.execute('CREATE TABLE items(id INTEGER PRIMARY KEY)'));
  const changes=await q.changes(['items']),live=await watchQuery(q,'SELECT * FROM items',{tables:['items']});
  await live.next();const event=observe(changes.next()),read=observe(live.next()),done=observe(live.done);
  f.messageError();await Promise.all([event.settled,read.settled,done.settled]);
  assert.equal(event.outcome.status,'rejected');assert.equal(read.outcome.status,'rejected');assert.equal(done.outcome.status,'rejected');
  assert.ok(code('ERR_FSQLITE_WORKER_TRANSPORT')(done.outcome.reason));await q.close().catch(()=>{});
  assert.equal(f.wrappers.size,0);disk(q.path,[]);
});

test('worker transport: idle table scan releases snapshot and rejects prefix completion on a receive failure',limits,async t=>{
  const f=await thread(t),q=await FrankenDBQueue.open({worker:f.worker});
  await q.transaction(tx=>tx.executeBatch('CREATE TABLE items(id INTEGER PRIMARY KEY);INSERT INTO items VALUES(1),(2);'));
  const scan=scanTable(q,'items',{batchSize:1});assert.equal((await scan.next()).value.id,1n);
  f.messageError();await assert.rejects(scan.done,code('ERR_FSQLITE_WORKER_TRANSPORT'));
  await assert.rejects(scan.next());await q.close().catch(()=>{});assert.equal(f.wrappers.size,0);disk(q.path,[1,2]);
});

test('worker transport: snapshot pool observes idle client failure, closes all replicas, and never serves a replacement',limits,async t=>{
  const writer=await thread(t),db=await FrankenDB.open({worker:writer.worker});
  await db.executeBatch('CREATE TABLE items(id INTEGER PRIMARY KEY);INSERT INTO items VALUES(1);');const image=await db.export();await db.close();
  const replicas=[await thread(t),await thread(t)];let creations=0;
  const pool=await FrankenSnapshotPool.open(image,{workers:2,worker:()=>replicas[creations++].worker});
  replicas[0].messageError();await assert.rejects(pool.query('SELECT * FROM items'),code('ERR_FSQLITE_POOL_UNUSABLE'));
  await pool.close().catch(()=>{});assert.equal(creations,2);assert.equal(pool.stats.state,'closed');
  assert.equal(replicas[0].wrappers.size,0);assert.equal(replicas[1].wrappers.size,0);assert.equal(replicas[1].terminated,1);
});

test('worker transport: fatal during managed SQL drains the host before transaction failure can dispose it',limits,async t=>{
  const f=await thread(t),q=await FrankenDBQueue.open({worker:f.worker});
  await q.transaction(tx=>tx.execute('CREATE TABLE items(id INTEGER PRIMARY KEY)'));
  const sql='INSERT INTO items VALUES(1)';await f.configure({pause:sql});
  const job=observe(q.transaction(tx=>tx.execute(sql)));await f.paused();let successorRan=false;
  const successor=observe(q.transaction(()=>{successorRan=true;}));f.decode();
  await f.until(m=>m.kind==='worker-fatal'&&!m.audit);await drain();
  assert.equal(job.outcome.status,'pending');assert.equal(f.terminated,0);
  f.resume();await Promise.all([job.settled,successor.settled]);await q.close().catch(()=>{});
  assert.ok(code('ERR_FSQLITE_WORKER_TRANSPORT')(job.outcome.reason));assert.equal(successorRan,false);
  assert.equal(successor.outcome.status,'rejected');assert.ok(f.messages.some(m=>m.fixture==='closed'));disk(q.path,[]);
});

test('worker transport: fatal during COMMIT returns uncertainty after drain, while the real committed write survives',limits,async t=>{
  const f=await thread(t),db=await FrankenDB.open({worker:f.worker});
  await db.execute('CREATE TABLE items(id INTEGER PRIMARY KEY)');await f.configure({pause:'COMMIT'});
  const job=observe(db.transaction(tx=>tx.execute('INSERT INTO items VALUES(1)')));await f.paused();f.decode();
  await f.until(m=>m.kind==='worker-fatal'&&!m.audit);await drain();
  assert.equal(job.outcome.status,'pending');assert.equal(f.terminated,0);f.resume();await job.settled;
  assert.ok(code('ERR_FSQLITE_WORKER_TRANSPORT')(job.outcome.reason));
  assert.equal(f.messages.filter(m=>m.fixture==='sql'&&m.sql==='INSERT INTO items VALUES(1)').length,1);
  assert.equal(f.messages.filter(m=>m.fixture==='sql'&&m.sql==='COMMIT').length,1);
  await db.close().catch(()=>{});disk(db.path,[1]);
});

test('worker transport: failed initialization joins an outstanding import/create before destroying its wrapper',limits,async t=>{
  const f=await thread(t);await f.configure({pauseInit:true});
  const opening=observe(FrankenDB.open({worker:f.worker}));await f.paused();f.decode();
  await f.until(m=>m.kind==='worker-fatal'&&!m.audit);await drain();
  assert.equal(opening.outcome.status,'pending');assert.equal(f.terminated,0);
  f.resume();await opening.settled;assert.ok(code('ERR_FSQLITE_WORKER_TRANSPORT')(opening.outcome.reason));
  assert.ok(f.messages.some(m=>m.fixture==='closed'));assert.equal(f.terminated,1);assert.equal(f.wrappers.size,0);
});
