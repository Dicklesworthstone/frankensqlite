// Actual Node threads running production worker.ts; only the WASM core is
// replaced with Node SQLite. The event bridge is not browser crash evidence.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { Worker } from 'node:worker_threads';
import { DatabaseSync } from 'node:sqlite';
import { FrankenDBQueue } from '../src/queue.ts';
import { observe, drain } from './helpers/controlled-worker.ts';
const limits={timeout:20000};

async function threaded(t) {
  const helper=new URL('../../worker/tests/helpers/result-worker.mjs',import.meta.url).href;
  const script=`
    import {parentPort} from 'node:worker_threads';
    await import(${JSON.stringify(helper)});
    let config={},resume;
    const pause=()=>new Promise(resolve=>{resume=resolve;parentPort.postMessage({fixture:'paused'});});
    const constructor=globalThis.__resultReferenceCore;
    for(const method of ['create','import']) {
      const original=constructor[method];
      constructor[method]=async(...args)=>{
        const core=await original(...args),batch=core.executeBatch.bind(core),execute=core.executeWithParams.bind(core);
        core.executeBatch=async sql=>{if(config.commit&&sql==='COMMIT')await pause();return batch(sql);};
        core.executeWithParams=async(sql,values)=>{
          if(config.row!==undefined&&sql.startsWith('INSERT INTO items')&&values[0]===config.row)await pause();
          return execute(sql,values);
        };
        return core;
      };
    }
    parentPort.on('message',message=>{
      if(message.fixture==='configure'){config=message.config;parentPort.postMessage({fixture:'configured'});}
      if(message.fixture==='resume'){config={};resume();}
      if(message.fixture==='crash')throw Error('intentional subscription worker crash');
    });
    parentPort.postMessage({fixture:'ready'});
  `;
  const native=new Worker(new URL('data:text/javascript,'+encodeURIComponent(script)),{execArgv:process.execArgv});
  const history=[],waiters=[],wrappers=new Map();
  native.on('message',message=>{
    history.push(message);
    for(const waiter of [...waiters])if(waiter.matches(message)){
      waiters.splice(waiters.indexOf(waiter),1);waiter.resolve(message);
    }
  });
  native.on('error',error=>{for(const waiter of waiters.splice(0))waiter.reject(error);});
  const until=matches=>{
    const found=history.find(matches);if(found)return Promise.resolve(found);
    return new Promise((resolve,reject)=>waiters.push({matches,resolve,reject}));
  };
  const worker={
    addEventListener(type,listener){
      const wrapped=type==='message'?data=>{if(data.kind&&!data.audit)listener({data});}:error=>listener({message:error.message});
      wrappers.set(listener,wrapped);native.on(type,wrapped);
    },
    removeEventListener(type,listener){const wrapped=wrappers.get(listener);if(wrapped)native.off(type,wrapped);wrappers.delete(listener);},
    postMessage(request,transfer){native.postMessage(request,transfer);},
    terminate(){void native.terminate();},
  };
  t.after(()=>native.terminate());
  await until(message=>message.fixture==='ready');
  const queue=await FrankenDBQueue.open({worker,resultEncoding:'binary',requestLimits:{maxPendingRequests:1}},{maxPendingJobs:32});
  t.after(()=>queue.close().catch(()=>{}));
  await queue.transaction(tx=>tx.executeBatch('CREATE TABLE items(id INTEGER PRIMARY KEY,value TEXT);CREATE TABLE audit(id);CREATE TRIGGER log AFTER INSERT ON items BEGIN INSERT INTO audit VALUES(new.id);END;'));
  return {queue,native,history,wrappers,
    async configure(config){history.length=0;native.postMessage({fixture:'configure',config});await until(m=>m.fixture==='configured');},
    paused:()=>until(m=>m.fixture==='paused'),
    cancelAck:()=>until(m=>m.kind==='cancel-transaction-result'&&!m.audit),
    resume:()=>native.postMessage({fixture:'resume'}),
    crash:()=>native.postMessage({fixture:'crash'}),
  };
}
function onDisk(q,expected) {
  const db=new DatabaseSync(q.path);
  try {
    assert.deepEqual(db.prepare('SELECT id,value FROM items ORDER BY id').all().map(Object.values),expected);
    assert.deepEqual(db.prepare('SELECT id FROM audit ORDER BY id').all().map(Object.values),expected.map(row=>[row[0]]));
    assert.deepEqual(db.prepare('PRAGMA integrity_check').all().map(Object.values),[['ok']]);
    assert.deepEqual(db.prepare("SELECT name FROM sqlite_master WHERE name LIKE '__fsqlite_watch_%'").all(),[]);
  } finally {db.close();}
}

test('real production worker: concurrent writers deliver commit ranges and trigger invalidations through binary result transport',limits,async t=>{
  const f=await threaded(t),q=f.queue,stream=await q.changes(['items','audit']);
  const notices=[];let commits=0n;
  const consumer=(async()=>{for await(const event of stream){notices.push(event);commits+=event.commits;if(commits===20n)break;}})();
  await Promise.all(Array.from({length:20},(_,i)=>q.transaction(async tx=>{
    await tx.execute('INSERT INTO items VALUES(?,?)',[i,`row-${i}`]);
    const statement=await tx.prepare('SELECT value FROM items WHERE id=?');
    assert.equal((await statement.get([i])).value,`row-${i}`);
  })));
  await consumer;assert.equal(commits,20n);assert.equal(notices[0].firstSequence,1n);
  assert.equal(notices.at(-1).lastSequence,20n);assert.ok(notices.every(e=>e.tables.join(',')==='audit,items'));
  assert.ok(f.history.some(m=>m.audit&&m.kind==='query-binary-result'&&m.before[0]>0&&m.after[0]===0));
  await q.close();onDisk(q,Array.from({length:20},(_,i)=>[i,`row-${i}`]));
});

test('real production worker: cancelling paused SQL rolls back earlier rows and trigger bits before the next notice',limits,async t=>{
  const f=await threaded(t),q=f.queue,c=new AbortController(),stream=await q.changes(['items','audit']);
  await f.configure({row:2});const next=observe(stream.next());
  const job=observe(q.transaction(async tx=>{
    await tx.execute('INSERT INTO items VALUES(?,?)',[1,'lost']);await tx.execute('INSERT INTO items VALUES(?,?)',[2,'paused']);
  },{signal:c.signal}));
  await f.paused();c.abort('cancel');assert.equal((await f.cancelAck()).accepted,true);
  await drain();assert.equal(job.outcome.status,'pending');assert.equal(next.outcome.status,'pending');
  f.resume();await job.settled;assert.equal(job.outcome.status,'rejected');assert.equal(q.changeSequence,0n);
  await q.transaction(tx=>tx.execute('INSERT INTO items VALUES(?,?)',[100,'keep']));await next.settled;
  assert.deepEqual(next.outcome.value.value,{tables:['audit','items'],firstSequence:1n,lastSequence:1n,commits:1n});
  await stream.return();await q.close();onDisk(q,[[100,'keep']]);
});

test('real production worker: late commit cancellation does not suppress a successful notification',limits,async t=>{
  const f=await threaded(t),q=f.queue,c=new AbortController(),stream=await q.changes(['items']);
  await f.configure({commit:true});const next=observe(stream.next());
  const job=q.transaction(async tx=>{await tx.execute("INSERT INTO items VALUES(1,'committed')");return 123;},{signal:c.signal});
  await f.paused();c.abort('too late');await drain();assert.equal(next.outcome.status,'pending');
  f.resume();assert.equal(await job,123);await next.settled;assert.equal(next.outcome.value.value.commits,1n);
  await stream.return();await q.close();onDisk(q,[[1,'committed']]);
});

test('real production worker: idle crash rejects pending stream and subscription without a follow-up query',limits,async t=>{
  const f=await threaded(t),q=f.queue,stream=await q.changes(['items']),sub=await q.subscribe(['audit'],()=>{});
  const next=observe(stream.next()),done=observe(sub.done),exited=new Promise(resolve=>f.native.once('exit',resolve));
  f.crash();await Promise.all([next.settled,done.settled,exited]);
  assert.equal(next.outcome.status,'rejected');assert.match(next.outcome.reason.message,/intentional subscription worker crash/);
  assert.equal(done.outcome.reason,next.outcome.reason);assert.equal(stream.closed,true);
  await q.close().catch(()=>{});assert.equal(q.stats.state,'closed');assert.equal(f.wrappers.size,0);onDisk(q,[]);
});

test('real production worker: crash in a write rejects later jobs, emits no commit, and reopens cleanly',limits,async t=>{
  const f=await threaded(t),q=f.queue,stream=await q.changes(['items']);await f.configure({row:2});
  const next=observe(stream.next());let laterRan=false;
  const job=observe(q.transaction(async tx=>{
    await tx.execute('INSERT INTO items VALUES(?,?)',[1,'uncommitted']);await tx.execute('INSERT INTO items VALUES(?,?)',[2,'paused']);
  }));
  const later=observe(q.transaction(()=>{laterRan=true;}));await f.paused();
  const exited=new Promise(resolve=>f.native.once('exit',resolve));f.crash();
  await Promise.all([next.settled,job.settled,later.settled,exited]);
  assert.equal(next.outcome.status,'rejected');assert.equal(job.outcome.status,'rejected');assert.equal(later.outcome.status,'rejected');
  assert.equal(laterRan,false);assert.equal(q.changeSequence,0n);await q.close().catch(()=>{});
  assert.equal(q.stats.pendingJobs,0);assert.equal(q.stats.subscriptions,0);onDisk(q,[]);
});
