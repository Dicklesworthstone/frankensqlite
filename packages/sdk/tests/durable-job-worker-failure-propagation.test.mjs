import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtempSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { DurableJobQueue, DURABLE_JOBS_TABLE } from '../src/durable-jobs.ts';
import { DurableJobWorker } from '../src/durable-job-worker.ts';
import { JobSqliteTarget, gate, tick } from './helpers/durable-jobs-sqlite-target.mjs';
const TABLE=`main."${DURABLE_JOBS_TABLE}"`;
const clock=()=>100;
const options={owner:'supervisor',clock,stopWhenIdle:true,cancelBlockedJobs:true,reapLimit:2};
const node=(id,parents=[],extra={})=>({queue:'work',id,payload:id,
  dependsOn:parents.map(id=>({queue:'work',id})),...extra});
// Serialize one reference connection's transactions, not production jobs or
// independent database owners. Production queue/worker methods are not replaced.
function owner(db) {
  let tail=Promise.resolve();
  return {transaction(work){const task=tail.then(()=>db.transaction(work));tail=task.catch(()=>{});return task;}};
}
async function fixture(encoding='UTF-8') {
  const db=new JobSqliteTarget(':memory:',`PRAGMA encoding='${encoding}';`);
  const database=owner(db),queue=await DurableJobQueue.open(database,'work',{clock});
  return {db,database,queue};
}
function boundary(promise) {
  let timer;return Promise.race([promise,new Promise((_,reject)=>{timer=setTimeout(()=>reject(new Error('required boundary not reached')),4000);})])
    .finally(()=>clearTimeout(timer));
}
function adapter(queue,extra={}) {
  return {...Object.fromEntries(['claim','renew','complete','completeWith','completeAndEnqueue','fail','reapExpired','cancelBlocked']
    .map(name=>[name,queue[name].bind(queue)])),...extra};
}
async function failedChain(queue,count=7) {
  await queue.enqueueBatch(Array.from({length:count+1},(_,i)=>node(`n${count-i}`,i?[`n${count-i+1}`]:[])));
  await queue.cancel(`n${count}`);
}

for(const encoding of ['UTF-8','UTF-16le','UTF-16be']) test(`${encoding}: supervisor drains failed chains across bounded sweeps`,async()=>{
  const {db,queue}=await fixture(encoding);let worker;
  try {
    await failedChain(queue,7);let calls=0;const limits=[];
    worker=DurableJobWorker.start(adapter(queue,{cancelBlocked:async limit=>{limits.push(limit);return queue.cancelBlocked(limit);}}),()=>{calls++;},options);
    await worker.done;
    assert.equal(calls,0);assert.equal(worker.stats.claimed,0);assert.equal(worker.stats.blockedCancellations,7);
    assert.equal((await queue.stats()).cancelled,8);assert(limits.length>=4&&limits.every(n=>n===2));
    assert.equal(db.rows(`SELECT sum(attempts) AS n FROM ${TABLE}`)[0].n,0);
  } finally {await worker?.stop({abort:true}).catch(()=>{});db.close();}
});

test('default policy preserves blocked jobs and never calls cleanup',async()=>{
  const {db,queue}=await fixture();let worker;
  try {
    await failedChain(queue);let calls=0;
    worker=DurableJobWorker.start(adapter(queue,{cancelBlocked:async()=>{calls++;throw new Error('unexpected');}}),()=>{},
      {owner:'legacy',clock,stopWhenIdle:true});
    await worker.done;assert.equal(calls,0);assert.equal(worker.stats.blockedCancellations,0);
    assert.equal((await queue.stats()).ready,7);
  }finally{await worker?.stop({abort:true}).catch(()=>{});db.close();}
});

test('unsupported legacy adapters are accepted by default and reject opt-in before any SQL',async()=>{
  const {db,queue}=await fixture();let worker;
  try {
    const legacy=adapter(queue);delete legacy.cancelBlocked;const before=db.serial;
    assert.throws(()=>DurableJobWorker.start(legacy,()=>{},options),/cancelBlocked/);
    assert.equal(db.serial,before);
    worker=DurableJobWorker.start(legacy,()=>{},{owner:'legacy',clock,stopWhenIdle:true});
    await worker.done;assert.equal(worker.stats.state,'stopped');
  }finally{await worker?.stop({abort:true}).catch(()=>{});db.close();}
});
for(const flag of [null,1,'yes',{}])test(`invalid supervisor policy ${String(flag)} rejects before admission`,async()=>{
  const {db,queue}=await fixture();
  try {
    const before=db.serial;assert.throws(()=>DurableJobWorker.start(queue,()=>{},{...options,cancelBlockedJobs:flag}),TypeError);
    assert.equal(db.serial,before);
  }finally{db.close();}
});

test('policy is captured once before asynchronous startup',async()=>{
  const {db,queue}=await fixture();let worker;
  try {
    await failedChain(queue);let reads=0;let enabled=true;
    const supplied={...options,get cancelBlockedJobs(){reads++;return enabled;}};
    worker=DurableJobWorker.start(queue,()=>{},supplied);enabled=false;
    await worker.done;assert.equal(reads,1);assert.equal(worker.stats.blockedCancellations,7);
  }finally{await worker?.stop({abort:true}).catch(()=>{});db.close();}
});

test('actual failed handler terminates its fan-in while healthy continuations run',async()=>{
  const {db,queue}=await fixture();let worker;
  try {
    await queue.enqueueBatch([node('failure',[],{maxAttempts:1}),node('bad-child',['failure']),
      node('healthy'),node('join',['failure','healthy'])]);
    const seen=[];
    worker=DurableJobWorker.start(queue,lease=>{
      seen.push(lease.id);if(lease.id==='failure')throw new Error('exhausted');
      return lease.id==='healthy'?{next:[node('healthy-child',['healthy'])],apply:async()=>{}}:'ok';
    },{...options,retryDelayMs:0});
    await worker.done;
    assert.deepEqual(seen,['failure','healthy','healthy-child']);
    assert.equal(worker.stats.failedJobs,1);assert.equal(worker.stats.completed,2);
    assert.equal(worker.stats.blockedCancellations,2);
    assert.equal((await queue.get('join')).attempts,0);
  }finally{await worker?.stop({abort:true}).catch(()=>{});db.close();}
});

test('retryable failure eventually releases its child without cancellation',async()=>{
  const {db,queue}=await fixture();let worker;
  try {
    await queue.enqueueBatch([node('root'),node('child',['root'])]);const seen=[];
    worker=DurableJobWorker.start(queue,lease=>{seen.push(lease.id);if(lease.id==='root'&&lease.attempt===1)throw new Error('retry');return 'done';},
      {...options,retryDelayMs:0});
    await worker.done;assert.deepEqual(seen,['root','root','child']);
    assert.equal(worker.stats.blockedCancellations,0);assert.equal((await queue.stats()).completed,2);
  }finally{await worker?.stop({abort:true}).catch(()=>{});db.close();}
});

test('startup recovers a crashed final attempt before cancelling descendants',async()=>{
  const {db,queue}=await fixture();let worker;
  try {
    await queue.enqueueBatch([node('root',[],{maxAttempts:1}),node('child',['root']),node('leaf',['child'])]);
    await queue.claim('crashed',10);
    // Existing lease now expired under the new, shared queue/worker clock.
    const restarted=await DurableJobQueue.open(owner(db),'work',{clock:()=>110});
    worker=DurableJobWorker.start(restarted,()=>{throw new Error('must not run');},{...options,clock:()=>110,reapLimit:1});
    await worker.done;assert.equal(worker.stats.reapedLeases,1);assert.equal(worker.stats.blockedCancellations,2);
  }finally{await worker?.stop({abort:true}).catch(()=>{});db.close();}
});

for(const abort of [false,true])test(`stop abort=${abort} joins an admitted cancellation transaction`,async()=>{
  const {db,queue}=await fixture();const entered=gate(),release=gate();let worker;
  try {
    await failedChain(queue,2);let pause=true;let handlers=0;
    db.afterSql=async sql=>{if(pause&&sql.startsWith(`UPDATE ${TABLE} AS candidate`)){pause=false;entered.resolve();await release.promise;}};
    worker=DurableJobWorker.start(queue,()=>{handlers++;},options);
    await boundary(entered.promise);let stopped=false;
    const done=worker.stop({abort}).then(()=>{stopped=true;});
    await tick();assert.equal(stopped,false);release.resolve();await done;
    assert.equal(worker.stats.blockedCancellations,2);assert.equal(handlers,0);
    assert.equal((await queue.stats()).cancelled,3);
  }finally{release.resolve();await worker?.stop({abort:true}).catch(()=>{});db.close();}
});

test('pre-aborted startup admits neither expiry recovery nor cancellation',async()=>{
  const {db,queue}=await fixture();let worker;
  try {
    await failedChain(queue);const before=db.serial,cancel=new AbortController();cancel.abort();
    worker=DurableJobWorker.start(queue,()=>{},{...options,signal:cancel.signal});await worker.done;
    assert.equal(db.serial,before);assert.equal(worker.stats.blockedCancellations,0);
  }finally{await worker?.stop({abort:true}).catch(()=>{});db.close();}
});

test('periodic recovery propagates failure while an unrelated handler is active',async()=>{
  const {db,queue}=await fixture();const handling=gate(),release=gate(),cancelled=gate();let worker;
  try {
    await queue.enqueueBatch([node('healthy'),node('root',[],{availableAt:1000}),node('child',['root'])]);
    const wrapped=adapter(queue,{cancelBlocked:async limit=>{const n=await queue.cancelBlocked(limit);if(n)cancelled.resolve();return n;}});
    worker=DurableJobWorker.start(wrapped,async()=>{handling.resolve();await release.promise;return 'done';},
      {...options,reapIntervalMs:5});
    await boundary(handling.promise);await queue.cancel('root');await boundary(cancelled.promise);
    assert.equal((await queue.get('child')).state,'cancelled');assert.equal(worker.stats.activeJobs,1);
    release.resolve();await worker.done;assert.equal(worker.stats.completed,1);assert.equal(worker.stats.blockedCancellations,1);
  }finally{release.resolve();await worker?.stop({abort:true}).catch(()=>{});db.close();}
});

for(const value of [-1,1.5,3,NaN,undefined,'1'])test(`bad cancellation acknowledgement ${String(value)} stops admissions`,async()=>{
  const {db,queue}=await fixture();let worker;
  try {
    await failedChain(queue,2);let claims=0,calls=0;
    worker=DurableJobWorker.start(adapter(queue,{
      cancelBlocked:async limit=>{calls++;await queue.cancelBlocked(limit);return value;},
      claim:async(...args)=>{claims++;return queue.claim(...args);},
    }),()=>{},options);
    await assert.rejects(worker.done,error=>error.phase==='cancel-blocked');
    assert.equal(calls,1);assert.equal(claims,0);assert.equal(worker.stats.blockedCancellations,0);
    assert.equal(worker.stats.state,'failed');
  }finally{await worker?.stop({abort:true}).catch(()=>{});db.close();}
});

for(const mode of ['WAL','DELETE'])test(`${mode}: unknown cleanup COMMIT stops, then a fresh supervisor reconciles`,async()=>{
  const path=join(mkdtempSync(join(tmpdir(),'worker-failed-')), 'jobs.db');
  const db=new JobSqliteTarget(path,`PRAGMA journal_mode=${mode};`);let worker;
  try {
    const queue=await DurableJobQueue.open(owner(db),'work',{clock});await failedChain(queue,2);let armed=false,calls=0;
    db.afterSql=sql=>{if(sql.startsWith(`UPDATE ${TABLE} AS candidate`))armed=true;};
    db.afterCommit=()=>{if(armed)throw new Error('lost cleanup commit response');};
    worker=DurableJobWorker.start(adapter(queue,{cancelBlocked:async n=>{calls++;return queue.cancelBlocked(n);}}),()=>{},options);
    await assert.rejects(worker.done,error=>error.phase==='cancel-blocked'&&/lost cleanup/.test(error.cause.message));
    assert.equal(calls,1);assert.equal(worker.stats.blockedCancellations,0);
  }finally{await worker?.stop({abort:true}).catch(()=>{});db.close();}
  const reopen=new JobSqliteTarget(path);let recovered;
  try {
    const queue=await DurableJobQueue.open(owner(reopen),'work',{clock});let calls=0;
    recovered=DurableJobWorker.start(queue,()=>{calls++;},options);await recovered.done;
    assert.equal(calls,0);assert.equal((await queue.stats()).cancelled,3);
    assert.equal(recovered.stats.blockedCancellations,0);
  }finally{await recovered?.stop({abort:true}).catch(()=>{});reopen.close();}
});

test('cleanup failure aborts and joins an active sibling handler before done rejects',async()=>{
  const {db,queue}=await fixture();const handling=gate(),aborted=gate(),release=gate();let worker;
  try {
    await queue.enqueue(node('healthy'));let started=false,calls=0;
    worker=DurableJobWorker.start(adapter(queue,{cancelBlocked:async limit=>{
      calls++;const n=await queue.cancelBlocked(limit);if(started)throw new Error('uncertain cleanup');return n;
    }}),async(_lease,context)=>{
      started=true;handling.resolve();context.signal.addEventListener('abort',()=>aborted.resolve(),{once:true});
      await release.promise;context.checkpoint();
    },{...options,reapIntervalMs:5});
    let finished=false;const done=worker.done.then(()=>{finished=true;},error=>{finished=true;return error;});
    await boundary(handling.promise);await boundary(aborted.promise);await tick();assert.equal(finished,false);
    release.resolve();const error=await done;assert.equal(error.phase,'cancel-blocked');
    const stoppedAt=calls;await tick();assert.equal(calls,stoppedAt);
    assert.equal((await queue.get('healthy')).state,'leased');assert.equal(worker.stats.activeJobs,0);
  }finally{release.resolve();await worker?.stop({abort:true}).catch(()=>{});db.close();}
});
