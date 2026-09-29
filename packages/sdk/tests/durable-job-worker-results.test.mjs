import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtempSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { DurableJobQueue, DURABLE_JOBS_TABLE } from '../src/durable-jobs.ts';
import { DurableJobWorker } from '../src/durable-job-worker.ts';
import { JobSqliteTarget, gate, tick } from './helpers/durable-jobs-sqlite-target.mjs';
const table=`main."${DURABLE_JOBS_TABLE}"`;
const options={owner:'worker',loadDependencyResults:true,stopWhenIdle:true};
const bodyRead=sql=>sql.includes('AS result_data');
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
function serialized(db){let tail=Promise.resolve();return {transaction(work){const run=tail.then(()=>db.transaction(work));tail=run.catch(()=>{});return run;}};}
async function fixture(t,{encoding='UTF-8',path=':memory:'}={}){
 const db=new JobSqliteTarget(path,`PRAGMA encoding='${encoding}';PRAGMA journal_mode=WAL;CREATE TABLE IF NOT EXISTS effects(id TEXT PRIMARY KEY,value TEXT);`);
 let now=100;const clock=()=>now;const q=await DurableJobQueue.open(serialized(db),'jobs',{clock});
 t.after(()=>db.close());return {db,q,clock,time:n=>{now=n;}};
}
async function readyJoin(f,result='upstream'){
 await f.q.enqueueBatch([{queue:'jobs',id:'parent',payload:'p'},{queue:'jobs',id:'join',payload:'j',dependsOn:[{queue:'jobs',id:'parent'}]}]);
 await f.q.complete(await f.q.claim('producer'),result);
}
const stoppedDuringInputs=e=>e.phase==='dependency-results';
for(const encoding of ['UTF-8','UTF-16le','UTF-16be']) test(`${encoding}: actual worker executes output-dependent fan-out/fan-in continuations`,async t=>{
 const f=await fixture(t,{encoding});await f.q.enqueue({id:'root',payload:'root'});
 const seen=[];
 const w=DurableJobWorker.start(f.q,(lease,context)=>{
   const inputs=context.dependencyResults;assert(Object.isFrozen(inputs));assert(inputs.every(Object.isFrozen));seen.push(lease.id);
   if(lease.id==='root')return {result:'5',next:[
     {queue:'jobs',id:'join',payload:'j',dependsOn:[{queue:'jobs',id:'left'},{queue:'jobs',id:'right'}]},
     {queue:'jobs',id:'right',payload:'r',dependsOn:[{queue:'jobs',id:'root'}]},
     {queue:'jobs',id:'left',payload:'l',dependsOn:[{queue:'jobs',id:'root'}]},
   ],apply:async tx=>{assert.deepEqual(inputs,[]);await tx.execute("INSERT INTO effects VALUES('root','created')");}};
   if(lease.id!=='join')return String(Number(inputs[0].result)*(lease.id==='left'?2:3));
   assert.deepEqual(inputs.map(x=>[x.id,x.result]),[['left','10'],['right','15']]);
   const sum=String(inputs.reduce((s,x)=>s+Number(x.result),0));
   return {result:sum,apply:tx=>tx.execute("INSERT INTO effects VALUES('join',?)",[sum])};
 },{...options,clock:f.clock});
 await w.done;assert.equal(w.stats.completed,4);assert.equal(new Set(seen).size,4);
 assert.equal((await f.q.get('join')).result,'25');assert.equal(f.db.rows("SELECT value FROM effects WHERE id='join'")[0].value,'25');
});

test('default workers preserve legacy adapters and do not load result inputs',async t=>{
 const f=await fixture(t);await f.q.enqueue({id:'job',payload:'x'});
 f.q.dependencyResults=()=>{throw new Error('must not read');};
 const w=DurableJobWorker.start(f.q,(_lease,ctx)=>{assert.equal(ctx.dependencyResults,undefined);return 'done';},{owner:'worker',stopWhenIdle:true,clock:f.clock});
 await w.done;assert.equal(w.stats.completed,1);
});

test('opt-in requires result support before any queue operation',async()=>{
 let calls=0;const adapter=Object.fromEntries(['claim','renew','complete','completeWith','fail','reapExpired'].map(k=>[k,async()=>{calls++;}]));
 assert.throws(()=>DurableJobWorker.start(adapter,()=>{},options),/dependencyResults support/);
 assert.equal(calls,0);
});

for(const extra of [{loadDependencyResults:1},{maxDependencyResultBytes:-1},{maxDependencyResultBytes:64*1024*1024+1},{maxDependencyResultBytes:0.5}])
 test(`invalid result policy rejects at startup: ${JSON.stringify(extra)}`,async t=>{
  const f=await fixture(t);f.db.statements.length=0;
  assert.throws(()=>DurableJobWorker.start(f.q,()=>{}, {...options,...extra}));assert.deepEqual(f.db.statements,[]);
 });

test('captured result budget cannot be changed while a claim is awaiting admission',async t=>{
 const f=await fixture(t);await readyJoin(f);const entered=gate(),release=gate();const original=f.q.claim.bind(f.q);
 f.q.claim=async(...args)=>{entered.resolve();await release.promise;return original(...args);};
 const policy={...options,clock:f.clock,maxDependencyResultBytes:8};
 const w=DurableJobWorker.start(f.q,(_lease,ctx)=>{assert.equal(ctx.dependencyResults[0].result,'upstream');return 'done';},policy);
 await entered.promise;policy.maxDependencyResultBytes=0;policy.loadDependencyResults=false;release.resolve();await w.done;
 assert.equal(w.stats.completed,1);
});

test('oversized input halts without calling handlers, completing, or scheduling handler failure',async t=>{
 const f=await fixture(t);await readyJoin(f);let called=0;
 const w=DurableJobWorker.start(f.q,()=>{called++;},{...options,clock:f.clock,maxDependencyResultBytes:1});
 await assert.rejects(w.done,stoppedDuringInputs);
 assert.equal(called,0);assert.equal(w.stats.started,0);assert.equal(w.stats.failedJobs,0);
 assert.equal((await f.q.get('join')).state,'leased');assert.equal(w.stats.activeJobs,0);
});

test('input results are owned before handler admission, not aliases of an adapter reply',async t=>{
 const f=await fixture(t);await readyJoin(f);
 const raw=[{queue:'jobs',id:'parent',result:'upstream',byteLength:8}];f.q.dependencyResults=async()=>raw;
 const w=DurableJobWorker.start(f.q,async(_lease,ctx)=>{
   raw[0].result='changed';raw.push({queue:'evil',id:'extra',result:'x',byteLength:1});await tick();
   assert.deepEqual(ctx.dependencyResults,[{queue:'jobs',id:'parent',result:'upstream',byteLength:8}]);return 'done';
 },{...options,clock:f.clock});await w.done;
});

const valid={queue:'jobs',id:'parent',result:'x',byteLength:1};
for(const [index,invalid] of [null,{},[{}],[{...valid,byteLength:-1}],[{...valid,byteLength:7}],[{...valid,result:null}],
 [{...valid,result:'\ud800',byteLength:3}],[valid,{...valid}],[{...valid,queue:''}],
 [{...valid,id:'a\0b'}],Array.from({length:129},()=>valid),
 [{...valid,id:'a'},{...valid,id:'b',byteLength:2}],
 [Object.defineProperty({...valid},'result',{get(){throw new Error('getter should not run');}})]].entries())
 test(`malformed adapter results halt before handler: case ${index}`,async t=>{
   const f=await fixture(t);await readyJoin(f);let called=0;f.q.dependencyResults=async()=>invalid;
   const w=DurableJobWorker.start(f.q,()=>{called++;},{...options,clock:f.clock});
   await assert.rejects(w.done,stoppedDuringInputs);assert.equal(called,0);assert.equal((await f.q.get('join')).state,'leased');
 });

for(const abort of [false,true]) test(`${abort?'aborting':'graceful'} stop joins an input read before settling`,async t=>{
 const f=await fixture(t);await readyJoin(f);const entered=gate(),release=gate();let called=0,stopped=false;
 f.db.beforeSql=async sql=>{if(bodyRead(sql)){entered.resolve();await release.promise;}};
 const w=DurableJobWorker.start(f.q,(_lease,ctx)=>{called++;assert.equal(ctx.dependencyResults[0].result,'upstream');return 'done';},{...options,clock:f.clock});
 await entered.promise;const pending=w.stop({abort}).then(()=>{stopped=true;});await tick();assert.equal(stopped,false);assert.equal(w.stats.activeJobs,1);
 release.resolve();await pending;assert.equal(f.db.depth,0);assert.equal(called,abort?0:1);
 assert.equal((await f.q.get('join')).state,abort?'ready':'completed');
});

test('lease expiry during result reading does not start a handler or invent a fresh budget',async t=>{
 const f=await fixture(t);await readyJoin(f);const entered=gate(),release=gate();let called=0;
 f.db.beforeSql=async sql=>{if(bodyRead(sql)){entered.resolve();await release.promise;}};
 const w=DurableJobWorker.start(f.q,()=>{called++;},{...options,clock:f.clock,leaseMs:1000});
 await entered.promise;f.time(1100);const pending=w.stop();release.resolve();await pending;
 assert.equal(called,0);assert.equal(w.stats.lostLeases,1);assert.equal(w.stats.completed,0);
});

test('same-job heartbeats do not overlap input reads; start after read completion',async t=>{
 const f=await fixture(t);await readyJoin(f);const entered=gate(),release=gate();let renewed=0;
 const renew=f.q.renew.bind(f.q);f.q.renew=async(...args)=>{renewed++;return renew(...args);};
 f.db.beforeSql=async sql=>{if(bodyRead(sql)){entered.resolve();await release.promise;}};
 const w=DurableJobWorker.start(f.q,async()=>{await sleep(25);return 'done';},{...options,clock:f.clock,leaseMs:1000,heartbeatMs:5});
 await entered.promise;await sleep(25);assert.equal(renewed,0);release.resolve();await w.done;assert(renewed>0);
});

test('input storage failure cancels and joins an already-running sibling',async t=>{
 const f=await fixture(t);await f.q.enqueueBatch([{queue:'jobs',id:'a',payload:'a'},{queue:'jobs',id:'b',payload:'b'}]);
 const entered=gate(),cleanup=gate();let bRan=false,aJoined=false;
 const read=f.q.dependencyResults.bind(f.q);
 f.q.dependencyResults=async(lease,controls)=>{if(lease.id==='b'){await entered.promise;throw new Error('input storage unavailable');}return read(lease,controls);};
 const w=DurableJobWorker.start(f.q,async(lease,ctx)=>{
   if(lease.id==='b'){bRan=true;return;}
   entered.resolve();if(!ctx.signal.aborted)await new Promise(r=>ctx.signal.addEventListener('abort',r,{once:true}));
   await cleanup.promise;aJoined=true;
 },{...options,clock:f.clock,concurrency:2});
 const verdict=assert.rejects(w.done,stoppedDuringInputs);await entered.promise;await sleep(10);
 assert.equal(aJoined,false);assert.equal(bRan,false);cleanup.resolve();await verdict;assert.equal(aJoined,true);assert.equal(w.stats.activeJobs,0);
});

for(const encoding of ['UTF-8','UTF-16le','UTF-16be'])test(`${encoding}: connection reopen resumes downstream data flow from retained parent outputs`,async t=>{
 const path=join(mkdtempSync(join(tmpdir(),'job-worker-results-')),'db.sqlite');
 const original=new JobSqliteTarget(path,`PRAGMA encoding='${encoding}';PRAGMA journal_mode=WAL;CREATE TABLE effects(id TEXT PRIMARY KEY,value TEXT);`);
 try{
   const q=await DurableJobQueue.open(serialized(original),'jobs',{clock:()=>100});
   await q.enqueueBatch([{queue:'jobs',id:'parent',payload:'p'},{queue:'jobs',id:'join',payload:'j',dependsOn:[{queue:'jobs',id:'parent'}]}]);
   await q.complete(await q.claim('producer'),'\ufeffvalue\0tail😀');
 }finally{original.close();}
 const f=await fixture(t,{path});let seen=0;
 const w=DurableJobWorker.start(f.q,(lease,ctx)=>{
   seen++;assert.equal(lease.id,'join');assert.equal(ctx.dependencyResults[0].result,'\ufeffvalue\0tail😀');
   return {result:'delivered',apply:tx=>tx.execute("INSERT INTO effects VALUES('join','once')")};
 },{...options,clock:f.clock});await w.done;assert.equal(seen,1);assert.equal((await f.q.get('parent')).attempts,1);
});
