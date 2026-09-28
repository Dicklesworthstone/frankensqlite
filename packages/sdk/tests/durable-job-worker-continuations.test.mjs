import assert from 'node:assert/strict';
import { test } from 'node:test';
import { DurableJobQueue, DURABLE_JOBS_TABLE } from '../src/durable-jobs.ts';
import { DurableJobWorker } from '../src/durable-job-worker.ts';
import { JobSqliteTarget, gate, tick } from './helpers/durable-jobs-sqlite-target.mjs';

const table=DURABLE_JOBS_TABLE;
const next=()=>[{queue:'children',id:'child',payload:'step two'}];
const policy={owner:'worker',clock:()=>100,stopWhenIdle:true,leaseMs:30000,heartbeatMs:10000};
const effect=tx=>tx.execute("INSERT INTO effects VALUES(1,'applied')");
async function setup(){
  const db=new JobSqliteTarget(':memory:','CREATE TABLE effects(id INTEGER PRIMARY KEY,value TEXT);');
  const q=await DurableJobQueue.open(db,'parents',{clock:()=>100});
  await q.enqueue({id:'parent',payload:'work'});return{db,q};
}
function legacyQueue(q){return Object.fromEntries(['claim','renew','complete','completeWith','fail','reapExpired'].map(k=>[k,q[k].bind(q)]));}

test('worker completion schedules follow-ups through the actual queue',async()=>{
  const p=await setup();let calls=0;
  const worker=DurableJobWorker.start(p.q,()=>({next:next(),result:'done',apply:async tx=>{calls++;await effect(tx);}}),policy);
  try{
    await worker.done;assert.equal(calls,1);assert.equal(worker.stats.completed,1);assert.equal(worker.stats.activeJobs,0);
    assert.equal((await p.q.get('parent')).state,'completed');assert.equal((await p.q.get('parent')).result,'done');
    const children=await DurableJobQueue.open(p.db,'children',{clock:()=>100});assert.equal((await children.get('child')).state,'ready');
    const downstream=DurableJobWorker.start(children,()=>({apply:tx=>tx.execute("INSERT INTO effects VALUES(2,'downstream')")}),policy);
    await downstream.done;assert.equal(downstream.stats.completed,1);assert.equal(p.db.rows().length,2);
  }finally{await worker.stop().catch(()=>{});p.db.close();}
});
test('same-queue three-stage workflow is consumed without a publication gap',async()=>{
  const p=await setup();let calls=0;
  const worker=DurableJobWorker.start(p.q,lease=>{
    const n=lease.id==='parent'?1:Number(lease.id.slice(5));calls++;
    return{...(n===3?{}:{next:[{queue:'parents',id:`step-${n+1}`,payload:'next'}]}),
      apply:tx=>tx.execute('INSERT INTO effects VALUES(?,?)',[n,`step ${n}`])};
  },policy);
  try{await worker.done;assert.equal(calls,3);assert.equal(worker.stats.completed,3);assert.equal(p.db.rows().length,3);assert.equal((await p.q.stats()).completed,3);}
  finally{await worker.stop().catch(()=>{});p.db.close();}
});
for(const result of ['plain result',null])test(`legacy queue without continuations still accepts ${String(result)}`,async()=>{
  const p=await setup();const worker=DurableJobWorker.start(legacyQueue(p.q),()=>result,policy);
  try{await worker.done;assert.equal(worker.stats.completed,1);assert.equal((await p.q.get('parent')).result,result);}
  finally{await worker.stop().catch(()=>{});p.db.close();}
});
test('unsupported queue does not silently discard requested continuations',async()=>{
  const p=await setup();let applied=0;
  const worker=DurableJobWorker.start(legacyQueue(p.q),()=>({next:next(),apply:async()=>{applied++;}}),policy);
  try{
    await worker.done;assert.equal(applied,0);assert.equal(worker.stats.completed,0);assert.equal(worker.stats.failedJobs,1);
    assert.equal((await p.q.get('parent')).state,'ready');assert.equal(p.db.rows(`SELECT count(*) AS n FROM ${table}`)[0].n,1);
  }finally{await worker.stop().catch(()=>{});p.db.close();}
});
test('invalid continuation result fails the job before application SQL',async()=>{
  const p=await setup();let applied=0;const children=next();children.push({...children[0]});
  const worker=DurableJobWorker.start(p.q,()=>({next:children,apply:async()=>{applied++;}}),policy);
  try{await worker.done;assert.equal(applied,0);assert.equal(worker.stats.failedJobs,1);assert.equal(worker.stats.completed,0);assert.deepEqual(p.db.rows(),[]);}
  finally{await worker.stop().catch(()=>{});p.db.close();}
});
test('worker owns continuation inputs before joining an in-flight heartbeat',async()=>{
  const p=await setup(),entered=gate(),release=gate(),returned=gate();const children=next();
  p.db.beforeSql=async sql=>{if(sql.includes('SET lease_expires_at')){entered.resolve();await release.promise;}};
  const worker=DurableJobWorker.start(p.q,async()=>{
    await entered.promise;returned.resolve();return{next:children,apply:effect};
  },{...policy,leaseMs:3000,heartbeatMs:5});
  try{
    await returned.promise;await tick();children[0].queue='wrong';children[0].id='wrong';children[0].payload='changed';children.push({queue:'other',id:'extra',payload:'extra'});
    release.resolve();await worker.done;
    assert.equal(worker.stats.completed,1);assert.equal(worker.stats.renewals,1);
    const rows=p.db.rows(`SELECT queue_name,job_id,payload FROM ${table} WHERE job_id<>'parent'`);
    assert.deepEqual(rows,[{queue_name:'children',job_id:'child',payload:'step two'}]);
  }finally{release.resolve();await worker.stop({abort:true}).catch(()=>{});p.db.close();}
});
for(const continuations of [false,true])test(`abort joins dropped apply SQL before rollback (${continuations?'continuation':'ordinary completion'})`,async()=>{
  const p=await setup(),entered=gate(),release=gate();let child;
  p.db.beforeSql=async sql=>{if(sql.startsWith('INSERT INTO effects')){entered.resolve();await release.promise;}};
  const worker=DurableJobWorker.start(p.q,()=>({...(continuations?{next:next()}:{}),apply:async tx=>{child=effect(tx);}}),policy);
  let stopped=false;
  try{
    await entered.promise;const stopping=worker.stop({abort:true,reason:new Error('cancel completion')});
    void stopping.then(()=>{stopped=true;},()=>{stopped=true;});await tick();assert.equal(stopped,false);
    release.resolve();await assert.rejects(stopping,{phase:'complete'});await child;
    assert.equal(worker.stats.activeJobs,0);assert.equal(worker.stats.completed,0);
    assert.equal((await p.q.get('parent')).state,'leased');assert.deepEqual(p.db.rows(),[]);
    assert.equal(p.db.rows(`SELECT count(*) AS n FROM ${table}`)[0].n,1);
  }finally{release.resolve();await worker.stop({abort:true}).catch(()=>{});await Promise.allSettled([child]);p.db.close();}
});
test('graceful stop drains an admitted continuation normally',async()=>{
  const p=await setup(),entered=gate(),release=gate();
  const worker=DurableJobWorker.start(p.q,()=>({next:next(),apply:async tx=>{entered.resolve();await release.promise;await effect(tx);}}),policy);
  try{
    await entered.promise;const stopping=worker.stop();release.resolve();await stopping;
    assert.equal(worker.stats.completed,1);assert.equal(worker.stats.activeJobs,0);
    assert.equal(p.db.rows(`SELECT count(*) AS n FROM ${table}`)[0].n,2);
  }finally{release.resolve();await worker.stop().catch(()=>{});p.db.close();}
});
test('stop during child publication joins the all-or-none transaction, without pretending rollback',async()=>{
  const p=await setup(),entered=gate(),release=gate();let stopped=false;
  p.db.afterSql=async(sql,params)=>{if(sql.includes(`INSERT INTO ${table}`)&&params[1]==='child'){entered.resolve();await release.promise;}};
  const worker=DurableJobWorker.start(p.q,()=>({next:next(),apply:effect}),policy);
  try{
    await entered.promise;const stopping=worker.stop({abort:true});void stopping.then(()=>{stopped=true;},()=>{stopped=true;});
    await tick();assert.equal(stopped,false);release.resolve();await stopping;
    assert.equal(worker.stats.completed,1);assert.equal((await p.q.get('parent')).state,'completed');
    assert.equal(p.db.rows(`SELECT count(*) AS n FROM ${table}`)[0].n,2);assert.equal(p.db.rows().length,1);
  }finally{release.resolve();await worker.stop().catch(()=>{});p.db.close();}
});
test('conflicting child stops worker without completing the parent or retrying its effects',async()=>{
  const p=await setup();const child=await DurableJobQueue.open(p.db,'children',{clock:()=>100});
  await child.enqueue({id:'child',payload:'different'});let calls=0;
  const worker=DurableJobWorker.start(p.q,()=>({next:next(),apply:async tx=>{calls++;await effect(tx);}}),policy);
  try{
    await assert.rejects(worker.done,{phase:'complete'});assert.equal(calls,1);assert.equal(worker.stats.state,'failed');assert.equal(worker.stats.completed,0);
    assert.equal((await p.q.get('parent')).state,'leased');assert.deepEqual(p.db.rows(),[]);assert.equal((await child.get('child')).payload,'different');
  }finally{await worker.stop().catch(()=>{});p.db.close();}
});
test('lost completion acknowledgement halts worker; reopening processing does not rerun parent',async()=>{
  const p=await setup();let complete=false,calls=0;
  p.db.afterSql=sql=>{if(sql.includes("state = 'completed'"))complete=true;};
  p.db.afterCommit=()=>{if(complete){p.db.afterCommit=null;throw Object.assign(new Error('lost response'),{sqlCommitted:true});}};
  const worker=DurableJobWorker.start(p.q,()=>({next:next(),apply:async tx=>{calls++;await effect(tx);}}),policy);
  try{
    await assert.rejects(worker.done,{phase:'complete'});assert.equal(worker.stats.completed,0);assert.equal((await p.q.get('parent')).state,'completed');
    const resumed=DurableJobWorker.start(p.q,()=>{calls++;throw new Error('must not repeat');},policy);await resumed.done;
    assert.equal(resumed.stats.claimed,0);assert.equal(calls,1);assert.equal(p.db.rows().length,1);
    assert.equal(p.db.rows(`SELECT count(*) AS n FROM ${table}`)[0].n,2);
  }finally{await worker.stop().catch(()=>{});p.db.close();}
});
