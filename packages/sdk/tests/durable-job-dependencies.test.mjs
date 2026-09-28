import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtempSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { DurableJobQueue, DURABLE_JOBS_TABLE } from '../src/durable-jobs.ts';
import { DurableJobWorker } from '../src/durable-job-worker.ts';
import { JobSqliteTarget, gate } from './helpers/durable-jobs-sqlite-target.mjs';

const jobs = `main."${DURABLE_JOBS_TABLE}"`;
const edges = 'main."__fsqlite_job_dependencies_v1"';
const input = id => ({ id, payload: id });
const ref = (id, queue = 'q') => ({ queue, id });
const code = value => error => error.code === value;
const conflict = code('ERR_FSQLITE_JOB_ID_CONFLICT');
async function fixture(path = ':memory:', ddl = '') {
  const db = new JobSqliteTarget(path, ddl + ';CREATE TABLE IF NOT EXISTS effects(id INTEGER PRIMARY KEY,value TEXT)');
  const clock = { now: 100 };
  const queue = await DurableJobQueue.open(db, 'q', { clock: () => clock.now });
  return { db, queue, clock };
}
async function roots(q) { await q.enqueue(input('a')); await q.enqueue(input('b')); }
async function finish(q) { const lease = await q.claim('worker'); assert(lease); await q.complete(lease); return lease.id; }

test('fan-in waits for ALL parents and does not consume an attempt or block ready lower-priority work', async () => {
  const {db,queue:q}=await fixture();
  try {
    await roots(q); await q.enqueue({...input('join'),priority:100,dependsOn:[ref('b'),ref('a')]});
    assert.equal((await q.stats()).available,2);
    assert.equal(await finish(q),'a');
    assert.equal((await q.get('join')).attempts,0);
    assert.equal((await q.stats()).available,1);
    assert.equal(await finish(q),'b');
    assert.equal((await q.stats()).available,1);
    assert.equal(await finish(q),'join'); assert.equal(await q.claim('worker'),null);
    assert.deepEqual(await q.dependencies('join'),[{...ref('a'),state:'completed'},{...ref('b'),state:'completed'}]);
  } finally {db.close();}
});
for (const outcome of ['ready','leased','dead','cancelled']) test(`a ${outcome} prerequisite never satisfies a join`,async()=>{
  const {db,queue:q}=await fixture();
  try {
    await q.enqueue({...input('p'),maxAttempts:1});
    if(outcome==='cancelled')await q.cancel('p');
    else if(outcome!=='ready'){const l=await q.claim('w');if(outcome==='dead')await q.fail(l,'failed');}
    await q.enqueue({...input('join'),priority:100,dependsOn:[ref('p')]});
    assert.equal((await q.dependencies('join'))[0].state,outcome);
    if(outcome==='ready')assert.equal((await q.claim('w')).id,'p');
    assert.equal(await q.claim('w'),null);assert.equal((await q.get('join')).attempts,0);
  }finally{db.close();}
});
test('cross-queue joins use exact case-sensitive identities and complete without a scheduler memory cache',async()=>{
  const {db,queue:q}=await fixture();
  try{
    const other=await DurableJobQueue.open(db,'Other',{clock:()=>100});
    await q.enqueue(input('p'));await other.enqueue(input('p'));
    await q.enqueue({...input('join'),dependsOn:[ref('p'),ref('p','Other')]});
    await finish(q);assert.equal(await q.claim('w'),null);
    await finish(other);assert.equal(await finish(q),'join');
  }finally{db.close();}
});
test('prerequisite set is immutable and idempotent independent of caller order',async()=>{
  const {db,queue:q}=await fixture();
  try{
    await roots(q);const job={...input('join'),dependsOn:[ref('b'),ref('a')]};
    assert.equal((await q.enqueue(job)).inserted,true);
    assert.equal((await q.enqueue({...job,dependsOn:[ref('a'),ref('b')]})).inserted,false);
    for(const dependsOn of [undefined,[],[ref('a')]]){
      let calls=0;await assert.rejects(q.enqueueWith({...input('join'),dependsOn},async()=>{calls++;}),conflict);
      assert.equal(calls,0);
    }
    await finish(q);await finish(q);await finish(q);
    assert.equal((await q.enqueue(job)).job.state,'completed');
  }finally{db.close();}
});
test('missing and self prerequisites roll back a new job before its application callback',async()=>{
  const {db,queue:q}=await fixture();
  try{for(const dependsOn of [[ref('absent')],[ref('join')]]){
    let calls=0;await assert.rejects(q.enqueueWith({...input('join'),dependsOn},async()=>{calls++;}));
    assert.equal(calls,0);assert.equal(await q.get('join'),null);assert.deepEqual(db.rows(`SELECT * FROM ${edges}`),[]);
  }}finally{db.close();}
});
test('existing independent job cannot acquire requirements and form a cycle',async()=>{
  const {db,queue:q}=await fixture();
  try{await q.enqueue(input('a'));await q.enqueue({...input('b'),dependsOn:[ref('a')]});
    await assert.rejects(q.enqueue({...input('a'),dependsOn:[ref('b')]}),conflict);
    assert.deepEqual(await q.dependencies('a'),[]);assert.equal(await finish(q),'a');assert.equal(await finish(q),'b');
  }finally{db.close();}
});
test('storage trigger refuses an old/unfiltered claim even with matching TEMP shadows',async()=>{
  const {db,queue:q}=await fixture();
  try{await roots(q);await q.enqueue({...input('join'),dependsOn:[ref('a'),ref('b')]});
    db.db.exec(`CREATE TEMP TABLE "${DURABLE_JOBS_TABLE}"(queue_name,job_id,state); INSERT INTO temp."${DURABLE_JOBS_TABLE}" VALUES('q','a','completed'),('q','b','completed');CREATE TEMP TABLE __fsqlite_job_dependencies_v1(queue_name,job_id,parent_queue,parent_id);`);
    await assert.rejects(db.transaction(tx=>tx.execute(`UPDATE ${jobs} SET state='leased',attempts=attempts+1,lease_owner='old',lease_token='token',lease_expires_at=500 WHERE job_id='join'`)),/prerequisites/);
    assert.equal((await q.get('join')).attempts,0);
    assert.deepEqual(db.rows('SELECT * FROM temp.__fsqlite_job_dependencies_v1'),[]);
  }finally{db.close();}
});
test('dependency requirements cannot be removed or changed by ordinary SQL updates',async()=>{
  const {db,queue:q}=await fixture();
  try{await roots(q);await q.enqueue({...input('join'),dependsOn:[ref('a')]});
    await assert.rejects(db.execute(`DELETE FROM ${edges}`),/immutable/);
    await assert.rejects(db.execute(`UPDATE ${edges} SET parent_id='b'`),/immutable/);
    assert.deepEqual(await q.dependencies('join'),[{...ref('a'),state:'ready'}]);
  }finally{db.close();}
});
test('missing parent evidence remains blocked, not equivalent to completion',async()=>{
  const {db,queue:q}=await fixture();
  try{await q.enqueue(input('a'));await q.enqueue({...input('join'),dependsOn:[ref('a')]});
    // Move the test-owned evidence aside instead of deleting files or tables.
    db.db.exec(`UPDATE ${jobs} SET job_id='preserved-a' WHERE job_id='a'`);
    assert.deepEqual(await q.dependencies('join'),[{...ref('a'),state:null}]);
    assert.equal((await q.claim('w')).id,'preserved-a');assert.equal(await q.claim('w'),null);
  }finally{db.close();}
});
test('open rejects incomplete dependency storage rather than recreating missing constraints',async()=>{
  const {db,queue:q}=await fixture();
  try{await q.enqueue(input('a'));await q.enqueue({...input('join'),dependsOn:[ref('a')]});
    db.db.exec('ALTER TABLE main.__fsqlite_job_dependencies_v1 RENAME TO preserved_dependencies');
    await assert.rejects(DurableJobQueue.open(db,'q'),code('ERR_FSQLITE_JOB_SCHEMA'));
    assert.equal(db.rows('SELECT * FROM preserved_dependencies').length,1);
  }finally{db.close();}
});
test('exact schema initialization is repeatable and new handles observe the same prerequisites',async()=>{
  const {db,queue:q}=await fixture();
  try{await roots(q);await q.enqueue({...input('join'),dependsOn:[ref('a')]});
    const before=db.rows('SELECT name,sql FROM main.sqlite_schema ORDER BY name');
    const reopened=await DurableJobQueue.open(db,'q',{clock:()=>100});
    assert.deepEqual(await reopened.dependencies('join'),await q.dependencies('join'));
    assert.deepEqual(db.rows('SELECT name,sql FROM main.sqlite_schema ORDER BY name'),before);
    assert.equal(await q.dependencies('absent'),null);
  }finally{db.close();}
});
test('job, requirements and enqueueWith effects roll back together',async()=>{
  const {db,queue:q}=await fixture();
  try{await roots(q);await assert.rejects(q.enqueueWith({...input('join'),dependsOn:[ref('a')]},async tx=>{
    await tx.execute("INSERT INTO effects VALUES(1,'effect')");throw new Error('abort');
  }),/abort/);assert.equal(await q.get('join'),null);assert.deepEqual(db.rows(`SELECT * FROM ${edges}`),[]);assert.deepEqual(db.rows(),[]);
  }finally{db.close();}
});
test('lost enqueue commit response recovers the same immutable dependency set without repeating work',async()=>{
  const {db,queue:q}=await fixture();
  try{await roots(q);db.afterCommit=()=>{throw new Error('lost commit');};let calls=0;
    const job={...input('join'),dependsOn:[ref('a'),ref('b')]};
    await assert.rejects(q.enqueueWith(job,async tx=>{calls++;await tx.execute("INSERT INTO effects VALUES(1,'once')");}),/lost commit/);
    db.afterCommit=null;assert.equal((await q.enqueueWith(job,async()=>{calls++;})).inserted,false);
    assert.equal(calls,1);assert.equal((await q.dependencies('join')).length,2);
  }finally{db.afterCommit=null;db.close();}
});
test('requirements are copied before asynchronous transaction admission',async()=>{
  const {db,queue:q}=await fixture();const entered=gate(),release=gate();
  let delay=false;
  try{await roots(q);const q2=await DurableJobQueue.open({transaction:async work=>{
    if(delay){entered.resolve();await release.promise;}return db.transaction(work);
  }},'q',{clock:()=>100});
    delay=true;const refs=[ref('a')],job={...input('join'),dependsOn:refs};
    const call=q2.enqueue(job);await entered.promise;refs[0].id='missing';refs.push(ref('b'));release.resolve();
    await call;assert.deepEqual(await q.dependencies('join'),[{...ref('a'),state:'ready'}]);
  }finally{release.resolve();db.close();}
});
for (const refs of [null,{},[{}],[{queue:'q',id:''}],[{queue:1,id:'a'}],[ref('a'),ref('a')],Array.from({length:129},(_,i)=>ref(String(i)))])
  test(`malformed prerequisites reject before SQL: ${JSON.stringify(refs).slice(0,60)}`,async()=>{
    const {db,queue:q}=await fixture();try{db.statements.length=0;await assert.rejects(q.enqueue({...input('join'),dependsOn:refs}));assert.deepEqual(db.statements,[]);}finally{db.close();}
  });
for(const mode of ['WAL','DELETE'])for(const encoding of ['UTF-8','UTF-16le','UTF-16be'])
  test(`${mode}/${encoding}: file reopen preserves requirements and exact Unicode parent identities`,async()=>{
    const path=join(mkdtempSync(join(tmpdir(),'job-join-')),'jobs.db');
    const p=await fixture(path,`PRAGMA encoding='${encoding}';PRAGMA journal_mode=${mode}`);
    const parents=['\uFEFF😀','\uE000','a"b'];
    try{for(const id of parents)await p.queue.enqueue(input(id));await p.queue.enqueue({...input('join'),priority:100,dependsOn:parents.map(id=>ref(id))});}
    finally{p.db.close();}
    const r=await fixture(path);try{
      for(let i=0;i<parents.length;i++){const lease=await r.queue.claim('w');assert(parents.includes(lease.id));await r.queue.complete(lease);}
      assert.equal(await finish(r.queue),'join');assert((await r.queue.dependencies('join')).every(p=>p.state==='completed'));
    }finally{r.db.close();}
  });
test('actual worker executes a join only after both prerequisite effects are committed',async()=>{
  const {db,queue:q}=await fixture();let worker;
  try{await roots(q);await q.enqueue({...input('join'),priority:100,dependsOn:[ref('a'),ref('b')]});
    const seen=[];worker=DurableJobWorker.start(q,lease=>({apply:async tx=>{
      if(lease.id==='join')assert.equal((await tx.query('SELECT count(*) AS n FROM effects')).rows[0].n,2);
      await tx.execute('INSERT INTO effects VALUES(?,?)',[seen.length+1,lease.id]);seen.push(lease.id);
    }}),{owner:'worker',clock:()=>100,stopWhenIdle:true});await worker.done;
    assert.deepEqual(seen,['a','b','join']);assert.equal(worker.stats.completed,3);
  }finally{await worker?.stop().catch(()=>{});db.close();}
});
test('continuation captures immutable prerequisites and remains blocked on another queue',async()=>{
  const {db,queue:q}=await fixture();let worker;
  try{const other=await DurableJobQueue.open(db,'other',{clock:()=>100});await other.enqueue(input('slow'));await q.enqueue(input('root'));
    worker=DurableJobWorker.start(q,lease=>({next:[{...input('join'),queue:'q',dependsOn:[ref('root'),ref('slow','other')]}],apply:async()=>{}}),
      {owner:'worker',clock:()=>100,stopWhenIdle:true});await worker.done;
    assert.equal(worker.stats.completed,1);assert.equal((await q.get('join')).attempts,0);
    await finish(other);assert.equal(await finish(q),'join');
  }finally{await worker?.stop().catch(()=>{});db.close();}
});

test('two independent WAL owners cannot both claim a released join',async()=>{
  const path=join(mkdtempSync(join(tmpdir(),'job-join-race-')),'jobs.db');
  const p=await fixture(path,'PRAGMA journal_mode=WAL');let peer;
  const seenA=gate(),seenB=gate(),releaseA=gate(),releaseB=gate();
  try{await roots(p.queue);await finish(p.queue);await finish(p.queue);
    await p.queue.enqueue({...input('join'),dependsOn:[ref('a'),ref('b')]});peer=await fixture(path);
    p.db.afterSql=async sql=>{if(sql.startsWith('SELECT job_id')){seenA.resolve();await releaseA.promise;}};
    peer.db.afterSql=async sql=>{if(sql.startsWith('SELECT job_id')){seenB.resolve();await releaseB.promise;}};
    const a=p.queue.claim('a'),b=peer.queue.claim('b');const rejected=assert.rejects(b,/locked|busy/i);
    await Promise.all([seenA.promise,seenB.promise]);releaseA.resolve();assert.equal((await a).id,'join');releaseB.resolve();await rejected;
    assert.equal((await p.queue.get('join')).attempts,1);
  }finally{releaseA.resolve();releaseB.resolve();peer?.db.close();p.db.close();}
});

import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
const childScript=fileURLToPath(new URL('./helpers/durable-dependency-child.mjs',import.meta.url));
const loader=fileURLToPath(new URL('./helpers/production-source-loader.mjs',import.meta.url));
async function killAt(path,cut){
  const child=spawn(process.execPath,['--experimental-transform-types',`--experimental-loader=${loader}`,childScript,path,cut],{stdio:['ignore','pipe','pipe','ipc']});
  let marker=false,timedOut=false,stderr='';child.stderr.on('data',data=>{stderr+=data;});
  child.on('message',m=>{if(m?.cut===cut){marker=true;child.kill('SIGKILL');}});
  const timer=setTimeout(()=>{timedOut=true;child.kill('SIGKILL');},10000);
  try{const result=await new Promise((resolve,reject)=>{child.once('error',reject);child.once('exit',(code,signal)=>resolve({code,signal}));});
    assert.equal(timedOut,false,'Watchdog termination is not a requested cut');assert.equal(marker,true,stderr);
    assert.equal(result.signal,'SIGKILL');
  }finally{clearTimeout(timer);}
}
for(const mode of ['WAL','DELETE'])for(const cut of ['before-edge','after-edge','before-commit','after-commit'])
  test(`${mode}/${cut}: actual SIGKILL preserves atomic job/prerequisites/effects publication`,async()=>{
    const path=join(mkdtempSync(join(tmpdir(),'job-join-kill-')),'jobs.db');const p=await fixture(path,`PRAGMA journal_mode=${mode}`);
    try{await roots(p.queue);}finally{p.db.close();}
    await killAt(path,cut);const r=await fixture(path);
    try{const committed=cut==='after-commit';assert.equal((await r.queue.get('join'))!==null,committed);
      assert.equal(r.db.rows(`SELECT * FROM ${edges}`).length,committed?2:0);assert.equal(r.db.rows().length,committed?1:0);
      if(committed){let called=0;const again=await r.queue.enqueueWith({id:'join',payload:'joined',dependsOn:[ref('a'),ref('b')]},async()=>{called++;});
        assert.equal(again.inserted,false);assert.equal(called,0);
        assert.equal(await finish(r.queue),'a');assert.equal(await finish(r.queue),'b');assert.equal(await finish(r.queue),'join');}
    }finally{r.db.close();}
  });
