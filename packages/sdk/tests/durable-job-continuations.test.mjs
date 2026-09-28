import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { DurableJobQueue, DURABLE_JOBS_TABLE } from '../src/durable-jobs.ts';
import { JobSqliteTarget, gate, tick } from './helpers/durable-jobs-sqlite-target.mjs';

const table=`main."${DURABLE_JOBS_TABLE}"`;
const ddl='CREATE TABLE effects(id INTEGER PRIMARY KEY,value TEXT);';
const parent={id:'parent',payload:'do work'};
const next=()=>[{queue:'next',id:'first',payload:'one',priority:7},{queue:'other',id:'second',payload:'two',availableAt:200}];
const effect=tx=>tx.execute("INSERT INTO effects VALUES(1,'applied')");
async function setup(path=':memory:', extra=''){
  const db=new JobSqliteTarget(path, extra+ddl), clock={now:100};
  const q=await DurableJobQueue.open(db,'parents',{clock:()=>clock.now});
  await q.enqueue(parent);const lease=await q.claim('worker',1000);
  return{db,clock,q,lease};
}
async function unchanged(p){
  assert.equal((await p.q.get('parent')).state,'leased');
  assert.deepEqual(p.db.rows(),[]);
  assert.equal(p.db.rows(`SELECT count(*) AS n FROM ${table}`)[0].n,1);
}
for(const encoding of ['UTF-8','UTF-16le','UTF-16be'])test(`${encoding}: effects and cross-queue continuations commit with parent`,async()=>{
  const p=await setup(':memory:',`PRAGMA encoding='${encoding}';`);
  try{
    const children=next();children[0].payload='日本語😀';
    const value={computed:'same object'};
    const result=await p.q.completeAndEnqueue(p.lease,children,async tx=>{await effect(tx);return value;},'parent-result');
    assert.equal(result.value,value);assert(Object.isFrozen(result));assert(Object.isFrozen(result.jobs));
    assert.equal(result.jobs.length,2);assert(result.jobs.every(r=>r.inserted));
    assert.deepEqual(result.jobs.map(r=>[r.job.queue,r.job.id,r.job.state]),[['next','first','ready'],['other','second','ready']]);
    assert.equal((await p.q.get('parent')).state,'completed');assert.equal((await p.q.get('parent')).result,'parent-result');
    assert.deepEqual(p.db.rows(),[{id:1,value:'applied'}]);
    const a=await DurableJobQueue.open(p.db,'next',{clock:()=>p.clock.now});
    const b=await DurableJobQueue.open(p.db,'other',{clock:()=>p.clock.now});
    assert.equal((await a.claim('child-worker')).payload,'日本語😀');assert.equal(await b.claim('child-worker'),null);
    p.clock.now=200;assert.equal((await b.claim('child-worker')).id,'second');
  }finally{p.db.close();}
});
test('exact existing child input deduplicates without reviving a completed job',async()=>{
  const p=await setup();
  try{
    const child=await DurableJobQueue.open(p.db,'next',{clock:()=>100});
    await child.enqueue(next()[0]);const lease=await child.claim('prior');await child.complete(lease,'already');
    const r=await p.q.completeAndEnqueue(p.lease,next(),effect);
    assert.equal(r.jobs[0].inserted,false);assert.equal(r.jobs[0].job.state,'completed');
    assert.equal(r.jobs[1].inserted,true);assert.equal((await child.get('first')).result,'already');
  }finally{p.db.close();}
});
for(const change of [{payload:'different'},{priority:42},{availableAt:99},{maxAttempts:99}])test(`conflicting later child rolls back earlier child and effects: ${Object.keys(change)[0]}`,async()=>{
  const p=await setup();
  try{
    const child=await DurableJobQueue.open(p.db,'other',{clock:()=>100});
    await child.enqueue({...next()[1],...change});
    await assert.rejects(p.q.completeAndEnqueue(p.lease,next(),effect),{code:'ERR_FSQLITE_JOB_ID_CONFLICT'});
    assert.equal((await p.q.get('parent')).state,'leased');assert.deepEqual(p.db.rows(),[]);
    assert.equal(p.db.rows(`SELECT count(*) AS n FROM ${table} WHERE queue_name='next'`)[0].n,0);
    assert.equal((await child.get('second')).state,'ready');
  }finally{p.db.close();}
});
test('stale lease never invokes work or publishes children',async()=>{
  const p=await setup();let calls=0;
  try{p.clock.now=1100;await assert.rejects(p.q.completeAndEnqueue(p.lease,next(),async()=>{calls++;}),{code:'ERR_FSQLITE_JOB_LEASE_LOST'});assert.equal(calls,0);await unchanged(p);}
  finally{p.db.close();}
});
test('lease expiry during child publication rolls back the whole continuation',async()=>{
  const p=await setup();
  p.db.afterSql=(sql,params)=>{if(sql.includes(`INSERT INTO ${table}`)&&params[1]==='first')p.clock.now=1100;};
  try{await assert.rejects(p.q.completeAndEnqueue(p.lease,next(),effect),{code:'ERR_FSQLITE_JOB_LEASE_LOST'});await unchanged(p);}
  finally{p.db.close();}
});
test('saved business handle is closed before child enqueue starts',async()=>{
  const p=await setup(),entered=gate(),release=gate();let saved;
  p.db.beforeSql=async(sql,params)=>{if(sql.includes(`INSERT INTO ${table}`)&&params[1]==='first'){entered.resolve();await release.promise;}};
  const call=p.q.completeAndEnqueue(p.lease,next(),async tx=>{saved=tx;await effect(tx);});
  try{
    await entered.promise;const n=p.db.statements.length;
    await assert.rejects(saved.execute("INSERT INTO effects VALUES(2,'late')"),{code:'ERR_FSQLITE_JOB_SCOPE_ENDED'});
    assert.equal(p.db.statements.length,n);release.resolve();await call;assert.equal(p.db.rows().length,1);
  }finally{release.resolve();await Promise.allSettled([call]);p.db.close();}
});
for(const api of ['execute','query'])test(`admitted unawaited ${api} settles before any continuation is visible`,async()=>{
  const p=await setup(),entered=gate(),release=gate();let child,settled=false;
  p.db.beforeSql=async sql=>{if(sql.startsWith('INSERT INTO effects')){entered.resolve();await release.promise;}};
  const call=p.q.completeAndEnqueue(p.lease,next(),async tx=>{child=tx[api]("INSERT INTO effects VALUES(1,'applied')"+(api==='query'?' RETURNING *':''));});
  void call.then(()=>{settled=true;},()=>{settled=true;});
  try{
    await entered.promise;await tick();assert.equal(settled,false);
    assert.equal(p.db.rows(`SELECT count(*) AS n FROM ${table}`)[0].n,1);
    release.resolve();await call;await child;assert.equal(p.db.rows().length,1);
  }finally{release.resolve();await Promise.allSettled([call,child]);p.db.close();}
});
test('caught SQL failure prevents both continuation and completion',async()=>{
  const p=await setup();
  try{await assert.rejects(p.q.completeAndEnqueue(p.lease,next(),async tx=>{await effect(tx);await effect(tx).catch(()=>{});}),/UNIQUE/);await unchanged(p);}
  finally{p.db.close();}
});
test('outer savepoint rollback undoes a returned continuation result',async()=>{
  const p=await setup();
  try{
    await assert.rejects(p.db.transaction(async()=>{
      assert.equal((await p.q.completeAndEnqueue(p.lease,next(),effect)).jobs.length,2);throw new Error('outer rollback');
    }),/outer rollback/);await unchanged(p);
  }finally{p.db.close();}
});
test('deferred commit failure rolls back parent, children and business SQL',async()=>{
  const p=await setup();
  p.db.db.exec('CREATE TABLE p(id PRIMARY KEY); CREATE TABLE c(id PRIMARY KEY,p REFERENCES p DEFERRABLE INITIALLY DEFERRED);');
  try{
    await assert.rejects(p.q.completeAndEnqueue(p.lease,next(),async tx=>{await effect(tx);await tx.execute('INSERT INTO c VALUES(1,9)');}),/FOREIGN KEY/);
    await unchanged(p);
  }finally{p.db.close();}
});
test('caller changes during admission cannot replace retained follow-up inputs',async()=>{
  const p=await setup();const children=next();
  const original=p.db.transaction.bind(p.db);
  p.db.transaction=async work=>{
    children[0].queue='wrong';children[0].id='wrong';children[0].payload='changed';children.pop();
    return original(work);
  };
  try{
    const r=await p.q.completeAndEnqueue(p.lease,children,effect);
    assert.deepEqual(r.jobs.map(j=>j.job.id),['first','second']);assert.equal(r.jobs[0].job.queue,'next');assert.equal(r.jobs[0].job.payload,'one');
  }finally{p.db.close();}
});
test('input limits, duplicate identities and direct self-continuation fail before SQL',async()=>{
  const p=await setup();let calls=0;
  try{
    for(const children of [[],Array(129).fill(next()[0]),[next()[0],next()[0]],
      [{queue:'parents',id:'parent',payload:'do work'}],[{queue:'',id:'x',payload:'x'}],
      [{queue:'next',id:'x',payload:'x'.repeat(1024*1024+1)}],
      Array.from({length:5},(_,i)=>({queue:'next',id:`large-${i}`,payload:'x'.repeat(1024*1024)}))]){
      p.db.statements.length=0;
      await assert.rejects(p.q.completeAndEnqueue(p.lease,children,async()=>{calls++;}));assert.equal(p.db.statements.length,0);
    }
    assert.equal(calls,0);await unchanged(p);
  }finally{p.db.close();}
});
test('exact 4 MiB total payload budget and same IDs in different queues are supported',async()=>{
  const p=await setup();
  try{
    const children=Array.from({length:4},(_,i)=>({queue:`queue-${i}`,id:'same-id',payload:'x'.repeat(1024*1024)}));
    const r=await p.q.completeAndEnqueue(p.lease,children,effect);
    assert.equal(r.jobs.length,4);assert.equal(r.jobs.reduce((n,j)=>n+j.job.payload.length,0),4*1024*1024);
  }finally{p.db.close();}
});
test('lost commit response leaves all children and cannot repeat the parent callback',async()=>{
  const p=await setup();let calls=0;
  try{
    p.db.afterCommit=()=>{throw new Error('lost response');};
    await assert.rejects(p.q.completeAndEnqueue(p.lease,next(),async tx=>{calls++;await effect(tx);}),/lost response/);
    p.db.afterCommit=null;
    await assert.rejects(p.q.completeAndEnqueue(p.lease,next(),async()=>{calls++;}),{code:'ERR_FSQLITE_JOB_LEASE_LOST'});
    assert.equal(calls,1);assert.equal(p.db.rows(`SELECT count(*) AS n FROM ${table}`)[0].n,3);
    assert.equal((await p.q.get('parent')).state,'completed');
  }finally{p.db.close();}
});
test('independent WAL reader sees no committed prefix and retains its old snapshot',async()=>{
  const path=join(mkdtempSync(join(tmpdir(),'jobs-reader-')),'jobs.db');
  const p=await setup(path,'PRAGMA journal_mode=WAL;');const reader=new JobSqliteTarget(path);let inspected=0;
  reader.db.exec('BEGIN');reader.rows(`SELECT * FROM ${table}`);
  p.db.afterSql=(sql,params)=>{
    if(sql.includes(`INSERT INTO ${table}`)&&params[1]==='first'){
      inspected++;assert.equal(reader.rows(`SELECT state FROM ${table} WHERE job_id='parent'`)[0].state,'leased');
      assert.equal(reader.rows(`SELECT count(*) AS n FROM ${table}`)[0].n,1);assert.deepEqual(reader.rows(),[]);
    }
  };
  try{
    await p.q.completeAndEnqueue(p.lease,next(),effect);assert.equal(inspected,1);
    assert.equal(reader.rows(`SELECT count(*) AS n FROM ${table}`)[0].n,1);
    reader.db.exec('COMMIT');assert.equal(reader.rows(`SELECT count(*) AS n FROM ${table}`)[0].n,3);assert.equal(reader.rows().length,1);
  }finally{reader.close();p.db.close();}
});
test('competing file-backed completion owners cannot both publish successors',async()=>{
  const path=join(mkdtempSync(join(tmpdir(),'jobs-compete-')),'jobs.db');
  const p=await setup(path,'PRAGMA journal_mode=WAL;PRAGMA busy_timeout=0;');
  const other=new JobSqliteTarget(path,'PRAGMA busy_timeout=0;');
  const q=await DurableJobQueue.open(other,'parents',{clock:()=>100});const entered=gate(),release=gate();let second=0;
  const first=p.q.completeAndEnqueue(p.lease,next(),async tx=>{entered.resolve();await release.promise;return effect(tx);});
  try{
    await entered.promise;
    await assert.rejects(q.completeAndEnqueue(p.lease,next(),async()=>{second++;}),/locked|busy/);
    assert.equal(second,0);release.resolve();await first;
    await assert.rejects(q.completeAndEnqueue(p.lease,next(),async()=>{second++;}),{code:'ERR_FSQLITE_JOB_LEASE_LOST'});
    assert.equal(second,0);assert.equal(other.rows(`SELECT count(*) AS n FROM ${table}`)[0].n,3);
  }finally{release.resolve();await Promise.allSettled([first]);other.close();p.db.close();}
});

async function killAt(path,lease,cut){
  const file=fileURLToPath(new URL('./helpers/durable-continuation-child.mjs',import.meta.url));
  await new Promise((resolve,reject)=>{
    const child=spawn(process.execPath,['--experimental-transform-types',file,path,JSON.stringify(lease),cut],{stdio:['ignore','ignore','pipe','ipc']});
    let reached=false,stderr='';
    const timer=setTimeout(()=>{child.kill('SIGKILL');reject(new Error(`watchdog, did not reach ${cut}: ${stderr}`));},10000);
    child.stderr.on('data',data=>{stderr+=data;});
    child.on('message',message=>{if(message.cut===cut){reached=true;child.kill('SIGKILL');}});
    child.on('error',error=>{clearTimeout(timer);reject(error);});
    child.on('exit',(code,signal)=>{clearTimeout(timer);if(reached&&signal==='SIGKILL')resolve();else reject(new Error(`unexpected exit ${code}/${signal}: ${stderr}`));});
  });
}
for(const journal of ['WAL','DELETE'])for(const cut of ['first-child','last-child','parent-completed','after-commit'])
  test(`${journal}: SIGKILL at ${cut} recovers all-or-none continuation`,async()=>{
    const path=join(mkdtempSync(join(tmpdir(),'jobs-kill-')),'jobs.db');
    const p=await setup(path,`PRAGMA journal_mode=${journal};`);const lease=p.lease;p.db.close();
    await killAt(path,lease,cut);
    const db=new JobSqliteTarget(path);const q=await DurableJobQueue.open(db,'parents',{clock:()=>100});
    try{
      const committed=cut==='after-commit';
      assert.equal((await q.get('parent')).state,committed?'completed':'leased');
      assert.equal(db.rows().length,committed?1:0);assert.equal(db.rows(`SELECT count(*) AS n FROM ${table}`)[0].n,committed?3:1);
      let calls=0;const resume=()=>q.completeAndEnqueue(lease,next(),async tx=>{calls++;await effect(tx);});
      if(committed){await assert.rejects(resume(),{code:'ERR_FSQLITE_JOB_LEASE_LOST'});assert.equal(calls,0);}
      else{await resume();assert.equal(calls,1);}
      assert.equal((await q.get('parent')).state,'completed');assert.equal(db.rows().length,1);
      assert.equal(db.rows(`SELECT count(*) AS n FROM ${table}`)[0].n,3);
      assert.equal(db.rows('PRAGMA integrity_check')[0].integrity_check,'ok');
    }finally{db.close();}
  });
