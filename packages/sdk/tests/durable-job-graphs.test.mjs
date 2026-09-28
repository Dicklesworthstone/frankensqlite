import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtempSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { DurableJobQueue, DURABLE_JOBS_TABLE } from '../src/durable-jobs.ts';
import { DurableJobWorker } from '../src/durable-job-worker.ts';
import { JobSqliteTarget, gate } from './helpers/durable-jobs-sqlite-target.mjs';

const table=`main."${DURABLE_JOBS_TABLE}"`;
const edges='main.__fsqlite_job_dependencies_v1';
const ref=(id,queue='q')=>({queue,id});
const job=(id,dependsOn=[],queue='q')=>({id,payload:id,queue,dependsOn});
const graph=()=>[job('join',[ref('a'),ref('b')]),job('b'),job('a')];
const file=()=>join(mkdtempSync(join(tmpdir(),'job-graph-')),'jobs.db');
async function setup(path=':memory:',ddl=''){
  const db=new JobSqliteTarget(path,ddl+';CREATE TABLE IF NOT EXISTS effects(id INTEGER PRIMARY KEY,value TEXT)');
  const q=await DurableJobQueue.open(db,'q',{clock:()=>100});return{db,q};
}
async function finish(q){const lease=await q.claim('w');assert(lease);await q.complete(lease);return lease.id;}

test('enqueueBatch publishes an unordered DAG atomically and returns caller order',async()=>{
  const{db,q}=await setup();try{
    const result=await q.enqueueBatch(graph());assert.deepEqual(result.map(r=>r.job.id),['join','b','a']);
    assert(result.every(r=>r.inserted));assert(Object.isFrozen(result));
    assert.equal((await q.stats()).available,2);assert.equal(await finish(q),'a');assert.equal(await finish(q),'b');assert.equal(await finish(q),'join');
  }finally{db.close();}
});
test('cross-queue graph resolves forward references without confusing equal ids',async()=>{
  const{db,q}=await setup();try{
    await q.enqueueBatch([job('join',[ref('same','left'),ref('same','right')]),job('same',[],'right'),job('same',[],'left')]);
    assert.equal(await q.claim('w'),null);
    for(const name of ['left','right']){const parent=await DurableJobQueue.open(db,name,{clock:()=>100});assert.equal(await finish(parent),'same');}
    assert.equal(await finish(q),'join');
  }finally{db.close();}
});
test('completeAndEnqueue publishes a forward-referenced subgraph and parent effects together',async()=>{
  const{db,q}=await setup();try{
    await q.enqueue({id:'parent',payload:'p'});const lease=await q.claim('w');
    const result=await q.completeAndEnqueue(lease,graph(),tx=>tx.execute("INSERT INTO effects VALUES(1,'parent')"));
    assert.deepEqual(result.jobs.map(r=>r.job.id),['join','b','a']);assert.equal((await q.get('parent')).state,'completed');
    assert.deepEqual(db.rows(),[{id:1,value:'parent'}]);assert.equal((await q.stats()).available,2);
  }finally{db.close();}
});
for(const nodes of [[job('self',[ref('self')])],[job('a',[ref('b')]),job('b',[ref('a')])],
  [job('a',[ref('b','r')]),job('b',[ref('a')],'r')]])
  test(`cycle is rejected before SQL: ${nodes.map(n=>n.queue+':'+n.id).join(',')}`,async()=>{
    const{db,q}=await setup();try{db.statements.length=0;
      await assert.rejects(q.enqueueBatch(nodes),{code:'ERR_FSQLITE_JOB_DEPENDENCY_CYCLE'});assert.deepEqual(db.statements,[]);
    }finally{db.close();}
  });
test('cyclic continuation is rejected before invoking application SQL or changing parent ownership',async()=>{
  const{db,q}=await setup();try{
    await q.enqueue({id:'parent',payload:'p'});const lease=await q.claim('w');let calls=0;db.statements.length=0;
    await assert.rejects(q.completeAndEnqueue(lease,[job('a',[ref('b')]),job('b',[ref('a')])],async()=>{calls++;}),{code:'ERR_FSQLITE_JOB_DEPENDENCY_CYCLE'});
    assert.equal(calls,0);assert.deepEqual(db.statements,[]);assert.equal((await q.get('parent')).state,'leased');
  }finally{db.close();}
});
test('late conflicting existing node rolls back newly inserted roots and dependencies',async()=>{
  const{db,q}=await setup();try{
    await q.enqueue({id:'existing',payload:'original'});
    await assert.rejects(q.enqueueBatch([job('a'),job('join',[ref('a')]),job('existing')]),{code:'ERR_FSQLITE_JOB_ID_CONFLICT'});
    assert.equal((await q.stats()).total,1);assert.equal(await q.get('a'),null);assert.deepEqual(db.rows(`SELECT * FROM ${edges}`),[]);
  }finally{db.close();}
});
test('missing external prerequisite aborts the entire otherwise-valid graph',async()=>{
  const{db,q}=await setup();try{
    await assert.rejects(q.enqueueBatch([job('a'),job('join',[ref('a'),ref('missing')])]),{code:'ERR_FSQLITE_JOB_DEPENDENCY_MISSING'});
    assert.equal((await q.stats()).total,0);assert.deepEqual(db.rows(`SELECT * FROM ${edges}`),[]);
  }finally{db.close();}
});
test('retries deduplicate completed roots without resetting their state or dependencies',async()=>{
  const{db,q}=await setup();try{
    await q.enqueueBatch(graph());await finish(q);const before=await q.get('a');
    const replay=await q.enqueueBatch(graph());assert(replay.every(r=>!r.inserted));assert.deepEqual(await q.get('a'),before);
    assert.equal(await finish(q),'b');assert.equal(await finish(q),'join');
  }finally{db.close();}
});
test('lost graph commit response reconciles all original node identities',async()=>{
  const{db,q}=await setup();try{
    db.afterCommit=()=>{throw new Error('lost graph acknowledgement');};await assert.rejects(q.enqueueBatch(graph()),/lost graph/);
    db.afterCommit=null;assert((await q.enqueueBatch(graph())).every(r=>!r.inserted));
    assert.equal((await q.stats()).total,3);assert.equal(db.rows(`SELECT * FROM ${edges}`).length,2);
  }finally{db.afterCommit=null;db.close();}
});
test('an independent WAL owner cannot claim a prefix while graph publication is paused',async()=>{
  const path=file(),p=await setup(path,'PRAGMA journal_mode=WAL'),peer=await setup(path);
  const entered=gate(),release=gate();let publication;
  try{p.db.afterSql=async(sql,params)=>{if(sql.startsWith(`INSERT INTO ${table}`)&&params[1]==='b'){entered.resolve();await release.promise;}};
    publication=p.q.enqueueBatch(graph());await entered.promise;assert.equal((await peer.q.stats()).total,0);assert.equal(await peer.q.claim('w'),null);
    release.resolve();await publication;assert.equal((await peer.q.stats()).total,3);assert.equal((await peer.q.claim('w')).id,'a');
  }finally{release.resolve();await publication?.catch(()=>{});peer.db.close();p.db.close();}
});
test('graph input and all dependency objects are captured before asynchronous admission',async()=>{
  const{db,q}=await setup(),entered=gate(),release=gate();let delayed=false,publication;
  try{const owner={transaction:async work=>{if(delayed){entered.resolve();await release.promise;}return db.transaction(work);}};
    const guarded=await DurableJobQueue.open(owner,'q',{clock:()=>100});delayed=true;
    const nodes=graph();publication=guarded.enqueueBatch(nodes);await entered.promise;
    nodes[0].dependsOn[0].id='missing';nodes[1].id='mutated';nodes.length=0;release.resolve();
    assert.equal((await publication).length,3);assert.equal((await q.dependencies('join'))[0].id,'a');
  }finally{release.resolve();await publication?.catch(()=>{});db.close();}
});
test('outer rollback and deferred commit failure undo every node and edge',async()=>{
  const{db,q}=await setup();try{
    await assert.rejects(db.transaction(async()=>{await q.enqueueBatch(graph());throw new Error('outer abort');}),/outer abort/);
    assert.equal((await q.stats()).total,0);
    db.db.exec('CREATE TABLE p(id PRIMARY KEY);CREATE TABLE c(id REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)');
    db.beforeCommit=async nested=>{if(!nested)await db.execute('INSERT INTO c VALUES(7)');};
    await assert.rejects(q.enqueueBatch(graph()),/FOREIGN KEY/);db.beforeCommit=null;
    assert.equal((await q.stats()).total,0);assert.deepEqual(db.rows(`SELECT * FROM ${edges}`),[]);
  }finally{db.beforeCommit=null;db.close();}
});
for(const input of [null,[],{},[job('a'),job('a')],[{id:'a',payload:'a'}],Array.from({length:129},(_,i)=>job(String(i)))])
  test(`invalid batch admission rejects before SQL: ${Array.isArray(input)?input.length:typeof input}`,async()=>{
    const{db,q}=await setup();try{db.statements.length=0;await assert.rejects(q.enqueueBatch(input));assert.deepEqual(db.statements,[]);}finally{db.close();}
  });
test('128 reverse-ordered jobs form a bounded chain without recursive traversal',async()=>{
  const{db,q}=await setup();try{
    const nodes=Array.from({length:128},(_,i)=>job(String(i),i?[ref(String(i-1))]:[])).reverse();
    const result=await q.enqueueBatch(nodes);assert.equal(result.length,128);assert.equal(result[0].job.id,'127');
    for(let i=0;i<128;i++)assert.equal(await finish(q),String(i));
    assert.equal((await q.stats()).completed,128);
  }finally{db.close();}
});
test('graph allows exactly 1024 edges and rejects excess before any SQL',async()=>{
  const{db,q}=await setup();try{
    const roots=Array.from({length:33},(_,i)=>job(`r${i}`));
    const children=Array.from({length:32},(_,i)=>job(`c${i}`,roots.slice(0,32).map(r=>ref(r.id))));
    const excessive=[...children.map((c,i)=>i===31?{...c,dependsOn:roots.map(r=>ref(r.id))}:c),...roots];
    db.statements.length=0;await assert.rejects(q.enqueueBatch(excessive),/1024/);assert.deepEqual(db.statements,[]);
    assert.equal((await q.enqueueBatch([...children,...roots])).length,65);assert.equal(db.rows(`SELECT count(*) AS n FROM ${edges}`)[0].n,1024);
  }finally{db.close();}
});
test('batch keeps the existing 4 MiB combined payload limit',async()=>{
  const{db,q}=await setup();try{
    const nodes=Array.from({length:4},(_,i)=>({...job(String(i)),payload:'x'.repeat(1024*1024)}));
    assert.equal((await q.enqueueBatch(nodes)).length,4);db.statements.length=0;
    await assert.rejects(q.enqueueBatch([...nodes,job('too-many-bytes')]),/4 MiB/);assert.deepEqual(db.statements,[]);
  }finally{db.close();}
});
for(const encoding of ['UTF-8','UTF-16le','UTF-16be'])test(`${encoding}: actual worker consumes fan-out/fan-in graph after root completion`,async()=>{
  const{db,q}=await setup(':memory:',`PRAGMA encoding='${encoding}'`);let worker;
  try{await q.enqueue({id:'root',payload:'root'});
    const seen=[];worker=DurableJobWorker.start(q,lease=>({
      ...(lease.id==='root'?{next:graph()}:{}),
      apply:async tx=>{
        if(lease.id==='join')assert.equal((await tx.query('SELECT count(*) AS n FROM effects')).rows[0].n,3);
        await tx.execute('INSERT INTO effects VALUES(?,?)',[seen.length+1,lease.id]);seen.push(lease.id);
      },
    }),{owner:'w',clock:()=>100,stopWhenIdle:true});await worker.done;
    assert.deepEqual(seen,['root','a','b','join']);assert.equal(worker.stats.completed,4);
  }finally{await worker?.stop().catch(()=>{});db.close();}
});
test('worker rejects a cyclic completion graph without executing its apply callback',async()=>{
  const{db,q}=await setup();let worker,calls=0;
  try{await q.enqueue({id:'root',payload:'root',maxAttempts:1});worker=DurableJobWorker.start(q,()=>({
    next:[job('a',[ref('b')]),job('b',[ref('a')])],apply:async()=>{calls++;},
  }),{owner:'w',clock:()=>100,stopWhenIdle:true});await worker.done;
    assert.equal(calls,0);assert.equal((await q.get('root')).state,'dead');assert.equal((await q.stats()).total,1);
  }finally{await worker?.stop().catch(()=>{});db.close();}
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
    assert.equal(timedOut,false,'A watchdog kill is not an observed publication cut');
    assert.equal(marker,true,stderr);assert.equal(result.signal,'SIGKILL');
  }finally{clearTimeout(timer);}
}
for(const mode of ['WAL','DELETE'])for(const cut of ['graph-after-node','graph-after-edge','graph-before-commit','graph-after-commit'])
  test(`${mode}/${cut}: SIGKILL leaves all workflow nodes and edges or none`,async()=>{
    const path=file(),p=await setup(path,`PRAGMA journal_mode=${mode}`);p.db.close();
    await killAt(path,cut);const r=await setup(path);
    try{const committed=cut==='graph-after-commit';assert.equal((await r.q.stats()).total,committed?3:0);
      assert.equal(r.db.rows(`SELECT count(*) AS n FROM ${edges}`)[0].n,committed?2:0);
      const recovered=await r.q.enqueueBatch(graph());assert(recovered.every(result=>result.inserted!==committed));
      assert.equal(await finish(r.q),'a');assert.equal(await finish(r.q),'b');assert.equal(await finish(r.q),'join');
      assert.equal((await r.q.stats()).total,3);
    }finally{r.db.close();}
  });

for(const nodes of [
  [job('\ud800')],
  [job('a',[],'\udfff')],
  [job('a',[ref('\ud800')])],
  [job('a',[ref('parent','\udfff')])],
])test('graph identifiers cannot collapse through malformed-Unicode SQL binding',async()=>{
  const{db,q}=await setup();try{
    db.statements.length=0;await assert.rejects(q.enqueueBatch(nodes),/well-formed Unicode/);assert.deepEqual(db.statements,[]);
  }finally{db.close();}
});
