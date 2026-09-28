import assert from 'node:assert/strict';
import { test } from 'node:test';
import { DurableJobQueue, DURABLE_JOBS_TABLE } from '../src/durable-jobs.ts';
import { JobSqliteTarget, gate, tick } from './helpers/durable-jobs-sqlite-target.mjs';

const TABLE = `main."${DURABLE_JOBS_TABLE}"`;
const EDGES = 'main.__fsqlite_job_dependencies_v1';
const clock = () => 100;
const ref = (id, queue = 'work') => ({ queue, id });
const node = (id, parents = [], extra = {}) => ({
  queue: 'work', id, payload: id, dependsOn: parents.map(id => ref(id)), ...extra,
});
async function fixture(encoding = 'UTF-8') {
  const db = new JobSqliteTarget(':memory:', `PRAGMA encoding='${encoding}';`);
  const queue = await DurableJobQueue.open(db, 'work', { clock });
  return { db, queue };
}

test('terminal failure propagates through a join without consuming descendant attempts', async () => {
  const { db, queue } = await fixture();
  try {
    await queue.enqueueBatch([node('join', ['left', 'right']), node('left', ['root']),
      node('right'), node('root', [], { maxAttempts: 1 })]);
    const root = await queue.claim('owner');
    // Alphabetic priority puts right first; complete it before failing root.
    assert.equal(root.id, 'right'); await queue.complete(root);
    const lease = await queue.claim('owner'); assert.equal(lease.id, 'root');
    await queue.fail(lease, 'permanent failure');
    assert.equal(await queue.cancelBlocked(), 2);
    assert.equal((await queue.get('left')).state, 'cancelled');
    assert.equal((await queue.get('join')).state, 'cancelled');
    assert.equal((await queue.get('join')).attempts, 0);
    assert.equal((await queue.get('right')).state, 'completed');
    assert.equal(await queue.claim('owner'), null);
    assert.equal(await queue.cancelBlocked(), 0);
  } finally { db.close(); }
});

test('a ready retrying parent does not authorize cancellation', async () => {
  const { db, queue } = await fixture();
  try {
    await queue.enqueueBatch([node('root'), node('child', ['root'])]);
    const lease = await queue.claim('owner'); await queue.fail(lease, 'retry', 1000);
    assert.equal(await queue.cancelBlocked(), 0);
    assert.equal((await queue.get('child')).state, 'ready');
    assert.equal((await queue.get('child')).attempts, 0);
  } finally { db.close(); }
});

test('bounded calls resume a reverse-ordered chain without replaying terminal work', async () => {
  const { db, queue } = await fixture();
  try {
    const jobs = Array.from({ length: 7 }, (_, i) => node(`n${6-i}`, i ? [`n${7-i}`] : []));
    await queue.enqueueBatch(jobs); await queue.cancel('n6');
    assert.equal(await queue.cancelBlocked(2), 2);
    assert.equal((await queue.stats()).cancelled, 3);
    assert.equal(await queue.cancelBlocked(3), 3);
    assert.equal(await queue.cancelBlocked(3), 1);
    assert.equal((await queue.stats()).cancelled, 7);
    assert.equal(db.rows(`SELECT sum(attempts) AS n FROM ${TABLE}`)[0].n, 0);
  } finally { db.close(); }
});

import { mkdtempSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { spawn } from 'node:child_process';
const freshPath = () => join(mkdtempSync(join(tmpdir(), 'failed-workflow-')), 'jobs.db');
async function failedBranch(db, size = 2) {
  const queue = await DurableJobQueue.open(db, 'work', { clock });
  await queue.enqueueBatch([node('root'), ...Array.from({ length: size }, (_, i) =>
    node(`child-${i}`, ['root']))]);
  await queue.cancel('root');
  return queue;
}
const states = db => db.rows(`SELECT job_id,state,attempts,last_error FROM ${TABLE} ORDER BY job_id`);

for (const encoding of ['UTF-8','UTF-16le','UTF-16be']) {
  for (const mode of ['WAL','DELETE']) test(`${mode}/${encoding}: cancellation and exact dedup survive reopen`, async () => {
    const path = freshPath();
    const db = new JobSqliteTarget(path, `PRAGMA encoding='${encoding}';PRAGMA journal_mode=${mode};`);
    try {
      const queue = await failedBranch(db);
      const edgeBytes = db.rows(`SELECT * FROM ${EDGES}`);
      assert.equal(await queue.cancelBlocked(), 2);
      assert.deepEqual(db.rows(`SELECT * FROM ${EDGES}`), edgeBytes);
    } finally { db.close(); }
    const reopened = new JobSqliteTarget(path);
    try {
      const queue = await DurableJobQueue.open(reopened, 'work', { clock });
      assert.equal(await queue.cancelBlocked(), 0);
      const result = await queue.enqueue(node('child-0', ['root']));
      assert.equal(result.inserted, false); assert.equal(result.job.state, 'cancelled');
      assert.equal(result.job.attempts, 0);
      assert.equal(await queue.claim('worker'), null);
      assert.match(result.job.lastError, /prerequisite/);
      assert.equal(reopened.rows('PRAGMA integrity_check')[0].integrity_check, 'ok');
    } finally { reopened.close(); }
  });
}

for (const parentState of ['ready','leased','completed','missing']) {
  test(`parent ${parentState} is not terminal failure evidence`, async () => {
    const { db, queue } = await fixture();
    try {
      await queue.enqueueBatch([node('root'),node('child',['root'])]);
      if (parentState === 'leased' || parentState === 'completed') {
        const lease = await queue.claim('worker');
        if (parentState === 'completed') await queue.complete(lease);
      }
      if (parentState === 'missing')
        db.db.exec(`UPDATE ${TABLE} SET job_id='moved' WHERE job_id='root'`);
      assert.equal(await queue.cancelBlocked(),0);
      assert.equal((await queue.get('child')).state,'ready');
      if (parentState === 'completed') assert.equal((await queue.claim('worker')).id,'child');
    } finally { db.close(); }
  });
}

test('scheduled jobs are cancelled without waiting for their now-impossible schedule', async () => {
  const { db, queue } = await fixture();
  try {
    await queue.enqueueBatch([node('root'),node('child',['root'],{availableAt:100000})]);
    await queue.cancel('root');
    assert.equal(await queue.cancelBlocked(),1);
    assert.equal((await queue.get('child')).availableAt,100000);
  } finally { db.close(); }
});

test('cross-queue cascades advance only through explicitly swept queues', async () => {
  const { db, queue } = await fixture();
  try {
    const other = await DurableJobQueue.open(db,'other',{clock});
    await queue.enqueueBatch([node('root'),node('middle',['root'],{queue:'other'}),
      node('leaf',[],{dependsOn:[ref('middle','other')]}), node('independent')]);
    await queue.cancel('root');
    assert.equal(await queue.cancelBlocked(),0);
    assert.equal(await other.cancelBlocked(),1);
    assert.equal(await queue.cancelBlocked(),1);
    assert.equal((await queue.get('independent')).state,'ready');
    assert.equal((await queue.get('leaf')).attempts,0);
  } finally { db.close(); }
});

test('one failed join input cancels only descendants, not its live sibling', async () => {
  const { db, queue } = await fixture();
  try {
    await queue.enqueueBatch([node('left'),node('right'),node('join',['left','right'])]);
    const left=await queue.claim('worker'); assert.equal(left.id,'left');
    await queue.cancel('right');
    assert.equal(await queue.cancelBlocked(),1);
    assert.equal((await queue.get('left')).state,'leased');
    await queue.complete(left);
    assert.equal((await queue.get('join')).state,'cancelled');
  } finally { db.close(); }
});

test('expired final-attempt parents become terminal only after expiry recovery', async () => {
  const db=new JobSqliteTarget(); let now=100;
  try {
    const queue=await DurableJobQueue.open(db,'work',{clock:()=>now});
    await queue.enqueueBatch([node('root',[],{maxAttempts:1}),node('child',['root'])]);
    await queue.claim('crashed-worker',10); now=110;
    assert.equal(await queue.cancelBlocked(),0);
    assert.equal(await queue.reapExpired(),1);
    assert.equal(await queue.cancelBlocked(),1);
    assert.equal((await queue.get('root')).state,'dead');
  } finally {db.close();}
});

for (const limit of [0,-1,1.5,1001,NaN,Infinity,'1',null]) {
  test(`invalid cancellation limit ${String(limit)} rejects before admission`, async () => {
    const {db,queue}=await fixture();
    try {
      const count=db.serial;
      await assert.rejects(queue.cancelBlocked(limit));
      assert.equal(db.serial,count);
    } finally {db.close();}
  });
}

test('65 eligible jobs use bounded pages without transferring payloads', async () => {
  const db=new JobSqliteTarget();
  try {
    const queue=await failedBranch(db,65); const pageSizes=[];
    const query=db.query.bind(db);
    db.query=async(sql,params)=>{
      const result=await query(sql,params);
      if(sql.startsWith('SELECT job_id FROM')){
        pageSizes.push(result.rows.length);
        assert(result.rows.every(r=>Object.keys(r).join(',')==='job_id'));
      }
      return result;
    };
    db.statements.length=0;
    assert.equal(await queue.cancelBlocked(65),65);
    assert.deepEqual(pageSizes,[32,32,1]);
    assert(!db.statements.some(s=>/^SELECT \*/.test(s)||s.includes('SELECT payload')));
  } finally {db.close();}
});

for(const failure of ['statement','false-count','ignored-write','commit']) {
  test(`${failure}: no cancelled prefix survives failure`, async()=>{
    const db=new JobSqliteTarget();
    try {
      const queue=await failedBranch(db); const before=states(db);
      if(failure==='statement') {
        let n=0; db.beforeSql=sql=>{if(sql.startsWith(`UPDATE ${TABLE} AS candidate`)&&++n===2)throw new Error('statement failed');};
      } else if(failure==='false-count') {
        const execute=db.execute.bind(db);
        db.execute=async(sql,params)=>{const n=await execute(sql,params);return sql.startsWith(`UPDATE ${TABLE} AS candidate`)?0:n;};
      } else if(failure==='ignored-write') {
        db.db.exec(`CREATE TRIGGER ignore_cancel BEFORE UPDATE OF state ON "${DURABLE_JOBS_TABLE}"
          WHEN NEW.job_id='child-1' AND NEW.state='cancelled' BEGIN SELECT RAISE(IGNORE); END`);
      } else {
        db.db.exec('CREATE TABLE fk_parent(id PRIMARY KEY);CREATE TABLE fk_child(id REFERENCES fk_parent(id) DEFERRABLE INITIALLY DEFERRED);');
        db.beforeCommit=()=>db.db.exec('INSERT INTO fk_child VALUES(99)');
      }
      await assert.rejects(queue.cancelBlocked());
      assert.deepEqual(states(db),before);
    } finally {db.close();}
  });
}

test('an enclosing rollback undoes a successful provisional sweep', async()=>{
  const db=new JobSqliteTarget();
  try {
    const queue=await failedBranch(db),before=states(db);
    await assert.rejects(db.transaction(async()=>{assert.equal(await queue.cancelBlocked(),2);throw new Error('outer rollback');}),/outer rollback/);
    assert.deepEqual(states(db),before);
  } finally {db.close();}
});

test('lost COMMIT response reconciles from terminal state without repeated effects',async()=>{
  const path=freshPath(),db=new JobSqliteTarget(path);
  try {
    const queue=await failedBranch(db);
    db.afterCommit=()=>{throw new Error('lost response');};
    await assert.rejects(queue.cancelBlocked(),/lost response/);
  } finally {db.close();}
  const reopen=new JobSqliteTarget(path);
  try {
    const queue=await DurableJobQueue.open(reopen,'work',{clock});
    assert.equal(await queue.cancelBlocked(),0); assert.equal((await queue.stats()).cancelled,3);
  } finally {reopen.close();}
});

test('incompatible or missing dependency storage is not recreated by cancellation',async()=>{
  const {db,queue}=await fixture();
  try {
    await queue.enqueueBatch([node('root'),node('child',['root'])]);await queue.cancel('root');
    db.db.exec(`ALTER TABLE ${EDGES} RENAME TO saved_dependencies`);
    await assert.rejects(queue.cancelBlocked(),{code:'ERR_FSQLITE_JOB_SCHEMA'});
    assert.equal(db.rows("SELECT count(*) AS n FROM main.sqlite_schema WHERE name='__fsqlite_job_dependencies_v1'")[0].n,0);
    assert.equal((await queue.get('child')).state,'ready');
  } finally {db.close();}
});

test('TEMP shadows cannot fabricate a terminal parent or receive cancellation',async()=>{
  const {db,queue}=await fixture();
  try {
    await queue.enqueueBatch([node('root'),node('child',['root'])]);
    db.db.exec(`CREATE TEMP TABLE "${DURABLE_JOBS_TABLE}" AS SELECT * FROM ${TABLE};
      UPDATE temp."${DURABLE_JOBS_TABLE}" SET state='dead' WHERE job_id='root';
      CREATE TEMP TABLE __fsqlite_job_dependencies_v1 AS SELECT * FROM ${EDGES};`);
    assert.equal(await queue.cancelBlocked(),0);
    await queue.cancel('root'); assert.equal(await queue.cancelBlocked(),1);
    assert.equal(db.rows(`SELECT state FROM temp."${DURABLE_JOBS_TABLE}" WHERE job_id='child'`)[0].state,'ready');
  } finally {db.close();}
});

test('independent WAL readers see all cancellations at one commit boundary',async()=>{
  const path=freshPath(), db=new JobSqliteTarget(path,'PRAGMA journal_mode=WAL;');
  const reader=new JobSqliteTarget(path); const entered=gate(),release=gate();
  try {
    const queue=await failedBranch(db),before=states(reader);let n=0;
    db.afterSql=async sql=>{if(sql.startsWith(`UPDATE ${TABLE} AS candidate`)&&++n===1){entered.resolve();await release.promise;}};
    const pending=queue.cancelBlocked(); await entered.promise;
    assert.deepEqual(states(reader),before);
    release.resolve();assert.equal(await pending,2);
    assert(states(reader).every(r=>r.state==='cancelled'));
  } finally {release.resolve();reader.close();db.close();}
});

test('competing file-backed sweepers reconcile without a global lock or double count',async()=>{
  const path=freshPath(),db=new JobSqliteTarget(path,'PRAGMA journal_mode=WAL;');
  const peer=new JobSqliteTarget(path);const entered=gate(),release=gate();
  try {
    const queue=await failedBranch(db),other=await DurableJobQueue.open(peer,'work',{clock});
    let pause=true;
    db.beforeSql=async sql=>{if(pause&&sql.startsWith(`UPDATE ${TABLE} AS candidate`)){pause=false;entered.resolve();await release.promise;}};
    const pending=queue.cancelBlocked(); const outcome=pending.then(value=>({value}),error=>({error}));
    await entered.promise;assert.equal(await other.cancelBlocked(),2);release.resolve();
    const result=await outcome;assert(result.error);assert.match(result.error.message,/locked|busy/i);
    assert.equal(await queue.cancelBlocked(),0);
    assert.equal((await queue.stats()).cancelled,3);
  } finally {release.resolve();peer.close();db.close();}
});

for(const mode of ['WAL','DELETE'])for(const cut of ['first-update','last-update','before-commit','after-commit']) {
  test(`${mode}: actual SIGKILL at ${cut} preserves all-or-none propagation`,{timeout:15000},async()=>{
    const path=freshPath(); const db=new JobSqliteTarget(path,`PRAGMA journal_mode=${mode};`);
    try {await failedBranch(db);}finally{db.close();}
    const child=spawn(process.execPath,[...process.execArgv,
      new URL('./helpers/failed-workflow-child.mjs',import.meta.url).pathname,path,cut],{stdio:['ignore','pipe','pipe','ipc']});
    let output='',reached=false;child.stderr.on('data',x=>output+=x);
    const timer=setTimeout(()=>child.kill('SIGKILL'),10000);
    child.on('message',message=>{if(message===cut){reached=true;child.kill('SIGKILL');}});
    const exit=await new Promise((resolve,reject)=>{child.once('error',reject);child.once('exit',(code,signal)=>resolve({code,signal}));});
    clearTimeout(timer);assert(reached,output);assert.equal(exit.signal,'SIGKILL');
    const reopen=new JobSqliteTarget(path);
    try {
      const queue=await DurableJobQueue.open(reopen,'work',{clock});
      const committed=cut==='after-commit';
      assert.equal((await queue.stats()).cancelled,committed?3:1);
      assert.equal(await queue.cancelBlocked(),committed?0:2);
      assert.equal(await queue.cancelBlocked(),0);
      assert.equal(reopen.rows('PRAGMA integrity_check')[0].integrity_check,'ok');
    }finally{reopen.close();}
  });
}
