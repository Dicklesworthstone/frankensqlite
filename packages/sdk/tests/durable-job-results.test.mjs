import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtempSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { DurableJobQueue, DURABLE_JOBS_TABLE } from '../src/durable-jobs.ts';
import { JobSqliteTarget, gate, tick } from './helpers/durable-jobs-sqlite-target.mjs';

const table = `main."${DURABLE_JOBS_TABLE}"`;
const dependencyTable = 'main.__fsqlite_job_dependencies_v1';
const MiB = 1024 * 1024;
const bodyRead = sql => sql.includes('AS result_data');
const metadataRead = sql => sql.includes('AS result_bytes');
const code = value => ({ code: value });
async function fixture(t, { encoding = 'UTF-8', values = ['first', 'second'], path = ':memory:' } = {}) {
  const db = new JobSqliteTarget(path, `PRAGMA encoding='${encoding}'; PRAGMA journal_mode=WAL;`);
  t.after(() => db.close());
  let now = 100;
  const clock = () => now;
  const q = await DurableJobQueue.open(db, 'join', { clock });
  const parents = await DurableJobQueue.open(db, 'parents', { clock });
  const refs = values.map((_, i) => ({ queue: 'parents', id: `p${String(i).padStart(3, '0')}` }));
  for (let i = 0; i < values.length; i++) {
    await parents.enqueue({ id: refs[i].id, payload: 'unused parent payload' });
    await parents.complete(await parents.claim('producer'), values[i]);
  }
  await q.enqueue({ id: 'join', payload: 'unused child payload', dependsOn: refs });
  const lease = await q.claim('consumer', 1000);
  return { db, q, parents, refs, lease, clock, time: n => { now = n; } };
}

for (const encoding of ['UTF-8', 'UTF-16le', 'UTF-16be']) {
  test(`${encoding}: completed parent results retain NULL, empty, BOM, NUL and Unicode`, async t => {
    const values = [null, '', '\ufeffBOM\0tail', '日本語😀', 'plain'];
    const { q, db, lease, refs } = await fixture(t, { encoding, values });
    const result = await q.dependencyResults(lease);
    assert.equal(Object.isFrozen(result), true);
    assert.deepEqual(result.map(r => r.result), values);
    assert.deepEqual(result.map(({queue,id}) => ({queue,id})), refs);
    for (let i = 0; i < result.length; i++) {
      assert.equal(Object.isFrozen(result[i]), true);
      const stored = db.rows(`SELECT coalesce(length(CAST(result AS BLOB)),0) AS n FROM ${table} WHERE queue_name='parents' AND job_id=?`, [refs[i].id])[0].n;
      assert.equal(result[i].byteLength, stored);
    }
    assert.equal((await q.get('join')).attempts, 1);
    assert.equal((await q.get('join')).state, 'leased');
  });
  test(`${encoding}: exact stored-byte quota succeeds and one byte less loads no bodies`, async t => {
    const {q,db,lease} = await fixture(t, {encoding,values:['hello','😀']});
    const required = db.rows(`SELECT sum(length(CAST(result AS BLOB))) AS n FROM ${table} WHERE queue_name='parents'`)[0].n;
    db.statements.length = 0;
    await assert.rejects(q.dependencyResults(lease,{maxBytes:required-1}),code('ERR_FSQLITE_JOB_RESULT_LIMIT'));
    assert.equal(db.statements.some(bodyRead),false);
    assert.deepEqual((await q.dependencyResults(lease,{maxBytes:required})).map(r=>r.result),['hello','😀']);
  });
  test(`${encoding}: malformed encoded parent result is not replacement-decoded`, async t => {
    const {q,db,lease} = await fixture(t,{encoding,values:['initial']});
    const invalid = encoding === 'UTF-8' ? "X'ff'" : encoding === 'UTF-16le' ? "X'00d8'" : "X'd800'";
    db.db.exec(`UPDATE ${table} SET result=CAST(${invalid} AS TEXT) WHERE queue_name='parents'`);
    await assert.rejects(q.dependencyResults(lease),code('ERR_FSQLITE_JOB_CORRUPT'));
  });
}

test('a root job has no result inputs; zero-byte quota distinguishes empty and NULL', async t => {
  const {q,lease} = await fixture(t,{values:[]});
  assert.deepEqual(await q.dependencyResults(lease,{maxBytes:0}),[]);
  const f = await fixture(t,{values:['',null]});
  assert.deepEqual((await f.q.dependencyResults(f.lease,{maxBytes:0})).map(r=>r.result),['',null]);
});

test('all 128 parents read in canonical identity order without parent payloads', async t => {
  const values = Array.from({length:128},(_,i)=>String(i));
  const {q,db,lease} = await fixture(t,{values});
  db.statements.length = 0;
  assert.deepEqual((await q.dependencyResults(lease)).map(r=>r.result),values);
  assert.equal(db.statements.filter(bodyRead).length,128);
  assert.equal(db.statements.filter(metadataRead).length,128);
  assert(!db.statements.some(sql=>/SELECT \*|SELECT payload|last_error/.test(sql)));
  assert(!db.statements.some(sql=>/^\s*(INSERT|UPDATE|DELETE|CREATE)/.test(sql)));
});

test('later oversized parent rejects the complete join before reading the first body', async t => {
  const {q,db,lease} = await fixture(t,{values:['small','large']});
  db.db.exec(`UPDATE ${table} SET result=CAST(zeroblob(${3*MiB}) AS TEXT) WHERE job_id='p001'`);
  db.statements.length = 0;
  await assert.rejects(q.dependencyResults(lease),code('ERR_FSQLITE_JOB_CORRUPT'));
  assert(!db.statements.some(bodyRead));
});

test('a body within raw UTF-16 ceiling but beyond the UTF-8 result limit rejects', async t => {
  const {q,db,lease} = await fixture(t,{values:['small']});
  db.db.exec(`UPDATE ${table} SET result=CAST(zeroblob(${MiB+1}) AS TEXT) WHERE job_id='p000'`);
  await assert.rejects(q.dependencyResults(lease),code('ERR_FSQLITE_JOB_CORRUPT'));
});

for (const state of ['ready','leased','dead','cancelled','missing']) {
  test(`an incomplete ${state} parent returns no partial result`, async t => {
    const {q,db,lease} = await fixture(t);
    if (state === 'missing') db.db.exec(`DELETE FROM ${table} WHERE queue_name='parents' AND job_id='p001'`);
    else if (state === 'leased') db.db.exec(`UPDATE ${table} SET state='leased',lease_owner='x',lease_token='x',lease_expires_at=9999 WHERE queue_name='parents' AND job_id='p001'`);
    else db.db.exec(`UPDATE ${table} SET state='${state}' WHERE queue_name='parents' AND job_id='p001'`);
    db.statements.length = 0;
    await assert.rejects(q.dependencyResults(lease),code('ERR_FSQLITE_JOB_DEPENDENCY_INCOMPLETE'));
    assert(!db.statements.some(bodyRead));
  });
}

for (const field of ['id','owner','token','attempt','queue']) {
  test(`a mismatched ${field} lease cannot read results`, async t => {
    const {q,db,lease} = await fixture(t);
    db.statements.length = 0;
    await assert.rejects(q.dependencyResults({...lease,[field]:field==='attempt'?2:'wrong'}));
    assert(!db.statements.some(metadataRead));
  });
}

test('expired and reclaimed receipts cannot read; current replacement receipt can', async t => {
  const {q,db,lease,time} = await fixture(t);
  time(1100);
  await assert.rejects(q.dependencyResults(lease),code('ERR_FSQLITE_JOB_LEASE_LOST'));
  const next = await q.claim('consumer',1000);
  assert.notEqual(next.token,lease.token);
  db.statements.length = 0;
  await assert.rejects(q.dependencyResults(lease),code('ERR_FSQLITE_JOB_LEASE_LOST'));
  assert(!db.statements.some(metadataRead));
  assert.equal((await q.dependencyResults(next)).length,2);
});

test('expiry while reading drains the SQL operation before rejecting', async t => {
  const {q,db,lease,time} = await fixture(t);
  const entered=gate(), release=gate(); let settled=false;
  db.beforeSql=async sql=>{if(bodyRead(sql)){entered.resolve();await release.promise;}};
  const pending=q.dependencyResults(lease).finally(()=>{settled=true;});
  const verdict=assert.rejects(pending,code('ERR_FSQLITE_JOB_LEASE_LOST'));
  await entered.promise; time(1100); await tick(); assert.equal(settled,false);
  release.resolve(); await verdict; assert.equal(db.depth,0);
});

for (const mode of ['cancel','timeout']) test(`${mode} during an admitted read waits for SQL cleanup`, async t => {
  const {q,db,lease}=await fixture(t); const controller=new AbortController();
  const entered=gate(),release=gate(); let settled=false;
  db.beforeSql=async sql=>{if(metadataRead(sql)){entered.resolve();await release.promise;}};
  const pending=q.dependencyResults(lease,mode==='cancel'?{signal:controller.signal}:{timeoutMs:20}).finally(()=>{settled=true;});
  const verdict=assert.rejects(pending);
  await entered.promise;
  if(mode==='cancel')controller.abort(new Error('cancel reading'));
  else await new Promise(resolve=>setTimeout(resolve,40));
  assert.equal(settled,false); release.resolve(); await verdict;
  assert.equal(db.depth,0); assert(!db.statements.some(bodyRead));
});

test('caller mutation during admission cannot substitute a different receipt or quota', async t => {
  const {q,db,lease}=await fixture(t); const entered=gate(),release=gate();
  let once=true; db.beforeSql=async()=>{if(once){once=false;entered.resolve();await release.promise;}};
  const input={...lease}, options={maxBytes:11}; const pending=q.dependencyResults(input,options);
  await entered.promise; input.id='other'; input.owner='other'; options.maxBytes=0; release.resolve();
  assert.equal((await pending).length,2);
});

for (const options of [{maxBytes:-1},{maxBytes:64*MiB+1},{maxBytes:1.5},{maxBytes:NaN},{timeoutMs:0},{signal:{}},null])
  test(`invalid result controls reject before SQL: ${JSON.stringify(options)}`,async t=>{
    const {q,db,lease}=await fixture(t); db.statements.length=0;
    await assert.rejects(q.dependencyResults(lease,options)); assert.deepEqual(db.statements,[]);
  });

test('same-named TEMP and attached job/dependency tables cannot replace result authority',async t=>{
  const {q,db,lease}=await fixture(t);
  db.db.exec(`CREATE TEMP TABLE "${DURABLE_JOBS_TABLE}" AS SELECT * FROM ${table};
    UPDATE temp."${DURABLE_JOBS_TABLE}" SET result='wrong';
    CREATE TEMP TABLE __fsqlite_job_dependencies_v1 AS SELECT * FROM ${dependencyTable};
    ATTACH ':memory:' AS other;
    CREATE TABLE other."${DURABLE_JOBS_TABLE}" AS SELECT * FROM temp."${DURABLE_JOBS_TABLE}";`);
  assert.deepEqual((await q.dependencyResults(lease)).map(r=>r.result),['first','second']);
});

test('missing dependency storage rejects without repairing it or treating join as a root',async t=>{
  const {q,db,lease}=await fixture(t);
  db.db.exec(`ALTER TABLE ${dependencyTable} RENAME TO saved_dependencies`);
  await assert.rejects(q.dependencyResults(lease),code('ERR_FSQLITE_JOB_SCHEMA'));
  assert.equal(db.rows("SELECT count(*) AS n FROM main.sqlite_schema WHERE name='__fsqlite_job_dependencies_v1'")[0].n,0);
});

test('a changed result cannot escape the size preflight through the later projection',async t=>{
  const {q,db,lease}=await fixture(t); let changed=false;
  db.beforeSql=async sql=>{if(bodyRead(sql)&&!changed){changed=true;db.db.exec(`UPDATE ${table} SET result=CAST(zeroblob(${3*MiB}) AS TEXT) WHERE queue_name='parents' AND job_id='p000'`);}};
  await assert.rejects(q.dependencyResults(lease),code('ERR_FSQLITE_JOB_CORRUPT'));
  assert.equal(db.rows(`SELECT length(result) AS n FROM ${table} WHERE queue_name='parents' AND job_id='p000'`)[0].n,5,'same-transaction mutation rolled back');
});

test('WAL snapshot stays coherent when another connection changes parent output',async t=>{
  const path=join(mkdtempSync(join(tmpdir(),'job-results-')),'db.sqlite');
  const {q,db,lease}=await fixture(t,{path}); const peer=new JobSqliteTarget(path);t.after(()=>peer.close());
  let changed=false;db.beforeSql=async sql=>{if(bodyRead(sql)&&!changed){changed=true;peer.db.exec(`UPDATE ${table} SET result='later' WHERE queue_name='parents' AND job_id='p001'`);}};
  assert.deepEqual((await q.dependencyResults(lease)).map(r=>r.result),['first','second']);
  db.beforeSql=null;
  assert.deepEqual((await q.dependencyResults(lease)).map(r=>r.result),['first','later']);
});

for (const journal of ['WAL','DELETE']) test(`${journal}: a fresh file owner recovers completed parent outputs without reexecuting them`,async t=>{
  const path=join(mkdtempSync(join(tmpdir(),'job-results-reopen-')),'db.sqlite');
  const original=new JobSqliteTarget(path,`PRAGMA journal_mode=${journal};`); let lease;
  try {
    const q=await DurableJobQueue.open(original,'q',{clock:()=>100});
    await q.enqueueBatch([{queue:'q',id:'parent',payload:'original'},{queue:'q',id:'child',payload:'join',dependsOn:[{queue:'q',id:'parent'}]}]);
    const parent=await q.claim('owner');original.afterCommit=()=>{throw new Error('lost parent response');};
    await assert.rejects(q.complete(parent,'durable output'),/lost parent response/);
    original.afterCommit=null;lease=await q.claim('consumer');
  } finally {original.close();}
  const reopened=new JobSqliteTarget(path);t.after(()=>reopened.close());
  const q=await DurableJobQueue.open(reopened,'q',{clock:()=>100});
  assert.deepEqual((await q.dependencyResults(lease)).map(r=>r.result),['durable output']);
  assert.equal((await q.get('parent')).attempts,1);
});

for (const encoding of ['UTF-8','UTF-16le','UTF-16be']) test(`${encoding}: corrupted edge bytes cannot select a replacement-character parent`,async t=>{
  const db=new JobSqliteTarget(':memory:',`PRAGMA encoding='${encoding}';`);t.after(()=>db.close());
  const q=await DurableJobQueue.open(db,'q');
  await q.enqueue({id:'\ufffd',payload:'p'});await q.complete(await q.claim('producer'),'wrong parent');
  await q.enqueue({id:'join',payload:'j'});const lease=await q.claim('reader');
  const bytes=encoding==='UTF-8'?"X'ff'":encoding==='UTF-16le'?"X'00d8'":"X'd800'";
  db.db.exec(`INSERT INTO ${dependencyTable} VALUES('q','join','q',CAST(${bytes} AS TEXT))`);
  db.statements.length=0;await assert.rejects(q.dependencyResults(lease));assert(!db.statements.some(bodyRead));
});
