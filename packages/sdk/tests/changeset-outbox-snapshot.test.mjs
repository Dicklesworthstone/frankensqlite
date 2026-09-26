import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtempSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { fork } from 'node:child_process';
import { SqliteTarget } from './helpers/production-sqlite-target.mjs';
import { ChangesetOutbox } from '../src/changeset-outbox.ts';
import { ChangesetFanout } from '../src/changeset-fanout.ts';
import { applyChangeset } from '../src/changeset-apply.ts';
import { decodeChangeset } from '../src/changeset-codec.ts';

const schema='CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT);CREATE TABLE audit(id INTEGER PRIMARY KEY,v TEXT);';
const triggers="CREATE TRIGGER log AFTER UPDATE ON t BEGIN INSERT INTO audit(v) VALUES(NEW.v); END;";
const seed="INSERT INTO t VALUES(1,'initial');";
const tables=['t','audit'];
const options={deliveryId:'source:trigger-operation',tables};
const contents=db=>tables.map(t=>db.rows(`SELECT * FROM ${t} ORDER BY id`));
const noSnapshot=sql=>/SELECT typeof\(s\./.test(sql);

for(const encoding of ['UTF-8','UTF-16le','UTF-16be'])test(`${encoding}: trigger work, retained payload and identity share one commit`,async()=>{
  const src=new SqliteTarget(':memory:',`PRAGMA encoding='${encoding}';`+schema+seed+triggers),dst=new SqliteTarget(':memory:',`PRAGMA encoding='${encoding}';`+schema+seed);
  const outbox=new ChangesetOutbox(src);let calls=0;
  try {
    const first=await outbox.recordSnapshot(async tx=>{calls++;await tx.execute('UPDATE t SET v=?',['\uFEFFpaid\0界😀']);return 42;},options);
    assert.equal(first.value,42);assert.equal(first.replayed,false);assert.equal(first.delivery.changes,2);assert.equal(first.delivery.sequence,1n);
    const loaded=await outbox.read(options.deliveryId);
    const applied=await applyChangeset(dst,loaded.changeset,{tables,deliveryId:options.deliveryId});assert.equal(applied.applied,2);
    assert.deepEqual(contents(dst),contents(src));
    await outbox.acknowledge(options.deliveryId,first.delivery.sha256);
    await src.execute("UPDATE t SET v='later'");src.statements=[];
    const replay=await outbox.recordSnapshot(()=>{calls++;throw new Error('replayed callback');},{...options,tables:['audit','t'],maxRows:1});
    assert.equal(replay.replayed,true);assert.equal(replay.delivery.acknowledged,true);assert.equal(calls,1);assert.equal(src.statements.some(noSnapshot),false);
  }finally{src.close();dst.close();}
});
test('ordinary record remains journal-based and refuses application triggers',async()=>{
  const src=new SqliteTarget(':memory:',schema+seed+triggers),outbox=new ChangesetOutbox(src);
  try{await assert.rejects(outbox.record(tx=>tx.execute("UPDATE t SET v='x'"),options));assert.equal(src.rows('SELECT v FROM t')[0][0],'initial');}
  finally{src.close();}
});
for(const first of ['record','recordSnapshot','bootstrap','bootstrapChunks'])test(`recordSnapshot identity cannot alias ${first}`,async()=>{
  const src=new SqliteTarget(':memory:',schema+seed),outbox=new ChangesetOutbox(src);
  try{
    if(first==='record'||first==='recordSnapshot')await outbox[first](tx=>tx.execute("UPDATE t SET v='x'"),options);
    else await outbox[first](options);
    const second=first==='recordSnapshot'?'record':'recordSnapshot';let called=false;
    await assert.rejects(outbox[second](()=>{called=true;},options),{code:'ERR_FSQLITE_OUTBOX_REUSE'});assert.equal(called,false);
  }finally{src.close();}
});
test('scope and indirect policy are retained independently of callback identity',async()=>{
  const src=new SqliteTarget(':memory:',schema+seed+triggers),outbox=new ChangesetOutbox(src);
  try{
    await outbox.recordSnapshot(tx=>tx.execute("UPDATE t SET v='x'"),options);
    await assert.rejects(outbox.recordSnapshot(()=>{}, {...options,tables:['t']}),{code:'ERR_FSQLITE_OUTBOX_REUSE'});
    await assert.rejects(outbox.recordSnapshot(()=>{}, {...options,indirect:true}),{code:'ERR_FSQLITE_OUTBOX_REUSE'});
  }finally{src.close();}
});
for(const failure of ['callback','after-scan','codec','capacity','cancel','deferred-commit'])test(`${failure}: no committed business writes, audit rows or outgoing message`,async()=>{
  const src=new SqliteTarget(':memory:',schema+seed+triggers),outbox=new ChangesetOutbox(src,{maxPayloadBytes:failure==='capacity'?1:1000000});
  const controller=new AbortController();
  const extra=failure==='after-scan'?{maxRows:1}:failure==='codec'?{limits:{maxChanges:1}}:failure==='cancel'?{signal:controller.signal}:{};
  try{
    if(failure==='deferred-commit')src.db.exec('CREATE TABLE parent(id INTEGER PRIMARY KEY);CREATE TABLE child(id INTEGER PRIMARY KEY,p REFERENCES parent DEFERRABLE INITIALLY DEFERRED);');
    await assert.rejects(outbox.recordSnapshot(async tx=>{
      await tx.execute("UPDATE t SET v='changed'");
      if(failure==='callback')throw new Error('stop');
      if(failure==='cancel')controller.abort();
      if(failure==='deferred-commit')await tx.execute('INSERT INTO child VALUES(1,99)');
    },{...options,...extra}));
    assert.equal(src.rows('SELECT v FROM t')[0][0],'initial');assert.equal(src.rows('SELECT * FROM audit').length,0);assert.deepEqual(await outbox.pending(),[]);
  }finally{src.close();}
});
test('full retained-identity capacity rejects before snapshots or business callback',async()=>{
  const src=new SqliteTarget(':memory:',schema+seed+triggers),outbox=new ChangesetOutbox(src,{maxEntries:1});
  try{
    await outbox.recordSnapshot(tx=>tx.execute("UPDATE t SET v='first'"),options);src.statements=[];let called=false;
    await assert.rejects(outbox.recordSnapshot(()=>{called=true;},{...options,deliveryId:'source:second'}),{code:'ERR_FSQLITE_OUTBOX_FULL'});
    assert.equal(called,false);assert.equal(src.statements.some(noSnapshot),false);
  }finally{src.close();}
});
test('source post-commit response loss recovers original trigger outcome after reopen',async()=>{
  const path=join(mkdtempSync(join(tmpdir(),'fsqlite-snapshot-outbox-')),'db.sqlite');
  let src=new SqliteTarget(path,schema+seed+triggers),outbox=new ChangesetOutbox(src);let calls=0;
  try{
    src.afterCommit=()=>{throw new Error('lost source response');};
    await assert.rejects(outbox.recordSnapshot(async tx=>{calls++;await tx.execute("UPDATE t SET v='paid'");},options),/lost source response/);
    src.close();src=new SqliteTarget(path);outbox=new ChangesetOutbox(src);src.statements=[];
    const replay=await outbox.recordSnapshot(()=>{calls++;},options);
    assert.equal(replay.replayed,true);assert.equal(calls,1);assert.equal(src.rows('SELECT * FROM audit').length,1);assert.equal(src.statements.some(noSnapshot),false);
  }finally{src.close();}
});
test('receiver lost response reuses its inbox while source trigger callback stays single-execution',async()=>{
  const src=new SqliteTarget(':memory:',schema+seed+triggers),dst=new SqliteTarget(':memory:',schema+seed),outbox=new ChangesetOutbox(src);let calls=0;
  try{
    const rec=await outbox.recordSnapshot(async tx=>{calls++;await tx.execute("UPDATE t SET v='paid'");},options);
    const load=await outbox.read(options.deliveryId);dst.afterCommit=()=>{throw new Error('lost receiver response');};
    await assert.rejects(applyChangeset(dst,load.changeset,{tables,deliveryId:options.deliveryId}));dst.afterCommit=null;
    assert.equal((await outbox.pending()).length,1);
    const applied=await applyChangeset(dst,load.changeset,{tables,deliveryId:options.deliveryId});assert.equal(applied.replayed,true);
    await outbox.acknowledge(options.deliveryId,rec.delivery.sha256);assert.equal(calls,1);assert.deepEqual(contents(dst),contents(src));
  }finally{src.close();dst.close();}
});
test('pending replay validates corruption before returning a retained operation',async()=>{
  const src=new SqliteTarget(':memory:',schema+seed+triggers),outbox=new ChangesetOutbox(src);let called=false;
  try{
    await outbox.recordSnapshot(tx=>tx.execute("UPDATE t SET v='paid'"),options);
    src.db.exec('UPDATE __fsqlite_changeset_outbox SET payload=zeroblob(length(payload))');
    await assert.rejects(outbox.recordSnapshot(()=>{called=true;},options),{code:'ERR_FSQLITE_OUTBOX_CORRUPT'});assert.equal(called,false);
  }finally{src.close();}
});
test('net-zero callback retains an empty BLOB and still deduplicates business work',async()=>{
  const src=new SqliteTarget(':memory:',schema+seed),outbox=new ChangesetOutbox(src);let calls=0;
  try{
    const run=async tx=>{calls++;await tx.execute("UPDATE t SET v='temporary'");await tx.execute("UPDATE t SET v='initial'");};
    const result=await outbox.recordSnapshot(run,options);
    assert.equal(result.delivery.byteLength,0);assert.equal(result.delivery.changes,0);
    assert.equal((await outbox.read(options.deliveryId)).changeset.length,0);
    assert.equal((await outbox.recordSnapshot(run,options)).replayed,true);assert.equal(calls,1);
  }finally{src.close();}
});
test('snapshot record follows bootstrap, and retained deltas preserve trigger-generated audit rows',async()=>{
  const src=new SqliteTarget(':memory:',schema+seed+triggers),dst=new SqliteTarget(':memory:',schema),outbox=new ChangesetOutbox(src);
  try{
    const baseline=await outbox.bootstrapChunks({deliveryId:'source:seed',tables,chunkRows:1});
    const update=await outbox.recordSnapshot(tx=>tx.execute("UPDATE t SET v='after seed'"),options);
    assert.equal(update.delivery.sequence,BigInt(baseline.chunks+1));
    for(const d of await outbox.pending()){
      const rec=await outbox.read(d.deliveryId);await applyChangeset(dst,rec.changeset,{tables,deliveryId:d.deliveryId});await outbox.acknowledge(d.deliveryId,d.sha256);
    }
    assert.deepEqual(contents(dst),contents(src));
  }finally{src.close();dst.close();}
});
test('required-replica fanout preserves triggered deltas until every member acknowledges',async()=>{
  const src=new SqliteTarget(':memory:',schema+seed+triggers),east=new SqliteTarget(':memory:',schema+seed),west=new SqliteTarget(':memory:',schema+seed);
  try{
    const fanout=await ChangesetFanout.open(src,['east','west']),outbox=new ChangesetOutbox(src);
    const rec=await outbox.recordSnapshot(tx=>tx.execute("UPDATE t SET v='shared'"),options);
    for(const [id,dst] of [['east',east],['west',west]]){
      const channel=fanout.forReplica(id),loaded=await channel.read(rec.delivery.deliveryId);
      assert.ok(loaded.changeset.byteLength>0);await applyChangeset(dst,loaded.changeset,{tables,deliveryId:rec.delivery.deliveryId});await channel.acknowledge(rec.delivery.deliveryId,rec.delivery.sha256);
      const stored=await outbox.read(rec.delivery.deliveryId);assert.equal(stored.changeset===null,id==='west');
    }
    assert.deepEqual(contents(east),contents(src));assert.deepEqual(contents(west),contents(src));
    assert.equal((await fanout.progress()).acknowledgedThrough,1n);
  }finally{src.close();east.close();west.close();}
});
test('source trigger cannot silently change required-replica authority during capture',async()=>{
  const src=new SqliteTarget(':memory:',schema+seed),fanout=await ChangesetFanout.open(src,['east','west']),outbox=new ChangesetOutbox(src);
  src.db.exec("CREATE TRIGGER attack AFTER UPDATE ON t BEGIN UPDATE __fsqlite_changeset_fanout SET roster='{}'; END;");
  try{
    await assert.rejects(outbox.recordSnapshot(tx=>tx.execute("UPDATE t SET v='bad'"),options));
    assert.equal(src.rows('SELECT v FROM t')[0][0],'initial');assert.equal((await fanout.progress()).sourceSequence,0n);
  }finally{src.close();}
});
test('false publication row counts roll back original writes and their triggers',async()=>{
  const src=new SqliteTarget(':memory:',schema+seed+triggers),outbox=new ChangesetOutbox(src);const execute=src.execute.bind(src);
  src.execute=async(sql,params)=>{const n=await execute(sql,params);return sql.startsWith('INSERT OR ABORT INTO main."__fsqlite_changeset_outbox"')?0:n;};
  try{await assert.rejects(outbox.recordSnapshot(tx=>tx.execute("UPDATE t SET v='bad'"),options));assert.equal(src.rows('SELECT v FROM t')[0][0],'initial');assert.equal(src.rows('SELECT * FROM audit').length,0);}
  finally{src.close();}
});
test('nested outer rollback includes snapshot record and source effects',async()=>{
  const src=new SqliteTarget(':memory:',schema+seed+triggers);
  try{
    await assert.rejects(src.transaction(async tx=>{
      const child={transaction:async work=>{await tx.execute('SAVEPOINT scoped');try{const result=await work(tx);await tx.execute('RELEASE scoped');return result;}catch(error){await tx.execute('ROLLBACK TO scoped');await tx.execute('RELEASE scoped');throw error;}}};
      await new ChangesetOutbox(child).recordSnapshot(t=>t.execute("UPDATE t SET v='provisional'"),options);throw new Error('abort outer');
    }));
    assert.equal(src.rows('SELECT v FROM t')[0][0],'initial');assert.deepEqual(await new ChangesetOutbox(src).pending(),[]);
  }finally{src.close();}
});
test('two file-backed owners overlap; only one can publish the same triggered operation',async()=>{
  const path=join(mkdtempSync(join(tmpdir(),'fsqlite-snapshot-race-')),'db.sqlite');
  const a=new SqliteTarget(path,schema+seed+triggers+'PRAGMA journal_mode=WAL;'),b=new SqliteTarget(path),oa=new ChangesetOutbox(a),ob=new ChangesetOutbox(b);
  // Establish source storage before the conflicting read-to-write promotions.
  await oa.recordSnapshot(()=>{}, {...options,deliveryId:'source:setup'});
  let entered=0,release;const gate=new Promise(r=>release=r);
  const work=async tx=>{entered++;if(entered===2)release();await gate;await tx.execute("UPDATE t SET v='once'");};
  try{
    const result=await Promise.allSettled([oa.recordSnapshot(work,options),ob.recordSnapshot(work,options)]);
    assert.equal(result.filter(r=>r.status==='fulfilled').length,1);assert.equal(a.rows('SELECT * FROM audit').length,1);
    const replay=await ob.recordSnapshot(()=>{throw new Error('must not run');},options);assert.equal(replay.replayed,true);
  }finally{a.close();b.close();}
});

for(const journal of ['WAL','DELETE'])for(const phase of ['after-work','after-payload','before-commit','after-commit'])test(`SIGKILL ${journal} ${phase}: atomic trigger/outbox recovery`,async()=>{
  const path=join(mkdtempSync(join(tmpdir(),'fsqlite-trigger-kill-')),'db.sqlite');
  const setup=new SqliteTarget(path,schema+seed+triggers+`PRAGMA journal_mode=${journal};`);
  // Ensure the reserved storage exists independently of the killed operation.
  await new ChangesetOutbox(setup).recordSnapshot(()=>{}, {...options,deliveryId:'source:setup'});setup.close();
  const child=fork(new URL('./helpers/snapshot-outbox-child.mjs',import.meta.url),[path,phase],{execArgv:process.execArgv,stdio:['ignore','ignore','pipe','ipc']});
  let diagnostic='';child.stderr.on('data',d=>diagnostic+=d);
  await new Promise((resolve,reject)=>{
    const timer=setTimeout(()=>{child.kill('SIGKILL');reject(new Error('child did not reach kill point: '+diagnostic));},10000);
    child.once('message',()=>{child.kill('SIGKILL');});
    child.once('error',e=>{clearTimeout(timer);reject(e);});
    child.once('exit',(code,signal)=>{clearTimeout(timer);signal==='SIGKILL'?resolve():reject(new Error('child exit '+code+': '+diagnostic));});
  });
  const src=new SqliteTarget(path),outbox=new ChangesetOutbox(src);let called=0;
  try{
    const committed=phase==='after-commit';assert.equal(src.rows('SELECT * FROM audit').length,committed?1:0);
    assert.equal((await outbox.read(options.deliveryId))!==null,committed);
    const result=await outbox.recordSnapshot(async tx=>{called++;await tx.execute("UPDATE t SET v='paid'");},options);
    assert.equal(result.replayed,committed);assert.equal(called,committed?0:1);assert.equal(src.rows('SELECT * FROM audit').length,1);
    const payload=await outbox.read(options.deliveryId);assert.equal(decodeChangeset(payload.changeset).reduce((n,t)=>n+t.changes.length,0),2);
  }finally{src.close();}
});
test('ordinary journal records still publish their original scope and replay contract',async()=>{
  const src=new SqliteTarget(':memory:',schema+seed),outbox=new ChangesetOutbox(src);
  try{
    const result=await outbox.record(async tx=>{await tx.execute("UPDATE t SET v='journal'");return 123;},options);
    assert.equal(result.value,123);assert.equal(result.delivery.changes,1);
    assert.equal(src.rows('SELECT scope FROM __fsqlite_changeset_outbox')[0][0],JSON.stringify({tables:['audit','t'],indirect:false}));
    assert.equal((await outbox.record(()=>{throw new Error('replay');},options)).replayed,true);
  }finally{src.close();}
});
test('admitted delayed writes and audit triggers finish before payload publication',async()=>{
  const src=new SqliteTarget(':memory:',schema+seed+triggers),outbox=new ChangesetOutbox(src);
  src.before=async(kind,sql)=>{if(sql.startsWith('UPDATE t'))await new Promise(r=>setTimeout(r,10));};
  try{
    const result=await outbox.recordSnapshot(tx=>{void tx.execute("UPDATE t SET v='delayed'");return 'ok';},options);
    assert.equal(result.value,'ok');assert.equal(result.delivery.changes,2);assert.equal(src.rows('SELECT * FROM audit').length,1);
    assert.equal(decodeChangeset((await outbox.read(options.deliveryId)).changeset).length,2);
  }finally{src.close();}
});
test('explicit forgetting ends operation retry protection but does not reset source sequencing',async()=>{
  const src=new SqliteTarget(':memory:',schema+seed+triggers),outbox=new ChangesetOutbox(src);
  try{
    const first=await outbox.recordSnapshot(tx=>tx.execute("UPDATE t SET v='first'"),options);
    await assert.rejects(outbox.forgetAcknowledged(options.deliveryId,first.delivery.sha256));
    await outbox.acknowledge(options.deliveryId,first.delivery.sha256);
    assert.equal(await outbox.forgetAcknowledged(options.deliveryId,first.delivery.sha256),true);
    const second=await outbox.recordSnapshot(tx=>tx.execute("UPDATE t SET v='second'"),{...options,deliveryId:'source:new-identity'});
    assert.equal(second.delivery.sequence,2n);assert.equal(src.rows('SELECT * FROM audit').length,2);
  }finally{src.close();}
});
