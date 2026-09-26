// These tests execute the production transfer orchestrator with explicitly
// substituted SQLite-backed source/receiver boundary fixtures. They do NOT run
// production bootstrap/store/fanout implementations or certify the Rust engine.
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fork } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { DatabaseSync } from 'node:sqlite';
import { ChangesetBootstrapTransfer, BootstrapTransferError } from '../src/changeset-bootstrap-transfer.ts';
import { Source, Receiver } from './helpers/bootstrap-transfer-fixture.mjs';
const gate = () => { let resolve; const promise=new Promise(r=>{resolve=r;}); return {promise,resolve}; };
const pause = ms => new Promise(r=>setTimeout(r,ms));
const errorCode = (code, phase) => error => {
  assert.ok(error instanceof BootstrapTransferError);
  assert.equal(error.code,`ERR_FSQLITE_BOOTSTRAP_TRANSFER_${code}`);
  if(phase) assert.equal(error.phase,phase);
  assert.equal(error.deliveryId,'source:seed'); return true;
};
function setup(t, sourceOptions = {}, options = {}) {
  const source=new Source(sourceOptions), receiver=new Receiver(source,options);
  t.after(()=>{receiver.close();source.close();});
  const settings={receiverId:'east',deliveryId:'source:seed',tables:['t'],transport:receiver,
    confirmSource:()=>source.confirm(),...options};
  const transfer=new ChangesetBootstrapTransfer(source,settings);
  return {source,receiver,transfer,settings};
}
test('complete native-session baseline installs before one source ACK; callbacks run outside SQL',async t=>{
  const {source,receiver,transfer}=setup(t);
  const result=await transfer.run();
  assert.equal(result.stopped,'installed');assert.equal(result.uploadedChunks,3);
  assert.equal(result.newlyAcknowledged,3);assert.equal(source.pending(),0);
  assert.deepEqual(receiver.rows(),[1,2,3].map(id=>({id,value:`row-${id-1}`})));
  assert.equal(source.acks.length,1);assert.equal(source.confirmations,6);
  assert.equal(result.receipt.replayed,false);assert.ok(Object.isFrozen(result));assert.ok(Object.isFrozen(result.receipt));
});
test('partial runs retain every source byte and resume from receiver status',async t=>{
  const {source,receiver,transfer}=setup(t);
  for(let i=1;i<3;i++) {
    const result=await transfer.run({maxChunks:1});assert.equal(result.stopped,'limit');assert.equal(result.receivedChunks,i);
    assert.equal(source.pending(),3);assert.equal(source.acks.length,0);assert.equal(receiver.rows().length,0);
  }
  const result=await transfer.run({maxChunks:1});assert.equal(result.installed,true);assert.equal(result.uploadedChunks,1);
  assert.deepEqual(receiver.calls.filter(s=>s.startsWith('stage:')),['stage:0','stage:1','stage:2']);
});
test('byte-bound runs do not skip a larger successor or load it past the remaining budget',async t=>{
  const {source,receiver,transfer}=setup(t);
  const bytes=Number(source.db.prepare('SELECT bytes FROM seed LIMIT 1').get().bytes);
  const first=await transfer.run({maxBytes:bytes});assert.equal(first.uploadedChunks,1);assert.equal(first.stopped,'limit');
  assert.equal(source.loads,1);assert.equal(receiver.rows().length,0);assert.equal(source.pending(),3);
});
test('oversized next chunk rejects before loading its payload',async t=>{
  const {source,receiver,settings}=setup(t,{count:1,payloadSize:1024});
  await assert.rejects(new ChangesetBootstrapTransfer(source,{...settings,maxChunkBytes:64}).run(),errorCode('LIMIT','manifest'));
  assert.equal(source.loads,0);assert.deepEqual(receiver.calls,[]);assert.equal(source.pending(),1);
});
test('a head that cannot fit a fresh run rejects rather than returning zero-progress success',async t=>{
  const {source,transfer}=setup(t);await assert.rejects(transfer.run({maxBytes:1}),errorCode('LIMIT','source-read'));assert.equal(source.loads,0);
});
test('empty seed is one empty upload and still requires installed confirmation',async t=>{
  const {source,receiver,transfer}=setup(t,{count:0});const r=await transfer.run({maxBytes:1});
  assert.equal(r.installed,true);assert.equal(r.uploadedBytes,0);assert.equal(r.uploadedChunks,1);assert.equal(r.receipt.changes,0);
  assert.equal(source.pending(),0);assert.equal(receiver.rows().length,0);
});
test('lost staging response recovers the retained prefix without repeating uploads',async t=>{
  const {source,receiver,transfer}=setup(t);let lost=true;
  receiver.hooks.afterStage=(_m,index,p)=>{if(index===0&&lost){lost=false;throw Error('lost upload response');}return p;};
  await assert.rejects(transfer.run(),errorCode('FAILED','stage'));assert.equal(source.pending(),3);assert.equal(receiver.rows().length,0);
  const r=await transfer.run();assert.equal(r.uploadedChunks,2);assert.equal(r.installed,true);
  assert.equal(receiver.calls.filter(x=>x==='stage:0').length,1);
});
test('lost installation response leaves source pending and re-confirms installed replay',async t=>{
  const {source,receiver,transfer}=setup(t);let lost=true,confirmed=0;
  receiver.hooks.confirm=()=>{confirmed++;};receiver.hooks.afterInstall=r=>{if(lost){lost=false;throw Error('lost install ACK');}return r;};
  await assert.rejects(transfer.run(),errorCode('FAILED','install'));assert.equal(source.pending(),3);assert.equal(receiver.rows().length,3);
  const r=await transfer.run();assert.equal(r.uploadedChunks,0);assert.equal(r.receipt.replayed,true);assert.equal(confirmed,2);assert.equal(source.pending(),0);
});
test('source ACK response loss is reconciled even after all source payloads are cleared',async t=>{
  const {source,receiver,transfer}=setup(t);let lost=true;
  source.hooks.afterAck=()=>{if(lost){lost=false;throw Error('lost source ACK');}};
  await assert.rejects(transfer.run(),errorCode('FAILED','source-ack'));assert.equal(source.pending(),0);
  source.events.length=0;const loads=source.loads;const result=await transfer.run();
  assert.equal(source.events[0],'confirm');assert.equal(source.loads,loads);assert.equal(result.newlyAcknowledged,0);assert.equal(result.receipt.replayed,true);
  assert.equal(receiver.rows().length,3);
});
test('failed source-ACK checkpoint never produces complete success; retry first confirms source',async t=>{
  const {source,transfer}=setup(t);let fail=true;
  source.hooks.confirm=()=>{if(source.acks.length&&fail){fail=false;throw Error('checkpoint unknown');}};
  await assert.rejects(transfer.run(),errorCode('FAILED','source-confirm'));assert.equal(source.pending(),0);
  source.events.length=0;const result=await transfer.run();assert.equal(source.events[0],'confirm');assert.equal(result.installed,true);
});
test('receiver confirmation failure preserves source and repeats confirmation, not row insertion',async t=>{
  const {source,receiver,transfer}=setup(t);let fail=true;
  receiver.hooks.confirm=()=>{if(fail){fail=false;throw Error('receiver publication failed');}};
  await assert.rejects(transfer.run(),errorCode('FAILED','install'));assert.equal(source.pending(),3);assert.equal(receiver.rows().length,3);
  assert.equal((await transfer.run()).receipt.replayed,true);
});
test('initial source confirmation failure sends nothing and reads no manifest',async t=>{
  const {source,receiver,transfer}=setup(t);source.hooks.confirm=()=>{throw Error('unknown source');};
  await assert.rejects(transfer.run(),errorCode('FAILED','source-confirm'));assert.equal(source.manifestReads,0);assert.deepEqual(receiver.calls,[]);
});
test('source confirmation after manifest recovery covers concurrently committed source publication',async t=>{
  const {source,receiver,transfer}=setup(t);source.hooks.confirm=n=>{if(n===2)throw Error('new state not durable');};
  await assert.rejects(transfer.run(),errorCode('FAILED','source-confirm'));assert.equal(source.manifestReads,1);assert.deepEqual(receiver.calls,[]);
});
test('failed per-chunk source confirmation does not upload the already loaded body',async t=>{
  const {source,receiver,transfer}=setup(t);source.hooks.confirm=n=>{if(n===3)throw Error('source replaced');};
  await assert.rejects(transfer.run(),errorCode('FAILED','source-confirm'));assert.equal(source.loads,1);assert.deepEqual(receiver.calls,['status']);
});
for(const [name,change] of [
  ['protocol',r=>({...r,protocol:'wrong'})],['route',r=>({...r,receiverId:'west'})],
  ['identity',r=>({...r,deliveryId:'other'})],['digest',r=>({...r,sha256:'0'.repeat(64)})],
  ['chunks',r=>({...r,chunks:r.chunks+1})],['rows',r=>({...r,changes:r.changes+1})],
  ['bytes',r=>({...r,byteLength:r.byteLength+1})],['not installed',r=>({...r,installed:false})],
  ['not confirmed',r=>({...r,confirmed:false})],['nonboolean replay',r=>({...r,replayed:1})],
  ['inherited receipt',r=>Object.create(r)],['getter receipt',r=>Object.defineProperty({...r},'sha256',{get(){throw Error('must not execute');}})],
  ['unexpected order',r=>({...r,order:{}})],
]) test(`invalid installation ${name} never enters source acknowledgement`,async t=>{
  const {source,receiver,transfer}=setup(t);receiver.hooks.afterInstall=change;
  await assert.rejects(transfer.run(),errorCode('STATE','install'));assert.equal(source.acks.length,0);assert.equal(source.pending(),3);
});
for(const [name,reply] of [
  ['negative count',{receivedChunks:-1,receivedBytes:0,receivedChanges:0,installed:false}],
  ['oversized prefix',{receivedChunks:4,receivedBytes:0,receivedChanges:0,installed:false}],
  ['incomplete installation',{receivedChunks:1,receivedBytes:1,receivedChanges:1,installed:true}],
  ['empty with bytes',{receivedChunks:0,receivedBytes:1,receivedChanges:0,installed:false}],
  ['empty with rows',{receivedChunks:0,receivedBytes:0,receivedChanges:1,installed:false}],
  ['string installed',{receivedChunks:0,receivedBytes:0,receivedChanges:0,installed:'false'}],
  ['inherited data',Object.create({receivedChunks:0,receivedBytes:0,receivedChanges:0,installed:false})],
]) test(`reject invalid receiver status: ${name}`,async t=>{
  const {source,receiver,transfer}=setup(t);receiver.hooks.status=()=>reply;
  await assert.rejects(transfer.run(),e=>e instanceof BootstrapTransferError&&e.phase==='status');assert.equal(source.loads,0);assert.equal(source.acks.length,0);
});
test('nonadvancing stage progress cannot spin or acknowledge',async t=>{
  const {source,receiver,transfer}=setup(t);receiver.hooks.afterStage=()=>({receivedChunks:0,receivedBytes:0,receivedChanges:0,installed:false});
  await assert.rejects(transfer.run(),errorCode('STATE','stage'));assert.equal(source.loads,1);assert.equal(source.acks.length,0);
});
test('staging cannot silently change byte and row prefix totals',async t=>{
  const {source,receiver,transfer}=setup(t);receiver.hooks.afterStage=(_m,_i,p)=>({...p,receivedBytes:p.receivedBytes-1});
  await assert.rejects(transfer.run(),errorCode('STATE','stage'));assert.equal(source.acks.length,0);
});
test('receiver restart may lose a volatile prefix; a new run resumes fresh status',async t=>{
  const {source,receiver,transfer}=setup(t);await transfer.run({maxChunks:1});
  receiver.db.exec('DELETE FROM chunks; UPDATE state SET hash=NULL');
  const result=await transfer.run();assert.equal(result.uploadedChunks,3);assert.equal(source.pending(),0);
});
test('missing source chunk is not recreated from application rows',async t=>{
  const {source,receiver,transfer}=setup(t);receiver.hooks.status=(_m,p)=>{source.db.prepare('DELETE FROM seed WHERE seq=1').run();return p;};
  await assert.rejects(transfer.run(),errorCode('STATE','source-read'));assert.equal(source.acks.length,0);assert.equal(receiver.rows().length,0);
});
test('cleared payload without installed receiver is a terminal state error',async t=>{
  const {source,receiver,transfer}=setup(t);receiver.hooks.status=(_m,p)=>{source.db.exec("UPDATE seed SET ack=1,payload=X''");return p;};
  await assert.rejects(transfer.run(),errorCode('STATE','status'));assert.equal(source.acks.length,0);
});
test('peer installation between status and source read is re-confirmed, not silently accepted',async t=>{
  const {source,receiver,transfer}=setup(t);let race=true;
  receiver.hooks.status=async(m,p,controls)=>{
    if(race){race=false;for(let i=0;i<m.chunks;i++)await receiver.stage(m,i,new Uint8Array(source.db.prepare('SELECT payload FROM seed WHERE seq=?').get(i+1).payload),controls);
      await receiver.install(m,controls);source.db.exec("UPDATE seed SET ack=1,payload=X''");}
    return p;
  };
  const r=await transfer.run();assert.equal(r.uploadedChunks,0);assert.equal(r.newlyAcknowledged,0);assert.equal(r.receipt.replayed,true);
});
test('ordered installation must confirm the trusted source incarnation and final seed sequence',async t=>{
  const {source,receiver,transfer}=setup(t,{}, {orderedSourceId:'incarnation:1'});
  const r=await transfer.run();assert.equal(r.receipt.order.streamId,'incarnation:1');assert.equal(r.receipt.order.sequence,'3');
  assert.equal(source.acks[0].controls.orderedSourceId,'incarnation:1');assert.ok(Object.isFrozen(r.receipt.order));
});
for(const [key,value] of [['protocol','bad'],['streamId','other'],['sequence','03']]) test(`wrong ordered ${key} retains source`,async t=>{
  const {source,receiver,transfer}=setup(t,{}, {orderedSourceId:'incarnation:1'});
  receiver.hooks.afterInstall=r=>({...r,order:{...r.order,[key]:value}});
  await assert.rejects(transfer.run(),errorCode('STATE','install'));assert.equal(source.pending(),3);assert.equal(source.acks.length,0);
});
test('missing ordered prefix is refused before source SQL acknowledgement',async t=>{
  const {source,receiver,transfer}=setup(t,{}, {orderedSourceId:'incarnation:1'});
  receiver.hooks.afterInstall=r=>{delete r.order;return r;};await assert.rejects(transfer.run(),errorCode('STATE','install'));assert.equal(source.pending(),3);
});
test('mutating transport receipt during source ACK cannot alter the accepted receipt',async t=>{
  const {source,receiver,transfer}=setup(t,{}, {orderedSourceId:'incarnation:1'});let wire;
  receiver.hooks.afterInstall=r=>{wire=r;return r;};
  source.hooks.beforeAck=receipt=>{wire.sha256='0'.repeat(64);wire.order.streamId='changed';assert.notEqual(receipt.sha256,wire.sha256);};
  const result=await transfer.run();assert.equal(result.receipt.order.streamId,'incarnation:1');assert.notEqual(result.receipt.sha256,wire.sha256);
});
test('fanout acknowledgement advances only this replica and preserves slow replica bytes',async t=>{
  const {source,receiver,settings}=setup(t);const east=new ChangesetBootstrapTransfer(source,{...settings,acknowledgement:'fanout'});
  const r=await east.run();assert.equal(r.newlyAcknowledged,3);assert.equal(source.pending(),3);assert.equal(source.acks[0].fanout,true);
  const westReceiver=new Receiver(source);t.after(()=>westReceiver.close());
  const west=new ChangesetBootstrapTransfer(source,{...settings,receiverId:'west',transport:westReceiver,acknowledgement:'fanout'});
  assert.equal((await west.run()).newlyAcknowledged,3);assert.equal(source.pending(),0);
  assert.deepEqual(receiver.rows(),westReceiver.rows());
});
test('pre-aborted runs do not touch source or transport',async t=>{
  const {source,receiver,transfer}=setup(t);await assert.rejects(transfer.run({signal:AbortSignal.abort()}),errorCode('CANCELLED','admission'));
  assert.equal(source.confirmations,0);assert.deepEqual(receiver.calls,[]);
});
test('cancellation drains stage before releasing admission and retains accepted prefix',async t=>{
  const {source,receiver,transfer}=setup(t);const entered=gate(),release=gate(),abort=new AbortController();
  receiver.hooks.beforeStage=async()=>{entered.resolve();await release.promise;};let settled=false;
  const pending=transfer.run({signal:abort.signal}).finally(()=>{settled=true;});await entered.promise;abort.abort();await pause(5);
  assert.equal(settled,false);await assert.rejects(transfer.run(),errorCode('BUSY','admission'));
  release.resolve();await assert.rejects(pending,errorCode('CANCELLED','stage'));assert.equal(source.pending(),3);
  delete receiver.hooks.beforeStage;assert.equal((await transfer.run()).installed,true);
});
test('cancellation after successful source ACK still drains source confirmation',async t=>{
  const {source,transfer}=setup(t);const abort=new AbortController(),entered=gate(),release=gate();
  source.hooks.afterAck=()=>abort.abort();source.hooks.confirm=async()=>{if(source.acks.length){entered.resolve();await release.promise;}};
  let settled=false;const pending=transfer.run({signal:abort.signal}).finally(()=>{settled=true;});await entered.promise;assert.equal(settled,false);
  release.resolve();await assert.rejects(pending,errorCode('CANCELLED','source-confirm'));assert.equal(source.pending(),0);
});
test('one monotonic deadline is propagated as decreasing remaining budgets',async t=>{
  const {source,receiver,transfer}=setup(t);const budgets=[];
  receiver.hooks.beforeStage=async(_m,_i,_b,c)=>{budgets.push(c.timeoutMs);await pause(15);};
  await transfer.run({timeoutMs:1000});assert.equal(budgets.length,3);assert.ok(budgets[0]>budgets[2]);assert.equal(source.pending(),0);
});
test('deadline expiry during awaited upload drains, then resumes from retained prefix',async t=>{
  const {source,receiver,transfer}=setup(t);receiver.hooks.beforeStage=()=>pause(35);
  await assert.rejects(transfer.run({timeoutMs:20}),e=>e instanceof BootstrapTransferError&&(e.code.endsWith('TIMEOUT')||e.cause?.name==='TimeoutError'));
  assert.equal(source.pending(),3);delete receiver.hooks.beforeStage;assert.equal((await transfer.run()).installed,true);
});
test('real source COMMIT failure rolls back acknowledgement and preserves pending bytes',async t=>{
  const {source,receiver,transfer}=setup(t);source.db.exec('PRAGMA foreign_keys=ON; CREATE TABLE p(id PRIMARY KEY); CREATE TABLE bad(p REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED);');
  let fail=true;source.hooks.beforeCommit=()=>{if(source.acks.length&&fail){fail=false;source.db.exec('INSERT INTO bad VALUES(99)');}};
  await assert.rejects(transfer.run(),errorCode('FAILED','source-ack'));assert.equal(source.pending(),3);assert.equal(receiver.rows().length,3);
  assert.equal((await transfer.run()).receipt.replayed,true);
});
test('late receiver failure rolls back all native application rows without source acknowledgement',async t=>{
  const {source,receiver,transfer}=setup(t);let rows=0;receiver.hooks.afterRow=()=>{if(++rows===2)throw Error('late chunk failure');};
  await assert.rejects(transfer.run(),errorCode('FAILED','install'));assert.equal(receiver.rows().length,0);assert.equal(source.pending(),3);
  delete receiver.hooks.afterRow;assert.equal((await transfer.run()).uploadedChunks,0);
});
test('independent SQL reader sees no partial baseline while the fixture installer applies chunks',async t=>{
  const dir=mkdtempSync(join(tmpdir(),'fsqlite-transfer-reader-')),path=join(dir,'receiver.db');
  const source=new Source(),receiver=new Receiver(source,{filename:path}),reader=new DatabaseSync(path);
  t.after(()=>{reader.close();receiver.close();source.close();});
  receiver.hooks.afterRow=()=>assert.equal(reader.prepare('SELECT count(*) AS n FROM t').get().n,0);
  const driver=new ChangesetBootstrapTransfer(source,{receiverId:'east',deliveryId:'source:seed',tables:['t'],transport:receiver,confirmSource:()=>source.confirm()});
  await driver.run();assert.equal(reader.prepare('SELECT count(*) AS n FROM t').get().n,3);
});
for(const options of [{maxChunks:0},{maxChunks:10001},{maxBytes:0},{maxBytes:2**31},{timeoutMs:0},{signal:{}}]) test(`invalid run admission ${JSON.stringify(options)}`,async t=>{
  const {source,receiver,transfer}=setup(t);await assert.rejects(transfer.run(options),errorCode('INPUT','admission'));assert.equal(source.confirmations,0);assert.deepEqual(receiver.calls,[]);
  assert.equal((await transfer.run()).installed,true,'failed admission releases active guard');
});
test('constructor captures route arrays and binds transport methods before asynchronous work',async t=>{
  const {source,receiver,settings}=setup(t);const tables=['T'];const d=new ChangesetBootstrapTransfer(source,{...settings,tables});
  tables[0]='changed';receiver.stage=()=>{throw Error('replaced method must not run');};assert.equal((await d.run()).installed,true);
});
for(const [name,options] of [['bad route',{receiverId:''}],['reserved',{tables:['__fsqlite_x']}],['duplicate',{tables:['T','t']}],['policy',{acknowledgement:'quorum'}],['bytes',{maxChunkBytes:0}]]) test(`constructor refuses ${name}`,async t=>{
  const {source,settings}=setup(t);assert.throws(()=>new ChangesetBootstrapTransfer(source,{...settings,...options}),BootstrapTransferError);
});
for(const cut of ['stage','receiver-commit','source-ack']) test(`SIGKILL and fresh-owner retry at ${cut}`,{timeout:20000},async t=>{
  const directory=mkdtempSync(join(tmpdir(),'fsqlite-transfer-kill-'));
  const sourcePath=join(directory,'source.db'),receiverPath=join(directory,'receiver.db');
  const source=new Source({filename:sourcePath}),receiver=new Receiver(source,{filename:receiverPath});receiver.close();source.close();
  const loader=fileURLToPath(new URL('./helpers/bootstrap-transfer-loader.mjs',import.meta.url));
  const child=fork(fileURLToPath(new URL('./helpers/bootstrap-transfer-child.mjs',import.meta.url)),[sourcePath,receiverPath,cut],{
    execArgv:[`--experimental-loader=${loader}`],stdio:['ignore','ignore','pipe','ipc'],env:process.env,
  });let stderr='';child.stderr.on('data',chunk=>{stderr+=chunk;});
  t.after(()=>{if(child.exitCode===null&&!child.killed)child.kill('SIGKILL');});
  const exited=new Promise(resolve=>child.once('exit',(code,signal)=>resolve({code,signal})));
  await new Promise((resolve,reject)=>{child.once('message',message=>message===cut?resolve():reject(Error('wrong cut')));child.once('exit',()=>reject(Error(`child exited early: ${stderr}`)));});
  child.kill('SIGKILL');assert.equal((await exited).signal,'SIGKILL');
  const reopened=new Source({filename:sourcePath,initialize:false}),destination=new Receiver(reopened,{filename:receiverPath,initialize:false});
  t.after(()=>{destination.close();reopened.close();});
  assert.equal(reopened.pending(),cut==='source-ack'?0:3);
  assert.equal(destination.rows().length,cut==='stage'?0:3);
  const transfer=new ChangesetBootstrapTransfer(reopened,{receiverId:'east',deliveryId:'source:seed',tables:['t'],transport:destination,confirmSource:()=>reopened.confirm()});
  const result=await transfer.run();assert.equal(result.installed,true);assert.equal(reopened.pending(),0);assert.equal(destination.rows().length,3);
  assert.equal(result.uploadedChunks,cut==='stage'?2:0);
});
test('actual retained-body size gates manifest recovery even when byte metadata underreports',async t=>{
  const {source,receiver,settings}=setup(t,{count:1,payloadSize:1024});
  source.db.exec('UPDATE seed SET bytes=1');
  await assert.rejects(new ChangesetBootstrapTransfer(source,{...settings,maxChunkBytes:64}).run(),errorCode('LIMIT','manifest'));
  assert.equal(source.manifestReads,0);assert.equal(source.loads,0);assert.deepEqual(receiver.calls,[]);
});
test('an acknowledged peer race can complete even when the old body would not fit the run',async t=>{
  const {source,receiver,transfer}=setup(t);let race=true;
  receiver.hooks.status=async(m,p,controls)=>{
    if(race){race=false;for(let i=0;i<m.chunks;i++)await receiver.stage(m,i,new Uint8Array(source.db.prepare('SELECT payload FROM seed WHERE seq=?').get(i+1).payload),controls);
      await receiver.install(m,controls);source.db.exec("UPDATE seed SET ack=1,payload=X''");}
    return p;
  };
  const r=await transfer.run({maxBytes:1});assert.equal(r.installed,true);assert.equal(r.uploadedBytes,0);assert.equal(r.receipt.replayed,true);
});
test('invalid size preflight reply never reaches manifest payload verification',async t=>{
  const {source,transfer}=setup(t);source.hooks.afterQuery=(sql,_p,rows)=>{if(sql.includes('coalesce(max(length'))rows[0][0]='oversized';};
  await assert.rejects(transfer.run(),errorCode('STATE','manifest'));assert.equal(source.manifestReads,0);assert.equal(source.loads,0);
});
test('reentrant run from source confirmation rejects instead of allocating a waiter',async t=>{
  const {source,transfer}=setup(t);let checked=false;
  source.hooks.confirm=async()=>{if(!checked){checked=true;await assert.rejects(transfer.run(),errorCode('BUSY','admission'));}};
  assert.equal((await transfer.run()).installed,true);assert.equal(checked,true);
});
test('receiver-confirmed success followed by cancellation never starts source ACK',async t=>{
  const {source,receiver,transfer}=setup(t);const abort=new AbortController();
  receiver.hooks.afterInstall=r=>{abort.abort();return r;};
  await assert.rejects(transfer.run({signal:abort.signal}),errorCode('CANCELLED','install'));
  assert.equal(source.acks.length,0);assert.equal(source.pending(),3);assert.equal(receiver.rows().length,3);
});
test('unexpected source ACK result still drains confirmation before rejecting',async t=>{
  const {source,transfer}=setup(t);source.hooks.ackResult=()=>-1;
  await assert.rejects(transfer.run(),errorCode('INPUT','source-confirm'));assert.equal(source.pending(),0);assert.equal(source.confirmations,6);
});
