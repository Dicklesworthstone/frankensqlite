// Protocol faults through the production SDK; native SQLite integrations below
// do not certify FrankenSQLite WASM or actual browser deserialization failures.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { FrankenWorkerClient } from '../src/worker-client.ts';
import { FrankenDB } from '../src/database.ts';
import * as errors from '../src/errors.ts';
import { ControlledWorker, observe, drain } from './helpers/controlled-worker.ts';
import { sqliteSnapshotWorker } from '../../worker/tests/helpers/snapshot-sqlite-core.mjs';
const limits = { timeout: 5000 };
const result = value => ({columns:['value'], columnCount:1, columnTypes:[], rows:[{value}], rowArrays:[[value]], changes:0});
const invalid = error => error?.code === 'ERR_FSQLITE_WORKER_RESPONSE' && error.transient === false && /may have executed/.test(error.suggestion);
function fixture(t) {
  const worker = new ControlledWorker(), client = new FrankenWorkerClient(worker);
  t.after(() => client.dispose());
  worker.onPost = request => {
    if (request.kind === 'close') queueMicrotask(() => worker.reply({kind:'close-result',requestId:request.requestId}));
  };
  return {worker,client};
}

for (const [label, error] of [
  ['null', null], ['string', 'fault'], ['missing code', {message:'fault'}],
  ['missing message', {code:'ERR_X'}], ['non-string message', {code:'ERR_X',message:{}}],
  ['bad boolean', {code:'ERR_X',message:'fault',transient:'true'}],
  ['bad cleanup', {code:'ERR_X',message:'fault',cleanupErrors:{}}],
  ['sparse cleanup', {code:'ERR_X',message:'fault',cleanupErrors:new Array(2)}],
  ['non-finite code', {code:'ERR_X',message:'fault',sqliteCode:NaN}],
]) {
  test(`reply decoding: ${label} errors settle instead of abandoning accepted SQL`, limits, async t => {
    const {worker,client}=fixture(t);
    const pending=observe(client.execute('INSERT INTO items VALUES(1)'));
    const id=worker.requests.at(-1).requestId;
    assert.doesNotThrow(()=>worker.reply({kind:'error',requestId:id,error}));
    await drain(); assert.equal(pending.outcome.status,'rejected'); assert.ok(invalid(pending.outcome.reason));
    assert.equal(client.requestQueue.pendingRequests,0);assert.equal(client.requestQueue.pendingBytes,0);
    assert.equal(worker.requests.length,1);
    worker.reply({kind:'error',requestId:id,error:null}); // Duplicate is inert.
    const good=client.query('SELECT 1');worker.reply({kind:'query-result',requestId:worker.requests.at(-1).requestId,data:result(1)});
    assert.deepEqual(await good,result(1));
  });
}

test('reply decoding: cyclic, deep and wide error graphs reject within bounded traversal',limits,async t=>{
  const {worker,client}=fixture(t);
  const cyclic={code:'ERR_X',message:'cycle'};cyclic.cause=cyclic;
  let deep={code:'ERR_X',message:'leaf'};for(let i=0;i<100;i++)deep={code:'ERR_X',message:'branch',cause:deep};
  const wide={code:'ERR_X',message:'root',cleanupErrors:new Array(1000000)};
  for(const error of [cyclic,deep,wide]) {
    const pending=observe(client.execute('write'));
    assert.doesNotThrow(()=>worker.reply({kind:'error',requestId:worker.requests.at(-1).requestId,error}));
    await drain();assert.equal(pending.outcome.status,'rejected');assert.ok(invalid(pending.outcome.reason));
  }
  assert.equal(worker.requests.length,3);assert.equal(client.requestQueue.pendingRequests,0);
});

test('reply decoding: valid nested SQLite and cleanup errors preserve precise metadata',()=>{
  const decoded=errors.decodeFrankenError({code:'ERR_WRAPPER',message:'outer',transient:false,batchIndex:3,
    cause:{code:'SQLITE_CONSTRAINT',message:'unique',sqliteCode:19,extendedCode:2067,userRecoverable:true,suggestion:'correct input'},
    cleanupErrors:[{code:'ERR_CLOSE',message:'cleanup',stack:'remote-stack'}]});
  assert.equal(decoded.code,'ERR_WRAPPER');assert.equal(decoded.batchIndex,3);
  assert.equal(decoded.cause.extendedCode,2067);assert.equal(decoded.cause.suggestion,'correct input');
  assert.equal(decoded.cleanupErrors[0].code,'ERR_CLOSE');assert.equal(decoded.cleanupErrors[0].stack,'remote-stack');
});

test('reply decoding: throwing getters cannot escape the event callback or strand a promise',limits,async t=>{
  const {worker,client}=fixture(t);const failure=Error('getter');
  const pending=observe(client.execute('write'));
  assert.doesNotThrow(()=>worker.reply({kind:'error',requestId:worker.requests.at(-1).requestId,
    error:{code:'ERR_X',get message(){throw failure;}}}));
  await drain();assert.ok(invalid(pending.outcome.reason));assert.equal(pending.outcome.reason.cause,failure);
});

test('reply decoding: mismatched operation kind rejects only the correlated request and never replays it',limits,async t=>{
  const {worker,client}=fixture(t);const one=client.execute('write'),two=client.query('SELECT 2');
  worker.reply({kind:'query-result',requestId:worker.requests[0].requestId,data:result(999)});
  worker.reply({kind:'query-result',requestId:worker.requests[1].requestId,data:result(2)});
  await assert.rejects(one,invalid);assert.deepEqual(await two,result(2));assert.equal(worker.requests.length,2);
});

test('reply decoding: malformed uncorrelated packets fail all accepted work and notify owners once',limits,async t=>{
  for(const data of [null,[],{}, {requestId:NaN}, {requestId:-1}]) {
    const {worker,client}=fixture(t);const faults=[];client.observeFailure(error=>faults.push(error));
    const a=observe(client.execute('one')),b=observe(client.query('two'));
    assert.doesNotThrow(()=>worker.reply(data));await drain();
    assert.equal(a.outcome.status,'rejected');assert.equal(a.outcome.reason,b.outcome.reason);assert.ok(invalid(a.outcome.reason));
    assert.deepEqual(faults,[a.outcome.reason]);assert.equal(client.requestQueue.pendingRequests,0);
    await assert.rejects(client.execute('future'),error=>error===a.outcome.reason);assert.equal(worker.requests.length,2);
  }
});

test('reply decoding: reentrant error getter disposal settles exactly once without recreating reservations',limits,async t=>{
  const {worker,client}=fixture(t);const reason=Error('disposed'),pending=observe(client.execute('write'));
  worker.reply({kind:'error',requestId:worker.requests.at(-1).requestId,error:{code:'ERR_X',get message(){client.dispose(reason);return 'late';}}});
  await pending.settled;assert.equal(pending.outcome.reason,reason);assert.equal(client.requestQueue.pendingRequests,0);
  assert.equal(worker.terminateCount,1);
});

test('reply decoding: success envelopes reject missing data and impossible counts',limits,async t=>{
  const {worker,client}=fixture(t);
  for(const [start,reply] of [
    [()=>client.execute('write'),{kind:'execute-result',changes:NaN}],
    [()=>client.execute('write'),{kind:'execute-result',changes:-1}],
    [()=>client.query('SELECT'),{kind:'query-result',data:null}],
    [()=>client.query('SELECT'),{kind:'query-result',data:{...result(1),rowArrays:[]}}],
    [()=>client.prepare('SELECT'),{kind:'prepare-result',data:{}}],
    [()=>client.export(),{kind:'export-result',data:{}}],
    [()=>client.checkpoint(),{kind:'checkpoint-result',data:{}}],
    [()=>client.executeMany('write',[[1]]),{kind:'execute-many-result',data:{executions:1,changes:2,changesPerExecution:[1]}}],
  ]) {
    const pending=start();worker.reply({...reply,requestId:worker.requests.at(-1).requestId});await assert.rejects(pending,invalid);
    assert.equal(client.requestQueue.pendingRequests,0);
  }
});

test('reply decoding: invalid ready response rejects open and releases the supplied worker',limits,async()=>{
  const worker=new ControlledWorker();worker.onPost=r=>queueMicrotask(()=>worker.reply({kind:'ready',requestId:r.requestId,data:null}));
  await assert.rejects(FrankenDB.open({worker}),invalid);assert.equal(worker.terminateCount,1);assert.equal(worker.messages.size,0);
});

test('reply decoding: corrupt write response in a managed transaction rolls back real SQL, once',limits,async t=>{
  const f=sqliteSnapshotWorker(),db=await FrankenDB.open({worker:f.worker});t.after(()=>db.close());
  await db.execute('CREATE TABLE items(id PRIMARY KEY)');
  const handle=f.host.handle.bind(f.host);let writes=0;
  f.host.handle=async request=>{
    const response=await handle(request);
    if(request.kind==='execute'&&request.sql.startsWith('INSERT')){writes++;return {kind:'error',requestId:request.requestId,error:null};}
    return response;
  };
  await assert.rejects(db.transaction(tx=>tx.execute('INSERT INTO items VALUES(1)')),invalid);
  const other=new DatabaseSync(db.path);t.after(()=>other.close());
  assert.equal(other.prepare('SELECT count(*) n FROM items').get().n,0);assert.equal(writes,1);
  assert.deepEqual(other.prepare('PRAGMA integrity_check').all().map(Object.values),[['ok']]);
});
