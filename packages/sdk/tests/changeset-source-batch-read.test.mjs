// Real persistent source classes and native SQLite snapshots; not Rust-engine qualification.
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { ChangesetOutbox, CHANGESET_OUTBOX_TABLE } from '../src/changeset-outbox.ts';
import { ChangesetFanout } from '../src/changeset-fanout.ts';
import { createOrderedChangesetBatchTransport, createOrderedChangesetReceiver } from '../src/changeset-ordered-delivery.ts';
import { ChangesetOrder } from '../src/changeset-order.ts';
import { applyChangeset } from '../src/changeset-apply.ts';
const T = `"${CHANGESET_OUTBOX_TABLE}"`;
const pause = () => { let resolve; const promise = new Promise(r => { resolve = r; }); return { promise, resolve }; };
const rows = (db, sql, params = []) => {
  const stmt = db.prepare(sql); stmt.setReadBigInts(true); stmt.setReturnArrays(true); return stmt.all(...params);
};
class Owner {
  constructor(path = ':memory:') {
    this.db = new DatabaseSync(path); this.depth = 0; this.begins = 0; this.serial = 0; this.before = null;
    this.db.exec('PRAGMA foreign_keys=ON; PRAGMA recursive_triggers=ON; CREATE TABLE IF NOT EXISTS items(id INTEGER PRIMARY KEY,value TEXT)');
  }
  async transaction(work) {
    const depth = this.depth++, name = `batch_read_${++this.serial}`; this.begins++;
    this.db.exec(depth ? `SAVEPOINT ${name}` : 'BEGIN'); let committed = false;
    const tx = {
      execute: async (sql, params = []) => { await this.before?.(sql, params); return Number(this.db.prepare(sql).run(...params).changes); },
      query: async (sql, params = []) => { await this.before?.(sql, params); return { rowArrays: rows(this.db, sql, params) }; },
      transaction: child => this.transaction(child),
    };
    try { const result = await work(tx); this.db.exec(depth ? `RELEASE ${name}` : 'COMMIT'); committed = true; return result; }
    catch (e) { if (!committed) this.db.exec(depth ? `ROLLBACK TO ${name}; RELEASE ${name}` : 'ROLLBACK'); throw e; }
    finally { this.depth--; }
  }
  close() { this.db.close(); }
}
async function fixture(t, fanout = false, count = 3, path) {
  const owner = new Owner(path); t.after(() => owner.close());
  const outbox = new ChangesetOutbox(owner), group = fanout ? await ChangesetFanout.open(owner, ['east','west']) : null;
  const deliveries = [];
  for (let n = 1; n <= count; n++) deliveries.push((await outbox.record(tx => tx.execute('INSERT INTO items VALUES(?,?)', [BigInt(n), `value-${n}`]),
    { deliveryId: `source:${n}`, tables: ['items'] })).delivery);
  return { owner, outbox, group, port: group?.forReplica('east') ?? outbox, deliveries, ids: deliveries.map(d => d.deliveryId) };
}
const ack = d => ({ deliveryId:d.deliveryId, sha256:d.sha256 });
const envelope = (record, receiverId = 'east') => ({ protocol:'fsqlite-changeset-v1',receiverId,
  deliveryId:record.delivery.deliveryId,sha256:record.delivery.sha256,changeset:record.changeset });
const fakeAck = m => ({ protocol:m.protocol,receiverId:m.receiverId,deliveryId:m.deliveryId,sha256:m.sha256,
  byteLength:m.changeset.byteLength,applied:1,omitted:0,replayed:false,confirmed:true,order:m.order });

for (const fanout of [false,true]) {
  const mode = fanout ? 'fanout' : 'single';
  test(`source batch ${mode}: one snapshot, input order, independently owned bytes`, async t => {
    const f = await fixture(t,fanout), start = f.owner.begins;
    const result = await f.port.readBatch([...f.ids].reverse());
    assert.equal(f.owner.begins-start,1); assert.ok(Object.isFrozen(result));
    assert.deepEqual(result.map(r=>r.delivery.deliveryId),[...f.ids].reverse());
    const db = new DatabaseSync(':memory:');
    try { db.exec('CREATE TABLE items(id INTEGER PRIMARY KEY,value TEXT)');
      for (const r of result) { assert.ok(Object.isFrozen(r)); assert.ok(Object.isFrozen(r.delivery)); assert.equal(db.applyChangeset(r.changeset),true); }
      assert.deepEqual(rows(db,'SELECT * FROM items ORDER BY id'),rows(f.owner.db,'SELECT * FROM items ORDER BY id'));
    } finally { db.close(); }
    const original = (await f.port.read(f.ids[2])).changeset;
    result[0].changeset.fill(0);
    assert.deepEqual((await f.port.readBatch([f.ids[2]]))[0].changeset,original);
    assert.equal((await f.outbox.pending()).length,3,'read must never acknowledge');
  });
  test(`source batch ${mode}: complete budget admission before any payload query`, async t => {
    const f = await fixture(t,fanout), total = f.deliveries.reduce((n,d)=>n+d.byteLength,0);let payloadQueries=0;
    f.owner.before = sql => { if (sql.startsWith('SELECT payload')) payloadQueries++; };
    await assert.rejects(f.port.readBatch(f.ids,{maxBytes:total-1}),{code:'ERR_FSQLITE_OUTBOX_FULL'});
    assert.equal(payloadQueries,0);
    assert.equal((await f.port.readBatch(f.ids,{maxBytes:total})).length,3); assert.equal(payloadQueries,3);
  });
  test(`source batch ${mode}: acknowledged members require no payload budget`, async t => {
    const f=await fixture(t,fanout); await f.port.acknowledge(f.ids[0],f.deliveries[0].sha256);
    const result=await f.port.readBatch(f.ids,{maxBytes:f.deliveries[1].byteLength+f.deliveries[2].byteLength});
    assert.equal(result[0].delivery.acknowledged,true); assert.equal(result[0].changeset,null);
    assert.ok(result.slice(1).every(r=>!r.delivery.acknowledged&&r.changeset.length));
    if(fanout) {
      const other=await f.group.forReplica('west').readBatch(f.ids);
      assert.ok(other.every(r=>!r.delivery.acknowledged&&r.changeset.length));
      assert.equal((await f.group.progress()).acknowledgedThrough,0n);
    }
  });
  test(`source batch ${mode}: missing final identity rejects before payload loading`,async t=>{
    const f=await fixture(t,fanout);let loads=0;
    f.owner.before=sql=>{if(sql.startsWith('SELECT payload'))loads++;};
    await assert.rejects(f.port.readBatch([f.ids[0],'missing']),{code:'ERR_FSQLITE_OUTBOX_STATE'});
    assert.equal(loads,0); assert.equal(f.owner.depth,0);
  });
  test(`source batch ${mode}: corrupt final payload never returns a partial result`,async t=>{
    const f=await fixture(t,fanout), good=(await f.port.read(f.ids[2])).changeset;
    f.owner.db.prepare(`UPDATE ${T} SET payload=zeroblob(length(payload)) WHERE seq=3`).run();
    await assert.rejects(f.port.readBatch(f.ids),{code:'ERR_FSQLITE_OUTBOX_CORRUPT'});
    assert.equal((await f.outbox.pending()).length,3);
    f.owner.db.prepare(`UPDATE ${T} SET payload=? WHERE seq=3`).run(good);
    assert.equal((await f.port.readBatch(f.ids)).length,3);
  });
  test(`source batch ${mode}: malformed requests reject before SQL or accessors`,async t=>{
    const f=await fixture(t,fanout), before=f.owner.begins;let calls=0;
    const accessor=[f.ids[0]];Object.defineProperty(accessor,'0',{get(){calls++;return f.ids[0];}});
    for(const ids of [[],new Array(1),new Array(257),accessor,[f.ids[0],f.ids[0]],[''],['a\0b'],{}])
      await assert.rejects(f.port.readBatch(ids));
    for(const maxBytes of [0,-1,1.5,NaN,64*1024*1024+1])await assert.rejects(f.port.readBatch(f.ids,{maxBytes}));
    assert.equal(calls,0);assert.equal(f.owner.begins,before);
  });
  test(`source batch ${mode}: identities and options captured before first await`,async t=>{
    const f=await fixture(t,fanout), entered=pause(), release=pause();let first=true;
    f.owner.before=async()=>{if(first){first=false;entered.resolve();await release.promise;}};
    const ids=[...f.ids], options={maxBytes:64*1024*1024}, pending=f.port.readBatch(ids,options);
    try { await Promise.race([entered.promise,pending]);ids.fill('changed');ids.length=0;options.maxBytes=1;
      release.resolve();assert.deepEqual((await pending).map(r=>r.delivery.deliveryId),f.ids);
    } finally{release.resolve();await Promise.allSettled([pending]);}
  });
  test(`source batch ${mode}: WAL peer acknowledgement cannot mix snapshot generations`,async t=>{
    const path=join(mkdtempSync(join(tmpdir(),'fsqlite-source-read-')),'source.db');
    const f=await fixture(t,fanout,3,path);f.owner.db.exec('PRAGMA journal_mode=WAL');
    const writer=new Owner(path);t.after(()=>writer.close());
    const port=fanout?(await ChangesetFanout.open(writer,['east','west'])).forReplica('east'):new ChangesetOutbox(writer);
    const entered=pause(),release=pause();let held=false;
    f.owner.before=async sql=>{if(!held&&sql.startsWith('SELECT payload')){held=true;entered.resolve();await release.promise;}};
    const pending=f.port.readBatch(f.ids);
    try {await Promise.race([entered.promise,pending]);await port.acknowledgeBatch(f.deliveries.map(ack));
      release.resolve();const snapshot=await pending;assert.ok(snapshot.every(r=>!r.delivery.acknowledged&&r.changeset.length));
      const after=await f.port.readBatch(f.ids,{maxBytes:1});assert.ok(after.every(r=>r.delivery.acknowledged&&r.changeset===null));
    }finally{release.resolve();await Promise.allSettled([pending]);}
  });
}

test('source batch: retained bootstrap manifest validation survives acknowledged reads',async t=>{
  const f=await fixture(t,false,0);f.owner.db.exec("INSERT INTO items VALUES(1,'seed'),(2,'seed2')");
  await f.outbox.bootstrapChunks({deliveryId:'seed',tables:['items'],chunkRows:1});
  const entries=await f.outbox.pending();await f.outbox.acknowledgeBatch(entries.map(ack));
  f.owner.db.prepare(`DELETE FROM ${T} WHERE seq=1`).run();
  await assert.rejects(f.outbox.readBatch([entries[1].deliveryId],{maxBytes:1}));
});
test('source batch: 256 empty identities and absent outbox are explicit',async t=>{
  const f=await fixture(t,false,0);await assert.rejects(f.outbox.readBatch(['missing']));
  for(let n=0;n<256;n++)await f.outbox.record(()=>{}, {tables:['items'],deliveryId:`empty:${n}`});
  const records=await f.outbox.readBatch(Array.from({length:256},(_,n)=>`empty:${n}`),{maxBytes:1});
  assert.equal(records.length,256);assert.ok(records.every(r=>r.changeset.length===0));
});

for(const fanout of [false,true])test(`batch transport uses a complete persistent source snapshot (${fanout})`,async t=>{
  const f=await fixture(t,fanout), records=[];
  for(const id of f.ids)records.push(await f.port.read(id));
  const receiverOwner=new Owner();t.after(()=>receiverOwner.close());
  const order=new ChangesetOrder(receiverOwner,{receiverId:'east',sourceId:'source:incarnation'});await order.initialize();
  const receiver=await createOrderedChangesetReceiver(order,{apply:(target,m,c)=>applyChangeset(target,m.changeset,{...c,tables:['items'],deliveryId:m.deliveryId}),confirmCommit:async()=>assert.equal(receiverOwner.depth,0)});
  let batchReads=0;const port={receiverId:fanout?'east':undefined,
    read(){assert.fail('must not split a snapshot into individual reads');},
    readBatch(ids,opts){batchReads++;return f.port.readBatch(ids,opts);}};
  const start=f.owner.begins;
  const send=createOrderedChangesetBatchTransport(port,{receiverId:'east',streamId:'source:incarnation',deliver:(m,c)=>{assert.equal(f.owner.depth,0);return receiver.receiveBatch(m,c);}});
  const acked=await send(records.map(r=>envelope(r)));
  assert.equal(batchReads,1);assert.equal(f.owner.begins-start,1);assert.equal(acked.length,3);
  assert.deepEqual(rows(receiverOwner.db,'SELECT * FROM items ORDER BY id'),rows(f.owner.db,'SELECT * FROM items ORDER BY id'));
  assert.equal((await f.outbox.pending()).length,3);
});
for(const mode of ['reject','short','accessor','wrong-id'])test(`batch transport refuses ${mode} snapshot without fallback or transmission`,async t=>{
  const f=await fixture(t),records=[]; for(const id of f.ids) records.push(await f.port.read(id));let read=0,sent=0,getters=0;
  const port={read(){read++;assert.fail('fallback');},async readBatch(){
    if(mode==='reject')throw new Error('snapshot unavailable');
    if(mode==='short')return records.slice(1);
    const copy=[...records];if(mode==='accessor')Object.defineProperty(copy,'0',{get(){getters++;return records[0];}});
    if(mode==='wrong-id')copy[2]={...records[2],delivery:{...records[2].delivery,deliveryId:'different'}};
    return copy;}};
  const send=createOrderedChangesetBatchTransport(port,{receiverId:'east',streamId:'s',deliver:async m=>{sent++;return m.map(fakeAck);}});
  await assert.rejects(send(records.map(r=>envelope(r))));assert.equal(read,0);assert.equal(sent,0);assert.equal(getters,0);
});
test('batch transport captures source method ownership and joins cancelled snapshot reads',async t=>{
  const f=await fixture(t),records=[]; for(const id of f.ids) records.push(await f.port.read(id)); const entered=pause(),release=pause(),c=new AbortController();let sent=0;
  class Port { #box; constructor(){this.#box=f.port;} read(){assert.fail('fallback');} async readBatch(ids,opts){entered.resolve();await release.promise;return this.#box.readBatch(ids,opts);} }
  const port=new Port();const send=createOrderedChangesetBatchTransport(port,{receiverId:'east',streamId:'s',deliver:async m=>{sent++;return m.map(fakeAck);}});
  port.readBatch=()=>assert.fail('method replacement');
  const pending=send(records.map(r=>envelope(r)),{signal:c.signal});let settled=false;void pending.then(()=>{settled=true;},()=>{settled=true;});
  try{await Promise.race([entered.promise,pending]);c.abort();await new Promise(r=>setImmediate(r));assert.equal(settled,false);
    await assert.rejects(send(records.map(r=>envelope(r))),{code:'ERR_FSQLITE_ORDERED_BUSY'});release.resolve();
    await assert.rejects(pending,{code:'ERR_FSQLITE_ORDERED_CANCELLED'});assert.equal(sent,0);
  }finally{release.resolve();await Promise.allSettled([pending]);}
});
test('batch transport legacy source fallback remains available',async t=>{
  const f=await fixture(t),records=[]; for(const id of f.ids) records.push(await f.port.read(id));let calls=0;
  const source={async read(id){calls++;return f.port.read(id);}};
  const send=createOrderedChangesetBatchTransport(source,{receiverId:'east',streamId:'s',deliver:async m=>m.map(fakeAck)});
  assert.equal((await send(records.map(r=>envelope(r)))).length,3);assert.equal(calls,3);
  assert.throws(()=>createOrderedChangesetBatchTransport({...source,readBatch:true},{receiverId:'east',streamId:'s',deliver:async()=>[]}),{code:'ERR_FSQLITE_ORDERED_INPUT'});
});

// Pump admission must use the same production snapshot capability, not only
// the later authoritative sender check. Wrappers below observe or fault real
// source calls; SQL, capture, source retention and receiver application are real.
import { ChangesetDeliveryPump } from '../src/changeset-delivery.ts';
async function pumpFixture(t, f, overrides = {}) {
  const destination = new Owner(); t.after(() => destination.close());
  const order = new ChangesetOrder(destination, { receiverId:'east', sourceId:'source:incarnation' });
  await order.initialize();
  const state = { snapshots:[], singles:0, exchanges:[], confirmations:0, applications:0 };
  const receiver = await createOrderedChangesetReceiver(order, {
    apply:(target,m,c)=>{state.applications++;return applyChangeset(target,m.changeset,{...c,tables:['items'],deliveryId:m.deliveryId});},
    confirmCommit:async()=>assert.equal(destination.depth,0),
  });
  const source = {
    receiverId:'east', pending:opts=>f.port.pending(opts),
    read:id=>{state.singles++;return f.port.read(id);},
    readBatch:async(ids,opts)=>{
      assert.ok(Object.isFrozen(ids)); assert.ok(Object.isFrozen(opts));
      const before=f.owner.begins;
      const result=await f.port.readBatch(ids,opts);
      assert.equal(f.owner.begins-before,1,'one source transaction per pump snapshot');
      state.snapshots.push({ids:[...ids],maxBytes:opts.maxBytes});return result;
    },
    acknowledge:(id,sum)=>f.port.acknowledge(id,sum),
    acknowledgeBatch:values=>f.port.acknowledgeBatch(values),
    ...overrides.source,
  };
  const send=createOrderedChangesetBatchTransport(f.port,{receiverId:'east',streamId:'source:incarnation',
    deliver:async(m,c)=>{assert.equal(f.owner.depth,0);state.exchanges.push(m.map(x=>x.deliveryId));return receiver.receiveBatch(m,c);}});
  const pump=new ChangesetDeliveryPump(source,{
    receiverId:'east',
    deliver:async(m,c)=>receiver.receive({...m,order:{protocol:'fsqlite-ordered-changeset-v1',streamId:'source:incarnation',sequence:(await f.port.read(m.deliveryId)).delivery.sequence.toString()}},c),
    deliverBatch:send,
    confirmSource:async()=>{assert.equal(f.owner.depth,0);state.confirmations++;},
    ...overrides.options,
  });
  return {pump,source,state,destination,receiver,order};
}
const tick=()=>new Promise(r=>setImmediate(r));
const pumpFailure=phase=>e=>e?.name==='ChangesetDeliveryError'&&e.phase===phase;

for(const fanout of [false,true]) for(const atomic of [false,true])
  test(`pump snapshot: production ${fanout?'fanout':'outbox'}, atomic ACK ${atomic}`,async t=>{
    const f=await fixture(t,fanout),p=await pumpFixture(t,f);
    const result=await p.pump.runBatch({atomicAcknowledgements:atomic});
    assert.equal(result.deliveries,3);assert.equal(result.stopped,'empty');
    assert.equal(p.state.singles,0);assert.equal(p.state.snapshots.length,1);
    assert.deepEqual(p.state.snapshots[0],{ids:f.ids,maxBytes:f.deliveries.reduce((n,d)=>n+d.byteLength,0)});
    assert.deepEqual(rows(p.destination.db,'SELECT * FROM items ORDER BY id'),rows(f.owner.db,'SELECT * FROM items ORDER BY id'));
    if(fanout){
      assert.equal((await f.group.progress()).acknowledgedThrough,0n);
      assert.equal((await f.group.forReplica('west').readBatch(f.ids)).filter(r=>r.changeset!==null).length,3);
    }else assert.equal((await f.outbox.pending()).length,0);
  });

test('pump snapshot: admit only the exact byte-bounded prefix before payload reads',async t=>{
  const f=await fixture(t),p=await pumpFixture(t,f),limit=f.deliveries[0].byteLength+f.deliveries[1].byteLength;
  const good=(await f.port.read(f.ids[2])).changeset;
  f.owner.db.prepare(`UPDATE ${T} SET payload=zeroblob(length(payload)) WHERE seq=3`).run();
  const result=await p.pump.runBatch({maxDeliveries:2,maxBatchBytes:limit,maxBytes:limit});
  assert.equal(result.deliveries,2);assert.equal(result.bytes,limit);
  assert.deepEqual(p.state.snapshots,[{ids:f.ids.slice(0,2),maxBytes:limit}]);
  assert.equal(p.state.singles,0);
  f.owner.db.prepare(`UPDATE ${T} SET payload=? WHERE seq=3`).run(good);
  assert.equal((await p.pump.runBatch()).deliveries,1);
});

test('pump snapshot: aggregate window and run limits select independent bounded prefixes',async t=>{
  const f=await fixture(t),p=await pumpFixture(t,f);
  const result=await p.pump.runBatch({maxBatchBytes:f.deliveries[0].byteLength,maxBytes:f.deliveries[0].byteLength+f.deliveries[1].byteLength});
  assert.equal(result.deliveries,2);assert.equal(result.stopped,'limit');
  assert.deepEqual(p.state.snapshots.map(x=>x.ids),f.ids.slice(0,2).map(id=>[id]));
  assert.deepEqual((await f.port.pending()).map(d=>d.deliveryId),[f.ids[2]]);
});

test('pump snapshot: oversized oldest message refuses before invoking either reader',async t=>{
  const f=await fixture(t),p=await pumpFixture(t,f,{options:{maxMessageBytes:1}});
  await assert.rejects(p.pump.runBatch(),pumpFailure('source-read'));
  assert.equal(p.state.snapshots.length,0);assert.equal(p.state.singles,0);assert.equal(p.state.exchanges.length,0);
});

for(const fault of ['reject','short','extra','hole','accessor','wrong-id','wrong-sequence','wrong-digest','wrong-size','missing-bytes','ack-with-bytes','oversized-bytes','bad-hash'])
  test(`pump snapshot: ${fault} response rejects the whole window without fallback`,async t=>{
    const f=await fixture(t);let reads=0,accessors=0;
    const p=await pumpFixture(t,f,{source:{readBatch:async(ids,opts)=>{
      reads++;if(fault==='reject')throw new Error('source snapshot failed');
      const result=(await f.port.readBatch(ids,opts)).map(r=>({delivery:{...r.delivery},changeset:r.changeset}));
      if(fault==='short')return result.slice(0,-1);
      if(fault==='extra')return [...result,result[0]];
      if(fault==='hole')delete result[2];
      if(fault==='accessor')Object.defineProperty(result,'2',{get(){accessors++;return result[0];}});
      if(fault==='wrong-id')result[2].delivery.deliveryId='different';
      if(fault==='wrong-sequence')result[2].delivery.sequence=9n;
      if(fault==='wrong-digest')result[2].delivery.sha256='0'.repeat(64);
      if(fault==='wrong-size')result[2].delivery.byteLength++;
      if(fault==='missing-bytes')result[2].changeset=null;
      if(fault==='ack-with-bytes')result[2].delivery.acknowledged=true;
      if(fault==='oversized-bytes')result[2].changeset=new Uint8Array(result[2].delivery.byteLength+1);
      if(fault==='bad-hash')result[2].changeset.fill(0);
      return result;
    }}});
    await assert.rejects(p.pump.runBatch(),pumpFailure('source-read'));
    assert.equal(reads,1);assert.equal(p.state.singles,0);assert.equal(accessors,0);
    assert.equal(p.state.exchanges.length,0);assert.equal(p.state.applications,0);
    assert.equal((await f.outbox.pending()).length,3);
  });

test('pump snapshot: actual late payload corruption prevents any remote application',async t=>{
  const f=await fixture(t),p=await pumpFixture(t,f);
  const original=(await f.port.read(f.ids[2])).changeset;
  f.owner.db.prepare(`UPDATE ${T} SET payload=zeroblob(length(payload)) WHERE seq=3`).run();
  await assert.rejects(p.pump.runBatch(),pumpFailure('source-read'));
  assert.equal(p.state.exchanges.length,0);assert.equal(p.state.singles,0);
  f.owner.db.prepare(`UPDATE ${T} SET payload=? WHERE seq=3`).run(original);
  assert.equal((await p.pump.runBatch()).deliveries,3);
  assert.equal(p.state.snapshots.length,1);
});

test('pump snapshot: all returned buffers and metadata are owned before the first digest awaits',async t=>{
  const f=await fixture(t);let returned,mutations=0;
  const p=await pumpFixture(t,f,{source:{readBatch:async(ids,opts)=>{
    returned=(await f.port.readBatch(ids,opts)).map(r=>({delivery:{...r.delivery},changeset:r.changeset}));return returned;
  }}});
  const real=crypto.subtle.digest;
  crypto.subtle.digest=function(...args){
    if(returned&&mutations===0){
      mutations++;
      for(const r of returned){r.changeset.fill(0);structuredClone(r.changeset.buffer,{transfer:[r.changeset.buffer]});r.delivery.deliveryId='changed';}
      returned.reverse();returned.length=0;
    }
    return real.apply(this,args);
  };
  try{
    assert.equal((await p.pump.runBatch()).deliveries,3);assert.equal(mutations,1);
    assert.deepEqual(rows(p.destination.db,'SELECT * FROM items ORDER BY id'),rows(f.owner.db,'SELECT * FROM items ORDER BY id'));
  }finally{crypto.subtle.digest=real;}
});

for(const ending of ['cancel','timeout'])test(`pump snapshot: ${ending} joins held source SQL before reopening admission`,async t=>{
  const f=await fixture(t),p=await pumpFixture(t,f),entered=pause(),release=pause(),controller=new AbortController();let held=false;
  f.owner.before=async sql=>{if(!held&&sql.startsWith('SELECT payload')){held=true;entered.resolve();await release.promise;}};
  const operation=p.pump.runBatch(ending==='cancel'?{signal:controller.signal}:{timeoutMs:250});
  let settled=false;void operation.then(()=>{settled=true;},()=>{settled=true;});
  try{
    await Promise.race([entered.promise,operation]);
    if(ending==='cancel')controller.abort();else await new Promise(r=>setTimeout(r,280));
    await tick();assert.equal(settled,false);assert.equal(f.owner.depth,1);
    await assert.rejects(p.pump.runBatch(),e=>e.code==='ERR_FSQLITE_DELIVERY_BUSY');
    release.resolve();await assert.rejects(operation,pumpFailure('source-read'));
    assert.equal(f.owner.depth,0);assert.equal(p.state.exchanges.length,0);assert.equal(p.state.snapshots.length,1);
    f.owner.before=null;assert.equal((await p.pump.runBatch()).deliveries,3);
  }finally{release.resolve();await Promise.allSettled([operation]);}
});

test('pump snapshot: source capability is bound once with private-field ownership',async t=>{
  const f=await fixture(t);let calls=0;
  class Port{
    #source=f.port;
    pending(o){return this.#source.pending(o);}read(id){assert.fail('individual pump read');}
    acknowledge(id,sum){return this.#source.acknowledge(id,sum);}
    readBatch(ids,o){calls++;return this.#source.readBatch(ids,o);}
  }
  const instance=new Port();
  const p=await pumpFixture(t,f);
  const send=createOrderedChangesetBatchTransport(f.port,{receiverId:'east',streamId:'source:incarnation',deliver:(m,c)=>p.receiver.receiveBatch(m,c)});
  const pump=new ChangesetDeliveryPump(instance,{receiverId:'east',deliver:async()=>assert.fail('single delivery'),deliverBatch:send,confirmSource:async()=>{}});
  instance.readBatch=()=>assert.fail('replaced method');
  assert.equal((await pump.runBatch()).deliveries,3);assert.equal(calls,1);
  assert.throws(()=>new ChangesetDeliveryPump({...p.source,readBatch:true},{receiverId:'east',deliver:async()=>{},confirmSource:async()=>{}}),e=>e.code==='ERR_FSQLITE_DELIVERY_INPUT');
});

test('pump snapshot: read-time ACK in the middle splits delivery without skipping its pending prefix',async t=>{
  const f=await fixture(t);let first=true,readCalls=0;
  const p=await pumpFixture(t,f,{source:{readBatch:async(ids,o)=>{
    readCalls++;if(first){first=false;await f.outbox.acknowledge(f.ids[1],f.deliveries[1].sha256);}return f.port.readBatch(ids,o);
  }}});
  const prior=await f.port.readBatch(f.ids.slice(0,2));
  await p.receiver.receiveBatch(prior.map((r,i)=>({...envelope(r),order:{protocol:'fsqlite-ordered-changeset-v1',streamId:'source:incarnation',sequence:String(i+1)}})));
  const result=await p.pump.runBatch();
  assert.equal(result.deliveries,2);assert.equal(result.replays,1);assert.equal(result.alreadyAcknowledged,1);
  assert.deepEqual(p.state.exchanges,[[f.ids[0]],[f.ids[2]]]);assert.equal(readCalls,2);assert.equal(p.state.singles,0);
  assert.equal(p.state.applications,3);assert.equal((await f.outbox.pending()).length,0);
});

test('pump snapshot: metadata ACK gaps exclude later identities from the read window',async t=>{
  const f=await fixture(t),p=await pumpFixture(t,f);
  const prior=await f.port.readBatch(f.ids.slice(0,2));
  await p.receiver.receiveBatch(prior.map((r,i)=>({...envelope(r),order:{protocol:'fsqlite-ordered-changeset-v1',streamId:'source:incarnation',sequence:String(i+1)}})));
  await f.outbox.acknowledge(f.ids[1],f.deliveries[1].sha256);
  const result=await p.pump.runBatch();assert.equal(result.deliveries,2);
  assert.deepEqual(p.state.snapshots.map(r=>r.ids),[[f.ids[0]],[f.ids[2]]]);
});

test('pump snapshot: 256 zero-byte identities remain readable with a one-byte option budget',async t=>{
  const f=await fixture(t,false,0);
  for(let n=0;n<256;n++)await f.outbox.record(()=>{},{tables:['items'],deliveryId:`empty:${n}`});
  const p=await pumpFixture(t,f);
  const result=await p.pump.runBatch({maxDeliveries:256,maxBatchDeliveries:256,maxBatchBytes:1,maxBytes:1,atomicAcknowledgements:true});
  assert.equal(result.deliveries,256);assert.equal(result.bytes,0);assert.equal(result.applied,0);
  assert.equal(p.state.snapshots.length,1);assert.equal(p.state.snapshots[0].ids.length,256);assert.equal(p.state.snapshots[0].maxBytes,1);
  assert.equal((await f.outbox.pending()).length,0);
});

test('pump snapshot: empty messages fit after the positive run byte allowance is exhausted',async t=>{
  const f=await fixture(t,false,1);
  await f.outbox.record(()=>{},{tables:['items'],deliveryId:'empty:next'});
  await f.outbox.record(tx=>tx.execute("INSERT INTO items VALUES(3,'later')"),{tables:['items'],deliveryId:'source:later'});
  const p=await pumpFixture(t,f);
  const result=await p.pump.runBatch({maxBatchDeliveries:1,maxBytes:f.deliveries[0].byteLength});
  assert.equal(result.deliveries,2);assert.equal(result.stopped,'limit');
  assert.deepEqual(p.state.snapshots.map(r=>r.maxBytes),[f.deliveries[0].byteLength,1]);
  assert.deepEqual((await f.outbox.pending()).map(d=>d.deliveryId),['source:later']);
});

test('pump snapshot: original bootstrap chunks retain ordered acknowledgement and replay',async t=>{
  const f=await fixture(t,false,0);f.owner.db.exec("INSERT INTO items VALUES(1,'a'),(2,'b'),(3,'c')");
  await f.outbox.bootstrapChunks({deliveryId:'seed',tables:['items'],chunkRows:1});
  const p=await pumpFixture(t,f);
  assert.equal((await p.pump.runBatch({atomicAcknowledgements:true})).deliveries,3);
  assert.equal(p.state.snapshots.length,1);assert.equal((await f.outbox.bootstrapChunks({deliveryId:'seed',tables:['items'],chunkRows:1})).complete,true);
});

test('pump snapshot: absent capability retains legacy reads and single run never opts into batch reads',async t=>{
  const f=await fixture(t),p=await pumpFixture(t,f,{source:{readBatch:undefined}});
  assert.equal((await p.pump.runBatch()).deliveries,3);assert.equal(p.state.singles,3);
  const second=await fixture(t),single=await pumpFixture(t,second,{source:{readBatch(){assert.fail('single run must not batch');}}});
  assert.equal((await single.pump.run()).deliveries,3);assert.equal(single.state.singles,3);
});

for(const fanout of [false,true])test(`pump snapshot: concurrent WAL ACK keeps all loaded members at one snapshot (${fanout})`,async t=>{
  const path=join(mkdtempSync(join(tmpdir(),'fsqlite-pump-snapshot-')),'source.db');
  const f=await fixture(t,fanout,3,path);f.owner.db.exec('PRAGMA journal_mode=WAL');
  let observed;
  const p=await pumpFixture(t,f,{source:{readBatch:async(ids,o)=>{observed=await f.port.readBatch(ids,o);return observed;}}});
  // The peer's ACKs are legitimate: this destination already committed exactly
  // these messages. The pump must use retained receipts, not reapply source DML.
  const prior=await f.port.readBatch(f.ids);
  await p.receiver.receiveBatch(prior.map((r,i)=>({...envelope(r),order:{protocol:'fsqlite-ordered-changeset-v1',streamId:'source:incarnation',sequence:String(i+1)}})));
  const peer=new Owner(path);t.after(()=>peer.close());
  const port=fanout?(await ChangesetFanout.open(peer,['east','west'])).forReplica('east'):new ChangesetOutbox(peer);
  const entered=pause(),release=pause();let held=false;
  f.owner.before=async sql=>{if(!held&&sql.startsWith('SELECT payload')){held=true;entered.resolve();await release.promise;}};
  const operation=p.pump.runBatch({atomicAcknowledgements:true});
  try{
    await Promise.race([entered.promise,operation]);
    await port.acknowledgeBatch(f.deliveries.map(ack));release.resolve();
    const result=await operation;
    assert.ok(observed.every(r=>!r.delivery.acknowledged&&r.changeset.length));
    assert.equal(result.deliveries,3);assert.equal(result.replays,3);assert.equal(p.state.applications,3);
    assert.equal(p.state.singles,0);assert.equal(p.state.exchanges.length,1);
    assert.ok((await f.port.readBatch(f.ids,{maxBytes:1})).every(r=>r.delivery.acknowledged&&r.changeset===null));
  }finally{release.resolve();await Promise.allSettled([operation]);}
});
