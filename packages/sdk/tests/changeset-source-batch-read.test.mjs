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
