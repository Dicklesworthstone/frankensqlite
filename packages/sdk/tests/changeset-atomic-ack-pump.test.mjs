// Actual outbox/fanout/capture code on reference SQLite, not Rust-engine evidence.
import assert from "node:assert/strict";
import { test } from "node:test";
import { DatabaseSync } from "node:sqlite";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ChangesetOutbox, CHANGESET_OUTBOX_TABLE } from "../src/changeset-outbox.ts";
import { ChangesetFanout } from "../src/changeset-fanout.ts";

const T = `"${CHANGESET_OUTBOX_TABLE}"`;
const pause = () => { let resolve; const promise = new Promise(r => { resolve = r; }); return { promise, resolve }; };
const rows = (db, sql, params = []) => {
  const stmt = db.prepare(sql); stmt.setReadBigInts(true); stmt.setReturnArrays(true); return stmt.all(...params);
};
class Owner {
  constructor(path = ":memory:") {
    this.db = new DatabaseSync(path);
    this.db.exec("PRAGMA foreign_keys=ON; PRAGMA recursive_triggers=ON; CREATE TABLE IF NOT EXISTS items(id INTEGER PRIMARY KEY,value TEXT)");
    this.depth = 0; this.serial = 0; this.begins = 0; this.beforeSql = null; this.beforeCommit = null; this.afterCommit = null;
  }
  async transaction(work) {
    const depth = this.depth++, name = `ack_${++this.serial}`;
    this.begins++; this.db.exec(depth ? `SAVEPOINT ${name}` : "BEGIN");
    let committed = false, active = true;
    const check = () => assert.ok(active, "expired owner");
    const tx = {
      execute: async (sql, params = []) => { check(); await this.beforeSql?.(sql, params); return Number(this.db.prepare(sql).run(...params).changes); },
      query: async (sql, params = []) => { check(); await this.beforeSql?.(sql, params); return { rowArrays: rows(this.db, sql, params) }; },
      transaction: child => { check(); return this.transaction(child); },
    };
    try {
      const result = await work(tx); active = false; await this.beforeCommit?.(depth);
      this.db.exec(depth ? `RELEASE ${name}` : "COMMIT"); committed = true;
      await this.afterCommit?.(depth); return result;
    } catch (error) {
      active = false;
      if (!committed) this.db.exec(depth ? `ROLLBACK TO ${name}; RELEASE ${name}` : "ROLLBACK");
      throw error;
    } finally { active = false; this.depth--; }
  }
  close() { this.db.close(); }
}

import { ChangesetDeliveryPump } from "../src/changeset-delivery.ts";
import { ChangesetOrder } from "../src/changeset-order.ts";
import { applyChangeset } from "../src/changeset-apply.ts";
import { createOrderedChangesetReceiver, createOrderedChangesetTransport, createOrderedChangesetBatchTransport } from "../src/changeset-ordered-delivery.ts";
import { createOrderedChangesetBatchHttpHandler, createOrderedChangesetBatchHttpTransport } from "../src/changeset-http.ts";
import { createServer } from "node:http";
import { once } from "node:events";
import { Readable } from "node:stream";

async function fixture(t, multicast = false) {
  const source = new Owner(), outbox = new ChangesetOutbox(source);
  t.after(() => source.close());
  const fanout = multicast ? await ChangesetFanout.open(source, ["east", "west"]) : null;
  for (let i = 1; i <= 3; i++) await outbox.record(tx => tx.execute("INSERT INTO items VALUES(?,?)", [BigInt(i), `row-${i}`]),
    { tables: ["items"], deliveryId: `source:${i}` });
  const f = { source, outbox, fanout, confirmed: 0, acknowledgements: [], confirmHook: null, deliveries: 0 };
  source.beforeSql = sql => { if (sql.includes("SET acknowledged=1") || sql.includes('UPDATE OR ABORT main."__fsqlite_changeset_fanout_progress"'))
    f.acknowledgements.push(source.serial); };
  f.make = async (id = "east", options = {}) => {
    const destination = new Owner(); t.after(() => destination.close());
    const order = new ChangesetOrder(destination, { receiverId: id, sourceId: "source:incarnation" }); await order.initialize();
    const settings = { tables: ["items"] }; let applications = 0;
    const receiver = await createOrderedChangesetReceiver(order, {
      apply: (target, message, controls) => { applications++; return applyChangeset(target, message.changeset, {
        ...settings, ...controls, deliveryId: message.deliveryId,
      }); },
      confirmCommit: async () => assert.equal(destination.depth, 0),
    });
    const port = options.port ?? fanout?.forReplica(id) ?? outbox;
    const single = createOrderedChangesetTransport(port, { receiverId: id, streamId: "source:incarnation", deliver: (m,c) => receiver.receive(m,c) });
    const exchange = options.exchange ?? ((m,c) => receiver.receiveBatch(m,c));
    const batch = createOrderedChangesetBatchTransport(port, { receiverId: id, streamId: "source:incarnation", deliver: async (m,c) => {
      f.deliveries++; assert.equal(source.depth, 0); return exchange(m,c);
    } });
    const pump = new ChangesetDeliveryPump(port, { receiverId: id, deliver: single, deliverBatch: batch,
      confirmSource: async () => { assert.equal(source.depth, 0); f.confirmed++; await f.confirmHook?.(); }, ...options.pump });
    return { pump, receiver, destination, order, settings, port, applications: () => applications };
  };
  return f;
}
const pendingCount = async f => (await f.outbox.pending()).length;
const retained = f => rows(f.source.db, `SELECT acknowledged,length(payload) FROM ${T} ORDER BY seq`);
const tick = () => new Promise(r => setImmediate(r));

for (const multicast of [false, true]) test(`atomic pump: real ${multicast ? "fanout" : "outbox"} commits one source window and confirms once`, async t => {
  const f = await fixture(t, multicast), east = await f.make();
  const result = await east.pump.runBatch({ atomicAcknowledgements: true });
  assert.equal(result.deliveries, 3); assert.equal(result.applied, 3);
  assert.equal(new Set(f.acknowledgements).size, 1); assert.equal(f.confirmed, 3);
  assert.deepEqual(rows(east.destination.db, "SELECT * FROM items ORDER BY id"), rows(f.source.db, "SELECT * FROM items ORDER BY id"));
  if (multicast) {
    assert.ok(retained(f).every(r => r[0] === 0n && r[1] > 0n));
    const west = await f.make("west"); f.acknowledgements.length = 0;
    await west.pump.runBatch({ atomicAcknowledgements: true });
    assert.equal(new Set(f.acknowledgements).size, 1);
    assert.equal((await f.fanout.progress()).acknowledgedThrough, 3n);
  }
  assert.ok(retained(f).every(r => r[0] === 1n && r[1] === 0n));
});

test("atomic pump: default mode preserves per-message ACKs and confirmations", async t => {
  const f = await fixture(t), east = await f.make();
  await east.pump.runBatch();
  assert.equal(new Set(f.acknowledgements).size, 3); assert.equal(f.confirmed, 5);
});

test("atomic pump: explicit atomic mode refuses missing capability before source I/O", async t => {
  const f = await fixture(t); let touches = 0;
  const port = { pending: () => { touches++; return f.outbox.pending(); }, read: id => f.outbox.read(id),
    acknowledge: (id, hash) => f.outbox.acknowledge(id, hash) };
  const east = await f.make("east", { port });
  await assert.rejects(east.pump.runBatch({ atomicAcknowledgements: true }), e => e.phase === "admission");
  assert.equal(touches, 0); assert.equal(f.confirmed, 0); assert.equal(f.deliveries, 0);
  await assert.rejects(east.pump.runBatch({ atomicAcknowledgements: "yes" }));
  assert.equal(touches, 0);
  await east.pump.runBatch(); assert.equal(await pendingCount(f), 0);
});

for (const multicast of [false,true]) test(`atomic pump: late source failure retains complete window after remote commit (${multicast})`, async t => {
  const f = await fixture(t, multicast), east = await f.make();
  const original = f.source.beforeSql;
  f.source.beforeSql = (sql, params) => {
    original(sql,params);
    if (sql.startsWith("UPDATE OR ABORT") && params.includes("source:3")) throw new Error("third source ACK failed");
  };
  await assert.rejects(east.pump.runBatch({ atomicAcknowledgements: true }), e => e.phase === "source-ack");
  assert.equal(east.applications(), 3); assert.ok(retained(f).every(r => r[0] === 0n && r[1] > 0n));
  if (multicast) assert.equal((await f.fanout.progress()).replicas[0].sequence, 0n);
  f.source.beforeSql = original;
  const replay = await east.pump.runBatch({ atomicAcknowledgements: true });
  assert.equal(replay.replays, 3); assert.equal(east.applications(), 3);
});

test("atomic pump: invalid final remote receipt cannot enter source acknowledgement", async t => {
  const f = await fixture(t), east = await f.make();
  const port = f.outbox; let ackCalls = 0;
  const source = { pending: o=>port.pending(o), read: id=>port.read(id), acknowledge: (...a)=>port.acknowledge(...a),
    acknowledgeBatch: a => { ackCalls++; return port.acknowledgeBatch(a); } };
  const pump = new ChangesetDeliveryPump(source, { receiverId:"east", deliver:()=>assert.fail("single"), confirmSource:async()=>{},
    deliverBatch: async messages => {
      const input=messages.map((m,i)=>({...m,order:{protocol:"fsqlite-ordered-changeset-v1",streamId:"source:incarnation",sequence:String(i+1)}}));
      const receipts=await east.receiver.receiveBatch(input); return receipts.map((r,i)=>i===2?{...r,sha256:"0".repeat(64)}:r);
    } });
  await assert.rejects(pump.runBatch({atomicAcknowledgements:true}));
  assert.equal(ackCalls,0); assert.equal(await pendingCount(f),3); assert.equal(east.applications(),3);
});

test("atomic pump: omissions require whole-window consent before any source ACK", async t => {
  const f=await fixture(t), east=await f.make();
  east.destination.db.exec("INSERT INTO items VALUES(2,'local')"); east.settings.onConflict=()=>"omit";
  await assert.rejects(east.pump.runBatch({atomicAcknowledgements:true}));
  assert.equal(await pendingCount(f),3); assert.equal(f.acknowledgements.length,0);
  const accepted = new ChangesetDeliveryPump(f.outbox,{receiverId:"east",deliver:()=>assert.fail("single"),
    confirmSource:async()=>{},allowOmissions:true,
    deliverBatch:createOrderedChangesetBatchTransport(f.outbox,{receiverId:"east",streamId:"source:incarnation",deliver:(m,c)=>east.receiver.receiveBatch(m,c)})});
  const result=await accepted.runBatch({atomicAcknowledgements:true});
  assert.equal(result.omitted,1);assert.equal(result.replays,3);assert.equal(await pendingCount(f),0);
});

test("atomic pump: cancellation during admitted source SQL joins commit and confirmation", async t => {
  const f=await fixture(t), east=await f.make(), entered=pause(),release=pause(),controller=new AbortController();
  const original=f.source.beforeSql;
  f.source.beforeSql=async(sql,params)=>{original(sql,params);if(sql.includes("SET acknowledged=1")&&params[0]==="source:2"){entered.resolve();await release.promise;}};
  const operation=east.pump.runBatch({atomicAcknowledgements:true,signal:controller.signal});let settled=false;
  void operation.then(()=>{settled=true;},()=>{settled=true;});
  try{
    await Promise.race([entered.promise,operation.then(()=>assert.fail("not held"))]);controller.abort();await tick();assert.equal(settled,false);
    await assert.rejects(east.pump.run(),e=>e.code==="ERR_FSQLITE_DELIVERY_BUSY");
    release.resolve();await assert.rejects(operation);assert.equal(f.confirmed,3);assert.equal(await pendingCount(f),0);
  }finally{release.resolve();await Promise.allSettled([operation]);}
});

test("atomic pump: lost source commit response reconciles all identities on next run", async t => {
  const f=await fixture(t),east=await f.make();
  f.source.afterCommit=()=>{if(f.acknowledgements.length)throw new Error("lost source commit response");};
  await assert.rejects(east.pump.runBatch({atomicAcknowledgements:true}),e=>e.phase==="source-ack");
  f.source.afterCommit=null;
  assert.equal(await pendingCount(f),0);assert.equal(f.confirmed,2);
  const result=await east.pump.runBatch({atomicAcknowledgements:true});
  assert.equal(result.stopped,"empty");assert.equal(f.confirmed,3);assert.equal(east.applications(),3);
});

test("atomic pump: failed post-ACK confirmation must be retried even when no source work remains", async t => {
  const f=await fixture(t),east=await f.make();
  f.confirmHook=()=>{if(f.confirmed===3)throw new Error("source storage confirmation failed");};
  await assert.rejects(east.pump.runBatch({atomicAcknowledgements:true}),e=>e.phase==="source-confirm");
  assert.equal(await pendingCount(f),0);f.confirmHook=null;
  assert.equal((await east.pump.runBatch({atomicAcknowledgements:true})).stopped,"empty");
  assert.equal(f.confirmed,4);assert.equal(east.applications(),3);
});

test("atomic pump: capture optional ACK method once, preserving private state", async t => {
  const f=await fixture(t);
  class Port {#box;constructor(box){this.#box=box;}pending(o){return this.#box.pending(o);}read(id){return this.#box.read(id);}
    acknowledge(id,hash){return this.#box.acknowledge(id,hash);}acknowledgeBatch(a){return this.#box.acknowledgeBatch(a);}}
  const port=new Port(f.outbox),east=await f.make("east",{port});
  port.acknowledgeBatch=()=>assert.fail("replaced ACK method");
  assert.equal((await east.pump.runBatch({atomicAcknowledgements:true})).deliveries,3);
});

test("atomic pump: confirm a fulfilled ACK before rejecting malformed result without reading getters", async t => {
  const f=await fixture(t);let reads=0;
  const source={pending:o=>f.outbox.pending(o),read:id=>f.outbox.read(id),acknowledge:(...a)=>f.outbox.acknowledge(...a),
    acknowledgeBatch:async a=>{await f.outbox.acknowledgeBatch(a);const result=[true,true,true];Object.defineProperty(result,'2',{get(){reads++;return true;}});return result;}};
  const east=await f.make("east",{port:source});
  await assert.rejects(east.pump.runBatch({atomicAcknowledgements:true}),e=>e.phase==="source-ack");
  assert.equal(reads,0);assert.equal(f.confirmed,3);assert.equal(await pendingCount(f),0);
});

test("atomic pump: production outbox crosses real batch HTTP and atomically reclaims source bytes", async t => {
  const f=await fixture(t),east=await f.make();
  const handler=createOrderedChangesetBatchHttpHandler(east.receiver,{authorize:()=>true});
  const failures=[];
  const server=createServer((req,res)=>{(async()=>{
    const request=new Request(`http://127.0.0.1:${server.address().port}/replicate`,{method:req.method,headers:req.headers,body:Readable.toWeb(req),duplex:"half"});
    const response=await handler(request);res.writeHead(response.status,Object.fromEntries(response.headers));res.end(Buffer.from(await response.arrayBuffer()));
  })().catch(e=>{failures.push(e);res.destroy();});});
  server.listen(0,"127.0.0.1");await once(server,"listening");
  t.after(async()=>{server.closeAllConnections();await new Promise(r=>server.close(r));assert.deepEqual(failures,[]);});
  const http=createOrderedChangesetBatchHttpTransport(`http://127.0.0.1:${server.address().port}/replicate`,{allowInsecureLoopback:true});
  const pump=new ChangesetDeliveryPump(f.outbox,{receiverId:"east",deliver:()=>assert.fail("single"),
    confirmSource:async()=>{assert.equal(f.source.depth,0);f.confirmed++;},
    deliverBatch:createOrderedChangesetBatchTransport(f.outbox,{receiverId:"east",streamId:"source:incarnation",deliver:http})});
  assert.equal((await pump.runBatch({atomicAcknowledgements:true})).deliveries,3);
  assert.equal(new Set(f.acknowledgements).size,1);assert.equal(f.confirmed,3);assert.equal(await pendingCount(f),0);
  assert.deepEqual(rows(east.destination.db,"SELECT * FROM items ORDER BY id"),rows(f.source.db,"SELECT * FROM items ORDER BY id"));
});

test("atomic pump: last fanout recipient failure restores earlier provisional payload reclamation", async t => {
  const f=await fixture(t,true),east=await f.make();
  await east.pump.runBatch({atomicAcknowledgements:true});
  const west=await f.make("west"),original=f.source.beforeSql,before=retained(f);
  f.source.beforeSql=(sql,params)=>{
    original(sql,params);
    if(sql.includes("SET acknowledged=1")&&params[0]==="source:3")throw new Error("last-recipient reclamation failed");
  };
  await assert.rejects(west.pump.runBatch({atomicAcknowledgements:true}),e=>e.phase==="source-ack");
  assert.deepEqual(retained(f),before);
  assert.deepEqual((await f.fanout.progress()).replicas.map(r=>r.sequence),[3n,0n]);
  assert.equal(west.applications(),3);
  f.source.beforeSql=original;
  assert.equal((await west.pump.runBatch({atomicAcknowledgements:true})).replays,3);
  assert.equal(west.applications(),3);assert.ok(retained(f).every(r=>r[0]===1n&&r[1]===0n));
});

test("atomic pump: cancelled post-ACK confirmation holds admission until settlement", async t=>{
  const f=await fixture(t),east=await f.make(),entered=pause(),release=pause(),controller=new AbortController();
  f.confirmHook=async()=>{if(f.confirmed===3){entered.resolve();await release.promise;}};
  const operation=east.pump.runBatch({atomicAcknowledgements:true,signal:controller.signal});let settled=false;
  void operation.then(()=>{settled=true;},()=>{settled=true;});
  try{
    await Promise.race([entered.promise,operation.then(()=>assert.fail("confirmation not held"))]);
    controller.abort();await tick();assert.equal(settled,false);assert.equal(f.source.depth,0);
    await assert.rejects(east.pump.runBatch(),e=>e.code==="ERR_FSQLITE_DELIVERY_BUSY");
    release.resolve();await assert.rejects(operation);assert.equal(await pendingCount(f),0);
    f.confirmHook=null;assert.equal((await east.pump.runBatch({atomicAcknowledgements:true})).stopped,"empty");
    assert.equal(f.confirmed,4);
  }finally{release.resolve();await Promise.allSettled([operation]);}
});
