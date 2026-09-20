import assert from 'node:assert/strict';
import { test } from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { streamSnapshotChangesets, snapshotChangeset, captureChangeset } from '../src/changeset-capture.ts';
import { decodeChangeset, invertChangeset } from '../src/changeset-codec.ts';
import { ChangesetOutbox, CHANGESET_OUTBOX_TABLE } from '../src/changeset-outbox.ts';
import { applyChangeset } from '../src/changeset-apply.ts';

// Actual SQL and native changeset application, not a simulated SQL interpreter.
class Target {
  constructor(schema = 'CREATE TABLE t(id INTEGER PRIMARY KEY, value);', path = ':memory:') {
    this.db = new DatabaseSync(path); this.db.exec('PRAGMA foreign_keys=ON;'+schema);
    this.depth = 0; this.serial = 0; this.pages = []; this.queries = [];
  }
  async execute(sql, params = []) { return Number(this.db.prepare(sql).run(...params).changes); }
  async query(sql, params = []) {
    this.queries.push(sql);
    const stmt = this.db.prepare(sql); stmt.setReadBigInts(true); stmt.setReturnArrays(true);
    const rowArrays = stmt.all(...params);
    if (sql.startsWith('SELECT typeof(')) {
      this.pages.push(rowArrays.length);
      assert.ok(rowArrays.length <= 32);
      assert.ok(!/OFFSET/.test(sql));
      const plan = this.db.prepare('EXPLAIN QUERY PLAN '+sql).all(...params);
      assert.ok(plan.every(row => !row.detail.includes('USE TEMP B-TREE')), JSON.stringify(plan));
    }
    return { rowArrays };
  }
  async transaction(work) {
    const nested = this.depth++ > 0, id = `scope_${++this.serial}`;
    this.db.exec(nested ? `SAVEPOINT ${id}` : 'BEGIN');
    try { const result = await work(this); this.db.exec(nested ? `RELEASE ${id}` : 'COMMIT'); return result; }
    catch (error) { this.db.exec(nested ? `ROLLBACK TO ${id}; RELEASE ${id}` : 'ROLLBACK'); throw error; }
    finally { this.depth--; }
  }
  rows(table = 't') {
    const count=this.db.prepare(`PRAGMA table_info('${table.replaceAll("'","''")}')`).all().length;
    const stmt = this.db.prepare(`SELECT * FROM "${table.replaceAll('"','""')}" ORDER BY ${Array.from({length:count},(_,i)=>i+1).join(',')}`);
    stmt.setReadBigInts(true); stmt.setReturnArrays(true); return stmt.all();
  }
  close() { this.db.close(); }
}
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
function populate(t, n = 100) {
  const insert = t.db.prepare('INSERT INTO t VALUES(?,?)');
  for (let i=0;i<n;i++) insert.run(i, `row-${i}`);
}
async function transfer(source, destination, options = {}) {
  const list=[];
  const result = await streamSnapshotChangesets(source, chunk => {
    assert.equal(chunk.index,list.length);
    assert.ok(Object.isFrozen(chunk));
    assert.ok(chunk.changeset.byteLength <= (options.chunkBytes ?? 1024*1024));
    assert.ok(chunk.changes <= (options.chunkRows ?? 1024));
    const tables=decodeChangeset(chunk.changeset);
    assert.ok(tables.length <= 1);
    assert.ok(tables.every(t=>t.changes.every(c=>c.operation==='insert')));
    assert.equal(destination.db.applyChangeset(chunk.changeset),true);
    list.push(chunk);
  }, {tables:['t'],...options});
  assert.equal(result.changes,list.reduce((n,c)=>n+c.changes,0));
  assert.equal(result.byteLength,list.reduce((n,c)=>n+c.changeset.byteLength,0));
  assert.equal(result.chunks,list.length);
  return {result,list};
}

for (const chunkRows of [1,3,31,32,33,100,101,1024]) test(`native apply across chunk row boundary ${chunkRows}`,async()=>{
  const a=new Target(),b=new Target();
  try {
    populate(a); const {list,result}=await transfer(a,b,{chunkRows});
    assert.equal(result.changes,100);assert.deepEqual(b.rows(),a.rows());
    assert.equal(list.length,Math.ceil(100/chunkRows));
    for(const chunk of list.toReversed()) assert.equal(b.db.applyChangeset(invertChangeset(chunk.changeset)),true);
    assert.deepEqual(b.rows(),[]);
  } finally {a.close();b.close();}
});
test('empty source emits exactly one empty seed with complete summary',async()=>{
  const a=new Target(),b=new Target();
  try{assert.deepEqual((await transfer(a,b)).result,{chunks:1,changes:0,byteLength:0});}finally{a.close();b.close();}
});
test('single-image snapshot and capture retain their behavior after shared reader refactor',async()=>{
  const a=new Target(),b=new Target();
  try{
    populate(a,100); const snap=await snapshotChangeset(a,{tables:['t']});
    assert.equal(snap.changes,100);assert.equal(b.db.applyChangeset(snap.changeset),true);
    a.db.exec('PRAGMA recursive_triggers=ON');
    const c=await captureChangeset(a,async tx=>{await tx.execute("UPDATE t SET value='new' WHERE id=1");await tx.execute('DELETE FROM t WHERE id=2');},{tables:['t']});
    assert.equal(b.db.applyChangeset(c.changeset),true);assert.deepEqual(b.rows(),a.rows());
  }finally{a.close();b.close();}
});
for (const suffix of ['', ' WITHOUT ROWID']) test(`mixed collations/directions and binary composite keys${suffix}`,async()=>{
  const ddl=`CREATE TABLE t(a TEXT COLLATE NOCASE,b INTEGER,c BLOB,value,PRIMARY KEY(b DESC,a ASC,c DESC))${suffix};`;
  const a=new Target(ddl),b=new Target(ddl);
  try{
    const stmt=a.db.prepare('INSERT INTO t VALUES(?,?,?,?)');
    for(let i=0;i<201;i++) stmt.run(`K${i%13}`,i%7,new Uint8Array([i]),i%2 ? 1.5 : 'data');
    await transfer(a,b,{chunkRows:7,chunkBytes:2048});assert.deepEqual(b.rows(),a.rows());
  }finally{a.close();b.close();}
});
for(const encoding of ['UTF-8','UTF-16le','UTF-16be']) test(`lossless text, int64 and real images in ${encoding}`,async()=>{
  const a=new Target(`PRAGMA encoding='${encoding}'; CREATE TABLE t(id INTEGER PRIMARY KEY,value);`),b=new Target();
  try{
    const stmt=a.db.prepare('INSERT INTO t VALUES(?,?)');
    const values=['a\0b','\uFEFFtext','🐘日本語',null,new Uint8Array([0,255]),1.5,1n,Infinity];
    values.forEach((v,i)=>stmt.run((1n<<62n)+BigInt(i),v));
    await transfer(a,b,{chunkRows:2});assert.deepEqual(b.rows(),a.rows());
  }finally{a.close();b.close();}
});
test('bounds actual SQL image pages as well as encoded chunks',async()=>{
  const a=new Target(),b=new Target();
  try{
    a.db.exec("WITH RECURSIVE n(x) AS(VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<35) INSERT INTO t SELECT x,zeroblob(1500) FROM n");
    await transfer(a,b,{chunkBytes:2048});assert.ok(a.pages.every(n=>n===1));assert.deepEqual(b.rows(),a.rows());
  }finally{a.close();b.close();}
});
test('single oversized row fails before transferring its value',async()=>{
  const a=new Target();try{
    a.db.exec('INSERT INTO t VALUES(1,zeroblob(1000000))');
    await assert.rejects(streamSnapshotChangesets(a,()=>assert.fail('no chunk'),{tables:['t'],chunkBytes:1024}),{code:'ERR_FSQLITE_CAPTURE_LIMIT'});
    assert.deepEqual(a.pages,[]);
  }finally{a.close();}
});
for(const [option,value] of [['maxRows',40],['maxCells',80],['maxBytes',6000],['maxChunks',2]]) test(`total ${option} failure cannot return a complete seed`,async()=>{
  const a=new Target();try{
    populate(a);let seen=0;
    await assert.rejects(streamSnapshotChangesets(a,()=>{seen++;},{tables:['t'],chunkRows:3,[option]:value}),{code:'ERR_FSQLITE_CAPTURE_LIMIT'});
    assert.ok(seen>0);assert.equal(a.depth,0);assert.equal(a.rows().length,100);
  }finally{a.close();}
});
for(const [option,value] of [['chunkRows',0],['chunkRows',100001],['chunkBytes',0],['chunkBytes',67108865],['maxChunks',100001],['maxRows',10000001],['maxBytes',1073741825],['maxCells',100000001]]) test(`invalid ${option}=${value} rejects before SQL`,async()=>{
  const a=new Target();try{
    await assert.rejects(streamSnapshotChangesets(a,()=>{}, {tables:['t'],[option]:value}),{code:'ERR_FSQLITE_CAPTURE_INPUT'});
    assert.equal(a.serial,0);
  }finally{a.close();}
});
test('every table and NULL key is preflighted before any sink call',async()=>{
  const a=new Target('CREATE TABLE t(id INTEGER PRIMARY KEY,value); CREATE TABLE bad(id TEXT PRIMARY KEY,value);');
  try{populate(a);a.db.exec("INSERT INTO bad VALUES(NULL,'lost')");await assert.rejects(streamSnapshotChangesets(a,()=>assert.fail('no chunk'),{tables:['t','bad']}),{code:'ERR_FSQLITE_CAPTURE_SCHEMA'});}finally{a.close();}
});
test('table ordering and immediate foreign keys survive chunk boundaries',async()=>{
  const ddl='CREATE TABLE p(id INTEGER PRIMARY KEY,value); CREATE TABLE t(id INTEGER PRIMARY KEY,value REFERENCES p);';
  const a=new Target(ddl),b=new Target(ddl);
  try{
    a.db.exec('INSERT INTO p VALUES(1,2),(3,4); INSERT INTO t VALUES(1,1),(2,3);');
    await transfer(a,b,{tables:['p','t'],chunkRows:1});assert.deepEqual(b.rows('p'),a.rows('p'));assert.deepEqual(b.rows(),a.rows());
  }finally{a.close();b.close();}
});
test('chunk sinks apply backpressure and cancellation drains the active sink',async()=>{
  const a=new Target();try{
    populate(a);const controller=new AbortController();let release,entered,settled=false,calls=0;
    const ready=new Promise(r=>entered=r),gate=new Promise(r=>release=r);
    const run=streamSnapshotChangesets(a,async()=>{calls++;entered();await gate;},{tables:['t'],chunkRows:1,signal:controller.signal}).finally(()=>{settled=true;});
    await ready;const queries=a.queries.length;controller.abort('stop');await delay(5);
    assert.equal(settled,false);assert.equal(calls,1);assert.equal(a.queries.length,queries);
    release();await assert.rejects(run,{code:'ERR_FSQLITE_CAPTURE_CANCELLED'});assert.equal(a.depth,0);
  }finally{a.close();}
});
test('sink failure preserves original error and rolls back owned transaction',async()=>{
  const a=new Target();try{
    populate(a);const expected=new Error('sink failed');
    await assert.rejects(streamSnapshotChangesets(a,()=>{throw expected;},{tables:['t'],chunkRows:1}),e=>e===expected);
    assert.equal(a.depth,0);assert.equal(a.rows().length,100);
  }finally{a.close();}
});
test('monotonic timeout checked after sink even when timer callbacks cannot run',async()=>{
  const a=new Target();try{
    populate(a,1);await assert.rejects(streamSnapshotChangesets(a,()=>{const end=performance.now()+20;while(performance.now()<end){};},{tables:['t'],timeoutMs:10}),{code:'ERR_FSQLITE_CAPTURE_TIMEOUT'});
  }finally{a.close();}
});
test('final sink cannot hide a schema change from snapshot validation',async()=>{
  const a=new Target();try{
    populate(a,1);await assert.rejects(streamSnapshotChangesets(a,()=>{a.db.exec('CREATE TABLE illegal(x)');},{tables:['t']}),{code:'ERR_FSQLITE_CAPTURE_SCHEMA'});
    assert.equal(a.db.prepare("SELECT count(*) n FROM sqlite_schema WHERE name='illegal'").get().n,0);
  }finally{a.close();}
});
test('mutating/transferring emitted bytes cannot change later keys or summary',async()=>{
  const a=new Target();try{
    populate(a,9);const images=[];let bytes=0;
    const result=await streamSnapshotChangesets(a,c=>{images.push(...decodeChangeset(c.changeset)[0].changes.map(x=>x.new[0]));bytes+=c.changeset.byteLength;structuredClone(c.changeset,{transfer:[c.changeset.buffer]});},{tables:['t'],chunkRows:1});
    assert.deepEqual(images,Array.from({length:9},(_,i)=>BigInt(i)));assert.equal(result.byteLength,bytes);
  }finally{a.close();}
});
test('one snapshot survives another connection committing across chunks and tables',async()=>{
  const file=join(mkdtempSync(join(tmpdir(),'fsqlite-stream-')),'source.db');
  const a=new Target('PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY,value); CREATE TABLE later(id INTEGER PRIMARY KEY,value);',file),b=new Target('',file);
  try{
    populate(a,99);a.db.exec("INSERT INTO later VALUES(1,'before')");const chunks=[];
    await streamSnapshotChangesets(a,c=>{chunks.push(c);if(c.index===0)b.db.exec("UPDATE t SET value='after'; UPDATE later SET value='after'; INSERT INTO t VALUES(1000,'new')");},{tables:['t','later'],chunkRows:7});
    const records=chunks.flatMap(c=>decodeChangeset(c.changeset).flatMap(t=>t.changes.map(x=>[t.name,...x.new])));
    assert.equal(records.length,100);assert.equal(records.at(-1)[2],'before');assert.ok(records.filter(x=>x[0]==='t').every(x=>x[2].startsWith('row-')));
  }finally{a.close();b.close();}
});
test('streams more rows than a single changeset allows without accumulating all images',async()=>{
  const a=new Target();try{
    a.db.exec("WITH RECURSIVE n(x) AS(VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<100001) INSERT INTO t SELECT x,x FROM n");
    let rows=0,last=0n;
    const result=await streamSnapshotChangesets(a,c=>{const values=decodeChangeset(c.changeset)[0].changes;assert.ok(values.length<=1000);for(const row of values){assert.equal(row.new[0],last+1n);last=row.new[0];}rows+=c.changes;},{tables:['t'],chunkRows:1000});
    assert.equal(rows,100001);assert.equal(result.chunks,101);assert.equal(last,100001n);
  }finally{a.close();}
});
test('streams a dataset larger than 64 MiB with bounded 1 MiB value pages',async()=>{
  const a=new Target();try{
    a.db.exec("WITH RECURSIVE n(x) AS(VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<70) INSERT INTO t SELECT x,zeroblob(1000000) FROM n");
    let total=0,rows=0;
    const result=await streamSnapshotChangesets(a,c=>{const t=decodeChangeset(c.changeset)[0];assert.equal(t.changes.length,1);assert.equal(t.changes[0].new[1].byteLength,1000000);total+=c.changeset.byteLength;rows++;},{tables:['t'],chunkBytes:1048576});
    assert.equal(rows,70);assert.ok(total>64*1024*1024);assert.equal(result.byteLength,total);assert.ok(a.pages.every(n=>n===1));
  }finally{a.close();}
});

const seedOptions={deliveryId:'source:seed',tables:['t'],chunkRows:3};
async function deliverPending(outbox, destination, loseFirst=false) {
  let first=loseFirst,replays=0;
  for(;;){
    const page=await outbox.pending({limit:1});if(!page.length)break;
    const message=await outbox.read(page[0].deliveryId);
    const result=await applyChangeset(destination,message.changeset,{tables:['t'],deliveryId:message.delivery.deliveryId});
    if(first){first=false;continue;} // Receiver committed, but no source ACK arrived.
    replays+=Number(result.replayed);
    await outbox.acknowledge(message.delivery.deliveryId,message.delivery.sha256);
  }
  return replays;
}
function noOutbox(t) {
  const exists=t.db.prepare('SELECT name FROM sqlite_schema WHERE name=?').get(CHANGESET_OUTBOX_TABLE);
  if(exists)assert.equal(t.db.prepare(`SELECT count(*) n FROM ${CHANGESET_OUTBOX_TABLE}`).get().n,0);
}
function pristineOutbox(t) {
  const template=new Target();
  // Obtain the actual implementation's schema without publishing it to this file.
  return new ChangesetOutbox(template).bootstrap({deliveryId:'template',tables:['t']}).then(()=>{
    const ddl=template.db.prepare('SELECT sql FROM sqlite_schema WHERE name=?').get(CHANGESET_OUTBOX_TABLE).sql;
    t.db.exec(ddl);template.close();
  });
}

test('chunked outbox stores one atomic seed range, then incremental changes follow',async()=>{
  const a=new Target(),b=new Target();try{
    populate(a,10);const outbox=new ChangesetOutbox(a);
    const seed=await outbox.bootstrapChunks(seedOptions);
    assert.equal(seed.replayed,false);assert.equal(seed.chunks,4);assert.equal(seed.changes,10);
    assert.equal(seed.firstSequence,1n);assert.equal(seed.lastSequence,4n);assert.equal(seed.complete,false);
    const ids=(await outbox.pending()).map(x=>x.deliveryId);
    assert.deepEqual(ids,['source:seed','source:seed/chunk/1','source:seed/chunk/2','source:seed/chunk/3']);
    a.db.exec('PRAGMA recursive_triggers=ON');
    const delta=await outbox.record(tx=>tx.execute("UPDATE t SET value='changed' WHERE id=0"),{deliveryId:'source:delta',tables:['t']});
    assert.equal(delta.delivery.sequence,5n);
    assert.equal(await deliverPending(outbox,b,true),1);assert.deepEqual(b.rows(),a.rows());
    const replay=await outbox.bootstrapChunks({...seedOptions,chunkRows:1,maxRows:1});
    assert.equal(replay.replayed,true);assert.equal(replay.chunks,4);assert.equal(replay.acknowledgedChunks,4);assert.equal(replay.complete,true);
    assert.equal(replay.sha256,seed.sha256);
  }finally{a.close();b.close();}
});
test('replay reads retained chunks, never resnapshots newer source rows',async()=>{
  const a=new Target(),b=new Target();try{
    populate(a,10);const outbox=new ChangesetOutbox(a);const seed=await outbox.bootstrapChunks(seedOptions);
    a.db.exec("UPDATE t SET value='later'; INSERT INTO t VALUES(100,'later')");a.queries.length=0;
    const again=await outbox.bootstrapChunks({...seedOptions,chunkBytes:1});
    assert.equal(again.changes,10);assert.equal(again.byteLength,seed.byteLength);
    assert.ok(a.queries.every(sql=>!sql.includes('FROM main."t"')));
    await deliverPending(outbox,b);assert.equal(b.rows().length,10);assert.equal(b.rows()[0][1],'row-0');
  }finally{a.close();b.close();}
});
test('empty chunked bootstrap still records a replayable first operation',async()=>{
  const a=new Target(),b=new Target();try{
    const outbox=new ChangesetOutbox(a);const seed=await outbox.bootstrapChunks(seedOptions);
    assert.equal(seed.chunks,1);assert.equal(seed.changes,0);assert.equal(seed.byteLength,0);
    await deliverPending(outbox,b);assert.equal((await outbox.bootstrapChunks(seedOptions)).complete,true);
  }finally{a.close();b.close();}
});
test('partial receiver work remains explicitly incomplete and later chunks remain pending',async()=>{
  const a=new Target(),b=new Target();try{
    populate(a,10);const outbox=new ChangesetOutbox(a);await outbox.bootstrapChunks(seedOptions);
    const first=await outbox.read(seedOptions.deliveryId);
    await applyChangeset(b,first.changeset,{tables:['t'],deliveryId:first.delivery.deliveryId});
    await outbox.acknowledge(first.delivery.deliveryId,first.delivery.sha256);
    const status=await outbox.bootstrapChunks(seedOptions);
    assert.equal(b.rows().length,3);assert.equal(status.acknowledgedChunks,1);assert.equal(status.complete,false);
    assert.equal((await outbox.pending()).length,3);
    await deliverPending(outbox,b);assert.deepEqual(b.rows(),a.rows());
  }finally{a.close();b.close();}
});
for(const options of [{maxEntries:2},{maxPayloadBytes:100}]) test(`outbox capacity failure rolls back every stored chunk ${JSON.stringify(options)}`,async()=>{
  const a=new Target();try{
    populate(a,10);const outbox=new ChangesetOutbox(a,options);
    await assert.rejects(outbox.bootstrapChunks(seedOptions),{code:'ERR_FSQLITE_OUTBOX_FULL'});noOutbox(a);
    assert.equal((await new ChangesetOutbox(a).bootstrapChunks(seedOptions)).replayed,false);
  }finally{a.close();}
});
test('oversized later source row cannot leave a committed bootstrap prefix',async()=>{
  const a=new Target();try{
    populate(a,70);a.db.exec('UPDATE t SET value=zeroblob(5000) WHERE id=69');
    const outbox=new ChangesetOutbox(a);
    await assert.rejects(outbox.bootstrapChunks({...seedOptions,chunkBytes:1024}),{code:'ERR_FSQLITE_CAPTURE_LIMIT'});noOutbox(a);
  }finally{a.close();}
});
test('cancellation after stored chunks drains and rolls back the complete source group',async()=>{
  const a=new Target();try{
    populate(a,10);const controller=new AbortController(),execute=a.execute.bind(a);let inserted=0;
    a.execute=async(sql,params)=>{const n=await execute(sql,params);if(sql.startsWith(`INSERT OR ABORT INTO main."${CHANGESET_OUTBOX_TABLE}"`)&&++inserted===2)controller.abort('stop');return n;};
    await assert.rejects(new ChangesetOutbox(a).bootstrapChunks({...seedOptions,signal:controller.signal}),{code:'ERR_FSQLITE_CAPTURE_CANCELLED'});
    assert.equal(inserted,2);noOutbox(a);assert.equal(a.depth,0);
  }finally{a.close();}
});
test('nested bootstrap remains provisional and enclosing rollback removes all chunks',async()=>{
  const a=new Target();try{
    populate(a,10);const expected=new Error('outer rollback');
    await assert.rejects(a.transaction(async tx=>{const result=await new ChangesetOutbox(tx).bootstrapChunks(seedOptions);assert.equal(result.chunks,4);throw expected;}),e=>e===expected);
    noOutbox(a);
  }finally{a.close();}
});
test('deferred commit failure removes payloads and the completion manifest',async()=>{
  const a=new Target('CREATE TABLE t(id INTEGER PRIMARY KEY,value);CREATE TABLE child(id REFERENCES t(id) DEFERRABLE INITIALLY DEFERRED);');
  try{
    populate(a,10);const transaction=a.transaction.bind(a);
    a.transaction=work=>transaction(async tx=>{const result=await work(tx);await tx.execute('INSERT INTO child VALUES(999)');return result;});
    await assert.rejects(new ChangesetOutbox(a).bootstrapChunks(seedOptions),/FOREIGN KEY/);noOutbox(a);
  }finally{a.close();}
});
test('lost source commit response recovers the complete retained manifest and chunks',async()=>{
  const a=new Target(),b=new Target();try{
    populate(a,10);const transaction=a.transaction.bind(a);let lose=true;
    a.transaction=async work=>{const result=await transaction(work);if(lose){lose=false;throw new Error('lost commit response');}return result;};
    const outbox=new ChangesetOutbox(a);await assert.rejects(outbox.bootstrapChunks(seedOptions),/lost commit response/);
    a.db.exec("UPDATE t SET value='newer'");
    assert.equal((await outbox.bootstrapChunks(seedOptions)).replayed,true);
    await deliverPending(outbox,b);assert.equal(b.rows()[0][1],'row-0');
  }finally{a.close();b.close();}
});
test('bootstrap ACKs must advance in order and partial group deletion is refused',async()=>{
  const a=new Target(),b=new Target();try{
    populate(a,10);const outbox=new ChangesetOutbox(a);const seed=await outbox.bootstrapChunks(seedOptions);
    const second=await outbox.read('source:seed/chunk/1');
    await assert.rejects(outbox.acknowledge(second.delivery.deliveryId,second.delivery.sha256),{code:'ERR_FSQLITE_OUTBOX_ACK'});
    await assert.rejects(outbox.forgetBootstrapChunks(seed.deliveryId,seed.sha256),{code:'ERR_FSQLITE_OUTBOX_STATE'});
    const first=await outbox.read(seed.deliveryId);await applyChangeset(b,first.changeset,{tables:['t'],deliveryId:seed.deliveryId});
    await outbox.acknowledge(seed.deliveryId,seed.sha256);
    await assert.rejects(outbox.forgetAcknowledged(seed.deliveryId,seed.sha256),{code:'ERR_FSQLITE_OUTBOX_STATE'});
    await deliverPending(outbox,b);
    await assert.rejects(outbox.forgetAcknowledged(second.delivery.deliveryId,second.delivery.sha256),{code:'ERR_FSQLITE_OUTBOX_STATE'});
  }finally{a.close();b.close();}
});
test('explicit whole-group cleanup preserves later deltas and cannot authorize reseeding',async()=>{
  const a=new Target(),b=new Target();try{
    populate(a,10);const outbox=new ChangesetOutbox(a);const seed=await outbox.bootstrapChunks(seedOptions);
    await deliverPending(outbox,b);a.db.exec('PRAGMA recursive_triggers=ON');
    const delta=await outbox.record(tx=>tx.execute("UPDATE t SET value='delta' WHERE id=0"),{deliveryId:'delta',tables:['t']});
    await assert.rejects(outbox.forgetBootstrapChunks(seed.deliveryId,'0'.repeat(64)),{code:'ERR_FSQLITE_OUTBOX_STATE'});
    assert.equal(await outbox.forgetBootstrapChunks(seed.deliveryId,seed.sha256),true);
    assert.equal(await outbox.forgetBootstrapChunks(seed.deliveryId,seed.sha256),false);
    assert.deepEqual((await outbox.pending()).map(x=>x.deliveryId),['delta']);
    await deliverPending(outbox,b);await outbox.forgetAcknowledged('delta',delta.delivery.sha256);
    assert.deepEqual(await outbox.pending(),[]);
    await assert.rejects(outbox.bootstrapChunks(seedOptions),{code:'ERR_FSQLITE_OUTBOX_STATE'});
  }finally{a.close();b.close();}
});
for(const method of ['bootstrap','record','child','scope','indirect'])test(`rejects cross-method/identity reuse: ${method}`,async()=>{
  const a=new Target();try{
    populate(a,10);const outbox=new ChangesetOutbox(a);await outbox.bootstrapChunks(seedOptions);
    if(method==='bootstrap')await assert.rejects(outbox.bootstrap(seedOptions),{code:'ERR_FSQLITE_OUTBOX_REUSE'});
    else if(method==='record')await assert.rejects(outbox.record(()=>assert.fail('no callback'),seedOptions),{code:'ERR_FSQLITE_OUTBOX_REUSE'});
    else if(method==='child')await assert.rejects(outbox.bootstrapChunks({...seedOptions,deliveryId:'source:seed/chunk/1'}),{code:'ERR_FSQLITE_OUTBOX_REUSE'});
    else if(method==='scope')await assert.rejects(outbox.bootstrapChunks({...seedOptions,tables:['other']}),{code:'ERR_FSQLITE_OUTBOX_REUSE'});
    else await assert.rejects(outbox.bootstrapChunks({...seedOptions,indirect:true}),{code:'ERR_FSQLITE_OUTBOX_REUSE'});
  }finally{a.close();}
});
for(const mutation of ['missing','total','truncated','identity','payload','ack-order'])test(`rejects damaged bootstrap ${mutation} rather than generating replacement data`,async()=>{
  const a=new Target();try{
    populate(a,10);const outbox=new ChangesetOutbox(a);await outbox.bootstrapChunks(seedOptions);
    if(mutation==='missing')a.db.exec(`DELETE FROM ${CHANGESET_OUTBOX_TABLE} WHERE seq=2`);
    else if(mutation==='payload')a.db.exec(`UPDATE ${CHANGESET_OUTBOX_TABLE} SET payload=zeroblob(byte_length) WHERE seq=2`);
    else if(mutation==='ack-order')a.db.exec(`UPDATE ${CHANGESET_OUTBOX_TABLE} SET acknowledged=1,payload=X'' WHERE seq=2`);
    else {
      const seq=mutation==='identity'?2:1,row=a.db.prepare(`SELECT scope FROM ${CHANGESET_OUTBOX_TABLE} WHERE seq=?`).get(seq),scope=JSON.parse(row.scope);
      if(mutation==='identity')scope.stream.id='wrong';
      else if(mutation==='total')scope.stream.summary.changes++;
      else scope.stream.summary.chunks--;
      a.db.prepare(`UPDATE ${CHANGESET_OUTBOX_TABLE} SET scope=? WHERE seq=?`).run(JSON.stringify(scope),seq);
    }
    a.queries.length=0;await assert.rejects(outbox.bootstrapChunks(seedOptions),{code:'ERR_FSQLITE_OUTBOX_CORRUPT'});
    assert.ok(a.queries.every(sql=>!sql.includes('FROM main."t"')));
  }finally{a.close();}
});
test('two file-backed bootstraps overlap, and the stale reader cannot append a second seed',async()=>{
  const path=join(mkdtempSync(join(tmpdir(),'fsqlite-chunk-race-')),'source.db');
  const a=new Target('PRAGMA journal_mode=WAL;CREATE TABLE t(id INTEGER PRIMARY KEY,value);',path),b=new Target('',path);
  try{
    populate(a,10);await pristineOutbox(a);const query=a.query.bind(a);let raced=false,winner;
    a.query=async(sql,params)=>{const result=await query(sql,params);if(!raced&&sql.startsWith('SELECT typeof(')){raced=true;winner=await new ChangesetOutbox(b).bootstrapChunks({...seedOptions,deliveryId:'winner'});}return result;};
    await assert.rejects(new ChangesetOutbox(a).bootstrapChunks(seedOptions),/locked|busy/i);
    assert.equal(raced,true);assert.equal(winner.chunks,4);
    assert.deepEqual((await new ChangesetOutbox(b).pending()).map(x=>x.deliveryId),['winner','winner/chunk/1','winner/chunk/2','winner/chunk/3']);
  }finally{a.close();b.close();}
});
test('an incremental writer can win before the first seed write; stale bootstrap aborts',async()=>{
  const path=join(mkdtempSync(join(tmpdir(),'fsqlite-chunk-delta-')),'source.db');
  const a=new Target('PRAGMA journal_mode=WAL;CREATE TABLE t(id INTEGER PRIMARY KEY,value);',path),b=new Target('',path);
  try{
    populate(a,10);await pristineOutbox(a);b.db.exec('PRAGMA recursive_triggers=ON');const query=a.query.bind(a);let raced=false;
    a.query=async(sql,params)=>{const result=await query(sql,params);if(!raced&&sql.startsWith('SELECT typeof(')){raced=true;await new ChangesetOutbox(b).record(tx=>tx.execute("UPDATE t SET value='winner' WHERE id=0"),{deliveryId:'delta',tables:['t']});}return result;};
    await assert.rejects(new ChangesetOutbox(a).bootstrapChunks(seedOptions),/locked|busy/i);
    assert.deepEqual((await new ChangesetOutbox(b).pending()).map(x=>x.deliveryId),['delta']);
  }finally{a.close();b.close();}
});
for(const cut of ['mid-chunks','before-commit','after-commit'])test(`SIGKILL ${cut} preserves the atomic source bootstrap decision`,async()=>{
  const path=join(mkdtempSync(join(tmpdir(),'fsqlite-chunk-kill-')),'source.db');
  const original=new Target('PRAGMA journal_mode=WAL;CREATE TABLE t(id INTEGER PRIMARY KEY,value);',path);
  populate(original,10);original.close();
  const code=`import assert from 'node:assert/strict';import {DatabaseSync} from 'node:sqlite';import {ChangesetOutbox} from ${JSON.stringify(new URL('../src/changeset-outbox.ts',import.meta.url).href)};
    ${Target.toString()}
    const a=new Target('',${JSON.stringify(path)});let inserts=0;
    const execute=a.execute.bind(a);a.execute=async(sql,params)=>{const n=await execute(sql,params);if(${JSON.stringify(cut)}==='mid-chunks'&&sql.startsWith('INSERT OR ABORT INTO main."__fsqlite_changeset_outbox"')&&++inserts===2)process.kill(process.pid,'SIGKILL');return n;};
    const transaction=a.transaction.bind(a);a.transaction=async work=>{const result=await transaction(async tx=>{const value=await work(tx);if(${JSON.stringify(cut)}==='before-commit')process.kill(process.pid,'SIGKILL');return value;});if(${JSON.stringify(cut)}==='after-commit')process.kill(process.pid,'SIGKILL');return result;};
    await new ChangesetOutbox(a).bootstrapChunks({deliveryId:'source:seed',tables:['t'],chunkRows:3});throw Error('missed crash cut');`;
  const child=spawn(process.execPath,['--experimental-loader='+new URL('./helpers/source-loader.mjs',import.meta.url).pathname,'--input-type=module','-e',code],{env:process.env,stdio:['ignore','ignore','pipe']});
  let error='';child.stderr.on('data',d=>{if(error.length<8000)error+=d;});
  const [status,signal]=await once(child,'close');assert.equal(status,null,error);assert.equal(signal,'SIGKILL',error);
  const a=new Target('',path),b=new Target();try{
    a.db.exec("UPDATE t SET value='newer'");const outbox=new ChangesetOutbox(a);
    const result=await outbox.bootstrapChunks(seedOptions);assert.equal(result.replayed,cut==='after-commit');assert.equal(result.chunks,4);
    await deliverPending(outbox,b);assert.equal(b.rows()[0][1],cut==='after-commit'?'row-0':'newer');
  }finally{a.close();b.close();}
});
test('chunked outbox retains a larger-than-64-MiB seed without one giant payload',async()=>{
  const a=new Target();try{
    a.db.exec("WITH RECURSIVE n(x) AS(VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<68) INSERT INTO t SELECT x,zeroblob(1000000) FROM n");
    const outbox=new ChangesetOutbox(a,{maxPayloadBytes:80*1024*1024});
    const result=await outbox.bootstrapChunks({...seedOptions,chunkBytes:1048576});
    assert.equal(result.chunks,68);assert.equal(result.changes,68);assert.ok(result.byteLength>64*1024*1024);
    const bounds=a.db.prepare(`SELECT max(byte_length) maximum,sum(byte_length) total FROM ${CHANGESET_OUTBOX_TABLE}`).get();
    assert.ok(bounds.maximum<=1048576);assert.equal(bounds.total,result.byteLength);
    assert.equal((await outbox.bootstrapChunks({...seedOptions,chunkBytes:1})).replayed,true);
  }finally{a.close();}
});

test('ordinary pending delivery pagination uses numeric sequence order beyond nine',async()=>{
  const a=new Target();try{
    a.db.exec('PRAGMA recursive_triggers=ON');const outbox=new ChangesetOutbox(a);
    for(let i=1;i<=45;i++)await outbox.record(tx=>tx.execute('INSERT INTO t VALUES(?,?)',[BigInt(i),'row']),{deliveryId:`delta-${i}`,tables:['t']});
    assert.deepEqual((await outbox.pending({limit:100})).map(x=>x.sequence),Array.from({length:45},(_,i)=>BigInt(i+1)));
    assert.deepEqual((await outbox.pending({limit:15,after:9n})).map(x=>x.sequence),Array.from({length:15},(_,i)=>BigInt(i+10)));
    const p=await outbox.pending({limit:1});assert.equal(p[0].sequence,1n);
  }finally{a.close();}
});
for(const column of ['t0','x0'])for(const layout of ['',' WITHOUT ROWID'])test(`snapshot key ${column} cannot resolve to typed projection aliases${layout}`,async()=>{
  const ddl=`CREATE TABLE t(${column} TEXT PRIMARY KEY,value)${layout};`,a=new Target(ddl),b=new Target(ddl);
  try{
    const stmt=a.db.prepare('INSERT INTO t VALUES(?,?)');
    for(let i=120;i>0;i--)stmt.run(String(i).padStart(4,'0'),i);
    await transfer(a,b,{chunkRows:7});assert.deepEqual(b.rows(),a.rows());
    const single=await snapshotChangeset(a,{tables:['t']});assert.equal(single.changes,120);
  }finally{a.close();b.close();}
});
