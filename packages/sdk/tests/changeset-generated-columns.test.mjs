import assert from 'node:assert/strict';
import { test } from 'node:test';
import { SqliteTarget } from './helpers/production-sqlite-target.mjs';
import { captureChangeset, snapshotChangeset, streamSnapshotChangesets } from '../src/changeset-capture.ts';
import { captureSnapshotChangeset } from '../src/changeset-snapshot-capture.ts';
import { applyChangeset, applyPatchset } from '../src/changeset-apply.ts';
import { decodeChangeset, invertChangeset } from '../src/changeset-codec.ts';
import { ChangesetOutbox } from '../src/changeset-outbox.ts';
import { ChangesetBootstrapReceiver } from '../src/changeset-bootstrap.ts';
import { ChangesetBootstrapTransfer } from '../src/changeset-bootstrap-transfer.ts';

const schema = `CREATE TABLE t(
 derived TEXT AS (printf('%s/%d',body,amount)) VIRTUAL,
 id INTEGER PRIMARY KEY,
 double_amount INTEGER AS (amount*2) STORED,
 amount INTEGER NOT NULL CHECK(amount>=0),
 body TEXT,
 mirror TEXT AS (upper(body)) STORED
);`;
const values = target => target.rows('SELECT id,amount,body,derived,double_amount,mirror FROM t ORDER BY id');
const options = { tables: ['t'], generatedColumns: 'recompute' };
const normalize = bytes => decodeChangeset(bytes).flatMap(t => t.changes.map(c =>
  JSON.stringify({ name: t.name, pk: t.primaryKey, ...c }, (_,v) => typeof v === 'bigint' ? `${v}n` : v))).sort();

test('native changeset application maps writable columns across interleaved generated columns', async () => {
 const source=new SqliteTarget(':memory:',schema), target=new SqliteTarget(':memory:',schema);let session;
 try {session=source.db.createSession();source.db.exec("INSERT INTO t(id,amount,body) VALUES(1,3,'alpha')");const bytes=session.changeset();
  assert.equal(decodeChangeset(bytes)[0].primaryKey.length,3);
  await applyChangeset(target,bytes,options);assert.deepEqual(values(target),values(source));
 } finally {session?.close();source.close();target.close();}
});
test('journal captures writable values only and matches native Session output', async () => {
 const source=new SqliteTarget(':memory:',schema);let session;
 try {session=source.db.createSession();const capture=await captureChangeset(source,tx=>tx.execute("INSERT INTO t(id,amount,body) VALUES(1,3,'alpha')"),options);
  assert.deepEqual(normalize(capture.changeset),normalize(session.changeset()));
 } finally {session?.close();source.close();}
});
test('snapshot uses the mapped INTEGER PRIMARY KEY instead of a generated column ordinal', async () => {
 const source=new SqliteTarget(':memory:',schema);try {source.db.exec("INSERT INTO t(id,amount,body) VALUES(1,3,'alpha')");
  const snapshot=await snapshotChangeset(source,options);assert.deepEqual(decodeChangeset(snapshot.changeset)[0].changes[0].new,[1n,3n,'alpha']);
 } finally {source.close();}
});
test('atomic bootstrap installs generated-column tables without direct generated writes', async () => {
 const source=new SqliteTarget(':memory:',schema), target=new SqliteTarget(':memory:',schema);
 try {source.db.exec("INSERT INTO t(id,amount,body) VALUES(1,3,'alpha'),(2,4,'beta')");
  await new ChangesetOutbox(source).bootstrapChunks({deliveryId:'source:seed',tables:['t'],generatedColumns:'recompute',chunkRows:1});
  const receiver=new ChangesetBootstrapReceiver(target,{receiverId:'east',tables:['t'],generatedColumns:'recompute',confirmCommit:async()=>{}});
  const transfer=new ChangesetBootstrapTransfer(source,{receiverId:'east',deliveryId:'source:seed',tables:['t'],transport:receiver,confirmSource:async()=>{}});
  assert.equal((await transfer.run()).stopped,'installed');assert.deepEqual(values(target),values(source));
 } finally {source.close();target.close();}
});

import { ChangesetOrder } from '../src/changeset-order.ts';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
for (const encoding of ['UTF-8', 'UTF-16le', 'UTF-16be']) {
  for (const strategy of ['journal', 'snapshot']) test(`${encoding}: ${strategy} mutations match native writable images and invert`, async () => {
    const ddl = `PRAGMA encoding='${encoding}';${schema}`;
    const source = new SqliteTarget(':memory:', ddl), destination = new SqliteTarget(':memory:', ddl);
    let session;
    try {
      for (const target of [source, destination]) target.db.exec("INSERT INTO t(id,amount,body) VALUES(1,2,'old'),(2,5,'delete')");
      const before = values(destination);
      session = source.db.createSession({ table: 't' });
      const capture = strategy === 'journal' ? captureChangeset : captureSnapshotChangeset;
      const result = await capture(source, async tx => {
        await tx.execute('UPDATE t SET amount=?,body=? WHERE id=1', [7n, '\uFEFFhi\0界😀']);
        await tx.execute('DELETE FROM t WHERE id=2');
        await tx.execute('INSERT INTO t(id,amount,body) VALUES(?,?,?)', [9223372036854775807n, 9n, 'new']);
      }, options);
      assert.deepEqual(normalize(result.changeset), normalize(session.changeset()));
      assert.equal(result.changes, 3);
      await applyChangeset(destination, result.changeset, options);
      assert.deepEqual(values(destination), values(source));
      await applyChangeset(destination, invertChangeset(result.changeset), options);
      assert.deepEqual(values(destination), before);
      assert.equal(destination.db.applyChangeset(result.changeset), true);
      assert.deepEqual(values(destination), values(source));
    } finally { session?.close(); source.close(); destination.close(); }
  });

  for (const withoutRowid of [false, true]) test(`${encoding}: streaming composite mixed-direction keys maps physical ordinals (${withoutRowid ? 'WITHOUT ROWID' : 'rowid'})`, async () => {
    const ddl = `PRAGMA encoding='${encoding}'; CREATE TABLE t(
      g0 TEXT AS (hex(k2)) VIRTUAL,
      value BLOB,
      g1 TEXT AS (k1||':'||k2) STORED,
      k1 TEXT NOT NULL COLLATE NOCASE,
      g2 INTEGER AS (length(value)) VIRTUAL,
      k2 INTEGER NOT NULL,
      PRIMARY KEY(k2 DESC,k1 ASC)
    )${withoutRowid ? ' WITHOUT ROWID' : ''}`;
    const source = new SqliteTarget(':memory:', ddl), target = new SqliteTarget(':memory:', ddl);
    let native;
    try {
      native = source.db.createSession({ table: 't' });
      for (let i=0;i<101;i++) await source.execute('INSERT INTO t(value,k1,k2) VALUES(?,?,?)',
        [new Uint8Array([i,0,255]), `key-${i%4}`, BigInt(i)]);
      const chunks = [];
      source.after = async (kind, sql, params, result) => {
        if (kind === 'query' && sql.startsWith('SELECT typeof(')) {
          assert.ok(result.rowArrays.length <= 32);
          const plan = source.db.prepare('EXPLAIN QUERY PLAN ' + sql).all(...params);
          assert.ok(plan.every(row => !row.detail.includes('USE TEMP B-TREE')));
        }
      };
      const summary = await streamSnapshotChangesets(source, chunk => {
        assert.ok(chunk.changeset.byteLength<=2048); chunks.push(chunk.changeset);
        assert.equal(decodeChangeset(chunk.changeset)[0].primaryKey.length,3);
      }, { tables:['t'],generatedColumns:'recompute', chunkRows:7, chunkBytes:2048 });
      assert.equal(summary.changes,101);
      assert.ok(summary.chunks>14);
      assert.deepEqual(chunks.flatMap(normalize).sort(), normalize(native.changeset()));
      for (const bytes of chunks) await applyChangeset(target, bytes, options);
      assert.deepEqual(target.rows('SELECT * FROM t ORDER BY k2,k1'), source.rows('SELECT * FROM t ORDER BY k2,k1'));
      const valueReads = source.statements.filter(sql => sql.startsWith('SELECT typeof('));
      assert.ok(valueReads.length>0);
    } finally { native?.close(); source.close(); target.close(); }
  });

  test(`${encoding}: generated bootstrap reopens, replays, and hands off sequence N+1`, async () => {
    const directory=mkdtempSync(join(tmpdir(),'fsqlite-generated-'));
    const ddl=`PRAGMA encoding='${encoding}';${schema}`;
    const source=new SqliteTarget(join(directory,'source.db'),ddl);
    let target=new SqliteTarget(join(directory,'target.db'),ddl);
    try {
      source.db.exec("INSERT INTO t(id,amount,body) VALUES(1,2,'a'),(2,3,'b'),(3,4,'c')");
      const outbox=new ChangesetOutbox(source);
      const seed=await outbox.bootstrapChunks({deliveryId:'source:seed',tables:['t'],generatedColumns:'recompute',chunkRows:1});
      const receiver=()=>new ChangesetBootstrapReceiver(target,{receiverId:'east',tables:['t'],generatedColumns:'recompute',orderedSourceId:'source:incarnation',confirmCommit:async()=>{}});
      const transfer=()=>new ChangesetBootstrapTransfer(source,{receiverId:'east',deliveryId:'source:seed',tables:['t'],orderedSourceId:'source:incarnation',transport:receiver(),confirmSource:async()=>{}});
      assert.equal((await transfer().run({maxChunks:1})).stopped,'limit');
      assert.deepEqual(values(target),[]);
      target.close(); target=new SqliteTarget(join(directory,'target.db'));
      assert.equal((await transfer().run()).stopped,'installed');
      assert.deepEqual(values(target),values(source));
      const delta=await outbox.record(tx=>tx.execute('UPDATE t SET amount=12 WHERE id=1'),{deliveryId:'source:delta',tables:['t'],generatedColumns:'recompute'});
      assert.equal(delta.delivery.sequence,BigInt(seed.chunks+1));
      const loaded=await outbox.read('source:delta');
      const order=new ChangesetOrder(target,{receiverId:'east',sourceId:'source:incarnation'});
      await order.apply({...loaded.delivery,changeset:loaded.changeset},(inside,bytes)=>applyChangeset(inside,bytes,options));
      assert.deepEqual(values(target),values(source));
      assert.equal((await transfer().run()).stopped,'installed');
      assert.deepEqual(values(target),values(source));
    } finally {source.close();target.close();}
  });
}

for (const format of ['changeset','patchset']) test(`${format}: native UPDATE/DELETE retains generated constraints and writable defaults`,async()=>{
  const source=new SqliteTarget(':memory:',schema);
  const target=new SqliteTarget(':memory:',schema.replace('mirror TEXT AS (upper(body)) STORED', "mirror TEXT AS (upper(body)) STORED, added TEXT DEFAULT 'receiver-default', added_calc TEXT AS(added||body) VIRTUAL"));
  let session;
  try {
    for(const db of [source,target]) db.db.exec("INSERT INTO t(id,amount,body) VALUES(1,4,'old'),(2,5,'delete')");
    session=source.db.createSession({table:'t'});
    source.db.exec("UPDATE t SET amount=6 WHERE id=1; DELETE FROM t WHERE id=2; INSERT INTO t(id,amount,body) VALUES(3,7,'new')");
    const bytes=session[format]();
    await (format==='patchset'?applyPatchset:applyChangeset)(target,bytes,options);
    assert.deepEqual(values(target),values(source));
    assert.deepEqual(target.rows('SELECT added,added_calc FROM t ORDER BY id'),[['receiver-default','receiver-defaultold'],['receiver-default','receiver-defaultnew']]);
  }finally{session?.close();source.close();target.close();}
});

for(const constraint of ['CHECK','UNIQUE']) test(`${constraint} on generated value rolls back every row and inbox decision`,async()=>{
  const source=new SqliteTarget(':memory:','CREATE TABLE t(id INTEGER PRIMARY KEY,a INTEGER)');
  const ddl=constraint==='CHECK'?'CREATE TABLE t(g INTEGER AS(a*2) STORED CHECK(g<10),id INTEGER PRIMARY KEY,a INTEGER)':
    'CREATE TABLE t(g INTEGER AS(a%2) VIRTUAL UNIQUE,id INTEGER PRIMARY KEY,a INTEGER)';
  const target=new SqliteTarget(':memory:',ddl);let session;
  try {
    session=source.db.createSession();source.db.exec('INSERT INTO t VALUES(1,1),(2,5)');
    await assert.rejects(applyChangeset(target,session.changeset(),{...options,deliveryId:'source:bad'}),new RegExp(constraint));
    assert.deepEqual(target.rows('SELECT * FROM t'),[]);
    assert.deepEqual(target.rows("SELECT name FROM sqlite_schema WHERE name='__fsqlite_changeset_receipts'"),[]);
  }finally{session?.close();source.close();target.close();}
});

test('receiver with too few writable columns rejects before any application rows',async()=>{
  const source=new SqliteTarget(':memory:','CREATE TABLE t(id INTEGER PRIMARY KEY,a,b)');
  const target=new SqliteTarget(':memory:','CREATE TABLE t(id INTEGER PRIMARY KEY,g1 AS(id+1),a,g2 AS(id+2),g3 AS(id+3))');let session;
  try{session=source.db.createSession();source.db.exec('INSERT INTO t VALUES(1,2,3)');
    await assert.rejects(applyChangeset(target,session.changeset(),options),/writable column count/);
    assert.deepEqual(target.rows(),[]);
  }finally{session?.close();source.close();target.close();}
});

test('generated values need not be copied into snapshots or their wire budget',async()=>{
  const ddl="CREATE TABLE t(id INTEGER PRIMARY KEY,a INTEGER,g TEXT AS(hex(zeroblob(a))) VIRTUAL)";
  const source=new SqliteTarget(':memory:',ddl), target=new SqliteTarget(':memory:',ddl);
  try{source.db.exec('INSERT INTO t(id,a) VALUES(1,1000000)');
    const seed=await snapshotChangeset(source,{...options,maxBytes:1024});
    assert.ok(seed.changeset.length<100);assert.equal(seed.changes,1);
    await applyChangeset(target,seed.changeset,options);
    assert.deepEqual(target.rows('SELECT length(g) FROM t'),[[2000000n]]);
  }finally{source.close();target.close();}
});

test('trigger-driven snapshot outbox includes writable audit state, not generated images',async()=>{
  const ddl=schema+'CREATE TABLE audit(id INTEGER PRIMARY KEY,n INTEGER,g INTEGER AS(n*3) STORED);';
  const source=new SqliteTarget(':memory:',ddl),target=new SqliteTarget(':memory:',ddl);
  try{source.db.exec('CREATE TRIGGER log AFTER INSERT ON t BEGIN INSERT INTO audit(id,n) VALUES(new.id,new.double_amount); END');
    const outbox=new ChangesetOutbox(source);
    const record=await outbox.recordSnapshot(tx=>tx.execute("INSERT INTO t(id,amount,body) VALUES(1,7,'trigger')"),{deliveryId:'source:trigger',tables:['t','audit'],generatedColumns:'recompute'});
    const payload=await outbox.read(record.delivery.deliveryId);
    assert.deepEqual(decodeChangeset(payload.changeset).map(t=>t.primaryKey.length),[3,2]);
    await applyChangeset(target,payload.changeset,{tables:['t','audit'],generatedColumns:'recompute'});
    assert.deepEqual(values(target),values(source));
    assert.deepEqual(target.rows('SELECT * FROM audit'),source.rows('SELECT * FROM audit'));
  }finally{source.close();target.close();}
});

for(const bad of ['hidden','key','cid']) test(`capture refuses malformed ${bad} generated metadata`,async()=>{
  const source=new SqliteTarget(':memory:',schema);
  source.after=async(kind,sql,params,result)=>{
    if(kind==='query'&&sql==="PRAGMA main.table_xinfo('t')"){
      if(bad==='hidden')result.rowArrays[0][6]=1n;
      if(bad==='key')result.rowArrays[0][5]=1n;
      if(bad==='cid')result.rowArrays[0][0]=22n;
    }
  };
  try{let called=false;await assert.rejects(captureChangeset(source,()=>{called=true;},options));assert.equal(called,false);}
  finally{source.close();}
});

for (const mode of ['journal','snapshot','snapshot-capture','apply']) test(`${mode}: default still rejects generated schemas before application work`,async()=>{
  const source=new SqliteTarget(':memory:',schema);let session;
  try {
    if(mode==='apply') {
      session=source.db.createSession();source.db.exec("INSERT INTO t(id,amount) VALUES(1,2)");
      const target=new SqliteTarget(':memory:',schema);
      try{await assert.rejects(applyChangeset(target,session.changeset(),{tables:['t']}),{code:'ERR_FSQLITE_CHANGESET_SCHEMA'});assert.deepEqual(values(target),[]);}
      finally{target.close();}
    } else if(mode==='snapshot')await assert.rejects(snapshotChangeset(source,{tables:['t']}),{code:'ERR_FSQLITE_CAPTURE_SCHEMA'});
    else{let called=false;await assert.rejects((mode==='journal'?captureChangeset:captureSnapshotChangeset)(source,()=>{called=true;},{tables:['t']}),{code:'ERR_FSQLITE_CAPTURE_SCHEMA'});assert.equal(called,false);}
  }finally{session?.close();source.close();}
});

test('unconfigured bootstrap stages but refuses recomputation and preserves all source bytes',async()=>{
  const source=new SqliteTarget(':memory:',schema),target=new SqliteTarget(':memory:',schema);
  try{source.db.exec('INSERT INTO t(id,amount) VALUES(1,2)');const outbox=new ChangesetOutbox(source);
    await outbox.bootstrapChunks({...options,deliveryId:'source:seed'});
    const receiver=new ChangesetBootstrapReceiver(target,{receiverId:'east',tables:['t'],confirmCommit:async()=>{}});
    const transfer=new ChangesetBootstrapTransfer(source,{receiverId:'east',deliveryId:'source:seed',tables:['t'],transport:receiver,confirmSource:async()=>{}});
    await assert.rejects(transfer.run());assert.deepEqual(values(target),[]);
    assert.equal((await outbox.pending()).length,1);
    assert.ok((await outbox.read('source:seed')).changeset.length>0);
  }finally{source.close();target.close();}
});

for(const mode of ['capture','apply','bootstrap']) test(`${mode}: rejects unknown generated policy before transaction admission`,async()=>{
  let admitted=false;const target={transaction:async()=>{admitted=true;throw new Error('must not admit');}};
  for(const generatedColumns of [true,null,'ignore','RECOMPUTE']){
    if(mode==='capture')await assert.rejects(captureChangeset(target,()=>{}, {...options,generatedColumns}),{code:'ERR_FSQLITE_CAPTURE_INPUT'});
    if(mode==='apply')await assert.rejects(applyChangeset(target,new Uint8Array(), {...options,generatedColumns}),{code:'ERR_FSQLITE_CHANGESET_INPUT'});
    if(mode==='bootstrap')assert.throws(()=>new ChangesetBootstrapReceiver(target,{receiverId:'east',...options,generatedColumns,confirmCommit:async()=>{}}),{code:'ERR_FSQLITE_BOOTSTRAP_INPUT'});
  }
  assert.equal(admitted,false);
});

test('capture mode is captured before transaction admission rather than read from mutated options',async()=>{
  const source=new SqliteTarget(':memory:',schema);
  try { const opts={...options}; const owner={transaction:async(work,controls)=>{opts.generatedColumns=undefined;return source.transaction(work,controls);}};
    const result=await captureChangeset(owner,tx=>tx.execute('INSERT INTO t(id,amount) VALUES(1,2)'),opts);
    assert.equal(result.changes,1);
  }finally{source.close();}
});

test('generated constraint failure in a late bootstrap chunk rolls back the complete install',async()=>{
  const source=new SqliteTarget(':memory:',schema);
  const destination=new SqliteTarget(':memory:',schema.replace('double_amount INTEGER AS (amount*2) STORED','double_amount INTEGER AS (amount*2) STORED CHECK(double_amount<10)'));
  let confirmations=0;
  try{source.db.exec("INSERT INTO t(id,amount,body) VALUES(1,2,'first'),(2,8,'bad')");
    const outbox=new ChangesetOutbox(source);await outbox.bootstrapChunks({...options,chunkRows:1,deliveryId:'source:seed'});
    const receiver=new ChangesetBootstrapReceiver(destination,{receiverId:'east',...options,confirmCommit:async()=>{confirmations++;}});
    const transfer=new ChangesetBootstrapTransfer(source,{receiverId:'east',deliveryId:'source:seed',tables:['t'],transport:receiver,confirmSource:async()=>{}});
    await assert.rejects(transfer.run());assert.deepEqual(values(destination),[]);assert.equal(confirmations,0);
    assert.equal((await outbox.pending()).length,2);
    assert.deepEqual(destination.rows('SELECT installed FROM __fsqlite_bootstrap_state'),[[0n]]);
    assert.ok(destination.rows('SELECT sum(length(payload)) FROM __fsqlite_bootstrap_chunks')[0][0]>0n);
  }finally{source.close();destination.close();}
});

for (const generatedSource of [true,false]) test(`${generatedSource?'generated-to-base':'base-to-generated'} uses identical writable wire positions`,async()=>{
  const plain='CREATE TABLE t(id INTEGER PRIMARY KEY,amount INTEGER NOT NULL,body TEXT)';
  const source=new SqliteTarget(':memory:',generatedSource?schema:plain),target=new SqliteTarget(':memory:',generatedSource?plain:schema);
  try{
    const captured=await captureChangeset(source,tx=>tx.execute("INSERT INTO t(amount,body) VALUES(7,'computed')"),options);
    const wire=decodeChangeset(captured.changeset)[0];assert.deepEqual(wire.primaryKey,[1,0,0]);assert.deepEqual(wire.changes[0].new,[1n,7n,'computed']);
    await applyChangeset(target,captured.changeset,options);
    assert.deepEqual(target.rows('SELECT id,amount,body FROM t'),source.rows('SELECT id,amount,body FROM t'));
    if(!generatedSource)assert.deepEqual(target.rows('SELECT double_amount,mirror FROM t'),[[14n,'COMPUTED']]);
  }finally{source.close();target.close();}
});
