import assert from 'node:assert/strict';
import { test } from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';
import { ChangesetOutbox, CHANGESET_OUTBOX_TABLE } from '../src/changeset-outbox.ts';
import { decodeChangeset } from '../src/changeset-codec.ts';

class Target {
  constructor(path=':memory:',initialize=true){
    this.db=new DatabaseSync(path);this.db.exec('PRAGMA recursive_triggers=ON;PRAGMA foreign_keys=ON;PRAGMA busy_timeout=0;');
    if(initialize)this.db.exec('CREATE TABLE t(id INTEGER PRIMARY KEY,value);');
    this.depth=0;this.serial=0;this.sql=[];
  }
  async execute(sql,params=[]){this.sql.push(sql);return Number(this.db.prepare(sql).run(...params).changes);}
  async query(sql,params=[]){this.sql.push(sql);const stmt=this.db.prepare(sql);stmt.setReadBigInts(true);stmt.setReturnArrays(true);return{rowArrays:stmt.all(...params)};}
  async transaction(work){
    const nested=this.depth++>0,sp=`outbox_test_${++this.serial}`;
    this.db.exec(nested?`SAVEPOINT ${sp}`:'BEGIN');
    try{const result=await work(this);this.db.exec(nested?`RELEASE ${sp}`:'COMMIT');return result;}
    catch(error){this.db.exec(nested?`ROLLBACK TO ${sp};RELEASE ${sp}`:'ROLLBACK');throw error;}
    finally{this.depth--;}
  }
  rows(){return this.db.prepare('SELECT * FROM t ORDER BY id').all().map(row=>({...row}));}
  close(){this.db.close();}
}
const options=(deliveryId='source:1')=>({deliveryId,tables:['t']});
const add=tx=>tx.execute("INSERT INTO t VALUES(1,'first')");
const count=target=>target.db.prepare(`SELECT count(*) AS n FROM ${CHANGESET_OUTBOX_TABLE}`).get().n;

test('source rows and real SQLite binary changeset are atomically recorded',async()=>{
  const source=new Target(),receiver=new Target(),outbox=new ChangesetOutbox(source);
  try{
    const recorded=await outbox.record(async tx=>{await add(tx);return 42;},options());
    assert.equal(recorded.replayed,false);assert.equal(recorded.value,42);assert.equal(recorded.delivery.sequence,1n);
    const page=await outbox.pending();assert.deepEqual(page,[recorded.delivery]);
    const read=await outbox.read('source:1');assert.ok(read.changeset instanceof Uint8Array);
    assert.equal(read.changeset.byteLength,read.delivery.byteLength);assert.equal(read.delivery.changes,1);
    assert.equal(receiver.db.applyChangeset(read.changeset),true);assert.deepEqual(receiver.rows(),source.rows());
    assert.equal(await outbox.acknowledge('source:1',read.delivery.sha256),true);
    assert.deepEqual(await outbox.pending(),[]);assert.equal((await outbox.read('source:1')).changeset,null);
    assert.equal(count(source),1);
  }finally{source.close();receiver.close();}
});
test('redelivery does not execute work or invent the original callback value',async()=>{
  const target=new Target(),outbox=new ChangesetOutbox(target);let called=0;
  try{
    const work=async tx=>{called++;await add(tx);return{anything:'not persisted'};};
    const first=await outbox.record(work,options()),second=await outbox.record(work,options());
    assert.equal(called,1);assert.equal(second.replayed,true);assert.equal('value' in second,false);
    assert.deepEqual(second.delivery,first.delivery);assert.equal(target.rows().length,1);
  }finally{target.close();}
});
test('acknowledgement reclaims payload bytes but retains operation deduplication',async()=>{
  const target=new Target(),outbox=new ChangesetOutbox(target);
  try{
    const first=await outbox.record(add,options());
    assert.equal(await outbox.acknowledge('source:1',first.delivery.sha256),true);
    assert.equal(await outbox.acknowledge('source:1',first.delivery.sha256),false);
    const retry=await outbox.record(()=>{throw new Error('must not replay');},options());
    assert.equal(retry.replayed,true);assert.equal(retry.delivery.acknowledged,true);
    assert.equal(target.db.prepare(`SELECT length(payload) AS n FROM ${CHANGESET_OUTBOX_TABLE}`).get().n,0);
  }finally{target.close();}
});
test('wrong and unknown acknowledgements do not remove pending payloads',async()=>{
  const target=new Target(),outbox=new ChangesetOutbox(target);
  try{
    const first=await outbox.record(add,options());
    await assert.rejects(outbox.acknowledge('source:1','0'.repeat(64)),{code:'ERR_FSQLITE_OUTBOX_ACK'});
    await assert.rejects(outbox.acknowledge('missing',first.delivery.sha256),{code:'ERR_FSQLITE_OUTBOX_ACK'});
    assert.equal((await outbox.pending()).length,1);assert.ok((await outbox.read('source:1')).changeset.byteLength>0);
  }finally{target.close();}
});
test('only an exact acknowledged identity can be explicitly forgotten',async()=>{
  const target=new Target(),outbox=new ChangesetOutbox(target);
  try{
    const first=await outbox.record(add,options());
    await assert.rejects(outbox.forgetAcknowledged('source:1',first.delivery.sha256),{code:'ERR_FSQLITE_OUTBOX_STATE'});
    await outbox.acknowledge('source:1',first.delivery.sha256);
    await assert.rejects(outbox.forgetAcknowledged('source:1','0'.repeat(64)),{code:'ERR_FSQLITE_OUTBOX_STATE'});
    assert.equal(await outbox.forgetAcknowledged('source:1',first.delivery.sha256),true);
    assert.equal(await outbox.forgetAcknowledged('source:1',first.delivery.sha256),false);
    assert.equal(await outbox.read('source:1'),null);assert.equal(target.rows().length,1);
    const next=await outbox.record(tx=>tx.execute("INSERT INTO t VALUES(2,'second')"),options('source:2'));
    assert.ok(next.delivery.sequence>first.delivery.sequence);
  }finally{target.close();}
});
test('payload pages use monotonic sequences, skip acknowledged entries and never load blobs',async()=>{
  const target=new Target(),outbox=new ChangesetOutbox(target);
  try{
    const items=[];
    for(let i=0;i<5;i++)items.push(await outbox.record(tx=>tx.execute('INSERT INTO t VALUES(?,?)',[BigInt(i),BigInt(i)]),options(`source:${i}`)));
    await outbox.acknowledge('source:1',items[1].delivery.sha256);
    target.sql=[];
    const a=await outbox.pending({limit:2}),b=await outbox.pending({limit:2,after:a.at(-1).sequence});
    assert.deepEqual([...a,...b].map(x=>x.deliveryId),['source:0','source:2','source:3','source:4']);
    assert.equal(target.sql.some(sql=>sql.startsWith('SELECT payload FROM')),false);
    assert.ok(Object.isFrozen(a));assert.ok(Object.isFrozen(a[0]));
  }finally{target.close();}
});
test('payload is owned and verified on every read',async()=>{
  const target=new Target(),outbox=new ChangesetOutbox(target);
  try{
    await outbox.record(add,options());
    const a=await outbox.read('source:1'),original=a.changeset.slice();a.changeset.fill(0);
    assert.deepEqual((await outbox.read('source:1')).changeset,original);
    target.db.exec(`UPDATE ${CHANGESET_OUTBOX_TABLE} SET payload=zeroblob(byte_length)`);
    await assert.rejects(outbox.read('source:1'),{code:'ERR_FSQLITE_OUTBOX_CORRUPT'});
    await assert.rejects(outbox.record(()=>{},options()),{code:'ERR_FSQLITE_OUTBOX_CORRUPT'});
  }finally{target.close();}
});
test('callback failure rolls back application data and creation of a new outbox',async()=>{
  const target=new Target(),outbox=new ChangesetOutbox(target),failure=new Error('work');
  try{
    await assert.rejects(outbox.record(async tx=>{await add(tx);throw failure;},options()),e=>e===failure);
    assert.deepEqual(target.rows(),[]);
    assert.equal(target.db.prepare('SELECT count(*) AS n FROM sqlite_schema WHERE name=?').get(CHANGESET_OUTBOX_TABLE).n,0);
    assert.deepEqual(await outbox.pending(),[]);
  }finally{target.close();}
});
test('failed payload retention rolls back source DML and frees capture TEMP state',async()=>{
  const target=new Target(),outbox=new ChangesetOutbox(target,{maxPayloadBytes:1});
  try{
    await assert.rejects(outbox.record(add,options()),{code:'ERR_FSQLITE_OUTBOX_FULL'});
    assert.deepEqual(target.rows(),[]);
    assert.deepEqual(target.db.prepare("SELECT name FROM temp.sqlite_schema WHERE name GLOB '__fsqlite_capture_*'").all(),[]);
    assert.deepEqual(await outbox.pending(),[]);
  }finally{target.close();}
});
test('retained-entry capacity rejects before callback but permits existing-ID recovery',async()=>{
  const target=new Target(),outbox=new ChangesetOutbox(target,{maxEntries:1});let called=false;
  try{
    const first=await outbox.record(add,options());
    await assert.rejects(outbox.record(()=>{called=true;},options('source:2')),{code:'ERR_FSQLITE_OUTBOX_FULL'});
    assert.equal(called,false);assert.equal((await outbox.record(()=>{throw new Error('replay');},options())).replayed,true);
    await outbox.acknowledge('source:1',first.delivery.sha256);
    await assert.rejects(outbox.record(()=>{},options('source:2')),{code:'ERR_FSQLITE_OUTBOX_FULL'});
    await outbox.forgetAcknowledged('source:1',first.delivery.sha256);
    assert.equal((await outbox.record(()=>{},options('source:2'))).replayed,false);
  }finally{target.close();}
});
test('acknowledging payloads restores byte capacity without forgetting IDs',async()=>{
  const target=new Target(),outbox=new ChangesetOutbox(target,{maxPayloadBytes:32});
  try{
    const a=await outbox.record(add,options());
    await assert.rejects(outbox.record(tx=>tx.execute("INSERT INTO t VALUES(2,'second')"),options('source:2')),{code:'ERR_FSQLITE_OUTBOX_FULL'});
    assert.equal(target.rows().length,1);await outbox.acknowledge('source:1',a.delivery.sha256);
    await outbox.record(tx=>tx.execute("INSERT INTO t VALUES(2,'second')"),options('source:2'));
    assert.equal(target.rows().length,2);assert.equal(count(target),2);
  }finally{target.close();}
});
test('deferred commit errors roll back both source rows and retained payload',async()=>{
  const target=new Target();target.db.exec('CREATE TABLE p(id PRIMARY KEY);CREATE TABLE child(id PRIMARY KEY,pid REFERENCES p DEFERRABLE INITIALLY DEFERRED);');
  const outbox=new ChangesetOutbox(target);
  try{
    await assert.rejects(outbox.record(tx=>tx.execute('INSERT INTO child VALUES(1,9)'),{deliveryId:'source:bad',tables:['child']}),/FOREIGN KEY/);
    assert.equal(target.db.prepare('SELECT count(*) AS n FROM child').get().n,0);assert.deepEqual(await outbox.pending(),[]);
  }finally{target.close();}
});
test('outer rollback undoes outbox record and acknowledgement together',async()=>{
  const target=new Target(),outbox=new ChangesetOutbox(target);
  try{
    await assert.rejects(target.transaction(async()=>{
      const result=await outbox.record(add,options());await outbox.acknowledge('source:1',result.delivery.sha256);throw new Error('outer');
    }),/outer/);
    assert.deepEqual(target.rows(),[]);assert.deepEqual(await outbox.pending(),[]);
  }finally{target.close();}
});
test('cancelled source work cannot leave a queued payload',async()=>{
  const target=new Target(),outbox=new ChangesetOutbox(target),abort=new AbortController();
  try{
    await assert.rejects(outbox.record(async tx=>{await add(tx);abort.abort();},{...options(),signal:abort.signal}),{code:'ERR_FSQLITE_CAPTURE_CANCELLED'});
    assert.deepEqual(target.rows(),[]);assert.deepEqual(await outbox.pending(),[]);
  }finally{target.close();}
});
test('scope identity is case-insensitive and order-independent, but cannot be changed on retry',async()=>{
  const target=new Target();target.db.exec('CREATE TABLE u(id PRIMARY KEY)');const outbox=new ChangesetOutbox(target);
  try{
    await outbox.record(add,{deliveryId:'source:1',tables:['u','T']});
    assert.equal((await outbox.record(()=>{throw new Error('replay');},{deliveryId:'source:1',tables:['t','U']})).replayed,true);
    await assert.rejects(outbox.record(()=>{},options()),{code:'ERR_FSQLITE_OUTBOX_REUSE'});
    await assert.rejects(outbox.record(()=>{},{deliveryId:'source:1',tables:['t','u'],indirect:true}),{code:'ERR_FSQLITE_OUTBOX_REUSE'});
  }finally{target.close();}
});
test('lost source commit acknowledgement recovers the retained delivery without repeating SQL',async()=>{
  const target=new Target();let lose=true,called=0;
  const uncertain={transaction:async work=>{const result=await target.transaction(work);if(lose){lose=false;throw new Error('lost commit ACK');}return result;}};
  const outbox=new ChangesetOutbox(uncertain);
  try{
    const work=async tx=>{called++;await add(tx);};
    await assert.rejects(outbox.record(work,options()),/lost commit ACK/);
    const recovered=await outbox.record(work,options());assert.equal(recovered.replayed,true);assert.equal(called,1);
    assert.equal(target.rows().length,1);assert.equal((await outbox.pending()).length,1);
  }finally{target.close();}
});
test('lost local ACK acknowledgement is safe to repeat and retains source deduplication',async()=>{
  const target=new Target(),normal=new ChangesetOutbox(target);
  try{
    const first=await normal.record(add,options());let lose=true;
    const uncertain=new ChangesetOutbox({transaction:async work=>{const result=await target.transaction(work);if(lose){lose=false;throw new Error('lost ACK');}return result;}});
    await assert.rejects(uncertain.acknowledge('source:1',first.delivery.sha256),/lost ACK/);
    assert.equal(await uncertain.acknowledge('source:1',first.delivery.sha256),false);
    assert.equal((await normal.record(()=>{throw new Error('replay');},options())).replayed,true);
  }finally{target.close();}
});
test('file-backed outbox survives a child-process reopen and is independently deliverable',async()=>{
  const directory=mkdtempSync(join(tmpdir(),'fsqlite-outbox-')),path=join(directory,'source.db');
  const target=new Target(path),outbox=new ChangesetOutbox(target);
  const first=await outbox.record(add,options());target.close();
  const moduleUrl=new URL('../src/changeset-outbox.ts',import.meta.url).href;
  const program=`import assert from 'node:assert/strict';import{DatabaseSync}from'node:sqlite';import{ChangesetOutbox}from${JSON.stringify(moduleUrl)};${Target.toString()}
    const target=new Target(${JSON.stringify(path)},false);const outbox=new ChangesetOutbox(target);
    const found=await outbox.read('source:1');assert.equal(found.delivery.sha256,${JSON.stringify(first.delivery.sha256)});
    const receiver=new Target();assert.equal(receiver.db.applyChangeset(found.changeset),true);assert.deepEqual(receiver.rows(),target.rows());
    assert.equal((await outbox.record(()=>{throw Error('must not run')},{deliveryId:'source:1',tables:['t']})).replayed,true);
    await outbox.acknowledge('source:1',found.delivery.sha256);receiver.close();target.close();console.log('reopened-and-delivered');`;
  const child=spawnSync(process.execPath,[...process.execArgv,'--input-type=module','-e',program],{encoding:'utf8',timeout:20000});
  assert.equal(child.status,0,child.stderr);assert.match(child.stdout,/reopened-and-delivered/);
  const reopened=new Target(path,false);
  try{const recovered=await new ChangesetOutbox(reopened).read('source:1');assert.equal(recovered.delivery.acknowledged,true);assert.equal(recovered.changeset,null);}finally{reopened.close();}
});
test('two file connections overlap at one absent ID; no duplicate source transaction commits',async()=>{
  const directory=mkdtempSync(join(tmpdir(),'fsqlite-outbox-race-')),path=join(directory,'source.db');
  const a=new Target(path),oa=new ChangesetOutbox(a);a.db.exec('PRAGMA journal_mode=WAL');
  await oa.record(()=>{},options('setup'));
  const b=new Target(path,false),ob=new ChangesetOutbox(b);
  let release,entered;const held=new Promise(r=>{release=r;}),started=new Promise(r=>{entered=r;});
  try{
    const pending=oa.record(async tx=>{entered();await held;await add(tx);},options());
    await started;
    const other=await ob.record(tx=>tx.execute("INSERT INTO t VALUES(2,'winner')"),options());
    assert.equal(other.replayed,false);release();await assert.rejects(pending,/locked|busy/i);
    assert.deepEqual(a.rows(),[{id:2,value:'winner'}]);
    assert.equal((await oa.record(()=>{throw new Error('replay');},options())).replayed,true);
    assert.equal(count(a),2);
  }finally{release?.();a.close();b.close();}
});
for(const [label,sql] of [
  ['wrong length',`UPDATE ${CHANGESET_OUTBOX_TABLE} SET byte_length=byte_length+1`],
  ['negative count',`UPDATE ${CHANGESET_OUTBOX_TABLE} SET change_count=-1`],
  ['invalid state',`UPDATE ${CHANGESET_OUTBOX_TABLE} SET acknowledged=2`],
  ['scope NUL',`UPDATE ${CHANGESET_OUTBOX_TABLE} SET scope=scope||char(0)||'suffix'`],
  ['invalid scope',`UPDATE ${CHANGESET_OUTBOX_TABLE} SET scope='{}'`],
  ['nonblob payload',`UPDATE ${CHANGESET_OUTBOX_TABLE} SET payload='text'`],
])test(`corrupt ${label} rejects without replaying source work`,async()=>{
  const target=new Target(),outbox=new ChangesetOutbox(target);let called=false;
  try{await outbox.record(add,options());target.db.exec(sql);await assert.rejects(outbox.record(()=>{called=true;},options()),e=>/^ERR_FSQLITE_OUTBOX_/.test(e.code));assert.equal(called,false);}finally{target.close();}
});
test('existing incompatible outbox schema is rejected, not overwritten',async()=>{
  const target=new Target();target.db.exec(`CREATE TABLE ${CHANGESET_OUTBOX_TABLE}(precious);INSERT INTO ${CHANGESET_OUTBOX_TABLE} VALUES(42)`);
  try{await assert.rejects(new ChangesetOutbox(target).record(add,options()),{code:'ERR_FSQLITE_OUTBOX_SCHEMA'});assert.equal(target.db.prepare(`SELECT precious FROM ${CHANGESET_OUTBOX_TABLE}`).get().precious,42);assert.deepEqual(target.rows(),[]);}finally{target.close();}
});
test('application triggers or new indexes on the outbox are rejected',async()=>{
  for(const ddl of [`CREATE TRIGGER evil AFTER UPDATE ON ${CHANGESET_OUTBOX_TABLE} BEGIN DELETE FROM t;END`, `CREATE INDEX extra ON ${CHANGESET_OUTBOX_TABLE}(sha256)`]){
    const target=new Target(),outbox=new ChangesetOutbox(target);
    try{await outbox.record(add,options());target.db.exec(ddl);await assert.rejects(outbox.pending(),{code:'ERR_FSQLITE_OUTBOX_SCHEMA'});assert.equal(target.rows().length,1);}finally{target.close();}
  }
});
test('read-only discovery does not create an outbox',async()=>{
  const target=new Target(),outbox=new ChangesetOutbox(target);
  try{assert.deepEqual(await outbox.pending(),[]);assert.equal(await outbox.read('missing'),null);assert.equal(await outbox.forgetAcknowledged('missing','0'.repeat(64)),false);assert.equal(target.db.prepare('SELECT count(*) AS n FROM sqlite_schema WHERE name=?').get(CHANGESET_OUTBOX_TABLE).n,0);}finally{target.close();}
});
test('empty changesets retain a stable operation receipt and valid empty payload',async()=>{
  const target=new Target(),outbox=new ChangesetOutbox(target);
  try{const first=await outbox.record(()=>42,options());assert.equal(first.delivery.changes,0);assert.equal(first.delivery.byteLength,0);assert.deepEqual(decodeChangeset((await outbox.read('source:1')).changeset),[]);assert.equal((await outbox.record(()=>{throw new Error('replay');},options())).replayed,true);}finally{target.close();}
});
test('input failures do not enter a transaction',async()=>{
  const outbox=new ChangesetOutbox({transaction:()=>{throw new Error('entered');}});
  for(const deliveryId of ['', 'x'.repeat(513), 'nul\0', '\ud800'])await assert.rejects(outbox.record(()=>{},options(deliveryId)),{code:'ERR_FSQLITE_OUTBOX_INPUT'});
  for(const page of [{limit:0},{limit:257},{after:-1n},{after:1}])await assert.rejects(outbox.pending(page),{code:'ERR_FSQLITE_OUTBOX_INPUT'});
});

test('outbox insertion failure after captured DML rolls back the source operation',async()=>{
  const target=new Target(),failure=new Error('injected storage failure');
  const execute=target.execute.bind(target);
  target.execute=async(sql,params)=>{if(sql.startsWith('INSERT OR ABORT INTO main."__fsqlite_changeset_outbox"'))throw failure;return execute(sql,params);};
  const outbox=new ChangesetOutbox(target);
  try{
    await assert.rejects(outbox.record(add,options()),e=>e===failure);assert.deepEqual(target.rows(),[]);
    assert.deepEqual(await outbox.pending(),[]);
    assert.deepEqual(target.db.prepare("SELECT name FROM temp.sqlite_schema WHERE name GLOB '__fsqlite_capture_*'").all(),[]);
  }finally{target.close();}
});
test('monotonic outbox sequences preserve signed-int64 precision above 2^53',async()=>{
  const target=new Target(),outbox=new ChangesetOutbox(target);
  try{
    await outbox.record(()=>{},options('setup'));
    target.db.prepare('UPDATE sqlite_sequence SET seq=? WHERE name=?').run(9007199254740993n,CHANGESET_OUTBOX_TABLE);
    const result=await outbox.record(add,options());assert.equal(result.delivery.sequence,9007199254740994n);
    assert.deepEqual((await outbox.pending({after:9007199254740993n})).map(x=>x.sequence),[9007199254740994n]);
  }finally{target.close();}
});
test('schema validation accepts harmless engine SQL formatting and rejects fake AUTOINCREMENT text',async()=>{
  const target=new Target(),outbox=new ChangesetOutbox(target);
  try{
    await outbox.record(add,options());
    const query=target.query.bind(target);
    target.query=async(sql,params)=>{
      const result=await query(sql,params);
      if(sql.startsWith('SELECT type, sql FROM main.sqlite_schema')&&result.rowArrays.length){
        result.rowArrays[0][1]=result.rowArrays[0][1].replace(/CREATE TABLE/,'create   table').replace(/AUTOINCREMENT/,'autoincrement');
      }
      return result;
    };
    assert.equal((await outbox.pending()).length,1);
    target.query=async(sql,params)=>{
      const result=await query(sql,params);
      if(sql.startsWith('SELECT type, sql FROM main.sqlite_schema')&&result.rowArrays.length){
        result.rowArrays[0][1]=result.rowArrays[0][1].replace(/AUTOINCREMENT/,"/* AUTOINCREMENT */");
      }
      return result;
    };
    await assert.rejects(outbox.pending(),{code:'ERR_FSQLITE_OUTBOX_SCHEMA'});
  }finally{target.close();}
});
for(const cut of ['before','after'])test(`SIGKILL ${cut} COMMIT preserves the source/outbox atomic decision`,async()=>{
  const directory=mkdtempSync(join(tmpdir(),`fsqlite-outbox-${cut}-`)),path=join(directory,'source.db');
  const seed=new Target(path);seed.close();
  const moduleUrl=new URL('../src/changeset-outbox.ts',import.meta.url).href;
  const program=`import{DatabaseSync}from'node:sqlite';import{writeSync}from'node:fs';import{ChangesetOutbox}from${JSON.stringify(moduleUrl)};${Target.toString()}
    const target=new Target(${JSON.stringify(path)},false);
    target.transaction=async work=>{target.db.exec('BEGIN');const result=await work(target);
      ${cut==='after'?"target.db.exec('COMMIT');":''}
      writeSync(1,'reached-${cut}-commit');process.kill(process.pid,'SIGKILL');return result;};
    await new ChangesetOutbox(target).record(tx=>tx.execute("INSERT INTO t VALUES(1,'first')"),{deliveryId:'source:1',tables:['t']});`;
  const child=spawnSync(process.execPath,[...process.execArgv,'--input-type=module','-e',program],{encoding:'utf8',timeout:20000});
  assert.equal(child.signal,'SIGKILL',child.stderr);assert.match(child.stdout,new RegExp(`reached-${cut}-commit`));
  const target=new Target(path,false),outbox=new ChangesetOutbox(target);
  try{
    if(cut==='before'){
      assert.deepEqual(target.rows(),[]);assert.equal(await outbox.read('source:1'),null);
      assert.equal((await outbox.record(add,options())).replayed,false);
    }else{
      assert.deepEqual(target.rows(),[{id:1,value:'first'}]);
      assert.ok((await outbox.read('source:1')).changeset.byteLength>0);
      assert.equal((await outbox.record(()=>{throw new Error('must not replay');},options())).replayed,true);
    }
  }finally{target.close();}
});
