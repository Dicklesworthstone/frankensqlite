import assert from 'node:assert/strict';
import { test } from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createHash } from 'node:crypto';
import { spawn } from 'node:child_process';
import { applyChangeset, applyPatchset } from '../src/changeset-apply.ts';
import { encodeChangeset, invertChangeset } from '../src/changeset-codec.ts';
import { ChangesetBootstrapReceiver, createBootstrapManifest } from '../src/changeset-bootstrap.ts';
import { ChangesetOrder } from '../src/changeset-order.ts';
import { withDeferredForeignKeys } from '../src/changeset-foreign-keys.ts';
import { createBootstrapHttpHandler, createBootstrapHttpTransport } from '../src/changeset-bootstrap-http.ts';
import { SqliteTarget } from './helpers/production-sqlite-target.mjs';
import { serveBootstrap } from './helpers/production-bootstrap-http-host.mjs';

const schema = 'CREATE TABLE parent(id INTEGER PRIMARY KEY,child INTEGER NOT NULL REFERENCES child(id));' +
  'CREATE TABLE child(id INTEGER PRIMARY KEY,parent INTEGER NOT NULL REFERENCES parent(id));';
const tables=['child','parent'];
const sha256=bytes=>createHash('sha256').update(bytes).digest('hex');
function nativeCycle(format='changeset',encoding='UTF-8') {
  const db=new DatabaseSync(':memory:');db.exec(`PRAGMA encoding='${encoding}';PRAGMA foreign_keys=ON;${schema}`);
  const sessions=tables.map(table=>db.createSession({table}));
  try{
    db.exec('BEGIN;PRAGMA defer_foreign_keys=ON;INSERT INTO child VALUES(1,1);INSERT INTO parent VALUES(1,1);COMMIT');
    return sessions.map(session=>new Uint8Array(session[format]()));
  }finally{sessions.forEach(session=>session.close());db.close();}
}
const combine=parts=>new Uint8Array(Buffer.concat(parts));
const cycles=nativeCycle();
const payload=combine(cycles);
const target=(encoding='UTF-8',path=':memory:')=>new SqliteTarget(path,`PRAGMA encoding='${encoding}';${schema}`);
const contents=db=>tables.map(table=>db.rows(`SELECT * FROM ${table} ORDER BY id`));
const flags=db=>[db.rows('PRAGMA foreign_keys')[0][0],db.rows('PRAGMA defer_foreign_keys')[0][0]];
const noInbox=db=>assert.equal(db.rows("SELECT count(*) FROM sqlite_schema WHERE name='__fsqlite_changeset_receipts'")[0][0],0n);
const manifest=()=>createBootstrapManifest({receiverId:'replica',deliveryId:'source:cycle',tables,chunks:2,changes:2,byteLength:payload.length},async index=>cycles[index]);
const receiver=(db,options={})=>new ChangesetBootstrapReceiver(db,{receiverId:'replica',tables,foreignKeys:'defer',confirmCommit:async()=>{},...options});
const stage=async(r,m)=>{for(let i=0;i<cycles.length;i++)await r.stage(m,i,cycles[i]);};
const code=kind=>({code:`ERR_FSQLITE_FOREIGN_KEY_${kind}`});

for(const encoding of ['UTF-8','UTF-16le','UTF-16be']) for(const format of ['changeset','patchset']) {
  test(`${encoding} ${format}: native cyclic rows apply as one FK-clean transaction and replay once`,async()=>{
    const db=target(encoding),native=new DatabaseSync(':memory:');native.exec('PRAGMA foreign_keys=ON;'+schema);
    const bytes=combine(nativeCycle(format,encoding)),apply=format==='changeset'?applyChangeset:applyPatchset;
    try{
      assert.deepEqual(await apply(db,bytes,{tables,deliveryId:'source:1',foreignKeys:'defer'}),{applied:2,omitted:0,replayed:false});
      assert.equal(native.applyChangeset(bytes),true);
      assert.deepEqual(contents(db),[[[1n,1n]],[[1n,1n]]]);assert.deepEqual(flags(db),[1n,0n]);
      const insertions=db.statements.filter(sql=>sql.startsWith('INSERT OR ABORT INTO main."child"')).length;
      assert.equal((await apply(db,bytes,{tables,deliveryId:'source:1',foreignKeys:'defer'})).replayed,true);
      assert.equal(db.statements.filter(sql=>sql.startsWith('INSERT OR ABORT INTO main."child"')).length,insertions);
      assert.equal(db.rows('PRAGMA foreign_key_check').length,0);
      if(format==='changeset'){
        const inverse=invertChangeset(bytes);
        assert.equal((await applyChangeset(db,inverse,{tables,foreignKeys:'defer'})).applied,2);
        assert.deepEqual(contents(db),[[],[]]);
      }
    }finally{db.close();native.close();}
  });
}
for(const [name,apply] of [['changeset',applyChangeset],['patchset',applyPatchset]]) {
  test(`${name}: legacy policy is unchanged and does not silently enable deferral`,async()=>{
    const db=target(),bytes=combine(nativeCycle(name));
    try{await assert.rejects(apply(db,bytes,{tables,deliveryId:'legacy'}),/FOREIGN KEY/);
      assert.deepEqual(contents(db),[[],[]]);assert.deepEqual(flags(db),[1n,0n]);noInbox(db);
      assert.equal(db.statements.some(sql=>sql==='PRAGMA defer_foreign_keys=ON'),false);
    }finally{db.close();}
  });
}
for(const invalid of [null,false,true,0,'off','disable','DEFER']) {
  test(`invalid FK policy ${String(invalid)} rejects before SQL or bootstrap admission`,async()=>{
    let calls=0;const owner={transaction:async()=>{calls++;throw new Error('must not execute');}};
    await assert.rejects(applyChangeset(owner,payload,{tables,foreignKeys:invalid}),/foreignKeys must be defer/);
    await assert.rejects(applyPatchset(owner,combine(nativeCycle('patchset')),{tables,foreignKeys:invalid}),/foreignKeys must be defer/);
    assert.throws(()=>receiver(owner,{foreignKeys:invalid}),/foreignKeys must be defer/);assert.equal(calls,0);
  });
}
test('unresolved FK violations abort the row, rebase hook and receipt together',async()=>{
  const db=target();db.db.exec('CREATE TABLE journal(id INTEGER PRIMARY KEY,bytes BLOB)');let hooks=0;
  try{await assert.rejects(applyChangeset(db,cycles[0],{tables,deliveryId:'orphan',foreignKeys:'defer',onRebase:async(tx,bytes)=>{
    hooks++;await tx.execute('INSERT INTO journal VALUES(1,?)',[bytes.length?bytes:new Uint8Array([0])]);
  }}),code('VIOLATION'));
    assert.equal(hooks,1);assert.deepEqual(contents(db),[[],[]]);assert.equal(db.rows('SELECT count(*) FROM journal')[0][0],0n);noInbox(db);assert.deepEqual(flags(db),[1n,0n]);
  }finally{db.close();}
});
test('omitting a before-image conflict cannot authorize remaining FK violations',async()=>{
  const db=new SqliteTarget(':memory:','CREATE TABLE parent(id INTEGER PRIMARY KEY);CREATE TABLE child(id INTEGER PRIMARY KEY,parent REFERENCES parent(id),v);INSERT INTO parent VALUES(1);INSERT INTO child VALUES(1,1,99)');let conflicts=0;
  const bytes=encodeChangeset([
    {name:'parent',primaryKey:[1],changes:[{operation:'delete',indirect:false,old:[1n]}]},
    {name:'child',primaryKey:[1,0,0],changes:[{operation:'delete',indirect:false,old:[1n,1n,0n]}]},
  ]);
  try{await assert.rejects(applyChangeset(db,bytes,{tables,deliveryId:'omit',foreignKeys:'defer',onConflict:c=>{assert.equal(c.kind,'data');conflicts++;return 'omit';}}),code('VIOLATION'));
    assert.equal(conflicts,1);assert.deepEqual(db.rows('SELECT * FROM parent'),[[1n]]);assert.deepEqual(db.rows('SELECT * FROM child'),[[1n,1n,99n]]);noInbox(db);
  }finally{db.close();}
});
test('foreign_keys=OFF is refused, never silently turned on or treated as validation',async()=>{
  const db=target();db.db.exec('PRAGMA foreign_keys=OFF');
  try{await assert.rejects(applyChangeset(db,payload,{tables,foreignKeys:'defer'}),code('STATE'));assert.deepEqual(flags(db),[0n,0n]);assert.deepEqual(contents(db),[[],[]]);
  }finally{db.close();}
});
for(const ns of ['main','temp','aux']) {
  test(`${ns}: pre-existing deferred violations retain their enforcement state`,async()=>{
    const db=target();let serial=0;
    if(ns==='aux')db.db.exec("ATTACH ':memory:' AS aux");
    db.db.exec(`CREATE TABLE ${ns}.p(id PRIMARY KEY);CREATE TABLE ${ns}.bad(id PRIMARY KEY,p REFERENCES p(id));BEGIN;PRAGMA defer_foreign_keys=ON;INSERT INTO ${ns}.bad VALUES(1,9)`);
    const owner={transaction:async work=>{const id=`nested_${++serial}`;db.db.exec(`SAVEPOINT ${id}`);try{const result=await work(db);db.db.exec(`RELEASE ${id}`);return result;}catch(e){db.db.exec(`ROLLBACK TO ${id};RELEASE ${id}`);throw e;}}};
    try{
      await assert.rejects(applyChangeset(owner,payload,{tables,foreignKeys:'defer'}),code('VIOLATION'));
      assert.equal(db.rows('PRAGMA defer_foreign_keys')[0][0],1n);
      assert.throws(()=>db.db.exec('COMMIT'),/FOREIGN KEY/);db.db.exec('ROLLBACK');assert.deepEqual(contents(db),[[],[]]);
    }finally{if(db.db.isTransaction)db.db.exec('ROLLBACK');db.close();}
  });
}
for(const prior of [0,1]) {
  test(`nested savepoint preserves prior deferral=${prior} and provisional rows roll back with outer owner`,async()=>{
    const db=target();db.db.exec(`BEGIN;PRAGMA defer_foreign_keys=${prior}`);
    const owner={transaction:async work=>{db.db.exec('SAVEPOINT nested');try{const result=await work(db);db.db.exec('RELEASE nested');return result;}catch(e){db.db.exec('ROLLBACK TO nested;RELEASE nested');throw e;}}};
    try{await applyChangeset(owner,payload,{tables,deliveryId:'nested',foreignKeys:'defer'});assert.deepEqual(flags(db),[1n,BigInt(prior)]);
      db.db.exec('ROLLBACK');assert.deepEqual(contents(db),[[],[]]);noInbox(db);
    }finally{if(db.db.isTransaction)db.db.exec('ROLLBACK');db.close();}
  });
}
test('explicitly wrapped owners compose idempotently with the policy',async()=>{
  const db=target();
  try{await applyChangeset(withDeferredForeignKeys(db),payload,{tables,foreignKeys:'defer'});
    assert.equal(db.statements.filter(sql=>sql==='PRAGMA defer_foreign_keys=ON').length,1);
    assert.equal(db.statements.filter(sql=>sql==='PRAGMA defer_foreign_keys=OFF').length,1);
  }finally{db.close();}
});
test('options are captured before async owner admission',async()=>{
  const db=target(),opts={tables,foreignKeys:'defer'};
  const owner={transaction:async(work,controls)=>{opts.foreignKeys='wrong';return db.transaction(work,controls);}};
  try{assert.equal((await applyChangeset(owner,payload,opts)).applied,2);}finally{db.close();}
});
test('restoration errors retain cleanup evidence and roll back receipt and rows',async()=>{
  const db=target(),failure=new Error('cleanup failed');db.before=(_,sql)=>{if(sql==='PRAGMA defer_foreign_keys=OFF')throw failure;};
  try{await assert.rejects(applyChangeset(db,payload,{tables,deliveryId:'cleanup',foreignKeys:'defer'}),e=>e.code==='ERR_FSQLITE_FOREIGN_KEY_STATE'&&e.cleanupErrors[0]===failure);
    assert.deepEqual(contents(db),[[],[]]);noInbox(db);db.before=null;assert.deepEqual(flags(db),[1n,0n]);
  }finally{db.close();}
});
for(const encoding of ['UTF-8','UTF-16le','UTF-16be']) for(const ordered of [false,true]) {
  test(`${encoding} ${ordered?'ordered':'unordered'}: cross-chunk cyclic bootstrap defers once around complete install`,async()=>{
    const db=target(encoding);const m=await manifest();let confirmations=0;
    const r=receiver(db,{...(ordered?{orderedSourceId:'source:epoch'}:{}),confirmCommit:async()=>{confirmations++;assert.equal(db.active,false);assert.deepEqual(flags(db),[1n,0n]);}});
    try{
      await stage(r,m);assert.deepEqual(contents(db),[[],[]]);assert.equal(db.statements.some(sql=>sql==='PRAGMA defer_foreign_keys=ON'),false);
      const receipt=await r.install(m);assert.equal(receipt.confirmed,true);assert.equal(confirmations,1);assert.deepEqual(contents(db),[[[1n,1n]],[[1n,1n]]]);
      assert.equal(db.statements.filter(sql=>sql==='PRAGMA defer_foreign_keys=ON').length,1,'Do not defer/check separately per chunk');
      if(ordered){const order=new ChangesetOrder(db,{receiverId:'replica',sourceId:'source:epoch'});assert.equal((await order.head()).sequence,2n);
        const delta=encodeChangeset([{name:'child',primaryKey:[1,0],changes:[{operation:'update',indirect:false,old:[1n,1n],new:[undefined,1n]}]}]);
        assert.equal((await order.apply({sequence:3n,deliveryId:'source:next',sha256:sha256(delta),changeset:delta},(inside,bytes)=>applyChangeset(inside,bytes,{tables,foreignKeys:'defer'}))).sequence,3n);
        assert.equal((await r.install(m)).replayed,true);assert.equal((await order.head()).sequence,3n);
      }else{assert.equal((await r.install(m)).replayed,true);}
      assert.equal(confirmations,2);assert.equal(db.rows("SELECT sum(length(payload)) FROM __fsqlite_bootstrap_chunks")[0][0],0n);
    }finally{db.close();}
  });
}
test('bootstrap default still rejects the cyclic seed and retains staging for explicit policy retry',async()=>{
  const db=target(),m=await manifest();let confirmations=0;
  try{
    const legacy=receiver(db,{foreignKeys:undefined,confirmCommit:async()=>{confirmations++;}});await stage(legacy,m);
    await assert.rejects(legacy.install(m),/FOREIGN KEY/);assert.deepEqual(contents(db),[[],[]]);assert.equal(confirmations,0);
    assert.equal((await legacy.status(m)).installed,false);assert.equal((await receiver(db).install(m)).confirmed,true);
  }finally{db.close();}
});
test('incomplete final FK graph rolls back installed marker and body reclamation',async()=>{
  const db=target(),bytes=cycles[0];let confirmations=0;
  const m=await createBootstrapManifest({receiverId:'replica',deliveryId:'missing',tables,chunks:1,changes:1,byteLength:bytes.length},async()=>bytes);
  const r=receiver(db,{confirmCommit:async()=>{confirmations++;}});
  try{await r.stage(m,0,bytes);await assert.rejects(r.install(m),code('VIOLATION'));assert.equal(confirmations,0);assert.deepEqual(contents(db),[[],[]]);
    assert.equal((await r.status(m)).installed,false);assert.equal(db.rows('SELECT sum(length(payload)) FROM __fsqlite_bootstrap_chunks')[0][0],BigInt(bytes.length));
  }finally{db.close();}
});
test('generated-column FKs recompute within the same deferred install',async()=>{
  const db=new SqliteTarget(':memory:','CREATE TABLE parent(id INTEGER PRIMARY KEY,child REFERENCES child(id));CREATE TABLE child(id INTEGER PRIMARY KEY,base, parent GENERATED ALWAYS AS(base) STORED REFERENCES parent(id))');
  const m=await manifest(),r=receiver(db,{generatedColumns:'recompute'});
  try{await stage(r,m);assert.equal((await r.install(m)).confirmed,true);assert.deepEqual(db.rows('SELECT id,base,parent FROM child'),[[1n,1n,1n]]);assert.deepEqual(flags(db),[1n,0n]);}
  finally{db.close();}
});
test('replay rechecks FK state before reconfirmation rather than trusting installed status',async()=>{
  const db=target(),m=await manifest();let confirmations=0;
  const r=receiver(db,{confirmCommit:async()=>{confirmations++;}});
  try{await stage(r,m);await r.install(m);db.db.exec('PRAGMA foreign_keys=OFF;UPDATE child SET parent=8;PRAGMA foreign_keys=ON');
    await assert.rejects(r.install(m),code('VIOLATION'));assert.equal(confirmations,1);assert.equal((await r.status(m)).installed,true);
  }finally{db.close();}
});
for(const cut of ['row','validation']){
  test(`cancel during ${cut} restores policy and retains the uninstalled seed`,async()=>{
    const db=target(),m=await manifest(),controller=new AbortController();let checks=0,confirmations=0;
    const r=receiver(db,{confirmCommit:async()=>{confirmations++;}});await stage(r,m);
    db.after=(_,sql)=>{if((cut==='row'&&sql.startsWith('INSERT OR ABORT INTO main."child"'))||(cut==='validation'&&sql.includes('pragma_foreign_key_check')&&++checks===3))controller.abort('stop');};
    try{await assert.rejects(r.install(m,{signal:controller.signal}),e=>/CANCELLED/.test(e.code));assert.deepEqual(contents(db),[[],[]]);assert.equal(confirmations,0);assert.deepEqual(flags(db),[1n,0n]);
      db.after=null;assert.equal((await r.status(m)).installed,false);await r.install(m);assert.equal(confirmations,1);
    }finally{db.close();}
  });
}
test('real HTTP cyclic bootstrap exposes no prefix and retries a lost install response',async()=>{
  const directory=mkdtempSync(join(tmpdir(),'fsqlite-fk-http-')),path=join(directory,'receiver.db');
  const db=target('UTF-8',path);db.db.exec('PRAGMA journal_mode=WAL');const observer=new SqliteTarget(path);
  const m=await manifest();let checked=false,confirmations=0,lost=false;
  const r=receiver(db,{confirmCommit:async()=>{confirmations++;}});
  db.after=(_,sql)=>{if(!checked&&sql.startsWith('INSERT OR ABORT INTO main."child"')){checked=true;assert.deepEqual(contents(observer),[[],[]]);}};
  const handler=createBootstrapHttpHandler(r,{authorize:()=>true});
  const host=await serveBootstrap(handler,async(request,response)=>{if(request.headers.get('x-fsqlite-bootstrap-action')==='install'&&!lost&&response.status===200){lost=true;return 'drop';}});
  try{
    const transport=createBootstrapHttpTransport(host.url,{allowInsecureLoopback:true});
    for(let i=0;i<2;i++)await transport.stage(m,i,cycles[i]);
    await assert.rejects(transport.install(m));assert.equal(checked,true);assert.deepEqual(contents(observer),[[[1n,1n]],[[1n,1n]]]);
    assert.equal((await transport.status(m)).installed,true);assert.equal((await transport.install(m)).replayed,true);assert.equal(confirmations,2);
  }finally{await host.close();observer.close();db.close();}
});

for(const journal of ['WAL','DELETE']) for(const cut of ['first-row','before-commit','after-commit','confirmation']) {
  test(`${journal}: SIGKILL at ${cut} preserves atomic cyclic installation and recoverable staging`,async()=>{
    const directory=mkdtempSync(join(tmpdir(),'fsqlite-deferred-kill-')),path=join(directory,'receiver.db'),m=await manifest();
    const initial=target('UTF-8',path);initial.db.exec(`PRAGMA journal_mode=${journal}`);
    try{await stage(receiver(initial),m);}finally{initial.close();}
    const outcome=await new Promise((resolve,reject)=>{
      const child=spawn(process.execPath,['--experimental-transform-types','--experimental-loader=./packages/sdk/tests/helpers/production-source-loader.mjs','packages/sdk/tests/helpers/deferred-bootstrap-child.mjs',path,cut],{stdio:['ignore','pipe','pipe']});
      let output='',errors='',expired=false;
      const timer=setTimeout(()=>{expired=true;child.kill('SIGKILL');},10000);
      child.stdout.on('data',data=>{output=(output+data).slice(-12000);});child.stderr.on('data',data=>{errors=(errors+data).slice(-12000);});
      child.on('error',error=>{clearTimeout(timer);reject(error);});
      child.on('exit',(code,signal)=>{clearTimeout(timer);resolve({code,signal,output,errors,expired});});
    });
    assert.equal(outcome.expired,false,outcome.errors);assert.equal(outcome.signal,'SIGKILL',outcome.errors);assert.match(outcome.output,new RegExp(`CUT:${cut}`));
    const reopened=new SqliteTarget(path),r=receiver(reopened);const committed=['after-commit','confirmation'].includes(cut);
    try{
      assert.equal((await r.status(m)).installed,committed);
      assert.deepEqual(contents(reopened),committed?[[[1n,1n]],[[1n,1n]]]:[[],[]]);
      assert.equal(reopened.rows('SELECT sum(length(payload)) FROM __fsqlite_bootstrap_chunks')[0][0],committed?0n:BigInt(payload.length));
      const result=await r.install(m);assert.equal(result.replayed,committed);assert.equal(result.confirmed,true);
      assert.deepEqual(contents(reopened),[[[1n,1n]],[[1n,1n]]]);assert.equal(reopened.rows('PRAGMA foreign_key_check').length,0);
      assert.equal(reopened.statements.some(sql=>sql.startsWith('INSERT OR ABORT INTO main."child"')),!committed);
    }finally{reopened.close();}
  });
}
