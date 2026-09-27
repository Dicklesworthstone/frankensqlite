import assert from 'node:assert/strict';
import { test } from 'node:test';
import { setImmediate as turn } from 'node:timers/promises';
import { DatabaseSync } from 'node:sqlite';
import { applyChangeset } from '../src/changeset-apply.ts';
import { decodeRebaseInfo } from '../src/changeset-codec.ts';
import { SqliteTarget } from './helpers/production-sqlite-target.mjs';

const schema = 'CREATE TABLE t(id INTEGER PRIMARY KEY,value TEXT); CREATE TABLE journal(id INTEGER PRIMARY KEY, data BLOB);';
const gate = () => { let resolve; const promise = new Promise(r => { resolve = r; }); return { promise, resolve }; };
const settledTurns = async () => { for (let i=0;i<8;i++) await turn(); };
function nativeUpdate() {
  const db = new DatabaseSync(':memory:'); db.exec(schema + "INSERT INTO t VALUES(1,'before')");
  const session = db.createSession({ table: 't' });
  try { db.exec("UPDATE t SET value='remote' WHERE id=1"); return new Uint8Array(session.changeset()); }
  finally { session.close(); db.close(); }
}
const payload = nativeUpdate();
function target(encoding='UTF-8') {
  return new SqliteTarget(':memory:', `PRAGMA encoding='${encoding}';` + schema + "INSERT INTO t VALUES(1,'local')");
}
const options = onRebase => ({ tables:['t'], deliveryId:'peer:1', onConflict:()=> 'replace', onRebase });
function rolledBack(db) {
  assert.deepEqual(db.rows(), [[1n,'local']]);
  assert.equal(db.rows('SELECT count(*) FROM journal')[0][0], 0n);
  assert.equal(db.rows("SELECT count(*) FROM sqlite_schema WHERE name='__fsqlite_changeset_receipts'")[0][0],0n);
}

for (const method of ['execute','query']) {
  test(`${method}: unawaited rebase journal SQL drains before receipt publication or COMMIT`, async () => {
    const db=target(), entered=gate(), release=gate(); let pending, finished=false, operation;
    const sql=method==='execute' ? 'INSERT INTO journal VALUES(1,?)' : 'INSERT INTO journal VALUES(1,?) RETURNING id';
    db.before=async (kind,text)=> { if(kind===method && text===sql) { entered.resolve(); await release.promise; } };
    try {
      pending=applyChangeset(db,payload,options(tx=> { operation=tx[method](sql,[new Uint8Array([1])]); }));
      void pending.then(()=>{finished=true;},()=>{finished=true;});
      await entered.promise; await settledTurns();
      const prematurelyFinished=finished, active=db.active;
      const published=db.statements.some(text=>text.startsWith('INSERT OR ABORT INTO main."__fsqlite_changeset_receipts"'));
      release.resolve(); await pending; await operation;
      assert.equal(prematurelyFinished,false); assert.equal(active,true); assert.equal(published,false);
      assert.deepEqual(db.rows(),[[1n,'remote']]); assert.equal(db.rows('SELECT count(*) FROM journal')[0][0],1n);
    } finally { release.resolve(); await pending?.catch(()=>{}); await operation?.catch(()=>{}); db.close(); }
  });
  test(`${method}: a caught SQL failure still rolls back rows, hook records and inbox receipt`,async()=>{
    const db=target();
    try {
      await assert.rejects(applyChangeset(db,payload,options(async tx=> {
        await tx.execute('INSERT INTO journal VALUES(1,?)',[new Uint8Array([1])]);
        await tx[method]('INSERT INTO journal VALUES(1,?)',[new Uint8Array([2])]).catch(()=>{});
      })),/UNIQUE/);
      rolledBack(db);
    } finally { db.close(); }
  });
  test(`${method}: retained hook executors cannot enter SQL after successful completion`,async()=>{
    const db=target();let saved;
    try {
      await applyChangeset(db,payload,options(tx=> { saved=tx; }));
      const before=db.statements.length;
      await assert.rejects(saved[method]('INSERT INTO journal VALUES(1,?)',[new Uint8Array([1])]),/scope has ended/);
      assert.equal(db.statements.length,before); assert.equal(db.rows('SELECT count(*) FROM journal')[0][0],0n);
    } finally { db.close(); }
  });
  test(`${method}: retained hook executors cannot contaminate a later transaction`,async()=>{
    const db=target();let saved;
    try {
      await applyChangeset(db,payload,options(tx=>{saved=tx;}));
      await db.transaction(async inside=>{
        await inside.execute("UPDATE t SET value='later' WHERE id=1");
        await assert.rejects(saved[method]('INSERT INTO journal VALUES(1,?)',[new Uint8Array([1])]),/scope has ended/);
      });
      assert.deepEqual(db.rows(),[[1n,'later']]);assert.equal(db.rows('SELECT count(*) FROM journal')[0][0],0n);
    }finally{db.close();}
  });
}

for(const phase of ['throw','cancel','timeout']) {
  test(`${phase}: admitted delayed hook SQL drains before rollback`,async()=>{
    const db=target(), entered=gate(), release=gate(), controller=new AbortController();
    const marker=new Error('hook failure');let pending,operation,finished=false;
    db.before=async(kind,sql)=>{if(kind==='execute'&&sql==='INSERT INTO journal VALUES(1,?)'){entered.resolve();await release.promise;}};
    try{
      pending=applyChangeset(db,payload,{...options(tx=>{
        operation=tx.execute('INSERT INTO journal VALUES(1,?)',[new Uint8Array([3])]);
        void operation.catch(()=>{});
        if(phase==='throw')throw marker;
      }),...(phase==='timeout'?{timeoutMs:150}:{}),signal:controller.signal});
      void pending.then(()=>{finished=true;},()=>{finished=true;});
      await entered.promise;
      if(phase==='cancel')controller.abort(marker);
      if(phase==='timeout')await new Promise(r=>setTimeout(r,180));
      await settledTurns();const early=finished, active=db.active;
      release.resolve();
      await assert.rejects(pending,e=>phase==='throw'?e===marker:e.code===`ERR_FSQLITE_CHANGESET_${phase==='cancel'?'CANCELLED':'TIMEOUT'}`);
      await operation.catch(()=>{});assert.equal(early,false);assert.equal(active,true);rolledBack(db);
    }finally{release.resolve();await pending?.catch(()=>{});await operation?.catch(()=>{});db.close();}
  });
}

test('callback error retains precedence while multiple delayed SQL failures are drained',async()=>{
  const db=target(),release=gate(),entered=gate(),marker={hook:'primary'};let pending;const tasks=[];
  db.before=async(_,sql)=>{if(sql==='INSERT INTO missing VALUES(1)'){entered.resolve();await release.promise;}};
  try{
    pending=applyChangeset(db,payload,options(tx=>{for(let i=0;i<3;i++){const task=tx.execute('INSERT INTO missing VALUES(1)');void task.catch(()=>{});tasks.push(task);}throw marker;}));
    void pending.catch(()=>{});await entered.promise;await settledTurns();release.resolve();
    await assert.rejects(pending,e=>e===marker);await Promise.allSettled(tasks);rolledBack(db);
  }finally{release.resolve();await pending?.catch(()=>{});await Promise.allSettled(tasks);db.close();}
});
for(const encoding of ['UTF-8','UTF-16le','UTF-16be']) {
  test(`${encoding}: native conflict decisions and exact replay skip the hook`,async()=>{
    const db=target(encoding);let calls=0,info;
    try{
      const opts=options(async(tx,bytes)=>{calls++;info=bytes;await tx.execute('INSERT INTO journal VALUES(1,?)',[bytes]);});
      assert.deepEqual(await applyChangeset(db,payload,opts),{applied:1,omitted:0,replayed:false});
      assert.equal(decodeRebaseInfo(info)[0].changes[0].replace,true);
      assert.equal((await applyChangeset(db,payload,opts)).replayed,true);assert.equal(calls,1);
      assert.deepEqual(db.rows('SELECT data FROM journal')[0][0],info);
    }finally{db.close();}
  });
}
for(const failure of [undefined,null,0,'sql failure']) {
  test(`falsy/non-Error admitted rejection ${String(failure)} cannot become a successful receipt`,async()=>{
    const db=target();
    db.before=async(_,sql)=>{if(sql==='SELECT broken')throw failure;};
    try{
      let caught=false;
      try{await applyChangeset(db,payload,options(async tx=>{await tx.query('SELECT broken').catch(()=>{});}));}
      catch(error){caught=true;assert.equal(error,failure);}
      assert.equal(caught,true);rolledBack(db);
    }finally{db.close();}
  });
}

test('lost COMMIT response reuses retained rows and hook output without invoking hook again',async()=>{
  const db=target();let calls=0;const marker=new Error('lost response');
  db.afterCommit=()=>{db.afterCommit=null;throw marker;};
  const opts=options(async tx=>{calls++;await tx.execute('INSERT INTO journal VALUES(1,?)',[new Uint8Array([5])]);});
  try{await assert.rejects(applyChangeset(db,payload,opts),e=>e===marker);
    assert.equal((await applyChangeset(db,payload,opts)).replayed,true);assert.equal(calls,1);assert.equal(db.rows('SELECT count(*) FROM journal')[0][0],1n);
  }finally{db.close();}
});
