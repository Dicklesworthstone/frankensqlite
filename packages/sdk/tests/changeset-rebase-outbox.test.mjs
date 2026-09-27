import assert from 'node:assert/strict';
import { test } from 'node:test';
import { ChangesetRebaseJournal } from '../src/changeset-rebase-journal.ts';
import { ChangesetOutbox } from '../src/changeset-outbox.ts';
import { applyChangeset } from '../src/changeset-apply.ts';
import { SqliteTarget } from './helpers/production-sqlite-target.mjs';

const schema = 'CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);';
const opts = { tables: ['t'] };
async function setup(encoding = 'UTF-8', mode = 'omit') {
  const source = new SqliteTarget(':memory:', `PRAGMA encoding='${encoding}';` + schema);
  const remote = new SqliteTarget(':memory:', `PRAGMA encoding='${encoding}';` + schema);
  const journal = new ChangesetRebaseJournal(source, { journalId: 'source:conflicts' });
  let calls = 0;
  const local = await journal.captureLocal('edit-1', tx => { calls++; return tx.execute("INSERT INTO t VALUES(1,'local')"); }, opts);
  const session = remote.db.createSession(); remote.db.exec("INSERT INTO t VALUES(1,'remote')");
  const bytes = new Uint8Array(session.changeset()); session.close();
  await journal.apply(bytes, { ...opts, deliveryId: 'remote:1', onConflict: () => mode });
  return { source, remote, journal, local, calls: () => calls, outbox: new ChangesetOutbox(source) };
}
for (const encoding of ['UTF-8', 'UTF-16le', 'UTF-16be']) {
  test(`${encoding}: original -> committed conflict -> retained rebased output converges`, async () => {
    const p = await setup(encoding);
    try {
      const expected = await p.journal.rebaseLocal('edit-1');
      const result = await p.journal.enqueueLocal('edit-1', opts);
      assert.equal(result.replayed, false);
      assert.equal(result.delivery.sequence, 1n);
      assert.equal(result.throughBookmark.position, 1);
      const outgoing = await p.outbox.read(result.delivery.deliveryId);
      assert.deepEqual(outgoing.changeset, expected.changeset);
      assert.equal(p.remote.db.applyChangeset(outgoing.changeset), true);
      assert.deepEqual(p.source.rows(), p.remote.rows());
      assert.deepEqual(await p.journal.readLocal('edit-1'), p.local.record);
      assert.equal(p.calls(), 1);
      assert.equal((await p.journal.enqueueLocal('edit-1', opts)).replayed, true);
    } finally { p.source.close(); p.remote.close(); }
  });
}
test('lost publication response retains original output after newer remote decisions', async () => {
  const p = await setup();
  try {
    p.source.afterCommit = () => { throw new Error('lost commit response'); };
    await assert.rejects(p.journal.enqueueLocal('edit-1', opts), /lost commit response/);
    p.source.afterCommit = null;
    const [before] = await p.outbox.pending();
    const bytesBefore = (await p.outbox.read(before.deliveryId)).changeset;
    const session = p.remote.db.createSession(); p.remote.db.exec("UPDATE t SET v='remote-new' WHERE id=1");
    const wire = new Uint8Array(session.changeset()); session.close();
    await p.journal.apply(wire, { ...opts, deliveryId: 'remote:2', onConflict: () => 'replace' });
    assert.notDeepEqual((await p.journal.rebaseLocal('edit-1')).changeset, bytesBefore);
    const retry = await p.journal.enqueueLocal('edit-1', opts);
    assert.equal(retry.replayed, true);
    assert.equal(retry.throughBookmark.position, 1);
    assert.deepEqual((await p.outbox.read(before.deliveryId)).changeset, bytesBefore);
    assert.equal((await p.outbox.pending()).length, 1);
    assert.equal(p.calls(), 1);
  } finally { p.source.close(); p.remote.close(); }
});
test('empty rebased result is retained as a real delivery and can be acknowledged', async () => {
  const p = await setup('UTF-8', 'replace');
  try {
    const result = await p.journal.enqueueLocal('edit-1', opts);
    assert.equal(result.delivery.changes, 0); assert.equal(result.delivery.byteLength, 0);
    const outgoing = await p.outbox.read(result.delivery.deliveryId);
    assert.deepEqual(outgoing.changeset, new Uint8Array());
    assert.equal((await applyChangeset(p.remote, outgoing.changeset, { ...opts, deliveryId: result.delivery.deliveryId })).applied, 0);
    assert.equal(await p.outbox.acknowledge(result.delivery.deliveryId, result.delivery.sha256), true);
    const again = await p.journal.enqueueLocal('edit-1', opts);
    assert.equal(again.replayed, true); assert.equal(again.delivery.acknowledged, true);
    assert.equal((await p.outbox.read(result.delivery.deliveryId)).changeset, null);
  } finally { p.source.close(); p.remote.close(); }
});

import { createHash } from 'node:crypto';
import { mkdtempSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { spawn } from 'node:child_process';
import { ChangesetFanout } from '../src/changeset-fanout.ts';
const journalId = 'source:conflicts';
const deliveryIdFor = (journal, id) => 'fsqlite-rebase-outbox-v1:' + createHash('sha256').update(JSON.stringify([journal,id])).digest('hex');
const localTable = '__fsqlite_rebase_journal_locals';
const entriesTable = '__fsqlite_rebase_journal_entries';
const outboxTable = '__fsqlite_changeset_outbox';
const errorCode = code => error => error.code === code;
const historyError = errorCode('ERR_FSQLITE_REBASE_JOURNAL_HISTORY');
const limitError = errorCode('ERR_FSQLITE_REBASE_JOURNAL_LIMIT');
const gate = () => { let resolve; const promise = new Promise(r => { resolve = r; }); return { promise, resolve }; };
const outputCount = t => t.rows("SELECT count(*) FROM sqlite_schema WHERE name='__fsqlite_changeset_outbox'")[0][0] === 0n ? 0n : t.rows(`SELECT count(*) FROM ${outboxTable}`)[0][0];

for (const indirect of [false,true]) test(`net-zero original retains verified ${indirect ? 'indirect' : 'direct'} scope`, async () => {
  const t = new SqliteTarget(':memory:', schema + 'CREATE TABLE empty(id PRIMARY KEY);');
  try {
    const j = new ChangesetRebaseJournal(t,{journalId});
    await j.captureLocal('zero',() => {},{tables:['t','empty'],indirect});
    const result = await j.enqueueLocal('zero',{tables:['EMPTY','T']});
    assert.equal(result.delivery.byteLength,0);
    assert.equal(result.afterBookmark.position,0); assert.equal(result.throughBookmark.position,0);
    const scope = JSON.parse(t.rows(`SELECT scope FROM ${outboxTable}`)[0][0]);
    assert.equal(scope.indirect,indirect); assert.deepEqual(scope.tables,['empty','t']);
    assert.equal((await j.enqueueLocal('zero',{tables:['t','empty']})).replayed,true);
    await assert.rejects(j.enqueueLocal('zero',opts),historyError);
  } finally { t.close(); }
});

test('numeric and bookmark bounds are pinned; retries cannot select a different prefix', async () => {
  const p = await setup();
  try {
    const tip = await p.journal.bookmark();
    const first = await p.journal.enqueueLocal('edit-1',{...opts,through:tip});
    assert.equal((await p.journal.enqueueLocal('edit-1',{...opts,through:1})).replayed,true);
    await assert.rejects(p.journal.enqueueLocal('edit-1',{...opts,through:0}),historyError);
    await assert.rejects(p.journal.enqueueLocal('edit-1',{...opts,through:{...tip,sha256:'0'.repeat(64)}}),historyError);
    assert.deepEqual((await p.journal.enqueueLocal('edit-1',opts)).delivery,first.delivery);
  } finally { p.source.close(); p.remote.close(); }
});
test('first publication may explicitly choose the original basis instead of the current tip', async () => {
  const p = await setup();
  try {
    const r = await p.journal.enqueueLocal('edit-1',{...opts,through:p.local.record.basis});
    assert.equal(r.throughBookmark.position,0);
    assert.deepEqual((await p.outbox.read(r.delivery.deliveryId)).changeset,p.local.record.changeset);
    assert.equal((await p.journal.enqueueLocal('edit-1',opts)).throughBookmark.position,0);
  } finally { p.source.close(); p.remote.close(); }
});
for (const through of [2,{format:'fsqlite-rebase-bookmark-v1',journalId,position:1,sha256:'0'.repeat(64)}])
  test(`unavailable or mismatched first history boundary rejects without publication: ${typeof through}`, async () => {
    const p=await setup();
    try {
      await assert.rejects(p.journal.enqueueLocal('edit-1',{...opts,through}));
      assert.equal(outputCount(p.source),0n); assert.deepEqual(await p.journal.readLocal('edit-1'),p.local.record);
    } finally { p.source.close(); p.remote.close(); }
  });

test('operation identities derive distinct source-qualified delivery IDs, stable across instances', async () => {
  const t=new SqliteTarget(':memory:',schema);
  try {
    const j=new ChangesetRebaseJournal(t,{journalId});
    await j.captureLocal('one',tx=>tx.execute("INSERT INTO t VALUES(1,'one')"),opts);
    await j.captureLocal('two',tx=>tx.execute("INSERT INTO t VALUES(2,'two')"),opts);
    const a=await j.enqueueLocal('one',opts), b=await j.enqueueLocal('two',opts);
    assert.equal(a.delivery.deliveryId,deliveryIdFor(journalId,'one'));
    assert.notEqual(a.delivery.deliveryId,b.delivery.deliveryId); assert.equal(b.delivery.sequence,2n);
    const reopened=new ChangesetRebaseJournal(t,{journalId});
    assert.deepEqual((await reopened.enqueueLocal('one',opts)).delivery,a.delivery);
    const other=new ChangesetRebaseJournal(t,{journalId:'other-source:conflicts'});
    await other.captureLocal('one',() => {},opts);
    assert.notEqual((await other.enqueueLocal('one',opts)).delivery.deliveryId,a.delivery.deliveryId);
  } finally { t.close(); }
});

test('caller mutation during asynchronous admission cannot change tables or a pinned bookmark', async () => {
  const p=await setup(), entered=gate(), release=gate();
  try {
    const tables=['t'], through={...await p.journal.bookmark()}, control={tables,through};
    const wrapped={ transaction: async(work,options)=>{entered.resolve(); await release.promise; return p.source.transaction(work,options);} };
    const j=new ChangesetRebaseJournal(wrapped,{journalId});
    const call=j.enqueueLocal('edit-1',control); await entered.promise;
    tables[0]='not_t'; through.position=0; through.sha256='0'.repeat(64); release.resolve();
    const r=await call; assert.equal(r.throughBookmark.position,1); assert.equal(r.delivery.changes,1);
  } finally { release.resolve(); p.source.close(); p.remote.close(); }
});

for (const options of [undefined,null,{}, {tables:[]},{tables:['t','T']},{tables:['sqlite_schema']},
  {tables:['__fsqlite_changeset_outbox']},{tables:[42]},{tables:['a\0b']},{tables:['a'.repeat(1025)]},
  {...opts,after:0},{...opts,through:-1},{...opts,through:100001},{...opts,through:{position:0}},
  {...opts,maxEntries:0},{...opts,maxPayloadBytes:0},{...opts,timeoutMs:0},{...opts,signal:{}}])
  test(`invalid publication options reject before transaction admission: ${JSON.stringify(options)?.slice(0,70)}`, async()=>{
    let entered=0;
    const j=new ChangesetRebaseJournal({transaction(){entered++; throw new Error('entered');}},{journalId});
    await assert.rejects(j.enqueueLocal('op',options)); assert.equal(entered,0);
  });

test('missing original is not recaptured from application rows',async()=>{
  const t=new SqliteTarget(':memory:',schema+"INSERT INTO t VALUES(1,'present')");
  try {
    const j=new ChangesetRebaseJournal(t,{journalId});
    await assert.rejects(j.enqueueLocal('missing',opts),errorCode('ERR_FSQLITE_REBASE_JOURNAL_MISSING'));
    assert.equal(outputCount(t),0n); assert.deepEqual(t.rows(),[[1n,'present']]);
  } finally { t.close(); }
});
for (const tables of [['other'],['t','extra']]) test(`wrong original scope rejects: ${tables}`,async()=>{
  const p=await setup();
  try {await assert.rejects(p.journal.enqueueLocal('edit-1',{tables}),historyError); assert.equal(outputCount(p.source),0n);}
  finally {p.source.close();p.remote.close();}
});

test('entry pressure rejects before loading the original or rebase history; a retained retry still works',async()=>{
  const p=await setup();
  try {
    const first=await p.journal.enqueueLocal('edit-1',{...opts,maxEntries:1});
    await p.journal.captureLocal('edit-2',tx=>tx.execute("INSERT INTO t VALUES(2,'next')"),opts);
    p.source.statements.length=0;
    await assert.rejects(p.journal.enqueueLocal('edit-2',{...opts,maxEntries:1}),limitError);
    assert(!p.source.statements.some(sql=>sql.includes(`FROM main."${localTable}"`)||sql.includes(`FROM main."${entriesTable}"`)));
    assert.deepEqual((await p.journal.enqueueLocal('edit-1',{...opts,maxEntries:1})).delivery,first.delivery);
  } finally {p.source.close();p.remote.close();}
});
test('outbox byte limit is enforced before publication, with an exact-fit success',async()=>{
  const p=await setup();
  try {
    const bytes=(await p.journal.rebaseLocal('edit-1')).changeset.length;
    await assert.rejects(p.journal.enqueueLocal('edit-1',{...opts,maxPayloadBytes:bytes-1}),limitError);
    assert.equal(outputCount(p.source),0n);
    assert.equal((await p.journal.enqueueLocal('edit-1',{...opts,maxPayloadBytes:bytes})).delivery.byteLength,bytes);
  } finally {p.source.close();p.remote.close();}
});

test('ordinary outbox recording cannot claim the derived publication identity',async()=>{
  const p=await setup();
  try {
    await p.outbox.record(()=>{}, {tables:['t'],deliveryId:deliveryIdFor(journalId,'edit-1')});
    await assert.rejects(p.journal.enqueueLocal('edit-1',opts),historyError);
    assert.equal(outputCount(p.source),1n);
  } finally {p.source.close();p.remote.close();}
});
for (const change of [
  scope=>{scope.rebasedLocal.throughBookmark.position=0;},
  scope=>{scope.rebasedLocal.throughBookmark.sha256='0'.repeat(64);},
  scope=>{scope.rebasedLocal.recordSha256='0'.repeat(64);},
  scope=>{scope.rebasedLocal.seal='0'.repeat(64);},
  scope=>{scope.rebasedLocal.operationId='different';},
  scope=>{scope.tables=['other'];},
]) test(`damaged retained publication binding rejects: ${change}`,async()=>{
  const p=await setup();
  try {
    const first=await p.journal.enqueueLocal('edit-1',opts);
    const scope=JSON.parse(p.source.rows(`SELECT scope FROM ${outboxTable}`)[0][0]); change(scope);
    await p.source.execute(`UPDATE ${outboxTable} SET scope=?`,[JSON.stringify(scope)]);
    await assert.rejects(p.journal.enqueueLocal('edit-1',opts));
    assert.equal(outputCount(p.source),1n);
    assert.equal(p.source.rows(`SELECT sha256 FROM ${outboxTable}`)[0][0],first.delivery.sha256);
  } finally {p.source.close();p.remote.close();}
});
test('same-length pending payload corruption rejects, never regenerates from history',async()=>{
  const p=await setup();
  try {
    await p.journal.enqueueLocal('edit-1',opts);
    await p.source.execute(`UPDATE ${outboxTable} SET payload=zeroblob(byte_length)`);
    await assert.rejects(p.journal.enqueueLocal('edit-1',opts),errorCode('ERR_FSQLITE_OUTBOX_CORRUPT'));
    assert.equal(outputCount(p.source),1n);
  } finally {p.source.close();p.remote.close();}
});
for (const corruption of [
  `UPDATE ${localTable} SET changeset=zeroblob(byte_length)`,
  `UPDATE ${localTable} SET basis_sha256='${'0'.repeat(64)}'`,
  `DELETE FROM ${entriesTable}`,
  'DELETE FROM __fsqlite_changeset_receipts',
]) test(`invalid original/history cannot create an outgoing delivery: ${corruption.slice(0,70)}`,async()=>{
  const p=await setup();
  try {
    await p.source.execute(corruption);
    await assert.rejects(p.journal.enqueueLocal('edit-1',opts)); assert.equal(outputCount(p.source),0n);
  } finally {p.source.close();p.remote.close();}
});

test('retained output recovery does not require newer remote history or application tables to be readable',async()=>{
  const p=await setup();
  try {
    const first=await p.journal.enqueueLocal('edit-1',opts);
    await p.source.execute(`DELETE FROM ${entriesTable}`);
    p.source.statements.length=0;
    const again=await p.journal.enqueueLocal('edit-1',opts);
    assert.deepEqual(again.delivery,first.delivery); assert.equal(again.replayed,true);
    assert(!p.source.statements.some(sql=>sql.includes(`FROM main."${entriesTable}"`)||sql.includes('FROM main."t"')));
  } finally {p.source.close();p.remote.close();}
});

test('two required replicas retain and reclaim the actual rebased payload at their shared frontier',async()=>{
  const p=await setup(), second=new SqliteTarget(':memory:',schema+"INSERT INTO t VALUES(1,'remote')");
  try {
    const fanout=await ChangesetFanout.open(p.source,['east','west']);
    const first=await p.journal.enqueueLocal('edit-1',opts);
    for (const [id,target] of [['east',p.remote],['west',second]]) {
      const bound=fanout.forReplica(id);
      const [pending]=await bound.pending(); const read=await bound.read(pending.deliveryId);
      const decision=await applyChangeset(target,read.changeset,{...opts,deliveryId:pending.deliveryId});
      assert.equal(decision.replayed,false); assert.deepEqual(target.rows(),p.source.rows());
      assert.equal((await applyChangeset(target,read.changeset,{...opts,deliveryId:pending.deliveryId})).replayed,true);
      await bound.acknowledge(pending.deliveryId,pending.sha256);
      assert.equal((await p.outbox.read(first.delivery.deliveryId)).changeset===null,id==='west');
    }
    assert.equal((await p.journal.enqueueLocal('edit-1',opts)).delivery.acknowledged,true);
  } finally {p.source.close();p.remote.close();second.close();}
});

for (const cut of ['cancel','timeout','wrong-count','deferred-commit']) test(`publication failure rolls back outbox only, preserving the committed original: ${cut}`,async()=>{
  const p=await setup(); const abort=new AbortController();
  try {
    const options={...opts};
    if(cut==='cancel') {
      options.signal=abort.signal;
      p.source.after=(kind,sql)=>{if(kind==='execute'&&sql.startsWith('INSERT OR ABORT INTO main."__fsqlite_changeset_outbox"')) abort.abort('stop');};
    } else if(cut==='timeout') {
      options.timeoutMs=10;
      p.source.before=async(kind,sql)=>{if(kind==='execute'&&sql.startsWith('INSERT OR ABORT INTO main."__fsqlite_changeset_outbox"')) await new Promise(r=>setTimeout(r,20));};
    } else if(cut==='wrong-count') {
      const execute=p.source.execute.bind(p.source);
      p.source.execute=async(sql,params)=>{const changed=await execute(sql,params);return sql.startsWith('INSERT OR ABORT INTO main."__fsqlite_changeset_outbox"') ? 0 : changed;};
    } else {
      p.source.db.exec('CREATE TABLE parent(id PRIMARY KEY); CREATE TABLE bad(id REFERENCES parent DEFERRABLE INITIALLY DEFERRED);');
      p.source.beforeCommit=()=>p.source.db.exec('INSERT INTO bad VALUES(99)');
    }
    await assert.rejects(p.journal.enqueueLocal('edit-1',options));
    p.source.before=null;p.source.after=null;p.source.beforeCommit=null;
    assert.equal(outputCount(p.source),0n);assert.deepEqual(await p.journal.readLocal('edit-1'),p.local.record);
    assert.deepEqual(p.source.rows(),[[1n,'local']]);
  } finally {p.source.close();p.remote.close();}
});
test('publication inside an enclosing transaction stays provisional until that transaction commits',async()=>{
  const p=await setup();
  try {
    await assert.rejects(p.source.transaction(async tx=>{
      const inner=new ChangesetRebaseJournal({transaction:work=>work(tx)},{journalId});
      assert.equal((await inner.enqueueLocal('edit-1',opts)).delivery.sequence,1n);
      throw new Error('outer rollback');
    }),/outer rollback/);
    assert.equal(outputCount(p.source),0n);assert.deepEqual(await p.journal.readLocal('edit-1'),p.local.record);
  } finally {p.source.close();p.remote.close();}
});

test('file-backed overlapping publishers cannot choose two outputs for one original',async()=>{
  const dir=mkdtempSync(join(tmpdir(),'fsqlite-rebase-outbox-race-')), file=join(dir,'source.db');
  const first=new SqliteTarget(file,schema); first.db.exec('PRAGMA journal_mode=WAL;');
  let second; const entered=gate(), release=gate();
  try {
    const j1=new ChangesetRebaseJournal(first,{journalId});
    await j1.captureLocal('one',tx=>tx.execute("INSERT INTO t VALUES(1,'value')"),opts);
    second=new SqliteTarget(file); const j2=new ChangesetRebaseJournal(second,{journalId});
    let paused=false;
    first.after=async(kind,sql)=>{if(!paused&&kind==='query'&&sql.includes(`FROM main."${localTable}"`)){paused=true;entered.resolve();await release.promise;}};
    const attempt=j1.enqueueLocal('one',opts); const rejected=assert.rejects(attempt,/locked|BUSY/i);
    await entered.promise; const winner=await j2.enqueueLocal('one',opts); release.resolve(); await rejected;
    first.after=null;
    assert.equal((await j1.enqueueLocal('one',opts)).replayed,true);
    assert.equal(outputCount(first),1n);
    assert.deepEqual((await new ChangesetOutbox(first).pending())[0],winner.delivery);
  } finally {release.resolve();first.close();second?.close();}
});

for (const encoding of ['UTF-8','UTF-16le','UTF-16be']) test(`${encoding}: maximum escaped identities and binary/REAL/text fields retain exact original evidence`,async()=>{
  const name='source:'+ '\u0001'.repeat(250) + '😀'.repeat(60); // 497 UTF-8 bytes
  const id='edit:'+ '界'.repeat(160);
  const ddl='CREATE TABLE t(k BLOB PRIMARY KEY, v, r REAL) WITHOUT ROWID;';
  const t=new SqliteTarget(':memory:',`PRAGMA encoding='${encoding}';`+ddl), peer=new SqliteTarget(':memory:',ddl);
  try {
    const j=new ChangesetRebaseJournal(t,{journalId:name});
    const native=t.db.createSession();
    const original=await j.captureLocal(id,async tx=>{
      await tx.execute('INSERT INTO t VALUES(?,?,CAST(? AS REAL))',[new Uint8Array([0,255,7]),'\uFEFFtext\0😀',2]);
      await tx.execute('INSERT INTO t VALUES(?,?,?)',[9007199254740993n,new Uint8Array(65536).fill(9),2.75]);
    },opts);
    const expected=new Uint8Array(native.changeset()); native.close();
    const r=await j.enqueueLocal(id,opts), bytes=(await new ChangesetOutbox(t).read(r.delivery.deliveryId)).changeset;
    assert.equal(peer.db.applyChangeset(bytes),true);
    // Independent native Session application reaches the same storage classes and bytes.
    const oracle=new SqliteTarget(':memory:',ddl);
    try {
      assert.equal(oracle.db.applyChangeset(expected),true);
      const query='SELECT typeof(k),hex(k),typeof(v),hex(v),typeof(r),r FROM t ORDER BY k';
      assert.deepEqual(peer.rows(query),oracle.rows(query));
    } finally {oracle.close();}
    assert.deepEqual(await j.readLocal(id),original.record);
    assert.equal((await j.enqueueLocal(id,opts)).replayed,true);
  } finally {t.close();peer.close();}
});

async function runCrash(file,cut) {
  const child=spawn(process.execPath,[
    '--experimental-transform-types','--experimental-loader=./packages/sdk/tests/helpers/production-source-loader.mjs',
    'packages/sdk/tests/helpers/rebase-outbox-child.mjs',file,cut,
  ],{stdio:['ignore','ignore','pipe','ipc']});
  let reached=false, timedOut=false, errors='';
  child.on('message',m=>{if(m.cut===cut)reached=true;});
  child.stderr.on('data',data=>{if(errors.length<8192)errors+=String(data);});
  const watchdog=setTimeout(()=>{timedOut=true;child.kill('SIGKILL');},10000);
  try {
    const ended=await new Promise((resolve,reject)=>{child.once('error',reject);child.once('exit',(code,signal)=>resolve({code,signal}));});
    assert.equal(timedOut,false,`A timeout kill is not recovery evidence: ${errors}`);
    assert.equal(reached,true,`Requested cut was not reached: ${errors}`);
    assert.deepEqual(ended,{code:null,signal:'SIGKILL'});
  } finally {clearTimeout(watchdog);}
}
for (const mode of ['WAL','DELETE']) for (const cut of ['before-insert','after-insert','before-commit','after-commit']) {
  test(`${mode}: SIGKILL at ${cut} recovers all-or-none rebased publication`,async()=>{
    const dir=mkdtempSync(join(tmpdir(),'fsqlite-rebase-outbox-crash-')), file=join(dir,'source.db');
    let source=new SqliteTarget(file,schema);
    const remote=new SqliteTarget(':memory:',schema);
    try {
      source.db.exec(`PRAGMA journal_mode=${mode}; PRAGMA synchronous=FULL;`);
      let j=new ChangesetRebaseJournal(source,{journalId});
      const local=await j.captureLocal('edit-1',tx=>tx.execute("INSERT INTO t VALUES(1,'local')"),opts);
      const session=remote.db.createSession(); remote.db.exec("INSERT INTO t VALUES(1,'remote')");
      const bytes=new Uint8Array(session.changeset()); session.close();
      await j.apply(bytes,{...opts,deliveryId:'remote:1',onConflict:()=>'omit'});
      const expected=await j.rebaseLocal('edit-1'); source.close(); source=null;
      await runCrash(file,cut);
      source=new SqliteTarget(file); j=new ChangesetRebaseJournal(source,{journalId});
      assert.equal(outputCount(source),cut==='after-commit'?1n:0n);
      assert.deepEqual(await j.readLocal('edit-1'),local.record);
      const r=await j.enqueueLocal('edit-1',opts);
      assert.equal(r.replayed,cut==='after-commit'); assert.equal(r.delivery.sequence,1n);
      assert.deepEqual(r.throughBookmark,expected.throughBookmark);
      const outgoing=(await new ChangesetOutbox(source).read(r.delivery.deliveryId)).changeset;
      assert.deepEqual(outgoing,expected.changeset); assert.equal(remote.db.applyChangeset(outgoing),true);
      assert.deepEqual(source.rows(),remote.rows());
    } finally {source?.close();remote.close();}
  });
}
