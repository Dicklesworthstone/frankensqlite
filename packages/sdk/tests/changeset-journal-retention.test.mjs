import assert from 'node:assert/strict';
import { test } from 'node:test';
import { ChangesetRebaseJournal } from '../src/changeset-rebase-journal.ts';
import { encodeChangeset } from '../src/changeset-codec.ts';
import { SqliteTarget } from './helpers/production-sqlite-target.mjs';

const schema = 'CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);';
const tables = ['t'];
const history = '__fsqlite_rebase_journal_entries';
const receipts = '__fsqlite_changeset_receipts';
const checkpoints = '__fsqlite_rebase_journal_retention';
const message = (id, value = `row-${id}`) => encodeChangeset([
  { name: 't', primaryKey: [1, 0], changes: [{ operation: 'insert', indirect: false, new: [BigInt(id), value] }] },
]);
const apply = (journal, id, options = {}) => journal.apply(message(id), { tables, deliveryId: `remote:${id}`, ...options });
function setup(options = {}, encoding = 'UTF-8', path = ':memory:') {
  const target = new SqliteTarget(path, `PRAGMA encoding='${encoding}';${schema}`);
  const journal = new ChangesetRebaseJournal(target, { journalId: 'source:conflicts', ...options });
  return { target, journal };
}
const errorCode = code => error => error.code === `ERR_FSQLITE_REBASE_JOURNAL_${code}`;

test('retire remote decisions and advance beyond an entry cap without resetting history', async () => {
  const { target, journal } = setup({ maxEntries: 2 });
  try {
    for (let id = 1; id <= 12; id += 2) {
      await apply(journal, id); await apply(journal, id + 1);
      const tip = await journal.bookmark();
      assert.equal(tip.position, id + 1);
      await assert.rejects(apply(journal, id + 2), errorCode('LIMIT'));
      const result = await journal.retireThrough(tip);
      assert.equal(result.removed, 2); assert.equal(result.retainedEntries, 0);
      assert.deepEqual(result.floor, tip);
      assert.deepEqual(await journal.bookmark(), tip);
      assert.equal((await journal.head()).position, id + 1);
      assert.equal(target.rows(`SELECT count(*) FROM ${history}`)[0][0], 0n);
      assert.equal(target.rows(`SELECT count(*) FROM ${receipts}`)[0][0], BigInt(id + 1));
    }
    assert.equal(target.rows().length, 12);
  } finally { target.close(); }
});
test('retirement refuses to cross a retained local edit basis', async () => {
  const { target, journal } = setup();
  try {
    const local = await journal.captureLocal('unpublished', tx => tx.execute("INSERT INTO t VALUES(90,'local')"), { tables });
    await apply(journal, 1);
    await assert.rejects(journal.retireThrough(await journal.bookmark()), errorCode('HISTORY'));
    assert.deepEqual(await journal.readLocal('unpublished'), local.record);
    assert.equal(target.rows(`SELECT count(*) FROM ${history}`)[0][0], 1n);
  } finally { target.close(); }
});
test('a saved boundary keeps its hash while old rebase ranges explicitly expire', async () => {
  const { target, journal } = setup();
  try {
    await apply(journal, 1); const first = await journal.bookmark();
    await apply(journal, 2); const tip = await journal.bookmark();
    await journal.retireThrough(first);
    assert.deepEqual(await journal.bookmark(), tip);
    const local = message(50);
    const result = await journal.rebase(local, { after: first, through: tip });
    assert.deepEqual(result.changeset, local);
    await assert.rejects(journal.rebase(local), errorCode('EXPIRED'));
    assert.equal((await journal.retireThrough(first)).removed, 0);
    assert.equal((await journal.retention()).floor.position, 1);
  } finally { target.close(); }
});

import { createHash } from 'node:crypto';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawn } from 'node:child_process';
import { applyChangeset } from '../src/changeset-apply.ts';
import { acknowledgeDelivery, find, load } from '../src/changeset-outbox-store.ts';
import { ChangesetFanout } from '../src/changeset-fanout.ts';
const locals = '__fsqlite_rebase_journal_locals';
const heads = '__fsqlite_rebase_journal_heads';
const jid = 'source:conflicts';
const gate = () => { let resolve; const promise = new Promise(r => { resolve = r; }); return { promise, resolve }; };
const existing = (t, name) => t.rows('SELECT count(*) FROM sqlite_schema WHERE name=?', [name])[0][0] !== 0n;
const state = t => ({ rows: t.rows(), history: existing(t,history) ? t.rows(`SELECT * FROM ${history} ORDER BY journal_id,position`) : null,
  heads: existing(t,heads) ? t.rows(`SELECT * FROM ${heads} ORDER BY journal_id`) : null,
  receipts: existing(t,receipts) ? t.rows(`SELECT * FROM ${receipts} ORDER BY delivery_id`) : null,
  floor: existing(t,checkpoints) ? t.rows(`SELECT * FROM ${checkpoints} ORDER BY journal_id`) : null });
async function seedConflicts(t, j, n = 3) {
  for (let id = 1; id <= n; id++) {
    t.db.prepare('INSERT INTO t VALUES(?,?)').run(id, `keep-${id}`);
    await apply(j,id,{onConflict:()=> 'omit'});
  }
  return j.bookmark();
}

for (const encoding of ['UTF-8','UTF-16le','UTF-16be']) test(`${encoding}: prune a nonempty prefix, preserve native rebasing and published-local retirement`, async () => {
  const {target,journal}=setup({},encoding), remote=new SqliteTarget(':memory:',schema);
  try {
    const firstSession=remote.db.createSession(); remote.db.exec("INSERT INTO t VALUES(1,'prefix')");
    await journal.apply(new Uint8Array(firstSession.changeset()),{tables,deliveryId:'remote:1'}); firstSession.close();
    const basis=await journal.bookmark();
    const original=await journal.captureLocal('edit',tx=>tx.execute("INSERT INTO t VALUES(2,'local')"),{tables});
    const conflict=remote.db.createSession(); remote.db.exec("INSERT INTO t VALUES(2,'remote')");
    await journal.apply(new Uint8Array(conflict.changeset()),{tables,deliveryId:'remote:2',onConflict:()=> 'omit'}); conflict.close();
    const tip=await journal.bookmark(), before=await journal.rebaseLocal('edit');
    const cut=await journal.retireThrough(basis);
    assert.equal(cut.removed,1); assert.equal(cut.retainedEntries,1);
    assert.deepEqual(await journal.rebaseLocal('edit'),before);
    assert.deepEqual(await journal.bookmark(),tip);
    const published=await journal.enqueueLocal('edit',{tables});
    await assert.rejects(journal.retireThrough(tip),errorCode('HISTORY'));
    const bytes=await target.transaction(async tx=>load(tx,await find(tx,published.delivery.deliveryId)));
    assert.equal(remote.db.applyChangeset(bytes),true); assert.deepEqual(remote.rows(),target.rows());
    await target.transaction(tx=>acknowledgeDelivery(tx,published.delivery.deliveryId,published.delivery.sha256));
    assert.equal((await journal.retireLocal('edit',original.record.recordSha256)).removed,true);
    assert.equal((await journal.retireThrough(tip)).removed,1);
    assert.deepEqual(await journal.bookmark(),tip);
    const next=await journal.captureLocal('next',()=>{}, {tables});
    assert.deepEqual(next.record.basis,tip);
    assert.deepEqual((await journal.rebaseLocal('next')).changeset,new Uint8Array());
  } finally {target.close();remote.close();}
});

test('retired delivery cannot rerun SQL or produce fabricated journal decisions',async()=>{
  const {target,journal}=setup();
  try {
    await apply(journal,1); const tip=await journal.bookmark();
    const inbox=target.rows(`SELECT * FROM ${receipts}`);
    await journal.retireThrough(tip); target.db.exec("UPDATE t SET v='later' WHERE id=1");
    let called=0;
    await assert.rejects(apply(journal,1,{onConflict:()=>{called++;return 'replace';}}),errorCode('MISSING'));
    assert.equal(called,0); assert.deepEqual(target.rows(),[[1n,'later']]);
    assert.deepEqual(target.rows(`SELECT * FROM ${receipts}`),inbox);
    assert.equal((await applyChangeset(target,message(1),{tables,deliveryId:'remote:1'})).replayed,true);
    await assert.rejects(journal.apply(message(1,'different'),{tables,deliveryId:'remote:1'}),{code:'ERR_FSQLITE_CHANGESET_DELIVERY_REUSE'});
    assert.deepEqual(await journal.bookmark(),tip); assert.equal(await journal.read('remote:1'),null);
  } finally {target.close();}
});
test('nonempty decision bytes are released and lower aggregate caps do not prevent cleanup',async()=>{
  const {target,journal}=setup();
  try {
    const tip=await seedConflicts(target,journal,4), before=await journal.head();
    assert(before.byteLength>0);
    const limited=new ChangesetRebaseJournal(target,{journalId:jid,maxEntries:1,maxBytes:1});
    await assert.rejects(limited.head(),errorCode('LIMIT'));
    assert.equal((await limited.retention()).retainedEntries,4);
    const result=await limited.retireThrough(tip);
    assert.equal(result.byteLength,before.byteLength); assert.equal(result.removed,4);
    assert.equal((await limited.head()).byteLength,0);
    await apply(limited,9); assert.equal((await limited.head()).position,5);
  } finally {target.close();}
});
test('empty journal retirement is read-only and requires its exact genesis bookmark',async()=>{
  const {target,journal}=setup();
  try {
    const tip=await journal.bookmark(), before=target.rows('SELECT name FROM sqlite_schema ORDER BY name');
    assert.deepEqual(await journal.retireThrough(tip),{removed:0,byteLength:0,retainedEntries:0,floor:tip});
    assert.deepEqual(target.rows('SELECT name FROM sqlite_schema ORDER BY name'),before);
    await assert.rejects(journal.retireThrough({...tip,sha256:'0'.repeat(64)}),errorCode('HISTORY'));
  } finally {target.close();}
});
for (const value of [null,1,{}, {format:'fsqlite-rebase-bookmark-v1',journalId:jid,position:-1,sha256:'0'.repeat(64)},
  {format:'fsqlite-rebase-bookmark-v1',journalId:jid,position:Number.MAX_SAFE_INTEGER+1,sha256:'0'.repeat(64)},
  {format:'fsqlite-rebase-bookmark-v1',journalId:jid,position:0,sha256:'A'.repeat(64)}])
  test(`invalid retirement bookmark fails before transaction: ${JSON.stringify(value)}`,async()=>{
    let calls=0; const j=new ChangesetRebaseJournal({transaction(){calls++;throw new Error('admitted');}},{journalId:jid});
    await assert.rejects(j.retireThrough(value)); assert.equal(calls,0);
  });
test('bookmark properties are owned before asynchronous admission; accessors reject',async()=>{
  const {target,journal}=setup(); const entered=gate(), release=gate();
  try {
    await apply(journal,1); const tip={...await journal.bookmark()};
    const wrapped=new ChangesetRebaseJournal({transaction:async work=>{entered.resolve();await release.promise;return target.transaction(work);}},{journalId:jid});
    const pending=wrapped.retireThrough(tip); await entered.promise; tip.position=0;tip.sha256='0'.repeat(64);release.resolve();
    assert.equal((await pending).floor.position,1);
    const getter={...tip}; Object.defineProperty(getter,'position',{get(){throw new Error('getter executed');}});
    await assert.rejects(journal.retireThrough(getter),errorCode('INPUT'));
  } finally {release.resolve();target.close();}
});
test('foreign, future and altered bookmarks cannot prune; expired historical requests reject',async()=>{
  const {target,journal}=setup();
  try {
    await apply(journal,1); const first=await journal.bookmark(); await apply(journal,2); const tip=await journal.bookmark();
    for(const wrong of [{...first,journalId:'other'},{...first,sha256:'0'.repeat(64)},{...tip,position:3}]) {
      const before=state(target); await assert.rejects(journal.retireThrough(wrong));assert.deepEqual(state(target),before);
    }
    await journal.retireThrough(tip);
    await assert.rejects(journal.retireThrough(first),errorCode('EXPIRED'));
    assert.deepEqual((await journal.retention()).floor,tip);
  } finally {target.close();}
});
test('independent journals retain separate floors and full-prefix hashes',async()=>{
  const {target,journal}=setup(); const other=new ChangesetRebaseJournal(target,{journalId:'other'});
  try {
    await apply(journal,1);await apply(other,2); const one=await journal.bookmark(),two=await other.bookmark();
    await journal.retireThrough(one);
    assert.deepEqual(await other.bookmark(),two);assert.equal((await other.retention()).floor.position,0);
    await other.retireThrough(two);await apply(journal,3);await apply(other,4);
    assert.equal((await journal.head()).position,2);assert.equal((await other.head()).position,2);
  } finally {target.close();}
});
for(const corrupt of ['sha256','position','record_sha256']) test(`corrupt checkpoint ${corrupt} fails closed`,async()=>{
  const {target,journal}=setup();
  try {
    await apply(journal,1); await journal.retireThrough(await journal.bookmark());
    target.db.prepare(`UPDATE ${checkpoints} SET ${corrupt}=?`).run(corrupt==='position'?2:'f'.repeat(64));
    const before=state(target);
    await assert.rejects(journal.retention(),errorCode('CORRUPT'));
    await assert.rejects(apply(journal,2),errorCode('CORRUPT'));
    assert.deepEqual(state(target),before);
  } finally {target.close();}
});
for(const mode of ['missing checkpoint','missing floor row','missing head','orphan checkpoint','extra index','trigger'])
  test(`damaged history storage is not repaired: ${mode}`,async()=>{
    const {target,journal}=setup();
    try {
      await apply(journal,1); const tip=await journal.bookmark(); await journal.retireThrough(tip);
      if(mode==='missing checkpoint') target.db.exec(`DROP TABLE ${checkpoints}`);
      if(mode==='missing floor row') target.db.exec(`DELETE FROM ${checkpoints}`);
      if(mode==='missing head') target.db.exec(`DELETE FROM ${heads}`);
      if(mode==='orphan checkpoint') target.db.exec(`DROP TABLE ${heads};DROP TABLE ${history}`);
      if(mode==='extra index') target.db.exec(`CREATE INDEX unexpected ON ${checkpoints}(position)`);
      if(mode==='trigger') target.db.exec(`CREATE TRIGGER unexpected AFTER UPDATE ON ${checkpoints} BEGIN SELECT 1;END`);
      const names=target.rows('SELECT name FROM sqlite_schema ORDER BY name');
      await assert.rejects(journal.retention());await assert.rejects(apply(journal,2));
      assert.deepEqual(target.rows('SELECT name FROM sqlite_schema ORDER BY name'),names);
      assert.deepEqual(target.rows(),[[1n,'row-1']]);
    } finally {target.close();}
  });
for(const where of [1,3]) test(`corrupt ${where===1?'retiring prefix':'live suffix'} is detected before deletion`,async()=>{
  const {target,journal}=setup();
  try {
    await seedConflicts(target,journal,1);const first=await journal.bookmark();
    for(let id=2;id<=3;id++){target.db.prepare('INSERT INTO t VALUES(?,?)').run(id,'keep');await apply(journal,id,{onConflict:()=> 'omit'});}
    target.db.prepare(`UPDATE ${history} SET sha256=? WHERE position=?`).run('0'.repeat(64),where);
    const before=state(target);await assert.rejects(journal.retireThrough(first),errorCode('CORRUPT'));assert.deepEqual(state(target),before);
  } finally {target.close();}
});
test('changed local basis cannot hide an earlier pin',async()=>{
  const {target,journal}=setup();
  try {
    await journal.captureLocal('pending',()=>{}, {tables});await apply(journal,1);const tip=await journal.bookmark();
    target.db.prepare(`UPDATE ${locals} SET basis_position=1,basis_sha256=?`).run(tip.sha256);
    const before=state(target);await assert.rejects(journal.retireThrough(tip),errorCode('CORRUPT'));assert.deepEqual(state(target),before);
  } finally {target.close();}
});
test('65 retained originals are validated in keyset pages of at most 32',async()=>{
  const {target,journal}=setup();
  try {
    await apply(journal,1);const tip=await journal.bookmark();
    for(let i=0;i<65;i++) await journal.captureLocal(`op-${String(i).padStart(3,'0')}`,()=>{}, {tables});
    const sizes=[];target.after=(kind,sql,params,result)=>{
      if(kind==='query'&&sql.includes(`FROM main."${locals}"`)&&sql.includes('ORDER BY operation_id LIMIT 32')) sizes.push(result.rowArrays.length);
    };
    assert.equal((await journal.retireThrough(tip)).removed,1);assert.deepEqual(sizes,[32,32,1]);
    target.after=null;assert.deepEqual((await journal.readLocal('op-064')).basis,tip);
  } finally {target.close();}
});
test('required replicas keep the pin until the original can be safely retired',async()=>{
  const {target,journal}=setup();
  try {
    const fanout=await ChangesetFanout.open(target,['east','west']);
    const original=await journal.captureLocal('edit',tx=>tx.execute("INSERT INTO t VALUES(90,'local')"),{tables});
    await apply(journal,1); const tip=await journal.bookmark(), published=await journal.enqueueLocal('edit',{tables});
    await fanout.forReplica('east').acknowledge(published.delivery.deliveryId,published.delivery.sha256);
    await assert.rejects(journal.retireLocal('edit',original.record.recordSha256));
    await assert.rejects(journal.retireThrough(tip),errorCode('HISTORY'));
    await fanout.forReplica('west').acknowledge(published.delivery.deliveryId,published.delivery.sha256);
    await journal.retireLocal('edit',original.record.recordSha256);
    assert.equal((await journal.retireThrough(tip)).removed,1);
  } finally {target.close();}
});

test('retirement failure at COMMIT rolls back the floor, deletion and accounting',async()=>{
  const {target,journal}=setup();
  try {
    const tip=await seedConflicts(target,journal), before=state(target);
    target.db.exec('CREATE TABLE p(id PRIMARY KEY);CREATE TABLE child(id REFERENCES p DEFERRABLE INITIALLY DEFERRED)');
    target.beforeCommit=()=>target.db.exec('INSERT INTO child VALUES(999)');
    await assert.rejects(journal.retireThrough(tip),/FOREIGN KEY/);
    target.beforeCommit=null;
    assert.deepEqual(state(target),before);assert.deepEqual(target.rows('SELECT * FROM child'),[]);
    assert.equal((await journal.retireThrough(tip)).removed,3);
  } finally {target.close();}
});
test('outer rollback preserves retirement evidence despite a successful nested result',async()=>{
  const {target,journal}=setup();
  try {
    const tip=await seedConflicts(target,journal), before=state(target);
    await assert.rejects(target.transaction(async tx=>{
      const nested=new ChangesetRebaseJournal({transaction:work=>work(tx)},{journalId:jid});
      assert.equal((await nested.retireThrough(tip)).removed,3);throw new Error('outer abort');
    }),/outer abort/);
    assert.deepEqual(state(target),before);
  } finally {target.close();}
});
test('lost commit reply reconciles the exact persisted floor without deleting again',async()=>{
  const {target,journal}=setup();
  try {
    const tip=await seedConflicts(target,journal);
    target.afterCommit=()=>{throw new Error('lost reply');};
    await assert.rejects(journal.retireThrough(tip),/lost reply/);target.afterCommit=null;
    assert.deepEqual(await journal.bookmark(),tip);const before=state(target);
    assert.equal((await journal.retireThrough(tip)).removed,0);assert.deepEqual(state(target),before);
  } finally {target.close();}
});
for(const phase of ['checkpoint','delete','head']) test(`cancellation after ${phase} write rolls back all retirement state`,async()=>{
  const {target,journal}=setup();
  try {
    const tip=await seedConflicts(target,journal), before=state(target), abort=new AbortController();
    target.after=(kind,sql)=>{
      if(kind==='execute'&&((phase==='checkpoint'&&sql.startsWith(`INSERT OR ABORT INTO main."${checkpoints}"`))||
        (phase==='delete'&&sql.startsWith(`DELETE FROM main."${history}"`))||
        (phase==='head'&&sql.startsWith(`UPDATE OR ABORT main."${heads}" SET byte_length=`)))) abort.abort();
    };
    await assert.rejects(journal.retireThrough(tip,{signal:abort.signal}),errorCode('CANCELLED'));
    target.after=null;assert(abort.signal.aborted);assert.deepEqual(state(target),before);
  } finally {target.close();}
});
test('cancelled retirement drains an admitted SQL operation before rollback',async()=>{
  const {target,journal}=setup(), entered=gate(), release=gate();
  try {
    const tip=await seedConflicts(target,journal), before=state(target), abort=new AbortController();
    target.before=async(kind,sql)=>{if(kind==='execute'&&sql.startsWith(`DELETE FROM main."${history}"`)){entered.resolve();await release.promise;}};
    let settled=false;const pending=journal.retireThrough(tip,{signal:abort.signal});
    pending.then(()=>{settled=true;},()=>{settled=true;});
    await entered.promise;abort.abort();await new Promise(r=>setTimeout(r,10));assert.equal(settled,false);
    release.resolve();await assert.rejects(pending,errorCode('CANCELLED'));target.before=null;assert.deepEqual(state(target),before);
  } finally {release.resolve();target.close();}
});
for(const changed of [0,99]) test(`false deleted-row count ${changed} cannot become successful retirement`,async()=>{
  const {target,journal}=setup();
  try {
    const tip=await seedConflicts(target,journal), before=state(target);
    const wrapped={transaction:work=>target.transaction(tx=>work({query:(sql,params)=>tx.query(sql,params),execute:async(sql,params)=>{
      const actual=await tx.execute(sql,params);return sql.startsWith(`DELETE FROM main."${history}"`)?changed:actual;
    }}))};
    await assert.rejects(new ChangesetRebaseJournal(wrapped,{journalId:jid}).retireThrough(tip),errorCode('CORRUPT'));
    assert.deepEqual(state(target),before);
  } finally {target.close();}
});
test('an existing WAL reader sees neither a pruned prefix nor a checkpoint before COMMIT',async()=>{
  const file=join(mkdtempSync(join(tmpdir(),'journal-retention-reader-')),'db.sqlite');
  const {target,journal}=setup({},'UTF-8',file);target.db.exec('PRAGMA journal_mode=WAL');
  const reader=new SqliteTarget(file);
  try {
    const tip=await seedConflicts(target,journal), before=state(reader);
    reader.db.exec('BEGIN');assert.deepEqual(state(reader),before);
    let observed=0;target.after=(kind,sql)=>{if(kind==='execute'&&sql.startsWith(`DELETE FROM main."${history}"`)){observed++;assert.deepEqual(state(reader),before);}};
    await journal.retireThrough(tip);assert.equal(observed,1);assert.deepEqual(state(reader),before);
    reader.db.exec('COMMIT');assert.equal(reader.rows(`SELECT count(*) FROM ${history}`)[0][0],0n);
    assert.deepEqual((await new ChangesetRebaseJournal(reader,{journalId:jid}).retention()).floor,tip);
  } finally {target.close();reader.close();}
});
test('competing WAL retirement owners preserve one atomic floor without forced retries',async()=>{
  const file=join(mkdtempSync(join(tmpdir(),'journal-retention-race-')),'db.sqlite');
  const {target,journal}=setup({},'UTF-8',file);target.db.exec('PRAGMA journal_mode=WAL');
  const peer=new SqliteTarget(file), other=new ChangesetRebaseJournal(peer,{journalId:jid});
  const entered=gate(),release=gate();
  try {
    const tip=await seedConflicts(target,journal);
    target.before=async(kind,sql)=>{if(kind==='execute'&&sql.startsWith(`CREATE TABLE main."${checkpoints}"`)){entered.resolve();await release.promise;}};
    const pending=journal.retireThrough(tip);const failed=assert.rejects(pending,/locked|busy/i);
    await entered.promise;assert.equal((await other.retireThrough(tip)).removed,3);release.resolve();await failed;
    target.before=null;assert.equal((await journal.retireThrough(tip)).removed,0);assert.deepEqual(await journal.bookmark(),tip);
    assert.equal(target.rows(`SELECT count(*) FROM ${receipts}`)[0][0],3n);
  } finally {release.resolve();target.close();peer.close();}
});
test('new remote commit racing retirement is preserved when the old snapshot loses promotion',async()=>{
  const file=join(mkdtempSync(join(tmpdir(),'journal-retention-append-')),'db.sqlite');
  const {target,journal}=setup({},'UTF-8',file);target.db.exec('PRAGMA journal_mode=WAL');
  const peer=new SqliteTarget(file), other=new ChangesetRebaseJournal(peer,{journalId:jid});
  const entered=gate(),release=gate();
  try {
    await apply(journal,1);const tip=await journal.bookmark();
    target.before=async(kind,sql)=>{if(kind==='execute'&&sql.startsWith(`CREATE TABLE main."${checkpoints}"`)){entered.resolve();await release.promise;}};
    const pending=journal.retireThrough(tip);const failed=assert.rejects(pending,/locked|busy/i);
    await entered.promise;await apply(other,2);const after=await other.bookmark();release.resolve();await failed;target.before=null;
    const retry=await journal.retireThrough(tip);assert.equal(retry.removed,1);assert.equal(retry.retainedEntries,1);
    assert.deepEqual(await journal.bookmark(),after);assert.equal((await journal.read('remote:2')).position,2);
  } finally {release.resolve();target.close();peer.close();}
});

for(const mode of ['WAL','DELETE']) for(const cut of ['checkpoint','delete','before-commit','after-commit'])
  test(`${mode}: actual SIGKILL after ${cut} retains an all-or-none history prefix`,async()=>{
    const file=join(mkdtempSync(join(tmpdir(),'journal-retention-kill-')),'db.sqlite');
    const {target,journal}=setup({},'UTF-8',file);target.db.exec(`PRAGMA journal_mode=${mode}`);
    const tip=await seedConflicts(target,journal), before=state(target);target.close();
    const child=spawn(process.execPath,[...process.execArgv,'packages/sdk/tests/helpers/journal-retention-child.mjs',file,cut,JSON.stringify(tip)],{stdio:['ignore','ignore','pipe','ipc']});
    let reached=false,err='';child.stderr.on('data',b=>{err+=b;});
    const outcome=await new Promise((resolve,reject)=>{
      const watchdog=setTimeout(()=>{child.kill('SIGKILL');reject(new Error('child did not reach requested cut: '+err));},10000);
      child.once('error',error=>{clearTimeout(watchdog);reject(error);});
      child.on('message',msg=>{if(msg?.cut===cut){reached=true;child.kill('SIGKILL');}});
      child.once('exit',(code,signal)=>{clearTimeout(watchdog);resolve({code,signal});});
    });
    assert.equal(reached,true,err);assert.equal(outcome.signal,'SIGKILL');
    const fresh=new SqliteTarget(file), reopened=new ChangesetRebaseJournal(fresh,{journalId:jid});
    try {
      const committed=cut==='after-commit';
      if(!committed) assert.deepEqual(state(fresh),before);
      assert.equal((await reopened.retention()).floor.position,committed?3:0);
      assert.deepEqual(await reopened.bookmark(),tip);
      assert.equal((await reopened.retireThrough(tip)).removed,committed?0:3);
      assert.deepEqual(fresh.rows(),before.rows);assert.deepEqual(fresh.rows(`SELECT * FROM ${receipts} ORDER BY delivery_id`),before.receipts);
      await apply(reopened,9);assert.equal((await reopened.head()).position,4);
    } finally {fresh.close();}
  });

for(const encoding of ['UTF-8','UTF-16le','UTF-16be']) test(`${encoding}: maximum escaped journal and delivery identities survive checkpoint reopen`,async()=>{
  const {target}=setup({},encoding), id='\u0001'.repeat(512);
  const journal=new ChangesetRebaseJournal(target,{journalId:id});
  try {
    await journal.apply(message(1),{tables,deliveryId:'x'.repeat(512)});const tip=await journal.bookmark();
    await journal.retireThrough(tip);const reopened=new ChangesetRebaseJournal(target,{journalId:id});
    assert.deepEqual(await reopened.bookmark(),tip);await apply(reopened,2);assert.equal((await reopened.head()).position,2);
  } finally {target.close();}
});
test('SQL projections reject NUL-hidden checkpoint tails before crossing the adapter',async()=>{
  const {target,journal}=setup();
  try {
    await apply(journal,1);await journal.retireThrough(await journal.bookmark());
    target.db.exec(`UPDATE ${checkpoints} SET sha256=sha256||char(0)||'hidden'`);
    let checked=false;target.after=(kind,sql,params,result)=>{
      if(kind==='query'&&sql.includes(`FROM main."${checkpoints}"`)){checked=true;assert.equal(result.rowArrays[0][1],null);}
    };
    await assert.rejects(journal.retention(),errorCode('CORRUPT'));assert(checked);
  } finally {target.close();}
});
test('deadline covers an admitted delayed deletion and rolls its checkpoint back',async()=>{
  const {target,journal}=setup();
  try {
    const tip=await seedConflicts(target,journal), before=state(target);let entered=false;
    target.before=async(kind,sql)=>{if(kind==='execute'&&sql.startsWith(`DELETE FROM main."${history}"`)){entered=true;await new Promise(r=>setTimeout(r,300));}};
    await assert.rejects(journal.retireThrough(tip,{timeoutMs:200}),errorCode('TIMEOUT'));target.before=null;
    assert(entered);assert.deepEqual(state(target),before);
  } finally {target.close();}
});
for(const pos of [100001,Number.MAX_SAFE_INTEGER]) test(`synthetic checkpoint tests safe position boundary ${pos}, not an executed long history`,async()=>{
  const {target,journal}=setup();
  try {
    await apply(journal,1);const tip=await journal.bookmark();await journal.retireThrough(tip);
    // A trusted historical-checkpoint fixture isolates integer boundary handling.
    // This does not claim to have executed pos applications or verified their hash.
    const seal=createHash('sha256').update(JSON.stringify(['fsqlite-rebase-retention-v1',jid,pos,tip.sha256])).digest('hex');
    target.db.prepare(`UPDATE ${heads} SET position=?`).run(BigInt(pos));
    target.db.prepare(`UPDATE ${checkpoints} SET position=?,record_sha256=?`).run(BigInt(pos),seal);
    if(pos===Number.MAX_SAFE_INTEGER) {
      const before=state(target);await assert.rejects(apply(journal,2),errorCode('LIMIT'));assert.deepEqual(state(target),before);
    } else {
      await apply(journal,2);assert.equal((await journal.read('remote:2')).position,pos+1);
      const original=await journal.captureLocal('after-high-floor',()=>{}, {tables});
      assert.equal(original.record.basis.position,pos+1);assert.deepEqual((await journal.readLocal('after-high-floor')).basis,original.record.basis);
      assert.equal((await journal.retireThrough(await journal.bookmark())).removed,1);
    }
  } finally {target.close();}
});
