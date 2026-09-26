import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { SqliteTarget } from './helpers/production-sqlite-target.mjs';
import { captureChangeset, snapshotChangeset } from '../src/changeset-capture.ts';
import { captureSnapshotChangeset } from '../src/changeset-snapshot-capture.ts';
import { decodeChangeset, invertChangeset } from '../src/changeset-codec.ts';
import { applyChangeset } from '../src/changeset-apply.ts';

const ddl = 'CREATE TABLE t(id INTEGER PRIMARY KEY, value, extra);';
const initial = "INSERT INTO t VALUES(1,'old',NULL),(2,'delete',X'0102');";
function normalized(bytes) {
  return decodeChangeset(bytes).flatMap(t => t.changes.map(({ indirect, ...c }) =>
    JSON.stringify({ table: t.name, pk: t.primaryKey, ...c }, (_, v) => typeof v === 'bigint' ? `${v}n` : v instanceof Uint8Array ? [...v] : v))).sort();
}
async function oracle(work, schema = ddl, seed = initial, triggers = '', tables = ['t'], options = {}) {
  const source = new SqliteTarget(':memory:', schema + seed + triggers);
  const target = new SqliteTarget(':memory:', schema + seed);
  const before = new SqliteTarget(':memory:', schema + seed);
  const session = source.db.createSession();
  try {
    const result = await captureSnapshotChangeset(source, work, { tables, ...options });
    assert.deepEqual(normalized(result.changeset), normalized(session.changeset()));
    assert.equal(target.db.applyChangeset(result.changeset), true);
    for (const table of tables) assert.deepEqual(target.rows(`SELECT * FROM "${table}" ORDER BY 1`), source.rows(`SELECT * FROM "${table}" ORDER BY 1`));
    assert.equal(target.db.applyChangeset(invertChangeset(result.changeset)), true);
    for (const table of tables) assert.deepEqual(target.rows(`SELECT * FROM "${table}" ORDER BY 1`), before.rows(`SELECT * FROM "${table}" ORDER BY 1`));
    assert.equal(source.statements.some(s => /CREATE TEMP (TRIGGER|TABLE)/.test(s)), false);
    return result;
  } finally { session.close(); source.close(); target.close(); before.close(); }
}

test('application trigger effects and generated insert keys become native-compatible net records', async () => {
  const result = await oracle(async tx => {
    await tx.execute("INSERT INTO t(value) VALUES('fresh')");
    await tx.execute("UPDATE t SET extra=9 WHERE id=1");
    await tx.execute('DELETE FROM t WHERE id=2'); return 42;
  }, ddl, initial, "CREATE TRIGGER normalize AFTER INSERT ON t BEGIN UPDATE t SET value=upper(NEW.value),extra=17 WHERE id=NEW.id; END;");
  assert.equal(result.value, 42); assert.equal(result.changes, 3); assert.equal(result.beforeRows, 2); assert.equal(result.afterRows, 2);
});
test('default touched-row capture still rejects triggers instead of silently changing strategy', async () => {
  const db = new SqliteTarget(':memory:', ddl + "CREATE TRIGGER tr AFTER INSERT ON t BEGIN SELECT 1; END;");
  try { await assert.rejects(captureChangeset(db, () => {}, { tables: ['t'] }), { code: 'ERR_FSQLITE_CAPTURE_SCHEMA' }); }
  finally { db.close(); }
});
test('BEFORE and recursive AFTER trigger work across captured tables is observed', async () => {
  await oracle(tx => tx.execute("UPDATE t SET value='start' WHERE id=1"),
    ddl + 'CREATE TABLE audit(id INTEGER PRIMARY KEY, message);', initial,
    "CREATE TRIGGER b BEFORE UPDATE ON t WHEN OLD.id=1 BEGIN INSERT INTO audit VALUES(1,'before'); UPDATE t SET value='side' WHERE id=2; END;" +
    "CREATE TRIGGER a AFTER UPDATE ON t WHEN NEW.id=2 BEGIN INSERT INTO audit VALUES(2,NEW.value); END;", ['t', 'audit']);
});
test('INSTEAD OF view writes capture underlying application rows', async () => {
  await oracle(tx => tx.execute("INSERT INTO input VALUES(7,'view')"), ddl, initial,
    'CREATE VIEW input AS SELECT id,value FROM t; CREATE TRIGGER tr INSTEAD OF INSERT ON input BEGIN INSERT INTO t(id,value) VALUES(NEW.id,NEW.value); END;');
});
for (const recursive of [0, 1]) test(`REPLACE, trigger-side deletion, and empty-to-nonempty transitions (recursive=${recursive})`, async () => {
  const source = new SqliteTarget(':memory:', ddl + initial + `PRAGMA recursive_triggers=${recursive};` +
    "CREATE TRIGGER tr AFTER INSERT ON t WHEN NEW.id=3 BEGIN DELETE FROM t WHERE id=2; END;");
  const target = new SqliteTarget(':memory:', ddl + initial);
  try {
    const result = await captureSnapshotChangeset(source, async tx => {
      await tx.execute("INSERT OR REPLACE INTO t VALUES(1,'replace',NULL)");
      await tx.execute("INSERT INTO t VALUES(3,'new',NULL)");
    }, { tables: ['t'] });
    await applyChangeset(target, result.changeset, { tables: ['t'] });
    assert.deepEqual(target.rows(), source.rows());
    assert.equal(source.rows('PRAGMA recursive_triggers')[0][0], BigInt(recursive));
  } finally { source.close(); target.close(); }
});
test('transient inserts/deletes, restored rows and a no-op callback produce no net change', async () => {
  const result = await oracle(async tx => {
    await tx.execute("INSERT INTO t VALUES(3,'temporary',NULL)"); await tx.execute('DELETE FROM t WHERE id=3');
    await tx.execute("UPDATE t SET value='intermediate' WHERE id=1"); await tx.execute("UPDATE t SET value='old' WHERE id=1");
  });
  assert.equal(result.changes, 0); assert.equal(result.changeset.length, 0); assert.equal(result.beforeRows, 2); assert.equal(result.afterRows, 2);
});
test('savepoint rollback eliminates trigger effects from the final delta', async () => {
  await oracle(async tx => {
    await tx.execute('SAVEPOINT inner'); await tx.execute("INSERT INTO t VALUES(3,'aborted',NULL)");
    await tx.execute('ROLLBACK TO inner'); await tx.execute('RELEASE inner'); await tx.execute("UPDATE t SET value='kept' WHERE id=1");
  }, ddl, initial, "CREATE TRIGGER tr AFTER INSERT ON t BEGIN UPDATE t SET extra=NEW.id WHERE id=1; END;");
});
for (const encoding of ['UTF-8', 'UTF-16le', 'UTF-16be']) test(`${encoding}: mixed key classes, large integers, NUL/BOM text, integral REAL and BLOBs`, async () => {
  const schema = `PRAGMA encoding='${encoding}'; CREATE TABLE t(id PRIMARY KEY, value, extra);`;
  const source = new SqliteTarget(':memory:', schema), target = new SqliteTarget(':memory:', schema);
  try {
    const keys = [1n, 1.5, '1', new Uint8Array([49]), '\0x', 'x\0', new Uint8Array(), 'r,1'];
    for (const key of keys) { await source.execute('INSERT INTO t VALUES(?,?,?)', [key, 'old', null]); await target.execute('INSERT INTO t VALUES(?,?,?)', [key, 'old', null]); }
    const result = await captureSnapshotChangeset(source, async tx => {
      await tx.execute('UPDATE t SET value=?,extra=CAST(7 AS REAL)', ['\uFEFFhello\0界😀']);
      await tx.execute('INSERT INTO t VALUES(?,?,?)', [9223372036854775807n, -9223372036854775808n, new Uint8Array([0, 255, 7])]);
    }, { tables: ['t'] });
    await applyChangeset(target, result.changeset, { tables: ['t'] });
    assert.deepEqual(target.rows('SELECT typeof(id),id,typeof(value),hex(CAST(value AS BLOB)),typeof(extra),extra FROM t ORDER BY id'),
      source.rows('SELECT typeof(id),id,typeof(value),hex(CAST(value AS BLOB)),typeof(extra),extra FROM t ORDER BY id'));
  } finally { source.close(); target.close(); }
});
test('primary key changes under NOCASE become DELETE and INSERT', async () => {
  const schema = 'CREATE TABLE t(id TEXT PRIMARY KEY COLLATE NOCASE, value);';
  const source = new SqliteTarget(':memory:', schema + "INSERT INTO t VALUES('a','old');"), target = new SqliteTarget(':memory:', schema + "INSERT INTO t VALUES('a','old');");
  try {
    const result = await captureSnapshotChangeset(source, tx => tx.execute("UPDATE t SET id='A'"), { tables: ['t'] });
    assert.deepEqual(decodeChangeset(result.changeset)[0].changes.map(c => c.operation), ['delete','insert']);
    await applyChangeset(target, result.changeset, { tables: ['t'] }); assert.deepEqual(target.rows(), source.rows());
  } finally { source.close(); target.close(); }
});
test('empty tables, deletion of all rows and quoted identifiers remain usable', async () => {
  const schema='CREATE TABLE "a b"("k" INTEGER PRIMARY KEY,v); CREATE TABLE t(id INTEGER PRIMARY KEY);';
  await oracle(async tx => { await tx.execute('DELETE FROM "a b"'); await tx.execute('INSERT INTO t VALUES(7)'); }, schema, 'INSERT INTO "a b" VALUES(1,2);', '', ['a b','t']);
});
test('whole-scope indirect option applies to every emitted net record', async () => {
  const result = await oracle(tx => tx.execute("UPDATE t SET value='changed'"), ddl, initial, '', ['t'], { indirect: true });
  assert.equal(decodeChangeset(result.changeset).every(t => t.changes.every(c => c.indirect)), true);
});
for (const field of ['maxRows','maxBytes','maxCells']) test(`${field} limits both snapshots and rolls back after-image overflow`, async () => {
  const source = new SqliteTarget(':memory:', ddl + "INSERT INTO t VALUES(1,'old',NULL);");
  try {
    let called = false;
    const options = { tables: ['t'], [field]: field === 'maxRows' ? 1 : field === 'maxCells' ? 3 : 200 };
    await assert.rejects(captureSnapshotChangeset(source, async tx => { called=true; await tx.execute("INSERT INTO t VALUES(2,'new',NULL)"); }, options));
    assert.equal(called, true); assert.equal(source.rows().length, 1);
    await assert.rejects(captureSnapshotChangeset(source, () => { throw new Error('should not start'); }, { ...options, [field]: 1 }), field==='maxRows' ? /should not start/ : /budget/);
  } finally { source.close(); }
});
test('oversized after-image is rejected before value projection transfer', async () => {
  const source = new SqliteTarget(':memory:', ddl + "INSERT INTO t VALUES(1,'old',NULL);");
  try {
    let changed = false, afterValueReads = 0;
    source.before = (kind, sql) => { if(changed && /SELECT typeof\(s\./.test(sql)) afterValueReads++; };
    await assert.rejects(captureSnapshotChangeset(source, async tx => { await tx.execute('UPDATE t SET value=zeroblob(10000)'); changed = true; }, { tables: ['t'], maxBytes: 1024 }));
    assert.equal(afterValueReads, 0); assert.equal(source.rows()[0][1], 'old');
  } finally { source.close(); }
});
test('output codec limits abort callback writes', async () => {
  const source = new SqliteTarget(':memory:', ddl + initial);
  try { await assert.rejects(captureSnapshotChangeset(source, tx=>tx.execute("UPDATE t SET value='changed'"), { tables:['t'],limits:{maxChanges:1}})); assert.equal(source.rows()[0][1], 'old'); }
  finally { source.close(); }
});
for (const extra of ['CREATE TABLE bad(x)', 'CREATE TABLE bad(id PRIMARY KEY,v GENERATED ALWAYS AS(id+1))', 'CREATE VIEW bad AS SELECT * FROM t', 'CREATE TABLE bad(id PRIMARY KEY); INSERT INTO bad VALUES(NULL)']) {
  test(`all-table preflight rejects unsupported schema before callback: ${extra}`, async () => {
    const source=new SqliteTarget(':memory:',ddl+initial+extra);let called=false;
    try { await assert.rejects(captureSnapshotChangeset(source,()=>{called=true;},{tables:['t','bad']}));assert.equal(called,false); }
    finally { source.close(); }
  });
}
test('callback DDL rolls back alongside DML', async () => {
  const source = new SqliteTarget(':memory:', ddl + initial);
  try { await assert.rejects(captureSnapshotChangeset(source,async tx=>{await tx.execute("UPDATE t SET value='new'");await tx.execute('CREATE TEMP TABLE changed(x)');},{tables:['t']}),{code:'ERR_FSQLITE_CAPTURE_SCHEMA'});assert.equal(source.rows()[0][1],'old');assert.deepEqual(source.rows("SELECT name FROM temp.sqlite_schema WHERE name='changed'"),[]); }
  finally { source.close(); }
});
test('cancellation after writes rolls back and no snapshot result escapes', async () => {
  const source = new SqliteTarget(':memory:', ddl + initial), controller=new AbortController();
  try { await assert.rejects(captureSnapshotChangeset(source,async tx=>{await tx.execute("UPDATE t SET value='new'");controller.abort();},{tables:['t'],signal:controller.signal}),{code:'ERR_FSQLITE_CAPTURE_CANCELLED'});assert.equal(source.rows()[0][1],'old'); }
  finally { source.close(); }
});
test('deadline covers callback and both snapshots, rather than resetting between phases', async () => {
  const source = new SqliteTarget(':memory:', ddl + initial);
  try { await assert.rejects(captureSnapshotChangeset(source,async tx=>{await tx.execute("UPDATE t SET value='new'");await new Promise(r=>setTimeout(r,20));},{tables:['t'],timeoutMs:5}),{code:'ERR_FSQLITE_CAPTURE_TIMEOUT'});assert.equal(source.rows()[0][1],'old'); }
  finally { source.close(); }
});
test('admitted unawaited SQL drains before collection and retained executors reject', async () => {
  const source = new SqliteTarget(':memory:', ddl + initial); let saved;
  source.before=async(kind,sql)=>{if(sql.startsWith('UPDATE t'))await new Promise(r=>setTimeout(r,10));};
  try {
    const result=await captureSnapshotChangeset(source,tx=>{saved=tx;void tx.execute("UPDATE t SET value='late' WHERE id=1");return 19;},{tables:['t']});
    assert.equal(result.value,19);assert.equal(result.changes,1);assert.equal(source.rows()[0][1],'late');
    await assert.rejects(saved.execute("UPDATE t SET value='escape'"));assert.equal(source.rows()[0][1],'late');
  } finally { source.close(); }
});
test('SQL errors abort even if callback suppresses a rejection', async () => {
  const source = new SqliteTarget(':memory:', ddl + initial);
  try { await assert.rejects(captureSnapshotChangeset(source,async tx=>{await tx.execute("UPDATE t SET value='new'");await tx.execute('INSERT INTO t(id) VALUES(1)').catch(()=>{});},{tables:['t']}));assert.equal(source.rows()[0][1],'old'); }
  finally { source.close(); }
});
test('file-backed concurrent writer cannot splice its rows into capture; stale source write conflicts', async () => {
  const path=join(mkdtempSync(join(tmpdir(),'fsqlite-snapshot-capture-')),'source.db');
  const source=new SqliteTarget(path,ddl+initial+'PRAGMA journal_mode=WAL;'),peer=new SqliteTarget(path);
  try {
    const readOnly=await captureSnapshotChangeset(source,async()=>{await peer.execute("INSERT INTO t VALUES(3,'peer',NULL)");},{tables:['t']});
    assert.equal(readOnly.changes,0);assert.equal(readOnly.afterRows,2);assert.equal(source.rows().length,3);
    await assert.rejects(captureSnapshotChangeset(source,async tx=>{await peer.execute("UPDATE t SET extra=4 WHERE id=2");await tx.execute("UPDATE t SET value='stale' WHERE id=1");},{tables:['t']}));
    assert.equal(source.rows()[0][1],'old');
  } finally { source.close();peer.close(); }
});
for (let seed=1;seed<=12;seed++) test(`native Session trigger workload ${seed}`,async()=>{
  await oracle(async tx=>{for(let i=0;i<12;i++){const id=BigInt((seed*17+i*13)%19+1);await tx.execute('INSERT INTO t(id,value,extra) VALUES(?,?,?) ON CONFLICT(id) DO UPDATE SET value=excluded.value,extra=excluded.extra',[id,`v-${i}`,BigInt(i)]);if(i%4===0)await tx.execute('DELETE FROM t WHERE id=?',[id]);}},
    ddl,initial,"CREATE TRIGGER normalize AFTER INSERT ON t BEGIN UPDATE t SET value=upper(NEW.value) WHERE id=NEW.id; END;");
});
test('cascading foreign keys and trigger-generated audit rows are captured together', async () => {
  const base='CREATE TABLE p(id INTEGER PRIMARY KEY);CREATE TABLE c(id INTEGER PRIMARY KEY,p INTEGER);CREATE TABLE audit(id INTEGER PRIMARY KEY,v);';
  const sourceSchema='CREATE TABLE p(id INTEGER PRIMARY KEY);CREATE TABLE c(id INTEGER PRIMARY KEY,p INTEGER REFERENCES p ON DELETE CASCADE);CREATE TABLE audit(id INTEGER PRIMARY KEY,v);';
  const seed='INSERT INTO p VALUES(1);INSERT INTO c VALUES(10,1),(11,1);';
  const source=new SqliteTarget(':memory:',sourceSchema+seed+"CREATE TRIGGER log AFTER DELETE ON c BEGIN INSERT INTO audit VALUES(OLD.id,'gone'); END;"),target=new SqliteTarget(':memory:',base+seed);
  const session=source.db.createSession();
  try {
    const result=await captureSnapshotChangeset(source,tx=>tx.execute('DELETE FROM p WHERE id=1'),{tables:['c','p','audit']});
    assert.equal(result.changes,5);assert.deepEqual(normalized(result.changeset),normalized(session.changeset()));
    await applyChangeset(target,result.changeset,{tables:['c','p','audit']});
    for(const t of ['c','p','audit'])assert.deepEqual(target.rows(`SELECT * FROM ${t} ORDER BY 1`),source.rows(`SELECT * FROM ${t} ORDER BY 1`));
  } finally {session.close();source.close();target.close();}
});
test('TEMP application triggers are observed without installing capture triggers', async()=>{
  await oracle(tx=>tx.execute("INSERT INTO t VALUES(9,'temp',NULL)"),ddl,initial,
    'CREATE TEMP TRIGGER tr AFTER INSERT ON main.t BEGIN UPDATE t SET extra=NEW.id WHERE id=1; END;');
});
test('bounded pages traverse a larger mixed-direction table around a small triggered mutation', async()=>{
  const schema='CREATE TABLE t(a INTEGER,b TEXT COLLATE NOCASE,v,PRIMARY KEY(a DESC,b ASC)) WITHOUT ROWID;';
  const db=new SqliteTarget(':memory:',schema);
  try {
    db.db.exec('BEGIN');const insert=db.db.prepare('INSERT INTO t VALUES(?,?,?)');for(let i=0;i<2048;i++)insert.run(i%64,String(i).padStart(5,'0'),'v');db.db.exec('COMMIT');
    db.db.exec("CREATE TRIGGER tr AFTER UPDATE ON t WHEN NEW.v='new' BEGIN UPDATE t SET v='effect' WHERE a=0; END;");
    let maxPage=0;db.after=(kind,sql,params,result)=>{if(sql.startsWith('SELECT typeof(s.'))maxPage=Math.max(maxPage,result.rowArrays.length);};
    const result=await captureSnapshotChangeset(db,tx=>tx.execute("UPDATE t SET v='new' WHERE a=1 AND b='00001'"),{tables:['t'],maxRows:3000});
    assert.equal(result.beforeRows,2048);assert.equal(result.afterRows,2048);assert.equal(result.changes,33);assert.ok(maxPage<=32);
  } finally {db.close();}
});
test('nested owner rolls snapshot-captured writes back with its outer decision', async()=>{
  const source=new SqliteTarget(':memory:',ddl+initial);
  try {
    await assert.rejects(source.transaction(async tx=>{
      const child={transaction:async work=>{await tx.execute('SAVEPOINT scope');try{const r=await work(tx);await tx.execute('RELEASE scope');return r;}catch(e){await tx.execute('ROLLBACK TO scope');await tx.execute('RELEASE scope');throw e;}}};
      const result=await captureSnapshotChangeset(child,t=>t.execute("UPDATE t SET value='provisional' WHERE id=1"),{tables:['t']});
      assert.equal(result.changes,1);throw new Error('outer rollback');
    }),/outer rollback/);
    assert.equal(source.rows()[0][1],'old');
  } finally {source.close();}
});
