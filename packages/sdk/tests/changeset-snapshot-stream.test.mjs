import assert from 'node:assert/strict';
import { test } from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { streamSnapshotChangesets, snapshotChangeset, captureChangeset } from '../src/changeset-capture.ts';
import { decodeChangeset, invertChangeset } from '../src/changeset-codec.ts';

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
