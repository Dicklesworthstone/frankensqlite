import assert from 'node:assert/strict';
import { test } from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { snapshotChangeset, captureChangeset } from '../src/changeset-capture.ts';
import { decodeChangeset, invertChangeset } from '../src/changeset-codec.ts';

// Real SQLite SQL/session oracle. Hooks inject interleavings or corrupt adapter
// replies; none of the SQL or binary changeset semantics are mocked.
class Target {
  constructor(ddl = '', path = ':memory:') {
    this.db = new DatabaseSync(path); this.db.exec(ddl);
    this.depth = 0; this.serial = 0; this.log = []; this.hook = undefined;
  }
  async execute(sql, params = []) {
    this.log.push(sql); return Number(this.db.prepare(sql).run(...params).changes);
  }
  async query(sql, params = []) {
    this.log.push(sql);
    const stmt = this.db.prepare(sql); stmt.setReadBigInts(true); stmt.setReturnArrays(true);
    const result = { rowArrays: stmt.all(...params) };
    await this.hook?.(sql, params, result);
    return result;
  }
  async transaction(work) {
    const nested = this.depth++ !== 0, name = `snapshot_test_${++this.serial}`;
    this.db.exec(nested ? `SAVEPOINT ${name}` : 'BEGIN');
    try { const result = await work(this); this.db.exec(nested ? `RELEASE ${name}` : 'COMMIT'); return result; }
    catch (error) {
      try { this.db.exec(nested ? `ROLLBACK TO ${name}; RELEASE ${name}` : 'ROLLBACK'); }
      catch (cleanup) { throw new AggregateError([error, cleanup], 'Test transaction and rollback failed'); }
      throw error;
    } finally { this.depth--; }
  }
  close() { this.db.close(); }
}
const quote = s => `"${s.replaceAll('"','""')}"`;
function rows(db, table) {
  const s = db.prepare(`SELECT * FROM ${quote(table)} ORDER BY 1,2`);
  s.setReadBigInts(true); s.setReturnArrays(true); return s.all();
}
function normalized(bytes) {
  return decodeChangeset(bytes).flatMap(t => t.changes.map(c => ({ table:t.name, pk:t.primaryKey, ...c })))
    .map(c => JSON.stringify(c, (_,v) => typeof v === 'bigint' ? `${v}n` : v)).sort();
}
const images = sql => sql.startsWith('SELECT typeof(') && sql.includes(' ORDER BY ');
async function oracle(ddl, seed, options = {}) {
  const source = new Target(ddl), receiver = new Target(ddl), session = source.db.createSession();
  try {
    seed(source.db);
    const tables = options.tables ?? ['t'], before = tables.map(t => rows(source.db,t));
    const native = session.changeset();
    const snapshot = await snapshotChangeset(source, { tables, ...options });
    assert.equal(receiver.db.applyChangeset(snapshot.changeset), true);
    assert.deepEqual(tables.map(t => rows(receiver.db,t)), before);
    assert.deepEqual(tables.map(t => rows(source.db,t)), before);
    assert.deepEqual(normalized(snapshot.changeset), normalized(native));
    assert.equal(snapshot.changes, before.reduce((n,r) => n+r.length,0));
    assert.equal(source.db.prepare("SELECT count(*) AS n FROM temp.sqlite_schema").get().n, 0);
    assert(source.log.every(sql => /^(SELECT|PRAGMA) /.test(sql)), 'snapshot must be read-only');
    assert.equal(receiver.db.applyChangeset(invertChangeset(snapshot.changeset)), true);
    assert(tables.every(t => rows(receiver.db,t).length === 0));
    return snapshot;
  } finally { session.close(); receiver.close(); source.close(); }
}
for (const n of [0,1,31,32,33,64,65,127]) test(`rowid snapshot/native session parity across ${n} rows`, async () => {
  await oracle('CREATE TABLE t(id INTEGER PRIMARY KEY, value)', db => {
    const s=db.prepare('INSERT INTO t VALUES(?,?)');
    for(let i=0;i<n;i++) s.run(BigInt(i)-70n, `value-${i}`);
  });
});
for (const suffix of ['', ' WITHOUT ROWID']) test(`mixed index order and explicit index collations${suffix}`, async () => {
  await oracle('CREATE TABLE t(a TEXT COLLATE NOCASE,b INTEGER,c BLOB,v,PRIMARY KEY(b DESC,a COLLATE RTRIM ASC,c DESC))'+suffix, db => {
    const s=db.prepare('INSERT INTO t VALUES(?,?,?,?)');
    for(let b=6;b>=0;b--) for(let a=5;a>=0;a--) for(let c=4;c>=0;c--) s.run(`A${a}`,b,new Uint8Array([c]),`${b}:${a}:${c}`);
  });
});
for (const encoding of ['UTF-8','UTF-16le','UTF-16be']) test(`typed values survive ${encoding} snapshot and native apply`, async () => {
  await oracle(`PRAGMA encoding='${encoding}'; CREATE TABLE t(id INTEGER PRIMARY KEY, a, b, c, d)`, db => {
    db.prepare('INSERT INTO t VALUES(?,?,?,?,?)').run(-(1n<<63n),9007199254740993n, '\uFEFFa\0😀é',new Uint8Array([0,255]),null);
    db.exec("INSERT INTO t VALUES(9223372036854775807,CAST(2 AS REAL),1e999,-1e999,X'')");
  });
});
test('mixed storage classes in a non-affinity primary key paginate without coercion', async () => {
  await oracle('CREATE TABLE t(k BLOB PRIMARY KEY, v) WITHOUT ROWID', db => {
    const s=db.prepare('INSERT INTO t VALUES(?,?)');
    for(let i=0;i<80;i++) { s.run(BigInt(i)*3n,`int${i}`); s.run(i*3+0.5,`real${i}`); s.run(String(i),`text${i}`); s.run(new Uint8Array([i]),`blob${i}`); }
  });
});
test('declared INTEGER PRIMARY KEY DESC uses its index, not a presumed rowid alias', async () => {
  await oracle('CREATE TABLE t(id INTEGER PRIMARY KEY DESC, v)', db => {
    const s=db.prepare('INSERT INTO t VALUES(?,?)'); for(let i=0;i<83;i++) s.run(i,`r${i}`);
  });
});
test('tables and columns with quotes and shadowed rowid names stay in main', async () => {
  const ddl='CREATE TABLE "weird\'table" ("rowid" TEXT, "_rowid_" TEXT, "oid" TEXT, "i""d" INTEGER PRIMARY KEY)';
  await oracle(ddl, db => { db.exec(`INSERT INTO "weird'table" VALUES('a','b','c',5)`); }, {tables:["weird'table"]});
  const s=new Target('CREATE TABLE t(id INTEGER PRIMARY KEY,v);INSERT INTO t VALUES(1,2);CREATE TEMP TABLE t(id,value,other)');
  try { assert.equal((await snapshotChangeset(s,{tables:['T']})).changes,1); } finally{s.close();}
});
test('snapshot permits source triggers, needs no recursive-trigger policy and fires no writes', async () => {
  const s=new Target("CREATE TABLE t(id INTEGER PRIMARY KEY,v);CREATE TABLE audit(n);INSERT INTO t VALUES(1,'v');CREATE TRIGGER tap AFTER UPDATE ON t BEGIN INSERT INTO audit VALUES(1);END");
  try {
    s.db.exec('PRAGMA recursive_triggers=OFF;PRAGMA query_only=ON');
    assert.equal((await snapshotChangeset(s,{tables:['t']})).changes,1);
    assert.equal(s.db.prepare('SELECT count(*) AS n FROM audit').get().n,0);
    assert.equal(s.db.prepare('PRAGMA recursive_triggers').get().recursive_triggers,0);
  } finally{s.close();}
});
test('parent-first multi-table seed includes existing data, not empty table headers', async () => {
  await oracle('PRAGMA foreign_keys=ON;CREATE TABLE p(id INTEGER PRIMARY KEY,v);CREATE TABLE t(id INTEGER PRIMARY KEY,p REFERENCES p(id));CREATE TABLE empty(id INTEGER PRIMARY KEY,v)', db => {
    db.exec("INSERT INTO p VALUES(1,'p');INSERT INTO t VALUES(2,1)");
  },{tables:['p','empty','t']});
});
for(const ddl of ['CREATE TABLE t(id,v)', 'CREATE TABLE t(id PRIMARY KEY,v GENERATED ALWAYS AS(id+1))',
  'CREATE VIEW t AS SELECT 1 AS id,2 AS v', 'CREATE VIRTUAL TABLE t USING fts5(v)',
  'CREATE TABLE t(id PRIMARY KEY,v);INSERT INTO t VALUES(NULL,5)',
  'CREATE TABLE t(a,b,v,PRIMARY KEY(a,b));INSERT INTO t VALUES(1,NULL,5)']) {
  test(`unsupported snapshot schema fails closed: ${ddl}`, async()=>{
    const s=new Target(ddl); try { await assert.rejects(snapshotChangeset(s,{tables:['t']}),{code:'ERR_FSQLITE_CAPTURE_SCHEMA'}); assert(!s.log.some(images)); } finally{s.close();}
  });
}
test('all schemas preflight before any row image, and duplicate/system names reject before admission',async()=>{
  const s=new Target('CREATE TABLE t(id INTEGER PRIMARY KEY,v);INSERT INTO t VALUES(1,2)');
  try{
    await assert.rejects(snapshotChangeset(s,{tables:['t','missing']}),{code:'ERR_FSQLITE_CAPTURE_SCHEMA'});assert(!s.log.some(images));
    for(const tables of [[],['t','T'],['sqlite_schema'],['__fsqlite_changeset_outbox']]){
      s.log=[];await assert.rejects(snapshotChangeset(s,{tables}),{code:'ERR_FSQLITE_CAPTURE_INPUT'});assert.equal(s.log.length,0);
    }
  }finally{s.close();}
});
for(const opts of [{maxRows:1},{maxCells:3},{maxBytes:100}]) test(`snapshot budget rejects before row-image transfer: ${JSON.stringify(opts)}`,async()=>{
  const s=new Target("CREATE TABLE t(id INTEGER PRIMARY KEY,v);INSERT INTO t VALUES(1,'x'),(2,'y')");
  try{await assert.rejects(snapshotChangeset(s,{tables:['t'],...opts}),{code:'ERR_FSQLITE_CAPTURE_LIMIT'});assert(!s.log.some(images));}finally{s.close();}
});
test('oversized individual postimage is length-checked before transferring its bytes',async()=>{
  const s=new Target("CREATE TABLE t(id INTEGER PRIMARY KEY,v);INSERT INTO t VALUES(1,zeroblob(1000000))");
  try{await assert.rejects(snapshotChangeset(s,{tables:['t'],maxBytes:1024}),{code:'ERR_FSQLITE_CAPTURE_LIMIT'});assert(!s.log.some(images));}finally{s.close();}
});
test('budgets are global across tables; wire budget remains independent',async()=>{
  const s=new Target('CREATE TABLE t(id INTEGER PRIMARY KEY,v);CREATE TABLE u(id INTEGER PRIMARY KEY,v);INSERT INTO t VALUES(1,2);INSERT INTO u VALUES(1,2)');
  try{
    await assert.rejects(snapshotChangeset(s,{tables:['t','u'],maxRows:1}),{code:'ERR_FSQLITE_CAPTURE_LIMIT'});
    await assert.rejects(snapshotChangeset(s,{tables:['t'],limits:{maxBytes:10}}),{code:'ERR_FSQLITE_CHANGESET_LIMIT'});
  }finally{s.close();}
});
test('options and table list are captured before asynchronous transaction admission',async()=>{
  const s=new Target('CREATE TABLE t(id INTEGER PRIMARY KEY,v);INSERT INTO t VALUES(1,2)');
  try{const tables=['t'],limits={maxBytes:1000};const p=snapshotChangeset(s,{tables,limits});tables[0]='missing';limits.maxBytes=1;assert.equal((await p).changes,1);}finally{s.close();}
});
test('pre-cancelled and mid-read cancelled snapshots reject and drain transaction',async()=>{
  const s=new Target('CREATE TABLE t(id INTEGER PRIMARY KEY,v);INSERT INTO t VALUES(1,2)');
  try{
    const before=AbortSignal.abort(new Error('before'));await assert.rejects(snapshotChangeset(s,{tables:['t'],signal:before}),{code:'ERR_FSQLITE_CAPTURE_CANCELLED'});assert.equal(s.log.length,0);
    const controller=new AbortController();s.hook=sql=>{if(sql.includes(' ORDER BY '))controller.abort();};
    await assert.rejects(snapshotChangeset(s,{tables:['t'],signal:controller.signal}),{code:'ERR_FSQLITE_CAPTURE_CANCELLED'});assert.equal(s.depth,0);assert(!s.log.some(images));
  }finally{s.close();}
});
test('deadline is monotonic through awaited reads',async()=>{
  const s=new Target('CREATE TABLE t(id INTEGER PRIMARY KEY,v)');
  try{s.hook=()=>new Promise(r=>setTimeout(r,15));await assert.rejects(snapshotChangeset(s,{tables:['t'],timeoutMs:5}),{code:'ERR_FSQLITE_CAPTURE_TIMEOUT'});assert.equal(s.depth,0);}finally{s.close();}
});
test('a file-backed snapshot retains one view across pages and tables despite another commit',async()=>{
  const path=join(mkdtempSync(join(tmpdir(),'fsqlite-snapshot-')),'db.sqlite');
  const s=new Target('PRAGMA journal_mode=WAL;CREATE TABLE t(id INTEGER PRIMARY KEY,v);CREATE TABLE u(id INTEGER PRIMARY KEY,v);INSERT INTO u VALUES(1,10)',path),writer=new DatabaseSync(path);
  try{
    for(let i=0;i<90;i++)s.db.prepare('INSERT INTO t VALUES(?,?)').run(i,i);
    const expected=[rows(s.db,'t'),rows(s.db,'u')];let wrote=false;
    s.hook=sql=>{if(images(sql)&&!wrote){wrote=true;writer.exec('BEGIN;UPDATE t SET v=999 WHERE id>=32;INSERT INTO t VALUES(99,99);UPDATE u SET v=999;COMMIT');}};
    const result=await snapshotChangeset(s,{tables:['t','u']});assert(wrote);
    const r=new DatabaseSync(':memory:');try{r.exec('CREATE TABLE t(id INTEGER PRIMARY KEY,v);CREATE TABLE u(id INTEGER PRIMARY KEY,v)');assert(r.applyChangeset(result.changeset));assert.deepEqual([rows(r,'t'),rows(r,'u')],expected);}finally{r.close();}
    assert.equal(s.db.prepare('SELECT v FROM u').get().v,999);
  }finally{writer.close();s.close();}
});
test('ten-thousand-row mixed-key snapshot uses indexed continuation, not OFFSET or sorting',async()=>{
  const s=new Target('CREATE TABLE t(a INTEGER,b TEXT COLLATE NOCASE,v,PRIMARY KEY(a DESC,b ASC)) WITHOUT ROWID');
  try{
    s.db.exec('BEGIN');const stmt=s.db.prepare('INSERT INTO t VALUES(?,?,?)');for(let i=0;i<10000;i++)stmt.run(i%100,`key${Math.floor(i/100).toString().padStart(3,'0')}`,i);s.db.exec('COMMIT');
    let pages=0;s.hook=(sql,params,result)=>{if(!sql.includes(' ORDER BY '))return;pages++;assert(result.rowArrays.length<=32);assert(!sql.includes('OFFSET'));const plan=s.db.prepare('EXPLAIN QUERY PLAN '+sql).all(...params);assert(!plan.some(r=>r.detail.includes('TEMP B-TREE')),JSON.stringify(plan));};
    assert.equal((await snapshotChangeset(s,{tables:['t']})).changes,10000);assert(pages>600);
  }finally{s.close();}
});
test('snapshot result validation rejects oversized, inconsistent and repeating adapter pages',async()=>{
  for(const corrupt of ['sizes','width','duplicate','missing']){
    const s=new Target('CREATE TABLE t(id INTEGER PRIMARY KEY,v);INSERT INTO t VALUES(1,2),(2,3)');
    try{s.hook=(sql,_params,r)=>{
      if(sql.startsWith('SELECT 96')&&corrupt==='sizes')r.rowArrays=Array.from({length:33},()=>[100n]);
      if(images(sql)){if(corrupt==='width')r.rowArrays[0]=[];if(corrupt==='duplicate')r.rowArrays[1]=r.rowArrays[0];if(corrupt==='missing')r.rowArrays.pop();}
    };await assert.rejects(snapshotChangeset(s,{tables:['t']}),{code:'ERR_FSQLITE_CAPTURE_RESULT'});}finally{s.close();}
  }
});
test('existing mutation capture still coalesces, rolls back and leaves no TEMP artifacts',async()=>{
  const s=new Target("PRAGMA recursive_triggers=ON;CREATE TABLE t(id INTEGER PRIMARY KEY,v);INSERT INTO t VALUES(1,'old')");
  try{
    const r=await captureChangeset(s,async tx=>{await tx.execute("UPDATE t SET v='middle'");await tx.execute("UPDATE t SET v='last'");await tx.execute("INSERT INTO t VALUES(2,'gone')");await tx.execute('DELETE FROM t WHERE id=2');},{tables:['t']});
    assert.equal(r.changes,1);assert.equal(r.touchedRows,2);assert.equal(decodeChangeset(r.changeset)[0].changes[0].operation,'update');
    await assert.rejects(captureChangeset(s,async tx=>{await tx.execute("UPDATE t SET v='bad'");throw Error('rollback');},{tables:['t']}),/rollback/);
    assert.equal(s.db.prepare('SELECT v FROM t').get().v,'last');assert.equal(s.db.prepare('SELECT count(*) AS n FROM temp.sqlite_schema').get().n,0);
  }finally{s.close();}
});
