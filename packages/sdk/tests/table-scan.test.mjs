// Production SDK/worker, SQLite references. Not a FrankenSQLite WASM certificate.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { FrankenDB } from '../src/database.ts';
import { captureTableScan, createTablePageReader } from '../src/table-scan.ts';
import { sqliteSnapshotWorker } from '../../worker/tests/helpers/snapshot-sqlite-core.mjs';
import { sqliteBindingFixture } from '../../worker/tests/helpers/bindings-core.mjs';
const limits = { timeout: 20000 };
const hasCode = code => error => error?.code === code || error?.cause?.code === code || error?.errors?.some(hasCode(code));
async function fixture(t) {
  const f = sqliteSnapshotWorker();
  const db = await FrankenDB.open({worker:f.worker});
  t.after(() => db.close().catch(() => {}));
  return {...f, db};
}
async function all(db, table, options = {}) {
  const settings = captureTableScan(table, options);
  return db.transaction(async tx => {
    const reader = await createTablePageReader(tx, settings), rows = [], sizes = [];
    while (!reader.exhausted) { const page = await reader.read(); sizes.push(page.length); rows.push(...page); }
    const queries = reader.queries;
    assert.deepEqual(await reader.read(), []); assert.equal(reader.queries, queries);
    return {rows, sizes, queries, columns:reader.columns};
  });
}
const dataQueries = f => f.worker.requests.filter(r=>r.kind==='query' && r.sql.startsWith('SELECT s.'));

test('scan pages: 10001 rows in bounded rowid pages, generated columns and exact order without OFFSET', limits, async t => {
  const f = await fixture(t);
  await f.db.executeBatch('CREATE TABLE items(id INTEGER PRIMARY KEY,value TEXT,g TEXT GENERATED ALWAYS AS (value || \'!\') VIRTUAL);'+
    "WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<10001) INSERT INTO items(id,value) SELECT x,printf('row-%d',x) FROM n;");
  const result = await all(f.db,'items',{batchSize:127});
  assert.equal(result.rows.length,10001); assert.equal(result.queries,79);
  assert.deepEqual(result.columns,['id','value','g']);
  for (let i=0;i<result.rows.length;i++) assert.deepEqual(result.rows[i],{id:i+1,value:`row-${i+1}`,g:`row-${i+1}!`});
  assert.ok(result.sizes.every(n=>n<=127));
  const queries=dataQueries(f);assert.equal(queries.length,79);
  for(const q of queries){assert.ok(q.sql.endsWith('LIMIT ?'));assert.ok(!q.sql.includes('OFFSET'));assert.equal(q.params.at(-1),127);}
  assert.ok(queries.slice(1).every(q=>q.sql.includes(' WHERE s."_rowid_" > ?')));
  const native=new DatabaseSync(f.db.path);t.after(()=>native.close());
  for(const q of [queries[1],queries.at(-1)]) {
    const plan=native.prepare('EXPLAIN QUERY PLAN '+q.sql).all(...q.params).map(row=>row.detail).join(';');
    assert.match(plan,/SEARCH s USING INTEGER PRIMARY KEY/);assert.doesNotMatch(plan,/TEMP B-TREE/);
  }
  assert.deepEqual(native.prepare('PRAGMA integrity_check').all().map(Object.values),[['ok']]);
});

test('scan pages: empty, single-row, exact-sized final page and reversed rowid ranges',limits,async t=>{
  const {db}=await fixture(t);await db.execute('CREATE TABLE items(id INTEGER PRIMARY KEY)');
  assert.deepEqual((await all(db,'items',{batchSize:2})).sizes,[0]);
  await db.execute('INSERT INTO items VALUES(-2)');assert.deepEqual((await all(db,'items',{batchSize:2})).sizes,[1]);
  await db.execute('INSERT INTO items VALUES(0),(7),(9)');
  const result=await all(db,'items',{batchSize:2,reverse:true});
  assert.deepEqual(result.rows,[{id:9},{id:7},{id:0},{id:-2}]);assert.deepEqual(result.sizes,[2,2,0]);
});

test('scan pages: aliases cannot overwrite user data and projections are canonical validated identifiers',limits,async t=>{
  const {db}=await fixture(t);
  await db.execute('CREATE TABLE items(id INTEGER PRIMARY KEY,"__fsqlite_scan_key_0", "__proto__", "constructor",data BLOB)');
  await db.execute("INSERT INTO items VALUES(1,'visible','ordinary',NULL,x'00ff')");
  const result=await all(db,'ITEMS',{columns:['DATA','__PROTO__','__fsqlite_scan_key_0','constructor'],batchSize:1});
  assert.deepEqual(result.columns,['data','__proto__','__fsqlite_scan_key_0','constructor']);
  assert.ok(Object.hasOwn(result.rows[0],'__proto__'));assert.equal(result.rows[0].__proto__,'ordinary');
  assert.deepEqual(result.rows[0].data,Uint8Array.of(0,255));assert.equal(result.rows[0].__fsqlite_scan_key_0,'visible');
  await assert.rejects(all(db,'items',{columns:['missing']}),hasCode('ERR_FSQLITE_SCAN_INPUT'));
});

test('scan pages: signed 64-bit rowids including both extremes are bound without arithmetic or rounding',limits,async t=>{
  const f=sqliteBindingFixture();t.after(()=>f.shutdown());
  const db=await FrankenDB.open({worker:f.worker});t.after(()=>db.close());
  await db.execute('CREATE TABLE items(id INTEGER PRIMARY KEY,value)');
  const keys=[-(2n**63n),-9007199254740993n,-1n,0n,9007199254740993n,2n**63n-1n];
  for(const key of keys)await db.execute('INSERT INTO items VALUES(?,?)',[key,String(key)]);
  assert.deepEqual((await all(db,'items',{batchSize:1})).rows.map(r=>BigInt(r.id)),keys);
  assert.deepEqual((await all(db,'items',{batchSize:2,reverse:true})).rows.map(r=>BigInt(r.id)),keys.toReversed());
});

test('scan pages: declared rowid names select an unshadowed hidden alias and all-shadowed tables reject',limits,async t=>{
  const {db}=await fixture(t);
  await db.execute('CREATE TABLE items("_ROWID_" TEXT,"rowid" TEXT,value)');
  await db.execute("INSERT INTO items VALUES('z','z',1),('a','a',2)");
  assert.deepEqual((await all(db,'items',{columns:['value'],batchSize:1})).rows,[{value:1},{value:2}]);
  await db.execute('CREATE TABLE blocked(rowid, _rowid_, oid, value)');
  await assert.rejects(all(db,'blocked'),hasCode('ERR_FSQLITE_SCAN_SCHEMA'));
});

test('scan pages: INTEGER PRIMARY KEY DESC uses the real rowid, not an incorrectly inferred key alias',limits,async t=>{
  const {db}=await fixture(t);await db.execute('CREATE TABLE items(id INTEGER PRIMARY KEY DESC,value)');
  await db.execute("INSERT INTO items VALUES(7,'first'),(2,'second'),(NULL,'third')");
  assert.deepEqual((await all(db,'items',{batchSize:1})).rows.map(row=>row.value),['first','second','third']);
});

test('scan pages: WITHOUT ROWID mixed-direction composite keys match independent SQL, seeking every continuation',limits,async t=>{
  const f=await fixture(t);
  await f.db.execute('CREATE TABLE items(a TEXT COLLATE NOCASE,b INTEGER,c TEXT COLLATE RTRIM,value,PRIMARY KEY(a DESC,b ASC,c DESC)) WITHOUT ROWID');
  await f.db.executeBatch("WITH RECURSIVE n(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM n WHERE x<1000) "+
    "INSERT INTO items SELECT CASE x%4 WHEN 0 THEN 'A' WHEN 1 THEN 'b' WHEN 2 THEN 'C' ELSE 'd' END,x%17,printf('%06d ',x),x FROM n;");
  const native=new DatabaseSync(f.db.path);t.after(()=>native.close());
  const expected=native.prepare('SELECT a,b,c,value FROM items ORDER BY a COLLATE NOCASE DESC,b ASC,c COLLATE RTRIM DESC').all().map(r=>({...r}));
  const result=await all(f.db,'items',{batchSize:13});assert.deepEqual(result.rows,expected);
  assert.ok(result.queries<=Math.ceil(1001/13)*3+3);
  for(const q of dataQueries(f).filter(q=>q.sql.includes(' WHERE '))) {
    const plan=native.prepare('EXPLAIN QUERY PLAN '+q.sql).all(...q.params).map(row=>row.detail).join(';');
    assert.match(plan,/SEARCH s USING PRIMARY KEY/);assert.doesNotMatch(plan,/TEMP B-TREE/);
    assert.ok(!q.sql.includes(' OR ')&&!q.sql.includes('OFFSET'));assert.ok(q.params.at(-1)<=13);
  }
  assert.deepEqual((await all(f.db,'items',{batchSize:16,reverse:true})).rows,expected.toReversed());
});

test('scan pages: composite blob and numeric keys retain SQLite storage-class ordering',limits,async t=>{
  const {db}=await fixture(t);
  await db.execute('CREATE TABLE items(a,b,c,value,PRIMARY KEY(a,b DESC,c)) WITHOUT ROWID');
  const values=[[2,1,'a','integer'],[2.5,1,'a','real'],['2',1,'a','text'],['a',0,'a','one'],['a',2,'a','two'],[Uint8Array.of(0),1,'a','blob0'],[Uint8Array.of(255),1,'a','blobff']];
  for(const row of values)await db.execute('INSERT INTO items VALUES(?,?,?,?)',row);
  const expected=(await db.query('SELECT a,b,c,value FROM items ORDER BY a,b DESC,c')).rows;
  assert.deepEqual((await all(db,'items',{batchSize:1})).rows,expected);
});

test('scan pages: consumer mutation of projected primary-key blobs never changes the continuation key',limits,async t=>{
  const {db}=await fixture(t);await db.execute('CREATE TABLE items(k BLOB PRIMARY KEY,value) WITHOUT ROWID');
  await db.execute("INSERT INTO items VALUES(x'01','a'),(x'02','b'),(x'03','c')");
  await db.transaction(async tx=>{
    const reader=await createTablePageReader(tx,captureTableScan('items',{batchSize:1}));
    const first=await reader.read();assert.equal(first[0].value,'a');first[0].k.fill(255);
    assert.equal((await reader.read())[0].value,'b');assert.equal((await reader.read())[0].value,'c');
  });
});

test('scan pages: quoted table and column names are never treated as SQL fragments',limits,async t=>{
  const {db}=await fixture(t);const table='a\'"; DROP TABLE other;--';
  await db.execute('CREATE TABLE other(id)');
  await db.execute(`CREATE TABLE "${table.replaceAll('"','""')}"("v\"\";--",id INTEGER PRIMARY KEY)`);
  await db.execute(`INSERT INTO "${table.replaceAll('"','""')}" VALUES(7,1)`);
  assert.deepEqual((await all(db,table,{columns:['v";--']})).rows,[{'v";--':7}]);
  assert.deepEqual((await db.query('SELECT * FROM other')).rowArrays,[]);
});

test('scan pages: views, virtual/shadow tables and absent tables reject before a data query',limits,async t=>{
  const f=await fixture(t);await f.db.executeBatch('CREATE TABLE items(id);CREATE VIEW v AS SELECT * FROM items;CREATE VIRTUAL TABLE f USING fts5(body);');
  for(const table of ['v','f','f_data','missing'])await assert.rejects(all(f.db,table),hasCode('ERR_FSQLITE_SCAN_SCHEMA'));
  assert.equal(dataQueries(f).length,0);
});

test('scan pages: capture bounds and snapshots columns without invoking an array iterator',()=>{
  const columns=['Value'];columns[Symbol.iterator]=()=>{throw Error('iterator');};
  const captured=captureTableScan('items',{columns,batchSize:1});columns[0]='changed';
  assert.deepEqual(captured.columns,['Value']);assert.ok(Object.isFrozen(captured.columns));
  for(const options of [{batchSize:0},{batchSize:4097},{batchSize:1.5},{reverse:'yes'},{columns:[]},{columns:['id','ID']},{columns:['x\0']},{columns:new Array(1025)}])
    assert.throws(()=>captureTableScan('items',options),hasCode('ERR_FSQLITE_SCAN_INPUT'));
  for(const table of ['','x\0','x'.repeat(1025),'sqlite_master',null])assert.throws(()=>captureTableScan(table),hasCode('ERR_FSQLITE_SCAN_INPUT'));
});

test('scan pages: malformed/unsupported metadata fails closed rather than falling back to unbounded SELECT',limits,async t=>{
  const f=await fixture(t);await f.db.execute('CREATE TABLE items(id)');
  const core=f.handles[0], query=core.query.bind(core);
  core.query=async sql=>sql.startsWith('PRAGMA main.table_list')?{columns:[],columnCount:0,columnTypes:[],rows:[],rowArrays:[],changes:0}:query(sql);
  await assert.rejects(all(f.db,'items'),hasCode('ERR_FSQLITE_SCAN_SCHEMA'));assert.equal(dataQueries(f).length,0);
});

test('scan pages: oversized and non-progressing core results reject without transparent replay',limits,async t=>{
  const f=await fixture(t);await f.db.executeBatch('CREATE TABLE items(id INTEGER PRIMARY KEY);INSERT INTO items VALUES(1),(2);');
  const core=f.handles[0], query=core.queryWithParams.bind(core);let first;
  core.queryWithParams=async(sql,params)=>{
    const result=await query(sql,params);
    if(sql.startsWith('SELECT s.')){if(!first)first=result;else return first;}
    return result;
  };
  await assert.rejects(all(f.db,'items',{batchSize:1}),hasCode('ERR_FSQLITE_SCAN_RESULT'));
  assert.equal(dataQueries(f).length,2);
  core.queryWithParams=async(sql,params)=>query(sql,sql.startsWith('SELECT s.')?[...params.slice(0,-1),2]:params);
  await assert.rejects(all(f.db,'items',{batchSize:1}),hasCode('ERR_FSQLITE_SCAN_RESULT'));
});
