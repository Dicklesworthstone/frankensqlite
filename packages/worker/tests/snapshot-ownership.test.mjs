// Production host, storage and ownership code; actual Node SQLite images.
// Web Locks/OPFS/IndexedDB are explicit models, not browser or WASM receipts.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { sqliteSnapshotWorker } from './helpers/snapshot-sqlite-core.mjs';
import { installOpfsModel, deferred } from './helpers/opfs-model.mjs';
import { installIndexedDbModel } from './helpers/indexeddb-model.mjs';

const OWNED = 'ERR_FSQLITE_SNAPSHOT_OWNED';
function fixture(t) {
  const storage = installOpfsModel();
  const idb = installIndexedDbModel();
  const hosts = [];
  // Register one cleanup so stores remain installed until every host closes.
  t.after(async () => {
    for (const f of hosts) await f.close();
    storage.restore();
  });
  function connection(hooks = {}) {
    const f = sqliteSnapshotWorker(hooks);
    let id = 0;
    const request = message => f.host.handle({ ...message, requestId: ++id });
    const result = async message => {
      const response = await request(message);
      if (response.kind === 'error') throw Object.assign(new Error(response.error.message), response.error);
      return response;
    };
    const host = { ...f, request, result,
      init(name = 'db', mode, persistence = 'opfs-snapshot', extra = {}) {
        if (arguments.length < 2) mode = 'exclusive';
        return result({kind:'init', config: {dbName:name, persistence,
          ...(mode === undefined ? {} : {snapshotOwnership:mode}), ...extra}});
      },
      sql: sql => result({kind:'execute-batch', sql}),
      query: async sql => (await result({kind:'query',sql})).data.rows.map(row=>({...row})),
      save: async () => (await result({kind:'checkpoint',publicationId:crypto.randomUUID()})).data,
      close: () => request({kind:'close'}),
    };
    hosts.push(host); return host;
  }
  return {storage,idb,connection};
}

for (const backend of ['opfs-snapshot','indexeddb-snapshot']) {
  test(`${backend}: exclusive ownership rejects competitors before core construction`, async t => {
    const {connection,storage} = fixture(t);
    const a = connection(), b = connection();
    const ready = await a.init('db','exclusive',backend);
    assert.equal(ready.data.snapshotOwnership,'exclusive');
    for (const mode of [undefined,'shared','exclusive']) {
      await assert.rejects(b.init('db',mode,backend),{code:OWNED});
      assert.deepEqual(b.counts(),{creates:0,imports:0});
    }
    assert.equal(storage.sessions.size,1);
    await a.sql('CREATE TABLE items(id INTEGER PRIMARY KEY, body TEXT)');
    await a.sql("INSERT INTO items VALUES(1,'committed')");
    const saved = await a.save();
    await a.close();
    const reopened = await b.init('db','exclusive',backend);
    assert.equal(reopened.data.snapshot.revision,saved.revision);
    assert.deepEqual(await b.query('SELECT * FROM items'),[{id:1,body:'committed'}]);
  });

  test(`${backend}: default/shared sessions coexist but still use snapshot CAS`, async t => {
    const {connection} = fixture(t);
    const a = connection(), b = connection(), c = connection();
    assert.equal((await a.init('db',undefined,backend)).data.snapshotOwnership,'shared');
    await b.init('db','shared',backend);
    await assert.rejects(c.init('db','exclusive',backend),{code:OWNED});
    await a.sql('CREATE TABLE items(id INTEGER PRIMARY KEY)'); await a.save();
    await b.sql('CREATE TABLE items(id INTEGER PRIMARY KEY)');
    await assert.rejects(b.save(),{code:'ERR_FSQLITE_SNAPSHOT_CONFLICT'});
    await a.close(); await assert.rejects(c.init('db','exclusive',backend),{code:OWNED});
    await b.close(); await c.init('db','exclusive',backend);
  });

  test(`${backend}: identical reinitialization transfers rather than releasing ownership`, async t => {
    const {connection,storage} = fixture(t);
    const a = connection(), b = connection();
    await a.init('db','exclusive',backend);
    await a.sql('CREATE TABLE items(id INTEGER PRIMARY KEY)'); await a.save();
    const locksBefore = [...storage.sessions];
    for (let i = 0; i < 3; i++) {
      const ready = await a.init('db','exclusive',backend);
      assert.equal(ready.data.snapshotOwnership,'exclusive');
      assert.deepEqual([...storage.sessions],locksBefore);
      await assert.rejects(b.init('db',undefined,backend),{code:OWNED});
    }
    assert.equal(a.counts().imports,3);
    await a.sql('INSERT INTO items VALUES (2)');
  });

  test(`${backend}: failed import releases a new owner, not a reused live owner`, async t => {
    const {connection,storage} = fixture(t);
    const seed = connection(); await seed.init('db','exclusive',backend);
    await seed.sql('CREATE TABLE items(id INTEGER PRIMARY KEY)'); await seed.save(); await seed.close();
    const hooks = {beforeImport(){throw new Error('import failed');}};
    const a = connection(hooks), b = connection();
    await assert.rejects(a.init('db','exclusive',backend),/import failed/);
    assert.equal(storage.sessions.size,0);
    hooks.beforeImport = undefined;
    await a.init('db','exclusive',backend);
    await a.sql('INSERT INTO items VALUES(9)'); // Deliberately unsaved live SQL.
    hooks.beforeImport = () => {throw new Error('replacement failed');};
    await assert.rejects(a.init('db','exclusive',backend),/replacement failed/);
    assert.equal(storage.sessions.size,1);
    await assert.rejects(b.init('db','exclusive',backend),{code:OWNED});
    assert.deepEqual(await a.query('SELECT id FROM items'),[{id:9}]);
  });

  test(`${backend}: blocked ownership policy changes preserve the old database`, async t => {
    const {connection} = fixture(t);
    const a = connection(); await a.init('db','exclusive',backend);
    await a.sql('CREATE TABLE unsaved(id INTEGER); INSERT INTO unsaved VALUES(7)');
    await assert.rejects(a.init('db','shared',backend),{code:OWNED});
    assert.deepEqual(await a.query('SELECT * FROM unsaved'),[{id:7}]);
    assert.deepEqual(a.counts(),{creates:1,imports:0});
  });

  test(`${backend}: ownership is acquired before asynchronous image import`, async t => {
    const {connection} = fixture(t);
    const seed = connection(); await seed.init('db','exclusive',backend);
    await seed.sql('CREATE TABLE items(id INTEGER)'); await seed.save(); await seed.close();
    const entered=deferred(), finish=deferred();
    const a=connection({beforeImport:async()=>{entered.resolve();await finish.promise;}}),b=connection();
    const opening=a.init('db','exclusive',backend);
    await entered.promise;
    await assert.rejects(b.init('db','exclusive',backend),{code:OWNED});
    assert.deepEqual(b.counts(),{creates:0,imports:0});
    finish.resolve(); await opening;
  });
}

test('different database names and different backends do not share session authority',async t=>{
  const {connection,storage}=fixture(t);
  await connection().init('db','exclusive','opfs-snapshot');
  await connection().init('other','exclusive','opfs-snapshot');
  await connection().init('db','exclusive','indexeddb-snapshot');
  assert.equal(storage.sessions.size,3);
});

test('queued close retains ownership through checkpoint publication and core cleanup',async t=>{
  const {connection,storage}=fixture(t);
  const a=connection(),b=connection(); await a.init();
  await a.sql('CREATE TABLE items(id INTEGER); INSERT INTO items VALUES(1)');
  const entered=deferred(),finish=deferred();
  storage.hooks.beforeClose=async()=>{entered.resolve();await finish.promise;};
  const saving=a.save(); await entered.promise;
  let closed=false; const closing=a.close().then(r=>{closed=true;return r;});
  await assert.rejects(b.init(),{code:OWNED}); assert.equal(closed,false);
  assert.equal(a.events.includes('close'),false);
  finish.resolve(); await saving; assert.equal((await closing).kind,'close-result');
  assert.ok(a.events.includes('free'));
  await b.init(); assert.deepEqual(await b.query('SELECT * FROM items'),[{id:1}]);
});

test('queued close retains ownership while an earlier SQL operation is outstanding',async t=>{
  const {connection}=fixture(t);
  const entered=deferred(),finish=deferred();
  const a=connection({beforeExecute:async sql=>{if(sql.startsWith('INSERT')){entered.resolve();await finish.promise;}}});
  const b=connection(); await a.init(); await a.sql('CREATE TABLE items(id INTEGER)');
  const inserting=a.result({kind:'execute',sql:'INSERT INTO items VALUES(1)'});await entered.promise;
  const closing=a.close(); await assert.rejects(b.init(),{code:OWNED});
  finish.resolve();await inserting;await closing;await b.init();
});

test('same-name reinitialization never drops the lease during old handle destructors',async t=>{
  const {connection,storage}=fixture(t);
  const observations=[];
  const a=connection({beforeClose(){observations.push(storage.sessions.size);}});
  await a.init();await a.sql('CREATE TABLE items(id INTEGER)');await a.save();
  await a.init();assert.deepEqual(observations,[1]);
  await a.close();assert.deepEqual(observations,[1,1]);assert.equal(storage.sessions.size,0);
});

test('reinitialization to another namespace retires only the old lease after core cleanup',async t=>{
  const {connection,storage}=fixture(t);
  const observations=[];
  const a=connection({beforeClose(){observations.push(storage.sessions.size);}}),b=connection();
  await a.init('one');await a.init('two');
  assert.deepEqual(observations,[2]); assert.equal(storage.sessions.size,1);
  await b.init('one');await assert.rejects(connection().init('two'),{code:OWNED});
});

for (const failureKind of ['close','statement']) {
  test(`${failureKind} cleanup failure never relinquishes ownership on a cleared second close`,async t=>{
    const {connection,storage}=fixture(t);
    const hooks={},a=connection(hooks),b=connection();await a.init();
    if(failureKind==='statement') {
      await a.result({kind:'prepare',sql:'SELECT 1'});
      hooks.statementFree=()=>{throw new Error('finalize failed');};
    } else hooks.beforeClose=()=>{throw new Error('close failed');};
    const first=await a.close();assert.equal(first.kind,'error');
    assert.equal(first.error.code,'ERR_FSQLITE_TRANSACTION_CONNECTION_UNUSABLE');
    assert.equal((await a.close()).kind,'error');assert.equal(storage.sessions.size,1);
    await assert.rejects(b.init(),{code:OWNED});
    // A real worker realm termination releases the Web Lock. This model does
    // not pretend another close of emptied fields proves native cleanup.
  });
}

test('transport failure during import disposes the candidate before releasing ownership',async t=>{
  const {connection,storage}=fixture(t);
  const seed=connection();await seed.init();await seed.sql('CREATE TABLE items(id INTEGER)');await seed.save();await seed.close();
  const entered=deferred(),finish=deferred();
  const a=connection({beforeImport:async()=>{entered.resolve();await finish.promise;}}),b=connection();
  const opening=a.init();const failed=assert.rejects(opening,/channel failed/);await entered.promise;
  const closing=a.host.failTransport(new Error('channel failed'));
  await assert.rejects(b.init(),{code:OWNED});finish.resolve();await failed;await closing;
  assert.deepEqual(a.events,['close','free']);assert.equal(storage.sessions.size,0);await b.init();
});

test('explicit policy fails without Web Locks before constructing a database',async t=>{
  const {connection}=fixture(t);const saved=navigator.locks;
  navigator.locks=undefined;
  try {
    const a=connection();
    for(const mode of ['exclusive','shared']) await assert.rejects(a.init('db',mode,'indexeddb-snapshot'),{code:'ERR_FSQLITE_SNAPSHOT_OWNERSHIP_UNAVAILABLE'});
    assert.deepEqual(a.counts(),{creates:0,imports:0});
    const ready=await a.init('db',undefined,'indexeddb-snapshot');
    assert.equal(ready.data.snapshotOwnership,undefined);await a.close();
  } finally {navigator.locks=saved;}
});

test('ownership policies are captured once at admission and reject invalid values',async t=>{
  const {connection,storage}=fixture(t);const a=connection();let reads=0;
  const config={dbName:'db',persistence:'opfs-snapshot',get snapshotOwnership(){reads++;return 'exclusive';}};
  const opening=a.result({kind:'init',config});assert.equal(reads,1);
  assert.equal((await opening).data.snapshotOwnership,'exclusive');assert.equal(reads,1);
  for(const policy of [null,false,0,'other']) {
    await assert.rejects(connection().init('other',policy),{code:'ERR_FSQLITE_SNAPSHOT_OWNERSHIP_INPUT'});
  }
  await assert.rejects(connection().init('db','exclusive','memory'),{code:'ERR_FSQLITE_SNAPSHOT_OWNERSHIP_INPUT'});
  assert.equal(storage.sessions.size,1);
});
