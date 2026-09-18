// Run with the repository's source-loader.mjs. Production worker host,
// admission, SQL scanner, transaction state and serialization run unchanged.
// Only the native/WASM database is substituted with the SQLite reference.
import assert from 'node:assert/strict';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { setImmediate as turn } from 'node:timers/promises';
import { executeManagedBatch, validateManagedSql } from '../src/transactions.ts';
import { sqliteSnapshotWorker } from './helpers/snapshot-sqlite-core.mjs';

function deferred() {
  let resolve;
  const promise = new Promise(r => { resolve = r; });
  return { promise, resolve };
}
function success(response, kind) {
  assert.notEqual(response.kind, 'error', JSON.stringify(response));
  if (kind !== undefined) assert.equal(response.kind, kind);
  return response;
}
function failure(response, code) {
  assert.equal(response.kind, 'error', JSON.stringify(response));
  assert.equal(response.error.code, code);
  return response.error;
}
async function fixture(t, hooks = {}) {
  const value = sqliteSnapshotWorker(hooks);
  let nextId = 0;
  const send = request => value.host.handle(structuredClone({ ...request, requestId: ++nextId }));
  t.after(async () => { await send({ kind: 'close' }); });
  success(await send({ kind: 'init', config: {} }), 'ready');
  success(await send({ kind: 'execute-batch', sql: `
    CREATE TABLE data(id INTEGER PRIMARY KEY, n INTEGER NOT NULL UNIQUE);
    INSERT INTO data VALUES(1,0);
    CREATE TABLE audit(n);
    CREATE TRIGGER audit_update AFTER UPDATE ON data BEGIN INSERT INTO audit VALUES(new.n); END;
  ` }));
  const begin = (id = '1', parentId) => send({ kind: 'transaction', action: 'begin', transactionId: id,
    ...(parentId === undefined ? {} : { parentId }) });
  const boundary = (action, id = '1') => send({ kind: 'transaction', action, transactionId: id });
  const script = (sql, id = '1') => send({ kind: 'execute-batch', sql, transactionId: id });
  const cancel = (id = '1') => send({ kind: 'cancel-transaction', targetTransactionId: id });
  const state = async () => {
    const data = success(await send({ kind: 'query', sql: 'SELECT n FROM data ORDER BY id' })).data.rowArrays;
    const audit = success(await send({ kind: 'query', sql: 'SELECT n FROM audit ORDER BY rowid' })).data.rowArrays;
    return { data, audit };
  };
  return { ...value, send, begin, boundary, script, cancel, state };
}

const shapes = [
  ['ordinary DDL and data', `CREATE TABLE q(a); INSERT INTO q VALUES(1); INSERT INTO q VALUES(2);`],
  ['quotes and comments', `-- prefix ; COMMIT\nCREATE TABLE q(a); /* ; END */ INSERT INTO q VALUES('a;''b'); INSERT INTO q VALUES('c'); -- end;`],
  ['quoted identifiers', 'CREATE TABLE [q]("a;b"); INSERT INTO `q`("a;b") VALUES(7);'],
  ['trigger CASE and embedded terminators', `CREATE TABLE q(a); CREATE TRIGGER "tr;END" AFTER INSERT ON q BEGIN UPDATE q SET a=CASE WHEN new.a=1 THEN 2 ELSE 3 END; SELECT '; END; COMMIT'; END; INSERT INTO q VALUES(1);`],
  ['TEMP trigger', `CREATE TABLE q(a); CREATE /*x*/ TEMPORARY TRIGGER tr AFTER INSERT ON q BEGIN UPDATE q SET a=a+1; UPDATE q SET a=a+1; END; INSERT INTO q VALUES(1);`],
  ['Tcl parameter suffix', `CREATE TABLE q(a); INSERT INTO q VALUES($a::b(foo;COMMIT;'bar)); INSERT INTO q VALUES(5);`],
  ['dollar identifier', `CREATE TABLE q(a$b); INSERT INTO q VALUES(7);`],
  ['leading empty statements and BOM', `\uFEFF;; CREATE TABLE q(a);; INSERT INTO q VALUES(3);;`],
  ['unterminated trailing block comment', `CREATE TABLE q(a); INSERT INTO q VALUES(9); /* unfinished`],
  ['last statement without semicolon', `CREATE TABLE q(a); INSERT INTO q VALUES(42)`],
  ['table rebuild', `CREATE TABLE old(a); INSERT INTO old VALUES(1),(2); CREATE TABLE q(a); INSERT INTO q SELECT a+1 FROM old; DROP TABLE old; CREATE INDEX qi ON q(a);`],
];
for (const [name, sql] of shapes) {
  test(`statement boundaries preserve SQLite behavior: ${name}`, async () => {
    const whole = new DatabaseSync(':memory:'), sliced = new DatabaseSync(':memory:');
    try {
      whole.exec(sql);
      const calls = [];
      await executeManagedBatch({ async executeBatch(part) { calls.push(part); sliced.exec(part); } }, sql, () => {});
      assert.deepEqual(sliced.prepare('SELECT * FROM q ORDER BY rowid').all(), whole.prepare('SELECT * FROM q ORDER BY rowid').all());
      assert.equal(sliced.prepare('PRAGMA integrity_check').get().integrity_check, 'ok');
      assert.ok(calls.length >= 2);
      if (name.includes('trigger')) assert.equal(calls.filter(s => /CREATE.*TRIGGER/is.test(s)).length, 1);
    } finally { whole.close(); sliced.close(); }
  });
}

for (const tail of ['COMMIT', 'END', 'ROLLBACK', 'SAVEPOINT x', 'RELEASE x', 'BEGIN', "SELECT 'unterminated", 'SELECT $x(white space)', 'SELECT 1 /*\0*/']) {
  test(`complete preflight runs no earlier SQL before refusing tail ${JSON.stringify(tail)}`, async () => {
    let calls = 0;
    await assert.rejects(executeManagedBatch({ async executeBatch() { calls++; } },
      `SELECT application_side_effect(); ${tail}`, () => {}));
    assert.equal(calls, 0);
  });
}

test('single statements retain the no-task-yield path and comments/empty input contract', async t => {
  let tasks = 0, calls = 0;
  const original = setTimeout;
  t.mock.method(globalThis, 'setTimeout', (...args) => { tasks++; return original(...args); });
  await executeManagedBatch({ async executeBatch(sql) { calls++; assert.equal(sql, 'SELECT 1;'); } }, 'SELECT 1;', () => {});
  assert.equal(calls, 1); assert.equal(tasks, 0);
  for (const sql of ['', '; ;', '--comment', '/*comment*/']) assert.throws(() => validateManagedSql(sql, true));
  assert.throws(() => validateManagedSql('SELECT 1; SELECT 2'));
});

test('production host runs a managed schema/data script and commits its trigger effects', async t => {
  const f = await fixture(t);
  success(await f.begin());
  success(await f.script('CREATE TABLE extra(n); INSERT INTO extra VALUES(10); UPDATE data SET n=1; UPDATE data SET n=2;'));
  success(await f.boundary('commit'));
  assert.deepEqual(await f.state(), { data: [[2]], audit: [[1], [2]] });
  assert.equal(success(await f.send({ kind: 'query', sql: 'PRAGMA integrity_check' })).data.rowArrays[0][0], 'ok');
});

test('task-delivered cancellation stops a large script and fences already-queued writes/COMMIT', async t => {
  let seen = 0, delivered;
  const hooks = { beforeBatch(sql) {
    if (!sql.includes('n=n+1')) return;
    if (++seen === 1) setTimeout(() => { delivered = f.cancel(); }, 0);
  } };
  const f = await fixture(t, hooks);
  success(await f.begin());
  const script = f.script('UPDATE data SET n=n+1;'.repeat(1000));
  const tail = f.script('UPDATE data SET n=9999;');
  const commit = f.boundary('commit');
  failure(await script, 'ERR_FSQLITE_TRANSACTION_CANCELLED');
  assert.equal((await delivered).accepted, true);
  assert.ok(seen > 0 && seen <= 32, `executed ${seen} statements before task cancellation`);
  failure(await tail, 'ERR_FSQLITE_TRANSACTION_CANCELLED');
  failure(await commit, 'ERR_FSQLITE_TRANSACTION_CANCELLED');
  success(await f.boundary('rollback'));
  assert.deepEqual(await f.state(), { data: [[0]], audit: [] });
  assert.equal(f.host.requestQueue.pendingRequests, 0);
});

test('cancellation joins an in-flight statement and retains queue capacity until it finishes', async t => {
  const entered = deferred(), release = deferred();
  let live = false;
  const f = await fixture(t, { async beforeBatch(sql) {
    if (sql.includes('n=n+1')) { live = true; entered.resolve(); await release.promise; live = false; }
  }, beforeClose() { assert.equal(live, false); } });
  success(await f.begin());
  let settled = false;
  const script = f.script('UPDATE data SET n=n+1; UPDATE data SET n=90;');
  void script.then(() => { settled = true; });
  await entered.promise;
  assert.equal((await f.cancel()).accepted, true);
  const rollback = f.boundary('rollback');
  await turn();
  assert.equal(settled, false);
  assert.equal(f.host.requestQueue.pendingRequests, 2);
  release.resolve();
  failure(await script, 'ERR_FSQLITE_TRANSACTION_CANCELLED');
  success(await rollback);
  assert.deepEqual(await f.state(), { data: [[0]], audit: [] });
});

test('child cancellation rolls back only the child; parent and later siblings survive', async t => {
  const f = await fixture(t);
  success(await f.begin());
  success(await f.script('UPDATE data SET n=5;'));
  success(await f.begin('2', '1'));
  const pending = f.script('UPDATE data SET n=6; UPDATE data SET n=7;', '2');
  assert.equal((await f.cancel('2')).accepted, true);
  failure(await pending, 'ERR_FSQLITE_TRANSACTION_CANCELLED');
  success(await f.boundary('rollback', '2'));
  success(await f.begin('3', '1'));
  success(await f.script('UPDATE data SET n=8;', '3'));
  success(await f.boundary('commit', '3'));
  success(await f.boundary('commit'));
  assert.deepEqual(await f.state(), { data: [[8]], audit: [[5], [8]] });
});

test('parent cancellation reaches an active child and cannot be cleared by child rollback', async t => {
  const f = await fixture(t);
  success(await f.begin()); success(await f.begin('2', '1'));
  const pending = f.script('UPDATE data SET n=5; UPDATE data SET n=6;', '2');
  assert.equal((await f.cancel()).accepted, true);
  failure(await pending, 'ERR_FSQLITE_TRANSACTION_CANCELLED');
  success(await f.boundary('rollback', '2'));
  failure(await f.script('UPDATE data SET n=99;'), 'ERR_FSQLITE_TRANSACTION_CANCELLED');
  success(await f.boundary('rollback'));
  assert.deepEqual(await f.state(), { data: [[0]], audit: [] });
});

test('a genuine SQLite constraint failure is retained and rolls back the entire script', async t => {
  const f = await fixture(t);
  success(await f.begin());
  const error = failure(await f.script('UPDATE data SET n=5; INSERT INTO data VALUES(2,5); UPDATE data SET n=6;'), 'ERR_SQLITE_ERROR');
  assert.match(error.message, /UNIQUE constraint failed/);
  failure(await f.boundary('commit'), 'ERR_FSQLITE_TRANSACTION_ABORTED');
  success(await f.boundary('rollback'));
  assert.deepEqual(await f.state(), { data: [[0]], audit: [] });
});

test('rollback failure closes the worker instead of releasing partial script work', async t => {
  let failRollback = false;
  const f = await fixture(t, { beforeBatch(sql) { if (failRollback && sql === 'ROLLBACK') throw new Error('rollback I/O fault'); } });
  success(await f.begin());
  success(await f.script('UPDATE data SET n=5;'));
  assert.equal((await f.cancel()).accepted, true);
  failRollback = true;
  const error = failure(await f.boundary('rollback'), 'ERR_FSQLITE_TRANSACTION_CONNECTION_UNUSABLE');
  assert.match(error.cause.message, /rollback I\/O fault/);
  failure(await f.send({ kind: 'execute', sql: 'UPDATE data SET n=99' }), 'ERR_FSQLITE_TRANSACTION_CONNECTION_UNUSABLE');
  const reopened = new DatabaseSync(f.handles[0].path);
  try { assert.equal(reopened.prepare('SELECT n FROM data').get().n, 0); } finally { reopened.close(); }
});

test('connection-wide transport failure stops the script then closes, without destroying live handles', async t => {
  const entered = deferred(), release = deferred(); let active = false;
  const f = await fixture(t, { async beforeBatch(sql) {
    if (sql.includes('n=n+1')) { active = true; entered.resolve(); await release.promise; active = false; }
  }, beforeClose() { assert.equal(active, false); } });
  success(await f.begin());
  const pending = f.script('UPDATE data SET n=n+1; UPDATE data SET n=99;');
  await entered.promise;
  const close = f.host.failTransport(new Error('transport failed'));
  await turn(); assert.ok(!f.events.includes('close'));
  release.resolve();
  assert.match(failure(await pending, 'ERR_FSQLITE_WORKER').message, /transport failed/);
  success(await close, 'close-result');
  assert.ok(!f.events.some(sql => sql.includes('n=99')));
  const reopened = new DatabaseSync(f.handles[0].path);
  try { assert.equal(reopened.prepare('SELECT n FROM data').get().n, 0); } finally { reopened.close(); }
});

test('manual scripts retain core-managed boundaries and partial-failure behavior', async t => {
  const f = await fixture(t);
  success(await f.send({ kind: 'execute-batch', sql: 'BEGIN; UPDATE data SET n=7; COMMIT;' }));
  assert.equal((await f.cancel('999')).accepted, false);
  assert.deepEqual(await f.state(), { data: [[7]], audit: [[7]] });
  failure(await f.send({ kind: 'execute-batch', sql: 'UPDATE data SET n=8; INVALID SQL;' }), 'ERR_SQLITE_ERROR');
  assert.deepEqual(await f.state(), { data: [[8]], audit: [[7], [8]] });
});
