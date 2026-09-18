// Production worker lifecycle against Node's SQLite reference, not WASM.
import assert from 'node:assert/strict';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { setImmediate as turn } from 'node:timers/promises';
import { WorkerConnectionHost } from '../src/connection.ts';
import { PreparedStatementBudget, resolvePreparedStatementLimits } from '../src/statement-budget.ts';

const fatal = 'ERR_FSQLITE_TRANSACTION_CONNECTION_UNUSABLE';
const limit = 'ERR_FSQLITE_STATEMENT_LIMIT';
const tooLarge = 'ERR_FSQLITE_STATEMENT_TOO_LARGE';
const metadata = 'ERR_FSQLITE_STATEMENT_METADATA';
function deferred() { let resolve; const promise = new Promise(r => { resolve = r; }); return { promise, resolve }; }
function ok(response, kind) { assert.notEqual(response.kind, 'error', JSON.stringify(response)); if (kind) assert.equal(response.kind, kind); return response; }
function error(response, code) { assert.equal(response.kind, 'error', JSON.stringify(response)); if (code) assert.equal(response.error.code, code); return response.error; }
async function fixture(t, statementLimits = {}, hooks = {}) {
  let sequence = 0, prepares = 0, generation = 0;
  const records = [], statements = [];
  const core = () => {
    const raw = new DatabaseSync(':memory:');
    raw.exec('CREATE TABLE data(value); INSERT INTO data VALUES(7)');
    const record = { raw, generation: ++generation, closed: false, closes: 0, frees: 0 };
    records.push(record);
    const result = (stmt, params = []) => {
      const columns = stmt.columns().map(c => c.name), rows = stmt.all(...params);
      return { columns, columnCount: columns.length, columnTypes: [], rows,
        rowArrays: rows.map(r => columns.map(c => r[c])), changes: 0 };
    };
    return {
      path: ':memory:',
      close() { record.closes++; if (!record.closed) { raw.close(); record.closed = true; } },
      free() { record.frees++; if (!record.closed) { raw.close(); record.closed = true; } },
      async execute(sql) { return Number(raw.prepare(sql).run().changes); },
      async executeWithParams(sql, params) { return Number(raw.prepare(sql).run(...params).changes); },
      async query(sql) { return result(raw.prepare(sql)); },
      async queryWithParams(sql, params) { return result(raw.prepare(sql), params); },
      async executeBatch(sql) { raw.exec(sql); },
      async prepare(sql) {
        prepares++;
        await hooks.beforePrepare?.(sql);
        if (hooks.alias) return hooks.alias;
        const stmt = raw.prepare(sql), columns = stmt.columns().map(c => c.name);
        const item = { freeCount: 0, freed: false, sql };
        const check = () => { assert.equal(item.freed, false, 'use after free'); };
        const handle = {
          get sql() { return hooks.sql ? hooks.sql(sql) : sql; },
          get columnCount() { return hooks.count ? hooks.count(columns.length) : columns.length; },
          columnNames() { return hooks.names ? hooks.names(columns) : columns; },
          free() { item.freeCount++; check(); item.freed = true; hooks.free?.(item); },
          async execute() { check(); return Number(stmt.run().changes); },
          async executeWithParams(params) { check(); return Number(stmt.run(...params).changes); },
          async query() { check(); return result(stmt); },
          async queryWithParams(params) { check(); return result(stmt, params); },
        };
        item.handle = handle; statements.push(item);
        await hooks.afterPrepare?.(handle);
        return handle;
      },
      async export() { return new Uint8Array(); },
    };
  };
  const host = new WorkerConnectionHost({ async load() { return { FrankenDB: {
    async create() { if (hooks.create) await hooks.create(); return core(); },
    async import() { throw new Error('bad import'); },
  } }; } }, {}, statementLimits);
  const send = request => host.handle({ requestId: ++sequence, ...request });
  t.after(async () => { await send({ kind: 'close' }); for (const r of records) if (!r.closed) r.raw.close(); });
  ok(await send({ kind: 'init', config: {} }), 'ready');
  return { host, send, records, statements, prepared: () => prepares };
}
const prepare = async (f, sql = 'SELECT value FROM data', transactionId) =>
  ok(await f.send({ kind: 'prepare', sql, ...(transactionId ? { transactionId } : {}) }), 'prepare-result').data.statementId;
const boundary = (f, action, id = '1', parentId) => f.send({ kind: 'transaction', action, transactionId: id,
  ...(parentId ? { parentId } : {}) });

for (const input of [{ maxStatements: 0 }, { maxStatements: 4097 }, { maxStatements: 1.2 },
  { maxBytes: 255 }, { maxBytes: 1024 ** 3 + 1 }, { maxBytes: Infinity }, { maxBytes: NaN }, null, []]) {
  test(`invalid prepared policy ${JSON.stringify(input)} is rejected`, () => {
    assert.throws(() => resolvePreparedStatementLimits(input), e => e.code === 'ERR_FSQLITE_STATEMENT_INPUT');
  });
}

test('retained handles remain charged after request completion; saturation never evicts one', async t => {
  const f = await fixture(t, { maxStatements: 2 });
  const first = await prepare(f), second = await prepare(f);
  const before = f.host.preparedStatements;
  assert.ok(Object.isFrozen(before)); assert.equal(before.statements, 2);
  assert.equal(f.host.requestQueue.pendingRequests, 0);
  const rejected = error(await f.send({ kind: 'prepare', sql: 'SELECT 99' }), limit);
  assert.equal(rejected.transient, true); assert.equal(f.prepared(), 2);
  assert.equal(f.host.preparedStatements.statements, 2);
  assert.equal(f.host.preparedStatements.bytes, before.bytes);
  for (const id of [first, second]) {
    assert.deepEqual(ok(await f.send({ kind: 'statement-query', statementId: id })).data.rowArrays, [[7]]);
  }
  ok(await f.send({ kind: 'statement-finalize', statementId: first }));
  assert.equal(f.host.preparedStatements.statements, 1);
  await prepare(f); assert.equal(f.prepared(), 3);
  assert.equal(f.statements[0].freeCount, 1);
  assert.equal(f.host.preparedStatements.rejectedStatements, 1);
});

test('exact metadata byte boundary includes both SQL copies and column names', async t => {
  const sql = 'SELECT 1';
  const size = 256 + sql.length * 4 + 16 + 2;
  const f = await fixture(t, { maxBytes: size });
  const id = await prepare(f, sql);
  assert.equal(f.host.preparedStatements.bytes, size);
  ok(await f.send({ kind: 'statement-finalize', statementId: id }));
  assert.equal(f.host.preparedStatements.bytes, 0);
  error(await f.send({ kind: 'prepare', sql: 'SELECT 11' }), tooLarge);
  assert.equal(f.statements.at(-1).freeCount, 1);
  assert.equal(f.host.preparedStatements.bytes, 0);
});

test('oversized SQL is refused before binding layout or native prepare', async t => {
  const f = await fixture(t, { maxBytes: 512 });
  error(await f.send({ kind: 'prepare', sql: `SELECT 1 /* ${'x'.repeat(1000)} */` }), tooLarge);
  assert.equal(f.prepared(), 0); assert.equal(f.host.preparedStatements.bytes, 0);
});

test('parameter slots, including numbered holes, consume the metadata budget', async t => {
  const f = await fixture(t, { maxBytes: 1024 });
  error(await f.send({ kind: 'prepare', sql: 'SELECT ?100' }), tooLarge);
  assert.equal(f.prepared(), 0); assert.equal(f.host.preparedStatements.statements, 0);
});

test('metadata growth hitting aggregate capacity releases only the candidate', async t => {
  const f = await fixture(t, { maxBytes: 700 });
  const first = await prepare(f);
  const bytes = f.host.preparedStatements.bytes;
  error(await f.send({ kind: 'prepare', sql: 'SELECT 1 AS a_long_column_name' }), limit);
  assert.equal(f.statements[1].freeCount, 1); assert.equal(f.statements[0].freeCount, 0);
  assert.equal(f.host.preparedStatements.bytes, bytes);
  assert.deepEqual(ok(await f.send({ kind: 'statement-query', statementId: first })).data.rowArrays, [[7]]);
});

for (const sql of ['SELECT ?0', 'THIS IS NOT SQL']) {
  test(`failed layout/core compilation releases reservation: ${sql}`, async t => {
    const f = await fixture(t, { maxStatements: 1 });
    error(await f.send({ kind: 'prepare', sql }));
    assert.equal(f.host.preparedStatements.statements, 0); assert.equal(f.host.preparedStatements.bytes, 0);
    await prepare(f);
  });
}

for (const field of ['sql', 'count', 'names']) {
  test(`throwing ${field} metadata cannot strand an unpublished handle`, async t => {
    const hooks = { [field]() { throw Object.assign(new Error('metadata failure'), { code: 'PROBE_METADATA' }); } };
    const f = await fixture(t, { maxStatements: 1 }, hooks);
    error(await f.send({ kind: 'prepare', sql: 'SELECT 1' }), 'PROBE_METADATA');
    assert.equal(f.statements[0].freeCount, 1);
    assert.equal(f.host.preparedStatements.statements, 0);
    delete hooks[field]; await prepare(f);
    ok(await f.send({ kind: 'close' }));
    assert.equal(f.statements[0].freeCount, 1); assert.equal(f.statements[1].freeCount, 1);
  });
}

for (const [label, hooks] of [
  ['non-string SQL', { sql: () => 1 }], ['negative columns', { count: () => -1 }],
  ['noninteger columns', { count: () => 1.5 }], ['huge columns', { count: () => 32769 }],
  ['non-array names', { names: () => ({ length: 1 }) }], ['wrong name count', { names: () => [] }],
  ['sparse names', { names: () => new Array(1) }], ['non-string name', { names: () => [1] }],
]) {
  test(`invalid prepared metadata: ${label}`, async t => {
    const f = await fixture(t, {}, hooks);
    error(await f.send({ kind: 'prepare', sql: 'SELECT 1' }), metadata);
    assert.equal(f.statements[0].freeCount, 1); assert.equal(f.host.preparedStatements.statements, 0);
  });
}

test('column metadata is captured once, copied by index and isolated from later mutation', async t => {
  let calls = 0;
  const names = ['value']; names[Symbol.iterator] = () => { throw new Error('unexpected iterator'); };
  const f = await fixture(t, {}, { names() { calls++; return names; } });
  const response = ok(await f.send({ kind: 'prepare', sql: 'SELECT value FROM data' }));
  names[0] = 'changed';
  assert.deepEqual(response.data.columnNames, ['value']); assert.equal(calls, 1);
});

for (const returnedSql of ['SELECT \0', "SELECT 'unterminated", 'SELECT ?0']) {
  test(`invalid returned SQL cannot strand a handle during SDK layout construction: ${JSON.stringify(returnedSql)}`, async t => {
    const f = await fixture(t, {}, { sql: () => returnedSql });
    error(await f.send({ kind: 'prepare', sql: 'SELECT 1' }), 'ERR_FSQLITE_BINDING_INPUT');
    assert.equal(f.statements[0].freeCount, 1); assert.equal(f.host.preparedStatements.statements, 0);
  });
}

test('cancellation while prepare is awaited frees the candidate before rollback and permits later reuse', async t => {
  const started = deferred(), release = deferred();
  const hooks = { async afterPrepare() { started.resolve(); await release.promise; } };
  const f = await fixture(t, { maxStatements: 1 }, hooks);
  ok(await boundary(f, 'begin'));
  const pending = f.send({ kind: 'prepare', sql: 'SELECT value FROM data', transactionId: '1' });
  await started.promise;
  assert.equal(f.host.preparedStatements.statements, 1); assert.equal(f.statements[0].freeCount, 0);
  assert.equal(ok(await f.send({ kind: 'cancel-transaction', targetTransactionId: '1' })).accepted, true);
  const queued = f.send({ kind: 'execute', sql: 'UPDATE data SET value=99', transactionId: '1' });
  release.resolve(); error(await pending, 'ERR_FSQLITE_TRANSACTION_CANCELLED');
  error(await queued, 'ERR_FSQLITE_TRANSACTION_CANCELLED');
  assert.equal(f.statements[0].freeCount, 1); assert.equal(f.host.preparedStatements.statements, 0);
  ok(await boundary(f, 'rollback')); delete hooks.afterPrepare; await prepare(f);
});

test('metadata callback cancellation cannot publish a now-unowned prepared statement', async t => {
  const hooks = {};
  const f = await fixture(t, {}, hooks); ok(await boundary(f, 'begin'));
  hooks.names = names => { void f.send({ kind: 'cancel-transaction', targetTransactionId: '1' }); return names; };
  error(await f.send({ kind: 'prepare', sql: 'SELECT 1', transactionId: '1' }), 'ERR_FSQLITE_TRANSACTION_CANCELLED');
  assert.equal(f.statements[0].freeCount, 1); assert.equal(f.host.preparedStatements.statements, 0);
  ok(await boundary(f, 'rollback'));
});

for (const moment of ['await', 'metadata']) {
  test(`transport failure during ${moment} never publishes a candidate or frees an active operation`, async t => {
    const started = deferred(), release = deferred(), hooks = {};
    const f = await fixture(t, {}, hooks); await prepare(f);
    const failure = new Error('transport failed'); let close;
    if (moment === 'await') hooks.afterPrepare = async () => { started.resolve(); await release.promise; };
    else hooks.names = names => { close = f.host.failTransport(failure); return names; };
    const pending = f.send({ kind: 'prepare', sql: 'SELECT 2' });
    if (moment === 'await') {
      await started.promise; close = f.host.failTransport(failure); await turn();
      assert.equal(f.statements[1].freeCount, 0); assert.equal(f.records[0].closes, 0); release.resolve();
    }
    error(await pending); ok(await close, 'close-result');
    assert.equal(f.host.preparedStatements.statements, 0);
    assert.deepEqual(f.statements.map(s => s.freeCount), [1, 1]);
  });
}

for (const cleanup of [new Error('free failed'), undefined, null, false]) {
  test(`unpublished cleanup failure fences SQL, even when thrown value is ${String(cleanup)}`, async t => {
    const hooks = { names() { throw new Error('primary metadata failure'); }, free() { throw cleanup; } };
    const f = await fixture(t, {}, hooks);
    const failure = error(await f.send({ kind: 'prepare', sql: 'SELECT 1' }), fatal);
    assert.equal(failure.cause.message, 'primary metadata failure'); assert.equal(failure.cleanupErrors.length, 1);
    error(await f.send({ kind: 'execute', sql: 'UPDATE data SET value=123' }), fatal);
    assert.equal(f.statements[0].freeCount, 1); assert.equal(f.host.preparedStatements.bytes, 0);
    assert.equal(f.records[0].closes, 1);
  });
}

test('explicit finalize failure fences the connection, does not retry free, and drains other handles', async t => {
  const hooks = {}; const f = await fixture(t, {}, hooks);
  const id = await prepare(f); await prepare(f);
  let observed;
  hooks.free = item => { observed ??= f.host.preparedStatements.statements; if (item === f.statements[0]) throw new Error('bad free'); };
  error(await f.send({ kind: 'statement-finalize', statementId: id }), fatal);
  assert.equal(observed, 2); assert.equal(f.host.preparedStatements.statements, 0);
  assert.deepEqual(f.statements.map(s => s.freeCount), [1, 1]);
  error(await f.send({ kind: 'prepare', sql: 'SELECT 9' }), fatal);
});

test('returning an already-owned handle is rejected without freeing that handle', async t => {
  const hooks = {}; const f = await fixture(t, {}, hooks);
  const id = await prepare(f); hooks.alias = f.statements[0].handle;
  error(await f.send({ kind: 'prepare', sql: 'SELECT 99' }), metadata);
  assert.equal(f.host.preparedStatements.statements, 1); assert.equal(f.statements[0].freeCount, 0);
  assert.deepEqual(ok(await f.send({ kind: 'statement-query', statementId: id })).data.rowArrays, [[7]]);
});

for (const action of ['commit', 'rollback']) {
  test(`transaction ${action} releases its prepared resources without manual finalization`, async t => {
    const f = await fixture(t, { maxStatements: 1 }); ok(await boundary(f, 'begin'));
    await prepare(f, 'SELECT value FROM data', '1'); ok(await boundary(f, action));
    assert.equal(f.host.preparedStatements.statements, 0); await prepare(f);
  });
}

test('a rejected child prepare is recoverable without releasing its parent statement', async t => {
  const f = await fixture(t, { maxStatements: 1 }); ok(await boundary(f, 'begin'));
  const parent = await prepare(f, 'SELECT value FROM data', '1'); ok(await boundary(f, 'begin', '2', '1'));
  error(await f.send({ kind: 'prepare', sql: 'SELECT 2', transactionId: '2' }), limit);
  ok(await boundary(f, 'rollback', '2'));
  assert.deepEqual(ok(await f.send({ kind: 'statement-query', statementId: parent, transactionId: '1' })).data.rowArrays, [[7]]);
  ok(await boundary(f, 'commit')); assert.equal(f.host.preparedStatements.statements, 0);
});

test('failed replacement preserves prepared accounting; successful replacement releases it', async t => {
  const f = await fixture(t, { maxStatements: 1 }); const id = await prepare(f);
  const before = f.host.preparedStatements;
  error(await f.send({ kind: 'init', config: { snapshot: new Uint8Array([0]) } }));
  assert.deepEqual(f.host.preparedStatements, before);
  assert.deepEqual(ok(await f.send({ kind: 'statement-query', statementId: id })).data.rowArrays, [[7]]);
  ok(await f.send({ kind: 'init', config: {} })); assert.equal(f.host.preparedStatements.statements, 0);
  await prepare(f); assert.equal(f.statements[0].freeCount, 1);
});

test('ordinary close remains behind an admitted prepare and releases its reservation once', async t => {
  const started = deferred(), release = deferred();
  const f = await fixture(t, {}, { async afterPrepare() { started.resolve(); await release.promise; } });
  const pending = f.send({ kind: 'prepare', sql: 'SELECT 1' }); await started.promise;
  const close = f.send({ kind: 'close' }); await turn();
  assert.equal(f.statements[0].freeCount, 0); assert.equal(f.host.preparedStatements.statements, 1);
  release.resolve(); ok(await pending, 'prepare-result'); ok(await close, 'close-result');
  assert.equal(f.statements[0].freeCount, 1); assert.equal(f.host.preparedStatements.statements, 0);
});

test('reservation release is idempotent and cannot resurrect an expired lease', () => {
  const budget = new PreparedStatementBudget({ maxStatements: 1, maxBytes: 512 });
  const lease = budget.reserve('SELECT 1'); lease.grow(16); lease.release(); lease.release();
  assert.equal(budget.stats.statements, 0); assert.equal(budget.stats.bytes, 0);
  assert.throws(() => lease.grow(1)); const next = budget.reserve('SELECT 2'); next.release();
});
