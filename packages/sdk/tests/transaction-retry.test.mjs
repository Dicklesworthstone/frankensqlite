// Run: node --test packages/sdk/tests/transaction-retry.test.mjs
// Executes production SDK transaction/handle/retry code with an injected
// transport backed by Node SQLite. This is NOT a WASM, IPC or browser test.
import assert from 'node:assert/strict';
import { readFileSync, mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { DatabaseSync } from 'node:sqlite';
import { setImmediate as turn, setTimeout as sleep } from 'node:timers/promises';
import test from 'node:test';
const { default: ts } = await import(process.env.FSQLITE_TYPESCRIPT_MODULE ?? 'typescript');
const root = fileURLToPath(new URL('../src/', import.meta.url));
const cache = new Map();
const replacements = new Map();

// Module-boundary injection, not a reimplementation of transaction/retry logic.
// Unexercised stream/admission surfaces are deliberately not certified here.
function production(name) {
  const path = join(root, `${name}.ts`);
  if (cache.has(path)) return cache.get(path).exports;
  const module = { exports: {} };
  cache.set(path, module);
  const { outputText, diagnostics } = ts.transpileModule(readFileSync(path, 'utf8'), {
    fileName: path, reportDiagnostics: true,
    compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 },
  });
  assert.equal(diagnostics?.filter(d => d.category === ts.DiagnosticCategory.Error).length, 0);
  const require = specifier => {
    if (replacements.has(specifier)) return replacements.get(specifier);
    assert.ok(specifier.startsWith('./'), `unmocked dependency: ${specifier}`);
    return production(specifier.slice(2));
  };
  new Function('require', 'module', 'exports', `${outputText}\n//# sourceURL=${path}`)(require, module, module.exports);
  return module.exports;
}
const { FrankenSQLiteError } = production('errors');
const { isTransactionConflict, resolveTransactionRetryOptions, runTransactionRetry,
  FrankenTransactionRetryError } = production('transaction-retry');
const names = { 5: 'SQLITE_BUSY', 261: 'SQLITE_BUSY_RECOVERY', 517: 'SQLITE_BUSY_SNAPSHOT', 773: 'SQLITE_BUSY_TIMEOUT' };
const busy = (extendedCode = 517, extra = {}) => new FrankenSQLiteError({
  code: names[extendedCode], sqliteCode: 5, extendedCode, transient: true, message: 'conflict', ...extra,
});
function sqliteError(error) {
  if (!Number.isSafeInteger(error?.errcode)) return error;
  return new FrankenSQLiteError({ code: names[error.errcode] ?? 'SQLITE_ERROR',
    sqliteCode: error.errcode & 255, extendedCode: error.errcode,
    transient: (error.errcode & 255) === 5, message: error.message });
}
function deferred() {
  let resolve;
  const promise = new Promise(r => { resolve = r; });
  return { promise, resolve };
}

class SqliteTransport {
  constructor(state) { this.state = state; this.resultEncoding = 'objects'; }
  assertOpen() { if (this.state.closed) throw this.state.closed; }
  async init() { return { path: this.state.path, persistence: 'memory' }; }
  observeFailure() { return () => {}; }
  async transaction(action, id, parentId) {
    this.assertOpen();
    const s = this.state;
    s.events.push({ action, id, parentId });
    if (action === 'begin' && parentId === undefined) s.attempts++;
    try {
      await s.beforeBoundary?.(action, id, parentId);
      const frame = s.stack.at(-1);
      if (action === 'begin') {
        s.raw.exec(parentId === undefined ? 'BEGIN' : `SAVEPOINT child_${id}`);
        s.stack.push({ id, parentId });
      } else {
        assert.equal(frame?.id, id);
        if (action === 'commit') s.raw.exec(frame.parentId === undefined ? 'COMMIT' : `RELEASE child_${id}`);
        else s.raw.exec(frame.parentId === undefined ? 'ROLLBACK' : `ROLLBACK TO child_${id}; RELEASE child_${id}`);
        s.stack.pop();
      }
      await s.afterBoundary?.(action, id, parentId);
    } catch (error) { throw sqliteError(error); }
  }
  cancelTransaction(id) { this.state.events.push({ action: 'cancel', id }); }
  async execute(sql, params = []) {
    this.assertOpen();
    await this.state.beforeExecute?.(sql);
    try { return Number(this.state.raw.prepare(sql).run(...params).changes); }
    catch (error) { throw sqliteError(error); }
  }
  async executeBatch(sql) {
    this.assertOpen();
    try { this.state.raw.exec(sql); }
    catch (error) { throw sqliteError(error); }
  }
  async query(sql, params = []) {
    this.assertOpen();
    try {
      const stmt = this.state.raw.prepare(sql);
      return { columns: stmt.columns().map(c => c.name), rows: stmt.all(...params) };
    } catch (error) { throw sqliteError(error); }
  }
  async prepare(sql) {
    this.assertOpen();
    const statementId = String(++this.state.nextStatement);
    const stmt = this.state.raw.prepare(sql);
    this.state.statements.set(statementId, stmt);
    const columnNames = stmt.columns().map(c => c.name);
    return { statementId, sql, columnCount: columnNames.length, columnNames };
  }
  async executePrepared(id, params = []) {
    this.assertOpen();
    try { return Number(this.state.statements.get(id).run(...params).changes); }
    catch (error) { throw sqliteError(error); }
  }
  async queryPrepared(id, params = []) {
    this.assertOpen();
    const stmt = this.state.statements.get(id);
    try { return { columns: stmt.columns().map(c => c.name), rows: stmt.all(...params) }; }
    catch (error) { throw sqliteError(error); }
  }
  async finalizePrepared(id) {
    this.state.events.push({ action: 'finalize', id });
    this.state.statements.delete(id);
    await this.state.onFinalize?.(id);
  }
  captureBindings(_id, params) { return [...params]; }
  dispose(error = new Error('closed')) {
    if (this.state.closed) return;
    this.state.closed = error;
    this.state.events.push({ action: 'dispose' });
    this.state.raw.close();
    if (this.state.disposeError) throw this.state.disposeError;
  }
  async close() { this.dispose(); }
}
replacements.set('./worker-client', { FrankenWorkerClient: SqliteTransport });
replacements.set('./utils', { normalizeOpenOptions: options => options ?? {}, resolveWorker: worker => worker });
replacements.set('./stream', {
  checkStreamCancellation() { throw new Error('stream is outside this test'); },
  executeRowStream() { throw new Error('stream is outside this test'); },
  streamOptions() { throw new Error('stream is outside this test'); },
});
replacements.set('@frankensqlite/worker', {
  resolveRequestLimits: limits => limits,
  resolveResultEncoding: encoding => encoding ?? 'objects',
  // Prepared tests below deliberately have no parameters; binding semantics
  // belong to the separate production binding/reference-engine keepers.
  parameterLayout(sql) {
    assert.ok(!/[?:@$]/.test(sql), 'this transport fixture only describes zero-parameter prepared SQL');
    return { count: 0, names: [] };
  },
});
const { FrankenDB } = production('database');
const fast = { maxAttempts: 4, timeoutMs: 5000, initialDelayMs: 0, maxDelayMs: 0 };
async function fixture(t, file = false) {
  const path = file ? join(mkdtempSync(join(tmpdir(), 'fsqlite-retry-')), 'db.sqlite') : ':memory:';
  const raw = new DatabaseSync(path);
  const state = { raw, path, stack: [], events: [], attempts: 0, statements: new Map(), nextStatement: 0 };
  t.after(() => { if (!state.closed) raw.close(); });
  const db = await FrankenDB.open({ worker: state });
  raw.exec('CREATE TABLE counts(value INTEGER); INSERT INTO counts VALUES(0); CREATE TABLE audit(v); CREATE TRIGGER log_update AFTER UPDATE ON counts BEGIN INSERT INTO audit VALUES(new.value); END');
  return { db, state, raw };
}
const actions = state => state.events.map(e => e.action);

for (const code of [5, 261, 517, 773]) {
  test(`classifier accepts confirmed BUSY code ${code}`, () => assert.equal(isTransactionConflict(busy(code)), true));
}
for (const [name, error] of [
  ['arbitrary transient object', { code: 'SQLITE_BUSY', transient: true }],
  ['message-only lock error', new Error('SQLITE_BUSY_SNAPSHOT: database locked')],
  ['constraint', new FrankenSQLiteError({ code: 'SQLITE_CONSTRAINT', sqliteCode: 19, message: 'constraint', transient: true })],
  ['LOCKED', new FrankenSQLiteError({ code: 'SQLITE_LOCKED', sqliteCode: 6, message: 'locked', transient: true })],
  ['storage publication CAS conflict', new FrankenSQLiteError({ code: 'ERR_FSQLITE_SNAPSHOT_CONFLICT', message: 'stale' })],
  ['fatal marker', busy(517, { userRecoverable: false })],
  ['explicit permanent', busy(517, { transient: false })],
  ['inconsistent primary', busy(517, { sqliteCode: 10 })],
  ['inconsistent extension', busy(517, { extendedCode: 19 })],
  ['cleanup failure', busy(517, { cleanupErrors: [{ code: 'SQLITE_BUSY', message: 'cleanup' }] })],
  ['mixed aggregate', new AggregateError([busy(), new Error('application')])],
  ['fatal wrapper with busy cause', new FrankenSQLiteError({ code: 'ERR_FSQLITE_BULK_CONNECTION_UNUSABLE', message: 'fatal', cause: { code: 'SQLITE_BUSY', message: 'busy' } })],
]) {
  test(`classifier refuses ${name}`, () => assert.equal(isTransactionConflict(error), false));
}
test('classifier validates every aggregate member without custom iterators/every or sparse holes', () => {
  const valid = new AggregateError([busy(), busy(5)]);
  valid.errors.every = () => { throw new Error('must not invoke'); };
  assert.equal(isTransactionConflict(valid), true);
  const sparse = new AggregateError([]); sparse.errors.length = 1;
  assert.equal(isTransactionConflict(sparse), false);
  const cyclic = new AggregateError([]); cyclic.errors.push(cyclic);
  assert.equal(isTransactionConflict(cyclic), false);
  const wrapped = new FrankenSQLiteError({ code: 'ERR_FSQLITE_TRANSACTION_ABORTED', message: 'scope failed', transient: false,
    cause: { code: 'SQLITE_BUSY_SNAPSHOT', sqliteCode: 5, extendedCode: 517, message: 'conflict' } });
  assert.equal(isTransactionConflict(wrapped), true);
  const malicious = busy(); Object.defineProperty(malicious, 'code', { get() { throw new Error('getter'); } });
  assert.equal(isTransactionConflict(malicious), false);
});

test('successful callback runs once, returns its value, and releases ownership', async t => {
  const { db, state, raw } = await fixture(t);
  const value = await db.transactionWithRetry(async (tx, info) => {
    assert.deepEqual(info, { attempt: 1, maxAttempts: 4 }); assert.ok(Object.isFrozen(info));
    await tx.execute('UPDATE counts SET value=value+1'); return { committed: true };
  }, fast);
  assert.deepEqual(value, { committed: true }); assert.equal(state.attempts, 1);
  assert.equal(raw.prepare('SELECT value FROM counts').get().value, 1);
  await db.execute('UPDATE counts SET value=value+1');
});

test('REAL SQLITE_BUSY_SNAPSHOT replays reads in a fresh transaction, not just the failed UPDATE', async t => {
  const { db, state, raw } = await fixture(t, true);
  raw.exec('PRAGMA journal_mode=WAL');
  const peer = new DatabaseSync(state.path); t.after(() => peer.close());
  const reads = []; let escaped;
  const result = await db.transactionWithRetry(async (tx, info) => {
    if (info.attempt === 2) await assert.rejects(escaped.query('SELECT value FROM counts'), e => e.code === 'ERR_FSQLITE_TRANSACTION_CLOSED');
    escaped = tx;
    const value = (await tx.query('SELECT value FROM counts')).rows[0].value;
    reads.push(value);
    if (info.attempt === 1) peer.exec('UPDATE counts SET value=10');
    await tx.execute('UPDATE counts SET value=?', [value + 1]);
    return value + 1;
  }, fast);
  assert.deepEqual(reads, [0, 10]); assert.equal(result, 11);
  assert.equal(peer.prepare('SELECT value FROM counts').get().value, 11);
  assert.deepEqual(actions(state), ['begin', 'rollback', 'begin', 'commit']);
});

test('REAL COMMIT BUSY rolls back earlier writes and trigger effects before replay', async t => {
  const { db, state, raw } = await fixture(t, true);
  const reader = new DatabaseSync(state.path); t.after(() => reader.close());
  reader.exec('BEGIN'); reader.prepare('SELECT * FROM counts').all();
  state.afterBoundary = action => { if (action === 'rollback') reader.exec('ROLLBACK'); };
  await db.transactionWithRetry(tx => tx.execute('UPDATE counts SET value=value+1'), fast);
  assert.equal(state.attempts, 2);
  assert.equal(raw.prepare('SELECT value FROM counts').get().value, 1);
  assert.equal(raw.prepare('SELECT count(*) n FROM audit').get().n, 1);
  assert.deepEqual(actions(state), ['begin', 'commit', 'rollback', 'begin', 'commit']);
});

test('replay waits for rollback acknowledgement, including prepared cleanup', async t => {
  const { db, state } = await fixture(t);
  const rolling = deferred(), release = deferred();
  state.beforeBoundary = action => { if (action === 'commit' && state.attempts === 1) throw busy(); };
  state.afterBoundary = async action => { if (action === 'rollback') { rolling.resolve(); await release.promise; } };
  let first;
  const pending = db.transactionWithRetry(async tx => {
    const prepared = await tx.prepare('UPDATE counts SET value=value+1');
    first ??= prepared; await prepared.execute();
  }, fast);
  await rolling.promise;
  assert.equal(state.attempts, 1); assert.equal(state.statements.size, 0);
  assert.deepEqual(actions(state), ['begin', 'finalize', 'commit', 'rollback']);
  release.resolve(); await pending;
  await assert.rejects(first.execute(), e => e.code === 'ERR_FSQLITE_TRANSACTION_CLOSED');
  assert.equal(state.attempts, 2); assert.equal(state.statements.size, 0);
});

test('failed BEGIN can retry, but never rolls back an existing manual transaction', async t => {
  const { db, state, raw } = await fixture(t);
  state.beforeBoundary = action => { if (action === 'begin' && state.attempts < 3) throw busy(5); };
  await db.transactionWithRetry(tx => tx.execute('UPDATE counts SET value=1'), fast);
  assert.deepEqual(actions(state), ['begin', 'begin', 'begin', 'commit']);
  state.beforeBoundary = undefined;
  raw.exec('BEGIN; UPDATE counts SET value=7');
  let callbacks = 0;
  await assert.rejects(db.transactionWithRetry(() => { callbacks++; }, fast), e => e.code === 'SQLITE_ERROR');
  assert.equal(callbacks, 0); assert.equal(raw.prepare('SELECT value FROM counts').get().value, 7);
  raw.exec('ROLLBACK'); assert.equal(raw.prepare('SELECT value FROM counts').get().value, 1);
});

test('ordinary transaction remains single-attempt (retry is explicitly opt-in)', async t => {
  const { db, state } = await fixture(t); const conflict = busy();
  state.beforeBoundary = action => { if (action === 'commit') throw conflict; };
  await assert.rejects(db.transaction(tx => tx.execute('UPDATE counts SET value=1')), e => e === conflict);
  assert.equal(state.attempts, 1);
});

test('attempt exhaustion preserves the LAST conflict object and no partial writes', async t => {
  const { db, state, raw } = await fixture(t); let last;
  state.beforeBoundary = action => { if (action === 'commit') { last = busy(); throw last; } };
  await assert.rejects(db.transactionWithRetry(tx => tx.execute('UPDATE counts SET value=value+1'),
    { ...fast, maxAttempts: 3 }), e => e === last);
  assert.equal(state.attempts, 3); assert.equal(raw.prepare('SELECT value FROM counts').get().value, 0);
  state.beforeBoundary = undefined; await db.execute('UPDATE counts SET value=1');
});

for (const explicit of [false, true]) {
  test(`${explicit ? 'manual' : 'automatic'} prepared cleanup failure is never replayed, even when BUSY-shaped`, async t => {
    const { db, state } = await fixture(t); const cleanup = busy();
    state.onFinalize = () => { throw cleanup; };
    await assert.rejects(db.transactionWithRetry(async tx => {
      const prepared = await tx.prepare('UPDATE counts SET value=value+1'); await prepared.execute();
      if (explicit) await prepared.finalize();
    }, fast), e => e === cleanup);
    assert.equal(state.attempts, 1);
    assert.equal(actions(state).filter(x => x === 'finalize').length, 1);
  });
}

test('rollback failure and worker cleanup failure are preserved, never retried or masked by abort', async t => {
  const { db, state } = await fixture(t); const control = new AbortController();
  const original = busy(), rollback = busy(5), disposal = new Error('transport disposal');
  state.disposeError = disposal;
  state.beforeBoundary = action => {
    if (action === 'commit') throw original;
    if (action === 'rollback') { control.abort('cancel racing cleanup'); throw rollback; }
  };
  await assert.rejects(db.transactionWithRetry(tx => tx.execute('UPDATE counts SET value=1'), { ...fast, signal: control.signal }), error => {
    assert.ok(error instanceof AggregateError); assert.deepEqual(error.errors, [original, rollback, disposal]); return true;
  });
  assert.equal(state.attempts, 1); assert.equal(actions(state).filter(x => x === 'dispose').length, 1);
  await assert.rejects(db.execute('SELECT 1'), e => e instanceof AggregateError);
});

test('caught child finalization failure still disqualifies the parent from replay', async t => {
  const { db, state, raw } = await fixture(t); const cleanup = busy();
  state.onFinalize = () => { throw cleanup; };
  state.beforeBoundary = action => { if (action === 'commit') throw busy(5); };
  await assert.rejects(db.transactionWithRetry(async tx => {
    try {
      await tx.transaction(async child => {
        const statement = await child.prepare('UPDATE counts SET value=value+1');
        await statement.execute();
      });
    } catch (error) { assert.equal(error, cleanup); }
    await tx.execute('UPDATE counts SET value=value+1');
  }, fast), e => e.code === 'SQLITE_BUSY');
  assert.equal(state.attempts, 1);
  assert.equal(raw.prepare('SELECT value FROM counts').get().value, 0);
});

test('lost COMMIT acknowledgement cannot duplicate an already committed write', async t => {
  const { db, state } = await fixture(t, true);
  const peer = new DatabaseSync(state.path); t.after(() => peer.close());
  state.afterBoundary = action => { if (action === 'commit') throw busy(); };
  await assert.rejects(db.transactionWithRetry(tx => tx.execute('UPDATE counts SET value=value+1'), fast), AggregateError);
  assert.equal(state.attempts, 1); assert.equal(peer.prepare('SELECT value FROM counts').get().value, 1);
});

test('mixed application/SQL errors and constraint failures do not replay', async t => {
  const { db, state, raw } = await fixture(t); const application = new Error('do not repeat side effect');
  state.beforeExecute = sql => { if (sql === 'UPDATE counts SET value=2') throw busy(); };
  await assert.rejects(db.transactionWithRetry(async tx => {
    await tx.execute('UPDATE counts SET value=1');
    try { await tx.execute('UPDATE counts SET value=2'); } catch { /* recorded by the scope */ }
    throw application;
  }, fast), error => error instanceof AggregateError && error.errors.includes(application));
  assert.equal(state.attempts, 1); assert.equal(raw.prepare('SELECT value FROM counts').get().value, 0);
});

test('uncaught child conflict restarts the OUTER transaction, not the same snapshot savepoint', async t => {
  const { db, state, raw } = await fixture(t);
  state.beforeBoundary = (action, _id, parentId) => {
    if (action === 'begin' && parentId !== undefined && state.attempts === 1) throw busy();
  };
  await db.transactionWithRetry(async tx => {
    await tx.execute('UPDATE counts SET value=value+1');
    await tx.transaction(child => child.execute('INSERT INTO audit VALUES(100)'));
  }, fast);
  assert.equal(state.attempts, 2); assert.equal(raw.prepare('SELECT value FROM counts').get().value, 1);
  assert.equal(raw.prepare('SELECT count(*) n FROM audit').get().n, 2);
});

test('connection lease survives backoff and rejects foreign SQL, close and competing retries', async t => {
  t.mock.method(Math, 'random', () => 0.95);
  const { db, state } = await fixture(t); const rolled = deferred();
  state.beforeBoundary = action => { if (action === 'commit' && state.attempts === 1) throw busy(); };
  state.afterBoundary = action => { if (action === 'rollback') rolled.resolve(); };
  const pending = db.transactionWithRetry(tx => tx.execute('UPDATE counts SET value=1'),
    { ...fast, initialDelayMs: 100, maxDelayMs: 100 });
  await rolled.promise; await turn();
  assert.equal(state.attempts, 1);
  const ownership = e => e.code === 'ERR_FSQLITE_TRANSACTION_OWNERSHIP';
  await assert.rejects(db.execute('UPDATE counts SET value=9'), ownership);
  await assert.rejects(db.close(), ownership);
  await assert.rejects(db.transactionWithRetry(() => {}, fast), ownership);
  await pending; await db.execute('UPDATE counts SET value=2');
});

test('pre-abort preserves the caller reason and starts no transaction', async t => {
  const { db, state } = await fixture(t); const controller = new AbortController(); const reason = () => 'not cloneable';
  controller.abort(reason);
  await assert.rejects(db.transactionWithRetry(() => {}, { ...fast, signal: controller.signal }), e => {
    assert.ok(e instanceof FrankenTransactionRetryError); assert.equal(e.code, 'ERR_FSQLITE_TRANSACTION_RETRY_CANCELLED');
    assert.equal(e.attempts, 0); assert.equal(e.cause, reason); return true;
  });
  assert.equal(state.attempts, 0); await db.execute('UPDATE counts SET value=1');
});

test('abort during callback waits for callback drain and successful rollback', async t => {
  const { db, state, raw } = await fixture(t); const started = deferred(), release = deferred(); const controller = new AbortController();
  let settled = false;
  const pending = db.transactionWithRetry(async tx => {
    await tx.execute('UPDATE counts SET value=1'); started.resolve(); await release.promise;
  }, { ...fast, signal: controller.signal });
  pending.then(() => { settled = true; }, () => { settled = true; });
  await started.promise; controller.abort('cancel'); await turn();
  assert.equal(settled, false); assert.equal(raw.prepare('SELECT value FROM counts').get().value, 1);
  release.resolve(); await assert.rejects(pending, e => e.code === 'ERR_FSQLITE_TRANSACTION_RETRY_CANCELLED');
  assert.equal(state.attempts, 1); assert.equal(raw.prepare('SELECT value FROM counts').get().value, 0);
});

test('deadline covers slow BEGIN, joins it and rolls back without ever calling the callback', async t => {
  const { db, state } = await fixture(t); const began = deferred(), release = deferred();
  state.afterBoundary = async action => { if (action === 'begin') { began.resolve(); await release.promise; } };
  let callbacks = 0, settled = false;
  const pending = db.transactionWithRetry(() => { callbacks++; }, { ...fast, timeoutMs: 10 });
  pending.then(() => { settled = true; }, () => { settled = true; });
  await began.promise; await sleep(25); assert.equal(settled, false);
  release.resolve(); await assert.rejects(pending, e => e.code === 'ERR_FSQLITE_TRANSACTION_RETRY_TIMEOUT' && e.attempts === 1);
  assert.equal(callbacks, 0); assert.deepEqual(actions(state), ['begin', 'rollback']);
});

test('successful COMMIT dispatched before the deadline remains authoritative', async t => {
  const { db, state, raw } = await fixture(t); const committed = deferred(), release = deferred();
  state.afterBoundary = async action => { if (action === 'commit') { committed.resolve(); await release.promise; } };
  const pending = db.transactionWithRetry(async tx => { await tx.execute('UPDATE counts SET value=1'); return 'saved'; }, { ...fast, timeoutMs: 10 });
  await committed.promise; await sleep(25); release.resolve();
  assert.equal(await pending, 'saved'); assert.equal(state.attempts, 1);
  assert.equal(raw.prepare('SELECT value FROM counts').get().value, 1); assert.ok(!actions(state).includes('rollback'));
});

test('deadline is not reset between attempts and task yields admit cancellation', async t => {
  const { db, state } = await fixture(t);
  state.beforeBoundary = action => { if (action === 'commit') throw busy(); };
  await assert.rejects(db.transactionWithRetry(tx => tx.execute('UPDATE counts SET value=1'),
    { ...fast, maxAttempts: 100, timeoutMs: 20 }), e => e.code === 'ERR_FSQLITE_TRANSACTION_RETRY_TIMEOUT');
  assert.ok(state.attempts < 100); assert.ok(state.attempts > 0);
  const controller = new AbortController(); setTimeout(() => controller.abort('yield reached'), 0);
  await assert.rejects(db.transactionWithRetry(tx => tx.execute('UPDATE counts SET value=1'),
    { ...fast, maxAttempts: 100, signal: controller.signal }), e => e.code === 'ERR_FSQLITE_TRANSACTION_RETRY_CANCELLED');
});

for (const invalid of [{ maxAttempts: 0 }, { maxAttempts: 101 }, { maxAttempts: 1.5 }, { timeoutMs: 0 },
  { timeoutMs: Infinity }, { timeoutMs: 2 ** 31 }, { initialDelayMs: -1 }, { maxDelayMs: NaN },
  { initialDelayMs: 20, maxDelayMs: 10 }, { signal: { aborted: false } }]) {
  test(`invalid retry policy ${JSON.stringify(invalid)} runs no SQL`, async t => {
    const { db, state } = await fixture(t);
    await assert.rejects(db.transactionWithRetry(() => {}, invalid)); assert.equal(state.attempts, 0);
    await db.execute('UPDATE counts SET value=1');
  });
}

test('reentrant option getter cannot steal authority from a newly started transaction', async t => {
  const { db, state } = await fixture(t); const release = deferred(); let other;
  await assert.rejects(db.transactionWithRetry(() => {}, { get maxAttempts() {
    other = db.transaction(async () => { await release.promise; }); return 3;
  } }), e => e.code === 'ERR_FSQLITE_TRANSACTION_OWNERSHIP');
  release.resolve(); await other; assert.equal(state.attempts, 1);
});

test('scheduler requires explicit recovery evidence; a BUSY-shaped rejection alone is insufficient', async () => {
  let calls = 0; const error = busy();
  await assert.rejects(runTransactionRetry(async () => { calls++; throw error; }, resolveTransactionRetryOptions(fast)), e => e === error);
  assert.equal(calls, 1);
});
