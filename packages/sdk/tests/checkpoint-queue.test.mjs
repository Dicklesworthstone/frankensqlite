// Run: node --test packages/sdk/tests/checkpoint-queue.test.mjs
// Production queue/SDK/journal/snapshot-store code, Node SQLite SQL/export,
// and the explicit IndexedDB transaction model. NOT worker IPC, WASM or a browser.
import assert from 'node:assert/strict';
import { readFileSync, writeFileSync, mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { DatabaseSync } from 'node:sqlite';
import { setImmediate as turn } from 'node:timers/promises';
import nodeTest from 'node:test';
import { installIndexedDbModel, ModelObjectStore } from '../../worker/tests/helpers/indexeddb-model.mjs';
const { default: ts } = await import(process.env.FSQLITE_TYPESCRIPT_MODULE ?? 'typescript');
const test = (name, fn) => nodeTest(name, { timeout: 3000 }, fn);
const root = fileURLToPath(new URL('../src/', import.meta.url));
const cache = new Map(), replacements = new Map();
function production(path) {
  if (cache.has(path)) return cache.get(path).exports;
  const module = { exports: {} }; cache.set(path, module);
  const { outputText, diagnostics } = ts.transpileModule(readFileSync(path, 'utf8'), {
    fileName: path, reportDiagnostics: true,
    compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 },
  });
  assert.equal(diagnostics?.filter(d => d.category === ts.DiagnosticCategory.Error).length, 0);
  const require = specifier => {
    if (replacements.has(specifier)) return replacements.get(specifier);
    assert.ok(specifier.startsWith('.'), `Unmocked dependency ${specifier}`);
    return production(resolve(dirname(path), `${specifier}.ts`));
  };
  new Function('require', 'module', 'exports', `${outputText}\n//# sourceURL=${path}`)(require, module, module.exports);
  return module.exports;
}
const sdk = name => production(join(root, `${name}.ts`));
const { IndexedDbSnapshotStore } = production(resolve(root, '../../worker/src/snapshot-store.ts'));
const { FrankenSQLiteError } = sdk('errors');
const { isTransactionConflict } = sdk('transaction-retry');
const busy = () => new FrankenSQLiteError({ code: 'SQLITE_BUSY_SNAPSHOT', sqliteCode: 5,
  extendedCode: 517, message: 'snapshot conflict', transient: true });
const deferred = () => { let resolve; const promise = new Promise(r => { resolve = r; }); return { promise, resolve }; };
const fast = { maxAttempts: 4, timeoutMs: 5000, initialDelayMs: 0, maxDelayMs: 0 };

// Replace only the transport/core boundary. All transaction ownership, retries,
// deadlines, journal collection, queue publication and storage CAS are production.
class SqliteTransport {
  constructor(state) { this.state = state; this.resultEncoding = 'structured-clone'; }
  assertOpen() { if (this.state.closed) throw new Error('closed'); }
  observeFailure(listener) { this.state.listener = listener; return () => { this.state.listener = null; }; }
  async init(config) {
    const s = this.state; s.inits++;
    s.persistence = config.persistence ?? 'memory';
    let saved = null;
    if (s.persistence === 'indexeddb-snapshot') {
      s.store = await IndexedDbSnapshotStore.open(config.dbName);
      saved = await s.store.load();
    }
    if (saved !== null) {
      const path = join(s.dir, 'restored.sqlite'); writeFileSync(path, saved.bytes);
      s.raw = new DatabaseSync(path); s.revision = saved.revision;
    } else s.raw = new DatabaseSync(':memory:');
    const ready = { path: config.dbName ?? ':memory:', persistence: s.persistence, snapshot: saved };
    s.afterReady?.(ready);
    return ready;
  }
  async transaction(action, id, parentId) {
    this.assertOpen(); const s = this.state;
    s.events.push(action); if (action === 'begin' && parentId === undefined) s.attempts++;
    await s.beforeBoundary?.(action, parentId);
    if (action === 'begin') {
      s.raw.exec(parentId === undefined ? 'BEGIN' : `SAVEPOINT child_${id}`);
      s.stack.push({ id, parentId });
    } else {
      const frame = s.stack.at(-1); assert.equal(frame?.id, id);
      s.raw.exec(action === 'commit'
        ? frame.parentId === undefined ? 'COMMIT' : `RELEASE child_${id}`
        : frame.parentId === undefined ? 'ROLLBACK' : `ROLLBACK TO child_${id}; RELEASE child_${id}`);
      s.stack.pop();
    }
    await s.afterBoundary?.(action, parentId);
  }
  cancelTransaction() { this.state.events.push('cancel'); }
  async execute(sql, params = []) { this.assertOpen(); this.state.sql++; return Number(this.state.raw.prepare(sql).run(...params).changes); }
  async executeBatch(sql) { this.assertOpen(); this.state.sql++; this.state.raw.exec(sql); }
  async query(sql, params = []) {
    this.assertOpen(); this.state.sql++;
    const stmt = this.state.raw.prepare(sql), columns = stmt.columns().map(c => c.name), rows = stmt.all(...params);
    return { columns, rows, rowArrays: rows.map(row => columns.map(name => row[name])) };
  }
  async export() {
    this.assertOpen(); const s = this.state; assert.equal(s.stack.length, 0);
    s.events.push('export'); await s.beforeExport?.();
    // VACUUM INTO produces a real standalone main-database image. No invented
    // page layout, JSON pseudo-database or model of SQL atomicity is used.
    const path = join(s.dir, `image-${++s.exports}.sqlite`);
    s.raw.prepare('VACUUM INTO ?').run(path);
    return new Uint8Array(readFileSync(path));
  }
  async checkpoint() {
    this.assertOpen(); const s = this.state;
    s.checkpoints++; s.events.push('checkpoint');
    const bytes = await this.export(); await s.beforeSave?.();
    const saved = await s.store.save(bytes, s.revision); s.revision = saved.revision;
    await s.afterSave?.(saved);
    return saved;
  }
  dispose() {
    const s = this.state; if (s.closed) return;
    s.closed = true; s.events.push('close'); s.raw?.close(); s.store?.close();
    if (s.closeError) throw s.closeError;
  }
  async close() { this.dispose(); }
}
replacements.set('./worker-client', { FrankenWorkerClient: SqliteTransport });
replacements.set('./utils', { normalizeOpenOptions: o => o ?? {}, resolveWorker: worker => worker });
replacements.set('./stream', { checkStreamCancellation() { throw new Error('not exercised'); },
  executeRowStream() { throw new Error('not exercised'); }, streamOptions() { throw new Error('not exercised'); } });
replacements.set('@frankensqlite/worker', { resolveRequestLimits: x => x,
  resolveResultEncoding: x => x ?? 'structured-clone' });
const { FrankenDBQueue, FrankenCheckpointCommitError } = sdk('queue');
const { FrankenDB } = sdk('database');
function stateFor(t) {
  const s = { dir: mkdtempSync(join(tmpdir(), 'fsqlite-checkpoint-queue-')), inits: 0,
    stack: [], events: [], attempts: 0, checkpoints: 0, exports: 0, sql: 0, revision: null };
  t.after(() => { if (!s.closed) { s.raw?.close(); s.store?.close(); s.closed = true; } });
  return s;
}
async function openQueue(t, name, options = { checkpointOnCommit: true }, persistence = 'indexeddb-snapshot') {
  const state = stateFor(t);
  const queue = await FrankenDBQueue.open({ worker: state, dbName: name, persistence }, options);
  t.after(async () => { await queue.close().catch(() => {}); });
  return { queue, state };
}
async function fixture(t, options, persistence) {
  const model = installIndexedDbModel(), name = crypto.randomUUID();
  const { queue, state } = await openQueue(t, name, options, persistence);
  state.raw.exec('CREATE TABLE counts(value INTEGER); INSERT INTO counts VALUES(0); CREATE TABLE audit(v); CREATE TRIGGER log_update AFTER UPDATE ON counts BEGIN INSERT INTO audit VALUES(new.value); END');
  return { queue, state, name, model };
}
const value = state => state.raw.prepare('SELECT value FROM counts').get().value;
async function savedValue(t, name) {
  const store = await IndexedDbSnapshotStore.open(name);
  const saved = await store.load(); store.close();
  if (saved === null) return null;
  const dir = mkdtempSync(join(tmpdir(), 'fsqlite-saved-checkpoint-'));
  const path = join(dir, 'db.sqlite'); writeFileSync(path, saved.bytes);
  const db = new DatabaseSync(path); t.after(() => db.close());
  assert.equal(db.prepare('PRAGMA integrity_check').get().integrity_check, 'ok');
  return { saved, value: db.prepare('SELECT value FROM counts').get().value,
    effects: db.prepare('SELECT count(*) n FROM audit').get().n };
}
async function failPublication(queue, state, cause = new Error('quota')) {
  state.beforeSave = () => { throw cause; };
  let failure;
  await assert.rejects(queue.transaction(async tx => { await tx.execute('UPDATE counts SET value=value+1'); return 'committed-value'; }), error => {
    assert.ok(error instanceof FrankenCheckpointCommitError); failure = error; return true;
  });
  return failure;
}

test('checkpoint policy default is unchanged for memory and snapshot queues', async t => {
  for (const persistence of ['memory', 'indexeddb-snapshot']) {
    const { queue, state } = await fixture(t, {}, persistence);
    assert.equal(queue.checkpointOnCommit, false);
    await queue.transaction(tx => tx.execute('UPDATE counts SET value=1'));
    assert.equal(state.checkpoints, 0); assert.equal(queue.snapshotRevision, null);
  }
});
for (const input of [null, 1, 'true', {}]) {
  test(`invalid checkpoint policy ${JSON.stringify(input)} opens no database`, async t => {
    const state = stateFor(t);
    await assert.rejects(FrankenDBQueue.open({ worker: state }, { checkpointOnCommit: input }), TypeError);
    assert.equal(state.inits, 0);
  });
}
test('checkpoint policy rejects memory mode and joins database cleanup', async t => {
  const state = stateFor(t);
  await assert.rejects(FrankenDBQueue.open({ worker: state }, { checkpointOnCommit: true }), e => e.code === 'ERR_FSQLITE_CHECKPOINT_MODE');
  assert.equal(state.closed, true); assert.deepEqual(state.events, ['close']);
});
test('mode rejection retains close errors', async t => {
  const state = stateFor(t); state.closeError = new Error('close failed');
  await assert.rejects(FrankenDBQueue.open({ worker: state }, { checkpointOnCommit: true }), e => {
    assert.ok(e instanceof AggregateError); assert.equal(e.errors[0].code, 'ERR_FSQLITE_CHECKPOINT_MODE');
    assert.equal(e.errors[1], state.closeError); return true;
  });
});
test('acknowledged job restores its SQL and trigger effects from a real SQLite image', async t => {
  const { queue, state, name } = await fixture(t); const returned = { accepted: 1 };
  assert.equal(queue.checkpointOnCommit, true);
  assert.equal(await queue.transaction(async tx => { await tx.execute('UPDATE counts SET value=7'); return returned; }), returned);
  const stored = await savedValue(t, name); assert.equal(stored.value, 7); assert.equal(stored.effects, 1);
  assert.equal(queue.snapshotRevision, stored.saved.revision); assert.equal(state.checkpoints, 1);
  await queue.close();
  const restored = await openQueue(t, name);
  assert.equal(value(restored.state), 7); assert.equal(restored.queue.snapshotRevision, stored.saved.revision);
});
test('publication retains FIFO capacity and close waits for all accepted commits to checkpoint', async t => {
  const { queue, state, name } = await fixture(t, { checkpointOnCommit: true, maxPendingJobs: 2 });
  const saving = deferred(), release = deferred(); let later = false, settled = false;
  state.beforeSave = async () => { if (state.checkpoints === 1) { saving.resolve(); await release.promise; } };
  const first = queue.transaction(tx => tx.execute('UPDATE counts SET value=value+1')).then(() => { settled = true; });
  await saving.promise;
  const second = queue.transaction(async tx => { later = true; return tx.execute('UPDATE counts SET value=value+1'); });
  await assert.rejects(queue.transaction(() => {}), e => e.code === 'ERR_FSQLITE_JOB_QUEUE_FULL');
  const closing = queue.close(); assert.equal(closing, queue.close());
  await turn(); assert.equal(settled, false); assert.equal(later, false); assert.equal(state.closed, undefined);
  assert.equal(queue.stats.activeJobs, 1); assert.equal(queue.stats.waitingJobs, 1);
  release.resolve(); await first; await second; await closing;
  assert.equal((await savedValue(t, name)).value, 2); assert.equal(state.checkpoints, 2);
  assert.equal(queue.stats.completedJobs, 2); assert.equal(queue.stats.failedJobs, 0);
});
test('put success is not acknowledgement: wait for the IndexedDB transaction complete event', async t => {
  const { queue, name } = await fixture(t); const staged = deferred(), release = deferred(); let settled = false;
  const original = ModelObjectStore.prototype.put;
  t.mock.method(ModelObjectStore.prototype, 'put', function(...args) {
    const result = original.apply(this, args), transaction = this.transaction;
    result.addEventListener('success', () => {
      const advance = transaction.advance.bind(transaction); transaction.advance = () => {};
      staged.resolve(); void release.promise.then(() => { transaction.advance = advance; advance(); });
    });
    return result;
  });
  const pending = queue.transaction(tx => tx.execute('UPDATE counts SET value=4')).then(() => { settled = true; });
  await staged.promise; await turn(); assert.equal(settled, false); assert.equal(queue.snapshotRevision, null);
  release.resolve(); await pending; assert.equal((await savedValue(t, name)).value, 4);
});
test('post-COMMIT publication failure exposes committed value, not a retryable SQL failure', async t => {
  const { queue, state, name } = await fixture(t); const cause = busy();
  const failure = await failPublication(queue, state, cause);
  assert.equal(failure.code, 'ERR_FSQLITE_COMMITTED_CHECKPOINT_FAILED'); assert.equal(failure.sqlCommitted, true);
  assert.equal(failure.checkpointConfirmed, false); assert.equal(failure.value, 'committed-value');
  assert.equal(failure.cause, cause); assert.equal(failure.previousRevision, null);
  assert.equal(isTransactionConflict(failure), false); assert.equal(value(state), 1);
  assert.equal(await savedValue(t, name), null); assert.equal(queue.stats.checkpointRecoveryRequired, true);
});
test('failed checkpoint fences already queued callbacks before even journal SQL executes', async t => {
  const { queue, state } = await fixture(t); const saving = deferred(), release = deferred(); let later = false;
  await queue.subscribe(['counts'], () => {});
  state.beforeSave = async () => { saving.resolve(); await release.promise; throw new Error('quota'); };
  const first = queue.transaction(tx => tx.execute('UPDATE counts SET value=1'));
  const firstFailure = assert.rejects(first, e => e instanceof FrankenCheckpointCommitError);
  await saving.promise; const before = state.sql;
  const queued = queue.transaction(() => { later = true; });
  const refused = assert.rejects(queued, e => e.code === 'ERR_FSQLITE_CHECKPOINT_RECOVERY_REQUIRED');
  release.resolve(); await firstFailure; await refused;
  assert.equal(later, false); assert.equal(state.sql, before); assert.equal(queue.stats.failedJobs, 2);
});
test('recovery checkpoint saves once, releases the fence and never replays the committed callback', async t => {
  const { queue, state, name } = await fixture(t); const failure = await failPublication(queue, state);
  const accepted = queue.stats.acceptedJobs;
  await assert.rejects(queue.transaction(() => {}), e => e.code === 'ERR_FSQLITE_CHECKPOINT_RECOVERY_REQUIRED' && e.cause === failure);
  await assert.rejects(queue.transactionWithRetry(() => {}, fast), e => e.code === 'ERR_FSQLITE_CHECKPOINT_RECOVERY_REQUIRED');
  await assert.rejects(queue.subscribe(['counts'], () => {}), e => e.code === 'ERR_FSQLITE_CHECKPOINT_RECOVERY_REQUIRED');
  assert.equal(queue.stats.acceptedJobs, accepted);
  const bytes = await queue.export(); assert.ok(bytes.length >= 512); assert.equal(queue.stats.checkpointRecoveryRequired, true);
  state.beforeSave = undefined; const receipt = await queue.checkpoint();
  assert.equal(queue.stats.checkpointRecoveryRequired, false); assert.equal(queue.snapshotRevision, receipt.revision);
  await queue.transaction(tx => tx.execute('UPDATE counts SET value=value+1'));
  const stored = await savedValue(t, name); assert.equal(stored.value, 2); assert.equal(stored.effects, 2);
});
test('failed and pre-cancelled recovery barriers do not clear the fence', async t => {
  const { queue, state } = await fixture(t); await failPublication(queue, state);
  await assert.rejects(queue.checkpoint(), /quota/); assert.equal(queue.stats.checkpointRecoveryRequired, true);
  const control = new AbortController(); control.abort('not now');
  const before = state.checkpoints; await assert.rejects(queue.checkpoint({ signal: control.signal }));
  assert.equal(state.checkpoints, before); assert.equal(queue.stats.checkpointRecoveryRequired, true);
});
test('failing SQL rolls back without checkpointing or requiring storage recovery', async t => {
  const { queue, state, name } = await fixture(t); const cause = new Error('callback failure');
  await assert.rejects(queue.transaction(async tx => { await tx.execute('UPDATE counts SET value=8'); throw cause; }), e => e === cause);
  assert.equal(value(state), 0); assert.equal(state.checkpoints, 0); assert.equal(await savedValue(t, name), null);
  assert.equal(queue.stats.checkpointRecoveryRequired, false);
  await queue.transaction(tx => tx.execute('UPDATE counts SET value=1'));
});
test('whole SQL retry checkpoints only the final successful attempt', async t => {
  const { queue, state, name } = await fixture(t); let failures = 1, callbacks = 0;
  state.beforeBoundary = (action, parent) => { if (action === 'commit' && parent === undefined && failures-- > 0) throw busy(); };
  await queue.transactionWithRetry(async tx => { callbacks++; await tx.execute('UPDATE counts SET value=value+1'); }, fast);
  assert.equal(callbacks, 2); assert.equal(state.checkpoints, 1);
  const saved = await savedValue(t, name); assert.equal(saved.value, 1); assert.equal(saved.effects, 1);
});
test('BUSY-shaped checkpoint errors are outside the retry loop and cannot duplicate committed SQL', async t => {
  const { queue, state } = await fixture(t); let callbacks = 0; state.beforeSave = () => { throw busy(); };
  await assert.rejects(queue.transactionWithRetry(async tx => { callbacks++; await tx.execute('UPDATE counts SET value=value+1'); }, fast), e => e instanceof FrankenCheckpointCommitError);
  assert.equal(callbacks, 1); assert.equal(state.checkpoints, 1); assert.equal(value(state), 1);
});
test('competing snapshot queues preserve CAS winner and expose the losing committed image for recovery', async t => {
  const { queue: winner, state: a, name } = await fixture(t);
  await winner.transaction(() => {});
  const { queue: loser, state: b } = await openQueue(t, name);
  const baseline = loser.snapshotRevision;
  await winner.transaction(tx => tx.execute('UPDATE counts SET value=10'));
  let failure;
  await assert.rejects(loser.transaction(tx => tx.execute('UPDATE counts SET value=20')), e => { failure = e; return e.cause?.code === 'ERR_FSQLITE_SNAPSHOT_CONFLICT'; });
  assert.equal(failure.previousRevision, baseline); assert.equal(value(b), 20); assert.equal(value(a), 10);
  assert.equal((await savedValue(t, name)).value, 10); assert.equal(loser.stats.checkpointRecoveryRequired, true);
  await assert.rejects(loser.checkpoint(), e => e.code === 'ERR_FSQLITE_SNAPSHOT_CONFLICT');
  const image = await loser.export(); assert.ok(image.length > 0); assert.equal(loser.snapshotRevision, baseline);
});
test('lost publication acknowledgement remains unknown until authoritative reopen reconciles lineage', async t => {
  const { queue, state, name } = await fixture(t); state.afterSave = () => { throw new Error('response lost'); };
  let failure;
  await assert.rejects(queue.transaction(tx => tx.execute('UPDATE counts SET value=3')), e => { failure = e; return e instanceof FrankenCheckpointCommitError; });
  assert.equal(failure.checkpointConfirmed, false); assert.equal((await savedValue(t, name)).value, 3);
  assert.equal(queue.snapshotRevision, null); assert.equal(queue.stats.checkpointRecoveryRequired, true);
  state.afterSave = undefined;
  // The worker advanced its revision while the SDK did not. A subsequent
  // publication may succeed, but its skipped parent is not a valid receipt.
  await assert.rejects(queue.checkpoint(), e => e.code === 'ERR_FSQLITE_SNAPSHOT_RECEIPT');
  assert.equal(queue.stats.checkpointRecoveryRequired, true);
  assert.ok((await queue.export()).length > 0);
  const { queue: reopened, state: restored } = await openQueue(t, name);
  assert.equal(value(restored), 3);
  const receipt = await reopened.checkpoint();
  assert.equal(reopened.snapshotRevision, receipt.revision);
  assert.equal((await savedValue(t, name)).effects, 1);
});
test('late cancellation cannot abandon saving an acknowledged COMMIT', async t => {
  const { queue, state, name } = await fixture(t); const control = new AbortController(); let now = 100;
  t.mock.method(performance, 'now', () => now);
  state.beforeSave = () => { now = 10000; control.abort('late'); };
  const result = await queue.transaction(async tx => { await tx.execute('UPDATE counts SET value=2'); return 'saved'; }, { timeoutMs: 100, signal: control.signal });
  assert.equal(result, 'saved'); assert.equal((await savedValue(t, name)).value, 2); assert.equal(state.events.includes('rollback'), false);
});
test('publication failure outranks late cancellation and preserves the original storage cause', async t => {
  const { queue, state } = await fixture(t); const control = new AbortController(), cause = new Error('storage failure');
  state.beforeSave = () => { control.abort('late'); throw cause; };
  await assert.rejects(queue.transaction(tx => tx.execute('UPDATE counts SET value=1'), { signal: control.signal }), e => e instanceof FrankenCheckpointCommitError && e.cause === cause);
  assert.equal(value(state), 1);
});
test('pre-commit cancellation rolls back and never checkpoints', async t => {
  const { queue, state } = await fixture(t); const control = new AbortController();
  await assert.rejects(queue.transaction(async tx => { await tx.execute('UPDATE counts SET value=5'); control.abort('early'); }, { signal: control.signal }));
  assert.equal(value(state), 0); assert.equal(state.checkpoints, 0); assert.equal(queue.stats.checkpointRecoveryRequired, false);
});
test('local committed notifications survive checkpoint failure; recovery emits no duplicate', async t => {
  const { queue, state } = await fixture(t); const changes = [], first = deferred();
  await queue.subscribe(['counts'], change => { changes.push(change); first.resolve(); });
  await failPublication(queue, state); await first.promise;
  assert.equal(changes.length, 1); assert.equal(changes[0].commits, 1n); assert.equal(queue.changeSequence, 1n);
  state.beforeSave = undefined; await queue.checkpoint(); await turn();
  assert.equal(changes.length, 1); assert.equal(queue.changeSequence, 1n);
  await queue.transaction(tx => tx.execute('UPDATE counts SET value=value+1'));
  assert.equal(queue.changeSequence, 2n);
});
test('read-only transaction jobs still checkpoint under the explicit per-transaction policy', async t => {
  const { queue, state, name } = await fixture(t);
  assert.equal(await queue.transaction(async tx => (await tx.query('SELECT value FROM counts')).rows[0].value), 0);
  assert.equal(state.checkpoints, 1); assert.equal((await savedValue(t, name)).value, 0);
});
test('unresolved checkpoint makes close reject after releasing resources, with one shared promise', async t => {
  const { queue, state } = await fixture(t); const failure = await failPublication(queue, state);
  const first = queue.close(); assert.equal(first, queue.close());
  await assert.rejects(first, e => e === failure); assert.equal(state.closed, true);
  assert.equal(queue.stats.state, 'closed'); assert.equal(queue.stats.checkpointRecoveryRequired, true);
  assert.equal(state.events.filter(e => e === 'close').length, 1);
});
test('unresolved checkpoint plus close failure retains both causes and committed-state evidence', async t => {
  const { queue, state } = await fixture(t); const failure = await failPublication(queue, state);
  state.closeError = new Error('close failure');
  await assert.rejects(queue.close(), e => e instanceof AggregateError && e.errors[0] === failure && e.errors[1] === state.closeError);
  assert.equal(state.closed, true);
});
test('failed export cannot clear checkpoint recovery', async t => {
  const { queue, state } = await fixture(t); await failPublication(queue, state);
  state.beforeExport = () => { throw new Error('export failed'); };
  await assert.rejects(queue.export(), /export failed/); assert.equal(queue.stats.checkpointRecoveryRequired, true);
});

test('an IndexedDB abort after put success retains the previous snapshot and fences the new commit', async t => {
  const { queue, state, name } = await fixture(t);
  await queue.transaction(tx => tx.execute('UPDATE counts SET value=1'));
  const old = queue.snapshotRevision;
  const original = ModelObjectStore.prototype.put;
  t.mock.method(ModelObjectStore.prototype, 'put', function(...args) {
    const request = original.apply(this, args), transaction = this.transaction;
    request.addEventListener('success', () => { transaction.error = new DOMException('quota', 'QuotaExceededError'); transaction.abort(); });
    return request;
  });
  await assert.rejects(queue.transaction(tx => tx.execute('UPDATE counts SET value=2')), e => e instanceof FrankenCheckpointCommitError);
  const stored = await savedValue(t, name);
  assert.equal(stored.value, 1); assert.equal(stored.saved.revision, old);
  assert.equal(value(state), 2); assert.equal(queue.stats.checkpointRecoveryRequired, true);
});
test('a recovery barrier already behind the failing job can unblock later accepted jobs in FIFO order', async t => {
  const { queue, state, name } = await fixture(t); const saving = deferred(), release = deferred();
  state.beforeSave = async () => {
    if (state.checkpoints === 1) { saving.resolve(); await release.promise; throw new Error('temporary quota'); }
  };
  const first = queue.transaction(tx => tx.execute('UPDATE counts SET value=1'));
  const failed = assert.rejects(first, e => e instanceof FrankenCheckpointCommitError);
  await saving.promise;
  const recovery = queue.checkpoint();
  const next = queue.transaction(tx => tx.execute('UPDATE counts SET value=2'));
  release.resolve(); await failed; await recovery; await next;
  assert.equal((await savedValue(t, name)).value, 2); assert.equal(queue.stats.checkpointRecoveryRequired, false);
  assert.equal(state.checkpoints, 3);
});
test('listener failure does not turn a saved commit into failure or suppress its checkpoint', async t => {
  const { queue, state, name } = await fixture(t); const cause = new Error('listener failed');
  const subscription = await queue.subscribe(['counts'], () => { throw cause; });
  const listenerFailed = assert.rejects(subscription.done, e => e === cause);
  await queue.transaction(tx => tx.execute('UPDATE counts SET value=9')); await listenerFailed;
  assert.equal((await savedValue(t, name)).value, 9); assert.equal(state.checkpoints, 1);
  assert.equal(queue.stats.checkpointRecoveryRequired, false);
});
test('callback return values from a failed SQL attempt are never exposed as committed checkpoint values', async t => {
  const { queue, state } = await fixture(t); const failed = busy();
  state.beforeBoundary = action => { if (action === 'commit') throw failed; };
  await assert.rejects(queue.transaction(async tx => { await tx.execute('UPDATE counts SET value=2'); return { doNotPublish: true }; }), e => e === failed);
  assert.equal(state.checkpoints, 0); assert.equal(value(state), 0); assert.equal(queue.stats.checkpointRecoveryRequired, false);
});

const unrelatedRevision = '00000000-0000-4000-8000-000000000001';
for (const [name, change] of [
  ['empty revision', saved => { saved.revision = ''; }],
  ['non-UUID revision', saved => { saved.revision = 'saved'; }],
  ['non-v4 revision', saved => { saved.revision = '00000000-0000-1000-8000-000000000001'; }],
  ['empty digest', saved => { saved.sha256 = ''; }],
  ['non-hex digest', saved => { saved.sha256 = 'g'.repeat(64); }],
  ['zero length', saved => { saved.byteLength = 0; }],
  ['unaligned length', saved => { saved.byteLength = 513; }],
  ['oversized length', saved => { saved.byteLength = 64 * 1024 * 1024 + 512; }],
  ['non-finite length', saved => { saved.byteLength = NaN; }],
  ['malformed parent', saved => { saved.parentRevision = ''; }],
  ['self-parent', saved => { saved.parentRevision = saved.revision; }],
]) {
  test(`receipt rejects ${name} without acknowledging or replaying the committed write`, async t => {
    const { queue, state, name: dbName } = await fixture(t);
    state.afterSave = change;
    await assert.rejects(queue.transaction(tx => tx.execute('UPDATE counts SET value=1')), error => {
      assert.ok(error instanceof FrankenCheckpointCommitError);
      assert.equal(error.cause.code, 'ERR_FSQLITE_SNAPSHOT_RECEIPT'); return true;
    });
    assert.equal(queue.snapshotRevision, null); assert.equal(queue.stats.checkpointRecoveryRequired, true);
    assert.equal((await savedValue(t, dbName)).value, 1); // Invalid ack is NOT proof of failed storage.
    state.afterSave = undefined;
    await assert.rejects(queue.checkpoint(), e => e.code === 'ERR_FSQLITE_SNAPSHOT_RECEIPT');
    assert.equal(state.checkpoints, 1); // Unknown lineage requires reopen, not another blind publication.
    assert.ok((await queue.export()).length > 0);
  });
}
test('receipt binds parent to the last acknowledged snapshot and cannot silently skip a generation', async t => {
  const { queue, state } = await fixture(t);
  await queue.transaction(tx => tx.execute('UPDATE counts SET value=1'));
  const previous = queue.snapshotRevision;
  state.afterSave = saved => { saved.parentRevision = unrelatedRevision; };
  await assert.rejects(queue.transaction(tx => tx.execute('UPDATE counts SET value=2')), e => e.cause?.code === 'ERR_FSQLITE_SNAPSHOT_RECEIPT');
  assert.equal(queue.snapshotRevision, previous); assert.equal(value(state), 2);
});
test('receipt capture owns frozen scalar metadata rather than returning transport-owned aliases', async t => {
  const { queue, state } = await fixture(t); let wire;
  state.afterSave = saved => { wire = saved; };
  const receipt = await queue.checkpoint();
  const revision = receipt.revision; assert.ok(Object.isFrozen(receipt));
  assert.notEqual(receipt, wire); wire.revision = unrelatedRevision; wire.sha256 = '';
  assert.equal(receipt.revision, revision); assert.equal(queue.snapshotRevision, revision);
  assert.match(receipt.sha256, /^[0-9a-f]{64}$/);
});
test('receipt accessors are rejected without executing transport-defined getters', async t => {
  const { queue, state } = await fixture(t); let getters = 0;
  state.afterSave = saved => Object.defineProperty(saved, 'sha256', { get() { getters++; return '0'.repeat(64); } });
  await assert.rejects(queue.transaction(tx => tx.execute('UPDATE counts SET value=1')), e => e.cause?.code === 'ERR_FSQLITE_SNAPSHOT_RECEIPT');
  assert.equal(getters, 0); assert.equal(queue.snapshotRevision, null);
});
test('initialization rejects malformed saved metadata and closes its worker before exposing a database', async t => {
  installIndexedDbModel(); const state = stateFor(t);
  state.afterReady = ready => { ready.snapshot = { revision: '', parentRevision: null, byteLength: 0, sha256: '' }; };
  await assert.rejects(FrankenDB.open({ worker: state, dbName: crypto.randomUUID(), persistence: 'indexeddb-snapshot' }), e => e.code === 'ERR_FSQLITE_SNAPSHOT_RECEIPT');
  assert.equal(state.closed, true); assert.equal(state.attempts, 0);
});
test('initialization refuses saved-snapshot claims in memory mode', async t => {
  const state = stateFor(t);
  state.afterReady = ready => { ready.snapshot = { revision: unrelatedRevision, parentRevision: null, byteLength: 512, sha256: '0'.repeat(64) }; };
  await assert.rejects(FrankenDB.open({ worker: state }), e => e.code === 'ERR_FSQLITE_SNAPSHOT_RECEIPT');
  assert.equal(state.closed, true);
});
test('invalid correlated worker responses leave checkpoint lineage unknown, not retryable', async t => {
  const { queue, state } = await fixture(t);
  state.beforeSave = () => { throw new FrankenSQLiteError({ code: 'ERR_FSQLITE_WORKER_RESPONSE', message: 'malformed reply' }); };
  await assert.rejects(queue.transaction(tx => tx.execute('UPDATE counts SET value=1')), e => e.cause?.code === 'ERR_FSQLITE_SNAPSHOT_RECEIPT');
  state.beforeSave = undefined;
  await assert.rejects(queue.checkpoint(), e => e.code === 'ERR_FSQLITE_SNAPSHOT_RECEIPT');
  assert.equal(state.checkpoints, 1); assert.equal(value(state), 1);
});
test('out-of-order checkpoint acknowledgements cannot advance or roll back acknowledged lineage', async t => {
  installIndexedDbModel(); const state = stateFor(t), firstSaved = deferred(), release = deferred();
  const db = await FrankenDB.open({ worker: state, dbName: crypto.randomUUID(), persistence: 'indexeddb-snapshot' });
  state.raw.exec('CREATE TABLE counts(value); INSERT INTO counts VALUES(1)');
  let replies = 0;
  state.afterSave = async () => { if (++replies === 1) { firstSaved.resolve(); await release.promise; } };
  const first = db.checkpoint(); const rejected = assert.rejects(first, e => e.code === 'ERR_FSQLITE_SNAPSHOT_RECEIPT');
  await firstSaved.promise;
  try { await assert.rejects(db.checkpoint(), e => e.code === 'ERR_FSQLITE_SNAPSHOT_RECEIPT'); }
  finally { release.resolve(); await rejected; }
  assert.equal(db.snapshotRevision, null); assert.equal(state.checkpoints, 2);
  await db.close();
});

test('normal concurrent checkpoint calls acknowledge their FIFO parent chain without false conflict', async t => {
  installIndexedDbModel(); const state = stateFor(t);
  const db = await FrankenDB.open({ worker: state, dbName: crypto.randomUUID(), persistence: 'indexeddb-snapshot' });
  state.raw.exec('CREATE TABLE counts(value); INSERT INTO counts VALUES(1)');
  // The production worker owns a FIFO. Reproduce that transport scheduling,
  // not simultaneous calls to the fixture's unsynchronized core connection.
  const checkpoint = SqliteTransport.prototype.checkpoint;
  let tail = Promise.resolve();
  t.mock.method(SqliteTransport.prototype, 'checkpoint', function() {
    const result = tail.then(() => checkpoint.call(this));
    tail = result.then(() => {}, () => {});
    return result;
  });
  const [first, second, third] = await Promise.all([db.checkpoint(), db.checkpoint(), db.checkpoint()]);
  assert.equal(first.parentRevision, null);
  assert.equal(second.parentRevision, first.revision);
  assert.equal(third.parentRevision, second.revision);
  assert.equal(db.snapshotRevision, third.revision); assert.equal(state.checkpoints, 3);
  await db.close();
});
test('initialization rejects snapshot getters without invoking them, retaining cleanup errors', async t => {
  installIndexedDbModel(); const state = stateFor(t); let invoked = false;
  state.closeError = new Error('cleanup failed');
  state.afterReady = ready => Object.defineProperty(ready, 'snapshot', { get() { invoked = true; return null; } });
  await assert.rejects(FrankenDB.open({ worker: state, dbName: crypto.randomUUID(), persistence: 'indexeddb-snapshot' }), e => {
    assert.ok(e instanceof AggregateError);
    assert.equal(e.errors[0].code, 'ERR_FSQLITE_SNAPSHOT_RECEIPT');
    assert.equal(e.errors[1], state.closeError); return true;
  });
  assert.equal(invoked, false); assert.equal(state.closed, true);
});
