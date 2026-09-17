// Run: node --test packages/sdk/tests/migrations.test.mjs
// Production SDK + worker SQL preflight, injected Node SQLite transport.
// Not native FrankenSQLite, worker IPC, WASM or browser certification.
import assert from 'node:assert/strict';
import { readFileSync, mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { DatabaseSync } from 'node:sqlite';
import { createHash } from 'node:crypto';
import { setImmediate as turn, setTimeout as sleep } from 'node:timers/promises';
import nodeTest from 'node:test';
import { installIndexedDbModel } from '../../worker/tests/helpers/indexeddb-model.mjs';
const { default: ts } = await import(process.env.FSQLITE_TYPESCRIPT_MODULE ?? 'typescript');
const test = (name, fn) => nodeTest(name, { timeout: 5000 }, fn);
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
const worker = name => production(resolve(root, `../../worker/src/${name}.ts`));
const { parameterLayout } = worker('bindings');
const { validateManagedSql } = worker('transactions');
const { IndexedDbSnapshotStore } = worker('snapshot-store');
const { FrankenSQLiteError } = sdk('errors');
const deferred = () => { let resolve; const promise = new Promise(r => { resolve = r; }); return { promise, resolve }; };
const names = { 5: 'SQLITE_BUSY', 261: 'SQLITE_BUSY_RECOVERY', 517: 'SQLITE_BUSY_SNAPSHOT', 773: 'SQLITE_BUSY_TIMEOUT' };
const busy = () => new FrankenSQLiteError({ code: 'SQLITE_BUSY_SNAPSHOT', sqliteCode: 5,
  extendedCode: 517, message: 'snapshot conflict', transient: true });
function sqliteError(error) {
  if (!Number.isSafeInteger(error?.errcode)) return error;
  return new FrankenSQLiteError({ code: names[error.errcode] ?? 'SQLITE_ERROR',
    sqliteCode: error.errcode & 255, extendedCode: error.errcode,
    transient: (error.errcode & 255) === 5, message: error.message });
}

class SqliteTransport {
  constructor(state) { this.s = state; this.resultEncoding = 'structured-clone'; }
  assertOpen() { if (this.s.closed) throw new Error('closed'); }
  observeFailure(listener) { this.s.listener = listener; return () => { this.s.listener = null; }; }
  async init(config) {
    const s = this.s; let saved = null;
    if (config.persistence === 'indexeddb-snapshot') {
      s.store = await IndexedDbSnapshotStore.open(config.dbName); saved = await s.store.load();
    }
    if (saved) { writeFileSync(s.path, saved.bytes); s.revision = saved.revision; }
    s.raw = new DatabaseSync(s.path);
    return { path: config.dbName ?? s.path, persistence: config.persistence ?? 'memory', snapshot: saved };
  }
  async transaction(action, id, parentId) {
    this.assertOpen(); const s = this.s;
    s.events.push(action); if (action === 'begin' && parentId === undefined) s.attempts++;
    try {
      await s.beforeBoundary?.(action, parentId);
      if (action === 'begin') {
        s.raw.exec(parentId === undefined ? 'BEGIN' : `SAVEPOINT child_${id}`); s.stack.push({ id, parentId });
      } else {
        const frame = s.stack.at(-1); assert.equal(frame?.id, id);
        s.raw.exec(action === 'commit' ? frame.parentId === undefined ? 'COMMIT' : `RELEASE child_${id}`
          : frame.parentId === undefined ? 'ROLLBACK' : `ROLLBACK TO child_${id}; RELEASE child_${id}`);
        s.stack.pop();
      }
      await s.afterBoundary?.(action, parentId);
    } catch (error) { throw sqliteError(error); }
  }
  cancelTransaction() { this.s.events.push('cancel'); }
  async execute(sql, params = []) {
    this.assertOpen(); this.s.sql.push(sql); await this.s.beforeSql?.(sql);
    try {
      const result = Number(this.s.raw.prepare(sql).run(...params).changes);
      await this.s.afterSql?.(sql); return result;
    } catch (error) { throw sqliteError(error); }
  }
  async executeBatch(sql) {
    this.assertOpen(); validateManagedSql(sql, true); this.s.sql.push(sql); await this.s.beforeSql?.(sql);
    try { this.s.raw.exec(sql); await this.s.afterSql?.(sql); }
    catch (error) { throw sqliteError(error); }
  }
  async query(sql, params = []) {
    this.assertOpen(); this.s.sql.push(sql); await this.s.beforeSql?.(sql);
    try {
      const stmt = this.s.raw.prepare(sql); if (this.s.bigints) stmt.setReadBigInts(true);
      const columns = stmt.columns().map(c => c.name), rows = stmt.all(...params);
      const result = { columns, rows, rowArrays: rows.map(row => columns.map(name => row[name])) };
      await this.s.afterSql?.(sql); return result;
    } catch (error) { throw sqliteError(error); }
  }
  async export() {
    this.assertOpen(); const s = this.s; assert.equal(s.stack.length, 0);
    s.events.push('export'); const path = join(s.dir, `image-${++s.exports}.sqlite`);
    s.raw.prepare('VACUUM INTO ?').run(path); return new Uint8Array(readFileSync(path));
  }
  async checkpoint() {
    const s = this.s; s.events.push('checkpoint'); s.checkpoints++;
    const bytes = await this.export(); await s.beforeSave?.();
    const saved = await s.store.save(bytes, s.revision); s.revision = saved.revision;
    await s.afterSave?.(saved); return saved;
  }
  dispose() { if (this.s.closed) return; this.s.closed = true; this.s.events.push('close'); this.s.raw?.close(); this.s.store?.close(); }
  async close() { this.dispose(); }
}
replacements.set('./worker-client', { FrankenWorkerClient: SqliteTransport });
replacements.set('./utils', { normalizeOpenOptions: o => o ?? {}, resolveWorker: worker => worker });
replacements.set('./stream', { checkStreamCancellation() { throw new Error('not exercised'); },
  executeRowStream() { throw new Error('not exercised'); }, streamOptions() { throw new Error('not exercised'); } });
replacements.set('@frankensqlite/worker', { parameterLayout, validateManagedSql,
  resolveRequestLimits: x => x, resolveResultEncoding: x => x ?? 'structured-clone' });
const { FrankenDB } = sdk('database');
const { FrankenMigrationPlan, FrankenMigrationError, MIGRATION_HISTORY_TABLE: H } = sdk('migrations');
const migration = (version, statements, name = `migration_${version}`) => ({ version, name, statements });
const base = [migration(1, ['CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT NOT NULL)',
  "INSERT INTO items VALUES (1, 'Ada')"]), migration(10, ['ALTER TABLE items ADD COLUMN active INTEGER DEFAULT 1',
  'CREATE UNIQUE INDEX item_name ON items(name)'])];
const plan = () => new FrankenMigrationPlan(base);
const code = expected => e => e.code === expected;
function stateFor(t) {
  const dir = mkdtempSync(join(tmpdir(), 'fsqlite-migration-'));
  const s = { dir, path: join(dir, 'db.sqlite'), sql: [], stack: [], events: [], attempts: 0, checkpoints: 0, exports: 0, revision: null };
  t.after(() => { if (!s.closed) { s.raw?.close(); s.store?.close(); s.closed = true; } });
  return s;
}
async function fixture(t) {
  const state = stateFor(t); const db = await FrankenDB.open({ worker: state });
  return { db, state, raw: state.raw };
}
const exists = (raw, name, schema = 'main') => raw.prepare(`SELECT count(*) n FROM ${schema}.sqlite_master WHERE name=?`).get(name).n !== 0;
const history = raw => raw.prepare(`SELECT version,name,sha256 FROM main."${H}" ORDER BY version`).all();

test('fresh inspection reports pending versions without creating history or changing user_version', async t => {
  const { db, raw } = await fixture(t); raw.exec('PRAGMA user_version=99');
  const status = await plan().inspect(db);
  assert.equal(status.currentVersion, 0); assert.deepEqual(status.pending.map(x => x.version), [1, 10]);
  assert.equal(exists(raw, H), false); assert.equal(raw.prepare('PRAGMA user_version').get().user_version, 99);
  assert.ok(Object.isFrozen(status) && Object.isFrozen(status.applied) && Object.isFrozen(status.pending));
});
test('all pending versions apply in one transaction; rerun verifies and performs no DDL/DML', async t => {
  const { db, raw, state } = await fixture(t); const p = plan();
  const result = await p.apply(db);
  assert.equal(result.previousVersion, 0); assert.equal(result.currentVersion, 10);
  assert.deepEqual(result.applied.map(x => x.version), [1, 10]);
  assert.deepEqual(state.events, ['begin', 'commit']);
  assert.equal(raw.prepare('SELECT active FROM items').get().active, 1);
  assert.ok(Object.isFrozen(result) && Object.isFrozen(result.applied) && Object.isFrozen(result.applied[0]));
  state.sql.length = 0;
  const again = await new FrankenMigrationPlan(base).apply(db);
  assert.deepEqual(again, { previousVersion: 10, currentVersion: 10, applied: [] });
  assert.ok(state.sql.every(sql => /^(SELECT|PRAGMA)/.test(sql))); assert.equal(history(raw).length, 2);
});
test('checksum has exact definition and domain separation, including statement boundaries and Unicode', async t => {
  const { db } = await fixture(t);
  const entry = migration(2, ["CREATE TABLE unicode(v TEXT DEFAULT 'é;𝄞')"], 'Δ upgrades');
  const p = new FrankenMigrationPlan([entry]); const status = await p.inspect(db);
  const expected = createHash('sha256').update(JSON.stringify(['frankensqlite-sdk-migration', 1, 2, entry.name, entry.statements])).digest('hex');
  assert.equal(status.pending[0].sha256, expected);
  await p.apply(db); assert.equal((await p.inspect(db)).applied[0].sha256, expected);
});
test('partial prefix resumes and file reopen preserves both data and exact migration identities', async t => {
  const { db, state, raw } = await fixture(t);
  await new FrankenMigrationPlan(base.slice(0, 1)).apply(db);
  const result = await plan().apply(db); assert.deepEqual(result.applied.map(x => x.version), [10]);
  const prior = history(raw); await db.close();
  const reopened = await FrankenDB.open({ worker: stateFor(t) }); await reopened.close();
  const s = stateFor(t); s.path = state.path;
  const peer = await FrankenDB.open({ worker: s }); const status = await plan().inspect(peer);
  assert.deepEqual(status.applied.map(x => ({ ...x })), prior.map(x => ({ ...x })));
  assert.equal(s.raw.prepare('PRAGMA integrity_check').get().integrity_check, 'ok');
});
test('failure in a later migration rolls back earlier DDL, seed data, history creation and trigger effects', async t => {
  const { db, raw } = await fixture(t);
  raw.exec('CREATE TABLE sentinel(n); INSERT INTO sentinel VALUES(1)');
  const p = new FrankenMigrationPlan([...base, migration(20, [
    'CREATE TRIGGER inserted AFTER INSERT ON items BEGIN UPDATE sentinel SET n=n+1; END',
    "INSERT INTO items(id,name) VALUES(2,'Grace')", "INSERT INTO items(id,name) VALUES(3,'Ada')",
  ])]);
  await assert.rejects(p.apply(db), e => e.sqliteCode === 19);
  assert.equal(exists(raw, H), false); assert.equal(exists(raw, 'items'), false);
  assert.equal(raw.prepare('SELECT n FROM sentinel').get().n, 1);
});
test('failed upgrade preserves the already committed prefix, then a corrected pending definition can resume', async t => {
  const { db, raw } = await fixture(t);
  await new FrankenMigrationPlan(base.slice(0, 1)).apply(db); const old = history(raw);
  await assert.rejects(new FrankenMigrationPlan([...base, migration(20, ['INSERT INTO missing VALUES(1)'])]).apply(db));
  assert.deepEqual(history(raw), old); assert.equal(exists(raw, 'item_name'), false);
  await plan().apply(db); assert.equal(history(raw).length, 2);
});
for (const [label, alter] of [
  ['SQL', e => ({ ...e, statements: [...e.statements, "INSERT INTO items VALUES (2, 'Grace')"] })],
  ['whitespace', e => ({ ...e, statements: e.statements.map(s => `${s} `) })],
  ['name', e => ({ ...e, name: 'renamed' })],
]) test(`applied ${label} drift refuses even a no-op run before any pending work`, async t => {
  const { db, raw, state } = await fixture(t); await plan().apply(db); state.sql.length = 0;
  await assert.rejects(new FrankenMigrationPlan([alter(base[0]), base[1]]).apply(db), code('ERR_FSQLITE_MIGRATION_DRIFT'));
  assert.ok(state.sql.every(s => /^(SELECT|PRAGMA)/.test(s))); assert.equal(history(raw).length, 2);
});
for (const [name, definitions] of [['older client', [base[0]]], ['omitted lower version', [base[1]]],
  ['empty client against nonempty history', []], ['inserted earlier version', [migration(1, base[0].statements), migration(5, ['CREATE TABLE later(v)']), base[1]]]]) {
  test(`history rejects ${name}`, async t => {
    const { db, raw } = await fixture(t); await plan().apply(db);
    await assert.rejects(new FrankenMigrationPlan(definitions).apply(db), code('ERR_FSQLITE_MIGRATION_HISTORY'));
    assert.equal(history(raw).length, 2);
  });
}
test('missing history in the middle is rejected rather than inferred from MAX(version)', async t => {
  const { db, raw } = await fixture(t); await plan().apply(db); raw.exec(`DELETE FROM "${H}" WHERE version=1`);
  await assert.rejects(plan().apply(db), code('ERR_FSQLITE_MIGRATION_HISTORY'));
});
for (const [label, ddl] of [
  ['view', `CREATE VIEW "${H}" AS SELECT 1 version, 'x' name, 'x' sha256`],
  ['wrong columns', `CREATE TABLE "${H}" (version INTEGER PRIMARY KEY, name TEXT NOT NULL)`],
  ['hidden generated field', `CREATE TABLE "${H}" (version INTEGER PRIMARY KEY, name TEXT NOT NULL, sha256 TEXT NOT NULL, extra AS (1))`],
  ['default injection', `CREATE TABLE "${H}" (version INTEGER PRIMARY KEY, name TEXT NOT NULL DEFAULT 'x', sha256 TEXT NOT NULL)`],
]) test(`refuses unsupported history ${label} without applying SQL`, async t => {
  const { db, raw } = await fixture(t); raw.exec(ddl);
  await assert.rejects(plan().apply(db), code('ERR_FSQLITE_MIGRATION_HISTORY')); assert.equal(exists(raw, 'items'), false);
});
for (const temp of [false, true]) test(`refuses ${temp ? 'TEMP' : 'permanent'} ledger triggers`, async t => {
  const { db, raw } = await fixture(t); await new FrankenMigrationPlan(base.slice(0, 1)).apply(db);
  raw.exec(`CREATE ${temp ? 'TEMP ' : ''}TRIGGER tamper AFTER INSERT ON main."${H}" BEGIN DELETE FROM "${H}"; END`);
  await assert.rejects(plan().apply(db), code('ERR_FSQLITE_MIGRATION_HISTORY'));
  assert.equal(exists(raw, 'item_name'), false);
});
test('TEMP shadow history cannot redirect main history reads or inserts', async t => {
  const { db, raw } = await fixture(t); raw.exec(`CREATE TEMP TABLE "${H}" (junk)`);
  await plan().apply(db); assert.equal(history(raw).length, 2);
  assert.equal(raw.prepare(`SELECT count(*) n FROM temp."${H}"`).get().n, 0);
});
test('migration which deletes reserved history is rolled back together with its data changes', async t => {
  const { db, raw } = await fixture(t); await plan().apply(db);
  const old = history(raw);
  await assert.rejects(new FrankenMigrationPlan([...base, migration(20, [
    "UPDATE items SET name='changed'", `DELETE FROM main."${H}"`,
  ])]).apply(db), code('ERR_FSQLITE_MIGRATION_HISTORY'));
  assert.deepEqual(history(raw), old); assert.equal(raw.prepare('SELECT name FROM items').get().name, 'Ada');
});
test('preserves native Rust migration ledger and PRAGMA user_version', async t => {
  const { db, raw } = await fixture(t);
  raw.exec("PRAGMA user_version=37; CREATE TABLE _schema_migrations(version); INSERT INTO _schema_migrations VALUES(99)");
  await plan().apply(db);
  assert.equal(raw.prepare('PRAGMA user_version').get().user_version, 37);
  assert.equal(raw.prepare('SELECT version FROM _schema_migrations').get().version, 99);
});
test('immutable plan captures arrays/getters once and never executes custom array iterators', async t => {
  const { db, raw } = await fixture(t); let reads = 0;
  const sql = ['CREATE TABLE captured(v)']; sql[Symbol.iterator] = () => { throw new Error('caller iterator'); };
  const source = [{ get version() { reads++; return 1; }, name: 'capture', statements: sql }];
  source[Symbol.iterator] = sql[Symbol.iterator]; const p = new FrankenMigrationPlan(source);
  sql[0] = 'CREATE TABLE wrong(v)'; source.length = 0;
  await p.apply(db); assert.equal(reads, 1); assert.equal(exists(raw, 'captured'), true); assert.equal(exists(raw, 'wrong'), false);
});
test('trigger semicolons, comments, escaped quotes and dollar identifiers stay intact', async t => {
  const { db, raw } = await fixture(t);
  const p = new FrankenMigrationPlan([migration(1, [
    '/* schema ; BEGIN */ CREATE TABLE item$store(v TEXT)', 'CREATE TABLE logs(v TEXT)',
    `CREATE /* ; */ TRIGGER log AFTER INSERT ON item$store BEGIN INSERT INTO logs VALUES('first; ''quoted'''); INSERT INTO logs VALUES(CASE WHEN new.v='x' THEN 'yes' ELSE 'no' END); END;`,
    "INSERT INTO item$store VALUES('x')",
  ])]);
  await p.apply(db); assert.equal(raw.prepare('SELECT count(*) n FROM logs').get().n, 2);
});
for (const sql of ['BEGIN', 'COMMIT', 'END', 'ROLLBACK', 'SAVEPOINT x', 'RELEASE x',
  'CREATE TABLE t(x); COMMIT; CREATE TABLE danger(v)', 'PRAGMA user_version=1',
  "ATTACH ':memory:' AS a", 'DETACH a', "VACUUM INTO 'file'", 'CREATE TEMP TABLE t(x)',
  'CREATE TEMPORARY VIEW v AS SELECT 1', 'CREATE VIRTUAL TABLE t USING fts5(v)',
  'INSERT INTO t VALUES(?)', 'INSERT INTO t VALUES($a::b(;COMMIT))', "INSERT INTO t VALUES('unterminated)",
  'CREATE TABLE t(v)\0', '-- only comments', 'SELECT 1']) {
  test(`preflight refuses ${JSON.stringify(sql)}`, () => {
    assert.throws(() => new FrankenMigrationPlan([migration(1, [sql])]), e => e instanceof FrankenMigrationError && e.code === 'ERR_FSQLITE_MIGRATION_SQL');
  });
}
for (const [label, input] of [
  ['zero', [migration(0, ['CREATE TABLE t(v)'])]], ['duplicate', [base[0], base[0]]],
  ['descending', [base[1], base[0]]], ['fraction', [migration(1.5, ['CREATE TABLE t(v)'])]],
  ['unsafe integer', [migration(2 ** 53, ['CREATE TABLE t(v)'])]], ['empty statements', [migration(1, [])]],
  ['empty name', [migration(1, ['CREATE TABLE t(v)'], ' ')]], ['hole', new Array(1)],
  ['statement hole', [migration(1, new Array(1))]], ['too many migrations', new Array(257)],
  ['too many statements', [migration(1, new Array(4097))]], ['wrong SQL type', [migration(1, [2])]],
  ['excess SQL', [migration(1, [' '.repeat(4 * 1024 * 1024 + 1)])]],
]) test(`invalid plan rejects ${label}`, () => assert.throws(() => new FrankenMigrationPlan(input), code('ERR_FSQLITE_MIGRATION_INPUT')));

test('empty plan is a true SQL no-op with no history creation', async t => {
  const { db, raw } = await fixture(t);
  assert.deepEqual(await new FrankenMigrationPlan([]).apply(db), { previousVersion: 0, currentVersion: 0, applied: [] });
  assert.equal(exists(raw, H), false);
});
test('pre-abort executes no SQL and timeout waits for in-flight migration SQL then rolls it back', async t => {
  const { db, raw, state } = await fixture(t); const controller = new AbortController(); controller.abort('stop');
  await assert.rejects(plan().apply(db, { signal: controller.signal })); assert.equal(state.attempts, 0);
  const started = deferred(), release = deferred();
  state.afterSql = async sql => { if (sql.startsWith('CREATE TABLE items')) { started.resolve(); await release.promise; } };
  let settled = false; const pending = plan().apply(db, { timeoutMs: 100 });
  pending.then(() => { settled = true; }, () => { settled = true; });
  await started.promise; await sleep(120); assert.equal(settled, false); release.resolve();
  await assert.rejects(pending, e => e.code === 'ERR_FSQLITE_TRANSACTION_TIMEOUT');
  assert.equal(exists(raw, 'items'), false); assert.equal(exists(raw, H), false);
});
test('connection authority is reserved before hashing; foreign SQL and another plan cannot overtake', async t => {
  const { db } = await fixture(t); const pending = plan().apply(db);
  await assert.rejects(db.execute('CREATE TABLE foreign_table(v)'), code('ERR_FSQLITE_TRANSACTION_OWNERSHIP'));
  await assert.rejects(plan().inspect(db), code('ERR_FSQLITE_TRANSACTION_OWNERSHIP')); await pending;
});
test('manual transaction is neither adopted nor rolled back after failed managed BEGIN', async t => {
  const { db, raw, state } = await fixture(t); raw.exec('BEGIN; CREATE TABLE manual(v)');
  await assert.rejects(plan().apply(db)); assert.equal(exists(raw, 'manual'), true);
  assert.deepEqual(state.events, ['begin']); raw.exec('ROLLBACK');
});
test('failed COMMIT and failed rollback preserve the original aggregate and never acknowledge migrations', async t => {
  const { db, state } = await fixture(t); const commit = busy(), rollback = new Error('rollback failed');
  state.beforeBoundary = action => { if (action === 'commit') throw commit; if (action === 'rollback') throw rollback; };
  await assert.rejects(plan().apply(db), e => e instanceof AggregateError && e.errors[0] === commit && e.errors[1] === rollback);
  assert.equal(state.closed, true); assert.equal(state.attempts, 1);
  const peer = new DatabaseSync(state.path); t.after(() => peer.close()); assert.equal(exists(peer, H), false);
});
test('bigint history/pragma values normalize safely', async t => {
  const { db, state } = await fixture(t); state.bigints = true;
  assert.equal((await plan().apply(db)).currentVersion, 10); assert.equal((await plan().inspect(db)).applied.length, 2);
});

test('qualified TEMP schema cannot be recorded as a persistent completed migration', async t => {
  const { db, raw } = await fixture(t);
  await assert.rejects(new FrankenMigrationPlan([migration(1, ['CREATE TABLE temp.transient(v)'])]).apply(db), code('ERR_FSQLITE_MIGRATION_HISTORY'));
  assert.equal(exists(raw, 'transient', 'temp'), false); assert.equal(exists(raw, H), false);
});
test('existing TEMP schema is preserved and transactional scratch tables may be created and removed', async t => {
  const { db, raw } = await fixture(t); raw.exec('CREATE TEMP TABLE existing(v)');
  await new FrankenMigrationPlan([migration(1, ['CREATE TABLE temp.scratch(v)', 'INSERT INTO temp.scratch VALUES(5)',
    'CREATE TABLE permanent AS SELECT v FROM temp.scratch', 'DROP TABLE temp.scratch'])]).apply(db);
  assert.equal(raw.prepare('SELECT v FROM permanent').get().v, 5); assert.equal(exists(raw, 'existing', 'temp'), true);
});
test('attached databases are refused before migration SQL or ledger creation', async t => {
  const { db, raw } = await fixture(t); raw.exec("ATTACH ':memory:' AS auxiliary");
  await assert.rejects(plan().apply(db), code('ERR_FSQLITE_MIGRATION_HISTORY'));
  assert.equal(exists(raw, H), false); assert.equal(exists(raw, 'items'), false);
});
test('table rebuild preserves rows and installs the new constraints atomically', async t => {
  const { db, raw } = await fixture(t);
  const p = new FrankenMigrationPlan([base[0], migration(2, [
    'CREATE TABLE new_items(id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, extra TEXT DEFAULT \'kept\')',
    'INSERT INTO new_items(id,name) SELECT id,name FROM items', 'DROP TABLE items', 'ALTER TABLE new_items RENAME TO items',
    'CREATE VIEW named AS SELECT name FROM items',
  ])]);
  await p.apply(db); assert.equal(raw.prepare('SELECT extra FROM items').get().extra, 'kept');
  assert.equal(raw.prepare('SELECT name FROM named').get().name, 'Ada');
  assert.throws(() => raw.prepare('INSERT INTO items(id,name) VALUES(2,?)').run('Ada'));
  assert.equal(raw.prepare('PRAGMA integrity_check').get().integrity_check, 'ok');
});
