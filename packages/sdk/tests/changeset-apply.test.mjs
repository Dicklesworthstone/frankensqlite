import assert from 'node:assert/strict';
import { test } from 'node:test';
import { DatabaseSync, constants } from 'node:sqlite';
import { applyChangeset, ChangesetApplyError } from '../src/changeset-apply.ts';
import { decodeChangeset, encodeChangeset, invertChangeset } from '../src/changeset-codec.ts';

// Real SQLite SQL/session oracle, not a fake engine and not WASM certification.
// The adapter implements only the ordinary owned-transaction SQL contract.
class SqlTarget {
  db = new DatabaseSync(':memory:');
  log = [];
  scopes = 0;
  afterExecute;
  afterQuery;
  constructor(sql = '') { this.db.exec(sql); }
  async execute(sql, params = []) {
    this.log.push(sql);
    const result = this.db.prepare(sql).run(...params);
    this.afterExecute?.(sql, params);
    return Number(result.changes);
  }
  async query(sql, params = []) {
    this.log.push(sql);
    const statement = this.db.prepare(sql);
    statement.setReadBigInts(true);
    const columns = statement.columns().map(column => column.name);
    const rowArrays = statement.all(...params).map(row => columns.map(column => row[column]));
    this.afterQuery?.(sql, rowArrays);
    return { rowArrays };
  }
  async transaction(work, options = {}) {
    const name = `apply_scope_${++this.scopes}`;
    this.db.exec(`SAVEPOINT ${name}`);
    try {
      const result = await work(this);
      if (options.signal?.aborted) throw options.signal.reason;
      this.db.exec(`RELEASE ${name}`);
      return result;
    } catch (error) {
      this.db.exec(`ROLLBACK TO ${name}; RELEASE ${name}`);
      throw error;
    }
  }
  rows(sql = 'SELECT * FROM t ORDER BY id') {
    const statement = this.db.prepare(sql);
    statement.setReadBigInts(true);
    return statement.all().map(row => ({ ...row }));
  }
}
const quote = name => `"${name.replaceAll('"', '""')}"`;
const insert = (...values) => ({ operation: 'insert', indirect: false, new: values });
const update = (old, next) => ({ operation: 'update', indirect: false, old, new: next });
const remove = (...values) => ({ operation: 'delete', indirect: false, old: values });
const wire = (changes, name = 't', primaryKey = [1, 0]) => encodeChangeset([{ name, primaryKey, changes }]);
const options = { tables: ['t'] };
const code = expected => error => error instanceof ChangesetApplyError && error.code === expected;
function target(t, sql = '') {
  const instance = new SqlTarget(sql);
  t.after(() => instance.db.close());
  return instance;
}

// SQL authored with the session extension is applied by BOTH independent paths.
test('native session INSERT/UPDATE/DELETE round-trip and inversion', async t => {
  const sql = "CREATE TABLE t(id INTEGER PRIMARY KEY, v, untouched TEXT); INSERT INTO t VALUES(1,'old','keep'),(2,'gone','stay');";
  const author = target(t, sql), actual = target(t, sql), oracle = target(t, sql);
  const session = author.db.createSession();
  author.db.exec("UPDATE t SET v='new' WHERE id=1; DELETE FROM t WHERE id=2; INSERT INTO t VALUES(3,42.0,'added');");
  const bytes = session.changeset();
  assert.equal(oracle.db.applyChangeset(bytes), true);
  const result = await applyChangeset(actual, bytes, options);
  assert.equal(result.applied, 3);
  assert.equal(result.omitted, 0);
  assert.deepEqual(actual.rows('SELECT *,typeof(v) AS storage FROM t ORDER BY id'), oracle.rows('SELECT *,typeof(v) AS storage FROM t ORDER BY id'));
  await applyChangeset(actual, invertChangeset(bytes), options);
  assert.deepEqual(actual.rows(), [{ id: 1n, v: 'old', untouched: 'keep' }, { id: 2n, v: 'gone', untouched: 'stay' }]);
  session.close();
});

test('composite WITHOUT ROWID keys retain physical column order', async t => {
  const sql = "CREATE TABLE t(b TEXT, a INTEGER, v BLOB, PRIMARY KEY(a,b)) WITHOUT ROWID; INSERT INTO t VALUES('x',1,x'0001');";
  const author = target(t, sql), actual = target(t, sql), oracle = target(t, sql);
  const session = author.db.createSession();
  author.db.exec("UPDATE t SET v=x'00ff' WHERE a=1; INSERT INTO t VALUES('y',2,x'');");
  const bytes = session.changeset();
  assert.deepEqual(decodeChangeset(bytes)[0].primaryKey, [2, 1, 0]);
  assert.equal(oracle.db.applyChangeset(bytes), true);
  await applyChangeset(actual, bytes, options);
  assert.deepEqual(actual.rows('SELECT * FROM t ORDER BY a,b'), oracle.rows('SELECT * FROM t ORDER BY a,b'));
  session.close();
});

test('all storage classes, signed 64-bit extremes and integral REALs survive', async t => {
  const actual = target(t, 'CREATE TABLE t(id INTEGER PRIMARY KEY,v)');
  const values = [null, -(1n << 63n), (1n << 63n) - 1n, 1, 1.5, Infinity, -Infinity, 'hello\0世界', new Uint8Array([0, 255]), new Uint8Array()];
  const bytes = wire(values.map((value, i) => insert(BigInt(i + 1), value)));
  await applyChangeset(actual, bytes, options);
  const stored = actual.rows('SELECT v,typeof(v) AS storage FROM t ORDER BY id');
  assert.deepEqual(stored.map(row => row.storage), ['null', 'integer', 'integer', 'real', 'real', 'real', 'real', 'text', 'blob', 'blob']);
  // Node 22's TEXT result conversion truncates at NUL; inspect actual SQL bytes
  // through BLOB conversion rather than treating that host limitation as data loss.
  const textBytes = actual.rows('SELECT CAST(v AS BLOB) AS bytes FROM t WHERE id=8')[0].bytes;
  stored[7].v = new TextDecoder().decode(textBytes);
  assert.deepEqual(stored.map(row => row.v), values);
});

test('UPDATE checks only changed fields; undefined differs from SQL NULL', async t => {
  const actual = target(t, "CREATE TABLE t(id PRIMARY KEY,v,local); INSERT INTO t VALUES(1,NULL,'receiver-only');");
  await applyChangeset(actual, wire([update([1n, null, undefined], [undefined, 'new', undefined])], 't', [1, 0, 0]), options);
  assert.deepEqual(actual.rows(), [{ id: 1n, v: 'new', local: 'receiver-only' }]);
});

test('trailing target columns use defaults and are excluded from before-images', async t => {
  const actual = target(t, "CREATE TABLE t(id PRIMARY KEY,v,extra TEXT DEFAULT 'local');");
  await applyChangeset(actual, wire([insert(1n, 'one')]), options);
  actual.db.exec("UPDATE t SET extra='receiver' WHERE id=1");
  await applyChangeset(actual, wire([update([1n, 'one'], [undefined, 'two'])]), options);
  assert.deepEqual(actual.rows(), [{ id: 1n, v: 'two', extra: 'receiver' }]);
  await applyChangeset(actual, wire([remove(1n, 'two')]), options);
  assert.deepEqual(actual.rows(), []);
});

for (const [label, expected, actualValue, nativeKind] of [
  ['before-image equality honors NOCASE', "'A'", "'a'", null],
  ['numeric text is not an integer', "'1'", '1', constants.SQLITE_CHANGESET_DATA],
  ['integer and equal REAL compare numerically', '1', '1.0', null],
  ['NULL differs from omitted/empty text', 'NULL', "''", constants.SQLITE_CHANGESET_DATA],
  ['blobs compare bytes, not object identity', "x'00ff'", "x'00ff'", null],
]) {
  test(label, async t => {
    const definition = 'CREATE TABLE t(id INTEGER PRIMARY KEY,v COLLATE NOCASE);';
    const author = target(t, definition + `INSERT INTO t VALUES(1,${expected});`);
    const actual = target(t, definition + `INSERT INTO t VALUES(1,${actualValue});`);
    const oracle = target(t, definition + `INSERT INTO t VALUES(1,${actualValue});`);
    const session = author.db.createSession();
    author.db.exec("UPDATE t SET v='changed' WHERE id=1");
    const bytes = session.changeset(), conflicts = [];
    oracle.db.applyChangeset(bytes, { onConflict(kind) { conflicts.push(kind); return constants.SQLITE_CHANGESET_ABORT; } });
    if (nativeKind === null) {
      assert.deepEqual(conflicts, []);
      await applyChangeset(actual, bytes, options);
    } else {
      assert.deepEqual(conflicts, [nativeKind]);
      await assert.rejects(applyChangeset(actual, bytes, options), error => code('ERR_FSQLITE_CHANGESET_CONFLICT')(error) && error.conflict.kind === 'data');
    }
    assert.deepEqual(actual.rows(), oracle.rows());
    session.close();
  });
}

test('key lookup retains declared collation', async t => {
  const actual = target(t, "CREATE TABLE t(id TEXT PRIMARY KEY COLLATE NOCASE,v); INSERT INTO t VALUES('a','old');");
  await applyChangeset(actual, wire([update(['A', 'old'], [undefined, 'new'])]), options);
  assert.deepEqual(actual.rows(), [{ id: 'a', v: 'new' }]);
});

for (const [sourceType, targetType, old, current, conflict] of [
  ['', 'TEXT', '1', "'1'", false],
  ['', 'INTEGER', "'1'", '1', false],
  ['', 'TEXT', '1.0', "'1'", true],
  ['', 'TEXT', '1.0', "'1.0'", false],
  ['TEXT COLLATE RTRIM', 'TEXT COLLATE RTRIM', "'A '", "'A'", false],
]) {
  test(`native target affinity: ${sourceType || 'NONE'} ${old} -> ${targetType} ${current}`, async t => {
    const author = target(t, `CREATE TABLE t(id PRIMARY KEY,v ${sourceType}); INSERT INTO t VALUES(1,${old});`);
    const actual = target(t, `CREATE TABLE t(id PRIMARY KEY,v ${targetType}); INSERT INTO t VALUES(1,${current});`);
    const oracle = target(t, `CREATE TABLE t(id PRIMARY KEY,v ${targetType}); INSERT INTO t VALUES(1,${current});`);
    const session = author.db.createSession();
    author.db.exec("UPDATE t SET v='changed'");
    const bytes = session.changeset(), seen = [];
    oracle.db.applyChangeset(bytes, { onConflict(kind) { seen.push(kind); return constants.SQLITE_CHANGESET_ABORT; } });
    assert.deepEqual(seen, conflict ? [constants.SQLITE_CHANGESET_DATA] : []);
    if (conflict) await assert.rejects(applyChangeset(actual, bytes, options), code('ERR_FSQLITE_CHANGESET_CONFLICT'));
    else await applyChangeset(actual, bytes, options);
    assert.deepEqual(actual.rows(), oracle.rows());
    session.close();
  });
}

test('abort rolls back earlier changes, even across tables', async t => {
  const actual = target(t, "CREATE TABLE t(id PRIMARY KEY,v); CREATE TABLE other(id PRIMARY KEY,v); INSERT INTO other VALUES(2,'local');");
  const bytes = encodeChangeset([
    { name: 't', primaryKey: [1, 0], changes: [insert(1n, 'first')] },
    { name: 'other', primaryKey: [1, 0], changes: [insert(2n, 'remote')] },
  ]);
  await assert.rejects(applyChangeset(actual, bytes, { tables: ['t', 'other'] }), error =>
    code('ERR_FSQLITE_CHANGESET_CONFLICT')(error) && error.conflict.kind === 'conflict' && error.conflict.changeIndex === 1);
  assert.deepEqual(actual.rows(), []);
  assert.deepEqual(actual.rows('SELECT * FROM other'), [{ id: 2n, v: 'local' }]);
});

test('explicit omission handles DATA, NOTFOUND and primary-key conflicts', async t => {
  const actual = target(t, "CREATE TABLE t(id PRIMARY KEY,v); INSERT INTO t VALUES(1,'local');");
  const seen = [];
  const bytes = wire([insert(1n, 'remote'), remove(99n, 'missing'), update([1n, 'old'], [undefined, 'remote']), insert(2n, 'new')]);
  const result = await applyChangeset(actual, bytes, { ...options, async onConflict(conflict) {
    seen.push(conflict.kind);
    assert.ok(Object.isFrozen(conflict));
    assert.ok(Object.isFrozen(conflict.change));
    assert.deepEqual(conflict.columns, ['id', 'v']);
    return 'omit';
  } });
  assert.equal(result.applied, 1);
  assert.equal(result.omitted, 3);
  assert.deepEqual(seen, ['conflict', 'not-found', 'data']);
  assert.deepEqual(actual.rows(), [{ id: 1n, v: 'local' }, { id: 2n, v: 'new' }]);
});

test('throwing and invalid conflict callbacks do not leave partial application', async t => {
  const actual = target(t, "CREATE TABLE t(id PRIMARY KEY,v); INSERT INTO t VALUES(2,'local');");
  const bytes = wire([insert(1n, 'new'), insert(2n, 'conflict')]), failure = new Error('callback failed');
  await assert.rejects(applyChangeset(actual, bytes, { ...options, onConflict() { throw failure; } }), error => error === failure);
  await assert.rejects(applyChangeset(actual, bytes, { ...options, onConflict: () => 'replace' }), code('ERR_FSQLITE_CHANGESET_INPUT'));
  assert.deepEqual(actual.rows(), [{ id: 2n, v: 'local' }]);
});

test('schema IGNORE/REPLACE cannot silently override INSERT/UPDATE application', async t => {
  const actual = target(t, "CREATE TABLE t(id PRIMARY KEY,v UNIQUE ON CONFLICT REPLACE); INSERT INTO t VALUES(8,'occupied'),(9,'other');");
  let callbacks = 0;
  const policy = { ...options, onConflict() { callbacks++; return 'omit'; } };
  await assert.rejects(applyChangeset(actual, wire([insert(1n, 'first'), insert(2n, 'occupied')]), policy), /UNIQUE constraint failed/);
  await assert.rejects(applyChangeset(actual, wire([insert(1n, 'first'), update([9n, 'other'], [undefined, 'occupied'])]), policy), /UNIQUE constraint failed/);
  assert.equal(callbacks, 0);
  assert.deepEqual(actual.rows(), [{ id: 8n, v: 'occupied' }, { id: 9n, v: 'other' }]);
});

test('RAISE(IGNORE) cannot report an unapplied row as applied', async t => {
  const actual = target(t, "CREATE TABLE t(id PRIMARY KEY,v); CREATE TRIGGER no_two BEFORE INSERT ON t WHEN NEW.id=2 BEGIN SELECT RAISE(IGNORE); END;");
  await assert.rejects(applyChangeset(actual, wire([insert(1n, 'first'), insert(2n, 'ignored')]), options), code('ERR_FSQLITE_CHANGESET_RESULT'));
  assert.deepEqual(actual.rows(), []);
});

test('foreign-key failure retains ordinary enforcement and rolls back', async t => {
  const actual = target(t, 'PRAGMA foreign_keys=ON; CREATE TABLE parent(id PRIMARY KEY); CREATE TABLE t(id PRIMARY KEY,v REFERENCES parent(id)); INSERT INTO parent VALUES(1);');
  await assert.rejects(applyChangeset(actual, wire([insert(1n, 1n), insert(2n, 2n)]), options), /FOREIGN KEY constraint failed/);
  assert.deepEqual(actual.rows(), []);
  assert.equal(actual.db.prepare('PRAGMA foreign_keys').get().foreign_keys, 1);
});

for (const definition of [
  'CREATE TABLE base(id PRIMARY KEY,v); CREATE VIEW t AS SELECT * FROM base;',
  'CREATE TABLE t(id,v PRIMARY KEY);',
  'CREATE TABLE t(id,v);',
  'CREATE TABLE t(id PRIMARY KEY,v,extra GENERATED ALWAYS AS (v));',
  'CREATE TABLE t(id,v,extra,PRIMARY KEY(id,extra));',
  'CREATE VIRTUAL TABLE t USING fts5(v);',
]) {
  test(`reject incompatible schema: ${definition}`, async t => {
    const actual = target(t, definition);
    await assert.rejects(applyChangeset(actual, wire([insert(1n, 'one')]), options), code('ERR_FSQLITE_CHANGESET_SCHEMA'));
    assert.equal(actual.log.some(sql => /^(INSERT|UPDATE|DELETE)/.test(sql)), false);
  });
}

test('preflight every table before any application write', async t => {
  const actual = target(t, 'CREATE TABLE t(id PRIMARY KEY,v);');
  const bytes = encodeChangeset([{ name: 't', primaryKey: [1, 0], changes: [insert(1n, 'one')] },
    { name: 'missing', primaryKey: [1, 0], changes: [insert(2n, 'two')] }]);
  await assert.rejects(applyChangeset(actual, bytes, { tables: ['t', 'missing'] }), code('ERR_FSQLITE_CHANGESET_SCHEMA'));
  assert.equal(actual.log.some(sql => /^INSERT/.test(sql)), false);
});

test('quoted table/column identifiers are not SQL and temp cannot shadow main', async t => {
  const name = 't\"; DROP TABLE secret; --', column = 'v\"quoted';
  const actual = target(t, `CREATE TABLE ${quote(name)}(id PRIMARY KEY,${quote(column)}); CREATE TABLE secret(x); CREATE TEMP TABLE ${quote(name)}(id PRIMARY KEY,${quote(column)});`);
  await applyChangeset(actual, wire([insert(1n, "'value';--")], name), { tables: [name] });
  assert.equal(actual.db.prepare(`SELECT count(*) AS n FROM main.${quote(name)}`).get().n, 1);
  assert.equal(actual.db.prepare(`SELECT count(*) AS n FROM temp.${quote(name)}`).get().n, 0);
  assert.equal(actual.db.prepare('SELECT count(*) AS n FROM secret').get().n, 0);
});

test('malformed input, limits, and unauthorized targets reject before SQL', async t => {
  const actual = target(t, 'CREATE TABLE t(id PRIMARY KEY,v);');
  const bytes = wire([insert(1n, 'one')]);
  await assert.rejects(applyChangeset(actual, bytes, { tables: [] }), code('ERR_FSQLITE_CHANGESET_INPUT'));
  await assert.rejects(applyChangeset(actual, bytes.subarray(0, bytes.length - 1), options), /Truncated/);
  await assert.rejects(applyChangeset(actual, bytes, { ...options, limits: { maxBytes: 2 } }), /maxBytes/);
  await assert.rejects(applyChangeset(actual, bytes, { tables: ['t', 'T'] }), code('ERR_FSQLITE_CHANGESET_INPUT'));
  await assert.rejects(applyChangeset(actual, bytes, { tables: ['sqlite_schema'] }), code('ERR_FSQLITE_CHANGESET_INPUT'));
  assert.equal(actual.scopes, 0);
  assert.deepEqual(actual.log, []);
});

test('input buffers and authorization are captured before asynchronous admission', async t => {
  const actual = target(t, 'CREATE TABLE t(id PRIMARY KEY,v);');
  const bytes = wire([insert(1n, new Uint8Array([0, 255]))]), names = ['t'];
  const promise = applyChangeset(actual, bytes, { tables: names });
  bytes.fill(0); names.length = 0;
  await promise;
  assert.deepEqual(actual.rows()[0].v, new Uint8Array([0, 255]));
});

test('cancellation before admission or after a write never commits a prefix', async t => {
  const actual = target(t, 'CREATE TABLE t(id PRIMARY KEY,v);');
  const bytes = wire([insert(1n, 'one'), insert(2n, 'two')]);
  const before = new AbortController(); before.abort('before');
  await assert.rejects(applyChangeset(actual, bytes, { ...options, signal: before.signal }), code('ERR_FSQLITE_CHANGESET_CANCELLED'));
  assert.equal(actual.scopes, 0);
  const during = new AbortController();
  actual.afterExecute = () => during.abort('during');
  await assert.rejects(applyChangeset(actual, bytes, { ...options, signal: during.signal }), code('ERR_FSQLITE_CHANGESET_CANCELLED'));
  assert.deepEqual(actual.rows(), []);
});

test('deadline expires across an awaited resolver and rolls back prior writes', async t => {
  const actual = target(t, "CREATE TABLE t(id PRIMARY KEY,v); INSERT INTO t VALUES(2,'local');");
  await assert.rejects(applyChangeset(actual, wire([insert(1n, 'one'), insert(2n, 'two')]), {
    ...options, timeoutMs: 50, async onConflict() { await new Promise(resolve => setTimeout(resolve, 70)); return 'omit'; },
  }), code('ERR_FSQLITE_CHANGESET_TIMEOUT'));
  assert.deepEqual(actual.rows(), [{ id: 2n, v: 'local' }]);
});

test('nested application rolls back independently of the surrounding transaction', async t => {
  const actual = target(t, 'CREATE TABLE t(id PRIMARY KEY,v);');
  await actual.transaction(async tx => {
    await tx.execute('INSERT INTO t VALUES(8,?)', ['outer']);
    await assert.rejects(applyChangeset(tx, wire([insert(1n, 'first'), insert(8n, 'conflict')]), options), code('ERR_FSQLITE_CHANGESET_CONFLICT'));
    await applyChangeset(tx, wire([insert(2n, 'second')]), options);
  });
  assert.deepEqual(actual.rows(), [{ id: 2n, v: 'second' }, { id: 8n, v: 'outer' }]);
});

test('malformed/multirow SQL result cannot authorize a write', async t => {
  const actual = target(t, 'CREATE TABLE t(id PRIMARY KEY,v);');
  actual.afterQuery = (sql, rows) => { if (sql.startsWith('SELECT CASE')) rows.push([1n], [1n]); };
  await assert.rejects(applyChangeset(actual, wire([insert(1n, 'one')]), options), code('ERR_FSQLITE_CHANGESET_RESULT'));
  assert.deepEqual(actual.rows(), []);
});

test('wide before-images use bounded expression depth instead of linear AND trees', async t => {
  const count = 1100, columns = Array.from({ length: count }, (_, i) => `c${i}`);
  const actual = target(t, `CREATE TABLE t(${columns.map((name, i) => quote(name) + (i === 0 ? ' PRIMARY KEY' : '')).join(',')});`);
  const row = columns.map((_, i) => BigInt(i)), pk = columns.map((_, i) => i === 0 ? 1 : 0);
  await applyChangeset(actual, wire([insert(...row)], 't', pk), options);
  await applyChangeset(actual, wire([remove(...row)], 't', pk), options);
  assert.equal(actual.db.prepare('SELECT count(*) AS n FROM t').get().n, 0);
});

test('20 deterministic native workloads agree with SQLite application', async t => {
  for (let seed = 0; seed < 20; seed++) {
    const sql = 'CREATE TABLE t(id INTEGER PRIMARY KEY,v);' +
      Array.from({ length: 12 }, (_, i) => `INSERT INTO t VALUES(${i},'old${i}');`).join('');
    const author = target(t, sql), actual = target(t, sql), oracle = target(t, sql);
    const session = author.db.createSession();
    for (let i = 0; i < 12; i++) {
      if ((i + seed) % 3 === 0) author.db.prepare('DELETE FROM t WHERE id=?').run(i);
      else author.db.prepare('UPDATE t SET v=? WHERE id=?').run(`new${seed}:${i}`, i);
    }
    author.db.prepare('INSERT INTO t VALUES(?,?)').run(100 + seed, BigInt(seed));
    const bytes = session.changeset();
    assert.equal(oracle.db.applyChangeset(bytes), true);
    await applyChangeset(actual, bytes, options);
    assert.deepEqual(actual.rows(), oracle.rows(), `seed ${seed}`);
    session.close();
  }
});
