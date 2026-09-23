// Production FK-scope SQL on actual Node SQLite. Not Rust/WASM certification.
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { withDeferredForeignKeys, ChangesetForeignKeyError } from '../src/changeset-foreign-keys.ts';

const SCHEMA = 'CREATE TABLE p(id INTEGER PRIMARY KEY, body TEXT); CREATE TABLE c(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id), data BLOB)';
class Target {
  constructor(path = ':memory:') {
    this.db = new DatabaseSync(path);
    this.db.exec('PRAGMA foreign_keys=ON');
    this.depth = 0; this.calls = 0; this.sql = []; this.failCommit = false; this.hook = null;
  }
  executor() {
    return {
      execute: async (sql, params = []) => {
        this.sql.push(sql);
        const overridden = await this.hook?.('execute', sql, params);
        if (overridden !== undefined) return overridden;
        return Number(this.db.prepare(sql).run(...params).changes);
      },
      query: async (sql, params = []) => {
        this.sql.push(sql);
        const overridden = await this.hook?.('query', sql, params);
        if (overridden !== undefined) return overridden;
        return { rowArrays: this.db.prepare(sql).all(...params).map(row => Object.values(row)) };
      },
    };
  }
  async transaction(work) {
    this.calls++;
    const depth = this.depth++, name = `test_${depth}`;
    this.db.exec(depth ? `SAVEPOINT ${name}` : 'BEGIN');
    try {
      const result = await work(this.executor());
      if (this.failCommit) throw new Error('injected commit failure');
      this.db.exec(depth ? `RELEASE ${name}` : 'COMMIT');
      return result;
    } catch (error) {
      this.db.exec(depth ? `ROLLBACK TO ${name}; RELEASE ${name}` : 'ROLLBACK');
      throw error;
    } finally { this.depth--; }
  }
  flag() { return this.db.prepare('PRAGMA defer_foreign_keys').get().defer_foreign_keys; }
  rows() { return this.db.prepare('SELECT c.id, c.pid, hex(c.data) AS data, hex(p.body) AS body FROM main.c AS c JOIN main.p AS p ON p.id=c.pid ORDER BY c.id').all().map(row => [row.id, row.pid, row.data, Buffer.from(row.body, 'hex').toString('utf8')]); }
}
function setup(t) { const target = new Target(); target.db.exec(SCHEMA); t.after(() => target.db.close()); return target; }
const code = suffix => error => error instanceof ChangesetForeignKeyError && error.code === `ERR_FSQLITE_FOREIGN_KEY_${suffix}`;
async function childFirst(tx) {
  await tx.execute('INSERT INTO main.c VALUES(1,7,?)', [new Uint8Array([0,255,1])]);
  await tx.execute('INSERT INTO main.p VALUES(7,?)', ['a\0🌍']);
  return 'committed';
}
function gate() { let release; const promise = new Promise(resolve => { release = resolve; }); return { promise, release }; }

test('child-first SQL fails normally and succeeds atomically with the new scope', async t => {
  const target = setup(t);
  await assert.rejects(target.transaction(childFirst), /FOREIGN KEY/);
  assert.deepEqual(target.rows(), []);
  assert.equal(await withDeferredForeignKeys(target).transaction(childFirst), 'committed');
  assert.deepEqual(target.rows(), [[1,7,'00FF01','a\0🌍']]);
  assert.equal(target.flag(), 0);
  assert.deepEqual(target.db.prepare('PRAGMA foreign_key_check').all(), []);
});

test('result agrees with actual native SQLite session application', async t => {
  const source = setup(t), receiver = setup(t), reference = setup(t);
  const session = source.db.createSession();
  source.db.exec('BEGIN; INSERT INTO p VALUES(7,\'native\'); INSERT INTO c VALUES(1,7,X\'00FF\'); COMMIT');
  const bytes = session.changeset(); session.close();
  assert.equal(reference.db.applyChangeset(bytes), true);
  await withDeferredForeignKeys(receiver).transaction(async tx => {
    await tx.execute("INSERT INTO c VALUES(1,7,X'00FF')");
    await tx.execute("INSERT INTO p VALUES(7,'native')");
  });
  assert.deepEqual(receiver.rows(), reference.rows());
});

for (const shape of ['delete', 'update', 'cycle', 'composite', 'without-rowid', 'restrict', 'cascade']) {
  test(`complete deferred transaction supports ${shape}`, async t => {
    const target = setup(t), db = target.db;
    if (shape === 'cycle') db.exec('CREATE TABLE a(id INTEGER PRIMARY KEY, b INTEGER REFERENCES b(id)); CREATE TABLE b(id INTEGER PRIMARY KEY, a INTEGER REFERENCES a(id))');
    if (shape === 'composite' || shape === 'without-rowid') db.exec(
      `CREATE TABLE cp(a TEXT COLLATE NOCASE, b INTEGER, PRIMARY KEY(a,b)) ${shape === 'without-rowid' ? 'WITHOUT ROWID' : ''}; CREATE TABLE cc(id PRIMARY KEY, a TEXT, b INTEGER, FOREIGN KEY(a,b) REFERENCES cp(a,b))`);
    if (shape === 'restrict') db.exec('CREATE TABLE r(id PRIMARY KEY, p INTEGER REFERENCES p(id) ON DELETE RESTRICT)');
    if (shape === 'cascade') db.exec('CREATE TABLE r(id PRIMARY KEY, p INTEGER REFERENCES p(id) ON DELETE CASCADE)');
    if (['delete','update','restrict','cascade'].includes(shape)) db.exec("INSERT INTO p VALUES(7,'before'); INSERT INTO c VALUES(1,7,X'00')");
    if (['restrict','cascade'].includes(shape)) db.exec('INSERT INTO r VALUES(1,7)');
    await withDeferredForeignKeys(target).transaction(async tx => {
      if (shape === 'cycle') { await tx.execute('INSERT INTO a VALUES(1,1)'); await tx.execute('INSERT INTO b VALUES(1,1)'); }
      else if (shape === 'composite' || shape === 'without-rowid') {
        await tx.execute("INSERT INTO cc VALUES(1,'UPPER',9223372036854775807)");
        await tx.execute("INSERT INTO cp VALUES('upper',9223372036854775807)");
      } else if (shape === 'update') {
        await tx.execute('UPDATE c SET pid=9 WHERE id=1'); await tx.execute('UPDATE p SET id=9 WHERE id=7');
      } else {
        await tx.execute('DELETE FROM p WHERE id=7'); await tx.execute('DELETE FROM c WHERE id=1');
        if (shape === 'restrict') await tx.execute('DELETE FROM r WHERE id=1');
      }
    });
    assert.deepEqual(db.prepare('PRAGMA foreign_key_check').all(), []);
    assert.equal(target.flag(), 0);
  });
}

test('unresolved FK rolls back rows, journal and receipt together', async t => {
  const target = setup(t);
  target.db.exec('CREATE TABLE receipt(id TEXT PRIMARY KEY); CREATE TABLE journal(info BLOB)');
  await assert.rejects(withDeferredForeignKeys(target).transaction(async tx => {
    await tx.execute('INSERT INTO c VALUES(1,999,NULL)');
    await tx.execute("INSERT INTO receipt VALUES('remote/1')");
    await tx.execute("INSERT INTO journal VALUES(X'00')");
  }), code('VIOLATION'));
  for (const table of ['c','receipt','journal']) assert.equal(target.db.prepare(`SELECT count(*) AS n FROM ${table}`).get().n, 0);
  assert.equal(target.flag(), 0);
});

test('child savepoint restores OFF before returning; parent writes remain immediate', async t => {
  const target = setup(t);
  await target.transaction(async () => {
    await withDeferredForeignKeys(target).transaction(childFirst);
    assert.equal(target.flag(), 0);
    await assert.rejects(target.executor().execute('INSERT INTO c VALUES(2,999,NULL)'), /FOREIGN KEY/);
    assert.equal(target.rows().length, 1);
  });
});

test('parent rollback also removes successfully released deferred child writes', async t => {
  const target = setup(t);
  await assert.rejects(target.transaction(async () => {
    await withDeferredForeignKeys(target).transaction(childFirst);
    throw Error('abort parent');
  }), /abort parent/);
  assert.deepEqual(target.rows(), []);
});

for (const setting of [0, 1]) {
  test(`failed nested callback preserves incoming pragma ${setting} and clean parent`, async t => {
    const target = setup(t);
    await target.transaction(async () => {
      target.db.exec(`PRAGMA defer_foreign_keys=${setting}`);
      await assert.rejects(withDeferredForeignKeys(target).transaction(async tx => {
        await tx.execute('INSERT INTO c VALUES(1,7,NULL)'); throw Error('hook failed');
      }), /hook failed/);
      assert.equal(target.flag(), setting);
      assert.deepEqual(target.db.prepare('SELECT * FROM c').all(), []);
      await target.executor().execute("INSERT INTO p VALUES(7,'parent retained')");
    });
    assert.equal(target.db.prepare('SELECT count(*) AS n FROM p').get().n, 1);
  });
}

for (const namespace of ['main', 'temp', "aux'🌍"]) {
  test(`pre-existing deferred violation in ${namespace} is rejected without erasing parent debt`, async t => {
    const target = setup(t), q = `"${namespace}"`;
    if (namespace !== 'main' && namespace !== 'temp') target.db.exec(`ATTACH ':memory:' AS ${q}`);
    if (namespace !== 'main') target.db.exec(`CREATE TABLE ${q}.p(id PRIMARY KEY); CREATE TABLE ${q}.c(id PRIMARY KEY, pid REFERENCES p(id))`);
    target.db.exec('BEGIN; PRAGMA defer_foreign_keys=ON'); target.depth = 1;
    try {
      target.db.exec(`INSERT INTO ${q}.c(id,pid) VALUES(8,99)`);
      let calls = 0;
      await assert.rejects(withDeferredForeignKeys(target).transaction(async () => { calls++; }), code('VIOLATION'));
      assert.equal(calls, 0); assert.equal(target.flag(), 1);
      assert.throws(() => target.db.exec('COMMIT'), /FOREIGN KEY/);
    } finally { target.db.exec('ROLLBACK'); target.depth = 0; }
  });
}

for (const namespace of ['temp', "aux'🌍"]) {
  test(`exit validation covers ${namespace}, not just the main changeset tables`, async t => {
    const target = setup(t), q = `"${namespace}"`;
    if (namespace !== 'temp') target.db.exec(`ATTACH ':memory:' AS ${q}`);
    target.db.exec(`CREATE TABLE ${q}.p(id PRIMARY KEY); CREATE TABLE ${q}.c(id PRIMARY KEY, pid REFERENCES p(id))`);
    await assert.rejects(withDeferredForeignKeys(target).transaction(async tx => {
      await childFirst(tx); await tx.execute(`INSERT INTO ${q}.c VALUES(1,999)`);
    }), code('VIOLATION'));
    assert.deepEqual(target.rows(), []); assert.equal(target.flag(), 0);
  });
}

test('foreign_keys OFF is rejected before callback and never changed', async t => {
  const target = setup(t); target.db.exec('PRAGMA foreign_keys=OFF');
  await assert.rejects(withDeferredForeignKeys(target).transaction(() => assert.fail('called')), code('STATE'));
  assert.equal(target.db.prepare('PRAGMA foreign_keys').get().foreign_keys, 0);
  assert.equal(target.flag(), 0);
});

test('callback pragma changes cannot bypass the final validation', async t => {
  const target = setup(t);
  await assert.rejects(withDeferredForeignKeys(target).transaction(async tx => {
    await tx.execute('INSERT INTO c VALUES(1,99,NULL)'); await tx.execute('PRAGMA defer_foreign_keys=OFF');
  }), code('STATE'));
  assert.equal(target.flag(), 0); assert.deepEqual(target.rows(), []);
});

test('callbacks cannot hide an FK violation by cycling the pragma off/on', async t => {
  const target = setup(t);
  await assert.rejects(withDeferredForeignKeys(target).transaction(async tx => {
    await tx.execute('INSERT INTO c VALUES(1,99,NULL)');
    await tx.execute('PRAGMA defer_foreign_keys=OFF'); await tx.execute('PRAGMA defer_foreign_keys=ON');
  }), code('VIOLATION'));
  assert.equal(target.db.prepare('SELECT count(*) AS n FROM c').get().n, 0);
});

test('pre-cancelled work is never admitted', async t => {
  const target = setup(t), abort = new AbortController(); abort.abort('stop');
  await assert.rejects(withDeferredForeignKeys(target).transaction(childFirst, { signal: abort.signal }), code('CANCELLED'));
  assert.equal(target.calls, 0);
});

test('mid-scope cancellation still restores state and rolls back every row', async t => {
  const target = setup(t), abort = new AbortController();
  await assert.rejects(withDeferredForeignKeys(target).transaction(async tx => {
    await tx.execute('INSERT INTO c VALUES(1,99,NULL)'); abort.abort('stop');
  }, { signal: abort.signal }), code('CANCELLED'));
  assert.equal(target.flag(), 0); assert.equal(target.db.prepare('SELECT count(*) AS n FROM c').get().n, 0);
});

test('deadline expiry still attempts restoration without racing active SQL', async t => {
  const target = setup(t);
  await assert.rejects(withDeferredForeignKeys(target).transaction(async tx => {
    await childFirst(tx); await delay(30);
  }, { timeoutMs: 10 }), code('TIMEOUT'));
  assert.equal(target.flag(), 0); assert.deepEqual(target.rows(), []);
});

test('unawaited admitted SQL drains before validation and restoration', async t => {
  const target = setup(t), entered = gate(), pending = gate();
  target.hook = async (kind, sql) => { if (kind === 'execute' && sql === 'INSERT INTO p VALUES(7,NULL)') { entered.release(); await pending.promise; } };
  let settled = false;
  const done = withDeferredForeignKeys(target).transaction(async tx => {
    await tx.execute('INSERT INTO c VALUES(1,7,NULL)'); void tx.execute('INSERT INTO p VALUES(7,NULL)');
  }).finally(() => { settled = true; });
  await entered.promise; await delay(0); assert.equal(settled, false); assert.equal(target.flag(), 1);
  pending.release(); await done; assert.equal(target.rows().length, 1); assert.equal(target.flag(), 0);
});

test('caught/unawaited SQL failure rejects the complete scope', async t => {
  const target = setup(t);
  await assert.rejects(withDeferredForeignKeys(target).transaction(async tx => {
    await childFirst(tx); await tx.execute('INSERT INTO p VALUES(7,NULL)').catch(() => {});
  }), /UNIQUE/);
  assert.deepEqual(target.rows(), []); assert.equal(target.flag(), 0);
});

test('executor cannot be used after the callback lifetime', async t => {
  const target = setup(t); let saved;
  await withDeferredForeignKeys(target).transaction(async tx => { saved = tx; });
  await assert.rejects(saved.execute('INSERT INTO c VALUES(1,99,NULL)'), code('STATE'));
  assert.equal(target.flag(), 0);
});

test('concurrent wrappers on the SAME target reject instead of racing connection pragmas', async t => {
  const target = setup(t), entered = gate(), pending = gate();
  const one = withDeferredForeignKeys(target), two = withDeferredForeignKeys(target);
  assert.equal(withDeferredForeignKeys(one), one);
  const done = one.transaction(async () => { entered.release(); await pending.promise; });
  await entered.promise;
  await assert.rejects(two.transaction(childFirst), code('BUSY'));
  pending.release(); await done;
  await two.transaction(childFirst);
});

test('different targets are independent, with no global writer serialization', async t => {
  const one = setup(t), two = setup(t), entered = gate(), pending = gate();
  const done = withDeferredForeignKeys(one).transaction(async () => { entered.release(); await pending.promise; });
  await entered.promise; await withDeferredForeignKeys(two).transaction(childFirst);
  assert.equal(two.rows().length, 1); pending.release(); await done;
});

test('restoration failure retains the original error and exposes cleanup uncertainty', async t => {
  const target = setup(t), original = Error('application failed');
  target.hook = async (kind, sql) => { if (kind === 'execute' && sql === 'PRAGMA defer_foreign_keys=OFF') throw Error('cleanup I/O'); };
  await assert.rejects(withDeferredForeignKeys(target).transaction(async tx => {
    await tx.execute('INSERT INTO c VALUES(1,99,NULL)'); throw original;
  }), error => {
    assert.ok(code('STATE')(error)); assert.equal(error.cause, original);
    assert.equal(error.cleanupErrors.length, 1); assert.match(error.cleanupErrors[0].message, /cleanup I\/O/); return true;
  });
  assert.equal(target.flag(), 0); // The actual top-level ROLLBACK resets it.
  assert.deepEqual(target.db.prepare('SELECT * FROM c').all(), []);
});

test('restoration failure after otherwise successful work also forces rollback', async t => {
  const target = setup(t);
  target.hook = async (kind, sql) => { if (kind === 'execute' && sql === 'PRAGMA defer_foreign_keys=OFF') return 0; };
  await assert.rejects(withDeferredForeignKeys(target).transaction(childFirst), code('STATE'));
  assert.deepEqual(target.rows(), []); assert.equal(target.flag(), 0);
});

test('commit failure is not relabeled as success or retried', async t => {
  const target = setup(t); target.failCommit = true;
  await assert.rejects(withDeferredForeignKeys(target).transaction(childFirst), /injected commit failure/);
  assert.equal(target.calls, 1); assert.deepEqual(target.rows(), []); assert.equal(target.flag(), 0);
});

test('file reopen retains rows and receipt after a lost acknowledgement', async t => {
  const path = join(mkdtempSync(join(tmpdir(), 'fsqlite-fk-')), 'replica.db');
  const first = new Target(path); first.db.exec(`${SCHEMA}; CREATE TABLE receipt(id TEXT PRIMARY KEY)`);
  await withDeferredForeignKeys(first).transaction(async tx => { await childFirst(tx); await tx.execute("INSERT INTO receipt VALUES('source/1')"); });
  first.db.close();
  const reopened = new Target(path); t.after(() => reopened.db.close());
  let applied = 0;
  const replayed = await withDeferredForeignKeys(reopened).transaction(async tx => {
    if ((await tx.query("SELECT 1 FROM receipt WHERE id='source/1'")).rowArrays.length) return true;
    applied++; await childFirst(tx); return false;
  });
  assert.equal(replayed, true); assert.equal(applied, 0); assert.equal(reopened.rows().length, 1);
});

for (const [sql, rowArrays] of [
  ['PRAGMA foreign_keys', []], ['PRAGMA defer_foreign_keys', [[2]]],
  ['SELECT name FROM pragma_database_list() LIMIT 128', []],
  ['SELECT name FROM pragma_database_list() LIMIT 128', [[null]]],
  ['SELECT name FROM pragma_database_list() LIMIT 128', Array.from({ length: 128 }, (_, i) => [`db${i}`])],
  ['SELECT name FROM pragma_database_list() LIMIT 128', [['main'], ['MAIN']]],
  ['SELECT 1 FROM pragma_foreign_key_check(NULL, ?) LIMIT 1', [[0]]],
]) {
  test(`invalid SQL metadata is refused before writes (${sql}/${JSON.stringify(rowArrays).slice(0,30)})`, async t => {
    const target = setup(t);
    target.hook = async (kind, query) => kind === 'query' && query === sql ? { rowArrays } : undefined;
    await assert.rejects(withDeferredForeignKeys(target).transaction(() => assert.fail('must not run')), code('STATE'));
    assert.equal(target.flag(), 0);
  });
}

for (const options of [{ timeoutMs: 0 }, { timeoutMs: Infinity }, { timeoutMs: 1.5 }, { signal: {} }]) {
  test(`invalid control ${JSON.stringify(options)} rejects before admission`, async t => {
    const target = setup(t);
    await assert.rejects(withDeferredForeignKeys(target).transaction(childFirst, options), code('INPUT'));
    assert.equal(target.calls, 0);
  });
}

test('pre-existing declared-deferred debt with pragma OFF is not erased by rejection', async t => {
  const target = setup(t);
  target.db.exec('CREATE TABLE d(id PRIMARY KEY, pid REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED); BEGIN; INSERT INTO d VALUES(1,999)'); target.depth = 1;
  try {
    assert.equal(target.flag(), 0);
    await assert.rejects(withDeferredForeignKeys(target).transaction(childFirst), code('VIOLATION'));
    assert.equal(target.flag(), 0);
    assert.throws(() => target.db.exec('COMMIT'), /FOREIGN KEY/);
  } finally { target.db.exec('ROLLBACK'); target.depth = 0; }
});

test('successful nested work preserves the caller ON mode and subsequent COMMIT enforcement', async t => {
  const target = setup(t);
  await assert.rejects(target.transaction(async tx => {
    target.db.exec('PRAGMA defer_foreign_keys=ON');
    await withDeferredForeignKeys(target).transaction(childFirst);
    assert.equal(target.flag(), 1);
    await tx.execute('INSERT INTO c VALUES(2,999,NULL)');
  }), /FOREIGN KEY/);
  assert.deepEqual(target.rows(), []); assert.equal(target.flag(), 0);
});

test('cancellation drains an in-flight write before rollback and releases admission afterwards', async t => {
  const target = setup(t), entered = gate(), pending = gate(), abort = new AbortController();
  const wrapped = withDeferredForeignKeys(target);
  target.hook = async (kind, sql) => {
    if (kind === 'execute' && sql === 'INSERT INTO main.c VALUES(1,7,?)') { entered.release(); await pending.promise; }
  };
  let settled = false;
  const result = wrapped.transaction(childFirst, { signal: abort.signal });
  void result.then(() => { settled = true; }, () => { settled = true; });
  await entered.promise; abort.abort('stop'); await delay(0);
  assert.equal(settled, false); assert.equal(target.flag(), 1);
  await assert.rejects(wrapped.transaction(childFirst), code('BUSY'));
  pending.release(); await assert.rejects(result, code('CANCELLED'));
  assert.equal(target.flag(), 0); assert.deepEqual(target.rows(), []);
  target.hook = null; await wrapped.transaction(childFirst);
});

test('target that refuses cleanup on cancellation reports STATE rather than claiming restoration', async t => {
  const target = setup(t), abort = new AbortController();
  target.hook = async (kind, sql) => {
    if (kind === 'execute' && abort.signal.aborted && sql.startsWith('PRAGMA defer_foreign_keys=')) throw Error('target already cancelled');
  };
  await assert.rejects(withDeferredForeignKeys(target).transaction(async tx => {
    await tx.execute('INSERT INTO c VALUES(1,99,NULL)'); abort.abort();
  }, { signal: abort.signal }), error => {
    assert.ok(code('STATE')(error)); assert.ok(code('CANCELLED')(error.cause));
    assert.equal(error.cleanupErrors.length, 1); return true;
  });
  assert.equal(target.flag(), 0); // Target's actual ROLLBACK, not helper restoration.
});

test('unknown deferral pragma that silently ignores ON is rejected before DML', async t => {
  const target = setup(t);
  target.hook = async (kind, sql) => kind === 'execute' && sql === 'PRAGMA defer_foreign_keys=ON' ? 0 : undefined;
  await assert.rejects(withDeferredForeignKeys(target).transaction(() => assert.fail('called')), code('STATE'));
  assert.equal(target.flag(), 0);
});

test('a changed schema roster is rejected without claiming ATTACH was rolled back', async t => {
  const target = setup(t);
  await assert.rejects(withDeferredForeignKeys(target).transaction(async tx => {
    await childFirst(tx); await tx.execute("ATTACH ':memory:' AS added");
  }), code('STATE'));
  assert.deepEqual(target.rows(), []); assert.equal(target.flag(), 0);
  // SQLite ATTACH itself is connection state, not rolled-back data. The API
  // forbids it in callbacks and does not pretend to undo external side effects.
  assert.ok(target.db.prepare('PRAGMA database_list').all().some(row => row.name === 'added'));
});

test('SQL NULL and bigint pragma results work without changing global FK policy', async t => {
  const target = setup(t);
  target.hook = async (kind, sql) => {
    if (kind === 'query' && ['PRAGMA foreign_keys','PRAGMA defer_foreign_keys'].includes(sql)) {
      return { rowArrays: [[BigInt(Object.values(target.db.prepare(sql).get())[0])]] };
    }
  };
  await withDeferredForeignKeys(target).transaction(async tx => {
    await tx.execute('INSERT INTO c VALUES(1,NULL,NULL)');
  });
  assert.equal(target.db.prepare('SELECT count(*) AS n FROM c').get().n, 1); assert.equal(target.flag(), 0);
});

test('a post-commit lost response is not retried or reported as rolled back', async t => {
  const target = setup(t), original = target.transaction;
  target.transaction = async function(work, options) { await original.call(this, work, options); throw Error('lost committed response'); };
  await assert.rejects(withDeferredForeignKeys(target).transaction(childFirst), /lost committed response/);
  assert.equal(target.calls, 1); assert.equal(target.rows().length, 1); assert.equal(target.flag(), 0);
});
